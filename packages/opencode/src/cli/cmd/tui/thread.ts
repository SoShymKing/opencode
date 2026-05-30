import { cmd } from "@/cli/cmd/cmd"
import { Rpc } from "@/util/rpc"
import { type rpc } from "./worker"
import path from "path"
import { fileURLToPath } from "url"
import { UI } from "@/cli/ui"
import * as Log from "@opencode-ai/core/util/log"
import { errorMessage } from "@/util/error"
import { withTimeout } from "@/util/timeout"
import { withNetworkOptions, resolveNetworkOptionsNoConfig } from "@/cli/network"
import { Filesystem } from "@/util/filesystem"
import type { GlobalEvent } from "@opencode-ai/sdk/v2"
import type { EventSource } from "./context/sdk"
import { win32DisableProcessedInput, win32InstallCtrlCGuard } from "./win32"
import { writeHeapSnapshot } from "v8"
import { TuiConfig } from "./config/tui"
import {
  OPENCODE_PROCESS_ROLE,
  OPENCODE_RUN_ID,
  ensureRunID,
  sanitizedProcessEnv,
} from "@opencode-ai/core/util/opencode-process"
import { validateSession } from "./validate-session"

declare global {
  const OPENCODE_WORKER_PATH: string
}

type WorkerFetchInput = Parameters<typeof rpc.fetch>[0]
type WorkerFetchOutput = Awaited<ReturnType<typeof rpc.fetch>>
type WorkerFetchCallOutput = WorkerFetchOutput | Promise<WorkerFetchOutput>
type WorkerFetchClient = {
  call(method: "fetch", input: WorkerFetchInput): Promise<WorkerFetchCallOutput>
}
type WorkerFetchClientInput = WorkerFetchClient | (() => WorkerFetchClient)
type AbortTimeoutInput = { sessionID: string; requestID?: string; error: unknown }
type WorkerFetchOptions = {
  abortTimeout?: number
  onAbortTimeout?: (input: AbortTimeoutInput) => void | Promise<void>
}
type RpcClient = ReturnType<typeof Rpc.client<typeof rpc>>
const ABORT_FETCH_TIMEOUT = 3000
const ABORT_RETRY_HEADER = "x-opencode-abort-retried-after-worker-restart"
const log = Log.create({ service: "tui.thread" })

function abortSessionID(pathname: string) {
  return /^\/session\/([^/]+)\/abort$/.exec(pathname)?.[1]
}

function abortTimeoutMessage(abortTimeout: number) {
  return `Abort request timed out after ${abortTimeout}ms`
}

function workerFetchClient(client: WorkerFetchClientInput) {
  return typeof client === "function" ? client() : client
}

function workerFetchResponse(result: WorkerFetchOutput, retried = false) {
  const headers = new Headers(result.headers)
  if (retried) headers.set(ABORT_RETRY_HEADER, "true")
  return new Response(result.body, {
    status: result.status,
    headers,
  })
}

export function createWorkerFetch(client: WorkerFetchClientInput, options: WorkerFetchOptions = {}): typeof fetch {
  let requestCount = 0
  const abortTimeout = options.abortTimeout ?? ABORT_FETCH_TIMEOUT
  const timeoutMessage = abortTimeoutMessage(abortTimeout)
  const fn = async (input: RequestInfo | URL, init?: RequestInit): Promise<Response> => {
    const request = new Request(input, init)
    const url = new URL(request.url)
    const sessionID = abortSessionID(url.pathname)
    const requestID = sessionID ? `abort-${requestCount++}` : undefined
    const started = Date.now()
    const body = request.body ? await request.text() : undefined
    if (sessionID) {
      log.info("worker fetch request", {
        requestID,
        sessionID,
        method: request.method,
        pathname: url.pathname,
        hasBody: body !== undefined,
        bodyLength: body?.length ?? 0,
        timeout: abortTimeout,
      })
    }
    const workerInput = {
      requestID,
      url: request.url,
      method: request.method,
      headers: Object.fromEntries(request.headers.entries()),
      body,
    }
    const call = () => workerFetchClient(client).call("fetch", workerInput)
    try {
      const response = call()
      const result = await (sessionID ? withTimeout(response, abortTimeout, timeoutMessage) : response)
      if (sessionID) {
        log.info("worker fetch response", {
          requestID,
          sessionID,
          method: request.method,
          pathname: url.pathname,
          status: result.status,
          bodyLength: result.body.length,
          elapsed: Date.now() - started,
        })
      }
      return workerFetchResponse(result)
    } catch (error) {
      const message = errorMessage(error)
      if (sessionID) {
        log.warn("worker fetch failed", {
          requestID,
          sessionID,
          method: request.method,
          pathname: url.pathname,
          error: message,
          elapsed: Date.now() - started,
          timeout: abortTimeout,
        })
      }
      if (sessionID && message === timeoutMessage && options.onAbortTimeout) {
        const retryStarted = Date.now()
        try {
          await options.onAbortTimeout({ sessionID, requestID, error })
          log.info("retrying abort after worker restart", {
            requestID,
            sessionID,
          })
          const result = await withTimeout(call(), abortTimeout, timeoutMessage)
          log.info("worker fetch retry response", {
            requestID,
            sessionID,
            method: request.method,
            pathname: url.pathname,
            status: result.status,
            bodyLength: result.body.length,
            elapsed: Date.now() - retryStarted,
            totalElapsed: Date.now() - started,
          })
          return workerFetchResponse(result, true)
        } catch (retryError) {
          log.warn("abort retry after worker restart failed", {
            requestID,
            sessionID,
            error: errorMessage(retryError),
            elapsed: Date.now() - retryStarted,
            totalElapsed: Date.now() - started,
          })
        }
      }
      throw error
    }
  }
  return fn as typeof fetch
}

function createEventSource(
  client: () => RpcClient,
  onRestart: (handler: () => void) => () => void,
): EventSource {
  return {
    subscribe: async (handler) => {
      let unsubscribeEvent = client().on<GlobalEvent>("global.event", (e) => {
        handler(e)
      })
      const unsubscribeRestart = onRestart(() => {
        unsubscribeEvent()
        unsubscribeEvent = client().on<GlobalEvent>("global.event", (e) => {
          handler(e)
        })
      })
      return () => {
        unsubscribeRestart()
        unsubscribeEvent()
      }
    },
  }
}

async function target() {
  if (typeof OPENCODE_WORKER_PATH !== "undefined") return OPENCODE_WORKER_PATH
  const dist = new URL("./cli/cmd/tui/worker.js", import.meta.url)
  if (await Filesystem.exists(fileURLToPath(dist))) return dist
  return new URL("./worker.ts", import.meta.url)
}

async function input(value?: string) {
  const piped = process.stdin.isTTY ? undefined : await Bun.stdin.text()
  if (!value) return piped
  if (!piped) return value
  return piped + "\n" + value
}

export function resolveThreadDirectory(project?: string, envPWD = process.env.PWD, cwd = process.cwd()) {
  const root = Filesystem.resolve(envPWD ?? cwd)
  if (project) return Filesystem.resolve(path.isAbsolute(project) ? project : path.join(root, project))
  return Filesystem.resolve(cwd)
}

export const TuiThreadCommand = cmd({
  command: "$0 [project]",
  describe: "start opencode tui",
  builder: (yargs) =>
    withNetworkOptions(yargs)
      .positional("project", {
        type: "string",
        describe: "path to start opencode in",
      })
      .option("model", {
        type: "string",
        alias: ["m"],
        describe: "model to use in the format of provider/model",
      })
      .option("continue", {
        alias: ["c"],
        describe: "continue the last session",
        type: "boolean",
      })
      .option("session", {
        alias: ["s"],
        type: "string",
        describe: "session id to continue",
      })
      .option("fork", {
        type: "boolean",
        describe: "fork the session when continuing (use with --continue or --session)",
      })
      .option("prompt", {
        type: "string",
        describe: "prompt to use",
      })
      .option("agent", {
        type: "string",
        describe: "agent to use",
      }),
  handler: async (args) => {
    // Keep ENABLE_PROCESSED_INPUT cleared even if other code flips it.
    // (Important when running under `bun run` wrappers on Windows.)
    const unguard = win32InstallCtrlCGuard()
    try {
      // Must be the very first thing — disables CTRL_C_EVENT before any Worker
      // spawn or async work so the OS cannot kill the process group.
      win32DisableProcessedInput()

      if (args.fork && !args.continue && !args.session) {
        UI.error("--fork requires --continue or --session")
        process.exitCode = 1
        return
      }

      // Resolve relative --project paths from PWD, then use the real cwd after
      // chdir so the thread and worker share the same directory key.
      const next = resolveThreadDirectory(args.project)
      const file = await target()
      try {
        process.chdir(next)
      } catch {
        UI.error("Failed to change directory to " + next)
        return
      }
      const cwd = Filesystem.resolve(process.cwd())
      const env = sanitizedProcessEnv({
        [OPENCODE_PROCESS_ROLE]: "worker",
        [OPENCODE_RUN_ID]: ensureRunID(),
      })

      function startWorker() {
        const worker = new Worker(file, { env })
        worker.onerror = (e) => {
          Log.Default.error("thread error", {
            message: e.message,
            filename: e.filename,
            lineno: e.lineno,
            colno: e.colno,
            error: e.error,
          })
        }
        return { worker, client: Rpc.client<typeof rpc>(worker) }
      }

      const restartListeners = new Set<() => void>()
      let current = startWorker()
      let stopped = false
      let restarting: Promise<void> | undefined
      const currentClient = () => current.client
      const onWorkerRestart = (handler: () => void) => {
        restartListeners.add(handler)
        return () => {
          restartListeners.delete(handler)
        }
      }
      const restartWorker = async (input: AbortTimeoutInput) => {
        if (stopped) return
        if (restarting) return restarting
        restarting = (async () => {
          const previous = current
          log.warn("abort timeout reached before worker fetch, restarting worker", {
            requestID: input.requestID,
            sessionID: input.sessionID,
            error: errorMessage(input.error),
          })
          current = startWorker()
          for (const listener of restartListeners) listener()
          previous.worker.terminate()
          log.info("worker restarted after abort timeout", {
            requestID: input.requestID,
            sessionID: input.sessionID,
          })
        })().finally(() => {
          restarting = undefined
        })
        return restarting
      }

      const error = (e: unknown) => {
        Log.Default.error("process error", { error: errorMessage(e) })
      }
      const reload = () => {
        currentClient().call("reload", undefined).catch((err) => {
          Log.Default.warn("worker reload failed", {
            error: errorMessage(err),
          })
        })
      }
      process.on("uncaughtException", error)
      process.on("unhandledRejection", error)
      process.on("SIGUSR2", reload)

      const stop = async () => {
        if (stopped) return
        stopped = true
        process.off("uncaughtException", error)
        process.off("unhandledRejection", error)
        process.off("SIGUSR2", reload)
        const active = current
        await withTimeout(active.client.call("shutdown", undefined), 5000).catch((error) => {
          Log.Default.warn("worker shutdown failed", {
            error: errorMessage(error),
          })
        })
        active.worker.terminate()
      }

      const prompt = await input(args.prompt)
      const config = await TuiConfig.get()

      const network = resolveNetworkOptionsNoConfig(args)
      const external =
        process.argv.includes("--port") ||
        process.argv.includes("--hostname") ||
        process.argv.includes("--mdns") ||
        network.mdns ||
        network.port !== 0 ||
        network.hostname !== "127.0.0.1"

      const transport = external
        ? {
            url: (await currentClient().call("server", network)).url,
            fetch: undefined,
            events: undefined,
          }
        : {
            url: "http://opencode.internal",
            fetch: createWorkerFetch(currentClient, { onAbortTimeout: restartWorker }),
            events: createEventSource(currentClient, onWorkerRestart),
          }

      try {
        await validateSession({
          url: transport.url,
          sessionID: args.session,
          directory: cwd,
          fetch: transport.fetch,
        })
      } catch (error) {
        UI.error(errorMessage(error))
        process.exitCode = 1
        return
      }

      setTimeout(() => {
        currentClient().call("checkUpgrade", { directory: cwd }).catch(() => {})
      }, 1000).unref?.()

      try {
        const { tui } = await import("./app")
        await tui({
          url: transport.url,
          async onSnapshot() {
            const tui = writeHeapSnapshot("tui.heapsnapshot")
            const server = await currentClient().call("snapshot", undefined)
            return [tui, server]
          },
          config,
          directory: cwd,
          fetch: transport.fetch,
          events: transport.events,
          args: {
            continue: args.continue,
            sessionID: args.session,
            agent: args.agent,
            model: args.model,
            prompt,
            fork: args.fork,
          },
        })
      } finally {
        await stop()
      }
    } finally {
      unguard?.()
    }
    process.exit(0)
  },
})

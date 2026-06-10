import { cmd } from "@/cli/cmd/cmd"
import { Rpc } from "@/util/rpc"
import { type rpc } from "../tui/worker"
import path from "path"
import { fileURLToPath } from "url"
import { UI } from "@/cli/ui"
import { errorMessage } from "@opencode-ai/tui/util/error"
import { withTimeout } from "@/util/timeout"
import { withNetworkOptions, resolveNetworkOptionsNoConfig } from "@/cli/network"
import { Filesystem } from "@/util/filesystem"
import type { GlobalEvent } from "@opencode-ai/sdk/v2"
import type { EventSource } from "@opencode-ai/tui/context/sdk"
import { writeHeapSnapshot } from "v8"
import { validateSession } from "../tui/validate-session"
import { win32InstallCtrlCGuard } from "@opencode-ai/tui/terminal-win32"

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
type AbortTimeoutInput = { sessionID: string }
type WorkerFetchOptions = {
  abortTimeout?: number
  onAbortTimeout?: (input: AbortTimeoutInput) => void | Promise<void>
}
type RpcClient = ReturnType<typeof Rpc.client<typeof rpc>>
const ABORT_FETCH_TIMEOUT = 3000
const ABORT_RETRY_HEADER = "x-opencode-abort-retried-after-worker-restart"

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
  const abortTimeout = options.abortTimeout ?? ABORT_FETCH_TIMEOUT
  const timeoutMessage = abortTimeoutMessage(abortTimeout)
  const fn = async (input: RequestInfo | URL, init?: RequestInit): Promise<Response> => {
    const request = new Request(input, init)
    const url = new URL(request.url)
    const sessionID = abortSessionID(url.pathname)
    const body = request.body ? await request.text() : undefined
    const workerInput = {
      url: request.url,
      method: request.method,
      headers: Object.fromEntries(request.headers.entries()),
      body,
    }
    const call = () => workerFetchClient(client).call("fetch", workerInput)
    try {
      const response = call()
      const result = await (sessionID ? withTimeout(response, abortTimeout, timeoutMessage) : response)
      return workerFetchResponse(result)
    } catch (error) {
      const message = errorMessage(error)
      if (sessionID && message === timeoutMessage && options.onAbortTimeout) {
        const result = await Promise.resolve(options.onAbortTimeout({ sessionID }))
          .then(() => withTimeout(call(), abortTimeout, timeoutMessage))
          .catch(() => undefined)
        if (result) {
          return workerFetchResponse(result, true)
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
  const dist = new URL("./cli/tui/worker.js", import.meta.url)
  if (await Filesystem.exists(fileURLToPath(dist))) return dist
  return new URL("../tui/worker.ts", import.meta.url)
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
    const unguard = win32InstallCtrlCGuard()
    try {
      const { TuiConfig } = await import("@/config/tui")
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
      function startWorker() {
        const worker = new Worker(file)
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
      const restartWorker = async () => {
        if (stopped) return
        if (restarting) return restarting
        restarting = (async () => {
          const previous = current
          current = startWorker()
          for (const listener of restartListeners) listener()
          previous.worker.terminate()
        })().finally(() => {
          restarting = undefined
        })
        return restarting
      }

      const reload = () => {
        currentClient()
          .call("reload", undefined)
          .catch(() => {})
      }
      process.on("SIGUSR2", reload)

      const stop = async () => {
        if (stopped) return
        stopped = true
        process.off("SIGUSR2", reload)
        const active = current
        await withTimeout(active.client.call("shutdown", undefined), 5000).catch(() => {})
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
        const { Effect } = await import("effect")
        const { run } = await import("../tui/layer")
        const { createLegacyTuiPluginHost } = await import("@/plugin/tui/runtime")
        await Effect.runPromise(
          run({
            url: transport.url,
            async onSnapshot() {
              const tui = writeHeapSnapshot("tui.heapsnapshot")
              const server = await currentClient().call("snapshot", undefined)
              return [tui, server]
            },
            config,
            pluginHost: createLegacyTuiPluginHost(),
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
          }),
        )
      } finally {
        await stop()
      }
    } finally {
      try {
        unguard?.()
      } catch {}
    }
    process.exit(0)
  },
})

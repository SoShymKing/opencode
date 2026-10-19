import { readFile, rm } from "node:fs/promises"
import type { DiscoverOptions, Endpoint, Info, EnsureOptions, StopOptions } from "../service.js"
import { contenderPool, spawnServiceContender } from "../service-contender.js"
import { defaultEnsureTiming, ensureTiming, type EnsureTiming } from "../service-timing.js"
import { matchesVersion } from "../service-version.js"
import { PtyHandoff } from "../pty-handoff.js"
import { decide, fallback, headers, probeResult, same } from "../service-probe.js"
import { decodeRegistration, ordinaryTime, preparing } from "../service-startup.js"

export * from "../service.js"
export { headers }

// Find, start, and stop the local opencode background service.
//
// The registration file is the complete discovery contract. This module is
// intentionally implemented with Node APIs so Promise clients do not need
// Effect or @effect/platform-node at runtime.

/** Discover a healthy, compatible local service without starting one. */
export async function discover(options: DiscoverOptions = {}) {
  const found = (await registered(options.file)).service
  if (found?.state !== "ready") return undefined
  if (!found.compatible) return undefined
  if (!matchesVersion(found.version, options)) return undefined
  return found.endpoint
}

/** Ensure a healthy, compatible local service is running. */
export async function ensure(options: EnsureOptions = {}): Promise<Endpoint> {
  const timing = ensureTiming(options)
  const elapsed = ordinaryTime(Date.now())
  const pool = contenderPool(timing)
  let timeouts: { readonly info: Info; readonly count: number } | undefined
  let announced = false

  const announce = (reason: "missing" | "version-mismatch", previousVersion?: string) => {
    if (announced) return
    announced = true
    options.onStart?.(reason, previousVersion)
  }
  const spawnContender = async (expected: Info | undefined) => {
    const [command, ...args] = options.command ?? ["opencode", "serve", "--service"]
    if (command === undefined) throw new Error("Missing service command")
    const env = await PtyHandoff.environment(options.file ?? fallback(), options.env)
    const current = await read(options.file)
    if (preparing(current) || (current !== undefined && (expected === undefined || !same(current, expected)))) return
    try {
      pool.add(spawnServiceContender(command, args, env))
    } catch (cause) {
      throw new Error("Failed to start server", { cause })
    }
  }

  try {
    while (true) {
      elapsed(Date.now(), preparing(await read(options.file)))
      const registration = await registered(options.file, timing.requestTimeout)
      const current = await read(options.file)
      const protectedOwner = preparing(current, registration.service, options)
      const duration = elapsed(Date.now(), protectedOwner)
      if (protectedOwner) {
        timeouts = undefined
        await delay(timing.pollInterval)
        continue
      }
      if (duration >= timing.promiseTimeout)
        throw pool.failure() ?? new Error("Timed out waiting for the background service to start")
      if (current !== undefined && (registration.info === undefined || !same(current, registration.info))) {
        timeouts = undefined
        await delay(timing.pollInterval)
        continue
      }
      if (registration.timedOut && registration.info !== undefined) {
        timeouts = {
          info: registration.info,
          count: timeouts !== undefined && same(timeouts.info, registration.info) ? timeouts.count + 1 : 1,
        }
        if (timeouts.count >= 3) {
          announce("missing")
          console.warn("Background service is unresponsive; recovery cannot preserve persistent terminals")
          if (await terminate(registration.info, { ...options, automatic: true, pty: "clear" }, timing)) {
            pool.evict(registration.info.pid)
            pool.recruitNow()
          }
          timeouts = undefined
        }
      } else timeouts = undefined

      if (registration.service !== undefined) {
        pool.serviceAnswered()
        const service = registration.service
        if (current?.startup === "database-preparation" && service.compatible && service.state === "failed")
          throw new Error("Background service failed to start")
        const decision = decide(service, options)
        if (decision._tag === "fail") throw decision.error
        if (decision._tag === "reuse") {
          await PtyHandoff.complete(options.file ?? fallback(), service.info)
          pool.adopt(service.info.pid)
          return service.endpoint
        }
        if (decision._tag === "replace") {
          announce("version-mismatch", service.version)
          if (service.state !== "ready")
            console.warn("Background service is not ready; replacement cannot preserve persistent terminals")
          if (await terminate(service.info, { file: options.file, pty: decision.pty, automatic: true }, timing).catch(() => false))
            pool.evict(service.info.pid)
        }
      } else {
        const latest = await read(options.file)
        if (preparing(latest) || (latest !== undefined && (current === undefined || !same(latest, current)))) continue
        const failed = pool.reap()
        if (failed !== undefined) throw failed
        if (pool.shouldRecruit(registration.info !== undefined)) {
          announce("missing")
          await spawnContender(latest)
        }
      }
      await delay(timing.pollInterval)
    }
  } finally {
    await pool.releaseAll()
  }
}

/** Stop the registered local service. */
export async function stop(options: StopOptions = {}) {
  const info = await read(options.file)
  // Terminal handoff is best-effort; it must never keep the old service running.
  await (
    options.pty === "handoff" && info !== undefined
      ? PtyHandoff.prepare(options.file ?? fallback(), info, defaultEnsureTiming.requestTimeout)
      : PtyHandoff.clear(options.file ?? fallback())
  ).catch((cause: unknown) => console.warn("Failed to prepare persistent terminals for replacement", cause))
  if (info !== undefined) await terminate(info, options, ensureTiming(options))
}

async function read(file?: string) {
  const text = await readFile(file ?? fallback(), "utf8").catch(() => undefined)
  if (text === undefined) return undefined
  try {
    return decodeRegistration(JSON.parse(text))
  } catch (error) {
    if (error instanceof SyntaxError) return undefined
    throw error
  }
}

async function registered(file?: string, timeout?: number) {
  const info = await read(file)
  if (info === undefined) return { info: undefined, service: undefined, timedOut: false }
  return { info, ...(await probeResult(info, timeout)) }
}

function signal(pid: number, name: NodeJS.Signals) {
  try {
    process.kill(pid, name)
  } catch {}
}

function stopped(pid: number) {
  try {
    process.kill(pid, 0)
    return false
  } catch {
    return true
  }
}

async function waitUntilStopped(pid: number, timing: EnsureTiming) {
  for (let attempt = 0; attempt <= timing.stopPollAttempts; attempt++) {
    if (stopped(pid)) return true
    if (attempt < timing.stopPollAttempts) await delay(timing.stopPollInterval)
  }
  return false
}

async function terminate(
  info: Info,
  options: StopOptions & { readonly automatic?: boolean },
  timing: EnsureTiming,
) {
  const current = await read(options.file)
  if (current === undefined || !same(current, info) || (options.automatic && preparing(current)))
    return false
  if (options.automatic) {
    await (
      options.pty === "handoff"
        ? PtyHandoff.prepare(options.file ?? fallback(), info, timing.requestTimeout)
        : PtyHandoff.clear(options.file ?? fallback())
    ).catch((cause: unknown) => console.warn("Failed to prepare persistent terminals for replacement", cause))
    const latest = await read(options.file)
    if (latest === undefined || !same(latest, info) || preparing(latest)) return false
  }
  signal(info.pid, "SIGTERM")
  // The registration can disappear or change hands before this process exits. Only the PID we
  // signalled can tell us whether it has stopped, so escalate based on that process.
  if (!(await waitUntilStopped(info.pid, timing))) {
    signal(info.pid, "SIGKILL")
    if (!(await waitUntilStopped(info.pid, timing))) throw new Error(`Server process ${info.pid} is still running`)
  }
  const latest = await read(options.file)
  if (latest === undefined || !same(latest, info)) return true
  await rm(options.file ?? fallback(), { force: true })
  return true
}

function delay(milliseconds: number) {
  return new Promise<void>((resolve) => setTimeout(resolve, milliseconds))
}

/** Promise-based local service lifecycle operations. */
export const Service = { discover, ensure, stop, headers }

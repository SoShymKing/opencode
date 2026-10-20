import { expect, test } from "bun:test"
import { randomUUID } from "node:crypto"
import { mkdir, rm } from "node:fs/promises"
import type { Worker } from "node:worker_threads"
import { Deferred, Effect, Exit, Fiber, Scope } from "effect"
import type { openWith, openWorker } from "@opencode/core/database/ws-native-bun"
import { headroom, lease, reserveWorker, Resources, weight, workerBytes } from "@opencode/core/database/ws-native-serial"
import { WsNativeBatch } from "@opencode/core/database/ws-native-batch"
import { richMessageData } from "./fixture/database-ws"

test("resource retries do not rescan unchanged queued weights", async () => {
  const source = await Bun.file(new URL("../src/database/ws-native-bun.ts", import.meta.url)).text()
  const enqueue = "queue.push({ job, ready: ready[index], weight: selected.weights[index] })"
  const retry = "if (queue.length) yield* dispatch"
  const wait = "yield* change"
  for (const anchor of [enqueue, retry, wait]) expect(source.split(anchor).length).toBe(2)
  const directory = new URL("../.cache/", import.meta.url)
  await mkdir(directory, { recursive: true })
  const adapter = new URL(`worker-planning-${randomUUID()}.ts`, directory)
  await Bun.write(adapter, `export const measurements = {
    weightReads: 0, retries: 0, measuring: false, expected: 9, waiting: new Set<object>(),
    allWaiting: Deferred.makeUnsafe<void>(), retried: Deferred.makeUnsafe<void>(),
  }\n` + source.replaceAll(/"\.\/([^"\n]+)"/g, '"../src/database/$1"')
    .replace(enqueue, `queue.push({ job, ready: ready[index], get weight() { measurements.weightReads++; return selected.weights[index] } })`)
    .replace(retry, `${retry}
      if (measurements.measuring && ++measurements.retries === measurements.expected)
        yield* Deferred.succeed(measurements.retried, undefined)`)
    .replace(wait, `measurements.waiting.add(ready)
      if (measurements.waiting.size === measurements.expected) yield* Deferred.succeed(measurements.allWaiting, undefined)
      ${wait}`))
  try {
    const measured: {
      openWith: typeof openWith
      openWorker: typeof openWorker
      measurements: { weightReads: number; retries: number; measuring: boolean; expected: number; waiting: Set<object>;
        allWaiting: Deferred.Deferred<void>; retried: Deferred.Deferred<void> }
    } = await import(adapter.href)
    await Effect.runPromise(Effect.gen(function* () {
      const workers: Worker[] = []
      const completed: string[] = []
      const firstWritten = yield* Deferred.make<void>()
      const data = richMessageData.replace('"first"', JSON.stringify("x".repeat(270000)))
      const jobs = ["msg_first", "msg_gate", ...Array.from({ length: 32 }, (_, index) => `msg_queued_${index}`)].map((id, seq): WsNativeBatch.Job => ({
        kind: "message", input: { row: { id, session_id: "ses_planning", type: "assistant", seq, time_created: 1, time_updated: 16, data },
          parts: undefined, previousNative: false, store: true },
      }))
      const pool = yield* measured.openWith(measured.openWorker(new URL("./fixture/ws-native-worker-gated.ts", import.meta.url)).pipe(Effect.map((opened) => {
        workers.push(opened.worker)
        return { ...opened, run: (items: readonly WsNativeBatch.Job[]) => opened.run(items).pipe(Effect.tap(() => Effect.sync(() => {
          completed.push(...items.map(WsNativeBatch.jobID))
        }))) }
      })))
      const available = yield* headroom
      const firstOrder: number[] = []
      const first = yield* pool.stream(jobs.slice(0, 2), (_, index) => Effect.gen(function* () {
        firstOrder.push(index)
        if (index === 0) yield* Deferred.succeed(firstWritten, undefined)
      })).pipe(Effect.forkChild)
      yield* Deferred.await(firstWritten)
      const parent = yield* Effect.scope
      const other = yield* Scope.fork(parent)
      const credit = yield* Scope.fork(parent)
      expect(yield* reserveWorker.pipe(Scope.provide(other))).toBe(true)
      yield* lease(4096, "credit").pipe(Scope.provide(credit))
      const queued = yield* Effect.forEach(Array.from({ length: 8 }, (_, index) => jobs.slice(2 + index * 4, 6 + index * 4)),
        (batch) => pool.run(batch).pipe(Effect.forkChild({ startImmediately: true })))
      yield* Deferred.await(measured.measurements.allWaiting).pipe(Effect.timeout("10 seconds"))
      expect(queued.every((fiber) => fiber.pollUnsafe() === undefined)).toBe(true)
      expect(completed).toEqual(["msg_first"])
      expect(yield* headroom).toBe(available - jobs.reduce((sum, job) => sum + weight(job), 0) - 2 * workerBytes - 4096)
      const before = measured.measurements.weightReads
      measured.measurements.measuring = true
      yield* Scope.close(credit, Exit.void)
      yield* Deferred.await(measured.measurements.retried).pipe(Effect.timeout("10 seconds"))
      measured.measurements.measuring = false
      expect(measured.measurements.retries).toBe(9)
      expect(completed).toEqual(["msg_first"])
      expect(queued.every((fiber) => fiber.pollUnsafe() === undefined)).toBe(true)
      expect(measured.measurements.weightReads - before).toBe(0)
      yield* Scope.close(other, Exit.void)
      const results = yield* Effect.forEach(queued, Fiber.join).pipe(Effect.timeout("10 seconds"))
      expect(results.flat()).toEqual([...(yield* WsNativeBatch.run(jobs.slice(2)))])
      expect(completed).toEqual(["msg_first", ...jobs.slice(2).map(WsNativeBatch.jobID)])
      expect(firstOrder).toEqual([0])
      expect(yield* headroom).toBe(available - jobs.slice(0, 2).reduce((sum, job) => sum + weight(job), 0) - workerBytes)
      workers.forEach((worker) => worker.postMessage("release"))
      yield* Fiber.join(first)
      expect(firstOrder).toEqual([0, 1])
      expect(yield* headroom).toBe(available)
      expect(workers.length).toBe(2)
    }).pipe(Effect.provideService(Resources, () => ({ cpu: 2, freeBytes: 1024 ** 3 })), Effect.scoped))
  } finally {
    await rm(adapter)
  }
})

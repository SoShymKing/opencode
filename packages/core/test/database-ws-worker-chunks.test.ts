import { expect, test } from "bun:test"
import { Deferred, Effect, Fiber } from "effect"
import type { Worker } from "node:worker_threads"
import { openWith, openWorker } from "@opencode/core/database/ws-native-bun"
import { headroom, lease, pipeline, Resources, weight, workerBytes } from "@opencode/core/database/ws-native-serial"
import { WsNativeBatch } from "@opencode/core/database/ws-native-batch"
import { richMessageData } from "./fixture/database-ws"

const quantum = 2 * 1024 ** 2
const resources = () => ({ cpu: 2, freeBytes: 1024 ** 3 })
const makeJobs = (ids: readonly string[]) => ids.map((id, seq): WsNativeBatch.Job => ({ kind: "message", input: {
  row: { id, session_id: "ses_chunks", type: "assistant", seq, time_created: 1, time_updated: 16,
    data: richMessageData.replace('"first"', JSON.stringify("x".repeat(id === "msg_gate" || id === "msg_oversized" ? 270000 : 24000 + (seq % 5) * 4000))) },
  parts: undefined, previousNative: false, store: true,
} }))

test("real Workers pack normal jobs into byte-bounded FIFO requests with oversized jobs alone", () => Effect.runPromise(Effect.gen(function* () {
  const jobs = makeJobs(Array.from({ length: 97 }, (_, index) => index === 32 ? "msg_oversized" : `msg_chunk_${index}`))
  const requests: { ids: string[]; bytes: number; worker: number }[] = []
  const pool = yield* openWith(openWorker().pipe(Effect.map((opened) => ({ ...opened,
    run: (items: readonly WsNativeBatch.Job[]) => Effect.sync(() => {
      requests.push({ ids: items.map(WsNativeBatch.jobID), bytes: items.reduce((sum, job) => sum + weight(job), 0), worker: opened.worker.threadId })
    }).pipe(Effect.andThen(opened.run(items))),
  }))))
  const available = yield* headroom
  expect(yield* pool.run(jobs)).toEqual([...(yield* WsNativeBatch.run(jobs))])
  expect(pool.threadIDs().length).toBe(2)
  expect(new Set(requests.map((request) => request.worker)).size).toBe(2)
  expect(requests.flatMap((request) => request.ids)).toEqual(jobs.map(WsNativeBatch.jobID))
  for (const request of requests) {
    if (request.bytes > quantum) expect(request.ids).toEqual(["msg_oversized"])
    else expect(request.bytes).toBeLessThanOrEqual(quantum)
  }
  expect(yield* headroom).toBe(available)
  console.info("Worker chunk operation counts", { jobs: jobs.length, requests: requests.length,
    packedRequests: requests.filter((request) => request.ids.length > 1).length,
    maxBoundedBytes: Math.max(...requests.filter((request) => request.ids.length > 1).map((request) => request.bytes), 0) })
  expect(requests.some((request) => request.ids.length > 1)).toBe(true)
  expect(requests.length).toBeLessThan(jobs.length / 2)
}).pipe(Effect.provideService(Resources, resources), Effect.scoped)))

test("failure inside a packed request closes Workers before releasing any payload credits", () => Effect.runPromise(Effect.gen(function* () {
  const jobs = makeJobs(["msg_gate", "msg_valid", "msg_bad", ...Array.from({ length: 16 }, (_, index) => `msg_tail_${index}`)])
    .map((job): WsNativeBatch.Job => job.kind === "message" && job.input.row.id === "msg_bad"
      ? { ...job, input: { ...job.input, row: { ...job.input.row, data: "{}" } } } : job)
  const requests: string[][] = []
  const exits: number[] = []
  const pool = yield* openWith(openWorker(new URL("./fixture/ws-native-worker-gated.ts", import.meta.url)).pipe(Effect.map((opened) => {
    opened.worker.once("exit", () => exits.push(Effect.runSync(headroom.pipe(Effect.provideService(Resources, resources)))))
    return { ...opened, run: (items: readonly WsNativeBatch.Job[]) => Effect.sync(() => {
      requests.push(items.map(WsNativeBatch.jobID))
    }).pipe(Effect.andThen(opened.run(items))) }
  })))
  const available = yield* headroom
  const result = yield* pool.run(jobs).pipe(Effect.result)
  expect(result).toMatchObject({ _tag: "Failure", failure: { messageID: "msg_bad" } })
  expect(requests.find((request) => request.includes("msg_bad"))?.length).toBeGreaterThan(1)
  const held = available - jobs.reduce((sum, job) => sum + weight(job), 0) - 2 * workerBytes
  expect(exits).toEqual([held, held])
  expect(pool.threadIDs()).toEqual([-1, -1])
  expect(yield* headroom).toBe(available)
}).pipe(Effect.provideService(Resources, resources), Effect.scoped)))

test("free Worker pulls later normal chunks across three batches before oversized gate release", () => Effect.runPromise(Effect.gen(function* () {
  const jobs = makeJobs(["msg_gate", ...Array.from({ length: 96 }, (_, index) => `msg_ready_${index}`)])
  const workers: Worker[] = []
  const requests: string[][] = []
  const completed: string[] = []
  const refilled = yield* Deferred.make<void>()
  const pool = yield* openWith(openWorker(new URL("./fixture/ws-native-worker-gated.ts", import.meta.url)).pipe(Effect.map((opened) => {
    workers.push(opened.worker)
    return { ...opened, run: (items: readonly WsNativeBatch.Job[]) => Effect.sync(() => {
      const bytes = items.reduce((sum, job) => sum + weight(job), 0)
      if (bytes > quantum) expect(items.map(WsNativeBatch.jobID)).toEqual(["msg_gate"])
      requests.push(items.map(WsNativeBatch.jobID))
    }).pipe(Effect.andThen(opened.run(items)), Effect.tap(() => Effect.gen(function* () {
      completed.push(...items.map(WsNativeBatch.jobID))
      if (completed.length === jobs.length - 1) yield* Deferred.succeed(refilled, undefined)
    }))) }
  })))
  const available = yield* headroom
  const accepted: string[] = []
  const results: WsNativeBatch.Result[] = []
  let reads = 0
  const read = Effect.gen(function* () {
    if (reads === 3) return undefined
    const batch = reads === 0 ? jobs.slice(0, 33) : jobs.slice(1 + reads * 32, 33 + reads * 32)
    yield* lease(batch.reduce((sum, job) => sum + weight(job), 0), WsNativeBatch.jobID(batch[0]))
    reads++
    return batch
  })
  const fiber = yield* pipeline(read, (batch, emit: (output: WsNativeBatch.Result, index: number) => Effect.Effect<void>) => pool.stream(batch, emit, true), (job, output) => Effect.sync(() => {
    accepted.push(WsNativeBatch.jobID(job))
    results.push(output)
  })).pipe(Effect.forkChild)
  yield* Effect.gen(function* () {
    yield* Deferred.await(refilled).pipe(Effect.timeout("10 seconds"))
    expect(reads).toBe(3)
    expect(workers.length).toBe(2)
    expect(completed).toEqual(jobs.slice(1).map(WsNativeBatch.jobID))
    expect(requests.flat()).toEqual(jobs.map(WsNativeBatch.jobID))
    expect(accepted).toEqual([])
    expect(available - (yield* headroom)).toBeGreaterThanOrEqual(jobs.reduce((sum, job) => sum + weight(job), 0))
    console.info("Gated chunk operation counts", { jobs: jobs.length, requests: requests.length, completed: completed.length })
    expect(requests.some((request) => request.length > 1)).toBe(true)
    expect(requests.length).toBeLessThan(jobs.length / 2)
    workers.forEach((worker) => worker.postMessage("release"))
    yield* Fiber.join(fiber)
    expect(accepted).toEqual(jobs.map(WsNativeBatch.jobID))
    expect(results).toEqual([...(yield* WsNativeBatch.run(jobs))])
    expect(yield* headroom).toBe(available)
  }).pipe(Effect.ensuring(Fiber.interrupt(fiber)))
}).pipe(Effect.provideService(Resources, resources), Effect.scoped)))

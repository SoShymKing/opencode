import { expect, test } from "bun:test"
import { Deferred, Effect, Exit, Fiber, Scope } from "effect"
import type { Worker } from "node:worker_threads"
import { openWith, openWorker } from "@opencode/core/database/ws-native-bun"
import { headroom, lease, pipeline, reserveWorker, Resources, weight, workerBytes } from "@opencode/core/database/ws-native-serial"
import { WsNativeBatch } from "@opencode/core/database/ws-native-batch"
import { richMessageData } from "./fixture/database-ws"

test("free Worker refills current, next, third and fourth batches while ordered consumption is gated", () => Effect.runPromise(Effect.gen(function* () {
  const workers: Worker[] = []
  const completed: string[] = []
  const dispatched: string[] = []
  const refilled = yield* Deferred.make<void>()
  const jobs = ["msg_gate", "msg_current_1", "msg_current_2", "msg_current_3", "msg_next_1", "msg_next_2",
    "msg_third_1", "msg_third_2", "msg_fourth_1", "msg_fourth_2"].map((id, seq): WsNativeBatch.Job => ({
    kind: "message", input: { row: { id, session_id: "ses_refill", type: "assistant", seq, time_created: 1, time_updated: 16,
      data: richMessageData.replace('"first"', JSON.stringify("x".repeat(270000))) }, parts: undefined, previousNative: false, store: true },
  }))
  const pool = yield* openWith(openWorker(new URL("./fixture/ws-native-worker-gated.ts", import.meta.url)).pipe(Effect.map((opened) => {
    workers.push(opened.worker)
    return { ...opened, run: (items: readonly WsNativeBatch.Job[]) => Effect.sync(() => {
      dispatched.push(...items.map(WsNativeBatch.jobID))
    }).pipe(Effect.andThen(opened.run(items)), Effect.tap(() => Effect.gen(function* () {
      completed.push(...items.map(WsNativeBatch.jobID))
      if (jobs.slice(1).every((job) => completed.includes(WsNativeBatch.jobID(job))))
        yield* Deferred.succeed(refilled, undefined)
    }))) }
  })))
  const available = yield* headroom
  const admitted = yield* Deferred.make<void>()
  let reads = 0
  const accepted: string[] = []
  const read = Effect.gen(function* () {
    if (reads === 4) return undefined
    const batch = reads === 0 ? jobs.slice(0, 4) : jobs.slice(2 + reads * 2, 4 + reads * 2)
    yield* lease(batch.reduce((sum, job) => sum + weight(job), 0), WsNativeBatch.jobID(batch[0]))
    reads++
    if (reads === 2) yield* Deferred.succeed(admitted, undefined)
    return batch
  })
  const fiber = yield* pipeline(read, (batch, emit) => pool.stream(batch, emit, true), (job) => Effect.sync(() => {
    accepted.push(WsNativeBatch.jobID(job))
  })).pipe(Effect.forkChild)
  yield* Effect.gen(function* () {
    yield* Deferred.await(admitted)
    yield* Deferred.await(refilled).pipe(Effect.timeout("10 seconds"))
    expect(workers.length).toBe(2)
    expect(dispatched).toEqual(jobs.map(WsNativeBatch.jobID))
    expect(completed.toSorted()).toEqual(jobs.slice(1).map(WsNativeBatch.jobID).toSorted())
    expect(reads).toBe(4)
    expect(accepted).toEqual([])
    const retained = available - (yield* headroom)
    const payload = jobs.reduce((sum, job) => sum + weight(job), 0)
    expect(retained).toBeGreaterThanOrEqual(payload)
    expect(retained).toBeLessThanOrEqual(payload + 2 * workerBytes)
    workers.forEach((worker) => worker.postMessage("release"))
    yield* Fiber.join(fiber)
    expect(accepted).toEqual(jobs.map(WsNativeBatch.jobID))
    expect(yield* headroom).toBe(available)
  }).pipe(Effect.ensuring(Fiber.interrupt(fiber)))
}).pipe(Effect.provideService(Resources, () => ({ cpu: 2, freeBytes: 1024 ** 3 })), Effect.scoped)))

test("next batch failure interrupts blocked current compute and releases both payload leases", () => Effect.runPromise(Effect.gen(function* () {
  const started = yield* Deferred.make<void>()
  const available = yield* headroom
  let reads = 0
  let stopped = false
  const result = yield* pipeline(Effect.gen(function* () {
    if (reads === 2) return undefined
    yield* lease(4096, "batch")
    return [reads++]
  }), (items) => items[0] === 0
    ? Deferred.succeed(started, undefined).pipe(Effect.andThen(Effect.never), Effect.ensuring(Effect.sync(() => { stopped = true })))
    : Deferred.await(started).pipe(Effect.andThen(Effect.fail("next batch failed"))),
  () => Effect.die("must not accept")).pipe(Effect.result, Effect.timeout("10 seconds"))
  expect(result).toMatchObject({ _tag: "Failure", failure: "next batch failed" })
  expect(stopped).toBe(true)
  expect(reads).toBe(2)
  expect(yield* headroom).toBe(available)
}).pipe(Effect.provideService(Resources, () => ({ cpu: 2, freeBytes: 1024 ** 3 })), Effect.scoped)))

test("capacity recovery replaces a retired slot while another Worker remains blocked", () => {
  let cpu = 2
  return Effect.runPromise(Effect.gen(function* () {
    const firstWritten = yield* Deferred.make<void>()
    const workers: Worker[] = []
    const jobs = ["msg_first", "msg_gate", "msg_recovered_1", "msg_recovered_2"].map((id, seq): WsNativeBatch.Job => ({
      kind: "message", input: { row: { id, session_id: "ses_refill", type: "assistant", seq, time_created: 1, time_updated: 16,
        data: richMessageData.replace('"first"', JSON.stringify("x".repeat(270000))) }, parts: undefined, previousNative: false, store: true },
    }))
    const pool = yield* openWith(openWorker(new URL("./fixture/ws-native-worker-gated.ts", import.meta.url)).pipe(Effect.map((opened) => {
      workers.push(opened.worker)
      return { ...opened, run: (items: readonly WsNativeBatch.Job[]) => opened.run(items).pipe(Effect.tap(() => Effect.sync(() => {
        if (items.some((job) => WsNativeBatch.jobID(job) === "msg_first")) cpu = 1
      }))) }
    })))
    const first = yield* pool.stream(jobs.slice(0, 2), (_, index) => index === 0
      ? Deferred.succeed(firstWritten, undefined).pipe(Effect.asVoid) : Effect.void).pipe(Effect.forkChild)
    yield* Effect.gen(function* () {
      yield* Deferred.await(firstWritten)
      expect(workers[0].threadId).toBe(-1)
      expect(workers[1].threadId).toBeGreaterThan(0)
      cpu = 2
      expect((yield* pool.run(jobs.slice(2)).pipe(Effect.timeout("10 seconds"))).length).toBe(2)
      expect(workers.length).toBe(3)
      workers[1].postMessage("release")
      yield* Fiber.join(first)
    }).pipe(Effect.ensuring(Fiber.interrupt(first)))
  }).pipe(Effect.provideService(Resources, () => ({ cpu, freeBytes: 1024 ** 3 })), Effect.scoped))
})

for (const [budget, admitted] of [[16384, 3], [28672, 4]]) test(`payload budget ${budget} retains completed batches and blocks materialization until ordered release`, () =>
  Effect.runPromise(Effect.gen(function* () {
    const sizes = [4096, 8192, 4096, 12288, 4096]
    const blocked = yield* Deferred.make<void>()
    const completed = yield* Deferred.make<void>()
    const writing = yield* Deferred.make<void>()
    const release = yield* Deferred.make<void>()
    const materialized: number[] = []
    const submitted: number[] = []
    const accepted: number[] = []
    let retained = 0
    const read = Effect.gen(function* () {
      const index = materialized.length
      if (index === sizes.length) return undefined
      const acquisition = yield* lease(sizes[index], String(index)).pipe(Effect.forkChild({ startImmediately: true }))
      if (!acquisition.pollUnsafe()) yield* Deferred.succeed(blocked, undefined)
      yield* Fiber.join(acquisition)
      retained += sizes[index]
      yield* Effect.addFinalizer(() => Effect.sync(() => { retained -= sizes[index] }))
      expect(retained).toBeLessThanOrEqual(budget)
      expect(yield* headroom).toBe(budget - retained)
      materialized.push(index)
      return [index]
    })
    const fiber = yield* pipeline(read, (items, emit) => Effect.gen(function* () {
      submitted.push(items[0])
      yield* emit(items[0], 0)
      if (submitted.length === admitted) yield* Deferred.succeed(completed, undefined)
    }), (item, output) => Effect.gen(function* () {
      expect(output).toBe(item)
      accepted.push(item)
      if (item !== 0) return
      yield* Deferred.succeed(writing, undefined)
      yield* Deferred.await(release)
    })).pipe(Effect.forkChild)
    yield* Effect.gen(function* () {
      yield* Effect.all([Deferred.await(blocked), Deferred.await(completed), Deferred.await(writing)], { concurrency: 3 }).pipe(Effect.timeout("10 seconds"))
      expect(materialized).toEqual(Array.from({ length: admitted }, (_, index) => index))
      expect(submitted).toEqual(materialized)
      expect(accepted).toEqual([0])
      expect(retained).toBe(budget)
      expect(yield* headroom).toBe(0)
      yield* Deferred.succeed(release, undefined)
      yield* Fiber.join(fiber)
      expect(accepted).toEqual([0, 1, 2, 3, 4])
      expect(retained).toBe(0)
      expect(yield* headroom).toBe(budget)
    }).pipe(Effect.ensuring(Fiber.interrupt(fiber)))
  }).pipe(Effect.provideService(Resources, () => ({ cpu: 1, freeBytes: 2 * budget })), Effect.scoped)))

for (const mode of ["tiny", "capacity return", "shutdown"] as const) test(`${mode} work settles while another Worker remains gated`, () =>
  Effect.runPromise(Effect.gen(function* () {
    const workers: Worker[] = []
    const exits: number[] = []
    const completed: string[] = []
    const firstWritten = yield* Deferred.make<void>()
    const large = richMessageData.replace('"first"', JSON.stringify("x".repeat(270000)))
    const jobs = ["msg_first", "msg_gate"].map((id, seq): WsNativeBatch.Job => ({ kind: "message", input: {
      row: { id, session_id: "ses_starvation", type: "assistant", seq, time_created: 1, time_updated: 16, data: large },
      parts: undefined, previousNative: false, store: true,
    } }))
    const later = (mode === "tiny" ? ["msg_tiny"] : ["msg_returned_1", "msg_returned_2"]).map((id, seq): WsNativeBatch.Job => ({ kind: "message", input: {
      row: { id, session_id: "ses_starvation", type: "assistant", seq: seq + 2, time_created: 1, time_updated: 16,
        data: mode === "tiny" ? richMessageData : large }, parts: undefined, previousNative: false, store: true,
    } }))
    const pool = yield* openWith(openWorker(new URL("./fixture/ws-native-worker-gated.ts", import.meta.url)).pipe(Effect.map((opened) => {
      workers.push(opened.worker)
      opened.worker.once("exit", () => exits.push(Effect.runSync(headroom.pipe(Effect.provideService(Resources, () => ({ cpu: 2, freeBytes: 1024 ** 3 }))))))
      return { ...opened, run: (items: readonly WsNativeBatch.Job[]) => opened.run(items).pipe(Effect.tap(() => Effect.sync(() => {
        completed.push(...items.map(WsNativeBatch.jobID))
      }))) }
    })))
    const available = yield* headroom
    const first = yield* pool.stream(jobs, (_, index) => index === 0
      ? Deferred.succeed(firstWritten, undefined).pipe(Effect.asVoid) : Effect.void).pipe(Effect.forkChild)
    yield* Effect.gen(function* () {
      yield* Deferred.await(firstWritten)
      expect(workers.length).toBe(2)
      expect(workers.every((worker) => worker.threadId > 0)).toBe(true)
      const parent = yield* Effect.scope
      const other = yield* Scope.fork(parent)
      if (mode !== "tiny") expect(yield* reserveWorker.pipe(Scope.provide(other))).toBe(true)
      const next = yield* pool.run(later).pipe(Effect.forkChild({ startImmediately: true }))
      if (mode !== "tiny") {
        expect(next.pollUnsafe()).toBeUndefined()
        expect(completed).toEqual(["msg_first"])
        expect(yield* headroom).toBe(available - [...jobs, ...later].reduce((sum, job) => sum + weight(job), 0) - 2 * workerBytes)
        if (mode === "shutdown") {
          const held = yield* headroom
          yield* Fiber.interrupt(first)
          expect((yield* Fiber.await(next))._tag).toBe("Failure")
          expect(exits).toEqual([held, held])
          expect(workers.every((worker) => worker.threadId === -1)).toBe(true)
          yield* Scope.close(other, Exit.void)
          yield* Effect.yieldNow
          expect((yield* pool.run(later).pipe(Effect.result))._tag).toBe("Failure")
          expect(workers.length).toBe(2)
          expect(yield* headroom).toBe(available)
          return
        }
        yield* Scope.close(other, Exit.void)
      }
      expect(yield* Fiber.join(next).pipe(Effect.timeout("5 seconds"))).toEqual([...(yield* WsNativeBatch.run(later))])
      if (mode === "capacity return") expect(completed).toEqual(["msg_first", ...later.map(WsNativeBatch.jobID)])
      expect(first.pollUnsafe()).toBeUndefined()
      expect(workers.length).toBe(2)
      workers.forEach((worker) => worker.postMessage("release"))
      yield* Fiber.join(first)
      expect(yield* headroom).toBe(available)
    }).pipe(Effect.ensuring(Fiber.interrupt(first)))
  }).pipe(Effect.provideService(Resources, () => ({ cpu: 2, freeBytes: 1024 ** 3 })), Effect.scoped)))

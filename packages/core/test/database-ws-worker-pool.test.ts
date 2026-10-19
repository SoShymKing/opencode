import { expect, test } from "bun:test"
import { Deferred, Effect, Fiber } from "effect"
import { open, openWith, openWorker, withPool } from "@opencode/core/database/ws-native-bun"
import { choose, headroom, reserve, Resources, weight, workerBytes } from "@opencode/core/database/ws-native-serial"
import { WsNativeBatch } from "@opencode/core/database/ws-native-batch"
import type { Worker } from "node:worker_threads"
import { richMessageData } from "./fixture/database-ws"

const jobs = Array.from({ length: 2 }, (_, index) => ({ kind: "message" as const, input: {
  row: { id: `msg_pool_${index}`, session_id: "ses_pool", type: "assistant", seq: index, time_created: 1, time_updated: 16,
    data: richMessageData.replace('"first"', JSON.stringify("x".repeat(270000))) },
  parts: undefined, previousNative: false, store: true,
} }))
const resources = () => ({ cpu: 2, freeBytes: 1024 ** 3 })
const tiny = { ...jobs[0], input: { ...jobs[0].input, row: { ...jobs[0].input.row, data: richMessageData } } }

test("operation pool retains idle workers across inline tails and reuses them next pass", () => Effect.runPromise(
  withPool(Effect.gen(function* () {
    const pool = yield* Effect.scoped(open)
    yield* pool.run([tiny])
    expect(pool.threadIDs()).toEqual([])
    yield* pool.run(jobs)
    const ids = pool.threadIDs()
    expect(ids.length).toBe(2)
    yield* pool.run([tiny])
    expect(pool.threadIDs()).toEqual(ids)
    const next = yield* withPool(Effect.scoped(open))
    yield* next.run(jobs)
    expect(next.threadIDs()).toEqual(ids)
  })).pipe(Effect.provideService(Resources, resources))))

test("retained workers dispatch only the active prefix and idle workers reserve no CPU", async () => {
  const workers: Worker[] = []
  const replies: number[] = []
  await Effect.runPromise(Effect.gen(function* () {
    const pool = yield* openWith(openWorker().pipe(Effect.tap((opened) => Effect.sync(() => {
      const index = workers.length
      workers.push(opened.worker)
      replies.push(0)
      opened.worker.on("message", () => { replies[index]++ })
    }))))
    const available = yield* headroom
    yield* pool.run([...jobs, jobs[0]])
    const ids = pool.threadIDs()
    expect(ids.length).toBe(3)
    expect(yield* pool.run(jobs)).toEqual([...(yield* WsNativeBatch.run(jobs))])
    expect(pool.threadIDs()).toEqual(ids)
    expect(replies).toEqual([2, 2, 1])
    yield* pool.run([tiny])
    expect(replies).toEqual([2, 2, 1])
    expect(pool.threadIDs()).toEqual(ids)
    expect(yield* headroom).toBe(available)
    yield* Effect.scoped(Effect.gen(function* () {
      expect((yield* reserve([...jobs, jobs[0]])).workers).toBe(3)
    }))
    yield* pool.run([...jobs, jobs[0]])
    expect(pool.threadIDs()).toEqual(ids)
    expect(workers.length).toBe(3)
    expect(replies).toEqual([3, 3, 2])
  }).pipe(Effect.provideService(Resources, () => ({ cpu: 3, freeBytes: 1024 ** 3 })), Effect.scoped))
  expect(workers.map((worker) => worker.threadId)).toEqual([-1, -1, -1])
})

test("real memory and CPU reductions close retained workers before returning", async () => {
  const workers: Worker[] = []
  let cpu = 2
  let freeBytes = 1024 ** 3
  await Effect.runPromise(Effect.gen(function* () {
    const pool = yield* openWith(openWorker().pipe(Effect.tap((opened) => Effect.sync(() => { workers.push(opened.worker) }))))
    yield* pool.run(jobs)
    const ids = pool.threadIDs()
    freeBytes = 2 * (workerBytes + weight(tiny))
    yield* pool.run([tiny])
    expect(pool.threadIDs()).toEqual(ids.slice(0, 1))
    expect(workers[1].threadId).toBe(-1)
    cpu = 1
    yield* pool.run([tiny])
    expect(pool.threadIDs()).toEqual([])
    expect(workers.map((worker) => worker.threadId)).toEqual([-1, -1])
  }).pipe(Effect.provideService(Resources, () => ({ cpu, freeBytes })), Effect.scoped))
})

test("hardware capacity excludes active reservations and payload but not idle CPU", () => {
  const snapshot = { cpu: 3, freeBytes: 2 * (3 * workerBytes + weight(tiny)) }
  expect(choose(snapshot, [weight(tiny)])).toMatchObject({ workers: 0, capacity: 3 })
  expect(choose(snapshot, [weight(tiny)], { cpu: 2, bytes: 0 })).toMatchObject({ workers: 0, capacity: 1 })
  expect(choose(snapshot, [weight(tiny)], { cpu: 0, bytes: 2 * workerBytes })).toMatchObject({ workers: 0, capacity: 1 })
  expect(choose({ ...snapshot, cpu: 1 }, [weight(tiny)])).toMatchObject({ workers: 0, capacity: 0 })
})

for (const mode of ["failure", "interruption"] as const) test(`inline tail ${mode} closes idle workers before releasing its payload`, () =>
  Effect.runPromise(Effect.gen(function* () {
    const exits: number[] = []
    const pool = yield* openWith(openWorker().pipe(Effect.tap((opened) => Effect.sync(() => {
      opened.worker.once("exit", () => exits.push(Effect.runSync(headroom.pipe(Effect.provideService(Resources, resources)))))
    }))))
    const available = yield* headroom
    yield* pool.run(jobs)
    const started = yield* Deferred.make<void>()
    const fiber = yield* pool.stream([tiny], () => Effect.gen(function* () {
      expect(pool.threadIDs().every((id) => id > 0)).toBe(true)
      expect(pool.threadIDs().length).toBe(2)
      yield* Deferred.succeed(started, undefined)
      if (mode === "failure") return yield* Effect.fail("stop")
      yield* Effect.never
    })).pipe(Effect.forkChild)
    yield* Deferred.await(started)
    if (mode === "interruption") yield* Fiber.interrupt(fiber)
    expect((yield* Fiber.await(fiber))._tag).toBe("Failure")
    expect(exits).toEqual([available - weight(tiny), available - weight(tiny)])
    expect(pool.threadIDs()).toEqual([-1, -1])
    expect(yield* headroom).toBe(available)
  }).pipe(Effect.provideService(Resources, resources), Effect.scoped)))

test("nested consumers reuse a lazy operation pool after inner scope closes; fresh operations get new workers", async () => {
  const consume = Effect.scoped(Effect.gen(function* () {
    const pool = yield* open
    yield* pool.run(jobs)
    return pool
  }))
  const first = await Effect.runPromise(withPool(Effect.gen(function* () {
    const lazy = yield* Effect.scoped(open)
    expect(lazy.threadIDs()).toEqual([])
    const pool = yield* consume
    const ids = pool.threadIDs()
    expect(ids.length).toBe(2)
    expect(ids.every((id) => id > 0)).toBe(true)
    const nested = yield* withPool(consume)
    expect(nested).toBe(pool)
    expect(nested.threadIDs()).toEqual(ids)
    return { pool, ids }
  })).pipe(Effect.provideService(Resources, resources)))
  expect(first.pool.threadIDs()).toEqual([-1, -1])
  const next = await Effect.runPromise(withPool(consume.pipe(Effect.map((pool) => pool.threadIDs()))).pipe(
    Effect.provideService(Resources, resources)))
  expect(next.every((id) => id > 0 && !first.ids.includes(id))).toBe(true)
})

for (const mode of ["failure", "interruption"] as const) test(`outer ${mode} waits for shared workers to exit`, () =>
  Effect.runPromise(Effect.gen(function* () {
    const started = yield* Deferred.make<Effect.Success<typeof open>>()
    const fiber = yield* withPool(Effect.gen(function* () {
      const pool = yield* Effect.scoped(open)
      yield* pool.run(jobs)
      yield* Deferred.succeed(started, pool)
      if (mode === "failure") return yield* Effect.fail("stop")
      yield* Effect.never
    })).pipe(Effect.forkChild)
    const pool = yield* Deferred.await(started)
    if (mode === "interruption") yield* Fiber.interrupt(fiber)
    expect((yield* Fiber.await(fiber))._tag).toBe("Failure")
    expect(pool.threadIDs()).toEqual([-1, -1])
  }).pipe(Effect.provideService(Resources, resources), Effect.scoped)))

test("serial withPool preserves the effect unchanged", async () => {
  const serial = await import("@opencode/core/database/ws-native-serial")
  const effect = Effect.succeed(42)
  expect(serial.withPool(effect)).toBe(effect)
})

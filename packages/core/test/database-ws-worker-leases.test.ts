import { expect, test } from "bun:test"
import { Effect, Exit, Fiber, Scope } from "effect"
import { headroom, lease, readingPlan, Resources } from "@opencode/core/database/ws-native-serial"

test("reading plan samples CPU and memory once", () => {
  let samples = 0
  const result = Effect.runSync(readingPlan().pipe(Effect.provideService(Resources, () => {
    samples++
    return { cpu: 3, freeBytes: 1024 ** 3 }
  })))
  expect(samples).toBe(1)
  expect(result.countLimit).toBe(1536)
  expect(result.available).toBe(512 * 1024 ** 2)
})

for (const action of ["release", "interrupt"] as const) test(`lease waits under temporary memory pressure until ${action}`, () => {
  let freeBytes = 2000
  return Effect.runPromise(Effect.gen(function* () {
    const parent = yield* Effect.scope
    const held = yield* Scope.fork(parent)
    yield* lease(400, "held").pipe(Scope.provide(held))
    freeBytes = 1000
    const pending = yield* lease(600, "next").pipe(Effect.scoped, Effect.result, Effect.forkChild({ startImmediately: true }))
    expect(pending.pollUnsafe()).toBeUndefined()
    if (action === "interrupt") yield* Fiber.interrupt(pending)
    freeBytes = 2000
    yield* Scope.close(held, Exit.void)
    if (action === "release") expect(yield* Fiber.join(pending)).toMatchObject({ _tag: "Success", success: 600 })
    expect(yield* headroom).toBe(1000)
  }).pipe(Effect.provideService(Resources, () => ({ cpu: 1, freeBytes })), Effect.scoped))
})

test("lease rejects an oversized request when no other reservation can drain", () => Effect.runPromise(Effect.gen(function* () {
  const result = yield* lease(600, "oversized").pipe(Effect.scoped, Effect.result)
  expect(result._tag).toBe("Failure")
  expect(yield* headroom).toBe(500)
}).pipe(Effect.provideService(Resources, () => ({ cpu: 1, freeBytes: 1000 })), Effect.scoped)))

test("lease does not wait on its own scope's retained payload", () => Effect.runPromise(Effect.gen(function* () {
  const result = yield* Effect.scoped(Effect.gen(function* () {
    yield* lease(400, "first")
    return yield* lease(700, "same-scope")
  })).pipe(Effect.result)
  expect(result._tag).toBe("Failure")
  expect(yield* headroom).toBe(1000)
}).pipe(Effect.provideService(Resources, () => ({ cpu: 1, freeBytes: 2000 })), Effect.scoped)))

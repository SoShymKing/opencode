import { expect, test } from "bun:test"
import { Effect, Exit, Fiber, Scope } from "effect"
import { headroom, lease, resourceChange, Resources } from "@opencode/core/database/ws-native-serial"

test("one resource release completes only one successive notification wait", () => Effect.runPromise(Effect.gen(function* () {
  const parent = yield* Effect.scope
  const first = yield* Scope.fork(parent)
  const second = yield* Scope.fork(parent)
  yield* lease(400, "first").pipe(Scope.provide(first))
  yield* lease(400, "second").pipe(Scope.provide(second))
  expect(yield* headroom).toBe(200)
  let completed = 0
  const waiter = yield* Effect.gen(function* () {
    const firstChange = yield* resourceChange
    yield* firstChange
    completed++
    const secondChange = yield* resourceChange
    yield* secondChange
    completed++
  }).pipe(Effect.forkChild({ startImmediately: true }))
  expect(waiter.pollUnsafe()).toBeUndefined()
  yield* Scope.close(first, Exit.void)
  yield* Effect.yieldNow
  expect(completed).toBe(1)
  expect(waiter.pollUnsafe()).toBeUndefined()
  expect(yield* headroom).toBe(600)
  yield* Scope.close(second, Exit.void)
  yield* Fiber.join(waiter)
  expect(completed).toBe(2)
  expect(yield* headroom).toBe(1000)
}).pipe(Effect.provideService(Resources, () => ({ cpu: 1, freeBytes: 2000 })), Effect.scoped)))

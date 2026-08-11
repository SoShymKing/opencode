import { EffectBridge } from "@/effect/bridge"
import { Cause, Deferred, Effect, Exit, Fiber, Queue, Scope } from "effect"

export const makeRetainedBridge = Effect.fn("test.makeRetainedBridge")(function* () {
  const bridge = yield* EffectBridge.make()
  const context = yield* Effect.context()
  const toolFiber = yield* Deferred.make<Fiber.Fiber<unknown, unknown>>()
  const requests = yield* Queue.unbounded<Effect.Effect<void, never, Scope.Scope>>()
  yield* Effect.gen(function* () {
    while (true) yield* (yield* Queue.take(requests))
  }).pipe(Effect.forkScoped({ startImmediately: true }))

  const retained: EffectBridge.Interface = {
    ...bridge,
    promise(effect) {
      return new Promise((resolve, reject) => {
        const request = Effect.gen(function* () {
          const fiber = yield* effect.pipe(Effect.provide(context), Effect.forkScoped({ startImmediately: true }))
          yield* Deferred.succeed(toolFiber, fiber)
          const exit = yield* Fiber.await(fiber)
          if (Exit.isSuccess(exit)) return resolve(exit.value)
          reject(Cause.squash(exit.cause))
        }).pipe(Effect.asVoid) as Effect.Effect<void, never, Scope.Scope>
        Effect.runSync(Queue.offer(requests, request))
      })
    },
  }
  return { retained, toolFiber }
})

import { Deferred, Effect, Exit, Fiber, Queue, Semaphore } from "effect"

export type Snapshot = {
  readonly output: string
  readonly truncated?: boolean
  readonly outputPath?: string
}

export type Interface = {
  readonly update: (snapshot: Snapshot, urgent?: boolean) => Effect.Effect<void>
  readonly flush: Effect.Effect<void>
  readonly close: Effect.Effect<void>
}

type Timer = {
  readonly interrupt: Deferred.Deferred<void>
  readonly done: Deferred.Deferred<void>
}

export const make = Effect.fn("ShellProgress.make")(function* (options: {
  readonly checkpointIntervalMs: number
  readonly publish: (snapshot: Snapshot) => Effect.Effect<void>
}) {
  const lock = yield* Semaphore.make(1)
  const timers = yield* Queue.unbounded<Timer>()
  const closedDone = yield* Deferred.make<void>()
  let latest: Snapshot | undefined
  let pending: Snapshot | undefined
  let active: Timer | undefined
  let published = false
  let closed = false
  let closeCalled = false

  const schedule = Effect.gen(function* () {
    active = {
      interrupt: yield* Deferred.make<void>(),
      done: yield* Deferred.make<void>(),
    }
    yield* Queue.offer(timers, active)
  })

  const worker = yield* Effect.forkScoped(
    Effect.gen(function* () {
      while (true) {
        const timer = yield* Queue.take(timers)
        yield* Effect.race(Effect.sleep(options.checkpointIntervalMs), Deferred.await(timer.interrupt))
        const exit = yield* lock.withPermit(
          Effect.uninterruptible(
            Effect.gen(function* () {
              if (active !== timer) {
                yield* Deferred.succeed(timer.done, undefined)
                return
              }
              const snapshot = pending
              active = undefined
              pending = undefined
              const exit = yield* Effect.gen(function* () {
                if (!closed && snapshot) {
                  yield* options.publish(snapshot)
                  yield* schedule
                }
              }).pipe(Effect.exit)
              if (Exit.isFailure(exit)) {
                closed = true
                active = undefined
                pending = undefined
                yield* Deferred.done(closedDone, exit)
              }
              yield* Deferred.done(timer.done, exit)
              return exit
            }),
          ),
        )
        if (exit) yield* exit
      }
    }),
    { startImmediately: true },
  )

  const update = Effect.fnUntraced(function* (snapshot: Snapshot, urgent = false) {
    const done = yield* lock.withPermit(
      Effect.uninterruptible(
        Effect.gen(function* () {
          if (closed || empty(snapshot) || equal(latest, snapshot)) return
          latest = snapshot
          if (!published || !active) {
            published = true
            yield* options.publish(snapshot)
            yield* schedule
            return
          }

          pending = snapshot
          if (urgent) {
            yield* Deferred.succeed(active.interrupt, undefined)
            return active.done
          }
        }),
      ),
    )
    if (done) yield* Deferred.await(done)
  })

  const flush = Effect.gen(function* () {
    const done = yield* lock.withPermit(
      Effect.gen(function* () {
        if (closed) return closedDone
        if (!pending || !active) return
        yield* Deferred.succeed(active.interrupt, undefined)
        return active.done
      }),
    )
    if (done) yield* Deferred.await(done)
  })

  const closeInternal = Effect.uninterruptible(
    Effect.gen(function* () {
      const state = yield* lock.withPermit(
        Effect.sync(() => {
          if (closed) return
          closed = true
          const state = { pending, active }
          pending = undefined
          active = undefined
          return state
        }),
      )
      if (!state) {
        yield* Deferred.await(closedDone)
        return
      }

      const exit = yield* Effect.gen(function* () {
        yield* Fiber.interrupt(worker)
        if (state.pending) yield* options.publish(state.pending)
      }).pipe(Effect.exit)
      if (state.active) yield* Deferred.done(state.active.done, exit)
      yield* Deferred.done(closedDone, exit)
      yield* exit
    }),
  )

  const close = Effect.uninterruptible(
    Effect.gen(function* () {
      closeCalled = true
      yield* closeInternal
    }),
  )

  yield* Effect.addFinalizer(() => (closeCalled ? Effect.void : closeInternal))
  return { update, flush, close } satisfies Interface
})

function empty(snapshot: Snapshot) {
  return snapshot.output.length === 0 && !snapshot.truncated && snapshot.outputPath === undefined
}

function equal(left: Snapshot | undefined, right: Snapshot) {
  return (
    left?.output === right.output &&
    (left.truncated ?? false) === (right.truncated ?? false) &&
    left.outputPath === right.outputPath
  )
}

export * as ShellProgress from "./shell-progress"

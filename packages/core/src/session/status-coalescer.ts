import { Effect, Fiber, Scope, Semaphore } from "effect"
import type { SessionStatusEvent } from "@opencode-ai/schema/session-status-event"

export const make = Effect.fnUntraced(function* (publish: (status: SessionStatusEvent.Info) => Effect.Effect<void>) {
  const scope = yield* Scope.Scope
  const publication = Semaphore.makeUnsafe(1)
  let latest: SessionStatusEvent.Info | undefined
  let revision = 0
  let published = 0
  let generation = 0
  let closed = false
  let worker: { fiber?: Fiber.Fiber<void> } | undefined

  const send = Effect.gen(function* () {
    if (closed || !latest || published === revision) return
    const current = revision
    yield* publish(latest)
    published = current
  })
  const cancel = Effect.gen(function* () {
    generation++
    const pending = worker
    if (pending?.fiber) yield* Fiber.interrupt(pending.fiber)
    if (worker === pending) worker = undefined
  })
  const flush = Effect.gen(function* () {
    yield* cancel
    yield* publication.withPermit(send)
  })
  const close = Effect.gen(function* () {
    closed = true
    yield* cancel
    yield* publication.withPermit(Effect.void)
  })
  yield* Effect.addFinalizer(() => close)

  const set = Effect.fnUntraced(function* (status: SessionStatusEvent.Info) {
    if (closed) return
    const defer = latest !== undefined && increasing(latest, status)
    latest = status
    revision++
    if (!defer) {
      yield* flush
      return
    }
    if (worker) return
    const pending: { fiber?: Fiber.Fiber<void> } = {}
    worker = pending
    const current = generation
    pending.fiber = yield* Effect.gen(function* () {
      while (true) {
        if (closed || current !== generation || published === revision) return
        yield* Effect.sleep("100 millis")
        yield* publication.withPermit(Effect.suspend(() => current === generation ? send : Effect.void))
      }
    }).pipe(
      Effect.ensuring(Effect.sync(() => {
        if (worker === pending) worker = undefined
      })),
      Effect.forkIn(scope),
    )
  })
  return { set, flush, close }
})

function increasing(previous: SessionStatusEvent.Info, next: SessionStatusEvent.Info) {
  if (previous.type === "idle" || next.type === "idle" || previous.type !== next.type) return false
  if (previous.type === "retry" && next.type === "retry" &&
    (previous.attempt !== next.attempt || previous.next !== next.next || previous.message !== next.message ||
      JSON.stringify(previous.action) !== JSON.stringify(next.action))) return false
  const before = previous.activity
  const after = next.activity
  if (!before || !after || before.model !== after.model || before.userMessageID !== after.userMessageID) return false
  if (before.streamEventCount === undefined || after.streamEventCount === undefined || after.streamEventCount === 0 ||
    after.streamEventCount < before.streamEventCount) return false
  if (before.lastStreamEventAt !== after.lastStreamEventAt &&
    (before.lastStreamEventAt === undefined || after.lastStreamEventAt === undefined ||
      after.lastStreamEventAt < before.lastStreamEventAt)) return false
  return after.streamEventCount > before.streamEventCount || after.lastStreamEventAt !== before.lastStreamEventAt
}

export * as SessionStatusCoalescer from "./status-coalescer"

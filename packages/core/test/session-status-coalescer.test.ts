import { expect } from "bun:test"
import { Deferred, Effect, Fiber, Scope, Exit } from "effect"
import { TestClock } from "effect/testing"
import { SessionStatusCoalescer } from "../src/session/status-coalescer"
import { SessionMessage } from "@opencode-ai/schema/session-message"
import type { SessionStatusEvent } from "@opencode-ai/schema/session-status-event"
import { it } from "./lib/effect"

const owner = SessionMessage.ID.create()
const busy = (count: number | undefined, userMessageID = owner): SessionStatusEvent.Info => ({
  type: "busy",
  activity: { model: "receiving", userMessageID, streamEventCount: count, lastStreamEventAt: count },
})

it.effect("status coalescer keeps one trailing window across a burst and quiet gap", () => Effect.gen(function* () {
  const statuses: SessionStatusEvent.Info[] = []
  const coalescer = yield* SessionStatusCoalescer.make((status) => Effect.sync(() => { statuses.push(status) }))
  yield* coalescer.set(busy(1))
  for (let count = 2; count <= 199; count++) yield* coalescer.set(busy(count))
  yield* TestClock.adjust("99 millis")
  expect(statuses).toEqual([busy(1)])
  yield* coalescer.set(busy(200))
  yield* TestClock.adjust("1 millis")
  expect(statuses).toEqual([busy(1), busy(200)])
  yield* TestClock.adjust("1 second")
  expect(statuses).toHaveLength(2)
  yield* coalescer.set(busy(201))
  yield* TestClock.adjust("99 millis")
  expect(statuses).toHaveLength(2)
  yield* TestClock.adjust("1 millis")
  expect(statuses.at(-1)).toEqual(busy(201))
  yield* coalescer.flush
  yield* coalescer.flush
  expect(statuses).toHaveLength(3)
}))

it.effect("status coalescer restarts after cancellation before worker startup", () => Effect.gen(function* () {
  const statuses: SessionStatusEvent.Info[] = []
  const coalescer = yield* SessionStatusCoalescer.make((status) => Effect.sync(() => { statuses.push(status) }))
  yield* coalescer.set(busy(1))
  yield* coalescer.set(busy(2))
  yield* coalescer.flush
  expect(statuses).toEqual([busy(1), busy(2)])
  yield* coalescer.set(busy(3))
  yield* TestClock.adjust("100 millis")
  expect(statuses).toEqual([busy(1), busy(2), busy(3)])
}))

it.effect("status coalescer barriers cancel pending owner reset phase retry and terminal updates", () => Effect.gen(function* () {
  const statuses: SessionStatusEvent.Info[] = []
  const coalescer = yield* SessionStatusCoalescer.make((status) => Effect.sync(() => { statuses.push(status) }))
  const next = SessionMessage.ID.create()
  const barriers: SessionStatusEvent.Info[] = [
    busy(3, next), busy(0), busy(undefined),
    { type: "busy", activity: { model: "settling", userMessageID: owner, streamEventCount: 200 } },
    { type: "retry", attempt: 1, next: 1000, message: "retry" },
    { type: "idle", terminal: { userMessageID: owner, reason: "completed" } },
  ]
  for (const barrier of barriers) {
    yield* coalescer.set(busy(1))
    yield* coalescer.set(busy(2))
    const before = statuses.length
    yield* coalescer.set(barrier)
    expect(statuses.at(-1)).toEqual(barrier)
    expect(statuses).toHaveLength(before + 1)
    yield* TestClock.adjust("100 millis")
    expect(statuses).toHaveLength(before + 1)
  }
  yield* coalescer.set(busy(1))
  yield* coalescer.set(busy(2))
  yield* coalescer.flush
  expect(statuses.at(-1)).toEqual(busy(2))
  const before = statuses.length
  yield* coalescer.set(busy(3))
  yield* coalescer.close
  yield* coalescer.set(busy(4))
  yield* TestClock.adjust("100 millis")
  expect(statuses).toHaveLength(before)
}))

it.effect("status coalescer keeps raw updates free during publication and cancels in-flight work", () => Effect.gen(function* () {
  const entered = yield* Deferred.make<void>()
  const release = yield* Deferred.make<void>()
  const statuses: SessionStatusEvent.Info[] = []
  const coalescer = yield* SessionStatusCoalescer.make((status) => Effect.gen(function* () {
    if (status.type === "busy" && status.activity?.streamEventCount === 2) {
      yield* Deferred.succeed(entered, undefined)
      yield* Deferred.await(release)
    }
    statuses.push(status)
  }))
  yield* coalescer.set(busy(1))
  yield* coalescer.set(busy(2))
  const clock = yield* TestClock.adjust("100 millis").pipe(Effect.forkScoped)
  yield* Deferred.await(entered)
  yield* coalescer.set(busy(3))
  yield* coalescer.set({ type: "idle" })
  yield* Deferred.succeed(release, undefined)
  yield* Fiber.join(clock)
  yield* TestClock.adjust("100 millis")
  expect(statuses).toEqual([busy(1), { type: "idle" }])
}))

it.effect("status coalescer retains dirty updates during publication and owns its scope", () => Effect.gen(function* () {
  const entered = yield* Deferred.make<void>()
  const release = yield* Deferred.make<void>()
  const scope = yield* Scope.make()
  const statuses: SessionStatusEvent.Info[] = []
  const coalescer = yield* SessionStatusCoalescer.make((status) => Effect.gen(function* () {
    if (status.type === "busy" && status.activity?.streamEventCount === 2) {
      yield* Deferred.succeed(entered, undefined)
      yield* Deferred.await(release)
    }
    statuses.push(status)
  })).pipe(Effect.provideService(Scope.Scope, scope))
  yield* coalescer.set(busy(1)).pipe(Effect.scoped)
  yield* coalescer.set(busy(2)).pipe(Effect.scoped)
  const clock = yield* TestClock.adjust("100 millis").pipe(Effect.forkScoped)
  yield* Deferred.await(entered)
  yield* coalescer.set(busy(3))
  yield* Deferred.succeed(release, undefined)
  yield* Fiber.join(clock)
  expect(statuses).toEqual([busy(1), busy(2)])
  yield* TestClock.adjust("99 millis")
  expect(statuses).toHaveLength(2)
  yield* TestClock.adjust("1 millis")
  expect(statuses.at(-1)).toEqual(busy(3))
  yield* coalescer.set(busy(4))
  yield* Scope.close(scope, Exit.void)
  yield* TestClock.adjust("100 millis")
  expect(statuses).toHaveLength(3)
}))

it.effect("status coalescer invalidates a timer waiting for the publication permit", () => Effect.gen(function* () {
  const entered = yield* Deferred.make<void>()
  const release = yield* Deferred.make<void>()
  const statuses: SessionStatusEvent.Info[] = []
  const coalescer = yield* SessionStatusCoalescer.make((status) => Effect.gen(function* () {
    if (status.type === "busy" && status.activity?.streamEventCount === 1) {
      yield* Deferred.succeed(entered, undefined)
      yield* Deferred.await(release)
    }
    statuses.push(status)
  }))
  const first = yield* coalescer.set(busy(1)).pipe(Effect.forkScoped)
  yield* Deferred.await(entered)
  yield* coalescer.set(busy(2))
  yield* TestClock.adjust("100 millis")
  const reset = yield* coalescer.set(busy(0)).pipe(Effect.forkScoped)
  yield* TestClock.adjust("0 millis")
  yield* Deferred.succeed(release, undefined)
  yield* Fiber.join(first)
  yield* Fiber.join(reset)
  yield* TestClock.adjust("100 millis")
  expect(statuses).toEqual([busy(1), busy(0)])
}))

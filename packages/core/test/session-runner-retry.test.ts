import { describe, expect } from "bun:test"
import { AIError, InvalidProviderOutputError, ProviderInternalError, TimeoutError, TransportError } from "@opencode/ai"
import { Agent } from "@opencode/schema/agent"
import { Model } from "@opencode/schema/model"
import { Provider } from "@opencode/schema/provider"
import { Session } from "@opencode/schema/session"
import { SessionRunnerRetry } from "@opencode/core/session/runner/retry"
import { Effect, Exit, Fiber, Option, Stream } from "effect"
import { TestClock } from "effect/testing"
import { it } from "./lib/effect"

const timeout = new AIError({
  reason: new TransportError({ message: "Timed out", transport: "http", operation: "request", code: "Timeout" }),
})
const internal = new AIError({ reason: new ProviderInternalError({ message: "internal" }) })

const input = (cause: AIError) => ({
  cause,
  error: { type: "provider.internal", message: cause.message },
  agent: Agent.ID.make("build"),
  model: Model.Ref.make({ id: Model.ID.make("model"), providerID: Provider.ID.make("provider") }),
  hook: () => Effect.void,
  retry: SessionRunnerRetry.isRetryable(cause),
})

// Decisions for `count` consecutive failures of the same cause within one step.
const decisions = (cause: AIError, count: number) =>
  Effect.gen(function* () {
    const policy = yield* SessionRunnerRetry.policy(Session.ID.make("ses_retry"))
    const results: boolean[] = []
    for (let i = 0; i < count; i++) results.push((yield* policy(input(cause))).retry)
    return results
  })

describe("SessionRunnerRetry.policy", () => {
  for (const child of [false, true]) it.effect(`uses semantic deadline budget for ${child ? "child" : "main"}, not generic timeouts`, () => Effect.gen(function* () {
    const fiber = yield* SessionRunnerRetry.firstEventDeadline(Stream.never, Boolean, true).pipe(
      Stream.runDrain, Effect.forkChild({ startImmediately: true }),
    )
    yield* TestClock.adjust(100_000)
    const cause = Option.getOrThrow(Exit.findErrorOption(yield* Fiber.await(fiber)))
    for (const fixture of [
      { cause, count: child ? 1 : 10 },
      { cause: timeout, count: 3 },
      { cause: new AIError({ reason: new TimeoutError({ message: cause.message, timeoutMs: 100_000 }) }), count: 3 },
    ]) {
      const policy = yield* SessionRunnerRetry.policy(Session.ID.create())
      const results: boolean[] = []
      for (let index = 0; index <= fixture.count; index++) results.push((yield* policy({
        ...input(fixture.cause), retry: true, noResponse: fixture.cause === cause,
        parentID: child ? Session.ID.create() : undefined,
        hook: (event) => Effect.sync(() => { event.decision = { retry: true, delay: 0 } }),
      })).retry)
      expect(results).toEqual([...Array(fixture.count).fill(true), false])
    }
  }))

  it.effect("stops retrying transport timeouts after three attempts", () =>
    Effect.gen(function* () {
      expect(yield* decisions(timeout, 4)).toEqual([true, true, true, false])
    }),
  )

  it.effect("keeps the general allowance for other transient failures", () =>
    Effect.gen(function* () {
      expect(yield* decisions(internal, 6)).toEqual([true, true, true, true, true, false])
    }),
  )

  for (const child of [false, true]) it.effect(`bounds empty responses for ${child ? "child" : "main"} sessions`, () =>
    Effect.gen(function* () {
      const policy = yield* SessionRunnerRetry.policy(Session.ID.make("ses_empty"))
      const count = child ? 1 : 10
      const cause = new AIError({ reason: new InvalidProviderOutputError({ message: "empty" }) })
      const results: boolean[] = []
      for (let index = 0; index <= count; index++) results.push((yield* policy({
        ...input(cause), retry: true, noResponse: true,
        parentID: child ? Session.ID.make("ses_parent") : undefined,
      })).retry)
      expect(results).toEqual([...Array(count).fill(true), false])
    }),
  )

  it.effect("allows one provider abort before output, never after output", () => Effect.gen(function* () {
    const policy = yield* SessionRunnerRetry.policy(Session.ID.make("ses_abort"))
    const cause = new AIError({ reason: new TransportError({ message: "abort", transport: "websocket", operation: "read", code: "AbortError" }) })
    const request = { ...input(cause), retry: true }
    expect((yield* policy(request)).retry).toBe(true)
    expect((yield* policy(request)).retry).toBe(false)
    const visible = yield* SessionRunnerRetry.policy(Session.ID.make("ses_visible"))
    expect((yield* visible({ ...request, outputStarted: true })).retry).toBe(false)
    const structural = yield* SessionRunnerRetry.policy(Session.ID.make("ses_structural"))
    expect((yield* structural({ ...request, outputStarted: true, noResponse: true })).retry).toBe(true)
  }))

  it.effect("resets consecutive failures only on accepted text or reasoning progress", () => Effect.gen(function* () {
    const policy = yield* SessionRunnerRetry.policy(Session.ID.make("ses_progress"))
    for (let index = 0; index < 5; index++) yield* policy(input(internal))
    expect((yield* policy({ ...input(internal), meaningfulProgress: false })).retry).toBe(false)
    expect((yield* policy({ ...input(internal), meaningfulProgress: true })).retry).toBe(true)
    for (let index = 0; index < 4; index++) expect((yield* policy(input(internal))).retry).toBe(true)
    expect((yield* policy(input(internal))).retry).toBe(false)
  }))

  it.effect("caps provider and hook delays at thirty seconds", () => Effect.gen(function* () {
    const policy = yield* SessionRunnerRetry.policy(Session.ID.make("ses_delay"))
    const cause = new AIError({ reason: new ProviderInternalError({ message: "busy", retryAfterMs: 900_000 }) })
    const decision = yield* policy({ ...input(cause), hook: (event) => Effect.sync(() => {
      expect(event.decision).toEqual({ retry: true, delay: 30_000 })
      event.decision = { retry: true, delay: 900_000 }
    }) })
    expect(decision.retry && decision.delay).toBe(30_000)
  }))

  it.effect("hooks cannot bypass exhaustion, cancellation, or invalid files", () => Effect.gen(function* () {
    const policy = yield* SessionRunnerRetry.policy(Session.ID.make("ses_hook"))
    for (let index = 0; index < 6; index++) yield* policy(input(internal))
    expect((yield* policy({ ...input(internal), hook: (event) => Effect.sync(() => {
      event.decision = { retry: true, delay: 0 }
    }) })).retry).toBe(false)
    for (const type of ["aborted", "provider.invalid-file"]) expect((yield* policy({
      ...input(internal), error: { type, message: "blocked" }, hook: () => Effect.die("must not call hook"),
    })).retry).toBe(false)
  }))

  it.effect("post-tool deadline ignores structural events and fails at 100000ms", () => Effect.gen(function* () {
    const fiber = yield* SessionRunnerRetry.firstEventDeadline(
      Stream.concat(Stream.succeed(false), Stream.never), Boolean, true,
    ).pipe(Stream.runDrain, Effect.forkChild({ startImmediately: true }))
    yield* TestClock.adjust(99_999)
    expect(fiber.pollUnsafe()).toBeUndefined()
    yield* TestClock.adjust(1)
    const exit = yield* Fiber.await(fiber)
    expect(Exit.isFailure(exit)).toBe(true)
    const failure = Option.getOrUndefined(Exit.findErrorOption(exit))
    expect(failure?.reason._tag).toBe("Timeout")
    if (failure?.reason._tag === "Timeout") expect(failure.reason.timeoutMs).toBe(100_000)
    if (Exit.isFailure(exit)) expect(Exit.hasInterrupts(exit)).toBe(false)
  }))

  it.effect("user interruption stays interruption before the post-tool deadline", () => Effect.gen(function* () {
    const fiber = yield* SessionRunnerRetry.firstEventDeadline(Stream.never, Boolean, true).pipe(
      Stream.runDrain, Effect.forkChild({ startImmediately: true }),
    )
    yield* Fiber.interrupt(fiber)
    expect(Exit.hasInterrupts(yield* Fiber.await(fiber))).toBe(true)
  }))

  for (const enabled of [false, true]) it.effect(`deadline guard preserves ${enabled ? "semantic output" : "ordinary requests"}`, () => Effect.gen(function* () {
    const fiber = yield* SessionRunnerRetry.firstEventDeadline(
      Stream.concat(Stream.succeed(enabled), Stream.never), Boolean, enabled,
    ).pipe(Stream.runDrain, Effect.forkChild({ startImmediately: true }))
    yield* TestClock.adjust(100_001)
    expect(fiber.pollUnsafe()).toBeUndefined()
    yield* Fiber.interrupt(fiber)
  }))
})

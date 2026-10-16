export * as SessionRunnerRetry from "./retry.js"

import { AIError, TimeoutError, isRetryable } from "@opencode/ai"
import { Agent } from "@opencode/schema/agent"
import { Model } from "@opencode/schema/model"
import { SessionError } from "@opencode/schema/session-error"
import { Clock, Deferred, Duration, Effect, Stream } from "effect"
import { Bus } from "../../bus.js"
import type { PluginHooks } from "../../plugin/hooks.js"
import { SessionEvent } from "../event.js"
import { SessionMessage } from "../message.js"
import { SessionSchema } from "../schema.js"

export { isRetryable }

export interface Context {
  readonly parentID?: SessionSchema.ID
  readonly noResponse?: boolean
  readonly outputStarted?: boolean
  readonly meaningfulProgress?: boolean
}

interface Input extends Context {
  readonly cause: AIError
  readonly error: SessionError.Error
  readonly agent: Agent.ID
  readonly model: Model.Ref
  readonly hook: (event: PluginHooks.Domains["session"]["retry"]) => Effect.Effect<void>
  readonly retry: boolean
}

export interface Decision {
  readonly retry: true
  readonly attempt: number
  readonly delay: number
}

/** Bound provider-requested delays so a hostile or buggy retry-after cannot stall a session for hours. */
const RETRY_AFTER_MAX = 30_000

const retryAfter = (input: Input) => {
  if (input.cause.reason._tag === "RateLimit" || input.cause.reason._tag === "ProviderInternal")
    return input.cause.reason.retryAfterMs === undefined
      ? undefined
      : Math.min(input.cause.reason.retryAfterMs, RETRY_AFTER_MAX)
  return undefined
}

// A timed-out attempt already waited minutes before failing, so the general allowance would let a
// dead provider hold a step for most of an hour. Cap those attempts well below it.
const MAX_TIMEOUT_RETRIES = 3

const isTimeout = (error: AIError) =>
  error.reason._tag === "Timeout" ||
  (error.reason._tag === "Transport" && error.reason.code === "Timeout")

export const isProviderAbort = (error: AIError) =>
  error.reason._tag === "Transport" &&
  (error.reason.code === "AbortError" || error.reason.code === "ABORT_ERR" || error.reason.code === "Aborted")

class PostToolDeadline extends AIError {
  constructor() {
    super({ reason: new TimeoutError({ message: "No semantic response after tool continuation", timeoutMs: 100_000 }) })
  }
}

export const isPostToolDeadline = (error: AIError) => error instanceof PostToolDeadline

export const firstEventDeadline = <A, E, R>(
  stream: Stream.Stream<A, E, R>,
  semantic: (event: A) => boolean,
  enabled: boolean,
) =>
  enabled
    ? Stream.unwrap(Effect.gen(function* () {
        const first = yield* Deferred.make<void>()
        return stream.pipe(
          Stream.tap((event) => semantic(event) ? Deferred.succeed(first, undefined) : Effect.void),
          Stream.interruptWhen(Deferred.await(first).pipe(
            Effect.timeoutOrElse({
              duration: 100_000,
              orElse: () => Effect.fail(new PostToolDeadline()),
            }),
            Effect.andThen(Effect.never),
          )),
        )
      }))
    : stream

export const policy = (sessionID: SessionSchema.ID) =>
  Effect.gen(function* () {
    let attempt = 1
    let timeouts = 0
    let failures = 0
    let empty = 0
    let aborts = 0
    return (input: Input) =>
      Effect.gen(function* () {
        if (input.error.type === "provider.invalid-file" || input.error.type === "aborted") return { retry: false as const }
        if (input.meaningfulProgress) failures = 0
        attempt++
        if (isTimeout(input.cause) && !isPostToolDeadline(input.cause)) timeouts++
        const abort = isProviderAbort(input.cause)
        if (abort) aborts++
        if (input.noResponse && !abort) empty++
        if (!input.noResponse && !abort) failures++
        const allowed = timeouts <= MAX_TIMEOUT_RETRIES &&
          (abort ? (!input.outputStarted || input.noResponse === true) && aborts <= 1 : input.noResponse ? empty <= (input.parentID ? 1 : 10) : failures <= 5)
        const delay = Math.min(RETRY_AFTER_MAX, Math.max(2_000 * 2 ** Math.min(attempt - 2, 4), retryAfter(input) ?? 0))
        const event: PluginHooks.Domains["session"]["retry"] = {
          sessionID,
          agent: input.agent,
          model: input.model,
          error: input.error,
          attempt,
          decision: input.retry && allowed ? { retry: true, delay } : { retry: false },
        }
        yield* input.hook(event)
        if (!allowed || !event.decision.retry) return { retry: false as const }
        const normalized =
          Number.isFinite(event.decision.delay) && event.decision.delay >= 0 ? Math.ceil(event.decision.delay) : delay
        return { retry: true as const, attempt, delay: Math.min(normalized, RETRY_AFTER_MAX) }
      })
  })

export const make = (bus: Bus.Interface, sessionID: SessionSchema.ID) =>
  Effect.gen(function* () {
    const decide = yield* policy(sessionID)
    const wait = (input: {
      readonly decision: Decision
      readonly assistantMessageID: SessionMessage.ID
      readonly error: SessionError.Error
    }) =>
      Effect.gen(function* () {
        const scheduled = yield* Clock.currentTimeMillis
        yield* bus.publish(SessionEvent.RetryScheduled, {
          sessionID,
          assistantMessageID: input.assistantMessageID,
          attempt: input.decision.attempt,
          at: scheduled + input.decision.delay,
          error: input.error,
        })
        const remaining = Math.max(0, scheduled + input.decision.delay - (yield* Clock.currentTimeMillis))
        yield* Effect.sleep(Duration.millis(remaining))
      })
    return { decide, wait }
  })

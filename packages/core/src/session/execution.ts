export * as SessionExecution from "./execution.js"

import { Cause, Context, Effect, Exit, Layer, Scope } from "effect"
import { SessionStatusEvent } from "@opencode/schema/session-status-event"
import { SessionStatusCoalescer } from "./status-coalescer.js"
import { Bus } from "../bus.js"
import { Database } from "../database/database.js"
import { Job } from "../job.js"
import { Instance } from "../instance/service.js"
import { makeGlobalNode } from "@opencode/util/effect/app-node"
import { SessionEvent } from "./event.js"
import { SessionRunCoordinator } from "./run-coordinator.js"
import { SessionRunner } from "./runner/index.js"
import { SessionSchema } from "./schema.js"
import { SessionStore } from "./store.js"
import { toSessionError } from "./to-session-error.js"
import { SessionInbox } from "./inbox.js"

export interface Interface {
  /** Snapshots active execution owned by this process. */
  readonly active: Effect.Effect<ReadonlySet<SessionSchema.ID>>
  readonly snapshot: Effect.Effect<ReadonlyMap<SessionSchema.ID, SessionStatusEvent.Info>>
  /** Checks process-local ownership, including interruption cleanup and terminal settlement. */
  readonly isActive: (sessionID: SessionSchema.ID) => Effect.Effect<boolean>
  /** Starts execution while idle or joins the active execution. */
  readonly resume: (sessionID: SessionSchema.ID) => Effect.Effect<void, SessionRunner.RunError>
  /** Registers newly recorded work. Repeated wakeups may coalesce. */
  readonly wake: (sessionID: SessionSchema.ID) => Effect.Effect<void>
  /**
   * Interrupt active work owned by this process. Idle interruption is a no-op. Resolves once
   * the interruption is accepted; cleanup settles asynchronously in the execution fiber.
   * Returns whether an active execution was interrupted. Compose with `awaitIdle` when
   * settlement matters. `awaitSettlement` waits only for the interrupted execution,
   * rather than fresh work admitted during its cleanup.
   */
  readonly interrupt: (
    sessionID: SessionSchema.ID,
    options?: {
      readonly resume?: boolean
      readonly reason?: "user" | "inactivity"
      readonly awaitSettlement?: boolean
    },
  ) => Effect.Effect<boolean>
  /** Resolves once this process owns no active execution for the Session. Returns immediately when idle and never starts work. */
  readonly awaitIdle: (sessionID: SessionSchema.ID) => Effect.Effect<void>
}

/** Routes execution from a Session ID to its selected instance's runner. */
export class Service extends Context.Service<Service, Interface>()("@opencode/SessionExecution") {}

type InterruptReason = "user" | "shutdown" | "inactivity"

export function terminal(exit: Exit.Exit<void, SessionRunner.RunError>, reason?: InterruptReason) {
  if (Exit.isSuccess(exit)) return { type: "succeeded" as const }
  if (Cause.hasInterrupts(exit.cause)) return { type: "interrupted" as const, reason: reason ?? "shutdown" }
  return { type: "failed" as const, error: toSessionError(Cause.squash(exit.cause)) }
}

/** Process-local execution: drains run in this process using the selected instance. */
export const layer = Layer.effect(
  Service,
  Effect.gen(function* () {
    const store = yield* SessionStore.Service
    const instances = yield* Instance.Service
    const bus = yield* Bus.Service
    const jobs = yield* Job.Service
    const db = (yield* Database.Service).db
    const snapshots = new Map<SessionSchema.ID, {
      status: SessionStatusEvent.Info
      activity?: SessionStatusEvent.Activity
      error?: string
      coalescer: Effect.Success<ReturnType<typeof SessionStatusCoalescer.make>>
      scope: Scope.Closeable
    }>()
    const reportLifecycle = <A>(sessionID: SessionSchema.ID, effect: Effect.Effect<A>) =>
      effect.pipe(
        Effect.tapCause((cause) =>
          Cause.hasInterruptsOnly(cause)
            ? Effect.void
            : Effect.logError("Failed to publish Session execution lifecycle", cause).pipe(
                Effect.annotateLogs({ sessionID }),
              ),
        ),
        Effect.asVoid,
      )
    // Write-ahead claim: starting records the durable intent that a turn is in flight, in the same
    // transaction as the started event. Terminals release it — except shutdown interruption, which
    // preserves the claim so the next server start resumes the turn. A claim that survives with no
    // terminal is the signature of a process that died without teardown (crash, SIGKILL, eviction);
    // recovery is a property of the database, never of a shutdown hook that may not run.
    const claimOnCommit = (sessionID: SessionSchema.ID) => ({
      commit: () => store.claim(sessionID),
    })
    const releaseOnCommit = (sessionID: SessionSchema.ID) => ({
      commit: () => store.release(sessionID),
    })
    const drain = Effect.fnUntraced(function* (
      sessionID: SessionSchema.ID,
      force: boolean,
      continuation?: SessionRunner.Continuation,
      promotable: SessionInbox.Promotable = "input",
    ): Effect.fn.Return<void, SessionRunner.RunError> {
      const session = yield* store.get(sessionID)
      if (!session) return yield* Effect.die(new Error(`Session not found: ${sessionID}`))
      const result = yield* SessionRunner.Service.use((runner) =>
        runner.drain({
          sessionID, force, continuation, promotable,
          onActivity: (activity) => Effect.gen(function* () {
            const snapshot = snapshots.get(sessionID)
            if (!snapshot) return
            snapshot.activity = activity
            snapshot.status = { type: "busy", activity }
            if (activity.model === "preparing") snapshot.error = undefined
            yield* snapshot.coalescer.set(snapshot.status)
          }),
          onError: (message) => Effect.sync(() => {
            const snapshot = snapshots.get(sessionID)
            if (snapshot) snapshot.error = message
          }),
        }),
      ).pipe(
        instances.provide(session),
        Effect.tapCause((cause) =>
          Cause.hasInterruptsOnly(cause)
            ? Effect.void
            : Effect.logError("Failed to drain Session", cause).pipe(Effect.annotateLogs({ sessionID })),
        ),
      )
      return yield* SessionRunner.DrainResult.$match(result, {
        Complete: () => Effect.void,
        Moved: (result) => drain(sessionID, false, result.continuation, promotable),
        Reloaded: (result) => drain(sessionID, result.force, result.continuation, promotable),
      })
    })
    const coordinator = yield* SessionRunCoordinator.make<SessionSchema.ID, SessionRunner.RunError, InterruptReason>({
      started: (sessionID) => Effect.gen(function* () {
        const ownerScope = yield* Scope.make()
        const coalescer = yield* SessionStatusCoalescer.make((status) =>
          bus.publish(SessionStatusEvent.Status, { sessionID, status }).pipe(Effect.asVoid),
        ).pipe(Scope.provide(ownerScope))
        snapshots.set(sessionID, { status: { type: "busy" }, coalescer, scope: ownerScope })
        yield* reportLifecycle(
          sessionID,
          bus.publish(SessionEvent.Execution.Started, { sessionID }, claimOnCommit(sessionID)),
        )
      }),
      drain: (sessionID, force, promotable) => drain(sessionID, force, undefined, promotable),
      // One terminal observation per busy period, covering every coalesced drain.
      settled: (sessionID, exit, reason) =>
        reportLifecycle(
          sessionID,
          Effect.gen(function* () {
            const outcome = terminal(exit, reason)
            const snapshot = snapshots.get(sessionID)
            if (snapshot) {
              yield* snapshot.coalescer.flush
              snapshot.status = {
                type: "idle",
                terminal: {
                  userMessageID: snapshot.activity?.userMessageID,
                  reason: outcome.type === "interrupted"
                    ? "cancelled"
                    : outcome.type === "failed" || snapshot.error !== undefined ? "error" : "completed",
                  message: snapshot.error ?? (Exit.isFailure(exit) && outcome.type === "failed" ? Cause.pretty(exit.cause) : undefined),
                },
              }
            }
            yield* Effect.gen(function* () {
              if (outcome.type === "succeeded") {
                yield* bus.publish(SessionEvent.Execution.Succeeded, { sessionID }, releaseOnCommit(sessionID))
                return
              }
              if (outcome.type === "interrupted") {
                // Deliberate stops release the claim; shutdown keeps it for restart continuity.
                if (outcome.reason !== "shutdown") yield* jobs.cancel(sessionID)
                yield* bus.publish(
                  SessionEvent.Execution.Interrupted,
                  { sessionID, reason: outcome.reason },
                  outcome.reason === "shutdown" ? undefined : releaseOnCommit(sessionID),
                )
                return
              }
              yield* bus.publish(
                SessionEvent.Execution.Failed,
                {
                  sessionID,
                  error: outcome.error,
                },
                releaseOnCommit(sessionID),
              )
            }).pipe(Effect.ensuring(Effect.gen(function* () {
              if (!snapshot) return
              yield* (snapshot.activity ? snapshot.coalescer.set(snapshot.status) : Effect.void).pipe(
                Effect.ensuring(Scope.close(snapshot.scope, Exit.void)),
                Effect.ensuring(Effect.sync(() => { snapshots.delete(sessionID) })),
              )
            })))
          }),
        ),
    })

    return Service.of({
      active: coordinator.active,
      snapshot: Effect.gen(function* () {
        const active = yield* coordinator.active
        return new Map(Array.from(snapshots).flatMap(([sessionID, snapshot]) =>
          active.has(sessionID) && snapshot.status.type !== "idle" ? [[sessionID, snapshot.status] as const] : [],
        ))
      }),
      isActive: coordinator.isActive,
      interrupt: (sessionID, options) =>
        Effect.gen(function* () {
          const interrupted = yield* coordinator.interrupt(sessionID, options?.reason ?? "user", options)
          if (!options?.resume) return interrupted
          // Resume steering input and between-turn control work from the interrupted
          // intent. Queued next-turn prompts stay parked: a steer-scoped drain never
          // promotes them, and a control item behind a queued prompt waits its turn.
          // Interruption acknowledges before cleanup settles, so this wake usually lands
          // on the stopping execution's doorbell and starts the successor at settle.
          // Reading the inbox concurrently with the dying drain is safe: delivery consumes
          // rows inside uninterruptible publications, so a steer row is either still
          // promotable here or was fully delivered and needs no resumption.
          const next = yield* SessionInbox.nextPromotable(db, sessionID, "input")
          if (next === undefined) return interrupted
          if (next.delivery === "steer" || next.type === "compaction" || next.type === "move")
            yield* coordinator.wake(sessionID, "steer")
          return interrupted
        }),
      resume: coordinator.run,
      wake: coordinator.wake,
      awaitIdle: coordinator.awaitIdle,
    })
  }),
)

export const node = makeGlobalNode({
  service: Service,
  layer,
  deps: [SessionStore.node, Instance.node, Bus.node, Database.node, Job.node],
})

/** Low-level compatibility layer for callers that only need durable Session recording. */
export const noopLayer = Layer.succeed(
  Service,
  Service.of({
    active: Effect.succeed(new Set()),
    snapshot: Effect.succeed(new Map()),
    isActive: () => Effect.succeed(false),
    resume: () => Effect.void,
    wake: () => Effect.void,
    interrupt: () => Effect.succeed(false),
    awaitIdle: () => Effect.void,
  }),
)

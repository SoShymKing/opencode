import { Cause, Effect, Exit, Layer, Scope } from "effect"
import { SessionStatusEvent } from "@opencode-ai/schema/session-status-event"
import { EventV2 } from "../../event"
import type { Location } from "../../location"
import { LocationServiceMap } from "../../location-service-map"
import { makeGlobalNode } from "../../effect/app-node"
import { SessionRunCoordinator } from "../run-coordinator"
import { SessionRunner } from "../runner"
import { SessionSchema } from "../schema"
import { SessionStore } from "../store"
import { SessionExecution } from "../execution"
import { SessionStatusCoalescer } from "../status-coalescer"

/** Current-process routing for implicit-local Locations. Future remote placement belongs here. */
const layer = Layer.effect(
  SessionExecution.Service,
  Effect.gen(function* () {
    const store = yield* SessionStore.Service
    const locations = yield* LocationServiceMap.Service
    const events = yield* EventV2.Service
    const scope = yield* Effect.scope
    const snapshots = new Map<SessionSchema.ID, {
      activity: SessionStatusEvent.Activity
      status: SessionStatusEvent.Info
      location: Location.Ref
      error?: string
      coalescer: Effect.Success<ReturnType<typeof SessionStatusCoalescer.make>>
      scope: Scope.Closeable
    }>()
    const coordinator = yield* SessionRunCoordinator.make<SessionSchema.ID, SessionRunner.RunError>({
      drain: Effect.fnUntraced(function* (sessionID: SessionSchema.ID, force) {
        if (snapshots.get(sessionID)?.status.type === "idle") snapshots.delete(sessionID)
        const session = yield* store.get(sessionID)
        if (!session) return yield* Effect.die(`Session not found: ${sessionID}`)
        return yield* SessionRunner.Service.use((runner) => runner.run({
          sessionID,
          force,
          onActivity: (activity) => Effect.gen(function* () {
            const previous = snapshots.get(sessionID)
            const ownerScope = previous?.scope ?? (yield* Scope.fork(scope))
            const coalescer = previous?.coalescer ?? (yield* SessionStatusCoalescer.make((status) =>
              events.publish(SessionStatusEvent.Status, { sessionID, status }, { location: session.location }).pipe(Effect.asVoid),
            ).pipe(Scope.provide(ownerScope)))
            const status: SessionStatusEvent.Info = { type: "busy", activity }
            snapshots.set(sessionID, {
              activity,
              status,
              location: session.location,
              error: activity.model === "preparing" ? undefined : previous?.error,
              coalescer,
              scope: ownerScope,
            })
            yield* coalescer.set(status)
          }),
          onError: (message) => Effect.sync(() => {
            const snapshot = snapshots.get(sessionID)
            if (snapshot) snapshot.error = message
          }),
        })).pipe(
          Effect.provide(locations.get(session.location)),
          Effect.tapCause((cause) =>
            Cause.hasInterruptsOnly(cause)
              ? Effect.void
              : Effect.logError("Failed to drain Session", cause).pipe(Effect.annotateLogs({ sessionID })),
          ),
        )
      }),
      onSettled: (sessionID, exit) => Effect.gen(function* () {
        const snapshot = snapshots.get(sessionID)
        if (!snapshot) return
        const failed = exit._tag === "Failure"
        const cancelled = failed && Cause.hasInterruptsOnly(exit.cause)
        const message = snapshot.error ?? (failed && !cancelled ? Cause.pretty(exit.cause) : undefined)
        yield* snapshot.coalescer.flush
        snapshot.status = {
          type: "idle",
          terminal: {
            userMessageID: snapshot.activity.userMessageID,
            reason: cancelled ? "cancelled" : message === undefined ? "completed" : "error",
            message,
          },
        }
        yield* snapshot.coalescer.set(snapshot.status)
        yield* snapshot.coalescer.close
        yield* Scope.close(snapshot.scope, Exit.void)
      }),
    })

    return SessionExecution.Service.of({
      active: coordinator.active,
      snapshot: Effect.sync(() => {
        const active = Effect.runSync(coordinator.active)
        snapshots.forEach((_snapshot, sessionID) => {
          if (!active.has(sessionID)) snapshots.delete(sessionID)
        })
        return new Map(Array.from(active).flatMap((sessionID) => {
          const status = snapshots.get(sessionID)?.status ?? { type: "busy" as const }
          return status.type === "idle" ? [] : [[sessionID, status] as const]
        }))
      }),
      interrupt: coordinator.interrupt,
      resume: coordinator.run,
      wake: coordinator.wake,
    })
  }),
)

export const node = makeGlobalNode({
  service: SessionExecution.Service,
  layer,
  deps: [SessionStore.node, LocationServiceMap.node, EventV2.node],
})

export * as SessionExecutionLocal from "./local"

import { Cause, Effect, Layer } from "effect"
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

/** Current-process routing for implicit-local Locations. Future remote placement belongs here. */
const layer = Layer.effect(
  SessionExecution.Service,
  Effect.gen(function* () {
    const store = yield* SessionStore.Service
    const locations = yield* LocationServiceMap.Service
    const events = yield* EventV2.Service
    const snapshots = new Map<SessionSchema.ID, {
      activity: SessionStatusEvent.Activity
      location: Location.Ref
      error?: string
    }>()
    const coordinator = yield* SessionRunCoordinator.make<SessionSchema.ID, SessionRunner.RunError>({
      drain: Effect.fnUntraced(function* (sessionID: SessionSchema.ID, force) {
        const session = yield* store.get(sessionID)
        if (!session) return yield* Effect.die(`Session not found: ${sessionID}`)
        return yield* SessionRunner.Service.use((runner) => runner.run({
          sessionID,
          force,
          onActivity: (activity) => Effect.gen(function* () {
            snapshots.set(sessionID, {
              activity,
              location: session.location,
              error: activity.model === "preparing" ? undefined : snapshots.get(sessionID)?.error,
            })
            yield* events.publish(SessionStatusEvent.Status, { sessionID, status: { type: "busy", activity } }, {
              location: session.location,
            })
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
        yield* events.publish(SessionStatusEvent.Status, {
          sessionID,
          status: {
            type: "idle",
            terminal: {
              userMessageID: snapshot.activity.userMessageID,
              reason: cancelled ? "cancelled" : message === undefined ? "completed" : "error",
              message,
            },
          },
        }, { location: snapshot.location })
        snapshots.delete(sessionID)
      }),
    })

    return SessionExecution.Service.of({
      active: coordinator.active,
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

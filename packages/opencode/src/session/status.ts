import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { InstanceState } from "@/effect/instance-state"
import { SessionID } from "./schema"
import { Effect, Layer, Context, Scope, Exit } from "effect"
import { SessionStatusCoalescer } from "@opencode-ai/core/session/status-coalescer"
import { EventV2Bridge } from "@/event-v2-bridge"
import { SessionStatusEvent } from "@opencode-ai/schema/session-status-event"

export const Info = SessionStatusEvent.Info
export type Info = SessionStatusEvent.Info

export const Event = SessionStatusEvent

export interface Interface {
  readonly get: (sessionID: SessionID) => Effect.Effect<Info>
  readonly list: () => Effect.Effect<Map<SessionID, Info>>
  readonly set: (sessionID: SessionID, status: Info) => Effect.Effect<void>
  readonly flush: (sessionID: SessionID) => Effect.Effect<void>
}

export class Service extends Context.Service<Service, Interface>()("@opencode/SessionStatus") {}

const layer = Layer.effect(
  Service,
  Effect.gen(function* () {
    const events = yield* EventV2Bridge.Service

    const state = yield* InstanceState.make(
      Effect.fn("SessionStatus.state")(function* () {
        return {
          data: new Map<SessionID, Info>(),
          scope: yield* Scope.Scope,
          coalescers: new Map<SessionID, {
            scope: Scope.Closeable
            value: Effect.Success<ReturnType<typeof SessionStatusCoalescer.make>>
          }>(),
        }
      }),
    )

    const get = Effect.fn("SessionStatus.get")(function* (sessionID: SessionID) {
      const current = yield* InstanceState.get(state)
      return current.data.get(sessionID) ?? { type: "idle" as const }
    })

    const list = Effect.fn("SessionStatus.list")(function* () {
      return new Map([...(yield* InstanceState.get(state)).data].filter(([, value]) => value.type !== "idle"))
    })

    const set = Effect.fn("SessionStatus.set")(function* (sessionID: SessionID, status: Info) {
      const current = yield* InstanceState.get(state)
      const data = current.data
      const previous = data.get(sessionID)
      if (status.type === "idle" && !status.terminal && previous?.type === "idle" && previous.terminal) return
      data.set(sessionID, status)
      const entry = current.coalescers.get(sessionID) ?? (yield* Effect.gen(function* () {
        const scope = yield* Scope.fork(current.scope)
        const value = yield* SessionStatusCoalescer.make((status) =>
          events.publish(Event.Status, { sessionID, status }).pipe(Effect.asVoid),
        ).pipe(Effect.provideService(Scope.Scope, scope))
        const entry = { scope, value }
        current.coalescers.set(sessionID, entry)
        return entry
      }))
      yield* entry.value.set(status)
      if (status.type === "idle") {
        yield* Scope.close(entry.scope, Exit.void)
        current.coalescers.delete(sessionID)
        yield* events.publish(Event.Idle, { sessionID })
        if (!status.terminal) data.delete(sessionID)
        return
      }
    })

    const flush = Effect.fn("SessionStatus.flush")(function* (sessionID: SessionID) {
      const current = yield* InstanceState.get(state)
      yield* current.coalescers.get(sessionID)?.value.flush ?? Effect.void
    })

    return Service.of({ get, list, set, flush })
  }),
)

export const node = LayerNode.make({ service: Service, layer: layer, deps: [EventV2Bridge.node] })

export * as SessionStatus from "./status"

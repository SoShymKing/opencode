import { EventV2Bridge } from "@/event-v2-bridge"
import { InstanceState } from "@/effect/instance-state"
import { GlobalBus } from "@/bus/global"
import { EventV2 } from "@opencode-ai/core/event"
import { Effect, Stream } from "effect"
import { HttpServerResponse } from "effect/unstable/http"
import { HttpApiBuilder } from "effect/unstable/httpapi"
import { Sse } from "effect/unstable/encoding"
import { EventApi } from "../groups/event"
import { EventBuffer } from "./event-buffer"

function eventData(data: unknown): Sse.Event {
  return {
    _tag: "Event",
    event: "message",
    id: undefined,
    data: JSON.stringify(data),
  }
}

function eventID() {
  return EventV2.ID.create()
}

export function eventResponse(events: Pick<EventV2.Interface, "listen">) {
  return Effect.gen(function* () {
    const instance = yield* InstanceState.context
    const workspaceID = yield* InstanceState.workspaceID
    // Listener registration is eager, so events published after this point cannot
    // be lost while the HTTP body fiber is starting or emitting server.connected.
    const buffer = yield* EventBuffer.make<{ id: string; type: string; properties: unknown }>()
    const unsubscribe = yield* events.listen((event) =>
      Effect.sync(() => {
        if (event.location?.directory !== instance.directory) return
        if (event.location.workspaceID !== undefined && event.location.workspaceID !== workspaceID) return
        buffer.offer({ id: event.id, type: event.type, properties: event.data })
      }),
    )
    yield* Effect.addFinalizer(() => unsubscribe)
    const listener = (event: {
      directory?: string
      payload: { id?: string; type?: string; properties?: unknown }
    }) => {
      if (event.directory !== instance.directory || event.payload.type !== "server.instance.disposed") return
      buffer.offer({
        id: event.payload.id ?? eventID(),
        type: "server.instance.disposed",
        properties: event.payload.properties ?? {},
      })
    }
    yield* Effect.acquireRelease(
      Effect.sync(() => GlobalBus.on("event", listener)),
      () => Effect.sync(() => GlobalBus.off("event", listener)),
    )
    const output = buffer.stream.pipe(Stream.takeUntil((event) => event.type === "server.instance.disposed"))
    const heartbeat = Stream.tick("10 seconds").pipe(
      Stream.drop(1),
      Stream.map(() => ({ id: eventID(), type: "server.heartbeat", properties: {} })),
    )

    yield* Effect.logInfo("event connected")
    return HttpServerResponse.stream(
      Stream.fromEffect(buffer.check.pipe(Effect.as({ id: eventID(), type: "server.connected", properties: {} }))).pipe(
        Stream.concat(output.pipe(Stream.merge(heartbeat, { haltStrategy: "left" }))),
        Stream.map(eventData),
        Stream.pipeThroughChannel(Sse.encode()),
        Stream.encodeText,
        Stream.ensuring(Effect.logInfo("event disconnected")),
      ),
      {
        contentType: "text/event-stream",
        headers: {
          "Cache-Control": "no-cache, no-transform",
          "X-Accel-Buffering": "no",
          "X-Content-Type-Options": "nosniff",
        },
      },
    )
  })
}

export const eventHandlers = HttpApiBuilder.group(EventApi, "event", (handlers) =>
  Effect.gen(function* () {
    const events = yield* EventV2Bridge.Service
    return handlers.handleRaw(
      "subscribe",
      Effect.fn("EventHttpApi.subscribe")(function* () {
        return yield* eventResponse(events)
      }),
    )
  }),
)

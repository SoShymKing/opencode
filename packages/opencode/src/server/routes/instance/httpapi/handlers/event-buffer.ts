import { EventV2 } from "@opencode-ai/core/event"
import { Cause, Effect, Queue, Stream } from "effect"

export const make = <Payload>() =>
  Effect.gen(function* () {
    const queue = yield* Effect.acquireRelease(
      Queue.dropping<Payload, EventV2.SubscriberOverflowError>(256),
      Queue.shutdown,
    )
    return {
      offer: (payload: Payload) => {
        if (Queue.offerUnsafe(queue, payload)) return
        if (Queue.failCauseUnsafe(queue, Cause.fail(new EventV2.SubscriberOverflowError({ capacity: 256 })))) {
          Effect.runSync(Queue.shutdown(queue))
        }
      },
      check: Effect.suspend(() =>
        queue.state._tag === "Done" ? Effect.failCause(queue.state.exit.cause) : Effect.void,
      ),
      stream: Stream.fromQueue(queue),
    }
  })

export * as EventBuffer from "./event-buffer"

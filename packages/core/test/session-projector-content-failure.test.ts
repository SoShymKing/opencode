import { describe, expect } from "bun:test"
import { Effect } from "effect"
import { MessageDecodeError } from "@opencode-ai/core/session/error"
import { SessionEvent } from "@opencode-ai/core/session/event"
import { at, contentBase, it, setup } from "./session-projector-content.fixture"

describe("SessionProjector content row failures", () => {
  it.effect("rolls back the event and child append when the assistant envelope is malformed", () =>
    Effect.gen(function* () {
      const fixture = yield* setup("bad_envelope")
      const beforeEvents = yield* fixture.storedEvents
      yield* fixture.corruptEnvelope("{}")

      const exit = yield* fixture.events
        .publish(SessionEvent.Text.Started, {
          ...contentBase(fixture),
          textID: "text",
          timestamp: at(1),
        })
        .pipe(Effect.exit)

      expect(exit._tag).toBe("Failure")
      expect(yield* fixture.envelopeText).toBe("{}")
      expect(yield* fixture.children).toEqual([])
      expect(yield* fixture.storedEvents).toEqual(beforeEvents)
    }),
  )

  it.effect("rolls back the event and selected row when the target child is malformed", () =>
    Effect.gen(function* () {
      const fixture = yield* setup("bad_target")
      const base = contentBase(fixture)
      yield* fixture.events.publish(SessionEvent.Text.Started, { ...base, textID: "text", timestamp: at(1) })
      yield* fixture.corruptChild(0, JSON.stringify({ text: 42 }))
      const beforeEvents = yield* fixture.storedEvents

      const exit = yield* fixture.events
        .publish(SessionEvent.Text.Ended, { ...base, textID: "text", text: "blocked", timestamp: at(2) })
        .pipe(Effect.exit)

      expect(exit._tag).toBe("Failure")
      expect((yield* fixture.children)[0]?.dataText).toBe(JSON.stringify({ text: 42 }))
      expect(yield* fixture.storedEvents).toEqual(beforeEvents)
    }),
  )

  it.effect("updates a valid target and accepts a no-op with an unrelated malformed sibling", () =>
    Effect.gen(function* () {
      const fixture = yield* setup("bad_sibling")
      const base = contentBase(fixture)
      yield* fixture.events.publish(SessionEvent.Text.Started, { ...base, textID: "target", timestamp: at(1) })
      yield* fixture.events.publish(SessionEvent.Reasoning.Started, {
        ...base,
        reasoningID: "broken",
        timestamp: at(2),
      })
      yield* fixture.corruptChild(1, JSON.stringify({ text: 42 }))

      yield* fixture.events.publish(SessionEvent.Text.Ended, {
        ...base,
        textID: "target",
        text: "updated",
        timestamp: at(3),
      })
      const afterUpdate = yield* fixture.children
      expect(afterUpdate[0]?.dataText).toBe(JSON.stringify({ text: "updated" }))
      expect(afterUpdate[1]?.dataText).toBe(JSON.stringify({ text: 42 }))

      yield* fixture.events.publish(SessionEvent.Text.Ended, {
        ...base,
        textID: "target",
        text: "updated",
        timestamp: at(4),
      })
      expect(yield* fixture.children).toEqual(afterUpdate)

      const error = yield* fixture.read.pipe(Effect.flip)
      expect(error).toEqual(
        new MessageDecodeError({ sessionID: fixture.sessionID, messageID: fixture.assistantMessageID }),
      )
    }),
  )
})

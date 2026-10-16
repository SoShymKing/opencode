import { describe, expect, test } from "bun:test"
import { Schema } from "effect"
import { SessionEvent } from "../src/session-event.js"
import { SessionMessage } from "../src/session-message.js"
import { Event } from "../src/event.js"
import { EventLog } from "../src/event-log.js"

describe("public event schemas", () => {
  test("assistant stream counts are optional nonnegative integers", () => {
    for (const field of [
      SessionMessage.Assistant.fields.streamEventCount,
      SessionEvent.Step.Ended.data.fields.streamEventCount,
      SessionEvent.Step.Failed.data.fields.streamEventCount,
    ]) {
      const schema = Schema.Struct({ streamEventCount: field })
      expect(Schema.encodeSync(schema)({ streamEventCount: undefined })).toEqual({})
      expect(Schema.decodeUnknownSync(schema)({})).toEqual({})
      expect(Schema.decodeUnknownSync(schema)({ streamEventCount: 0 })).toEqual({ streamEventCount: 0 })
      expect(() => Schema.decodeUnknownSync(schema)({ streamEventCount: -1 })).toThrow()
      expect(() => Schema.decodeUnknownSync(schema)({ streamEventCount: 1.5 })).toThrow()
    }
    expect(SessionEvent.Definitions.some((definition) => definition.type.includes("stream.updated"))).toBe(false)
  })

  test("definition is pure", () => {
    const definitions = Event.inventory()
    Event.ephemeral({ type: "test.pure", schema: { value: Schema.String } })
    expect(definitions).toEqual([])
  })

  test("latest selection is independent of declaration order", () => {
    const historical = Event.durable({
      type: "test.versioned",
      durable: { aggregate: "id", version: 1 },
      schema: { id: Schema.String },
    })
    const current = Event.durable({
      type: "test.versioned",
      durable: { aggregate: "id", version: 2 },
      schema: { id: Schema.String, value: Schema.String },
    })

    expect(Event.latest([historical, current]).get(current.type)).toBe(current)
    expect(Event.latest([current, historical]).get(current.type)).toBe(current)
  })

  test("durable definitions are indexed by type and version", () => {
    const definition = Event.durable({
      type: "test.durable",
      durable: { aggregate: "id", version: 1 },
      schema: { id: Schema.String },
    })

    expect(Event.durableMap([definition]).get("test.durable.1")).toBe(definition)
  })

  test("synced marker encodes the captured watermark", () => {
    expect(
      Schema.encodeSync(EventLog.Synced)({
        type: "log.synced",
        aggregateID: "ses_test",
        seq: Event.Seq.make(1),
      }),
    ).toEqual({ type: "log.synced", aggregateID: "ses_test", seq: 1 })
    expect(Schema.encodeSync(EventLog.Synced)({ type: "log.synced", aggregateID: "ses_test" })).toEqual({
      type: "log.synced",
      aggregateID: "ses_test",
    })
  })
})

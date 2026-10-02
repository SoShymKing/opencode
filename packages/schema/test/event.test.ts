import { describe, expect, test } from "bun:test"
import { Schema } from "effect"
import { Event } from "../src/event"
import { SessionEvent } from "../src/session-event"
import { SessionMessage } from "../src/session-message"
import { SessionV1 } from "../src/v1/session"

describe("public event schemas", () => {
  test("assistant stream counts are optional nonnegative integers", () => {
    for (const field of [
      SessionV1.Assistant.fields.streamEventCount,
      SessionMessage.Assistant.fields.streamEventCount,
      SessionEvent.Step.Started.data.fields.streamEventCount,
    ]) {
      const schema = Schema.Struct({ streamEventCount: field })
      expect(Schema.encodeSync(schema)({ streamEventCount: undefined })).toEqual({})
      expect(Schema.decodeUnknownSync(schema)({})).toEqual({})
      expect(Schema.decodeUnknownSync(schema)({ streamEventCount: 0 })).toEqual({ streamEventCount: 0 })
      expect(() => Schema.decodeUnknownSync(schema)({ streamEventCount: -1 })).toThrow()
      expect(() => Schema.decodeUnknownSync(schema)({ streamEventCount: 1.5 })).toThrow()
    }
    expect(SessionEvent.Step.StreamUpdated.durable).toEqual({ aggregate: "sessionID", version: 1 })
  })

  test("definition is pure", () => {
    const definitions = Event.inventory()
    Event.define({ type: "test.pure", schema: { value: Schema.String } })
    expect(definitions).toEqual([])
  })

  test("latest selection is independent of declaration order", () => {
    const historical = Event.define({
      type: "test.versioned",
      durable: { aggregate: "id", version: 1 },
      schema: { id: Schema.String },
    })
    const current = Event.define({
      type: "test.versioned",
      durable: { aggregate: "id", version: 2 },
      schema: { id: Schema.String, value: Schema.String },
    })

    expect(Event.latest([historical, current]).get(current.type)).toBe(current)
    expect(Event.latest([current, historical]).get(current.type)).toBe(current)
  })

  test("durable definitions are indexed by type and version", () => {
    const definition = Event.define({
      type: "test.durable",
      durable: { aggregate: "id", version: 1 },
      schema: { id: Schema.String },
    })

    expect(Event.durable([definition]).get("test.durable.1")).toBe(definition)
  })
})

import { expect, test } from "bun:test"
import { Schema } from "effect"
import { SessionEvent } from "../src/session-event.js"

test("fork source cursor is optional and accepts integers starting at minus one", () => {
  const schema = Schema.Struct({ instructionSourceSeq: SessionEvent.Forked.data.fields.instructionSourceSeq })
  expect(Schema.encodeSync(schema)({})).toEqual({})
  expect(Schema.encodeSync(schema)({ instructionSourceSeq: undefined })).toEqual({})
  expect(Schema.decodeUnknownSync(schema)({})).toEqual({})
  for (const instructionSourceSeq of [-1, 0]) {
    expect(Schema.decodeUnknownSync(schema)({ instructionSourceSeq })).toEqual({ instructionSourceSeq })
    expect(Schema.encodeSync(schema)({ instructionSourceSeq })).toEqual({ instructionSourceSeq })
  }
  for (const instructionSourceSeq of [-2, 0.5]) {
    expect(() => Schema.decodeUnknownSync(schema)({ instructionSourceSeq })).toThrow()
  }
})

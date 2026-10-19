import { expect, test } from "bun:test"
import { Schema } from "effect"
import { Base64, FileAttachment } from "../src/prompt.js"

const pattern = "^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$"

test.each(["AAAA", "AAA=", "AA=="])("round-trips an 8 MiB attachment ending in %s", (ending) => {
  const data = "A".repeat(8 * 1024 * 1024 - 4) + ending
  expect(Schema.is(Base64)(data)).toBe(true)
  const decoded = Schema.decodeUnknownSync(FileAttachment)({ data, mime: "application/octet-stream", source: { type: "inline" } })
  const encoded = Schema.encodeSync(FileAttachment)(decoded)
  expect(decoded.data === data && encoded.data === data).toBe(true)
})

test.each(["", "AAAA", "AA==", "AAA=", "+/09", "/x==", "//9="])("accepts existing valid syntax %j", (data) => {
  expect(Schema.is(Base64)(data)).toBe(true)
})

test.each(["A", "AA", "AAA", "=AAA", "A=AA", "AA=A", "A===", "====", "AAAA=", "AA-_", "AA?=", " AA=", "AAAA\n", "AAAA\r", "AAAA\r\n", "AAAA\u2028", "AAAA\u2029", "AA\nA", "\n", "\t", "\u0000"])("rejects invalid syntax %j", (data) => {
  expect(Schema.is(Base64)(data)).toBe(false)
})

test("matches the previous predicate for short alphabet and padding combinations", () => {
  const previous = new RegExp(pattern, "u")
  const valid = Schema.is(Base64)
  let inputs = [""]
  for (const length of [1, 2, 3, 4, 5]) {
    inputs = inputs.flatMap((prefix) => ["A", "/", "+", "=", "\n"].map((char) => prefix + char))
    expect(inputs.every((input) => input.length === length && valid(input) === previous.test(input))).toBe(true)
  }
})

test("rejects invalid characters near the end of a large payload", () => {
  expect(Schema.is(Base64)("A".repeat(8 * 1024 * 1024 - 4) + "AA?=")).toBe(false)
})

test("preserves the Base64 identifier and JSON Schema document", () => {
  expect(Schema.toJsonSchemaDocument(Base64)).toEqual({
    dialect: "draft-2020-12",
    schema: { $ref: "#/$defs/Prompt.Base64" },
    definitions: { "Prompt.Base64": { type: "string", pattern } },
  })
})

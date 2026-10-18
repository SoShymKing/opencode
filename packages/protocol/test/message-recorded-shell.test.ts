import { expect, test } from "bun:test"
import { Schema } from "effect"
import { SessionMessage } from "@opencode/schema/session-message"
import { PublicSessionMessage, SessionMessagesQuery } from "../src/groups/message.js"

test("message type filter accepts recorded shells without changing omitted or idle filters", () => {
  const decode = Schema.decodeUnknownSync(SessionMessagesQuery)
  expect(decode({ type: "shell-recorded" })).toEqual({ type: "shell-recorded" })
  expect(decode({})).toEqual({})
  expect(() => decode({ type: "idle" })).toThrow()
  expect(() => decode({ type: "unknown" })).toThrow()
})

test("public messages retain canonical shell schema identities", () => {
  expect(PublicSessionMessage.members).toContain(SessionMessage.RecordedShell)
  expect(PublicSessionMessage.members).toContain(SessionMessage.Shell)
})

test.each([{}, { completed: 2000 }])("recorded shell wire roundtrip with time %j", (completion) => {
  const wire = {
    id: "msg_recorded",
    type: "shell-recorded" as const,
    callID: "call_recorded",
    command: "printf output",
    output: "raw\r\noutput\u0000\u001b[31m",
    time: { created: 1000, ...completion },
  }
  const source = Schema.decodeUnknownSync(SessionMessage.RecordedShell)(wire)
  const encoded = Schema.encodeSync(PublicSessionMessage)(source)
  expect(encoded).toEqual(wire)
  expect(encoded).not.toHaveProperty("shellID")
  expect(encoded).not.toHaveProperty("status")
  expect(encoded).not.toHaveProperty("exit")
  expect(Schema.decodeUnknownSync(PublicSessionMessage)(JSON.parse(JSON.stringify(encoded)))).toEqual(source)
})

test("native shell wire roundtrip preserves its lifecycle and paged output", () => {
  const wire = {
    id: "msg_native",
    type: "shell" as const,
    shellID: "sh_native",
    command: "printf output",
    status: "exited" as const,
    exit: 0,
    output: { output: "output", cursor: 6, size: 6, truncated: false },
    time: { created: 1000, completed: 2000 },
  }
  const source = Schema.decodeUnknownSync(SessionMessage.Shell)(wire)
  const encoded = Schema.encodeSync(PublicSessionMessage)(source)
  expect(encoded).toEqual(wire)
  expect(Schema.decodeUnknownSync(PublicSessionMessage)(JSON.parse(JSON.stringify(encoded)))).toEqual(source)
})

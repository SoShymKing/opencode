import { describe, expect, test } from "bun:test"
import { Schema } from "effect"
import { LLM, SessionError } from "../src/index.js"
import { SessionMessage } from "../src/session-message.js"

describe("SessionError", () => {
  test("exports one identified open envelope", () => {
    expect(SessionError.Error.ast.annotations?.identifier).toBe("Session.StructuredError")
    expect(
      Object.keys(SessionError).filter((key) => key !== "SessionError").sort(),
    ).toEqual(["Error", "RejectedAttachment"])
  })

  test("round trips current and future error types through JSON", () => {
    const values: SessionError.Error[] = [
      { type: "provider.rate-limit", message: "Slow down" },
      { type: "provider.rate-limit", message: "Slow down", status: 429, response: { body: '{"error":{}}' } },
      { type: "provider.auth", message: "Authentication failed" },
      { type: "provider.future-condition", message: "A future provider failure" },
      { type: "unknown", message: "Unexpected" },
    ]
    const codec = Schema.fromJsonString(SessionError.Error)

    for (const value of values) {
      const encoded = Schema.encodeSync(codec)(value)
      expect(Schema.decodeUnknownSync(codec)(encoded)).toEqual(value)
    }
  })

  test("accepts future fields while exposing only the stable envelope", () => {
    expect(
      Schema.decodeUnknownSync(SessionError.Error)({
        type: "provider.timeout",
        message: "Timeout",
        retryAfterMs: 2_500,
      }),
    ).toEqual({ type: "provider.timeout", message: "Timeout" })
  })

  test("rejects missing envelope fields", () => {
    expect(() => Schema.decodeUnknownSync(SessionError.Error)({ type: "provider.auth" })).toThrow()
    expect(() => Schema.decodeUnknownSync(SessionError.Error)({ message: "Missing type" })).toThrow()
    expect(() =>
      Schema.decodeUnknownSync(SessionError.Error)({ type: "provider.auth", message: "Failed", response: {} }),
    ).toThrow()
  })

  test("round trips safe user and tool references through JSON", () => {
    const value: SessionError.Error = {
      type: "provider.attachment-rejected",
      message: "Attachment rejected",
      rejectedAttachments: [
        { source: "user", messageID: SessionMessage.ID.make("msg_user"), fileIndex: 0 },
        { source: "tool", messageID: SessionMessage.ID.make("msg_tool"), partIndex: 1, contentIndex: 0 },
      ],
    }
    const codec = Schema.fromJsonString(SessionError.Error)
    expect(Schema.decodeUnknownSync(codec)(Schema.encodeSync(codec)(value))).toEqual(value)
    expect(SessionError.RejectedAttachment.ast.annotations?.identifier).toBe("Session.RejectedAttachment")
  })

  test("rejects invalid reference IDs, discriminants and indices", () => {
    const user = { source: "user", messageID: "msg_user", fileIndex: 0 }
    const tool = { source: "tool", messageID: "msg_tool", partIndex: 0, contentIndex: 0 }
    const values = [
      ...["", "msg", "ses_wrong", 1].flatMap((messageID) => [{ ...user, messageID }, { ...tool, messageID }]),
      ...[-1, 0.5].flatMap((index) => [
        { ...user, fileIndex: index },
        { ...tool, partIndex: index },
        { ...tool, contentIndex: index },
      ]),
      { source: "user", messageID: "msg_user" },
      { source: "tool", messageID: "msg_tool", partIndex: 0 },
      { ...user, source: "provider" },
    ]
    for (const value of values) {
      expect(() => Schema.decodeUnknownSync(SessionError.RejectedAttachment)(value)).toThrow()
    }
  })

  test("omits undefined references without changing existing fields", () => {
    expect(
      Schema.encodeSync(SessionError.Error)({
        type: "unknown",
        message: "Failed",
        status: undefined,
        response: undefined,
        rejectedAttachments: undefined,
      }),
    ).toEqual({ type: "unknown", message: "Failed" })
  })

  test("drops payload fields from references at the boundary", () => {
    const reference = { source: "user", messageID: "msg_user", fileIndex: 0 } as const
    const decoded = Schema.decodeUnknownSync(SessionError.RejectedAttachment)({
      ...reference,
      bytes: [1, 2],
      url: "https://example.invalid",
      filename: "fixture.png",
      providerContent: "fixture",
    })
    expect(Schema.encodeSync(SessionError.RejectedAttachment)(decoded)).toEqual(reference)
  })

  test("decodes SessionMessage.Info with durable error references", () => {
    const value = {
      id: "msg_assistant",
      type: "assistant",
      agent: "build",
      model: { providerID: "openai", id: "test" },
      content: [],
      time: { created: 0 },
      error: {
        type: "provider.attachment-rejected",
        message: "Rejected",
        rejectedAttachments: [{ source: "tool", messageID: "msg_tool", partIndex: 0, contentIndex: 1 }],
      },
    } as const
    expect(Schema.encodeSync(SessionMessage.Info)(Schema.decodeUnknownSync(SessionMessage.Info)(value))).toEqual(value)
  })

  test("supports a cold direct error import before SessionMessage", () => {
    const result = Bun.spawnSync([process.execPath, "-e", `
      import { SessionError } from './src/session-error.ts'
      import { SessionMessage } from './src/session-message.ts'
      import { ID } from './src/session-message-id.ts'
      import { Schema } from 'effect'
      import { strict } from 'node:assert'
      strict.equal(SessionMessage.ID, ID)
      strict.equal(Schema.decodeUnknownSync(SessionError.RejectedAttachment)({
        source: 'user', messageID: 'msg_cold', fileIndex: 0,
      }).messageID, 'msg_cold')
      strict.equal(Schema.decodeUnknownSync(SessionMessage.Info)({
        id: 'msg_cold', type: 'synthetic', text: '', time: { created: 0 },
      }).id, 'msg_cold')
    `], { cwd: `${import.meta.dir}/..` })
    expect(result.stderr.toString()).toBe("")
    expect(result.exitCode).toBe(0)
  })
})

test("FinishReason is the closed normalized provider set", () => {
  const reasons = ["stop", "length", "tool-calls", "content-filter", "error", "unknown"] as const
  expect(reasons.map((reason) => Schema.decodeUnknownSync(LLM.FinishReason)(reason))).toEqual([...reasons])
  expect(() => Schema.decodeUnknownSync(LLM.FinishReason)("other")).toThrow()
})

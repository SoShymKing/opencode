import { expect, test } from "bun:test"
import type { ContentPart, ToolResultValue } from "@opencode/ai"
import { Schema } from "effect"
import { Model } from "@opencode/schema/model"
import { SessionMessage } from "@opencode/schema/session-message"
import { AttachmentQuarantine } from "@opencode/core/session/attachment-quarantine"
import { createConverter, toLLMMessages } from "@opencode/core/session/runner/to-llm-message"

const decode = Schema.decodeUnknownSync(SessionMessage.Info)
const model = Schema.decodeUnknownSync(Model.Ref)({ providerID: "provider", id: "model" })
const outer = { provider: { signature: "signed" }, other: { extra: false } }
const resultMetadata = { provider: { result: "opaque" } }
const assistant = (content: unknown[], extra: Record<string, unknown> = {}) => decode({
  type: "assistant", id: "msg_recorded", agent: "build", model,
  time: { created: 0, completed: 1 }, content, ...extra,
})
const tool = (state: Record<string, unknown>, extra: Record<string, unknown> = {}) => ({
  type: "tool", id: "call", name: "lookup", time: { created: 0, completed: 1 },
  state: { status: "completed", input: {}, content: [], ...state }, ...extra,
})
const parts = (message: SessionMessage.Info, destination = model) =>
  toLLMMessages([message], destination, "adapter").flatMap((message) => message.content)
const metadata = (part: ContentPart | undefined) => {
  if (part?.type !== "reasoning" && part?.type !== "text" && part?.type !== "tool-call" && part?.type !== "tool-result")
    throw new Error("Expected reasoning, text, tool call or tool result")
  return part.providerMetadata
}

test("recorded shell preserves only historical command, output and metadata", () => {
  const source = decode({ type: "shell-recorded", id: "msg_shell", callID: "old-call",
    command: "printf example", output: "example", metadata: { background: true, imported: true }, time: { created: 0 } })
  const output = toLLMMessages([source], model)
  expect(output).toHaveLength(1)
  expect(output[0]?.role).toBe("user")
  expect(output[0]?.metadata).toEqual({ background: true, imported: true })
  const text = output[0]?.content[0]
  expect(text?.type).toBe("text")
  if (text?.type !== "text") throw new Error("Missing historical text")
  expect(text.text).toContain("printf example")
  expect(text.text).toContain("example")
  for (const value of [output[0], output[0]?.metadata, text]) {
    for (const field of ["exit", "status", "success", "completed", "shellID"])
      expect(value).not.toHaveProperty(field)
  }
})

test("recorded files remain unavailable references without changing user text", () => {
  const file = { uri: "https://example.invalid/missing.png", mime: "image/png", name: "missing.png",
    description: "archived image", source: { text: "@image", start: 2, end: 8 } }
  const source = decode({ type: "user", id: "msg_user", text: "  original\n", recordedFiles: [file], time: { created: 0 } })
  const output = parts(source)
  expect(output).toHaveLength(2)
  expect(output[0]).toMatchObject({ type: "text", text: "  original\n" })
  const reference = output[1]
  if (reference?.type !== "text") throw new Error("Missing file reference")
  expect(JSON.parse(reference.text.slice(reference.text.indexOf("\n") + 1))).toEqual(file)
  expect(reference.text).toContain("unavailable")
  expect(output.some((part) => part.type === "media")).toBe(false)
})

test("recorded reasoning outer maps obey model, error and interruption constraints", () => {
  const reasoning = { type: "reasoning", text: "thought", recordedProviderMetadata: outer }
  expect(metadata(parts(assistant([reasoning]))[0])).toEqual(outer)
  const other = Schema.decodeUnknownSync(Model.Ref)({ ...model, id: "other" })
  expect(metadata(parts(assistant([reasoning]), other)[0])).toBeUndefined()
  expect(parts(assistant([reasoning], { error: { type: "unknown", message: "stopped" } }))[0])
    .toMatchObject({ type: "text", text: "thought" })
  expect(metadata(parts(assistant([{ ...reasoning, time: { created: 0 } }]))[0])).toBeUndefined()
})

test.each([null, false, "", 0, [], {}, { payload: [false, null] }].map((result) => ({ result })))("hosted recorded raw result preserves %j", ({ result }) => {
  const output = parts(assistant([tool({}, { executed: true, recorded: { result, providerMetadata: outer, providerResultMetadata: resultMetadata } })]))
  expect(metadata(output[0])).toEqual(outer)
  expect(output[1]).toMatchObject({ type: "tool-result", providerExecuted: true,
    result: { type: "json", value: result }, providerMetadata: resultMetadata })
})

test("hosted result metadata crosses models but not providers; call proof stays model-scoped", () => {
  const source = assistant([tool({}, { executed: true, recorded: { result: false, providerMetadata: outer, providerResultMetadata: resultMetadata } })])
  const switched = parts(source, Schema.decodeUnknownSync(Model.Ref)({ ...model, id: "other" }))
  expect(metadata(switched[0])).toBeUndefined()
  expect(metadata(switched[1])).toEqual(resultMetadata)
  const foreign = parts(source, Schema.decodeUnknownSync(Model.Ref)({ providerID: "foreign", id: "other" }))
  expect(foreign.map(metadata)).toEqual([undefined, undefined])
  const errored = assistant([tool({}, { executed: true, recorded: { result: null, providerMetadata: outer } })],
    { error: { type: "unknown", message: "stopped" } })
  expect(parts(errored).map(metadata)).toEqual([outer, outer])
  const local = assistant([tool({}, { recorded: { providerMetadata: outer } })],
    { error: { type: "unknown", message: "stopped" } })
  expect(parts(local).map(metadata)).toEqual([undefined, undefined])
})

test("hosted recorded result envelopes are not wrapped twice and absent results use content", () => {
  const result = { type: "text", value: "raw text" }
  expect(parts(assistant([tool({}, { executed: true, recorded: { result } })]))[1])
    .toMatchObject({ result })
  expect(parts(assistant([tool({ metadata: { count: 0 } }, { executed: true, recorded: {} })]))[1])
    .toMatchObject({ result: { type: "json", value: { count: 0 } } })
})

test("local completed results use structured, single text and multi content semantics", () => {
  for (const fixture of [
    { content: [], expected: { type: "json", value: { count: 0 } } },
    { content: [{ type: "text", text: "" }], expected: { type: "text", value: "" } },
    { content: [{ type: "text", text: "one" }, { type: "text", text: "two" }],
      expected: { type: "content", value: [{ type: "text", text: "one" }, { type: "text", text: "two" }] } },
  ]) {
    const output = parts(assistant([tool({ content: fixture.content, metadata: { count: 0 } },
      { recorded: { result: "not a local output", providerMetadata: outer } })]))
    expect(output[1]).toMatchObject({ type: "tool-result", result: fixture.expected, providerMetadata: outer })
  }
  expect(parts(assistant([tool({})]))[1]).toMatchObject({ result: { type: "json", value: undefined } })
})

test("imported errors preserve progress, structured data and explicit raw result; native errors stay unchanged", () => {
  const state = { status: "error", error: { type: "unknown", message: "failed" },
    content: [{ type: "text", text: "progress" }], metadata: { count: 1 } }
  const imported = parts(assistant([tool(state, { recorded: { result: false } })]))[1]
  expect(imported).toMatchObject({ result: { type: "error", value: {
    error: state.error, content: state.content, structured: state.metadata, result: false,
  } } })
  const native = parts(assistant([tool(state)]))[1]
  expect(native).toMatchObject({ result: { type: "error", value: { error: state.error, content: state.content } } })
  if (native?.type !== "tool-result") throw new Error("Missing tool result")
  expect(native.result.value).not.toHaveProperty("structured")
  expect(native.result.value).not.toHaveProperty("result")
  const absent = parts(assistant([tool(state, { recorded: {} })]))[1]
  if (absent?.type !== "tool-result") throw new Error("Missing imported error")
  expect(absent.result.value).not.toHaveProperty("result")
})

test.each([null, false, "", 0, [], {}, { payload: [false, null] }].map((result) => ({ result })))("hosted error preserves opaque result %j", ({ result }) => {
  const output = parts(assistant([tool({ status: "error", error: { type: "unknown", message: "failed" },
    content: [{ type: "text", text: "progress" }], metadata: { count: 1 },
  }, { executed: true, recorded: { result, providerResultMetadata: resultMetadata } })]))[1]
  if (output?.type !== "tool-result") throw new Error("Missing hosted error")
  expect(output.result).toEqual({ type: "error", value: result })
  expect(output.providerMetadata).toEqual(resultMetadata)
})

test("hosted errors preserve typed envelopes and distinguish absent raw results", () => {
  const state = { status: "error", error: { type: "unknown", message: "failed" },
    content: [{ type: "text", text: "progress" }], metadata: { count: 1 } }
  const result = { type: "json", value: { opaque: false } } satisfies ToolResultValue
  const envelope = parts(assistant([tool(state, { executed: true, recorded: { result } })]))[1]
  if (envelope?.type !== "tool-result") throw new Error("Missing hosted envelope")
  expect(envelope.result).toEqual(result)
  const absent = parts(assistant([tool(state, { executed: true, recorded: {} })]))[1]
  if (absent?.type !== "tool-result") throw new Error("Missing hosted error bundle")
  expect(absent.result).toEqual({ type: "error", value: {
    error: state.error, content: state.content, structured: state.metadata,
  } })
})

test("quarantine keeps source positions, omission and opaque result rejection", () => {
  const content = [{ type: "text", text: "progress" }, { type: "file", mime: "image/png", uri: "data:image/png;base64,YQ==" }]
  const source = assistant([{ type: "text", text: "before" }, tool({ content }, { recorded: {} })])
  const origins: AttachmentQuarantine.Origin[] = []
  toLLMMessages([source], model, undefined, { attachmentOrigins: origins })
  expect(origins.map((origin) => origin.ref)).toEqual([{ source: "tool", messageID: source.id, partIndex: 1, contentIndex: 1 }])
  const ref = origins[0]?.ref
  if (!ref) throw new Error("Missing attachment origin")
  const rejected: boolean[] = []
  const output = toLLMMessages([source], model, undefined, { rejectedAttachments: [ref], onRejectedAttachment: () => rejected.push(true) })
  expect(output.at(-1)?.content[0]).toMatchObject({ result: { type: "text", value: "progress" } })
  expect(rejected).toEqual([true])
  const opaque = assistant([{ type: "text", text: "before" }, tool({ content }, { executed: true, recorded: { result: null } })])
  expect(() => toLLMMessages([opaque], model, undefined, { rejectedAttachments: [ref] })).toThrow(AttachmentQuarantine.Unavailable)
  const error = assistant([{ type: "text", text: "before" }, tool({ content,
    status: "error", error: { type: "unknown", message: "failed" },
  }, { executed: true, recorded: { result: false } })])
  expect(() => toLLMMessages([error], model, undefined, { rejectedAttachments: [ref] })).toThrow(AttachmentQuarantine.Unavailable)
})

test("converter invalidates changed recorded raw payloads and provider maps", () => {
  const convert = createConverter()
  const make = (result: boolean) => assistant([tool({}, { executed: true, recorded: {
    result, providerMetadata: { provider: { flag: result } },
  } })])
  const first = convert([make(false)], model)
  expect(convert([make(false)], model)).toEqual(first)
  const changed = convert([make(true)], model)
  expect(changed).toEqual(toLLMMessages([make(true)], model))
  expect(changed).not.toEqual(first)
  expect(changed[0]?.content[1]).toMatchObject({ result: { type: "json", value: true } })
})

import { expect, test } from "bun:test"
import { Effect, Schema } from "effect"
import { SessionMessage } from "@opencode/schema/session-message"
import { WsNativeMessage } from "../src/database/ws-native-message"
import { messageID, sessionID, richMessageData, richContent, normalizedMessageData } from "./fixture/database-ws"

function row(type: string, data: object) {
  return { id: messageID, sessionID, type, data: JSON.stringify({ time: { created: 1 }, ...data }) }
}

async function convert(input: Parameters<typeof WsNativeMessage.convert>[0]) {
  const result = await Effect.runPromise(WsNativeMessage.convert(input))
  const encoded = await Effect.runPromise(Schema.encodeEffect(SessionMessage.Info)(result.message))
  expect(await Effect.runPromise(Schema.decodeUnknownEffect(SessionMessage.Info)(encoded))).toEqual(result.message)
  return { ...result, encoded }
}

test("all simple WS variants retain facts without invented shell lifecycle", async () => {
  for (const [type, data, expected] of [
    ["agent-switched", { agent: "build" }, { type: "agent-switched", agent: "build" }],
    ["model-switched", { model: { providerID: "test", id: "model" } }, { type: "model-switched", model: { providerID: "test", id: "model" } }],
    ["user", { text: "original" }, { type: "user", text: "original" }],
    ["system", { text: "policy" }, { type: "system", text: "policy" }],
    ["synthetic", { sessionID, text: "synthetic" }, { type: "synthetic", text: "synthetic" }],
    ["shell", { callID: "call", command: "printf x", output: "x", time: { created: 1, completed: 2 } }, { type: "shell-recorded", callID: "call", command: "printf x", output: "x", time: { created: 1, completed: 2 } }],
    ["compaction", { reason: "manual", summary: "summary", recent: "recent" }, { type: "compaction", status: "completed", reason: "manual", summary: "summary", recent: "recent" }],
  ] satisfies [string, object, object][]) {
    const result = await convert(row(type, { ...data, metadata: { fixture: false } }))
    expect<object>(result.encoded).toEqual({ id: messageID, time: { created: 1 }, metadata: { fixture: false }, ...expected })
    expect(result.identities).toEqual([])
  }
})

test("inline and normalized rich assistants retain every tool state and original identities", async () => {
  const parts = richContent.map(({ id, type, ...data }, position) => ({ id, type, position, data: JSON.stringify(data) }))
  const inline = await convert({ id: messageID, sessionID, type: "assistant", data: richMessageData })
  const parent = { agent: "build", model: { providerID: "test", id: "model" }, time: { created: 1, completed: 16 }, finish: "tool-calls", cost: 0.125, streamEventCount: 27, tokens: { input: 31, output: 17, reasoning: 5, cache: { read: 7, write: 3 } }, snapshot: { start: "before", end: "after", files: ["result.txt"] } }
  const split = await convert({ ...row("assistant", parent), parts: parts.toReversed() })
  expect(split).toEqual(inline)
  expect(inline.identities).toEqual(richContent.map((part, position) => ({ position, type: part.type, sourceID: part.id, nativeID: part.type === "text" ? String(position) : part.type === "reasoning" ? "0" : part.id })))
  expect(inline.encoded).toMatchObject({ finish: "tool-calls", cost: 0.125, streamEventCount: 27, tokens: { input: 31, output: 17, reasoning: 5, cache: { read: 7, write: 3 } }, snapshot: { start: "before", end: "after", files: ["result.txt"] }, time: { created: 1, completed: 16 } })
  if (inline.encoded.type !== "assistant") throw new Error("expected assistant")
  expect(inline.encoded.time).not.toHaveProperty("streamed")
  expect(inline.encoded.content).toEqual([
    { type: "text", text: "first" }, { type: "text", text: "second" },
    { type: "reasoning", text: "Check the boundary", recordedProviderMetadata: { test: { signature: "fake-signature" } }, time: { created: 2, completed: 3 } },
    { type: "tool", id: "pending-call", name: "read", recorded: {}, state: { status: "streaming", input: '{"path":' }, time: { created: 4 } },
    { type: "tool", id: "running-call", name: "bash", recorded: { content: [{ type: "text", text: "running" }] }, state: { status: "running", input: { command: "printf running" }, metadata: { pid: 42 } }, time: { created: 5, ran: 6 } },
    { type: "tool", id: "empty-call", name: "read", recorded: { result: "" }, state: { status: "completed", input: { path: "empty.txt" }, metadata: {}, content: [] }, time: { created: 7, ran: 8, completed: 9 } },
    { type: "tool", id: "hosted-call", name: "web_search", executed: true, recorded: { providerMetadata: { test: { source: "hosted" } }, providerResultMetadata: { test: { request: "fake-request" } }, result: { url: "https://example.invalid/result", score: 0.5 }, attachments: [{ uri: "data:text/plain;base64,7ZWc6riA", mime: "text/plain", name: "\uD55C\uAE00.txt" }], outputPaths: ["result.txt"] }, state: { status: "completed", input: { query: "fixture" }, metadata: { hits: 1 }, content: [{ type: "text", text: "Found \uD55C\uAE00" }] }, time: { created: 10, ran: 11, completed: 12 } },
    { type: "tool", id: "error-call", name: "read", recorded: { result: null }, state: { status: "error", input: { path: "missing.txt" }, metadata: { code: "ENOENT" }, content: [], error: { type: "unknown", message: "File not found" } }, time: { created: 13, ran: 14, completed: 15 } },
  ])
})

test("results preserve presence and JSON values; namespaces and pruned times stay recorded", async () => {
  for (const result of [undefined, null, false, "", [], {}, 0]) {
    const output = await convert(row("assistant", { agent: "build", model: { providerID: "test", id: "model" }, finish: "future-finish", content: [
      { type: "tool", id: "hosted", name: "search", provider: { executed: false, metadata: { alpha: { empty: null }, beta: {} }, resultMetadata: {} }, state: { status: "completed", input: {}, structured: {}, content: [], ...(result === undefined ? {} : { result }), attachments: [], outputPaths: [] }, time: { created: 1, pruned: 9 } },
    ] }))
    if (output.encoded.type !== "assistant") throw new Error("expected assistant")
    expect(output.encoded.rawFinish).toBe("future-finish")
    expect(output.encoded).not.toHaveProperty("finish")
    const part = output.encoded.content[0]
    if (part?.type !== "tool") throw new Error("expected tool")
    expect(part.recorded).toEqual({ providerMetadata: { alpha: { empty: null }, beta: {} }, providerResultMetadata: {}, attachments: [], outputPaths: [], pruned: 9, ...(result === undefined ? {} : { result }) })
    expect(part.executed).toBe(false)
    expect(part).not.toHaveProperty("providerState")
    expect(part.time).toEqual({ created: 1 })
  }
})

test("files decode offline and missing URI references and mentions remain intact", async () => {
  const source = { start: 0, end: 4, text: "file" }
  const result = await convert(row("user", { text: "file original", files: [
    { uri: "data:text/plain;base64,aGk=", mime: "text/plain", name: "hello", description: "desc", source },
    { uri: "data:text/plain,hello%20world", mime: "text/plain" },
    { uri: "file:///unavailable.txt", mime: "text/plain", name: "missing", source },
    { uri: "https://example.invalid/file", mime: "image/png" },
  ], agents: [{ name: "build", source }] }))
  expect(result.encoded).toEqual({ id: messageID, type: "user", time: { created: 1 }, text: "file original", files: [
    { data: "aGk=", mime: "text/plain", source: { type: "inline" }, name: "hello", description: "desc", mention: source },
    { data: "aGVsbG8gd29ybGQ=", mime: "text/plain", source: { type: "inline" } },
  ], recordedFiles: [{ uri: "file:///unavailable.txt", mime: "text/plain", name: "missing", source }, { uri: "https://example.invalid/file", mime: "image/png" }], agents: [{ name: "build", mention: source }] })
})

test("empty normalized assistant and separate text/reasoning ordinals", async () => {
  const empty = await convert({ id: messageID, sessionID, type: "assistant", data: normalizedMessageData, parts: [] })
  expect(empty.identities).toEqual([])
  const output = await convert(row("assistant", { agent: "build", model: { providerID: "test", id: "model" }, content: [
    { type: "reasoning", id: "r1", text: "" }, { type: "text", id: "t1", text: "" }, { type: "reasoning", id: "r2", text: "" }, { type: "text", id: "t2", text: "" },
  ] }))
  expect(output.identities.map((part) => part.nativeID)).toEqual(["0", "0", "1", "1"])
})

test("binary data URI bytes, empty optional arrays, assistant errors and file tool content survive", async () => {
  const user = await convert(row("user", { text: "", files: [{ uri: "data:application/octet-stream,%00%FF%80", mime: "application/octet-stream" }], agents: [] }))
  expect(user.encoded).toMatchObject({ text: "", agents: [], files: [{ data: "AP+A", mime: "application/octet-stream", source: { type: "inline" } }] })
  const empty = await convert(row("user", { text: "", files: [] }))
  expect(empty.encoded).toMatchObject({ files: [] })
  const source = { start: 1, end: 2, text: "x" }
  const result = await convert(row("assistant", { agent: "build", model: { providerID: "test", id: "model" }, error: { type: "unknown", message: "source failure" }, content: [
    { id: "call", type: "tool", name: "read", state: { status: "completed", input: { nested: [false, null] }, structured: { nested: { empty: [] } }, content: [{ type: "file", uri: "https://example.invalid/result", mime: "image/png", name: "result" }], attachments: [{ uri: "file:///result", mime: "image/png", source, description: "original" }] }, time: { created: 1 } },
  ] }))
  expect(result.encoded).toMatchObject({ error: { type: "unknown", message: "source failure" }, content: [{
    state: { status: "completed", input: { nested: [false, null] }, metadata: { nested: { empty: [] } }, content: [{ type: "file", uri: "https://example.invalid/result", mime: "image/png", name: "result" }] },
    recorded: { attachments: [{ uri: "file:///result", mime: "image/png", source, description: "original" }] },
  }] })
})

test("invalid JSON, embedded identity, layouts, positions and session mismatch fail with typed errors", async () => {
  const assistant = { id: messageID, sessionID, type: "assistant", data: normalizedMessageData }
  const part = { position: 0, id: "text", type: "text", data: '{"text":"ok"}' }
  for (const input of [
    { ...assistant, data: "{" }, { ...assistant, data: "null" },
    row("system", { text: "x", id: messageID }), row("system", { text: "x", type: "system" }),
    row("synthetic", { text: "x", sessionID: "ses_other" }), row("invalid", {}),
    assistant, { ...assistant, data: richMessageData, parts: [part] },
    { ...row("system", { text: "x" }), parts: [part] },
    ...[-1, 0.5, 1].map((position) => ({ ...assistant, parts: [{ ...part, position }] })),
    { ...assistant, parts: [part, part] },
    { ...assistant, parts: [{ ...part, data: '{"id":"text","text":"x"}' }] },
    { ...assistant, parts: [{ ...part, data: '{"type":"text","text":"x"}' }] },
    row("user", { text: "x", files: [{ uri: "data:text/plain,%ZZ", mime: "text/plain" }] }),
    row("user", { text: "x", files: [{ uri: "data:text/plain;base64,%%%", mime: "text/plain" }] }),
    row("user", { text: "x", files: [{ uri: "data:text/plain", mime: "text/plain" }] }),
  ]) {
    const result = await Effect.runPromise(WsNativeMessage.convert(input).pipe(Effect.result))
    expect(result._tag).toBe("Failure")
    if (result._tag === "Failure") expect(result.failure._tag).toBe("WsNativeMessageError")
  }
})

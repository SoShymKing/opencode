import { describe, expect, test } from "bun:test"
import { AIError, CompactionPart, InvalidRequestError, LanguageModel, Media, Message, ProviderID } from "@opencode/ai"
import { OpenAIChat } from "@opencode/ai/protocols"
import { TestLLM } from "@opencode/ai/testing"
import { Cause, Effect, Exit, Layer, Schema } from "effect"
import { eq } from "drizzle-orm"
import { Bus } from "@opencode/core/bus"
import { App } from "@opencode/core/app"
import { Database } from "@opencode/core/database/database"
import { AppNodeBuilder } from "@opencode/core/effect/app-node-builder"
import { ProjectTable } from "@opencode/core/project/sql"
import { PluginHooks } from "@opencode/core/plugin/hooks"
import { Session } from "@opencode/core/session"
import { AttachmentQuarantine } from "@opencode/core/session/attachment-quarantine"
import { SessionHistory } from "@opencode/core/session/history"
import { SessionModelRequest } from "@opencode/core/session/model-request"
import { SessionModelTransport } from "@opencode/core/session/model-transport"
import { encodeStoredMessage, insertMessageChildren } from "@opencode/core/session/message-storage"
import { SessionProjector } from "@opencode/core/session/projector"
import { SessionStore } from "@opencode/core/session/store"
import { SessionRunnerModel } from "@opencode/core/session/runner/model"
import { SessionStep } from "@opencode/core/session/runner/step"
import { SessionProviderContext } from "@opencode/core/session/provider-context"
import { recentUserMessages } from "@opencode/core/session/compaction"
import { createConverter, toLLMMessages } from "@opencode/core/session/runner/to-llm-message"
import { SessionMessageTable } from "@opencode/core/session/sql"
import { Snapshot } from "@opencode/core/snapshot"
import { ToolOutput } from "@opencode/core/tool-output"
import { Agent } from "@opencode/schema/agent"
import { Project } from "@opencode/schema/project"
import { AbsolutePath } from "@opencode/schema/schema"
import { SessionError } from "@opencode/schema/session-error"
import { SessionEvent } from "@opencode/schema/session-event"
import { SessionMessage } from "@opencode/schema/session-message"
import { LayerNode } from "@opencode/util/effect/layer-node"
import { testEffect } from "./lib/effect"

const model = SessionRunnerModel.resolved(OpenAIChat.route.model({ id: "test", provider: "test" }), {
  capabilities: { tools: true, input: ["text", "image", "pdf"], output: ["text"] },
  cost: [], limit: { context: 100_000, output: 1_000 }, transport: "websocket",
})
const agent = Agent.Info.default(Agent.ID.make("build"))
const tools = { definitions: [], execute: () => Effect.die("No tool execution expected") }
const decode = Schema.decodeUnknownSync(SessionMessage.Info)
const refs = Schema.decodeUnknownSync(Schema.Array(SessionError.RejectedAttachment))
const user = decode({ id: "msg_user", type: "user", text: "input", time: { created: 0 }, files: [
  { mime: "text/plain", data: "dGV4dA==", source: { type: "inline" } },
  { mime: "image/png", data: "aW1hZ2U=", source: { type: "inline" }, mention: { text: "image", start: 0, end: 5 } },
  { mime: "image/png", data: "aW1hZ2U=", source: { type: "inline" }, mention: { text: "image", start: 0, end: 5 } },
] })
const assistant = (executed: boolean) => decode({
  id: "msg_tools", type: "assistant", agent: "build", model: model.ref,
  time: { created: 1, completed: 2 }, content: [
    { type: "text", text: "result" },
    { type: "tool", id: "call-image", name: "read", executed, time: { created: 1, completed: 2 },
      state: { status: "completed", input: {}, content: [
        { type: "text", text: "keep" }, { type: "file", mime: "image/png", uri: "data:image/png;base64,dG9vbA==" },
      ] } },
  ],
})
const expected = refs([
  { source: "user", messageID: "msg_user", fileIndex: 1 },
  { source: "tool", messageID: "msg_tools", partIndex: 1, contentIndex: 1 },
])
const lower = (messages: readonly SessionMessage.Info[], rejectedAttachments: readonly AttachmentQuarantine.Ref[] = []) =>
  SessionModelRequest.baseTranscript({ agent, model, tools, initial: "", messages, rejectedAttachments })

describe("attachment provenance", () => {
  test("cached base transcript keys resolved model identity without keying object identity", () => {
    const batches: number[] = []
    const converter = createConverter((lower) => (...args) => {
      batches.push(args[0].length)
      return lower(...args)
    })
    for (const resolved of [model, { ...model },
      { ...model, model: OpenAIChat.route.model({ id: "other", provider: "test" }) },
      { ...model, model: LanguageModel.update(OpenAIChat.route.model({ id: "other" }), { provider: ProviderID.make("other") }) },
    ]) {
      const input = { agent, model: resolved, tools, initial: "", messages: [user, assistant(false)] }
      const cached = SessionModelRequest.baseTranscript(input, converter)
      expect(cached.messages).toEqual(SessionModelRequest.baseTranscript(input).messages)
      expect(AttachmentQuarantine.batch(cached.messages, cached.attachmentOrigins)).toEqual({ coverage: "complete", refs: expected })
    }
    expect(batches).toEqual([2, 2, 2])
  })

  test("rejects same-asset wrapper overrides during batch attribution and provenance carry", () => {
    const transcript = lower([user])
    const changed = transcript.messages.map((message) => Message.make({ ...message, content: message.content.map((part) =>
      part.type === "media" ? { ...part, metadata: { ...part.metadata, file_id: "hook-file" } } : part,
    ) }))
    const originalMedia = transcript.messages.flatMap((message) => message.content).find((part) => part.type === "media")
    const changedMedia = changed.flatMap((message) => message.content).find((part) => part.type === "media")
    expect(changedMedia?.media).toBe(originalMedia?.media)
    expect(AttachmentQuarantine.batch(changed, transcript.attachmentOrigins)).toEqual({ coverage: "unknown" })
    const carried = AttachmentQuarantine.afterReplacement(changed, changed, transcript.attachmentOrigins)
    expect(AttachmentQuarantine.batch(changed, carried)).toEqual({ coverage: "unknown" })
    const replaced = AttachmentQuarantine.afterReplacement(transcript.messages, changed, transcript.attachmentOrigins)
    expect(AttachmentQuarantine.batch(changed, replaced)).toEqual({ coverage: "unknown" })
  })

  test("marks replay changed only when quarantine removes an actively emitted attachment", () => {
    expect(lower([user], expected).replayChanged).toBe(true)
    expect(lower([assistant(false)], expected).replayChanged).toBe(true)
    expect(lower([], expected).replayChanged).toBe(false)
    expect(lower([decode({ id: "msg_summary", type: "compaction", status: "completed", reason: "manual", summary: "summary", recent: "", time: { created: 0 } })], expected).replayChanged).toBe(false)
    expect(lower([user], refs([{ source: "user", messageID: user.id, fileIndex: 2 }])).replayChanged).toBe(false)
  })

  for (const executed of [false, true]) test(`filters addressed ${executed ? "hosted" : "local"} files without promoting duplicates`, () => {
    const messages = [user, assistant(executed)]
    const original = Schema.encodeSync(Schema.Array(SessionMessage.Info))(messages)
    const submitted = lower(messages)
    expect(AttachmentQuarantine.batch(submitted.messages, submitted.attachmentOrigins)).toEqual({ coverage: "complete", refs: expected })
    const filtered = lower(messages, expected)
    expect(AttachmentQuarantine.batch(filtered.messages, filtered.attachmentOrigins)).toEqual({ coverage: "complete", refs: [] })
    expect(filtered.messages.flatMap((message) => message.content).filter((part) => part.type === "tool-call")).toHaveLength(1)
    expect(filtered.messages.flatMap((message) => message.content).filter((part) => part.type === "tool-result")).toHaveLength(1)
    expect(Schema.encodeSync(Schema.Array(SessionMessage.Info))(messages)).toEqual(original)
  })

  test("keeps cloned or inserted media provenance unknown", () => {
    const submitted = lower([user])
    expect(AttachmentQuarantine.batch(toLLMMessages([user], model.ref), submitted.attachmentOrigins)).toEqual({ coverage: "unknown" })
    expect(AttachmentQuarantine.failure({ type: "provider.invalid-file", message: "sanitized" }, { coverage: "unknown" }))
      .toEqual({ type: "provider.invalid-file", message: "sanitized" })
    expect(AttachmentQuarantine.batch([Message.assistant(CompactionPart.make({ provider: ProviderID.make("test"), encrypted: "opaque" }))], []))
      .toEqual({ coverage: "unknown" })
  })

  test("keeps an all-file tool result valid and blocks opaque hosted result replay", () => {
    const message = decode({ id: "msg_only_file", type: "assistant", agent: "build", model: model.ref, time: { created: 0, completed: 1 },
      content: [{ type: "tool", id: "only", name: "read", time: { created: 0, completed: 1 }, state: { status: "completed", input: {}, content: [
        { type: "file", mime: "image/png", uri: "data:image/png;base64,b25seQ==" },
      ] } }],
    })
    const rejectedAttachments = refs([{ source: "tool", messageID: message.id, partIndex: 0, contentIndex: 0 }])
    const result = lower([message], rejectedAttachments).messages.flatMap((item) => item.content).find((part) => part.type === "tool-result")
    expect(result?.result.type).toBe("text")
    const opaque = decode({ ...Schema.encodeSync(SessionMessage.Info)(message), content: [
      { type: "tool", id: "only", name: "read", executed: true, providerResultState: { opaque: "state" }, time: { created: 0, completed: 1 }, state: { status: "completed", input: {}, content: [
        { type: "file", mime: "image/png", uri: "data:image/png;base64,b25seQ==" },
      ] } },
    ] })
    const submitted = lower([opaque])
    expect(AttachmentQuarantine.batch(submitted.messages, submitted.attachmentOrigins)).toEqual({ coverage: "unknown" })
    expect(() => lower([opaque], rejectedAttachments)).toThrow(AttachmentQuarantine.Unavailable)
  })
})

const it = testEffect(Layer.merge(AppNodeBuilder.build(LayerNode.group([
  Database.node, Bus.node, SessionProjector.node, SessionStore.node, App.node, PluginHooks.node, ToolOutput.node,
]), [Bus.node.replace(Bus.configured({ persist: true }))]), TestLLM.testLayer()))

it.effect("bounds stored tool text after hooks without changing history or attachment attribution", () => Effect.gen(function* () {
  const database = yield* Database.Service
  const bus = yield* Bus.Service
  const store = yield* SessionStore.Service
  const output = yield* ToolOutput.Service
  const hooks = yield* PluginHooks.Service
  yield* output.transform((editor) => editor.configure({ maxLines: 2, maxBytes: 6 }))
  const sessionID = Session.ID.make("ses_tool_bounds")
  yield* database.db.insert(ProjectTable).values({ id: Project.ID.global, worktree: AbsolutePath.make("/project"), sandboxes: [] }).run()
  yield* bus.publish(SessionEvent.Created, { sessionID, projectID: Project.ID.global, location: { directory: AbsolutePath.make("/project") }, slug: "bounds", version: "test" })
  const large = "\u00e9\u00e9\n\u00e9\nthird\n".repeat(1000)
  const native = decode({ id: "msg_bounds", type: "assistant", agent: "build", model: model.ref,
    time: { created: 1, completed: 2 }, content: [
      { type: "tool", id: "text", name: "read", time: { created: 1, completed: 2 },
        state: { status: "completed", input: { large }, metadata: { truncated: false }, content: [{ type: "text", text: large }] } },
      { type: "tool", id: "mixed", name: "read", time: { created: 1, completed: 2 },
        state: { status: "completed", input: {}, content: [{ type: "text", text: large }, { type: "text", text: large },
          { type: "file", mime: "image/png", uri: "data:image/png;base64,aW1hZ2U=" }] } },
      { type: "tool", id: "json", name: "read", time: { created: 1, completed: 2 },
        state: { status: "completed", input: {}, metadata: { large }, content: [] } },
      { type: "tool", id: "hosted", name: "search", executed: true, recorded: { result: { type: "text", value: large } },
        time: { created: 1, completed: 2 }, state: { status: "completed", input: {}, content: [{ type: "text", text: large }] } },
    ] })
  const stored = yield* encodeStoredMessage(native)
  yield* database.db.insert(SessionMessageTable).values({ ...stored.envelope, id: native.id, session_id: sessionID, seq: 1, time_created: 0 }).run()
  yield* insertMessageChildren(database.db, stored.parts)
  const session = yield* store.get(sessionID)
  if (!session) throw new Error("Missing seeded session")
  const requests = yield* SessionModelRequest.Service.pipe(Effect.provide(SessionModelRequest.layer), Effect.provideService(SessionModelTransport.Service, {
    bind: () => ({ execute: () => Effect.die("unused") }), invalidateContinuation: () => Effect.void, close: () => Effect.void, closeAll: Effect.void,
  }))
  const converter = createConverter()
  const loaded = yield* store.message(native.id)
  if (!loaded) throw new Error("Missing stored message")
  const history = [loaded.message]
  const base = () => SessionModelRequest.baseTranscript({ agent, model, tools, initial: "", messages: history }, converter)
  const original = base()
  const originalFile = original.messages.flatMap((message) => message.content).flatMap((part) => part.type === "tool-result" && part.result.type === "content" ? part.result.value : []).find((item) => item.type === "file")
  const prepared = yield* requests.primary({ session, agent: agent.id, model, ...base() })
  const results = prepared.request.messages.flatMap((message) => message.content).filter((part) => part.type === "tool-result")
  expect(results.find((part) => part.id === "text")?.result).toEqual({ type: "text", value: expect.stringMatching(/^\u00e9\u00e9\n\[/) })
  const mixed = results.find((part) => part.id === "mixed")?.result
  if (mixed?.type !== "content") throw new Error("Expected mixed content")
  expect(mixed.value.filter((item) => item.type === "text").map((item) => item.text).join("\n").length).toBeLessThan(100)
  expect(mixed.value.find((item) => item.type === "file")).toEqual(originalFile)
  expect(prepared.attachmentOrigins?.[0]?.value).toBe(mixed.value.find((item) => item.type === "file"))
  expect(prepared.attachmentBatch).toEqual({ coverage: "complete", refs: refs([{ source: "tool", messageID: native.id, partIndex: 1, contentIndex: 2 }]) })
  expect(results.find((part) => part.id === "json")?.result).toEqual({ type: "json", value: { large } })
  expect(results.find((part) => part.id === "hosted")?.result).toEqual({ type: "text", value: large })
  expect(prepared.request.messages.flatMap((message) => message.content).find((part) => part.type === "tool-call")?.input).toEqual({ large })
  expect((yield* requests.primary({ session, agent: agent.id, model, ...base() })).request.messages).toEqual(prepared.request.messages)
  yield* output.transform((editor) => editor.configure({ maxBytes: 7 }))
  const resized = yield* requests.primary({ session, agent: agent.id, model, ...base() })
  expect(resized.request.messages.flatMap((message) => message.content).filter((part) => part.type === "tool-result").find((part) => part.id === "text")?.result)
    .toEqual({ type: "text", value: expect.stringMatching(/^\u00e9\u00e9\n\u00e9\n\[/) })
  const source = original.messages.find((message) => message.content.some((part) => part.type === "tool-result" && part.id === "text"))
  if (!source) throw new Error("Missing text result")
  const opaque = [
    Message.make({ ...source, native: { test: { large } } }),
    Message.make({ ...source, providerMetadata: { test: { large } } }),
    Message.make({ ...source, content: source.content.map((part) => part.type === "tool-result" ? { ...part, providerMetadata: { test: { large } } } : part) }),
    Message.make({ ...source, content: source.content.map((part) => part.type === "tool-result" ? { ...part, result: { type: "text", value: { large } } } : part) }),
  ]
  expect((yield* requests.primary({ session, agent: agent.id, model, system: [], messages: opaque })).request.messages).toEqual(opaque)
  const hook = yield* hooks.register("session", "context", (event) => Effect.sync(() => {
    for (const message of event.messages) for (const part of message.content)
      if (part.type === "tool-result" && part.id === "text") Object.assign(part, { result: { type: "text", value: "hook\n".repeat(1000) } })
    event.messages.push(Message.user(large))
  }))
  const hooked = yield* requests.primary({ session, agent: agent.id, model, ...base() })
  expect(hooked.request.messages.flatMap((message) => message.content).filter((part) => part.type === "tool-result").find((part) => part.id === "text")?.result)
    .toEqual({ type: "text", value: expect.stringMatching(/^hook\n\[/) })
  expect(hooked.request.messages.at(-1)?.content).toEqual(Message.user(large).content)
  expect(hooked.attachmentBatch).toEqual(prepared.attachmentBatch)
  yield* hook.dispose
  expect(base().messages).toEqual(original.messages)
  expect((yield* store.message(native.id))?.message).toEqual(native)
  expect(yield* encodeStoredMessage(native)).toEqual(stored)
  const reloaded = yield* store.message(native.id)
  if (!reloaded) throw new Error("Missing reloaded message")
  history[0] = decode({ ...Schema.encodeSync(SessionMessage.Info)(reloaded.message), content: [
    { type: "tool", id: "text", name: "read", time: { created: 1, completed: 2 },
      state: { status: "completed", input: {}, content: [{ type: "text", text: "new\nx\ny\n" }] } },
  ] })
  const changed = yield* requests.primary({ session, agent: agent.id, model, ...base() })
  const cold = yield* requests.primary({ session, agent: agent.id, model, ...lower(history) })
  expect(changed.request.messages).toEqual(cold.request.messages)
  expect(changed.request.messages.flatMap((message) => message.content).find((part) => part.type === "tool-result")?.result)
    .toEqual({ type: "text", value: expect.stringMatching(/^new\nx\n\[/) })
}))

it.effect("retains continuation for unknown coverage and rejects context-hook wrapper attribution", () => Effect.gen(function* () {
  const session = Schema.decodeUnknownSync(Session.Info)({
    id: "ses_reset_policy", projectID: "global", cost: 0,
    tokens: { input: 0, output: 0, reasoning: 0, cache: { read: 0, write: 0 } },
    time: { created: 0, updated: 0 }, location: { directory: "/project" },
  })
  const resets: Session.ID[] = []
  const closed: Session.ID[] = []
  const transport = {
    bind: () => ({ execute: () => Effect.die("unused") }),
    invalidateContinuation: (id: Session.ID) => Effect.sync(() => { resets.push(id) }),
    close: (id: Session.ID) => Effect.sync(() => { closed.push(id) }),
    closeAll: Effect.void,
  }
  const requests = yield* SessionModelRequest.Service.pipe(Effect.provide(SessionModelRequest.layer), Effect.provideService(SessionModelTransport.Service, transport))
  const hooks = yield* PluginHooks.Service
  const converter = createConverter()
  const history = [decode({ ...Schema.encodeSync(SessionMessage.Info)(user), metadata: { nested: { value: "original" } } }), assistant(false)]
  const cached = () => SessionModelRequest.baseTranscript({ agent, model, tools, initial: "", messages: history }, converter)
  const baseline = cached()
  const original = Schema.encodeSync(Schema.Array(Message))(baseline.messages)
  const identity = yield* hooks.register("session", "context", (event) => Effect.sync(() => {
    expect(event.messages[0]).toBeInstanceOf(Message)
    expect(event.messages[0]).not.toBe(baseline.messages[0])
    expect(event.messages[0]?.metadata).toEqual({ nested: { value: "original" } })
    const media = event.messages.flatMap((message) => message.content).find((part) => part.type === "media")
    const canonical = baseline.messages.flatMap((message) => message.content).find((part) => part.type === "media")
    expect(media?.media).not.toBe(canonical?.media)
    if (!media || !canonical) throw new Error("Expected copied and canonical media")
    expect(Object.getPrototypeOf(media.media)).toBe(Object.getPrototypeOf(canonical.media))
  }))
  const copied = yield* requests.primary({ session, agent: agent.id, model, ...cached() })
  expect(copied.attachmentBatch).toEqual({ coverage: "complete", refs: expected })
  yield* identity.dispose
  const poison = yield* hooks.register("session", "context", (event) => Effect.sync(() => {
    const nested = event.messages[0]?.metadata?.nested
    if (typeof nested !== "object" || nested === null) throw new Error("Expected nested metadata")
    Object.assign(nested, { value: "poisoned" })
    const text = event.messages[0]?.content.find((part) => part.type === "text")
    if (!text) throw new Error("Expected text")
    Object.assign(text, { text: "poisoned" })
  }))
  yield* requests.primary({ session, agent: agent.id, model, ...cached() })
  yield* poison.dispose
  expect(Schema.encodeSync(Schema.Array(Message))(cached().messages)).toEqual(original)
  const untouched = yield* requests.primary({ session, agent: agent.id, model, ...cached() })
  expect(untouched.request.messages[0]).toBe(baseline.messages[0])
  expect(untouched.attachmentBatch).toEqual({ coverage: "complete", refs: expected })
  const filtered = () => SessionModelRequest.baseTranscript({ agent, model, tools, initial: "", messages: history, rejectedAttachments: expected }, converter)
  expect(filtered().replayChanged).toBe(true)
  expect(filtered().replayChanged).toBe(true)
  const checkpoint = yield* requests.primary({ session, agent: agent.id, model, system: [],
    messages: [Message.assistant(CompactionPart.make({ provider: ProviderID.make("openai"), encrypted: "opaque" }))], webSocket: "session",
  })
  expect(checkpoint.attachmentBatch).toEqual({ coverage: "unknown" })
  const http = yield* hooks.register("session", "http.request", () => Effect.void)
  const text = yield* requests.primary({ session, agent: agent.id, model, system: [], messages: [Message.user("text")], webSocket: "session" })
  expect(text.attachmentBatch).toEqual({ coverage: "unknown" })
  yield* http.dispose
  const send = yield* hooks.register("session", "experimental.ws.send", () => Effect.void)
  const sendHook = yield* requests.primary({ session, agent: agent.id, model, system: [], messages: [Message.user("text")], webSocket: "session" })
  expect(sendHook.attachmentBatch).toEqual({ coverage: "unknown" })
  yield* send.dispose
  const rewrite = yield* hooks.register("session", "context", (event) => Effect.sync(() => {
    event.messages = event.messages.map((message) => Message.make({ ...message, content: message.content.map((part) =>
      part.type === "media" ? { ...part, metadata: { ...part.metadata, file_id: "hook-file" } } : part,
    ) }))
  }))
  const overridden = yield* requests.primary({ session, agent: agent.id, model, ...cached(), webSocket: "session" })
  expect(overridden.attachmentBatch).toEqual({ coverage: "unknown" })
  expect(AttachmentQuarantine.failure({ type: "provider.invalid-file", message: "sanitized" }, overridden.attachmentBatch ?? { coverage: "unknown" }))
    .toEqual({ type: "provider.invalid-file", message: "sanitized" })
  yield* rewrite.dispose
  const clone = yield* hooks.register("session", "context", (event) => Effect.sync(() => {
    event.messages = [...Schema.decodeUnknownSync(Schema.Array(Message))(Schema.encodeSync(Schema.Array(Message))(event.messages))]
  }))
  const cloned = yield* requests.primary({ session, agent: agent.id, model, ...cached(), webSocket: "session" })
  expect(cloned.attachmentBatch).toEqual({ coverage: "unknown" })
  yield* clone.dispose
  yield* requests.primary({ session, agent: agent.id, model, ...lower([], expected), webSocket: "session" })
  expect(resets).toEqual([])
  expect(closed).toEqual([])
  yield* requests.primary({ session, agent: agent.id, model, ...lower([user]), webSocket: "session" })
  yield* requests.primary({ session, agent: agent.id, model, ...lower([user], expected), webSocket: "session" })
  expect(resets).toEqual([session.id, session.id])
  expect(closed).toEqual([])
}))

for (const executed of [false, true]) it.effect(`persists safe ${executed ? "hosted" : "local"} rejection refs and filters after reload`, () => Effect.gen(function* () {
  const database = yield* Database.Service
  const bus = yield* Bus.Service
  const store = yield* SessionStore.Service
  const llm = yield* TestLLM.Test
  const sessionID = Session.ID.make("ses_quarantine")
  yield* database.db.insert(ProjectTable).values({ id: Project.ID.global, worktree: AbsolutePath.make("/project"), sandboxes: [] }).run()
  yield* bus.publish(SessionEvent.Created, { sessionID, projectID: Project.ID.global, location: { directory: AbsolutePath.make("/project") }, slug: "quarantine", version: "test" })
  const native = [user, assistant(executed)]
  yield* Effect.forEach(native, (message, index) => Effect.gen(function* () {
    const stored = yield* encodeStoredMessage(message)
    yield* database.db.insert(SessionMessageTable).values({ ...stored.envelope, id: message.id, session_id: sessionID, seq: index + 1, time_created: 0 }).run()
    yield* insertMessageChildren(database.db, stored.parts)
  }))
  yield* Bus.reserveSequence(database.db, sessionID, 2)
  const session = yield* store.get(sessionID)
  if (!session) throw new Error("Missing seeded session")
  const closes: Session.ID[] = []
  const requests = yield* SessionModelRequest.Service.pipe(Effect.provide(SessionModelRequest.layer), Effect.provideService(SessionModelTransport.Service, {
    bind: () => ({ execute: () => Effect.die("TestLLM owns transport") }),
    invalidateContinuation: (id) => Effect.sync(() => { closes.push(id) }), close: () => Effect.die("Unexpected lifecycle close"), closeAll: Effect.void,
  }))
  const transcript = lower(native)
  const prepared = yield* requests.primary({ session, agent: agent.id, model, tools, ...transcript, webSocket: "session" })
  expect(prepared.attachmentBatch).toEqual({ coverage: "complete", refs: expected })
  expect(closes).toEqual([sessionID])
  yield* llm.push(TestLLM.failAfter(new AIError({ reason: new InvalidRequestError({
    message: "unsafe provider detail", body: JSON.stringify({ error: { type: "invalid_request_error", param: "input", code: "invalid_file", message: "unsafe provider detail" } }),
  }) })))
  const steps = yield* SessionStep.make.pipe(Effect.provide(Layer.mock(Snapshot.Service)({ capture: () => Effect.undefined })))
  const failureID = SessionMessage.ID.make("msg_rejection")
  const result = yield* steps.attempt({
    isLocationClosed: () => false, sessionID, assistantMessageID: failureID, agent: agent.id, model, prepared,
    retry: (_cause, error, retry) => { expect(retry).toBe(false); expect(error.rejectedAttachments).toEqual(expected); return Effect.succeed({ retry: false }) },
    recoverContinuation: true, recoverOverflow: Effect.succeed(false),
  }).pipe(Effect.scoped, Effect.exit)
  expect(Exit.isFailure(result)).toBe(true)
  const stored = yield* store.message(failureID)
  if (stored?.message.type !== "assistant") throw new Error("Missing durable failure")
  expect(stored.message.error).toEqual({ type: "provider.invalid-file", message: "The provider rejected an attached file. Remove or replace the file and try again.", rejectedAttachments: expected })
  yield* bus.publish(SessionEvent.Compaction.Started, { sessionID, reason: "auto", recent: "" })
  yield* bus.publish(SessionEvent.Compaction.Ended, { sessionID, reason: "auto", text: "", recent: "",
    providerContext: SessionProviderContext.encode({ providerID: model.ref.providerID, provider: "test", modelID: "test", route: "test", protocol: "test", endpoint: "digest" }, transcript.messages),
  })
  const reloaded = yield* SessionHistory.quarantinedEntries(database.db, sessionID, "latest")
  expect(reloaded.rejectedAttachments).toEqual([...expected])
  expect(reloaded.entries.some((entry) => SessionProviderContext.isCheckpoint(entry.message))).toBe(false)
  expect(recentUserMessages(reloaded.entries.map((entry) => entry.message), model, 10_000, reloaded.rejectedAttachments)
    .flatMap((message) => message.content).some((part) => part.type === "media")).toBe(false)
  const replay = lower(reloaded.entries.map((entry) => entry.message), reloaded.rejectedAttachments)
  const next = yield* requests.primary({ session, agent: agent.id, model, tools, ...replay, webSocket: "session" })
  expect(next.attachmentBatch).toEqual({ coverage: "complete", refs: [] })
  expect(closes).toEqual([sessionID, sessionID])
  expect((yield* store.message(user.id))?.message).toEqual(user)
  expect((yield* store.message(native[1].id))?.message).toEqual(native[1])
  expect(yield* llm.requests()).toHaveLength(1)
  yield* database.db.delete(SessionMessageTable).where(eq(SessionMessageTable.id, user.id)).run()
  const unavailable = yield* SessionHistory.quarantinedEntries(database.db, sessionID, "latest").pipe(Effect.exit)
  expect(Exit.isFailure(unavailable)).toBe(true)
  if (Exit.isFailure(unavailable)) expect(Cause.squash(unavailable.cause)).toBeInstanceOf(AttachmentQuarantine.Unavailable)
}))

it.effect("marks hook-inserted media unknown and excludes unsupported media from submitted refs", () => Effect.gen(function* () {
  const session = Schema.decodeUnknownSync(Session.Info)({
    id: "ses_request", projectID: "global", cost: 0,
    tokens: { input: 0, output: 0, reasoning: 0, cache: { read: 0, write: 0 } },
    time: { created: 0, updated: 0 }, location: { directory: "/project" },
  })
  const requests = yield* SessionModelRequest.Service.pipe(Effect.provide(SessionModelRequest.layer), Effect.provideService(SessionModelTransport.Service, {
    bind: () => ({ execute: () => Effect.die("unused") }), invalidateContinuation: () => Effect.void, close: () => Effect.void, closeAll: Effect.void,
  }))
  const transcript = lower([user, assistant(false)])
  const unsupported = yield* requests.primary({ session, agent: agent.id, model: { ...model, capabilities: { ...model.capabilities, input: ["text"] } }, ...transcript })
  expect(unsupported.attachmentBatch).toEqual({ coverage: "complete", refs: [] })
  const mixed = decode({ id: "msg_mixed", type: "assistant", agent: "build", model: { id: "test", providerID: "test" }, time: { created: 0, completed: 1 },
    content: [{ type: "tool", id: "mixed", name: "read", time: { created: 0, completed: 1 }, state: { status: "completed", input: {}, content: [
      { type: "file", mime: "application/pdf", uri: "data:application/pdf;base64,cGRm" },
      { type: "file", mime: "image/png", uri: "data:image/png;base64,aW1hZ2U=" },
    ] } }],
  })
  const partlySupported = yield* requests.primary({ session, agent: agent.id, model: { ...model, capabilities: { ...model.capabilities, input: ["text", "image"] } }, ...lower([mixed]) })
  expect(partlySupported.attachmentBatch).toEqual({ coverage: "complete", refs: refs([{ source: "tool", messageID: mixed.id, partIndex: 0, contentIndex: 1 }]) })
  const hooks = yield* PluginHooks.Service
  yield* hooks.register("session", "context", (event) => Effect.sync(() => {
    event.messages = [...event.messages, Message.user(Message.media(Media.base64("aG9vaw==", "image/png")))]
  }))
  const modified = yield* requests.primary({ session, agent: agent.id, model, ...transcript })
  expect(modified.attachmentBatch).toEqual({ coverage: "unknown" })
}))

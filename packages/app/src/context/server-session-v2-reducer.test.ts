import { describe, expect, test } from "bun:test"
import { Schema } from "effect"
import { SessionEvent } from "@opencode-ai/schema/session-event"
import { SessionMessage } from "@opencode-ai/schema/session-message"
import { normalizeSessionMessages } from "@/utils/session-message"
import type { NativeServerEvent } from "./server-sdk"
import { createV2SessionReducer } from "./server-session-v2-reducer"

type NativeMessage = typeof SessionMessage.Message.Encoded
const base = { timestamp: 1, sessionID: "ses_1" }
const assistant = { ...base, assistantMessageID: "msg_assistant" }
const tool = { ...assistant, callID: "call_1" }
const model = { id: "model", providerID: "provider" }
function driver(source: NativeMessage[] = []) {
  const reducer = createV2SessionReducer()
  let messages = source
  return {
    apply(event: NativeServerEvent) {
      Schema.decodeUnknownSync(SessionEvent.All)(event)
      const result = reducer.reduce(messages, event)
      if (result) {
        Schema.decodeUnknownSync(Schema.Array(Schema.toEncoded(SessionMessage.Message)))(result.messages)
        messages = result.messages
      }
      return result
    },
    messages: () => messages,
  }
}

describe("native95 session reducer", () => {
  test("does not promote an admitted queued prompt", () => {
    const result = driver().apply({ id: "evt_admitted", type: "session.next.prompt.admitted", data: {
      ...base, messageID: "msg_user", prompt: { text: "hello" }, delivery: "queue",
    } })
    expect(result).toMatchObject({ messages: [], touched: [] })
  })

  test("promotes the full prompt without prior admission", () => {
    const prompt = { text: "hello", files: [{ uri: "file:///repo/a", mime: "text/plain", source: { text: "@a", start: 0, end: 2 } }], agents: [{ name: "build" }] }
    const result = driver().apply({ id: "evt_prompt", metadata: { retained: true }, type: "session.next.prompted", data: {
      ...base, messageID: "msg_user", prompt, delivery: "steer",
    } })
    expect(result).toMatchObject({ sessionID: "ses_1", touched: ["msg_user"], messages: [{ id: "msg_user", type: "user", ...prompt, metadata: { retained: true }, time: { created: 1 } }] })
    expect(result?.missing).toBeUndefined()
  })

  test("projects streaming assistant content with explicit IDs", () => {
    const run = driver()
    run.apply({ id: "evt_prompt", type: "session.next.prompted", data: { ...base, messageID: "msg_user", prompt: { text: "hello" }, delivery: "steer" } })
    run.apply({ id: "evt_step", type: "session.next.step.started", data: { ...assistant, agent: "build", model } })
    run.apply({ id: "evt_start", type: "session.next.text.started", data: { ...assistant, textID: "text_a" } })
    run.apply({ id: "evt_delta", type: "session.next.text.delta", data: { ...assistant, textID: "text_a", delta: "hel" } })
    run.apply({ id: "evt_end", type: "session.next.text.ended", data: { ...assistant, textID: "text_a", text: "hello" } })
    expect(run.messages()[0]).toMatchObject({ id: "msg_user", type: "user", text: "hello" })
    expect(run.messages()[1]).toMatchObject({ id: "msg_assistant", type: "assistant", content: [{ type: "text", id: "text_a", text: "hello" }] })
    expect(normalizeSessionMessages("ses_1", run.messages()).parts.get("msg_assistant")?.[0]?.id).toBe("msg_assistant:text_a")
  })

  test("retains rich tool progress, result, paths, provider data and times", () => {
    const run = driver()
    run.apply({ id: "evt_step", type: "session.next.step.started", data: { ...assistant, agent: "build", model } })
    run.apply({ id: "evt_start", type: "session.next.tool.input.started", data: { ...tool, name: "edit" } })
    run.apply({ id: "evt_delta", type: "session.next.tool.input.delta", data: { ...tool, delta: "{}" } })
    expect(run.messages()[0]).toMatchObject({ content: [{ state: { status: "pending", input: "{}" } }] })
    run.apply({ id: "evt_input", type: "session.next.tool.input.ended", data: { ...tool, text: "{\"path\":\"a\"}" } })
    run.apply({ id: "evt_called", type: "session.next.tool.called", data: { ...tool, timestamp: 2, tool: "edit", input: { path: "a" }, provider: { executed: true, metadata: { vendor: { input: true } } } } })
    const content = [{ type: "text" as const, text: "done" }, { type: "file" as const, uri: "file:///repo/a", mime: "text/plain" }]
    run.apply({ id: "evt_progress", type: "session.next.tool.progress", data: { ...tool, structured: { progress: 1 }, content } })
    expect(run.messages()[0]).toMatchObject({ content: [{ state: { status: "running", structured: { progress: 1 }, content } }] })
    run.apply({ id: "evt_success", type: "session.next.tool.success", data: { ...tool, timestamp: 3, structured: { retained: true }, content, outputPaths: ["a"], result: { value: 9 }, provider: { executed: false, metadata: { vendor: { result: true } } } } })
    run.apply({ id: "evt_retry", type: "session.next.retried", data: { ...base, attempt: 2, error: { message: "retry", isRetryable: true } } })
    expect(run.messages()[0]).toMatchObject({ type: "assistant", content: [{
      id: "call_1", name: "edit", state: { status: "completed", input: { path: "a" }, structured: { retained: true }, content, outputPaths: ["a"], result: { value: 9 } },
      provider: { executed: true, metadata: { vendor: { input: true } }, resultMetadata: { vendor: { result: true } } }, time: { created: 1, ran: 2, completed: 3 },
    }] })
  })

  test("preserves error tool progress and result", () => {
    const run = driver()
    run.apply({ id: "evt_step", type: "session.next.step.started", data: { ...assistant, agent: "build", model } })
    run.apply({ id: "evt_start", type: "session.next.tool.input.started", data: { ...tool, name: "bash" } })
    run.apply({ id: "evt_call", type: "session.next.tool.called", data: { ...tool, tool: "bash", input: {}, provider: { executed: false } } })
    run.apply({ id: "evt_progress", type: "session.next.tool.progress", data: { ...tool, structured: { progress: 1 }, content: [{ type: "text", text: "partial" }] } })
    run.apply({ id: "evt_failed", type: "session.next.tool.failed", data: { ...tool, timestamp: 4, error: { type: "unknown", message: "failed" }, result: { partial: true }, provider: { executed: true } } })
    expect(run.messages()[0]).toMatchObject({ content: [{ state: { status: "error", structured: { progress: 1 }, content: [{ type: "text", text: "partial" }], error: { type: "unknown", message: "failed" }, result: { partial: true } }, time: { completed: 4 } }] })
  })

  test("updates reasoning metadata after recovered content without replacing its identity", () => {
    const run = driver([{ id: "msg_assistant", type: "assistant", agent: "build", model, metadata: { canonical: true }, snapshot: { start: "before" }, content: [{ type: "reasoning", id: "reason", text: "recovered", time: { created: 2 }, providerMetadata: { vendor: { before: true } } }], time: { created: 1 } }])
    run.apply({ id: "evt_end", type: "session.next.reasoning.ended", data: { ...assistant, timestamp: 5, reasoningID: "reason", text: "full", providerMetadata: { vendor: { after: true } } } })
    expect(run.messages()[0]).toMatchObject({ metadata: { canonical: true }, snapshot: { start: "before" }, content: [{ id: "reason", text: "full", time: { created: 2, completed: 5 }, providerMetadata: { vendor: { after: true } } }] })
  })

  test("reports missing message or content instead of touching unrelated content", () => {
    const run = driver([{ id: "msg_assistant", type: "assistant", agent: "build", model, content: [{ type: "text", id: "other", text: "safe" }], time: { created: 1 } }])
    expect(run.apply({ id: "evt_delta", type: "session.next.text.delta", data: { ...assistant, textID: "missing", delta: "lost" } })).toMatchObject({ missing: "msg_assistant", touched: [] })
    expect(run.messages()[0]).toMatchObject({ content: [{ id: "other", text: "safe" }] })
  })

  test("folds native shell and compaction boundaries without invented status fields", () => {
    const run = driver()
    run.apply({ id: "evt_shell", type: "session.next.shell.started", data: { ...base, messageID: "msg_shell", callID: "shell", command: "pwd" } })
    run.apply({ id: "evt_shell_end", type: "session.next.shell.ended", data: { ...base, timestamp: 2, callID: "shell", output: "/repo" } })
    run.apply({ id: "evt_compaction", type: "session.next.compaction.started", data: { ...base, messageID: "msg_compact", reason: "auto" } })
    run.apply({ id: "evt_compact_delta", type: "session.next.compaction.delta", data: { ...base, messageID: "msg_compact", text: "fragment" } })
    run.apply({ id: "evt_compact_end", type: "session.next.compaction.ended", data: { ...base, timestamp: 3, messageID: "msg_compact", reason: "auto", text: "summary", recent: "recent" } })
    expect(run.messages()).toEqual([
      { id: "msg_shell", type: "shell", callID: "shell", command: "pwd", output: "/repo", time: { created: 1, completed: 2 } },
      { id: "msg_compact", type: "compaction", reason: "auto", summary: "summary", recent: "recent", time: { created: 3 } },
    ])
  })

  test("isolates live text and reasoning IDs in two assistant messages", () => {
    const run = driver(["msg_a", "msg_b"].map((id) => ({
      id, type: "assistant", agent: "build", model, content: [
        { type: "text", id: "shared", text: id },
        { type: "text", id: "other", text: "safe" },
      ], time: { created: 1 },
    })))
    run.apply({ id: "evt_text", type: "session.next.text.delta", data: { ...base, assistantMessageID: "msg_b", textID: "shared", delta: ":new" } })
    run.apply({ id: "evt_reason", type: "session.next.reasoning.started", data: { ...base, timestamp: 2, assistantMessageID: "msg_b", reasoningID: "reason", providerMetadata: { vendor: { retained: true } } } })
    run.apply({ id: "evt_reason_delta", type: "session.next.reasoning.delta", data: { ...base, assistantMessageID: "msg_b", reasoningID: "reason", delta: "thinking" } })
    run.apply({ id: "evt_reason_end", type: "session.next.reasoning.ended", data: { ...base, timestamp: 3, assistantMessageID: "msg_b", reasoningID: "reason", text: "thought" } })
    expect(run.messages()[0]).toMatchObject({ content: [{ id: "shared", text: "msg_a" }, { id: "other", text: "safe" }] })
    expect(run.messages()[1]).toMatchObject({ content: [
      { id: "shared", text: "msg_b:new" }, { id: "other", text: "safe" },
      { id: "reason", text: "thought", providerMetadata: { vendor: { retained: true } }, time: { created: 2, completed: 3 } },
    ] })
  })

  test("settles a recovered assistant while preserving canonical metadata and content", () => {
    const run = driver([{ id: "msg_assistant", type: "assistant", agent: "build", model, metadata: { canonical: true }, snapshot: { start: "before" }, content: [{ type: "text", id: "text", text: "recovered" }], time: { created: 2 } }])
    const tokens = { input: 10, output: 5, reasoning: 2, cache: { read: 1, write: 0 } }
    const result = run.apply({ id: "evt_end", type: "session.next.step.ended", data: { ...assistant, timestamp: 5, finish: "stop", cost: 0.1, tokens, snapshot: "after", files: ["a"] } })
    expect(result?.touched).toEqual(["msg_assistant"])
    expect(run.messages()[0]).toMatchObject({ metadata: { canonical: true }, snapshot: { start: "before", end: "after", files: ["a"] }, content: [{ id: "text", text: "recovered" }], finish: "stop", cost: 0.1, tokens, time: { created: 2, completed: 5 } })
  })

  test("retains settled usage and snapshot fields when a step fails", () => {
    const run = driver([{ id: "msg_assistant", type: "assistant", agent: "build", model, cost: 0.1, snapshot: { start: "before", files: ["a"] }, content: [], time: { created: 2 } }])
    run.apply({ id: "evt_failed", type: "session.next.step.failed", data: { ...assistant, timestamp: 5, error: { type: "unknown", message: "failure" } } })
    expect(run.messages()[0]).toMatchObject({ cost: 0.1, snapshot: { start: "before", files: ["a"] }, finish: "error", error: { type: "unknown", message: "failure" }, time: { created: 2, completed: 5 } })
  })

  test("records native agent, model, system and synthetic messages with their explicit IDs", () => {
    const run = driver()
    run.apply({ id: "evt_agent", type: "session.next.agent.switched", data: { ...base, messageID: "msg_agent", agent: "build" } })
    run.apply({ id: "evt_model", type: "session.next.model.switched", data: { ...base, timestamp: 2, messageID: "msg_model", model } })
    run.apply({ id: "evt_context", type: "session.next.context.updated", metadata: { canonical: true }, data: { ...base, timestamp: 3, messageID: "msg_context", text: "context" } })
    run.apply({ id: "evt_synthetic", type: "session.next.synthetic", data: { ...base, timestamp: 4, messageID: "msg_synthetic", text: "synthetic" } })
    expect(run.messages()).toEqual([
      { id: "msg_agent", type: "agent-switched", agent: "build", time: { created: 1 } },
      { id: "msg_model", type: "model-switched", model, time: { created: 2 } },
      { id: "msg_context", type: "system", text: "context", metadata: { canonical: true }, time: { created: 3 } },
      { id: "msg_synthetic", type: "synthetic", sessionID: "ses_1", text: "synthetic", time: { created: 4 } },
    ])
  })
})

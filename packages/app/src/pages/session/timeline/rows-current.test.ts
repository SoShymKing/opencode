import { describe, expect, mock, test } from "bun:test"
import type { SessionMessageInfo } from "@opencode-ai/client/promise"
import { normalizeSessionMessages } from "@/utils/session-message"
import type { CurrentSessionMessage } from "@/utils/session-message"
import type { ServerSessionEvent } from "@/context/server-sdk"
import { createV2SessionReducer } from "@/context/server-session-v2-reducer"

mock.module("@opencode-ai/session-ui/message-part", () => ({
  renderable: () => true,
  groupParts: (refs: Array<{ messageID: string; part: { id: string } }>) =>
    refs.map((ref) => ({
      type: "part" as const,
      key: ref.part.id,
      ref: { messageID: ref.messageID, partID: ref.part.id },
    })),
}))

const { Timeline, TimelineRow } = await import("./rows")

describe("current session timeline rows", () => {
  test("blocks older unfinished stream counts when the latest assistant is completed", () => {
    const source = [
      { id: "msg_user", type: "user", text: "current", time: { created: 1 } },
      {
        id: "msg_old",
        type: "assistant",
        agent: "build",
        model: { id: "model", providerID: "provider" },
        content: [],
        streamEventCount: 99,
        time: { created: 2 },
      },
      {
        id: "msg_latest",
        type: "assistant",
        agent: "build",
        model: { id: "model", providerID: "provider" },
        content: [],
        streamEventCount: 7,
        time: { created: 3, completed: 4 },
      },
    ] satisfies CurrentSessionMessage[]
    const normalized = normalizeSessionMessages("ses_1", source)
    const messages = new Map(normalized.messages.map((message) => [message.id, message]))
    const result = Timeline.constructSessionMessageRows(
      source,
      (id) => messages.get(id),
      (id) => normalized.parts.get(id) ?? [],
      true,
      "busy",
      true,
      normalized.messages.filter((message) => message.role === "user"),
    )
    const thinking = result.rows.filter((row) => row._tag === "Thinking")
    expect(thinking).toHaveLength(1)
    expect(thinking[0]?.streamEventCount).toBe(0)
    expect(thinking[0] && TimelineRow.key(thinking[0])).toBe("thinking:msg_user")
  })

  test("projects only the current response's absolute stream count without reasoning content", () => {
    const reducer = createV2SessionReducer()
    let source: CurrentSessionMessage[] = [
      { id: "msg_old_user", type: "user", text: "previous", time: { created: 1 } },
      {
        id: "msg_old_assistant",
        type: "assistant",
        agent: "build",
        model: { id: "model", providerID: "provider" },
        content: [],
        streamEventCount: 99,
        time: { created: 2, completed: 3 },
      },
      { id: "msg_user", type: "user", text: "current", time: { created: 4 } },
    ]
    const thinking = () => {
      const normalized = normalizeSessionMessages("ses_1", source)
      const messages = new Map(normalized.messages.map((message) => [message.id, message]))
      const result = Timeline.constructSessionMessageRows(
        source,
        (id) => messages.get(id),
        (id) => normalized.parts.get(id) ?? [],
        true,
        "busy",
        true,
        normalized.messages.filter((message) => message.role === "user"),
      )
      expect(result.rows.filter((row) => row._tag === "Thinking")).toHaveLength(1)
      return result.rows.find((row) => row._tag === "Thinking")
    }
    const apply = (event: ServerSessionEvent) => {
      source = reducer.reduce(source, event)?.messages ?? source
    }
    const started = {
      id: "evt_start",
      type: "session.next.step.started",
      data: {
        sessionID: "ses_1",
        timestamp: 5,
        assistantMessageID: "msg_assistant",
        agent: "build",
        model: { id: "model", providerID: "provider" },
      },
    } satisfies ServerSessionEvent
    const counted = {
      id: "evt_count",
      type: "session.next.step.stream.updated",
      data: { sessionID: "ses_1", timestamp: 6, assistantMessageID: "msg_assistant", streamEventCount: 11 },
    } satisfies ServerSessionEvent

    expect(thinking()?.streamEventCount).toBe(0)
    apply(started)
    expect(thinking()?.streamEventCount).toBeUndefined()
    apply(counted)
    apply(counted)
    apply({ ...started, data: { ...started.data, streamEventCount: 0 } })
    const row = thinking()
    expect(row?.streamEventCount).toBe(11)
    expect(row && TimelineRow.key(row)).toBe("thinking:msg_user")
    expect(normalizeSessionMessages("ses_1", source).messages.at(-1)).toMatchObject({ streamEventCount: 11 })
    expect(normalizeSessionMessages("ses_1", source).parts.get("msg_assistant")).toEqual([])
    apply({
      id: "evt_end",
      type: "session.next.step.ended",
      data: {
        sessionID: "ses_1",
        timestamp: 7,
        assistantMessageID: "msg_assistant",
        finish: "stop",
        cost: 0,
        tokens: { input: 0, output: 0, reasoning: 0, cache: { read: 0, write: 0 } },
      },
    })
    expect(thinking()?.streamEventCount).toBe(0)
    apply(counted)
    expect(source.at(-1)).toMatchObject({ streamEventCount: 11, time: { created: 5, completed: 7 } })
    apply({
      ...started,
      id: "evt_next",
      data: { ...started.data, timestamp: 8, assistantMessageID: "msg_next", streamEventCount: 0 },
    })
    apply(started)
    apply(counted)
    expect(thinking()?.streamEventCount).toBe(0)
    expect(source.at(-1)).toMatchObject({ id: "msg_next", streamEventCount: 0, time: { created: 8 } })
    apply({
      id: "evt_failed",
      type: "session.next.step.failed",
      data: {
        sessionID: "ses_1",
        timestamp: 9,
        assistantMessageID: "msg_next",
        error: { type: "unknown", message: "Synthetic failure" },
      },
    })
    expect(source.at(-1)).toMatchObject({ finish: "error", time: { completed: 9 } })
  })

  test("derives turns and tagged rows from chronological current messages", () => {
    const source = [
      { id: "msg_1", type: "user", text: "first", time: { created: 1 } },
      {
        id: "msg_2",
        type: "assistant",
        agent: "build",
        model: { id: "model", providerID: "provider" },
        content: [{ type: "text", text: "answer" }],
        time: { created: 2, completed: 3 },
      },
      { id: "msg_3", type: "user", text: "second", time: { created: 4 } },
      {
        id: "msg_4",
        type: "assistant",
        agent: "build",
        model: { id: "model", providerID: "provider" },
        content: [{ type: "reasoning", text: "working" }],
        time: { created: 5 },
      },
    ] satisfies SessionMessageInfo[]
    const normalized = normalizeSessionMessages("ses_1", source)
    const messages = new Map(normalized.messages.map((message) => [message.id, message]))

    const result = Timeline.constructSessionMessageRows(
      source,
      (messageID) => messages.get(messageID),
      (messageID) => normalized.parts.get(messageID) ?? [],
      true,
      "busy",
      true,
      normalized.messages.filter((message) => message.role === "user"),
    )

    expect(result.activeMessageID).toBe("msg_3")
    expect(result.rows.map(TimelineRow.key)).toEqual([
      "user-message:msg_1",
      "assistant-part:msg_1:msg_2:text:0",
      "turn-gap:msg_3",
      "user-message:msg_3",
      "assistant-part:msg_3:msg_4:reasoning:0",
    ])
  })

  test("renders a current shell message as a standalone turn", () => {
    const source = [
      {
        id: "msg_shell",
        type: "shell",
        shellID: "shell_1",
        command: "pwd",
        status: "exited",
        exit: 0,
        output: { output: "/repo", cursor: 5, size: 5, truncated: false },
        time: { created: 1, completed: 2 },
      },
    ] satisfies SessionMessageInfo[]
    const normalized = normalizeSessionMessages("ses_1", source)
    const messages = new Map(normalized.messages.map((message) => [message.id, message]))

    const result = Timeline.constructSessionMessageRows(
      source,
      (messageID) => messages.get(messageID),
      (messageID) => normalized.parts.get(messageID) ?? [],
      true,
      "idle",
      true,
      normalized.messages.filter((message) => message.role === "user"),
    )

    expect(result.activeMessageID).toBe("msg_shell")
    expect(result.rows.map(TimelineRow.key)).toEqual([
      "user-message:msg_shell",
      "assistant-part:msg_shell:msg_shell:tool",
    ])
  })

  test("keeps a projected parent missing from the source page before newer turns", () => {
    const source = [
      { id: "msg_user_1", type: "user", text: "first question", time: { created: 1 } },
      {
        id: "msg_assistant_1",
        type: "assistant",
        agent: "build",
        model: { id: "model", providerID: "provider" },
        content: [{ type: "text", text: "first answer" }],
        time: { created: 2, completed: 3 },
      },
      { id: "msg_user_2", type: "user", text: "second question", time: { created: 4 } },
      {
        id: "msg_assistant_2",
        type: "assistant",
        agent: "build",
        model: { id: "model", providerID: "provider" },
        content: [{ type: "text", text: "second answer" }],
        time: { created: 5, completed: 6 },
      },
    ] satisfies SessionMessageInfo[]
    const normalized = normalizeSessionMessages("ses_1", source)
    const messages = new Map(normalized.messages.map((message) => [message.id, message]))

    const result = Timeline.constructSessionMessageRows(
      source.slice(1),
      (messageID) => messages.get(messageID),
      (messageID) => normalized.parts.get(messageID) ?? [],
      true,
      "idle",
      true,
      normalized.messages.filter((message) => message.role === "user"),
    )

    expect(result.rows.map(TimelineRow.key)).toEqual([
      "user-message:msg_user_1",
      "assistant-part:msg_user_1:msg_assistant_1:text:0",
      "turn-gap:msg_user_2",
      "user-message:msg_user_2",
      "assistant-part:msg_user_2:msg_assistant_2:text:0",
    ])
  })

  test("renders an optimistic user turn and thinking before the protocol message arrives", () => {
    const source = [
      { id: "msg_z", type: "user", text: "existing", time: { created: 1 } },
    ] satisfies SessionMessageInfo[]
    const normalized = normalizeSessionMessages("ses_1", source)
    const optimistic = {
      id: "msg_a",
      sessionID: "ses_1",
      role: "user" as const,
      time: { created: 2 },
      agent: "build",
      model: { modelID: "model", providerID: "provider" },
    }
    const result = Timeline.constructSessionMessageRows(
      source,
      (messageID) =>
        messageID === optimistic.id ? optimistic : normalized.messages.find((message) => message.id === messageID),
      () => [],
      true,
      "busy",
      true,
      [...normalized.messages.filter((message) => message.role === "user"), optimistic],
    )

    expect(result.activeMessageID).toBe(optimistic.id)
    expect(result.rows.map(TimelineRow.key)).toEqual([
      "user-message:msg_z",
      "turn-gap:msg_a",
      "user-message:msg_a",
      "thinking:msg_a",
    ])
  })

  test("removes a failed assistant error when the turn continues streaming", () => {
    const source = [
      { id: "msg_user", type: "user", text: "recover", time: { created: 1 } },
      {
        id: "msg_failed",
        type: "assistant",
        agent: "build",
        model: { id: "model", providerID: "provider" },
        content: [],
        error: { type: "ProviderError", message: "temporary failure" },
        time: { created: 2, completed: 3 },
      },
      {
        id: "msg_recovery",
        type: "assistant",
        agent: "build",
        model: { id: "model", providerID: "provider" },
        content: [{ type: "text", text: "streaming again" }],
        time: { created: 4 },
      },
    ] satisfies SessionMessageInfo[]
    const normalized = normalizeSessionMessages("ses_1", source)
    const messages = new Map(normalized.messages.map((message) => [message.id, message]))

    const result = Timeline.constructSessionMessageRows(
      source,
      (messageID) => messages.get(messageID),
      (messageID) => normalized.parts.get(messageID) ?? [],
      true,
      "busy",
      true,
      normalized.messages.filter((message) => message.role === "user"),
    )

    expect(result.rows.map((row) => row._tag)).toEqual(["UserMessage", "AssistantPart"])
  })
})

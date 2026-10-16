import { describe, expect, test } from "bun:test"
import type {
  ModelRef,
  SessionMessageAssistant,
  SessionMessageAssistantTool,
  SessionMessageInfo,
} from "@opencode/client/promise"
import { createStore } from "solid-js/store"
import { createRoot } from "solid-js"
import { timelinePresets } from "./detail"
import {
  createTimelineProjection,
  createReactiveTimelineProjection,
  reasoningHeading,
  reuseTimelineRows,
  thinkingStreamCount,
  Timeline,
  TimelineRow,
  type PartGroup,
} from "./projection"

const context = (key: string, partIDs: string[], identity: { userMessageID?: string; messageID?: string } = {}) =>
  new TimelineRow.AssistantPart({
    userMessageID: identity.userMessageID ?? "user-1",
    group: {
      key,
      type: "context",
      refs: partIDs.map((partID) => ({ messageID: identity.messageID ?? "assistant-1", partID })),
    } satisfies PartGroup,
    previousAssistantPart: false,
  })

const patch = (key: string, partIDs: string[], userMessageID = "user-1") =>
  new TimelineRow.AssistantPart({
    userMessageID,
    group: {
      key,
      type: "file",
      refs: partIDs.map((partID) => ({ messageID: "assistant-1", partID })),
    } satisfies PartGroup,
    previousAssistantPart: false,
  })

const part = (key: string, partID: string) =>
  new TimelineRow.AssistantPart({
    userMessageID: "user-1",
    group: {
      key,
      type: "part",
      ref: { messageID: "assistant-1", partID },
    } satisfies PartGroup,
    previousAssistantPart: false,
  })

const user = (userMessageID = "user-1") => new TimelineRow.UserMessage({ userMessageID })

const keys = (rows: TimelineRow.TimelineRow[]) => rows.map(TimelineRow.key)

describe("thinking stream counts", () => {
  const message: SessionMessageAssistant = {
    id: "assistant",
    type: "assistant",
    agent: "build",
    model: { id: "model", providerID: "provider" },
    content: [{ type: "reasoning", text: "many text deltas are not stream events" }],
    streamEventCount: 12,
    time: { created: 0 },
  }

  test("keeps unknown counts unknown and zero distinct when an attempt resets", () => {
    expect(thinkingStreamCount(
      { type: "busy", activity: { userMessageID: "user", model: "preparing" } }, message, "user",
    )).toBeUndefined()
    expect(thinkingStreamCount(
      { type: "busy", activity: { userMessageID: "user", model: "waiting", streamEventCount: 0 } }, message, "user",
    )).toBe(0)
    expect(thinkingStreamCount(
      { type: "busy", activity: { userMessageID: "user", model: "receiving", streamEventCount: 3 } }, message, "user",
    )).toBe(3)
    expect(thinkingStreamCount(
      { type: "retry", attempt: 2, message: "retry", next: 1 }, message, "user",
    )).toBeUndefined()
  })

  test("uses only matching live activity or the current assistant measurement", () => {
    expect(thinkingStreamCount({ type: "busy" }, message, "user")).toBe(12)
    expect(thinkingStreamCount(
      { type: "busy", activity: { userMessageID: "other", model: "receiving", streamEventCount: 99 } }, message, "user",
    )).toBeUndefined()
    expect(thinkingStreamCount({ type: "busy" }, { ...message, streamEventCount: undefined }, "user")).toBeUndefined()
  })
})

describe("Timeline.resolveContent", () => {
  const assistant = (content: SessionMessageAssistant["content"]): SessionMessageAssistant => ({
    id: "assistant",
    type: "assistant",
    agent: "build",
    model: { id: "model", providerID: "provider" },
    content,
    time: { created: 0 },
  })

  const tool = (id: string): SessionMessageAssistantTool => ({
    id,
    type: "tool",
    name: "read",
    state: { status: "running", input: {}, metadata: {} },
    time: { created: 0 },
  })

  test("resolves interleaved ordinals and current store references", () => {
    const [store, setStore] = createStore({
      message: assistant([
        { type: "text", text: "" },
        { type: "reasoning", text: "", time: { created: 0 } },
        tool("read"),
        { type: "text", text: "answer" },
        { type: "reasoning", text: "thought", time: { created: 0 } },
      ]),
    })

    expect(Timeline.resolveContent(store.message, "assistant:text:0")).toBe(store.message.content[0])
    expect(Timeline.resolveContent(store.message, "assistant:reasoning:0")).toBe(store.message.content[1])
    expect(Timeline.resolveContent(store.message, "read")).toBe(store.message.content[2])
    expect(Timeline.resolveContent(store.message, "assistant:text:1")).toBe(store.message.content[3])
    expect(Timeline.resolveContent(store.message, "assistant:reasoning:1")).toBe(store.message.content[4])
    const original = store.message.content[3]
    setStore("message", "content", 3, { type: "text", text: "updated" })
    expect(Timeline.resolveContent(store.message, "assistant:text:1")).toBe(original)
    expect(Timeline.resolveContent(store.message, "assistant:text:1")).toMatchObject({ text: "updated" })

    setStore("message", "content", () => [{ type: "text" as const, text: "replacement" }, tool("replacement-tool")])
    expect(Timeline.resolveContent(store.message, "assistant:text:0")).toBe(store.message.content[0])
    expect(Timeline.resolveContent(store.message, "replacement-tool")).toBe(store.message.content[1])
    expect(Timeline.resolveContent(store.message, "read")).toBeUndefined()
    expect(Timeline.resolveContent(store.message, "assistant:text:1")).toBeUndefined()
  })

  test("stops reading as soon as the part is found", () => {
    const message = assistant([tool("first")])
    message.content.push({
      get type(): "text" {
        throw new Error("read past the matching part")
      },
      text: "later",
    })
    expect(Timeline.resolveContent(message, "first")).toBe(message.content[0])
  })

  test("updates cached tool rows when content changes inside the same message array", () => {
    const [store, setStore] = createStore<{ messages: SessionMessageAssistant[] }>({
      messages: [assistant([{
        ...tool("read"),
        state: {
          status: "completed",
          input: { path: "src/a.ts" },
          content: [{ type: "text", text: "first" }],
          metadata: {},
        },
      }])],
    })

    const messages = store.messages

    const projection = createTimelineProjection({
      sessionMessages: store.messages,
      status: { type: "idle" },
      reasoningMode: "compact",
      timelineDetail: timelinePresets[0].value,
    })

    expect(projection.rows.map(TimelineRow.key)).toEqual(["assistant-part:read:part:assistant:read"])

    setStore("messages", 0, "content", 0, (content) =>
      content.type === "tool" && content.state.status === "completed"
        ? { ...content, state: { ...content.state, metadata: { loaded: ["AGENTS.md"] } } }
        : content,
    )

    expect(store.messages).toBe(messages)

    const updated = createTimelineProjection({
      sessionMessages: store.messages,
      status: { type: "idle" },
      reasoningMode: "compact",
      timelineDetail: timelinePresets[0].value,
      previousRows: projection.rows,
    })

    expect(updated.rows.map(TimelineRow.key)).toEqual(["assistant-part:part:part:assistant:read"])
    expect(updated.rows).not.toBe(projection.rows)
  })
})

describe("reuseTimelineRows", () => {
  test.each([
    {
      name: "reuses an unchanged context group",
      previous: [context("context:a", ["a", "b"])],
      rows: [context("context:a", ["a", "b"])],
      expected: ["assistant-part:context:context:a"],
      reused: [[0, 0]],
    },
    {
      name: "preserves the group key when a member is appended",
      previous: [context("context:a", ["a"])],
      rows: [context("context:a", ["a", "b"])],
      expected: ["assistant-part:context:context:a"],
      reused: [],
    },
    {
      name: "preserves a patch group key when a member is appended",
      previous: [patch("patch:a", ["a"])],
      rows: [patch("patch:a", ["a", "b"])],
      expected: ["assistant-part:file:patch:a"],
      reused: [],
    },
    {
      name: "preserves the group key when the first member is removed",
      previous: [context("context:a", ["a", "b"])],
      rows: [context("context:b", ["b"])],
      expected: ["assistant-part:context:context:a"],
      reused: [],
    },
    {
      name: "lets only the natural owner retain an old key after a split",
      previous: [context("context:a", ["a", "b"])],
      rows: [context("context:a", ["a"]), context("context:b", ["b"])],
      expected: ["assistant-part:context:context:a", "assistant-part:context:context:b"],
      reused: [],
    },
    {
      name: "preserves the file group identity when its first member becomes standalone",
      previous: [patch("part:a", ["a", "b"])],
      rows: [part("part:a", "a"), patch("part:b", ["b"])],
      expected: ["assistant-part:part:part:a", "assistant-part:file:part:a"],
      reused: [],
    },
    {
      name: "preserves the file group identity before a later standalone member",
      previous: [patch("part:a", ["a", "b"])],
      rows: [patch("part:b", ["b"]), part("part:a", "a")],
      expected: ["assistant-part:file:part:a", "assistant-part:part:part:a"],
      reused: [],
    },
    {
      name: "chooses the earliest prior key when groups merge",
      previous: [context("context:a", ["a"]), context("context:b", ["b"])],
      rows: [context("context:b", ["b", "a"])],
      expected: ["assistant-part:context:context:a"],
      reused: [],
    },
    {
      name: "reserves an old key for its natural owner when two new groups compete",
      previous: [context("context:a", ["a", "b"])],
      rows: [context("context:b", ["b"]), context("context:a", ["a"])],
      expected: ["assistant-part:context:context:b", "assistant-part:context:context:a"],
      reused: [],
    },
    {
      // A history prepend can regroup a page-boundary turn under its real user
      // message; the same parts must keep their identity across that move.
      name: "reuses context identity when the same parts move to another user message",
      previous: [context("context:a", ["a", "b"], { userMessageID: "user-1" })],
      rows: [context("context:b", ["b"], { userMessageID: "user-2" })],
      expected: ["assistant-part:context:context:a"],
      reused: [],
    },
    {
      name: "does not reuse context identity across assistant messages",
      previous: [context("context:assistant-1:a", ["a"], { messageID: "assistant-1" })],
      rows: [context("context:assistant-2:a", ["a"], { messageID: "assistant-2" })],
      expected: ["assistant-part:context:context:assistant-2:a"],
      reused: [],
    },
    {
      name: "reuses an unaffected ordinary row",
      previous: [user()],
      rows: [user()],
      expected: ["user-message:user-1"],
      reused: [[0, 0]],
    },
    {
      name: "does not create accidental key collisions",
      previous: [context("context:a", ["a", "b", "c"])],
      rows: [context("context:b", ["b"]), context("context:a", ["a"]), context("context:c", ["c"])],
      expected: [
        "assistant-part:context:context:b",
        "assistant-part:context:context:a",
        "assistant-part:context:context:c",
      ],
      reused: [],
    },
  ])("$name", ({ previous, rows, expected, reused }) => {
    const result = reuseTimelineRows([...previous], [...rows])

    expect(keys(result)).toEqual([...expected])
    expect(new Set(keys(result)).size).toBe(result.length)
    reused.forEach(([resultIndex, previousIndex]) => expect(result[resultIndex]).toBe(previous[previousIndex]))
  })
})

describe("createTimelineProjection", () => {
  test.each(["final sibling", "markers", "orphan"] as const)("inherits agent and model from %s without changing native messages", (source) => {
    const first: SessionMessageAssistant = {
      id: "first", type: "assistant", agent: "first-agent",
      model: { id: "first-model", providerID: "first-provider" },
      content: [{ type: "text", text: "first answer" }], time: { created: 1, completed: 2 },
    }
    const final: SessionMessageAssistant = {
      id: "final", type: "assistant", agent: "final-agent",
      model: { id: "final-model", providerID: "final-provider", variant: "high" },
      content: [{ type: "text", text: "final answer" }], time: { created: 3, completed: 4 },
    }
    const root = { id: "user", type: "user", text: "question", time: { created: source === "final sibling" ? 0 : 5 } } satisfies SessionMessageInfo
    const messages: SessionMessageInfo[] = source === "final sibling" ? [root, first, final] : source === "orphan" ? [first, final, root] : [
      { id: "agent", type: "agent-switched", agent: final.agent, time: { created: 1 } },
      { id: "model", type: "model-switched", model: final.model, time: { created: 2 } },
      root,
    ]
    const before = structuredClone(messages)

    const result = createTimelineProjection({ sessionMessages: messages, status: { type: "idle" }, reasoningMode: "full" })

    expect(result.userContextByID.get("user")).toEqual({
      agent: "final-agent", model: { id: "final-model", providerID: "final-provider", variant: "high" },
    })
    expect(result.sessionMessageByID.get("user")).toBe(root)
    expect(messages).toEqual(before)
    expect([...result.userContextByID.keys()]).toEqual(["user"])
  })

  test("projects a touched native message without rewriting sibling content or unrelated turns", () => {
    createRoot((dispose) => {
      const [store, setStore] = createStore<{ messages: SessionMessageInfo[] }>({ messages: [
        { id: "user", type: "user", text: "question", time: { created: 1 } },
        { id: "selected", type: "assistant", agent: "first", model: { id: "first", providerID: "provider" }, content: [{ type: "text", text: "before" }], time: { created: 2, completed: 3 } },
        { id: "sibling", type: "assistant", agent: "final", model: { id: "final", providerID: "provider" }, content: [{ type: "text", text: "sibling" }], time: { created: 4, completed: 5 } },
        { id: "other-user", type: "user", text: "unrelated", time: { created: 6 } },
        { id: "other", type: "assistant", agent: "other", model: { id: "other", providerID: "provider" }, content: [{ type: "text", text: "unrelated answer" }], time: { created: 7, completed: 8 } },
      ] })
      const result = createReactiveTimelineProjection({ sessionMessages: () => store.messages, status: () => ({ type: "idle" }), reasoningMode: () => "full" })
      const sibling = result.sessionMessageByID().get("sibling")
      const other = result.sessionMessageByID().get("other")
      const keys = result.rows().map(TimelineRow.key)

      setStore("messages", 1, (message) => message.type === "assistant" ? { ...message, content: [{ type: "text", text: "after" }] } : message)

      const selected = result.sessionMessageByID().get("selected")
      expect(selected?.type === "assistant" ? selected.content.map((content) => content.type === "text" ? content.text : undefined) : []).toEqual(["after"])
      expect(result.userContextByID().get("user")).toEqual({ agent: "final", model: { id: "final", providerID: "provider" } })
      expect(result.sessionMessageByID().get("sibling")).toBe(sibling)
      expect(result.sessionMessageByID().get("other")).toBe(other)
      expect([sibling, other].map((message) => message?.type === "assistant" ? message.content.map((content) => content.type === "text" ? content.text : undefined) : [])).toEqual([["sibling"], ["unrelated answer"]])
      expect(result.assistantMessagesByParent().get("user")?.map((message) => message.id)).toEqual(["selected", "sibling"])
      expect(result.assistantMessagesByParent().get("other-user")?.map((message) => message.id)).toEqual(["other"])
      expect(result.rows().map(TimelineRow.key)).toEqual(keys)
      dispose()
    })
  })

  test("keeps shell boundaries and every compaction when native context is inherited", () => {
    const messages = [
      { id: "user", type: "user", text: "question", time: { created: 1 } },
      { id: "assistant", type: "assistant", agent: "review", model: { id: "selected", providerID: "provider" }, content: [], time: { created: 2, completed: 3 } },
      { id: "compact-1", type: "compaction", status: "completed", reason: "auto", summary: "first", recent: "recent", time: { created: 4 } },
      { id: "compact-2", type: "compaction", status: "completed", reason: "auto", summary: "second", recent: "recent", time: { created: 5 } },
      { id: "shell", type: "shell", shellID: "shell-1", command: "pwd", status: "exited", exit: 0, output: { output: "/repo", cursor: 5, size: 5, truncated: false }, time: { created: 6, completed: 7 } },
      { id: "shell-assistant", type: "assistant", agent: "shell-agent", model: { id: "shell-model", providerID: "provider" }, content: [], time: { created: 8, completed: 9 } },
      { id: "next", type: "user", text: "next", time: { created: 10 } },
    ] satisfies SessionMessageInfo[]

    const result = createTimelineProjection({ sessionMessages: messages, status: { type: "idle" }, reasoningMode: "full" })

    expect(result.userContextByID.get("user")).toEqual({ agent: "review", model: { id: "selected", providerID: "provider" } })
    expect(result.userContextByID.get("next")).toEqual({ agent: "shell-agent", model: { id: "shell-model", providerID: "provider" } })
    expect(result.rows.map(TimelineRow.key).filter((key) => key.startsWith("notice:compact"))).toEqual(["notice:compact-1", "notice:compact-2"])
    expect(result.rows.some((row) => TimelineRow.key(row) === "shell:shell")).toBe(true)
    expect(result.sessionMessageByID.get("shell")).toBe(messages[4])
  })

  test("keeps canonical attachments and rich tool snapshots unchanged in native indexes", () => {
    const assistant = { id: "assistant", type: "assistant", agent: "review", model: { id: "selected", providerID: "provider" },
        metadata: { retained: true }, streamEventCount: 0, snapshot: { start: "before", end: "after", files: ["a.ts"] },
        content: [
          { type: "reasoning", text: "thought", time: { created: 2, completed: 3 } },
          { type: "text", text: "answer" },
          { type: "tool", id: "edit", name: "edit", executed: true, providerState: { request: "input" }, providerResultState: { response: "output" },
            state: { status: "completed", input: { path: "/repo/a.ts" }, content: [{ type: "text", text: "done" }, { type: "file", uri: "file:///repo/a.ts", mime: "text/plain" }], metadata: { files: [{ file: "a.ts", additions: 1, deletions: 0 }] } },
            time: { created: 3, ran: 4, completed: 5 } },
        ], cost: 0.1, tokens: { input: 10, output: 5, reasoning: 2, cache: { read: 1, write: 0 } }, time: { created: 2, completed: 5 } } satisfies SessionMessageAssistant
    const messages = [
      { id: "user", type: "user", text: "inspect @a.ts", files: [{ data: "YQ==", mime: "text/plain", source: { type: "uri", uri: "file:///repo/a.ts" }, mention: { text: "@a.ts", start: 8, end: 13 } }], agents: [{ name: "review", mention: { text: "@review", start: 0, end: 7 } }], time: { created: 1 } },
      assistant,
    ] satisfies SessionMessageInfo[]
    const before = structuredClone(messages)

    const result = createTimelineProjection({ sessionMessages: messages, status: { type: "idle" }, reasoningMode: "full" })

    expect(result.sessionMessageByID.get("user")).toBe(messages[0])
    expect(result.assistantMessagesByParent.get("user")?.[0]).toBe(assistant)
    expect(Timeline.resolveContent(assistant, "edit")).toBe(assistant.content[2])
    expect(Timeline.resolveContent(assistant, "assistant:reasoning:0")).toBe(assistant.content[0])
    expect(messages).toEqual(before)
  })

  test("builds current message, parent, context, and row indexes", () => {
    const selectedModel = { id: "selected", providerID: "provider" } satisfies ModelRef
    const assistantModel = { id: "assistant", providerID: "provider", variant: "fast" } satisfies ModelRef

    const messages = [
      { id: "agent", type: "agent-switched", agent: "explore", time: { created: 1 } },
      { id: "model", type: "model-switched", model: selectedModel, time: { created: 2 } },
      { id: "user-1", type: "user", text: "first", time: { created: 3 } },
      {
        id: "assistant-1",
        type: "assistant",
        agent: "build",
        model: assistantModel,
        content: [{ type: "text", text: "answer" }],
        time: { created: 4, completed: 5 },
      },
      {
        id: "user-2",
        type: "user",
        text: "second",
        metadata: {
          agent: "review",
          model: { modelID: "override", providerID: "custom", variant: "precise" },
        },
        time: { created: 6 },
      },
    ] satisfies SessionMessageInfo[]

    const result = createTimelineProjection({
      sessionMessages: messages,
      status: { type: "busy" },
      reasoningMode: "full",
    })

    expect(result.activeMessageID).toBe("user-2")
    expect(result.messageByID).toBe(result.sessionMessageByID)
    expect(result.sessionMessageByID.get("assistant-1")).toBe(messages[3])
    expect(result.assistantMessagesByParent.get("user-1")?.map((message) => message.id)).toEqual(["assistant-1"])
    expect(result.assistantMessagesByParent.has("user-2")).toBe(false)
    expect(result.userContextByID.get("user-1")).toEqual({ agent: "build", model: assistantModel })
    expect(result.userContextByID.get("user-2")).toEqual({
      agent: "review",
      model: { id: "override", providerID: "custom", variant: "precise" },
    })
    expect(result.messageRowIndex.get("user-1")).toBe(0)
    expect(result.messageLastRowIndex.get("user-1")).toBe(3)
    expect(result.lastAssistantGroupKey.get("user-1")).toBe("part:assistant-1:assistant-1:text:0")
    expect(result.rowByKey.get("user-message:user-1")).toBe(result.rows[2])
  })

  test("reuses a stable projected row array", () => {
    const messages = [
      { id: "user-1", type: "user", text: "first", time: { created: 1 } },
      {
        id: "assistant-1",
        type: "assistant",
        agent: "build",
        model: { id: "model", providerID: "provider" },
        content: [{ type: "text", text: "answer" }],
        time: { created: 2, completed: 3 },
      },
    ] satisfies SessionMessageInfo[]

    const first = createTimelineProjection({
      sessionMessages: messages,
      status: { type: "idle" },
      reasoningMode: "full",
    })

    const second = createTimelineProjection({
      sessionMessages: messages,
      status: { type: "idle" },
      reasoningMode: "full",
      previousRows: first.rows,
    })

    expect(second.rows).toBe(first.rows)
    expect(second.rows[0]).toBe(first.rows[0])
    expect(second.rows[1]).toBe(first.rows[1])
  })

  test("indexes a leading partial assistant turn under its projected turn ID", () => {
    const messages = [
      {
        id: "assistant-1",
        type: "assistant",
        agent: "build",
        model: { id: "model", providerID: "provider" },
        content: [{ type: "text", text: "partial answer" }],
        time: { created: 2, completed: 3 },
      },
      {
        id: "assistant-2",
        type: "assistant",
        agent: "build",
        model: { id: "model", providerID: "provider" },
        content: [{ type: "text", text: "final answer" }],
        time: { created: 4, completed: 5 },
      },
    ] satisfies SessionMessageInfo[]

    const result = createTimelineProjection({
      sessionMessages: messages,
      status: { type: "idle" },
      reasoningMode: "full",
    })

    expect(result.assistantMessagesByParent.get("assistant-1")?.map((message) => message.id)).toEqual([
      "assistant-1",
      "assistant-2",
    ])
  })
})

describe("reasoningHeading", () => {
  test("returns the most recent thinking summary heading as sections stream in", () => {
    const sections = [
      "**Designing Interactive Experience**\nI'm focusing on crafting a fun, tactile, and interactive visual toy.",
      "**Analyzing Reading Recommendations**\nI'm currently dissecting whether Luke Parker should read the *Throne of Glass* series.",
      "**Constructing Reader Tools**\nI'm now building out interactive tools.",
    ]

    expect(reasoningHeading(sections[0])).toBe("Designing Interactive Experience")
    expect(reasoningHeading(sections.slice(0, 2).join("\n\n"))).toBe("Analyzing Reading Recommendations")
    expect(reasoningHeading(sections.join("\n\n"))).toBe("Constructing Reader Tools")
  })

  test("keeps the previous heading while the next bold heading is still streaming", () => {
    const text = [
      "**Designing Interactive Experience**\nI'm focusing on crafting a fun, tactile, and interactive visual toy.",
      "**Analyzing Reading",
    ].join("\n\n")

    expect(reasoningHeading(text)).toBe("Designing Interactive Experience")
  })

  test("returns the latest heading across markdown heading styles and ignores inline bold spans", () => {
    expect(reasoningHeading("## First step\n\nDetails.\n\n## Second step\n\nMore details.")).toBe("Second step")
    expect(reasoningHeading("<h2>First step</h2>\n<p>Body</p>\n<h3>Second step</h3>")).toBe("Second step")
    expect(reasoningHeading("First step\n===\n\nSecond step\n---")).toBe("Second step")
    expect(
      reasoningHeading("**Designing Interactive Experience**\n\n**Note** that this line ends with **bold**"),
    ).toBe("Designing Interactive Experience")
  })
})

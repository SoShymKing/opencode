import { describe, expect, test } from "bun:test"
import type { SessionMessageInfo } from "@opencode-ai/client/promise"
import { normalizeTouchedSessionMessages } from "./server-session-v2-projection"

type Assistant = Extract<SessionMessageInfo, { type: "assistant" }>

const model = (id: string) => ({ id, providerID: `${id}-provider` })
const user = (id: string): SessionMessageInfo => ({ id, type: "user", text: id, time: { created: 1 } })
const assistant = (
  id: string,
  input: { agent?: string; modelID?: string; text?: string } = {},
): Assistant => ({
  id,
  type: "assistant",
  agent: input.agent ?? "build",
  model: model(input.modelID ?? "default"),
  content: [{ type: "text", text: input.text ?? id }],
  time: { created: 2 },
})
const compaction = (id: string): SessionMessageInfo => ({
  id,
  type: "compaction",
  status: "completed",
  reason: "auto",
  summary: id,
  recent: "recent",
  time: { created: 3 },
})

describe("normalizeTouchedSessionMessages", () => {
  test("uses final sibling assistant metadata while projecting only touched legacy IDs", () => {
    const source = [
      user("user"),
      assistant("assistant-1", { agent: "first", modelID: "first-model" }),
      assistant("assistant-2", { agent: "final", modelID: "final-model" }),
    ]

    const result = normalizeTouchedSessionMessages("session", source, ["assistant-1"])

    expect(result.messages.map((message) => message.id)).toEqual(["user", "assistant-1"])
    expect(result.messages[0]).toMatchObject({
      agent: "final",
      model: { modelID: "final-model", providerID: "final-model-provider" },
    })
    expect([...result.parts.keys()]).toEqual(["user", "assistant-1"])
    expect(result.touched).toEqual(new Set(["assistant-1", "user"]))
  })

  test("preserves every compaction attached to a touched root unit", () => {
    const source = [user("user"), compaction("compaction-1"), compaction("compaction-2")]

    const result = normalizeTouchedSessionMessages("session", source, ["compaction-1"])

    expect(result.messages.map((message) => message.id)).toEqual(["user"])
    expect(result.parts.get("user")?.map((part) => part.id)).toEqual([
      "user:text:0",
      "compaction-1:compaction",
      "compaction-2:compaction",
    ])
    expect(result.touched).toEqual(new Set(["compaction-1", "user"]))
  })

  test("seeds a touched root with latest marker-derived context", () => {
    const source = [
      { id: "agent-marker", type: "agent-switched", agent: "review", time: { created: 1 } },
      { id: "model-marker", type: "model-switched", model: model("marker-model"), time: { created: 2 } },
      user("user"),
    ] satisfies SessionMessageInfo[]

    const result = normalizeTouchedSessionMessages("session", source, ["agent-marker", "model-marker", "user"])

    expect(result.messages).toEqual([
      expect.objectContaining({
        id: "user",
        agent: "review",
        model: { modelID: "marker-model", providerID: "marker-model-provider" },
      }),
    ])
    expect(result.touched).toEqual(new Set(["agent-marker", "model-marker", "user"]))
  })

  test("treats a touched shell as a standalone hard boundary with incoming context", () => {
    const source = [
      user("previous-user"),
      assistant("previous-assistant", { agent: "shell-agent", modelID: "shell-model" }),
      {
        id: "shell",
        type: "shell",
        shellID: "shell-id",
        command: "printf hello",
        status: "exited",
        exit: 0,
        output: { output: "hello", cursor: 5, size: 5, truncated: false },
        time: { created: 3, completed: 4 },
      },
      user("next-user"),
    ] satisfies SessionMessageInfo[]

    const result = normalizeTouchedSessionMessages("session", source, ["shell"])

    expect(result.messages).toEqual([
      expect.objectContaining({ id: "shell", role: "user", agent: "shell-agent" }),
      expect.objectContaining({ id: "shell:assistant", role: "assistant", parentID: "shell" }),
    ])
    expect([...result.parts.keys()]).toEqual(["shell", "shell:assistant"])
    expect(result.touched).toEqual(new Set(["shell", "shell:assistant"]))
  })

  test("uses orphan assistant context without projecting the orphan", () => {
    const source = [assistant("orphan", { agent: "orphan-agent", modelID: "orphan-model" }), user("user")]

    const result = normalizeTouchedSessionMessages("session", source, ["user"])

    expect(result.messages).toEqual([
      expect.objectContaining({
        id: "user",
        agent: "orphan-agent",
        model: { modelID: "orphan-model", providerID: "orphan-model-provider" },
      }),
    ])
  })

  test("normalizes disconnected touched turns independently in source order", () => {
    const source = [user("user-1"), assistant("assistant-1"), user("user-2"), assistant("assistant-2")]

    const result = normalizeTouchedSessionMessages("session", source, ["assistant-1", "assistant-2"])

    expect(result.messages.map((message) => [message.id, message.role === "assistant" ? message.parentID : undefined])).toEqual([
      ["user-1", undefined],
      ["assistant-1", "user-1"],
      ["user-2", undefined],
      ["assistant-2", "user-2"],
    ])
    expect(result.touched).toEqual(new Set(["assistant-1", "assistant-2", "user-1", "user-2"]))
  })

  test("does not normalize an unrelated large assistant", () => {
    const unrelated = {
      id: "unrelated-assistant",
      type: "assistant",
      agent: "unrelated",
      model: model("unrelated-model"),
      get content(): Assistant["content"] {
        throw new Error("unrelated assistant normalized")
      },
      time: { created: 4 },
    } satisfies Assistant
    const source = [user("selected-user"), assistant("selected-assistant"), user("unrelated-user"), unrelated]

    const result = normalizeTouchedSessionMessages("session", source, ["selected-assistant"])

    expect(result.messages.map((message) => message.id)).toEqual(["selected-user", "selected-assistant"])
  })

  test("keeps raw marker IDs without selecting their neighboring unit", () => {
    const source = [
      user("user"),
      { id: "marker", type: "agent-switched", agent: "review", time: { created: 2 } },
      assistant("assistant"),
    ] satisfies SessionMessageInfo[]

    const result = normalizeTouchedSessionMessages("session", source, ["marker"])

    expect(result.messages).toEqual([])
    expect(result.parts.size).toBe(0)
    expect(result.touched).toEqual(new Set(["marker"]))
  })
})

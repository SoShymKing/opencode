import { describe, expect, test } from "bun:test"
import { SessionMessage } from "@opencode-ai/schema/session-message"
import { Schema } from "effect"
import { normalizeSessionMessages, sessionMessagePartID } from "./session-message"
type NativeMessage = typeof SessionMessage.Message.Encoded
const native = Schema.decodeUnknownSync(Schema.Array(Schema.toEncoded(SessionMessage.Message)))

describe("normalizeSessionMessages", () => {
  test("projects current turns into stable legacy rendering records", () => {
    const source = [
      { id: "msg_1", type: "agent-switched", agent: "build", time: { created: 1 } },
      {
        id: "msg_2",
        type: "model-switched",
        model: { id: "claude", providerID: "anthropic", variant: "high" },
        time: { created: 2 },
      },
      {
        id: "msg_3",
        type: "user",
        text: "inspect @src/client.ts",
        files: [
          {
            uri: "data:text/plain;base64,aGVsbG8=",
            mime: "text/plain",
            name: "note.txt",
          },
          {
            uri: "data:text/plain;base64,ZXhwb3J0IHt9",
            mime: "text/plain",
            name: "client.ts",
            source: { text: "@src/client.ts", start: 8, end: 22 },
          },
        ],
        agents: [{ name: "review", source: { text: "@review", start: 0, end: 7 } }],
        time: { created: 3 },
      },
      {
        id: "msg_4",
        type: "assistant",
        agent: "build",
        model: { id: "claude", providerID: "anthropic", variant: "high" },
        content: [
          { type: "reasoning", id: "reason_1", text: "Thinking", time: { created: 4, completed: 5 } },
          { type: "text", id: "text_1", text: "Result" },
          {
            type: "tool",
            id: "call_1",
            name: "read",
            state: {
              status: "completed",
              input: { filePath: "note.txt" },
              structured: { title: "note.txt" },
              content: [{ type: "text", text: "hello" }],
            },
            time: { created: 5, ran: 6, completed: 7 },
          },
        ],
        cost: 0.1,
        tokens: { input: 10, output: 5, reasoning: 2, cache: { read: 1, write: 0 } },
        time: { created: 4, completed: 7 },
      },
      {
        id: "msg_5",
        type: "compaction",
        reason: "auto",
        summary: "summary",
        recent: "recent",
        time: { created: 8 },
      },
    ] satisfies NativeMessage[]

    const result = normalizeSessionMessages("ses_1", native(source))

    expect(result.messages).toHaveLength(2)
    expect(result.messages[0]).toMatchObject({
      id: "msg_3",
      role: "user",
      agent: "build",
      model: { providerID: "anthropic", modelID: "claude", variant: "high" },
    })
    expect(result.messages[1]).toMatchObject({ id: "msg_4", role: "assistant", parentID: "msg_3", cost: 0.1 })
    expect(result.parts.get("msg_3")?.map((part) => part.id)).toEqual([
      "msg_3:text:0",
      "msg_3:file:0",
      "msg_3:file:1",
      "msg_3:agent:0",
      "msg_5:compaction",
    ])
    expect(result.parts.get("msg_3")?.[2]).toMatchObject({
      type: "file",
      source: {
        type: "file",
        path: "src/client.ts",
        text: { value: "@src/client.ts", start: 8, end: 22 },
      },
    })
    expect(result.parts.get("msg_4")?.map((part) => part.id)).toEqual(["msg_4:reason_1", "msg_4:text_1", "msg_4:call_1"])
    expect(result.parts.get("msg_4")?.[2]).toMatchObject({
      type: "tool",
      tool: "read",
      state: { status: "completed", output: "hello" },
    })
  })

  test("does not invent a parent for an assistant-only page", () => {
    const source = [
      {
        id: "msg_2",
        type: "assistant",
        agent: "build",
        model: { id: "model", providerID: "provider" },
        content: [{ type: "text", id: "text_1", text: "orphan" }],
        time: { created: 2 },
      },
    ] satisfies NativeMessage[]

    expect(normalizeSessionMessages("ses_1", native(source)).messages).toEqual([])
  })

  test("projects a current shell message into a renderable standalone turn", () => {
    const source = [
      {
        id: "msg_shell",
        type: "shell",
        callID: "shell_1",
        command: "printf hello",
        output: "hello",
        time: { created: 1, completed: 2 },
      },
    ] satisfies NativeMessage[]

    const result = normalizeSessionMessages("ses_1", native(source))

    expect(result.messages).toEqual([
      expect.objectContaining({ id: "msg_shell", role: "user" }),
      expect.objectContaining({ id: "msg_shell:assistant", role: "assistant", parentID: "msg_shell" }),
    ])
    expect(result.parts.get("msg_shell")).toEqual([expect.objectContaining({ type: "text", text: "printf hello" })])
    expect(result.parts.get("msg_shell:assistant")).toEqual([
      expect.objectContaining({
        type: "tool",
        tool: "bash",
        state: expect.objectContaining({
          status: "completed",
          input: { command: "printf hello" },
          output: "hello",
          title: "Shell",
        }),
      }),
    ])
  })

  test("adapts current edit fields for the legacy edit renderer", () => {
    const source = [
      { id: "msg_user", type: "user", text: "edit it", time: { created: 1 } },
      {
        id: "msg_assistant",
        type: "assistant",
        agent: "build",
        model: { id: "model", providerID: "provider" },
        content: [
          {
            type: "tool",
            id: "call_edit",
            name: "edit",
            state: {
              status: "completed",
              input: { path: "/repo/README.md", oldString: "old", newString: "new" },
              content: [{ type: "text", text: "Edited file successfully" }],
              structured: {
                files: [
                  {
                    file: "README.md",
                    patch: "@@ -1 +1 @@\n-old\n+new",
                    additions: 1,
                    deletions: 1,
                    status: "modified",
                  },
                ],
                replacements: 1,
              },
            },
            time: { created: 2, ran: 3, completed: 4 },
          },
        ],
        time: { created: 2, completed: 4 },
      },
    ] satisfies NativeMessage[]

    const result = normalizeSessionMessages("ses_1", native(source))

    expect(result.parts.get("msg_assistant")).toEqual([
      expect.objectContaining({
        type: "tool",
        tool: "edit",
        state: expect.objectContaining({
          status: "completed",
          input: expect.objectContaining({ path: "/repo/README.md", filePath: "/repo/README.md" }),
          metadata: expect.objectContaining({
            filediff: {
              file: "README.md",
              patch: "@@ -1 +1 @@\n-old\n+new",
              additions: 1,
              deletions: 1,
            },
          }),
        }),
      }),
    ])
  })

  test("scopes explicit content IDs and keeps identity after empty text omission", () => {
    const source = [
      { id: "msg_user", type: "user", text: "inspect", time: { created: 1 } },
      ...["msg_a", "msg_b"].map((id) => ({
        id, type: "assistant" as const, agent: "build", model: { id: "model", providerID: "provider" },
        content: [
          { type: "text" as const, id: "empty", text: "" },
          { type: "text" as const, id: "shared", text: "answer" },
          { type: "tool" as const, id: "call", name: "edit", state: { status: "pending" as const, input: "{}" }, time: { created: 2 } },
        ], time: { created: 2 },
      })),
    ] satisfies NativeMessage[]
    const result = normalizeSessionMessages("ses_1", native(source))
    expect(result.parts.get("msg_a")?.map((part) => part.id)).toEqual(["msg_a:shared", "msg_a:call"])
    expect(result.parts.get("msg_b")?.map((part) => part.id)).toEqual(["msg_b:shared", "msg_b:call"])
    expect(result.parts.get("msg_b")?.[1]).toMatchObject({ callID: "call", state: { status: "pending", raw: "{}" } })
    expect(sessionMessagePartID("msg_a", "shared")).toBe("msg_a:shared")
    expect(sessionMessagePartID("msg_a", "text:0")).toBe("msg_a:text:0")
  })

  test("projects native synthetic text without a description", () => {
    const source = [{ id: "msg_s", sessionID: "ses_1", type: "synthetic", text: "context", metadata: { retained: true }, time: { created: 3 } }] satisfies NativeMessage[]
    expect(normalizeSessionMessages("ses_1", native(source)).parts.get("msg_s")).toEqual([
      expect.objectContaining({ id: "msg_s:text:0", text: "context", synthetic: true }),
    ])
    expect(source[0].metadata).toEqual({ retained: true })
  })

  test("retains canonical rich snapshot fields while adapting UI tool states", () => {
    const source = native([
      { id: "msg_user", type: "user", text: "run", time: { created: 1 } },
      { id: "msg_assistant", type: "assistant", agent: "build", model: { id: "model", providerID: "provider" },
        metadata: { canonical: true }, streamEventCount: 0, snapshot: { start: "before", end: "after", files: ["a"] }, time: { created: 2, completed: 9 },
        content: [
          { type: "tool", id: "pending", name: "write", state: { status: "pending", input: "{\"path\":\"a\"}" }, time: { created: 2 } },
          { type: "tool", id: "running", name: "write", state: { status: "running", input: { path: "a" }, structured: { progress: 1 }, content: [{ type: "text", text: "partial" }] }, time: { created: 3, ran: 4 } },
          { type: "tool", id: "completed", name: "write", provider: { executed: true, metadata: { vendor: { request: true } }, resultMetadata: { vendor: { response: true } } },
            state: { status: "completed", input: { path: "a" }, structured: { done: true }, content: [{ type: "text", text: "done" }, { type: "file", uri: "file:///repo/a", mime: "text/plain" }],
              attachments: [{ uri: "file:///repo/b", mime: "text/plain", description: "attachment" }], outputPaths: ["a", "b"], result: { preserved: true } }, time: { created: 5, ran: 6, completed: 7, pruned: 8 } },
          { type: "tool", id: "error", name: "write", state: { status: "error", input: { path: "a" }, structured: { partial: true }, content: [{ type: "text", text: "partial" }], error: { type: "unknown", message: "failure" }, result: { preserved: true } }, time: { created: 8, completed: 9 } },
        ],
      },
    ])
    const before = structuredClone(source)
    const result = normalizeSessionMessages("ses_1", source)
    expect(source).toEqual(before)
    expect(result.messages[1]).toMatchObject({ time: { created: 2, completed: 9 }, streamEventCount: 0 })
    expect(result.parts.get("msg_assistant")).toEqual([
      expect.objectContaining({ id: "msg_assistant:pending", callID: "pending", state: expect.objectContaining({ status: "pending", input: { path: "a", filePath: "a" } }) }),
      expect.objectContaining({ id: "msg_assistant:running", state: expect.objectContaining({ status: "running", metadata: { progress: 1 }, time: { start: 4 } }) }),
      expect.objectContaining({ id: "msg_assistant:completed", metadata: { executed: true, metadata: { vendor: { request: true } }, resultMetadata: { vendor: { response: true } } },
        state: expect.objectContaining({ status: "completed", output: "done", time: { start: 6, end: 7 }, attachments: [
          expect.objectContaining({ id: "msg_assistant:completed:file:1", url: "file:///repo/a" }),
          expect.objectContaining({ id: "msg_assistant:completed:file:2", url: "file:///repo/b" }),
        ] }) }),
      expect.objectContaining({ id: "msg_assistant:error", state: expect.objectContaining({ status: "error", error: "failure", metadata: { partial: true }, time: { start: 8, end: 9 } }) }),
    ])
  })
})

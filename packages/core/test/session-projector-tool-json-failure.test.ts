import { describe, expect } from "bun:test"
import { eq } from "drizzle-orm"
import { Effect } from "effect"
import { SessionEvent } from "@opencode-ai/core/session/event"
import { SessionMessage } from "@opencode-ai/core/session/message"
import { SessionMessageTable } from "@opencode-ai/core/session/sql"
import { at, encodeAssistant, it, setup, text, tool } from "./session-projector-tool-json.fixture"

describe("SessionProjector tool JSON failures", () => {
  it.effect("preserves pending and running failure rules", () =>
    Effect.gen(function* () {
      const fixture = yield* setup("failure")
      const base = { sessionID: fixture.sessionID, assistantMessageID: fixture.assistantMessageID }
      yield* fixture.events.publish(SessionEvent.Tool.Input.Started, {
        ...base,
        callID: "pending",
        name: "bash",
        timestamp: at(1),
      })
      yield* fixture.events.publish(SessionEvent.Tool.Failed, {
        ...base,
        callID: "pending",
        error: { type: "unknown", message: "pending" },
        provider: { executed: false },
        result: "pending-result",
        timestamp: at(2),
      })
      yield* fixture.events.publish(SessionEvent.Tool.Input.Started, {
        ...base,
        callID: "running",
        name: "bash",
        timestamp: at(3),
      })
      yield* fixture.events.publish(SessionEvent.Tool.Called, {
        ...base,
        callID: "running",
        tool: "bash",
        input: { command: "pwd" },
        provider: { executed: false, metadata: { call: { kept: true } } },
        timestamp: at(4),
      })
      yield* fixture.events.publish(SessionEvent.Tool.Progress, {
        ...base,
        callID: "running",
        structured: { phase: "saved" },
        content: text("saved"),
        timestamp: at(5),
      })
      yield* fixture.events.publish(SessionEvent.Tool.Failed, {
        ...base,
        callID: "running",
        error: { type: "unknown", message: "running" },
        provider: { executed: true, metadata: { result: { kept: true } } },
        timestamp: at(6),
      })
      const assistant = yield* fixture.read
      expect(tool(assistant, "pending")).toMatchObject({
        provider: { executed: false },
        state: { status: "error", input: {}, structured: {}, content: [], result: "pending-result" },
        time: { completed: at(2) },
      })
      expect(tool(assistant, "running")).toMatchObject({
        provider: { executed: true, metadata: { call: { kept: true } }, resultMetadata: { result: { kept: true } } },
        state: { status: "error", input: { command: "pwd" }, structured: { phase: "saved" }, content: text("saved") },
        time: { ran: at(4), completed: at(6) },
      })
    }),
  )

  it.effect("keeps missing and stale events inert and targets the last duplicate ID", () =>
    Effect.gen(function* () {
      const fixture = yield* setup("duplicates")
      const base = { sessionID: fixture.sessionID, assistantMessageID: fixture.assistantMessageID, callID: "same" }
      yield* fixture.events.publish(SessionEvent.Tool.Called, {
        ...base,
        tool: "bash",
        input: {},
        provider: { executed: false },
        timestamp: at(1),
      })
      yield* fixture.events.publish(SessionEvent.Tool.Input.Started, { ...base, name: "first", timestamp: at(2) })
      yield* fixture.events.publish(SessionEvent.Tool.Input.Started, { ...base, name: "second", timestamp: at(3) })
      yield* fixture.events.publish(SessionEvent.Tool.Success, {
        ...base,
        structured: {},
        content: [],
        provider: { executed: false },
        timestamp: at(4),
      })
      yield* fixture.events.publish(SessionEvent.Tool.Input.Ended, { ...base, text: "last", timestamp: at(5) })
      yield* fixture.events.publish(SessionEvent.Tool.Called, {
        ...base,
        tool: "bash",
        input: { selected: true },
        provider: { executed: false },
        timestamp: at(6),
      })
      yield* fixture.events.publish(SessionEvent.Tool.Input.Ended, { ...base, text: "stale", timestamp: at(7) })
      yield* fixture.events.publish(SessionEvent.Tool.Progress, {
        ...base,
        callID: "missing",
        structured: { bad: true },
        content: [],
        timestamp: at(8),
      })
      yield* fixture.events.publish(SessionEvent.Tool.Input.Started, {
        ...base,
        assistantMessageID: SessionMessage.ID.make("msg_missing"),
        name: "missing",
        timestamp: at(9),
      })
      const matches = (yield* fixture.read).content.filter(
        (item): item is SessionMessage.AssistantTool => item.type === "tool" && item.id === "same",
      )
      expect(matches).toMatchObject([
        { name: "first", state: { status: "pending", input: "" } },
        { name: "second", state: { status: "running", input: { selected: true } } },
      ])
    }),
  )

  it.effect("rolls back event persistence when stored tool data is invalid", () =>
    Effect.gen(function* () {
      const fixture = yield* setup("rollback")
      const row = yield* fixture.db
        .select()
        .from(SessionMessageTable)
        .where(eq(SessionMessageTable.id, fixture.assistantMessageID))
        .get()
        .pipe(Effect.orDie)
      if (!row) return yield* Effect.die("Missing assistant")
      const invalid = JSON.stringify({ ...row.data, content: [{ type: "tool", id: "broken" }] })
      yield* fixture.corrupt(invalid)
      const exit = yield* fixture.events
        .publish(SessionEvent.Tool.Called, {
          sessionID: fixture.sessionID,
          assistantMessageID: fixture.assistantMessageID,
          callID: "broken",
          tool: "bash",
          input: {},
          provider: { executed: false },
          timestamp: at(1),
        })
        .pipe(Effect.exit)
      expect(exit._tag).toBe("Failure")
      yield* fixture.expectRollback(invalid)
    }),
  )

  it.effect("rolls back Input.Started when required assistant fields are missing", () =>
    Effect.gen(function* () {
      const fixture = yield* setup("malformed_top_level")
      const { id: _, type: __, ...data } = encodeAssistant(fixture.initial)
      const invalid = JSON.stringify({ content: data.content, time: data.time })
      yield* fixture.corrupt(invalid)
      const exit = yield* fixture.events
        .publish(SessionEvent.Tool.Input.Started, {
          sessionID: fixture.sessionID,
          assistantMessageID: fixture.assistantMessageID,
          callID: "call",
          name: "bash",
          timestamp: at(1),
        })
        .pipe(Effect.exit)
      expect(exit._tag).toBe("Failure")
      yield* fixture.expectRollback(invalid)
    }),
  )

  it.effect("rolls back matching tool events when an unrelated sibling is malformed", () =>
    Effect.gen(function* () {
      const pending = SessionMessage.AssistantTool.make({
        type: "tool",
        id: "call",
        name: "bash",
        state: SessionMessage.ToolStatePending.make({ status: "pending", input: "" }),
        time: { created: at(0) },
      })
      const fixture = yield* setup("malformed_sibling_matching", [pending])
      const { id: _, type: __, ...data } = encodeAssistant(fixture.initial)
      const invalid = JSON.stringify({ ...data, content: [...data.content, { type: "text", id: "broken" }] })
      yield* fixture.corrupt(invalid)
      const exit = yield* fixture.events
        .publish(SessionEvent.Tool.Input.Ended, {
          sessionID: fixture.sessionID,
          assistantMessageID: fixture.assistantMessageID,
          callID: "call",
          text: "raw",
          timestamp: at(1),
        })
        .pipe(Effect.exit)
      expect(exit._tag).toBe("Failure")
      yield* fixture.expectRollback(invalid)
    }),
  )

  it.effect("rolls back stale tool events when an unrelated sibling is malformed", () =>
    Effect.gen(function* () {
      const completed = SessionMessage.AssistantTool.make({
        type: "tool",
        id: "call",
        name: "bash",
        state: SessionMessage.ToolStateCompleted.make({ status: "completed", input: {}, structured: {}, content: [] }),
        time: { created: at(0), completed: at(0) },
      })
      const fixture = yield* setup("malformed_sibling_stale", [completed])
      const { id: _, type: __, ...data } = encodeAssistant(fixture.initial)
      const invalid = JSON.stringify({ ...data, content: [...data.content, { type: "reasoning", id: "broken" }] })
      yield* fixture.corrupt(invalid)
      const exit = yield* fixture.events
        .publish(SessionEvent.Tool.Progress, {
          sessionID: fixture.sessionID,
          assistantMessageID: fixture.assistantMessageID,
          callID: "call",
          structured: {},
          content: [],
          timestamp: at(1),
        })
        .pipe(Effect.exit)
      expect(exit._tag).toBe("Failure")
      yield* fixture.expectRollback(invalid)
    }),
  )
})

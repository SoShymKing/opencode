import { describe, expect } from "bun:test"
import { asc, eq } from "drizzle-orm"
import { Effect } from "effect"
import { EventTable } from "@opencode-ai/core/event/sql"
import { SessionEvent } from "@opencode-ai/core/session/event"
import { SessionMessage } from "@opencode-ai/core/session/message"
import { at, it, setup, text, tool } from "./session-projector-tool-json.fixture"

describe("SessionProjector tool JSON", () => {
  it.effect("uses targeted JSON writes for all tool lifecycle events", () =>
    Effect.gen(function* () {
      const fixture = yield* setup("mechanism", [
        SessionMessage.AssistantText.make({ type: "text", id: "text", text: "keep" }),
      ])
      const queries: string[] = []
      const unsafe = fixture.db.$client.unsafe
      const descriptor = Object.getOwnPropertyDescriptor(fixture.db.$client, "unsafe")
      if (!descriptor) return yield* Effect.die("Missing SQL client unsafe descriptor")
      Object.defineProperty(fixture.db.$client, "unsafe", {
        configurable: true,
        value: <A extends object>(query: string, params?: ReadonlyArray<unknown>) => {
          queries.push(query)
          return unsafe<A>(query, params)
        },
      })
      const base = { sessionID: fixture.sessionID, assistantMessageID: fixture.assistantMessageID }
      yield* Effect.gen(function* () {
        yield* fixture.events.publish(SessionEvent.Tool.Input.Started, {
          ...base,
          callID: "success",
          name: "bash",
          timestamp: at(1),
        })
        yield* fixture.events.publish(SessionEvent.Tool.Input.Ended, {
          ...base,
          callID: "success",
          text: "raw",
          timestamp: at(2),
        })
        yield* fixture.events.publish(SessionEvent.Tool.Called, {
          ...base,
          callID: "success",
          tool: "bash",
          input: {},
          provider: { executed: false },
          timestamp: at(3),
        })
        yield* fixture.events.publish(SessionEvent.Tool.Progress, {
          ...base,
          callID: "success",
          structured: {},
          content: [],
          timestamp: at(4),
        })
        yield* fixture.events.publish(SessionEvent.Tool.Success, {
          ...base,
          callID: "success",
          structured: {},
          content: [],
          provider: { executed: false },
          timestamp: at(5),
        })
        yield* fixture.events.publish(SessionEvent.Tool.Input.Started, {
          ...base,
          callID: "failed",
          name: "bash",
          timestamp: at(6),
        })
        yield* fixture.events.publish(SessionEvent.Tool.Failed, {
          ...base,
          callID: "failed",
          error: { type: "unknown", message: "failed" },
          provider: { executed: false },
          timestamp: at(7),
        })
      }).pipe(Effect.ensuring(Effect.sync(() => Object.defineProperty(fixture.db.$client, "unsafe", descriptor))))
      const writes = queries.filter((query) => query.toLowerCase().startsWith('update "session_message"'))
      expect(writes).toHaveLength(7)
      expect(writes.every((query) => query.includes("json_insert") || query.includes("json_replace"))).toBe(true)
    }),
  )

  it.effect("preserves every success state and step-by-step replay", () =>
    Effect.gen(function* () {
      const fixture = yield* setup("success")
      const base = { sessionID: fixture.sessionID, assistantMessageID: fixture.assistantMessageID, callID: "call" }
      const transitions = [
        fixture.events.publish(SessionEvent.Tool.Input.Started, { ...base, name: "bash", timestamp: at(1) }),
        fixture.events.publish(SessionEvent.Tool.Input.Ended, { ...base, text: "raw", timestamp: at(2) }),
        fixture.events.publish(SessionEvent.Tool.Called, {
          ...base,
          tool: "bash",
          input: { command: "pwd" },
          provider: { executed: true, metadata: { source: { call: true } } },
          timestamp: at(3),
        }),
        fixture.events.publish(SessionEvent.Tool.Progress, {
          ...base,
          structured: { phase: "running" },
          content: text("working"),
          timestamp: at(4),
        }),
        fixture.events.publish(SessionEvent.Tool.Success, {
          ...base,
          structured: { phase: "done" },
          content: text("complete"),
          outputPaths: ["out"],
          provider: { executed: false, metadata: { source: { result: true } } },
          timestamp: at(5),
        }),
      ]
      const snapshots: SessionMessage.Assistant[] = []
      for (const transition of transitions) {
        yield* transition
        snapshots.push(yield* fixture.read)
      }
      expect(snapshots.map((assistant) => tool(assistant, "call"))).toMatchObject([
        { state: { status: "pending", input: "" }, time: { created: at(1) } },
        { state: { status: "pending", input: "raw" }, time: { created: at(1) } },
        {
          provider: { executed: true, metadata: { source: { call: true } } },
          state: { status: "running", input: { command: "pwd" }, structured: {}, content: [] },
          time: { created: at(1), ran: at(3) },
        },
        { state: { status: "running", structured: { phase: "running" }, content: text("working") } },
        {
          provider: {
            executed: true,
            metadata: { source: { call: true } },
            resultMetadata: { source: { result: true } },
          },
          state: {
            status: "completed",
            structured: { phase: "done" },
            content: text("complete"),
            outputPaths: ["out"],
          },
          time: { completed: at(5) },
        },
      ])
      const stored = yield* fixture.db
        .select()
        .from(EventTable)
        .where(eq(EventTable.aggregate_id, fixture.sessionID))
        .orderBy(asc(EventTable.seq))
        .all()
        .pipe(Effect.orDie)
      yield* fixture.events.remove(fixture.sessionID)
      yield* fixture.write(fixture.initial)
      for (const [index, event] of stored.entries()) {
        yield* fixture.events.replay({
          id: event.id,
          aggregateID: event.aggregate_id,
          seq: event.seq,
          type: event.type,
          data: event.data,
        })
        expect(yield* fixture.read).toEqual(snapshots[index])
      }
    }),
  )

  it.effect("uses JSON serialization for unknown tool results", () =>
    Effect.gen(function* () {
      const fixture = yield* setup("serialization")
      const base = { sessionID: fixture.sessionID, assistantMessageID: fixture.assistantMessageID, callID: "call" }
      yield* fixture.events.publish(SessionEvent.Tool.Input.Started, { ...base, name: "bash", timestamp: at(1) })
      yield* fixture.events.publish(SessionEvent.Tool.Called, {
        ...base,
        tool: "bash",
        input: {},
        provider: { executed: false },
        timestamp: at(2),
      })
      yield* fixture.events.publish(SessionEvent.Tool.Success, {
        ...base,
        structured: {},
        content: [],
        provider: { executed: false },
        result: { value: "raw", toJSON: () => ({ value: "serialized" }) },
        timestamp: at(3),
      })
      expect(tool(yield* fixture.read, "call")?.state).toMatchObject({
        status: "completed",
        result: { value: "serialized" },
      })
    }),
  )
})

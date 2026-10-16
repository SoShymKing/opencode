import { describe, expect } from "bun:test"
import { eq } from "drizzle-orm"
import { Effect, Schema } from "effect"
import { Event } from "@opencode/schema/event"
import { Money } from "@opencode/schema/money"
import { Session } from "@opencode/schema/session"
import { SessionEvent } from "@opencode/core/session/event"
import { SessionMessagePartTable, SessionMessageTable, SessionTable } from "@opencode/core/session/sql"
import { hydrateSelection } from "@opencode/core/session/message-storage"
import { at, contentBase, it, setup, text, tool } from "./session-projector-content.fixture"

const decodeJson = Schema.decodeUnknownSync(Schema.fromJsonString(Schema.Unknown))

describe("SessionProjector content rows", () => {
  it.effect("addresses text and reasoning by per-kind ordinal, not mixed position", () =>
    Effect.gen(function* () {
      const fixture = yield* setup("ordinals")
      const base = contentBase(fixture)
      yield* fixture.events.publish(SessionEvent.Reasoning.Started, { ...base, ordinal: 0 })
      yield* fixture.events.publish(SessionEvent.Text.Started, { ...base, ordinal: 0 })
      yield* fixture.events.publish(SessionEvent.Text.Started, { ...base, ordinal: 1 })
      const before = yield* fixture.children
      yield* fixture.events.publish(SessionEvent.Text.Ended, { ...base, ordinal: 0, text: "first" })
      const after = yield* fixture.children
      expect(after[0]).toEqual(before[0])
      expect(after[2]).toEqual(before[2])
      expect(decodeJson(after[1]?.dataText ?? "")).toEqual({ text: "first" })
      expect(after.map((row) => [row.position, row.type, row.id])).toEqual([
        [0, "reasoning", "0"], [1, "text", "0"], [2, "text", "1"],
      ])
    }),
  )

  it.effect("updates metadata without reading or replacing a malformed child", () =>
    Effect.gen(function* () {
      const fixture = yield* setup("metadata")
      const base = contentBase(fixture)
      yield* fixture.events.publish(SessionEvent.Text.Started, { ...base, ordinal: 0 })
      yield* fixture.corruptChild(0, JSON.stringify({ text: 42 }))
      const before = yield* fixture.children
      yield* fixture.events.publish(SessionEvent.RetryScheduled, {
        ...base, attempt: 1, at: 100, error: { type: "unknown", message: "retry" },
      })
      yield* fixture.events.publish(SessionEvent.Step.Ended, {
        ...base, finish: "stop", streamEventCount: 12, cost: Money.USD.make(0),
        tokens: { input: 0, output: 0, reasoning: 0, cache: { read: 0, write: 0 } },
      })
      expect(yield* fixture.children).toEqual(before)
      expect(decodeJson(yield* fixture.envelopeText)).toMatchObject({ streamEventCount: 12, finish: "stop" })
    }),
  )

  it.effect("replaces child content in bulk and forks settled children with new parent IDs", () =>
    Effect.gen(function* () {
      const fixture = yield* setup("fork")
      const base = contentBase(fixture)
      yield* fixture.events.publish(SessionEvent.Text.Started, { ...base, ordinal: 0 })
      const before = yield* fixture.envelopeText
      yield* fixture.events.publish(SessionEvent.MessageContentUpdated, {
        sessionID: fixture.sessionID, messageID: fixture.assistantMessageID,
        content: [{ type: "text", text: "replacement" }, { type: "text", text: "second" }],
      })
      expect(yield* fixture.envelopeText).toBe(before)
      expect((yield* fixture.children).map((row) => [row.position, row.id])).toEqual([[0, "0"], [1, "1"]])
      yield* fixture.events.publish(SessionEvent.Step.Ended, {
        ...base, finish: "stop", cost: Money.USD.make(0),
        tokens: { input: 0, output: 0, reasoning: 0, cache: { read: 0, write: 0 } },
      })
      const forkID = Session.ID.make("ses_projector_content_fork_copy")
      yield* fixture.events.publish(SessionEvent.Forked, {
        sessionID: forkID, parentID: fixture.sessionID,
        boundary: { type: "through", messageID: fixture.assistantMessageID },
      })
      const copied = yield* hydrateSelection({ db: fixture.db, where: eq(SessionMessageTable.session_id, forkID) })
      expect(copied).toHaveLength(1)
      expect(copied[0]?.message.id).not.toBe(fixture.assistantMessageID)
      expect(copied[0]?.message).toMatchObject({ content: [{ type: "text", text: "replacement" }, { type: "text", text: "second" }] })
    }),
  )

  it.effect("archives and restores without adding a history message", () =>
    Effect.gen(function* () {
      const fixture = yield* setup("archive")
      const before = yield* fixture.envelopeText
      yield* fixture.events.publish(SessionEvent.Archived, { sessionID: fixture.sessionID })
      expect((yield* fixture.db.select().from(SessionTable).where(eq(SessionTable.id, fixture.sessionID)).get())?.time_archived).toBe(0)
      yield* fixture.events.publish(SessionEvent.Restored, { sessionID: fixture.sessionID })
      expect((yield* fixture.db.select().from(SessionTable).where(eq(SessionTable.id, fixture.sessionID)).get())?.time_archived).toBeNull()
      expect(yield* fixture.db.select().from(SessionMessageTable).where(eq(SessionMessageTable.session_id, fixture.sessionID)).all()).toHaveLength(1)
      expect(yield* fixture.envelopeText).toBe(before)
    }),
  )

  it.effect("projects mixed text, reasoning, and tool lifecycles into ordered child rows", () =>
    Effect.gen(function* () {
      const fixture = yield* setup("mixed")
      const base = contentBase(fixture)
      const envelopeBefore = yield* fixture.envelopeText

      yield* fixture.events.publish(SessionEvent.Text.Started, { ...base, ordinal: 0 })
      yield* fixture.events.publish(SessionEvent.Text.Ended, {
        ...base,
        ordinal: 0,
        text: "answer",
      })
      yield* fixture.events.publish(SessionEvent.Reasoning.Started, {
        ...base,
        ordinal: 0,
        state: { test: { start: true } },
      })
      yield* fixture.events.publish(SessionEvent.Reasoning.Ended, {
        ...base,
        ordinal: 0,
        text: "because",
        state: { test: { end: true } },
      })
      yield* fixture.events.publish(SessionEvent.Tool.Input.Started, {
        ...base,
        id: "tool",
        name: "bash",
      })
      yield* fixture.events.publish(SessionEvent.Tool.Input.Ended, {
        ...base,
        id: "tool",
        text: "raw",
      })
      yield* fixture.events.publish(SessionEvent.Tool.Called, {
        ...base,
        id: "tool",
        input: { command: "pwd" },
        executed: true,
        state: { test: { call: true } },
      })
      yield* fixture.events.publish(SessionEvent.Tool.Progress, {
        ...base,
        id: "tool",
        metadata: { phase: "running" },
      })
      yield* fixture.events.publish(SessionEvent.Tool.Success, {
        ...base,
        id: "tool",
        metadata: { phase: "done" },
        content: text("complete"),
        executed: false,
        resultState: { test: { result: true } },
      })

      const assistant = yield* fixture.read
      expect(assistant.content).toMatchObject([
        { type: "text", text: "answer" },
        {
          type: "reasoning",
          text: "because",
          state: { test: { end: true } },
          time: { created: at(0), completed: at(0) },
        },
        {
          type: "tool",
          id: "tool",
          executed: true,
          providerState: { test: { call: true } },
          providerResultState: { test: { result: true } },
          state: {
            status: "completed",
            input: { command: "pwd" },
            metadata: { phase: "done" },
            content: text("complete"),
          },
        },
      ])
      expect((yield* fixture.children).map((row) => row.position)).toEqual([0, 1, 2])
      expect(yield* fixture.envelopeText).toBe(envelopeBefore)
      expect(decodeJson(envelopeBefore)).not.toHaveProperty("content")
      expect((yield* fixture.storedEvents).some((event) => event.type.startsWith("session.tool.progress"))).toBe(false)
    }),
  )

  it.effect("updates only the greatest duplicate position and keeps retries and stale events inert", () =>
    Effect.gen(function* () {
      const fixture = yield* setup("bounded")
      const base = contentBase(fixture)
      yield* fixture.events.publish(SessionEvent.Text.Started, { ...base, ordinal: 0 })
      const retryID = Event.ID.make("evt_projector_content_exact_retry")
      yield* fixture.events.publish(
        SessionEvent.Text.Started,
        { ...base, ordinal: 0 },
        { id: retryID },
      )
      const retry = (yield* fixture.serializedEvents).find((event) => event.id === retryID)
      if (!retry) return yield* Effect.die("Missing exact retry event")
      yield* fixture.events.replay(retry)
      expect(yield* fixture.children).toHaveLength(2)

      const before = yield* fixture.children
      yield* fixture.events.publish(SessionEvent.Text.Ended, {
        ...base,
        ordinal: 0,
        text: "latest",
      })
      const after = yield* fixture.children
      expect(after[0]).toEqual(before[0])
      expect(decodeJson(after[1]?.dataText ?? "")).toEqual({ text: "latest" })

      yield* fixture.events.publish(SessionEvent.Tool.Input.Started, {
        ...base,
        id: "stale",
        name: "bash",
      })
      yield* fixture.events.publish(SessionEvent.Tool.Failed, {
        ...base,
        id: "stale",
        error: { type: "unknown", message: "failed" },
        executed: false,
      })
      const settled = (yield* fixture.children).at(-1)
      yield* fixture.events.publish(SessionEvent.Tool.Progress, {
        ...base,
        id: "stale",
        metadata: { phase: "ignored" },
      })
      expect((yield* fixture.children).at(-1)).toEqual(settled)
      expect(tool(yield* fixture.read, "stale")?.state.status).toBe("error")
    }),
  )

  it.effect("replays the same envelope and ordered children from an empty projection", () =>
    Effect.gen(function* () {
      const fixture = yield* setup("replay")
      const base = contentBase(fixture)
      yield* fixture.events.publish(SessionEvent.Text.Started, { ...base, ordinal: 0 })
      yield* fixture.events.publish(SessionEvent.Text.Ended, {
        ...base,
        ordinal: 0,
        text: "done",
      })
      yield* fixture.events.publish(SessionEvent.Reasoning.Started, {
        ...base,
        ordinal: 0,
      })
      const expected = yield* fixture.read
      const expectedRows = yield* fixture.children
      const recorded = yield* fixture.serializedEvents

      yield* fixture.events.remove(fixture.sessionID)
      yield* fixture.db
        .delete(SessionMessageTable)
        .where(eq(SessionMessageTable.id, fixture.assistantMessageID))
        .run()
        .pipe(Effect.orDie)
      expect(
        yield* fixture.db
          .select()
          .from(SessionMessagePartTable)
          .where(eq(SessionMessagePartTable.message_id, fixture.assistantMessageID))
          .all()
          .pipe(Effect.orDie),
      ).toEqual([])

      yield* Effect.forEach(recorded, (event) => fixture.events.replay(event), { discard: true })
      expect(yield* fixture.read).toEqual(expected)
      expect(yield* fixture.children).toEqual(expectedRows)
    }),
  )
})

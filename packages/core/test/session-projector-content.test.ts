import { describe, expect } from "bun:test"
import { eq } from "drizzle-orm"
import { Effect, Schema } from "effect"
import { EventV2 } from "@opencode-ai/core/event"
import { SessionEvent } from "@opencode-ai/core/session/event"
import { SessionMessagePartTable, SessionMessageTable } from "@opencode-ai/core/session/sql"
import { at, contentBase, it, setup, text, tool } from "./session-projector-content.fixture"

const decodeJson = Schema.decodeUnknownSync(Schema.UnknownFromJsonString)

describe("SessionProjector content rows", () => {
  it.effect("projects mixed text, reasoning, and tool lifecycles into ordered child rows", () =>
    Effect.gen(function* () {
      const fixture = yield* setup("mixed")
      const base = contentBase(fixture)
      const envelopeBefore = yield* fixture.envelopeText

      yield* fixture.events.publish(SessionEvent.Text.Started, { ...base, textID: "text", timestamp: at(1) })
      yield* fixture.events.publish(SessionEvent.Text.Ended, {
        ...base,
        textID: "text",
        text: "answer",
        timestamp: at(2),
      })
      yield* fixture.events.publish(SessionEvent.Reasoning.Started, {
        ...base,
        reasoningID: "reasoning",
        providerMetadata: { test: { start: true } },
        timestamp: at(3),
      })
      yield* fixture.events.publish(SessionEvent.Reasoning.Ended, {
        ...base,
        reasoningID: "reasoning",
        text: "because",
        providerMetadata: { test: { end: true } },
        timestamp: at(4),
      })
      yield* fixture.events.publish(SessionEvent.Tool.Input.Started, {
        ...base,
        callID: "tool",
        name: "bash",
        timestamp: at(5),
      })
      yield* fixture.events.publish(SessionEvent.Tool.Input.Ended, {
        ...base,
        callID: "tool",
        text: "raw",
        timestamp: at(6),
      })
      yield* fixture.events.publish(SessionEvent.Tool.Called, {
        ...base,
        callID: "tool",
        tool: "bash",
        input: { command: "pwd" },
        provider: { executed: true, metadata: { test: { call: true } } },
        timestamp: at(7),
      })
      yield* fixture.events.publish(SessionEvent.Tool.Progress, {
        ...base,
        callID: "tool",
        structured: { phase: "running" },
        content: text("working"),
        timestamp: at(8),
      })
      yield* fixture.events.publish(SessionEvent.Tool.Success, {
        ...base,
        callID: "tool",
        structured: { phase: "done" },
        content: text("complete"),
        outputPaths: ["out"],
        provider: { executed: false, metadata: { test: { result: true } } },
        result: { value: "raw", toJSON: () => ({ value: "serialized" }) },
        timestamp: at(9),
      })

      const assistant = yield* fixture.read
      expect(assistant.content).toMatchObject([
        { type: "text", id: "text", text: "answer" },
        {
          type: "reasoning",
          id: "reasoning",
          text: "because",
          providerMetadata: { test: { end: true } },
          time: { created: at(3), completed: at(4) },
        },
        {
          type: "tool",
          id: "tool",
          provider: {
            executed: true,
            metadata: { test: { call: true } },
            resultMetadata: { test: { result: true } },
          },
          state: {
            status: "completed",
            input: { command: "pwd" },
            structured: { phase: "done" },
            content: text("complete"),
            outputPaths: ["out"],
            result: { value: "serialized" },
          },
        },
      ])
      expect((yield* fixture.children).map((row) => row.position)).toEqual([0, 1, 2])
      expect(yield* fixture.envelopeText).toBe(envelopeBefore)
      expect(decodeJson(envelopeBefore)).not.toHaveProperty("content")
    }),
  )

  it.effect("updates only the greatest duplicate position and keeps retries and stale events inert", () =>
    Effect.gen(function* () {
      const fixture = yield* setup("bounded")
      const base = contentBase(fixture)
      yield* fixture.events.publish(SessionEvent.Text.Started, { ...base, textID: "same", timestamp: at(1) })
      const retryID = EventV2.ID.make("evt_projector_content_exact_retry")
      yield* fixture.events.publish(
        SessionEvent.Text.Started,
        { ...base, textID: "same", timestamp: at(2) },
        { id: retryID },
      )
      const retry = (yield* fixture.serializedEvents).find((event) => event.id === retryID)
      if (!retry) return yield* Effect.die("Missing exact retry event")
      yield* fixture.events.replay(retry)
      expect(yield* fixture.children).toHaveLength(2)

      const before = yield* fixture.children
      yield* fixture.events.publish(SessionEvent.Text.Ended, {
        ...base,
        textID: "same",
        text: "latest",
        timestamp: at(3),
      })
      const after = yield* fixture.children
      expect(after[0]).toEqual(before[0])
      expect(decodeJson(after[1]?.dataText ?? "")).toEqual({ text: "latest" })

      yield* fixture.events.publish(SessionEvent.Tool.Input.Started, {
        ...base,
        callID: "stale",
        name: "bash",
        timestamp: at(4),
      })
      yield* fixture.events.publish(SessionEvent.Tool.Failed, {
        ...base,
        callID: "stale",
        error: { type: "unknown", message: "failed" },
        provider: { executed: false },
        timestamp: at(5),
      })
      const settled = (yield* fixture.children).at(-1)
      yield* fixture.events.publish(SessionEvent.Tool.Progress, {
        ...base,
        callID: "stale",
        structured: { phase: "ignored" },
        content: [],
        timestamp: at(6),
      })
      expect((yield* fixture.children).at(-1)).toEqual(settled)
      expect(tool(yield* fixture.read, "stale")?.state.status).toBe("error")
    }),
  )

  it.effect("replays the same envelope and ordered children from an empty projection", () =>
    Effect.gen(function* () {
      const fixture = yield* setup("replay")
      const base = contentBase(fixture)
      yield* fixture.events.publish(SessionEvent.Text.Started, { ...base, textID: "text", timestamp: at(1) })
      yield* fixture.events.publish(SessionEvent.Text.Ended, {
        ...base,
        textID: "text",
        text: "done",
        timestamp: at(2),
      })
      yield* fixture.events.publish(SessionEvent.Reasoning.Started, {
        ...base,
        reasoningID: "reasoning",
        timestamp: at(3),
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

      yield* fixture.events.replayAll(recorded)
      expect(yield* fixture.read).toEqual(expected)
      expect(yield* fixture.children).toEqual(expectedRows)
    }),
  )
})

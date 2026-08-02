import { describe, expect } from "bun:test"
import { eq, sql } from "drizzle-orm"
import { Effect, Layer, Schema } from "effect"
import { Database } from "@opencode-ai/core/database/database"
import { AppNodeBuilder } from "@opencode-ai/core/effect/app-node-builder"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { EventV2 } from "@opencode-ai/core/event"
import { Location } from "@opencode-ai/core/location"
import { ProjectV2 } from "@opencode-ai/core/project"
import { ProjectTable } from "@opencode-ai/core/project/sql"
import { AbsolutePath } from "@opencode-ai/core/schema"
import { SessionV2 } from "@opencode-ai/core/session"
import { MessageDecodeError } from "@opencode-ai/core/session/error"
import { SessionExecution } from "@opencode-ai/core/session/execution"
import { SessionMessage } from "@opencode-ai/core/session/message"
import { SessionHistory } from "@opencode-ai/core/session/history"
import { SessionProjector } from "@opencode-ai/core/session/projector"
import { SessionStore } from "@opencode-ai/core/session/store"
import { SessionMessagePartTable, SessionMessageTable, SessionTable } from "@opencode-ai/core/session/sql"
import { ModelV2 } from "@opencode-ai/core/model"
import { ProviderV2 } from "@opencode-ai/core/provider"
import { testEffect } from "./lib/effect"

const projects = Layer.succeed(
  ProjectV2.Service,
  ProjectV2.Service.of({
    resolve: (directory) => Effect.succeed({ id: ProjectV2.ID.global, directory }),
    directories: () => Effect.succeed([]),
    commit: () => Effect.void,
  }),
)
const it = testEffect(
  AppNodeBuilder.build(
    LayerNode.group([Database.node, EventV2.node, SessionProjector.node, SessionStore.node, SessionV2.node]),
    [
      [ProjectV2.node, projects],
      [SessionExecution.node, SessionExecution.noopLayer],
    ],
  ),
)
const location = Location.Ref.make({ directory: AbsolutePath.make("/project") })

const GapEvent = EventV2.define({
  type: "test.session.history.gap",
  durable: { aggregate: "sessionID", version: 1 },
  schema: { sessionID: SessionV2.ID, value: Schema.String },
})

describe("SessionV2.history", () => {
  it.effect("returns an exhausted page for a migrated Session with no event sequence", () =>
    Effect.gen(function* () {
      const db = (yield* Database.Service).db
      const session = yield* SessionV2.Service
      const sessionID = SessionV2.ID.make("ses_empty_history")
      yield* db
        .insert(ProjectTable)
        .values({ id: ProjectV2.ID.global, worktree: AbsolutePath.make("/project"), sandboxes: [] })
        .onConflictDoNothing()
        .run()
      yield* db
        .insert(SessionTable)
        .values({
          id: sessionID,
          project_id: ProjectV2.ID.global,
          slug: "empty-history",
          directory: "/project",
          title: "Empty history",
          version: "test",
        })
        .run()

      const first = yield* session.history({ sessionID, limit: 10 })

      expect(first).toEqual({ events: [], hasMore: false })
    }),
  )

  it.effect("treats after as an exclusive aggregate sequence", () =>
    Effect.gen(function* () {
      const session = yield* SessionV2.Service
      const created = yield* session.create({ location })
      yield* session.switchAgent({ sessionID: created.id, agent: "one" })
      yield* session.switchAgent({ sessionID: created.id, agent: "two" })

      const page = yield* session.history({ sessionID: created.id, after: 1, limit: 10 })

      expect(page.events.map((event) => event.durable?.seq)).toEqual([2])
      expect(page.hasMore).toBe(false)
    }),
  )

  it.effect("paginates public events in aggregate order across filtered gaps without duplicates", () =>
    Effect.gen(function* () {
      const session = yield* SessionV2.Service
      const events = yield* EventV2.Service
      const created = yield* session.create({ location })
      yield* session.switchAgent({ sessionID: created.id, agent: "one" })
      yield* events.publish(GapEvent, { sessionID: created.id, value: "filtered" })
      yield* session.switchAgent({ sessionID: created.id, agent: "two" })
      yield* session.switchAgent({ sessionID: created.id, agent: "three" })

      const first = yield* session.history({ sessionID: created.id, limit: 2 })
      const after = first.events.at(-1)?.durable?.seq
      const second = yield* session.history({
        sessionID: created.id,
        after,
        limit: 2,
      })
      const sequence = [...first.events, ...second.events].map((event) => event.durable?.seq)

      expect(first.hasMore).toBe(true)
      expect(second.hasMore).toBe(false)
      expect(sequence).toEqual([1, 3, 4])
      expect(new Set(sequence).size).toBe(sequence.length)
    }),
  )

  it.effect("includes events committed between pages", () =>
    Effect.gen(function* () {
      const session = yield* SessionV2.Service
      const created = yield* session.create({ location })
      yield* session.switchAgent({ sessionID: created.id, agent: "one" })
      yield* session.switchAgent({ sessionID: created.id, agent: "two" })

      const first = yield* session.history({ sessionID: created.id, limit: 1 })
      yield* session.switchAgent({ sessionID: created.id, agent: "later" })
      const second = yield* session.history({
        sessionID: created.id,
        after: first.events.at(-1)?.durable?.seq,
        limit: 10,
      })

      expect(first.hasMore).toBe(true)
      expect([...first.events, ...second.events].map((event) => event.durable?.seq)).toEqual([1, 2, 3])
      expect(second.hasMore).toBe(false)
    }),
  )

  it.effect("reports exhaustion for exact-limit and limit-plus-one pages", () =>
    Effect.gen(function* () {
      const session = yield* SessionV2.Service
      const created = yield* session.create({ location })
      yield* session.switchAgent({ sessionID: created.id, agent: "one" })
      yield* session.switchAgent({ sessionID: created.id, agent: "two" })

      const exact = yield* session.history({ sessionID: created.id, limit: 2 })
      const oneMore = yield* session.history({ sessionID: created.id, limit: 1 })
      const exhausted = yield* session.history({
        sessionID: created.id,
        after: oneMore.events.at(-1)?.durable?.seq,
        limit: 1,
      })

      expect(exact.events).toHaveLength(2)
      expect(exact.hasMore).toBe(false)
      expect(oneMore.events).toHaveLength(1)
      expect(oneMore.hasMore).toBe(true)
      expect(exhausted.events).toHaveLength(1)
      expect(exhausted.hasMore).toBe(false)
    }),
  )

  it.effect("fails with NotFoundError for a missing Session", () =>
    Effect.gen(function* () {
      const session = yield* SessionV2.Service
      const error = yield* session.history({ sessionID: SessionV2.ID.make("ses_missing"), limit: 10 }).pipe(Effect.flip)

      expect(error._tag).toBe("Session.NotFoundError")
    }),
  )

  it.effect("reports public message ownership when stored data cannot be decoded", () =>
    Effect.gen(function* () {
      const db = (yield* Database.Service).db
      const session = yield* SessionV2.Service
      const created = yield* session.create({ location })
      const messageID = SessionMessage.ID.make("msg_malformed")
      yield* db
        .run(
          sql`INSERT INTO session_message (id, session_id, type, seq, time_created, time_updated, data)
          VALUES (${messageID}, ${created.id}, 'assistant', 0, 0, 0, ${"{}"})`,
        )
        .pipe(Effect.orDie)

      const error = yield* session.messages({ sessionID: created.id }).pipe(Effect.flip)

      expect(error).toEqual(new MessageDecodeError({ sessionID: created.id, messageID }))
    }),
  )

  it.effect("recovers interrupted tools after compaction in parent and child order", () =>
    Effect.gen(function* () {
      const db = (yield* Database.Service).db
      const session = yield* SessionV2.Service
      const created = yield* session.create({ location })
      const model = { id: ModelV2.ID.make("history"), providerID: ProviderV2.ID.make("history") }
      const olderID = SessionMessage.ID.make("msg_interrupted_older")
      const compactionID = SessionMessage.ID.make("msg_interrupted_compaction")
      const firstID = SessionMessage.ID.make("msg_interrupted_first")
      const secondID = SessionMessage.ID.make("msg_interrupted_second")
      yield* Effect.forEach(
        [
          {
            id: olderID,
            type: "assistant",
            seq: 10,
            data: { agent: "build", model, time: { created: 10 } },
          },
          {
            id: compactionID,
            type: "compaction",
            seq: 11,
            data: { reason: "manual", summary: "summary", recent: "recent", time: { created: 11 } },
          },
          {
            id: firstID,
            type: "assistant",
            seq: 12,
            data: { agent: "build", model, time: { created: 12 } },
          },
          {
            id: secondID,
            type: "assistant",
            seq: 13,
            data: { agent: "build", model, time: { created: 13 } },
          },
        ] as const,
        (row) =>
          db.run(sql`
            INSERT INTO session_message (id, session_id, type, seq, time_created, time_updated, data)
            VALUES (${row.id}, ${created.id}, ${row.type}, ${row.seq}, ${row.seq}, ${row.seq}, ${JSON.stringify(row.data)})
          `),
        { discard: true },
      )
      yield* db
        .insert(SessionMessagePartTable)
        .values([
          {
            message_id: olderID,
            position: 0,
            id: "tool-before-compaction",
            type: "tool",
            data: { name: "bash", state: { status: "pending", input: "" }, time: { created: 10 } },
          },
          {
            message_id: firstID,
            position: 1,
            id: "tool-running",
            type: "tool",
            data: {
              name: "bash",
              state: { status: "running", input: {}, structured: {}, content: [] },
              time: { created: 12 },
            },
          },
          {
            message_id: firstID,
            position: 3,
            id: "tool-pending",
            type: "tool",
            data: { name: "bash", state: { status: "pending", input: "" }, time: { created: 12 } },
          },
          {
            message_id: secondID,
            position: 0,
            id: "tool-second-parent",
            type: "tool",
            data: { name: "bash", state: { status: "pending", input: "" }, time: { created: 13 } },
          },
        ])
        .run()
        .pipe(Effect.orDie)
      yield* db.run(sql`
        INSERT INTO session_message_part (message_id, position, id, type, data)
        VALUES (${firstID}, 2, 'corrupt-unrelated-text', 'text', '{}')
      `)

      const interrupted = yield* SessionHistory.interruptedTools(db, created.id)

      expect(
        interrupted.map((item) => ({
          assistantMessageID: item.assistantMessageID,
          id: item.tool.id,
          status: item.tool.state.status,
        })),
      ).toEqual([
        { assistantMessageID: firstID, id: "tool-running", status: "running" },
        { assistantMessageID: firstID, id: "tool-pending", status: "pending" },
        { assistantMessageID: secondID, id: "tool-second-parent", status: "pending" },
      ])
      expect(
        yield* db
          .select({ id: SessionMessagePartTable.id })
          .from(SessionMessagePartTable)
          .where(eq(SessionMessagePartTable.message_id, olderID))
          .all()
          .pipe(Effect.orDie),
      ).toEqual([{ id: "tool-before-compaction" }])
    }),
  )
})

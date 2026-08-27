import { afterAll, describe, expect } from "bun:test"
import { Database as NativeDatabase } from "bun:sqlite"
import { mkdtemp, rm } from "node:fs/promises"
import { tmpdir } from "node:os"
import { join } from "node:path"
import { EffectCache } from "drizzle-orm/cache/core/cache-effect"
import { EffectLogger } from "drizzle-orm/effect-core"
import { and, asc, desc, eq, gt, lt, sql } from "drizzle-orm"
import { Deferred, Effect, Fiber, Layer, Schema } from "effect"
import { EffectDrizzleSqlite } from "@opencode-ai/effect-drizzle-sqlite"
import { Database } from "@opencode-ai/core/database/database"
import { DatabaseMigration } from "@opencode-ai/core/database/migration"
import { AppNodeBuilder } from "@opencode-ai/core/effect/app-node-builder"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { AbsolutePath } from "@opencode-ai/core/schema"
import { MessageDecodeError } from "@opencode-ai/core/session/error"
import { decodeChild, decodeEnvelope, hydrateSelection } from "@opencode-ai/core/session/message-storage"
import { SessionMessage } from "@opencode-ai/core/session/message"
import { SessionSchema } from "@opencode-ai/core/session/schema"
import { SessionMessagePartTable, SessionMessageTable, SessionTable } from "@opencode-ai/core/session/sql"
import { SessionStore } from "@opencode-ai/core/session/store"
import { ProjectV2 } from "@opencode-ai/core/project"
import { ProjectTable } from "@opencode-ai/core/project/sql"
import { layer as sqliteLayer } from "../src/database/sqlite.bun"
import { testEffect } from "./lib/effect"

type LoggedQuery = {
  readonly query: string
  readonly params: readonly unknown[]
}

type Probe = {
  active: boolean
  queries: LoggedQuery[]
  beforeChild?: () => Promise<void>
}

type PartRow = {
  readonly position: number
  readonly id: string
  readonly type: "text" | "reasoning" | "tool"
  readonly dataText: string
}

const temporary = await mkdtemp(join(tmpdir(), "opencode-message-storage-"))
const filename = join(temporary, "storage.sqlite")
const probe: Probe = { active: false, queries: [] }

const logger = Layer.succeed(
  EffectLogger,
  EffectLogger.of({
    logQuery: (query, params) =>
      Effect.gen(function* () {
        if (!probe.active) return
        probe.queries.push({ query, params })
        if (query.includes("session_message_part") && probe.beforeChild) {
          yield* Effect.promise(probe.beforeChild)
        }
      }),
  }),
)

const database = Layer.effect(
  Database.Service,
  Effect.gen(function* () {
    const db = yield* EffectDrizzleSqlite.make()
    yield* Effect.forEach(
      [
        "PRAGMA journal_mode = WAL",
        "PRAGMA synchronous = NORMAL",
        "PRAGMA busy_timeout = 5000",
        "PRAGMA foreign_keys = ON",
      ],
      (statement) => db.run(statement),
      { discard: true },
    )
    yield* DatabaseMigration.apply(db, filename)
    return Database.Service.of({ db })
  }),
).pipe(Layer.provide(sqliteLayer({ filename })), Layer.provide(EffectCache.Default), Layer.provide(logger))

const it = testEffect(database)
const storeIt = testEffect(
  AppNodeBuilder.build(LayerNode.group([Database.node, SessionStore.node]), [[Database.node, database]]),
)
const projectID = ProjectV2.ID.make("project-message-storage")
const directory = AbsolutePath.make("/message-storage")
const assistantEnvelope = JSON.stringify({
  agent: "build",
  model: { providerID: "test", id: "model" },
  time: { created: 1 },
})

function setupSession(sessionID: SessionSchema.ID) {
  return Effect.gen(function* () {
    const db = (yield* Database.Service).db
    yield* db
      .insert(ProjectTable)
      .values({ id: projectID, worktree: directory, sandboxes: [] })
      .onConflictDoNothing()
      .run()
    yield* db
      .insert(SessionTable)
      .values({
        id: sessionID,
        project_id: projectID,
        slug: sessionID,
        directory,
        title: sessionID,
        version: "test",
      })
      .run()
  })
}

function insertMessage(input: {
  readonly sessionID: SessionSchema.ID
  readonly messageID: SessionMessage.ID
  readonly seq: number
  readonly type: SessionMessage.Type
  readonly dataText: string
  readonly parts?: readonly PartRow[]
}) {
  return Effect.gen(function* () {
    const db = (yield* Database.Service).db
    yield* db.run(sql`
      INSERT INTO session_message (id, session_id, type, seq, time_created, time_updated, data)
      VALUES (${input.messageID}, ${input.sessionID}, ${input.type}, ${input.seq}, 1, 1, ${input.dataText})
    `)
    yield* Effect.forEach(
      input.parts ?? [],
      (part) =>
        db.run(sql`
          INSERT INTO session_message_part (message_id, position, id, type, data)
          VALUES (${input.messageID}, ${part.position}, ${part.id}, ${part.type}, ${part.dataText})
        `),
      { discard: true },
    )
  })
}

function selectedQueries() {
  return probe.queries.filter((entry) => /\bfrom "session_message(?:_part)?"/i.test(entry.query))
}

async function removeTemporary(retries = 30): Promise<void> {
  try {
    await rm(temporary, { recursive: true, force: true })
  } catch (error) {
    if (retries === 0 || !error || typeof error !== "object" || !("code" in error) || error.code !== "EBUSY") {
      throw error
    }
    Bun.gc(true)
    await Bun.sleep(100)
    return removeTemporary(retries - 1)
  }
}

afterAll(async () => {
  await removeTemporary()
})

describe("Session message storage", () => {
  it.effect("decodes normalized envelopes and children from relational ownership", () =>
    Effect.gen(function* () {
      const sessionID = SessionSchema.ID.make("ses_decode_complete")
      const messageID = SessionMessage.ID.make("msg_decode_complete")

      const envelope = yield* decodeEnvelope({ sessionID, messageID, type: "assistant", dataText: assistantEnvelope })
      const child = yield* decodeChild({
        sessionID,
        messageID,
        position: 4,
        id: "text-1",
        type: "text",
        dataText: JSON.stringify({ text: "hello" }),
      })

      expect(envelope).toMatchObject({ id: messageID, type: "assistant", content: [] })
      expect(child).toEqual({ id: "text-1", type: "text", text: "hello" })
    }),
  )

  it.effect("maps malformed and stale raw data to selected relational IDs", () =>
    Effect.gen(function* () {
      const sessionID = SessionSchema.ID.make("ses_decode_error")
      const messageID = SessionMessage.ID.make("msg_decode_error")
      const expected = new MessageDecodeError({ sessionID, messageID })

      const malformedEnvelope = yield* decodeEnvelope({
        sessionID,
        messageID,
        type: "assistant",
        dataText: "{",
      }).pipe(Effect.flip)
      const staleEnvelope = yield* decodeEnvelope({
        sessionID,
        messageID,
        type: "assistant",
        dataText: JSON.stringify({
          agent: "build",
          model: { providerID: "test", id: "model" },
          content: [],
          time: { created: 1 },
        }),
      }).pipe(Effect.flip)
      const malformedChild = yield* decodeChild({
        sessionID,
        messageID,
        position: 0,
        id: "selected-id",
        type: "text",
        dataText: "[]",
      }).pipe(Effect.flip)
      const staleChild = yield* decodeChild({
        sessionID,
        messageID,
        position: 0,
        id: "selected-id",
        type: "text",
        dataText: JSON.stringify({ id: "embedded-id", text: "wrong owner" }),
      }).pipe(Effect.flip)

      expect(malformedEnvelope).toEqual(expected)
      expect(staleEnvelope).toEqual(expected)
      expect(malformedChild).toEqual(expected)
      expect(staleChild).toEqual(expected)
    }),
  )

  it.effect("hydrates complete messages in parent and child position order", () =>
    Effect.gen(function* () {
      const db = (yield* Database.Service).db
      const sessionID = SessionSchema.ID.make("ses_hydrate_order")
      const firstID = SessionMessage.ID.make("msg_hydrate_assistant_first")
      const secondID = SessionMessage.ID.make("msg_hydrate_assistant_second")
      yield* setupSession(sessionID)
      yield* insertMessage({
        sessionID,
        messageID: firstID,
        seq: 1,
        type: "assistant",
        dataText: assistantEnvelope,
        parts: [{ position: 7, id: "first-only", type: "text", dataText: JSON.stringify({ text: "first owner" }) }],
      })
      yield* insertMessage({
        sessionID,
        messageID: secondID,
        seq: 2,
        type: "assistant",
        dataText: assistantEnvelope,
        parts: [
          { position: 8, id: "second", type: "text", dataText: JSON.stringify({ text: "second" }) },
          { position: 2, id: "first", type: "text", dataText: JSON.stringify({ text: "first" }) },
        ],
      })

      const ascending = yield* hydrateSelection({
        db,
        where: eq(SessionMessageTable.session_id, sessionID),
        order: "asc",
      })
      const descending = yield* hydrateSelection({
        db,
        where: eq(SessionMessageTable.session_id, sessionID),
        order: "desc",
      })

      expect(ascending.map((row) => row.message)).toMatchObject([
        { id: firstID, content: [{ id: "first-only", text: "first owner" }] },
        { id: secondID, content: [{ id: "first", text: "first" }, { id: "second", text: "second" }] },
      ])
      expect(descending.map((row) => row.message.id)).toEqual([secondID, firstID])
    }),
  )

  it.effect("folds ascending and descending cursor anchors into both statements", () =>
    Effect.gen(function* () {
      const db = (yield* Database.Service).db
      const sessionID = SessionSchema.ID.make("ses_hydrate_cursor")
      const ids = ["msg_cursor_1", "msg_cursor_2", "msg_cursor_3"].map((id) => SessionMessage.ID.make(id))
      yield* setupSession(sessionID)
      yield* Effect.forEach(
        ids,
        (messageID, index) =>
          insertMessage({
            sessionID,
            messageID,
            seq: index + 1,
            type: "user",
            dataText: JSON.stringify({ text: String(index + 1), time: { created: index + 1 } }),
          }),
        { discard: true },
      )
      const anchor = db
        .select({ seq: SessionMessageTable.seq })
        .from(SessionMessageTable)
        .where(and(eq(SessionMessageTable.session_id, sessionID), eq(SessionMessageTable.id, ids[1])))
        .limit(1)

      const next = yield* hydrateSelection({
        db,
        where: and(eq(SessionMessageTable.session_id, sessionID), gt(SessionMessageTable.seq, anchor)),
        order: "asc",
      })
      const previous = yield* hydrateSelection({
        db,
        where: and(eq(SessionMessageTable.session_id, sessionID), lt(SessionMessageTable.seq, anchor)),
        order: "desc",
      })

      expect(next.map((row) => row.message.id)).toEqual([ids[2]])
      expect(previous.map((row) => row.message.id)).toEqual([ids[0]])
    }),
  )

  it.effect("always executes exactly two hydration statements", () =>
    Effect.gen(function* () {
      const db = (yield* Database.Service).db
      const sessionID = SessionSchema.ID.make("ses_hydrate_count")
      const messageID = SessionMessage.ID.make("msg_hydrate_count")
      yield* setupSession(sessionID)
      yield* insertMessage({
        sessionID,
        messageID,
        seq: 1,
        type: "assistant",
        dataText: assistantEnvelope,
        parts: [{ position: 0, id: "text", type: "text", dataText: JSON.stringify({ text: "value" }) }],
      })

      for (const where of [
        eq(SessionMessageTable.session_id, sessionID),
        eq(SessionMessageTable.id, messageID),
        eq(SessionMessageTable.id, SessionMessage.ID.make("msg_hydrate_missing")),
        eq(SessionMessageTable.session_id, SessionSchema.ID.make("ses_hydrate_empty")),
      ]) {
        probe.queries = []
        probe.active = true
        yield* hydrateSelection({ db, where, order: "asc", limit: 1 })
        probe.active = false
        expect(selectedQueries()).toHaveLength(2)
      }
    }),
  )

  storeIt.effect("keeps single-message missing and decode failures defect-only", () =>
    Effect.gen(function* () {
      const store = yield* SessionStore.Service
      const sessionID = SessionSchema.ID.make("ses_store_single")
      const messageID = SessionMessage.ID.make("msg_store_single")
      const malformedID = SessionMessage.ID.make("msg_store_malformed")
      yield* setupSession(sessionID)
      yield* insertMessage({ sessionID, messageID, seq: 1, type: "assistant", dataText: assistantEnvelope })
      yield* insertMessage({ sessionID, messageID: malformedID, seq: 2, type: "assistant", dataText: "{}" })

      for (const selected of [messageID, SessionMessage.ID.make("msg_store_missing")]) {
        probe.queries = []
        probe.active = true
        const result = yield* store.message(selected)
        probe.active = false
        expect(selectedQueries()).toHaveLength(2)
        expect(result?.message.id).toBe(selected === messageID ? messageID : undefined)
      }
      const defect = yield* store.message(malformedID).pipe(Effect.catchDefect((cause) => Effect.succeed(cause)))

      expect(defect).toEqual(new MessageDecodeError({ sessionID, messageID: malformedID }))
    }),
  )

  it.effect("rejects children owned by non-assistant parents", () =>
    Effect.gen(function* () {
      const db = (yield* Database.Service).db
      const sessionID = SessionSchema.ID.make("ses_child_user")
      const messageID = SessionMessage.ID.make("msg_child_user")
      yield* setupSession(sessionID)
      yield* insertMessage({
        sessionID,
        messageID,
        seq: 1,
        type: "user",
        dataText: JSON.stringify({ text: "question", time: { created: 1 } }),
        parts: [{ position: 0, id: "text", type: "text", dataText: JSON.stringify({ text: "invalid" }) }],
      })

      const error = yield* hydrateSelection({ db, where: eq(SessionMessageTable.id, messageID) }).pipe(Effect.flip)

      expect(error).toEqual(new MessageDecodeError({ sessionID, messageID }))
    }),
  )

  it.effect("uses parent and child indexes without temporary sorting", () =>
    Effect.gen(function* () {
      const db = (yield* Database.Service).db
      const sessionID = SessionSchema.ID.make("ses_hydrate_plan")
      yield* setupSession(sessionID)
      yield* insertMessage({
        sessionID,
        messageID: SessionMessage.ID.make("msg_hydrate_plan"),
        seq: 1,
        type: "assistant",
        dataText: assistantEnvelope,
        parts: [{ position: 0, id: "text", type: "text", dataText: JSON.stringify({ text: "value" }) }],
      })
      probe.queries = []
      probe.active = true
      yield* hydrateSelection({
        db,
        where: eq(SessionMessageTable.session_id, sessionID),
        order: "asc",
        limit: 10,
      })
      probe.active = false

      const decodeBindings = Schema.decodeUnknownSync(Schema.Array(Schema.Union([Schema.String, Schema.Number])))
      const decodePlan = Schema.decodeUnknownSync(Schema.Array(Schema.Struct({ detail: Schema.String })))
      const plans = yield* Effect.acquireUseRelease(
        Effect.sync(() => new NativeDatabase(filename, { readonly: true })),
        (native) =>
          Effect.sync(() =>
            selectedQueries().flatMap((entry) =>
              decodePlan(native.query(`EXPLAIN QUERY PLAN ${entry.query}`).all(...decodeBindings(entry.params))),
            ),
          ),
        (native) => Effect.sync(() => native.close()),
      )

      expect(plans.some((row) => row.detail.includes("session_message_session_seq_idx"))).toBe(true)
      expect(plans.some((row) => row.detail.includes("sqlite_autoindex_session_message_part_1"))).toBe(true)
      expect(plans.some((row) => row.detail.includes("TEMP B-TREE"))).toBe(false)
    }),
  )

  it.effect("keeps parent and child reads in one SQLite snapshot", () =>
    Effect.gen(function* () {
      const db = (yield* Database.Service).db
      const sessionID = SessionSchema.ID.make("ses_hydrate_snapshot")
      const messageID = SessionMessage.ID.make("msg_hydrate_snapshot")
      yield* setupSession(sessionID)
      yield* insertMessage({ sessionID, messageID, seq: 1, type: "assistant", dataText: assistantEnvelope })
      const childReached = yield* Deferred.make<void>()
      const writerCommitted = yield* Deferred.make<void>()
      probe.beforeChild = () =>
        Effect.runPromise(
          Deferred.succeed(childReached, undefined).pipe(Effect.andThen(Deferred.await(writerCommitted))),
        )
      probe.active = true
      const fiber = yield* hydrateSelection({ db, where: eq(SessionMessageTable.id, messageID) }).pipe(Effect.forkChild)
      yield* Deferred.await(childReached)

      const writer = new NativeDatabase(filename)
      writer.run("PRAGMA journal_mode = WAL")
      writer
        .query("INSERT INTO session_message_part (message_id, position, id, type, data) VALUES (?, ?, ?, ?, ?)")
        .run(messageID, 0, "committed", "text", JSON.stringify({ text: "after" }))
      writer.close()
      yield* Deferred.succeed(writerCommitted, undefined)
      const before = yield* Fiber.join(fiber)
      probe.beforeChild = undefined
      probe.active = false
      const after = yield* hydrateSelection({ db, where: eq(SessionMessageTable.id, messageID) })

      expect(before[0]?.message).toMatchObject({ content: [] })
      expect(after[0]?.message).toMatchObject({ content: [{ id: "committed", text: "after" }] })
    }),
  )

  it.effect("cascades children when a parent message is deleted", () =>
    Effect.gen(function* () {
      const db = (yield* Database.Service).db
      const sessionID = SessionSchema.ID.make("ses_parent_cascade")
      const messageID = SessionMessage.ID.make("msg_parent_cascade")
      yield* setupSession(sessionID)
      yield* insertMessage({
        sessionID,
        messageID,
        seq: 1,
        type: "assistant",
        dataText: assistantEnvelope,
        parts: [{ position: 0, id: "text", type: "text", dataText: JSON.stringify({ text: "value" }) }],
      })

      yield* db.delete(SessionMessageTable).where(eq(SessionMessageTable.id, messageID)).run().pipe(Effect.orDie)

      expect(
        yield* db
          .select()
          .from(SessionMessagePartTable)
          .where(eq(SessionMessagePartTable.message_id, messageID))
          .all()
          .pipe(Effect.orDie),
      ).toEqual([])
    }),
  )

  it.effect("cascades message children when a session is deleted", () =>
    Effect.gen(function* () {
      const db = (yield* Database.Service).db
      const sessionID = SessionSchema.ID.make("ses_session_cascade")
      const messageID = SessionMessage.ID.make("msg_session_cascade")
      yield* setupSession(sessionID)
      yield* insertMessage({
        sessionID,
        messageID,
        seq: 1,
        type: "assistant",
        dataText: assistantEnvelope,
        parts: [{ position: 0, id: "text", type: "text", dataText: JSON.stringify({ text: "value" }) }],
      })

      yield* db.delete(SessionTable).where(eq(SessionTable.id, sessionID)).run().pipe(Effect.orDie)

      expect(
        yield* db
          .select()
          .from(SessionMessagePartTable)
          .where(eq(SessionMessagePartTable.message_id, messageID))
          .all()
          .pipe(Effect.orDie),
      ).toEqual([])
    }),
  )
})

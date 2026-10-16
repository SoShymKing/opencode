import { describe, expect } from "bun:test"
import { eq, sql } from "drizzle-orm"
import { Effect, Schema } from "effect"
import { Bus } from "@opencode/core/bus"
import { Database } from "@opencode/core/database/database"
import { ProjectTable } from "@opencode/core/project/sql"
import { SessionProjector } from "@opencode/core/session/projector"
import { SessionStore } from "@opencode/core/session/store"
import { SessionMessagePartTable, SessionMessageTable, SessionTable } from "@opencode/core/session/sql"
import { Event } from "@opencode/schema/event"
import { Project } from "@opencode/schema/project"
import { AbsolutePath } from "@opencode/schema/schema"
import { Session } from "@opencode/schema/session"
import { SessionEvent } from "@opencode/schema/session-event"
import { SessionMessage } from "@opencode/schema/session-message"
import { LayerNode } from "@opencode/util/effect/layer-node"
import { testEffect } from "./lib/effect"

const it = testEffect(
  LayerNode.compile(LayerNode.group([Database.node, Bus.node, SessionProjector.node, SessionStore.node]), {
    replacements: [Bus.node.replace(Bus.configured({ persist: true }))],
  }),
)

const seedSessions = (rows: { id: string; updated: number }[]) =>
  Effect.gen(function* () {
    const database = yield* Database.Service
    const bus = yield* Bus.Service
    const directory = AbsolutePath.make("/project")
    yield* database.db.insert(ProjectTable).values({ id: Project.ID.global, worktree: directory, sandboxes: [] }).run()
    yield* Effect.forEach(rows, (row) =>
      Effect.gen(function* () {
        const sessionID = Session.ID.make(row.id)
        yield* bus.publish(SessionEvent.Created, {
          sessionID,
          projectID: Project.ID.global,
          location: { directory },
          slug: "store-test",
          version: "test",
        })
        yield* bus.replay({
          id: Event.ID.create(),
          created: row.updated,
          aggregateID: sessionID,
          seq: 1,
          type: Bus.versionedType(SessionEvent.Renamed.type, 1),
          data: { sessionID, title: row.id },
        })
      }),
    )
    return bus
  })

describe("SessionStore", () => {
  it.effect("keeps archive and period filters on both sides of a list cursor", () =>
    Effect.gen(function* () {
      yield* seedSessions([
        { id: "ses_old", updated: 10 },
        { id: "ses_boundary", updated: 20 },
        { id: "ses_new", updated: 30 },
        { id: "ses_archived", updated: 25 },
      ])
      const database = yield* Database.Service
      const store = yield* SessionStore.Service
      yield* database.db.update(SessionTable)
        .set({ time_archived: 40, time_updated: 25 })
        .where(eq(SessionTable.id, Session.ID.make("ses_archived"))).run()

      expect((yield* store.list({ updatedAfter: 20 })).map((session) => session.id)).toEqual([
        Session.ID.make("ses_new"), Session.ID.make("ses_boundary"),
      ])
      expect((yield* store.list({ archived: true, updatedAfter: 20 })).map((session) => session.id)).toEqual([
        Session.ID.make("ses_archived"),
      ])
      for (const filter of [{ includeArchived: true }, { includeArchived: true, archived: false }])
        expect((yield* store.list({ ...filter, updatedAfter: 20 })).map((session) => String(session.id))).toEqual([
          "ses_new", "ses_archived", "ses_boundary",
        ])
      expect((yield* store.list({ includeArchived: false })).map((session) => String(session.id))).toEqual([
        "ses_new", "ses_boundary", "ses_old",
      ])
      expect((yield* store.list({ includeArchived: true, archived: true })).map((session) => String(session.id))).toEqual([
        "ses_archived",
      ])
      expect((yield* store.list({ updatedAfter: 20, anchor: {
        id: Session.ID.make("ses_new"), time: 30, direction: "next",
      } })).map((session) => session.id)).toEqual([Session.ID.make("ses_boundary")])
      expect((yield* store.list({ archived: false, updatedAfter: 20, anchor: {
        id: Session.ID.make("ses_boundary"), time: 20, direction: "previous",
      } })).map((session) => session.id)).toEqual([Session.ID.make("ses_new")])
    }),
  )

  it.effect("hydrates only selected parents and ordered children for pages and single-message reads", () =>
    Effect.gen(function* () {
      const sessionID = Session.ID.make("ses_normalized")
      yield* seedSessions([{ id: sessionID, updated: 0 }])
      const database = yield* Database.Service
      const store = yield* SessionStore.Service
      yield* Effect.forEach([1, 2, 3], (seq) => database.db.run(sql`
        INSERT INTO session_message (id, session_id, type, seq, time_created, time_updated, data)
        VALUES (${`msg_normalized_${seq}`}, ${sessionID}, 'assistant', ${seq}, 0, 0,
          ${JSON.stringify({ agent: "build", model: { id: "test", providerID: "test" }, time: { created: 0 } })})
      `))
      yield* database.db.insert(SessionMessagePartTable).values([
        { message_id: SessionMessage.ID.make("msg_normalized_2"), position: 1, id: "text-1", type: "text", data: { text: "second" } },
        { message_id: SessionMessage.ID.make("msg_normalized_2"), position: 0, id: "text-0", type: "text", data: { text: "first" } },
      ]).run()
      yield* database.db.run(sql`
        INSERT INTO session_message_part (message_id, position, id, type, data)
        VALUES ('msg_normalized_3', 0, 'malformed', 'tool', '{}')
      `)

      const page = yield* store.messages({ sessionID, order: "asc", limit: 1,
        cursor: { id: SessionMessage.ID.make("msg_normalized_1"), direction: "next" } })
      expect(page).toEqual([Schema.decodeUnknownSync(SessionMessage.Info)({
        id: "msg_normalized_2", type: "assistant", agent: "build",
        model: { id: "test", providerID: "test" }, time: { created: 0 },
        content: [{ type: "text", text: "first" }, { type: "text", text: "second" }],
      })])
      expect(yield* store.message(SessionMessage.ID.make("msg_normalized_2"))).toEqual({ sessionID, message: page[0] })
      yield* database.db.delete(SessionMessageTable).where(eq(SessionMessageTable.id, SessionMessage.ID.make("msg_normalized_2"))).run()
      expect(yield* database.db.select().from(SessionMessagePartTable)
        .where(eq(SessionMessagePartTable.message_id, SessionMessage.ID.make("msg_normalized_2"))).all()).toEqual([])
    }),
  )

  it.effect("lists mixed archives by updated time and ID with exclusive two-item pages in either direction", () =>
    Effect.gen(function* () {
      yield* seedSessions([
        { id: "ses_d", updated: 20 },
        { id: "ses_z", updated: 10 },
        { id: "ses_a", updated: 30 },
        { id: "ses_c", updated: 20 },
        { id: "ses_y", updated: 10 },
        { id: "ses_e", updated: 30 },
        { id: "ses_b", updated: 20 },
      ])
      const store = yield* SessionStore.Service
      const database = yield* Database.Service
      yield* Effect.forEach(["ses_z", "ses_c", "ses_a"], (id) => database.db.update(SessionTable)
        .set({ time_archived: 40, time_updated: sql`${SessionTable.time_updated}` })
        .where(eq(SessionTable.id, Session.ID.make(id))).run())
      expect((yield* store.list({ includeArchived: true })).map((session) => String(session.id))).toEqual([
        "ses_e",
        "ses_a",
        "ses_d",
        "ses_c",
        "ses_b",
        "ses_z",
        "ses_y",
      ])
      expect((yield* store.list({ includeArchived: true, order: "asc" })).map((session) => String(session.id))).toEqual([
        "ses_y",
        "ses_z",
        "ses_b",
        "ses_c",
        "ses_d",
        "ses_a",
        "ses_e",
      ])
      const pages: { order: "asc" | "desc"; direction: "next" | "previous"; ids: string[] }[] = [
        { order: "asc", direction: "next", ids: ["ses_d", "ses_a"] },
        { order: "asc", direction: "previous", ids: ["ses_z", "ses_b"] },
        { order: "desc", direction: "next", ids: ["ses_b", "ses_z"] },
        { order: "desc", direction: "previous", ids: ["ses_a", "ses_d"] },
      ]
      yield* Effect.forEach(pages, (page) =>
        Effect.gen(function* () {
          const sessions = yield* store.list({
            includeArchived: true,
            order: page.order,
            limit: 2,
            anchor: { id: Session.ID.make("ses_c"), time: 20, direction: page.direction },
          })
          expect(sessions.map((session) => String(session.id))).toEqual(page.ids)
        }),
      )
    }),
  )

  it.effect("pages messages by durable sequence, not timestamp or ID, and scopes cursor lookup", () =>
    Effect.gen(function* () {
      const sessionID = Session.ID.make("ses_messages")
      const foreignID = Session.ID.make("ses_foreign")
      const bus = yield* seedSessions([
        { id: sessionID, updated: 0 },
        { id: foreignID, updated: 0 },
      ])
      const store = yield* SessionStore.Service
      yield* Effect.forEach(
        [
          { id: "evt_z", created: 300 },
          { id: "evt_b", created: 700 },
          { id: "evt_x", created: 100 },
          { id: "evt_c", created: 400 },
          { id: "evt_w", created: 200 },
          { id: "evt_a", created: 600 },
          { id: "evt_y", created: 500 },
        ],
        (event, index) =>
          bus.replay({
            id: Event.ID.make(event.id),
            created: event.created,
            aggregateID: sessionID,
            seq: index + 2,
            type: Bus.versionedType(SessionEvent.Synthetic.type, 1),
            data: { sessionID, text: event.id },
          }),
      )
      yield* bus.publish(
        SessionEvent.Synthetic,
        { sessionID: foreignID, text: "foreign" },
        {
          id: Event.ID.make("evt_foreign"),
        },
      )
      expect((yield* store.messages({ sessionID })).map((message) => String(message.id))).toEqual([
        "msg_y",
        "msg_a",
        "msg_w",
        "msg_c",
        "msg_x",
        "msg_b",
        "msg_z",
      ])
      expect((yield* store.messages({ sessionID, order: "asc" })).map((message) => String(message.id))).toEqual([
        "msg_z",
        "msg_b",
        "msg_x",
        "msg_c",
        "msg_w",
        "msg_a",
        "msg_y",
      ])
      const pages: { order: "asc" | "desc"; direction: "next" | "previous"; ids: string[] }[] = [
        { order: "asc", direction: "next", ids: ["msg_w", "msg_a"] },
        { order: "asc", direction: "previous", ids: ["msg_b", "msg_x"] },
        { order: "desc", direction: "next", ids: ["msg_x", "msg_b"] },
        { order: "desc", direction: "previous", ids: ["msg_a", "msg_w"] },
      ]
      yield* Effect.forEach(pages, (page) =>
        Effect.gen(function* () {
          const messages = yield* store.messages({
            sessionID,
            order: page.order,
            limit: 2,
            cursor: { id: SessionMessage.ID.make("msg_c"), direction: page.direction },
          })
          expect(messages.map((message) => String(message.id))).toEqual(page.ids)
        }),
      )
      expect(yield* store.messages({ sessionID: Session.ID.make("ses_missing") })).toEqual([])
      expect(
        yield* store.messages({
          sessionID,
          cursor: { id: SessionMessage.ID.make("msg_missing"), direction: "next" },
        }),
      ).toEqual([])
      expect(
        yield* store.messages({
          sessionID,
          order: "asc",
          cursor: { id: SessionMessage.ID.make("msg_foreign"), direction: "next" },
        }),
      ).toEqual([])
    }),
  )
})

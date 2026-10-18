import { expect, test } from "bun:test"
import { Database } from "bun:sqlite"
import { join } from "node:path"
import { sql } from "drizzle-orm"
import { Cause, Effect, Layer } from "effect"
import { LayerNode } from "@opencode/util/effect/layer-node"
import { Money } from "@opencode/schema/money"
import { Event } from "@opencode/schema/event"
import { AppNodeBuilder } from "../src/effect/app-node-builder.js"
import { Service, node } from "../src/database/database.js"
import { EffectDrizzleSqlite } from "../src/database/drizzle.js"
import { Sqlite } from "../src/database/sqlite.js"
import { sqliteLayer } from "../src/database/sqlite.bun.js"
import { WsNative } from "../src/database/ws-native.js"
import { DatabaseWsArchive } from "../src/database/ws-archive.js"
import { Bus } from "../src/bus.js"
import { Instructions } from "../src/instructions/index.js"
import { Project } from "../src/project.js"
import { AbsolutePath, RelativePath } from "../src/schema.js"
import { SessionEvent } from "../src/session/event.js"
import { SessionSchema } from "../src/session/schema.js"
import { SessionMessage } from "../src/session/message.js"
import { SessionProjector } from "../src/session/projector.js"
import { InstructionState } from "../src/session/instruction-state.js"
import { SessionWsRebuild } from "../src/session/ws-rebuild.js"
import { setupWs, setupPreviousNative, baseline, sessionID } from "./fixture/database-ws.js"
import { tempGlobalLayer } from "./fixture/global.js"
import { tmpdir } from "./fixture/tmpdir.js"

const root = SessionSchema.ID.make(sessionID)
const child = SessionSchema.ID.make("ses_rebuild_child")
const grandchild = SessionSchema.ID.make("ses_rebuild_grandchild")
const deleted = SessionSchema.ID.make("ses_rebuild_deleted")
const unrelated = SessionSchema.ID.make("ses_rebuild_unrelated")
const key = Instructions.Key.make("fixture/policy")
const created = (id: SessionSchema.ID, parentID?: SessionSchema.ID) => ({
  sessionID: id, parentID, projectID: Project.ID.make("project"), slug: "native-child", version: "fixture",
  location: { directory: AbsolutePath.make("/fixture") }, subpath: RelativePath.make(""), title: "native",
})

function runtime(filename: string, previous?: boolean) {
  const sqlite = sqliteLayer({ filename })
  const database = Layer.effect(Service, Effect.gen(function* () {
    const db = yield* EffectDrizzleSqlite.makeWithDefaults()
    yield* db.run("PRAGMA foreign_keys = ON")
    if (previous !== undefined) {
      yield* (previous ? setupPreviousNative : setupWs)(db, { directory: "/fixture", rich: true })
      if (!previous) {
        yield* db.run("INSERT INTO event_sequence VALUES ('ses_source_deleted', 1, 'source-owner')")
        const data = JSON.stringify({ sessionID: "ses_source_deleted", info: {
          id: "ses_source_deleted", parentID: root, projectID: "project", slug: "source-deleted", directory: "/fixture",
          title: "Source deleted", version: "fixture", time: { created: 1, updated: 2 },
        } })
        yield* db.run(sql`INSERT INTO event VALUES ('evt_source_child_created', 'ses_source_deleted', 0, 'session.created.1', ${data}),
          ('evt_source_child_deleted', 'ses_source_deleted', 1, 'session.deleted.1', ${data})`)
      }
      yield* db.run("UPDATE session SET parent_id = 'ses_migration' WHERE id = 'ses_legacy'")
      yield* WsNative.convert(db)
    }
    return { db }
  }).pipe(Effect.orDie)).pipe(Layer.provide(sqlite), Layer.provide(tempGlobalLayer))
  return AppNodeBuilder.build(LayerNode.group([node, Bus.node, SessionProjector.node]), [node.replace(database)]).pipe(Layer.provideMerge(sqlite))
}

const populate = Effect.gen(function* () {
  const database = yield* Service
  const db = database.db
  const bus = yield* Bus.Service
  expect(yield* db.get("SELECT title, time_created, summary_additions FROM session_v2 WHERE id = 'ses_migration'"))
    .toEqual({ title: "Source session", time_created: 1, summary_additions: 3 })
  expect(yield* db.get("SELECT json_extract(data, '$.text') AS text FROM session_message WHERE id = 'msg_legacy'"))
    .toEqual({ text: "Legacy transcript" })
  yield* bus.publish(SessionEvent.Created, created(unrelated))
  yield* bus.publish(SessionEvent.Synthetic, { sessionID: unrelated, text: "native without retained events" })
  const boundary = yield* bus.publish(SessionEvent.Synthetic, { sessionID: root, text: "native boundary" })
  yield* bus.publish(SessionEvent.InstructionsUpdated, { sessionID: root, delta: { [key]: Instructions.hash("native instructions") }, text: "native change" }, {
    commit: () => db.run(sql`INSERT INTO instruction_blob VALUES (${Instructions.hash("native instructions")}, '"native instructions"')`).pipe(Effect.orDie),
  })
  yield* bus.publish(SessionEvent.Forked, { sessionID: child, parentID: root,
    boundary: { type: "through", messageID: SessionMessage.ID.fromEvent(boundary.id) }, ...yield* InstructionState.captureFork(db, root) })
  const childBoundary = yield* bus.publish(SessionEvent.Synthetic, { sessionID: child, text: "child boundary" })
  yield* bus.publish(SessionEvent.Forked, { sessionID: grandchild, parentID: child,
    boundary: { type: "through", messageID: SessionMessage.ID.fromEvent(childBoundary.id) }, ...yield* InstructionState.captureFork(db, child) })
  yield* bus.publish(SessionEvent.Created, created(deleted, grandchild))
  yield* bus.publish(SessionEvent.Deleted, { sessionID: deleted })
  yield* bus.replay({ id: Event.ID.make("evt_rebuild_rename"), aggregateID: root, seq: 53,
    type: "session.renamed.1", created: 2, data: { sessionID: root, title: "Native renamed" } }, { ownerID: "owner_fixture", publish: false })
  yield* bus.publish(SessionEvent.Compaction.Ended, { sessionID: root, reason: "manual", text: "summary", recent: "recent", cost: Money.USD.make(0),
    tokens: { input: 0, output: 0, reasoning: 0, cache: { read: 0, write: 0 } } }, { metadata: { fixture: "retained envelope" } })
  yield* db.run("UPDATE session_v2 SET time_suspended = 123, resume_attempts = 2 WHERE id = 'ses_migration'")
  yield* db.run("UPDATE session_message SET time_updated = 87654 WHERE session_id = 'ses_migration'")
  yield* db.run("UPDATE session_message SET data = json_set(data, '$.metadata', json('{\"large\":9007199254740993,\"empty\":null}')) WHERE type = 'compaction' AND session_id = 'ses_migration'")
  yield* db.run("INSERT OR REPLACE INTO instruction_entry VALUES ('ses_migration', 'fixture/policy', 'null', 0, 23, 45)")
  yield* db.run("INSERT OR REPLACE INTO instruction_entry VALUES ('ses_migration', 'fixture/current-only', NULL, 1, 12, 34)")
  yield* db.run("DELETE FROM instruction_entry WHERE key = 'fixture/removed'")
  yield* db.run("UPDATE project SET name = 'current global edit'")
  yield* db.run("UPDATE credential SET label = 'current label'")
  yield* db.run("INSERT OR REPLACE INTO kv VALUES ('fixture:state', '{\"current\":true}', 12, 34)")
  yield* bus.claim(root, "current-owner")
  yield* bus.claim(child, "child-owner")
  yield* db.run("UPDATE event SET rowid = rowid * 10, data = ' ' || data || ' '")
  const native = yield* Sqlite.Native
  if (!(native instanceof Database)) throw new Error("Expected Bun SQLite")
  return native.serialize()
})

async function withClone<A, E>(previous: boolean, body: Effect.Effect<A, E, Service | Bus.Service>) {
  await using directory = await tmpdir()
  const bytes = await Effect.runPromise(populate.pipe(Effect.provide(runtime(":memory:", previous)), Effect.scoped))
  const filename = join(directory.path, "disposable.sqlite")
  await Bun.write(filename, bytes)
  return Effect.runPromise(body.pipe(Effect.provide(runtime(filename)), Effect.scoped))
}

for (const previous of [false, true]) test(`hybrid ${previous ? "P" : "WS"} reconstruction preserves current authority and recursive forks`, () => withClone(previous, Effect.gen(function* () {
  const database = yield* Service
  const db = database.db
  const bus = yield* Bus.Service
  const events = yield* db.all("SELECT rowid, * FROM event ORDER BY rowid")
  const entries = yield* db.all("SELECT * FROM instruction_entry ORDER BY session_id, key")
  const blobs = yield* db.all("SELECT * FROM instruction_blob ORDER BY hash")
  const messages = yield* db.all("SELECT id, time_updated, json_extract(data, '$.metadata') AS metadata, json_type(data, '$.metadata') AS presence FROM session_message ORDER BY id")
  const result = yield* SessionWsRebuild.verifyClone({ db, bus, roots: [root] })
  expect(result.sessions).toContain(child)
  expect(result.sessions).toContain(grandchild)
  expect(result.sessions).toContain(deleted)
  expect(result.sessions).toContain("ses_legacy")
  if (!previous) expect(result.sessions).toContain("ses_source_deleted")
  expect(result.sessions).not.toContain(unrelated)
  expect(yield* db.all("SELECT rowid, * FROM event ORDER BY rowid")).toEqual(events)
  expect(yield* db.all("SELECT * FROM instruction_entry ORDER BY session_id, key")).toEqual(entries)
  expect(yield* db.all("SELECT * FROM instruction_blob ORDER BY hash")).toEqual(blobs)
  expect(yield* db.all("SELECT id, time_updated, json_extract(data, '$.metadata') AS metadata, json_type(data, '$.metadata') AS presence FROM session_message ORDER BY id")).toEqual(messages)
  expect(yield* db.get("SELECT * FROM session_v2 WHERE id = 'ses_rebuild_deleted'")).toBeUndefined()
  expect(yield* db.all("SELECT * FROM event WHERE aggregate_id = 'ses_rebuild_unrelated'")).toEqual([])
  expect(yield* db.get("SELECT json_extract(data, '$.text') AS text FROM session_message WHERE session_id = 'ses_rebuild_unrelated'"))
    .toEqual({ text: "native without retained events" })
  expect(yield* db.get("SELECT name FROM project")).toEqual({ name: "current global edit" })
  expect(yield* db.get("SELECT time_suspended, resume_attempts FROM session_v2 WHERE id = 'ses_migration'"))
    .toEqual({ time_suspended: 123, resume_attempts: 2 })
  expect(yield* db.get("SELECT frozen_baseline FROM instruction_state WHERE session_id = 'ses_migration'"))
    .toEqual({ frozen_baseline: null })
  expect(yield* db.get("SELECT title, time_created, summary_additions FROM session_v2 WHERE id = 'ses_migration'"))
    .toEqual({ title: "Native renamed", time_created: 1, summary_additions: 3 })
  expect(yield* db.get("SELECT title FROM session_v2 WHERE id = 'ses_rebuild_child'"))
    .toEqual({ title: "Source session (fork #1)" })
  expect(yield* db.all("SELECT json_extract(data, '$.text') AS text FROM session_message_part WHERE message_id = 'msg_migration' AND type = 'text' ORDER BY position"))
    .toEqual([{ text: "first" }, { text: "second" }])
  expect(yield* db.all("SELECT id, delivery, enqueued_seq FROM session_inbox WHERE id IN ('msg_pending_steer', 'msg_pending_queue') ORDER BY enqueued_seq"))
    .toEqual([{ id: "msg_pending_steer", delivery: "steer", enqueued_seq: 40 }, { id: "msg_pending_queue", delivery: "queue", enqueued_seq: 43 }])
  expect(yield* db.get("SELECT data -> '$.metadata.large' AS value FROM session_message WHERE type = 'compaction' AND session_id = 'ses_migration' ORDER BY seq DESC LIMIT 1"))
    .toEqual({ value: "9007199254740993" })
  if (!previous) expect(yield* InstructionState.initial(db, child, [])).toBe(`${baseline}\n\nCurrent policy differs from the baseline\n\nnative change`)
  yield* SessionWsRebuild.verifyClone({ db, bus, roots: [root] })
})))

for (const [name, mutation, error] of [
  ["cursor gap", "UPDATE event_sequence SET seq = seq + 1 WHERE aggregate_id = 'ses_migration'", "event_sequence"],
  ["body mismatch", "UPDATE session_message SET data = json_set(data, '$.text', 'unexplained') WHERE type = 'synthetic' AND session_id = 'ses_migration'", "session_message"],
  ["large integer body mismatch", "UPDATE session_v2 SET metadata = replace(metadata, '9007199254740993', '9007199254740992') WHERE id = 'ses_migration'", "session_v2"],
  ["unexplained pending work", "INSERT INTO session_pending VALUES ('msg_unexplained', 'ses_migration', 'user', '{\"text\":\"unexplained\"}', 'queue', 300, 300)", "Unexplained current session_pending"],
  ["missing current blob", "DELETE FROM instruction_blob WHERE value = '\"native instructions\"'", "Unresolved current instruction hash"],
  ["missing tail event", "DELETE FROM event WHERE seq = 51 AND aggregate_id = 'ses_migration'", "Sequence mismatch"],
]) test(`rejects ${name} and rolls back the complete clone`, () => withClone(false, Effect.gen(function* () {
  const database = yield* Service
  const db = database.db
  const bus = yield* Bus.Service
  yield* db.run(mutation)
  const tables = yield* db.all<{ name: string }>("SELECT name FROM sqlite_schema WHERE type = 'table' ORDER BY name")
  const before = yield* Effect.forEach(tables, (table) => db.all(sql`SELECT rowid, * FROM ${DatabaseWsArchive.identifier(table.name)} ORDER BY rowid`))
  const result = yield* SessionWsRebuild.verifyClone({ db, bus, roots: [root] }).pipe(Effect.exit)
  expect(result._tag).toBe("Failure")
  if (result._tag === "Failure") expect(Cause.pretty(result.cause)).toContain(error)
  expect(yield* Effect.forEach(tables, (table) => db.all(sql`SELECT rowid, * FROM ${DatabaseWsArchive.identifier(table.name)} ORDER BY rowid`))).toEqual(before)
})))

test("rejects a SessionTransfer-style commit-only descendant prefix without manufacturing events", () => withClone(false, Effect.gen(function* () {
  const database = yield* Service
  const db = database.db
  const bus = yield* Bus.Service
  const imported = SessionSchema.ID.make("ses_transfer_descendant")
  yield* bus.publish(SessionEvent.Created, created(imported, root), { commit: (seq) => Effect.gen(function* () {
    yield* db.run(sql`INSERT INTO session_message (id, session_id, type, seq, time_created, time_updated, data)
      VALUES ('msg_transferred', ${imported}, 'synthetic', ${seq + 1}, 10, 20, '{"text":"commit-only transcript","time":{"created":10}}')`).pipe(Effect.orDie)
    yield* Bus.reserveSequence(db, imported, seq + 1)
  }) })
  const tables = yield* db.all<{ name: string }>("SELECT name FROM sqlite_schema WHERE type = 'table' ORDER BY name")
  const before = yield* Effect.forEach(tables, (table) => db.all(sql`SELECT rowid, * FROM ${DatabaseWsArchive.identifier(table.name)} ORDER BY rowid`))
  const result = yield* SessionWsRebuild.verifyClone({ db, bus, roots: [root] }).pipe(Effect.exit)
  expect(result._tag).toBe("Failure")
  if (result._tag === "Failure") expect(Cause.pretty(result.cause)).toContain("Missing rebuilt message msg_transferred")
  expect(yield* Effect.forEach(tables, (table) => db.all(sql`SELECT rowid, * FROM ${DatabaseWsArchive.identifier(table.name)} ORDER BY rowid`))).toEqual(before)
})))

test("restores a prefix referencing absent Project A, then replays its move to current Project B", () => withClone(false, Effect.gen(function* () {
  const database = yield* Service
  const db = database.db
  const bus = yield* Bus.Service
  yield* db.run("INSERT INTO project (id, worktree, time_created, time_updated, sandboxes) VALUES ('destination', '/destination', 1, 2, '[]')")
  yield* db.run("UPDATE session_v2 SET path = 'resolved-before-move' WHERE id = 'ses_migration'")
  yield* bus.publish(SessionEvent.Moved, { sessionID: root, projectID: Project.ID.make("destination"),
    location: { directory: AbsolutePath.make("/destination") }, subpath: RelativePath.make("after-move") })
  yield* db.run("UPDATE session_v2 SET project_id = 'destination', path = 'current-resolved-path'")
  yield* db.run("DELETE FROM project WHERE id = 'project'")
  const projects = yield* db.all("SELECT * FROM project")
  const previous = yield* db.all("SELECT json_extract(data, '$.previous') AS previous FROM session_message WHERE type = 'location-switched'")
  yield* SessionWsRebuild.verifyClone({ db, bus, roots: [root] })
  expect(yield* db.all("SELECT * FROM project")).toEqual(projects)
  expect(yield* db.all("SELECT json_extract(data, '$.previous') AS previous FROM session_message WHERE type = 'location-switched'")).toEqual(previous)
  expect(yield* db.get("SELECT project_id, path, directory FROM session_v2 WHERE id = 'ses_migration'"))
    .toEqual({ project_id: "destination", path: "current-resolved-path", directory: "/destination" })
  expect(yield* db.all("PRAGMA foreign_key_check")).toEqual([])
})))

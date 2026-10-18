import { expect, test } from "bun:test"
import path from "node:path"
import { SqliteClient } from "@effect/sql-sqlite-bun"
import { DateTime, Effect } from "effect"
import type { SqlClient } from "effect/sql/SqlClient"
import { sql } from "drizzle-orm"
import { EffectDrizzleSqlite } from "@opencode/core/database/drizzle"
import { WsNative } from "@opencode/core/database/ws-native"
import { DatabaseWsArchive } from "@opencode/core/database/ws-archive"
import { hydrateSelection } from "@opencode/core/session/message-storage"
import { SessionInbox } from "@opencode/core/session/inbox"
import { SessionSchema } from "@opencode/core/session/schema"
import { DatabaseMigration } from "@opencode/core/database/migration"
import { WsNativeProfile } from "@opencode/core/database/ws-native-profile"
import { sqliteLayer } from "@opencode/core/database/sqlite.bun"
import { tmpdir } from "./fixture/tmpdir"
import { tempGlobalLayer } from "./fixture/global"
import { setupPreviousNative, setupWs, normalizedMessageData, rawJSON, type WsLineage } from "./fixture/database-ws"

const run = <A, E>(effect: Effect.Effect<A, E, SqlClient>) => Effect.runPromise(effect.pipe(
  Effect.provide(SqliteClient.layer({ filename: ":memory:", disableWAL: true })), Effect.scoped,
))

const stepData = '{ "sessionID":"ses_migration", "assistantMessageID":"msg_migration", "agent":"build", "model":{"providerID":"test","id":"model"}'

for (const rich of [false, true]) test(`P conversion preserves ${rich ? "rich" : "minimal"} state`, () => run(Effect.gen(function* () {
  const db = yield* EffectDrizzleSqlite.makeWithDefaults()
  yield* setupPreviousNative(db, { directory: "/fixture", rich })
  yield* db.run(sql`INSERT INTO event VALUES ('evt_native_step', 'ses_migration', 7, 3, 'session.step.started.1', ${`${stepData}${rich ? ', "snapshot":"before"' : ""} }`})`)
  yield* db.run("ALTER TABLE session ADD COLUMN resume_attempts INTEGER")
  yield* db.run("UPDATE session SET resume_attempts = 99")
  yield* db.run(sql`UPDATE kv SET value = ${rawJSON}`)
  const pendingRaw = '{ "metadata":{"raw":1.00}, "text":"Steer next" }'
  yield* db.run(sql`UPDATE session_pending SET data = ${pendingRaw} WHERE type = 'user'`)
  const original = new Map<string, unknown[]>()
  for (const table of ["instruction_blob", "instruction_entry", "instruction_state", "kv", "session", "session_pending", "event", "message", "part"])
    original.set(table, yield* db.all(sql`SELECT * FROM ${DatabaseWsArchive.identifier(table)}`))
  yield* WsNative.validateSource(db)
  yield* WsNative.convert(db)
  yield* WsNative.validate(db)
  expect((yield* hydrateSelection({ db })).length).toBe(rich ? 2 : 1)
  expect(yield* db.get("SELECT data FROM session_message WHERE id = 'msg_migration'")).toEqual({ data: normalizedMessageData })
  expect(yield* db.all("SELECT * FROM event")).toEqual([])
  expect(yield* db.get("SELECT count(*) AS count FROM session_v2 WHERE resume_attempts != 0")).toEqual({ count: 0 })
  expect(yield* db.get("SELECT codec_version, through_seq FROM _ws_import_root WHERE aggregate_id = 'ses_migration'")).toEqual({ codec_version: 2, through_seq: 50 })
  for (const table of ["instruction_blob", "instruction_entry", "kv"])
    expect(yield* db.all(sql`SELECT * FROM ${DatabaseWsArchive.identifier(table)}`)).toEqual(original.get(table) ?? [])
  expect(yield* db.all("SELECT session_id, epoch_start, through_seq, initial_values, current_values FROM instruction_state")).toEqual(original.get("instruction_state") ?? [])
  if (rich) {
    expect(yield* db.get("SELECT payload FROM session_inbox WHERE type = 'user'")).toEqual({ payload: pendingRaw })
    expect(yield* db.get("SELECT value FROM kv")).toEqual({ value: rawJSON })
    expect(yield* db.get("SELECT parent_id, fork_session_id, fork_boundary, title, time_suspended FROM session_v2 WHERE id = 'ses_fork'")).toEqual({ parent_id: "ses_migration", fork_session_id: "ses_migration", fork_boundary: '{"type":"through","messageID":"msg_migration"}', title: null, time_suspended: 22 })
    expect(yield* db.all("SELECT id, type, delivery, enqueued_seq, time_created FROM session_inbox ORDER BY enqueued_seq")).toEqual([
      { id: "msg_pending_steer", type: "user", delivery: "steer", enqueued_seq: 40, time_created: 40 },
      { id: "msg_pending_queue", type: "synthetic", delivery: "queue", enqueued_seq: 43, time_created: 43 },
      { id: "msg_pending_compaction", type: "compaction", delivery: "steer", enqueued_seq: 44, time_created: 44 },
    ])
    expect((yield* SessionInbox.nextPromotable(db, SessionSchema.ID.make("ses_migration"), "input"))?.type).toBe("compaction")
  }
  const manifest = yield* db.get<{ id: string }>("SELECT id FROM _ws_archive")
  if (!manifest) throw new Error("Missing archive")
  const archive = yield* DatabaseWsArchive.read(db, manifest.id)
  if (!archive) throw new Error("Missing manifest")
  expect(archive.lineage_ids.length).toBe(57)
  for (const [name, rows] of original) {
    const retained = DatabaseWsArchive.sourceTable(archive, name)
    if (!retained) throw new Error("Missing source table")
    expect(yield* db.all(sql`SELECT * FROM ${DatabaseWsArchive.identifier(retained)}`)).toEqual(rows)
  }
  const before = yield* db.all("SELECT * FROM sqlite_schema")
  yield* WsNative.convert(db)
  expect(yield* db.all("SELECT * FROM sqlite_schema")).toEqual(before)
})))

for (const [name, change] of [
  ["unknown pending type", "UPDATE session_pending SET type = 'move' WHERE type = 'compaction'"],
  ["unknown control fields", "UPDATE session_pending SET data = '{\"force\":true}' WHERE type = 'compaction'"],
  ["null user delivery", "UPDATE session_pending SET delivery = NULL WHERE type = 'user'"],
  ["invalid native message", "UPDATE session_message SET data = '{}'"],
  ["unknown native message field", "UPDATE session_message SET data = json_set(data, '$.unsupported', true)"],
  ["unknown source event", "UPDATE event SET type = 'session.unknown.1'"],
  ["unknown step version", `UPDATE event SET type = 'session.step.started.2', data = '${stepData} }'`],
  ["unsupported step field", `UPDATE event SET type = 'session.step.started.1', data = '${stepData}, "unsupported":true }'`],
  ["current-only step started field", `UPDATE event SET type = 'session.step.started.1', data = '${stepData}, "started":3 }'`],
  ["missing instruction blob", "DELETE FROM instruction_blob"],
  ["invalid instruction values", "UPDATE instruction_state SET current_values = '[]'"],
  ["pending beyond watermark", "UPDATE session_pending SET admitted_seq = 51 WHERE type = 'compaction'"],
]) test(`P rejects ${name} unchanged`, () => run(Effect.gen(function* () {
  const db = yield* EffectDrizzleSqlite.makeWithDefaults()
  yield* setupPreviousNative(db, { directory: "/fixture", rich: true })
  yield* db.run(change)
  const tables = yield* db.all<{ name: string }>("SELECT name FROM sqlite_schema WHERE type = 'table'")
  const before = new Map<string, unknown[]>()
  for (const table of tables) before.set(table.name, yield* db.all(sql`SELECT * FROM ${DatabaseWsArchive.identifier(table.name)}`))
  const ddl = yield* db.all("SELECT * FROM sqlite_schema")
  expect((yield* WsNative.validateSource(db).pipe(Effect.result))._tag).toBe("Failure")
  expect((yield* WsNative.convert(db).pipe(Effect.result))._tag).toBe("Failure")
  expect(yield* db.all("SELECT * FROM sqlite_schema")).toEqual(ddl)
  for (const [name, rows] of before) expect(yield* db.all(sql`SELECT * FROM ${DatabaseWsArchive.identifier(name)}`)).toEqual(rows)
})))

for (const snapshot of [false, true]) test(`P profile preserves retained step start ${snapshot ? "with" : "without"} snapshot`, async () => {
  await using temp = await tmpdir()
  const filename = path.join(temp.path, "source.db")
  const data = `${stepData}${snapshot ? ', "snapshot":"before"' : ""} }`
  await Effect.runPromise(Effect.gen(function* () {
    const db = yield* EffectDrizzleSqlite.makeWithDefaults()
    yield* setupPreviousNative(db, { directory: temp.path, rich: true })
    yield* db.run(sql`INSERT INTO event VALUES ('evt_native_step', 'ses_migration', 7, 3, 'session.step.started.1', ${data})`)
  }).pipe(Effect.provide(sqliteLayer({ filename })), Effect.scoped))
  const before = await Bun.file(filename).bytes()
  await Effect.runPromise(Effect.gen(function* () {
    const db = yield* EffectDrizzleSqlite.makeWithDefaults()
    yield* DatabaseMigration.apply(db)
    expect(yield* WsNativeProfile.run(db, filename)).toBe(true)
    yield* WsNative.validate(db)
    expect(yield* db.all("SELECT * FROM event")).toEqual([])
    const history = yield* hydrateSelection({ db })
    expect(history.find((row) => row.message.id === "msg_migration")?.message).toMatchObject({
      type: "assistant", agent: "build", model: { providerID: "test", id: "model" },
      time: { created: DateTime.makeUnsafe(1) }, content: [{ type: "text", text: "first" }, { type: "text", text: "second" }],
    })
    expect(yield* db.get("SELECT data FROM session_message WHERE id = 'msg_migration'")).toEqual({ data: normalizedMessageData })
    const roots = yield* db.all("SELECT * FROM _ws_import_root")
    expect(yield* db.get("SELECT codec_version, through_seq FROM _ws_import_root WHERE aggregate_id = 'ses_migration'")).toEqual({ codec_version: 2, through_seq: 50 })
    const manifest = yield* db.get<{ id: string }>("SELECT id FROM _ws_archive")
    if (!manifest) throw new Error("Missing archive")
    const archive = yield* DatabaseWsArchive.read(db, manifest.id)
    if (!archive) throw new Error("Missing manifest")
    expect(archive.lineage_ids.length).toBe(57)
    const retained = DatabaseWsArchive.sourceTable(archive, "event")
    if (!retained) throw new Error("Missing source events")
    const events = yield* db.all(sql`SELECT * FROM ${DatabaseWsArchive.identifier(retained)}`)
    expect(events).toContainEqual({ id: "evt_native_step", aggregate_id: "ses_migration", seq: 7, created: 3, type: "session.step.started.1", data })
    expect(yield* WsNativeProfile.run(db, filename)).toBe(true)
    expect(yield* db.all("SELECT * FROM _ws_import_root")).toEqual(roots)
    expect(yield* db.all("SELECT id FROM _ws_archive")).toEqual([manifest])
    expect(yield* db.all(sql`SELECT * FROM ${DatabaseWsArchive.identifier(retained)}`)).toEqual(events)
    expect(yield* hydrateSelection({ db })).toEqual(history)
  }).pipe(Effect.provide(sqliteLayer({ filename: ":memory:" })), Effect.provide(tempGlobalLayer), Effect.scoped))
  expect(await Bun.file(filename).bytes()).toEqual(before)
})

const rawLineages: WsLineage[] = ["v01", "v02"]
for (const lineage of rawLineages) test(`WS ${lineage} preserves raw compatible envelope and child tokens`, () => run(Effect.gen(function* () {
  const db = yield* EffectDrizzleSqlite.makeWithDefaults()
  yield* setupWs(db, { directory: "/fixture", lineage })
  yield* db.run("ALTER TABLE session ADD COLUMN time_suspended INTEGER")
  yield* db.run("UPDATE session SET time_suspended = 99")
  const header = '"agent":"build","model":{"providerID":"test","id":"model"},"time":{"created":1.00},"metadata":{"raw":1e+09}'
  yield* db.run(sql`UPDATE session_message SET data = ${`{${header}${lineage === "v01" ? ',"content":[{"id":"text-1","type":"text","text":"first"},{"id":"text-2","type":"text","text":"second"}]' : ""}}`}`)
  if (lineage === "v02") yield* db.run("UPDATE session_message_part SET data = '{ \"text\" : \"first\" }' WHERE position = 0")
  yield* WsNative.convert(db)
  expect(yield* db.get("SELECT time_suspended, resume_attempts FROM session_v2")).toEqual({ time_suspended: null, resume_attempts: 0 })
  expect(yield* db.get("SELECT data FROM session_message")).toEqual({ data: `{${header}}` })
  if (lineage === "v02") expect(yield* db.get("SELECT data FROM session_message_part WHERE position = 0")).toEqual({ data: '{ "text" : "first" }' })
})))

test("P native tools and legacy event prefixes retain their native contracts", () => run(Effect.gen(function* () {
  const db = yield* EffectDrizzleSqlite.makeWithDefaults()
  yield* setupPreviousNative(db, { directory: "/fixture", rich: true })
  const header = '"agent":"build","model":{"providerID":"test","id":"model"}'
  const tail = '"time":{"created":1.00},"cost":1.2500'
  const content = '[{"type":"text","text":"native","state":{"opaque":1.00}},{"type":"tool","id":"native-call","name":"read","state":{"status":"running","input":{"path":"fixture"},"metadata":{"phase":"running"}},"providerState":{"signature":"native"},"time":{"created":1.00,"ran":2}}]'
  yield* db.run(sql`UPDATE session_message SET data = ${`{${header},"content":${content},${tail}}`}`)
  yield* db.run("INSERT INTO event_sequence VALUES ('ses_legacy', 1, 'legacy-owner')")
  const legacyEvent = '{ "sessionID":"ses_legacy", "messageID":"msg_absent" }'
  yield* db.run(sql`INSERT INTO event VALUES ('evt_legacy_removed', 'ses_legacy', 1, 2, 'message.removed.1', ${legacyEvent})`)
  yield* WsNative.convert(db)
  expect(yield* db.get("SELECT data FROM session_message WHERE id = 'msg_migration'")).toEqual({ data: `{${header},${tail}}` })
  expect(yield* db.get("SELECT data FROM session_message_part WHERE message_id = 'msg_migration' AND position = 0")).toEqual({ data: '{"text":"native","state":{"opaque":1.00}}' })
  const message = (yield* hydrateSelection({ db })).find((row) => row.message.id === "msg_migration")?.message
  expect(message).toMatchObject({ type: "assistant", content: [
    { type: "text", text: "native", state: { opaque: 1 } },
    { type: "tool", id: "native-call", providerState: { signature: "native" }, state: { status: "running", metadata: { phase: "running" } } },
  ] })
  expect(yield* db.get("SELECT through_seq, codec_version FROM _ws_import_root WHERE aggregate_id = 'ses_legacy'")).toEqual({ through_seq: 1, codec_version: 2 })
  expect(yield* db.get("SELECT seq, owner_id FROM event_sequence WHERE aggregate_id = 'ses_legacy'")).toEqual({ seq: 1, owner_id: "legacy-owner" })
  expect(yield* db.all("SELECT * FROM event")).toEqual([])
})))

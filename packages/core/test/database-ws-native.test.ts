import { expect, test } from "bun:test"
import { SqliteClient } from "@effect/sql-sqlite-bun"
import { Effect } from "effect"
import type { SqlClient } from "effect/sql/SqlClient"
import { EffectDrizzleSqlite } from "@opencode/core/database/drizzle"
import { WsNative } from "@opencode/core/database/ws-native"
import { DatabaseMigration } from "@opencode/core/database/migration"
import { hydrateSelection } from "@opencode/core/session/message-storage"
import { setupWs, baseline, snapshot, rawJSON, rawBlobHex, type WsLineage } from "./fixture/database-ws"
import type { Global } from "@opencode/util/global"
import { tempGlobalLayer } from "./fixture/global"
import { DatabaseWsArchive } from "@opencode/core/database/ws-archive"
import { sql } from "drizzle-orm"

const run = <A, E>(effect: Effect.Effect<A, E, SqlClient | Global.Service>) => Effect.runPromise(effect.pipe(
  Effect.provide(SqliteClient.layer({ filename: ":memory:", disableWAL: true })), Effect.provide(tempGlobalLayer), Effect.scoped,
))
const lineages: WsLineage[] = ["v01", "v02", "expanded", "expanded-alias", "split-alias", "historical-part"]
const rawEvent = '{ "sessionID":"ses_migration", "timestamp":49, "attempt":1.00, "error":{"message":"retry","isRetryable":true} }'
for (const lineage of lineages) test(`minimal WS ${lineage} becomes native once`, () => run(Effect.gen(function* () {
  const db = yield* EffectDrizzleSqlite.makeWithDefaults()
  yield* setupWs(db, { directory: "/fixture", lineage, rich: false })
  yield* db.run("ALTER TABLE session ADD COLUMN extra BLOB")
  yield* db.run("UPDATE session SET extra = X'00FF'")
  yield* db.run(sql`INSERT INTO event VALUES ('evt_archived', 'ses_migration', 49, 'session.next.retried.1', ${rawEvent})`)
  yield* db.run("INSERT INTO project_directory VALUES ('project', '/worktree', 'git_worktree', NULL, 9)")
  const session = yield* db.get("SELECT metadata, path, cost, time_created, time_updated FROM session")
  yield* WsNative.validateSource(db)
  yield* WsNative.convert(db)
  yield* WsNative.validate(db)
  expect((yield* hydrateSelection({ db }))[0]?.seq).toBe(7)
  expect(yield* db.get("SELECT time_active FROM project")).toEqual({ time_active: 2 })
  expect(yield* db.get("SELECT provider, binding FROM workspace")).toEqual({ provider: "ws-local", binding: '{"directory":"/fixture"}' })
  expect(yield* db.get("SELECT seq, owner_id FROM event_sequence")).toEqual({ seq: 50, owner_id: "owner_fixture" })
  expect(yield* db.get("SELECT through_seq, codec_version FROM _ws_import_root")).toEqual({ through_seq: 50, codec_version: 1 })
  expect(yield* db.all("SELECT * FROM event")).toEqual([])
  expect(yield* db.get("SELECT metadata, path, cost, time_created, time_updated FROM session_v2")).toEqual(session)
  expect(yield* db.get("SELECT strategy, time_created FROM worktree WHERE directory = '/worktree'")).toEqual({ strategy: "git", time_created: 9 })
  const manifest = yield* db.get<{ id: string }>("SELECT id FROM _ws_archive")
  expect(manifest).toBeDefined()
  if (!manifest) throw new Error("Missing archive")
  const archive = yield* DatabaseWsArchive.read(db, manifest.id)
  if (!archive) throw new Error("Missing manifest")
  const original = DatabaseWsArchive.sourceTable(archive, "session")
  if (!original) throw new Error("Missing archived session")
  expect(yield* db.get(sql`SELECT hex(extra) AS bytes FROM ${DatabaseWsArchive.identifier(original)}`)).toEqual({ bytes: "00FF" })
  const events = DatabaseWsArchive.sourceTable(archive, "event")
  if (!events) throw new Error("Missing archived event")
  expect(yield* db.get(sql`SELECT data FROM ${DatabaseWsArchive.identifier(events)}`)).toEqual({ data: rawEvent })
  expect((yield* db.all("SELECT * FROM migration")).length).toBe(DatabaseMigration.currentMigrationIDs.size)
  const before = yield* db.all("SELECT * FROM sqlite_schema")
  yield* WsNative.convert(db)
  expect(yield* db.all("SELECT * FROM sqlite_schema")).toEqual(before)
})))

for (const [name, change, error] of [
  ["JSON", "UPDATE session SET metadata = '{'", "Invalid JSON in session.metadata"],
  ["permission", "UPDATE session SET permission = '[{}]'", "session.permission"],
  ["remote workspace", "UPDATE workspace SET type = 'remote'", "Unsupported workspace"],
  ["watermark", "UPDATE event_sequence SET seq = 6", "sequence watermark"],
  ["required column", "ALTER TABLE session RENAME COLUMN path TO old_path", "Missing source column session.path"],
  ["message", "UPDATE session_message SET data = '{}'", "WsNativeMessageError"],
  ["unknown event", "INSERT INTO event VALUES ('evt_unknown', 'ses_migration', 49, 'source.only.1', '{}')", "source event"],
]) test(`rejects ${name} without source mutation`, () => run(Effect.gen(function* () {
  const db = yield* EffectDrizzleSqlite.makeWithDefaults()
  yield* setupWs(db, { directory: "/fixture", rich: false })
  yield* db.run(change)
  const before = yield* db.all("SELECT * FROM sqlite_schema")
  const result = yield* WsNative.convert(db).pipe(Effect.result)
  expect(result._tag).toBe("Failure")
  if (result._tag === "Failure") {
    expect(result.failure).toBeInstanceOf(WsNative.WsNativeError)
    expect(result.failure.message).toContain(error)
  }
  expect(yield* db.all("SELECT * FROM sqlite_schema")).toEqual(before)
})))

test("target insert failure rolls back capture, schema, and projections", () => run(Effect.gen(function* () {
  const db = yield* EffectDrizzleSqlite.makeWithDefaults()
  yield* setupWs(db, { directory: "/fixture", rich: true })
  yield* db.transaction((tx) => Effect.gen(function* () {
    yield* tx.run("PRAGMA defer_foreign_keys = ON")
    yield* tx.run("UPDATE part SET message_id = 'msg_migration'")
    yield* tx.run("UPDATE message SET id = 'msg_migration'")
  }))
  yield* WsNative.validateSource(db)
  const before = yield* db.all("SELECT * FROM sqlite_schema")
  const source = yield* db.all("SELECT * FROM session")
  const result = yield* WsNative.convert(db).pipe(Effect.result)
  expect(result._tag).toBe("Failure")
  expect(yield* db.all("SELECT * FROM sqlite_schema")).toEqual(before)
  expect(yield* db.all("SELECT * FROM session")).toEqual(source)
})))

for (const lineage of lineages) test(`rich WS ${lineage} preserves native and archived facts`, () => run(Effect.gen(function* () {
  const db = yield* EffectDrizzleSqlite.makeWithDefaults()
  yield* setupWs(db, { directory: "/fixture", lineage, rich: true })
  yield* db.run(`UPDATE session SET cost = 2.75, tokens_input = 17, tokens_output = 13, tokens_reasoning = 5,
    tokens_cache_read = 3, tokens_cache_write = 2, revert = '{"messageID":"msg_legacy","snapshot":"legacy-snapshot"}' WHERE id = 'ses_legacy'`)
  const sessions = yield* db.all("SELECT id, metadata, cost, tokens_input, tokens_output, tokens_reasoning, tokens_cache_read, tokens_cache_write, time_created, time_updated, time_archived, revert FROM session ORDER BY id")
  const originals = new Map<string, unknown[]>()
  for (const name of ["session", "message", "part", "todo", "session_share", "data_migration", "fixture_extension", "event", "session_input", "session_context_epoch"])
    originals.set(name, yield* db.all(sql`SELECT * FROM ${DatabaseWsArchive.identifier(name)}`))
  yield* WsNative.validateSource(db)
  yield* WsNative.convert(db)
  yield* WsNative.validate(db)
  expect(yield* db.all("SELECT id, metadata, cost, tokens_input, tokens_output, tokens_reasoning, tokens_cache_read, tokens_cache_write, time_created, time_updated, time_archived, revert FROM session_v2 ORDER BY id")).toEqual(sessions)
  const visible = yield* hydrateSelection({ db })
  expect(visible.length).toBe(6)
  expect(visible.find((item) => item.message.id === "msg_legacy")?.message).toMatchObject({ type: "user", text: "Legacy transcript" })
  expect(visible.find((item) => item.message.id === "msg_user_explicit")?.seq).toBe(4)
  expect(yield* db.all("SELECT id, delivery, enqueued_seq, time_created FROM session_inbox ORDER BY enqueued_seq")).toEqual([
    { id: "msg_pending_steer", delivery: "steer", enqueued_seq: 40, time_created: 40 },
    { id: "msg_pending_queue", delivery: "queue", enqueued_seq: 43, time_created: 43 },
  ])
  expect(yield* db.get("SELECT seq, owner_id FROM event_sequence WHERE aggregate_id = 'ses_legacy'")).toEqual({ seq: 0, owner_id: null })
  expect(yield* db.get("SELECT frozen_baseline, initial_values, epoch_start, through_seq, imported_removals FROM instruction_state")).toEqual({ frozen_baseline: baseline, initial_values: "{}", epoch_start: 1, through_seq: 50, imported_removals: '{"fixture/policy":"Policy removed"}' })
  expect(yield* db.all("SELECT key, b.value FROM instruction_state s, json_each(s.current_values) j JOIN instruction_blob b ON b.hash = j.value ORDER BY key")).toEqual([
    { key: "fixture/nullable", value: null }, { key: "fixture/policy", value: '"Current policy differs from the baseline"' },
  ])
  const manifest = yield* db.get<{ id: string }>("SELECT id FROM _ws_archive")
  if (!manifest) throw new Error("Missing archive")
  const archive = yield* DatabaseWsArchive.read(db, manifest.id)
  if (!archive) throw new Error("Missing manifest")
  for (const [name, rows] of originals) {
    const retained = DatabaseWsArchive.sourceTable(archive, name)
    if (!retained) throw new Error(`Missing archived ${name}`)
    expect(yield* db.all(sql`SELECT * FROM ${DatabaseWsArchive.identifier(retained)}`)).toEqual(rows)
  }
  const extension = DatabaseWsArchive.sourceTable(archive, "fixture_extension")
  const epoch = DatabaseWsArchive.sourceTable(archive, "session_context_epoch")
  if (!extension || !epoch) throw new Error("Missing source evidence")
  expect(yield* db.get(sql`SELECT json_text, hex(bytes) AS bytes FROM ${DatabaseWsArchive.identifier(extension)}`)).toEqual({ json_text: rawJSON, bytes: rawBlobHex })
  expect(yield* db.get(sql`SELECT baseline, snapshot FROM ${DatabaseWsArchive.identifier(epoch)}`)).toEqual({ baseline, snapshot })
  expect(yield* db.get("SELECT metadata FROM session_v2 WHERE id = 'ses_migration'")).toEqual({ metadata: rawJSON })
  expect(yield* db.all("SELECT * FROM event")).toEqual([])
  const before = yield* db.all("SELECT * FROM sqlite_schema")
  yield* WsNative.convert(db)
  expect(yield* db.all("SELECT * FROM sqlite_schema")).toEqual(before)
})))

test("pending payload, ordered permissions, and Azure credentials keep source evidence", () => run(Effect.gen(function* () {
  const db = yield* EffectDrizzleSqlite.makeWithDefaults()
  yield* setupWs(db, { directory: "/fixture", rich: false })
  const prompt = { text: "Pending input", metadata: { note: "retained" }, agents: [{ name: "build" }],
    files: [{ uri: "data:text/plain;base64,aGVsbG8=", mime: "text/plain", name: "inline.txt" },
      { uri: "file:///fixture/original.txt", mime: "text/plain", name: "original.txt" }], unknown: { keep: true } }
  yield* db.run(sql`INSERT INTO session_input VALUES ('msg_pending_files', 'ses_migration', ${JSON.stringify(prompt)}, 'queue', 12, NULL, 11)`)
  const permission = [{ permission: "write", pattern: "*.txt", action: "deny" },
    { permission: "patch", pattern: "safe.txt", action: "ask" }, { permission: "bash", pattern: "printf *", action: "allow" }]
  yield* db.run(sql`UPDATE session SET permission = ${JSON.stringify(permission)}`)
  yield* db.run("UPDATE permission SET action = 'write'")
  const credential = '{ "type":"oauth", "methodID":"azure-cli", "access":"fixture-token", "refresh":"fixture-refresh", "expires":10, "metadata":{"resource":"fixture-resource"} }'
  yield* db.run(sql`UPDATE credential SET integration_id = 'azure', value = ${credential}`)
  yield* WsNative.convert(db)
  const inbox = yield* db.get<{ payload: string }>("SELECT payload FROM session_inbox")
  expect(inbox).toBeDefined()
  if (!inbox) throw new Error("Missing inbox")
  expect(JSON.parse(inbox.payload)).toMatchObject({ text: "Pending input", metadata: { note: "retained" }, agents: [{ name: "build" }],
    files: [{ data: "aGVsbG8=", mime: "text/plain", name: "inline.txt", source: { type: "inline" } }],
    recordedFiles: [{ uri: "file:///fixture/original.txt", mime: "text/plain", name: "original.txt" }],
  })
  expect(yield* db.get("SELECT permission FROM session_v2")).toEqual({ permission: JSON.stringify([
    { action: "edit", resource: "*.txt", effect: "deny" }, { action: "edit", resource: "safe.txt", effect: "ask" },
    { action: "shell", resource: "printf *", effect: "allow" },
  ]) })
  expect(yield* db.get("SELECT action FROM permission")).toEqual({ action: "edit" })
  expect(yield* db.get("SELECT value FROM credential")).toEqual({ value: '{"type":"external","methodID":"azure-cli","metadata":{"resource":"fixture-resource"}}' })
  const manifest = yield* db.get<{ id: string }>("SELECT id FROM _ws_archive")
  if (!manifest) throw new Error("Missing archive")
  const archive = yield* DatabaseWsArchive.read(db, manifest.id)
  if (!archive) throw new Error("Missing manifest")
  for (const [table, column, value] of [["credential", "value", credential], ["session_input", "prompt", JSON.stringify(prompt)]]) {
    const retained = DatabaseWsArchive.sourceTable(archive, table)
    if (!retained) throw new Error("Missing source table")
    expect(yield* db.get(sql`SELECT ${DatabaseWsArchive.identifier(column)} AS value FROM ${DatabaseWsArchive.identifier(retained)}`)).toEqual({ value })
  }
})))

for (const [name, change] of [
  ["invalid snapshot", "UPDATE session_context_epoch SET snapshot = '{'"],
  ["epoch beyond watermark", "UPDATE session_context_epoch SET baseline_seq = 51"],
  ["admission beyond watermark", "UPDATE session_input SET admitted_seq = 51 WHERE promoted_seq IS NULL AND delivery = 'queue'"],
  ["promoted identity mismatch", "UPDATE session_input SET promoted_seq = 5 WHERE promoted_seq IS NOT NULL"],
  ["invalid delivery", "UPDATE session_input SET delivery = 'later' WHERE promoted_seq IS NULL"],
  ["invalid legacy message", "UPDATE message SET data = '{}'"],
  ["invalid legacy part", "UPDATE part SET data = '{}'"],
  ["legacy transform omission", "UPDATE part SET data = '{\"type\":\"compaction\",\"auto\":false}'"],
  ["missing WS watermark", "DELETE FROM event_sequence WHERE aggregate_id = 'ses_migration'"],
  ["unknown payload version", "UPDATE event SET type = 'session.next.step.started.99' WHERE seq = 7"],
  ["saved permission collision", "INSERT INTO permission VALUES ('permission_write', 'project', 'write', '*', 1, 2), ('permission_patch', 'project', 'patch', '*', 1, 2)"],
]) test(`rich source rejects ${name} without mutation`, () => run(Effect.gen(function* () {
  const db = yield* EffectDrizzleSqlite.makeWithDefaults()
  yield* setupWs(db, { directory: "/fixture", rich: true })
  yield* db.run(change)
  const tables = yield* db.all<{ name: string }>("SELECT name FROM sqlite_schema WHERE type = 'table'")
  const before = new Map<string, unknown[]>()
  for (const table of tables) before.set(table.name, yield* db.all(sql`SELECT * FROM ${DatabaseWsArchive.identifier(table.name)}`))
  const ddl = yield* db.all("SELECT * FROM sqlite_schema")
  const result = yield* WsNative.convert(db).pipe(Effect.result)
  expect(result._tag).toBe("Failure")
  if (result._tag === "Failure") expect(result.failure).toBeInstanceOf(WsNative.WsNativeError)
  expect(yield* db.all("SELECT * FROM sqlite_schema")).toEqual(ddl)
  for (const [name, rows] of before) expect(yield* db.all(sql`SELECT * FROM ${DatabaseWsArchive.identifier(name)}`)).toEqual(rows)
})))

test("empty session without source watermark reserves an empty native prefix", () => run(Effect.gen(function* () {
  const db = yield* EffectDrizzleSqlite.makeWithDefaults()
  yield* setupWs(db, { directory: "/fixture", rich: false })
  yield* db.run("DELETE FROM session_message")
  yield* db.run("DELETE FROM event_sequence")
  yield* WsNative.convert(db)
  expect(yield* db.get("SELECT seq FROM event_sequence")).toEqual({ seq: -1 })
  expect(yield* db.get("SELECT through_seq FROM _ws_import_root")).toEqual({ through_seq: -1 })
  expect(yield* hydrateSelection({ db })).toEqual([])
})))

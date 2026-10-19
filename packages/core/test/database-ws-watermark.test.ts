import { expect, test } from "bun:test"
import { SqliteClient } from "@effect/sql-sqlite-bun"
import { Effect } from "effect"
import type { SqlClient } from "effect/sql/SqlClient"
import type { Global } from "@opencode/util/global"
import { sql } from "drizzle-orm"
import { EffectDrizzleSqlite } from "@opencode/core/database/drizzle"
import { WsNative } from "@opencode/core/database/ws-native"
import { DatabaseWsArchive } from "@opencode/core/database/ws-archive"
import { Bus } from "@opencode/core/bus"
import { InstructionState } from "@opencode/core/session/instruction-state"
import { WsInstructionFrame } from "@opencode/core/session/ws-instruction-frame"
import { SessionSchema } from "@opencode/core/session/schema"
import { baseline, setupWs } from "./fixture/database-ws"
import { tempGlobalLayer } from "./fixture/global"

const run = <A, E>(effect: Effect.Effect<A, E, SqlClient | Global.Service>) => Effect.runPromise(effect.pipe(
  Effect.provide(SqliteClient.layer({ filename: ":memory:", disableWAL: true })), Effect.provide(tempGlobalLayer), Effect.scoped,
))

for (const watermark of [undefined, 0, 1, 9]) test(`legacy watermark ${watermark ?? "missing"} covers projection without changing archived facts`, () => run(Effect.gen(function* () {
  const db = yield* EffectDrizzleSqlite.makeWithDefaults()
  yield* setupWs(db, { directory: "/fixture", rich: true })
  yield* db.run("INSERT INTO message SELECT 'msg_second', session_id, time_created+1, time_updated+1, data FROM message WHERE id='msg_legacy'")
  if (watermark !== undefined) yield* db.run(sql`INSERT INTO event_sequence VALUES ('ses_legacy', ${watermark}, 'owner_legacy')`)
  const original = yield* db.all("SELECT * FROM event_sequence ORDER BY aggregate_id")
  const unchanged = yield* db.get("SELECT * FROM event_sequence WHERE aggregate_id='ses_migration'")
  yield* WsNative.validateSource(db)

  yield* WsNative.convert(db)

  yield* WsNative.validate(db)
  expect(yield* db.all("SELECT seq FROM session_message WHERE session_id='ses_legacy' ORDER BY seq")).toEqual([{ seq: 0 }, { seq: 1 }])
  expect(yield* db.get("SELECT seq, owner_id FROM event_sequence WHERE aggregate_id='ses_legacy'")).toEqual({
    seq: Math.max(watermark ?? -1, 1), owner_id: watermark === undefined ? null : "owner_legacy",
  })
  expect(yield* db.get("SELECT through_seq FROM _ws_import_root WHERE aggregate_id='ses_legacy'")).toEqual({ through_seq: Math.max(watermark ?? -1, 1) })
  expect(yield* db.get("SELECT * FROM event_sequence WHERE aggregate_id='ses_migration'")).toEqual(unchanged)
  expect(yield* db.all("SELECT * FROM event")).toEqual([])
  const manifest = yield* db.get<{ id: string }>("SELECT id FROM _ws_archive")
  if (!manifest) throw new Error("Missing archive")
  const archive = yield* DatabaseWsArchive.read(db, manifest.id)
  if (!archive) throw new Error("Missing manifest")
  const source = DatabaseWsArchive.sourceTable(archive, "event_sequence")
  if (!source) throw new Error("Missing archived watermark")
  expect(yield* db.all(sql`SELECT * FROM ${DatabaseWsArchive.identifier(source)} ORDER BY aggregate_id`)).toEqual(original)
  expect((yield* db.run(sql`UPDATE ${DatabaseWsArchive.identifier(source)} SET seq=9`).pipe(Effect.result))._tag).toBe("Failure")
  expect(yield* db.all(sql`SELECT * FROM ${DatabaseWsArchive.identifier(source)} ORDER BY aggregate_id`)).toEqual(original)
})))

test("selected legacy restore raises only the selected watermark and retains its owner", () => run(Effect.gen(function* () {
  const db = yield* EffectDrizzleSqlite.makeWithDefaults()
  yield* setupWs(db, { directory: "/fixture", rich: true })
  yield* db.run("INSERT INTO message SELECT 'msg_second', session_id, time_created+1, time_updated+1, data FROM message WHERE id='msg_legacy'")
  yield* db.run("INSERT INTO event_sequence VALUES ('ses_legacy', 0, 'owner_legacy')")
  yield* WsNative.convert(db)
  const manifest = yield* db.get<{ id: string }>("SELECT id FROM _ws_archive")
  if (!manifest) throw new Error("Missing archive")
  const archive = yield* DatabaseWsArchive.read(db, manifest.id)
  if (!archive) throw new Error("Missing manifest")
  yield* db.run("DELETE FROM session_v2 WHERE id='ses_legacy'")
  yield* db.run("UPDATE event_sequence SET seq=0")
  const unrelated = yield* db.all("SELECT * FROM session_message WHERE session_id='ses_migration' ORDER BY seq")
  const unselected = yield* db.get("SELECT * FROM event_sequence WHERE aggregate_id='ses_migration'")

  yield* db.transaction((tx) => WsNative.restoreSessions(tx, archive, ["ses_legacy"]))

  expect(yield* db.get("SELECT seq, owner_id FROM event_sequence WHERE aggregate_id='ses_legacy'")).toEqual({ seq: 1, owner_id: "owner_legacy" })
  expect(yield* db.get("SELECT * FROM event_sequence WHERE aggregate_id='ses_migration'")).toEqual(unselected)
  expect(yield* db.all("SELECT * FROM session_message WHERE session_id='ses_migration' ORDER BY seq")).toEqual(unrelated)
  expect(yield* db.all("SELECT * FROM event")).toEqual([])
})))

test("expanded legacy prefix keeps frozen instruction capture at the immutable import boundary", () => run(Effect.gen(function* () {
  const db = yield* EffectDrizzleSqlite.makeWithDefaults()
  const sessionID = SessionSchema.ID.make("ses_legacy")
  yield* setupWs(db, { directory: "/fixture", rich: true })
  yield* db.run("INSERT INTO message SELECT 'msg_second', session_id, time_created+1, time_updated+1, data FROM message WHERE id='msg_legacy'")
  yield* db.run("INSERT INTO event_sequence VALUES ('ses_legacy', 0, 'owner_legacy')")
  yield* db.run("INSERT INTO session_context_epoch SELECT 'ses_legacy', baseline, snapshot, 0 FROM session_context_epoch WHERE session_id='ses_migration'")
  const epochs = yield* db.all("SELECT * FROM session_context_epoch ORDER BY session_id")

  yield* WsNative.convert(db)

  const roots = yield* db.all("SELECT * FROM _ws_import_root ORDER BY aggregate_id")
  const manifest = yield* db.get<{ id: string }>("SELECT id FROM _ws_archive")
  if (!manifest) throw new Error("Missing archive")
  const archive = yield* DatabaseWsArchive.read(db, manifest.id)
  if (!archive) throw new Error("Missing manifest")
  const source = DatabaseWsArchive.sourceTable(archive, "session_context_epoch")
  if (!source) throw new Error("Missing archived epoch")
  const initial = yield* db.get("SELECT epoch_start, frozen_baseline, initial_values, current_values, imported_removals FROM instruction_state WHERE session_id='ses_legacy'")
  for (const restored of [false, true]) {
    if (restored) {
      yield* Bus.reserveSequence(db, sessionID, 9)
      yield* Bus.reserveSequence(db, sessionID, 0)
      yield* db.run("DELETE FROM session_v2 WHERE id='ses_legacy'")
      yield* db.transaction((tx) => WsNative.restoreSessions(tx, archive, [sessionID]))
    }
    const capture = yield* InstructionState.captureFork(db, sessionID)
    if (capture.instructionSourceSeq === undefined || capture.instructions === undefined) throw new Error("Missing capture")
    const frame = yield* WsInstructionFrame.resolve(db, { sessionID, throughSeq: capture.instructionSourceSeq, values: capture.instructions })
    expect(capture.instructionSourceSeq).toBe(1)
    expect(frame.values).toEqual(capture.instructions)
    expect(frame.baseline).toBe(baseline)
    expect(frame.removals).toEqual({ "fixture/policy": "Policy removed" })
    expect(yield* db.get("SELECT epoch_start, frozen_baseline, initial_values, current_values, imported_removals FROM instruction_state WHERE session_id='ses_legacy'")).toEqual(initial)
    expect(yield* db.get("SELECT seq, owner_id FROM event_sequence WHERE aggregate_id='ses_legacy'")).toEqual({ seq: restored ? 9 : 1, owner_id: "owner_legacy" })
    expect(yield* db.all("SELECT * FROM _ws_import_root ORDER BY aggregate_id")).toEqual(roots)
    expect(yield* db.all(sql`SELECT * FROM ${DatabaseWsArchive.identifier(source)} ORDER BY session_id`)).toEqual(epochs)
    expect(yield* db.all("SELECT * FROM event")).toEqual([])
    yield* WsNative.validate(db)
    if (!restored) {
      yield* Bus.reserveSequence(db, sessionID, capture.instructionSourceSeq + 1)
      yield* Bus.reserveSequence(db, sessionID, capture.instructionSourceSeq)
      expect(yield* db.get("SELECT seq, owner_id FROM event_sequence WHERE aggregate_id='ses_legacy'")).toEqual({ seq: 2, owner_id: "owner_legacy" })
      expect(yield* db.all("SELECT * FROM _ws_import_root ORDER BY aggregate_id")).toEqual(roots)
    }
  }
})))

for (const change of [
  "INSERT INTO event_sequence VALUES ('ses_legacy', -2, 'owner_legacy')",
  "UPDATE message SET data='{}' WHERE id='msg_legacy'",
]) test(`invalid legacy watermark source rolls back: ${change.startsWith("INSERT") ? "negative sequence" : "malformed message"}`, () => run(Effect.gen(function* () {
  const db = yield* EffectDrizzleSqlite.makeWithDefaults()
  yield* setupWs(db, { directory: "/fixture", rich: true })
  yield* db.run(change)
  const ddl = yield* db.all("SELECT * FROM sqlite_schema")
  const tables = yield* db.all<{ name: string }>("SELECT name FROM sqlite_schema WHERE type='table'")
  const original = new Map<string, unknown[]>()
  for (const table of tables) original.set(table.name, yield* db.all(sql`SELECT * FROM ${DatabaseWsArchive.identifier(table.name)}`))

  const result = yield* WsNative.convert(db).pipe(Effect.result)

  expect(result._tag).toBe("Failure")
  expect(yield* db.all("SELECT * FROM sqlite_schema")).toEqual(ddl)
  for (const [name, rows] of original) expect(yield* db.all(sql`SELECT * FROM ${DatabaseWsArchive.identifier(name)}`)).toEqual(rows)
})))

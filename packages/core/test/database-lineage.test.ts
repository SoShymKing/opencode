import { expect, test } from "bun:test"
import { Effect, Exit } from "effect"
import { sql } from "drizzle-orm"
import { EffectDrizzleSqlite } from "../src/database/drizzle.js"
import { sqliteLayer } from "../src/database/sqlite.bun.js"
import { DatabaseLineage } from "../src/database/lineage.js"
import { DatabaseMigration } from "../src/database/migration.js"
import { Database } from "../src/database/database.js"
import { Global } from "@opencode/util/global"
import { tmpdir } from "./fixture/tmpdir.js"

const run = (body: (db: EffectDrizzleSqlite.EffectSQLiteDatabase) => Effect.Effect<void, unknown>) =>
  Effect.runPromise(Effect.gen(function* () {
    yield* body(yield* EffectDrizzleSqlite.makeWithDefaults())
  }).pipe(Effect.provide(sqliteLayer({ filename: ":memory:" })), Effect.scoped))

test("ws compact history rejects without changing rows or either journal", () => run((db) => Effect.gen(function* () {
  yield* db.run("CREATE TABLE session (id TEXT PRIMARY KEY)")
  yield* db.run("CREATE TABLE session_message (id TEXT PRIMARY KEY, data TEXT)")
  yield* db.run("INSERT INTO session_message VALUES ('message', '{\"text\":\"keep\"}')")
  yield* db.run("CREATE TABLE migration (id TEXT PRIMARY KEY, time_completed INTEGER)")
  yield* db.run("INSERT INTO migration VALUES ('v01_baseline', 1)")
  const before = yield* db.all(sql`SELECT * FROM session_message`)
  const outcome = yield* DatabaseLineage.requireNative(db, DatabaseLineage.legacyBaselineMigrationIDs).pipe(Effect.flip)
  expect(outcome).toBeInstanceOf(DatabaseLineage.WsUpgradeRequiredError)
  expect(yield* db.all(sql`SELECT * FROM session_message`)).toEqual(before)
  expect(yield* db.all(sql`SELECT id FROM migration`)).toEqual([{ id: "v01_baseline" }])
})))

test("complete ws baseline accepts the metadata alias as evidence but rejects upgrade", () => run((db) => Effect.gen(function* () {
  yield* db.run("CREATE TABLE session (id TEXT PRIMARY KEY)")
  yield* db.run("CREATE TABLE session_message (id TEXT PRIMARY KEY)")
  yield* db.run("CREATE TABLE __drizzle_migrations (name TEXT)")
  for (const id of DatabaseLineage.legacyBaselineMigrationIDs)
    yield* db.run(sql`INSERT INTO __drizzle_migrations VALUES (${id === DatabaseLineage.legacySessionMetadataMigrationID ? DatabaseLineage.legacySessionMetadataAlias : id})`)
  const result = yield* DatabaseLineage.inspect(db, DatabaseLineage.legacyBaselineMigrationIDs)
  expect(result.baselineComplete).toBe(true)
  expect(result.ws).toBe(true)
  expect(Exit.isFailure(yield* Effect.exit(DatabaseLineage.requireNative(db, DatabaseLineage.legacyBaselineMigrationIDs)))).toBe(true)
  expect(yield* db.all(sql`SELECT name FROM sqlite_master WHERE name = 'migration'`)).toEqual([])
})))

test("unknown timestamp in second ledger rejects even with a native journal", () => run((db) => Effect.gen(function* () {
  yield* db.run("CREATE TABLE migration (id TEXT PRIMARY KEY)")
  yield* db.run("INSERT INTO migration VALUES ('20261010180456_project_time_archived')")
  yield* db.run("CREATE TABLE __drizzle_migrations (created_at INTEGER)")
  yield* db.run("INSERT INTO __drizzle_migrations VALUES (1)")
  expect(Exit.isFailure(yield* Effect.exit(DatabaseLineage.requireNative(db, ["20261010180456_project_time_archived"])))).toBe(true)
  expect(yield* db.all(sql`SELECT * FROM __drizzle_migrations`)).toEqual([{ created_at: 1 }])
})))

test("native timestamp maps without seeding a journal", () => run((db) => Effect.gen(function* () {
  yield* db.run("CREATE TABLE __drizzle_migrations (created_at INTEGER)")
  yield* db.run(sql`INSERT INTO __drizzle_migrations VALUES (${Date.UTC(2026, 9, 10, 18, 4, 56)})`)
  const result = yield* DatabaseLineage.requireNative(db, ["20261010180456_project_time_archived"])
  expect([...result.completed]).toEqual(["20261010180456_project_time_archived"])
  expect(yield* db.all(sql`SELECT name FROM sqlite_master WHERE name = 'migration'`)).toEqual([])
})))

test("populated canonical event history blocks historical reset", () => run((db) => Effect.gen(function* () {
  yield* db.run("CREATE TABLE event (data TEXT)")
  yield* db.run("INSERT INTO event VALUES ('keep')")
  expect(Exit.isFailure(yield* Effect.exit(DatabaseLineage.requireNative(db, DatabaseLineage.legacyBaselineMigrationIDs)))).toBe(true)
  expect(yield* db.all(sql`SELECT data FROM event`)).toEqual([{ data: "keep" }])
})))

test("public native migration rejects ws before journal creation", () => run((db) => Effect.gen(function* () {
  yield* db.run("CREATE TABLE session (id TEXT PRIMARY KEY)")
  yield* db.run("CREATE TABLE session_message (id TEXT PRIMARY KEY, session_id TEXT REFERENCES session(id))")
  yield* db.run("INSERT INTO session VALUES ('ses_ws')")
  yield* db.run("INSERT INTO session_message VALUES ('msg_ws', 'ses_ws')")
  const result = yield* Effect.exit(DatabaseMigration.apply(db).pipe(
    Effect.provideService(Global.Service, Global.make({ data: process.env.OPENCODE_TEST_HOME })),
  ))
  expect(result._tag).toBe("Failure")
  expect(yield* db.all(sql`SELECT name FROM sqlite_master WHERE name = 'migration'`)).toEqual([])
  expect(yield* db.all(sql`SELECT id FROM session_message`)).toEqual([{ id: "msg_ws" }])
})))

test("file database startup rejects ws without changing database bytes", async () => {
  await using directory = await tmpdir()
  const filename = `${directory.path}/ws.sqlite`
  await Effect.runPromise(Effect.gen(function* () {
    const db = yield* EffectDrizzleSqlite.makeWithDefaults()
    yield* db.run("CREATE TABLE session (id TEXT PRIMARY KEY)")
    yield* db.run("CREATE TABLE migration (id TEXT PRIMARY KEY)")
    yield* db.run("INSERT INTO migration VALUES ('v01_baseline')")
    yield* db.run("INSERT INTO session VALUES ('ses_ws')")
  }).pipe(Effect.provide(sqliteLayer({ filename })), Effect.scoped))
  const before = await Bun.file(filename).bytes()
  const result = await Effect.runPromise(Effect.exit(Effect.gen(function* () {
    yield* Database.Service
  }).pipe(Effect.provide(Database.layer({ path: filename })),
    Effect.provideService(Global.Service, Global.make({ data: directory.path })), Effect.scoped)))
  expect(result._tag).toBe("Failure")
  expect(await Bun.file(filename).bytes()).toEqual(before)
})

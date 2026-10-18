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

const previousIDs = [...DatabaseLineage.legacyBaselineMigrationIDs,
  "20260702134641_add_session_context_entry", "20260703090000_reset_v2_event_rename_sweep",
  "20260703181610_event_created_column", "20260703190000_reset_v2_shell_event_payloads",
  "20260703200000_reset_v2_session_events", "20260705180000_rename_instructions",
  "20260706223930_add-session-fork", "20260707010146_durable_session_inbox",
  "20260707120000_migrate_prelaunch_v2_state", "20260709013000_generic_session_input",
  "20260709025533_drop-todo", "20260709163752_time_suspended",
  "20260709190621_session_pending_table", "20260710025429_instruction_sync",
  "20260716020354_kv", "20260722011141_delete_tool_progress_events",
  "20260722170000_canonical_tool_results", "20260729022634_session_fork_boundary",
  "20260730195856_optional_session_title",
]

function previousSchema(db: EffectDrizzleSqlite.EffectSQLiteDatabase) {
  return Effect.gen(function* () {
    yield* db.run(`CREATE TABLE project (id TEXT PRIMARY KEY, worktree TEXT NOT NULL,
      vcs TEXT, name TEXT, icon_url TEXT, icon_url_override TEXT, icon_color TEXT,
      time_created INTEGER NOT NULL, time_updated INTEGER NOT NULL, time_initialized INTEGER,
      sandboxes TEXT NOT NULL, commands TEXT)`)
    yield* db.run(`CREATE TABLE session (id TEXT PRIMARY KEY, project_id TEXT NOT NULL REFERENCES project(id),
      workspace_id TEXT, parent_id TEXT, fork_session_id TEXT, fork_boundary TEXT, slug TEXT NOT NULL,
      directory TEXT NOT NULL, path TEXT, title TEXT, version TEXT NOT NULL, share_url TEXT,
      summary_additions INTEGER, summary_deletions INTEGER, summary_files INTEGER, summary_diffs TEXT,
      metadata TEXT, cost REAL NOT NULL DEFAULT 0, tokens_input INTEGER NOT NULL DEFAULT 0,
      tokens_output INTEGER NOT NULL DEFAULT 0, tokens_reasoning INTEGER NOT NULL DEFAULT 0,
      tokens_cache_read INTEGER NOT NULL DEFAULT 0, tokens_cache_write INTEGER NOT NULL DEFAULT 0,
      revert TEXT, permission TEXT, agent TEXT, model TEXT, time_created INTEGER NOT NULL,
      time_updated INTEGER NOT NULL, time_compacting INTEGER, time_archived INTEGER, time_suspended INTEGER)`)
    yield* db.run(`CREATE TABLE session_message (id TEXT PRIMARY KEY,
      session_id TEXT NOT NULL REFERENCES session(id), type TEXT NOT NULL, seq INTEGER NOT NULL,
      time_created INTEGER NOT NULL, time_updated INTEGER NOT NULL, data TEXT NOT NULL)`)
    yield* db.run("CREATE TABLE instruction_blob (hash TEXT PRIMARY KEY, value TEXT)")
    yield* db.run(`CREATE TABLE instruction_entry (session_id TEXT NOT NULL REFERENCES session(id),
      key TEXT NOT NULL, value TEXT, removed INTEGER NOT NULL DEFAULT 0,
      time_created INTEGER NOT NULL, time_updated INTEGER NOT NULL, PRIMARY KEY (session_id, key))`)
    yield* db.run(`CREATE TABLE instruction_state (session_id TEXT PRIMARY KEY REFERENCES session(id),
      epoch_start INTEGER NOT NULL, through_seq INTEGER NOT NULL, initial_values TEXT NOT NULL, current_values TEXT NOT NULL)`)
    yield* db.run(`CREATE TABLE session_pending (id TEXT PRIMARY KEY, session_id TEXT NOT NULL REFERENCES session(id),
      type TEXT NOT NULL, data TEXT NOT NULL, delivery TEXT, admitted_seq INTEGER NOT NULL, time_created INTEGER NOT NULL)`)
    yield* db.run("INSERT INTO project (id, worktree, time_created, time_updated, sandboxes) VALUES ('project', '.', 1, 1, '[]')")
    yield* db.run(`INSERT INTO session (id, project_id, slug, directory, version, time_created, time_updated)
      VALUES ('session', 'project', 'session', '.', 'test', 1, 1)`)
    yield* db.run(`INSERT INTO session_message VALUES ('message', 'session', 'user', 1, 1, 1, '{"text":"keep"}')`)
    yield* db.run(`INSERT INTO instruction_state VALUES ('session', 1, 1, '{}', '{}')`)
    yield* db.run(`INSERT INTO session_pending VALUES ('pending', 'session', 'user', '{}', 'queue', 2, 1)`)
  })
}

function previousLedger(db: EffectDrizzleSqlite.EffectSQLiteDatabase, format: string) {
  return Effect.gen(function* () {
    expect(previousIDs).toHaveLength(57)
    expect<readonly string[]>(DatabaseLineage.previousNativeMigrationIDs).toEqual(previousIDs)
    yield* db.run("CREATE TABLE migration (id TEXT PRIMARY KEY)")
    yield* db.run("CREATE TABLE __drizzle_migrations (name TEXT, created_at INTEGER)")
    for (const [index, id] of previousIDs.entries()) {
      if (format === "named" || (format === "split" && index % 2 === 0)) {
        yield* db.run(sql`INSERT INTO migration VALUES (${id})`)
        continue
      }
      if (format === "split") {
        yield* db.run(sql`INSERT INTO __drizzle_migrations (name) VALUES (${id})`)
        continue
      }
      const timestamp = Date.UTC(Number(id.slice(0, 4)), Number(id.slice(4, 6)) - 1,
        Number(id.slice(6, 8)), Number(id.slice(8, 10)), Number(id.slice(10, 12)), Number(id.slice(12, 14)))
      yield* db.run(sql`INSERT INTO __drizzle_migrations (created_at) VALUES (${timestamp})`)
    }
  })
}

test.each(["named", "split", "timestamp"])("complete P57 %s ledger recognizes previous-native before WS without writes", (format) => run((db) => Effect.gen(function* () {
  yield* previousSchema(db)
  yield* previousLedger(db, format)
  const before = yield* db.all(sql`SELECT * FROM sqlite_master`)
  const result = yield* DatabaseLineage.inspect(db, [...DatabaseMigration.currentMigrationIDs])
  expect(result.previousNative).toBe(true)
  expect(result.ws).toBe(false)
  expect(result.baselineComplete).toBe(true)
  expect(result.completed.size).toBe(57)
  expect(result.tables.has("session_v2")).toBe(false)
  expect(yield* DatabaseLineage.requireNative(db, [...DatabaseMigration.currentMigrationIDs]).pipe(Effect.flip))
    .toBeInstanceOf(DatabaseLineage.UnsafeMigrationHistoryError)
  expect(yield* db.all(sql`SELECT * FROM sqlite_master`)).toEqual(before)
  expect(yield* db.all(sql`SELECT data FROM session_message`)).toEqual([{ data: '{"text":"keep"}' }])
  expect(yield* db.all(sql`SELECT id FROM session_pending`)).toEqual([{ id: "pending" }])
})))

test.each([
  "DELETE FROM migration WHERE id = '20260702134641_add_session_context_entry'",
  "DELETE FROM migration WHERE id = '20260730195856_optional_session_title'",
  "INSERT INTO migration VALUES ('v01_baseline')",
  "INSERT INTO migration VALUES ('v02_session_message_part')",
  "INSERT INTO migration VALUES ('20260802133449_session_message_part')",
  "INSERT INTO migration VALUES ('20260804233008_loose_psylocke')",
  "INSERT INTO migration VALUES ('20260702134641_unknown')",
  "INSERT INTO __drizzle_migrations (created_at) VALUES (1)",
  "DROP TABLE instruction_blob",
  "DROP TABLE instruction_entry",
  "DROP TABLE session_pending",
  "ALTER TABLE instruction_state DROP COLUMN initial_values",
  "ALTER TABLE instruction_state DROP COLUMN current_values",
  "ALTER TABLE instruction_state DROP COLUMN epoch_start",
  "ALTER TABLE instruction_state DROP COLUMN through_seq",
  "ALTER TABLE session DROP COLUMN fork_boundary",
  "ALTER TABLE session DROP COLUMN time_suspended",
  "CREATE TABLE session_v2 (id TEXT PRIMARY KEY)",
])("rejects incomplete or mixed P evidence: %s", (change) => run((db) => Effect.gen(function* () {
  yield* previousSchema(db)
  yield* previousLedger(db, "named")
  yield* db.run(change)
  const before = yield* db.all(sql`SELECT * FROM migration`)
  expect(yield* DatabaseLineage.inspect(db, [...DatabaseMigration.currentMigrationIDs]).pipe(Effect.flip))
    .toBeInstanceOf(DatabaseLineage.UnsafeMigrationHistoryError)
  expect(yield* db.all(sql`SELECT * FROM migration`)).toEqual(before)
  expect(yield* db.all(sql`SELECT data FROM session_message`)).toEqual([{ data: '{"text":"keep"}' }])
})))

test("P marker and toy instruction table are not sufficient evidence", () => run((db) => Effect.gen(function* () {
  yield* db.run("CREATE TABLE instruction_state (session_id TEXT)")
  yield* db.run("CREATE TABLE migration (id TEXT)")
  yield* db.run("INSERT INTO migration VALUES ('20260730195856_optional_session_title')")
  expect(yield* DatabaseLineage.inspect(db, []).pipe(Effect.flip)).toBeInstanceOf(DatabaseLineage.UnsafeMigrationHistoryError)
})))

test("complete P57 ledger does not excuse toy tables or incorrect references", () => run((db) => Effect.gen(function* () {
  yield* previousLedger(db, "named")
  expect(yield* DatabaseLineage.inspect(db, []).pipe(Effect.flip)).toBeInstanceOf(DatabaseLineage.UnsafeMigrationHistoryError)
  yield* previousSchema(db)
  yield* db.run("DROP TABLE instruction_state")
  yield* db.run(`CREATE TABLE instruction_state (session_id TEXT PRIMARY KEY REFERENCES project(id),
    epoch_start INTEGER NOT NULL, through_seq INTEGER NOT NULL, initial_values TEXT NOT NULL, current_values TEXT NOT NULL)`)
  expect(yield* DatabaseLineage.inspect(db, []).pipe(Effect.flip)).toBeInstanceOf(DatabaseLineage.UnsafeMigrationHistoryError)
})))

test("P57 file inspection and blocked native replay preserve source bytes", async () => {
  await using directory = await tmpdir()
  const filename = `${directory.path}/previous.sqlite`
  await Effect.runPromise(Effect.gen(function* () {
    const db = yield* EffectDrizzleSqlite.makeWithDefaults()
    yield* previousSchema(db)
    yield* previousLedger(db, "split")
  }).pipe(Effect.provide(sqliteLayer({ filename })), Effect.scoped))
  const before = await Bun.file(filename).bytes()
  await Effect.runPromise(Effect.gen(function* () {
    const db = yield* EffectDrizzleSqlite.makeWithDefaults()
    expect((yield* DatabaseLineage.inspect(db, [...DatabaseMigration.currentMigrationIDs])).previousNative).toBe(true)
    expect(yield* DatabaseMigration.preflight(db).pipe(Effect.flip)).toBeInstanceOf(DatabaseLineage.UnsafeMigrationHistoryError)
  }).pipe(Effect.provide(sqliteLayer({ filename })), Effect.scoped))
  expect(await Bun.file(filename).bytes()).toEqual(before)
})

test("fresh and current native remain native with retained source tables", () => run((db) => Effect.gen(function* () {
  expect((yield* DatabaseLineage.requireNative(db, [...DatabaseMigration.currentMigrationIDs])).previousNative).toBe(false)
  yield* previousSchema(db)
  yield* db.run("CREATE TABLE session_v2 (id TEXT PRIMARY KEY)")
  yield* db.run("CREATE TABLE migration (id TEXT PRIMARY KEY)")
  for (const id of DatabaseMigration.currentMigrationIDs)
    yield* db.run(sql`INSERT INTO migration VALUES (${id})`)
  const result = yield* DatabaseLineage.requireNative(db, [...DatabaseMigration.currentMigrationIDs])
  expect(result.previousNative).toBe(false)
  expect(result.ws).toBe(false)
})))

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

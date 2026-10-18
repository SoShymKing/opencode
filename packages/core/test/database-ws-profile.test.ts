import { expect, test } from "bun:test"
import { sql } from "drizzle-orm"
import { Cause, Effect } from "effect"
import { sqliteLayer } from "../src/database/sqlite.bun"
import { EffectDrizzleSqlite } from "../src/database/drizzle"
import { Database } from "../src/database/database"
import { DatabaseMigration } from "../src/database/migration"
import { V1Migration } from "../src/database/v1-migration"
import { DatabaseWsArchive } from "../src/database/ws-archive"
import { WsNativeProfile } from "../src/database/ws-native-profile"
import { DatabaseArtifact } from "../src/database/artifact"
import { DatabaseLease } from "../src/database/lease"
import { Global } from "@opencode/util/global"
import { tmpdir } from "./fixture/tmpdir"
import { tempGlobalLayer } from "./fixture/global"
import { setupWs, setupPreviousNative, sessionID } from "./fixture/database-ws"
import path from "node:path"

test("imports committed WAL facts while a source writer holds a shared lease", async () => {
  await using temp = await tmpdir()
  const filename = path.join(temp.path, "source.db")
  await Effect.runPromise(Effect.gen(function* () {
    yield* DatabaseLease.shared(filename)
    const source = yield* EffectDrizzleSqlite.makeWithDefaults()
    yield* source.run(sql`PRAGMA journal_mode = WAL`)
    yield* setupWs(source, { directory: temp.path })
    yield* source.run(sql`UPDATE project SET name = 'committed-in-wal'`)
    const before = yield* Effect.promise(() => Bun.file(filename).bytes())
    const wal = yield* Effect.promise(() => Bun.file(`${filename}-wal`).bytes())
    yield* Effect.gen(function* () {
      const target = yield* EffectDrizzleSqlite.makeWithDefaults()
      yield* DatabaseMigration.apply(target)
      expect(yield* WsNativeProfile.run(target, filename)).toBe(true)
      expect(yield* target.get(sql`SELECT name FROM project WHERE id = 'project'`)).toEqual({ name: "committed-in-wal" })
    }).pipe(Effect.provide(sqliteLayer({ filename: ":memory:" })), Effect.scoped)
    expect(yield* Effect.promise(() => Bun.file(filename).bytes())).toEqual(before)
    expect(yield* Effect.promise(() => Bun.file(`${filename}-wal`).bytes())).toEqual(wal)
  }).pipe(Effect.provide(sqliteLayer({ filename })), Effect.provide(tempGlobalLayer), Effect.scoped))
})

test("rejects an exclusive source lease before opening a profile snapshot", async () => {
  await using temp = await tmpdir()
  const filename = path.join(temp.path, "source.db")
  await Effect.runPromise(Effect.gen(function* () {
    const db = yield* EffectDrizzleSqlite.makeWithDefaults()
    yield* setupWs(db, { directory: temp.path })
  }).pipe(Effect.provide(sqliteLayer({ filename })), Effect.scoped))
  const before = await Bun.file(filename).bytes()
  await Effect.runPromise(Effect.gen(function* () {
    const db = yield* EffectDrizzleSqlite.makeWithDefaults()
    yield* DatabaseMigration.apply(db)
    yield* Effect.scoped(Effect.gen(function* () {
      yield* DatabaseLease.exclusive(filename)
      const result = yield* WsNativeProfile.run(db, filename).pipe(Effect.exit)
      expect(result._tag).toBe("Failure")
      if (result._tag === "Failure") expect(Cause.squash(result.cause)).toBeInstanceOf(DatabaseLease.DatabaseLeaseError)
      expect(yield* db.all(sql`SELECT * FROM _ws_archive`)).toEqual([])
    }))
    expect(yield* WsNativeProfile.run(db, filename)).toBe(true)
  }).pipe(Effect.provide(sqliteLayer({ filename: ":memory:" })), Effect.provide(tempGlobalLayer), Effect.scoped))
  expect(await Bun.file(filename).bytes()).toEqual(before)
})

test("recognizes the same source snapshot fingerprint before converting it again", async () => {
  await using temp = await tmpdir()
  const filename = path.join(temp.path, "source.db")
  const snapshot = path.join(temp.path, "snapshot.db")
  await Effect.runPromise(Effect.gen(function* () {
    const db = yield* EffectDrizzleSqlite.makeWithDefaults()
    yield* setupWs(db, { directory: temp.path })
    yield* DatabaseArtifact.snapshot(db, snapshot)
  }).pipe(Effect.provide(sqliteLayer({ filename })), Effect.scoped))
  const fingerprint = await Effect.runPromise(DatabaseArtifact.digest(snapshot))
  const before = await Bun.file(filename).bytes()
  await Effect.runPromise(Effect.gen(function* () {
    const db = yield* EffectDrizzleSqlite.makeWithDefaults()
    yield* DatabaseMigration.apply(db)
    const key = `migration.ws-profile.${fingerprint.sha256}`
    yield* db.run(sql`INSERT INTO kv (key, value, time_created, time_updated) VALUES (${key}, '{"completed":true}', 1, 1)`)
    expect(yield* WsNativeProfile.run(db, filename)).toBe(true)
    expect(yield* db.all(sql`SELECT key FROM kv`)).toEqual([{ key }])
    expect(yield* db.all(sql`SELECT * FROM _ws_archive`)).toEqual([])
  }).pipe(Effect.provide(sqliteLayer({ filename: ":memory:" })), Effect.provide(tempGlobalLayer), Effect.scoped))
  expect(await Bun.file(filename).bytes()).toEqual(before)
})

test.each(["none", "project", "session", "account"])("transfers staged facts atomically with %s collision", async (collision) => {
  await using temp = await tmpdir()
  const filename = path.join(temp.path, "staged.db")
  const archive = await Effect.runPromise(Effect.gen(function* () {
    const db = yield* EffectDrizzleSqlite.makeWithDefaults()
    yield* db.run(sql`CREATE TABLE source_fact (bytes BLOB, exact INTEGER)`)
    yield* db.run(sql`CREATE UNIQUE INDEX source_fact_exact ON source_fact(exact)`)
    yield* db.run(sql`CREATE TABLE source_child (exact INTEGER REFERENCES source_fact(exact))`)
    yield* db.run(sql`INSERT INTO source_fact (rowid, bytes, exact) VALUES (9223372036854775806, X'00FF', 9223372036854775807)`)
    yield* db.run(sql`INSERT INTO source_child VALUES (9223372036854775807)`)
    const archive = yield* db.transaction((tx) => DatabaseWsArchive.capture(tx, []))
    yield* DatabaseMigration.apply(db)
    yield* db.transaction((tx) => Effect.gen(function* () {
      yield* DatabaseWsArchive.persist(tx, archive)
      yield* tx.run(sql`INSERT INTO project (id, worktree, time_created, time_updated, sandboxes) VALUES ('imported', '/', 1, 2, '[]')`)
      yield* tx.run(sql`INSERT INTO session_v2 (id, project_id, slug, directory, version, time_created, time_updated) VALUES ('ses_imported', 'imported', 'imported', '/', '1', 1, 2)`)
      yield* tx.run(sql`INSERT INTO event_sequence VALUES ('ses_imported', 7, NULL)`)
      yield* tx.run(sql`INSERT INTO account VALUES ('acc_imported', 'imported@example.invalid', 'https://example.invalid', 'fake', 'fake', 10, 1, 2)`)
      yield* tx.run(sql`INSERT INTO account_state VALUES (1, 'acc_imported', 'org_imported')`)
      yield* tx.run(sql`INSERT INTO session_message VALUES ('msg_imported', 'ses_imported', 'user', 0, 1, 2, '{ "text": "imported", "time": {"created":1} }')`)
      yield* tx.run(sql`INSERT INTO session_inbox VALUES ('msg_pending', 'ses_imported', 'user', '{ "text": "next" }', 'queue', 7, 2)`)
      yield* tx.run(sql`INSERT INTO instruction_state (session_id, epoch_start, through_seq, initial_values, current_values) VALUES ('ses_imported', 0, 0, '{}', '{}')`)
      yield* DatabaseWsArchive.persistRoot(tx, { aggregate_id: "ses_imported", archive_id: archive.id, through_seq: 7, codec_version: 1 })
    }))
    return archive
  }).pipe(Effect.provide(sqliteLayer({ filename })), Effect.provide(tempGlobalLayer), Effect.scoped))
  await Effect.runPromise(Effect.gen(function* () {
    const db = yield* EffectDrizzleSqlite.makeWithDefaults()
    yield* DatabaseMigration.apply(db)
    yield* db.run(sql`PRAGMA foreign_keys = ON`)
    yield* db.run(sql`INSERT INTO event_sequence VALUES ('native', 9, 'native-owner')`)
    yield* db.run(sql`INSERT INTO event VALUES ('evt_native', 'native', 9, 1, 'session.renamed.1', '{}')`)
    yield* db.run(sql`INSERT INTO account VALUES (${collision === "account" ? "acc_imported" : "acc_native"}, 'native@example.invalid', 'https://example.invalid', 'fake-native', 'fake-native', 10, 1, 2)`)
    yield* db.run(sql`INSERT INTO account_state VALUES (1, ${collision === "account" ? "acc_imported" : "acc_native"}, 'org_native')`)
    if (collision !== "none") {
      yield* db.run(sql`INSERT INTO project (id, worktree, time_created, time_updated, sandboxes) VALUES (${collision === "project" ? "imported" : "native"}, '/native', 1, 2, '[]')`)
      if (collision === "session")
        yield* db.run(sql`INSERT INTO session_v2 (id, project_id, slug, directory, version, time_created, time_updated) VALUES ('ses_imported', 'native', 'native', '/native', '1', 1, 2)`)
      const before = yield* db.all(sql`SELECT * FROM sqlite_schema ORDER BY name`)
      const result = yield* WsNativeProfile.merge(db, filename, "migration.ws-profile.test").pipe(Effect.exit)
      expect(result._tag).toBe("Failure")
      if (result._tag === "Failure") expect(Cause.pretty(result.cause)).toContain("constraint")
      expect(yield* db.all(sql`SELECT * FROM sqlite_schema ORDER BY name`)).toEqual(before)
      expect(yield* db.all(sql`SELECT * FROM _ws_archive`)).toEqual([])
      expect(yield* db.all(sql`SELECT * FROM _ws_import_root`)).toEqual([])
      expect(yield* db.get(sql`SELECT key FROM kv WHERE key = 'migration.ws-profile.test'`)).toBeUndefined()
      expect(yield* db.all(sql`SELECT id FROM event`)).toEqual([{ id: "evt_native" }])
      return
    }
    yield* db.transaction((tx) => DatabaseWsArchive.persist(tx, {
      id: "existing-archive", version: 1, lineage_ids: [], objects: [], tables: [],
    }))
    yield* WsNativeProfile.merge(db, filename, "migration.ws-profile.test")
    const retained = DatabaseWsArchive.sourceTable(archive, "source_fact")
    if (!retained) throw new Error("Missing retained table")
    expect(yield* db.get(sql`SELECT CAST(rowid AS TEXT) AS rowid, hex(bytes) AS bytes, CAST(exact AS TEXT) AS exact FROM ${DatabaseWsArchive.identifier(retained)}`)).toEqual({ rowid: "9223372036854775806", bytes: "00FF", exact: "9223372036854775807" })
    expect(yield* DatabaseWsArchive.read(db, archive.id)).toEqual(archive)
    expect(yield* db.get(sql`SELECT active_account_id, active_org_id FROM account_state`)).toEqual({ active_account_id: "acc_native", active_org_id: "org_native" })
    expect(yield* db.get(sql`SELECT email FROM account WHERE id = 'acc_imported'`)).toEqual({ email: "imported@example.invalid" })
    expect(yield* db.get(sql`SELECT data FROM session_message WHERE id = 'msg_imported'`)).toEqual({ data: '{ "text": "imported", "time": {"created":1} }' })
    expect(yield* db.get(sql`SELECT payload, delivery, enqueued_seq FROM session_inbox WHERE id = 'msg_pending'`)).toEqual({ payload: '{ "text": "next" }', delivery: "queue", enqueued_seq: 7 })
    expect(yield* db.get(sql`SELECT initial_values, current_values FROM instruction_state WHERE session_id = 'ses_imported'`)).toEqual({ initial_values: "{}", current_values: "{}" })
    expect(yield* db.all(sql`SELECT id FROM event`)).toEqual([{ id: "evt_native" }])
    expect(yield* db.get(sql`SELECT seq, owner_id FROM event_sequence WHERE aggregate_id = 'native'`)).toEqual({ seq: 9, owner_id: "native-owner" })
    yield* WsNativeProfile.merge(db, filename, "migration.ws-profile.test")
    expect(yield* db.get(sql`SELECT count(*) AS count FROM _ws_archive`)).toEqual({ count: 2 })
    expect((yield* db.run(sql`UPDATE ${DatabaseWsArchive.identifier(retained)} SET exact = 0`).pipe(Effect.exit))._tag).toBe("Failure")
    const before = yield* db.all(sql`SELECT * FROM sqlite_schema ORDER BY name`)
    expect((yield* WsNativeProfile.merge(db, filename, "migration.ws-profile.other").pipe(Effect.exit))._tag).toBe("Failure")
    expect(yield* db.all(sql`SELECT * FROM sqlite_schema ORDER BY name`)).toEqual(before)
    expect(yield* db.get(sql`SELECT key FROM kv WHERE key = 'migration.ws-profile.other'`)).toBeUndefined()
  }).pipe(Effect.provide(sqliteLayer({ filename: ":memory:" })), Effect.provide(tempGlobalLayer), Effect.scoped))
})

for (const source of [setupWs, setupPreviousNative]) {
  for (const gate of ["completed", "native", "legacy"]) {
  test(`imports ${source.name} through the ${gate} V1 gate and preserves native history`, async () => {
    await using temp = await tmpdir()
    const filename = path.join(temp.path, "source.db")
    await Effect.runPromise(Effect.gen(function* () {
      const db = yield* EffectDrizzleSqlite.makeWithDefaults()
      yield* source(db, { directory: temp.path, rich: true })
      yield* db.run(sql`CREATE TABLE profile_rowids (value BLOB, exact INTEGER)`)
      yield* db.run(sql`INSERT INTO profile_rowids (rowid, value, exact) VALUES (9223372036854775806, X'00FF', 9223372036854775807)`)
    }).pipe(Effect.provide(sqliteLayer({ filename })), Effect.scoped))
    const before = await Bun.file(filename).bytes()
    await Effect.runPromise(Effect.gen(function* () {
      const db = yield* EffectDrizzleSqlite.makeWithDefaults()
      yield* DatabaseMigration.apply(db)
      yield* db.run(sql`INSERT INTO project (id, worktree, time_created, time_updated, sandboxes) VALUES ('native', '/', 1, 2, '[]')`)
      yield* db.run(sql`INSERT INTO session_v2 (id, project_id, slug, directory, version, time_created, time_updated) VALUES ('ses_native', 'native', 'native', '/', '1', 1, 2)`)
      yield* db.run(sql`UPDATE session_v2 SET time_suspended = 123, resume_attempts = 2 WHERE id = 'ses_native'`)
      yield* db.run(sql`INSERT INTO event_sequence VALUES ('ses_native', 9, 'native-owner')`)
      yield* db.run(sql`INSERT INTO event VALUES ('evt_native', 'ses_native', 9, 1, 'session.renamed.1', '{ "title": "native" }')`)
      yield* db.run(sql`INSERT INTO account (id, email, url, access_token, refresh_token, token_expiry, time_created, time_updated) VALUES ('acc_native', 'native@example.invalid', 'https://example.invalid', 'fake', 'fake', 10, 1, 2)`)
      yield* db.run(sql`INSERT INTO account_state (id, active_account_id, active_org_id) VALUES (1, 'acc_native', 'org_native')`)
      if (gate === "completed") yield* db.run(sql`INSERT INTO kv (key, value, time_created, time_updated) VALUES ('migration.v1-v2', '{"phase":"completed"}', 1, 1)`)
      if (gate === "legacy") {
        yield* db.run(sql`CREATE TABLE session (id TEXT PRIMARY KEY, project_id TEXT)`)
        yield* db.run(sql`INSERT INTO session VALUES (${sessionID}, 'project'), ('ses_native', 'native')`)
      }
      const run = V1Migration.run({ nextDatabasePath: filename }).pipe(Effect.provideService(Database.Service, { db }))
      yield* run
      expect(yield* db.get(sql`SELECT id FROM session_v2 WHERE id = ${sessionID}`)).toEqual({ id: sessionID })
      expect(yield* db.get(sql`SELECT seq, owner_id FROM event_sequence WHERE aggregate_id = 'ses_native'`)).toEqual({ seq: 9, owner_id: "native-owner" })
      expect(yield* db.get(sql`SELECT time_suspended, resume_attempts FROM session_v2 WHERE id = 'ses_native'`)).toEqual({ time_suspended: 123, resume_attempts: 2 })
      expect(yield* db.get(sql`SELECT data FROM event WHERE id = 'evt_native'`)).toEqual({ data: '{ "title": "native" }' })
      expect(yield* db.all(sql`SELECT id FROM event`)).toEqual([{ id: "evt_native" }])
      expect(yield* db.get(sql`SELECT count(*) AS count FROM _ws_archive`)).toEqual({ count: 1 })
      const roots = yield* db.all(sql`SELECT * FROM _ws_import_root`)
      expect(roots.length).toBeGreaterThan(0)
      expect(yield* db.get(sql`SELECT codec_version FROM _ws_import_root WHERE aggregate_id = ${sessionID}`)).toEqual({ codec_version: source === setupWs ? 1 : 2 })
      expect(yield* db.get(sql`SELECT count(*) AS count FROM session_inbox WHERE session_id = ${sessionID}`)).toEqual({ count: source === setupWs ? 2 : 3 })
      expect((yield* db.all(sql`SELECT * FROM session_message_part`)).length).toBeGreaterThan(0)
      expect(yield* db.get(sql`SELECT session_id FROM instruction_state WHERE session_id = ${sessionID}`)).toEqual({ session_id: sessionID })
      expect(yield* db.get(sql`SELECT id FROM workspace WHERE id = 'wrk_fixture'`)).toEqual({ id: "wrk_fixture" })
      expect(yield* db.get(sql`SELECT id FROM credential WHERE id = 'cred_fixture'`)).toEqual({ id: "cred_fixture" })
      expect(yield* db.get(sql`SELECT active_account_id FROM account_state WHERE id = 1`)).toEqual({ active_account_id: "acc_native" })
      const archiveID = yield* db.get<{ id: string }>(sql`SELECT id FROM _ws_archive`)
      if (!archiveID) throw new Error("Missing archive")
      const archive = yield* DatabaseWsArchive.read(db, archiveID.id)
      if (!archive) throw new Error("Missing manifest")
      const extension = DatabaseWsArchive.sourceTable(archive, "fixture_extension")
      const events = DatabaseWsArchive.sourceTable(archive, "event")
      const rowids = DatabaseWsArchive.sourceTable(archive, "profile_rowids")
      if (!extension || !events || !rowids) throw new Error("Missing retained source tables")
      expect(yield* db.get(sql`SELECT hex(bytes) AS bytes, absent FROM ${DatabaseWsArchive.identifier(extension)}`)).toEqual({ bytes: "00017F80FF004142", absent: null })
      expect((yield* db.all(sql`SELECT * FROM ${DatabaseWsArchive.identifier(events)}`)).length).toBeGreaterThan(0)
      expect(yield* db.get(sql`SELECT CAST(rowid AS TEXT) AS rowid, hex(value) AS bytes, CAST(exact AS TEXT) AS exact FROM ${DatabaseWsArchive.identifier(rowids)}`)).toEqual({ rowid: "9223372036854775806", bytes: "00FF", exact: "9223372036854775807" })
      yield* run
      expect(yield* db.get(sql`SELECT count(*) AS count FROM _ws_archive`)).toEqual({ count: 1 })
      expect(yield* db.all(sql`SELECT * FROM _ws_import_root`)).toEqual(roots)
    }).pipe(
      Effect.provideService(Global.Service, Global.make({ data: temp.path })),
      Effect.provide(sqliteLayer({ filename: ":memory:" })), Effect.scoped,
    ))
    expect(await Bun.file(filename).bytes()).toEqual(before)
  })
  }
}

for (const collision of ["project", "session"]) {
  test(`rolls back the complete profile on a ${collision} collision`, async () => {
    await using temp = await tmpdir()
    const filename = path.join(temp.path, "source.db")
    await Effect.runPromise(Effect.gen(function* () {
      const db = yield* EffectDrizzleSqlite.makeWithDefaults()
      yield* setupWs(db, { directory: temp.path, rich: true })
    }).pipe(Effect.provide(sqliteLayer({ filename })), Effect.scoped))
    await Effect.runPromise(Effect.gen(function* () {
      const db = yield* EffectDrizzleSqlite.makeWithDefaults()
      yield* DatabaseMigration.apply(db)
      yield* db.run(sql`INSERT INTO project (id, worktree, time_created, time_updated, sandboxes) VALUES (${collision === "project" ? "project" : "native"}, '/native', 1, 2, '[]')`)
      if (collision === "session")
        yield* db.run(sql`INSERT INTO session_v2 (id, project_id, slug, directory, version, time_created, time_updated) VALUES (${sessionID}, 'native', 'native', '/native', '1', 1, 2)`)
      const schema = yield* db.all(sql`SELECT * FROM sqlite_schema ORDER BY name`)
      const result = yield* V1Migration.run({ nextDatabasePath: filename }).pipe(Effect.provideService(Database.Service, { db }), Effect.exit)
      expect(result._tag).toBe("Failure")
      if (result._tag === "Failure") expect(Cause.pretty(result.cause)).toContain("constraint")
      expect(yield* db.all(sql`SELECT * FROM sqlite_schema ORDER BY name`)).toEqual(schema)
      expect(yield* db.all(sql`SELECT * FROM credential`)).toEqual([])
      expect(yield* db.all(sql`SELECT key FROM kv WHERE key LIKE 'migration.ws-profile.%'`)).toEqual([])
      expect(yield* db.get(sql`SELECT worktree FROM project`)).toEqual({ worktree: "/native" })
    }).pipe(
      Effect.provideService(Global.Service, Global.make({ data: temp.path })),
      Effect.provide(sqliteLayer({ filename: ":memory:" })), Effect.scoped,
    ))
  })
}

test("V1 cleanup preserves native events and imported roots while clearing stale orphans", async () => {
  await using temp = await tmpdir()
  await Effect.runPromise(Effect.gen(function* () {
    const db = yield* EffectDrizzleSqlite.makeWithDefaults()
    yield* DatabaseMigration.apply(db)
    yield* db.run(sql`CREATE TABLE session (id TEXT PRIMARY KEY, project_id TEXT)`)
    yield* db.run(sql`CREATE TABLE IF NOT EXISTS _ws_archive (id TEXT PRIMARY KEY, manifest TEXT NOT NULL)`)
    yield* db.run(sql`INSERT INTO _ws_archive VALUES ('archive', '{}')`)
    yield* db.run(sql`CREATE TABLE IF NOT EXISTS _ws_import_root (aggregate_id TEXT PRIMARY KEY, archive_id TEXT, through_seq INTEGER, codec_version INTEGER)`)
    yield* db.run(sql`INSERT INTO _ws_import_root VALUES ('ses_imported', 'archive', 7, 1)`)
    yield* db.run(sql`INSERT INTO session VALUES ('ses_imported', 'native'), ('ses_native', 'native')`)
    yield* db.run(sql`INSERT INTO project (id, worktree, time_created, time_updated, sandboxes) VALUES ('native', '/', 1, 2, '[]')`)
    yield* db.run(sql`INSERT INTO session_v2 (id, project_id, slug, directory, version, time_created, time_updated) VALUES ('ses_native', 'native', 'native', '/', '1', 1, 2)`)
    for (const id of ["ses_native", "ses_imported", "owned", "stale"]) {
      yield* db.run(sql`INSERT INTO event_sequence VALUES (${id}, 7, ${id === "owned" || id === "ses_native" ? "native-owner" : null})`)
      yield* db.run(sql`INSERT INTO event VALUES (${`evt_${id}`}, ${id}, 7, 1, 'session.renamed.1', '{}')`)
    }
    yield* V1Migration.run().pipe(Effect.provideService(Database.Service, { db }))
    expect(yield* db.all(sql`SELECT aggregate_id FROM event ORDER BY aggregate_id`)).toEqual([{ aggregate_id: "owned" }, { aggregate_id: "ses_imported" }, { aggregate_id: "ses_native" }])
    expect(yield* db.get(sql`SELECT seq FROM event_sequence WHERE aggregate_id = 'ses_imported'`)).toEqual({ seq: 7 })
    expect(yield* db.get(sql`SELECT seq, owner_id FROM event_sequence WHERE aggregate_id = 'ses_native'`)).toEqual({ seq: 7, owner_id: "native-owner" })
  }).pipe(
    Effect.provideService(Global.Service, Global.make({ data: temp.path })),
    Effect.provide(sqliteLayer({ filename: ":memory:" })), Effect.scoped,
  ))
})

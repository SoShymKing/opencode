import { expect, test } from "bun:test"
import { SqliteClient } from "@effect/sql-sqlite-bun"
import { sql } from "drizzle-orm"
import { Effect } from "effect"
import type { SqlClient } from "effect/sql/SqlClient"
import { EffectDrizzleSqlite } from "@opencode/core/database/drizzle"
import { DatabaseWsArchive } from "@opencode/core/database/ws-archive"

const raw = '{ "n":9007199254740993, "exponent":1.2300e+40, "escaped":"\\u0061\\/" }'
const run = <A, E>(effect: Effect.Effect<A, E, SqlClient>) => Effect.runPromise(effect.pipe(
  Effect.provide(SqliteClient.layer({ filename: ":memory:", disableWAL: true })), Effect.scoped,
))
const source = Effect.gen(function* () {
  const db = yield* EffectDrizzleSqlite.makeWithDefaults()
  yield* db.run(sql`PRAGMA foreign_keys = ON`)
  yield* db.run(sql`CREATE TABLE event_sequence (aggregate_id TEXT PRIMARY KEY, seq INTEGER, owner_id TEXT)`)
  yield* db.run(sql`CREATE TABLE event (id TEXT PRIMARY KEY, aggregate_id TEXT REFERENCES event_sequence(aggregate_id), seq INTEGER, type TEXT, data TEXT, extra BLOB)`)
  yield* db.run(sql`CREATE UNIQUE INDEX "event_aggregate_seq_idx" ON event(aggregate_id, seq)`)
  yield* db.run(sql`CREATE TABLE migration (id TEXT PRIMARY KEY, extra TEXT)`)
  yield* db.run(sql`CREATE TABLE parent (code TEXT)`)
  yield* db.run(sql`CREATE UNIQUE INDEX "quoted"" ON index" ON parent(code)`)
  yield* db.run(sql`CREATE TABLE child (code TEXT REFERENCES parent(code))`)
  yield* db.run(sql`INSERT INTO parent VALUES ('retained')`)
  yield* db.run(sql`INSERT INTO child VALUES ('retained')`)
  yield* db.run(sql`CREATE TABLE "odd""; table" (id INTEGER PRIMARY KEY AUTOINCREMENT, "column"" name", nullable, exact)`)
  yield* db.run(sql`CREATE VIEW "source view" AS SELECT id, data FROM event`)
  yield* db.run(sql`CREATE TRIGGER "source trigger" AFTER INSERT ON migration BEGIN UPDATE event_sequence SET seq = seq; END`)
  yield* db.run(sql`INSERT INTO event_sequence VALUES ('aggregate', 9, 'source-owner')`)
  yield* db.run(sql`INSERT INTO event VALUES ('original-1', 'aggregate', 2, 'session.created.1', ${raw}, X'00FF80')`)
  yield* db.run(sql`INSERT INTO event VALUES ('original-2', 'aggregate', 9, 'session.updated.3', '{}', NULL)`)
  yield* db.run(sql`INSERT INTO migration VALUES ('source-migration', 'retained')`)
  yield* db.run(sql`INSERT INTO "odd""; table" VALUES (11, X'00FF', NULL, 9223372036854775807)`)
  return db
})
const native = (tx: Parameters<Parameters<EffectDrizzleSqlite.EffectSQLiteDatabase["transaction"]>[0]>[0]) =>
  Effect.gen(function* () {
    yield* tx.run(sql`CREATE TABLE _ws_archive (id TEXT PRIMARY KEY NOT NULL, manifest TEXT NOT NULL)`)
    yield* tx.run(sql`CREATE TABLE _ws_import_root (aggregate_id TEXT PRIMARY KEY NOT NULL, archive_id TEXT NOT NULL REFERENCES _ws_archive(id), through_seq INTEGER NOT NULL, codec_version INTEGER NOT NULL)`)
    yield* tx.run(sql`CREATE TABLE event (id TEXT PRIMARY KEY, aggregate_id TEXT, seq INTEGER, type TEXT, data TEXT)`)
    yield* tx.run(sql`CREATE UNIQUE INDEX event_aggregate_seq_idx ON event(aggregate_id, seq)`)
    yield* tx.run(sql`CREATE TABLE session (id TEXT PRIMARY KEY)`)
  })

test("retains exact source cells, events, lineage and schema evidence when captured", () => run(Effect.gen(function* () {
  const db = yield* source
  const evidence = yield* db.all<DatabaseWsArchive.Archive["objects"][number]>(sql`SELECT type, name, tbl_name, sql FROM sqlite_schema ORDER BY type, name`)
  const archive = yield* db.transaction((tx) => Effect.gen(function* () {
    const archive = yield* DatabaseWsArchive.capture(tx, ["source-migration"])
    yield* native(tx)
    yield* DatabaseWsArchive.persist(tx, archive)
    return archive
  }))
  expect(yield* DatabaseWsArchive.read(db, archive.id)).toEqual(archive)
  expect(archive.objects).toEqual(evidence)
  expect(archive.lineage_ids).toEqual(["source-migration"])
  expect(archive.version).toBe(1)
  const events = DatabaseWsArchive.sourceTable(archive, "event")
  const extra = DatabaseWsArchive.sourceTable(archive, 'odd"; table')
  const ledger = DatabaseWsArchive.sourceTable(archive, "migration")
  const sequence = DatabaseWsArchive.sourceTable(archive, "event_sequence")
  const internal = DatabaseWsArchive.sourceTable(archive, "sqlite_sequence")
  if (!events || !extra || !ledger || !sequence || !internal) throw new Error("Missing source table")
  expect(yield* db.all(sql`SELECT id, aggregate_id, seq, type, data, hex(extra) AS bytes, extra IS NULL AS absent FROM ${DatabaseWsArchive.identifier(events)} ORDER BY seq`)).toEqual([
    { id: "original-1", aggregate_id: "aggregate", seq: 2, type: "session.created.1", data: raw, bytes: "00FF80", absent: 0 },
    { id: "original-2", aggregate_id: "aggregate", seq: 9, type: "session.updated.3", data: "{}", bytes: "", absent: 1 },
  ])
  expect(yield* db.all(sql`SELECT id, hex("column"" name") AS bytes, nullable, CAST(exact AS TEXT) AS exact FROM ${DatabaseWsArchive.identifier(extra)}`)).toEqual([
    { id: 11, bytes: "00FF", nullable: null, exact: "9223372036854775807" },
  ])
  expect(yield* db.all(sql`SELECT * FROM ${DatabaseWsArchive.identifier(ledger)}`)).toEqual([{ id: "source-migration", extra: "retained" }])
  expect(yield* db.all(sql`SELECT * FROM ${DatabaseWsArchive.identifier(sequence)}`)).toEqual([{ aggregate_id: "aggregate", seq: 9, owner_id: "source-owner" }])
  expect(yield* db.all(sql`SELECT * FROM ${DatabaseWsArchive.identifier(internal)}`)).toEqual([{ name: 'odd"; table', seq: 11 }])
  expect(yield* db.all(sql`SELECT * FROM event`)).toEqual([])
  expect(yield* db.all(sql`PRAGMA foreign_key_check`)).toEqual([])
  expect(DatabaseWsArchive.sourceTable(archive, "missing")).toBeUndefined()
  expect(yield* DatabaseWsArchive.read(db, "missing")).toBeUndefined()
})))

test("blocks source mutation and repeat capture when an archive exists", () => run(Effect.gen(function* () {
  const db = yield* source
  const archive = yield* db.transaction((tx) => Effect.gen(function* () {
    const archive = yield* DatabaseWsArchive.capture(tx, [])
    yield* native(tx)
    yield* DatabaseWsArchive.persist(tx, archive)
    return archive
  }))
  const table = DatabaseWsArchive.sourceTable(archive, "event")
  if (!table) throw new Error("Missing source table")
  for (const statement of [
    sql`DELETE FROM ${DatabaseWsArchive.identifier(table)}`,
    sql`UPDATE ${DatabaseWsArchive.identifier(table)} SET data = '{}'`,
    sql`INSERT INTO ${DatabaseWsArchive.identifier(table)} (id) VALUES ('new')`,
    sql`DELETE FROM _ws_archive`,
    sql`UPDATE _ws_archive SET manifest = '{}'`,
    sql`INSERT OR REPLACE INTO _ws_archive VALUES (${archive.id}, '{}')`,
  ]) expect((yield* db.run(statement).pipe(Effect.exit))._tag).toBe("Failure")
  expect((yield* db.transaction((tx) => DatabaseWsArchive.capture(tx, [])).pipe(Effect.exit))._tag).toBe("Failure")
  expect((yield* db.transaction((tx) => DatabaseWsArchive.persist(tx, archive)).pipe(Effect.exit))._tag).toBe("Failure")
})))

test("retains independent immutable aggregate roots after projection deletion", () => run(Effect.gen(function* () {
  const db = yield* source
  const root = yield* db.transaction((tx) => Effect.gen(function* () {
    const archive = yield* DatabaseWsArchive.capture(tx, [])
    yield* native(tx)
    yield* DatabaseWsArchive.persist(tx, archive)
    const root = { aggregate_id: "aggregate", archive_id: archive.id, through_seq: 9, codec_version: 1 }
    yield* DatabaseWsArchive.persistRoot(tx, root)
    yield* DatabaseWsArchive.persistRoot(tx, { ...root, aggregate_id: "other", through_seq: 2 })
    yield* tx.run(sql`INSERT INTO session VALUES ('aggregate')`)
    return root
  }))
  yield* db.run(sql`DELETE FROM session`)
  yield* db.run(sql`DELETE FROM event`)
  expect(yield* DatabaseWsArchive.readRoot(db, "aggregate")).toEqual(root)
  expect(yield* DatabaseWsArchive.readRoot(db, "other")).toEqual({ ...root, aggregate_id: "other", through_seq: 2 })
  expect(yield* DatabaseWsArchive.readRoot(db, "missing")).toBeUndefined()
  expect((yield* db.run(sql`DELETE FROM _ws_import_root`).pipe(Effect.exit))._tag).toBe("Failure")
  expect((yield* db.run(sql`UPDATE _ws_import_root SET through_seq = 10`).pipe(Effect.exit))._tag).toBe("Failure")
  expect((yield* db.run(sql`INSERT OR REPLACE INTO _ws_import_root VALUES ('aggregate', ${root.archive_id}, 10, 2)`).pipe(Effect.exit))._tag).toBe("Failure")
})))

test("rejects virtual source tables before changing source schema", () => run(Effect.gen(function* () {
  const db = yield* source
  yield* db.run(sql`CREATE VIRTUAL TABLE search USING fts5(content)`)
  yield* db.run(sql`INSERT INTO search VALUES ('retained')`)
  const evidence = yield* db.all(sql`SELECT * FROM sqlite_schema ORDER BY type, name`)
  expect((yield* db.transaction((tx) => DatabaseWsArchive.capture(tx, [])).pipe(Effect.exit))._tag).toBe("Failure")
  expect(yield* db.all(sql`SELECT * FROM sqlite_schema ORDER BY type, name`)).toEqual(evidence)
  expect(yield* db.all(sql`SELECT * FROM search`)).toEqual([{ content: "retained" }])
})))

test("rolls back source renames, DDL and archive records when conversion fails", () => run(Effect.gen(function* () {
  const db = yield* source
  const evidence = yield* db.all(sql`SELECT * FROM sqlite_schema ORDER BY type, name`)
  yield* db.transaction((tx) => Effect.gen(function* () {
    const archive = yield* DatabaseWsArchive.capture(tx, ["source-migration"])
    yield* native(tx)
    yield* DatabaseWsArchive.persist(tx, archive)
    yield* DatabaseWsArchive.persistRoot(tx, { aggregate_id: "aggregate", archive_id: archive.id, through_seq: 9, codec_version: 1 })
    return yield* Effect.fail("conversion failed")
  })).pipe(Effect.ignore)
  expect(yield* db.all(sql`SELECT * FROM sqlite_schema ORDER BY type, name`)).toEqual(evidence)
  expect(yield* db.get(sql`SELECT data FROM event WHERE id = 'original-1'`)).toEqual({ data: raw })
})))

test("appends distinct archive and root records without permitting replacement", () => run(Effect.gen(function* () {
  const db = yield* source
  const first = yield* db.transaction((tx) => Effect.gen(function* () {
    const archive = yield* DatabaseWsArchive.capture(tx, ["first-lineage"])
    yield* native(tx)
    yield* DatabaseWsArchive.persist(tx, archive)
    yield* DatabaseWsArchive.persistRoot(tx, { aggregate_id: "first", archive_id: archive.id, through_seq: 9, codec_version: 1 })
    return archive
  }))
  const second: DatabaseWsArchive.Archive = { id: crypto.randomUUID(), version: 1, lineage_ids: ["second-lineage"], objects: [], tables: [] }
  const root = { aggregate_id: "second", archive_id: second.id, through_seq: 0, codec_version: 2 }
  yield* db.transaction((tx) => Effect.gen(function* () {
    yield* DatabaseWsArchive.persist(tx, second)
    yield* DatabaseWsArchive.persistRoot(tx, root)
  }))
  for (const archive of [first, second]) {
    for (const statement of [
      sql`INSERT OR REPLACE INTO _ws_archive VALUES (${archive.id}, '{}')`,
      sql`UPDATE _ws_archive SET manifest = '{}' WHERE id = ${archive.id}`,
      sql`DELETE FROM _ws_archive WHERE id = ${archive.id}`,
    ]) expect((yield* db.run(statement).pipe(Effect.exit))._tag).toBe("Failure")
    expect(yield* DatabaseWsArchive.read(db, archive.id)).toEqual(archive)
  }
  for (const aggregate of ["first", "second"]) {
    expect((yield* db.run(sql`INSERT OR REPLACE INTO _ws_import_root VALUES (${aggregate}, ${second.id}, 99, 3)`).pipe(Effect.exit))._tag).toBe("Failure")
  }
  expect(yield* DatabaseWsArchive.readRoot(db, "second")).toEqual(root)
  expect(yield* DatabaseWsArchive.readRoot(db, "first")).toEqual({ aggregate_id: "first", archive_id: first.id, through_seq: 9, codec_version: 1 })
})))

test("retains arbitrary source evidence with WS-prefixed names", () => run(Effect.gen(function* () {
  const db = yield* source
  yield* db.run(sql`CREATE TABLE _ws_archive (unknown BLOB, raw TEXT)`)
  yield* db.run(sql`INSERT INTO _ws_archive VALUES (X'00FF80', ${raw})`)
  yield* db.run(sql`CREATE INDEX _ws_custom_index ON _ws_archive(raw)`)
  yield* db.run(sql`CREATE VIEW _ws_custom_view AS SELECT * FROM _ws_archive`)
  const archive = yield* db.transaction((tx) => Effect.gen(function* () {
    const archive = yield* DatabaseWsArchive.capture(tx, [])
    yield* native(tx)
    yield* DatabaseWsArchive.persist(tx, archive)
    return archive
  }))
  const table = DatabaseWsArchive.sourceTable(archive, "_ws_archive")
  if (!table) throw new Error("Missing source table")
  expect(yield* db.all(sql`SELECT hex(unknown) AS bytes, raw FROM ${DatabaseWsArchive.identifier(table)}`)).toEqual([{ bytes: "00FF80", raw }])
  expect(archive.objects.map((object) => object.name)).toContain("_ws_custom_index")
  expect(archive.objects.map((object) => object.name)).toContain("_ws_custom_view")
  expect((yield* db.transaction((tx) => DatabaseWsArchive.capture(tx, [])).pipe(Effect.exit))._tag).toBe("Failure")
})))

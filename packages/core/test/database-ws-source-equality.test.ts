import { expect, test } from "bun:test"
import { join } from "node:path"
import { sql } from "drizzle-orm"
import { SQLiteDialect } from "drizzle-orm/sqlite-core"
import { Effect } from "effect"
import { DatabaseArtifact } from "../src/database/artifact"
import { EffectDrizzleSqlite } from "../src/database/drizzle"
import { sqliteLayer } from "../src/database/sqlite.bun"
import { WsNativeFile } from "../src/database/ws-native-file"
import { tmpdir } from "./fixture/tmpdir"

test("source equality accepts a production snapshot with an empty orphan sqlite_sequence", async () => {
  await using temp = await tmpdir()
  const source = join(temp.path, "source.db")
  const snapshot = join(temp.path, "snapshot.db")
  await Effect.runPromise(Effect.gen(function* () {
    const db = yield* EffectDrizzleSqlite.makeWithDefaults()
    yield* db.run("CREATE TABLE facts (id INTEGER PRIMARY KEY, value TEXT)")
    yield* db.run("INSERT INTO facts VALUES (1, 'retained')")
    yield* db.run("CREATE TABLE discarded (id INTEGER PRIMARY KEY AUTOINCREMENT)")
    yield* db.run("DROP TABLE discarded")
    expect(yield* db.all("SELECT * FROM sqlite_sequence")).toEqual([])
    yield* DatabaseArtifact.snapshot(db, snapshot)
    yield* db.run(sql`ATTACH DATABASE ${snapshot} AS replacement_backup`)
    expect(yield* db.all("SELECT type, name, tbl_name, sql FROM replacement_backup.sqlite_schema ORDER BY name"))
      .toEqual(yield* db.all("SELECT type, name, tbl_name, sql FROM main.sqlite_schema ORDER BY name"))
    expect(yield* db.all("SELECT * FROM replacement_backup.sqlite_sequence")).toEqual([])
  }).pipe(Effect.provide(sqliteLayer({ filename: source })), Effect.scoped))
  await WsNativeFile.requireSameSource(source, snapshot).pipe(Effect.runPromise)
})

test("source equality rejects a sequence-only mutation with a retained AUTOINCREMENT table", async () => {
  await using temp = await tmpdir()
  const source = join(temp.path, "source.db")
  const snapshot = join(temp.path, "snapshot.db")
  await Effect.runPromise(Effect.gen(function* () {
    const db = yield* EffectDrizzleSqlite.makeWithDefaults()
    yield* db.run("CREATE TABLE facts (id INTEGER PRIMARY KEY AUTOINCREMENT, value TEXT)")
    yield* db.run("INSERT INTO facts (value) VALUES ('retained')")
    yield* DatabaseArtifact.snapshot(db, snapshot)
  }).pipe(Effect.provide(sqliteLayer({ filename: source })), Effect.scoped))
  await WsNativeFile.requireSameSource(source, snapshot).pipe(Effect.runPromise)
  await Effect.runPromise(Effect.gen(function* () {
    const db = yield* EffectDrizzleSqlite.makeWithDefaults()
    yield* db.run("UPDATE sqlite_sequence SET seq = 42 WHERE name = 'facts'")
  }).pipe(Effect.provide(sqliteLayer({ filename: source })), Effect.scoped))
  await expect(WsNativeFile.requireSameSource(source, snapshot).pipe(Effect.runPromise)).rejects.toThrow("replacement target differs from its backup")
})

for (const layout of [
  { ddl: "CREATE TABLE facts (id TEXT PRIMARY KEY, value TEXT)", rows: "(2, 'one', 'retained'), (7, 'two', 'retained')" },
  { ddl: "CREATE TABLE facts (id TEXT, value TEXT)", rows: "(2, 'same', 'retained'), (7, 'same', 'retained')" },
]) test(`source equality accepts a production snapshot with gapped hidden rowids: ${layout.ddl}`, async () => {
  await using temp = await tmpdir()
  const source = join(temp.path, "source.db")
  const snapshot = join(temp.path, "snapshot.db")
  const rowids = await Effect.runPromise(Effect.gen(function* () {
    const db = yield* EffectDrizzleSqlite.makeWithDefaults()
    yield* db.run(layout.ddl)
    yield* db.run(`INSERT INTO facts (rowid, id, value) VALUES ${layout.rows}`)
    yield* DatabaseArtifact.snapshot(db, snapshot)
    yield* db.run(sql`ATTACH DATABASE ${snapshot} AS replacement_backup`)
    expect(yield* db.all("SELECT id, value FROM replacement_backup.facts ORDER BY rowid"))
      .toEqual(yield* db.all("SELECT id, value FROM main.facts ORDER BY rowid"))
    return {
      source: yield* db.all<{ rowid: number }>("SELECT rowid FROM main.facts ORDER BY rowid"),
      snapshot: yield* db.all<{ rowid: number }>("SELECT rowid FROM replacement_backup.facts ORDER BY rowid"),
    }
  }).pipe(Effect.provide(sqliteLayer({ filename: source })), Effect.scoped))
  await expect(WsNativeFile.requireSameSource(source, snapshot).pipe(Effect.runPromise), JSON.stringify(rowids)).resolves.toBeUndefined()
})

for (const encoding of ["UTF-8", "UTF-16le"]) {
  for (const change of [
    "UPDATE facts SET text_value = 'A' || char(0) || 'z'",
    "UPDATE facts SET numeric_value = CAST(1 AS REAL)",
    "UPDATE facts SET numeric_value = 9223372036854775807",
    "UPDATE facts SET text_value = CAST(text_value AS BLOB)",
    "UPDATE facts SET payload = CAST(payload AS TEXT)",
    "UPDATE facts SET payload = CAST(substr(payload, 1, 65535) || X'FF' AS BLOB)",
    "UPDATE facts SET id = 9223372036854775807",
    "DELETE FROM facts",
    "INSERT INTO facts VALUES (1, X'00', 'extra', NULL)",
    "PRAGMA user_version = 7",
    "CREATE TABLE extra (id INTEGER)",
  ]) test(`source equality rejects ${encoding}: ${change}`, async () => {
    await using temp = await tmpdir()
    const source = join(temp.path, "source.db")
    const snapshot = join(temp.path, "snapshot.db")
    await Effect.runPromise(Effect.gen(function* () {
      const db = yield* EffectDrizzleSqlite.makeWithDefaults()
      yield* db.run(sql.raw(`PRAGMA encoding = '${encoding}'`))
      yield* db.run("CREATE TABLE facts (id INTEGER PRIMARY KEY, payload BLOB, text_value TEXT COLLATE NOCASE, numeric_value)")
      yield* db.run("INSERT INTO facts VALUES (9223372036854775806, zeroblob(65536), 'A' || char(0) || 'Z', 9223372036854775806)")
      yield* DatabaseArtifact.snapshot(db, snapshot)
    }).pipe(Effect.provide(sqliteLayer({ filename: source })), Effect.scoped))
    await WsNativeFile.requireSameSource(source, snapshot).pipe(Effect.runPromise)
    await Effect.runPromise(Effect.gen(function* () {
      const db = yield* EffectDrizzleSqlite.makeWithDefaults()
      yield* db.run(change)
    }).pipe(Effect.provide(sqliteLayer({ filename: source })), Effect.scoped))
    await expect(WsNativeFile.requireSameSource(source, snapshot).pipe(Effect.runPromise)).rejects.toThrow("replacement target differs from its backup")
  })
}

const layouts = [
  { ddl: "CREATE TABLE facts (id INTEGER PRIMARY KEY, value TEXT)", rows: "(1, 'one'), (2, 'two')", indexed: true },
  { ddl: "CREATE TABLE facts (id TEXT, ordinal INTEGER, value BLOB, PRIMARY KEY(id, ordinal)) WITHOUT ROWID", rows: "('a' || char(0), 1, X'00FF'), ('b', 2, X'FF00')", indexed: true },
  { ddl: "CREATE TABLE facts (rowid, _rowid_, oid, id TEXT NOT NULL PRIMARY KEY, value)", rows: "(NULL, NULL, NULL, 'one', 1), (NULL, NULL, NULL, 'two', 2)", indexed: true },
  { ddl: "CREATE TABLE facts (rowid, _rowid_, oid, id INTEGER PRIMARY KEY, value)", rows: "(NULL, NULL, NULL, 1, 1), (NULL, NULL, NULL, 2, 2)", indexed: true },
  { ddl: "CREATE TABLE facts (rowid, _rowid_, oid, id TEXT PRIMARY KEY, value)", rows: "(NULL, NULL, NULL, NULL, 'one'), (NULL, NULL, NULL, NULL, 'two')", indexed: false },
  { ddl: "CREATE TABLE facts (rowid, _rowid_, oid, id TEXT PRIMARY KEY, value)", rows: "(NULL, NULL, NULL, 'one', 1), (NULL, NULL, NULL, 'two', 2)", indexed: true },
  { ddl: "CREATE TABLE facts (rowid, _rowid_, oid, id INTEGER PRIMARY KEY DESC, value)", rows: "(NULL, NULL, NULL, NULL, 'one'), (NULL, NULL, NULL, NULL, 'two')", indexed: false },
  { ddl: "CREATE TABLE facts (rowid, _rowid_, oid, id TEXT, ordinal INTEGER NOT NULL, value, PRIMARY KEY(id, ordinal))", rows: "(NULL, NULL, NULL, NULL, 1, 'one'), (NULL, NULL, NULL, NULL, 1, 'two')", indexed: false },
  { ddl: "CREATE TABLE facts (rowid, _rowid_, oid, value)", rows: "(NULL, NULL, NULL, 'one'), (NULL, NULL, NULL, 'one'), (NULL, NULL, NULL, 'two')", indexed: false },
]

for (const layout of layouts) test(`source equality preserves keys and multiplicity: ${layout.ddl}`, async () => {
  await using temp = await tmpdir()
  const source = join(temp.path, "source.db")
  const snapshot = join(temp.path, "snapshot.db")
  await Effect.runPromise(Effect.gen(function* () {
    const db = yield* EffectDrizzleSqlite.makeWithDefaults()
    yield* db.run(layout.ddl)
    yield* db.run(`INSERT INTO facts VALUES ${layout.rows}`)
    yield* DatabaseArtifact.snapshot(db, snapshot)
    yield* db.run(sql`ATTACH DATABASE ${snapshot} AS replacement_backup`)
    const queries: Parameters<typeof db.get>[0][] = []
    const dialect = new SQLiteDialect()
    const execute = db.get.bind(db)
    db.get = (query) => {
      const text = typeof query === "string" ? query : dialect.sqlToQuery(query.getSQL()).sql
      if (text.includes('main."facts"') && (text.includes("AS source") || text.includes("GROUP BY"))) queries.push(query)
      return execute(query)
    }
    yield* db.transaction((tx) => WsNativeFile.requireSameSnapshot(tx))
    yield* WsNativeFile.requireSameSnapshot(db)
    expect(queries.length).toBeGreaterThan(0)
    for (const query of queries) expect((typeof query === "string" ? query : dialect.sqlToQuery(query.getSQL()).sql).startsWith("SELECT 1 FROM")).toBe(true)
    if (layout.indexed) {
      for (const query of queries) {
        const statement = typeof query === "string" ? sql.raw(query) : query.getSQL()
        const text = dialect.sqlToQuery(statement).sql
        expect(text).not.toContain("GROUP BY")
        expect(text).not.toContain("hex(")
        const plan = yield* db.all<{ detail: string }>(sql`EXPLAIN QUERY PLAN ${statement}`)
        expect(plan.some((row) => row.detail.includes("TEMP B-TREE"))).toBe(false)
        expect(plan.some((row) => row.detail.includes("SEARCH snapshot USING"))).toBe(true)
      }
    }
    yield* db.run(!layout.indexed && !layout.ddl.includes("PRIMARY KEY")
      ? "UPDATE facts SET value = CASE value WHEN 'one' THEN 'two' ELSE 'one' END"
      : "UPDATE facts SET value = 'changed' WHERE value IS NOT NULL")
    expect((yield* WsNativeFile.requireSameSnapshot(db).pipe(Effect.exit))._tag).toBe("Failure")
  }).pipe(Effect.provide(sqliteLayer({ filename: source })), Effect.scoped))
})

for (const pair of [{ before: "-0.0", after: "0.0", equal: true }, { before: "0.1", after: "0.10000000000000002", equal: false }]) test(`REAL source equality keeps SQLite quote equivalence: ${pair.before}, ${pair.after}`, async () => {
  await using temp = await tmpdir()
  const source = join(temp.path, "source.db")
  const snapshot = join(temp.path, "snapshot.db")
  await Effect.runPromise(Effect.gen(function* () {
    const db = yield* EffectDrizzleSqlite.makeWithDefaults()
    yield* db.run("CREATE TABLE facts (id INTEGER PRIMARY KEY, value REAL)")
    yield* db.run(`INSERT INTO facts VALUES (1, ${pair.before})`)
    yield* DatabaseArtifact.snapshot(db, snapshot)
    yield* db.run(`UPDATE facts SET value = ${pair.after}`)
  }).pipe(Effect.provide(sqliteLayer({ filename: source })), Effect.scoped))
  expect((await WsNativeFile.requireSameSource(source, snapshot).pipe(Effect.exit, Effect.runPromise))._tag).toBe(pair.equal ? "Success" : "Failure")
})

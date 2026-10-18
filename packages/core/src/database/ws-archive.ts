export * as DatabaseWsArchive from "./ws-archive.js"

import { eq, sql } from "drizzle-orm"
import { Effect, Schema } from "effect"
import type { EffectDrizzleSqlite } from "./drizzle.js"
import { WsArchiveTable, WsImportRootTable } from "../event/sql.js"

type Database = EffectDrizzleSqlite.EffectSQLiteDatabase
type Transaction = Parameters<Parameters<Database["transaction"]>[0]>[0]

const ObjectEvidence = Schema.Struct({
  type: Schema.String,
  name: Schema.String,
  tbl_name: Schema.String,
  sql: Schema.NullOr(Schema.String),
})
const Manifest = Schema.Struct({
  id: Schema.String,
  version: Schema.Literal(1),
  lineage_ids: Schema.Array(Schema.String),
  objects: Schema.Array(ObjectEvidence),
  tables: Schema.Array(Schema.Struct({ name: Schema.String, retained: Schema.String })),
})
export type Archive = typeof Manifest.Type
export type Root = typeof WsImportRootTable.$inferInsert

export class ArchiveError extends Error {
  override readonly name = "WsArchiveError"
}

export function capture(tx: Transaction, lineageIDs: readonly string[]) {
  return Effect.gen(function* () {
    const objects = yield* tx.all<typeof ObjectEvidence.Type>(
      sql`SELECT type, name, tbl_name, sql FROM sqlite_schema ORDER BY type, name`,
    )
    if (objects.some((object) => object.type === "trigger"
      && /^_ws_source_[0-9a-f]{32}_\d+$/.test(object.tbl_name)
      && object.name === `${object.tbl_name}_insert_immutable`
      && object.sql?.includes("RAISE(ABORT, 'WS archive is immutable')")))
      return yield* Effect.fail(new ArchiveError("WS source has already been captured"))
    if (objects.some((object) => /^CREATE\s+VIRTUAL\s+TABLE\b/i.test(object.sql ?? "")))
      return yield* Effect.fail(new ArchiveError("Virtual source tables require a dedicated archive codec"))
    const id = crypto.randomUUID()
    const tables = objects.filter((object) => object.type === "table").map((object, index) => ({
      name: object.name,
      retained: `_ws_source_${id.replaceAll("-", "")}_${index}`,
    }))
    const archive: Archive = { id, version: 1, lineage_ids: [...lineageIDs], objects, tables }
    for (const table of tables.filter((table) => table.name.startsWith("sqlite_"))) {
      yield* tx.run(sql`CREATE TABLE ${identifier(table.retained)} AS SELECT * FROM ${identifier(table.name)}`)
    }
    for (const object of objects) {
      if (object.type === "trigger") yield* tx.run(sql`DROP TRIGGER ${identifier(object.name)}`)
      if (object.type === "view") yield* tx.run(sql`DROP VIEW ${identifier(object.name)}`)
    }
    for (const table of tables.filter((table) => !table.name.startsWith("sqlite_"))) {
      yield* tx.run(sql`ALTER TABLE ${identifier(table.name)} RENAME TO ${identifier(table.retained)}`)
    }
    for (const object of objects.filter((object) => object.type === "index" && object.sql !== null)) {
      const updated = yield* tx.get<{ sql: string }>(sql`SELECT sql FROM sqlite_schema WHERE name = ${object.name}`)
      if (!updated) return yield* Effect.fail(new ArchiveError("Source index disappeared during capture"))
      const prefix = /^CREATE\s+(UNIQUE\s+)?INDEX\s+(?:IF\s+NOT\s+EXISTS\s+)?(?:"(?:[^"]|"")*"|'(?:[^']|'')*'|`(?:[^`]|``)*`|\[[^\]]*\]|[^\s]+)\s+ON\s+/i
      const match = updated.sql.match(prefix)
      if (!match) return yield* Effect.fail(new ArchiveError("Unsupported source index DDL"))
      yield* tx.run(sql`DROP INDEX ${identifier(object.name)}`)
      yield* tx.run(sql.raw(updated.sql.replace(prefix, () =>
        `CREATE ${match[1] ?? ""}INDEX "_ws_index_${id.replaceAll("-", "")}_${objects.indexOf(object)}" ON `,
      )))
    }
    for (const table of tables) yield* protect(tx, table.retained, true)
    return archive
  })
}

export function sourceTable(archive: Archive, name: string) {
  return archive.tables.find((table) => table.name === name)?.retained
}

export function identifier(name: string) {
  return sql.raw(`"${name.replaceAll('"', '""')}"`)
}

export function persist(tx: Transaction, archive: Archive) {
  return Effect.gen(function* () {
    yield* tx.insert(WsArchiveTable).values({ id: archive.id, manifest: JSON.stringify(archive) })
    yield* protect(tx, "_ws_archive", false)
    yield* tx.run(sql`CREATE TRIGGER IF NOT EXISTS _ws_archive_replace_immutable
      BEFORE INSERT ON _ws_archive
      WHEN EXISTS (SELECT 1 FROM _ws_archive WHERE id = NEW.id)
      BEGIN SELECT RAISE(ABORT, 'WS archive is immutable'); END`)
    yield* protect(tx, "_ws_import_root", false)
    yield* tx.run(sql`CREATE TRIGGER IF NOT EXISTS _ws_import_root_replace_immutable
      BEFORE INSERT ON _ws_import_root
      WHEN EXISTS (SELECT 1 FROM _ws_import_root WHERE aggregate_id = NEW.aggregate_id)
      BEGIN SELECT RAISE(ABORT, 'WS import root is immutable'); END`)
  })
}

export function read(db: Pick<Database, "select">, id: string) {
  return Effect.gen(function* () {
    const row = yield* db.select().from(WsArchiveTable).where(eq(WsArchiveTable.id, id)).get()
    if (!row) return
    return yield* Schema.decodeUnknownEffect(Schema.fromJsonString(Manifest))(row.manifest)
  })
}

export function persistRoot(tx: Transaction, root: Root) {
  return tx.insert(WsImportRootTable).values(root)
}

export function readRoot(db: Pick<Database, "select">, aggregateID: string) {
  return db.select().from(WsImportRootTable).where(eq(WsImportRootTable.aggregate_id, aggregateID)).get()
}

function protect(tx: Transaction, table: string, inserts: boolean) {
  return Effect.gen(function* () {
    for (const operation of inserts ? ["INSERT", "UPDATE", "DELETE"] : ["UPDATE", "DELETE"]) {
      yield* tx.run(sql`CREATE TRIGGER IF NOT EXISTS ${identifier(`${table}_${operation.toLowerCase()}_immutable`)}
        BEFORE ${sql.raw(operation)} ON ${identifier(table)}
        BEGIN SELECT RAISE(ABORT, 'WS archive is immutable'); END`)
    }
  })
}

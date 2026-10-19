export * as WsNativeFile from "./ws-native-file"

import { basename, dirname, join } from "node:path"
import { Effect } from "effect"
import { withPool } from "#ws-native-worker"
import { sql } from "drizzle-orm"
import type { EffectDrizzleSqlite } from "./drizzle"
import { DatabaseArtifact } from "./artifact"
import { DatabaseLease } from "./lease"
import { DatabaseLegacyV01 } from "./legacy-v01"
import { DatabaseLineage } from "./lineage"
import { DatabaseMigration } from "./migration"
import { DatabaseReplacement } from "./replacement"
import { WsNativeStage } from "./ws-native-stage"

type Database = EffectDrizzleSqlite.EffectSQLiteDatabase
const nativeIDs = [...DatabaseMigration.currentMigrationIDs]

export function admit(target: string, begin: Effect.Effect<void> = Effect.void) {
  if (target === ":memory:") return Effect.void
  return Effect.gen(function* () {
    const needed = yield* Effect.gen(function* () {
      yield* DatabaseLease.shared(target)
      if (yield* DatabaseReplacement.pending(target)) {
        yield* begin
        return true
      }
      if (!(yield* DatabaseArtifact.exists(target))) return false
      yield* DatabaseArtifact.requireRegularFile(target)
      return yield* DatabaseLegacyV01.useDatabase(target, true, (db) => Effect.gen(function* () {
        const state = yield* DatabaseLineage.inspect(db, nativeIDs)
        if (state.ws || state.previousNative) {
          yield* begin
          return true
        }
        const native = yield* DatabaseMigration.preflight(db)
        if ((native.tables.has("session") || native.tables.has("session_v2")) &&
          nativeIDs.some((id) => !native.completed.has(id))) yield* begin
        return yield* DatabaseArtifact.exists(`${target}.ws-stage.json`)
      }))
    }).pipe(Effect.scoped)
    if (!needed) return
    yield* Effect.gen(function* () {
      yield* DatabaseLease.exclusive(target)
      if (yield* DatabaseReplacement.pending(target)) {
        const marker = yield* DatabaseReplacement.readMarker(`${target}.replacement.json`)
        if (marker.version === 1) return yield* promote(target, marker)
      } else {
        const state = yield* DatabaseLegacyV01.useDatabase(target, true, (db) => DatabaseLineage.inspect(db, nativeIDs))
        if (!state.ws && !state.previousNative) return yield* WsNativeStage.retire(target)
        const before = yield* evidence(target)
        const staged = yield* WsNativeStage.prepare(target, target)
        yield* WsNativeStage.handoff(target)
        const targetPreimage = yield* preimage(target, before)
        const marker = {
          format: "opencode.database.replacement", version: 2, stageFormat: "native", phase: "prepared",
          staged, targetPreimage,
        } satisfies DatabaseLegacyV01.PendingReplacement
        yield* DatabaseArtifact.write(`${target}.replacement.json`, `${JSON.stringify(marker)}\n`)
      }
      yield* WsNativeStage.activateAndRetire(target)
    }).pipe(Effect.scoped)
  }).pipe(withPool)
}

function promote(target: string, marker: Extract<DatabaseLegacyV01.PendingReplacement, { readonly version: 1 }>) {
  return Effect.gen(function* () {
    const oldStage = join(dirname(target), marker.staged.name)
    const exists = yield* DatabaseArtifact.exists(oldStage)
    if (!exists && marker.phase !== "backup_ready")
      return yield* Effect.fail(new DatabaseLegacyV01.DatabaseLegacyV01Error("prepared replacement stage is missing"))
    const source = exists ? oldStage : target
    yield* DatabaseReplacement.validate(source, marker.staged)
    yield* DatabaseReplacement.requireNoSidecars(source)
    const before = yield* evidence(target)
    if (exists && marker.phase === "backup_ready") {
      const targetExists = yield* DatabaseArtifact.exists(target)
      if (Boolean(marker.backup) !== targetExists)
        return yield* Effect.fail(new DatabaseLegacyV01.DatabaseLegacyV01Error("replacement target no longer matches backup state"))
    }
    if (marker.phase === "backup_ready" && marker.backup) {
      const backup = join(dirname(target), marker.backup.name)
      yield* DatabaseReplacement.validate(backup, marker.backup)
      yield* DatabaseReplacement.requireNoSidecars(backup)
      yield* validateSourceFile(backup)
      if (exists) yield* requireSameSource(target, backup)
    }
    const staged = yield* WsNativeStage.prepare(source, target)
    yield* DatabaseReplacement.validate(source, marker.staged)
    yield* WsNativeStage.handoff(target)
    const targetPreimage = yield* preimage(target, before)
    const next = { ...marker, version: 2, stageFormat: "native", staged, targetPreimage } satisfies DatabaseLegacyV01.PendingReplacement
    yield* DatabaseArtifact.replaceWithText(`${target}.replacement.json`, `${JSON.stringify(next)}\n`)
    yield* WsNativeStage.activateAndRetire(target)
    const retained = [basename(target), ...(marker.phase === "backup_ready" && marker.backup ? [marker.backup.name] : [])]
    if (exists && !retained.some((name) => process.platform === "win32"
      ? name.toLowerCase() === marker.staged.name.toLowerCase()
      : name === marker.staged.name)) {
      yield* DatabaseReplacement.validate(oldStage, marker.staged)
      yield* DatabaseReplacement.requireNoSidecars(oldStage)
      yield* DatabaseArtifact.remove(oldStage)
    }
  })
}

function preimage(target: string, before: Effect.Success<ReturnType<typeof evidence>>) {
  return Effect.gen(function* () {
    if (before.main) yield* validateSourceFile(target)
    const after = yield* evidence(target)
    if (JSON.stringify(before) !== JSON.stringify(after))
      return yield* Effect.fail(new DatabaseLegacyV01.DatabaseLegacyV01Error("database source changed during conversion"))
    if (!before.main) return null
    yield* DatabaseLegacyV01.useDatabase(target, false, (db) => Effect.gen(function* () {
      yield* db.run("PRAGMA busy_timeout = 0")
      yield* checkpoint(db)
      const journal = yield* db.all<{ journal_mode: string }>("PRAGMA journal_mode = DELETE")
      if (journal[0]?.journal_mode !== "delete")
        return yield* Effect.fail(new DatabaseLegacyV01.DatabaseLegacyV01Error("database journal mode is busy"))
    }))
    yield* DatabaseReplacement.requireNoSidecars(target)
    return yield* DatabaseArtifact.digest(target)
  })
}

function evidence(target: string) {
  return Effect.gen(function* () {
    const main = (yield* DatabaseArtifact.exists(target)) ? yield* DatabaseArtifact.digest(target) : null
    const wal = `${target}-wal`
    if (yield* DatabaseArtifact.exists(wal)) {
      yield* DatabaseArtifact.requireRegularFile(wal)
      const digest = yield* DatabaseArtifact.digest(wal)
      return { main, wal: digest.bytes === 0 ? null : digest }
    }
    return { main, wal: null }
  })
}

export function requireSameSource(target: string, backup: string) {
  return DatabaseLegacyV01.useDatabase(target, true, (db) => Effect.gen(function* () {
    yield* db.run(sql`ATTACH DATABASE ${backup} AS replacement_backup`)
    yield* db.transaction(requireSameSnapshot, { behavior: "deferred" })
  }))
}

export function requireSameSnapshot(tx: Database) {
  return Effect.gen(function* () {
    yield* tx.get("SELECT name FROM main.sqlite_schema LIMIT 1")
    const schema = sql`SELECT type, name, tbl_name, sql FROM main.sqlite_schema`
    const prior = sql`SELECT type, name, tbl_name, sql FROM replacement_backup.sqlite_schema`
    const changed = () => Effect.fail(new DatabaseLegacyV01.DatabaseLegacyV01Error("replacement target differs from its backup"))
    if ((yield* tx.get(sql`SELECT 1 FROM (${schema} EXCEPT ${prior}) LIMIT 1`)) || (yield* tx.get(sql`SELECT 1 FROM (${prior} EXCEPT ${schema}) LIMIT 1`)))
      return yield* changed()
    for (const pragma of ["user_version", "application_id"]) {
      if (JSON.stringify(yield* tx.all(sql.raw(`PRAGMA main.${pragma}`))) !==
        JSON.stringify(yield* tx.all(sql.raw(`PRAGMA replacement_backup.${pragma}`)))) return yield* changed()
    }
    for (const table of yield* tx.all<{ name: string }>(sql`SELECT name FROM main.sqlite_schema WHERE type = 'table'`)) {
      const current = sql`main.${sql.identifier(table.name)}`
      const previous = sql`replacement_backup.${sql.identifier(table.name)}`
      const count = yield* tx.get<{ equal: number }>(sql`SELECT (SELECT count(*) FROM ${current}) = (SELECT count(*) FROM ${previous}) AS equal`)
      if (!count?.equal) return yield* changed()
      const columns = yield* tx.all<{ name: string; pk: number; notnull: number }>(sql`SELECT name, pk, "notnull" FROM pragma_table_xinfo(${table.name}, 'main')`)
      const layout = yield* tx.get<{ wr: number }>(sql`SELECT wr FROM pragma_table_list WHERE schema = 'main' AND name = ${table.name}`)
      const rowid = layout?.wr === 0 ? ["rowid", "_rowid_", "oid"].find((name) => !columns.some((column) => column.name.toLowerCase() === name)) : undefined
      const primary = columns.filter((column) => column.pk).sort((left, right) => left.pk - right.pk)
      const nullable = !rowid && primary.some((column) => !column.notnull) && Boolean(
        (yield* tx.get(sql`SELECT 1 FROM ${current} WHERE ${sql.join(primary.map((column) => sql`${sql.identifier(column.name)} IS NULL`), sql` OR `)} LIMIT 1`)) ||
        (yield* tx.get(sql`SELECT 1 FROM ${previous} WHERE ${sql.join(primary.map((column) => sql`${sql.identifier(column.name)} IS NULL`), sql` OR `)} LIMIT 1`)))
      const keys = rowid ? [rowid] : nullable ? [] : primary.map((column) => column.name)
      if (keys.length) {
        const join = sql.join(keys.map((key) => sql`snapshot.${sql.identifier(key)} = source.${sql.identifier(key)}`), sql` AND `)
        const difference = sql.join(columns.map((column) => {
          const source = sql`source.${sql.identifier(column.name)}`
          const snapshot = sql`snapshot.${sql.identifier(column.name)}`
          return sql`(typeof(${source}) <> typeof(${snapshot}) OR CASE WHEN typeof(${source}) IN ('text', 'blob')
            THEN CAST(${source} AS BLOB) IS NOT CAST(${snapshot} AS BLOB) ELSE quote(${source}) COLLATE BINARY IS NOT quote(${snapshot}) END)`
        }), sql` OR `)
        if (yield* tx.get(sql`SELECT 1 FROM ${current} AS source LEFT JOIN ${previous} AS snapshot ON ${join}
          WHERE snapshot.${sql.identifier(keys[0])} IS NULL OR ${difference} LIMIT 1`)) return yield* changed()
        continue
      }
      const fields = sql.join(columns.flatMap((column) => {
        const name = sql.identifier(column.name)
        return [sql`typeof(${name})`, sql`CASE WHEN typeof(${name}) IN ('text', 'blob') THEN CAST(${name} AS BLOB) ELSE quote(${name}) END`]
      }), sql`, `)
      const source = sql`SELECT ${fields}, count(*) FROM ${current} GROUP BY ${fields}`
      const snapshot = sql`SELECT ${fields}, count(*) FROM ${previous} GROUP BY ${fields}`
      if ((yield* tx.get(sql`SELECT 1 FROM (${source} EXCEPT ${snapshot}) LIMIT 1`)) || (yield* tx.get(sql`SELECT 1 FROM (${snapshot} EXCEPT ${source}) LIMIT 1`))) return yield* changed()
    }
  })
}

export function requireStagedSource(target: string) {
  return Effect.gen(function* () {
    if (!(yield* DatabaseArtifact.exists(`${target}.ws-stage.json`))) return
    const marker = yield* WsNativeStage.read(target)
    if (process.platform === "win32" ? marker.source.name.toLowerCase() !== basename(target).toLowerCase() : marker.source.name !== basename(target)) return
    if (!(yield* DatabaseArtifact.exists(join(dirname(target), marker.work)))) return
    const snapshot = join(dirname(target), marker.snapshot.name)
    yield* DatabaseReplacement.validate(snapshot, marker.snapshot)
    yield* DatabaseReplacement.requireNoSidecars(snapshot)
    yield* requireSameSource(target, snapshot)
  })
}

export function snapshotSource(source: string, destination: string) {
  return Effect.gen(function* () {
    yield* validateSourceFile(source)
    yield* DatabaseLegacyV01.useDatabase(source, true, (db) => DatabaseArtifact.snapshot(db, destination))
    yield* DatabaseReplacement.requireNoSidecars(destination)
  })
}

export function validateSourceFile(filename: string) {
  return DatabaseArtifact.requireRegularFile(filename).pipe(Effect.andThen(
    DatabaseLegacyV01.useDatabase(filename, true, (db) => Effect.gen(function* () {
      const state = yield* DatabaseLineage.inspect(db, nativeIDs)
      if (state.ws || state.previousNative) {
        const { WsNative } = yield* Effect.promise(() => import("./ws-native"))
        yield* WsNative.validateSource(db)
        return
      }
      yield* DatabaseMigration.preflight(db)
      yield* integrity(db)
    })),
  ))
}

export function validateNativeFile(filename: string) {
  return DatabaseLegacyV01.useDatabase(filename, true, (db) => Effect.gen(function* () {
    yield* DatabaseMigration.preflight(db)
    const { WsNative } = yield* Effect.promise(() => import("./ws-native"))
    yield* WsNative.validate(db)
  }))
}

function checkpoint(db: Database) {
  return Effect.gen(function* () {
    const rows = yield* db.all<{ busy: number }>("PRAGMA wal_checkpoint(TRUNCATE)")
    if (rows.length !== 1 || rows[0]?.busy !== 0)
      return yield* Effect.fail(new DatabaseLegacyV01.DatabaseLegacyV01Error("database checkpoint is busy"))
  })
}

function integrity(db: Database) {
  return Effect.gen(function* () {
    const rows = yield* db.all<{ quick_check: string }>("PRAGMA quick_check")
    if (rows.length !== 1 || rows[0]?.quick_check !== "ok" || (yield* db.all("PRAGMA foreign_key_check")).length)
      return yield* Effect.fail(new DatabaseLegacyV01.DatabaseLegacyV01Error("database integrity check failed"))
  })
}

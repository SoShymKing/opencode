export * as WsNativeFile from "./ws-native-file"

import { randomUUID } from "node:crypto"
import { basename, dirname, join } from "node:path"
import { Effect } from "effect"
import { sql } from "drizzle-orm"
import type { EffectDrizzleSqlite } from "./drizzle"
import { DatabaseArtifact } from "./artifact"
import { DatabaseLease } from "./lease"
import { DatabaseLegacyV01 } from "./legacy-v01"
import { DatabaseLineage } from "./lineage"
import { DatabaseMigration } from "./migration"
import { DatabaseReplacement } from "./replacement"

type Database = EffectDrizzleSqlite.EffectSQLiteDatabase
const nativeIDs = [...DatabaseMigration.currentMigrationIDs]

export function admit(target: string) {
  if (target === ":memory:") return Effect.void
  return Effect.gen(function* () {
    const needed = yield* Effect.gen(function* () {
      yield* DatabaseLease.shared(target)
      if (yield* DatabaseReplacement.pending(target)) return true
      if (!(yield* DatabaseArtifact.exists(target))) return false
      yield* DatabaseArtifact.requireRegularFile(target)
      return yield* DatabaseLegacyV01.useDatabase(target, true, (db) => Effect.gen(function* () {
        const state = yield* DatabaseLineage.inspect(db, nativeIDs)
        if (state.ws || state.previousNative) return true
        yield* DatabaseMigration.preflight(db)
        return false
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
        if (!state.ws && !state.previousNative) return
        const before = yield* evidence(target)
        const staged = yield* convertFile(target)
        const targetPreimage = yield* preimage(target, before)
        const marker = {
          format: "opencode.database.replacement", version: 2, stageFormat: "native", phase: "prepared",
          staged, targetPreimage,
        } satisfies DatabaseLegacyV01.PendingReplacement
        yield* DatabaseArtifact.write(`${target}.replacement.json`, `${JSON.stringify(marker)}\n`)
      }
      yield* DatabaseReplacement.activateLocked(target)
    }).pipe(Effect.scoped)
  })
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
    const staged = yield* convertFile(source)
    yield* DatabaseReplacement.validate(source, marker.staged)
    const targetPreimage = yield* preimage(target, before)
    const next = { ...marker, version: 2, stageFormat: "native", staged, targetPreimage } satisfies DatabaseLegacyV01.PendingReplacement
    yield* DatabaseArtifact.replaceWithText(`${target}.replacement.json`, `${JSON.stringify(next)}\n`)
    yield* DatabaseReplacement.activateLocked(target)
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

function convertFile(source: string) {
  const staged = `${source}.native-${randomUUID()}.db`
  return Effect.gen(function* () {
    const { WsNative } = yield* Effect.promise(() => import("./ws-native"))
    yield* DatabaseLegacyV01.useDatabase(source, true, (db) => Effect.gen(function* () {
      yield* WsNative.validateSource(db)
      yield* DatabaseArtifact.snapshot(db, staged)
    }))
    yield* DatabaseLegacyV01.useDatabase(staged, false, (db) => Effect.gen(function* () {
      yield* WsNative.convert(db)
      yield* WsNative.validate(db)
      yield* checkpoint(db)
    }))
    yield* DatabaseReplacement.requireNoSidecars(staged)
    yield* DatabaseArtifact.syncFile(staged)
    return { name: basename(staged), ...(yield* DatabaseArtifact.digest(staged)) }
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

function requireSameSource(target: string, backup: string) {
  return DatabaseLegacyV01.useDatabase(target, true, (db) => Effect.gen(function* () {
    yield* db.run(sql`ATTACH DATABASE ${backup} AS replacement_backup`)
    yield* db.transaction((tx) => Effect.gen(function* () {
      const schema = sql`SELECT type, name, tbl_name, sql FROM main.sqlite_schema`
      const prior = sql`SELECT type, name, tbl_name, sql FROM replacement_backup.sqlite_schema`
      const changed = () => Effect.fail(new DatabaseLegacyV01.DatabaseLegacyV01Error("replacement target differs from its backup"))
      if ((yield* tx.all(sql`${schema} EXCEPT ${prior}`)).length || (yield* tx.all(sql`${prior} EXCEPT ${schema}`)).length)
        return yield* changed()
      for (const pragma of ["user_version", "application_id"]) {
        if (JSON.stringify(yield* tx.all(sql.raw(`PRAGMA main.${pragma}`))) !==
          JSON.stringify(yield* tx.all(sql.raw(`PRAGMA replacement_backup.${pragma}`)))) return yield* changed()
      }
      for (const table of yield* tx.all<{ name: string }>(sql`SELECT name FROM main.sqlite_schema WHERE type = 'table'`)) {
        const columns = yield* tx.all<{ name: string }>(sql`SELECT name FROM pragma_table_info(${table.name})`)
        const fields = sql.join(columns.flatMap((column) => {
          const name = sql.identifier(column.name)
          return [sql`typeof(${name})`, sql`CASE WHEN typeof(${name}) IN ('text', 'blob') THEN hex(${name}) ELSE quote(${name}) END`]
        }), sql`, `)
        const current = sql`SELECT ${fields}, count(*) FROM main.${sql.identifier(table.name)} GROUP BY ${fields}`
        const previous = sql`SELECT ${fields}, count(*) FROM replacement_backup.${sql.identifier(table.name)} GROUP BY ${fields}`
        if ((yield* tx.all(sql`${current} EXCEPT ${previous}`)).length || (yield* tx.all(sql`${previous} EXCEPT ${current}`)).length)
          return yield* changed()
      }
    }))
  }))
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

export * as DatabaseMigration from "./migration.js"
export { UnsafeMigrationHistoryError, WsUpgradeRequiredError, legacyBaselineMigrationIDs, legacySessionMetadataMigrationID, legacySessionMetadataAlias, legacyPartMigrationID } from "./lineage.js"

import { sql } from "drizzle-orm"
import { Effect } from "effect"
import { supportsForeignKeyToggle } from "#sqlite"
import type { EffectDrizzleSqlite } from "./drizzle.js"
import { migrations } from "./migration.gen.js"
import schema from "./schema.gen.js"
import type { Global } from "@opencode/util/global"
import { DatabaseLineage } from "./lineage.js"
import previousV2Migration from "./migration/20260804233008_loose_psylocke.js"

type Database = EffectDrizzleSqlite.EffectSQLiteDatabase
type Transaction = Parameters<Parameters<Database["transaction"]>[0]>[0]

export type Migration = {
  readonly id: string
  readonly foreignKeys?: boolean
  readonly up: (tx: Transaction) => Effect.Effect<void, unknown, Global.Service>
}

export const currentMigrationIDs = new Set(migrations.map((migration) => migration.id))

export function preflight(db: Pick<Database, "all">) {
  return Effect.gen(function* () {
    const state = yield* DatabaseLineage.requireNative(db, [...currentMigrationIDs])
    if (state.tables.has("session") || state.tables.has("session_v2")) {
      if (!state.completed.size)
        return yield* Effect.fail(new DatabaseLineage.UnsafeMigrationHistoryError([], "session schema has no migration evidence"))
      return state
    }
    if ([...state.tables].some((name) => !name.startsWith("_")))
      return yield* Effect.fail(new DatabaseLineage.UnsafeMigrationHistoryError([...state.completed], "nonempty database without sessions"))
    if (state.completed.size)
      return yield* Effect.fail(new DatabaseLineage.UnsafeMigrationHistoryError([...state.completed], "ledger without schema"))
    return state
  })
}

export function apply(db: Database, filename = ":memory:") {
  return Effect.gen(function* () {
    const source = yield* DatabaseLineage.inspect(db, [...currentMigrationIDs])
    if (source.ws || source.previousNative) {
      const { WsNative } = yield* Effect.promise(() => import("./ws-native.js"))
      yield* WsNative.convert(db)
      return
    }
    const state = yield* db.transaction((tx) => Effect.gen(function* () {
      const state = yield* preflight(tx)
      if (state.tables.has("session") || state.tables.has("session_v2")) return state
      yield* schema.up(tx)
      if (!(yield* tx.get(sql`SELECT name FROM sqlite_master WHERE name = 'session_message_part'`)))
        return yield* Effect.fail(new DatabaseLineage.UnsafeMigrationHistoryError([], "generated schema lacks normalized parts; run the schema generator"))
      yield* tx.run(sql`CREATE TABLE migration (id TEXT PRIMARY KEY, time_completed INTEGER NOT NULL)`)
      yield* Effect.forEach(migrations, (migration) => journal(tx, migration.id), { discard: true })
    }), { behavior: "immediate" })
    if (!state) return
    if (filename !== ":memory:" && migrations.some((migration) => !state.completed.has(migration.id))) {
      const { DatabaseArtifact } = yield* Effect.promise(() => import("./artifact.js"))
      yield* DatabaseArtifact.snapshot(db, `${filename}.backup-${Date.now()}-${crypto.randomUUID()}.db`)
    }
    yield* applyOnly(db, migrations)
  })
}

function journal(tx: Transaction, id: string) {
  return tx.run(sql`INSERT OR IGNORE INTO migration (id, time_completed) VALUES (${id}, ${Date.now()})`)
}

export function applyOnly(db: Database, input: readonly Migration[]) {
  return Effect.gen(function* () {
    const source = yield* DatabaseLineage.inspect(db, [...currentMigrationIDs, ...input.map((item) => item.id)])
    if (source.previousNative && input.includes(previousV2Migration) && input.every((item) => migrations.includes(item))) {
      const { WsNative } = yield* Effect.promise(() => import("./ws-native.js"))
      yield* WsNative.convert(db)
      return
    }
    const state = yield* DatabaseLineage.requireNative(db, [...currentMigrationIDs, ...input.map((item) => item.id)])
    yield* db.transaction((tx) => Effect.gen(function* () {
      yield* tx.run(sql`CREATE TABLE IF NOT EXISTS migration (id TEXT PRIMARY KEY, time_completed INTEGER NOT NULL)`)
      yield* Effect.forEach([...state.completed], (id) => journal(tx, id), { discard: true })
    }), { behavior: "immediate" })
    for (const migration of input) {
      if (state.completed.has(migration.id)) continue
      const started = Date.now()
      yield* Effect.logInfo("database migration started", { migration: migration.id })
      const apply = db.transaction((tx) => Effect.gen(function* () {
        if (yield* tx.get(sql`SELECT id FROM migration WHERE id = ${migration.id}`)) return
        yield* migration.up(tx)
        yield* journal(tx, migration.id)
      }), { behavior: "immediate" })
      const run = migration.foreignKeys !== false ? apply : Effect.gen(function* () {
        const relax = supportsForeignKeyToggle
          ? db.run(sql`PRAGMA foreign_keys = OFF`)
          : db.run(sql`PRAGMA defer_foreign_keys = ON`)
        const restore = supportsForeignKeyToggle ? db.run(sql`PRAGMA foreign_keys = ON`) : Effect.void
        yield* relax
        yield* apply.pipe(Effect.ensuring(restore.pipe(Effect.orDie)))
      })
      yield* run.pipe(Effect.tapError((error) => Effect.logError("database migration failed", {
        migration: migration.id, durationMs: Date.now() - started, error,
      })))
      yield* Effect.logInfo("database migration completed", { migration: migration.id, durationMs: Date.now() - started })
    }
  })
}

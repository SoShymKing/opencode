export * as DatabaseMigration from "./migration"

import { randomUUID } from "node:crypto"
import { sql } from "drizzle-orm"
import { Effect, Semaphore } from "effect"
import type { EffectDrizzleSqlite } from "@opencode-ai/effect-drizzle-sqlite"
import { migrations } from "./migration.gen"
import sessionMessagePartMigration from "./migration/v02_session_message_part"
import schema from "./schema.gen"
import { DatabaseArtifact } from "./artifact"

type Database = EffectDrizzleSqlite.EffectSQLiteDatabase
type Transaction = Parameters<Parameters<Database["transaction"]>[0]>[0]
type JournalReader = Pick<Database, "all">
const lock = Semaphore.makeUnsafe(1)

export const legacySessionMetadataMigrationID = "20260511173437_session-metadata"
export const legacyBaselineMigrationIDs = [
  "20260127222353_familiar_lady_ursula",
  "20260211171708_add_project_commands",
  "20260213144116_wakeful_the_professor",
  "20260225215848_workspace",
  "20260227213759_add_session_workspace_id",
  "20260228203230_blue_harpoon",
  "20260303231226_add_workspace_fields",
  "20260309230000_move_org_to_state",
  "20260312043431_session_message_cursor",
  "20260323234822_events",
  "20260410174513_workspace-name",
  "20260413175956_chief_energizer",
  "20260423070820_add_icon_url_override",
  "20260427172553_slow_nightmare",
  "20260428004200_add_session_path",
  "20260501142318_next_venus",
  "20260504145000_add_sync_owner",
  "20260507164347_add_workspace_time",
  "20260510033149_session_usage",
  "20260511000411_data_migration_state",
  legacySessionMetadataMigrationID,
  "20260601010001_normalize_storage_paths",
  "20260601202201_amazing_prowler",
  "20260602002951_lowly_union_jack",
  "20260602182828_add_project_directories",
  "20260603001617_session_message_projection_indexes",
  "20260603040000_session_message_projection_order",
  "20260603141458_session_input_inbox",
  "20260603160727_jittery_ezekiel_stane",
  "20260604172448_event_sourced_session_input",
  "20260605003541_add_session_context_snapshot",
  "20260605042240_add_context_epoch_agent",
  "20260611035744_credential",
  "20260611192811_lush_chimera",
  "20260612174303_project_dir_strategy",
  "20260622142730_simplify_session_context_epoch",
  "20260622170816_reset_v2_session_state",
  "20260622202450_simplify_session_input",
] as const
export const legacySessionMetadataAlias = "20260530232709_lovely_romulus"
export const legacyPartMigrationID = "20260802133449_session_message_part"
export const currentMigrationIDs = new Set([
  ...migrations.map((migration) => migration.id),
  ...legacyBaselineMigrationIDs,
  legacySessionMetadataAlias,
  legacyPartMigrationID,
])

export type Migration = {
  readonly id: string
  readonly up: (tx: Transaction) => Effect.Effect<void, unknown>
}

export class UnsafeMigrationHistoryError extends Error {
  override readonly name = "UnsafeMigrationHistoryError"

  constructor(readonly ids: readonly string[]) {
    super(`Unsafe legacy migration history: ${ids.length === 0 ? "no completed migration evidence" : ids.join(", ")}`)
  }
}

export class MigrationBackupError extends Error {
  override readonly name = "MigrationBackupError"

  constructor(
    readonly operation: "open" | "sync" | "close" | "publish" | "cleanup" | "sync-directory",
    options: ErrorOptions,
  ) {
    super(`Failed to ${operation} database migration backup`, options)
  }
}

export function apply(db: Database, filename: string) {
  return lock.withPermit(
    Effect.gen(function* () {
      const tables = yield* db.all<{ readonly name: string }>(
        sql`SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%'`,
      )
      if (tables.some((table) => table.name === "session")) return yield* migrateExisting(db, filename)
      if (tables.length > 0) return yield* Effect.die("Database is not empty and has no session table")
      if (yield* initializeFresh(db)) return
      return yield* migrateExisting(db, filename)
    }),
  )
}

function initializeFresh(db: Database) {
  return db.transaction(
    (tx) =>
      Effect.gen(function* () {
        const tables = yield* tx.all<{ readonly name: string }>(
          sql`SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%'`,
        )
        if (tables.some((table) => table.name === "session")) return false
        if (tables.length > 0) return yield* Effect.die("Database is not empty and has no session table")
        yield* schema.up(tx)
        yield* tx.run(
          sql`CREATE TABLE ${sql.identifier("migration")} (id TEXT PRIMARY KEY, time_completed INTEGER NOT NULL)`,
        )
        yield* Effect.forEach(
          migrations,
          (migration) =>
            tx.run(
              sql`INSERT INTO ${sql.identifier("migration")} (id, time_completed) VALUES (${migration.id}, ${Date.now()})`,
            ),
          { discard: true },
        )
        return true
      }),
    { behavior: "immediate" },
  )
}

function migrateExisting(db: Database, filename: string) {
  return Effect.gen(function* () {
    const state = yield* migrationState(yield* readHistory(db))
    if (!state.pending) return
    if (filename !== ":memory:") yield* backup(db, filename)
    yield* db.transaction(
      (tx) =>
        Effect.gen(function* () {
          const current = yield* migrationState(yield* readHistory(tx))
          if (!current.pending) return
          yield* tx.run(
            sql`CREATE TABLE IF NOT EXISTS ${sql.identifier("migration")} (id TEXT PRIMARY KEY, time_completed INTEGER NOT NULL)`,
          )
          if (current.baselineMarker) yield* journal(tx, "v01_baseline")
          if (!current.partComplete) yield* sessionMessagePartMigration.up(tx)
          if (current.partMarker) yield* journal(tx, "v02_session_message_part")
        }),
      { behavior: "immediate" },
    )
  })
}

function readHistory(db: JournalReader) {
  return Effect.gen(function* () {
    const tables = new Set(
      (yield* db.all<{ readonly name: string }>(sql`
          SELECT name FROM sqlite_master
          WHERE type = 'table' AND name IN ('migration', '__drizzle_migrations')
        `)).map((row) => row.name),
    )
    const migration = tables.has("migration")
      ? yield* db.all<{ readonly id: string }>(sql`SELECT id FROM ${sql.identifier("migration")}`)
      : []
    const drizzleColumns = tables.has("__drizzle_migrations")
      ? yield* db.all<{ readonly name: string }>(sql`SELECT name FROM pragma_table_info('__drizzle_migrations')`)
      : []
    const drizzle = drizzleColumns.some((column) => column.name === "name")
      ? yield* db.all<{ readonly id: string }>(
          sql`SELECT name AS id FROM ${sql.identifier("__drizzle_migrations")} WHERE name IS NOT NULL`,
        )
      : []
    return new Set([...migration, ...drizzle].map((row) => row.id))
  })
}

function migrationState(history: ReadonlySet<string>) {
  const baselineComplete =
    history.has("v01_baseline") ||
    legacyBaselineMigrationIDs.every(
      (id) => history.has(id) || (id === legacySessionMetadataMigrationID && history.has(legacySessionMetadataAlias)),
    )
  if (!baselineComplete) return Effect.fail(new UnsafeMigrationHistoryError([...history].sort()))
  const partComplete = history.has("v02_session_message_part") || history.has(legacyPartMigrationID)
  return Effect.succeed({
    pending: !history.has("v01_baseline") || !history.has("v02_session_message_part"),
    baselineMarker: !history.has("v01_baseline"),
    partComplete,
    partMarker: !history.has("v02_session_message_part"),
  })
}

function backup(db: Database, filename: string) {
  return DatabaseArtifact.snapshot(db, `${filename}.backup-${Date.now()}-${process.pid}-${randomUUID()}.db`).pipe(
    Effect.mapError((cause) => new MigrationBackupError(backupOperation(cause), { cause })),
  )
}

function backupOperation(error: DatabaseArtifact.Error): MigrationBackupError["operation"] {
  switch (error.operation) {
    case "open":
    case "sync":
    case "close":
    case "publish":
    case "sync-directory":
      return error.operation
    case "remove":
      return "cleanup"
    case "hash":
    case "replace":
    case "snapshot":
    case "stat":
    case "write":
      return "publish"
  }
}

function journal(tx: Transaction, id: string) {
  return tx.run(
    sql`INSERT OR IGNORE INTO ${sql.identifier("migration")} (id, time_completed) VALUES (${id}, ${Date.now()})`,
  )
}

export function applyOnly(db: Database, input: readonly Migration[]) {
  return Effect.gen(function* () {
    yield* db.run(
      sql`CREATE TABLE IF NOT EXISTS ${sql.identifier("migration")} (id TEXT PRIMARY KEY, time_completed INTEGER NOT NULL)`,
    )
    const completed = new Set(
      (yield* db.all<{ readonly id: string }>(sql`SELECT id FROM ${sql.identifier("migration")}`)).map((row) => row.id),
    )
    for (const migration of input) {
      if (completed.has(migration.id)) continue
      yield* db.transaction((tx) =>
        Effect.gen(function* () {
          yield* migration.up(tx)
          yield* journal(tx, migration.id)
        }),
      )
    }
  })
}

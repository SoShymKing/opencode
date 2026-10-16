export * as DatabaseLineage from "./lineage.js"

import { sql } from "drizzle-orm"
import { Effect } from "effect"
import type { EffectDrizzleSqlite } from "./drizzle.js"

type Reader = Pick<EffectDrizzleSqlite.EffectSQLiteDatabase, "all">

export class UnsafeMigrationHistoryError extends Error {
  override readonly name = "UnsafeMigrationHistoryError"
  constructor(readonly ids: readonly string[], reason = "unrecognized or incomplete migration history") {
    super(`Unsafe database migration history: ${reason}: ${ids.join(", ")}`)
  }
}

export class WsUpgradeRequiredError extends Error {
  override readonly name = "WsUpgradeRequiredError"
  constructor(readonly ids: readonly string[], readonly baselineComplete: boolean) {
    super("ws database upgrade is blocked before mutation: instruction initial values, shell exit semantics, tool/event payloads, permission rules, workspace and pending-input mappings are not proven. Keep the database and use the ws client; native historical migrations and V1 import cannot convert this history.")
  }
}

export const legacySessionMetadataMigrationID = "20260511173437_session-metadata"
export const legacySessionMetadataAlias = "20260530232709_lovely_romulus"
export const legacyPartMigrationID = "20260802133449_session_message_part"
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

export function inspect(db: Reader, nativeIDs: readonly string[]) {
  return Effect.gen(function* () {
    const tables = new Set((yield* db.all<{ name: string }>(
      sql`SELECT name FROM sqlite_master WHERE type = 'table' AND name NOT LIKE 'sqlite_%'`,
    )).map((row) => row.name))
    const completed = new Set(tables.has("migration")
      ? (yield* db.all<{ id: string }>(sql`SELECT id FROM migration`)).map((row) => row.id)
      : [])
    const known = new Set([...nativeIDs, ...legacyBaselineMigrationIDs, legacySessionMetadataAlias,
      legacyPartMigrationID, "v01_baseline", "v02_session_message_part", "20260730195856_optional_session_title"])
    if (tables.has("__drizzle_migrations")) {
      const columns = new Set((yield* db.all<{ name: string }>(
        sql`SELECT name FROM pragma_table_info('__drizzle_migrations')`,
      )).map((row) => row.name))
      if (columns.has("name")) {
        const rows = yield* db.all<{ id: string }>(sql`SELECT name AS id FROM __drizzle_migrations WHERE name IS NOT NULL`)
        rows.forEach((row) => completed.add(row.id))
      }
      if (columns.has("created_at")) {
        const rows = yield* db.all<{ created_at: number; prefix: string | null }>(sql`
          SELECT created_at, strftime('%Y%m%d%H%M%S', created_at / 1000, 'unixepoch') AS prefix
          FROM __drizzle_migrations WHERE created_at IS NOT NULL`)
        for (const row of rows) {
          const matches = [...known].filter((id) => id.startsWith(`${row.prefix}_`))
          if (!Number.isSafeInteger(row.created_at) || row.created_at % 1000 !== 0 || matches.length !== 1)
            return yield* Effect.fail(new UnsafeMigrationHistoryError([...completed], `unknown timestamp ${row.created_at}`))
          completed.add(matches[0])
        }
      }
      if (!columns.has("name") && !columns.has("created_at"))
        return yield* Effect.fail(new UnsafeMigrationHistoryError([...completed], "unreadable Drizzle ledger"))
    }
    if ([...completed].some((id) => !known.has(id)))
      return yield* Effect.fail(new UnsafeMigrationHistoryError([...completed]))
    const baselineComplete = completed.has("v01_baseline") || legacyBaselineMigrationIDs.every(
      (id) => completed.has(id) || (id === legacySessionMetadataMigrationID && completed.has(legacySessionMetadataAlias)),
    )
    const references = tables.has("session_message")
      ? yield* db.all<{ table: string }>(sql`PRAGMA foreign_key_list('session_message')`)
      : []
    const populated = tables.has("session_message") && (yield* db.all(sql`SELECT 1 FROM session_message LIMIT 1`)).length > 0
    const ws = completed.has("v01_baseline") || completed.has("v02_session_message_part") || completed.has(legacyPartMigrationID) ||
      (!tables.has("session_v2") && tables.has("session_message") &&
        (baselineComplete || (populated && references.some((row) => row.table === "session"))))
    return { tables, completed, baselineComplete, ws }
  })
}

export function requireNative(db: Reader, nativeIDs: readonly string[]) {
  return Effect.gen(function* () {
    const state = yield* inspect(db, nativeIDs)
    if (state.ws) return yield* Effect.fail(new WsUpgradeRequiredError([...state.completed], state.baselineComplete))
    for (const id of ["20260622170816_reset_v2_session_state", "20260804233008_loose_psylocke"]) {
      if (!nativeIDs.includes(id) || state.completed.has(id)) continue
      for (const table of ["session_message", "event", "event_sequence", "session_input", "session_context_epoch"]) {
        if (!state.tables.has(table)) continue
        if ((yield* db.all(sql`SELECT 1 FROM ${sql.identifier(table)} LIMIT 1`)).length)
          return yield* Effect.fail(new UnsafeMigrationHistoryError([...state.completed], `${id} would destroy populated ${table}`))
      }
    }
    return state
  })
}

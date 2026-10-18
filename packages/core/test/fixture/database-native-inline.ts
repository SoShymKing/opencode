import { sql } from "drizzle-orm"
import { Effect } from "effect"
import type { EffectDrizzleSqlite } from "@opencode/core/database/drizzle"

export const nativeInlineRevision = "eeec76bb62"
export const nativeInlineIDs = [
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
  "20260511173437_session-metadata",
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
  "20260804233008_loose_psylocke",
  "20260805200742_import_legacy_credentials",
  "20260808023530_workspace_domain",
  "20260811161259_execution_claim_attempts",
  "20260812181746_session_inbox",
  "20260812213948_worktree",
  "20260819222447_session_viewed_state",
  "20260823191254_nullable_workspace_binding",
  "20260910120000_clear_v1_session_permission",
  "20260923013825_project_time_active",
  "20261007190000_azure_cli_external_credential",
  "20261010180456_project_time_archived",
] as const

export const nativeData = JSON.stringify({
  agent: "build", model: { providerID: "test", id: "model" },
  content: [{ type: "text", text: "first" }, { type: "text", text: "second" }], time: { created: 1 },
})

export function setupNativeInline(db: EffectDrizzleSqlite.EffectSQLiteDatabase) {
  return Effect.gen(function* () {
    const ddl = yield* Effect.promise(() => Bun.file(new URL("database-native-inline.sql", import.meta.url)).text())
    yield* db.run("PRAGMA foreign_keys = ON")
    for (const statement of ddl.split(";").map((statement) => statement.trim()).filter(Boolean))
      yield* db.run(statement)
    yield* db.run(sql`CREATE TABLE migration (id TEXT PRIMARY KEY, time_completed INTEGER NOT NULL)`)
    for (const id of nativeInlineIDs) yield* db.run(sql`INSERT INTO migration VALUES (${id}, 1)`)
    yield* db.run(sql`INSERT INTO project (id, worktree, time_created, time_updated, sandboxes) VALUES ('project', '/repo', 1, 1, '[]')`)
    yield* db.run(sql`INSERT INTO session_v2 (id, project_id, slug, directory, version, time_created, time_updated)
      VALUES ('ses_native_inline', 'project', 'native-inline', '/repo', 'fixture', 1, 1)`)
    yield* db.run(sql`INSERT INTO session_message (id, session_id, type, seq, time_created, time_updated, data)
      VALUES ('msg_migration', 'ses_native_inline', 'assistant', 0, 1, 1, ${nativeData})`)
  })
}

import { readFileSync } from "node:fs"
import { sql } from "drizzle-orm"
import { Effect } from "effect"
import type { EffectDrizzleSqlite } from "@opencode/core/database/drizzle"
import { legacySessionID, model, rawJSON, sessionID } from "./database-source-data"

export type SourceDatabase = EffectDrizzleSqlite.EffectSQLiteDatabase
export type SourceOptions = { readonly directory: string; readonly rich?: boolean }

const files = ["common", "ws", "ws-parts", "previous-native"] as const
const statements = new Map(files.map((name) => [
  name, readFileSync(new URL(`database-source-${name}.sql`, import.meta.url), "utf8")
    .split(";").map((statement) => statement.trim()).filter(Boolean),
]))

export function sourceDDL(db: SourceDatabase, name: typeof files[number]) {
  return Effect.forEach(statements.get(name) ?? [], (statement) => db.run(statement), { discard: true })
}

export function setupCommon(db: SourceDatabase, options: SourceOptions) {
  return Effect.gen(function* () {
    yield* sourceDDL(db, "common")
    yield* db.run(sql`CREATE TABLE migration (id TEXT PRIMARY KEY, time_completed INTEGER NOT NULL)`)
    yield* db.run(sql`INSERT INTO project (id, worktree, vcs, name, icon_url, icon_url_override, icon_color, time_created, time_updated, time_initialized, sandboxes, commands)
      VALUES ('project', ${options.directory}, 'git', 'Source fixture', 'https://example.invalid/icon', NULL, 'blue', 1, 2, 1, '[]', '{"start":"printf fixture"}')`)
    yield* db.run(sql`INSERT INTO project_directory VALUES ('project', ${options.directory}, 'main', NULL, 1)`)
    yield* db.run(sql`INSERT INTO workspace VALUES ('wrk_fixture', 'worktree', 'Fixture', 'fixture', ${options.directory}, ${rawJSON}, 'project', 2)`)
    yield* db.run(sql`INSERT INTO account VALUES ('acc_fixture', 'fixture@example.invalid', 'https://example.invalid', 'fake-access@example.invalid', 'fake-refresh@example.invalid', 1000, 1, 2)`)
    yield* db.run(sql`INSERT INTO account_state VALUES (1, 'acc_fixture', 'org_fixture')`)
    yield* db.run(sql`INSERT INTO control_account VALUES ('control@example.invalid', 'https://example.invalid', 'fake-control-access@example.invalid', 'fake-control-refresh@example.invalid', NULL, 1, 1, 2)`)
    yield* db.run(sql`INSERT INTO credential VALUES ('cred_fixture', 'test', 'Synthetic only', '{"type":"key","key":"fake-key@example.invalid"}', NULL, NULL, 1, 1, 2)`)
    yield* db.run(sql`INSERT INTO permission VALUES ('permission_fixture', 'project', 'read', '*', 1, 2)`)
    yield* db.run(sql`INSERT INTO data_migration VALUES ('fixture-data', 2)`)
    yield* db.run(sql`INSERT INTO session
      (id, project_id, workspace_id, slug, directory, path, title, version, share_url, summary_additions, summary_deletions, summary_files, summary_diffs, metadata, cost, tokens_input, tokens_output, tokens_reasoning, tokens_cache_read, tokens_cache_write, revert, permission, agent, model, time_created, time_updated)
      VALUES (${sessionID}, 'project', 'wrk_fixture', 'fixture-session', ${options.directory}, 'src', 'Source session', 'fixture', 'https://example.invalid/share', 3, 1, 1, '[]', ${rawJSON}, 0.125, 31, 17, 5, 7, 3, NULL, '[]', 'build', ${JSON.stringify(model)}, 1, 50)`)
    yield* db.run(sql`INSERT INTO session_share VALUES (${sessionID}, 'share_fixture', 'fake-share@example.invalid', 'https://example.invalid/share', 1, 2)`)
    yield* db.run(sql`INSERT INTO event_sequence VALUES (${sessionID}, 50, 'owner_fixture')`)
    if (!options.rich) return
    yield* db.run(sql`INSERT INTO session (id, project_id, slug, directory, title, version, time_created, time_updated, time_archived)
      VALUES (${legacySessionID}, 'project', 'legacy-session', ${options.directory}, 'Legacy source session', 'fixture', 1, 2, 3)`)
    yield* db.run(sql`INSERT INTO message VALUES ('msg_legacy', ${legacySessionID}, 1, 2, '{"role":"user","time":{"created":1},"agent":"build","model":{"providerID":"test","modelID":"model"}}')`)
    yield* db.run(sql`INSERT INTO part VALUES ('prt_legacy', 'msg_legacy', ${legacySessionID}, 1, 2, '{"type":"text","text":"Legacy transcript"}')`)
    yield* db.run(sql`ALTER TABLE session ADD COLUMN fixture_extra BLOB`)
    yield* db.run(sql`UPDATE session SET fixture_extra = X'00017F80FF004142' WHERE id = ${sessionID}`)
    yield* db.run(sql`CREATE TABLE fixture_extension (id INTEGER PRIMARY KEY, json_text TEXT NOT NULL, bytes BLOB NOT NULL, absent TEXT)`)
    yield* db.run(sql`INSERT INTO fixture_extension VALUES (1, ${rawJSON}, X'00017F80FF004142', NULL)`)
  })
}

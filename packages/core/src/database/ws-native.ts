export * as WsNative from "./ws-native.js"

import { sql } from "drizzle-orm"
import { Effect, Schema } from "effect"
import type { EffectDrizzleSqlite } from "./drizzle.js"
import { DatabaseLineage } from "./lineage.js"
import { DatabaseMigration } from "./migration.js"
import { DatabaseWsArchive } from "./ws-archive.js"
import { WsNativeScan } from "./ws-native-scan.js"
import { WsNativeWorkspace } from "./ws-native-workspace.js"
import { WsNativeSource } from "./ws-native-source.js"
import { WsNativePrevious } from "./ws-native-previous.js"
import { Instructions } from "../instructions/index.js"
import { SessionTable } from "../session/sql.js"
import azureCredential from "./migration/20261007190000_azure_cli_external_credential.js"
import type { V1Migration } from "./v1-migration.js"
import schema from "./schema.gen.js"

type Database = EffectDrizzleSqlite.EffectSQLiteDatabase
type Reader = Pick<Database, "all" | "get">
const identifier = DatabaseWsArchive.identifier
const required = {
  account: "id email url access_token refresh_token token_expiry time_created time_updated",
  account_state: "id active_account_id active_org_id",
  control_account: "email url access_token refresh_token token_expiry active time_created time_updated",
  credential: "id integration_id label value connector_id method_id active time_created time_updated",
  project: "id worktree vcs name icon_url icon_url_override icon_color time_created time_updated time_initialized sandboxes commands",
  project_directory: "project_id directory type strategy time_created",
  permission: "id project_id action resource time_created time_updated",
  event_sequence: "aggregate_id seq owner_id",
  event: "id aggregate_id seq type data",
  session: "id project_id workspace_id parent_id slug directory path title version share_url summary_additions summary_deletions summary_files summary_diffs metadata cost tokens_input tokens_output tokens_reasoning tokens_cache_read tokens_cache_write revert permission agent model time_created time_updated time_compacting time_archived",
  session_message: "id session_id type seq time_created time_updated data",
  workspace: "id type directory time_used",
  session_context_epoch: "session_id baseline snapshot baseline_seq",
  session_input: "id session_id prompt delivery admitted_seq promoted_seq time_created",
  message: "id session_id time_created time_updated data",
  part: "id message_id session_id time_created time_updated data",
}
const compatible = ["account", "account_state", "control_account", "credential", "project", "project_directory", "permission", "event_sequence"]
const previousSession = "fork_session_id fork_boundary time_suspended"
const layouts: Readonly<Record<string, string>> = { ...required, ...WsNativePrevious.columns }

export class WsNativeError extends Schema.TaggedError<WsNativeError>()("WsNativeError", {
  message: Schema.String,
}) {}
const fail = (message: string) => new WsNativeError({ message })

export function validateSource(db: Reader) {
  return Effect.gen(function* () {
    const state = yield* DatabaseLineage.inspect(db, [...DatabaseMigration.currentMigrationIDs])
    if ((!state.ws && !state.previousNative) || !state.baselineComplete)
      return yield* fail("Source is not a complete supported WS or previous-native lineage")
    const requirements = state.previousNative
      ? { ...Object.fromEntries(Object.entries(required).filter(([name]) => name !== "session_context_epoch" && name !== "session_input")),
        ...WsNativePrevious.columns, session: `${required.session} ${previousSession}`, event: `${required.event} created` }
      : required
    for (const [table, names] of Object.entries(requirements)) {
      const columns = yield* db.all<{ name: string }>(sql`SELECT name FROM pragma_table_info(${table})`)
      for (const name of names.split(" ")) {
        if (!columns.some((column) => column.name === name)) return yield* fail(`Missing source column ${table}.${name}`)
      }
    }
    for (const [table, column, target] of [
      ["session", "project_id", "project"], ["session_message", "session_id", "session"],
      ["event", "aggregate_id", "event_sequence"], ["project_directory", "project_id", "project"],
      ["permission", "project_id", "project"], ["workspace", "project_id", "project"],
    ]) {
      const references = yield* db.all<{ table: string; from: string; to: string }>(sql`SELECT "table", "from", "to" FROM pragma_foreign_key_list(${table})`)
      if (!references.some((reference) => reference.table === target && reference.from === column && reference.to === (target === "event_sequence" ? "aggregate_id" : "id")))
        return yield* fail(`Missing source foreign key ${table}.${column}`)
    }
    for (const table of state.previousNative ? ["session_input", "session_context_epoch", "session_inbox"] : ["session_pending", "session_inbox", "instruction_state", "instruction_entry", "instruction_blob"]) {
      if (state.tables.has(table) && (yield* db.get(sql`SELECT 1 FROM ${identifier(table)} LIMIT 1`)))
        return yield* fail(`Populated ${table} is not supported by the minimal WS bridge`)
    }
    yield* integrity(db)
    for (const [table, columns] of Object.entries({
      session: ["summary_diffs", "metadata", "revert", "permission", "model"],
      project: ["sandboxes", "commands"], credential: ["value"], event: ["data"],
    })) {
      for (const column of columns) {
        if (yield* db.get(sql`SELECT 1 FROM ${identifier(table)} WHERE ${identifier(column)} IS NOT NULL AND NOT json_valid(${identifier(column)}) LIMIT 1`))
          return yield* fail(`Invalid JSON in ${table}.${column}`)
      }
    }
    if (yield* db.get(sql`SELECT 1 FROM workspace WHERE type NOT IN ('worktree', 'folder') OR directory IS NULL OR directory = '' LIMIT 1`))
      return yield* fail("Unsupported workspace type or missing local directory")
    if (yield* db.get(sql`SELECT 1 FROM session s LEFT JOIN workspace w ON w.id = s.workspace_id WHERE s.workspace_id IS NOT NULL AND w.id IS NULL LIMIT 1`))
      return yield* fail("Missing session workspace")
    yield* watermark(db, "session", "session_message", "event")
    yield* WsNativeScan.consume(db, { parent: "session_message", child: !state.previousNative && state.tables.has("session_message_part") ? "session_message_part" : undefined,
      previousNative: state.previousNative }, () => Effect.void)
    yield* WsNativeSource.read(db, undefined, state.previousNative)
    if (state.previousNative) yield* WsNativePrevious.read(db)
    return state
  })
}

export function convert(db: Database) {
  return db.transaction((tx) => Effect.gen(function* () {
    const state = yield* DatabaseLineage.inspect(tx, [...DatabaseMigration.currentMigrationIDs])
    if (state.tables.has("session_v2") && !state.ws && !state.previousNative) {
      if (state.completed.size !== DatabaseMigration.currentMigrationIDs.size || ![...DatabaseMigration.currentMigrationIDs].every((id) => state.completed.has(id)))
        return yield* fail("Native database does not have the complete current ledger")
      yield* validate(db)
      return
    }
    const source = yield* validateSource(tx)
    yield* tx.run("PRAGMA defer_foreign_keys = ON")
    const archive = yield* DatabaseWsArchive.capture(tx, [...source.completed])
    yield* schema.up(tx)
    yield* DatabaseWsArchive.persist(tx, archive)
    yield* restore(tx, archive)
    yield* validate(db)
    yield* tx.run("CREATE TABLE migration (id TEXT PRIMARY KEY, time_completed INTEGER NOT NULL)")
    for (const id of DatabaseMigration.currentMigrationIDs)
      yield* tx.run(sql`INSERT INTO migration VALUES (${id}, ${Date.now()})`)
  }), { behavior: "immediate" }).pipe(Effect.mapError((error) => error instanceof WsNativeError ? error : fail(String(error))))
}

export function restore(db: Database, archive: DatabaseWsArchive.Archive) {
  return Effect.gen(function* () {
    const previousNative = DatabaseLineage.previousNativeMigrationIDs.every((id) => archive.lineage_ids.includes(id))
    for (const target of [...compatible, ...(previousNative ? ["kv", "instruction_blob"] : [])]) {
      const source = DatabaseWsArchive.sourceTable(archive, target)
      if (!source) return yield* fail(`Archive is missing ${target}`)
      const proven = layouts[target].split(" ")
      const targetColumns = yield* db.all<{ name: string }>(sql`SELECT name FROM pragma_table_info(${target})`)
      const columns = sql.join(targetColumns.filter((column) => proven.includes(column.name)).map((column) => identifier(column.name)), sql`, `)
      yield* db.run(sql`INSERT INTO ${identifier(target)} (${columns}) SELECT ${columns} FROM ${identifier(source)}`)
    }
    const rich = yield* WsNativeSource.read(db, archive, previousNative, {
      epoch: (epoch) => Effect.gen(function* () {
        for (const entry of Object.values(epoch.snapshot)) yield* db.run(sql`INSERT INTO instruction_blob (hash, value)
          VALUES (${Instructions.hash(entry.value)}, ${entry.value === null ? null : JSON.stringify(entry.value)}) ON CONFLICT DO NOTHING`)
      }),
    })
    yield* db.run("UPDATE project SET time_active = time_updated")
    for (const grant of rich.grants)
      yield* db.run(sql`UPDATE permission SET action = ${grant.action} WHERE id = ${grant.id}`)
    yield* db.transaction(azureCredential.up)
    yield* db.run(`INSERT INTO worktree SELECT project_id, directory, CASE WHEN strategy = 'git_worktree' THEN 'git' WHEN strategy IS NOT NULL THEN strategy WHEN type = 'git_worktree' THEN 'git' END, time_created FROM project_directory`)
    const workspace = DatabaseWsArchive.sourceTable(archive, "workspace")
    if (!workspace) return yield* fail("Archive is missing workspace")
    yield* db.run(sql`INSERT INTO workspace (id, provider, binding, created_at, last_used_at)
      SELECT id, ${WsNativeWorkspace.Provider}, json_object('directory', directory), time_used, time_used FROM ${identifier(workspace)}`)
    yield* restoreSessions(db, archive)
    if (previousNative) {
      const entries = DatabaseWsArchive.sourceTable(archive, "instruction_entry")
      if (!entries) return yield* fail("Archive is missing instruction_entry")
      const columns = sql.join(WsNativePrevious.columns.instruction_entry.split(" ").map(identifier), sql`, `)
      yield* db.run(sql`INSERT INTO instruction_entry (${columns}) SELECT ${columns} FROM ${identifier(entries)}`)
    }
    yield* db.transaction((tx) => Effect.gen(function* () {
      for (const row of yield* tx.all<{ aggregate_id: string; seq: number }>(sql`SELECT aggregate_id, seq FROM event_sequence`))
        yield* DatabaseWsArchive.persistRoot(tx, { aggregate_id: row.aggregate_id, archive_id: archive.id, through_seq: row.seq, codec_version: previousNative ? 2 : 1 })
    }))
  })
}

export function restoreSessions(db: Database, archive: DatabaseWsArchive.Archive, sessionIDs?: readonly string[]) {
  return Effect.scoped(Effect.gen(function* () {
    const previousNative = DatabaseLineage.previousNativeMigrationIDs.every((id) => archive.lineage_ids.includes(id))
    const owns = (id: string) => sessionIDs === undefined || sessionIDs.includes(id)
    const selected = (column: string) => sessionIDs === undefined ? sql`1` : sql`${sql.raw(column)} IN (${sql.join(sessionIDs.map((id) => sql`${id}`), sql`, `)})`
    for (const target of ["session_v2", "event_sequence", ...(previousNative ? ["instruction_state"] : [])]) {
      const sourceName = target === "session_v2" ? "session" : target
      const source = DatabaseWsArchive.sourceTable(archive, sourceName)
      if (!source) return yield* fail(`Archive is missing ${target}`)
      const proven = `${layouts[sourceName]} ${previousNative && sourceName === "session" ? previousSession : ""}`.split(" ")
      const targetColumns = yield* db.all<{ name: string }>(sql`SELECT name FROM pragma_table_info(${target})`)
      const columns = sql.join(targetColumns.filter((column) => proven.includes(column.name)).map((column) => identifier(column.name)), sql`, `)
      yield* db.run(sql`INSERT INTO ${identifier(target)} (${columns}) SELECT ${columns} FROM ${identifier(source)}
        WHERE ${selected(target === "session_v2" ? "id" : target === "event_sequence" ? "aggregate_id" : "session_id")}
        ${target === "event_sequence" ? sql`ON CONFLICT (aggregate_id) DO NOTHING` : sql``}`)
    }
    const parent = DatabaseWsArchive.sourceTable(archive, "session_message")
    if (!parent) return yield* fail("Archive is missing session_message")
    yield* WsNativeScan.consume(db, { parent, child: previousNative ? undefined : DatabaseWsArchive.sourceTable(archive, "session_message_part"),
      previousNative, store: owns }, (row, stored) => Effect.gen(function* () {
      if (!stored) return
      yield* db.run(sql`INSERT INTO session_message (id, session_id, type, seq, time_created, time_updated, data)
        VALUES (${row.id}, ${row.session_id}, ${stored.type}, ${row.seq}, ${row.time_created}, ${row.time_updated}, ${stored.data})`)
      for (const part of stored.parts) yield* db.run(sql`INSERT INTO session_message_part (message_id, position, id, type, data)
        VALUES (${row.id}, ${part.position}, ${part.id}, ${part.type}, ${part.data})`)
    }))
    const legacyMessages = DatabaseWsArchive.sourceTable(archive, "message")
    const legacyParts = DatabaseWsArchive.sourceTable(archive, "part")
    if (!legacyMessages || !legacyParts) return yield* fail("Archive is missing legacy message or part tables")
    yield* WsNativeScan.rows(db, { table: "session_v2", bytes: sql`0`, columns: sql`m.id`, where: sql`
      ${selected("m.id")} AND NOT EXISTS (SELECT 1 FROM session_message WHERE session_id = m.id)
      AND EXISTS (SELECT 1 FROM ${identifier(legacyMessages)} WHERE session_id = m.id)` }, (sessions: readonly { id: string }[]) => Effect.scoped(Effect.gen(function* () {
      const { V1Migration } = yield* Effect.promise(() => import("./v1-migration.js"))
      const { open } = yield* Effect.promise(() => import("#ws-native-worker"))
      const worker = yield* open
      for (const candidate of sessions) {
        const session = yield* db.select().from(SessionTable).where(sql`${SessionTable.id} = ${candidate.id}`).get()
        if (!session) return yield* fail(`Missing legacy session: ${candidate.id}`)
        const transformed = yield* V1Migration.prepareSession(db, session, { message: legacyMessages, part: legacyParts })
        yield* validateLegacyProjection(transformed)
        let offset = 0
        while (offset < transformed.messages.length) {
          const wave: V1Migration.TransformResult["messages"][number][] = []
          let bytes = 0
          for (const row of transformed.messages.slice(offset, offset + WsNativeScan.rowLimit)) {
            const size = Buffer.byteLength(JSON.stringify(row.data))
            if (wave.length && bytes + size > WsNativeScan.byteLimit) break
            wave.push(row)
            bytes += size
          }
          const results = yield* worker.run(wave.map((row) => ({ kind: "native-encode", row: { id: row.id, type: row.type, data: row.data } })))
          for (const [index, row] of wave.entries()) {
            const result = results[index]
            if (result.kind !== "native-encode") return yield* fail(`Unexpected legacy encode result: ${row.id}`)
            yield* db.run(sql`INSERT INTO session_message (id, session_id, type, seq, time_created, time_updated, data)
              VALUES (${row.id}, ${row.session_id}, ${row.type}, ${row.seq}, ${row.time_created}, ${row.time_updated}, ${result.output.data})`)
            for (const part of result.output.parts) yield* db.run(sql`INSERT INTO session_message_part (message_id, position, id, type, data)
              VALUES (${row.id}, ${part.position}, ${part.id}, ${part.type}, ${part.data})`)
          }
          offset += wave.length
        }
      }
    })))
    yield* db.run(sql`INSERT INTO event_sequence (aggregate_id, seq, owner_id)
      SELECT s.id, coalesce(max(m.seq), -1), NULL FROM session_v2 s LEFT JOIN session_message m ON m.session_id = s.id
      WHERE ${selected("s.id")} AND NOT EXISTS (SELECT 1 FROM event_sequence e WHERE e.aggregate_id = s.id) GROUP BY s.id`)
    const rich = yield* WsNativeSource.read(db, archive, previousNative, {
      epoch: (epoch) => Effect.gen(function* () {
        if (!owns(epoch.sessionID)) return
        yield* db.run(sql`INSERT INTO instruction_state
      (session_id, epoch_start, through_seq, frozen_baseline, initial_values, current_values, imported_removals)
      VALUES (${epoch.sessionID}, ${epoch.baselineSeq}, ${epoch.throughSeq}, ${epoch.baseline}, '{}',
        ${JSON.stringify(Object.fromEntries(Object.entries(epoch.snapshot).map(([key, entry]) => [key, Instructions.hash(entry.value)])))},
         ${JSON.stringify(Object.fromEntries(Object.entries(epoch.snapshot).flatMap(([key, entry]) => entry.removed === undefined ? [] : [[key, entry.removed]])))})`)
      }),
      input: (input) => Effect.gen(function* () {
        if (!owns(input.row.session_id) || input.row.promoted_seq !== null) return
        yield* db.run(sql`INSERT INTO session_inbox (id, session_id, type, payload, delivery, enqueued_seq, time_created)
        VALUES (${input.row.id}, ${input.row.session_id}, 'user', ${JSON.stringify(input.payload)}, ${input.delivery}, ${input.row.admitted_seq}, ${input.row.time_created})`)
      }),
    })
    for (const permission of rich.permissions.filter((item) => owns(item.id)))
      yield* db.run(sql`UPDATE session_v2 SET permission = ${JSON.stringify(permission.rules)} WHERE id = ${permission.id}`)
    if (previousNative) yield* WsNativePrevious.read(db, archive, (item) => Effect.gen(function* () {
      if (!owns(item.row.session_id)) return
      yield* db.run(sql`INSERT INTO session_inbox (id, session_id, type, payload, delivery, enqueued_seq, time_created)
        VALUES (${item.row.id}, ${item.row.session_id}, ${item.row.type}, ${item.row.data}, ${item.delivery}, ${item.row.admitted_seq}, ${item.row.time_created})`)
    }))
  }))
}

export function validateLegacyProjection(transformed: V1Migration.TransformResult & { sourceMessageIDs: ReadonlySet<string> }) {
  const projected = new Set(transformed.messages.map((row) => row.id))
  const missing = [...transformed.sourceMessageIDs].filter((id) => !projected.has(id) && !transformed.omittedMessageIDs.has(id))
  if (transformed.warnings.length || missing.length)
    return Effect.fail(fail(`Legacy transform rejected: unexplained missing source IDs ${missing.length}, warnings ${transformed.warnings.length}, reason classes ${JSON.stringify([...new Set(transformed.warnings.map((warning) => warning.reason))])}`))
  return Effect.void
}

export function validate(db: Database) {
  return Effect.gen(function* () {
    yield* integrity(db)
    yield* watermark(db, "session_v2", "session_message", "event")
    yield* WsNativeScan.validateNative(db)
    if (yield* db.get(sql`SELECT 1 FROM _ws_import_root r LEFT JOIN event_sequence s ON s.aggregate_id = r.aggregate_id WHERE s.aggregate_id IS NULL OR s.seq < r.through_seq OR r.through_seq < -1 OR r.codec_version NOT IN (1, 2) LIMIT 1`))
      return yield* fail("Invalid WS import watermark")
    if (yield* db.get(sql`SELECT 1 FROM event e JOIN _ws_import_root r ON r.aggregate_id = e.aggregate_id WHERE e.seq <= r.through_seq LIMIT 1`))
      return yield* fail("Imported prefix must not contain native events")
    for (const root of yield* db.all<{ archive_id: string; codec_version: number }>(sql`SELECT DISTINCT archive_id, codec_version FROM _ws_import_root`)) {
      const archive = yield* DatabaseWsArchive.read(db, root.archive_id)
      if (!archive) return yield* fail("Missing import archive")
      const previousNative = DatabaseLineage.previousNativeMigrationIDs.every((id) => archive.lineage_ids.includes(id))
      const ws = archive.lineage_ids.includes("v01_baseline") || DatabaseLineage.legacyBaselineMigrationIDs.every((id) =>
        archive.lineage_ids.includes(id) || (id === DatabaseLineage.legacySessionMetadataMigrationID && archive.lineage_ids.includes(DatabaseLineage.legacySessionMetadataAlias)))
      if (!previousNative && !ws) return yield* fail("Import archive has unsupported source lineage")
      if (root.codec_version !== (previousNative ? 2 : 1)) return yield* fail("Import codec does not match source lineage")
    }
  })
}

function integrity(db: Reader) {
  return Effect.gen(function* () {
    const rows = yield* db.all<{ integrity_check: string }>(sql`PRAGMA integrity_check`)
    if (rows.some((row) => row.integrity_check !== "ok")) return yield* fail("Database integrity check failed")
    if ((yield* db.all(sql`PRAGMA foreign_key_check`)).length) return yield* fail("Database foreign key check failed")
  })
}

function watermark(db: Reader, sessions: string, parent: string, events: string) {
  return Effect.gen(function* () {
    if (yield* db.get(sql`SELECT 1 FROM event_sequence WHERE typeof(seq) != 'integer' OR seq < -1 OR seq > 9007199254740991 LIMIT 1`))
      return yield* fail("Invalid event_sequence watermark")
    if (sessions !== "session" && (yield* db.get(sql`SELECT 1 FROM ${identifier(sessions)} s LEFT JOIN event_sequence e ON e.aggregate_id = s.id WHERE e.aggregate_id IS NULL LIMIT 1`)))
      return yield* fail("Session is missing event_sequence watermark")
    for (const table of [parent, events]) {
      const owner = table === parent ? "session_id" : "aggregate_id"
      if (yield* db.get(sql`SELECT 1 FROM ${identifier(table)} m LEFT JOIN event_sequence e ON e.aggregate_id = m.${identifier(owner)} WHERE e.aggregate_id IS NULL OR m.seq > e.seq OR m.seq < 0 OR typeof(m.seq) != 'integer' LIMIT 1`))
        return yield* fail(`Invalid ${table} sequence watermark`)
    }
  })
}

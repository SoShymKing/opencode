export * as SessionWsRebuild from "./ws-rebuild.js"

import { sql } from "drizzle-orm"
import { Effect, Schema } from "effect"
import { isDeepStrictEqual } from "node:util"
import { Event } from "@opencode/schema/event"
import type { Bus } from "../bus.js"
import type { Database } from "../database/database.js"
import { DatabaseWsArchive } from "../database/ws-archive.js"
import { WsNative } from "../database/ws-native.js"

const identifier = DatabaseWsArchive.identifier
const object = Schema.fromJsonString(Schema.Record(Schema.String, Schema.Json))
const fail = (message: string) => new WsNative.WsNativeError({ message })
type Row = Record<string, unknown>

/** DISPOSABLE CLONE ONLY: use a rowid-preserving serialize/export of one current snapshot.
 * The supplied real Bus must use this database, persist:false, and only SessionProjector.
 * Current blobs, entries, execution claims, placement and message envelopes are hybrid inputs.
 * No file activation, runtime services, commit hooks, or general log rebuild is performed.
 */
export function verifyClone(input: {
  readonly db: Database.Interface["db"]
  readonly bus: Bus.Interface
  readonly roots: readonly string[]
}) {
  const db = input.db
  return db.transaction(() => Effect.gen(function* () {
    yield* db.run("PRAGMA defer_foreign_keys = ON")
    if (!input.roots.length) return yield* fail("Select at least one imported Session root")
    const roots = yield* db.all<{ aggregate_id: string; archive_id: string; through_seq: number }>("SELECT * FROM _ws_import_root")
    if (input.roots.some((id) => !roots.some((root) => root.aggregate_id === id)))
      return yield* fail("Missing selected import root")
    const archives = yield* Effect.forEach([...new Set(roots.map((root) => root.archive_id))], (id) => Effect.gen(function* () {
      const archive = yield* DatabaseWsArchive.read(db, id)
      if (!archive) return yield* fail("Missing import archive")
      return archive
    }))
    const edges = yield* db.all<{ id: string; parent_id: string | null; fork_session_id: string | null }>(
      "SELECT id, parent_id, fork_session_id FROM session_v2 UNION SELECT aggregate_id, json_extract(data, '$.parentID'), NULL FROM event WHERE type IN ('session.created.1', 'session.forked.2')",
    )
    for (const archive of archives) {
      const table = DatabaseWsArchive.sourceTable(archive, "session")
      if (!table) return yield* fail("Missing archived Session projection")
      const columns = yield* db.all<{ name: string }>(sql`SELECT name FROM pragma_table_info(${table})`)
      edges.push(...yield* db.all<{ id: string; parent_id: string | null; fork_session_id: string | null }>(sql`
        SELECT id, parent_id, ${columns.some((column) => column.name === "fork_session_id") ? identifier("fork_session_id") : sql`NULL`} AS fork_session_id FROM ${identifier(table)}`))
      const events = DatabaseWsArchive.sourceTable(archive, "event")
      if (!events) return yield* fail("Missing archived events")
      edges.push(...yield* db.all<{ id: string; parent_id: string | null; fork_session_id: string | null }>(sql`
        SELECT aggregate_id AS id, coalesce(json_extract(data, '$.parentID'), json_extract(data, '$.info.parentID')) AS parent_id,
          json_extract(data, '$.info.fork.sessionID') AS fork_session_id FROM ${identifier(events)}
        WHERE type IN ('session.created.1', 'session.forked.2', 'session.updated.1')`))
    }
    if (input.roots.some((id) => !edges.some((edge) => edge.id === id)))
      return yield* fail("Selected import root has no Session prefix evidence")
    const scope = new Set(input.roots)
    for (;;) {
      const size = scope.size
      for (const edge of edges) if ((edge.parent_id && scope.has(edge.parent_id)) || (edge.fork_session_id && scope.has(edge.fork_session_id))) scope.add(edge.id)
      if (scope.size === size) break
    }
    const selected = sql.join([...scope].map((id) => sql`${id}`), sql`, `)
    if (yield* db.get(sql`SELECT 1 FROM session_pending WHERE session_id IN (${selected}) LIMIT 1`))
      return yield* fail("Unexplained current session_pending work")
    const tables = yield* db.all<{ name: string }>("SELECT name FROM sqlite_schema WHERE type = 'table' ORDER BY name")
    const before = new Map<string, Row[]>()
    for (const table of tables) before.set(table.name, yield* db.all<Row>(sql`SELECT ${table.name === "event" ? sql`rowid, *` : sql`*`} FROM ${identifier(table.name)}`))
    const messages = yield* db.all<{ id: string; type: string; time_updated: number; data: string }>(sql`SELECT * FROM session_message WHERE session_id IN (${selected})`)
    const messageIDs = new Set(messages.map((row) => row.id))
    const sessions = yield* db.all<{ id: string; time_suspended: number | null; resume_attempts: number; project_id: string; path: string | null; slug: string }>(sql`SELECT * FROM session_v2 WHERE id IN (${selected})`)
    const cursors = yield* db.all<{ aggregate_id: string; seq: number; owner_id: string | null }>(sql`SELECT * FROM event_sequence WHERE aggregate_id IN (${selected})`)
    const entries = yield* db.all<{ session_id: string; key: string; value: string | null; removed: number; time_created: number; time_updated: number }>(sql`SELECT * FROM instruction_entry WHERE session_id IN (${selected})`)
    const tail = yield* db.all<{ rowid: number; id: string; aggregate_id: string; seq: number; type: string; data: string; created: number }>(sql`SELECT rowid, * FROM event WHERE aggregate_id IN (${selected}) ORDER BY rowid`)
    for (const row of tail) {
      const root = roots.find((root) => root.aggregate_id === row.aggregate_id)
      if (root && row.seq <= root.through_seq) return yield* fail("Native event overlaps imported prefix")
    }
    yield* db.run(sql`DELETE FROM session_v2 WHERE id IN (${selected})`)
    yield* db.run(sql`DELETE FROM event WHERE aggregate_id IN (${selected})`)
    yield* db.run(sql`DELETE FROM event_sequence WHERE aggregate_id IN (${selected})`)
    for (const archive of archives) {
      const imported = roots.filter((root) => scope.has(root.aggregate_id) && root.archive_id === archive.id)
      if (!imported.length) continue
      yield* WsNative.restoreSessions(db, archive, imported.map((root) => root.aggregate_id))
      for (const root of imported) yield* db.run(sql`INSERT INTO event_sequence (aggregate_id, seq, owner_id)
        VALUES (${root.aggregate_id}, ${root.through_seq}, NULL) ON CONFLICT (aggregate_id) DO UPDATE SET seq = excluded.seq, owner_id = NULL`)
    }
    for (const row of tail) yield* input.bus.replay({
      id: yield* Schema.decodeUnknownEffect(Event.ID)(row.id), aggregateID: row.aggregate_id,
      seq: row.seq, type: row.type, created: row.created, data: yield* Schema.decodeUnknownEffect(object)(row.data),
    }, { publish: false })
    const forks = new Set(tail.filter((row) => row.type === "session.forked.2").map((row) => row.aggregate_id))
    for (const row of sessions) {
      yield* db.run(sql`UPDATE session_v2 SET time_suspended = ${row.time_suspended}, resume_attempts = ${row.resume_attempts},
        project_id = ${row.project_id}, path = ${row.path} WHERE id = ${row.id}`)
      if (forks.has(row.id)) yield* db.run(sql`UPDATE session_v2 SET slug = ${row.slug} WHERE id = ${row.id}`)
    }
    for (const row of messages) {
      const rebuilt = yield* db.get(sql`SELECT 1 FROM session_message WHERE id = ${row.id}`)
      if (!rebuilt) return yield* fail(`Missing rebuilt message ${row.id}`)
      for (const key of row.type === "location-switched" ? ["metadata", "previous"] : ["metadata"]) {
        const path = `$.${key}`
        yield* db.run(sql`UPDATE session_message SET data = CASE WHEN json_type(${row.data}, ${path}) IS NULL
          THEN json_remove(data, ${path}) ELSE json_set(data, ${path}, json(${row.data} -> ${path})) END WHERE id = ${row.id}`)
      }
      yield* db.run(sql`UPDATE session_message SET time_updated = ${row.time_updated} WHERE id = ${row.id}`)
    }
    yield* db.run(sql`DELETE FROM instruction_entry WHERE session_id IN (${selected})`)
    for (const row of entries) yield* db.run(sql`INSERT INTO instruction_entry VALUES
      (${row.session_id}, ${row.key}, ${row.value}, ${row.removed}, ${row.time_created}, ${row.time_updated})`)
    for (const row of cursors) yield* db.run(sql`UPDATE event_sequence SET owner_id = ${row.owner_id} WHERE aggregate_id = ${row.aggregate_id}`)
    yield* db.run(sql`DELETE FROM event WHERE aggregate_id IN (${selected})`)
    for (const row of tail) yield* db.run(sql`INSERT INTO event (rowid, id, aggregate_id, seq, type, data, created)
      VALUES (${row.rowid}, ${row.id}, ${row.aggregate_id}, ${row.seq}, ${row.type}, ${row.data}, ${row.created})`)
    const owned = (table: string, row: Row) => {
      if (table === "session_v2") return typeof row.id === "string" && scope.has(row.id)
      if (table === "session_message_part") return typeof row.message_id === "string" && messageIDs.has(row.message_id)
      if (["session_message", "session_inbox", "instruction_state", "instruction_entry", "session_pending"].includes(table))
        return typeof row.session_id === "string" && scope.has(row.session_id)
      if (["event", "event_sequence"].includes(table)) return typeof row.aggregate_id === "string" && scope.has(row.aggregate_id)
      return false
    }
    for (const table of tables) {
      const original = before.get(table.name) ?? []
      const rebuilt = yield* db.all<Row>(sql`SELECT ${table.name === "event" ? sql`rowid, *` : sql`*`} FROM ${identifier(table.name)}`)
      const order = (rows: Row[]) => rows.sort((left, right) => JSON.stringify(left).localeCompare(JSON.stringify(right)))
      if (!isDeepStrictEqual(order(original.filter((row) => !owned(table.name, row))), order(rebuilt.filter((row) => !owned(table.name, row)))))
        return yield* fail(`Changed out-of-scope or authoritative rows in ${table.name}`)
      const normalize = (rows: Row[]) => Effect.forEach(rows.filter((row) => owned(table.name, row)), (row) => Effect.gen(function* () {
        const result = { ...row }
        for (const column of jsonColumns[table.name] ?? []) {
          const value = row[column]
          if (typeof value === "string") result[column] = yield* db.all(sql`SELECT fullkey, type,
            CASE WHEN type IN ('integer', 'real') THEN ${value} -> fullkey ELSE atom END AS value
            FROM json_tree(${value}) ORDER BY fullkey`)
        }
        return result
      }))
      if (!isDeepStrictEqual(order(yield* normalize(original)), order(yield* normalize(rebuilt))))
        return yield* fail(`Unexplained projection difference or unsupported non-event prefix in ${table.name}`)
    }
    if (yield* db.get(sql`SELECT 1 FROM instruction_state s, json_each(s.current_values) j LEFT JOIN instruction_blob b ON b.hash = j.value
      WHERE s.session_id IN (${selected}) AND b.hash IS NULL UNION ALL
      SELECT 1 FROM instruction_state s, json_each(s.initial_values) j LEFT JOIN instruction_blob b ON b.hash = j.value
      WHERE s.session_id IN (${selected}) AND b.hash IS NULL LIMIT 1`)) return yield* fail("Unresolved current instruction hash")
    yield* WsNative.validate(db)
    return { sessions: [...scope], events: tail.length }
  }), { behavior: "immediate" })
}

const jsonColumns: Readonly<Record<string, readonly string[]>> = {
  session_v2: ["fork_boundary", "summary_diffs", "metadata", "revert", "permission", "model"],
  session_message: ["data"], session_message_part: ["data"], session_inbox: ["payload"],
  instruction_state: ["initial_values", "current_values", "imported_removals"],
}

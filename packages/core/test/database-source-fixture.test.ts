import { expect, test } from "bun:test"
import { createHash } from "node:crypto"
import { SqliteClient } from "@effect/sql-sqlite-bun"
import { EffectDrizzleSqlite } from "@opencode/core/database/drizzle"
import { sql } from "drizzle-orm"
import { Effect } from "effect"
import {
  baseline, historicalPartID, instructionCurrentHash, instructionInitialHash, messageData, messageID,
  metadataAlias, nativeMessageData, normalizedMessageData, pendingQueueID, pendingSteerID,
  previousNativeIDs, rawBlobHex, rawJSON, richContent, sessionID, setupPreviousNative, setupWs,
  snapshot, userMessageID, wsBaselineIDs, wsEvents, wsLineages,
} from "./fixture/database-ws"
import type { SourceDatabase } from "./fixture/database-source-setup"
import { sourceDDL } from "./fixture/database-source-setup"

function run<A, E>(use: (db: SourceDatabase) => Effect.Effect<A, E>) {
  return Effect.runPromise(EffectDrizzleSqlite.makeWithDefaults().pipe(
    Effect.flatMap(use), Effect.provide(SqliteClient.layer({ filename: ":memory:", disableWAL: true })), Effect.scoped,
  ))
}

test.each([
  { source: "ws", hash: "e65d1cd09fc8ecbeb6f7bdc0f7d30217fcf4174830d944dc04cad8e62576e47e" },
  { source: "previous-native", hash: "f8ade3ed2d7f15fac1f6ca46a10fdcc1ec182eeef9b5c8591b2562b1d1644dae" },
] as const)("frozen $source DDL matches its historical schema fingerprint", ({ source, hash }) => run((db) => Effect.gen(function* () {
  yield* sourceDDL(db, source)
  yield* sourceDDL(db, "common")
  if (source === "ws") yield* sourceDDL(db, "ws-parts")
  const schema = (yield* db.all<{ name: string; sql: string }>(sql`SELECT name, sql FROM sqlite_master WHERE sql IS NOT NULL ORDER BY name`))
    .map((row) => ({ name: row.name, sql: row.sql.replace(/\s/g, "") }))
  expect(schema).toHaveLength(38)
  expect(createHash("sha256").update(JSON.stringify(schema)).digest("hex")).toBe(hash)
})))

test.each([...wsLineages])("WS %s has complete real schema and a minimal transcript", (lineage) => run((db) => Effect.gen(function* () {
  yield* setupWs(db, { directory: "/fixture/repo", lineage })
  const normalized = lineage === "v02" || lineage === "historical-part"
  expect(yield* db.get(sql`PRAGMA quick_check`)).toEqual({ quick_check: "ok" })
  expect(yield* db.all(sql`PRAGMA foreign_key_check`)).toEqual([])
  expect(yield* db.all(sql`PRAGMA foreign_key_list(session_message)`)).toContainEqual(expect.objectContaining({ table: "session", on_delete: "CASCADE" }))
  expect(yield* db.get(sql`SELECT hex(CAST(data AS BLOB)) bytes FROM session_message WHERE id = ${messageID}`)).toEqual({ bytes: Buffer.from(normalized ? normalizedMessageData : messageData).toString("hex").toUpperCase() })
  expect(yield* db.get(sql`SELECT count(*) count FROM session_message`)).toEqual({ count: 1 })
  expect(yield* db.get(sql`SELECT count(*) count FROM pragma_table_info('session')`)).toEqual({ count: 29 })
  expect(yield* db.get(sql`SELECT count(*) count FROM pragma_table_info('event')`)).toEqual({ count: 5 })
  const ledger = yield* db.all<{ id: string }>(sql`SELECT id FROM migration`)
  const named = lineage === "split-alias" ? yield* db.all<{ id: string }>(sql`SELECT name id FROM __drizzle_migrations`) : []
  const ids = [...ledger, ...named].map((row) => row.id)
  expect(ids).toHaveLength(lineage === "v01" ? 1 : lineage === "v02" ? 2 : lineage === "historical-part" ? 39 : 38)
  if (lineage === "expanded-alias" || lineage === "split-alias") {
    expect(ids).toContain(metadataAlias)
    expect(ids).not.toContain("20260511173437_session-metadata")
  }
  if (lineage === "historical-part") expect(ids).toContain(historicalPartID)
  if (normalized) {
    expect(yield* db.all(sql`SELECT position, id, type, data FROM session_message_part ORDER BY position`)).toEqual([
      { position: 0, id: "text-1", type: "text", data: '{"text":"first"}' },
      { position: 1, id: "text-2", type: "text", data: '{"text":"second"}' },
    ])
    expect((yield* Effect.exit(db.run(sql`INSERT INTO session_message_part VALUES (${messageID}, 2, 'invalid', 'text', '[]')`)))._tag).toBe("Failure")
  }
  expect((yield* Effect.exit(db.run(sql`INSERT INTO session_message VALUES ('msg_orphan', 'missing', 'assistant', 1, 1, 1, '{}')`)))._tag).toBe("Failure")
  expect((yield* Effect.exit(db.run(sql`INSERT INTO session_message VALUES ('msg_duplicate', ${sessionID}, 'assistant', 7, 1, 1, '{}')`)))._tag).toBe("Failure")
})))

test.each(["v01", "v02"] as const)("rich WS %s preserves all source tables and bytes", (lineage) => run((db) => Effect.gen(function* () {
  yield* setupWs(db, { directory: "/fixture/repo", lineage, rich: true })
  const tables = yield* db.all<{ name: string }>(sql`SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name`)
  expect(tables).toHaveLength(lineage === "v02" ? 22 : 21)
  for (const table of tables) expect(yield* db.get(sql`SELECT count(*) > 0 populated FROM ${sql.identifier(table.name)}`)).toEqual({ populated: 1 })
  expect(yield* db.all(sql`PRAGMA foreign_key_check`)).toEqual([])
  expect(yield* db.get(sql`SELECT json_text, hex(bytes) bytes, absent FROM fixture_extension`)).toEqual({ json_text: rawJSON, bytes: rawBlobHex, absent: null })
  expect(yield* db.get(sql`SELECT hex(fixture_extra) bytes, metadata FROM session WHERE id = ${sessionID}`)).toEqual({ bytes: rawBlobHex, metadata: rawJSON })
  expect(yield* db.get(sql`SELECT baseline, snapshot, baseline_seq FROM session_context_epoch`)).toEqual({ baseline, snapshot, baseline_seq: 1 })
  expect(yield* db.all(sql`SELECT id, delivery, admitted_seq, promoted_seq FROM session_input ORDER BY admitted_seq`)).toEqual([
    { id: userMessageID, delivery: "steer", admitted_seq: 2, promoted_seq: 4 },
    { id: pendingSteerID, delivery: "steer", admitted_seq: 40, promoted_seq: null },
    { id: pendingQueueID, delivery: "queue", admitted_seq: 43, promoted_seq: null },
  ])
  expect(yield* db.get(sql`SELECT seq, owner_id FROM event_sequence`)).toEqual({ seq: 50, owner_id: "owner_fixture" })
  expect(yield* db.all(sql`SELECT id, seq, type, data FROM event ORDER BY seq`)).toEqual(wsEvents.map((event) => ({ ...event, data: JSON.stringify(event.data) })))
  const content = lineage === "v01"
    ? yield* db.all<{ type: string; status: string | null }>(sql`SELECT json_extract(value, '$.type') type, json_extract(value, '$.state.status') status FROM session_message, json_each(data, '$.content') WHERE session_message.id = ${messageID}`)
    : yield* db.all<{ type: string; status: string | null }>(sql`SELECT type, json_extract(data, '$.state.status') status FROM session_message_part WHERE message_id = ${messageID} ORDER BY position`)
  expect(content).toHaveLength(richContent.length)
  expect(content.filter((part) => part.type === "tool").map((part) => part.status)).toEqual(["pending", "running", "completed", "completed", "error"])
  expect(yield* db.get(sql`SELECT json_extract(data, '$.exit') exit FROM session_message WHERE type = 'shell'`)).toEqual({ exit: null })
})))

test.each([false, true])("previous native rich=%s uses its own schema and full57 ledger", (rich) => run((db) => Effect.gen(function* () {
  yield* setupPreviousNative(db, { directory: "/fixture/repo", rich })
  expect(yield* db.get(sql`PRAGMA quick_check`)).toEqual({ quick_check: "ok" })
  expect(yield* db.all(sql`PRAGMA foreign_key_check`)).toEqual([])
  expect(yield* db.all(sql`SELECT id FROM migration ORDER BY rowid`)).toEqual(previousNativeIDs.map((id) => ({ id })))
  expect(previousNativeIDs).toHaveLength(57)
  expect(wsBaselineIDs).toHaveLength(38)
  expect(yield* db.get(sql`SELECT hex(CAST(data AS BLOB)) bytes FROM session_message WHERE id = ${messageID}`)).toEqual({ bytes: Buffer.from(nativeMessageData).toString("hex").toUpperCase() })
  expect(yield* db.get(sql`SELECT count(*) count FROM pragma_table_info('event')`)).toEqual({ count: 6 })
  expect(yield* db.all(sql`SELECT name FROM sqlite_master WHERE name IN ('session_v2', 'session_context_epoch', 'session_input', 'todo', 'session_message_part')`)).toEqual([])
  if (!rich) return
  const tables = yield* db.all<{ name: string }>(sql`SELECT name FROM sqlite_master WHERE type = 'table'`)
  expect(tables).toHaveLength(23)
  for (const table of tables) expect(yield* db.get(sql`SELECT count(*) > 0 populated FROM ${sql.identifier(table.name)}`)).toEqual({ populated: 1 })
  expect(yield* db.get(sql`SELECT initial_values, current_values, epoch_start, through_seq FROM instruction_state`)).toEqual({ initial_values: JSON.stringify({ "fixture/policy": instructionInitialHash }), current_values: JSON.stringify({ "fixture/policy": instructionCurrentHash }), epoch_start: 1, through_seq: 13 })
  expect(instructionInitialHash).not.toBe(instructionCurrentHash)
  expect(yield* db.get(sql`SELECT created FROM event`)).toEqual({ created: 4 })
  expect((yield* Effect.exit(db.run(sql`INSERT INTO session_pending VALUES ('msg_duplicate_compaction', ${sessionID}, 'compaction', '{}', NULL, 45, 45)`)))._tag).toBe("Failure")
})))

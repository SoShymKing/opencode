import { createHash } from "node:crypto"
import { sql } from "drizzle-orm"
import { Effect } from "effect"
import { setupCommon, sourceDDL } from "./database-source-setup"
import type { SourceDatabase, SourceOptions } from "./database-source-setup"
import {
  baseline, forkSessionID, historicalPartID, messageContent, messageData, messageID, metadataAlias,
  nativeMessageData, normalizedMessageData, pendingQueueID, pendingSteerID, previousNativeIDs,
  prompt, richContent, richMessageData, sessionID, snapshot, userMessageID, wsBaselineIDs, wsEvents,
} from "./database-source-data"
import type { WsLineage } from "./database-source-data"

export * from "./database-source-data"

export function setupWs(db: SourceDatabase, options: SourceOptions & { readonly lineage?: WsLineage }) {
  return Effect.gen(function* () {
    const lineage = options.lineage ?? "v01"
    const normalized = lineage === "v02" || lineage === "historical-part"
    const ids = lineage === "v01" ? ["v01_baseline"]
      : lineage === "v02" ? ["v01_baseline", "v02_session_message_part"]
      : lineage === "historical-part" ? [...wsBaselineIDs, historicalPartID]
      : wsBaselineIDs.map((id) => lineage !== "expanded" && id === "20260511173437_session-metadata" ? metadataAlias : id)
    yield* db.run("PRAGMA foreign_keys = ON")
    yield* sourceDDL(db, "ws")
    yield* setupCommon(db, options)
    if (lineage === "split-alias") yield* db.run(sql`CREATE TABLE __drizzle_migrations (id INTEGER PRIMARY KEY, hash text NOT NULL, created_at numeric, name text, applied_at TEXT)`)
    for (const [index, id] of ids.entries()) {
      yield* lineage === "split-alias" && index % 2 === 1
        ? db.run(sql`INSERT INTO __drizzle_migrations (hash, name, applied_at) VALUES ('fixture-hash', ${id}, '2026-08-02T00:00:00.000Z')`)
        : db.run(sql`INSERT INTO migration VALUES (${id}, 1)`)
    }
    yield* db.run(sql`INSERT INTO session_message VALUES (${messageID}, ${sessionID}, 'assistant', 7, 1, 16, ${options.rich ? richMessageData : messageData})`)
    if (normalized) {
      yield* sourceDDL(db, "ws-parts")
      const content = options.rich ? richContent : messageContent
      const parent = options.rich
        ? JSON.stringify({ agent: "build", model: { providerID: "test", id: "model" }, time: { created: 1, completed: 16 }, finish: "tool-calls", cost: 0.125, streamEventCount: 27, tokens: { input: 31, output: 17, reasoning: 5, cache: { read: 7, write: 3 } }, snapshot: { start: "before", end: "after", files: ["result.txt"] } })
        : normalizedMessageData
      yield* db.run(sql`UPDATE session_message SET data = ${parent} WHERE id = ${messageID}`)
      for (const [position, part] of content.entries()) {
        const { id, type, ...data } = part
        yield* db.run(sql`INSERT INTO session_message_part VALUES (${messageID}, ${position}, ${id}, ${type}, ${JSON.stringify(data)})`)
      }
    }
    if (!options.rich) return
    yield* db.run(sql`INSERT INTO todo VALUES (${sessionID}, 'Verify archived bytes', 'pending', 'high', 0, 1, 2)`)
    yield* db.run(sql`INSERT INTO session_context_epoch VALUES (${sessionID}, ${baseline}, ${snapshot}, 1)`)
    yield* db.run(sql`INSERT INTO session_input VALUES
      (${userMessageID}, ${sessionID}, ${JSON.stringify(prompt)}, 'steer', 2, 4, 1),
      (${pendingSteerID}, ${sessionID}, '{"text":"Steer next"}', 'steer', 40, NULL, 40),
      (${pendingQueueID}, ${sessionID}, '{"text":"Queue later"}', 'queue', 43, NULL, 43)`)
    yield* db.run(sql`INSERT INTO session_message VALUES (${userMessageID}, ${sessionID}, 'user', 4, 2, 2, ${JSON.stringify({ ...prompt, time: { created: 2 } })})`)
    yield* db.run(sql`INSERT INTO session_message VALUES ('msg_context_explicit', ${sessionID}, 'system', 13, 5, 5, '{"text":"Current policy differs from the baseline","time":{"created":5}}')`)
    yield* db.run(sql`INSERT INTO session_message VALUES ('msg_shell_explicit', ${sessionID}, 'shell', 20, 20, 21, '{"callID":"shell-call","command":"printf fixture","output":"fixture","time":{"created":20,"completed":21}}')`)
    yield* db.run(sql`INSERT INTO session_message VALUES ('msg_compaction_explicit', ${sessionID}, 'compaction', 30, 30, 31, '{"reason":"manual","summary":"Checked the files","recent":"Keep the pending work","time":{"created":30}}')`)
    for (const event of wsEvents) yield* db.run(sql`INSERT INTO event VALUES (${event.id}, ${sessionID}, ${event.seq}, ${event.type}, ${JSON.stringify(event.data)})`)
  })
}

export const instructionInitial = JSON.stringify("Original native instructions")
export const instructionCurrent = JSON.stringify("Updated native instructions")
export const instructionInitialHash = createHash("sha256").update(instructionInitial).digest("hex")
export const instructionCurrentHash = createHash("sha256").update(instructionCurrent).digest("hex")

export function setupPreviousNative(db: SourceDatabase, options: SourceOptions) {
  return Effect.gen(function* () {
    yield* db.run("PRAGMA foreign_keys = ON")
    yield* sourceDDL(db, "previous-native")
    yield* setupCommon(db, options)
    for (const id of previousNativeIDs) yield* db.run(sql`INSERT INTO migration VALUES (${id}, 1)`)
    yield* db.run(sql`INSERT INTO session_message VALUES (${messageID}, ${sessionID}, 'assistant', 7, 1, 16, ${nativeMessageData})`)
    if (!options.rich) return
    yield* db.run(sql`INSERT INTO kv VALUES ('fixture:state', '{"enabled":true}', 1, 2)`)
    yield* db.run(sql`INSERT INTO instruction_blob VALUES (${instructionInitialHash}, ${instructionInitial}), (${instructionCurrentHash}, ${instructionCurrent})`)
    yield* db.run(sql`INSERT INTO instruction_entry VALUES (${sessionID}, 'fixture/policy', ${instructionCurrent}, 0, 1, 13), (${sessionID}, 'fixture/removed', NULL, 1, 1, 13)`)
    yield* db.run(sql`INSERT INTO instruction_state VALUES (${sessionID}, 1, 13, ${JSON.stringify({ "fixture/policy": instructionInitialHash })}, ${JSON.stringify({ "fixture/policy": instructionCurrentHash })})`)
    yield* db.run(sql`INSERT INTO session_pending VALUES
      (${pendingSteerID}, ${sessionID}, 'user', '{"text":"Steer next"}', 'steer', 40, 40),
      (${pendingQueueID}, ${sessionID}, 'synthetic', '{"text":"Queue later","description":"fixture"}', 'queue', 43, 43),
      ('msg_pending_compaction', ${sessionID}, 'compaction', '{}', NULL, 44, 44)`)
    yield* db.run(sql`INSERT INTO session (id, project_id, parent_id, fork_session_id, fork_boundary, slug, directory, title, version, time_created, time_updated, time_suspended)
      VALUES (${forkSessionID}, 'project', ${sessionID}, ${sessionID}, '{"type":"through","messageID":"msg_migration"}', 'fork-session', ${options.directory}, NULL, 'fixture', 20, 21, 22)`)
    yield* db.run(sql`INSERT INTO event VALUES ('evt_native_text', ${sessionID}, 10, 4, 'session.text.ended.1', '{"sessionID":"ses_migration","assistantMessageID":"msg_migration","ordinal":0,"text":"first"}')`)
  })
}

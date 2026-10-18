export * as WsNativeSource from "./ws-native-source.js"

import { sql } from "drizzle-orm"
import { Effect, Schema } from "effect"
import { SessionInbox } from "@opencode/schema/session-inbox"
import { SessionV1 } from "@opencode/schema/session-v1"
import { SessionSchema } from "../session/schema.js"
import { ConfigMigrateV1 } from "../v1/config/migrate.js"
import type { EffectDrizzleSqlite } from "./drizzle.js"
import { DatabaseWsArchive } from "./ws-archive.js"
import { WsEventCodec } from "./ws-event-codec.js"
import { WsNativeMessage } from "./ws-native-message.js"

type Reader = Pick<EffectDrizzleSqlite.EffectSQLiteDatabase, "all" | "get">
const jsonObject = Schema.fromJsonString(Schema.Record(Schema.String, Schema.Json))
const snapshot = Schema.fromJsonString(Schema.Record(Schema.String, Schema.Struct({
  value: Schema.Json, removed: Schema.optional(Schema.String),
})))
const rules = Schema.fromJsonString(Schema.Array(Schema.Struct({
  permission: Schema.String, pattern: Schema.String, action: Schema.Literals(["allow", "deny", "ask"]),
})))

export class SourceError extends Schema.TaggedError<SourceError>()("WsNativeSourceError", {
  message: Schema.String,
}) {}
const fail = (message: string) => new SourceError({ message })

export function read(db: Reader, archive?: DatabaseWsArchive.Archive, previousNative = false) {
  return Effect.gen(function* () {
    const table = (name: string) => {
      const source = archive ? DatabaseWsArchive.sourceTable(archive, name) : name
      if (!source) throw fail(`Archive is missing ${name}`)
      return DatabaseWsArchive.identifier(source)
    }
    const events = previousNative ? [] : yield* db.all<WsEventCodec.Row>(sql`SELECT * FROM ${table("event")} ORDER BY aggregate_id, seq`)
    for (const row of events) yield* Effect.try({
      try: () => WsEventCodec.decode(row),
      catch: (cause) => fail(`Invalid source event ${row.id}: ${String(cause)}`),
    })
    const permissions = yield* Effect.forEach(
      yield* db.all<{ id: string; permission: string }>(sql`SELECT id, permission FROM ${table("session")} WHERE permission IS NOT NULL`),
      (row) => Effect.gen(function* () {
        const decoded = yield* Schema.decodeUnknownEffect(rules)(row.permission).pipe(
          Effect.mapError((error) => fail(`Invalid session.permission ${row.id}: ${error.message}`)),
        )
        return { id: row.id, rules: decoded.map((rule) => ({ action: ConfigMigrateV1.normalizeAction(rule.permission), resource: rule.pattern, effect: rule.action })) }
      }),
    )
    const grants = (yield* db.all<{ id: string; project_id: string; action: string; resource: string }>(sql`SELECT id, project_id, action, resource FROM ${table("permission")}`))
      .map((row) => ({ ...row, action: ConfigMigrateV1.normalizeAction(row.action) }))
    const keys = grants.map((row) => JSON.stringify([row.project_id, row.action, row.resource]))
    if (new Set(keys).size !== keys.length) return yield* fail("Saved permission action rename collision")
    const epochs = previousNative ? [] : yield* Effect.forEach(yield* db.all<{
      session_id: string; baseline: string; snapshot: string; baseline_seq: number; seq: number | null;
    }>(sql`SELECT c.*, e.seq FROM ${table("session_context_epoch")} c LEFT JOIN ${table("event_sequence")} e ON e.aggregate_id = c.session_id`), (row) => Effect.gen(function* () {
      if (row.seq === null || !Number.isSafeInteger(row.baseline_seq) || row.baseline_seq < 0 || row.baseline_seq > row.seq)
        return yield* fail(`Invalid context epoch watermark ${row.session_id}`)
      return {
        sessionID: yield* Schema.decodeUnknownEffect(SessionSchema.ID)(row.session_id),
        baseline: yield* Schema.decodeUnknownEffect(Schema.String)(row.baseline),
        baselineSeq: row.baseline_seq,
        snapshot: yield* Schema.decodeUnknownEffect(snapshot)(row.snapshot), throughSeq: row.seq,
      }
    }))
    const inputs = previousNative ? [] : yield* Effect.forEach(yield* db.all<{
      id: string; session_id: string; prompt: string; delivery: string; admitted_seq: number; promoted_seq: number | null; time_created: number; seq: number | null;
    }>(sql`SELECT i.*, e.seq FROM ${table("session_input")} i LEFT JOIN ${table("event_sequence")} e ON e.aggregate_id = i.session_id ORDER BY i.session_id, i.admitted_seq`), (row) => Effect.gen(function* () {
      if (row.seq === null || !Number.isSafeInteger(row.admitted_seq) || row.admitted_seq < 0 || row.admitted_seq > row.seq ||
        (row.promoted_seq !== null && (!Number.isSafeInteger(row.promoted_seq) || row.promoted_seq < row.admitted_seq || row.promoted_seq > row.seq)))
        return yield* fail(`Invalid session_input watermark ${row.id}`)
      const visible = yield* db.get<{ session_id: string; type: string; seq: number }>(sql`SELECT session_id, type, seq FROM ${table("session_message")} WHERE id = ${row.id}`)
      if (row.promoted_seq === null ? visible !== undefined : !visible || visible.session_id !== row.session_id || visible.type !== "user" || visible.seq !== row.promoted_seq)
        return yield* fail(`Input promotion does not match visible message ${row.id}`)
      const prompt = yield* Schema.decodeUnknownEffect(jsonObject)(row.prompt)
      const converted = yield* WsNativeMessage.convert({ id: row.id, sessionID: row.session_id, type: "user",
        data: JSON.stringify({ text: prompt.text, files: prompt.files, agents: prompt.agents, metadata: prompt.metadata, time: { created: row.time_created } }),
      })
      if (converted.message.type !== "user") return yield* fail(`Invalid user input ${row.id}`)
      const payload = yield* Schema.decodeUnknownEffect(Schema.toType(SessionInbox.UserPayload))({
        text: converted.message.text, files: converted.message.files, recordedFiles: converted.message.recordedFiles,
        agents: converted.message.agents, metadata: converted.message.metadata,
      })
      return { row, delivery: yield* Schema.decodeUnknownEffect(SessionInbox.Delivery)(row.delivery),
        payload: yield* Schema.encodeEffect(SessionInbox.UserPayload)(payload) }
    }))
    const legacy = yield* db.all<{ id: string; session_id: string; data: string }>(sql`SELECT m.* FROM ${table("message")} m WHERE NOT EXISTS (SELECT 1 FROM ${table("session_message")} n WHERE n.session_id = m.session_id)`)
    for (const row of legacy) {
      const data = yield* Schema.decodeUnknownEffect(jsonObject)(row.data)
      yield* Schema.decodeUnknownEffect(SessionV1.Info)({ ...data, id: row.id, sessionID: row.session_id })
    }
    const parts = yield* db.all<{ id: string; message_id: string; session_id: string; data: string }>(sql`SELECT p.* FROM ${table("part")} p WHERE NOT EXISTS (SELECT 1 FROM ${table("session_message")} n WHERE n.session_id = p.session_id)`)
    for (const row of parts) {
      if (!legacy.some((message) => message.id === row.message_id && message.session_id === row.session_id))
        return yield* fail(`Orphan legacy part ${row.id}`)
      const data = yield* Schema.decodeUnknownEffect(jsonObject)(row.data)
      yield* Schema.decodeUnknownEffect(SessionV1.Part)({ ...data, id: row.id, messageID: row.message_id, sessionID: row.session_id })
    }
    return { permissions, grants, epochs, inputs }
  })
}

export * as WsNativeSource from "./ws-native-source.js"

import { sql } from "drizzle-orm"
import { Effect, Schema } from "effect"
import { SessionInbox } from "@opencode/schema/session-inbox"
import { SessionSchema } from "../session/schema.js"
import { ConfigMigrateV1 } from "../v1/config/migrate.js"
import type { EffectDrizzleSqlite } from "./drizzle.js"
import { DatabaseWsArchive } from "./ws-archive.js"
import type { WsEventCodec } from "./ws-event-codec.js"
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

export type Epoch = {
  sessionID: typeof SessionSchema.ID.Type; baseline: string; baselineSeq: number;
  snapshot: typeof snapshot.Type; throughSeq: number;
}
type InputRow = {
  id: string; session_id: string; delivery: string; admitted_seq: number; promoted_seq: number | null;
  time_created: number; seq: number | null;
}
export type Input = { row: InputRow; delivery: typeof SessionInbox.Delivery.Type; payload: typeof SessionInbox.UserPayload.Encoded }
export type Visitors<E = never, R = never> = {
  epoch?: (epoch: Epoch) => Effect.Effect<void, E, R>;
  input?: (input: Input) => Effect.Effect<void, E, R>;
}

export function read<E = never, R = never>(db: Reader, archive?: DatabaseWsArchive.Archive, previousNative = false, visitors: Visitors<E, R> = {}) {
  return Effect.scoped(Effect.gen(function* () {
    const { WsNativeScan } = yield* Effect.promise(() => import("./ws-native-scan.js"))
    const { open } = yield* Effect.promise(() => import("#ws-native-worker"))
    const worker = yield* open
    const source = (name: string) => {
      const source = archive ? DatabaseWsArchive.sourceTable(archive, name) : name
      if (!source) throw fail(`Archive is missing ${name}`)
      return source
    }
    const table = (name: string) => DatabaseWsArchive.identifier(source(name))
    if (!previousNative) yield* WsNativeScan.rows(db, { table: source("event"), bytes: sql`octet_length(m.data)` },
      (rows: readonly WsEventCodec.Row[]) => worker.run(rows.map((row) => ({ kind: "ws-event", row }))).pipe(
        Effect.mapError((error) => fail(`Invalid source event ${error.messageID}: ${error.message}`)), Effect.asVoid))
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
    if (!previousNative) yield* WsNativeScan.rows(db, {
      table: source("session_context_epoch"), key: "session_id", bytes: sql`octet_length(m.baseline) + octet_length(m.snapshot)`,
      columns: sql`m.*, (SELECT seq FROM ${table("event_sequence")} e WHERE e.aggregate_id = m.session_id) AS seq`,
    }, (rows: readonly {
      session_id: string; baseline: string; snapshot: string; baseline_seq: number; seq: number | null;
    }[]) => Effect.forEach(rows, (row) => Effect.gen(function* () {
      if (row.seq === null || !Number.isSafeInteger(row.baseline_seq) || row.baseline_seq < 0 || row.baseline_seq > row.seq)
        return yield* fail(`Invalid context epoch watermark ${row.session_id}`)
      const epoch = {
        sessionID: yield* Schema.decodeUnknownEffect(SessionSchema.ID)(row.session_id),
        baseline: yield* Schema.decodeUnknownEffect(Schema.String)(row.baseline),
        baselineSeq: row.baseline_seq,
        snapshot: yield* Schema.decodeUnknownEffect(snapshot)(row.snapshot), throughSeq: row.seq,
      }
      if (visitors.epoch) yield* visitors.epoch(epoch)
    }), { discard: true }))
    if (!previousNative) yield* WsNativeScan.rows(db, {
      table: source("session_input"), bytes: sql`octet_length(m.prompt)`,
      columns: sql`m.*, (SELECT seq FROM ${table("event_sequence")} e WHERE e.aggregate_id = m.session_id) AS seq`,
    }, (rows: readonly (InputRow & { prompt: string })[]) => Effect.forEach(rows, (row) => Effect.gen(function* () {
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
      const input = { row: { id: row.id, session_id: row.session_id, delivery: row.delivery, admitted_seq: row.admitted_seq,
        promoted_seq: row.promoted_seq, time_created: row.time_created, seq: row.seq }, delivery: yield* Schema.decodeUnknownEffect(SessionInbox.Delivery)(row.delivery),
        payload: yield* Schema.encodeEffect(SessionInbox.UserPayload)(payload) }
      if (visitors.input) yield* visitors.input(input)
    }), { discard: true }))
    const legacyOnly = sql`NOT EXISTS (SELECT 1 FROM ${table("session_message")} n WHERE n.session_id = m.session_id)`
    yield* WsNativeScan.rows(db, { table: source("message"), bytes: sql`octet_length(m.data)`, where: legacyOnly },
      (rows: readonly { id: string; session_id: string; data: string; time_created: number; time_updated: number }[]) =>
        worker.run(rows.map((row) => ({ kind: "legacy-message", mode: "validate", row }))).pipe(Effect.asVoid))
    const orphan = yield* db.get<{ id: string }>(sql`SELECT m.id FROM ${table("part")} m WHERE ${legacyOnly}
      AND NOT EXISTS (SELECT 1 FROM ${table("message")} p WHERE p.id = m.message_id AND p.session_id = m.session_id) LIMIT 1`)
    if (orphan) return yield* fail(`Orphan legacy part ${orphan.id}`)
    yield* WsNativeScan.rows(db, { table: source("part"), bytes: sql`octet_length(m.data)`, where: legacyOnly },
      (rows: readonly { id: string; message_id: string; session_id: string; data: string; time_created: number; time_updated: number }[]) =>
        worker.run(rows.map((row) => ({ kind: "legacy-part", mode: "validate", row }))).pipe(Effect.asVoid))
    return { permissions, grants }
  }))
}

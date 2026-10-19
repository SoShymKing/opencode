export * as WsNativePrevious from "./ws-native-previous.js"

import { sql } from "drizzle-orm"
import { Effect, Schema, type SchemaAST } from "effect"
import { SessionInbox } from "@opencode/schema/session-inbox"
import { SessionMessage } from "@opencode/schema/session-message"
import { SessionEvent } from "@opencode/schema/session-event"
import { Instruction } from "@opencode/schema/instruction"
import { Session } from "@opencode/schema/session"
import type { EffectDrizzleSqlite } from "./drizzle.js"
import { DatabaseWsArchive } from "./ws-archive.js"
import { WsEventCodec } from "./ws-event-codec.js"

type Reader = Pick<EffectDrizzleSqlite.EffectSQLiteDatabase, "all" | "get">
const json = Schema.fromJsonString(Schema.Record(Schema.String, Schema.Json))
const options = { onExcessProperty: "error" } satisfies SchemaAST.ParseOptions
const pendingMessage = Schema.Union([
  Schema.Struct({ type: Schema.Literal("user"), data: SessionInbox.UserPayload, delivery: SessionInbox.Delivery }),
  Schema.Struct({ type: Schema.Literal("synthetic"), data: SessionInbox.SyntheticPayload, delivery: SessionInbox.Delivery }),
])
const base = { sessionID: Session.ID }
const input = { ...base, inputID: SessionMessage.ID }
const legacyEvents = new Set(["session.created.1", "session.updated.1", "session.deleted.1", "message.updated.1", "message.removed.1", "message.part.updated.1", "message.part.removed.1"])
const event = <const Type extends string, Fields extends Schema.Struct.Fields>(type: Type, fields: Fields) =>
  Schema.Struct({ type: Schema.Literal(type), data: Schema.Struct(fields) })
const events = Schema.Union([
  ...[
    SessionEvent.AgentSelected, SessionEvent.ModelSelected, SessionEvent.Renamed, SessionEvent.UsageRecorded,
    SessionEvent.Deleted, SessionEvent.Forked, SessionEvent.Execution.Started, SessionEvent.Execution.Succeeded,
    SessionEvent.Execution.Failed, SessionEvent.Execution.Interrupted, SessionEvent.InstructionsUpdated,
    SessionEvent.Synthetic, SessionEvent.Skill.Activated, SessionEvent.Shell.Started, SessionEvent.Shell.Ended,
    SessionEvent.Step.Ended, SessionEvent.Step.Failed, SessionEvent.Text.Started,
    SessionEvent.Text.Ended, SessionEvent.Reasoning.Started, SessionEvent.Reasoning.Ended,
    SessionEvent.Tool.Input.Started, SessionEvent.Tool.Input.Ended, SessionEvent.Tool.Called,
    SessionEvent.Tool.Success, SessionEvent.Tool.Failed, SessionEvent.RetryScheduled,
    SessionEvent.Compaction.Started, SessionEvent.Compaction.Ended, SessionEvent.Compaction.Failed,
    SessionEvent.RevertEvent.Staged, SessionEvent.RevertEvent.Cleared, SessionEvent.RevertEvent.Committed,
  ].map((definition) => Schema.Struct({ type: Schema.Literal(`${definition.type}.${definition.durable.version}`), data: definition.data })),
  event("session.step.started.1", {
    ...base,
    assistantMessageID: SessionMessage.ID,
    agent: SessionEvent.Step.Started.data.fields.agent,
    model: SessionEvent.Step.Started.data.fields.model,
    snapshot: SessionEvent.Step.Started.data.fields.snapshot,
  }),
  event("session.moved.1", { ...SessionEvent.Moved.data.fields, projectID: Schema.optional(SessionEvent.Moved.data.fields.projectID) }),
  event("session.input.promoted.1", input),
  event("session.input.admitted.1", { ...input, input: pendingMessage }),
  event("session.compaction.admitted.1", input),
])

export const columns = {
  kv: "key value time_created time_updated",
  instruction_blob: "hash value",
  instruction_entry: "session_id key value removed time_created time_updated",
  instruction_state: "session_id epoch_start through_seq initial_values current_values",
  session_pending: "id session_id type data delivery admitted_seq time_created",
}

export class SourceError extends Schema.TaggedError<SourceError>()("PreviousNativeSourceError", {
  message: Schema.String,
}) {}
const fail = (message: string) => new SourceError({ message })

export function message(row: { id: string; type: string; data: string }) {
  return Effect.gen(function* () {
    const value = yield* Schema.decodeUnknownEffect(json)(row.data)
    if ("id" in value || "type" in value) return yield* fail(`P message data contains identity: ${row.id}`)
    return yield* Schema.decodeUnknownEffect(SessionMessage.Info)({ ...value, id: row.id, type: row.type }, options)
  })
}

export function validateEvent(row: WsEventCodec.Row & { created: number }) {
  return Effect.gen(function* () {
    if (!Number.isSafeInteger(row.created)) return yield* fail(`Invalid P event time: ${row.id}`)
    if (legacyEvents.has(row.type)) {
      yield* Effect.try({ try: () => WsEventCodec.decode(row), catch: (cause) => fail(`Invalid P legacy event: ${String(cause)}`) })
      return
    }
    const value = yield* Schema.decodeUnknownEffect(json)(row.data)
    yield* Schema.decodeUnknownEffect(events)({ type: row.type, data: value }, options)
    if (value.sessionID !== row.aggregate_id) return yield* fail(`P event aggregate mismatch: ${row.id}`)
  })
}

type PendingRow = {
  id: string; session_id: string; type: string; data: string; delivery: string | null; admitted_seq: number; time_created: number; seq: number | null;
}
export type Pending = { row: PendingRow; delivery: typeof SessionInbox.Delivery.Type }

export function read<E = never, R = never>(db: Reader, archive?: DatabaseWsArchive.Archive, acceptPending?: (pending: Pending) => Effect.Effect<void, E, R>) {
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
    yield* WsNativeScan.rows(db, { table: source("event"), bytes: sql`octet_length(m.data)`, workers: true },
      (rows: readonly (WsEventCodec.Row & { created: number })[]) => worker.run(rows.map((row) => ({ kind: "p-event", row }))).pipe(Effect.asVoid))
    yield* WsNativeScan.rows(db, { table: source("instruction_blob"), key: "hash", bytes: sql`coalesce(octet_length(m.value), 0)` },
      (rows: readonly { hash: string; value: string | null }[]) => Effect.forEach(rows, (row) => Effect.gen(function* () {
      yield* Schema.decodeUnknownEffect(Instruction.Hash)(row.hash)
      if (row.value !== null) yield* Schema.decodeUnknownEffect(Schema.fromJsonString(Schema.Json))(row.value)
    }), { discard: true }))
    let cursor: { session_id: string; key: string } | undefined
    while (true) {
      const keys = yield* db.all<{ session_id: string; key: string }>(sql`SELECT session_id, key FROM ${table("instruction_entry")}
        WHERE ${cursor ? sql`(session_id, key) > (${cursor.session_id}, ${cursor.key})` : sql`1`}
        ORDER BY session_id, key LIMIT 128`)
      if (!keys.length) break
      for (const key of keys) {
        const row = yield* db.get<{ value: string | null; removed: number }>(sql`SELECT value, removed FROM ${table("instruction_entry")}
          WHERE session_id = ${key.session_id} AND key = ${key.key}`)
        if (!row) return yield* fail("P instruction entry disappeared")
        if (row.removed !== 0 && row.removed !== 1) return yield* fail("Invalid P instruction removal flag")
        if (row.value !== null) yield* Schema.decodeUnknownEffect(Schema.fromJsonString(Schema.Json))(row.value)
        cursor = key
      }
    }
    yield* WsNativeScan.rows(db, { table: source("instruction_state"), key: "session_id",
      bytes: sql`octet_length(m.initial_values) + octet_length(m.current_values)`,
      columns: sql`m.*, (SELECT seq FROM ${table("event_sequence")} e WHERE e.aggregate_id = m.session_id) AS seq`,
    }, (rows: readonly { session_id: string; epoch_start: number; through_seq: number; initial_values: string; current_values: string; seq: number | null }[]) => Effect.forEach(rows, (row) => Effect.gen(function* () {
      if (row.seq === null || !Number.isSafeInteger(row.epoch_start) || !Number.isSafeInteger(row.through_seq) || row.epoch_start < 0 || row.through_seq < row.epoch_start || row.through_seq > row.seq)
        return yield* fail(`Invalid P instruction watermark: ${row.session_id}`)
      for (const text of [row.initial_values, row.current_values]) {
        const values = yield* Schema.decodeUnknownEffect(Schema.fromJsonString(Instruction.Values))(text)
        for (const hash of new Set(Object.values(values))) {
          if (!(yield* db.get(sql`SELECT 1 FROM ${table("instruction_blob")} WHERE hash = ${hash}`)))
            return yield* fail(`Missing P instruction blob: ${row.session_id}`)
        }
      }
    }), { discard: true }))
    yield* WsNativeScan.rows(db, { table: source("session"), bytes: sql`coalesce(octet_length(m.fork_boundary), 0)`,
      columns: sql`m.id, m.fork_session_id, m.fork_boundary, m.time_suspended`,
    }, (rows: readonly { id: string; fork_session_id: string | null; fork_boundary: string | null; time_suspended: number | null }[]) => Effect.forEach(rows, (row) => Effect.gen(function* () {
      if (row.fork_boundary !== null) yield* Schema.decodeUnknownEffect(Schema.fromJsonString(Session.ForkBoundary))(row.fork_boundary)
      if (row.fork_session_id !== null && !(yield* db.get(sql`SELECT 1 FROM ${table("session")} WHERE id = ${row.fork_session_id}`)))
        return yield* fail(`Missing P fork parent: ${row.id}`)
      if (row.time_suspended !== null && !Number.isSafeInteger(row.time_suspended)) return yield* fail(`Invalid P suspension time: ${row.id}`)
    }), { discard: true }))
    yield* WsNativeScan.rows(db, { table: source("session_pending"), bytes: sql`octet_length(m.data)`,
      columns: sql`m.*, (SELECT seq FROM ${table("event_sequence")} e WHERE e.aggregate_id = m.session_id) AS seq`,
    }, (rows: readonly PendingRow[]) => Effect.forEach(rows, (row) => Effect.gen(function* () {
      if (row.seq === null || !Number.isSafeInteger(row.admitted_seq) || row.admitted_seq < 0 || row.admitted_seq > row.seq)
        return yield* fail(`Invalid P pending watermark: ${row.id}`)
      if (yield* db.get(sql`SELECT 1 FROM ${table("session_message")} WHERE id = ${row.id}`)) return yield* fail(`P pending input is already visible: ${row.id}`)
      const data = yield* Schema.decodeUnknownEffect(json)(row.data)
      if (row.type === "compaction" && (Object.keys(data).length || row.delivery !== null)) return yield* fail("Invalid P compaction control")
      const item = yield* Schema.decodeUnknownEffect(SessionInbox.Info)({
        id: row.id, sessionID: row.session_id, type: row.type, payload: data,
        delivery: row.type === "compaction" ? "steer" : row.delivery, time: { created: row.time_created },
      }, options)
      if (item.type !== "user" && item.type !== "synthetic" && item.type !== "compaction") return yield* fail(`Unknown P pending type: ${row.type}`)
      if (acceptPending) yield* acceptPending({ row, delivery: item.delivery })
    }), { discard: true }))
  }))
}

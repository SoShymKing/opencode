export * as WsNativeScan from "./ws-native-scan.js"

import { sql, type SQL } from "drizzle-orm"
import { Effect } from "effect"
import { open } from "#ws-native-worker"
import type { EffectDrizzleSqlite } from "./drizzle.js"
import { DatabaseWsArchive } from "./ws-archive.js"
import { WsNativeBatch } from "./ws-native-batch.js"
import { WsNativeMessage } from "./ws-native-message.js"
import type { EnvelopeRow, ChildRow } from "../session/message-storage.js"

export const rowLimit = 128
export const byteLimit = 1024 * 1024
const identifier = DatabaseWsArchive.identifier
export type Reader = Pick<EffectDrizzleSqlite.EffectSQLiteDatabase, "all">

export function rows<Row, E, R>(db: Reader,
  source: { readonly table: string; readonly key?: string | { readonly first: "time_created"; readonly then: "id" }; readonly bytes: SQL; readonly columns?: SQL; readonly where?: SQL },
  accept: (rows: readonly Row[]) => Effect.Effect<void, E, R>) {
  return Effect.gen(function* () {
    const composite = typeof source.key === "object" ? source.key : undefined
    const key = sql`m.${sql.identifier(composite?.then ?? (typeof source.key === "string" ? source.key : "id"))}`
    const first = composite ? sql`m.${sql.identifier(composite.first)}` : undefined
    const order = first ? sql`${first}, ${key}` : key
    const seek = first ? sql`(${first}, ${key})` : key
    type Candidate = { key: string | number; first: number; bytes: number }
    const value = (row: Candidate) => first ? sql`(${row.first}, ${row.key})` : sql`${row.key}`
    let cursor: Candidate | undefined
    while (true) {
      const candidates = yield* db.all<Candidate>(sql`
        SELECT ${key} AS key, ${first ?? sql`0`} AS first, ${source.bytes} AS bytes FROM ${identifier(source.table)} m
        WHERE (${source.where ?? sql`1`}) AND ${cursor === undefined ? sql`1` : sql`${seek} > ${value(cursor)}`}
        ORDER BY ${order} LIMIT ${rowLimit}`)
      if (!candidates.length) return
      const admitted: Candidate[] = []
      let bytes = 0
      for (const candidate of candidates) {
        if (admitted.length && bytes + candidate.bytes > byteLimit) break
        admitted.push(candidate)
        bytes += candidate.bytes
      }
      const selected = yield* db.all<Row>(sql`SELECT ${source.columns ?? sql`m.*`} FROM ${identifier(source.table)} m
        WHERE (${source.where ?? sql`1`}) AND ${first
          ? sql`${seek} BETWEEN ${value(admitted[0])} AND ${value(admitted[admitted.length - 1])}`
          : sql`${key} IN (${sql.join(admitted.map(value), sql`, `)})`} ORDER BY ${order}`)
      if (selected.length !== admitted.length) return yield* new WsNativeMessage.WsNativeMessageError({
        messageID: String(admitted[0].key), message: "Source rows changed during scan",
      })
      yield* accept(selected)
      cursor = admitted[admitted.length - 1]
    }
  })
}

export function validateNative(db: Pick<EffectDrizzleSqlite.EffectSQLiteDatabase, "transaction">) {
  return Effect.scoped(Effect.gen(function* () {
    const worker = yield* open
    yield* db.transaction((tx) => rows(tx, {
      table: "session_message", columns: sql`m.session_id AS sessionID, m.id AS messageID, m.type, m.data AS dataText`,
      bytes: sql`octet_length(m.data) + coalesce((SELECT sum(octet_length(p.data)) FROM session_message_part p WHERE p.message_id = m.id), 0)`,
    }, (parents: readonly EnvelopeRow[]) => Effect.gen(function* () {
      const jobs: WsNativeBatch.Job[] = []
      for (const parent of parents) {
        const children = yield* tx.all<Omit<ChildRow, "sessionID">>(sql`SELECT message_id AS messageID, position, id, type, data AS dataText
          FROM session_message_part WHERE message_id = ${parent.messageID} ORDER BY position`)
        jobs.push({ kind: "native-validate", parent, children })
      }
      yield* worker.run(jobs)
    })))
  }))
}
type Source = {
  readonly parent: string
  readonly child: string | undefined
  readonly previousNative: boolean
  readonly store?: (sessionID: string) => boolean
}

export function consume<E, R>(db: Pick<EffectDrizzleSqlite.EffectSQLiteDatabase, "all" | "get">, source: Source,
  accept: (row: WsNativeBatch.Row, output: WsNativeBatch.Output) => Effect.Effect<void, E, R>) {
  return Effect.scoped(Effect.gen(function* () {
    let cursor: { session_id: string; seq: number; id: string } | undefined
    let converter: { process: typeof WsNativeBatch.process } | undefined
    while (true) {
      const candidates = yield* db.all<{ id: string; session_id: string; seq: number; bytes: number }>(sql`
        SELECT m.id, m.session_id, m.seq, octet_length(m.data) + ${source.child
          ? sql`coalesce((SELECT sum(octet_length(p.data)) FROM ${identifier(source.child)} p WHERE p.message_id = m.id), 0)` : sql`0`} AS bytes
        FROM ${identifier(source.parent)} m
        WHERE ${cursor ? sql`(m.session_id, m.seq, m.id) > (${cursor.session_id}, ${cursor.seq}, ${cursor.id})` : sql`1`}
        ORDER BY m.session_id, m.seq, m.id LIMIT ${rowLimit}`)
      if (!candidates.length) return
      const batch: WsNativeBatch.Input[] = []
      let bytes = 0
      for (const candidate of candidates) {
        if (batch.length && bytes + candidate.bytes > byteLimit) break
        const row = yield* db.get<WsNativeBatch.Row>(sql`SELECT id, session_id, type, seq, time_created, time_updated, data
          FROM ${identifier(source.parent)} WHERE id = ${candidate.id}`)
        if (!row) return yield* new WsNativeMessage.WsNativeMessageError({ messageID: candidate.id, message: "Source message disappeared during scan" })
        const parts = source.child ? yield* db.all<NonNullable<WsNativeBatch.Input["parts"]>[number]>(sql`
          SELECT position, id, type, data FROM ${identifier(source.child)} WHERE message_id = ${row.id} ORDER BY position`) : undefined
        batch.push({ row, parts, previousNative: source.previousNative, store: source.store?.(row.session_id) ?? false })
        bytes += candidate.bytes
        cursor = candidate
      }
      if (!converter) converter = candidates.length === rowLimit || bytes >= byteLimit || batch.length < candidates.length
        ? yield* open : { process: WsNativeBatch.process }
      const outputs = yield* converter.process(batch)
      for (const [index, item] of batch.entries()) yield* accept(item.row, outputs[index])
    }
  }))
}

export * as WsNativeScan from "./ws-native-scan.js"

import { sql, type SQL } from "drizzle-orm"
import { Effect } from "effect"
import { open } from "#ws-native-worker"
import type { EffectDrizzleSqlite } from "./drizzle.js"
import { DatabaseWsArchive } from "./ws-archive.js"
import { WsNativeBatch } from "./ws-native-batch.js"
import { WsNativeMessage } from "./ws-native-message.js"
import type { EnvelopeRow, ChildRow } from "../session/message-storage.js"
import { estimate, planning, memoryError } from "./ws-native-serial.js"

export const rowLimit = 128
export const byteLimit = 1024 * 1024
const identifier = DatabaseWsArchive.identifier
export type Reader = Pick<EffectDrizzleSqlite.EffectSQLiteDatabase, "all">

export function rows<Row, E, R>(db: Reader,
  source: { readonly table: string; readonly key?: string | { readonly first: "time_created"; readonly then: "id" }; readonly bytes: SQL; readonly rows?: SQL; readonly columns?: SQL; readonly where?: SQL; readonly workers?: boolean },
  accept: (rows: readonly Row[]) => Effect.Effect<void, E, R>) {
  return Effect.gen(function* () {
    const composite = typeof source.key === "object" ? source.key : undefined
    const key = sql`m.${sql.identifier(composite?.then ?? (typeof source.key === "string" ? source.key : "id"))}`
    const first = composite ? sql`m.${sql.identifier(composite.first)}` : undefined
    const order = first ? sql`${first}, ${key}` : key
    const seek = first ? sql`(${first}, ${key})` : key
    type Candidate = { key: string | number; first: number; bytes: number; rows: number }
    const value = (row: Candidate) => first ? sql`(${row.first}, ${row.key})` : sql`${row.key}`
    let cursor: Candidate | undefined
    let pending: Row[] = []
    let needed = 0
    while (true) {
      const candidates = yield* db.all<Candidate>(sql`
        SELECT ${key} AS key, ${first ?? sql`0`} AS first, ${source.bytes} AS bytes, ${source.rows ?? sql`1`} AS rows FROM ${identifier(source.table)} m
        WHERE (${source.where ?? sql`1`}) AND ${cursor === undefined ? sql`1` : sql`${seek} > ${value(cursor)}`}
        ORDER BY ${order} LIMIT ${rowLimit}`)
      if (!candidates.length) {
        if (pending.length) yield* accept(pending)
        return
      }
      const admitted: Candidate[] = []
      const { available } = yield* planning(needed, pending.length)
      let bytes = 0
      for (const candidate of candidates) {
        if (admitted.length && bytes + candidate.bytes > byteLimit) break
        const size = estimate(candidate.bytes, candidate.rows)
        if (needed + size > available) {
          if (pending.length || admitted.length) break
          return yield* memoryError(String(candidate.key), size, available)
        }
        admitted.push(candidate)
        bytes += candidate.bytes
        needed += size
        if (source.workers) {
          const target = yield* planning(needed, pending.length + admitted.length)
          if (target.workers && target.ready) break
        }
      }
      if (!admitted.length) {
        yield* accept(pending)
        pending = []
        needed = 0
        continue
      }
      const selected = yield* db.all<Row>(sql`SELECT ${source.columns ?? sql`m.*`} FROM ${identifier(source.table)} m
        WHERE (${source.where ?? sql`1`}) AND ${first
          ? sql`${seek} BETWEEN ${value(admitted[0])} AND ${value(admitted[admitted.length - 1])}`
          : sql`${key} IN (${sql.join(admitted.map(value), sql`, `)})`} ORDER BY ${order}`)
      if (selected.length !== admitted.length) return yield* new WsNativeMessage.WsNativeMessageError({
        messageID: String(admitted[0].key), message: "Source rows changed during scan",
      })
      pending.push(...selected)
      cursor = admitted[admitted.length - 1]
      if (!source.workers || (yield* planning(needed, pending.length)).ready) {
        yield* accept(pending)
        pending = []
        needed = 0
      }
    }
  })
}

export function validateNative(db: Pick<EffectDrizzleSqlite.EffectSQLiteDatabase, "transaction">) {
  return Effect.scoped(Effect.gen(function* () {
    const worker = yield* open
    yield* db.transaction((tx) => rows(tx, {
      table: "session_message", columns: sql`m.session_id AS sessionID, m.id AS messageID, m.type, m.data AS dataText`,
      bytes: sql`octet_length(m.data) + coalesce((SELECT sum(octet_length(p.data)) FROM session_message_part p WHERE p.message_id = m.id), 0)`,
      rows: sql`1 + (SELECT count(*) FROM session_message_part p WHERE p.message_id = m.id)`,
      workers: true,
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
    const converter = yield* open
    const batch: WsNativeBatch.Input[] = []
    let needed = 0
    const flush = Effect.gen(function* () {
      const outputs = yield* converter.process(batch)
      for (const [index, item] of batch.entries()) yield* accept(item.row, outputs[index])
      batch.length = 0
      needed = 0
    })
    while (true) {
      const candidates = yield* db.all<{ id: string; session_id: string; seq: number; bytes: number; rows: number }>(sql`
        SELECT m.id, m.session_id, m.seq, octet_length(m.data) + ${source.child
          ? sql`coalesce((SELECT sum(octet_length(p.data)) FROM ${identifier(source.child)} p WHERE p.message_id = m.id), 0)` : sql`0`} AS bytes,
          ${source.child ? sql`1 + (SELECT count(*) FROM ${identifier(source.child)} p WHERE p.message_id = m.id)` : sql`1`} AS rows
        FROM ${identifier(source.parent)} m
        WHERE ${cursor ? sql`(m.session_id, m.seq, m.id) > (${cursor.session_id}, ${cursor.seq}, ${cursor.id})` : sql`1`}
        ORDER BY m.session_id, m.seq, m.id LIMIT ${rowLimit}`)
      if (!candidates.length) {
        if (batch.length) yield* flush
        return
      }
      const admitted: typeof candidates = []
      const { available } = yield* planning(needed, batch.length)
      let bytes = 0
      let childRows = 0
      for (const candidate of candidates) {
        if (admitted.length && (bytes + candidate.bytes > byteLimit || childRows + candidate.rows - 1 > rowLimit)) break
        const size = estimate(candidate.bytes, candidate.rows)
        if (needed + size > available) {
          if (batch.length || admitted.length) break
          return yield* memoryError(candidate.id, size, available)
        }
        admitted.push(candidate)
        bytes += candidate.bytes
        childRows += candidate.rows - 1
        needed += size
        const target = yield* planning(needed, batch.length + admitted.length)
        if (target.workers && target.ready) break
      }
      if (!admitted.length) {
        yield* flush
        continue
      }
      const ids = sql.join(admitted.map((candidate) => sql`${candidate.id}`), sql`, `)
      const parents = yield* db.all<WsNativeBatch.Row>(sql`SELECT id, session_id, type, seq, time_created, time_updated, data
        FROM ${identifier(source.parent)} WHERE id IN (${ids}) ORDER BY session_id, seq, id`)
      const byID = new Map(parents.map((row) => [row.id, row]))
      for (const candidate of admitted) {
        const row = byID.get(candidate.id)
        if (!row || row.session_id !== candidate.session_id || row.seq !== candidate.seq)
          return yield* new WsNativeMessage.WsNativeMessageError({ messageID: candidate.id, message: "Source rows changed during scan" })
      }
      const children = source.child ? yield* db.all<NonNullable<WsNativeBatch.Input["parts"]>[number] & { message_id: string }>(sql`
        SELECT message_id, position, id, type, data FROM ${identifier(source.child)} WHERE message_id IN (${ids}) ORDER BY message_id, position`) : undefined
      const parts = new Map<string, NonNullable<WsNativeBatch.Input["parts"]>[number][]>()
      for (const child of children ?? []) {
        const group = parts.get(child.message_id)
        if (group) group.push(child)
        else parts.set(child.message_id, [child])
      }
      for (const candidate of admitted) {
        const row = byID.get(candidate.id)
        if (row) batch.push({ row, parts: children === undefined ? undefined : parts.get(row.id) ?? [],
          previousNative: source.previousNative, store: source.store?.(row.session_id) ?? false })
      }
      cursor = admitted[admitted.length - 1]
      if ((yield* planning(needed, batch.length)).ready) yield* flush
    }
  }))
}

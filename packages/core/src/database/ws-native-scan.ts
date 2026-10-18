export * as WsNativeScan from "./ws-native-scan.js"

import { sql } from "drizzle-orm"
import { Effect } from "effect"
import { open } from "#ws-native-worker"
import type { EffectDrizzleSqlite } from "./drizzle.js"
import { DatabaseWsArchive } from "./ws-archive.js"
import { WsNativeBatch } from "./ws-native-batch.js"
import { WsNativeMessage } from "./ws-native-message.js"

export const rowLimit = 128
export const byteLimit = 1024 * 1024
const identifier = DatabaseWsArchive.identifier
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
        SELECT m.id, m.session_id, m.seq, length(CAST(m.data AS BLOB)) + ${source.child
          ? sql`coalesce((SELECT sum(length(CAST(p.data AS BLOB))) FROM ${identifier(source.child)} p WHERE p.message_id = m.id), 0)` : sql`0`} AS bytes
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

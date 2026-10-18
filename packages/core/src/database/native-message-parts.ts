import { SessionMessage } from "@opencode/schema/session-message"
import { sql } from "drizzle-orm"
import { Effect, Schema } from "effect"
import type { EffectDrizzleSqlite } from "./drizzle.js"
import { DatabaseLegacyV01Json } from "./legacy-v01-json.js"

type Database = Pick<EffectDrizzleSqlite.EffectSQLiteDatabase, "all" | "run">
const decode = Schema.decodeUnknownEffect(SessionMessage.Info)

export class MessageNormalizationError extends Error {
  override readonly name = "MessageNormalizationError"
  constructor(readonly messageID: string, options: ErrorOptions) {
    super(`Cannot normalize assistant message ${messageID}`, options)
  }
}

export function normalizeMessages(db: Database, sessionID?: string) {
  return Effect.scoped(Effect.gen(function* () {
    const { WsNativeScan } = yield* Effect.promise(() => import("./ws-native-scan.js"))
    const { open } = yield* Effect.promise(() => import("#ws-native-worker"))
    const worker = yield* open
    yield* WsNativeScan.rows(db, { table: "session_message", bytes: sql`octet_length(m.data)`,
      key: sessionID === undefined ? "id" : "seq",
      columns: sql`m.id, m.data`, where: sql`m.type = 'assistant' ${sessionID === undefined ? sql`` : sql`AND m.session_id = ${sessionID}`}` },
    (rows: readonly { id: string; data: string }[]) => Effect.gen(function* () {
      const results = yield* worker.run(rows.map((row) => ({ kind: "native-normalize", row }))).pipe(
        Effect.mapError((cause) => new MessageNormalizationError(cause.messageID, { cause })),
      )
      for (const [index, result] of results.entries()) {
        if (result.kind !== "native-normalize") return yield* Effect.die(new Error("Unexpected normalization result"))
        const row = rows[index]
        for (const part of result.output.parts) yield* db.run(sql`INSERT INTO session_message_part (message_id, position, id, type, data)
          VALUES (${row.id}, ${part.position}, ${part.id}, ${part.type}, ${part.data})`)
        yield* db.run(sql`UPDATE session_message SET data = ${result.output.data} WHERE id = ${row.id}`)
      }
    }))
  }))
}

export function normalizeMessage(row: { readonly id: string; readonly data: string }) {
  return Effect.gen(function* () {
    const parsed = yield* Effect.try({
      try: () => {
        const data = DatabaseLegacyV01Json.object(row.data)
        if (DatabaseLegacyV01Json.has(data, "id") || DatabaseLegacyV01Json.has(data, "type"))
          throw new Error("Parent payload contains identity")
        const parts = DatabaseLegacyV01Json.objects(data, "content")
        if (!parts) throw new Error("Assistant payload has no content")
        return { data, parts }
      },
      catch: (cause) => new MessageNormalizationError(row.id, { cause }),
    })
    const message = yield* decode({ ...parsed.data.value, id: row.id, type: "assistant" }, { onExcessProperty: "error" }).pipe(
      Effect.mapError((cause) => new MessageNormalizationError(row.id, { cause })),
    )
    if (message.type !== "assistant") return yield* Effect.fail(new MessageNormalizationError(row.id, { cause: "Expected assistant" }))
    const ordinals = { text: 0, reasoning: 0 }
    const parts = message.content.map((part, position) => {
      const id = part.type === "tool" ? part.id : String(ordinals[part.type]++)
      return { position, id, type: part.type, data: DatabaseLegacyV01Json.remove(parsed.parts[position], new Set(["id", "type"])) }
    })
    return { type: message.type, data: DatabaseLegacyV01Json.remove(parsed.data, new Set(["content"])), parts }
  })
}

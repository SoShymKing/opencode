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
  return Effect.gen(function* () {
    const rows = yield* db.all<{ id: string; data: string }>(sql`
      SELECT id, data FROM session_message WHERE type = 'assistant'
      ${sessionID === undefined ? sql`` : sql`AND session_id = ${sessionID}`} ORDER BY session_id, seq, id`)
    for (const row of rows) {
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
      for (const [position, part] of message.content.entries()) {
        const id = part.type === "tool" ? part.id : String(ordinals[part.type]++)
        yield* db.run(sql`INSERT INTO session_message_part (message_id, position, id, type, data)
          VALUES (${row.id}, ${position}, ${id}, ${part.type}, ${DatabaseLegacyV01Json.remove(parsed.parts[position], new Set(["id", "type"]))})`)
      }
      yield* db.run(sql`UPDATE session_message SET data = ${DatabaseLegacyV01Json.remove(parsed.data, new Set(["content"]))} WHERE id = ${row.id}`)
    }
  })
}

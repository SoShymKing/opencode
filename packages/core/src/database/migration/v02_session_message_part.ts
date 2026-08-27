import { SessionMessage } from "@opencode-ai/schema/session-message"
import { sql } from "drizzle-orm"
import { Effect, Schema } from "effect"
import { DatabaseLegacyV01Json } from "../legacy-v01-json"
import type { DatabaseMigration } from "../migration"

type Transaction = Parameters<DatabaseMigration.Migration["up"]>[0]
type MessageRow = { readonly id: string; readonly data: string }

class SessionMessagePartMigrationError extends Error {
  override readonly name = "SessionMessagePartMigrationError"

  constructor(readonly messageID: string, reason: string, options?: ErrorOptions) {
    super(`Failed to migrate assistant ${messageID}: ${reason}`, options)
  }
}

const decodeMessage = Schema.decodeUnknownEffect(SessionMessage.Message)
const identity = new Set(["id", "type"])
const content = new Set(["content"])

export default {
  id: "v02_session_message_part",
  up(tx) {
    return Effect.gen(function* () {
      yield* tx.run(`
        CREATE TABLE \`session_message_part\` (
          \`message_id\` text NOT NULL,
          \`position\` integer NOT NULL,
          \`id\` text NOT NULL,
          \`type\` text NOT NULL,
          \`data\` text NOT NULL CHECK(json_valid("data") AND json_type("data") = 'object'),
          PRIMARY KEY(\`message_id\`, \`position\`),
          FOREIGN KEY (\`message_id\`) REFERENCES \`session_message\`(\`id\`) ON UPDATE no action ON DELETE cascade
        );
      `)
      yield* tx.run(
        `CREATE INDEX \`session_message_part_lookup_idx\` ON \`session_message_part\` (\`message_id\`,\`type\`,\`id\`,"position" desc);`,
      )
      const rows = yield* tx.all<MessageRow>(
        sql`SELECT id, data FROM session_message WHERE type = 'assistant' ORDER BY session_id, seq, id`,
      )
      yield* Effect.forEach(rows, (row) => convertAssistant(tx, row), { discard: true })
    })
  },
} satisfies DatabaseMigration.Migration

function convertAssistant(tx: Transaction, row: MessageRow) {
  return Effect.gen(function* () {
    const parsed = yield* parse(row)
    yield* decodeMessage({ ...parsed.data.value, id: row.id, type: "assistant" }, { onExcessProperty: "error" }).pipe(
      Effect.asVoid,
      Effect.mapError(() => new SessionMessagePartMigrationError(row.id, "does not match the repository schema")),
    )
    yield* tx.run(
      sql`UPDATE session_message SET data = ${DatabaseLegacyV01Json.remove(parsed.data, content)} WHERE id = ${row.id}`,
    )
    yield* Effect.forEach(
      parsed.parts,
      (part, position) =>
        tx.run(sql`INSERT INTO session_message_part (message_id, position, id, type, data)
          VALUES (${row.id}, ${position}, ${part.id}, ${part.type}, ${DatabaseLegacyV01Json.remove(part.data, identity)})`),
      { discard: true },
    )
  })
}

function parse(row: MessageRow) {
  return Effect.try({
    try: () => {
      const data = DatabaseLegacyV01Json.object(row.data)
      if (DatabaseLegacyV01Json.has(data, "id") || DatabaseLegacyV01Json.has(data, "type")) {
        throw new SessionMessagePartMigrationError(row.id, "stores identity in data")
      }
      const values = DatabaseLegacyV01Json.objects(data, "content")
      if (!values) throw new SessionMessagePartMigrationError(row.id, "has no legacy content")
      const parts = values.map((value) => {
        const id = DatabaseLegacyV01Json.string(value, "id")
        const type = DatabaseLegacyV01Json.string(value, "type")
        if (!id || !type) throw new SessionMessagePartMigrationError(row.id, "content is malformed")
        return { id, type, data: value }
      })
      return { data, parts }
    },
    catch: (cause) =>
      cause instanceof SessionMessagePartMigrationError
        ? cause
        : new SessionMessagePartMigrationError(row.id, "content is malformed", { cause }),
  })
}

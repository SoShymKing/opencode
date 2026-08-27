import { describe, expect, test } from "bun:test"
import path from "node:path"
import { DatabaseMigration } from "@opencode-ai/core/database/migration"
import { hydrateSelection } from "@opencode-ai/core/session/message-storage"
import { SessionMessage } from "@opencode-ai/core/session/message"
import { SessionMessageTable } from "@opencode-ai/core/session/sql"
import { eq, sql } from "drizzle-orm"
import { Effect } from "effect"
import {
  apply,
  backupPaths,
  inspectBackup,
  messageContent,
  messageID,
  messageParts,
  normalizedMessageBytes,
  sessionID,
  setupExisting,
  useDatabase,
} from "./fixture/database-migration"
import { tmpdir } from "./fixture/tmpdir"

const canonicalIDs = ["v01_baseline", "v02_session_message_part"]
const envelope = {
  agent: "build",
  model: { providerID: "test", id: "model" },
  time: { created: 1 },
} as const
const bytes = (value: string) => Buffer.from(value).toString("hex").toUpperCase()

describe("DatabaseMigration assistant content", () => {
  test("converts embedded assistant content before journaling v02", async () => {
    await using tmp = await tmpdir()
    const filename = path.join(tmp.path, "content.sqlite")
    const userID = "msg_migration_user"
    const userData = JSON.stringify({ text: "question", files: [], agents: [], time: { created: 1 } })

    await useDatabase(filename, (db) =>
      Effect.gen(function* () {
        yield* setupExisting(db, { migrationIDs: ["v01_baseline"] })
        yield* db.run(sql`INSERT INTO session_message (
          id, session_id, type, seq, time_created, time_updated, data
        ) VALUES (${userID}, ${sessionID}, 'user', 1, 1, 1, ${userData})`)
        yield* DatabaseMigration.apply(db, filename)
      }),
    )
    const stored = await useDatabase(filename, (db) =>
      Effect.gen(function* () {
        return {
          journal: yield* db.all<{ readonly id: string }>(sql`SELECT id FROM migration ORDER BY rowid`),
          parent: yield* db.get<{ readonly bytes: string }>(
            sql`SELECT hex(CAST(data AS blob)) AS bytes FROM session_message WHERE id = ${messageID}`,
          ),
          user: yield* db.get<{ readonly bytes: string }>(
            sql`SELECT hex(CAST(data AS blob)) AS bytes FROM session_message WHERE id = ${userID}`,
          ),
          parts: yield* db.all<{ readonly position: number; readonly id: string; readonly type: string; readonly data: string }>(
            sql`SELECT position, id, type, data FROM session_message_part ORDER BY position`,
          ),
        }
      }),
    )

    expect(stored.journal.map((row) => row.id)).toEqual(canonicalIDs)
    const hydrated = await useDatabase(filename, (db) =>
      hydrateSelection({ db, where: eq(SessionMessageTable.id, SessionMessage.ID.make(messageID)) }),
    )

    expect(stored.parent).toEqual({ bytes: normalizedMessageBytes })
    expect(stored.user).toEqual({ bytes: bytes(userData) })
    expect(stored.parts).toEqual(messageParts)
    expect(hydrated[0]?.message).toMatchObject({ id: messageID, type: "assistant", content: messageContent })
  })

  test("removes empty assistant content without inserting parts", async () => {
    const data = JSON.stringify({ ...envelope, content: [] })
    const state = await useDatabase(":memory:", (db) =>
      Effect.gen(function* () {
        yield* setupExisting(db, { migrationIDs: ["v01_baseline"] })
        yield* db.run(sql`UPDATE session_message SET data = ${data} WHERE id = ${messageID}`)
        yield* DatabaseMigration.apply(db, ":memory:")
        return {
          parent: yield* db.get<{ readonly bytes: string }>(
            sql`SELECT hex(CAST(data AS blob)) AS bytes FROM session_message WHERE id = ${messageID}`,
          ),
          parts: yield* db.get<{ readonly count: number }>(sql`SELECT count(*) AS count FROM session_message_part`),
          hydrated: yield* hydrateSelection({
            db,
            where: eq(SessionMessageTable.id, SessionMessage.ID.make(messageID)),
          }),
        }
      }),
    )

    expect(state.parent).toEqual({ bytes: normalizedMessageBytes })
    expect(state.parts).toEqual({ count: 0 })
    expect(state.hydrated[0]?.message).toMatchObject({ content: [] })
  })

  test.each([
    ["non-array content", JSON.stringify({ ...envelope, content: {} })],
    [
      "duplicate content",
      '{"agent":"build","model":{"providerID":"test","id":"model"},"time":{"created":1},"content":[],"content":[]}',
    ],
    [
      "schema-invalid content",
      JSON.stringify({ ...envelope, content: [messageContent[0], { id: "bad", type: "text", text: 1 }] }),
    ],
  ])("rolls back malformed assistant storage for %s", async (_name, data) => {
    await using tmp = await tmpdir()
    const filename = path.join(tmp.path, "invalid.sqlite")
    const history = DatabaseMigration.legacyBaselineMigrationIDs

    await useDatabase(filename, (db) =>
      Effect.gen(function* () {
        yield* setupExisting(db, { migrationIDs: history })
        yield* db.run(sql`UPDATE session_message SET data = ${data} WHERE id = ${messageID}`)
      }),
    )
    await expect(apply(filename)).rejects.toThrow()

    const backups = await backupPaths(filename)
    const backup = backups[0]
    if (!backup) throw new Error("Expected migration backup")
    expect(backups).toHaveLength(1)
    expect(inspectBackup(backup)).toEqual({
      quickCheck: "ok",
      journal: history.map((id) => ({ id })),
      partTable: null,
      dataBytes: bytes(data),
    })
    expect(
      await useDatabase(filename, (db) =>
        Effect.gen(function* () {
          return {
            journal: yield* db.all<{ readonly id: string }>(sql`SELECT id FROM migration ORDER BY rowid`),
            part: yield* db.get(sql`SELECT name FROM sqlite_master WHERE name = 'session_message_part'`),
            dataBytes: yield* db.get<{ readonly bytes: string }>(
              sql`SELECT hex(CAST(data AS blob)) AS bytes FROM session_message WHERE id = ${messageID}`,
            ),
          }
        }),
      ),
    ).toEqual({ journal: history.map((id) => ({ id })), part: undefined, dataBytes: { bytes: bytes(data) } })
  })
})

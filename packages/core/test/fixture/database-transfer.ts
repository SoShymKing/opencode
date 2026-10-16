import { Database as SqliteDatabase } from "bun:sqlite"
import path from "node:path"
import sessionMessagePartMigration from "../../src/database/ws-message-parts"
import { useDatabase } from "./database-migration"
import { sql } from "drizzle-orm"
import { Effect } from "effect"

export const assistantContent = [
  { type: "text", id: "text-unicode", text: "hello 한국어 🌍" },
  {
    type: "reasoning",
    id: "reasoning-nested",
    text: "nested",
    providerMetadata: { provider: { trace: { depth: 2 } } },
  },
  {
    type: "tool",
    id: "tool-call",
    name: "read",
    state: {
      status: "completed",
      input: { path: "README.md", nested: { values: [1, true, null] } },
      structured: { result: { lines: ["one", "two"] } },
      content: [{ type: "text", text: "tool output" }],
    },
    time: { created: 1, completed: 2 },
  },
  { type: "text", id: "text-empty", text: "" },
] as const

export const assistantEnvelope = {
  agent: "build",
  model: { id: "test-model", providerID: "test-provider" },
  time: { created: 1, completed: 2 },
}

export const userEnvelope = {
  text: "preserve me",
  files: [],
  agents: [],
  metadata: { nested: { unicode: "雪" } },
  time: { created: 1 },
}

export async function createCurrentDatabase(filename: string) {
  await useDatabase(filename, (db) =>
    Effect.gen(function* () {
      yield* db.run("PRAGMA journal_mode = WAL")
      yield* db.run("PRAGMA foreign_keys = ON")
      yield* db.run(`CREATE TABLE project (
        id text PRIMARY KEY, worktree text NOT NULL, time_created integer NOT NULL,
        time_updated integer NOT NULL, sandboxes text NOT NULL
      )`)
      yield* db.run(`CREATE TABLE session (
        id text PRIMARY KEY, project_id text NOT NULL REFERENCES project(id),
        slug text NOT NULL, directory text NOT NULL, title text NOT NULL,
        version text NOT NULL, time_created integer NOT NULL, time_updated integer NOT NULL
      )`)
      yield* db.run(`CREATE TABLE session_message (
        id text PRIMARY KEY, session_id text NOT NULL REFERENCES session(id) ON DELETE CASCADE,
        type text NOT NULL, seq integer NOT NULL, time_created integer NOT NULL,
        time_updated integer NOT NULL, data text NOT NULL
      )`)
      yield* db.run("CREATE TABLE migration (id text PRIMARY KEY, time_completed integer NOT NULL)")
      yield* db.transaction((tx) => sessionMessagePartMigration.up(tx))
      yield* db.run("INSERT INTO migration VALUES ('v01_baseline', 1), ('v02_session_message_part', 1)")
      yield* db.run(sql`INSERT INTO project (
        id, worktree, time_created, time_updated, sandboxes
      ) VALUES ('project', ${path.dirname(filename)}, 1, 1, '[]')`)
      yield* db.run(sql`INSERT INTO session (
        id, project_id, slug, directory, title, version, time_created, time_updated
      ) VALUES ('session', 'project', 'session', ${path.dirname(filename)}, 'Session', '1', 1, 1)`)
      yield* db.run(sql`INSERT INTO session_message (
        id, session_id, type, seq, time_created, time_updated, data
      ) VALUES ('msg_user', 'session', 'user', 0, 1, 1, ${JSON.stringify(userEnvelope)})`)
      yield* db.run(sql`INSERT INTO session_message (
        id, session_id, type, seq, time_created, time_updated, data
      ) VALUES ('msg_assistant', 'session', 'assistant', 1, 1, 2, ${JSON.stringify(assistantEnvelope)})`)
      yield* db.run(sql`INSERT INTO session_message (
        id, session_id, type, seq, time_created, time_updated, data
      ) VALUES ('msg_empty', 'session', 'assistant', 2, 1, 1, ${JSON.stringify({ ...assistantEnvelope, time: { created: 1 } })})`)
      yield* Effect.forEach(
        assistantContent,
        (part, position) => {
          const { id, type, ...data } = part
          return db.run(sql`INSERT INTO session_message_part (
            message_id, position, id, type, data
          ) VALUES ('msg_assistant', ${position}, ${id}, ${type}, ${JSON.stringify(data)})`)
        },
        { discard: true },
      )
      yield* db.run("PRAGMA wal_checkpoint(TRUNCATE)")
    }),
  )
}

export function inspectMessages(filename: string) {
  using database = new SqliteDatabase(filename, { readonly: true })
  return {
    quickCheck: database.query<{ readonly quick_check: string }, []>("PRAGMA quick_check").get()?.quick_check,
    foreignKeys: database.query("PRAGMA foreign_key_check").all(),
    journal: database.query<{ readonly id: string }, []>("SELECT id FROM migration ORDER BY rowid").all(),
    rows: database
      .query<
        { readonly id: string; readonly type: string; readonly data: string },
        []
      >("SELECT id, type, data FROM session_message ORDER BY seq")
      .all(),
    parts: database
      .query<
        {
          readonly message_id: string
          readonly position: number
          readonly id: string
          readonly type: string
          readonly data: string
        },
        []
      >("SELECT message_id, position, id, type, data FROM session_message_part ORDER BY message_id, position")
      .all(),
  }
}

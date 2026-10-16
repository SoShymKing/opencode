import { describe, expect } from "bun:test"
import { desc, eq, sql } from "drizzle-orm"
import { Effect } from "effect"
import { Database } from "@opencode/core/database/database"
import { ProjectTable } from "@opencode/core/project/sql"
import { SessionHistory } from "@opencode/core/session/history"
import { SessionMessagePartTable, SessionMessageTable, SessionTable } from "@opencode/core/session/sql"
import { Project } from "@opencode/schema/project"
import { Provider } from "@opencode/schema/provider"
import { AbsolutePath } from "@opencode/schema/schema"
import { Session } from "@opencode/schema/session"
import { SessionMessage } from "@opencode/schema/session-message"
import { LayerNode } from "@opencode/util/effect/layer-node"
import { testEffect } from "./lib/effect"

const it = testEffect(LayerNode.compile(Database.node))
const sessionID = Session.ID.make("ses_normalized_history")
const provenance = {
  providerID: Provider.ID.make("test"), provider: "test", modelID: "test", route: "test",
  protocol: "test", endpoint: "test-endpoint-digest",
}
const seed = Effect.gen(function* () {
  const database = yield* Database.Service
  yield* database.db.insert(ProjectTable).values({
    id: Project.ID.global, worktree: AbsolutePath.make("/project"), sandboxes: [],
  }).run()
  yield* database.db.insert(SessionTable).values({
    id: sessionID, project_id: Project.ID.global, directory: "/project", slug: "history", version: "test",
  }).run()
  return database.db
})

describe("normalized SessionHistory", () => {
  it.effect("uses completed compatible boundaries and retires pre-checkpoint instruction updates", () =>
    Effect.gen(function* () {
      const db = yield* seed
      yield* Effect.forEach([
        { type: "user", data: { text: "input" } },
        { type: "system", data: { text: "old instructions" } },
        { type: "compaction", data: { status: "completed", reason: "manual", summary: "", recent: "",
          providerContext: { version: 1, provenance, messages: [] } } },
        { type: "assistant", data: { agent: "build", model: { id: "test", providerID: "test" } } },
        { type: "compaction", data: { status: "running", reason: "manual", summary: "", recent: "" } },
        { type: "system", data: { text: "new instructions" } },
      ], (row, index) => db.run(sql`
        INSERT INTO session_message (id, session_id, type, seq, time_created, time_updated, data)
        VALUES (${`msg_history_${index}`}, ${sessionID}, ${row.type}, ${index}, 0, 0,
          ${JSON.stringify({ ...row.data, time: { created: 0 } })})
      `))
      yield* db.insert(SessionMessagePartTable).values({
        message_id: SessionMessage.ID.make("msg_history_3"), position: 0, id: "text-0", type: "text", data: { text: "answer" },
      }).run()

      const latest = yield* SessionHistory.load(db, sessionID, "latest")
      expect(latest.map((message) => message.id)).toEqual(["msg_history_2", "msg_history_3", "msg_history_4", "msg_history_5"].map((id) => SessionMessage.ID.make(id)))
      expect(latest[1]?.type === "assistant" && latest[1].content).toEqual([{ type: "text", text: "answer" }])
      expect((yield* SessionHistory.load(db, sessionID, provenance)).map((message) => message.id))
        .toEqual(latest.map((message) => message.id))
      expect((yield* SessionHistory.load(db, sessionID, { ...provenance, modelID: "other" })).map((message) => message.id))
        .toEqual(["msg_history_0", "msg_history_3", "msg_history_4", "msg_history_5"].map((id) => SessionMessage.ID.make(id)))
      expect((yield* SessionHistory.load(db, sessionID, "local")).map((message) => message.id))
        .toEqual(["msg_history_0", "msg_history_3", "msg_history_4", "msg_history_5"].map((id) => SessionMessage.ID.make(id)))
      expect(yield* SessionHistory.latestUserID(db, sessionID)).toBe(SessionMessage.ID.make("msg_history_0"))
      expect((yield* SessionHistory.firstUserMessage(db, sessionID))?.id).toBe(SessionMessage.ID.make("msg_history_0"))
      const rows = yield* db.select().from(SessionMessageTable)
        .where(eq(SessionMessageTable.session_id, sessionID)).orderBy(desc(SessionMessageTable.seq)).all()
      expect((yield* SessionHistory.hydrateRows(db, rows)).map((message) => message.id))
        .toEqual(rows.map((row) => row.id))
    }),
  )

  it.effect("rejects malformed native checkpoint payloads with message ownership", () =>
    Effect.gen(function* () {
      const db = yield* seed
      yield* db.run(sql`
        INSERT INTO session_message (id, session_id, type, seq, time_created, time_updated, data)
        VALUES ('msg_invalid_checkpoint', ${sessionID}, 'compaction', 1, 0, 0,
          ${JSON.stringify({ status: "completed", reason: "manual", summary: "", recent: "", time: { created: 0 },
            providerContext: { version: 1, provenance, messages: [{ invalid: true }] } })})
      `)
      const error = yield* SessionHistory.load(db, sessionID, "latest").pipe(Effect.flip)
      expect(error.sessionID).toBe(sessionID)
      expect(error.messageID).toBe(SessionMessage.ID.make("msg_invalid_checkpoint"))
    }),
  )

  it.effect("finds streaming and running tools without decoding unrelated children or assistant envelopes", () =>
    Effect.gen(function* () {
      const db = yield* seed
      yield* db.run(sql`
        INSERT INTO session_message (id, session_id, type, seq, time_created, time_updated, data)
        VALUES ('msg_interrupted', ${sessionID}, 'assistant', 1, 0, 0, '{}')
      `)
      yield* db.insert(SessionMessagePartTable).values([
        { message_id: SessionMessage.ID.make("msg_interrupted"), position: 3, id: "running", type: "tool",
          data: { name: "bash", state: { status: "running", input: {}, metadata: {} }, time: { created: 0 } } },
        { message_id: SessionMessage.ID.make("msg_interrupted"), position: 1, id: "streaming", type: "tool",
          data: { name: "bash", state: { status: "streaming", input: "" }, time: { created: 0 } } },
      ]).run()
      yield* db.run(sql`
        INSERT INTO session_message_part (message_id, position, id, type, data)
        VALUES ('msg_interrupted', 2, 'bad-text', 'text', '{}'),
          ('msg_interrupted', 4, 'bad-settled-tool', 'tool', '{"state":{"status":"completed"}}')
      `)

      const interrupted = yield* SessionHistory.interruptedTools(db, sessionID)
      expect(interrupted.map((item) => [item.assistantMessageID, item.tool.id, item.tool.state.status])).toEqual([
        ["msg_interrupted", "streaming", "streaming"], ["msg_interrupted", "running", "running"],
      ])
    }),
  )
})

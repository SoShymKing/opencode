import { expect } from "bun:test"
import { Effect, Schema } from "effect"
import { Database } from "@opencode/core/database/database"
import { Instructions } from "@opencode/core/instructions/index"
import { ProjectTable } from "@opencode/core/project/sql"
import { SessionHistory } from "@opencode/core/session/history"
import { InstructionState } from "@opencode/core/session/instruction-state"
import { encodeStoredMessage, insertMessageChildren } from "@opencode/core/session/message-storage"
import { SessionMessageTable, SessionTable } from "@opencode/core/session/sql"
import { Project } from "@opencode/schema/project"
import { AbsolutePath } from "@opencode/schema/schema"
import { Session } from "@opencode/schema/session"
import { SessionMessage } from "@opencode/schema/session-message"
import { LayerNode } from "@opencode/util/effect/layer-node"
import { testEffect } from "./lib/effect"

const it = testEffect(LayerNode.compile(Database.node))
const sessionID = Session.ID.make("ses_ws_history")
const key = Instructions.Key.make("test/history")
const sources = Instructions.make({
  key,
  codec: Schema.toCodecJson(Schema.String),
  read: Effect.succeed("current"),
  render: { initial: String, changed: (_previous, current) => current },
})
const seed = (compaction = true) => Effect.gen(function* () {
  const database = yield* Database.Service
  const db = database.db
  yield* db.insert(ProjectTable).values({
    id: Project.ID.global, worktree: AbsolutePath.make("/project"), sandboxes: [],
  }).run()
  yield* db.insert(SessionTable).values({
    id: sessionID, project_id: Project.ID.global, directory: "/project", slug: "ws-history", version: "test",
  }).run()
  yield* Effect.forEach([
    { seq: 1, type: "system", text: "retired" },
    { seq: 2, type: "system", text: "boundary" },
    { seq: 3, type: "user", text: "old input" },
    { seq: 4, type: "system", text: "update before summary" },
    ...(compaction ? [{ seq: 5, type: "compaction", status: "completed", reason: "manual", summary: "summary", recent: "" }] : []),
    { seq: 6, type: "system", text: "update after summary" },
    { seq: 7, type: "user", text: "new input" },
  ], (row) => Effect.gen(function* () {
    const message = yield* Schema.decodeUnknownEffect(SessionMessage.Info)({
      ...row, id: `msg_ws_history_${row.seq}`, time: { created: row.seq },
    })
    const stored = yield* encodeStoredMessage(message)
    yield* db.insert(SessionMessageTable).values({ ...stored.envelope, session_id: sessionID, seq: row.seq }).run()
    yield* insertMessageChildren(db, stored.parts)
  }))
  return db
})

for (const baseline of ["  exact imported\n\nbaseline\n", ""]) {
  it.effect(`keeps chronological updates before compaction with frozen baseline ${JSON.stringify(baseline)}`, () => Effect.gen(function* () {
    const db = yield* seed()
    yield* InstructionState.importWS(db, { sessionID, baseline, baselineSeq: 2, throughSeq: 7, snapshot: { [key]: { value: "current" } } })

    const runner = yield* SessionHistory.entriesForRunner(db, sessionID, sources, "latest")
    const preview = yield* SessionHistory.preview(db, sessionID, sources, "local")

    expect(runner.initial).toBe(baseline)
    expect(runner.entries.map((entry) => entry.seq)).toEqual([4, 5, 6, 7])
    expect(preview.initial).toBe(baseline)
    expect(preview.messages).toEqual(runner.entries.map((entry) => entry.message))
    expect(preview.instructionUpdate).toBe("")
  }))
}

it.effect("retires systems at the imported boundary even after the compaction cutoff", () => Effect.gen(function* () {
  const db = yield* seed()
  yield* InstructionState.importWS(db, { sessionID, baseline: "frozen", baselineSeq: 6, throughSeq: 7, snapshot: {} })

  const history = yield* SessionHistory.load(db, sessionID, "latest")

  expect(history.map((message) => message.id)).toEqual([5, 7].map((seq) => SessionMessage.ID.make(`msg_ws_history_${seq}`)))
}))

it.effect("retires imported systems without a compaction and retains ordinary messages", () => Effect.gen(function* () {
  const db = yield* seed(false)
  yield* InstructionState.importWS(db, { sessionID, baseline: "frozen", baselineSeq: 2, throughSeq: 7, snapshot: {} })

  const history = yield* SessionHistory.load(db, sessionID, "latest")

  expect(history.map((message) => message.id)).toEqual([3, 4, 6, 7].map((seq) => SessionMessage.ID.make(`msg_ws_history_${seq}`)))
}))

it.effect("restores native compaction selection after a real epoch advance", () => Effect.gen(function* () {
  const db = yield* seed()
  yield* InstructionState.importWS(db, { sessionID, baseline: "frozen", baselineSeq: 2, throughSeq: 7, snapshot: { [key]: { value: "current" } } })
  yield* InstructionState.advanceEpoch(db, sessionID, 5)

  const runner = yield* SessionHistory.entriesForRunner(db, sessionID, sources, "latest")
  const preview = yield* SessionHistory.preview(db, sessionID, sources, "local")

  expect(runner.initial).toBe("current")
  expect(runner.entries.map((entry) => entry.seq)).toEqual([5, 6, 7])
  expect(preview.initial).toBe("current")
  expect(preview.messages).toEqual(runner.entries.map((entry) => entry.message))
}))

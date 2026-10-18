import { expect } from "bun:test"
import { eq, sql } from "drizzle-orm"
import { DateTime, Effect } from "effect"
import { LayerNode } from "@opencode/util/effect/layer-node"
import { Money } from "@opencode/schema/money"
import { AppNodeBuilder } from "../src/effect/app-node-builder.js"
import { Database } from "../src/database/database.js"
import { Bus } from "../src/bus.js"
import { WsArchiveTable, WsImportRootTable } from "../src/event/sql.js"
import { Instructions } from "../src/instructions/index.js"
import { Project } from "../src/project.js"
import { ProjectTable } from "../src/project/sql.js"
import { AbsolutePath } from "../src/schema.js"
import { SessionEvent } from "../src/session/event.js"
import { SessionSchema } from "../src/session/schema.js"
import { SessionMessage } from "../src/session/message.js"
import { SessionHistory } from "../src/session/history.js"
import { SessionProjector } from "../src/session/projector.js"
import { InstructionState } from "../src/session/instruction-state.js"
import { WsInstructionFrame } from "../src/session/ws-instruction-frame.js"
import { InstructionStateTable, SessionMessageTable, SessionTable } from "../src/session/sql.js"
import { encodeStoredMessage } from "../src/session/message-storage.js"
import { testEffect } from "./lib/effect.js"

const it = testEffect(AppNodeBuilder.build(LayerNode.group([Database.node, Bus.node, SessionProjector.node])))
const parentID = SessionSchema.ID.make("ses_frame_parent")
const childID = SessionSchema.ID.make("ses_frame_child")
const grandchildID = SessionSchema.ID.make("ses_frame_grandchild")
const key = Instructions.Key.make("source/key")
const retired = Instructions.Key.make("source/retired")
const snapshot = { [key]: { value: "imported", removed: "old removal" }, [retired]: { value: [], removed: "retired removal" } }

const setup = (baseline = " exact baseline\n", notice = "source update", sourceThrough = notice ? 1 : 0) => Effect.gen(function* () {
  const database = yield* Database.Service
  const db = database.db
  const bus = yield* Bus.Service
  yield* db.insert(ProjectTable).values({ id: Project.ID.global, worktree: AbsolutePath.make("/project"), sandboxes: [] }).run()
  yield* db.insert(SessionTable).values({ id: parentID, project_id: Project.ID.global, slug: "frame", directory: "/project", version: "test" }).run()
  yield* db.run("CREATE TABLE _frame_events (id TEXT, aggregate_id TEXT, seq INTEGER, type TEXT, data TEXT)")
  yield* db.run("CREATE TABLE _frame_epoch (session_id TEXT, baseline TEXT, baseline_seq INTEGER, snapshot TEXT)")
  yield* db.run("CREATE TABLE _frame_messages (id TEXT, session_id TEXT, type TEXT, seq INTEGER, time_created INTEGER, time_updated INTEGER, data TEXT)")
  const info = { id: parentID, projectID: "global", slug: "frame", title: "frame", directory: "/project", version: "source", time: { created: 0, updated: 0 } }
  if (sourceThrough >= 0) yield* db.run(sql`INSERT INTO _frame_events VALUES ('evt_source_created', ${parentID}, 0, 'session.created.1', ${JSON.stringify({ sessionID: parentID, info })})`)
  yield* db.run(sql`INSERT INTO _frame_epoch VALUES (${parentID}, ${baseline}, ${Math.min(0, sourceThrough)}, ${JSON.stringify(snapshot)})`)
  if (notice) {
    yield* db.run(sql`INSERT INTO _frame_events VALUES ('evt_source_notice', ${parentID}, 1, 'session.next.context.updated.1', ${JSON.stringify({ sessionID: parentID, messageID: "msg_source_notice", text: notice, timestamp: 1000 })})`)
    const encoded = yield* encodeStoredMessage({ id: SessionMessage.ID.make("msg_source_notice"), type: "system", text: notice, time: { created: DateTime.makeUnsafe(1000) } })
    yield* db.insert(SessionMessageTable).values({ ...encoded.envelope, session_id: parentID, seq: 1 }).run()
    yield* db.run(sql`INSERT INTO _frame_messages VALUES ('msg_source_notice', ${parentID}, 'system', 1, 1000, 1000, ${JSON.stringify(encoded.envelope.data)})`)
  }
  yield* db.insert(WsArchiveTable).values({ id: "frame", manifest: JSON.stringify({ id: "frame", version: 1, lineage_ids: [], objects: [], tables: [
    { name: "event", retained: "_frame_events" }, { name: "session_context_epoch", retained: "_frame_epoch" },
    { name: "session_message", retained: "_frame_messages" },
  ] }) }).run()
  yield* db.insert(WsImportRootTable).values({ aggregate_id: parentID, archive_id: "frame", through_seq: sourceThrough, codec_version: 1 }).run()
  yield* InstructionState.importWS(db, { sessionID: parentID, baseline, baselineSeq: Math.min(0, sourceThrough), throughSeq: sourceThrough, snapshot })
  yield* Bus.reserveSequence(db, parentID, sourceThrough)
  const boundary = yield* bus.publish(SessionEvent.Synthetic, { sessionID: parentID, text: "boundary" })
  return { db, bus, boundary: SessionMessage.ID.fromEvent(boundary.id) }
})

it.effect("uses archived projection notices when their source events are absent", () => Effect.gen(function* () {
  const { db, bus, boundary } = yield* setup(" exact baseline\n", "source update", 3)
  yield* db.run("DELETE FROM _frame_events WHERE type = 'session.next.context.updated.1'")
  yield* db.run(sql`INSERT INTO _frame_messages VALUES
    ('msg_third', ${parentID}, 'system', 3, 1, 1, '{"text":"third","time":{"created":1}}'),
    ('msg_second', ${parentID}, 'system', 2, 2000, 2000, '{"text":"second","time":{"created":2000}}'),
    ('msg_outside', ${parentID}, 'system', 4, 0, 0, '{"text":"outside import prefix","time":{"created":0}}')`)
  yield* db.delete(SessionMessageTable).where(eq(SessionMessageTable.type, "system")).run()
  yield* bus.publish(SessionEvent.InstructionsUpdated, { sessionID: parentID, delta: {}, text: "native notice" })
  yield* bus.publish(SessionEvent.Forked, { sessionID: childID, parentID, boundary: { type: "through", messageID: boundary }, ...yield* InstructionState.captureFork(db, parentID) })
  expect(yield* InstructionState.initial(db, childID, [])).toBe(" exact baseline\n\n\nsource update\n\nsecond\n\nthird\n\nnative notice")
}))

for (const invalid of [
  "UPDATE _ws_archive SET manifest = json_remove(manifest, '$.tables[2]')",
  "DROP TABLE _frame_messages",
  "UPDATE _frame_messages SET data = '{\"text\":42,\"time\":{\"created\":1000}}'",
]) {
  it.effect(`rejects an unavailable or invalid archive projection: ${invalid}`, () => Effect.gen(function* () {
    const { db } = yield* setup()
    yield* db.run(invalid)
    expect((yield* WsInstructionFrame.resolve(db, { sessionID: parentID, throughSeq: 1,
      values: { [key]: Instructions.hash("imported"), [retired]: Instructions.hash([]) },
    }).pipe(Effect.exit))._tag).toBe("Failure")
  }))
}

for (const compact of [false, true]) {
  it.effect(`reconstructs captured current context at an old boundary after parent update${compact ? " and compaction" : ""}`, () => Effect.gen(function* () {
    const { db, bus, boundary } = yield* setup()
    yield* bus.publish(SessionEvent.InstructionsUpdated, { sessionID: parentID, delta: { [key]: Instructions.hash("captured") }, text: "native captured" })
    const captured = yield* InstructionState.captureFork(db, parentID)
    yield* bus.publish(SessionEvent.InstructionsUpdated, { sessionID: parentID, delta: { [key]: Instructions.hash("later") }, text: "later update" })
    if (compact) yield* bus.publish(SessionEvent.Compaction.Ended, { sessionID: parentID, reason: "manual", text: "summary", recent: "recent", cost: Money.USD.make(0), tokens: { input: 0, output: 0, reasoning: 0, cache: { read: 0, write: 0 } } })
    yield* bus.publish(SessionEvent.Forked, { sessionID: childID, parentID, boundary: { type: "through", messageID: boundary }, ...captured })
    expect(yield* InstructionState.initial(db, childID, [])).toBe(" exact baseline\n\n\nsource update\n\nnative captured")
    expect(yield* db.select().from(InstructionStateTable).where(eq(InstructionStateTable.session_id, childID)).get()).toMatchObject({
      initial_values: {}, current_values: captured.instructions, frozen_baseline: " exact baseline\n\n\nsource update\n\nnative captured",
      imported_removals: { [retired]: "retired removal" }, epoch_start: 2, through_seq: 2,
    })
    expect((yield* SessionHistory.load(db, childID, "latest")).filter((message) => message.type === "system")).toEqual([])
  }))
}

it.effect("rebases copied notices and recursively preserves child and grandchild frames", () => Effect.gen(function* () {
  const { db, bus } = yield* setup()
  const update = yield* bus.publish(SessionEvent.InstructionsUpdated, { sessionID: parentID, delta: { [key]: Instructions.hash("captured") }, text: "native captured" })
  yield* bus.publish(SessionEvent.Forked, { sessionID: childID, parentID, boundary: { type: "through", messageID: SessionMessage.ID.fromEvent(update.id) }, ...yield* InstructionState.captureFork(db, parentID) })
  const copied = yield* SessionHistory.load(db, childID, "latest")
  expect(copied.map((message) => message.type)).toEqual(["synthetic"])
  const childUpdate = yield* bus.publish(SessionEvent.InstructionsUpdated, { sessionID: childID, delta: { [retired]: "removed" }, text: "child removal" })
  expect((yield* SessionHistory.load(db, childID, "latest")).filter((message) => message.type === "system").map((message) => message.text)).toEqual(["child removal"])
  const captured = yield* InstructionState.captureFork(db, childID)
  yield* bus.publish(SessionEvent.Compaction.Ended, { sessionID: childID, reason: "manual", text: "summary", recent: "recent", cost: Money.USD.make(0), tokens: { input: 0, output: 0, reasoning: 0, cache: { read: 0, write: 0 } } })
  yield* db.delete(InstructionStateTable).where(eq(InstructionStateTable.session_id, parentID)).run()
  yield* bus.publish(SessionEvent.Forked, { sessionID: grandchildID, parentID: childID, boundary: { type: "through", messageID: SessionMessage.ID.fromEvent(childUpdate.id) }, ...captured })
  expect(yield* InstructionState.initial(db, grandchildID, [])).toBe(" exact baseline\n\n\nsource update\n\nnative captured\n\nchild removal")
  expect((yield* SessionHistory.load(db, grandchildID, "latest")).map((message) => message.type)).toEqual(["synthetic"])
  expect((yield* db.select().from(InstructionStateTable).where(eq(InstructionStateTable.session_id, grandchildID)).get())?.imported_removals).toEqual({})
  yield* bus.publish(SessionEvent.InstructionsUpdated, { sessionID: grandchildID, delta: {}, text: "future grandchild notice" })
  expect((yield* SessionHistory.load(db, grandchildID, "latest")).filter((message) => message.type === "system").map((message) => message.text)).toEqual(["future grandchild notice"])
}))

for (const sourceThrough of [-1, 0]) {
  it.effect(`preserves an empty frozen baseline at source cursor ${sourceThrough}`, () => Effect.gen(function* () {
    const { db, bus, boundary } = yield* setup("", "", sourceThrough)
    yield* bus.publish(SessionEvent.Forked, { sessionID: childID, parentID, boundary: { type: "through", messageID: boundary }, ...yield* InstructionState.captureFork(db, parentID) })
    expect(yield* InstructionState.initial(db, childID, [])).toBe("")
    expect(yield* InstructionState.captureFork(db, childID)).toEqual({ instructions: { [key]: Instructions.hash("imported"), [retired]: Instructions.hash([]) }, instructionSourceSeq: sourceThrough + 1 })
  }))
}

it.effect("excludes source notices already incorporated into the archived baseline", () => Effect.gen(function* () {
  const { db, bus, boundary } = yield* setup()
  yield* db.run("UPDATE _frame_epoch SET baseline_seq = 1")
  yield* bus.publish(SessionEvent.Forked, { sessionID: childID, parentID, boundary: { type: "through", messageID: boundary }, ...yield* InstructionState.captureFork(db, parentID) })
  expect(yield* InstructionState.initial(db, childID, [])).toBe(" exact baseline\n")
}))

it.effect("rejects invalid frozen captures and uses ordinary native initialization after epoch retirement", () => Effect.gen(function* () {
  const { db, bus, boundary } = yield* setup()
  expect((yield* WsInstructionFrame.resolve(db, { sessionID: parentID, throughSeq: 1, values: {} }).pipe(Effect.exit))._tag).toBe("Failure")
  const compacted = yield* bus.publish(SessionEvent.Compaction.Ended, { sessionID: parentID, reason: "manual", text: "summary", recent: "recent", cost: Money.USD.make(0), tokens: { input: 0, output: 0, reasoning: 0, cache: { read: 0, write: 0 } } })
  expect((yield* WsInstructionFrame.resolve(db, { sessionID: parentID, throughSeq: compacted.durable.seq, values: { [key]: Instructions.hash("imported"), [retired]: Instructions.hash([]) } }).pipe(Effect.exit))._tag).toBe("Failure")
  const captured = yield* InstructionState.captureFork(db, parentID)
  expect(captured).not.toHaveProperty("instructionSourceSeq")
  yield* bus.publish(SessionEvent.Forked, { sessionID: childID, parentID, boundary: { type: "through", messageID: boundary }, ...captured })
  expect(yield* db.select().from(InstructionStateTable).where(eq(InstructionStateTable.session_id, childID)).get()).toMatchObject({
    initial_values: captured.instructions, current_values: captured.instructions, frozen_baseline: null, imported_removals: null,
  })
}))

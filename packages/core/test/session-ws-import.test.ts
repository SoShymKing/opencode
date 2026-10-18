import { describe, expect, test } from "bun:test"
import { Effect, Schema, Stream } from "effect"
import { SessionWsImport } from "../src/session/ws-import.js"
import { WsEventCodec } from "../src/database/ws-event-codec.js"
import { WsMessageCodec } from "../src/database/ws-message-codec.js"
import { Bus } from "../src/bus.js"
import { Database } from "../src/database/database.js"
import { AppNodeBuilder } from "../src/effect/app-node-builder.js"
import { LayerNode } from "@opencode/util/effect/layer-node"
import { SessionEvent } from "@opencode/schema/session-event"
import { SessionID } from "@opencode/schema/session-id"
import { Event } from "@opencode/schema/event"
import { SessionMessage } from "@opencode/schema/session-message"
import { EventTable, WsArchiveTable, WsImportRootTable } from "../src/event/sql.js"
import { testEffect } from "./lib/effect.js"
import { sql } from "drizzle-orm"

const sessionID = "ses_source"
const info = { id: sessionID, projectID: "global", slug: "source", directory: "/source", title: "Original",
  version: "source", time: { created: 10, updated: 20 } }
const model = { providerID: "provider", id: "model" }
const row = (seq: number, type: string, data: Record<string, unknown>): WsEventCodec.Row => ({
  id: `evt_unrelated_${seq}`, aggregate_id: sessionID, seq, type, data: JSON.stringify({ sessionID, ...data }),
})
const created = row(0, "session.created.1", { info })
const next = (seq: number, type: string, data: Record<string, unknown>) => row(seq, `session.next.${type}`, { timestamp: 100 + seq, ...data })
const encode = Schema.encodeSync(WsMessageCodec.Message)

describe("WS source event fold", () => {
  test("preserves distinct admission and promotion timestamps", () => {
    const input = { messageID: "msg_promoted", prompt: { text: "input" }, delivery: "steer" }
    const admitted = next(1, "prompt.admitted.1", { ...input, timestamp: 1 })
    const source = [created, admitted, next(2, "prompted.1", { ...input, timestamp: 2 })]
    const result = SessionWsImport.fold(source)
    expect(result.events.map((event) => event.row)).toEqual(source)
    expect(result.inputs[0]).toMatchObject({ admitted: 1, promoted: 2 })
    expect(result.messages.map(({ message }) => encode(message))).toMatchObject([{ type: "user", text: "input", time: { created: 2 } }])
    for (const conflict of [{ prompt: { text: "different" } }, { delivery: "queue" }]) {
      expect(() => SessionWsImport.fold([created, admitted, next(2, "prompted.1", { ...input, ...conflict, timestamp: 2 })])).toThrow("Source input promotion conflict")
    }
    expect(() => SessionWsImport.fold([...source, next(3, "prompted.1", input)])).toThrow("Duplicate source message")
  })

  test("preserves source envelopes, explicit IDs, gaps, move without message, and end-only compaction", () => {
    const source = [created, next(4, "prompted.1", { messageID: "msg_boundary", prompt: { text: "input" }, delivery: "steer" }),
      next(5, "moved.1", { location: { directory: "/destination" } }),
      next(6, "compaction.ended.1", { messageID: "msg_checkpoint", reason: "manual", text: "summary", recent: "recent" })]
    const result = SessionWsImport.fold(source)
    expect(result.events.map((event) => event.row)).toEqual(source)
    expect(result.messages.map((value) => [value.seq, value.message.id, value.message.type])).toEqual([
      [4, "msg_boundary", "user"], [6, "msg_checkpoint", "compaction"],
    ])
    expect(result.info?.directory).toBe("/destination")
    expect(result.evidence).toEqual({ missing: ["sequence:1:3"], projectionRequired: true })
  })

  test("keeps durable progress on failure and hosted results without inventing content", () => {
    const assistant = { assistantMessageID: "msg_answer" }
    const tool = { ...assistant, callID: "call_explicit" }
    const result = SessionWsImport.fold([created,
      next(1, "step.started.1", { ...assistant, agent: "build", model }),
      next(2, "tool.input.started.1", { ...tool, name: "hosted" }),
      next(3, "tool.called.1", { ...tool, tool: "hosted", input: { query: "x" }, provider: { executed: true, metadata: { provider: { call: 1 } } } }),
      next(4, "tool.progress.1", { ...tool, structured: { checkpoint: 7 }, content: [{ type: "text", text: "partial" }] }),
      next(5, "tool.failed.1", { ...tool, error: { type: "unknown", message: "failed" }, result: { hosted: "result" }, provider: { executed: false } }),
    ])
    expect(result.messages.map((value) => encode(value.message))).toEqual([{
      id: "msg_answer", type: "assistant", agent: "build", model, time: { created: 101 }, content: [{
        type: "tool", id: "call_explicit", name: "hosted", time: { created: 102, ran: 103, completed: 105 },
        provider: { executed: true, metadata: { provider: { call: 1 } } },
        state: { status: "error", input: { query: "x" }, structured: { checkpoint: 7 }, content: [{ type: "text", text: "partial" }],
          error: { type: "unknown", message: "failed" }, result: { hosted: "result" } },
      }],
    }])
  })

  test("source revert retains the boundary and removes only later messages and inputs", () => {
    const result = SessionWsImport.fold([created,
      next(1, "prompted.1", { messageID: "msg_boundary", prompt: { text: "keep" }, delivery: "steer" }),
      next(2, "synthetic.1", { messageID: "msg_after", text: "remove" }),
      next(3, "prompt.admitted.1", { messageID: "msg_pending", prompt: { text: "remove" }, delivery: "queue" }),
      next(4, "revert.committed.1", { messageID: "msg_boundary" }),
    ])
    expect(result.messages.map((value) => value.message.id)).toEqual(["msg_boundary"])
    expect(result.inputs.map((value) => value.data.messageID)).toEqual(["msg_boundary"])
  })

  test("replaces only the last matching content ID and keeps empty hosted successes", () => {
    const assistant = { assistantMessageID: "msg_answer" }
    const tool = { ...assistant, callID: "call_empty" }
    const result = SessionWsImport.fold([created,
      next(1, "step.started.1", { ...assistant, agent: "build", model, snapshot: "before" }),
      next(2, "text.started.1", { ...assistant, textID: "text" }),
      next(3, "text.ended.1", { ...assistant, textID: "text", text: "first" }),
      next(4, "text.started.1", { ...assistant, textID: "text" }),
      next(5, "text.ended.1", { ...assistant, textID: "text", text: "second" }),
      next(6, "reasoning.started.1", { ...assistant, reasoningID: "thought", providerMetadata: { provider: { start: true } } }),
      next(7, "reasoning.ended.1", { ...assistant, reasoningID: "thought", text: "reason", providerMetadata: { provider: { end: true } } }),
      next(8, "tool.input.started.1", { ...tool, name: "hosted" }),
      next(9, "tool.input.ended.1", { ...tool, text: "{}" }),
      next(10, "tool.called.1", { ...tool, tool: "hosted", input: {}, provider: { executed: true } }),
      next(11, "tool.success.1", { ...tool, structured: {}, content: [], result: { native: 42 }, provider: { executed: true } }),
      next(12, "step.stream.updated.1", { ...assistant, streamEventCount: 19 }),
      next(13, "step.ended.2", { ...assistant, finish: "stop", cost: 7, tokens: { input: 1, output: 2, reasoning: 3, cache: { read: 4, write: 5 } }, snapshot: "after", files: ["changed.txt"] }),
    ])
    const message = result.messages[0]?.message
    if (message?.type !== "assistant") throw new Error("Expected assistant")
    expect(message.content.slice(0, 2)).toEqual([{ type: "text", id: "text", text: "first" }, { type: "text", id: "text", text: "second" }])
    expect(encode(message)).toMatchObject({ streamEventCount: 19, finish: "stop", cost: 7,
      snapshot: { start: "before", end: "after", files: ["changed.txt"] }, time: { created: 101, completed: 113 },
      content: [{}, {}, { type: "reasoning", providerMetadata: { provider: { end: true } }, time: { created: 106, completed: 107 } },
        { type: "tool", state: { status: "completed", content: [], result: { native: 42 } } }],
    })
    expect(result.info?.cost).toBeUndefined()
  })

  test("switches, frozen text, shell completion, retry and failed steps keep source semantics", () => {
    const result = SessionWsImport.fold([created,
      next(1, "agent.switched.1", { messageID: "msg_agent", agent: "other" }),
      next(2, "model.switched.1", { messageID: "msg_model", model }),
      next(3, "context.updated.1", { messageID: "msg_system", text: "exact source bytes\n" }),
      next(4, "shell.started.1", { messageID: "msg_shell", callID: "call_shell", command: "command" }),
      next(5, "shell.ended.1", { callID: "call_shell", output: "output" }),
      next(6, "step.started.1", { assistantMessageID: "msg_first", agent: "other", model }),
      next(7, "step.started.1", { assistantMessageID: "msg_second", agent: "other", model }),
      next(8, "retried.1", { attempt: 2, error: { message: "retry", isRetryable: true } }),
      next(9, "step.failed.2", { assistantMessageID: "msg_second", error: { type: "unknown", message: "failure" } }),
      next(10, "compaction.started.1", { messageID: "msg_not_projected", reason: "auto" }),
      next(11, "revert.staged.1", { revert: { messageID: "msg_first" } }),
      next(12, "revert.cleared.1", {}),
    ])
    expect(result.messages.map((value) => value.message.id)).toEqual(["msg_agent", "msg_model", "msg_system", "msg_shell", "msg_first", "msg_second"])
    expect(result.messages.map((value) => encode(value.message))).toMatchObject([
      { type: "agent-switched", agent: "other" }, { type: "model-switched", model },
      { type: "system", text: "exact source bytes\n" },
      { type: "shell", callID: "call_shell", output: "output", time: { created: 104, completed: 105 } },
      { type: "assistant", time: { completed: 107 } }, { type: "assistant", finish: "error", time: { completed: 109 } },
    ])
    expect(result.info?.revert).toBeUndefined()
  })

  test("V1 replacement subtracts old usage and removal subtracts the replacement", () => {
    const tokens = { input: 2, output: 3, reasoning: 4, cache: { read: 5, write: 6 } }
    const part = { id: "prt_usage", sessionID, messageID: "msg_legacy", type: "step-finish", reason: "stop", cost: 2, tokens }
    const source = [created, row(1, "message.part.updated.1", { part, time: 100 }),
      row(2, "message.part.updated.1", { part: { ...part, cost: 5 }, time: 200 })]
    const replaced = SessionWsImport.fold(source)
    expect(replaced.info?.cost).toBe(5)
    expect(replaced.info?.tokens).toEqual(tokens)
    expect(replaced.legacyParts[0]?.created).toBe(100)
    const removed = SessionWsImport.fold([...source, row(3, "message.removed.1", { messageID: "msg_legacy" })])
    expect(removed.info?.cost).toBe(0)
    expect(removed.legacyParts).toEqual([])
  })

  test("rejects native-created payloads, unknown versions, mismatched identities and duplicate sequences", () => {
    expect(() => WsEventCodec.decode(row(0, "session.created.1", { title: "native" }))).toThrow()
    expect(() => WsEventCodec.decode(row(0, "session.next.tool.success.99", {}))).toThrow()
    expect(() => WsEventCodec.decode(row(0, "session.created.1", { info: { ...info, id: "ses_other" } }))).toThrow()
    expect(() => SessionWsImport.fold([created, created])).toThrow()
  })
})

const it = testEffect(AppNodeBuilder.build(LayerNode.group([Database.node, Bus.node])))

it.effect("reads the internal source prefix and authoritative frozen instructions without native decoding", () => Effect.gen(function* () {
  const db = (yield* Database.Service).db
  yield* db.run("CREATE TABLE _fixture_events (id TEXT, aggregate_id TEXT, seq INTEGER, type TEXT, data TEXT)")
  yield* db.run("CREATE TABLE _fixture_epoch (session_id TEXT, baseline TEXT, baseline_seq INTEGER, snapshot TEXT)")
  const raw = ` { "sessionID": "${sessionID}", "info": ${JSON.stringify(info)} } `
  yield* db.run(sql`INSERT INTO _fixture_events VALUES ('evt_original', ${sessionID}, 0, 'session.created.1', ${raw})`)
  yield* db.run(sql`INSERT INTO _fixture_epoch VALUES (${sessionID}, '', 3, '{"source":{"value":"current","removed":"remove"}}')`)
  yield* db.insert(WsArchiveTable).values({ id: "archive", manifest: JSON.stringify({ id: "archive", version: 1,
    lineage_ids: ["source-lineage"], objects: [], tables: [{ name: "event", retained: "_fixture_events" },
      { name: "session_context_epoch", retained: "_fixture_epoch" }] }) }).run()
  yield* db.insert(WsImportRootTable).values({ aggregate_id: sessionID, archive_id: "archive", through_seq: 9, codec_version: 1 }).run()
  const imported = yield* SessionWsImport.history(db, sessionID)
  expect(imported.facts.events[0]?.row.data).toBe(raw)
  expect(imported.root.through_seq).toBe(9)
  expect(imported.instructions).toEqual({ baseline: "", baselineSeq: 3, snapshot: { source: { value: "current", removed: "remove" } }, initial: "unknown" })
  expect(imported.facts.evidence.projectionRequired).toBe(true)
}))

it.effect("retains imported native tails with default persistence off, including replay duplicate checks", () => Effect.gen(function* () {
  const db = (yield* Database.Service).db
  const bus = yield* Bus.Service
  yield* db.insert(WsArchiveTable).values({ id: "archive", manifest: "{}" }).run()
  yield* db.insert(WsImportRootTable).values({ aggregate_id: "ses_imported", archive_id: "archive", through_seq: 9, codec_version: 1 }).run()
  yield* Bus.reserveSequence(db, "ses_imported", 9)
  const retained = yield* bus.publish(SessionEvent.Renamed, { sessionID: SessionID.make("ses_imported"), title: "native tail" })
  yield* bus.publish(SessionEvent.Renamed, { sessionID: SessionID.make("ses_unrelated"), title: "not retained" })
  expect((yield* db.select().from(EventTable).all()).map((value) => [value.id, value.seq])).toEqual([[retained.id, 10]])
  const items = yield* Stream.runCollect(bus.log({ aggregateID: "ses_imported" }))
  expect(Array.from(items).map((value) => value.type)).toEqual(["session.renamed", "log.synced"])
  yield* bus.replay({ id: retained.id, aggregateID: "ses_imported", seq: 10, created: retained.created, type: "session.renamed.1", data: retained.data })
  const divergence = yield* Effect.exit(bus.replay({ id: Event.ID.create(), aggregateID: "ses_imported", seq: 10,
    created: retained.created, type: "session.renamed.1", data: retained.data }))
  expect(divergence._tag).toBe("Failure")
  yield* bus.remove("ses_imported")
  expect((yield* db.select().from(EventTable).all()).map((value) => value.id)).toEqual([retained.id])
}))

it.effect("retains fork descendants, batch publications and deletion facts without live ancestry rows", () => Effect.gen(function* () {
  const db = (yield* Database.Service).db
  const bus = yield* Bus.Service
  yield* db.insert(WsArchiveTable).values({ id: "archive", manifest: "{}" }).run()
  yield* db.insert(WsImportRootTable).values({ aggregate_id: "ses_imported", archive_id: "archive", through_seq: 9, codec_version: 1 }).run()
  const child = SessionID.make("ses_child")
  yield* bus.publish(SessionEvent.Forked, { sessionID: child, parentID: SessionID.make("ses_imported"),
    boundary: { type: "through", messageID: SessionMessage.ID.make("msg_boundary") } })
  yield* bus.publishAll([
    [SessionEvent.Renamed, { sessionID: child, title: "child title" }],
    [SessionEvent.Archived, { sessionID: child }],
  ])
  yield* bus.publish(SessionEvent.Deleted, { sessionID: child })
  yield* bus.remove(child)
  const events = yield* db.select().from(EventTable).all()
  expect(events.map((event) => [event.aggregate_id, event.seq, event.type])).toEqual([
    [child, 0, "session.forked.2"], [child, 1, "session.renamed.1"], [child, 2, "session.archived.1"], [child, 3, "session.deleted.2"],
  ])
}))

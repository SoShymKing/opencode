import { expect } from "bun:test"
import path from "node:path"
import { mkdir } from "node:fs/promises"
import { Config } from "@opencode/core/config"
import { EffectDrizzleSqlite } from "@opencode/core/database/drizzle"
import { DatabaseWsArchive } from "@opencode/core/database/ws-archive"
import { sqliteLayer } from "@opencode/core/database/sqlite.bun"
import { llmClient } from "@opencode/core/effect/app-node-platform"
import { InstructionDiscovery } from "@opencode/core/instruction-discovery"
import { SessionInbox } from "@opencode/core/session/inbox"
import { SessionMessage } from "@opencode/core/session/message"
import { SessionRunnerModel } from "@opencode/core/session/runner/model"
import { Global } from "@opencode/util/global"
import { Effect, Layer, Schema } from "effect"
import { LanguageModel, LLMClient } from "../../ai/src"
import { OpenAIChat } from "../../ai/src/protocols/openai-chat"
import { TestLLM } from "../../ai/src/testing"
import { baseline, messageID, pendingQueueID, pendingSteerID, prompt, sessionID, setupWs, userMessageID, wsEvents } from "../../core/test/fixture/database-ws"
import { tempGlobalLayer } from "../../core/test/fixture/global"
import { tmpdirScoped } from "../../core/test/fixture/tmpdir"
import { it } from "../../core/test/lib/effect"
import { ServerFetch } from "../src/fetch"

type Handler = (request: Request) => Promise<Response>
const updates = [
  { id: "msg_policy_first", event: "evt_policy_first", seq: 45, text: "First chronological policy" },
  { id: "msg_policy_second", event: "evt_policy_second", seq: 47, text: "Second chronological policy" },
]

function request(handler: Handler, endpoint: string, payload?: unknown) {
  return Effect.promise(async () => {
    const response = await handler(new Request(`http://opencode.local/api/session/${sessionID}/${endpoint}`, payload === undefined ? undefined : {
      method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify(payload),
    }))
    const body: unknown = await response.json()
    expect({ status: response.status, body }).toMatchObject({ status: 200 })
    return body
  })
}

const messages = (handler: Handler) => request(handler, "message?order=asc").pipe(
  Effect.flatMap(Schema.decodeUnknownEffect(Schema.Struct({ data: Schema.Array(SessionMessage.Info) }))),
  Effect.map((response) => response.data),
)
const inbox = (handler: Handler) => request(handler, "inbox").pipe(
  Effect.flatMap(Schema.decodeUnknownEffect(Schema.Struct({ data: Schema.Array(SessionInbox.Info) }))),
  Effect.map((response) => response.data),
)

function database<A, E>(filename: string, use: (db: EffectDrizzleSqlite.EffectSQLiteDatabase) => Effect.Effect<A, E>) {
  return EffectDrizzleSqlite.makeWithDefaults().pipe(
    Effect.flatMap(use), Effect.provide(sqliteLayer({ filename })), Effect.scoped,
  )
}

for (const resume of ["prompt retry", "native restart claim"] as const) {
  it.live(`converts a full WS file and drains once through ${resume}`, () => Effect.gen(function* () {
    const directory = yield* tmpdirScoped()
    yield* Effect.promise(() => mkdir(path.join(directory.path, "src")))
    const filename = path.join(directory.path, "source.db")
    yield* database(filename, (db) => Effect.gen(function* () {
      yield* setupWs(db, { directory: directory.path, rich: true })
      yield* db.run(`UPDATE session_input SET prompt = '${JSON.stringify({ text: "Steer next", files: prompt.files })}' WHERE id = '${pendingSteerID}'`)
      yield* db.run(`UPDATE event SET data = json_set(data, '$.prompt.files', json('${JSON.stringify(prompt.files)}')) WHERE id = 'evt_steer_unrelated'`)
      yield* db.run("INSERT INTO session_message SELECT 'msg_post_compaction', session_id, type, 44, 44, 44, json_set(data, '$.time.created', 44, '$.time.completed', 44) FROM session_message WHERE id = 'msg_migration'")
      for (const update of updates) {
        yield* db.run(`INSERT INTO session_message VALUES ('${update.id}', 'ses_migration', 'system', ${update.seq}, ${update.seq}, ${update.seq}, '${JSON.stringify({ text: update.text, time: { created: update.seq } })}')`)
        yield* db.run(`INSERT INTO event VALUES ('${update.event}', 'ses_migration', ${update.seq}, 'session.next.context.updated.1', '${JSON.stringify({ sessionID, timestamp: update.seq, messageID: update.id, text: update.text })}')`)
      }
    }))
    const llm = yield* TestLLM.Test.pipe(Effect.provide(TestLLM.testLayer()))
    const model = SessionRunnerModel.resolved(
      LanguageModel.make({ id: "model", provider: "test", route: OpenAIChat.route }),
      { capabilities: { tools: true, input: ["text"], output: ["text"] }, cost: [], limit: { context: 200_000, output: 8_192 } },
    )
    const boot = () => ServerFetch.make({
      database: { path: filename }, models: { fetch: false }, fs: { filewatcher: false, fff: false },
      config: { project: false, claude: false }, events: { persist: true },
    }, { overrides: [
      Global.node.replace(tempGlobalLayer),
      Config.node.replace(Config.configured({ project: false, global: false, claude: false })),
      InstructionDiscovery.node.replace(InstructionDiscovery.configured({ project: false, global: false })),
      llmClient.replace(Layer.succeed(LLMClient.Service, llm)),
      SessionRunnerModel.node.replace(Layer.succeed(SessionRunnerModel.Service, { resolve: () => Effect.succeed(model) })),
    ] })

    yield* Effect.scoped(Effect.gen(function* () {
      const handler = yield* boot()
      const initial = yield* messages(handler)
      expect(initial.map((message) => message.id)).toEqual([
        userMessageID, messageID, "msg_context_explicit", "msg_shell_explicit", "msg_compaction_explicit", "msg_post_compaction", ...updates.map((update) => update.id),
      ].map((id) => SessionMessage.ID.make(id)))
      expect(initial[0]).toMatchObject({ type: "user", text: prompt.text, files: [{ data: "7ZWc6riA", mime: "text/plain", name: prompt.files[0].name, source: { type: "inline" } }] })
      expect(initial[1]).toMatchObject({ type: "assistant", content: expect.arrayContaining([
        { type: "text", text: "first" }, { type: "text", text: "second" },
        expect.objectContaining({ type: "tool", name: "web_search", executed: true }),
      ]) })
      expect(initial[3]).not.toHaveProperty("exit")
      expect(initial[3]).toMatchObject({ type: "shell-recorded", command: "printf fixture", output: "fixture" })
      expect(initial[3]).not.toHaveProperty("shellID")
      expect(yield* inbox(handler)).toMatchObject([
        { id: pendingSteerID, type: "user", delivery: "steer", payload: { text: "Steer next" } },
        { id: pendingQueueID, type: "user", delivery: "queue", payload: { text: "Queue later" } },
      ])
      expect(yield* llm.requests()).toEqual([])
    }))

    const source = yield* database(filename, (db) => Effect.gen(function* () {
      const root = yield* DatabaseWsArchive.readRoot(db, sessionID)
      expect(root).toMatchObject({ aggregate_id: sessionID, through_seq: 50, codec_version: 1 })
      if (!root) throw new Error("Missing WS import root")
      const archive = yield* DatabaseWsArchive.read(db, root.archive_id)
      if (!archive) throw new Error("Missing WS archive")
      const events = DatabaseWsArchive.sourceTable(archive, "event")
      if (!events) throw new Error("Missing archived source events")
      expect(yield* db.all(`SELECT id, seq, type, data FROM "${events.replaceAll('"', '""')}" ORDER BY seq`))
        .toEqual([
          ...wsEvents.map((event) => ({ ...event, data: JSON.stringify(event.id === "evt_steer_unrelated"
            ? { ...event.data, prompt: { text: "Steer next", files: prompt.files } } : event.data) })),
          ...updates.map((update) => ({ id: update.event, seq: update.seq, type: "session.next.context.updated.1",
            data: JSON.stringify({ sessionID, timestamp: update.seq, messageID: update.id, text: update.text }) })),
        ])
      expect(yield* db.get("SELECT seq FROM event_sequence WHERE aggregate_id = 'ses_migration'")).toEqual({ seq: 50 })
      expect(yield* db.get("SELECT workspace_id FROM session_v2 WHERE id = 'ses_migration'")).toEqual({ workspace_id: "wrk_fixture" })
      const inputs = DatabaseWsArchive.sourceTable(archive, "session_input")
      if (!inputs) throw new Error("Missing archived source inputs")
      const pending = yield* db.all(`SELECT id, delivery, admitted_seq, promoted_seq, prompt FROM "${inputs.replaceAll('"', '""')}" ORDER BY admitted_seq`)
      expect(pending).toEqual([
        { id: userMessageID, delivery: "steer", admitted_seq: 2, promoted_seq: 4, prompt: JSON.stringify(prompt) },
        { id: pendingSteerID, delivery: "steer", admitted_seq: 40, promoted_seq: null, prompt: JSON.stringify({ text: "Steer next", files: prompt.files }) },
        { id: pendingQueueID, delivery: "queue", admitted_seq: 43, promoted_seq: null, prompt: '{"text":"Queue later"}' },
      ])
      expect(yield* db.get("SELECT time_suspended FROM session_v2 WHERE id = 'ses_migration'")).toEqual({ time_suspended: null })
      if (resume === "native restart claim")
        yield* db.run("UPDATE session_v2 SET time_suspended = 100 WHERE id = 'ses_migration'")
      return { root, archive, inputs, pending }
    }))

    yield* llm.push(TestLLM.text("Steer completed", "steer-answer"), TestLLM.text("Queue completed", "queue-answer"))
    const completed = yield* Effect.scoped(Effect.gen(function* () {
      const handler = yield* boot()
      if (resume === "prompt retry") {
        const gate = yield* llm.gate()
        yield* request(handler, "prompt", {
          id: pendingSteerID, text: "Must not replace source input", metadata: { retry: "different" }, delivery: "queue",
        })
        yield* gate.started.pipe(Effect.timeout("20 seconds"))
        expect(yield* inbox(handler)).toMatchObject([
          { id: pendingQueueID, delivery: "queue", payload: { text: "Queue later" } },
        ])
        yield* gate.release
      }
      yield* llm.wait(2).pipe(Effect.timeout("20 seconds"))
      const terminal = yield* messages(handler).pipe(
        Effect.repeat({ until: (items) => items.some((message) => message.type === "assistant" &&
          message.finish === "stop" && message.time.completed !== undefined &&
          message.content.some((part) => part.type === "text" && part.text === "Queue completed")) }),
        Effect.timeout("20 seconds"),
      )
      expect(yield* inbox(handler)).toEqual([])
      expect(terminal.filter((message) => message.type === "user").map((message) => ({ id: message.id, text: message.text, metadata: message.metadata })))
        .toEqual([
          { id: SessionMessage.ID.make(userMessageID), text: prompt.text, metadata: undefined },
          { id: SessionMessage.ID.make(pendingSteerID), text: "Steer next", metadata: undefined },
          { id: SessionMessage.ID.make(pendingQueueID), text: "Queue later", metadata: undefined },
        ])
      const requests = yield* llm.requests()
      expect(requests).toHaveLength(2)
      for (const current of requests) {
        expect(current.system.map((part) => part.text)).toContain(baseline)
        const policies = ["Current policy differs from the baseline", ...updates.map((update) => update.text)]
        expect(current.messages.filter((message) => message.role === "system").flatMap((message) =>
          message.content.flatMap((part) => part.type === "text" && policies.includes(part.text) ? [part.text] : [])))
          .toEqual(policies)
        expect(current.messages.find((message) => message.id === pendingSteerID)).toEqual(expect.objectContaining({
          role: "user", content: expect.arrayContaining([
            expect.objectContaining({ type: "text", text: "Steer next" }),
            expect.objectContaining({ type: "text", text: `\n\nAttached file: ${prompt.files[0].name}\n\n\uD55C\uAE00`,
              metadata: { attachment: { source: { type: "inline" }, name: prompt.files[0].name } } }),
          ]),
        }))
        expect(current.messages.find((message) => message.id === "msg_post_compaction")).toEqual(expect.objectContaining({
          role: "assistant", content: expect.arrayContaining([
            expect.objectContaining({ type: "text", text: "first" }),
            expect.objectContaining({ type: "text", text: "second" }),
            expect.objectContaining({ type: "tool-call", id: "hosted-call", name: "web_search", input: { query: "fixture" },
              providerExecuted: true, providerMetadata: { test: { source: "hosted" } } }),
            expect.objectContaining({ type: "tool-result", id: "hosted-call", name: "web_search", providerExecuted: true,
              result: { type: "json", value: { url: "https://example.invalid/result", score: 0.5 } },
              providerMetadata: { test: { request: "fake-request" } } }),
          ]),
        }))
        expect(current.messages.some((message) => message.id === userMessageID || message.id === "msg_shell_explicit")).toBe(false)
      }
      const first = requests[0].messages.flatMap((message) => message.content.flatMap((part) => part.type === "text" ? [part.text] : [])).join("\n")
      const second = requests[1].messages.flatMap((message) => message.content.flatMap((part) => part.type === "text" ? [part.text] : [])).join("\n")
      expect(first).toContain("Steer next")
      expect(first).toContain("Checked the files")
      expect(first).not.toContain("Queue later")
      expect(second.indexOf("Steer next")).toBeLessThan(second.indexOf("Queue later"))
      expect(second).not.toContain("Must not replace source input")
      yield* database(filename, (db) => db.get<{ time_suspended: number | null }>("SELECT time_suspended FROM session_v2 WHERE id = 'ses_migration'")).pipe(
        Effect.repeat({ until: (row) => row?.time_suspended === null }), Effect.timeout("20 seconds"),
      )
      return terminal
    }))

    const tail = yield* database(filename, (db) => Effect.gen(function* () {
      expect(yield* db.get("SELECT time_suspended FROM session_v2 WHERE id = 'ses_migration'")).toEqual({ time_suspended: null })
      expect(yield* DatabaseWsArchive.readRoot(db, sessionID)).toEqual(source.root)
      expect(yield* DatabaseWsArchive.read(db, source.root.archive_id)).toEqual(source.archive)
      const events = yield* db.all<{ seq: number; type: string }>("SELECT seq, type FROM event WHERE aggregate_id = 'ses_migration' ORDER BY seq")
      expect(events.length).toBeGreaterThan(0)
      expect(events.every((event) => event.seq > 50)).toBe(true)
      expect(events.filter((event) => event.type === "session.inbox.enqueued.1")).toEqual([])
      expect(events.filter((event) => event.type === "session.inbox.delivered.1")).toHaveLength(2)
      return events
    }))
    yield* Effect.scoped(Effect.gen(function* () {
      const handler = yield* boot()
      expect(yield* messages(handler)).toEqual(completed)
      expect(yield* inbox(handler)).toEqual([])
      expect(yield* llm.requests()).toHaveLength(2)
    }))
    yield* database(filename, (db) => Effect.gen(function* () {
      expect(yield* db.all("SELECT seq, type FROM event WHERE aggregate_id = 'ses_migration' ORDER BY seq")).toEqual(tail)
      expect(yield* DatabaseWsArchive.readRoot(db, sessionID)).toEqual(source.root)
      expect(yield* DatabaseWsArchive.read(db, source.root.archive_id)).toEqual(source.archive)
      expect(yield* db.all(`SELECT id, delivery, admitted_seq, promoted_seq, prompt FROM "${source.inputs.replaceAll('"', '""')}" ORDER BY admitted_seq`)).toEqual(source.pending)
      expect(yield* db.get("SELECT workspace_id FROM session_v2 WHERE id = 'ses_migration'")).toEqual({ workspace_id: "wrk_fixture" })
    }))
  }), { timeout: 60_000 })
}

import { describe, expect } from "bun:test"
import { eq, sql } from "drizzle-orm"
import { Effect, Stream } from "effect"
import { Bus } from "@opencode/core/bus"
import { Database } from "@opencode/core/database/database"
import { AppNodeBuilder } from "@opencode/core/effect/app-node-builder"
import { ProjectTable } from "@opencode/core/project/sql"
import { Session } from "@opencode/core/session"
import { MessageDecodeError } from "@opencode/core/session/error"
import { SessionExecution } from "@opencode/core/session/execution"
import { SessionHistory } from "@opencode/core/session/history"
import { SessionProjector } from "@opencode/core/session/projector"
import { SessionStore } from "@opencode/core/session/store"
import { SessionMessagePartTable, SessionTable } from "@opencode/core/session/sql"
import { Agent } from "@opencode/schema/agent"
import { Event } from "@opencode/schema/event"
import { Project } from "@opencode/schema/project"
import { AbsolutePath } from "@opencode/schema/schema"
import { SessionEvent } from "@opencode/schema/session-event"
import { SessionMessage } from "@opencode/schema/session-message"
import { LayerNode } from "@opencode/util/effect/layer-node"
import { testEffect } from "./lib/effect"

const it = testEffect(AppNodeBuilder.build(
  LayerNode.group([Database.node, Bus.node, SessionProjector.node, SessionStore.node, Session.node]),
  [Bus.node.replace(Bus.configured({ persist: true })), SessionExecution.node.replace(SessionExecution.noopLayer)],
))
const sessionID = Session.ID.make("ses_history")
const directory = AbsolutePath.make("/project")
const isEvent = (item: Bus.LogItem): item is Event.Payload => !Bus.isSynced(item)
const seed = Effect.gen(function* () {
  const database = yield* Database.Service
  const bus = yield* Bus.Service
  yield* database.db.insert(ProjectTable).values({ id: Project.ID.global, worktree: directory, sandboxes: [] }).run()
  yield* bus.publish(SessionEvent.Created, {
    sessionID, projectID: Project.ID.global, location: { directory }, slug: "history", version: "test",
  })
  return bus
})

describe("Session durable history", () => {
  it.effect("exhausts a migrated Session with no event sequence", () =>
    Effect.gen(function* () {
      const database = yield* Database.Service
      const bus = yield* Bus.Service
      yield* database.db.insert(ProjectTable).values({ id: Project.ID.global, worktree: directory, sandboxes: [] }).run()
      yield* database.db.insert(SessionTable).values({
        id: sessionID, project_id: Project.ID.global, directory, slug: "empty-history", version: "test",
      }).run()

      const events = yield* bus.log({ aggregateID: sessionID }).pipe(Stream.filter((event) => !Bus.isSynced(event)), Stream.runCollect)
      expect(events).toEqual([])
    }),
  )

  it.effect("treats after as an exclusive aggregate sequence", () =>
    Effect.gen(function* () {
      const bus = yield* seed
      yield* bus.publish(SessionEvent.AgentSelected, { sessionID, agent: Agent.ID.make("one") })
      yield* bus.publish(SessionEvent.AgentSelected, { sessionID, agent: Agent.ID.make("two") })

      const events = yield* bus.log({ aggregateID: sessionID, after: 1 }).pipe(
        Stream.filter(isEvent), Stream.runCollect,
      )
      expect(events.map((event) => event.durable?.seq)).toEqual([Event.Seq.make(2)])
    }),
  )

  it.effect("resumes filtered event streams in aggregate order across gaps without duplicates", () =>
    Effect.gen(function* () {
      const bus = yield* seed
      yield* bus.publish(SessionEvent.AgentSelected, { sessionID, agent: Agent.ID.make("one") })
      yield* bus.publish(SessionEvent.Renamed, { sessionID, title: "filtered" })
      yield* bus.publish(SessionEvent.AgentSelected, { sessionID, agent: Agent.ID.make("two") })
      yield* bus.publish(SessionEvent.AgentSelected, { sessionID, agent: Agent.ID.make("three") })

      const first = yield* bus.log({ aggregateID: sessionID }).pipe(
        Stream.filter(isEvent), Stream.filter((event) => event.type === SessionEvent.AgentSelected.type), Stream.take(2), Stream.runCollect,
      )
      const second = yield* bus.log({ aggregateID: sessionID, after: first.at(-1)?.durable?.seq }).pipe(
        Stream.filter(isEvent), Stream.filter((event) => event.type === SessionEvent.AgentSelected.type), Stream.take(2), Stream.runCollect,
      )
      const sequence = [...first, ...second].map((event) => event.durable?.seq)
      expect(first).toHaveLength(2)
      expect(second).toHaveLength(1)
      expect(sequence).toEqual([1, 3, 4].map((seq) => Event.Seq.make(seq)))
      expect(new Set(sequence).size).toBe(sequence.length)
    }),
  )

  it.effect("includes events committed between reads", () =>
    Effect.gen(function* () {
      const bus = yield* seed
      yield* bus.publish(SessionEvent.AgentSelected, { sessionID, agent: Agent.ID.make("one") })
      yield* bus.publish(SessionEvent.AgentSelected, { sessionID, agent: Agent.ID.make("two") })
      const first = yield* bus.log({ aggregateID: sessionID, after: 0 }).pipe(
        Stream.filter(isEvent), Stream.take(1), Stream.runCollect,
      )
      yield* bus.publish(SessionEvent.AgentSelected, { sessionID, agent: Agent.ID.make("later") })
      const second = yield* bus.log({ aggregateID: sessionID, after: first.at(-1)?.durable?.seq }).pipe(
        Stream.filter(isEvent), Stream.runCollect,
      )
      expect([...first, ...second].map((event) => event.durable?.seq)).toEqual([1, 2, 3].map((seq) => Event.Seq.make(seq)))
    }),
  )

  it.effect("reports the captured watermark for exact-limit and limit-plus-one reads", () =>
    Effect.gen(function* () {
      const bus = yield* seed
      yield* bus.publish(SessionEvent.AgentSelected, { sessionID, agent: Agent.ID.make("one") })
      yield* bus.publish(SessionEvent.AgentSelected, { sessionID, agent: Agent.ID.make("two") })
      const exact = yield* bus.log({ aggregateID: sessionID, after: 0 }).pipe(Stream.runCollect)
      const one = yield* bus.log({ aggregateID: sessionID, after: 0 }).pipe(Stream.take(1), Stream.runCollect)
      const exhausted = yield* bus.log({ aggregateID: sessionID, after: 2 }).pipe(Stream.runCollect)

      expect(exact.filter((event) => !Bus.isSynced(event))).toHaveLength(2)
      expect(exact.at(-1)?.type).toBe("log.synced")
      expect(one).toHaveLength(1)
      expect(one[0]?.type).not.toBe("log.synced")
      expect(exhausted).toHaveLength(1)
      expect(exhausted[0]?.type).toBe("log.synced")
    }),
  )

  it.effect("fails with NotFoundError for a missing Session", () =>
    Effect.gen(function* () {
      const session = yield* Session.Service
      const error = yield* session.get(Session.ID.make("ses_missing")).pipe(Effect.flip)
      expect(error._tag).toBe("Session.NotFoundError")
    }),
  )

  it.effect("reports message ownership when stored data cannot be decoded", () =>
    Effect.gen(function* () {
      yield* seed
      const database = yield* Database.Service
      const store = yield* SessionStore.Service
      const messageID = SessionMessage.ID.make("msg_malformed")
      yield* database.db.run(sql`
        INSERT INTO session_message (id, session_id, type, seq, time_created, time_updated, data)
        VALUES (${messageID}, ${sessionID}, 'assistant', 1, 0, 0, '{}')
      `)
      const error = yield* store.messages({ sessionID }).pipe(Effect.flip)
      expect(error).toEqual(new MessageDecodeError({ sessionID, messageID }))
    }),
  )

  it.effect("recovers interrupted tools after completed compaction in parent and child order", () =>
    Effect.gen(function* () {
      yield* seed
      const database = yield* Database.Service
      const db = database.db
      const olderID = SessionMessage.ID.make("msg_interrupted_older")
      const firstID = SessionMessage.ID.make("msg_interrupted_first")
      const secondID = SessionMessage.ID.make("msg_interrupted_second")
      yield* Effect.forEach([
        { id: olderID, type: "assistant", seq: 10, data: {} },
        { id: "msg_interrupted_compaction", type: "compaction", seq: 11, data: { status: "completed" } },
        { id: firstID, type: "assistant", seq: 12, data: {} },
        { id: secondID, type: "assistant", seq: 13, data: {} },
      ], (row) => db.run(sql`
        INSERT INTO session_message (id, session_id, type, seq, time_created, time_updated, data)
        VALUES (${row.id}, ${sessionID}, ${row.type}, ${row.seq}, 0, 0, ${JSON.stringify(row.data)})
      `))
      yield* db.insert(SessionMessagePartTable).values([
        { message_id: olderID, position: 0, id: "tool-before-compaction", type: "tool",
          data: { name: "bash", state: { status: "streaming", input: "" }, time: { created: 10 } } },
        { message_id: firstID, position: 1, id: "tool-running", type: "tool",
          data: { name: "bash", state: { status: "running", input: {}, metadata: {} }, time: { created: 12 } } },
        { message_id: firstID, position: 3, id: "tool-streaming", type: "tool",
          data: { name: "bash", state: { status: "streaming", input: "" }, time: { created: 12 } } },
        { message_id: secondID, position: 0, id: "tool-second-parent", type: "tool",
          data: { name: "bash", state: { status: "streaming", input: "" }, time: { created: 13 } } },
      ]).run()
      yield* db.run(sql`
        INSERT INTO session_message_part (message_id, position, id, type, data)
        VALUES (${firstID}, 2, 'corrupt-unrelated-text', 'text', '{}')
      `)

      const interrupted = yield* SessionHistory.interruptedTools(db, sessionID)
      expect(interrupted.map((item) => ({ assistantMessageID: item.assistantMessageID, id: item.tool.id, status: item.tool.state.status })))
        .toEqual([
          { assistantMessageID: firstID, id: "tool-running", status: "running" },
          { assistantMessageID: firstID, id: "tool-streaming", status: "streaming" },
          { assistantMessageID: secondID, id: "tool-second-parent", status: "streaming" },
        ])
      expect(yield* db.select({ id: SessionMessagePartTable.id }).from(SessionMessagePartTable)
        .where(eq(SessionMessagePartTable.message_id, olderID)).all()).toEqual([{ id: "tool-before-compaction" }])
    }),
  )
})

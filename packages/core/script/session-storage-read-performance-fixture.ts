import { isDeepStrictEqual } from "node:util"
import { and, asc, eq, gt, sql } from "drizzle-orm"
import { DateTime, Effect, Schema } from "effect"
import type { Database } from "@opencode-ai/core/database/database"
import { ModelV2 } from "@opencode-ai/core/model"
import { Project } from "@opencode-ai/core/project"
import { ProjectTable } from "@opencode-ai/core/project/sql"
import { ProviderV2 } from "@opencode-ai/core/provider"
import { AbsolutePath, RelativePath } from "@opencode-ai/core/schema"
import { SessionV2 } from "@opencode-ai/core/session"
import { SessionHistory } from "@opencode-ai/core/session/history"
import { SessionMessage } from "@opencode-ai/core/session/message"
import { SessionMessageTable, SessionTable } from "@opencode-ai/core/session/sql"
import { Snapshot } from "@opencode-ai/core/snapshot"
import { BenchmarkContractError, pragmas } from "./session-projector-performance-fixture"

export { BenchmarkContractError, pragmas }
export const readWorkloads = [
  { name: "page", size: 50 },
  { name: "page", size: 200 },
  { name: "single-message", size: 32 },
  { name: "single-message", size: 128 },
  { name: "single-message", size: 512 },
  { name: "single-message", size: 2048 },
  { name: "runner-context", size: 200 },
  { name: "interrupted-tools", size: 32 },
  { name: "interrupted-tools", size: 128 },
  { name: "interrupted-tools", size: 512 },
  { name: "interrupted-tools", size: 2048 },
  { name: "revert-envelope-scan", size: 200 },
] as const
export type ReadWorkload = (typeof readWorkloads)[number]
export type ReadRunnerOptions = Readonly<{ process: number; warmups: 5; samples: 30; invalidCursor?: boolean; malformedRow?: boolean }>
export type DatabaseService = Database.Interface["db"]
export type ReadFixture = Effect.Success<ReturnType<typeof setupReadFixture>>

const created = DateTime.makeUnsafe(0)
const model = { id: ModelV2.ID.make("storage-read"), providerID: ProviderV2.ID.make("benchmark") }
const encodeMessage = Schema.encodeSync(SessionMessage.Message)
const decodeEnvelope = Schema.decodeUnknownSync(Schema.Struct({ snapshot: Schema.optional(SessionMessage.Assistant.fields.snapshot) }))

function content(count: number, interrupted: boolean) {
  return Array.from({ length: count }, (_, index): SessionMessage.AssistantContent => {
    if (interrupted && index < 4)
      return SessionMessage.AssistantTool.make({
        type: "tool",
        id: `tool-${index}`,
        name: "read",
        state:
          index % 2 === 0
            ? SessionMessage.ToolStatePending.make({ status: "pending", input: "{}" })
            : SessionMessage.ToolStateRunning.make({
                status: "running",
                input: {},
                structured: {},
                content: [{ type: "text", text: "running" }],
              }),
        time: { created },
      })
    if (index % 3 === 0) return SessionMessage.AssistantText.make({ type: "text", id: `text-${index}`, text: `part-${index}` })
    if (index % 3 === 1)
      return SessionMessage.AssistantReasoning.make({
        type: "reasoning",
        id: `reasoning-${index}`,
        text: `why-${index}`,
      })
    return SessionMessage.AssistantTool.make({
      type: "tool",
      id: `tool-${index}`,
      name: "read",
      state: SessionMessage.ToolStateCompleted.make({ status: "completed", input: {}, structured: {}, content: [] }),
      time: { created, completed: created },
    })
  })
}

function assistant(id: string, count: number, interrupted = false, snapshot?: number) {
  return SessionMessage.Assistant.make({
    id: SessionMessage.ID.make(id),
    type: "assistant",
    agent: "build",
    model,
    content: content(count, interrupted),
    ...(snapshot === undefined
      ? {}
      : {
          snapshot: {
            start: Snapshot.ID.make(`snap-${snapshot}`),
            files: [RelativePath.make(`file-${snapshot % 7}.ts`)],
          },
        }),
    time: { created },
  })
}

function row(sessionID: SessionV2.ID, seq: number, message: SessionMessage.Message) {
  const encoded = encodeMessage(message)
  const { id: _id, type, ...data } = encoded
  return { id: message.id, session_id: sessionID, type, seq, time_created: seq, data }
}

export function seedMessages(db: DatabaseService, sessionID: SessionV2.ID, messages: readonly SessionMessage.Message[]) {
  return Effect.gen(function* () {
    const normalized = yield* db
      .get<{
        readonly name: string
      }>(sql`SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'session_message_part'`)
      .pipe(Effect.orDie)
    if (!normalized)
      return yield* db
        .insert(SessionMessageTable)
        .values(messages.map((message, index) => row(sessionID, index, message)))
        .run()
        .pipe(Effect.orDie)
    yield* Effect.forEach(
      messages,
      (message, seq) => {
        const encoded = encodeMessage(message)
        const type = encoded.type
        const data = encoded.type === "assistant" ? (({ id: _id, type: _type, content: _content, ...envelope }) => envelope)(encoded) : (({ id: _id, type: _type, ...stored }) => stored)(encoded)
        return Effect.gen(function* () {
          yield* db.run(sql`INSERT INTO session_message(id, session_id, type, seq, time_created, time_updated, data) VALUES (${message.id}, ${sessionID}, ${type}, ${seq}, ${seq}, ${seq}, ${JSON.stringify(data)})`).pipe(Effect.orDie)
          if (type !== "assistant") return
          yield* Effect.forEach(
            encoded.content,
            (part, position) => {
              const { id, type: partType, ...partData } = part
              return db.run(sql`INSERT INTO session_message_part(message_id, position, id, type, data) VALUES (${message.id}, ${position}, ${id}, ${partType}, ${JSON.stringify(partData)})`).pipe(Effect.orDie)
            },
            { discard: true },
          )
        })
      },
      { discard: true },
    )
  })
}

export function setupReadFixture(db: DatabaseService, workload: ReadWorkload, label: string, malformed = false) {
  return Effect.gen(function* () {
    const sessionID = SessionV2.ID.make(`ses_read_${workload.name}_${workload.size}_${label}`)
    const messages: SessionMessage.Message[] = []
    if (workload.name === "page") for (let index = 0; index < workload.size; index++) messages.push(assistant(`msg_${label}_${index}`, index % 5 === 0 ? 32 : index % 3))
    if (workload.name === "single-message") messages.push(assistant(`msg_${label}_single`, workload.size))
    if (workload.name === "runner-context") {
      messages.push(
        SessionMessage.System.make({
          id: SessionMessage.ID.make(`msg_${label}_baseline`),
          type: "system",
          text: "baseline",
          time: { created },
        }),
      )
      messages.push(
        SessionMessage.Compaction.make({
          id: SessionMessage.ID.make(`msg_${label}_compact`),
          type: "compaction",
          reason: "auto",
          summary: "summary",
          recent: "recent",
          time: { created },
        }),
      )
      messages.push(...Array.from({ length: workload.size }, (_, index) => assistant(`msg_${label}_${index}`, index % 4)))
    }
    if (workload.name === "interrupted-tools") messages.push(assistant(`msg_${label}_interrupted`, workload.size, true))
    if (workload.name === "revert-envelope-scan") {
      messages.push(
        SessionMessage.User.make({
          id: SessionMessage.ID.make(`msg_${label}_boundary`),
          type: "user",
          text: "boundary",
          files: [],
          agents: [],
          time: { created },
        }),
      )
      messages.push(...Array.from({ length: workload.size }, (_, index) => assistant(`msg_${label}_${index}`, 32, false, index)))
    }
    yield* db
      .insert(ProjectTable)
      .values({ id: Project.ID.global, worktree: AbsolutePath.make("/project"), sandboxes: [] })
      .onConflictDoNothing()
      .run()
      .pipe(Effect.orDie)
    yield* db
      .insert(SessionTable)
      .values({
        id: sessionID,
        project_id: Project.ID.global,
        slug: label,
        directory: "/project",
        title: "read performance",
        version: "benchmark",
      })
      .run()
      .pipe(Effect.orDie)
    yield* seedMessages(db, sessionID, messages)
    if (malformed) yield* db.run(sql`UPDATE session_message SET data = ${"{}"} WHERE id = ${messages[0]?.id}`).pipe(Effect.orDie)
    return { sessionID, messages, boundaryID: messages[0]?.id }
  })
}

export function executeRead(db: DatabaseService, sessions: SessionV2.Interface, fixture: ReadFixture, workload: ReadWorkload, invalidCursor = false) {
  if (workload.name === "page")
    return sessions.messages({
      sessionID: fixture.sessionID,
      limit: workload.size,
      order: "asc",
      cursor: invalidCursor ? { id: SessionMessage.ID.make("msg_missing_cursor"), direction: "next" } : undefined,
    })
  if (workload.name === "single-message")
    return sessions.message({
      sessionID: fixture.sessionID,
      messageID: fixture.messages[0]?.id ?? SessionMessage.ID.make("msg_missing"),
    })
  if (workload.name === "runner-context") return SessionHistory.entriesForRunner(db, fixture.sessionID, -1)
  if (workload.name === "interrupted-tools") return SessionHistory.interruptedTools(db, fixture.sessionID)
  return Effect.gen(function* () {
    const boundary = yield* db
      .select({ seq: SessionMessageTable.seq })
      .from(SessionMessageTable)
      .where(and(eq(SessionMessageTable.session_id, fixture.sessionID), eq(SessionMessageTable.id, fixture.boundaryID ?? SessionMessage.ID.make("msg_missing"))))
      .get()
      .pipe(Effect.orDie)
    const rows = boundary
      ? yield* db
          .select()
          .from(SessionMessageTable)
          .where(and(eq(SessionMessageTable.session_id, fixture.sessionID), eq(SessionMessageTable.type, "assistant"), gt(SessionMessageTable.seq, boundary.seq)))
          .orderBy(asc(SessionMessageTable.seq))
          .all()
          .pipe(Effect.orDie)
      : []
    return rows.map((item) => ({ id: item.id, snapshot: decodeEnvelope(item.data).snapshot }))
  })
}

export function validateRead(result: unknown, fixture: ReadFixture, workload: ReadWorkload) {
  if (workload.name === "page" && isDeepStrictEqual(result, fixture.messages)) return true
  if (workload.name === "single-message" && isDeepStrictEqual(result, fixture.messages[0])) return true
  if (
    workload.name === "runner-context" &&
    isDeepStrictEqual(
      result,
      fixture.messages.map((message, seq) => ({ seq, message })),
    )
  )
    return true
  if (workload.name === "interrupted-tools") {
    const expected = fixture.messages.flatMap((message) => (message.type === "assistant" ? message.content.flatMap((part) => (part.type === "tool" && (part.state.status === "pending" || part.state.status === "running") ? [{ assistantMessageID: message.id, tool: part }] : [])) : []))
    if (isDeepStrictEqual(result, expected)) return true
  }
  if (workload.name === "revert-envelope-scan") {
    const expected = fixture.messages.flatMap((message) => (message.type === "assistant" ? [{ id: message.id, snapshot: message.snapshot }] : []))
    if (isDeepStrictEqual(result, expected)) return true
  }
  throw new BenchmarkContractError(`${workload.name}/${workload.size} payload or order mismatch`)
}

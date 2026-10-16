import { isDeepStrictEqual } from "node:util"
import { and, asc, eq, gt, sql } from "drizzle-orm"
import { DateTime, Effect, Schema } from "effect"
import type { Database } from "@opencode/core/database/database"
import { Model } from "@opencode/schema/model"
import { Agent } from "@opencode/schema/agent"
import { Project } from "@opencode/schema/project"
import { ProjectTable } from "@opencode/core/project/sql"
import { Provider } from "@opencode/schema/provider"
import { AbsolutePath, RelativePath } from "@opencode/schema/schema"
import { Session } from "@opencode/schema/session"
import { SessionHistory } from "@opencode/core/session/history"
import { SessionMessage } from "@opencode/schema/session-message"
import { SessionMessageTable, SessionTable } from "@opencode/core/session/sql"
import { Snapshot } from "@opencode/schema/snapshot"
import { SessionStore } from "@opencode/core/session/store"
import { replaceContent } from "@opencode/core/session/projector-content"
import { InstructionState } from "@opencode/core/session/instruction-state"
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
const model = { id: Model.ID.make("storage-read"), providerID: Provider.ID.make("benchmark") }
const encodeMessage = Schema.encodeSync(SessionMessage.Info)
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
            ? SessionMessage.ToolStateStreaming.make({ status: "streaming", input: "{}" })
            : SessionMessage.ToolStateRunning.make({
                status: "running",
                input: {},
                metadata: {},
              }),
        time: { created },
      })
    if (index % 3 === 0) return SessionMessage.AssistantText.make({ type: "text", text: `part-${index}` })
    if (index % 3 === 1)
      return SessionMessage.AssistantReasoning.make({
        type: "reasoning",
        text: `why-${index}`,
      })
    return SessionMessage.AssistantTool.make({
      type: "tool",
      id: `tool-${index}`,
      name: "read",
      state: SessionMessage.ToolStateCompleted.make({ status: "completed", input: {}, content: [{ type: "text", text: "complete" }] }),
      time: { created, completed: created },
    })
  })
}

function assistant(id: string, count: number, interrupted = false, snapshot?: number) {
  return SessionMessage.Assistant.make({
    id: SessionMessage.ID.make(id),
    type: "assistant",
    agent: Agent.ID.make("build"),
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

function row(sessionID: Session.ID, seq: number, message: SessionMessage.Info) {
  const encoded = encodeMessage(message)
  const { id: _id, type, ...data } = encoded.type === "assistant" ? (({ content: _content, ...envelope }) => envelope)(encoded) : encoded
  return { id: message.id, session_id: sessionID, type, seq, time_created: seq, data }
}

export function seedMessages(db: DatabaseService, sessionID: Session.ID, messages: readonly SessionMessage.Info[]) {
  return Effect.gen(function* () {
    yield* db.insert(SessionMessageTable).values(messages.map((message, index) => row(sessionID, index, message))).run().pipe(Effect.orDie)
    yield* Effect.forEach(messages, (message) => message.type === "assistant" ? replaceContent(db, message) : Effect.void, { discard: true })
  })
}

export function setupReadFixture(db: DatabaseService, workload: ReadWorkload, label: string, malformed = false) {
  return Effect.gen(function* () {
    const sessionID = Session.ID.make(`ses_read_${workload.name}_${workload.size}_${label}`)
    const messages: SessionMessage.Info[] = []
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
        SessionMessage.CompactionCompleted.make({
          id: SessionMessage.ID.make(`msg_${label}_compact`),
          type: "compaction",
          status: "completed",
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
    yield* InstructionState.initialize(db, sessionID, -1, {})
    if (malformed) yield* db.run(sql`UPDATE session_message SET data = ${"{}"} WHERE id = ${messages[0]?.id}`).pipe(Effect.orDie)
    return { sessionID, messages, boundaryID: messages[0]?.id }
  })
}

export function executeRead(db: DatabaseService, sessions: SessionStore.Interface, fixture: ReadFixture, workload: ReadWorkload, invalidCursor = false) {
  if (workload.name === "page")
    return sessions.messages({
      sessionID: fixture.sessionID,
      limit: workload.size,
      order: "asc",
      cursor: invalidCursor ? { id: SessionMessage.ID.make("msg_missing_cursor"), direction: "next" } : undefined,
    })
  if (workload.name === "single-message")
    return sessions.message(fixture.messages[0]?.id ?? SessionMessage.ID.make("msg_missing")).pipe(Effect.map((entry) => entry?.message))
  if (workload.name === "runner-context") return SessionHistory.entriesForRunner(db, fixture.sessionID, [], "local")
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
      { initial: "", entries: fixture.messages.map((message, seq) => ({ sessionID: fixture.sessionID, seq, message })).slice(1), rejectedAttachments: [] },
    )
  )
    return true
  if (workload.name === "interrupted-tools") {
    const expected = fixture.messages.flatMap((message) => (message.type === "assistant" ? message.content.flatMap((part) => (part.type === "tool" && (part.state.status === "streaming" || part.state.status === "running") ? [{ assistantMessageID: message.id, tool: part }] : [])) : []))
    if (isDeepStrictEqual(result, expected)) return true
  }
  if (workload.name === "revert-envelope-scan") {
    const expected = fixture.messages.flatMap((message) => (message.type === "assistant" ? [{ id: message.id, snapshot: message.snapshot }] : []))
    if (isDeepStrictEqual(result, expected)) return true
  }
  throw new BenchmarkContractError(`${workload.name}/${workload.size} payload or order mismatch`)
}

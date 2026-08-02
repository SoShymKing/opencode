import fs from "node:fs/promises"
import { eq } from "drizzle-orm"
import { DateTime, Effect, Schema } from "effect"
import { Database } from "@opencode-ai/core/database/database"
import { EventV2 } from "@opencode-ai/core/event"
import { ModelV2 } from "@opencode-ai/core/model"
import { Project } from "@opencode-ai/core/project"
import { ProjectTable } from "@opencode-ai/core/project/sql"
import { ProviderV2 } from "@opencode-ai/core/provider"
import { AbsolutePath } from "@opencode-ai/core/schema"
import { SessionV2 } from "@opencode-ai/core/session"
import { SessionEvent } from "@opencode-ai/core/session/event"
import { SessionMessage } from "@opencode-ai/core/session/message"
import { SessionMessageTable, SessionTable } from "@opencode-ai/core/session/sql"

export const bucketSizes = [32, 128, 512] as const
export const measuredSamples = 9
export const baselineEndToEndNs = 106_291_400
export const baselineP95Ns = 127_608_900
export const created = DateTime.makeUnsafe(0)
export const updated = DateTime.makeUnsafe(1)
export const encodeAssistant = Schema.encodeSync(SessionMessage.Assistant)
export const decodeAssistant = Schema.decodeUnknownSync(SessionMessage.Assistant)
export const eventSequence = [
  SessionEvent.Tool.Input.Started.type,
  SessionEvent.Tool.Input.Ended.type,
  SessionEvent.Tool.Called.type,
  SessionEvent.Tool.Progress.type,
  SessionEvent.Tool.Success.type,
] as const
const model = { id: ModelV2.ID.make("projector-performance"), providerID: ProviderV2.ID.make("benchmark") }

export type PartCount = (typeof bucketSizes)[number]
export type DatabaseService = Database.Interface["db"]

function makeContent(partCount: PartCount): SessionMessage.AssistantContent[] {
  return Array.from({ length: partCount }, (_, index) => {
    const text = `part-${index.toString().padStart(4, "0")}-${"x".repeat(96)}`
    if (index % 3 === 0) return SessionMessage.AssistantText.make({ type: "text", id: `text-${index}`, text })
    if (index % 3 === 1)
      return SessionMessage.AssistantReasoning.make({
        type: "reasoning",
        id: `reasoning-${index}`,
        text,
        providerMetadata: { benchmark: { index } },
        time: { created, completed: created },
      })
    return SessionMessage.AssistantTool.make({
      type: "tool",
      id: `seed-tool-${index}`,
      name: "read",
      state: SessionMessage.ToolStateCompleted.make({
        status: "completed",
        input: { path: `src/file-${index}.ts` },
        structured: { index },
        content: [{ type: "text", text }],
      }),
      time: { created, completed: created },
    })
  })
}

export function makeFixture(partCount: PartCount, label: string) {
  const sessionID = SessionV2.ID.make(`ses_projector_perf_${partCount}_${label}`)
  const assistantMessageID = SessionMessage.ID.make(`msg_projector_perf_${partCount}_${label}`)
  const callID = `call-projector-perf-${partCount}-${label}`
  const initial = SessionMessage.Assistant.make({
    id: assistantMessageID,
    type: "assistant",
    agent: "build",
    model,
    content: makeContent(partCount),
    time: { created },
  })
  const settled = SessionMessage.AssistantTool.make({
    type: "tool",
    id: callID,
    name: "bash",
    provider: { executed: false },
    state: SessionMessage.ToolStateCompleted.make({
      status: "completed",
      input: { command: "pwd" },
      structured: { phase: "done" },
      content: [{ type: "text", text: "complete" }],
      outputPaths: [],
    }),
    time: { created: updated, ran: updated, completed: updated },
  })
  const expected = SessionMessage.Assistant.make({ ...initial, content: [...initial.content, settled] })
  return { partCount, label, sessionID, assistantMessageID, callID, initial, expected }
}
export type Fixture = ReturnType<typeof makeFixture>

export function assistantRow(fixture: Fixture) {
  const { id: _, type, ...data } = encodeAssistant(fixture.initial)
  return {
    id: fixture.assistantMessageID,
    session_id: fixture.sessionID,
    type,
    seq: -1,
    time_created: DateTime.toEpochMillis(created),
    data,
  }
}

export const setupProject = (db: DatabaseService) =>
  db
    .insert(ProjectTable)
    .values({ id: Project.ID.global, worktree: AbsolutePath.make("/project"), sandboxes: [] })
    .run()
    .pipe(Effect.orDie)

export function setupFixture(db: DatabaseService, fixture: Fixture) {
  return Effect.gen(function* () {
    yield* db
      .insert(SessionTable)
      .values({
        id: fixture.sessionID,
        project_id: Project.ID.global,
        slug: fixture.label,
        directory: "/project",
        title: "projector performance",
        version: "benchmark",
      })
      .run()
      .pipe(Effect.orDie)
    yield* db.insert(SessionMessageTable).values(assistantRow(fixture)).run().pipe(Effect.orDie)
  })
}

export function publishUpdate(events: EventV2.Interface, fixture: Fixture, observe: Effect.Effect<void> = Effect.void) {
  const data = { sessionID: fixture.sessionID, timestamp: updated, assistantMessageID: fixture.assistantMessageID }
  const options = (index: number) => ({
    id: EventV2.ID.make(`evt_projector_perf_${fixture.partCount}_${fixture.label}_${index}`),
  })
  return Effect.gen(function* () {
    const published: SessionEvent.Event[] = []
    published.push(
      yield* events.publish(
        SessionEvent.Tool.Input.Started,
        { ...data, callID: fixture.callID, name: "bash" },
        options(0),
      ),
    )
    yield* observe
    published.push(
      yield* events.publish(
        SessionEvent.Tool.Input.Ended,
        { ...data, callID: fixture.callID, text: '{"command":"pwd"}' },
        options(1),
      ),
    )
    yield* observe
    published.push(
      yield* events.publish(
        SessionEvent.Tool.Called,
        { ...data, callID: fixture.callID, tool: "bash", input: { command: "pwd" }, provider: { executed: false } },
        options(2),
      ),
    )
    yield* observe
    published.push(
      yield* events.publish(
        SessionEvent.Tool.Progress,
        {
          ...data,
          callID: fixture.callID,
          structured: { phase: "running" },
          content: [{ type: "text", text: "working" }],
        },
        options(3),
      ),
    )
    yield* observe
    published.push(
      yield* events.publish(
        SessionEvent.Tool.Success,
        {
          ...data,
          callID: fixture.callID,
          structured: { phase: "done" },
          content: [{ type: "text", text: "complete" }],
          provider: { executed: false },
        },
        options(4),
      ),
    )
    yield* observe
    return published
  })
}

export function readAssistant(db: DatabaseService, fixture: Fixture) {
  return Effect.gen(function* () {
    const row = yield* db
      .select()
      .from(SessionMessageTable)
      .where(eq(SessionMessageTable.id, fixture.assistantMessageID))
      .get()
      .pipe(Effect.orDie)
    if (!row) return yield* Effect.die(`Missing assistant ${fixture.assistantMessageID}`)
    return decodeAssistant({ ...row.data, id: row.id, type: row.type })
  })
}

export async function removeTemporary(directory: string, retries = 30): Promise<void> {
  try {
    await fs.rm(directory, { recursive: true, force: true })
  } catch (error) {
    if (retries === 0 || !error || typeof error !== "object" || !("code" in error) || error.code !== "EBUSY")
      throw error
    Bun.gc(true)
    await Bun.sleep(100)
    return removeTemporary(directory, retries - 1)
  }
}

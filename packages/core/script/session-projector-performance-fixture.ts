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

export const siblingCounts = [512, 2048] as const
export const workloadNames = ["tool", "text", "reasoning", "envelope-only", "duplicate-id-latest-match"] as const
export const processCount = 5, warmupSamples = 5, measuredSamples = 30
export const pragmas = { journalMode: "WAL", synchronous: "NORMAL", walAutocheckpoint: 0, foreignKeys: true, busyTimeoutMs: 5000 } as const
export const workloadEventSequences = {
  tool: [SessionEvent.Tool.Input.Started.type, SessionEvent.Tool.Input.Ended.type, SessionEvent.Tool.Called.type, SessionEvent.Tool.Progress.type, SessionEvent.Tool.Success.type],
  text: [SessionEvent.Text.Started.type, SessionEvent.Text.Ended.type],
  reasoning: [SessionEvent.Reasoning.Started.type, SessionEvent.Reasoning.Ended.type],
  "envelope-only": [SessionEvent.Step.Ended.type],
  "duplicate-id-latest-match": [SessionEvent.Text.Ended.type],
} as const satisfies Record<WorkloadName, readonly string[]>

export type SiblingCount = (typeof siblingCounts)[number]
export type WorkloadName = (typeof workloadNames)[number]
export type DatabaseService = Database.Interface["db"]
type Summary = { readonly medianNs: number; readonly p95Ns: number }

export class BenchmarkContractError extends Error {
  constructor(readonly reason: string) {
    super(reason)
  }
}

export function summarizeSamples(samples: readonly number[]): Summary {
  if (samples.length !== measuredSamples)
    throw new BenchmarkContractError(`Expected ${measuredSamples} samples, received ${samples.length}`)
  const sorted = samples.toSorted((left, right) => left - right)
  const lower = sorted[14]
  const upper = sorted[15]
  const p95 = sorted[28]
  if (lower === undefined || upper === undefined || p95 === undefined)
    throw new BenchmarkContractError("Benchmark sample summary is incomplete")
  return { medianNs: (lower + upper) / 2, p95Ns: p95 }
}

export function centralSummary(summaries: readonly Summary[]) {
  if (summaries.length !== processCount)
    throw new BenchmarkContractError(`Expected ${processCount} process summaries, received ${summaries.length}`)
  const medians = summaries.map((item) => item.medianNs).toSorted((left, right) => left - right)
  const p95s = summaries.map((item) => item.p95Ns).toSorted((left, right) => left - right)
  const centralMedianNs = medians[2]
  const centralP95Ns = p95s[2]
  if (centralMedianNs === undefined || centralP95Ns === undefined)
    throw new BenchmarkContractError("Central process summary is incomplete")
  return { centralMedianNs, centralP95Ns }
}

export const created = DateTime.makeUnsafe(0)
export const updated = DateTime.makeUnsafe(1)
export const encodeAssistant = Schema.encodeSync(SessionMessage.Assistant)
export const decodeAssistant = Schema.decodeUnknownSync(SessionMessage.Assistant)
const model = { id: ModelV2.ID.make("projector-performance"), providerID: ProviderV2.ID.make("benchmark") }

function makeContent(siblings: SiblingCount, workload: WorkloadName): SessionMessage.AssistantContent[] {
  return Array.from({ length: siblings }, (_, index) => {
    const text = `part-${index.toString().padStart(4, "0")}-${"x".repeat(96)}`
    if (workload === "duplicate-id-latest-match" && (index === 0 || index === siblings - 1))
      return SessionMessage.AssistantText.make({ type: "text", id: "duplicate-text", text })
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

export function makeFixture(input: {
  readonly siblings: SiblingCount
  readonly workload: WorkloadName
  readonly label: string
  readonly mismatchExpected?: boolean
}) {
  const sessionID = SessionV2.ID.make(`ses_projector_perf_${input.workload}_${input.siblings}_${input.label}`)
  const assistantMessageID = SessionMessage.ID.make(`msg_projector_perf_${input.workload}_${input.siblings}_${input.label}`)
  const targetID = `target-${input.workload}-${input.siblings}-${input.label}`
  const initial = SessionMessage.Assistant.make({
    id: assistantMessageID,
    type: "assistant",
    agent: "build",
    model,
    content: makeContent(input.siblings, input.workload),
    time: { created },
  })
  const appended =
    input.workload === "tool"
      ? SessionMessage.AssistantTool.make({
          type: "tool",
          id: targetID,
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
      : input.workload === "text"
        ? SessionMessage.AssistantText.make({ type: "text", id: targetID, text: "complete" })
        : input.workload === "reasoning"
          ? SessionMessage.AssistantReasoning.make({
              type: "reasoning",
              id: targetID,
              text: "complete",
              providerMetadata: { benchmark: { phase: "done" } },
              time: { created: updated, completed: updated },
            })
          : undefined
  const expected = SessionMessage.Assistant.make({
    ...initial,
    content:
      input.workload === "duplicate-id-latest-match"
        ? initial.content.with(initial.content.length - 1, {
            ...initial.content[initial.content.length - 1],
            type: "text",
            id: "duplicate-text",
            text: "complete",
          })
        : appended
          ? [...initial.content, appended]
          : initial.content,
    ...(input.workload === "envelope-only"
      ? {
          finish: "stop",
          cost: 0,
          tokens: { input: 1, output: 2, reasoning: 3, cache: { read: 4, write: 5 } },
          time: { created, completed: updated },
        }
      : {}),
  })
  return {
    ...input,
    sessionID,
    assistantMessageID,
    targetID,
    initial,
    expected: input.mismatchExpected ? SessionMessage.Assistant.make({ ...expected, finish: "mismatch" }) : expected,
  }
}
export type Fixture = ReturnType<typeof makeFixture>

export function assistantRow(fixture: Fixture) {
  const { id: _id, type, ...data } = encodeAssistant(fixture.initial)
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

export function publishUpdate(events: EventV2.Interface, fixture: Fixture) {
  const data = { sessionID: fixture.sessionID, timestamp: updated, assistantMessageID: fixture.assistantMessageID }
  const options = (index: number) => ({ id: EventV2.ID.make(`evt_${fixture.label}_${index}`) })
  return Effect.gen(function* () {
    switch (fixture.workload) {
      case "tool":
        return [
          yield* events.publish(SessionEvent.Tool.Input.Started, { ...data, callID: fixture.targetID, name: "bash" }, options(0)),
          yield* events.publish(SessionEvent.Tool.Input.Ended, { ...data, callID: fixture.targetID, text: '{"command":"pwd"}' }, options(1)),
          yield* events.publish(SessionEvent.Tool.Called, { ...data, callID: fixture.targetID, tool: "bash", input: { command: "pwd" }, provider: { executed: false } }, options(2)),
          yield* events.publish(SessionEvent.Tool.Progress, { ...data, callID: fixture.targetID, structured: { phase: "running" }, content: [{ type: "text", text: "working" }] }, options(3)),
          yield* events.publish(SessionEvent.Tool.Success, { ...data, callID: fixture.targetID, structured: { phase: "done" }, content: [{ type: "text", text: "complete" }], provider: { executed: false } }, options(4)),
        ]
      case "text":
        return [
          yield* events.publish(SessionEvent.Text.Started, { ...data, textID: fixture.targetID }, options(0)),
          yield* events.publish(SessionEvent.Text.Ended, { ...data, textID: fixture.targetID, text: "complete" }, options(1)),
        ]
      case "reasoning":
        return [
          yield* events.publish(SessionEvent.Reasoning.Started, { ...data, reasoningID: fixture.targetID }, options(0)),
          yield* events.publish(SessionEvent.Reasoning.Ended, { ...data, reasoningID: fixture.targetID, text: "complete", providerMetadata: { benchmark: { phase: "done" } } }, options(1)),
        ]
      case "envelope-only":
        return [yield* events.publish(SessionEvent.Step.Ended, { ...data, finish: "stop", cost: 0, tokens: { input: 1, output: 2, reasoning: 3, cache: { read: 4, write: 5 } } }, options(0))]
      case "duplicate-id-latest-match":
        return [yield* events.publish(SessionEvent.Text.Ended, { ...data, textID: "duplicate-text", text: "complete" }, options(0))]
    }
  })
}

export function readAssistant(db: DatabaseService, fixture: Fixture) {
  return Effect.gen(function* () {
    const row = yield* db.select().from(SessionMessageTable).where(eq(SessionMessageTable.id, fixture.assistantMessageID)).get().pipe(Effect.orDie)
    if (!row) return yield* Effect.die(new BenchmarkContractError(`Missing assistant ${fixture.assistantMessageID}`))
    return decodeAssistant({ ...row.data, id: row.id, type: row.type })
  })
}

export async function removeTemporary(directory: string, retries = 30): Promise<void> {
  try {
    await fs.rm(directory, { recursive: true, force: true })
  } catch (error) {
    if (retries === 0 || !error || typeof error !== "object" || !("code" in error) || error.code !== "EBUSY") throw error
    Bun.gc(true)
    await Bun.sleep(100)
    return removeTemporary(directory, retries - 1)
  }
}

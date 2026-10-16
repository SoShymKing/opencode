import fs from "node:fs/promises"
import { eq } from "drizzle-orm"
import { Clock, DateTime, Effect, Schema } from "effect"
import { Database } from "@opencode/core/database/database"
import { Bus } from "@opencode/core/bus"
import { Event } from "@opencode/schema/event"
import { Agent } from "@opencode/schema/agent"
import { Money } from "@opencode/schema/money"
import { TokenUsage } from "@opencode/schema/token-usage"
import { Model } from "@opencode/schema/model"
import { Project } from "@opencode/schema/project"
import { ProjectTable } from "@opencode/core/project/sql"
import { Provider } from "@opencode/schema/provider"
import { AbsolutePath } from "@opencode/schema/schema"
import { Session } from "@opencode/schema/session"
import { SessionEvent } from "@opencode/schema/session-event"
import { SessionMessage } from "@opencode/schema/session-message"
import { SessionMessageTable, SessionTable } from "@opencode/core/session/sql"
import { hydrateSelection } from "@opencode/core/session/message-storage"
import { replaceContent } from "@opencode/core/session/projector-content"

export const siblingCounts = [512, 2048] as const
export const workloadNames = ["tool", "text", "reasoning", "envelope-only", "duplicate-id-latest-match"] as const
export const processCount = 5, warmupSamples = 5, measuredSamples = 30
export const pragmas = { journalMode: "WAL", synchronous: "NORMAL", walAutocheckpoint: 0, foreignKeys: true, busyTimeoutMs: 5000 } as const
export const workloadEventSequences = {
  tool: [SessionEvent.Tool.Input.Started.type, SessionEvent.Tool.Input.Ended.type, SessionEvent.Tool.Called.type, SessionEvent.Tool.Success.type],
  text: [SessionEvent.Text.Started.type, SessionEvent.Text.Ended.type],
  reasoning: [SessionEvent.Reasoning.Started.type, SessionEvent.Reasoning.Ended.type],
  "envelope-only": [SessionEvent.Step.Ended.type],
  "duplicate-id-latest-match": [SessionEvent.Tool.Success.type],
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
const model = { id: Model.ID.make("projector-performance"), providerID: Provider.ID.make("benchmark") }
const tokens = TokenUsage.Info.make({ input: 1, output: 2, reasoning: 3, cache: { read: 4, write: 5 } })

function makeContent(siblings: SiblingCount, workload: WorkloadName): SessionMessage.AssistantContent[] {
  return Array.from({ length: siblings }, (_, index) => {
    const text = `part-${index.toString().padStart(4, "0")}-${"x".repeat(96)}`
    if (workload === "duplicate-id-latest-match" && (index === 0 || index === siblings - 1))
      return SessionMessage.AssistantTool.make({ type: "tool", id: "duplicate-tool", name: "read", state: SessionMessage.ToolStateRunning.make({ status: "running", input: {}, metadata: { text } }), time: { created } })
    if (index % 3 === 0) return SessionMessage.AssistantText.make({ type: "text", text })
    if (index % 3 === 1)
      return SessionMessage.AssistantReasoning.make({
        type: "reasoning",
        text,
        state: { benchmark: { index } },
        time: { created, completed: created },
      })
    return SessionMessage.AssistantTool.make({
      type: "tool",
      id: `seed-tool-${index}`,
      name: "read",
      state: SessionMessage.ToolStateCompleted.make({
        status: "completed",
        input: { path: `src/file-${index}.ts` },
        metadata: { index },
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
  const sessionID = Session.ID.make(`ses_projector_perf_${input.workload}_${input.siblings}_${input.label}`)
  const assistantMessageID = SessionMessage.ID.make(`msg_projector_perf_${input.workload}_${input.siblings}_${input.label}`)
  const targetID = `target-${input.workload}-${input.siblings}-${input.label}`
  const initial = SessionMessage.Assistant.make({
    id: assistantMessageID,
    type: "assistant",
    agent: Agent.ID.make("build"),
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
          executed: false,
          state: SessionMessage.ToolStateCompleted.make({
            status: "completed",
            input: { command: "pwd" },
            metadata: { phase: "done" },
            content: [{ type: "text", text: "complete" }],
          }),
          time: { created: updated, ran: updated, completed: updated },
        })
      : input.workload === "text"
        ? SessionMessage.AssistantText.make({ type: "text", text: "complete" })
        : input.workload === "reasoning"
          ? SessionMessage.AssistantReasoning.make({
              type: "reasoning",
              text: "complete",
              state: { benchmark: { phase: "done" } },
              time: { created: updated, completed: updated },
            })
          : undefined
  const expected = SessionMessage.Assistant.make({
    ...initial,
    content:
      input.workload === "duplicate-id-latest-match"
        ? initial.content.with(initial.content.length - 1, SessionMessage.AssistantTool.make({ type: "tool", id: "duplicate-tool", name: "read", executed: false, state: SessionMessage.ToolStateCompleted.make({ status: "completed", input: {}, content: [{ type: "text", text: "complete" }] }), time: { created, completed: updated } }))
        : appended
          ? [...initial.content, appended]
          : initial.content,
    ...(input.workload === "envelope-only"
      ? {
          finish: "stop",
          cost: Money.USD.make(0),
          tokens,
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
    expected: input.mismatchExpected ? SessionMessage.Assistant.make({ ...expected, rawFinish: "mismatch" }) : expected,
  }
}
export type Fixture = ReturnType<typeof makeFixture>

export function assistantRow(fixture: Fixture) {
  const { id: _id, type, content: _content, ...data } = encodeAssistant(fixture.initial)
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
    yield* replaceContent(db, fixture.initial)
  })
}

export function publishUpdate(events: Bus.Interface, fixture: Fixture) {
  const data = { sessionID: fixture.sessionID, assistantMessageID: fixture.assistantMessageID }
  const options = (index: number) => ({ id: Event.ID.make(`evt_${fixture.label}_${index}`) })
  const ordinal = fixture.initial.content.filter((part) => part.type === fixture.workload).length
  return Clock.clockWith((clock) => Effect.gen(function* () {
    switch (fixture.workload) {
      case "tool":
        return [
          yield* events.publish(SessionEvent.Tool.Input.Started, { ...data, id: fixture.targetID, name: "bash" }, options(0)),
          yield* events.publish(SessionEvent.Tool.Input.Ended, { ...data, id: fixture.targetID, text: '{"command":"pwd"}' }, options(1)),
          yield* events.publish(SessionEvent.Tool.Called, { ...data, id: fixture.targetID, input: { command: "pwd" }, executed: false }, options(2)),
          yield* events.publish(SessionEvent.Tool.Progress, { ...data, id: fixture.targetID, metadata: { phase: "running" } }, options(3)),
          yield* events.publish(SessionEvent.Tool.Success, { ...data, id: fixture.targetID, metadata: { phase: "done" }, content: [{ type: "text", text: "complete" }], executed: false }, options(4)),
        ]
      case "text":
        return [
          yield* events.publish(SessionEvent.Text.Started, { ...data, ordinal }, options(0)),
          yield* events.publish(SessionEvent.Text.Ended, { ...data, ordinal, text: "complete" }, options(1)),
        ]
      case "reasoning":
        return [
          yield* events.publish(SessionEvent.Reasoning.Started, { ...data, ordinal }, options(0)),
          yield* events.publish(SessionEvent.Reasoning.Ended, { ...data, ordinal, text: "complete", state: { benchmark: { phase: "done" } } }, options(1)),
        ]
      case "envelope-only":
        return [yield* events.publish(SessionEvent.Step.Ended, { ...data, finish: "stop", cost: Money.USD.make(0), tokens }, options(0))]
      case "duplicate-id-latest-match":
        return [yield* events.publish(SessionEvent.Tool.Success, { ...data, id: "duplicate-tool", content: [{ type: "text", text: "complete" }], executed: false }, options(0))]
    }
  }).pipe(Effect.provideService(Clock.Clock, {
    currentTimeMillisUnsafe: () => 1,
    currentTimeMillis: Effect.succeed(1),
    currentTimeNanosUnsafe: () => clock.currentTimeNanosUnsafe(),
    currentTimeNanos: clock.currentTimeNanos,
    monotonicTimeNanosUnsafe: () => clock.monotonicTimeNanosUnsafe(),
    monotonicTimeNanos: clock.monotonicTimeNanos,
    sleep: (duration) => clock.sleep(duration),
  })))
}

export function readAssistant(db: DatabaseService, fixture: Fixture) {
  return Effect.gen(function* () {
    const [row] = yield* hydrateSelection({ db, where: eq(SessionMessageTable.id, fixture.assistantMessageID), limit: 1 })
    if (!row) return yield* Effect.die(new BenchmarkContractError(`Missing assistant ${fixture.assistantMessageID}`))
    return row.message
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

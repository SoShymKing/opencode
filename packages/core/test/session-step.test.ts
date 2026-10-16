import { expect } from "bun:test"
import { AIError, LanguageModel, LLM, LLMEvent, TimeoutError, TransportError } from "@opencode/ai"
import { OpenAIChat } from "@opencode/ai/protocols/openai-chat"
import { TestLLM } from "@opencode/ai/testing"
import { Agent } from "@opencode/core/agent"
import { Bus } from "@opencode/core/bus"
import { Database } from "@opencode/core/database/database"
import { AppNodeBuilder } from "@opencode/core/effect/app-node-builder"
import { EventTable } from "@opencode/core/event/sql"
import { Project } from "@opencode/core/project"
import { ProjectTable } from "@opencode/core/project/sql"
import { AbsolutePath, RelativePath } from "@opencode/core/schema"
import { Session } from "@opencode/core/session"
import { SessionMessage } from "@opencode/core/session/message"
import { SessionHistory } from "@opencode/core/session/history"
import { SessionProjector } from "@opencode/core/session/projector"
import { SessionRunnerModel } from "@opencode/core/session/runner/model"
import { SessionStep } from "@opencode/core/session/runner/step"
import { SessionRunnerRetry } from "@opencode/core/session/runner/retry"
import { SessionMessageTable, SessionTable } from "@opencode/core/session/sql"
import { Snapshot } from "@opencode/core/snapshot"
import { ToolOutput } from "@opencode/core/tool-output"
import { Money } from "@opencode/schema/money"
import { LayerNode } from "@opencode/util/effect/layer-node"
import { asc, eq } from "drizzle-orm"
import { Deferred, Effect, Exit, Fiber, Layer, Logger, Stream } from "effect"
import { TestClock } from "effect/testing"
import { testEffect } from "./lib/effect"

const it = testEffect(
  Layer.merge(
    AppNodeBuilder.build(LayerNode.group([Database.node, Bus.node, SessionProjector.node, ToolOutput.node]), [
      Bus.node.replace(Bus.configured({ persist: true })),
    ]),
    TestLLM.testLayer(),
  ),
)

const retryStep = Effect.gen(function* () {
  const db = (yield* Database.Service).db
  const sessionID = Session.ID.create()
  yield* db.insert(ProjectTable).values({ id: Project.ID.global, worktree: AbsolutePath.make("/project"), sandboxes: [] }).run()
  yield* db.insert(SessionTable).values({ id: sessionID, project_id: Project.ID.global, slug: "retry", directory: "/project", version: "test" }).run()
  const steps = yield* SessionStep.make.pipe(Effect.provide(Layer.mock(Snapshot.Service)({ capture: () => Effect.succeed(undefined) })))
  const model = SessionRunnerModel.resolved(
    LanguageModel.make({ id: "retry-model", provider: "retry-provider", route: OpenAIChat.route }),
    { capabilities: { tools: true, input: ["text"], output: ["text"] }, limit: { context: 100_000, output: 1_000 }, cost: [] },
  )
  const policy = yield* SessionRunnerRetry.policy(sessionID)
  return {
    steps,
    input: {
      isLocationClosed: () => false, sessionID, assistantMessageID: SessionMessage.ID.create(),
      agent: Agent.defaultID, model, postToolContinuation: true,
      prepared: {
        retry: () => Effect.void,
        request: LLM.request({ model: model.model, prompt: "Continue" }), options: {},
        executeTool: () => Effect.succeed({ content: [] }),
      },
      retry: (cause, error, retry, context) => policy({
        ...context, cause, error, retry, agent: Agent.defaultID, model: model.ref,
        hook: (event) => Effect.sync(() => { event.decision = { retry: true, delay: 0 } }),
      }),
      recoverContinuation: false, recoverOverflow: Effect.succeed(false),
    } satisfies Parameters<typeof steps.attempt>[0],
  }
})

for (const child of [false, true]) for (const timeout of ["deadline", "provider", "transport"] as const) it.effect(`Step forwards ${timeout} timeout with ${child ? "child" : "main"} budget`, () => Effect.gen(function* () {
  const harness = yield* retryStep
  const llm = yield* TestLLM.Test
  const parentID = child ? Session.ID.create() : undefined
  const budget = timeout === "deadline" ? child ? 1 : 10 : 3
  for (let index = 0; index <= budget; index++) {
    const started = yield* Deferred.make<void>()
    yield* llm.push(Stream.fromEffect(Deferred.succeed(started, undefined)).pipe(Stream.drain, Stream.concat(
      timeout === "deadline" ? Stream.never : Stream.fail(new AIError({ reason: timeout === "provider"
        ? new TimeoutError({ message: "No semantic response after tool continuation", timeoutMs: 100_000 })
        : new TransportError({ message: "timeout", transport: "websocket", operation: "read", code: "Timeout" }),
      })),
    )))
    const run = yield* harness.steps.attempt({
      ...harness.input, parentID, assistantMessageID: SessionMessage.ID.create(),
      retry: (cause, error, retry, context) => {
        expect(context).toMatchObject({ parentID, noResponse: timeout === "deadline", outputStarted: false })
        return harness.input.retry(cause, error, retry, context)
      },
    }).pipe(Effect.exit, Effect.forkChild({ startImmediately: true }))
    yield* Deferred.await(started)
    if (timeout === "deadline") yield* TestClock.adjust(100_000)
    const result = yield* Fiber.join(run)
    expect(Exit.isSuccess(result)).toBe(index < budget)
    if (Exit.isSuccess(result)) expect(result.value._tag).toBe("Retry")
  }
}))

for (const scenario of ["progress", "abort", "watchdog", "generic-timeout", "empty", "success", "cancel"] as const) {
  it.effect(`Step retry diagnostics ${scenario}`, () => Effect.gen(function* () {
    const harness = yield* retryStep
    const llm = yield* TestLLM.Test
    const logs: ReturnType<typeof Logger.formatStructured.log>[] = []
    let retryCalls = 0
    const started = yield* Deferred.make<void>()
    const failure = new AIError({ reason: new TransportError({
      message: "private-provider-message", body: "private-payload", url: "https://private.invalid/?key=secret",
      transport: "websocket", operation: "read", code: scenario === "abort" ? "AbortError" : "ECONNRESET",
      ...(scenario === "abort" ? { phase: "receive", cause: { abortSource: "network_abort", phase: "model_stream" } } : {}),
    }) })
    const stream = scenario === "progress"
      ? TestLLM.failAfter(failure, LLMEvent.textStart({ id: "text" }), LLMEvent.textDelta({ id: "text", text: "private-text" }))
      : scenario === "abort" ? Stream.fail(failure)
      : scenario === "generic-timeout" ? Stream.fail(new AIError({ reason: new TimeoutError({
        message: "No semantic response after tool continuation", timeoutMs: 100_000,
        cause: { abortSource: "post_tool_first_event_timeout" },
      }) }))
      : scenario === "empty" ? Stream.empty
      : scenario === "success" ? Stream.fromIterable(TestLLM.text("private-text", "text"))
      : Stream.never
    yield* llm.push(Stream.fromEffect(Deferred.succeed(started, undefined)).pipe(Stream.drain, Stream.concat(stream)))
    const run = yield* harness.steps.attempt({ ...harness.input, retry: (...args) => {
      retryCalls++
      return harness.input.retry(...args)
    } }).pipe(
      Effect.exit,
      Effect.provide(Logger.layer([Logger.make((entry) => logs.push(Logger.formatStructured.log(entry)))])),
      Effect.forkChild({ startImmediately: true }),
    )
    yield* Deferred.await(started)
    if (scenario === "watchdog") yield* TestClock.adjust(100_000)
    if (scenario === "cancel") yield* Fiber.interrupt(run)
    const result = yield* Fiber.await(run)
    const retries = logs.flatMap((entry) => Array.isArray(entry.message) && entry.message[0] === "model.no_response.retrying_continuation" ? [entry.message[1]] : [])
    const watchdogs = logs.filter((entry) => Array.isArray(entry.message) && entry.message[0] === "model.no_response.post_tool_first_event_timeout")
    expect(watchdogs).toHaveLength(scenario === "watchdog" ? 1 : 0)
    expect(retries).toHaveLength(scenario === "success" || scenario === "cancel" ? 0 : 1)
    expect(retryCalls).toBe(scenario === "success" || scenario === "cancel" ? 0 : 1)
    if (retries.length) expect(retries[0]).toMatchObject({
      providerID: "retry-provider", modelID: "retry-model", attempt: 1, retryAttempt: 1,
      isPostToolContinuation: true, elapsedMs: scenario === "watchdog" ? 100_000 : 0,
      errorName: scenario === "empty" ? "InvalidProviderOutput" : scenario === "watchdog" || scenario === "generic-timeout" ? "Timeout" : "Transport",
      abortSource: scenario === "watchdog" ? "post_tool_first_event_timeout" : scenario === "abort" ? "network_abort" : "unknown",
      phase: scenario === "empty" ? "message_finalization" : scenario === "abort" ? "model_stream" : "post_tool_continuation",
      ...(scenario === "watchdog" ? { firstStreamEventAt: undefined, firstVisiblePartAt: undefined, partCount: 0 } : {}),
      ...(scenario === "progress" ? { firstStreamEventAt: 0, firstVisiblePartAt: 0, partCount: 1 } : {}),
    })
    expect(JSON.stringify(logs)).not.toMatch(/private-|secret/)
    if (scenario === "cancel") expect(Exit.hasInterrupts(result)).toBe(true)
    if (scenario !== "cancel") {
      expect(Exit.isSuccess(result)).toBe(true)
      if (Exit.isSuccess(result)) {
        expect(Exit.isSuccess(result.value)).toBe(true)
        if (Exit.isSuccess(result.value)) expect(result.value.value._tag).toBe(scenario === "success" ? "Completed" : scenario === "progress" ? "Continue" : "Retry")
      }
    }
  }))
}

for (const fixture of [
  { finish: "stop", toolChoice: undefined },
  { finish: "content-filter", toolChoice: undefined },
  { finish: "stop", toolChoice: "none" },
] as const) {
  it.effect(`settles ${fixture.finish} with tool choice ${fixture.toolChoice ?? "default"}`, () =>
    Effect.gen(function* () {
      const db = (yield* Database.Service).db
      const llm = yield* TestLLM.Test
      const sessionID = Session.ID.create()
      const assistantMessageID = SessionMessage.ID.create()
      const start = Snapshot.ID.make("before")
      const end = Snapshot.ID.make("after")
      const files = [RelativePath.make("changed.ts")]
      let captures = 0
      let executions = 0
      const steps = yield* SessionStep.make.pipe(
        Effect.provide(
          Layer.mock(Snapshot.Service)({
            capture: () => Effect.sync(() => (captures++ === 0 ? start : end)),
            files: (input) => {
              expect(input).toEqual({ from: start, to: end })
              return Effect.succeed(files)
            },
          }),
        ),
      )
      yield* db
        .insert(ProjectTable)
        .values({ id: Project.ID.global, worktree: AbsolutePath.make("/project"), sandboxes: [] })
        .run()
      yield* db
        .insert(SessionTable)
        .values({ id: sessionID, project_id: Project.ID.global, slug: "step", directory: "/project", version: "test" })
        .run()
      const model = SessionRunnerModel.resolved(
        LanguageModel.make({ id: "test-model", provider: "test", route: OpenAIChat.route }),
        {
          capabilities: { tools: true, input: ["text"], output: ["text"] },
          limit: { context: 100_000, output: 1_000 },
          cost: [
            {
              input: Money.USDPerMillionTokens.make(1),
              output: Money.USDPerMillionTokens.make(2),
              cache: { read: Money.USDPerMillionTokens.make(0.1), write: Money.USDPerMillionTokens.make(0.5) },
            },
          ],
        },
      )
      yield* llm.push(
        TestLLM.complete(
          {
            reason: { normalized: fixture.finish },
            usage: {
              inputTokens: 15,
              outputTokens: 6,
              nonCachedInputTokens: 10,
              cacheReadInputTokens: 3,
              cacheWriteInputTokens: 2,
              reasoningTokens: 2,
            },
          },
          LLMEvent.toolCall({ id: "call-test", name: "test", input: {} }),
        ),
      )
      const result = yield* steps
        .attempt({
          isLocationClosed: () => false,
          sessionID,
          assistantMessageID,
          agent: Agent.defaultID,
          model,
          prepared: {
            retry: () => Effect.void,
            request: LLM.request({ model: model.model, prompt: "Run one tool", toolChoice: fixture.toolChoice }),
            options: {},
            executeTool: () =>
              Effect.sync(() => {
                executions++
                return { content: [{ type: "text", text: "Completed tool" }] }
              }),
          },
          retry: (_cause, _error, retry) =>
            Effect.succeed(retry ? { retry: true, attempt: 2, delay: 0 } : { retry: false }),
          recoverContinuation: true,
          recoverOverflow: Effect.succeed(false),
        })
        .pipe(Effect.exit)
      expect(Exit.isSuccess(result)).toBe(fixture.finish === "stop")
      expect(executions).toBe(fixture.toolChoice === "none" ? 0 : 1)
      if (Exit.isSuccess(result))
        expect(result.value).toEqual(
          SessionStep.Outcome.Completed({ needsContinuation: fixture.toolChoice !== "none" }),
        )
      expect(yield* llm.requests()).toHaveLength(1)
      expect(captures).toBe(2)
      const row = yield* db
        .select()
        .from(SessionMessageTable)
        .where(eq(SessionMessageTable.id, assistantMessageID))
        .get()
      const message = row ? yield* SessionHistory.decodeMessageRow(db, row) : undefined
      expect(message).toMatchObject({
        finish: fixture.finish,
        tokens: { input: 10, output: 4, reasoning: 2, cache: { read: 3, write: 2 } },
        snapshot: { start, end, files },
        content: [{ type: "tool", state: { status: fixture.toolChoice === "none" ? "error" : "completed" } }],
      })
      expect(message).toHaveProperty("cost", expect.closeTo(0.0000233, 10))
      const events = yield* db
        .select({ type: EventTable.type })
        .from(EventTable)
        .where(eq(EventTable.aggregate_id, sessionID))
        .orderBy(asc(EventTable.seq))
        .all()
      const types = events.map((event) => event.type)
      const terminal = fixture.finish === "stop" ? "session.step.ended.1" : "session.step.failed.1"
      expect(types.filter((type) => type === "session.step.streamed.1")).toHaveLength(1)
      expect(types.filter((type) => type === terminal)).toHaveLength(1)
      expect(types.indexOf("session.step.streamed.1")).toBeLessThan(types.indexOf(terminal))
      expect(
        types.indexOf(fixture.toolChoice === "none" ? "session.tool.failed.2" : "session.tool.success.2"),
      ).toBeLessThan(types.indexOf(terminal))
    }),
  )
}

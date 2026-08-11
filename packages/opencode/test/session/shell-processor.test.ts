import { EventV2 } from "@opencode-ai/core/event"
import { SessionV1 } from "@opencode-ai/core/v1/session"
import { MessageV2 } from "@/session/message-v2"
import { MessageID } from "@/session/schema"
import { Truncate } from "@/tool/truncate"
import { isRecord } from "@/util/record"
import { expect } from "bun:test"
import { Deferred, Effect, Fiber } from "effect"
import { TestClock } from "effect/testing"
import { callID, inSession, it, start, wait } from "./shell-processor.fixture"
import { makeScenario } from "./shell-process.fixture"
import { captureHistory, replayFresh, toolParts } from "./shell-replay.fixture"

const options = (signal: AbortSignal) => ({ toolCallId: callID, messages: [], abortSignal: signal })

const toolPart = Effect.fn("test.shellToolPart")(function* (messageID: MessageID) {
  const parts = yield* MessageV2.parts(messageID)
  const part = parts.find(
    (item): item is SessionV1.ToolPart => item.type === "tool" && item.callID === callID,
  )
  if (!part) return yield* Effect.die(new Error("shell tool part unavailable"))
  return part
})

it.effect("completes 100 shell chunks through SessionTools and SessionProcessor", () =>
  inSession((directory) =>
    Effect.gen(function* () {
      // Given
      const harness = yield* start(directory)
      const scenario = yield* makeScenario(Array.from({ length: 100 }, (_, index) => `chunk-${index}\n`))
      yield* harness.process.offer(scenario)
      const controller = new AbortController()

      // When
      const result = yield* Effect.promise(() =>
        harness.shell.execute?.({ command: "fixture" }, options(controller.signal)),
      )
      if (!isRecord(result) || typeof result.output !== "string" || !isRecord(result.metadata))
        return yield* Effect.die(new Error("invalid shell result"))
      yield* harness.llm.complete(result)
      expect(yield* wait(Fiber.join(harness.processor), "processor did not complete")).toBe("continue")

      // Then
      const part = yield* toolPart(harness.message.id)
      expect(part.state).toMatchObject({ status: "completed", output: result.output, metadata: result.metadata })
      expect(result.output).toContain("chunk-0")
      expect(result.output).toContain("chunk-99")
      expect(yield* Deferred.isDone(scenario.exited)).toBe(true)
      expect(yield* Deferred.isDone(scenario.killed)).toBe(false)
      const history = yield* captureHistory(harness.chat.id)
      const parts = toolParts(history, callID)
      const running = parts.flatMap((item) =>
        item.state.status === "running" && typeof item.state.metadata?.output === "string" ? [item.state] : [],
      )
      expect(running).toHaveLength(2)
      expect(running[0]?.metadata?.output).toContain("chunk-0")
      expect(running[1]?.metadata?.output).toContain("chunk-99")
      expect(parts).toHaveLength(5)
      expect(
        yield* replayFresh({
          directory,
          projectID: harness.chat.projectID,
          messageID: harness.message.id,
          callID,
          history,
        }),
      ).toEqual(part)
    }),
  ),
)

it.effect("replays outputPath and pending preview after retained tool interruption", () =>
  inSession((directory) =>
    Effect.gen(function* () {
      // Given
      const harness = yield* start(directory)
      const preview = "LATEST-PREVIEW"
      const scenario = yield* makeScenario(["a".repeat(Truncate.MAX_BYTES + 1), preview], true)
      yield* harness.process.offer(scenario)
      const controller = new AbortController()
      const completion = yield* Effect.sync(() =>
        harness.shell.execute?.({ command: "fixture", timeout: 1_000_000_000 }, options(controller.signal)),
      )
      const execution = yield* Deferred.make<{ readonly failed: boolean }>()
      completion.then(
        () => Effect.runSync(Deferred.succeed(execution, { failed: false })),
        () => Effect.runSync(Deferred.succeed(execution, { failed: true })),
      )
      const retained = yield* wait(Deferred.await(harness.toolFiber), "tool fiber was not retained")
      yield* wait(Deferred.await(scenario.drained), "fake process did not drain")
      yield* Deferred.succeed(scenario.release, undefined)
      yield* wait(Deferred.await(scenario.outputDone), "fake process output did not finish")

      // When
      yield* Fiber.interrupt(retained)
      expect((yield* Deferred.await(execution)).failed).toBe(true)
      const processor = yield* Fiber.interrupt(harness.processor).pipe(Effect.forkScoped({ startImmediately: true }))
      yield* TestClock.adjust("60001 millis")
      yield* Fiber.join(processor)

      // Then
      expect(controller.signal.aborted).toBe(false)
      expect(yield* Deferred.isDone(scenario.killed)).toBe(true)
      expect(yield* Deferred.isDone(scenario.exited)).toBe(true)
      const part = yield* toolPart(harness.message.id)
      expect(part.state.status).toBe("error")
      if (part.state.status !== "error") return
      const metadata = part.state.metadata ?? {}
      expect(part.state.error).toBe("Tool execution aborted")
      expect(metadata).toMatchObject({ truncated: true, interrupted: true })
      expect(metadata.outputPath).toBeString()
      expect(metadata.output).toContain(preview)
      const history = yield* captureHistory(harness.chat.id)
      yield* TestClock.adjust("60001 millis")
      expect((yield* captureHistory(harness.chat.id)).latest).toBe(history.latest)
      expect(
        yield* replayFresh({
          directory,
          projectID: harness.chat.projectID,
          messageID: harness.message.id,
          callID,
          history,
        }),
      ).toEqual(part)
    }),
  ),
)

it.effect("completes and replays a shell command with no output", () =>
  inSession((directory) =>
    Effect.gen(function* () {
      // Given
      const harness = yield* start(directory)
      const scenario = yield* makeScenario([])
      yield* harness.process.offer(scenario)
      const controller = new AbortController()

      // When
      const result = yield* Effect.promise(() =>
        harness.shell.execute?.({ command: "fixture" }, options(controller.signal)),
      )
      if (!isRecord(result) || typeof result.output !== "string" || !isRecord(result.metadata))
        return yield* Effect.die(new Error("invalid shell result"))
      yield* harness.llm.complete(result)
      yield* wait(Fiber.join(harness.processor), "processor did not complete")

      // Then
      expect(result).toMatchObject({ output: "(no output)", metadata: { exit: 0, truncated: false } })
      const part = yield* toolPart(harness.message.id)
      expect(part.state.status).toBe("completed")
      const history = yield* captureHistory(harness.chat.id)
      expect(
        yield* replayFresh({
          directory,
          projectID: harness.chat.projectID,
          messageID: harness.message.id,
          callID,
          history,
        }),
      ).toEqual(part)
    }),
  ),
)

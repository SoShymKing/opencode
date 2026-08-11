import { expect } from "bun:test"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { FSUtil } from "@opencode-ai/core/fs-util"
import { Cause, Deferred, Effect, Exit, Fiber, Layer } from "effect"
import { TestClock } from "effect/testing"
import path from "path"
import { ShellProgress } from "@/tool/shell-progress"
import { Truncate } from "@/tool/truncate"
import { provideInstance, testInstanceStoreLayer } from "../fixture/fixture"
import { testEffect } from "../lib/effect"
import { captureShellProgress, executeShell, shellCheckpointNodes } from "./shell-checkpoint.fixture"

const it = testEffect(Layer.mergeAll(LayerNode.compile(shellCheckpointNodes), testInstanceStoreLayer))
const directory = path.join(__dirname, "../..")
const execute = (options: Parameters<typeof executeShell>[0]) => executeShell(options).pipe(provideInstance(directory))

it.live("coalesces fixed 100-chunk output into leading and trailing metadata", () =>
  Effect.gen(function* () {
    // Given
    const chunks = Array.from({ length: 100 }, (_, index) => `chunk-${index + 1}\n`)
    const updates: ShellProgress.Snapshot[] = []

    // When
    const result = yield* execute({
      chunks,
      context: captureShellProgress(updates),
    })

    // Then
    expect(result.output).toBe(chunks.join(""))
    expect(updates).toEqual([{ output: "chunk-1\n" }, { output: chunks.join("") }])
  }),
)

it.effect("drains output after process exit before settling", () =>
  Effect.gen(function* () {
    // Given
    const drained = yield* Deferred.make<void>()
    const release = yield* Deferred.make<void>()
    const fiber = yield* execute({
      chunks: ["first", "second"],
      drained,
      release,
      exitBeforeDrain: true,
    }).pipe(Effect.forkChild)
    yield* Deferred.await(drained)

    // When
    yield* Deferred.succeed(release, undefined)
    const result = yield* Fiber.join(fiber)

    // Then
    expect(result.output).toBe("firstsecond")
  }),
)

it.effect("preserves process and metadata cleanup defects", () =>
  Effect.gen(function* () {
    // Given
    const drained = yield* Deferred.make<void>()
    const controller = new AbortController()
    let publications = 0
    const fiber = yield* execute({
      chunks: ["first", "second"],
      hang: true,
      drained,
      killDefect: "original-kill-boom",
      context: {
        ...captureShellProgress([], controller.signal),
        metadata: () =>
          Effect.sync(() => {
            publications++
            if (publications === 2) throw new Error("finalizer-metadata-boom")
          }),
      },
    }).pipe(Effect.forkChild)
    yield* Deferred.await(drained)

    // When
    controller.abort()
    const exit = yield* Fiber.await(fiber)

    // Then
    expect(Exit.isFailure(exit)).toBe(true)
    if (Exit.isFailure(exit)) {
      expect(
        exit.cause.reasons
          .filter(Cause.isDieReason)
          .map((reason) => (reason.defect instanceof Error ? reason.defect.message : reason.defect)),
      ).toEqual(["original-kill-boom", "finalizer-metadata-boom"])
    }
  }),
)

it.live("publishes truncation path urgently and retains it through trailing flush", () =>
  Effect.gen(function* () {
    // Given
    const chunks = ["a".repeat(Truncate.MAX_BYTES - 10), "b".repeat(20), "tail"]
    const updates: ShellProgress.Snapshot[] = []

    // When
    const result = yield* execute({
      chunks,
      context: captureShellProgress(updates),
    })

    // Then
    expect(updates).toHaveLength(3)
    expect(updates[0]?.outputPath).toBeUndefined()
    expect(updates.slice(1).every((snapshot) => snapshot.truncated && snapshot.outputPath)).toBe(true)
    expect(updates.at(-1)?.outputPath).toBe(result.metadata.outputPath)
    expect(yield* (yield* FSUtil.Service).readFileString(result.metadata.outputPath ?? "")).toBe(chunks.join(""))
  }),
)

it.live("publishes final line-truncation discovery before settlement", () =>
  Effect.gen(function* () {
    // Given
    const output = Array.from({ length: Truncate.MAX_LINES + 1 }, (_, index) => `${index}\n`).join("")
    const updates: ShellProgress.Snapshot[] = []

    // When
    const result = yield* execute({
      chunks: [output],
      context: captureShellProgress(updates),
    })

    // Then
    expect(updates).toHaveLength(2)
    expect(updates.at(-1)).toMatchObject({
      truncated: true,
      outputPath: result.metadata.outputPath,
    })
    expect(yield* (yield* FSUtil.Service).readFileString(result.metadata.outputPath ?? "")).toBe(output)
  }),
)

it.live("preserves no-output success and nonzero exit contracts", () =>
  Effect.gen(function* () {
    // Given
    const successUpdates: ShellProgress.Snapshot[] = []
    const errorUpdates: ShellProgress.Snapshot[] = []

    // When
    const success = yield* execute({
      chunks: [],
      context: captureShellProgress(successUpdates),
    })
    const error = yield* execute({
      chunks: [],
      exitCode: 42,
      context: captureShellProgress(errorUpdates),
    })

    // Then
    expect(success).toMatchObject({
      output: "(no output)",
      metadata: { exit: 0, truncated: false },
    })
    expect(error).toMatchObject({
      output: "(no output)",
      metadata: { exit: 42, truncated: false },
    })
    expect(successUpdates).toEqual([])
    expect(errorUpdates).toEqual([])
  }),
)

it.effect("flushes pending metadata before timeout settlement", () =>
  Effect.gen(function* () {
    // Given
    const drained = yield* Deferred.make<void>()
    const updates: ShellProgress.Snapshot[] = []
    const fiber = yield* execute({
      chunks: ["first", "second"],
      hang: true,
      drained,
      timeout: 1,
      context: captureShellProgress(updates),
    }).pipe(Effect.forkChild)
    yield* Deferred.await(drained)

    // When
    yield* TestClock.adjust(101)
    const result = yield* Fiber.join(fiber)

    // Then
    expect(result.metadata.exit).toBeNull()
    expect(result.output).toContain("shell tool terminated command after exceeding timeout 1 ms")
    expect(updates).toEqual([{ output: "first" }, { output: "firstsecond" }])
  }),
)

it.effect("flushes pending metadata before AbortSignal settlement", () =>
  Effect.gen(function* () {
    // Given
    const drained = yield* Deferred.make<void>()
    const controller = new AbortController()
    const updates: ShellProgress.Snapshot[] = []
    const fiber = yield* execute({
      chunks: ["first", "second"],
      hang: true,
      drained,
      context: captureShellProgress(updates, controller.signal),
    }).pipe(Effect.forkChild)
    yield* Deferred.await(drained)

    // When
    controller.abort()
    const result = yield* Fiber.join(fiber)
    yield* TestClock.adjust(60_000)

    // Then
    expect(result.output).toContain("User aborted the command")
    expect(updates).toEqual([{ output: "first" }, { output: "firstsecond" }])
  }),
)

it.effect("flushes pending metadata before Effect interruption and stops later publication", () =>
  Effect.gen(function* () {
    // Given
    const drained = yield* Deferred.make<void>()
    const updates: ShellProgress.Snapshot[] = []
    const fiber = yield* execute({
      chunks: ["first", "second"],
      hang: true,
      drained,
      context: captureShellProgress(updates),
    }).pipe(Effect.forkChild)
    yield* Deferred.await(drained)

    // When
    yield* Fiber.interrupt(fiber)
    const exit = yield* Fiber.await(fiber)
    yield* TestClock.adjust(60_000)

    // Then
    expect(Exit.isFailure(exit) && Cause.hasInterruptsOnly(exit.cause)).toBe(true)
    expect(updates).toEqual([{ output: "first" }, { output: "firstsecond" }])
  }),
)

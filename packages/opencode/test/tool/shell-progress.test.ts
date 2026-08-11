import { describe, expect } from "bun:test"
import { Cause, Effect, Exit, Fiber } from "effect"
import { TestClock } from "effect/testing"
import { ShellProgress } from "@/tool/shell-progress"
import { it } from "../lib/effect"

const first = { output: "first" }, second = { output: "second" }, publishDefect = "injected publish defect"

function fixture() {
  const publications: ShellProgress.Snapshot[] = []
  return Effect.gen(function*() {
    const progress = yield* ShellProgress.make({
      checkpointIntervalMs: 100,
      publish: (snapshot) => Effect.sync(() => publications.push(snapshot)),
    })
    return { progress, publications }
  })
}

function defectFixture() {
  let publications = 0
  return Effect.gen(function*() {
    const progress = yield* ShellProgress.make({
      checkpointIntervalMs: 100,
      publish: () =>
        Effect.sync(() => (publications += 1)).pipe(
          Effect.andThen(() => (publications === 2 ? Effect.die(publishDefect) : Effect.void)),
        ),
    })
    yield* progress.update(first).pipe(Effect.andThen(progress.update(second)))
    return { progress, publications: () => publications }
  })
}

function within<A, R>(effect: Effect.Effect<A, never, R>) {
  return Effect.gen(function*() {
    const fiber = yield* Effect.race(
      effect.pipe(Effect.map((value) => ({ _tag: "Done" as const, value }))),
      Effect.sleep(1).pipe(Effect.as({ _tag: "Timeout" as const })),
    ).pipe(Effect.forkScoped({ startImmediately: true }))
    yield* TestClock.adjust(1)
    return yield* Fiber.join(fiber)
  })
}

function expectPublishDefect(exit: Exit.Exit<void>) {
  expect(Exit.isFailure(exit) && Cause.pretty(exit.cause).includes(publishDefect)).toBe(true)
}

describe("ShellProgress", () => {
  it.effect("ignores empty and structurally unchanged snapshots", () =>
    Effect.scoped(
      Effect.gen(function*() {
        // Given
        const test = yield* fixture()

        // When
        yield* test.progress.update({ output: "" })
        yield* test.progress.update(first)
        yield* test.progress.update({ output: "first" })
        yield* TestClock.adjust(100)

        // Then
        expect(test.publications).toEqual([first])
      }),
    ),
  )

  it.effect("publishes first changed non-empty snapshot immediately", () =>
    Effect.scoped(
      Effect.gen(function*() {
        // Given
        const test = yield* fixture()

        // When
        yield* test.progress.update(first)

        // Then
        expect(test.publications).toEqual([first])
      }),
    ),
  )

  it.effect("coalesces 100 updates into latest pending snapshot", () =>
    Effect.scoped(
      Effect.gen(function*() {
        // Given
        const test = yield* fixture()
        yield* test.progress.update(first)

        // When
        yield* Effect.forEach(Array.from({ length: 100 }, (_, index) => index + 1), (index) =>
          test.progress.update({ output: `chunk-${index}` }),
        )
        yield* TestClock.adjust(100)

        // Then
        expect(test.publications).toEqual([first, { output: "chunk-100" }])
      }),
    ),
  )

  it.effect("publishes pending snapshot at exact interval boundary", () =>
    Effect.scoped(
      Effect.gen(function*() {
        // Given
        const test = yield* fixture()
        yield* test.progress.update(first)
        yield* TestClock.adjust(90)
        yield* test.progress.update(second)
        yield* TestClock.adjust(9)
        expect(test.publications).toEqual([first])

        // When
        yield* TestClock.adjust(1)

        // Then
        expect(test.publications).toEqual([first, second])
      }),
    ),
  )

  it.effect("replaces pending snapshot with latest changed value", () =>
    Effect.scoped(
      Effect.gen(function*() {
        // Given
        const test = yield* fixture()
        yield* test.progress.update(first)

        // When
        yield* test.progress.update({ output: "middle" })
        yield* test.progress.update(second)
        yield* TestClock.adjust(100)

        // Then
        expect(test.publications).toEqual([first, second])
      }),
    ),
  )

  it.effect("publishes urgent output-path discovery immediately", () =>
    Effect.scoped(
      Effect.gen(function*() {
        // Given
        const test = yield* fixture()
        const urgent = { output: "latest", truncated: true, outputPath: "/tmp/shell-output" }
        yield* test.progress.update(first)
        yield* test.progress.update(second)

        // When
        yield* test.progress.update(urgent, true)

        // Then
        expect(test.publications).toEqual([first, urgent])
        yield* TestClock.adjust(100)
        expect(test.publications).toEqual([first, urgent])
      }),
    ),
  )

  it.effect("flushes pending latest snapshot at most once", () =>
    Effect.scoped(
      Effect.gen(function*() {
        // Given
        const test = yield* fixture()
        yield* test.progress.update(first)
        yield* test.progress.update(second)

        // When
        yield* test.progress.flush
        yield* test.progress.flush

        // Then
        expect(test.publications).toEqual([first, second])
        yield* TestClock.adjust(100)
        expect(test.publications).toEqual([first, second])
      }),
    ),
  )

  it.effect("close flushes pending value once and is idempotent", () =>
    Effect.scoped(
      Effect.gen(function*() {
        // Given
        const test = yield* fixture()
        yield* test.progress.update(first)
        yield* test.progress.update(second)

        // When
        yield* Effect.all([test.progress.close, test.progress.close], { concurrency: "unbounded" })

        // Then
        expect(test.publications).toEqual([first, second])
      }),
    ),
  )

  it.effect("close racing interval publication emits latest value once", () =>
    Effect.scoped(
      Effect.gen(function*() {
        // Given
        const test = yield* fixture()
        yield* test.progress.update(first)
        yield* test.progress.update(second)

        // When
        yield* Effect.all([test.progress.close, TestClock.adjust(100)], { concurrency: "unbounded" })
        yield* TestClock.adjust(100)

        // Then
        expect(test.publications).toEqual([first, second])
      }),
    ),
  )

  it.effect("ignores updates after close", () =>
    Effect.scoped(
      Effect.gen(function*() {
        // Given
        const test = yield* fixture()
        yield* test.progress.update(first)
        yield* test.progress.close

        // When
        yield* test.progress.update(second, true)
        yield* TestClock.adjust(100)

        // Then
        expect(test.publications).toEqual([first])
      }),
    ),
  )

  it.effect("scope cleanup flushes once and cannot publish afterward", () =>
    Effect.gen(function*() {
      // Given
      const publications: ShellProgress.Snapshot[] = []
      const progress = yield* Effect.scoped(
        Effect.gen(function*() {
          const scoped = yield* ShellProgress.make({
            checkpointIntervalMs: 100,
            publish: (snapshot) => Effect.sync(() => publications.push(snapshot)),
          })
          yield* scoped.update(first)
          yield* scoped.update(second)
          return scoped
        }),
      )

      // When
      yield* progress.update({ output: "after-scope" }, true)
      yield* TestClock.adjust(100)

      // Then
      expect(publications).toEqual([first, second])
    }),
  )

  it.effect("flush propagates worker publish defect instead of timing out", () =>
    Effect.gen(function*() {
      const cleanup = yield* Effect.scoped(
        Effect.gen(function*() {
          // Given
          const test = yield* defectFixture()

          // When
          const result = yield* within(test.progress.flush.pipe(Effect.exit))

          // Then
          expect(result._tag).toBe("Done")
          if (result._tag === "Done") expectPublishDefect(result.value)
        }),
      ).pipe(Effect.exit)
      expectPublishDefect(cleanup)
    }),
  )

  it.effect("urgent worker defect closes coordinator and reaches later waiters", () =>
    Effect.gen(function*() {
      const cleanup = yield* Effect.scoped(
        Effect.gen(function*() {
          // Given
          const test = yield* defectFixture()

          // When
          const urgent = yield* within(test.progress.update({ output: "urgent" }, true).pipe(Effect.exit))

          // Then
          expect(urgent._tag).toBe("Done")
          if (urgent._tag === "Done") expectPublishDefect(urgent.value)
          yield* test.progress.update({ output: "after-defect" })
          expect(test.publications()).toBe(2)
          expectPublishDefect(yield* test.progress.flush.pipe(Effect.exit))
          expectPublishDefect(yield* test.progress.close.pipe(Effect.exit))
        }),
      ).pipe(Effect.exit)
      expect(Exit.isSuccess(cleanup)).toBe(true)
    }),
  )

  it.effect("concurrent close callers receive same trailing publish defect", () =>
    Effect.gen(function*() {
      const cleanup = yield* Effect.scoped(
        Effect.gen(function*() {
          // Given
          const test = yield* defectFixture()

          // When
          const result = yield* within(
            Effect.all([test.progress.close.pipe(Effect.exit), test.progress.close.pipe(Effect.exit)], {
              concurrency: "unbounded",
            }),
          )

          // Then
          expect(result._tag).toBe("Done")
          if (result._tag !== "Done") return
          expectPublishDefect(result.value[0])
          expectPublishDefect(result.value[1])
          if (Exit.isFailure(result.value[0]) && Exit.isFailure(result.value[1])) {
            expect(Cause.pretty(result.value[0].cause)).toBe(Cause.pretty(result.value[1].cause))
          }
        }),
      ).pipe(Effect.exit)
      expect(Exit.isSuccess(cleanup)).toBe(true)
    }),
  )

  it.effect("scope cleanup propagates trailing publish defect without hanging", () =>
    Effect.scoped(
      Effect.gen(function*() {
        // Given
        const cleanup = Effect.scoped(defectFixture().pipe(Effect.asVoid))

        // When
        const result = yield* within(cleanup.pipe(Effect.exit))

        // Then
        expect(result._tag).toBe("Done")
        if (result._tag === "Done") expectPublishDefect(result.value)
      }),
    ),
  )
})

import { describe, expect } from "bun:test"
import { Deferred, Effect, Fiber } from "effect"
import { PromptHistory } from "@/session/prompt-history"
import { testEffect } from "../lib/effect"
import { addAssistant, addText, addUser, layer, measure, withSession } from "./prompt-history.fixtures"
import { registerFallbackTests } from "./prompt-history-fallback.fixtures"
import { registerOrderingTests } from "./prompt-history-ordering.fixtures"

const it = testEffect(layer)

describe("PromptHistory", () => {
  it.instance("loads over 100 messages and skips hydration when sequence is unchanged", () =>
    withSession(({ sessionID }) =>
      Effect.gen(function* () {
        yield* Effect.forEach(Array.from({ length: 101 }, (_, index) => index), (index) => addUser(sessionID, `${index}`), {
          discard: true,
        })
        const history = yield* PromptHistory.make(sessionID)
        const first = yield* measure(history.refresh())
        expect(first.value).toHaveLength(101)
        expect(first.counts).toEqual({ sequence: 2, pages: 3, hydrated: 0, ranges: 0 })

        const second = yield* measure(history.refresh())

        expect(second.counts).toEqual({ sequence: 1, pages: 0, hydrated: 0, ranges: 0 })
      }),
    ),
  )

  it.instance("hydrates one touched tail message for one bounded range", () =>
    withSession(({ session, sessionID }) =>
      Effect.gen(function* () {
        const tail = yield* addUser(sessionID, "before")
        const history = yield* PromptHistory.make(sessionID)
        yield* history.refresh()
        yield* session.updatePart({ ...tail.part, text: "after" })

        const refreshed = yield* measure(history.refresh())

        expect(refreshed.value[0]?.parts).toContainEqual({ ...tail.part, text: "after" })
        expect(refreshed.counts).toEqual({ sequence: 1, pages: 0, hydrated: 1, ranges: 1 })
      }),
    ),
  )

  it.instance("deduplicates multiple events touching one message", () =>
    withSession(({ session, sessionID }) =>
      Effect.gen(function* () {
        const tail = yield* addUser(sessionID, "zero")
        const history = yield* PromptHistory.make(sessionID)
        yield* history.refresh()
        yield* session.updatePart({ ...tail.part, text: "one" })
        yield* session.updatePart({ ...tail.part, text: "two" })

        const refreshed = yield* measure(history.refresh())

        expect(refreshed.counts.hydrated).toBe(1)
        expect(refreshed.value[0]?.parts).toContainEqual({ ...tail.part, text: "two" })
      }),
    ),
  )

  it.instance("appends only an unambiguous newer user and assistant tail", () =>
    withSession(({ sessionID }) =>
      Effect.gen(function* () {
        yield* addUser(sessionID, "first")
        const history = yield* PromptHistory.make(sessionID)
        yield* history.refresh()
        const user = yield* addUser(sessionID, "second")
        const assistant = yield* addAssistant(sessionID, user.info.id)
        yield* addText(sessionID, assistant.id, "reply")

        const refreshed = yield* history.refresh()

        expect(refreshed.map((item) => item.info.id)).toEqual([expect.any(String), user.info.id, assistant.id])
      }),
    ),
  )

  it.instance("appends consecutive provider attempts sharing the latest user parent", () =>
    withSession(({ sessionID }) =>
      Effect.gen(function* () {
        const user = yield* addUser(sessionID, "continue")
        const history = yield* PromptHistory.make(sessionID)
        yield* history.refresh()
        yield* addAssistant(sessionID, user.info.id)
        yield* history.refresh()
        const retry = yield* addAssistant(sessionID, user.info.id)

        const refreshed = yield* measure(history.refresh())

        expect(refreshed.value.at(-1)?.info.id).toBe(retry.id)
        expect(refreshed.counts).toEqual({ sequence: 1, pages: 0, hydrated: 1, ranges: 1 })
      }),
    ),
  )

  it.instance("closes initial load race and returns committed update once", () =>
    withSession(({ session, sessionID }) =>
      Effect.gen(function* () {
        const tail = yield* addUser(sessionID, "before")
        const loaded = yield* Deferred.make<void>()
        const release = yield* Deferred.make<void>()
        const history = yield* PromptHistory.make(sessionID, {
          load: (load) =>
            load.pipe(
              Effect.tap(() => Deferred.succeed(loaded, undefined)),
              Effect.tap(() => Deferred.await(release)),
            ),
        })
        const refresh = yield* history.refresh().pipe(Effect.forkChild)
        yield* Deferred.await(loaded)
        yield* session.updatePart({ ...tail.part, text: "during" })
        yield* Deferred.succeed(release, undefined)

        const result = yield* Fiber.join(refresh)

        expect(result).toHaveLength(1)
        expect(result[0]?.parts).toContainEqual({ ...tail.part, text: "during" })
      }),
    ),
  )

  it.instance("ignores verified session-only updates", () =>
    withSession(({ session, sessionID }) =>
      Effect.gen(function* () {
        yield* addUser(sessionID, "stable")
        const history = yield* PromptHistory.make(sessionID)
        const initial = yield* history.refresh()
        yield* session.setTitle({ sessionID, title: "changed" })

        const refreshed = yield* measure(history.refresh())

        expect(refreshed.value).toEqual(initial)
        expect(refreshed.counts).toEqual({ sequence: 1, pages: 0, hydrated: 0, ranges: 1 })
      }),
    ),
  )

  it.instance("caller mutation cannot poison retained source", () =>
    withSession(({ sessionID }) =>
      Effect.gen(function* () {
        yield* addUser(sessionID, "pristine")
        const history = yield* PromptHistory.make(sessionID)
        const first = yield* history.refresh()
        const part = first[0]?.parts[0]
        if (part?.type === "text") part.text = "poisoned"
        first.splice(0, 1)

        const second = yield* history.refresh()

        expect(second).toHaveLength(1)
        expect(second[0]?.parts[0]).toMatchObject({ type: "text", text: "pristine" })
      }),
    ),
  )

  it.instance("a new run starts with a cold full load", () =>
    withSession(({ sessionID }) =>
      Effect.gen(function* () {
        yield* addUser(sessionID, "restart")
        const firstRun = yield* PromptHistory.make(sessionID)
        yield* firstRun.refresh()

        const secondRun = yield* PromptHistory.make(sessionID)
        const refreshed = yield* measure(secondRun.refresh())

        expect(refreshed.counts.sequence).toBe(2)
        expect(refreshed.counts.pages).toBeGreaterThan(0)
      }),
    ),
  )
})

registerFallbackTests()
registerOrderingTests()

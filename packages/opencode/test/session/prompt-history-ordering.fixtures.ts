import { describe, expect } from "bun:test"
import { SessionV1 } from "@opencode-ai/core/v1/session"
import { Effect } from "effect"
import { MessageV2 } from "@/session/message-v2"
import { PromptHistory } from "@/session/prompt-history"
import { PartID, type SessionID } from "@/session/schema"
import { Session } from "@/session/session"
import { testEffect } from "../lib/effect"
import { addAssistant, addText, addUser, layer, measure, withSession } from "./prompt-history.fixtures"

const it = testEffect(layer)

export function registerOrderingTests() {
  describe("PromptHistory ordering validation", () => {
    it.instance("falls back when cached summary assistant becomes ordinary", () =>
      withSession(({ session, sessionID }) =>
        Effect.gen(function* () {
          const seeded = yield* seedHistory(sessionID, { compaction: true, summary: true })
          const history = yield* PromptHistory.make(sessionID)
          yield* history.refresh()
          yield* session.updateMessage({ ...seeded.summary, summary: false })

          expect(yield* history.refresh()).toEqual(yield* MessageV2.filterCompactedEffect(sessionID))
          expect((yield* measure(history.refresh())).counts.pages).toBeGreaterThan(0)
        }),
      ),
    )

    it.instance("falls back when cached compaction part becomes ordinary", () =>
      withSession(({ session, sessionID }) =>
        Effect.gen(function* () {
          const seeded = yield* seedHistory(sessionID, { compaction: true, summary: true })
          const history = yield* PromptHistory.make(sessionID)
          yield* history.refresh()
          yield* session.updatePart({
            id: seeded.marker.id,
            sessionID,
            messageID: seeded.boundary.info.id,
            type: "text",
            text: "ordinary",
          } satisfies SessionV1.TextPart)

          expect(yield* history.refresh()).toEqual(yield* MessageV2.filterCompactedEffect(sessionID))
          expect((yield* measure(history.refresh())).counts.pages).toBeGreaterThan(0)
        }),
      ),
    )

    it.instance("falls back when summary appears after target capture before hydration", () =>
      withSession(({ session, sessionID }) =>
        Effect.gen(function* () {
          const seeded = yield* seedHistory(sessionID, { compaction: true, summary: false })
          const history = yield* PromptHistory.make(sessionID, {
            readRange: (read) =>
              read.pipe(Effect.tap(() => session.updateMessage({ ...seeded.summary, summary: true }))),
          })
          yield* history.refresh()
          yield* session.updateMessage({ ...seeded.summary, finish: "updated" })

          expect(yield* history.refresh()).toEqual(yield* MessageV2.filterCompactedEffect(sessionID))
          expect((yield* measure(history.refresh())).counts.pages).toBeGreaterThan(0)
        }),
      ),
    )

    it.instance("falls back when compaction appears after target capture before hydration", () =>
      withSession(({ session, sessionID }) =>
        Effect.gen(function* () {
          const seeded = yield* seedHistory(sessionID, { compaction: false, summary: true })
          const history = yield* PromptHistory.make(sessionID, {
            readRange: (read) =>
              read.pipe(
                Effect.tap(() =>
                  session.updatePart({
                    id: seeded.marker.id,
                    sessionID,
                    messageID: seeded.boundary.info.id,
                    type: "compaction",
                    auto: true,
                    tail_start_id: seeded.before.info.id,
                  } satisfies SessionV1.CompactionPart),
                ),
              ),
          })
          yield* history.refresh()
          yield* session.updatePart({ ...seeded.marker, text: "target" })

          expect(yield* history.refresh()).toEqual(yield* MessageV2.filterCompactedEffect(sessionID))
          expect((yield* measure(history.refresh())).counts.pages).toBeGreaterThan(0)
        }),
      ),
    )

    it.instance("falls back when existing part ID is published under another message owner", () =>
      withSession(({ session, sessionID }) =>
        Effect.gen(function* () {
          const first = yield* addUser(sessionID, "first")
          const second = yield* addUser(sessionID, "second")
          const history = yield* PromptHistory.make(sessionID)
          yield* history.refresh()
          yield* session.updatePart({
            ...first.part,
            messageID: second.info.id,
            text: "moved",
          })

          expect(yield* history.refresh()).toEqual(yield* MessageV2.filterCompactedEffect(sessionID))
          expect((yield* measure(history.refresh())).counts.pages).toBeGreaterThan(0)
        }),
      ),
    )
  })
}

const seedHistory = Effect.fn("PromptHistoryTest.seedHistory")(function* (
  sessionID: SessionID,
  options: { readonly compaction: boolean; readonly summary: boolean },
) {
  const session = yield* Session.Service
  const before = yield* addUser(sessionID, "before")
  const assistant = yield* addAssistant(sessionID, before.info.id)
  yield* addText(sessionID, assistant.id, "before reply")
  const boundary = yield* addUser(sessionID, "boundary")
  const marker = options.compaction
    ? ({
        id: PartID.ascending(),
        sessionID,
        messageID: boundary.info.id,
        type: "compaction",
        auto: true,
        tail_start_id: before.info.id,
      } satisfies SessionV1.CompactionPart)
    : ({
        id: PartID.ascending(),
        sessionID,
        messageID: boundary.info.id,
        type: "text",
        text: "ordinary",
      } satisfies SessionV1.TextPart)
  yield* session.updatePart(marker)
  const summary = yield* addAssistant(sessionID, boundary.info.id, { summary: options.summary })
  yield* addText(sessionID, summary.id, "summary")
  yield* addUser(sessionID, "after")
  return { before, boundary, marker, summary }
})

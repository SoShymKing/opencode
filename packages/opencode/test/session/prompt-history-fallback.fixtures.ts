import { describe, expect } from "bun:test"
import { EventV2 } from "@opencode-ai/core/event"
import { SessionV1 } from "@opencode-ai/core/v1/session"
import { SessionEvent } from "@opencode-ai/schema/session-event"
import { SessionMessage } from "@opencode-ai/schema/session-message"
import { DateTime, Effect } from "effect"
import { MessageV2 } from "@/session/message-v2"
import { PromptHistory } from "@/session/prompt-history"
import { MessageID, PartID, type SessionID } from "@/session/schema"
import { Session } from "@/session/session"
import { testEffect } from "../lib/effect"
import { addAssistant, addText, addUser, layer, measure, withSession } from "./prompt-history.fixtures"

const it = testEffect(layer)

export function registerFallbackTests() {
  describe("PromptHistory fallback", () => {
    const fallback = (
      name: string,
      arrange: (input: {
        readonly session: Session.Interface
        readonly sessionID: SessionID
        readonly user: { readonly info: SessionV1.User; readonly part: SessionV1.TextPart }
        readonly assistant: SessionV1.Assistant
      }) => Effect.Effect<void, never, Session.Service>,
    ) =>
      it.instance(name, () =>
        withSession(({ session, sessionID }) =>
          Effect.gen(function* () {
            const user = yield* addUser(sessionID, "user")
            const assistant = yield* addAssistant(sessionID, user.info.id)
            yield* addText(sessionID, assistant.id, "assistant")
            let invalidations = 0
            const history = yield* PromptHistory.make(sessionID, { invalidate: () => invalidations++ })
            yield* history.refresh()
            yield* arrange({ session, sessionID, user, assistant })

            expect(yield* history.refresh()).toEqual(yield* MessageV2.filterCompactedEffect(sessionID))
            expect((yield* measure(history.refresh())).counts.pages).toBeGreaterThan(0)
            expect(invalidations).toBe(2)
          }),
        ),
      )

    fallback("falls back on message removal and stays disabled", ({ session, sessionID, assistant }) =>
      session.removeMessage({ sessionID, messageID: assistant.id }).pipe(Effect.asVoid),
    )
    fallback("falls back on part removal", ({ session, sessionID, user }) =>
      session.removePart({ sessionID, messageID: user.info.id, partID: user.part.id }).pipe(Effect.asVoid),
    )
    fallback("falls back on revert-bearing session update", ({ session, sessionID, user }) =>
      session.setRevert({ sessionID, revert: { messageID: user.info.id }, summary: undefined }),
    )
    fallback("falls back on compaction part", ({ session, sessionID, user }) =>
      session
        .updatePart({ id: PartID.ascending(), sessionID, messageID: user.info.id, type: "compaction", auto: true })
        .pipe(Effect.asVoid),
    )
    fallback("falls back on summary assistant", ({ session, assistant }) =>
      session.updateMessage({ ...assistant, summary: true }).pipe(Effect.asVoid),
    )
    fallback("falls back on non-tail insertion", ({ sessionID }) =>
      addUser(sessionID, "old", { id: MessageID.ascending("msg_00000000000000000000000000"), time: 0 }).pipe(
        Effect.asVoid,
      ),
    )
    fallback("falls back on ambiguous assistant parent", ({ sessionID }) =>
      addAssistant(sessionID, MessageID.ascending("msg_missing_parent")).pipe(Effect.asVoid),
    )

    it.instance("falls back on current session.next event", () =>
      withSession(({ sessionID }) =>
        Effect.gen(function* () {
          yield* addUser(sessionID, "legacy")
          const history = yield* PromptHistory.make(sessionID)
          yield* history.refresh()
          yield* (yield* EventV2.Service).publish(SessionEvent.AgentSwitched, {
            timestamp: DateTime.makeUnsafe(0),
            sessionID,
            messageID: SessionMessage.ID.make("msg_current"),
            agent: "test",
          })

          expect(yield* history.refresh()).toEqual(yield* MessageV2.filterCompactedEffect(sessionID))
          expect((yield* measure(history.refresh())).counts.pages).toBeGreaterThan(0)
        }),
      ),
    )

    it.instance("falls back on a sequence gap", () =>
      withSession(({ session, sessionID }) =>
        Effect.gen(function* () {
          const user = yield* addUser(sessionID, "zero")
          const history = yield* PromptHistory.make(sessionID, {
            readRange: (read) => read.pipe(Effect.map((events) => events.slice(1))),
          })
          yield* history.refresh()
          yield* session.updatePart({ ...user.part, text: "one" })
          yield* session.updatePart({ ...user.part, text: "two" })

          expect(yield* history.refresh()).toEqual(yield* MessageV2.filterCompactedEffect(sessionID))
          expect((yield* measure(history.refresh())).counts.pages).toBeGreaterThan(0)
        }),
      ),
    )

    it.instance("falls back on durable decode failure", () =>
      withSession(({ session, sessionID }) =>
        Effect.gen(function* () {
          const user = yield* addUser(sessionID, "zero")
          const history = yield* PromptHistory.make(sessionID, {
            readRange: () => Effect.die(new TypeError("malformed durable event")),
          })
          yield* history.refresh()
          yield* session.updatePart({ ...user.part, text: "one" })

          expect(yield* history.refresh()).toEqual(yield* MessageV2.filterCompactedEffect(sessionID))
        }),
      ),
    )

    it.instance("falls back on event missing required message ID", () =>
      withSession(({ session, sessionID }) =>
        Effect.gen(function* () {
          const user = yield* addUser(sessionID, "zero")
          const history = yield* PromptHistory.make(sessionID, {
            readRange: (read) =>
              read.pipe(
                Effect.map((events) => events.map((event) => ({ ...event, data: { sessionID } }))),
              ),
          })
          yield* history.refresh()
          yield* session.updatePart({ ...user.part, text: "one" })

          expect(yield* history.refresh()).toEqual(yield* MessageV2.filterCompactedEffect(sessionID))
        }),
      ),
    )

    it.instance("falls back when delta exceeds internal cap", () =>
      withSession(({ session, sessionID }) =>
        Effect.gen(function* () {
          const user = yield* addUser(sessionID, "zero")
          const history = yield* PromptHistory.make(sessionID)
          yield* history.refresh()
          yield* Effect.forEach(
            Array.from({ length: 300 }, (_, index) => index),
            (index) => session.updatePart({ ...user.part, text: `${index}` }),
            { discard: true },
          )

          expect(yield* history.refresh()).toEqual(yield* MessageV2.filterCompactedEffect(sessionID))
          expect((yield* measure(history.refresh())).counts.pages).toBeGreaterThan(0)
        }),
      ),
    )

    it.instance("falls back on sequence regression", () =>
      withSession(({ sessionID }) =>
        Effect.gen(function* () {
          yield* addUser(sessionID, "stable")
          const history = yield* PromptHistory.make(sessionID)
          yield* history.refresh()
          yield* (yield* EventV2.Service).remove(sessionID)

          expect(yield* history.refresh()).toEqual(yield* MessageV2.filterCompactedEffect(sessionID))
          expect((yield* measure(history.refresh())).counts.pages).toBeGreaterThan(0)
        }),
      ),
    )
  })
}

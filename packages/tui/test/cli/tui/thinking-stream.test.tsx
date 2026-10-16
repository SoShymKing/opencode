/** @jsxImportSource @opentui/solid */
import { expect, test } from "bun:test"
import { unwrap } from "solid-js/store"
import type { SessionMessageAssistant } from "@opencode/client"
import { createAppFixture } from "../../fixture/app"
import { waitForHighlightedFrame } from "../../fixture/frame"
import { directory, json, type FetchHandler } from "../../fixture/tui-client"
import { mount } from "../cmd/tui/sync-fixture"

const sessionID = "ses_thinking_stream"
const owner = "msg_user"
const assistantID = "msg_assistant"
const user = { id: owner, type: "user", text: "Current input", time: { created: 1 } }

function assistant(count?: number): SessionMessageAssistant {
  return {
    id: assistantID, type: "assistant", agent: "build", model: { providerID: "test", id: "model" },
    content: [], time: { created: 2 }, streamEventCount: count,
  }
}

async function fixture(input: {
  count?: number
  mode?: "hide" | "show"
  reasoning?: string
  completed?: boolean
  pending?: boolean
  fetch?: FetchHandler
} = {}) {
  const message = {
    ...assistant(input.count),
    content: input.reasoning === undefined ? [] : [{ type: "reasoning", text: input.reasoning, time: {
      created: 2, ...(input.completed ? { completed: 3 } : {}),
    } }],
    time: { created: 2, ...(input.completed ? { completed: 3 } : {}) },
  }
  const pending = { id: "msg_pending", sessionID, type: "user", payload: { text: "Waiting input" },
    delivery: "queue", time: { created: 4 } }
  const setup = await createAppFixture({
    width: 100, height: 35,
    args: { sessionID },
    config: { animations: false, tabs: { mode: "off" }, session: { sidebar: "hide", grouping: "none", thinking: input.mode ?? "hide" } },
    fetch: async (url, request) => {
      const response = await input.fetch?.(url, request)
      if (response) return response
      if (url.pathname === `/api/session/${sessionID}`) return json({ data: {
        id: sessionID, title: "Stream fixture", projectID: "project", location: { directory }, agent: "build",
        model: assistant().model, cost: 0,
        tokens: { input: 0, output: 0, reasoning: 0, cache: { read: 0, write: 0 } }, time: { created: 1, updated: 3 },
      } })
      if (url.pathname === `/api/session/${sessionID}/message`) return json({ data: [message, user], cursor: {} })
      if (url.pathname === `/api/session/${sessionID}/inbox`) return json({ data: input.pending ? [pending] : [] })
      if (url.pathname === "/api/session/active") return json({ data: { [sessionID]: { type: "running", status: {
        type: "busy", activity: { userMessageID: owner, model: input.count === undefined ? "none" : "receiving", streamEventCount: input.count },
      } } } })
    },
  })
  try {
    await setup.ready
    await setup.waitForFrame((frame) => frame.includes("Current input") && frame.includes("stream"))
    return setup
  } catch (error) {
    await setup[Symbol.asyncDispose]()
    throw error
  }
}

test.each([0, 1, 11, undefined])("native prompt renders current count %s with an empty public summary", async (count) => {
  await using setup = await fixture({ count })
  const label = count === undefined ? "streams unavailable" : `${count} ${count === 1 ? "stream" : "streams"}`
  const frame = await setup.waitForFrame((frame) => frame.includes(label))
  expect(frame).toContain(label)
  expect(frame).not.toContain("Thought:")
})

test.each(["hide", "show"] as const)("native prompt keeps the empty-summary counter visible in %s mode", async (mode) => {
  await using setup = await fixture({ count: 11, mode, reasoning: "" })
  const frame = setup.captureCharFrame()
  expect(frame).toContain("11 streams")
  expect(frame.match(/11 streams/g)).toHaveLength(1)
})

test("native refresh keeps live assistant counts and separate pending input", async () => {
  const snapshot = Promise.withResolvers<Response>()
  const requested = Promise.withResolvers<void>()
  let reads = 0
  const pending = { id: "msg_native_pending", sessionID, type: "user", delivery: "queue", payload: { text: "pending" }, time: { created: 3 } }
  using setup = await mount((url) => {
    if (url.pathname.endsWith("/inbox")) return json({ data: [pending] })
    if (!url.pathname.endsWith("/message")) return
    if (++reads === 1) return json({ data: [assistant(11), user], cursor: {} })
    requested.resolve()
    return snapshot.promise
  })
  await setup.data.session.message.sync(sessionID)
  await setup.data.session.pending.sync(sessionID)
  setup.data.session.message.invalidate(sessionID)
  const refresh = setup.data.session.message.sync(sessionID)
  await requested.promise
  await setup.emit({ id: "evt_stream", created: 4, type: "session.status", data: {
    sessionID, status: { type: "busy", activity: { userMessageID: owner, model: "receiving", streamEventCount: 12 } },
  } })
  snapshot.resolve(json({ data: [{ ...assistant(11), content: [{ type: "text", text: "restored" }] }, user], cursor: {} }))
  await refresh
  expect(setup.data.session.pending.list(sessionID).map((item) => item.id)).toContain(pending.id)
  expect(setup.data.session.input.has(sessionID, pending.id)).toBe(true)
  expect(unwrap(setup.data.session.message.get(sessionID, assistantID))).toMatchObject({ streamEventCount: 12, content: [{ type: "text", text: "restored" }] })
})

test("durable pending input stays visible in the native queue without becoming delivered history", async () => {
  await using setup = await fixture({ pending: true, count: 11 })
  const frame = await setup.waitForFrame((frame) => frame.includes("Waiting input"))
  expect(frame).toContain("Waiting input")
  expect(frame).toContain("11 streams")
})

test("live activity before an assistant exists retains its owner and distinguishes zero from unknown", async () => {
  await using setup = await fixture({ count: 11, pending: true, fetch: (url) => {
    if (url.pathname.endsWith("/message")) return json({ data: [user], cursor: {} })
  } })
  expect(setup.captureCharFrame()).toContain("11 streams")
  setup.events.emit({ id: "evt_reset", created: 5, type: "session.status", data: { sessionID, status: {
    type: "busy", activity: { userMessageID: "msg_next", model: "preparing", streamEventCount: 0 },
  } } })
  expect(await setup.waitForFrame((frame) => frame.includes("0 streams"))).toContain("Waiting input")
  setup.events.emit({ id: "evt_unknown", created: 6, type: "session.status", data: { sessionID, status: {
    type: "busy", activity: { userMessageID: "msg_next", model: "none" },
  } } })
  expect(await setup.waitForFrame((frame) => frame.includes("streams unavailable"))).not.toContain("11 streams")
})

test("text deltas do not invent raw provider event counts", async () => {
  await using setup = await fixture({ fetch: (url) => {
    if (url.pathname.endsWith("/message")) return json({ data: [{ ...assistant(), content: [{ type: "text", text: "" }] }, user], cursor: {} })
  } })
  setup.events.emit({ id: "evt_text", created: 4, type: "session.text.delta", data: { sessionID, assistantMessageID: assistantID, ordinal: 0, delta: "Visible text delta" } })
  const frame = await waitForHighlightedFrame(setup, (frame) => frame.includes("Visible text delta"))
  expect(frame).toContain("streams unavailable")
  expect(frame).not.toContain("1 stream")
})

test("new native attempts reset counts and ignore an old owner's terminal status", async () => {
  await using setup = await fixture({ count: 11 })
  setup.events.emit({ id: "evt_next", created: 5, type: "session.status", data: { sessionID, status: {
    type: "busy", activity: { userMessageID: "msg_next", model: "preparing", streamEventCount: 0 },
  } } })
  await setup.waitForFrame((frame) => frame.includes("0 streams"))
  setup.events.emit({ id: "evt_old", created: 6, type: "session.status", data: { sessionID, status: {
    type: "idle", terminal: { userMessageID: owner, reason: "completed" },
  } } })
  const frame = await setup.waitForFrame((frame) => frame.includes("0 streams"))
  expect(frame).not.toContain("11 streams")
})

test("completed and earlier assistant counts stay out of the current prompt label", async () => {
  await using setup = await fixture({ completed: true, fetch: (url) => {
    if (url.pathname.endsWith("/message")) return json({ data: [{ ...assistant(11), time: { created: 2, completed: 3 } }, user], cursor: {} })
  } })
  expect(setup.captureCharFrame()).toContain("streams unavailable")
  expect(setup.captureCharFrame()).not.toContain("11 streams")
})

test("live native activity updates the empty-summary label from zero to eleven", async () => {
  await using setup = await fixture({ count: 0, reasoning: "" })
  expect(setup.captureCharFrame()).toContain("0 streams")
  setup.events.emit({ id: "evt_count", created: 4, type: "session.status", data: { sessionID, status: {
    type: "busy", activity: { userMessageID: owner, model: "receiving", streamEventCount: 11 },
  } } })
  expect(await setup.waitForFrame((frame) => frame.includes("11 streams"))).not.toContain("0 streams")
})

test.each([false, true])("native reasoning preserves title and completion copy when done=%s", async (done) => {
  await using setup = await fixture({ count: 11, reasoning: "**Inspecting**", completed: done, pending: true })
  const frame = await setup.waitForFrame((frame) => frame.includes("Inspecting"))
  expect(frame).toContain(done ? "Thought" : "Thinking")
  expect(frame.match(/11 streams/g)).toHaveLength(1)
  expect(frame.match(/Inspecting/g)).toHaveLength(1)
  expect(frame).toContain("Waiting input")
})

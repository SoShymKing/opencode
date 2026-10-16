/** @jsxImportSource @opentui/solid */
import { expect, test } from "bun:test"
import type { SessionMessageInfo } from "@opencode/client"
import { For } from "solid-js"
import { useData } from "../../../../src/context/data"
import { json, mount, wait } from "./sync-fixture"

const sessionID = "ses_hydration_race"
const messageID = "msg_hydration_race"
const assistant = {
  id: messageID,
  type: "assistant",
  agent: "build",
  model: { id: "model", providerID: "test" },
  content: [{ type: "text", text: "" }],
  time: { created: 1 },
} satisfies SessionMessageInfo

function Messages() {
  const data = useData()
  return <For each={data.session.message.list(sessionID)}>{(message) => (
    <text>{message.type === "assistant" ? message.content.flatMap((part) => part.type === "text" ? [part.text] : []).join("") : message.id}</text>
  )}</For>
}

test.each(["error", "EOF"])("SSE %s reconnects and cleanup cancels the final native stream once", async (failure) => {
  let requests = 0
  let cancelled = 0
  const closed = Promise.withResolvers<void>()
  const setup = await mount((url, request) => {
    if (url.pathname !== "/api/event") return
    requests++
    if (requests === 2) request.signal.addEventListener("abort", () => {
      cancelled++
      closed.resolve()
    }, { once: true })
    return new Response(new ReadableStream<Uint8Array>({
      start(controller) {
        if (requests === 1) {
          if (failure === "error") controller.error(new Error("SSE overflow"))
          if (failure === "EOF") controller.close()
          return
        }
        controller.enqueue(new TextEncoder().encode('data: {"type":"server.connected","data":{}}\n\n'))
      },
    }), { headers: { "content-type": "text/event-stream" } })
  })
  try {
    expect(requests).toBe(2)
    expect(setup.client.connection.status()).toBe("connected")
  } finally {
    setup[Symbol.dispose]()
  }
  await closed.promise
  expect(cancelled).toBe(1)
})

test.each([false, true])("late native active snapshots cannot overwrite live activity with another reconnect=%s", async (reconnect) => {
  const snapshot = Promise.withResolvers<Response>()
  const requested = Promise.withResolvers<void>()
  let reads = 0
  using setup = await mount((url) => {
    if (url.pathname !== "/api/session/active") return
    if (++reads === 2) {
      requested.resolve()
      return snapshot.promise
    }
    return json({ data: {} })
  })
  await setup.emit({ id: "evt_reconnect", type: "server.connected", data: {} })
  await requested.promise
  if (reconnect) await setup.emit({ id: "evt_reconnect_again", type: "server.connected", data: {} })
  await setup.emit({ id: "evt_status", created: 3, type: "session.status", data: {
    sessionID, status: { type: "busy", activity: { userMessageID: "msg_user", model: "receiving", streamEventCount: 211 } },
  } })
  snapshot.resolve(json({ data: { [sessionID]: { type: "running", status: {
    type: "busy", activity: { model: "receiving", streamEventCount: 200 },
  } } } }))
  await setup.frame()
  expect(setup.data.session.current(sessionID)).toMatchObject({ activity: { streamEventCount: 211 } })
  expect(setup.data.session.status(sessionID)).toBe("running")
})

test("forced native hydration waits for older inflight work then fetches fresh history", async () => {
  const old = Promise.withResolvers<Response>()
  const requested = Promise.withResolvers<void>()
  let reads = 0
  using setup = await mount((url) => {
    if (url.pathname !== `/api/session/${sessionID}/message`) return
    if (++reads === 1) {
      requested.resolve()
      return old.promise
    }
    return json({ data: [{ ...assistant, content: [{ type: "text", text: "fresh" }] }], cursor: {} })
  }, Messages)
  const initial = setup.data.session.message.sync(sessionID)
  await requested.promise
  setup.data.session.message.invalidate(sessionID)
  const forced = setup.data.session.message.sync(sessionID)
  old.resolve(json({ data: [assistant], cursor: {} }))
  await Promise.all([initial, forced])
  expect(reads).toBe(2)
  expect(await setup.frame()).toContain("fresh")
})

test("reconnect invalidates cached native history and restores quiet activity", async () => {
  let reads = 0
  let connected = false
  using setup = await mount((url) => {
    if (url.pathname === `/api/session/${sessionID}/message`)
      return json({ data: [{ ...assistant, content: [{ type: "text", text: String(++reads) }] }], cursor: {} })
    if (url.pathname === "/api/session/active") return json({ data: connected ? { [sessionID]: { type: "running", status: {
      type: "busy", activity: { userMessageID: "msg_user", model: "receiving", streamEventCount: 200 },
    } } } : {} })
  }, Messages)
  await setup.data.session.message.sync(sessionID)
  connected = true
  setup.client.connection.internal.resync("fixture reconnect")
  await wait(() => setup.client.connection.status() !== "connected")
  await wait(() => setup.client.connection.status() === "connected")
  await setup.data.session.message.sync(sessionID)
  await wait(() => setup.data.session.current(sessionID)?.type === "busy")
  expect(reads).toBe(2)
  expect(await setup.frame()).toContain("2")
  expect(setup.data.session.current(sessionID)).toMatchObject({ activity: { streamEventCount: 200 } })
})

test.each(["during", "before", "orphan"] as const)("native hydration preserves text streamed %s the read", async (timing) => {
  const snapshot = Promise.withResolvers<Response>()
  const requested = Promise.withResolvers<void>()
  let reads = 0
  using setup = await mount((url) => {
    if (url.pathname !== `/api/session/${sessionID}/message`) return
    if (++reads === 1) return json({ data: timing === "orphan" ? [] : [assistant], cursor: {} })
    requested.resolve()
    return snapshot.promise
  }, Messages)
  await setup.data.session.message.sync(sessionID)
  if (timing === "before") await setup.emit({ id: "evt_before", created: 2, type: "session.text.delta", data: {
    sessionID, assistantMessageID: messageID, ordinal: 0, delta: "visible streamed content",
  } })
  if (timing === "before") expect(await setup.frame()).toContain("visible streamed content")
  setup.data.session.message.invalidate(sessionID)
  const hydrate = setup.data.session.message.sync(sessionID)
  await requested.promise
  if (timing !== "before") await setup.emit({ id: "evt_during", created: 3, type: "session.text.delta", data: {
    sessionID, assistantMessageID: messageID, ordinal: 0, delta: timing === "orphan" ? "ignored until part exists" : "visible live content",
  } })
  snapshot.resolve(json({ data: [{ ...assistant, content: [{ type: "text", text: timing === "orphan" ? "hydrated" : "" }] }], cursor: {} }))
  await hydrate
  expect(await setup.frame()).toContain(timing === "orphan" ? "hydrated" : timing === "before" ? "visible streamed content" : "visible live content")
})

test("native history retains server creation-time and ID order", async () => {
  const ids = ["msg_z", "msg_b", "msg_m", "msg_a"]
  using setup = await mount((url) => {
    if (url.pathname.endsWith("/message")) return json({ data: ids.toReversed().map((id, index) => ({
      ...assistant, id, time: { created: [30, 20, 20, 10][index] },
    })), cursor: {} })
  })
  await setup.data.session.message.sync(sessionID)
  expect(setup.data.session.message.list(sessionID).map((message) => message.id)).toEqual(ids)
})

test("native bounded hydration retains live rows and the older-page cursor", async () => {
  const snapshot = Promise.withResolvers<Response>()
  const requested = Promise.withResolvers<void>()
  const limits: Array<string | null> = []
  using setup = await mount((url) => {
    if (!url.pathname.endsWith("/message")) return
    limits.push(url.searchParams.get("limit"))
    requested.resolve()
    return snapshot.promise
  })
  const hydrate = setup.data.session.message.sync(sessionID)
  await requested.promise
  await setup.emit({ id: "evt_live", created: 30, type: "session.step.started", durable: { aggregateID: sessionID, seq: 1, version: 1 },
    data: { sessionID, assistantMessageID: "msg_live", started: 30, agent: "build", model: assistant.model } })
  snapshot.resolve(json({ data: Array.from({ length: 20 }, (_, index) => ({
    ...assistant, id: `msg_${20 - index}`, time: { created: 20 - index },
  })), cursor: { next: "older" } }))
  await hydrate
  expect(limits).toEqual(["20"])
  expect(setup.data.session.message.list(sessionID)).toHaveLength(21)
  expect(setup.data.session.message.list(sessionID).at(-1)?.id).toBe("msg_live")
  expect(setup.data.session.message.more(sessionID)).toBe(true)
})

test.each(["message", "session"] as const)("native %s removed during hydration does not regain stale content", async (removed) => {
  const snapshot = Promise.withResolvers<Response>()
  const requested = Promise.withResolvers<void>()
  let reads = 0
  using setup = await mount((url) => {
    if (!url.pathname.endsWith("/message")) return
    if (++reads === 1) return json({ data: [assistant], cursor: {} })
    requested.resolve()
    return snapshot.promise
  })
  await setup.data.session.message.sync(sessionID)
  setup.data.session.message.invalidate(sessionID)
  const hydrate = setup.data.session.message.sync(sessionID)
  await requested.promise
  if (removed === "message") await setup.emit({ id: "evt_revert", created: 3, type: "session.revert.committed",
    durable: { aggregateID: sessionID, seq: 1, version: 1 }, data: { sessionID, to: messageID } })
  if (removed === "session") await setup.emit({ id: "evt_removed", created: 3, type: "session.deleted",
    durable: { aggregateID: sessionID, seq: 1, version: 2 }, data: { sessionID } })
  snapshot.resolve(json({ data: [assistant], cursor: {} }))
  await hydrate
  expect(setup.data.session.message.list(sessionID)).toEqual([])
  expect(setup.data.session.message.get(sessionID, messageID)).toBeUndefined()
})

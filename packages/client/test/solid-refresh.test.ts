import { expect, test } from "bun:test"
import { createRoot } from "solid-js"
import { unwrap } from "solid-js/store"
import { createData, settleMs, type CreateDataInput, type DataCurrentStatus } from "../src/solid"
import { OpenCode, type OpenCodeEvent, type SessionInfo, type SessionMessageInfo } from "../src/promise"
import { createHistoryRevisions } from "../src/solid/history"

test("config reads and update refreshes are opt-in", async () => {
  const listeners = new Set<Parameters<CreateDataInput["event"]["listen"]>[0]>()
  const requests: string[] = []
  const location = { directory: "/project" }
  let model = "provider/first"
  const api = OpenCode.make({
    baseUrl: "http://opencode.local",
    fetch: async (input, init) => {
      const request = input instanceof Request ? input : new Request(input, init)
      const url = new URL(request.url)
      requests.push(url.pathname)
      if (url.pathname === "/api/location") return Response.json(location)
      if (url.pathname === "/api/config") return Response.json([{ type: "document", info: { model } }])
      if (url.pathname === "/api/mcp/resource")
        return Response.json({ location, data: { resources: [], templates: [] } })
      return Response.json({ location, data: [] })
    },
  })
  const setup = createRoot((dispose) => ({
    data: createData({
      api: () => api,
      directory: location.directory,
      event: {
        on: () => () => {},
        listen(handler) {
          listeners.add(handler)
          return () => listeners.delete(handler)
        },
      },
    }),
    dispose,
  }))
  const event: OpenCodeEvent = { id: "evt_config", created: 1, type: "config.updated", location, data: {} }
  try {
    await setup.data.location.sync()
    listeners.forEach((listener) => listener({ name: event.type, details: event }))
    expect(requests).not.toContain("/api/config")

    await setup.data.location.config.sync()
    expect(requests.filter((path) => path === "/api/config")).toHaveLength(1)
    model = "provider/second"
    listeners.forEach((listener) => listener({ name: event.type, details: event }))
    await setup.data.location.config.sync()
    expect(requests.filter((path) => path === "/api/config")).toHaveLength(2)
    expect(setup.data.location.config.list()).toEqual([{ type: "document", info: { model } }])
  } finally {
    setup.dispose()
  }
})

test("event refreshes report failures, remain retryable, and preserve explicit read errors", async () => {
  const listeners = new Set<Parameters<CreateDataInput["event"]["listen"]>[0]>()
  const reported = Promise.withResolvers<unknown>()
  const errors: unknown[] = []
  const state = { offline: true, requests: 0 }
  const session: SessionInfo = {
    id: "ses_refresh_failure",
    projectID: "project",
    cost: 0,
    tokens: { input: 0, output: 0, reasoning: 0, cache: { read: 0, write: 0 } },
    time: { created: 0, updated: 0, idle: 2 },
    location: { directory: "/project" },
  }
  const api = OpenCode.make({
    baseUrl: "http://opencode.local",
    fetch: async () => {
      state.requests++
      if (state.offline) throw new TypeError("Failed to fetch")
      return Response.json({ data: { ...session, title: "Recovered" } })
    },
  })
  const setup = createRoot((dispose) => ({
    data: createData({
      api: () => api,
      directory: "/project",
      event: {
        on: () => () => {},
        listen(handler) {
          listeners.add(handler)
          return () => listeners.delete(handler)
        },
      },
      onError(error) {
        errors.push(error)
        reported.resolve(error)
      },
    }),
    dispose,
  }))
  const event: OpenCodeEvent = {
    id: "evt_refresh_failure",
    created: 2,
    type: "session.viewed",
    durable: { aggregateID: session.id, seq: 1, version: 1 },
    data: { sessionID: session.id, idle: 2 },
  }
  try {
    setup.data.session.remember(session)
    setup.data.session.invalidate(session.id)
    await expect(setup.data.session.sync(session.id)).rejects.toThrow("Transport")
    expect(errors).toEqual([])
    listeners.forEach((listener) => listener({ name: event.type, details: event }))
    expect(String(await reported.promise)).toContain("Transport")
    expect(errors).toHaveLength(1)
    expect(setup.data.session.get(session.id)?.title).toBeUndefined()
    state.offline = false
    listeners.forEach((listener) => listener({ name: event.type, details: event }))
    await setup.data.session.sync(session.id)
    expect(setup.data.session.get(session.id)?.title).toBe("Recovered")
    expect(state.requests).toBe(3)
  } finally {
    setup.dispose()
  }
})

test.each(["reconnecting", "disposed"] as const)("background reads respect %s owners", async (mode) => {
  const listeners = new Set<Parameters<CreateDataInput["event"]["listen"]>[0]>()
  const pending = Promise.withResolvers<Response>()
  const errors: unknown[] = []
  const state = { requests: 0 }
  const api = OpenCode.make({
    baseUrl: "http://opencode.local",
    fetch: () => {
      state.requests++
      return pending.promise
    },
  })
  const setup = createRoot((dispose) => ({
    data: createData({
      api: () => api,
      directory: "/project",
      event: {
        on: () => () => {},
        listen(handler) {
          listeners.add(handler)
          return () => listeners.delete(handler)
        },
      },
      connection: { status: () => (mode === "reconnecting" ? "reconnecting" : "connected") },
      onError: (error) => errors.push(error),
    }),
    dispose,
  }))
  const event: OpenCodeEvent = {
    id: "evt_refresh_owner",
    type: "command.updated",
    location: { directory: "/project" },
    data: {},
  }
  listeners.forEach((listener) => listener({ name: event.type, details: event }))
  if (mode === "reconnecting") {
    expect(state.requests).toBe(0)
    setup.dispose()
    return
  }
  const joined = setup.data.location.command.sync()
  setup.dispose()
  pending.reject(new TypeError("Failed to fetch"))
  await expect(joined).rejects.toThrow("Transport")
  expect(state.requests).toBe(1)
  expect(errors).toEqual([])
})

test("a burst of mcp.status.changed events refetches the server list once it settles", async () => {
  const listeners = new Set<Parameters<CreateDataInput["event"]["listen"]>[0]>()
  const requests: string[] = []
  const location = { directory: "/project" }
  const api = OpenCode.make({
    baseUrl: "http://opencode.local",
    fetch: async (input, init) => {
      const request = input instanceof Request ? input : new Request(input, init)
      requests.push(new URL(request.url).pathname)
      return Response.json({ location, data: [] })
    },
  })
  const setup = createRoot((dispose) => ({
    data: createData({
      api: () => api,
      directory: location.directory,
      event: {
        on: () => () => {},
        listen(handler) {
          listeners.add(handler)
          return () => listeners.delete(handler)
        },
      },
    }),
    dispose,
  }))
  try {
    await setup.data.location.mcp.server.sync()
    expect(requests.filter((path) => path === "/api/mcp")).toHaveLength(1)
    for (const server of ["a", "b", "c", "d", "e"]) {
      const event: OpenCodeEvent = { id: `evt_${server}`, created: 1, type: "mcp.status.changed", location, data: { server } }
      listeners.forEach((listener) => listener({ name: event.type, details: event }))
    }
    expect(requests.filter((path) => path === "/api/mcp")).toHaveLength(1)
    await new Promise((resolve) => setTimeout(resolve, settleMs * 2))
    expect(requests.filter((path) => path === "/api/mcp")).toHaveLength(2)
  } finally {
    setup.dispose()
  }
})

test("invalidations that land before a queued reload starts are absorbed by it", async () => {
  const pending: Array<PromiseWithResolvers<Response>> = []
  const location = { directory: "/project" }
  const api = OpenCode.make({
    baseUrl: "http://opencode.local",
    fetch: () => {
      const deferred = Promise.withResolvers<Response>()
      pending.push(deferred)
      return deferred.promise
    },
  })
  const setup = createRoot((dispose) => ({
    data: createData({
      api: () => api,
      directory: location.directory,
      event: { on: () => () => {}, listen: () => () => {} },
    }),
    dispose,
  }))
  try {
    const first = setup.data.location.agent.sync()
    expect(pending).toHaveLength(1)
    setup.data.location.agent.invalidate()
    const second = setup.data.location.agent.sync()
    setup.data.location.agent.invalidate()
    const third = setup.data.location.agent.sync()
    setup.data.location.agent.invalidate()
    expect(pending).toHaveLength(1)
    pending[0].resolve(Response.json({ location, data: [] }))
    await first
    await Promise.resolve()
    expect(pending).toHaveLength(2)
    pending[1].resolve(Response.json({ location, data: [{ name: "reviewer" }] }))
    await Promise.all([second, third])
    expect(pending).toHaveLength(2)
    await setup.data.location.agent.sync()
    expect(pending).toHaveLength(2)
  } finally {
    setup.dispose()
  }
})

test.each(["asked", "duplicate", "replied", "ack"] as const)("permission snapshot preserves concurrent %s facts", async (mode) => {
  const response = Promise.withResolvers<Response>()
  const setup = historyFixture(async (request) => request.method === "GET" ? response.promise : new Response(null, { status: 204 }))
  const permission = { id: "per_race", sessionID: "ses_race", action: "bash", resources: ["test"] }
  const pending = mode === "asked" || mode === "duplicate"
  try {
    if (mode === "duplicate") setup.emit({ id: "evt_first", type: "permission.asked", data: permission })
    const reading = setup.data.session.permission.sync(permission.sessionID)
    if (pending) setup.emit({ id: "evt_ask", type: "permission.asked", data: permission })
    if (mode === "replied") setup.emit({ id: "evt_reply", type: "permission.replied", data: { sessionID: permission.sessionID, requestID: permission.id, reply: "once" } })
    if (mode === "ack") await setup.data.session.permission.reply({ sessionID: permission.sessionID, requestID: permission.id, reply: "once" })
    response.resolve(Response.json({ data: pending ? [] : [permission] }))
    await reading
    expect(unwrap(setup.data.session.permission.list(permission.sessionID))).toEqual(pending ? [permission] : [])
    expect(setup.data.session.permission.sessions()).toEqual(pending ? [permission.sessionID] : [])
  } finally { setup.dispose() }
})

test.each(["local", "global"] as const)("form snapshot preserves concurrent facts in %s scope", async (scope) => {
  for (const mode of ["created", "duplicate", "replied", "cancelled", "ack", "reply-ack"] as const) {
    const response = Promise.withResolvers<Response>()
    const setup = historyFixture(async (request) => request.method === "GET" ? response.promise : new Response(null, { status: 204 }))
    const location = { directory: "/project" }
    const other = { directory: "/other" }
    const form = { id: "form_race", sessionID: scope === "global" ? "global" : "ses_race", title: "Request", fields: [{ type: "string" as const, key: "answer" }] }
    const pending = mode === "created" || mode === "duplicate"
    try {
      if (scope === "global") setup.emit({ id: "evt_other", type: "form.created", location: other, data: { form } })
      if (mode === "duplicate") setup.emit({ id: "evt_first", type: "form.created", location, data: { form } })
      const reading = setup.data.session.form.sync(form.sessionID, location)
      if (pending) setup.emit({ id: "evt_create", type: "form.created", location, data: { form } })
      if (mode === "replied") setup.emit({ id: "evt_reply", type: "form.replied", location, data: { id: form.id, sessionID: form.sessionID, answer: {} } })
      if (mode === "cancelled") setup.emit({ id: "evt_cancel", type: "form.cancelled", location, data: { id: form.id, sessionID: form.sessionID } })
      if (mode === "ack") await setup.data.session.form.cancel({ sessionID: form.sessionID, formID: form.id }, location)
      if (mode === "reply-ack") await setup.data.session.form.reply({ sessionID: form.sessionID, formID: form.id, answer: {} }, location)
      response.resolve(Response.json({ location, data: pending ? [] : [form] }))
      await reading
      expect(unwrap(setup.data.session.form.list(form.sessionID, location))).toEqual(pending ? [{ ...form, ...(scope === "global" ? { location } : {}) }] : [])
      expect(setup.data.session.form.sessions()).toEqual(pending && scope === "local" ? [form.sessionID] : [])
      if (scope === "global") expect(unwrap(setup.data.session.form.list("global", other))).toEqual([{ ...form, location: other }])
    } finally { setup.dispose() }
  }
})

test("unscoped global settlement removes stale forms from every in-flight location", async () => {
  const response = Promise.withResolvers<void>()
  const form = { id: "form_global", sessionID: "global", title: "Request", fields: [{ type: "string" as const, key: "answer" }] }
  const locations = [{ directory: "/project" }, { directory: "/other" }]
  const setup = historyFixture(async (request) => {
    await response.promise
    return Response.json({ location: { directory: new URL(request.url).searchParams.get("directory") }, data: [form] })
  })
  try {
    const reading = locations.map((location) => setup.data.session.form.sync("global", location))
    setup.emit({ id: "evt_cancel", type: "form.cancelled", data: { id: form.id, sessionID: "global" } })
    response.resolve()
    await Promise.all(reading)
    for (const location of locations) expect(setup.data.session.form.list("global", location)).toEqual([])
  } finally { setup.dispose() }
})

test.each(["reconnect", "evict", "dispose"] as const)("request snapshots reject late publication after %s", async (mode) => {
  const response = Promise.withResolvers<void>()
  const setup = historyFixture(async (request) => {
    const pathname = new URL(request.url).pathname
    if (pathname === "/api/session/active") return Response.json({ data: {} })
    if (pathname === "/api/project") return Response.json([])
    if (pathname === "/api/location") return Response.json({ directory: "/project" })
    if (!pathname.includes("/session/")) return Response.json({ location: { directory: "/project" }, data: [] })
    await response.promise
    return Response.json({ data: [pathname.includes("permission")
      ? { id: "per_stale", sessionID: "ses_race", action: "bash", resources: [] }
      : { id: "form_stale", sessionID: "ses_race", title: "Request", fields: [{ type: "string", key: "answer" }] }] })
  })
  try {
    const permission = setup.data.session.permission.sync("ses_race")
    const form = setup.data.session.form.sync("ses_race")
    if (mode === "reconnect") setup.emit({ type: "server.connected", data: {} })
    if (mode === "evict") setup.data.session.evict("ses_race")
    if (mode === "dispose") setup.dispose()
    response.resolve()
    await Promise.all([permission, form])
    expect(setup.data.session.permission.list("ses_race")).toBeUndefined()
    expect(setup.data.session.form.list("ses_race")).toBeUndefined()
  } finally { setup.dispose() }
})

function historyFixture(fetch: (request: Request) => Promise<Response>) {
  const listeners = new Set<Parameters<CreateDataInput["event"]["listen"]>[0]>()
  const api = OpenCode.make({
    baseUrl: "http://opencode.local",
    fetch: (input, init) => fetch(input instanceof Request ? input : new Request(input, init)),
  })
  return createRoot((dispose) => ({
    data: createData({
      api: () => api,
      directory: "/project",
      event: {
        on: () => () => {},
        listen(handler) {
          listeners.add(handler)
          return () => listeners.delete(handler)
        },
      },
    }),
    emit: (event: OpenCodeEvent) => listeners.forEach((listener) => listener({ name: event.type, details: event })),
    dispose,
  }))
}

test.each(["activity", "ended", "restart"] as const)("live %s counts hydrate fetched content without promoting pending input", async (source) => {
  const response = Promise.withResolvers<Response>()
  const assistant: SessionMessageInfo = {
    id: "msg_assistant", type: "assistant", agent: "build", model: { providerID: "provider", id: "model" },
    content: [], time: { created: 2 }, streamEventCount: 11,
    ...(source === "restart" ? { error: { type: "unknown", message: "previous attempt" }, finish: "error" } : {}),
  }
  const user: SessionMessageInfo = { id: "msg_owner", type: "user", text: "owner", time: { created: 1 } }
  const pending = { id: "msg_pending", sessionID: "ses_history", type: "user", delivery: "queue", payload: { text: "pending" }, time: { created: 3 } }
  let reads = 0
  const setup = historyFixture(async (request) => {
    if (new URL(request.url).pathname.endsWith("/inbox")) return Response.json({ data: [pending] })
    return ++reads === 1 ? Response.json({ data: [assistant, user], cursor: {} }) : response.promise
  })
  try {
    await setup.data.session.message.sync("ses_history")
    await setup.data.session.pending.sync("ses_history")
    setup.data.session.message.invalidate("ses_history")
    const reading = setup.data.session.message.sync("ses_history")
    if (source === "activity") setup.emit({ id: "evt_count", created: 4, type: "session.status", data: {
      sessionID: "ses_history", status: { type: "busy", activity: { userMessageID: user.id, model: "receiving", streamEventCount: 12 } },
    } })
    if (source === "ended") setup.emit({ id: "evt_end", created: 4, type: "session.step.ended",
      durable: { aggregateID: "ses_history", seq: 1, version: 1 }, data: {
        sessionID: "ses_history", assistantMessageID: assistant.id, streamEventCount: 12, finish: "stop", cost: 0,
        tokens: { input: 0, output: 0, reasoning: 0, cache: { read: 0, write: 0 } },
      },
    })
    if (source === "restart") setup.emit({ id: "evt_restart", created: 4, type: "session.step.started",
      durable: { aggregateID: "ses_history", seq: 1, version: 1 }, data: {
        sessionID: "ses_history", assistantMessageID: assistant.id, started: 4, agent: "build", model: assistant.model,
      },
    })
    response.resolve(Response.json({ data: [{ ...assistant, content: [{ type: "text", text: "restored" }] }, user], cursor: {} }))
    await reading
    const restored = unwrap(setup.data.session.message.get("ses_history", assistant.id))
    expect(restored).toMatchObject({
      streamEventCount: source === "restart" ? 11 : 12, content: [{ type: "text", text: "restored" }],
    })
    if (source === "restart" && restored?.type === "assistant") {
      expect(restored.error).toBeUndefined()
      expect(restored.finish).toBeUndefined()
    }
    expect(setup.data.session.pending.list("ses_history").map((item) => item.id)).toEqual([pending.id])
    expect(setup.data.session.input.has("ses_history", pending.id)).toBe(true)
  } finally {
    setup.dispose()
  }
})

test.each(["text", "reasoning"] as const)("pre-read %s deltas survive durable checkpoints until ended", async (kind) => {
  const assistant: SessionMessageInfo = {
    id: "msg_stream", type: "assistant", agent: "build", model: { providerID: "provider", id: "model" },
    content: [{ type: kind, text: "" }], time: { created: 1 },
  }
  let checkpoint = ""
  let reads = 0
  const setup = historyFixture(async () => Response.json({
    data: [{ ...assistant, agent: ++reads === 1 ? "build" : "review", content: [{ type: kind, text: checkpoint }] }], cursor: {},
  }))
  const read = async () => {
    setup.data.session.message.invalidate("ses_stream")
    await setup.data.session.message.sync("ses_stream")
    return setup.data.session.message.get("ses_stream", assistant.id)
  }
  try {
    await read()
    setup.emit({ type: `session.${kind}.delta`, data: {
      sessionID: "ses_stream", assistantMessageID: assistant.id, delta: "visible streamed content",
    } })
    expect(await read()).toMatchObject({ agent: "review", content: [{ text: "visible streamed content" }] })
    checkpoint = "visible"
    expect(await read()).toMatchObject({ content: [{ text: "visible streamed content" }] })
    setup.emit({
      id: "evt_end", created: 2, type: `session.${kind}.ended`,
      durable: { aggregateID: "ses_stream", seq: 1, version: 1 },
      data: { sessionID: "ses_stream", assistantMessageID: assistant.id, text: "final" },
    })
    checkpoint = "authoritative replacement"
    expect(await read()).toMatchObject({ content: [{ text: checkpoint }] })
  } finally {
    setup.dispose()
  }
})

test.each(["replacement", "completed", "omitted"] as const)("pre-read streaming does not override an authoritative %s", async (mode) => {
  let reads = 0
  const setup = historyFixture(async () => Response.json({ data: [{
    id: "msg_stream", type: "assistant", agent: "build", model: { providerID: "provider", id: "model" },
    content: ++reads === 1 ? [{ type: "text", text: "" }] : mode === "omitted" ? [] : [{ type: "text", text: mode === "completed" ? "" : "replacement" }],
    time: { created: 1, ...(reads > 1 && mode === "completed" ? { completed: 2 } : {}) },
  }], cursor: {} }))
  try {
    await setup.data.session.message.sync("ses_stream")
    setup.emit({ type: "session.text.delta", data: {
      sessionID: "ses_stream", assistantMessageID: "msg_stream", delta: "live",
    } })
    setup.data.session.message.invalidate("ses_stream")
    await setup.data.session.message.sync("ses_stream")
    expect(setup.data.session.message.get("ses_stream", "msg_stream")).toMatchObject({
      content: mode === "omitted" ? [] : [{ text: mode === "completed" ? "" : "replacement" }],
    })
  } finally {
    setup.dispose()
  }
})

test("status rollback restores its previous value only once", () => {
  const setup = historyFixture(async () => Response.json({}))
  try {
    setup.data.session.setStatus("ses_owner", "running")
    const rollback = setup.data.session.setStatus("ses_owner", "idle")
    rollback()
    expect(setup.data.session.status("ses_owner")).toBe("running")
    setup.data.session.setStatus("ses_owner", "idle")
    rollback()
    expect(setup.data.session.status("ses_owner")).toBe("idle")
  } finally {
    setup.dispose()
  }
})

test("status rollback restores an initially absent status", () => {
  const setup = historyFixture(async () => Response.json({}))
  try {
    const rollback = setup.data.session.setStatus("ses_owner", "running")
    rollback()
    expect(setup.data.session.status("ses_owner")).toBe("idle")
    expect(setup.data.session.active()).toEqual([])
  } finally {
    setup.dispose()
  }
})

test.each(["snapshot", "deleted"] as const)("%s removal invalidates an idle status rollback", async (writer) => {
  const setup = historyFixture(async (request) => {
    const pathname = new URL(request.url).pathname
    if (pathname === "/api/session/active") return Response.json({ data: {} })
    if (pathname === "/api/project") return Response.json([])
    if (pathname === "/api/location") return Response.json({ directory: "/project" })
    return Response.json({ location: { directory: "/project" }, data: [] })
  })
  try {
    setup.data.session.setStatus("ses_owner", "running")
    const rollback = setup.data.session.setStatus("ses_owner", "idle")
    if (writer === "snapshot") {
      setup.emit({ type: "server.connected", data: {} })
      await new Promise<void>((resolve) => setImmediate(resolve))
    }
    if (writer === "deleted") setup.emit({
      id: "evt_deleted", created: 1, type: "session.deleted",
      durable: { aggregateID: "ses_owner", seq: 1, version: 2 }, data: { sessionID: "ses_owner" },
    })
    rollback()
    expect(setup.data.session.status("ses_owner")).toBe("idle")
    expect(setup.data.session.active()).toEqual([])
  } finally {
    setup.dispose()
  }
})

test.each(["local", "status", "execution", "snapshot", "detailed snapshot"] as const)(
  "same-value %s status supersedes an optimistic rollback",
  async (writer) => {
    const setup = historyFixture(async (request) => {
      const pathname = new URL(request.url).pathname
      if (pathname === "/api/session/active") return Response.json({ data: {
        ses_owner: { type: "running", ...(writer === "detailed snapshot" ? { status: { type: "busy" } } : {}) },
      } })
      if (pathname === "/api/project") return Response.json([])
      if (pathname === "/api/location") return Response.json({ directory: "/project" })
      return Response.json({ location: { directory: "/project" }, data: [] })
    })
    try {
      const rollback = setup.data.session.setStatus("ses_owner", "running")
      if (writer === "local") setup.data.session.setStatus("ses_owner", "running")
      if (writer === "status") setup.emit({
        id: "evt_status", created: 1, type: "session.status",
        data: { sessionID: "ses_owner", status: { type: "busy" } },
      })
      if (writer === "execution") setup.emit({
        id: "evt_execution", created: 1, type: "session.execution.started",
        durable: { aggregateID: "ses_owner", seq: 1, version: 1 }, data: { sessionID: "ses_owner" },
      })
      if (writer === "snapshot" || writer === "detailed snapshot") {
        setup.emit({ type: "server.connected", data: {} })
        await new Promise<void>((resolve) => setImmediate(resolve))
      }
      rollback()
      expect(setup.data.session.status("ses_owner")).toBe("running")
    } finally {
      setup.dispose()
    }
  },
)

test.each([undefined, "oldest"])("bounded refresh retains older pages and cursor %s", async (cursor) => {
  const requests: Array<string | null> = []
  const setup = historyFixture(async (request) => {
    const page = new URL(request.url).searchParams.get("cursor")
    requests.push(page)
    return Response.json({
      data: [{ id: page ? "msg_old" : "msg_new", type: "user", text: "history", time: { created: page ? 1 : 2 } }],
      cursor: { next: page ? cursor : "older" },
    })
  })
  try {
    await setup.data.session.message.sync("ses_history")
    await setup.data.session.message.loadMore("ses_history")
    setup.data.session.message.invalidate("ses_history")
    await setup.data.session.message.sync("ses_history")
    expect(setup.data.session.message.list("ses_history").map((item) => item.id)).toEqual(["msg_old", "msg_new"])
    expect(setup.data.session.message.more("ses_history")).toBe(cursor !== undefined)
    await setup.data.session.message.loadMore("ses_history")
    expect(requests).toEqual([null, "older", null, ...(cursor ? [cursor] : [])])
  } finally {
    setup.dispose()
  }
})

test("history refresh preserves live text and tool progress while accepting untouched metadata", async () => {
  const response = Promise.withResolvers<Response>()
  const assistant: SessionMessageInfo = {
    id: "msg_assistant", type: "assistant", agent: "build", model: { providerID: "provider", id: "model" },
    content: [
      { type: "text", text: "before" },
      { type: "tool", id: "tool", name: "bash", time: { created: 1 }, state: { status: "running", input: {}, metadata: {} } },
    ],
    time: { created: 1 },
  }
  let calls = 0
  const setup = historyFixture(async () => ++calls === 1 ? Response.json({ data: [assistant], cursor: {} }) : response.promise)
  try {
    await setup.data.session.message.sync("ses_history")
    setup.data.session.message.invalidate("ses_history")
    const reading = setup.data.session.message.sync("ses_history")
    setup.emit({ type: "session.text.delta", data: { sessionID: "ses_history", assistantMessageID: assistant.id, delta: " live" } })
    setup.emit({ type: "session.tool.progress", data: { sessionID: "ses_history", assistantMessageID: assistant.id, id: "tool", metadata: { progress: 2 } } })
    response.resolve(Response.json({ data: [{ ...assistant, agent: "review", cost: 2 }], cursor: {} }))
    await reading
    expect(setup.data.session.message.get("ses_history", assistant.id)).toMatchObject({
      agent: "review", cost: 2,
      content: [{ type: "text", text: "before live" }, { type: "tool", state: { metadata: { progress: 2 } } }],
    })
  } finally {
    setup.dispose()
  }
})

test("current activity accepts count resets and ignores an old owner's terminal status", async () => {
  const response = Promise.withResolvers<Response>()
  const messages: SessionMessageInfo[] = [
    { id: "msg_owner", type: "user", text: "current", time: { created: 1 } },
    { id: "msg_assistant", type: "assistant", agent: "build", model: { providerID: "provider", id: "model" }, content: [], time: { created: 2 } },
  ]
  let calls = 0
  const setup = historyFixture(async () => ++calls === 1 ? Response.json({ data: messages.toReversed(), cursor: {} }) : response.promise)
  const status = (value: DataCurrentStatus) => setup.emit({
    id: "evt_status", created: 3, type: "session.status", data: { sessionID: "ses_history", status: value },
  })
  try {
    await setup.data.session.message.sync("ses_history")
    status({ type: "busy", activity: { userMessageID: "msg_owner", model: "receiving", streamEventCount: 200 } })
    setup.data.session.message.invalidate("ses_history")
    const reading = setup.data.session.message.sync("ses_history")
    status({ type: "busy", activity: { userMessageID: "msg_owner", model: "preparing", streamEventCount: 0 } })
    status({ type: "idle", terminal: { userMessageID: "msg_previous", reason: "completed" } })
    response.resolve(Response.json({ data: messages.toReversed(), cursor: {} }))
    await reading
    expect(setup.data.session.status("ses_history")).toBe("running")
    expect(setup.data.session.current("ses_history")).toEqual({ type: "busy", activity: {
      userMessageID: "msg_owner", model: "preparing", streamEventCount: 0,
    } })
    expect(setup.data.session.message.get("ses_history", "msg_assistant")).toMatchObject({ streamEventCount: 0 })
  } finally {
    setup.dispose()
  }
})

test("history reconciliation retains a content replacement over stale assistant parts", () => {
  const history = createHistoryRevisions()
  const assistant: SessionMessageInfo = {
    id: "msg_assistant", type: "assistant", agent: "build", model: { providerID: "provider", id: "model" },
    content: [{ type: "text", text: "removed" }], time: { created: 1 },
  }
  const revision = history.capture()
  history.touch("ses_history", assistant.id)
  expect(history.merge("ses_history", revision, [assistant], [{ ...assistant, content: [] }])).toEqual([
    { ...assistant, content: [] },
  ])
})

test.each(["archived", "restored"] as const)("native %s refreshes metadata without evicting history", async (action) => {
  const info: SessionInfo = {
    id: "ses_history", projectID: "project", cost: 0,
    tokens: { input: 0, output: 0, reasoning: 0, cache: { read: 0, write: 0 } },
    location: { directory: "/project" }, time: { created: 1, updated: 2, ...(action === "restored" ? { archived: 2 } : {}) },
  }
  const updated = { ...info, time: { created: 1, updated: 9, ...(action === "archived" ? { archived: 8 } : {}) } }
  let reads = 0
  const setup = historyFixture(async (request) => {
    if (new URL(request.url).pathname.endsWith("/message")) return Response.json({
      data: [{ id: "msg_retained", type: "user", text: "retained", time: { created: 1 } }], cursor: { next: "older" },
    })
    reads++
    return Response.json({ data: updated })
  })
  try {
    setup.data.session.remember(info)
    await setup.data.session.message.sync(info.id)
    const messages = setup.data.session.message.list(info.id)
    setup.emit({
      id: "evt_archive", created: 7, type: `session.${action}`,
      durable: { aggregateID: info.id, seq: 1, version: 1 }, data: { sessionID: info.id },
    })
    await setup.data.session.sync(info.id)
    expect(reads).toBe(1)
    expect(setup.data.session.get(info.id)?.time).toEqual(updated.time)
    expect(setup.data.session.message.list(info.id)).toBe(messages)
    expect(setup.data.session.message.more(info.id)).toBe(true)
  } finally {
    setup.dispose()
  }
})

test("reconnect rejects old history and a forced refresh waits then fetches again", async () => {
  const response = Promise.withResolvers<Response>()
  const started = Promise.withResolvers<void>()
  let reads = 0
  const setup = historyFixture(async (request) => {
    const pathname = new URL(request.url).pathname
    if (pathname.endsWith("/message")) {
      if (++reads === 1) return response.promise
      started.resolve()
      return Response.json({ data: [{ id: "msg_fresh", type: "user", text: "fresh", time: { created: 2 } }], cursor: {} })
    }
    if (pathname === "/api/session/active") return Response.json({ data: {} })
    if (pathname === "/api/project") return Response.json([])
    if (pathname === "/api/location") return Response.json({ directory: "/project" })
    return Response.json({ location: { directory: "/project" }, data: [] })
  })
  try {
    const first = setup.data.session.message.sync("ses_history")
    setup.emit({ type: "server.connected", data: {} })
    const second = setup.data.session.message.sync("ses_history")
    expect(reads).toBe(1)
    response.resolve(Response.json({ data: [{ id: "msg_stale", type: "user", text: "stale", time: { created: 1 } }], cursor: {} }))
    await first
    expect(setup.data.session.message.list("ses_history")).toEqual([])
    await started.promise
    await second
    expect(reads).toBe(2)
    expect(setup.data.session.message.list("ses_history").map((item) => item.id)).toEqual(["msg_fresh"])
  } finally {
    setup.dispose()
  }
})

test("active hydration preserves a live count reset and restores a later reconnect snapshot", async () => {
  const response = Promise.withResolvers<Response>()
  const status = (count: number): DataCurrentStatus => ({
    type: "busy", activity: { userMessageID: "msg_owner", model: "receiving", streamEventCount: count },
  })
  let reads = 0
  const setup = historyFixture(async (request) => {
    const pathname = new URL(request.url).pathname
    if (pathname === "/api/session/active") {
      if (++reads === 1) return response.promise
      if (reads === 3) return Response.json({ data: {} })
      return Response.json({ data: { ses_history: { type: "running", status: status(7) } } })
    }
    if (pathname === "/api/project") return Response.json([])
    if (pathname === "/api/location") return Response.json({ directory: "/project" })
    return Response.json({ location: { directory: "/project" }, data: [] })
  })
  try {
    setup.emit({ type: "server.connected", data: {} })
    setup.emit({ id: "evt_reset", created: 1, type: "session.status", data: { sessionID: "ses_history", status: status(0) } })
    response.resolve(Response.json({ data: { ses_history: { type: "running", status: status(200) } } }))
    await new Promise<void>((resolve) => setImmediate(resolve))
    expect(setup.data.session.current("ses_history")).toEqual(status(0))
    setup.emit({ type: "server.connected", data: {} })
    await new Promise<void>((resolve) => setImmediate(resolve))
    expect(setup.data.session.current("ses_history")).toEqual(status(7))
    expect(reads).toBe(2)
    setup.emit({ type: "server.connected", data: {} })
    await new Promise<void>((resolve) => setImmediate(resolve))
    expect(setup.data.session.current("ses_history")).toEqual({ type: "idle" })
  } finally {
    setup.dispose()
  }
})

test.each(["ended", "failed"] as const)("a step %s event stores the core's final raw count", async (outcome) => {
  const setup = historyFixture(async () => Response.json({ data: [{
    id: "msg_assistant", type: "assistant", agent: "build", model: { providerID: "provider", id: "model" },
    content: [], time: { created: 1 },
  }], cursor: {} }))
  const event = { id: "evt_final", created: 2, durable: { aggregateID: "ses_history", seq: 1, version: 1 as const } }
  const data = { sessionID: "ses_history", assistantMessageID: "msg_assistant", streamEventCount: 37 }
  try {
    await setup.data.session.message.sync(data.sessionID)
    if (outcome === "ended") setup.emit({ ...event, type: "session.step.ended", data: {
      ...data, finish: "stop", cost: 0, tokens: { input: 0, output: 0, reasoning: 0, cache: { read: 0, write: 0 } },
    } })
    if (outcome === "failed") setup.emit({ ...event, type: "session.step.failed", data: {
      ...data, error: { type: "unknown", message: "failed" },
    } })
    expect(setup.data.session.message.get(data.sessionID, data.assistantMessageID)).toMatchObject({
      streamEventCount: 37, time: { completed: 2 },
    })
  } finally {
    setup.dispose()
  }
})

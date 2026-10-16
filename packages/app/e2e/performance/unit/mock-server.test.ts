import { expect, test } from "bun:test"
import type { Page, Route } from "@playwright/test"
import { createMockServerHandler, mockOpenCodeServer } from "../../utils/mock-server"
import type { OpenCodeEvent } from "@opencode/client/promise"

test("serves an empty config document list for composer defaults", async () => {
  const server = createMockServerHandler({
    provider: {},
    directory: "C:/OpenCode",
    project: {},
    sessions: [],
    pageMessages: () => ({ items: [] }),
  })

  try {
    const response = await server.handler(
      new Request("http://localhost/api/config?location%5Bdirectory%5D=C%3A%2FOpenCode"),
    )

    expect(response.status).toBe(200)
    expect(await response.json()).toEqual([])
  } finally {
    await server.dispose()
  }
})

test("applies message latency after a list response gate is released", async () => {
  const events: string[] = []
  const gate = Promise.withResolvers<void>()
  const started = Promise.withResolvers<void>()
  let handler: ((route: Route) => Promise<void>) | undefined

  const page = {
    addInitScript: () => Promise.resolve(),
    on: () => page,
    route: (_url: string, callback: (route: Route) => Promise<void>) => {
      handler = callback

      return Promise.resolve()
    },
  } as unknown as Page

  await mockOpenCodeServer(page, {
    provider: {},
    directory: "C:/OpenCode",
    project: {},
    sessions: [{ id: "session" }],
    messageDelay: 25,
    beforeMessagesResponse: () => {
      events.push("before")
      started.resolve()

      return gate.promise
    },
    onMessages: (request) => events.push(request.phase),
    pageMessages: () => {
      events.push("page")

      return { items: [] }
    },
  })

  const response = handler!({
    request: () => ({
      url: () => "http://127.0.0.1:4096/api/session/session/message",
      method: () => "GET",
      headers: () => ({}),
      postDataBuffer: () => null,
    }),
    fulfill: () => {
      events.push("fulfill")

      return Promise.resolve()
    },
  } as unknown as Route)

  await started.promise
  expect(events).toEqual(["start", "before"])

  const released = performance.now()
  gate.resolve()
  await response
  expect(performance.now() - released).toBeGreaterThanOrEqual(20)
  expect(events).toEqual(["start", "before", "page", "end", "fulfill"])
})

test("routes requests through the HttpApi contract", async () => {
  const connected = Promise.withResolvers<{ integrationID: string; body: unknown }>()
  let handler: ((route: Route) => Promise<void>) | undefined

  const page = {
    addInitScript: () => Promise.resolve(),
    on: () => page,
    route: (_url: string, callback: (route: Route) => Promise<void>) => {
      handler = callback

      return Promise.resolve()
    },
  } as unknown as Page

  await mockOpenCodeServer(page, {
    provider: {},
    directory: "C:/OpenCode",
    project: {},
    sessions: [],
    pageMessages: () => ({ items: [] }),
    onConnectKey: connected.resolve,
  })

  const body = Buffer.from(JSON.stringify({ key: "secret" }))
  let status: number | undefined
  await handler!({
    request: () => ({
      url: () => "http://127.0.0.1:4096/api/integration/anthropic/connect/key",
      method: () => "POST",
      headers: () => ({ "content-type": "application/json" }),
      postDataBuffer: () => body,
    }),
    fulfill: (response: Parameters<Route["fulfill"]>[0]) => {
      status = response?.status

      return Promise.resolve()
    },
  } as unknown as Route)

  expect(status).toBe(204)
  expect(await connected.promise).toEqual({ integrationID: "anthropic", body: { key: "secret" } })
})

test("archive and restore mutate native inventory without deleting transcript or pending input", async () => {
  const events: OpenCodeEvent[] = []
  const messages = [{ id: "msg_archive_user", type: "user" as const, text: "Keep history", time: { created: 1 } }]
  const inbox = [{ id: "inb_archive_pending", sessionID: "ses_archive", type: "user", delivery: "steer", payload: { text: "Keep pending" }, time: { created: 2 } }]
  const server = createMockServerHandler({
    provider: {}, directory: "C:/OpenCode", project: {},
    sessions: [{ id: "ses_archive", time: { created: 1, updated: 2 } }],
    pageMessages: () => ({ items: messages }), inbox,
  }, (batch) => events.push(...batch))
  try {
    const archive = await server.handler(new Request("http://localhost/api/session/ses_archive/archive", { method: "POST" }))
    expect(archive.status).toBe(204)
    const archived = await server.handler(new Request("http://localhost/api/session?archived=true&updatedAfter=2"))
    expect(await archived.json()).toMatchObject({ data: [{ id: "ses_archive", time: { archived: expect.any(Number) } }] })
    const hidden = await server.handler(new Request("http://localhost/api/session?archived=false"))
    expect(await hidden.json()).toMatchObject({ data: [] })
    const restore = await server.handler(new Request("http://localhost/api/session/ses_archive/restore", { method: "POST" }))
    expect(restore.status).toBe(204)
    const visible = await server.handler(new Request("http://localhost/api/session?archived=false"))
    expect(await visible.json()).toMatchObject({ data: [{ id: "ses_archive" }] })
    const history = await server.handler(new Request("http://localhost/api/session/ses_archive/message"))
    expect(await history.json()).toMatchObject({ data: messages })
    const pending = await server.handler(new Request("http://localhost/api/session/ses_archive/inbox"))
    expect(await pending.json()).toEqual({ data: inbox })
    expect(events.map((event) => event.type)).toEqual(["session.archived", "session.restored"])
  } finally {
    await server.dispose()
  }
})

test("active snapshot preserves raw provider counts without deriving text deltas", async () => {
  const server = createMockServerHandler({
    provider: {}, directory: "C:/OpenCode", project: {}, sessions: [], pageMessages: () => ({ items: [] }),
    sessionStatus: { ses_count: { type: "busy", activity: { userMessageID: "msg_count", model: "receiving", streamEventCount: 200 } } },
  })
  try {
    const snapshot = await server.handler(new Request("http://localhost/api/session/active"))
    expect(await snapshot.json()).toEqual({ data: { ses_count: { type: "running", status: { type: "busy", activity: {
      userMessageID: "msg_count", model: "receiving", streamEventCount: 200,
    } } } } })
  } finally {
    await server.dispose()
  }
})

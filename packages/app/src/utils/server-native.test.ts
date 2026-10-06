import { describe, expect, test } from "bun:test"
import type { SessionMessage } from "@opencode-ai/schema/session-message"
import type { SessionInput } from "@opencode-ai/schema/session-input"
import type { SessionEvent } from "@opencode-ai/schema/session-event"
import { createApiForServer, createSdkForServer } from "./server"
import { createCompatibleApi } from "./server-compat"
import type { SessionActiveSnapshot } from "./server-compat"

const message = {
  type: "assistant",
  id: "msg_assistant",
  agent: "build",
  model: { id: "model", providerID: "provider" },
  time: { created: 10 },
  content: [
    { type: "text", id: "content_text", text: "result" },
    {
      type: "tool",
      id: "call_tool",
      name: "read",
      provider: { executed: true, metadata: { provider: { key: "value" } } },
      state: { status: "pending", input: "{" },
      time: { created: 11 },
    },
    {
      type: "tool",
      id: "call_completed",
      name: "read",
      state: {
        status: "completed",
        input: { path: "/repo/a.ts" },
        structured: { lines: 3 },
        content: [{ type: "text", text: "result" }],
        attachments: [{ uri: "file:///repo/a.ts", mime: "text/plain", name: "a.ts" }],
      },
      time: { created: 11, completed: 12 },
    },
  ],
} satisfies typeof SessionMessage.Message.Encoded

const admitted = {
  admittedSeq: 3,
  id: "msg_input",
  sessionID: "ses_test",
  prompt: { text: "input", files: [{ uri: "file:///repo/a.ts?start=2&end=4", mime: "text/plain" }] },
  delivery: "queue",
  timeCreated: 12,
} satisfies typeof SessionInput.Admitted.Encoded

const event = {
  id: "evt_test",
  type: "session.next.prompt.admitted",
  location: { directory: "/repo" },
  durable: { aggregateID: "ses_test", seq: 3, version: 1 },
  data: {
    timestamp: 12,
    sessionID: admitted.sessionID,
    messageID: admitted.id,
    prompt: admitted.prompt,
    delivery: admitted.delivery,
  },
} satisfies typeof SessionEvent.PromptAdmitted.Encoded

function setup(response: (request: Request) => Response | Promise<Response>) {
  const requests: Request[] = []
  const fetcher = Object.assign(
    async (input: string | URL | Request, init?: RequestInit) => {
      const request = new Request(input, init)
      requests.push(request)
      request.signal.throwIfAborted()
      return response(request)
    },
    { preconnect: globalThis.fetch.preconnect },
  )
  const server = { url: "http://boundary.test", username: "test", password: "fixture" }
  const current = createApiForServer({ server, fetch: fetcher })
  const api = createCompatibleApi({
    current,
    protocol: Promise.resolve("v2"),
    legacy: () => createSdkForServer({ server, fetch: fetcher }),
  })
  return { current, api, requests }
}

describe("native server boundary", () => {
  test("nests caller prompt attachments and returns admission without user fiction", async () => {
    const input = {
      sessionID: admitted.sessionID,
      id: admitted.id,
      text: admitted.prompt.text,
      files: [{
        uri: admitted.prompt.files[0].uri,
        name: "a.ts",
        description: "selection",
        mention: { start: 0, end: 5, text: "@a.ts" },
      }],
      agents: [{ name: "review", mention: { start: 6, end: 13, text: "@review" } }],
      delivery: "queue" as const,
      resume: false,
      agent: "build",
      model: { providerID: "provider", modelID: "model" },
      variant: "test",
      legacyParts: [{ type: "text" as const, text: "legacy" }],
    }
    const { api, requests } = setup((request) =>
      request.url.endsWith("/prompt")
        ? Response.json({ data: admitted })
        : new Response(null, { status: 204 }),
    )
    const result = await api.session.prompt(input, { headers: { "x-test": "request" } })
    expect(result).toEqual(admitted)
    expect(result).not.toHaveProperty("type")
    expect(requests.map((request) => new URL(request.url).pathname)).toEqual([
      `/api/session/${input.sessionID}/agent`,
      `/api/session/${input.sessionID}/model`,
      `/api/session/${input.sessionID}/prompt`,
    ])
    expect(await requests[0]?.json()).toEqual({ agent: input.agent })
    expect(await requests[1]?.json()).toEqual({
      model: { id: input.model.modelID, providerID: input.model.providerID, variant: input.variant },
    })
    expect(await requests[2]?.json()).toEqual({
      id: input.id,
      prompt: {
        text: input.text,
        files: input.files.map(({ mention, ...file }) => ({ ...file, source: mention })),
        agents: input.agents.map(({ mention, ...agent }) => ({ ...agent, source: mention })),
      },
      delivery: input.delivery,
      resume: input.resume,
    })
    expect(requests.every((request) => request.method === "POST")).toBe(true)
    expect(requests.every((request) => request.headers.get("Authorization") === `Basic ${btoa("test:fixture")}`)).toBe(true)
    expect(requests.every((request) => request.headers.get("x-test") === "request")).toBe(true)
  })

  test("keeps native history page and opaque cursors", async () => {
    const page = { data: [message], cursor: { previous: "older", next: "newer" } }
    const { current, requests } = setup(() => Response.json(page))
    expect(await current.message.list({ sessionID: "ses_test", limit: 17, cursor: "opaque" })).toEqual(page)
    expect(new URL(requests[0]?.url ?? "").searchParams.get("limit")).toBe("17")
    expect(new URL(requests[0]?.url ?? "").searchParams.get("cursor")).toBe("opaque")
  })

  test("rejects malformed history at the native read boundary", async () => {
    const { current } = setup(() => Response.json({
      data: [{ ...message, content: [{ type: "text", text: "missing id" }] }], cursor: {},
    }))
    await expect(current.message.list({ sessionID: "ses_test" })).rejects.toBeDefined()
  })

  test("unwraps single native message through the existing namespace", async () => {
    const { current } = setup(() => Response.json({ data: message }))
    expect(await current.session.message({ sessionID: "ses_test", messageID: message.id })).toEqual(message)
  })

  test("rejects malformed single messages", async () => {
    const { current } = setup(() => Response.json({ data: { ...message, time: { created: "invalid" } } }))
    await expect(current.session.message({ sessionID: "ses_test", messageID: message.id })).rejects.toBeDefined()
  })

  test("rejects the old pending-user shape as a native admission", async () => {
    const { current } = setup(() => Response.json({
      data: { ...admitted, prompt: undefined, type: "user", data: { text: admitted.prompt.text } },
    }))
    await expect(current.session.prompt({ sessionID: "ses_test", text: admitted.prompt.text })).rejects.toBeDefined()
  })

  test("decodes native SSE fields without changing timestamps", async () => {
    const { current, requests } = setup(() => new Response(`data: ${JSON.stringify(event)}\n\n`, {
      headers: { "content-type": "text/event-stream" },
    }))
    const events = await Array.fromAsync(current.event.subscribe())
    expect(events).toEqual([event])
    expect(requests[0]?.url).toBe("http://boundary.test/api/event")
    expect(requests[0]?.headers.get("Authorization")).toBe(`Basic ${btoa("test:fixture")}`)
  })

  test("rejects malformed native SSE fields", async () => {
    const { current } = setup(() => new Response(
      `data: ${JSON.stringify({ ...event, data: { ...event.data, timestamp: "invalid" } })}\n\n`,
      { headers: { "content-type": "text/event-stream" } },
    ))
    await expect(Array.fromAsync(current.event.subscribe())).rejects.toBeDefined()
  })

  test("keeps active map unwrapping", async () => {
    const data = { ses_test: { type: "running" as const } }
    const { current } = setup(() => Response.json({ data }))
    expect(await current.session.active()).toEqual(data)
  })

  test("preserves rich active statuses including an idle terminal in running presence", async () => {
    const data = {
      ses_test: { type: "running", status: { type: "busy", activity: {
        userMessageID: "msg_input", model: "receiving", streamEventCount: 200, lastStreamEventAt: 12,
      } } },
      ses_retry: { type: "running", status: { type: "retry", attempt: 2, message: "retry", next: 20,
        action: { reason: "auth", provider: "provider", title: "title", message: "message", label: "label", link: "https://fixture.test" },
      } },
      ses_ended: { type: "running", status: { type: "idle", terminal: { userMessageID: "msg_input", reason: "completed" } } },
      ses_bare: { type: "running" },
    } satisfies SessionActiveSnapshot
    const { api } = setup(() => Response.json({ data }))
    expect(await api.session.active()).toEqual(data)
  })

  test("forwards active request headers and cancellation through compatibility", async () => {
    const controller = new AbortController()
    const { api, requests } = setup(() => Response.json({ data: {} }))
    await api.session.active({ signal: controller.signal, headers: { "x-test": "active" } })
    controller.abort()
    expect(requests[0]?.signal.aborted).toBe(true)
    expect(requests[0]?.headers.get("x-test")).toBe("active")
  })

  test("rejects malformed rich active status", async () => {
    const { current } = setup(() => Response.json({ data: {
      ses_test: { type: "running", status: { type: "busy", activity: { model: "receiving", streamEventCount: "invalid" } } },
    } }))
    await expect(current.session.active()).rejects.toBeDefined()
  })

  test("propagates declared prompt errors", async () => {
    const error = { _tag: "ConflictError", message: "fixture" }
    const { api } = setup(() => Response.json(error, { status: 409 }))
    await expect(api.session.prompt({ sessionID: "ses_test", text: "input" })).rejects.toEqual(error)
  })

  test("does not admit a prompt when native selection fails", async () => {
    const error = { _tag: "SessionNotFoundError", sessionID: "ses_test" }
    const { api, requests } = setup(() => Response.json(error, { status: 404 }))
    await expect(api.session.prompt({ sessionID: "ses_test", text: "input", agent: "build" })).rejects.toEqual(error)
    expect(requests.map((request) => new URL(request.url).pathname)).toEqual(["/api/session/ses_test/agent"])
  })

  test("propagates cancellation to platform fetch", async () => {
    const controller = new AbortController()
    controller.abort()
    const { current, requests } = setup(() => Response.json({ data: admitted }))
    await expect(current.session.prompt(
      { sessionID: "ses_test", text: "input" }, { signal: controller.signal },
    )).rejects.toMatchObject({ message: "Transport" })
    expect(requests[0]?.signal.aborted).toBe(true)
  })

  test("propagates read request options and errors", async () => {
    const error = { _tag: "SessionNotFoundError", sessionID: "ses_test" }
    const controller = new AbortController()
    const { current, requests } = setup(() => Response.json(error, { status: 404 }))
    await expect(current.session.message(
      { sessionID: "ses_test", messageID: message.id },
      { signal: controller.signal, headers: { "x-test": "read" } },
    )).rejects.toEqual(error)
    controller.abort()
    expect(requests[0]?.signal.aborted).toBe(true)
    expect(requests[0]?.headers.get("x-test")).toBe("read")
  })

  test("propagates SSE cancellation to platform fetch", async () => {
    const controller = new AbortController()
    controller.abort()
    const { current, requests } = setup(() => new Response(null, { status: 401 }))
    await expect(Array.fromAsync(current.event.subscribe({ signal: controller.signal })))
      .rejects.toMatchObject({ message: "Transport" })
    expect(requests[0]?.signal.aborted).toBe(true)
  })
})

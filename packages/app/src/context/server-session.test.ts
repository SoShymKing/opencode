import { describe, expect, test } from "bun:test"
import type { retry } from "@opencode-ai/core/util/retry"
import type { NativeServerEvent } from "./server-sdk"
import type { Message, OpencodeClient, Part, Session } from "@opencode-ai/sdk/v2/client"
import { createOpencodeClient } from "@opencode-ai/sdk/v2/client"
import { createServerSession } from "./server-session"
import type { ServerApi } from "@/utils/server"
import { normalizeSessionMessages, type NativeSessionMessage } from "@/utils/session-message"
import { unwrap } from "solid-js/store"

type MessageApi = ServerApi["message"]
type SessionApi = Pick<ServerApi["session"], "get" | "message">

const session = (id: string, parentID?: string): Session => ({
  id,
  slug: id,
  projectID: "project",
  directory: "/repo",
  title: id,
  version: "1",
  parentID,
  time: { created: 1, updated: 1 },
})

type UserMessage = Extract<Message, { role: "user" }>
type AssistantMessage = Extract<Message, { role: "assistant" }>
type TextPart = Extract<Part, { type: "text" }>
type MessageResponse = {
  data: { info: Message; parts: Part[] }[]
  response: { headers: Headers }
}
type SingleMessageResponse = { data: MessageResponse["data"][number] }

const userMessage = (id: string, input: Partial<UserMessage> = {}): UserMessage => ({
  id,
  sessionID: "child",
  role: "user",
  time: { created: 1 },
  agent: "build",
  model: { providerID: "provider", modelID: "model" },
  ...input,
})

const assistantMessage = (id: string, parentID: string, input: Partial<AssistantMessage> = {}): AssistantMessage => ({
  id,
  sessionID: "child",
  role: "assistant",
  time: { created: Number(id.at(-1)), completed: Number(id.at(-1)) },
  parentID,
  modelID: "model",
  providerID: "provider",
  mode: "build",
  agent: "build",
  path: { cwd: "/repo", root: "/repo" },
  cost: 0,
  tokens: { input: 0, output: 0, reasoning: 0, cache: { read: 0, write: 0 } },
  ...input,
})

const textPart = (messageID: string, input: Partial<TextPart> = {}): TextPart => ({
  id: "part",
  sessionID: "child",
  messageID,
  type: "text",
  text: "text",
  ...input,
})

const response = (data: MessageResponse["data"] = [], cursor?: string): MessageResponse => ({
  data,
  response: { headers: new Headers(cursor ? { "x-next-cursor": cursor } : undefined) },
})

const singleResponse = (info: Message, parts: Part[] = []): SingleMessageResponse => ({ data: { info, parts } })

const deferredResponse = () => Promise.withResolvers<MessageResponse>()

type NativePage = Awaited<ReturnType<MessageApi["list"]>>
const nativeUser: NativeSessionMessage = { id: "msg_user", type: "user", text: "hello", time: { created: 1 } }
const nativeAssistant = (content: Extract<NativeSessionMessage, { type: "assistant" }>["content"], extra: Partial<Extract<NativeSessionMessage, { type: "assistant" }>> = {}): Extract<NativeSessionMessage, { type: "assistant" }> => ({
  id: "msg_assistant", type: "assistant", agent: "build", model: { id: "model", providerID: "provider" }, content, time: { created: 2 }, ...extra,
})
const nativePage = (assistant: NativeSessionMessage, cursor?: string): NativePage => ({ data: [assistant, nativeUser], cursor: cursor ? { next: cursor } : {} })
function nativeContext(...pages: (NativePage | Promise<NativePage>)[]) {
  const calls: Parameters<MessageApi["list"]>[0][] = []
  const waiting = new Map<number, () => void>()
  let index = 0
  const api: MessageApi = { list: async (input) => { calls.push(input); waiting.get(calls.length)?.(); return await pages[index++] } }
  const sessionApi: SessionApi = {
    get: async () => ({ id: "child", title: "child", projectID: "project", agent: "build", model: { id: "model", providerID: "provider" }, location: { directory: "/repo" }, time: { created: 1, updated: 1 }, cost: 0, tokens: { input: 0, output: 0, reasoning: 0, cache: { read: 0, write: 0 } } }),
    message: async () => { throw new Error("unexpected parent read") },
  }
  return { store: createServerSession({} as OpencodeClient, sessionApi, api, { retry: retryImmediately }), sessionApi, calls, requested(count: number) { return calls.length >= count ? Promise.resolve() : new Promise<void>((resolve) => waiting.set(count, resolve)) } }
}

const nativeRecovered = nativeAssistant([
  { type: "text", id: "text", text: "recovered" },
  { type: "tool", id: "call", name: "read", state: { status: "completed", input: {}, structured: { done: true }, content: [{ type: "text", text: "output" }] }, time: { created: 2, ran: 2, completed: 3 } },
], { finish: "stop", time: { created: 2, completed: 3 } })

function missingNativeText(store: ReturnType<typeof nativeContext>["store"], messageID = "msg_assistant", textID = "text") {
  store.applyV2({ id: `evt_missing_${messageID}`, type: "session.next.text.ended", data: { timestamp: 3, sessionID: "child", assistantMessageID: messageID, textID, text: "recovered" } })
}

function addPendingNativeInputs(store: ReturnType<typeof nativeContext>["store"], firstID = nativeUser.id) {
  const inputs = [
    { messageID: firstID, partID: "raw-first", text: "hello", created: 99 },
    { messageID: "msg_second", partID: "raw-second", text: "second", created: 100 },
  ]
  for (const input of inputs) {
    store.optimistic.add({ sessionID: "child", message: userMessage(input.messageID, { time: { created: input.created } }), parts: [textPart(input.messageID, { id: input.partID, text: input.text })] })
    store.set("session_status", "child", { type: "busy" })
    store.applyV2({ id: `evt_admit_${input.messageID}`, type: "session.next.prompt.admitted", data: { timestamp: input.created, sessionID: "child", messageID: input.messageID, prompt: { text: input.text }, delivery: "steer" } })
  }
}

function messageClient(...responses: Array<MessageResponse | Promise<MessageResponse>>) {
  let index = 0
  const requests: unknown[] = []
  const waiting = new Map<number, () => void>()
  const client = {
    session: {
      get: async () => ({ data: session("child", "root") }),
      messages: (input: unknown) => {
        requests.push(input)
        waiting.get(requests.length)?.()
        waiting.delete(requests.length)
        return responses[index++]
      },
    },
  } as unknown as OpencodeClient
  return Object.assign(client, {
    requests,
    requested(count: number) {
      if (requests.length >= count) return Promise.resolve()
      return new Promise<void>((resolve) => waiting.set(count, resolve))
    },
  })
}

function rootMessageClient(
  pages: Array<MessageResponse | Promise<MessageResponse>>,
  roots: Array<SingleMessageResponse | Promise<SingleMessageResponse>>,
) {
  let pageIndex = 0
  let rootIndex = 0
  const requests: unknown[] = []
  const rootRequests: unknown[] = []
  const rootWaiting = new Map<number, () => void>()
  const client = {
    session: {
      get: async () => ({ data: session("child", "root") }),
      messages: (input: unknown) => {
        requests.push(input)
        return pages[pageIndex++]
      },
      message: (input: unknown) => {
        rootRequests.push(input)
        rootWaiting.get(rootRequests.length)?.()
        rootWaiting.delete(rootRequests.length)
        return roots[rootIndex++]
      },
    },
  } as unknown as OpencodeClient
  return Object.assign(client, {
    requests,
    rootRequests,
    rootRequested(count: number) {
      if (rootRequests.length >= count) return Promise.resolve()
      return new Promise<void>((resolve) => rootWaiting.set(count, resolve))
    },
  })
}

const retryImmediately: typeof retry = async (task, options = {}) => {
  const attempts = options.attempts ?? 3
  for (let attempt = 0; ; attempt++) {
    try {
      return await task()
    } catch (error) {
      if (attempt === attempts - 1) throw error
    }
  }
}

function setup(sessions: Record<string, Session>) {
  const get: unknown[] = []
  const messages: unknown[] = []
  const client = {
    session: {
      get: async (input: unknown) => {
        get.push(input)
        const id = (input as { sessionID: string }).sessionID
        return { data: sessions[id] }
      },
      messages: async (input: unknown) => {
        messages.push(input)
        return response()
      },
      diff: async () => ({ data: [] }),
      todo: async () => ({ data: [] }),
    },
  } as unknown as OpencodeClient
  return { get, messages, store: createServerSession(client) }
}

describe("server session", () => {
  test("reconnect force waits for older inflight history then fetches fresh history", async () => {
    const older = deferredResponse()
    const client = messageClient(older.promise, response([{ info: userMessage("fresh"), parts: [] }]))
    const store = createServerSession(client)
    const loading = store.sync("child")
    await client.requested(1)
    const refreshing = store.sync("child", { force: true })
    older.resolve(response([{ info: userMessage("old"), parts: [] }]))
    await Promise.all([loading, refreshing])
    expect(client.requests.length).toBe(2)
    expect(store.data.message.child?.map((message) => message.id)).toEqual(["fresh"])
  })

  test("reconnect snapshot restores quiet activity and clears inactive stale busy", () => {
    const ctx = setup({ child: session("child"), ended: session("ended") })
    ctx.store.set("session_status", "child", { type: "busy" })
    ctx.store.set("session_status", "ended", { type: "busy" })
    const snapshot = ctx.store.snapshot.capture("session_status")
    ctx.store.snapshot.status({ child: { type: "busy", activity: {
      model: "receiving", streamEventCount: 200, userMessageID: "msg_owner",
    } } }, snapshot)
    expect(ctx.store.data.session_status.child).toEqual({ type: "busy", activity: {
      model: "receiving", streamEventCount: 200, userMessageID: "msg_owner",
    } })
    expect(ctx.store.data.session_status.ended).toBeUndefined()
  })

  test("reconnect snapshot yields to live status and local pending status", () => {
    const ctx = setup({ child: session("child") })
    ctx.store.remember(session("child"))
    const snapshot = ctx.store.snapshot.capture("session_status")
    ctx.store.apply({ type: "session.status", properties: { sessionID: "child", status: {
      type: "busy", activity: { model: "receiving", streamEventCount: 201 },
    } } })
    ctx.store.set("session_status", "pending", { type: "busy" })
    ctx.store.snapshot.status({ child: { type: "busy", activity: {
      model: "waiting", streamEventCount: 200,
    } } }, snapshot)
    expect(ctx.store.data.session_status.child).toEqual({ type: "busy", activity: {
      model: "receiving", streamEventCount: 201,
    } })
    expect(ctx.store.data.session_status.pending).toEqual({ type: "busy" })
  })

  test("reconnect snapshot rejects an older connection and permits count reset", () => {
    const ctx = setup({ child: session("child") })
    ctx.store.set("session_status", "child", { type: "busy", activity: { model: "receiving", streamEventCount: 200 } })
    const old = ctx.store.snapshot.capture("session_status")
    ctx.store.snapshot.connect()
    const fresh = ctx.store.snapshot.capture("session_status")
    ctx.store.snapshot.status({ child: { type: "busy", activity: { model: "waiting", streamEventCount: 2 } } }, fresh)
    ctx.store.snapshot.status({ child: { type: "busy", activity: { model: "receiving", streamEventCount: 200 } } }, old)
    expect(ctx.store.data.session_status.child).toEqual({ type: "busy", activity: { model: "waiting", streamEventCount: 2 } })
  })

  test("reconnect force preserves failed optimistic and unpromoted pending arrays", async () => {
    const client = messageClient(response())
    const store = createServerSession(client)
    store.remember(session("child"))
    store.optimistic.add({ sessionID: "child", message: userMessage("failed"), parts: [textPart("failed")] })
    store.applyV2({ id: "evt_pending", type: "session.next.prompt.admitted", data: {
      sessionID: "child", messageID: "msg_pending", timestamp: 2, prompt: { text: "queued" }, delivery: "queue",
    } })
    await store.sync("child", { force: true })
    expect(store.data.message.child?.map((message) => message.id)).toEqual(["failed", "msg_pending"])
    expect(store.data.part.failed).toEqual([textPart("failed")])
    expect(store.data.pending_input.child).toEqual({ failed: true, msg_pending: true })
  })

  test("retains an admitted pending user outside canonical history during a refresh", async () => {
    const ctx = setup({ child: session("child") })
    ctx.store.remember(session("child"))
    ctx.store.applyV2({
      id: "evt_admitted", type: "session.next.prompt.admitted",
      data: { sessionID: "child", messageID: "msg_pending", timestamp: 2, prompt: { text: "followup" }, delivery: "queue" },
    })
    await ctx.store.sync("child", { force: true })
    expect(ctx.store.data.session_message.child).toEqual([])
    expect(ctx.store.data.message.child?.map((message) => message.id)).toEqual(["msg_pending"])
    expect(ctx.store.data.pending_input.child).toEqual({ msg_pending: true })
  })

  test("keeps canonical admissions pending through confirmation and cancellation until each prompt is promoted", () => {
    const ctx = setup({ child: session("child") })
    ctx.store.remember(session("child"))
    const admitted = {
      id: "evt_admitted", type: "session.next.prompt.admitted" as const,
      data: { sessionID: "child", messageID: "msg_pending", timestamp: 2, prompt: { text: "followup" }, delivery: "steer" as const },
    }
    ctx.store.applyV2(admitted)
    expect(ctx.store.data.session_message.child).toEqual([])
    expect(ctx.store.data.message.child?.map((message) => message.id)).toEqual(["msg_pending"])
    ctx.store.apply({ type: "message.part.updated", properties: { part: textPart("msg_pending") } })
    ctx.store.apply({ type: "session.status", properties: { sessionID: "child", status: { type: "idle", terminal: { userMessageID: "msg_owner", reason: "cancelled" } } } })
    expect(ctx.store.data.pending_input.child).toEqual({ msg_pending: true })
    ctx.store.applyV2({ ...admitted, id: "evt_promoted", type: "session.next.prompted" })
    expect(ctx.store.data.pending_input.child).toEqual({})
    expect(ctx.store.data.session_message.child?.find((message) => message.id === "msg_pending")).toMatchObject({ type: "user", text: "followup" })
    ctx.store.set("pending_input", "child", "msg_missed", true)
    ctx.store.applyV2({ ...admitted, id: "evt_missed", type: "session.next.prompted", data: { ...admitted.data, messageID: "msg_missed" } })
    expect(ctx.store.data.pending_input.child).toEqual({})
    expect(ctx.store.data.message.child?.map((message) => message.id)).toContain("msg_missed")
  })

  test("preserves pending optimistic confirmations and clears V1 inputs through the selected owner only", () => {
    const ctx = setup({ child: session("child") })
    ctx.store.remember(session("child"))
    const users = [1, 2, 3].map((created) => userMessage(`msg_${created}`, { time: { created } }))
    users.forEach((message) => {
      ctx.store.optimistic.add({ sessionID: "child", message, parts: [textPart(message.id, { id: `prt_${message.id}` })] })
      ctx.store.apply({ type: "message.updated", properties: { info: message } })
      ctx.store.apply({ type: "message.part.updated", properties: { part: textPart(message.id, { id: `prt_${message.id}` }) } })
    })
    ctx.store.apply({ type: "session.status", properties: { sessionID: "child", status: { type: "idle" } } })
    expect(ctx.store.data.pending_input.child).toEqual({ msg_1: true, msg_2: true, msg_3: true })
    const status = { type: "busy", activity: { userMessageID: "msg_2", model: "waiting", streamEventCount: 0 } } as const
    ctx.store.apply({ type: "session.status", properties: { sessionID: "child", status } })
    ctx.store.apply({ type: "session.status", properties: { sessionID: "child", status } })
    expect(ctx.store.data.pending_input.child).toEqual({ msg_3: true })
    expect(ctx.store.data.session_status.child).toEqual(status)
  })

  test("keeps a newly started native assistant after held history releases", async () => {
    const pending = Promise.withResolvers<NativePage>()
    const initial = nativeAssistant([{ type: "text", id: "text", text: "old" }], { time: { created: 2, completed: 3 } })
    const ctx = nativeContext(nativePage(initial), pending.promise)
    await ctx.store.sync("child")
    const history = ctx.store.sync("child", { force: true })
    await ctx.requested(2)
    ctx.store.applyV2({ id: "evt_start", type: "session.next.step.started", data: { timestamp: 4, sessionID: "child", assistantMessageID: "msg_new", agent: "build", model: { id: "model", providerID: "provider" } } })
    ctx.store.applyV2({ id: "evt_text_start", type: "session.next.text.started", data: { timestamp: 5, sessionID: "child", assistantMessageID: "msg_new", textID: "new-text" } })
    ctx.store.applyV2({ id: "evt_text_end", type: "session.next.text.ended", data: { timestamp: 6, sessionID: "child", assistantMessageID: "msg_new", textID: "new-text", text: "live new turn" } })
    pending.resolve(nativePage(initial))
    await history
    expect(ctx.store.data.session_message.child.map((message) => message.id)).toEqual([nativeUser.id, "msg_assistant", "msg_new"])
    expect(ctx.store.data.message.child?.find((message) => message.id === "msg_new")).toMatchObject({ role: "assistant", parentID: nativeUser.id })
    expect(ctx.store.data.part.msg_new).toMatchObject([{ id: "msg_new:new-text", text: "live new turn" }])
  })

  test("ignores delayed native hydration after fresh history commits completed text and tool parts", async () => {
    const page = Promise.withResolvers<NativePage>()
    const hydration = Promise.withResolvers<NativeSessionMessage>()
    const ctx = nativeContext(nativePage(nativeAssistant([])), page.promise)
    ctx.sessionApi.message = () => hydration.promise
    await ctx.store.sync("child")
    const history = ctx.store.sync("child", { force: true })
    await ctx.requested(2)
    missingNativeText(ctx.store)
    page.resolve(nativePage(nativeRecovered))
    await history
    hydration.resolve(nativeAssistant([]))
    await hydration.promise
    expect(ctx.store.data.session_message.child.find((message) => message.id === "msg_assistant")).toEqual(nativeRecovered)
    expect(ctx.store.data.part.msg_assistant).toMatchObject([{ id: "msg_assistant:call", callID: "call", state: { status: "completed", output: "output" } }, { id: "msg_assistant:text", text: "recovered" }])
  })

  test("ignores delayed native hydration after a live update mutates the existing Solid source object", async () => {
    const hydration = Promise.withResolvers<NativeSessionMessage>()
    const ctx = nativeContext(nativePage(nativeAssistant([{ type: "text", id: "text", text: "base" }])))
    ctx.sessionApi.message = () => hydration.promise
    await ctx.store.sync("child")
    const previous = ctx.store.data.session_message.child.find((message) => message.id === "msg_assistant")
    missingNativeText(ctx.store, "msg_assistant", "missing")
    if (previous?.type === "assistant") previous.content.forEach((content) => {
      if (content.type === "text") Object.assign(unwrap(content), { text: "live" })
    })
    ctx.store.data.part.msg_assistant.forEach((part) => {
      if (part.type === "text") Object.assign(unwrap(part), { text: "live" })
    })
    expect(ctx.store.data.session_message.child.find((message) => message.id === "msg_assistant")).toBe(previous)
    hydration.resolve(nativeAssistant([]))
    await hydration.promise
    expect(ctx.store.data.part.msg_assistant).toMatchObject([{ id: "msg_assistant:text", text: "live" }])
    expect(ctx.store.data.session_message.child.find((message) => message.id === "msg_assistant")).toMatchObject({ content: [{ id: "text", text: "live" }] })
  })

  test("ignores delayed native hydration from an older connection epoch", async () => {
    const hydration = Promise.withResolvers<NativeSessionMessage>()
    const ctx = nativeContext(nativePage(nativeAssistant([])))
    ctx.sessionApi.message = () => hydration.promise
    await ctx.store.sync("child")
    missingNativeText(ctx.store)
    ctx.store.snapshot.connect()
    hydration.resolve(nativeRecovered)
    await hydration.promise
    expect(ctx.store.data.session_message.child.find((message) => message.id === "msg_assistant")).toMatchObject({ content: [] })
    expect(ctx.store.data.part.msg_assistant).toBeUndefined()
  })

  test("forced native history waits for older hydration before issuing its fresh read", async () => {
    const hydration = Promise.withResolvers<NativeSessionMessage>()
    const ctx = nativeContext(nativePage(nativeAssistant([])), nativePage(nativeRecovered))
    ctx.sessionApi.message = () => hydration.promise
    await ctx.store.sync("child")
    missingNativeText(ctx.store)
    const history = ctx.store.sync("child", { force: true })
    await Promise.resolve()
    const readsBeforeHydration = ctx.calls.length
    hydration.resolve(nativeAssistant([]))
    await history
    expect(readsBeforeHydration).toBe(1)
    expect(ctx.calls.length).toBe(2)
    expect(ctx.store.data.session_message.child.find((message) => message.id === "msg_assistant")).toEqual(nativeRecovered)
  })

  test("hydrates independent missing native messages and coalesces requests for the same message", async () => {
    const first = Promise.withResolvers<NativeSessionMessage>()
    const second = Promise.withResolvers<NativeSessionMessage>()
    const ctx = nativeContext({ data: [nativeUser], cursor: {} })
    const calls: string[] = []
    ctx.sessionApi.message = (input) => { calls.push(input.messageID); return input.messageID === "msg_assistant" ? first.promise : second.promise }
    await ctx.store.sync("child")
    missingNativeText(ctx.store)
    missingNativeText(ctx.store, "msg_second")
    missingNativeText(ctx.store)
    first.resolve(nativeRecovered)
    second.resolve(nativeAssistant([{ type: "text", id: "text", text: "second" }], { id: "msg_second", time: { created: 4, completed: 5 } }))
    await Promise.all([first.promise, second.promise])
    expect(calls).toEqual(["msg_assistant", "msg_second"])
    expect(ctx.store.data.message.child?.map((message) => message.id)).toEqual([nativeUser.id, "msg_assistant", "msg_second"])
    expect(ctx.store.data.part.msg_assistant).toMatchObject([{ id: "msg_assistant:call", callID: "call" }, { id: "msg_assistant:text", text: "recovered" }])
    expect(ctx.store.data.part.msg_second).toMatchObject([{ id: "msg_second:text", text: "second" }])
  })

  test("positive native hydration projects a complete missing assistant snapshot", async () => {
    const hydration = Promise.withResolvers<NativeSessionMessage>()
    const ctx = nativeContext({ data: [nativeUser], cursor: {} })
    const calls: Parameters<SessionApi["message"]>[0][] = []
    ctx.sessionApi.message = (input) => { calls.push(input); return hydration.promise }
    await ctx.store.sync("child")
    missingNativeText(ctx.store)
    hydration.resolve(nativeRecovered)
    await hydration.promise
    expect(calls).toEqual([{ sessionID: "child", messageID: "msg_assistant" }])
    expect(ctx.store.data.message.child?.find((message) => message.id === "msg_assistant")).toMatchObject({ role: "assistant", parentID: nativeUser.id, time: { completed: 3 } })
    expect(ctx.store.data.part.msg_assistant).toMatchObject([{ id: "msg_assistant:call", callID: "call", state: { status: "completed" } }, { id: "msg_assistant:text", text: "recovered" }])
  })

  test("old-generation hydration cannot commit or remove newer hydration bookkeeping after eviction", async () => {
    const old = Promise.withResolvers<NativeSessionMessage>()
    const fresh = Promise.withResolvers<NativeSessionMessage>()
    const ctx = nativeContext({ data: [nativeUser], cursor: {} }, { data: [nativeUser], cursor: {} }, nativePage(nativeRecovered))
    let calls = 0
    ctx.sessionApi.message = () => (++calls === 1 ? old.promise : fresh.promise)
    await ctx.store.sync("child")
    missingNativeText(ctx.store)
    ctx.store.apply({ type: "session.deleted", properties: { sessionID: "child" } })
    await ctx.store.sync("child")
    missingNativeText(ctx.store)
    old.resolve(nativeAssistant([]))
    await old.promise
    await Promise.resolve()
    expect(ctx.store.data.session_message.child.map((message) => message.id)).toEqual([nativeUser.id])
    const history = ctx.store.sync("child", { force: true })
    await Promise.resolve()
    const readsBeforeHydration = ctx.calls.length
    fresh.resolve(nativeRecovered)
    await history
    expect(calls).toBe(2)
    expect(readsBeforeHydration).toBe(2)
    expect(ctx.store.data.session_message.child.find((message) => message.id === "msg_assistant")).toEqual(nativeRecovered)
  })

  test.each(["active-first", "history-first"])("settles inactive after matching promotion with another admitted local input pending: %s", async (order) => {
    const page = Promise.withResolvers<NativePage>()
    const active = Promise.withResolvers<Record<string, never>>()
    const ctx = nativeContext(page.promise)
    addPendingNativeInputs(ctx.store)
    expect(ctx.store.data.session_message.child).toEqual([])
    ctx.store.snapshot.connect()
    const capture = ctx.store.snapshot.capture("session_status")
    const history = ctx.store.sync("child", { force: true })
    await ctx.requested(1)
    if (order === "active-first") active.resolve({})
    page.resolve(nativePage(nativeAssistant([], { finish: "stop", time: { created: 2, completed: 3 } })))
    await history
    if (order === "history-first") active.resolve({})
    ctx.store.snapshot.status(await active.promise, capture)

    expect(ctx.store.data.part[nativeUser.id]).toMatchObject([{ id: "msg_user:text:0", text: "hello" }])
    expect(ctx.store.data.part.msg_second).toMatchObject([{ id: "raw-second", text: "second" }])
    expect(ctx.store.data.message.child?.filter((message) => message.id === nativeUser.id)).toHaveLength(1)
    expect(ctx.store.data.message.child?.some((message) => message.id === "msg_second")).toBe(true)
    expect(ctx.store.data.session_message.child.map((message) => message.id)).toEqual([nativeUser.id, "msg_assistant"])
    expect(ctx.store.data.session_working("child")).toBe(false)
    expect(ctx.store.data.pending_input.child).toEqual({ msg_second: true })
  })

  test.each(["incoming", "retained"])("keeps genuine local waiting when the old canonical user is only %s", async (source) => {
    const ctx = nativeContext({ data: source === "incoming" ? [nativeUser] : [], cursor: {} })
    ctx.store.set("session_message", "child", [nativeUser])
    addPendingNativeInputs(ctx.store, "msg_new")
    ctx.store.snapshot.connect()
    const capture = ctx.store.snapshot.capture("session_status")
    await ctx.store.sync("child", { force: true })
    ctx.store.snapshot.status({}, capture)

    expect(ctx.store.data.session_working("child")).toBe(true)
    expect(ctx.store.data.part.msg_new).toMatchObject([{ id: "raw-first", text: "hello" }])
    expect(ctx.store.data.part.msg_second).toMatchObject([{ id: "raw-second", text: "second" }])
    expect(ctx.store.data.pending_input.child).toEqual({ msg_new: true, msg_second: true })
  })

  test.each(["local-before", "local-after", "wire", "epoch"])("vetoes promotion settlement after a newer %s change", async (change) => {
    const page = Promise.withResolvers<NativePage>()
    const ctx = nativeContext(page.promise)
    addPendingNativeInputs(ctx.store)
    ctx.store.snapshot.connect()
    const capture = ctx.store.snapshot.capture("session_status")
    const history = ctx.store.sync("child", { force: true })
    await ctx.requested(1)
    if (change === "local-before") ctx.store.set("session_status", "child", { type: "busy" })
    page.resolve({ data: [nativeUser], cursor: {} })
    await history
    if (change === "local-after") ctx.store.set("session_status", "child", { type: "busy" })
    if (change === "wire") ctx.store.apply({ type: "session.status", properties: { sessionID: "child", status: { type: "busy" } } })
    if (change === "epoch") ctx.store.snapshot.connect()
    ctx.store.snapshot.status({}, capture)

    expect(ctx.store.data.session_working("child")).toBe(true)
    expect(ctx.store.data.part.msg_second).toMatchObject([{ id: "raw-second", text: "second" }])
    if (change === "local-before" || change === "local-after") {
      ctx.store.snapshot.status({}, ctx.store.snapshot.capture("session_status"))
      expect(ctx.store.data.session_working("child")).toBe(true)
    }
  })

  test("waits an older info request before forced fresh info and history reads", async () => {
    const old = Promise.withResolvers<{ data: Session }>()
    const calls: string[] = []
    const client = createOpencodeClient({ baseUrl: "http://fixture", fetch: Object.assign(async (input: string | URL | Request) => {
      const url = new URL(input instanceof Request ? input.url : input)
      if (url.pathname.endsWith("/message")) { calls.push("history"); return Response.json([]) }
      calls.push("info")
      return Response.json(calls.length === 1 ? (await old.promise).data : session("child"))
    }, { preconnect: fetch.preconnect }) })
    const store = createServerSession(client)
    const resolving = store.resolve("child")
    const forced = store.sync("child", { force: true })
    await Promise.resolve()
    old.resolve({ data: session("child") })
    await Promise.all([resolving, forced])
    expect(calls).toEqual(["info", "info", "history"])
  })

  test("does not treat native Step.ended as drain settlement", () => {
    const ctx = nativeContext()
    ctx.store.applyV2({ id: "evt_start", type: "session.next.step.started", data: { timestamp: 2, sessionID: "child", assistantMessageID: "msg_step", agent: "build", model: { id: "model", providerID: "provider" } } })
    const capture = ctx.store.snapshot.capture("session_status")
    ctx.store.applyV2({ id: "evt_end", type: "session.next.step.ended", data: { timestamp: 3, sessionID: "child", assistantMessageID: "msg_step", finish: "tool-calls", cost: 0, tokens: { input: 0, output: 0, reasoning: 0, cache: { read: 0, write: 0 } } } })
    ctx.store.snapshot.status({}, capture)
    expect(ctx.store.data.session_status.child).toEqual({ type: "busy" })
  })

  test("distinguishes queued local waiting from observed native execution", () => {
    const ctx = nativeContext()
    ctx.store.optimistic.add({ sessionID: "child", message: userMessage("msg_queue"), parts: [textPart("msg_queue")] })
    ctx.store.set("session_status", "child", { type: "busy" })
    ctx.store.snapshot.status({}, ctx.store.snapshot.capture("session_status"))
    expect(ctx.store.data.session_status.child).toEqual({ type: "busy" })
    ctx.store.applyV2({ id: "evt_start", type: "session.next.step.started", data: { timestamp: 2, sessionID: "child", assistantMessageID: "msg_step", agent: "build", model: { id: "model", providerID: "provider" } } })
    ctx.store.snapshot.status({}, ctx.store.snapshot.capture("session_status"))
    expect(ctx.store.data.session_status.child).toBeUndefined()
    expect(ctx.store.data.part.msg_queue).toBeDefined()
  })

  test("preserves a new local provisional busy write after older observed execution", () => {
    const ctx = nativeContext()
    ctx.store.applyV2({ id: "evt_start", type: "session.next.step.started", data: { timestamp: 2, sessionID: "child", assistantMessageID: "msg_step", agent: "build", model: { id: "model", providerID: "provider" } } })
    ctx.store.optimistic.add({ sessionID: "child", message: userMessage("msg_waiting"), parts: [textPart("msg_waiting")] })
    ctx.store.set("session_status", "child", { type: "busy" })
    ctx.store.snapshot.status({}, ctx.store.snapshot.capture("session_status"))
    expect(ctx.store.data.session_status.child).toEqual({ type: "busy" })
  })

  test("caps native reads and retains older pages after forced refresh", async () => {
    const older: NativeSessionMessage = { id: "msg_older", type: "user", text: "older", time: { created: 0 } }
    const ctx = nativeContext(nativePage(nativeAssistant([{ type: "text", id: "text", text: "initial" }]), "older"), { data: [older], cursor: {} }, nativePage(nativeAssistant([{ type: "text", id: "text", text: "fresh" }])))
    await ctx.store.sync("child", { messageLimit: 4096 })
    await ctx.store.history.loadMore("child", 4096)
    await ctx.store.sync("child", { force: true, messageLimit: 4096 })
    expect(ctx.calls.map((call) => call?.limit)).toEqual([200, 200, 200])
    expect(ctx.store.data.message.child?.map((message) => message.id)).toEqual([older.id, nativeUser.id, "msg_assistant"])
  })

  test("merges live native fields without losing fetched unrelated content or rich tool state", async () => {
    const pending = Promise.withResolvers<NativePage>()
    const tool = { type: "tool", id: "call", name: "read", state: { status: "running", input: {}, structured: {}, content: [] }, time: { created: 2, ran: 2 } } as const
    const initial = nativeAssistant([{ type: "text", id: "text", text: "base" }, tool])
    const ctx = nativeContext(nativePage(initial), pending.promise)
    await ctx.store.sync("child")
    const loading = ctx.store.sync("child", { force: true })
    await ctx.requested(2)
    ctx.store.applyV2({ id: "evt_text", type: "session.next.text.ended", data: { timestamp: 3, sessionID: "child", assistantMessageID: "msg_assistant", textID: "text", text: "LIVE" } })
    ctx.store.applyV2({ id: "evt_tool", type: "session.next.tool.success", data: { timestamp: 4, sessionID: "child", assistantMessageID: "msg_assistant", callID: "call", structured: { live: true }, content: [{ type: "text", text: "output" }], result: { kept: true }, provider: { executed: false } } })
    pending.resolve(nativePage(nativeAssistant([{ type: "text", id: "text", text: "fetched" }, { type: "reasoning", id: "reason", text: "recovered" }, { ...tool, provider: { executed: false, metadata: { rich: { value: 1 } } } }], { cost: 9 })))
    await loading
    const source = ctx.store.data.session_message.child.find((message) => message.id === "msg_assistant")
    expect(source).toMatchObject({ cost: 9, content: [{ id: "text", text: "LIVE" }, { id: "reason", text: "recovered" }, { id: "call", provider: { metadata: { rich: { value: 1 } } }, state: { status: "completed", structured: { live: true }, result: { kept: true } } }] })
    ctx.store.applyV2({ id: "evt_meta", type: "session.next.step.ended", data: { timestamp: 5, sessionID: "child", assistantMessageID: "msg_assistant", finish: "tool-calls", cost: 10, tokens: { input: 0, output: 0, reasoning: 0, cache: { read: 0, write: 0 } } } })
    const projection = normalizeSessionMessages("child", ctx.store.data.session_message.child)
    expect(ctx.store.data.part.msg_assistant).toEqual((projection.parts.get("msg_assistant") ?? []).toSorted((left, right) => left.id.localeCompare(right.id)))
    expect(ctx.store.data.session_status.child).toEqual({ type: "busy" })
  })

  test("keeps HTTP content and authoritative status through counter-only native updates", async () => {
    const pending = Promise.withResolvers<NativePage>()
    const ctx = nativeContext(nativePage(nativeAssistant([{ type: "text", id: "text", text: "old" }])), pending.promise)
    await ctx.store.sync("child")
    const status = { type: "retry", attempt: 1, message: "retry", next: 10,
      activity: { model: "receiving", streamEventCount: 7 },
      action: { reason: "auth", provider: "provider", title: "Reconnect", message: "Reconnect", label: "Connect" },
    } as const
    const expectedStatus = structuredClone(status)
    ctx.store.apply({ type: "session.status", properties: { sessionID: "child", status } })
    const history = ctx.store.sync("child", { force: true })
    await ctx.requested(2)
    ctx.store.applyV2({ id: "evt_count", type: "session.next.step.stream.updated", data: {
      sessionID: "child", assistantMessageID: "msg_assistant", timestamp: 4, streamEventCount: 8,
    } })
    expect(ctx.store.data.session_status.child).toEqual(expectedStatus)
    pending.resolve(nativePage(nativeRecovered))
    await history
    expect(ctx.store.data.session_message.child.find((message) => message.id === "msg_assistant")).toMatchObject({
      streamEventCount: 8, content: nativeRecovered.content,
    })
    expect(ctx.store.data.part.msg_assistant).toMatchObject([
      { id: "msg_assistant:call", state: { status: "completed", output: "output" } },
      { id: "msg_assistant:text", text: "recovered" },
    ])
    const idle = { type: "idle", terminal: { userMessageID: nativeUser.id, reason: "completed" } } as const
    ctx.store.apply({ type: "session.status", properties: { sessionID: "child", status: idle } })
    ctx.store.applyV2({ id: "evt_late_count", type: "session.next.step.stream.updated", data: {
      sessionID: "child", assistantMessageID: "msg_assistant", timestamp: 5, streamEventCount: 9,
    } })
    expect(ctx.store.data.session_status.child).toEqual(idle)
    expect(ctx.store.data.message.child?.find((message) => message.id === "msg_assistant")).toMatchObject({ streamEventCount: 8, time: { completed: 3 } })
  })

  test.each([
    ["base", "base suffix", "base suffix"],
    ["base su", "base suffix", "base suffix"],
    ["base suffix", "base suffix", undefined],
    ["different", "different", undefined],
    ["", "", undefined],
  ])("selects native delta suffix against HTTP %s", async (fetched, expected, accumulator) => {
    const ctx = nativeContext(nativePage(nativeAssistant([{ type: "text", id: "text", text: "base" }])), nativePage(nativeAssistant([{ type: "text", id: "text", text: fetched }])))
    await ctx.store.sync("child")
    ctx.store.applyV2({ id: "evt_delta", type: "session.next.text.delta", data: { timestamp: 3, sessionID: "child", assistantMessageID: "msg_assistant", textID: "text", delta: " suffix" } })
    await ctx.store.sync("child", { force: true })
    if (expected) expect(ctx.store.data.part.msg_assistant).toMatchObject([{ text: expected }])
    if (!expected) expect(ctx.store.data.part.msg_assistant).toBeUndefined()
    if (accumulator) expect(ctx.store.data.part_text_accum_delta["msg_assistant:text"]).toBe(accumulator)
    if (!accumulator) expect(ctx.store.data.part_text_accum_delta["msg_assistant:text"]).toBeUndefined()
    const source = ctx.store.data.session_message.child.find((message) => message.id === "msg_assistant")
    expect(source).toMatchObject({ content: [{ text: expected }] })
  })

  test("confirms incoming native user IDs once while preserving queued raw inputs", async () => {
    const ctx = nativeContext({ data: [nativeUser], cursor: {} }, { data: [], cursor: {} })
    ctx.store.optimistic.add({ sessionID: "child", message: userMessage(nativeUser.id, { time: { created: 99 } }), parts: [textPart(nativeUser.id, { id: "raw", text: "raw" })] })
    ctx.store.optimistic.add({ sessionID: "child", message: userMessage("msg_queued"), parts: [textPart("msg_queued", { id: "queued", text: "queued" })] })
    await ctx.store.sync("child")
    expect(ctx.store.data.part[nativeUser.id]).toMatchObject([{ id: "msg_user:text:0", text: "hello" }])
    expect(ctx.store.data.pending_input.child).toEqual({ msg_queued: true })
    ctx.store.optimistic.remove({ sessionID: "child", messageID: nativeUser.id })
    expect(ctx.store.data.message.child?.some((message) => message.id === nativeUser.id)).toBe(true)
    await ctx.store.sync("child", { force: true })
    expect(ctx.store.data.part.msg_queued).toMatchObject([{ id: "queued", text: "queued" }])
    expect(ctx.store.data.pending_input.child).toEqual({ msg_queued: true })
  })

  test("preserves native reasoning suffix and tombstones without restoring omitted content", async () => {
    const initial = nativeAssistant([{ type: "reasoning", id: "reason", text: "base" }, { type: "text", id: "removed", text: "remove" }])
    const ctx = nativeContext(nativePage(initial), nativePage(initial), nativePage(nativeAssistant([])))
    await ctx.store.sync("child")
    ctx.store.applyV2({ id: "evt_reason", type: "session.next.reasoning.delta", data: { timestamp: 3, sessionID: "child", assistantMessageID: "msg_assistant", reasoningID: "reason", delta: " suffix" } })
    ctx.store.apply({ type: "message.part.removed", properties: { sessionID: "child", messageID: "msg_assistant", partID: "msg_assistant:removed" } })
    await ctx.store.sync("child", { force: true })
    expect(ctx.store.data.part.msg_assistant).toMatchObject([{ id: "msg_assistant:reason", text: "base suffix" }])
    await ctx.store.sync("child", { force: true })
    expect(ctx.store.data.part.msg_assistant).toBeUndefined()
    expect(ctx.store.data.part_text_accum_delta["msg_assistant:reason"]).toBeUndefined()
    expect(ctx.store.data.session_message.child.find((message) => message.id === "msg_assistant")).toMatchObject({ content: [] })
  })

  test("keeps a known native suffix when the original HTTP content has an empty prefix", async () => {
    const empty = nativeAssistant([{ type: "text", id: "text", text: "" }])
    const ctx = nativeContext(nativePage(empty), nativePage(empty))
    await ctx.store.sync("child")
    ctx.store.applyV2({ id: "evt_delta", type: "session.next.text.delta", data: { timestamp: 3, sessionID: "child", assistantMessageID: "msg_assistant", textID: "text", delta: "known" } })
    await ctx.store.sync("child", { force: true })
    expect(ctx.store.data.part.msg_assistant).toMatchObject([{ id: "msg_assistant:text", text: "known" }])
    expect(ctx.store.data.part_text_accum_delta["msg_assistant:text"]).toBe("known")
    expect(ctx.store.data.session_message.child.find((message) => message.id === "msg_assistant")).toMatchObject({ content: [{ id: "text", text: "known" }] })
  })

  test("preserves live source fields through native older-root cursor extension", async () => {
    const first = Promise.withResolvers<NativePage>()
    const root = Promise.withResolvers<NativePage>()
    const initial = nativeAssistant([{ type: "text", id: "text", text: "base" }])
    const ctx = nativeContext(nativePage(initial), first.promise, root.promise)
    await ctx.store.sync("child")
    const loading = ctx.store.sync("child", { force: true })
    await ctx.requested(2)
    ctx.store.applyV2({ id: "evt_text", type: "session.next.text.ended", data: { timestamp: 3, sessionID: "child", assistantMessageID: "msg_assistant", textID: "text", text: "live" } })
    first.resolve({ data: [initial], cursor: { next: "root" } })
    await ctx.requested(3)
    ctx.store.applyV2({ id: "evt_meta", type: "session.next.step.ended", data: { timestamp: 5, sessionID: "child", assistantMessageID: "msg_assistant", finish: "tool-calls", cost: 10, tokens: { input: 0, output: 0, reasoning: 0, cache: { read: 0, write: 0 } } } })
    root.resolve({ data: [nativeUser], cursor: {} })
    await loading
    expect(ctx.store.data.session_message.child.find((message) => message.id === "msg_assistant")).toMatchObject({ finish: "tool-calls", content: [{ id: "text", text: "live" }] })
    expect(ctx.calls[2]).toEqual({ sessionID: "child", limit: 2, cursor: "root" })
  })

  test("native retry discards failed full values but retains known deltas", async () => {
    const failed = Promise.withResolvers<NativePage>()
    const retried = Promise.withResolvers<NativePage>()
    const initial = nativeAssistant([{ type: "text", id: "full", text: "base" }, { type: "reasoning", id: "delta", text: "base" }])
    const ctx = nativeContext(nativePage(initial), failed.promise, retried.promise)
    await ctx.store.sync("child")
    const loading = ctx.store.sync("child", { force: true })
    await ctx.requested(2)
    ctx.store.applyV2({ id: "evt_text", type: "session.next.text.ended", data: { timestamp: 3, sessionID: "child", assistantMessageID: "msg_assistant", textID: "full", text: "failed live" } })
    ctx.store.applyV2({ id: "evt_delta", type: "session.next.reasoning.delta", data: { timestamp: 4, sessionID: "child", assistantMessageID: "msg_assistant", reasoningID: "delta", delta: " suffix" } })
    failed.reject(new Error("retry"))
    await ctx.requested(3)
    retried.resolve(nativePage(nativeAssistant([{ type: "text", id: "full", text: "retry fresh" }, { type: "reasoning", id: "delta", text: "base" }])))
    await loading
    expect(ctx.store.data.session_message.child.find((message) => message.id === "msg_assistant")).toMatchObject({ content: [{ id: "full", text: "retry fresh" }, { id: "delta", text: "base suffix" }] })
  })

  test("native cursor retry keeps pre-extension values but discards failed extension full text", async () => {
    const first = Promise.withResolvers<NativePage>()
    const failed = Promise.withResolvers<NativePage>()
    const retried = Promise.withResolvers<NativePage>()
    const initial = nativeAssistant([{ type: "text", id: "text", text: "base" }])
    const ctx = nativeContext(nativePage(initial), first.promise, failed.promise, retried.promise)
    await ctx.store.sync("child")
    const loading = ctx.store.sync("child", { force: true })
    await ctx.requested(2)
    ctx.store.applyV2({ id: "evt_baseline", type: "session.next.text.ended", data: { timestamp: 3, sessionID: "child", assistantMessageID: "msg_assistant", textID: "text", text: "baseline live" } })
    first.resolve({ data: [initial], cursor: { next: "root" } })
    await ctx.requested(3)
    ctx.store.applyV2({ id: "evt_failed", type: "session.next.text.ended", data: { timestamp: 4, sessionID: "child", assistantMessageID: "msg_assistant", textID: "text", text: "failed extension" } })
    failed.reject(new Error("retry"))
    await ctx.requested(4)
    retried.resolve({ data: [nativeUser], cursor: {} })
    await loading
    expect(ctx.store.data.part.msg_assistant).toMatchObject([{ text: "baseline live" }])
    expect(ctx.store.data.session_message.child.find((message) => message.id === "msg_assistant")).toMatchObject({ content: [{ text: "baseline live" }] })
  })

  test("a promoted native input clears raw optimistic IDs but admission alone does not", () => {
    const ctx = nativeContext()
    ctx.store.optimistic.add({ sessionID: "child", message: userMessage(nativeUser.id, { time: { created: 99 } }), parts: [textPart(nativeUser.id, { id: "raw" })] })
    ctx.store.applyV2({ id: "evt_admit", type: "session.next.prompt.admitted", data: { timestamp: 2, sessionID: "child", messageID: nativeUser.id, prompt: { text: "hello" }, delivery: "queue" } })
    expect(ctx.store.data.part[nativeUser.id]).toMatchObject([{ id: "raw" }])
    expect(ctx.store.data.session_message.child).toEqual([])
    ctx.store.applyV2({ id: "evt_promote", type: "session.next.prompted", data: { timestamp: 3, sessionID: "child", messageID: nativeUser.id, prompt: { text: "hello" }, delivery: "queue" } })
    ctx.store.optimistic.remove({ sessionID: "child", messageID: nativeUser.id })
    expect(ctx.store.data.part[nativeUser.id]).toMatchObject([{ id: "msg_user:text:0", text: "hello" }])
    expect(ctx.store.data.message.child?.map((message) => ({ id: message.id, created: message.time.created }))).toEqual([{ id: nativeUser.id, created: 3 }])
  })

  test("forces a fresh read after an older sync finishes", async () => {
    const older = deferredResponse()
    const client = messageClient(older.promise, response([{ info: userMessage("fresh"), parts: [] }]))
    const store = createServerSession(client)
    const first = store.sync("child")
    await client.requested(1)
    const forced = store.sync("child", { force: true })
    older.resolve(response([{ info: userMessage("old"), parts: [] }]))
    await Promise.all([first, forced])
    expect(client.requests.length).toBe(2)
    expect(store.data.message.child?.map((item) => item.id)).toEqual(["fresh"])
  })

  test("forces a fresh read after an older history prepend finishes", async () => {
    const older = deferredResponse()
    const client = messageClient(response([{ info: userMessage("latest"), parts: [] }], "cursor"), older.promise, response())
    const store = createServerSession(client)
    await store.sync("child")
    const prepend = store.history.loadMore("child")
    await client.requested(2)
    const forced = store.sync("child", { force: true })
    older.resolve(response([{ info: userMessage("older", { time: { created: 0 } }), parts: [] }], "next"))
    await Promise.all([prepend, forced])
    expect(client.requests.length).toBe(3)
    expect(store.history.loading("child")).toBe(false)
  })

  test("guards status snapshots against local writes and connection epochs", () => {
    const store = createServerSession({} as OpencodeClient)
    store.set("session_status", "stale", { type: "retry", attempt: 1, message: "retry", next: 10 })
    const capture = store.snapshot.capture("session_status")
    store.set("session_status", "local", { type: "busy" })
    store.snapshot.status({}, capture)
    expect(store.data.session_status.stale).toBeUndefined()
    expect(store.data.session_status.local).toEqual({ type: "busy" })
    const old = store.snapshot.capture("session_status")
    store.snapshot.connect()
    store.snapshot.status({ local: { type: "idle" } }, old)
    expect(store.data.session_status.local).toEqual({ type: "busy" })
  })

  test.each(["inferred", "authoritative", "authoritative repeated"])("preserves bare busy snapshot provenance for native retry: %s", (provenance) => {
    const store = nativeContext().store
    const captured = { ...store.snapshot.capture("session_status"), inferred: new Set(provenance === "inferred" ? ["child"] : []) }
    store.snapshot.status({ child: { type: "busy" } }, captured)
    if (provenance === "authoritative repeated") {
      const repeated = { ...store.snapshot.capture("session_status"), inferred: new Set(["child"]) }
      store.snapshot.status({ child: { type: "busy" } }, repeated)
    }

    store.applyV2({ id: "evt_retry", type: "session.next.retried", data: {
      sessionID: "child", timestamp: 10, attempt: 1, error: { message: "Fixture retry", isRetryable: true },
    } })

    expect(store.data.session_status.child).toEqual(provenance === "inferred"
      ? { type: "retry", attempt: 1, message: "Fixture retry", next: 10 }
      : { type: "busy" })
  })

  test("guards older status responses after a newer snapshot commits", () => {
    const store = createServerSession({} as OpencodeClient)
    store.set("session_status", "child", { type: "busy" })
    const older = store.snapshot.capture("session_status")
    const newer = store.snapshot.capture("session_status")
    store.snapshot.status({}, newer)
    store.snapshot.status({ child: { type: "busy" } }, older)
    expect(store.data.session_status.child).toBeUndefined()
  })

  test("projects V2 session events into current and legacy message state", () => {
    const ctx = setup({ child: session("child") })
    ctx.store.remember(session("child"))
    ctx.store.set("session_message", "child", [
      {
        id: "msg_1_user",
        type: "user",
        text: "hello",
        time: { created: 1 },
      },
    ])
    const apply = (input: NativeServerEvent) => ctx.store.applyV2(input)

    apply({
      id: "evt_step",
      type: "session.next.step.started",
      durable: { aggregateID: "child", seq: 1, version: 1 },
      location: { directory: "/repo" },
      data: {
        sessionID: "child",
        timestamp: 2,
        assistantMessageID: "msg_2_assistant",
        agent: "build",
        model: { id: "model", providerID: "provider" },
      },
    })
    apply({
      id: "evt_text_start",
      type: "session.next.text.started",
      durable: { aggregateID: "child", seq: 2, version: 1 },
      location: { directory: "/repo" },
      data: { sessionID: "child", timestamp: 3, assistantMessageID: "msg_2_assistant", textID: "txt_1" },
    })
    apply({
      id: "evt_text_delta",
      type: "session.next.text.delta",
      location: { directory: "/repo" },
      data: { sessionID: "child", timestamp: 4, assistantMessageID: "msg_2_assistant", textID: "txt_1", delta: "world" },
    })

    expect(ctx.store.data.session_message.child?.at(-1)).toMatchObject({
      id: "msg_2_assistant",
      type: "assistant",
      content: [{ type: "text", text: "world" }],
    })
    expect(ctx.store.data.message.child?.map((message) => message.id)).toEqual(["msg_1_user", "msg_2_assistant"])
    expect(ctx.store.data.message.child?.[0]).toMatchObject({
      agent: "build",
      model: { modelID: "model", providerID: "provider" },
    })
    expect(ctx.store.data.part.msg_2_assistant).toMatchObject([{ type: "text", text: "world" }])
  })

  test("resolves lineage by session ID without directory", async () => {
    const ctx = setup({ child: session("child", "root"), root: session("root") })

    const result = await ctx.store.lineage.resolve("child")

    expect(result.root.id).toBe("root")
    expect(ctx.get).toEqual([{ sessionID: "child" }, { sessionID: "root" }])
    expect(ctx.store.lineage.peek("child")).toEqual(result)
  })

  test("loads session content through the server client", async () => {
    const ctx = setup({ root: session("root") })

    await ctx.store.sync("root")

    expect(ctx.get).toEqual([{ sessionID: "root" }])
    expect(ctx.messages).toEqual([{ sessionID: "root", limit: 20, before: undefined }])
    expect(ctx.store.data.message.root).toEqual([])
  })

  test("loads current session content through the current message API", async () => {
    const requests: unknown[] = []
    const user: NativeSessionMessage = { id: "msg_z_user", type: "user", text: "hello", time: { created: 1 } }
    const assistant: NativeSessionMessage = {
      id: "msg_a_assistant",
      type: "assistant",
      agent: "build",
      model: { id: "model", providerID: "provider" },
      content: [{ type: "text", id: "txt_1", text: "hi" }],
      time: { created: 2, completed: 3 },
    }
    const client = {
      session: {
        messages: () => {
          throw new Error("legacy message endpoint called")
        },
      },
    } as unknown as OpencodeClient
    const messageApi: MessageApi = {
      list: async (input: unknown) => {
        requests.push(input)
        return { data: [assistant, user], cursor: {} }
      },
    }
    const store = createServerSession(client, {} as SessionApi, messageApi)
    store.remember(session("root"))

    await store.sync("root")

    expect(requests).toEqual([{ sessionID: "root", limit: 20, order: "desc" }])
    expect(store.data.session_message.root.map((message) => message.id)).toEqual([user.id, assistant.id])
    expect(store.data.message.root.map((message) => message.id)).toEqual([user.id, assistant.id])
  })

  test("extends a current page to include the user for split assistant turns", async () => {
    const user = { id: "msg_1_user", type: "user", text: "hello", time: { created: 1 } } as const
    const assistant = (id: string, created: number) => ({
      id,
      type: "assistant" as const,
      agent: "build",
      model: { id: "model", providerID: "provider" },
      content: [{ type: "text" as const, id: "txt_1", text: id }],
      time: { created, completed: created },
    })
    const assistants = [
      assistant("msg_2_assistant", 2),
      assistant("msg_3_assistant", 3),
      assistant("msg_4_assistant", 4),
    ]
    const pages: NativePage[] = [
      { data: assistants.slice(1).toReversed(), cursor: { next: "older" } },
      { data: [assistants[0], user], cursor: {} },
    ]
    const requests: unknown[] = []
    const messageApi: MessageApi = {
      list: async (input: unknown) => {
        requests.push(input)
        return pages.shift()!
      },
    }
    const store = createServerSession({} as OpencodeClient, {} as SessionApi, messageApi)
    store.remember(session("root"))

    await store.sync("root")

    expect(requests).toEqual([
      { sessionID: "root", limit: 20, order: "desc" },
      { sessionID: "root", limit: 20, cursor: "older" },
    ])
    expect(store.data.message.root.map((message) => message.id)).toEqual([
      user.id,
      ...assistants.map((item) => item.id),
    ])
    expect(assistants.map((item) => store.data.part[item.id]?.[0]?.type)).toEqual(["text", "text", "text"])
  })

  test("indexes V1 messages for the current timeline projection", async () => {
    const user = userMessage("message-1", { sessionID: "root" })
    const assistant = assistantMessage("message-2", user.id, { sessionID: "root" })
    const client = messageClient(
      response([
        { info: user, parts: [textPart(user.id, { sessionID: "root" })] },
        { info: assistant, parts: [textPart(assistant.id, { sessionID: "root" })] },
      ]),
    )
    const messageApi = {
      list: () => {
        throw new Error("current message endpoint called")
      },
    } as unknown as MessageApi
    const store = createServerSession(client, {} as SessionApi, messageApi, {
      protocol: Promise.resolve("v1"),
    })
    store.remember(session("root"))

    await store.sync("root")

    expect(store.data.message.root.map((message) => message.id)).toEqual([user.id, assistant.id])
    expect(store.data.session_message.root).toMatchObject([
      { id: user.id, type: "user", text: "text" },
      { id: assistant.id, type: "assistant" },
    ])

    const next = userMessage("message-3", { sessionID: "root" })
    store.apply({ type: "message.updated", properties: { info: next } })
    expect(store.data.session_message.root.map((message) => message.id)).toEqual([user.id, assistant.id, next.id])

    store.set("pending_input", "root", { [user.id]: true, [next.id]: true })
    store.apply({ type: "session.status", properties: { sessionID: "root", status: { type: "busy", activity: { userMessageID: user.id, model: "waiting" } } } })
    expect(store.data.pending_input.root).toEqual({ [next.id]: true })
    store.set("pending_input", "root", { [user.id]: true, [next.id]: true })
    store.apply({ type: "message.updated", properties: { info: { ...assistant, time: { created: 1 } } } })
    expect(store.data.pending_input.root).toEqual({ [next.id]: true })

    store.apply({ type: "message.removed", properties: { sessionID: "root", messageID: next.id } })
    expect(store.data.session_message.root.map((message) => message.id)).toEqual([user.id, assistant.id])
  })

  test("backfills an assistant-only initial page through its user root", async () => {
    const user = userMessage("message-1")
    const assistants = [assistantMessage("message-2", user.id), assistantMessage("message-3", user.id)]
    const client = rootMessageClient(
      [
        response(
          assistants.map((info) => ({ info, parts: [] })),
          "older",
        ),
      ],
      [singleResponse(user)],
    )
    const store = createServerSession(client)

    await store.sync("child")

    expect(client.requests).toEqual([{ sessionID: "child", limit: 20, before: undefined }])
    expect(client.rootRequests).toEqual([{ sessionID: "child", messageID: user.id }])
    expect(store.data.message.child).toEqual([user, ...assistants])
    expect(store.history.more("child")).toBe(true)
  })

  test("keeps assistant history when its deleted parent cannot be backfilled", async () => {
    const missing = Promise.withResolvers<SingleMessageResponse>()
    const assistant = assistantMessage("message-2", "message-missing")
    const client = rootMessageClient([response([{ info: assistant, parts: [] }], "older")], [missing.promise])
    const store = createServerSession(client)
    const loading = store.sync("child")
    await client.rootRequested(1)

    missing.reject(new Error("Message not found: message-missing", { cause: { status: 404 } }))
    await loading

    expect(client.rootRequests).toEqual([{ sessionID: "child", messageID: "message-missing" }])
    expect(store.data.message.child).toEqual([assistant])
    expect(store.history.more("child")).toBe(true)
  })

  test("drops a cached parent when a forced refresh confirms it was deleted", async () => {
    const missing = Promise.withResolvers<SingleMessageResponse>()
    const parent = userMessage("message-1")
    const part = textPart(parent.id)
    const assistant = assistantMessage("message-2", parent.id)
    const client = rootMessageClient(
      [
        response([
          { info: parent, parts: [part] },
          { info: assistant, parts: [] },
        ]),
        response([{ info: assistant, parts: [] }], "older"),
      ],
      [missing.promise],
    )
    const store = createServerSession(client)
    await store.sync("child")
    const loading = store.sync("child", { force: true })
    await client.rootRequested(1)

    missing.reject(new Error(`Message not found: ${parent.id}`, { cause: { status: 404 } }))
    await loading

    expect(store.data.message.child).toEqual([assistant])
    expect(store.data.part[parent.id]).toBeUndefined()
  })

  test("does not let an optimistic user suppress initial root backfill", async () => {
    const user = userMessage("message-1")
    const part = textPart(user.id)
    const assistants = [assistantMessage("message-2", user.id), assistantMessage("message-3", user.id)]
    const client = rootMessageClient(
      [
        response(
          assistants.map((info) => ({ info, parts: [] })),
          "older",
        ),
      ],
      [singleResponse(user)],
    )
    const store = createServerSession(client)
    store.optimistic.add({ sessionID: "child", message: user, parts: [part] })

    await store.sync("child")
    store.optimistic.remove({ sessionID: "child", messageID: user.id })

    expect(client.requests).toHaveLength(1)
    expect(client.rootRequests).toHaveLength(1)
    expect(store.data.message.child).toEqual([user, ...assistants])
  })

  test("backfills the parent of fetched assistants when another user is cached", async () => {
    const unrelated = userMessage("message-0", { time: { created: 0 } })
    const user = userMessage("message-1")
    const assistants = [assistantMessage("message-2", user.id), assistantMessage("message-3", user.id)]
    const client = rootMessageClient(
      [
        response([{ info: unrelated, parts: [] }]),
        response(
          assistants.map((info) => ({ info, parts: [] })),
          "older",
        ),
      ],
      [singleResponse(user)],
    )
    const store = createServerSession(client)
    await store.sync("child")

    await store.sync("child", { force: true })

    expect(client.requests).toHaveLength(2)
    expect(client.rootRequests).toHaveLength(1)
    expect(store.data.message.child).toEqual([unrelated, user, ...assistants])
  })

  test("preserves cached history between an injected parent and the page boundary", async () => {
    const user = userMessage("message-1")
    const cached = userMessage("message-3", { time: { created: 3 } })
    const assistant = assistantMessage("message-4", user.id)
    const client = rootMessageClient(
      [response([{ info: cached, parts: [] }]), response([{ info: assistant, parts: [] }], "older")],
      [singleResponse(user)],
    )
    const store = createServerSession(client)
    await store.sync("child")

    await store.sync("child", { force: true })

    expect(store.data.message.child).toEqual([user, cached, assistant])
  })

  test("refreshes a cached parent omitted by an assistant-only replacement page", async () => {
    const stale = userMessage("message-1", { summary: { title: "stale", diffs: [] } })
    const fresh = { ...stale, summary: { title: "fresh", diffs: [] } }
    const stalePart = textPart(stale.id, { text: "stale" })
    const freshPart = { ...stalePart, text: "fresh" }
    const assistant = assistantMessage("message-2", stale.id)
    const client = rootMessageClient(
      [response([{ info: stale, parts: [stalePart] }]), response([{ info: assistant, parts: [] }], "older")],
      [singleResponse(fresh, [freshPart])],
    )
    const store = createServerSession(client)
    await store.sync("child")

    await store.sync("child", { force: true })

    expect(client.rootRequests).toEqual([{ sessionID: "child", messageID: stale.id }])
    expect(store.data.message.child).toEqual([fresh, assistant])
    expect(store.data.part[stale.id]).toEqual([freshPart])
  })

  test("refreshes a confirmed optimistic parent while preserving pending parts", async () => {
    const stale = userMessage("message-1", { summary: { title: "stale", diffs: [] } })
    const fresh = { ...stale, summary: { title: "fresh", diffs: [] } }
    const confirmed = textPart(stale.id, { id: "confirmed", text: "stale" })
    const refreshed = { ...confirmed, text: "fresh" }
    const pending = textPart(stale.id, { id: "pending", text: "pending" })
    const assistant = assistantMessage("message-2", stale.id)
    const client = rootMessageClient(
      [response([{ info: stale, parts: [confirmed] }]), response([{ info: assistant, parts: [] }], "older")],
      [singleResponse(fresh, [refreshed])],
    )
    const store = createServerSession(client)
    store.optimistic.add({ sessionID: "child", message: stale, parts: [confirmed, pending] })
    await store.sync("child")

    await store.sync("child", { force: true })

    expect(client.rootRequests).toEqual([{ sessionID: "child", messageID: stale.id }])
    expect(store.data.message.child).toEqual([fresh, assistant])
    expect(store.data.part[stale.id]).toEqual([refreshed, pending])
  })

  test("uses a parent received by SSE during the replacement load", async () => {
    const pending = deferredResponse()
    const user = userMessage("message-1")
    const assistant = assistantMessage("message-2", user.id)
    const client = rootMessageClient([pending.promise], [])
    const store = createServerSession(client)
    const loading = store.sync("child")

    store.apply({ type: "message.updated", properties: { info: user } })
    pending.resolve(response([{ info: assistant, parts: [] }], "older"))
    await loading

    expect(client.rootRequests).toEqual([])
    expect(store.data.message.child).toEqual([user, assistant])
  })

  test("uses a successful retry over events received by a failed backfill attempt", async () => {
    const failed = deferredResponse()
    const user = userMessage("message-1")
    const live = { ...user, agent: "stale" }
    const assistants = [assistantMessage("message-2", user.id), assistantMessage("message-3", user.id)]
    const client = rootMessageClient(
      [
        response(
          assistants.map((info) => ({ info, parts: [] })),
          "older",
        ),
      ],
      [failed.promise.then((result) => ({ data: result.data[0]! })), singleResponse(user)],
    )
    const store = createServerSession(client, { retry: retryImmediately })
    const loading = store.sync("child")
    await client.rootRequested(1)

    store.apply({ type: "message.updated", properties: { info: live } })
    failed.reject(new Error("retry"))
    await loading

    expect(client.requests).toHaveLength(1)
    expect(client.rootRequests).toHaveLength(2)
    expect(store.data.message.child).toEqual([user, ...assistants])
  })

  test("preserves newer-page events across a failed parent retry", async () => {
    const failed = deferredResponse()
    const user = userMessage("message-1")
    const assistant = assistantMessage("message-2", user.id)
    const live = { ...assistant, cost: 1 }
    const client = rootMessageClient(
      [response([{ info: assistant, parts: [] }], "older")],
      [failed.promise.then((result) => ({ data: result.data[0]! })), singleResponse(user)],
    )
    const store = createServerSession(client, { retry: retryImmediately })
    const loading = store.sync("child")
    await client.rootRequested(1)

    store.apply({ type: "message.updated", properties: { info: live } })
    failed.reject(new Error("retry"))
    await loading

    expect(store.data.message.child).toEqual([user, live])
  })

  test("preserves unrelated message events across a failed parent retry", async () => {
    const failed = deferredResponse()
    const user = userMessage("message-1")
    const assistant = assistantMessage("message-2", user.id)
    const live = userMessage("message-4", { time: { created: 4 } })
    const client = rootMessageClient(
      [response([{ info: assistant, parts: [] }], "older")],
      [failed.promise.then((result) => ({ data: result.data[0]! })), singleResponse(user)],
    )
    const store = createServerSession(client, { retry: retryImmediately })
    const loading = store.sync("child")
    await client.rootRequested(1)

    store.apply({ type: "message.updated", properties: { info: live } })
    failed.reject(new Error("retry"))
    await loading

    expect(store.data.message.child).toEqual([user, assistant, live])
  })

  test("preserves newer-page part events across a failed parent retry", async () => {
    const failed = deferredResponse()
    const user = userMessage("message-1")
    const assistant = assistantMessage("message-2", user.id)
    const stale = textPart(assistant.id, { text: "stale" })
    const live = { ...stale, text: "live" }
    const client = rootMessageClient(
      [response([{ info: assistant, parts: [stale] }], "older")],
      [failed.promise.then((result) => ({ data: result.data[0]! })), singleResponse(user)],
    )
    const store = createServerSession(client, { retry: retryImmediately })
    const loading = store.sync("child")
    await client.rootRequested(1)

    store.apply({ type: "message.part.updated", properties: { sessionID: "child", part: live, time: 2 } })
    failed.reject(new Error("retry"))
    await loading

    expect(store.data.part[assistant.id]).toEqual([live])
  })

  test("merges live events into the initial page", async () => {
    const pending = deferredResponse()
    const user = userMessage("message-1")
    const live = userMessage("message-2", { time: { created: 2 } })
    const livePart = textPart(live.id, { text: "live" })
    const store = createServerSession(messageClient(pending.promise))
    const loading = store.sync("child")

    store.apply({ type: "message.updated", properties: { info: live } })
    store.apply({ type: "message.part.updated", properties: { sessionID: "child", part: livePart, time: 2 } })
    pending.resolve(response([{ info: user, parts: [] }]))
    await loading

    expect(store.data.message.child).toEqual([user, live])
    expect(store.data.part[live.id]).toEqual([livePart])
  })

  test("preserves same-ID live updates over the initial page", async () => {
    const pending = deferredResponse()
    const fetched = userMessage("message")
    const fetchedPart = textPart(fetched.id, { text: "fetched" })
    const live = { ...fetched, time: { created: 2 } }
    const livePart = { ...fetchedPart, text: "live" }
    const store = createServerSession(messageClient(pending.promise))
    const loading = store.sync("child")

    store.apply({ type: "message.updated", properties: { info: live } })
    store.apply({ type: "message.part.updated", properties: { sessionID: "child", part: livePart, time: 2 } })
    pending.resolve(response([{ info: fetched, parts: [fetchedPart] }]))
    await loading

    expect(store.data.message.child).toEqual([live])
    expect(store.data.part[live.id]).toEqual([livePart])
  })

  test("preserves removals received during the initial load", async () => {
    const pending = deferredResponse()
    const removed = userMessage("message-1")
    const kept = { ...removed, id: "message-2" }
    const part = textPart(kept.id, { text: "removed" })
    const store = createServerSession(messageClient(pending.promise))
    const loading = store.sync("child")

    store.apply({ type: "message.removed", properties: { sessionID: "child", messageID: removed.id } })
    store.apply({
      type: "message.part.removed",
      properties: { sessionID: "child", messageID: kept.id, partID: part.id },
    })
    pending.resolve(
      response([
        { info: removed, parts: [] },
        { info: kept, parts: [part] },
      ]),
    )
    await loading

    expect(store.data.message.child).toEqual([kept])
    expect(store.data.part[kept.id]).toBeUndefined()
  })

  test("keeps removal tracking isolated across load generations", async () => {
    const firstResponse = deferredResponse()
    const secondResponse = deferredResponse()
    const message = userMessage("message")
    const store = createServerSession(messageClient(firstResponse.promise, secondResponse.promise))
    const first = store.sync("child")

    store.apply({ type: "message.removed", properties: { sessionID: "child", messageID: message.id } })
    store.apply({
      type: "session.deleted",
      properties: { sessionID: "child", info: session("child", "root") },
    })
    const second = store.sync("child")

    firstResponse.resolve(response())
    await first
    secondResponse.resolve(response([{ info: message, parts: [] }]))
    await second

    expect(store.data.message.child).toEqual([message])
  })

  test("tracks removals in a replacement load generation", async () => {
    const firstResponse = deferredResponse()
    const secondResponse = deferredResponse()
    const message = userMessage("message")
    const store = createServerSession(messageClient(firstResponse.promise, secondResponse.promise))
    const first = store.sync("child")
    store.apply({
      type: "session.deleted",
      properties: { sessionID: "child", info: session("child", "root") },
    })
    const second = store.sync("child")

    store.apply({ type: "message.removed", properties: { sessionID: "child", messageID: message.id } })
    firstResponse.resolve(response())
    await first
    secondResponse.resolve(response([{ info: message, parts: [] }]))
    await second

    expect(store.data.message.child).toEqual([])
  })

  test("preserves remove then re-add when a refresh omits the message", async () => {
    const pending = deferredResponse()
    const message = userMessage("message")
    const store = createServerSession(messageClient(response([{ info: message, parts: [] }]), pending.promise))
    await store.sync("child")
    const refreshing = store.sync("child", { force: true })

    store.apply({ type: "message.removed", properties: { sessionID: "child", messageID: message.id } })
    store.apply({ type: "message.updated", properties: { info: message } })
    pending.resolve(response())
    await refreshing

    expect(store.data.message.child).toEqual([message])
  })

  test("preserves a re-added message without restoring removed parts", async () => {
    const pending = deferredResponse()
    const message = userMessage("message")
    const part = textPart(message.id, { text: "stale" })
    const store = createServerSession(messageClient(response([{ info: message, parts: [] }]), pending.promise))
    await store.sync("child")
    const refreshing = store.sync("child", { force: true })

    store.apply({ type: "message.removed", properties: { sessionID: "child", messageID: message.id } })
    store.apply({ type: "message.updated", properties: { info: message } })
    pending.resolve(response([{ info: message, parts: [part] }]))
    await refreshing

    expect(store.data.message.child).toEqual([message])
    expect(store.data.part[message.id]).toBeUndefined()
  })

  test("preserves optimistic parts re-added after removal during a refresh", async () => {
    const pending = deferredResponse()
    const message = userMessage("message")
    const stale = textPart(message.id, { id: "stale", text: "stale" })
    const part = textPart(message.id, { id: "optimistic", text: "optimistic" })
    const store = createServerSession(
      messageClient(response([{ info: message, parts: [] }]), pending.promise, response()),
    )
    await store.sync("child")
    const refreshing = store.sync("child", { force: true })

    store.apply({ type: "message.removed", properties: { sessionID: "child", messageID: message.id } })
    store.optimistic.add({ sessionID: "child", message, parts: [part] })
    pending.resolve(response([{ info: message, parts: [stale] }]))
    await refreshing

    expect(store.data.message.child).toEqual([message])
    expect(store.data.part[message.id]).toEqual([part])

    await store.sync("child", { force: true })
    expect(store.data.message.child).toEqual([message])
    expect(store.data.part[message.id]).toEqual([part])
  })

  test("drops stale event content omitted by a complete initial page", async () => {
    const stale = userMessage("stale")
    const store = createServerSession(messageClient(response()))
    store.apply({ type: "message.updated", properties: { info: stale } })

    await store.sync("child")

    expect(store.data.message.child).toEqual([])
  })

  test("preserves event content outside an incomplete initial page", async () => {
    const live = userMessage("message-1")
    const fetched = userMessage("message-2", { time: { created: 2 } })
    const store = createServerSession(messageClient(response([{ info: fetched, parts: [] }], "older")))
    store.apply({ type: "message.updated", properties: { info: live } })

    await store.sync("child")

    expect(store.data.message.child).toEqual([live, fetched])
  })

  test("does not restore removed optimistic content on refresh", async () => {
    const message = userMessage("message")
    const part = textPart(message.id, { text: "removed" })
    const kept = { ...message, id: "kept" }
    const keptPart = { ...part, id: "kept-part", messageID: kept.id }
    const store = createServerSession(messageClient(response([{ info: kept, parts: [] }])))
    store.optimistic.add({ sessionID: "child", message, parts: [part] })
    store.optimistic.add({ sessionID: "child", message: kept, parts: [keptPart] })

    store.apply({ type: "message.removed", properties: { sessionID: "child", messageID: message.id } })
    store.apply({
      type: "message.part.removed",
      properties: { sessionID: "child", messageID: kept.id, partID: keptPart.id },
    })
    await store.sync("child", { force: true })

    expect(store.data.message.child).toEqual([kept])
    expect(store.data.part[message.id]).toBeUndefined()
    expect(store.data.part[kept.id]).toBeUndefined()
  })

  test("replaces confirmed optimistic content with the initial page", async () => {
    const optimistic = userMessage("message")
    const fetched = { ...optimistic, time: { created: 2 } }
    const store = createServerSession(messageClient(response([{ info: fetched, parts: [] }])))
    store.optimistic.add({ sessionID: "child", message: optimistic, parts: [] })

    await store.sync("child")

    expect(store.data.message.child).toEqual([fetched])
  })

  test("replaces a confirmed optimistic part with fetched content", async () => {
    const pending = deferredResponse()
    const message = userMessage("message")
    const optimistic = textPart(message.id, { text: "optimistic" })
    const fetched = { ...optimistic, text: "fetched" }
    const store = createServerSession(messageClient(pending.promise))
    const loading = store.sync("child")

    store.optimistic.add({ sessionID: "child", message, parts: [optimistic] })
    pending.resolve(response([{ info: message, parts: [fetched] }]))
    await loading

    expect(store.data.part[message.id]).toEqual([fetched])
  })

  test("rolls back only unconfirmed optimistic parts", async () => {
    const pending = deferredResponse()
    const message = userMessage("message")
    const confirmed = textPart(message.id, { id: "confirmed", text: "confirmed" })
    const pendingPart = textPart(message.id, { id: "pending", text: "pending" })
    const store = createServerSession(messageClient(pending.promise))
    const loading = store.sync("child")
    store.optimistic.add({ sessionID: "child", message, parts: [confirmed, pendingPart] })

    pending.resolve(response([{ info: message, parts: [confirmed] }]))
    await loading
    store.optimistic.remove({ sessionID: "child", messageID: message.id })

    expect(store.data.message.child).toEqual([message])
    expect(store.data.part[message.id]).toEqual([confirmed])
  })

  test("updates confirmed optimistic parts from later pages", async () => {
    const message = userMessage("message")
    const confirmed = textPart(message.id, { id: "confirmed", text: "first" })
    const updated = { ...confirmed, text: "updated" }
    const pendingPart = textPart(message.id, { id: "pending", text: "pending" })
    const store = createServerSession(
      messageClient(response([{ info: message, parts: [confirmed] }]), response([{ info: message, parts: [updated] }])),
    )
    store.optimistic.add({ sessionID: "child", message, parts: [confirmed, pendingPart] })
    await store.sync("child")

    await store.sync("child", { force: true })
    store.optimistic.remove({ sessionID: "child", messageID: message.id })

    expect(store.data.part[message.id]).toEqual([updated])
  })

  test("does not restore a confirmed optimistic part after its removal event", async () => {
    const message = userMessage("message")
    const confirmed = textPart(message.id, { id: "confirmed", text: "confirmed" })
    const pendingPart = textPart(message.id, { id: "pending", text: "pending" })
    const store = createServerSession(
      messageClient(response([{ info: message, parts: [confirmed] }]), response([{ info: message, parts: [] }])),
    )
    store.optimistic.add({ sessionID: "child", message, parts: [confirmed, pendingPart] })
    await store.sync("child")
    store.apply({
      type: "message.part.removed",
      properties: { sessionID: "child", messageID: message.id, partID: confirmed.id },
    })

    await store.sync("child", { force: true })

    expect(store.data.part[message.id]).toEqual([pendingPart])
  })

  test("clears delta buffers when removing optimistic content", () => {
    const message = userMessage("message")
    const part = textPart(message.id, { text: "optimistic" })
    const store = setup({ child: session("child") }).store
    store.optimistic.add({ sessionID: "child", message, parts: [part] })
    store.apply({
      type: "message.part.delta",
      properties: { sessionID: "child", messageID: message.id, partID: part.id, field: "text", delta: " delta" },
    })

    store.optimistic.remove({ sessionID: "child", messageID: message.id })

    expect(store.data.part[message.id]).toBeUndefined()
    expect(store.data.part_text_accum_delta[part.id]).toBeUndefined()
  })

  test("does not remove content confirmed by a message event", () => {
    const message = userMessage("message")
    const part = textPart(message.id)
    const store = setup({ child: session("child") }).store
    store.optimistic.add({ sessionID: "child", message, parts: [part] })
    store.apply({ type: "message.updated", properties: { sessionID: "child", info: message } })

    store.optimistic.remove({ sessionID: "child", messageID: message.id })

    expect(store.data.message.child).toEqual([message])
    expect(store.data.part[message.id]).toBeUndefined()
  })

  test("does not remove parts confirmed by part events", () => {
    const message = userMessage("message")
    const part = textPart(message.id)
    const store = setup({ child: session("child") }).store
    store.optimistic.add({ sessionID: "child", message, parts: [part] })
    store.apply({ type: "message.updated", properties: { sessionID: "child", info: message } })
    store.apply({ type: "message.part.updated", properties: { sessionID: "child", part, time: 2 } })

    store.optimistic.remove({ sessionID: "child", messageID: message.id })

    expect(store.data.message.child).toEqual([message])
    expect(store.data.part[message.id]).toEqual([part])
  })

  test("treats a part event as confirmation when it precedes the message event", () => {
    const message = userMessage("message")
    const part = textPart(message.id)
    const store = setup({ child: session("child") }).store
    store.optimistic.add({ sessionID: "child", message, parts: [part] })
    store.apply({ type: "message.part.updated", properties: { sessionID: "child", part, time: 2 } })

    store.optimistic.remove({ sessionID: "child", messageID: message.id })

    expect(store.data.message.child).toEqual([message])
    expect(store.data.part[message.id]).toEqual([part])
  })

  test("clears stale parts when the initial page has none", async () => {
    const pending = deferredResponse()
    const message = userMessage("message")
    const part = textPart(message.id, { text: "stale" })
    const store = createServerSession(messageClient(pending.promise))
    store.apply({ type: "message.updated", properties: { info: message } })
    store.apply({ type: "message.part.updated", properties: { sessionID: "child", part, time: 1 } })
    const loading = store.sync("child")

    pending.resolve(response([{ info: message, parts: [] }]))
    await loading

    expect(store.data.part[message.id]).toBeUndefined()
  })

  test("clears delta buffers for parts omitted by the initial page", async () => {
    const pending = deferredResponse()
    const message = userMessage("message")
    const kept = textPart(message.id, { id: "part-1", text: "kept" })
    const removed: Part = { ...kept, id: "part-2", text: "removed" }
    const store = createServerSession(messageClient(pending.promise))
    store.apply({ type: "message.updated", properties: { info: message } })
    store.apply({ type: "message.part.updated", properties: { sessionID: "child", part: kept, time: 1 } })
    store.apply({ type: "message.part.updated", properties: { sessionID: "child", part: removed, time: 1 } })
    store.apply({
      type: "message.part.delta",
      properties: { sessionID: "child", messageID: message.id, partID: removed.id, field: "text", delta: " delta" },
    })
    const loading = store.sync("child")

    pending.resolve(response([{ info: message, parts: [kept] }]))
    await loading

    expect(store.data.part[message.id]).toEqual([kept])
    expect(store.data.part_text_accum_delta[removed.id]).toBeUndefined()
  })

  test("clears a stale delta buffer when a refresh replaces its part", async () => {
    const message = userMessage("message")
    const stale = textPart(message.id, { text: "stale" })
    const fetched = { ...stale, text: "fetched" }
    const store = createServerSession(
      messageClient(response([{ info: message, parts: [stale] }]), response([{ info: message, parts: [fetched] }])),
    )
    await store.sync("child")
    store.apply({
      type: "message.part.delta",
      properties: { sessionID: "child", messageID: message.id, partID: stale.id, field: "text", delta: " delta" },
    })

    await store.sync("child", { force: true })

    expect(store.data.part[message.id]).toEqual([fetched])
    expect(store.data.part_text_accum_delta[stale.id]).toBeUndefined()
  })

  test("preserves a non-durable delta received before refresh", async () => {
    const message = userMessage("message")
    const part = textPart(message.id, { text: "stale" })
    const store = createServerSession(
      messageClient(response([{ info: message, parts: [part] }]), response([{ info: message, parts: [{ ...part }] }])),
    )
    await store.sync("child")
    store.apply({
      type: "message.part.delta",
      properties: { sessionID: "child", messageID: message.id, partID: part.id, field: "text", delta: " delta" },
    })

    await store.sync("child", { force: true })

    expect(store.data.part[message.id]).toEqual([{ ...part, text: "stale delta" }])
    expect(store.data.part_text_accum_delta[part.id]).toBe("stale delta")
  })

  test("accepts fetched text that intentionally replaces an accumulated prefix", async () => {
    const message = userMessage("message")
    const part = textPart(message.id, { text: "abc" })
    const fetched = { ...part, text: "ab" }
    const store = createServerSession(
      messageClient(response([{ info: message, parts: [part] }]), response([{ info: message, parts: [fetched] }])),
    )
    await store.sync("child")
    store.apply({
      type: "message.part.delta",
      properties: { sessionID: "child", messageID: message.id, partID: part.id, field: "text", delta: "def" },
    })

    await store.sync("child", { force: true })

    expect(store.data.part[message.id]).toEqual([fetched])
    expect(store.data.part_text_accum_delta[part.id]).toBeUndefined()
  })

  test("preserves an unpersisted delta suffix after partial server catch-up", async () => {
    const message = userMessage("message")
    const part = textPart(message.id, { text: "a" })
    const fetched = { ...part, text: "ab" }
    const store = createServerSession(
      messageClient(response([{ info: message, parts: [part] }]), response([{ info: message, parts: [fetched] }])),
    )
    await store.sync("child")
    store.apply({
      type: "message.part.delta",
      properties: { sessionID: "child", messageID: message.id, partID: part.id, field: "text", delta: "bc" },
    })

    await store.sync("child", { force: true })

    expect(store.data.part[message.id]).toEqual([{ ...part, text: "abc" }])
    expect(store.data.part_text_accum_delta[part.id]).toBe("abc")
  })

  test("clears delta state after exact server catch-up", async () => {
    const message = userMessage("message")
    const part = textPart(message.id, { text: "a" })
    const fetched = { ...part, text: "ab" }
    const store = createServerSession(
      messageClient(response([{ info: message, parts: [part] }]), response([{ info: message, parts: [fetched] }])),
    )
    await store.sync("child")
    store.apply({
      type: "message.part.delta",
      properties: { sessionID: "child", messageID: message.id, partID: part.id, field: "text", delta: "b" },
    })

    await store.sync("child", { force: true })

    expect(store.data.part[message.id]).toEqual([fetched])
    expect(store.data.part_text_accum_delta[part.id]).toBeUndefined()
  })

  test("uses the successful retry response over events from a failed attempt", async () => {
    const failed = Promise.withResolvers<MessageResponse>()
    const retried = Promise.withResolvers<MessageResponse>()
    const message = userMessage("message")
    const stale = textPart(message.id, { text: "stale" })
    const intermediate = { ...stale, text: "intermediate" }
    const fetched = { ...stale, text: "fetched" }
    const client = messageClient(failed.promise, retried.promise)
    const store = createServerSession(client, { retry: retryImmediately })
    store.apply({ type: "message.updated", properties: { info: message } })
    store.apply({ type: "message.part.updated", properties: { sessionID: "child", part: stale, time: 1 } })
    const loading = store.sync("child")

    store.apply({ type: "message.part.updated", properties: { sessionID: "child", part: intermediate, time: 2 } })
    failed.reject(new Error("failed to fetch"))
    await client.requested(2)
    retried.resolve(response([{ info: message, parts: [fetched] }]))
    await loading

    expect(store.data.part[message.id]).toEqual([fetched])
  })

  test("preserves non-durable deltas across message retries", async () => {
    const failed = Promise.withResolvers<MessageResponse>()
    const retried = Promise.withResolvers<MessageResponse>()
    const message = userMessage("message")
    const part = textPart(message.id, { text: "stale" })
    const client = messageClient(failed.promise, retried.promise)
    const store = createServerSession(client, { retry: retryImmediately })
    store.apply({ type: "message.updated", properties: { info: message } })
    store.apply({ type: "message.part.updated", properties: { sessionID: "child", part, time: 1 } })
    const loading = store.sync("child")

    store.apply({
      type: "message.part.delta",
      properties: { sessionID: "child", messageID: message.id, partID: part.id, field: "text", delta: " delta" },
    })
    failed.reject(new Error("failed to fetch"))
    await client.requested(2)
    retried.resolve(response([{ info: message, parts: [part] }]))
    await loading

    expect(store.data.part[message.id]).toEqual([{ ...part, text: "stale delta" }])
  })

  test("preserves part removals across message retries", async () => {
    const failed = Promise.withResolvers<MessageResponse>()
    const retried = Promise.withResolvers<MessageResponse>()
    const message = userMessage("message")
    const part = textPart(message.id)
    const client = messageClient(response([{ info: message, parts: [part] }]), failed.promise, retried.promise)
    const store = createServerSession(client, { retry: retryImmediately })
    await store.sync("child")
    const loading = store.sync("child", { force: true })

    store.apply({
      type: "message.part.removed",
      properties: { sessionID: "child", messageID: message.id, partID: part.id },
    })
    failed.reject(new Error("failed to fetch"))
    await client.requested(3)
    retried.resolve(response([{ info: message, parts: [part] }]))
    await loading

    expect(store.data.part[message.id]).toBeUndefined()
  })

  test("preserves message removals across message retries", async () => {
    const failed = Promise.withResolvers<MessageResponse>()
    const retried = Promise.withResolvers<MessageResponse>()
    const message = userMessage("message")
    const part = textPart(message.id)
    const client = messageClient(response([{ info: message, parts: [part] }]), failed.promise, retried.promise)
    const store = createServerSession(client, { retry: retryImmediately })
    await store.sync("child")
    const loading = store.sync("child", { force: true })

    store.apply({ type: "message.removed", properties: { sessionID: "child", messageID: message.id } })
    failed.reject(new Error("failed to fetch"))
    await client.requested(3)
    retried.resolve(response([{ info: message, parts: [part] }]))
    await loading

    expect(store.data.message.child).toEqual([])
    expect(store.data.part[message.id]).toBeUndefined()
  })

  test("preserves optimistic re-adds across message retries", async () => {
    const failed = Promise.withResolvers<MessageResponse>()
    const retried = Promise.withResolvers<MessageResponse>()
    const message = userMessage("message")
    const stale = textPart(message.id, { id: "stale", text: "stale" })
    const optimistic = textPart(message.id, { id: "optimistic", text: "optimistic" })
    const client = messageClient(response([{ info: message, parts: [stale] }]), failed.promise, retried.promise)
    const store = createServerSession(client, { retry: retryImmediately })
    await store.sync("child")
    const loading = store.sync("child", { force: true })

    store.apply({ type: "message.removed", properties: { sessionID: "child", messageID: message.id } })
    store.optimistic.add({ sessionID: "child", message, parts: [optimistic] })
    failed.reject(new Error("failed to fetch"))
    await client.requested(3)
    retried.resolve(response([{ info: message, parts: [stale] }]))
    await loading

    expect(store.data.message.child).toEqual([message])
    expect(store.data.part[message.id]).toEqual([optimistic])
  })

  test("accepts part omission from a successful retry after an earlier delta", async () => {
    const failed = Promise.withResolvers<MessageResponse>()
    const retried = Promise.withResolvers<MessageResponse>()
    const message = userMessage("message")
    const part = textPart(message.id)
    const client = messageClient(response([{ info: message, parts: [part] }]), failed.promise, retried.promise)
    const store = createServerSession(client, { retry: retryImmediately })
    await store.sync("child")
    const loading = store.sync("child", { force: true })

    store.apply({
      type: "message.part.delta",
      properties: { sessionID: "child", messageID: message.id, partID: part.id, field: "text", delta: " delta" },
    })
    failed.reject(new Error("failed to fetch"))
    await client.requested(3)
    retried.resolve(response([{ info: message, parts: [] }]))
    await loading

    expect(store.data.part[message.id]).toBeUndefined()
    expect(store.data.part_text_accum_delta[part.id]).toBeUndefined()
  })

  test("clears load-owned orphan parts when all retries fail", async () => {
    const first = Promise.withResolvers<MessageResponse>()
    const second = Promise.withResolvers<MessageResponse>()
    const third = Promise.withResolvers<MessageResponse>()
    const message = userMessage("message")
    const part = textPart(message.id)
    const client = messageClient(first.promise, second.promise, third.promise)
    const store = createServerSession(client, { retry: retryImmediately })
    const loading = store.sync("child").catch((error) => error)

    store.apply({ type: "message.part.updated", properties: { sessionID: "child", part, time: 2 } })
    first.reject(new Error("failed to fetch"))
    await client.requested(2)
    second.reject(new Error("failed to fetch"))
    await client.requested(3)
    third.reject(new Error("failed to fetch"))
    await loading

    expect(store.data.part[message.id]).toBeUndefined()
  })

  test("preserves live updates during a forced refresh", async () => {
    const pending = deferredResponse()
    const stale = userMessage("message")
    const stalePart = textPart(stale.id, { text: "stale" })
    const store = createServerSession(messageClient(response([{ info: stale, parts: [stalePart] }]), pending.promise))
    await store.sync("child")
    const refreshing = store.sync("child", { force: true })
    const live = { ...stale, time: { created: 2 } }

    store.apply({ type: "message.updated", properties: { info: live } })
    store.apply({
      type: "message.part.delta",
      properties: { sessionID: "child", messageID: stale.id, partID: stalePart.id, field: "text", delta: " live" },
    })
    pending.resolve(response([{ info: stale, parts: [stalePart] }]))
    await refreshing

    expect(store.data.message.child).toEqual([live])
    expect(store.data.part[stale.id]).toEqual([{ ...stalePart, text: "stale live" }])
  })

  test("keeps fetched message metadata when only a part changes", async () => {
    const pending = deferredResponse()
    const stale = userMessage("message")
    const fetched = { ...stale, time: { created: 2 } }
    const part = textPart(stale.id, { text: "stale" })
    const store = createServerSession(messageClient(response([{ info: stale, parts: [part] }]), pending.promise))
    await store.sync("child")
    const refreshing = store.sync("child", { force: true })

    store.apply({
      type: "message.part.delta",
      properties: { sessionID: "child", messageID: stale.id, partID: part.id, field: "text", delta: " live" },
    })
    pending.resolve(response([{ info: fetched, parts: [part] }]))
    await refreshing

    expect(store.data.message.child).toEqual([fetched])
    expect(store.data.part[stale.id]).toEqual([{ ...part, text: "stale live" }])
  })

  test("preserves a part update when a forced refresh omits its message", async () => {
    const pending = deferredResponse()
    const message = userMessage("message")
    const stale = textPart(message.id, { text: "stale" })
    const live = { ...stale, text: "live" }
    const store = createServerSession(messageClient(response([{ info: message, parts: [stale] }]), pending.promise))
    await store.sync("child")
    const refreshing = store.sync("child", { force: true })

    store.apply({ type: "message.part.updated", properties: { sessionID: "child", part: live, time: 2 } })
    pending.resolve(response())
    await refreshing

    expect(store.data.message.child).toEqual([message])
    expect(store.data.part[message.id]).toEqual([live])
  })

  test("ignores a late part update after its message is removed", async () => {
    const pending = deferredResponse()
    const message = userMessage("message")
    const part = textPart(message.id)
    const store = createServerSession(messageClient(pending.promise))
    const loading = store.sync("child")

    store.apply({ type: "message.updated", properties: { info: message } })
    store.apply({ type: "message.removed", properties: { sessionID: "child", messageID: message.id } })
    store.apply({ type: "message.part.updated", properties: { sessionID: "child", part, time: 2 } })
    pending.resolve(response([{ info: message, parts: [part] }]))
    await loading

    expect(store.data.message.child).toEqual([])
    expect(store.data.part[message.id]).toBeUndefined()
  })

  test("ignores a late part update after a completed message removal", () => {
    const message = userMessage("message")
    const part = textPart(message.id)
    const store = setup({ child: session("child") }).store
    store.apply({ type: "message.updated", properties: { info: message } })
    store.apply({ type: "message.removed", properties: { sessionID: "child", messageID: message.id } })

    store.apply({ type: "message.part.updated", properties: { sessionID: "child", part, time: 2 } })

    expect(store.data.part[message.id]).toBeUndefined()
  })

  test("does not restore a completed message removal from a stale refresh", async () => {
    const message = userMessage("message")
    const part = textPart(message.id)
    const store = createServerSession(
      messageClient(response([{ info: message, parts: [part] }]), response([{ info: message, parts: [part] }])),
    )
    await store.sync("child")
    store.apply({ type: "message.removed", properties: { sessionID: "child", messageID: message.id } })

    await store.sync("child", { force: true })

    expect(store.data.message.child).toEqual([])
    expect(store.data.part[message.id]).toBeUndefined()
  })

  test("does not restore a completed part removal from a stale refresh", async () => {
    const message = userMessage("message")
    const part = textPart(message.id)
    const store = createServerSession(
      messageClient(response([{ info: message, parts: [part] }]), response([{ info: message, parts: [part] }])),
    )
    await store.sync("child")
    store.apply({
      type: "message.part.removed",
      properties: { sessionID: "child", messageID: message.id, partID: part.id },
    })

    await store.sync("child", { force: true })

    expect(store.data.part[message.id]).toBeUndefined()
  })

  test("does not cache skipped optimistic parts", () => {
    const message = userMessage("message")
    const part = { id: "part", sessionID: "child", messageID: message.id, type: "step-start" as const }
    const store = setup({ child: session("child") }).store

    store.optimistic.add({ sessionID: "child", message, parts: [part] })

    expect(store.data.part[message.id]).toEqual([])
  })

  test("clears stale delta buffers when replacing optimistic parts", () => {
    const message = userMessage("message")
    const stale = textPart(message.id, { id: "stale", text: "stale" })
    const optimistic = textPart(message.id, { id: "optimistic", text: "optimistic" })
    const store = setup({ child: session("child") }).store
    store.optimistic.add({ sessionID: "child", message, parts: [stale] })
    store.apply({
      type: "message.part.delta",
      properties: { sessionID: "child", messageID: message.id, partID: stale.id, field: "text", delta: " delta" },
    })

    store.optimistic.add({ sessionID: "child", message, parts: [optimistic] })

    expect(store.data.part_text_accum_delta[stale.id]).toBeUndefined()
    expect(store.data.part_text_accum_delta[optimistic.id]).toBeUndefined()
  })

  test("preserves removals during history prepend", async () => {
    const pending = deferredResponse()
    const latest = userMessage("message-2", { time: { created: 2 } })
    const older = { ...latest, id: "message-1", time: { created: 1 } }
    const store = createServerSession(messageClient(response([{ info: latest, parts: [] }], "older"), pending.promise))
    await store.sync("child")
    const loading = store.history.loadMore("child")

    store.apply({ type: "message.removed", properties: { sessionID: "child", messageID: older.id } })
    pending.resolve(response([{ info: older, parts: [] }]))
    await loading

    expect(store.data.message.child).toEqual([latest])
  })

  test("does not scan cached messages for user roots during history prepend", async () => {
    const guard = { active: false }
    const latest = new Proxy(userMessage("message-2", { time: { created: 2 } }), {
      get(target, property, receiver) {
        if (guard.active && property === "role") throw new Error("cached role accessed")
        return Reflect.get(target, property, receiver)
      },
    })
    const older = userMessage("message-1")
    const store = createServerSession(
      messageClient(response([{ info: latest, parts: [] }], "older"), response([{ info: older, parts: [] }])),
    )
    await store.sync("child")
    guard.active = true

    await store.history.loadMore("child")

    expect(store.data.message.child).toEqual([older, latest])
  })

  for (const cursor of [undefined, "cached-older-cursor"]) {
    test(`preserves ${cursor ? "unfinished" : "complete"} pagination when a bounded refresh retains older history`, async () => {
      const older = userMessage("message-1")
      const latest = userMessage("message-2", { time: { created: 2 } })
      const client = messageClient(
        response([{ info: older, parts: [] }, { info: latest, parts: [] }], cursor),
        response([{ info: latest, parts: [] }], "recent-tail-cursor"),
        response(),
      )
      const store = createServerSession(client)
      await store.sync("child")

      await store.sync("child", { force: true, messageLimit: 1 })

      expect(store.data.message.child).toEqual([older, latest])
      expect(store.history.more("child")).toBe(cursor !== undefined)
      await store.history.loadMore("child")
      expect(client.requests).toEqual([
        { sessionID: "child", limit: 20, before: undefined },
        { sessionID: "child", limit: 1, before: undefined },
        ...(cursor ? [{ sessionID: "child", limit: 200, before: cursor }] : []),
      ])
    })
  }

  test("uses new pagination when a bounded refresh replaces the cached prefix", async () => {
    const cached = userMessage("message-2", { time: { created: 2 } })
    const replacement = userMessage("message-1")
    const client = messageClient(
      response([{ info: cached, parts: [] }], "cached-older-cursor"),
      response([{ info: replacement, parts: [] }], "replacement-cursor"),
      response(),
    )
    const store = createServerSession(client)
    await store.sync("child")

    await store.sync("child", { force: true, messageLimit: 1 })

    expect(store.data.message.child).toEqual([replacement])
    await store.history.loadMore("child")
    expect(client.requests.at(-1)).toEqual({ sessionID: "child", limit: 200, before: "replacement-cursor" })
  })

  test("uses new pagination when a bounded refresh only backfills a cached parent", async () => {
    const parent = userMessage("message-1")
    const assistant = assistantMessage("message-2", parent.id)
    const client = rootMessageClient(
      [
        response([{ info: parent, parts: [] }, { info: assistant, parts: [] }]),
        response([{ info: assistant, parts: [] }], "recent-tail-cursor"),
        response(),
      ],
      [singleResponse(parent)],
    )
    const store = createServerSession(client)
    await store.sync("child")

    await store.sync("child", { force: true, messageLimit: 1 })

    expect(store.history.more("child")).toBe(true)
    await store.history.loadMore("child")
    expect(client.requests.at(-1)).toEqual({ sessionID: "child", limit: 200, before: "recent-tail-cursor" })
  })

  test("preserves loaded history during an incomplete refresh", async () => {
    const older = userMessage("message-1")
    const latest = userMessage("message-2", { time: { created: 2 } })
    const fresh = userMessage("message-3", { time: { created: 3 } })
    const store = createServerSession(
      messageClient(
        response(
          [
            { info: older, parts: [] },
            { info: latest, parts: [] },
          ],
          "older",
        ),
        response(
          [
            { info: latest, parts: [] },
            { info: fresh, parts: [] },
          ],
          "older",
        ),
      ),
    )
    await store.sync("child")

    await store.sync("child", { force: true })

    expect(store.data.message.child).toEqual([older, latest, fresh])
  })

  test("drops stale recent messages omitted by an incomplete refresh", async () => {
    const third = userMessage("message-3", { time: { created: 3 } })
    const fourth = userMessage("message-4", { time: { created: 4 } })
    const stale = userMessage("message-5", { time: { created: 5 } })
    const store = createServerSession(
      messageClient(
        response(
          [
            { info: fourth, parts: [] },
            { info: stale, parts: [] },
          ],
          "older",
        ),
        response(
          [
            { info: third, parts: [] },
            { info: fourth, parts: [] },
          ],
          "older",
        ),
      ),
    )
    await store.sync("child")

    await store.sync("child", { force: true })

    expect(store.data.message.child).toEqual([third, fourth])
  })

  test("uses message creation time for incomplete refresh boundaries", async () => {
    const older = userMessage("msg_z", { time: { created: 1 } })
    const boundary = userMessage("msg_m", { time: { created: 2 } })
    const stale = userMessage("msg_a", { time: { created: 3 } })
    const store = createServerSession(
      messageClient(
        response(
          [
            { info: older, parts: [] },
            { info: stale, parts: [] },
          ],
          "older",
        ),
        response([{ info: boundary, parts: [] }], "older"),
      ),
    )
    await store.sync("child")

    await store.sync("child", { force: true })

    expect(store.data.message.child).toEqual([older, boundary])
  })

  test("preserves a part update for a message being loaded from history", async () => {
    const pending = deferredResponse()
    const latest = userMessage("message-2", { time: { created: 2 } })
    const older = userMessage("message-1")
    const stale = textPart(older.id, { text: "stale" })
    const live = { ...stale, text: "live" }
    const store = createServerSession(messageClient(response([{ info: latest, parts: [] }], "older"), pending.promise))
    await store.sync("child")
    const loading = store.history.loadMore("child")

    store.apply({ type: "message.part.updated", properties: { sessionID: "child", part: live, time: 2 } })
    pending.resolve(response([{ info: older, parts: [stale] }]))
    await loading

    expect(store.data.part[older.id]).toEqual([live])
  })

  test("does not clear newer orphan parts after terminal history prepend", async () => {
    const pending = deferredResponse()
    const latest = userMessage("message-2", { time: { created: 2 } })
    const older = userMessage("message-1")
    const newer = userMessage("message-3", { time: { created: 3 } })
    const part = textPart(newer.id, { text: "live" })
    const store = createServerSession(messageClient(response([{ info: latest, parts: [] }], "older"), pending.promise))
    await store.sync("child")
    const loading = store.history.loadMore("child")

    store.apply({ type: "message.part.updated", properties: { sessionID: "child", part, time: 3 } })
    pending.resolve(response([{ info: older, parts: [] }]))
    await loading
    store.apply({ type: "message.updated", properties: { sessionID: "child", info: newer } })

    expect(store.data.part[newer.id]).toEqual([part])
  })

  test("accepts an authoritative history part after an earlier unknown-parent update", async () => {
    const pending = deferredResponse()
    const history = deferredResponse()
    const latest = userMessage("message-2", { time: { created: 2 } })
    const older = userMessage("message-1")
    const part = textPart(older.id, { text: "live" })
    const store = createServerSession(messageClient(pending.promise, history.promise))
    const loading = store.sync("child")

    store.apply({ type: "message.part.updated", properties: { sessionID: "child", part, time: 2 } })
    pending.resolve(response([{ info: latest, parts: [] }], "older"))
    await loading

    expect(store.data.part[older.id]).toEqual([part])

    const loadingHistory = store.history.loadMore("child")
    history.resolve(response([{ info: older, parts: [{ ...part, text: "stale" }] }]))
    await loadingHistory

    expect(store.data.part[older.id]).toEqual([{ ...part, text: "stale" }])
  })

  test("preserves an unknown-parent part removal across pages", async () => {
    const initial = deferredResponse()
    const history = deferredResponse()
    const latest = userMessage("message-2", { time: { created: 2 } })
    const older = userMessage("message-1")
    const part = textPart(older.id)
    const store = createServerSession(messageClient(initial.promise, history.promise))
    const loading = store.sync("child")

    store.apply({
      type: "message.part.removed",
      properties: { sessionID: "child", messageID: older.id, partID: part.id },
    })
    initial.resolve(response([{ info: latest, parts: [] }], "older"))
    await loading
    const loadingHistory = store.history.loadMore("child")
    history.resolve(response([{ info: older, parts: [part] }]))
    await loadingHistory

    expect(store.data.part[older.id]).toBeUndefined()
  })

  test("clears orphaned parts when a refresh drops a message", async () => {
    const message = userMessage("message")
    const part = textPart(message.id, { text: "stale" })
    const store = createServerSession(messageClient(response([{ info: message, parts: [part] }]), response()))
    await store.sync("child")
    store.apply({
      type: "message.part.delta",
      properties: { sessionID: "child", messageID: message.id, partID: part.id, field: "text", delta: " delta" },
    })
    await store.sync("child", { force: true })

    expect(store.data.message.child).toEqual([])
    expect(store.data.part[message.id]).toBeUndefined()
    expect(store.data.part_text_accum_delta[part.id]).toBeUndefined()
  })

  test("applies events without a directory store", () => {
    const ctx = setup({})
    ctx.store.apply({ type: "session.created", properties: { sessionID: "root", info: session("root") } })
    ctx.store.apply({ type: "session.status", properties: { sessionID: "root", status: { type: "busy" } } })

    expect(ctx.store.get("root")?.directory).toBe("/repo")
    expect(ctx.store.data.session_working("root")).toBe(true)
    expect(ctx.get).toEqual([])
  })

  test("preserves pinned session content under server-wide cache pressure", () => {
    const ctx = setup({})
    ctx.store.pin("active")
    ctx.store.optimistic.add({
      sessionID: "active",
      message: {
        id: "message",
        sessionID: "active",
        role: "assistant",
        time: { created: 1 },
        parentID: "parent",
        modelID: "model",
        providerID: "provider",
        mode: "build",
        agent: "agent",
        path: { cwd: "/repo", root: "/repo" },
        cost: 0,
        tokens: { input: 0, output: 0, reasoning: 0, cache: { read: 0, write: 0 } },
      },
      parts: [],
    })

    for (let index = 0; index < 50; index++) {
      ctx.store.remember(session(`session-${index}`))
      ctx.store.apply({
        type: "session.status",
        properties: { sessionID: `session-${index}`, status: { type: "idle" } },
      })
    }

    expect(ctx.store.data.message.active?.map((message) => message.id)).toEqual(["message"])
    expect(ctx.store.data.session_status["session-0"]).toBeUndefined()
  })
})

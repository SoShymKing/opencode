import { describe, expect, test } from "bun:test"
import type { OpencodeClient } from "@opencode-ai/sdk/v2/client"
import type {
  McpListInput,
  McpResourceCatalogInput,
  SessionApi,
  SessionInfo,
  SessionListInput,
} from "@opencode-ai/client/promise"
import { QueryClient } from "@tanstack/solid-query"
import { canDisposeDirectory, pickDirectoriesToEvict } from "./global-sync/eviction"
import { estimateRootSessionTotal, loadRootSessions } from "./global-sync/session-load"
import { loadActiveSessionsQuery, loadMcpQuery, loadMcpResourcesQuery, seedActiveSessionStatuses, recoverSessionConnection, createNativeSessionPoller } from "./server-sync"
import { ServerScope } from "@/utils/server-scope"
import { createServerSession } from "./server-session"
import type { ServerApi } from "@/utils/server"

type McpApi = ServerApi["mcp"]

describe("MCP queries", () => {
  test("loads current servers for the requested location", async () => {
    const calls: unknown[] = []
    const queryClient = new QueryClient()
    const result = await queryClient.fetchQuery(
      loadMcpQuery(ServerScope.local, "/project", {
        list: async (input: McpListInput = {}) => {
          calls.push(input)
          return {
            location: { directory: "/project", project: { id: "project", directory: "/project" } },
            data: [
              { name: "docs", status: { status: "connected" } },
              { name: "search", status: { status: "pending" } },
            ],
          }
        },
      } as unknown as McpApi),
    )

    expect(calls).toEqual([{ location: { directory: "/project" } }])
    expect(result).toEqual({ docs: { status: "connected" }, search: { status: "pending" } })
  })

  test("loads and keys the current resource catalog", async () => {
    const calls: unknown[] = []
    const queryClient = new QueryClient()
    const result = await queryClient.fetchQuery(
      loadMcpResourcesQuery(ServerScope.local, "/project", {
        resource: {
          catalog: async (input: McpResourceCatalogInput = {}) => {
            calls.push(input)
            return {
              location: { directory: "/project", project: { id: "project", directory: "/project" } },
              data: {
                resources: [{ server: "docs", name: "Guide", uri: "docs://guide" }],
                templates: [],
              },
            }
          },
        },
      } as unknown as McpApi),
    )

    expect(calls).toEqual([{ location: { directory: "/project" } }])
    expect(result).toEqual({ "docs:docs://guide": { server: "docs", name: "Guide", uri: "docs://guide" } })
  })
})

describe("active session query", () => {
  test("seeds fresh active presence while retained history is still loading", async () => {
    const session = createServerSession({} as OpencodeClient)
    session.set("message", "cached", [])
    const history = Promise.withResolvers<void>()
    const started = Promise.withResolvers<void>()
    const statuses = Promise.withResolvers<Record<string, { type: "busy" }>>()
    const read = Promise.withResolvers<void>()
    const recovery = recoverSessionConnection({
      session: { ...session, sync: async () => { started.resolve(); await history.promise } },
      statuses: async () => { const value = await statuses.promise; read.resolve(); return value },
      capture: session.snapshot.capture("session_status"),
    })
    await started.promise
    statuses.resolve({ active: { type: "busy" } })
    await read.promise
    await Promise.resolve()
    expect(session.data.session_status.active).toEqual({ type: "busy" })
    history.resolve()
    await recovery
  })

  test.each(["wire", "epoch", "local"])("settlement keeps newer %s writes while final history is loading", async (mode) => {
    const session = createServerSession({} as OpencodeClient)
    session.set("session_status", "running", { type: "busy" })
    const history = Promise.withResolvers<void>()
    const started = Promise.withResolvers<void>()
    const recovery = recoverSessionConnection({
      session: { ...session, sync: async () => { started.resolve(); await history.promise } },
      statuses: async () => ({}),
      capture: session.snapshot.capture("session_status"),
      settleOnly: true,
    })
    await started.promise
    if (mode === "wire") session.applyV2({ id: "evt_step", type: "session.next.step.started", data: { timestamp: 2, sessionID: "running", assistantMessageID: "msg_step", agent: "build", model: { id: "model", providerID: "provider" } } })
    if (mode === "epoch") session.snapshot.connect()
    if (mode === "local") session.set("session_status", "running", { type: "retry", attempt: 2, message: "newer", next: 10 })
    history.resolve()
    await recovery
    expect(session.data.session_status.running).toEqual(mode === "local" ? { type: "retry", attempt: 2, message: "newer", next: 10 } : { type: "busy" })
  })

  test("recovers off-page retained history before clearing absent busy state", async () => {
    const session = createServerSession({} as OpencodeClient)
    session.set("message", "cached", [])
    session.set("session_status", "cached", { type: "busy" })
    const history = Promise.withResolvers<void>()
    const started = Promise.withResolvers<void>()
    const calls: string[] = []
    const recovery = recoverSessionConnection({
      session: { ...session, sync: async (id, options) => { calls.push(id); expect(options?.force).toBe(true); started.resolve(); await history.promise } },
      statuses: async () => ({}),
      capture: session.snapshot.capture("session_status"),
    })
    await started.promise
    expect(session.data.session_status.cached).toEqual({ type: "busy" })
    history.resolve()
    await recovery
    expect(calls).toEqual(["cached"])
    expect(session.data.session_status.cached).toBeUndefined()
  })

  test("failed active reads still refresh retained history without clearing status", async () => {
    const session = createServerSession({} as OpencodeClient)
    session.set("message", "cached", [])
    session.set("session_status", "cached", { type: "retry", attempt: 1, message: "retry", next: 1 })
    const calls: string[] = []
    await expect(recoverSessionConnection({
      session: { ...session, sync: async (id) => { calls.push(id) } },
      statuses: async () => { throw new Error("offline") },
      capture: session.snapshot.capture("session_status"),
    })).rejects.toThrow("offline")
    expect(calls).toEqual(["cached"])
    expect(session.data.session_status.cached.type).toBe("retry")
  })

  test("native poller keeps one request, aborts on timeout, and cleans up", async () => {
    const tasks = new Map<() => void, number>()
    const pending = Promise.withResolvers<void>()
    const finished = Promise.withResolvers<void>()
    const signals: AbortSignal[] = []
    const poller = createNativeSessionPoller({
      needed: () => true,
      epoch: () => 1,
      poll: async (signal) => { signals.push(signal); await pending.promise; finished.resolve() },
      schedule: (task, ms) => { tasks.set(task, ms); return () => { tasks.delete(task) } },
    })
    poller.start()
    const tick = [...tasks].find(([, ms]) => ms === 1000)?.[0]
    expect(tick).toBeDefined()
    tick?.()
    poller.start()
    expect(signals.length).toBe(1)
    const timeout = [...tasks].find(([, ms]) => ms === 10000)?.[0]
    timeout?.()
    expect(signals[0].aborted).toBe(true)
    poller.dispose()
    pending.resolve()
    await finished.promise
    expect(tasks.size).toBe(0)
    expect(signals.length).toBe(1)
  })

  test("native poller retries failed membership reads only at the next fixed cadence", async () => {
    const tasks = new Map<() => void, number>()
    const scheduled = Promise.withResolvers<() => void>()
    let calls = 0
    const poller = createNativeSessionPoller({
      needed: () => true,
      epoch: () => 1,
      poll: async () => { calls++; throw new Error("offline") },
      schedule: (task, ms) => { tasks.set(task, ms); if (calls > 0 && ms === 1000) scheduled.resolve(task); return () => { tasks.delete(task) } },
    })
    poller.start()
    const tick = [...tasks].find(([, ms]) => ms === 1000)?.[0]
    tick?.()
    const retry = await scheduled.promise
    expect(calls).toBe(1)
    expect(tasks.get(retry)).toBe(1000)
    poller.dispose()
    retry()
    expect(calls).toBe(1)
    expect(tasks.size).toBe(0)
  })

  test("native poller aborts an old connection request before scheduling the next epoch", async () => {
    const tasks = new Map<() => void, number>()
    const scheduled = Promise.withResolvers<() => void>()
    const pending = Promise.withResolvers<void>()
    const signals: AbortSignal[] = []
    let epoch = 1
    const poller = createNativeSessionPoller({
      needed: () => true,
      epoch: () => epoch,
      poll: async (signal) => { signals.push(signal); await pending.promise },
      schedule: (task, ms) => { tasks.set(task, ms); if (epoch === 2 && ms === 1000) scheduled.resolve(task); return () => { tasks.delete(task) } },
    })
    poller.start()
    const tick = [...tasks].find(([, ms]) => ms === 1000)?.[0]
    tick?.()
    epoch = 2
    poller.reset()
    expect(signals[0].aborted).toBe(true)
    expect(signals.length).toBe(1)
    const next = await scheduled.promise
    next()
    expect(signals.length).toBe(2)
    poller.dispose()
    pending.resolve()
    expect(signals[1].aborted).toBe(true)
  })

  test("loads active sessions immediately and once per server cache", async () => {
    let calls = 0
    const queryClient = new QueryClient()
    const options = loadActiveSessionsQuery(ServerScope.local, {
      active: async () => {
        calls++
        return { ses_running: { type: "running" } }
      },
    })

    expect(await queryClient.fetchQuery(options)).toEqual({ ses_running: { type: "running" } })
    expect(await queryClient.fetchQuery(options)).toEqual({ ses_running: { type: "running" } })
    expect(calls).toBe(1)
    expect(options.enabled).toBe(true)
    expect([...options.queryKey]).toEqual([ServerScope.local, "activeSessions"])
  })

  test("does not overwrite statuses already written by events", () => {
    const session = createServerSession({} as OpencodeClient)
    session.set("session_status", "ses_retry", { type: "retry", attempt: 2, message: "retrying", next: 10 })

    seedActiveSessionStatuses(session, {
      ses_running: { type: "running" },
      ses_retry: { type: "running" },
    })

    expect(session.data.session_status.ses_running).toEqual({ type: "busy" })
    expect(session.data.session_status.ses_retry).toEqual({
      type: "retry",
      attempt: 2,
      message: "retrying",
      next: 10,
    })
  })
})

describe("pickDirectoriesToEvict", () => {
  test("keeps pinned stores and evicts idle stores", () => {
    const now = 5_000
    const picks = pickDirectoriesToEvict({
      stores: ["a", "b", "c", "d"],
      state: new Map([
        ["a", { lastAccessAt: 1_000 }],
        ["b", { lastAccessAt: 4_900 }],
        ["c", { lastAccessAt: 4_800 }],
        ["d", { lastAccessAt: 3_000 }],
      ]),
      pins: new Set(["a"]),
      max: 2,
      ttl: 1_500,
      now,
    })

    expect(picks).toEqual(["d", "c"])
  })
})

describe("loadRootSessions", () => {
  test("loads and normalizes a limited page of root sessions", async () => {
    const calls: SessionListInput[] = []

    const result = await loadRootSessions({
      api: {
        list: async (query = {}) => {
          calls.push(query)
          return { data: [sessionInfo("session-1")], cursor: {} }
        },
      } satisfies Pick<SessionApi, "list">,
      directory: "dir",
      limit: 10,
    })

    expect(result.data).toEqual([
      expect.objectContaining({ id: "session-1", directory: "dir", slug: "session-1", version: "" }),
    ])
    expect(result.limited).toBe(true)
    expect(calls).toEqual([{ directory: "dir", parentID: null, limit: 10, order: "desc" }])
  })

  test("propagates list failures", () => {
    expect(
      loadRootSessions({
        api: {
          list: async () => {
            throw new Error("failed")
          },
        } satisfies Pick<SessionApi, "list">,
        directory: "dir",
        limit: 25,
      }),
    ).rejects.toThrow("failed")
  })
})

function sessionInfo(id: string) {
  return {
    id,
    projectID: "project-1",
    agent: "build",
    model: { id: "model-1", providerID: "provider-1" },
    cost: 0,
    tokens: { input: 0, output: 0, reasoning: 0, cache: { read: 0, write: 0 } },
    time: { created: 1, updated: 1 },
    title: id,
    location: { directory: "dir" },
  } as SessionInfo
}

describe("estimateRootSessionTotal", () => {
  test("keeps exact total for full fetches", () => {
    expect(estimateRootSessionTotal({ count: 42, limit: 10, limited: false })).toBe(42)
  })

  test("marks has-more for full-limit limited fetches", () => {
    expect(estimateRootSessionTotal({ count: 10, limit: 10, limited: true })).toBe(11)
  })

  test("keeps exact total when limited fetch is under limit", () => {
    expect(estimateRootSessionTotal({ count: 9, limit: 10, limited: true })).toBe(9)
  })
})

describe("canDisposeDirectory", () => {
  test("rejects pinned or inflight directories", () => {
    expect(
      canDisposeDirectory({
        directory: "dir",
        hasStore: true,
        pinned: true,
        booting: false,
        loadingSessions: false,
      }),
    ).toBe(false)
    expect(
      canDisposeDirectory({
        directory: "dir",
        hasStore: true,
        pinned: false,
        booting: true,
        loadingSessions: false,
      }),
    ).toBe(false)
    expect(
      canDisposeDirectory({
        directory: "dir",
        hasStore: true,
        pinned: false,
        booting: false,
        loadingSessions: true,
      }),
    ).toBe(false)
  })

  test("accepts idle unpinned directory store", () => {
    expect(
      canDisposeDirectory({
        directory: "dir",
        hasStore: true,
        pinned: false,
        booting: false,
        loadingSessions: false,
      }),
    ).toBe(true)
  })
})

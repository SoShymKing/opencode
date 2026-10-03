import { describe, expect, test } from "bun:test"
import type { OpencodeClient } from "@opencode-ai/sdk/v2/client"
import { createOpencodeClient } from "@opencode-ai/sdk/v2/client"
import type {
  McpListInput,
  McpResourceCatalogInput,
  SessionApi,
  SessionInfo,
  SessionListInput,
} from "@opencode-ai/client/promise"
import { QueryClient } from "@tanstack/solid-query"
import { createGlobalEmitter } from "@solid-primitives/event-bus"
import { canDisposeDirectory, pickDirectoriesToEvict } from "./global-sync/eviction"
import { estimateRootSessionTotal, loadRootSessions } from "./global-sync/session-load"
import { loadActiveSessionsQuery, loadMcpQuery, loadMcpResourcesQuery, seedActiveSessionStatuses, resyncServerSessions } from "./server-sync"
import { ServerScope } from "@/utils/server-scope"
import { createServerSession } from "./server-session"
import type { ServerApi } from "@/utils/server"
import { createApiForServer } from "@/utils/server"
import { createCompatibleApi } from "@/utils/server-compat"
import type { ServerEvent } from "./server-sdk"

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
  test("reconnect native wire snapshot keeps full status through the installed client bridge", async () => {
    const fetch = Object.assign(async () => Response.json({ data: {
      ses_running: { type: "running", status: { type: "busy", activity: {
        userMessageID: "msg_owner", model: "receiving", streamEventCount: 200,
      } } },
      ses_unknown: { type: "running" },
    } }), { preconnect() {} })
    const current = createApiForServer({ server: { url: "http://fixture" }, fetch })
    const legacy = createOpencodeClient({ baseUrl: "http://fixture", fetch })
    const api = createCompatibleApi({ protocol: Promise.resolve("v2"), current, legacy: () => legacy })
    const session = createServerSession(legacy)
    await resyncServerSessions({ event: { name: "global", details: { type: "server.connected" } }, session, api: api.session, refreshDirectories: async () => undefined })
    expect(session.data.session_status.ses_running).toEqual({ type: "busy", activity: {
      userMessageID: "msg_owner", model: "receiving", streamEventCount: 200,
    } })
    expect(session.data.session_status.ses_unknown).toEqual({ type: "busy" })
  })

  test("reconnect legacy active adapter keeps full status instead of bare running", async () => {
    const fetch = Object.assign(async () => Response.json({
      ses_running: { type: "busy", activity: { userMessageID: "msg_owner", model: "receiving", streamEventCount: 200 } },
      ses_ended: { type: "idle" },
    }), { preconnect() {} })
    const current = createApiForServer({ server: { url: "http://fixture" }, fetch })
    const legacy = createOpencodeClient({ baseUrl: "http://fixture", fetch })
    const api = createCompatibleApi({ protocol: Promise.resolve("v1"), current, legacy: () => legacy })
    expect(await api.session.active()).toEqual({
      ses_running: { type: "running", status: { type: "busy", activity: { userMessageID: "msg_owner", model: "receiving", streamEventCount: 200 } } },
    })
  })

  test("reconnect global event restores retained history before the recent bootstrap guard", async () => {
    let reads = 0
    const refreshed = Promise.withResolvers<void>()
    const fetch = Object.assign(async (input: RequestInfo | URL) => {
      const url = new URL(input instanceof Request ? input.url : String(input))
      if (url.pathname === "/session/ses_ended/message") {
        reads++
        if (reads === 2) refreshed.resolve()
        return Response.json([])
      }
      if (url.pathname === "/session/ses_ended")
        return Response.json({ id: "ses_ended", directory: "/repo", title: "ended", slug: "ended", projectID: "project", version: "1", time: { created: 1, updated: 1 } })
      if (url.pathname === "/session/status" || url.pathname === "/global/config") return Response.json({})
      if (url.pathname === "/provider") return Response.json({ all: [], connected: [], default: {} })
      if (url.pathname === "/path") return Response.json({ state: "", config: "", worktree: "", directory: "", home: "" })
      return Response.json({ data: [] })
    }, { preconnect() {} })
    const client = createOpencodeClient({ baseUrl: "http://fixture", fetch })
    const events = createGlobalEmitter<{ [key: string]: ServerEvent }>()
    const session = createServerSession(client)
    const completed = Promise.withResolvers<void>()
    const unsubscribe = events.listen((event) => {
      void resyncServerSessions({ event, session, api: { active: async () => ({}) },
        refreshDirectories: async () => undefined,
      }).then(() => completed.resolve())
    })
    try {
      await session.sync("ses_ended")
      events.emit("global", { id: "evt_connected", type: "server.connected", properties: {} })
      await refreshed.promise
      await completed.promise
      expect(reads).toBe(2)
    } finally {
      unsubscribe()
    }
  })

  test("reconnect fetches a fresh quiet active snapshot despite an existing query cache", async () => {
    const queryClient = new QueryClient()
    const session = createServerSession({} as OpencodeClient)
    let count = 1
    let calls = 0
    const api = { active: async () => {
      calls++
      return { ses_running: { type: "running" as const, status: { type: "busy" as const, activity: {
        model: "receiving" as const, userMessageID: "msg_owner", streamEventCount: count,
      } } } }
    } }
    await queryClient.fetchQuery(loadActiveSessionsQuery(ServerScope.local, api))
    count = 200
    let bootstrapCalls = 0
    await resyncServerSessions({ event: { name: "global", details: { type: "server.connected" } }, session, api, refreshDirectories: async () => { bootstrapCalls++ } })
    expect(calls).toBe(2)
    expect(bootstrapCalls).toBe(1)
    expect(session.data.session_status.ses_running).toEqual({ type: "busy", activity: {
      model: "receiving", userMessageID: "msg_owner", streamEventCount: 200,
    } })
  })

  test("reconnect reloads retained inactive history even with a recent bootstrap", async () => {
    let historyCalls = 0
    const session = createServerSession(createOpencodeClient({
      baseUrl: "http://fixture",
      fetch: Object.assign(async (input: RequestInfo | URL) => {
        const url = new URL(input instanceof Request ? input.url : String(input))
        if (url.pathname.endsWith("/message")) {
          historyCalls++
          return Response.json([])
        }
        return Response.json({ id: "ses_ended", directory: "/repo", title: "ended", slug: "ended", projectID: "project", version: "1", time: { created: 1, updated: 1 } })
      }, { preconnect() {} }),
    }))
    await session.sync("ses_ended")
    session.set("session_status", "ses_ended", { type: "busy" })
    await resyncServerSessions({ event: { name: "global", details: { type: "server.connected" } }, session, api: { active: async () => ({}) }, refreshDirectories: async () => undefined })
    expect(historyCalls).toBe(2)
    expect(session.data.session_status.ses_ended).toBeUndefined()
  })

  test("reconnect starts a fresh pass while an older connection snapshot is pending", async () => {
    const session = createServerSession({} as OpencodeClient)
    const old = Promise.withResolvers<{ ses_running: { type: "running"; status: { type: "busy" } } }>()
    let calls = 0
    const api = { active: () => {
      calls++
      return calls === 1 ? old.promise : Promise.resolve({})
    } }
    const input = { event: { name: "global", details: { type: "server.connected" } }, session, api, refreshDirectories: async () => undefined }
    const first = resyncServerSessions(input)
    await resyncServerSessions(input)
    old.resolve({ ses_running: { type: "running", status: { type: "busy" } } })
    await first
    expect(calls).toBe(2)
    expect(session.data.session_status.ses_running).toBeUndefined()
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
    const captured = session.snapshot.capture("session_status")
    session.set("session_status", "ses_retry", { type: "retry", attempt: 2, message: "retrying", next: 10 })

    seedActiveSessionStatuses(session, {
      ses_running: { type: "running" },
      ses_retry: { type: "running" },
    }, captured)

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

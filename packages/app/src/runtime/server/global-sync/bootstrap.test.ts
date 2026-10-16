import { describe, expect, test } from "bun:test"
import { QueryClient } from "@tanstack/solid-query"
import { OpenCode } from "@opencode/client/promise"
import { createData } from "@opencode/client/solid"
import { createRoot } from "solid-js"
import { createStore } from "solid-js/store"
import { bootstrapDirectory, bootstrapGlobal, loadGlobalConfigQuery, loadPathQuery, loadProjectsQuery } from "./bootstrap"
import type { State } from "./types"
import { ServerScope } from "@/runtime/server/scope"
import { ServerConnection } from "@/runtime/server/registry"
import type { ServerApi } from "@/runtime/server/api"
import { createServerTransport } from "@/runtime/server/client"
import type { ServerSync } from "@/runtime/server/sync"
import { worktreeInventoryKey } from "@/workspaces/inventory"

test("directory catalog bootstrap reuses data loaded by initial readers until invalidated", async () => {
  const requests: string[] = []
  const started = Promise.withResolvers<void>()
  const release = Promise.withResolvers<void>()
  const location = { directory: "/repo", project: { id: "project", directory: "/repo" } }
  const api = OpenCode.make({
    baseUrl: "http://opencode.local",
    fetch: Object.assign(async (input: RequestInfo | URL, init?: RequestInit) => {
      const url = new URL(new Request(input, init).url)
      requests.push(url.pathname)
      if (url.pathname === "/api/location") return Response.json(location)
      if (url.pathname === "/api/mcp/resource")
        return Response.json({ location, data: { resources: [], templates: [] } })
      if (url.pathname === "/api/reference") {
        started.resolve()
        await release.promise
      }
      return Response.json({ location, data: [] })
    }, { preconnect() {} }),
  })
  const setup = createRoot((dispose) => ({
    data: createData({ api: () => api, directory: "/repo", event: { on: () => () => {}, listen: () => () => {} } }),
    dispose,
  }))
  try {
    await Promise.all([
      setup.data.location.provider.sync(),
      setup.data.location.model.sync(),
      setup.data.location.agent.sync(),
      setup.data.location.mcp.server.sync(),
      setup.data.location.mcp.resource.sync(),
    ])
    requests.length = 0
    const state = { complete: false }
    const pending = setup.data.location.sync().then(() => {
      state.complete = true
    })
    await started.promise
    expect(state.complete).toBe(false)
    expect(requests).not.toContain("/api/provider")
    expect(requests).not.toContain("/api/model")
    expect(requests).not.toContain("/api/agent")
    expect(requests).not.toContain("/api/mcp")
    expect(requests).not.toContain("/api/mcp/resource")
    release.resolve()
    await pending
    expect(setup.data.location.reference.list()).toEqual([])
    setup.data.location.invalidate()
    requests.length = 0
    await setup.data.location.sync()
    expect(requests).toEqual(
      expect.arrayContaining(["/api/provider", "/api/model", "/api/agent", "/api/mcp", "/api/mcp/resource"]),
    )
  } finally {
    release.resolve()
    setup.dispose()
  }
})

test.each(["permission", "form"] as const)("initial session %s sync awaits and publishes pending requests", async (kind) => {
  const started = Promise.withResolvers<void>()
  const response = Promise.withResolvers<Response>()
  const request =
    kind === "permission"
      ? { id: "per_initial", sessionID: "ses_initial", action: "read", resources: ["/repo"] }
      : { id: "frm_initial", sessionID: "ses_initial", title: "Question", metadata: { kind: "question" }, fields: [] }
  const requests: string[] = []
  const api = OpenCode.make({
    baseUrl: "http://opencode.local",
    fetch: Object.assign(async (input: RequestInfo | URL, init?: RequestInit) => {
      requests.push(new URL(new Request(input, init).url).pathname)
      started.resolve()
      return response.promise
    }, { preconnect() {} }),
  })
  const setup = createRoot((dispose) => ({
    data: createData({ api: () => api, directory: "/repo", event: { on: () => () => {}, listen: () => () => {} } }),
    dispose,
  }))
  const state = { complete: false }
  const pending = setup.data.session[kind].sync("ses_initial").then(() => {
    state.complete = true
  })
  try {
    await started.promise
    expect(state.complete).toBe(false)
    expect(setup.data.session[kind].list("ses_initial")).toBeUndefined()
    response.resolve(Response.json({ data: [request] }))
    await pending
    expect(setup.data.session[kind].list("ses_initial")?.map((item) => item.id)).toEqual([request.id])
    await setup.data.session[kind].sync("ses_initial")
    expect(requests).toEqual([`/api/session/ses_initial/${kind}`])
  } finally {
    response.resolve(Response.json({ data: [] }))
    await pending
    setup.dispose()
  }
})

test("directory bootstrap waits for deferred location resolution before completing", async () => {
  const started = Promise.withResolvers<void>()
  const response = Promise.withResolvers<Awaited<ReturnType<ServerApi["location"]["get"]>>>()
  const queryClient = new QueryClient()
  const [store, setStore] = createStore<State>({
    status: "loading",
    agent: [],
    command: [],
    reference: [],
    project: "",
    projectMeta: undefined,
    icon: undefined,
    provider_ready: false,
    provider: { all: new Map(), connected: [], default: {} },
    config: {},
    path: { state: "", config: "", worktree: "", directory: "/repo", home: "" },
    mcp_ready: false,
    mcp: {},
    mcp_resource: {},
    lsp_ready: false,
    lsp: [],
    vcs: undefined,
  })
  const state = { complete: false }
  const pending = bootstrapDirectory({
    directory: "/repo",
    scope: ServerScope.local,
    mcp: false,
    store,
    setStore,
    queryClient,
    global: { config: {}, path: store.path, project: [] },
    translate: (key) => key,
    api: {
      project: { list: async () => [] },
      location: {
        get: () => {
          started.resolve()
          return response.promise
        },
      },
    },
  }).then(() => {
    state.complete = true
  })
  try {
    await started.promise
    expect(state.complete).toBe(false)
    expect(store.status).toBe("partial")
    response.resolve({ directory: "/repo", project: { id: "project", directory: "/repo", canonical: "/repo" } })
    await pending
    expect(store.status).toBe("complete")
    expect(store.project).toBe("project")
  } finally {
    response.resolve({ directory: "/repo", project: { id: "project", directory: "/repo", canonical: "/repo" } })
    await pending
    queryClient.clear()
  }
})

test("global bootstrap reuses seeded queries until invalidated and preserves project inventory", async () => {
  const requests: string[] = []
  const api = OpenCode.make({
    baseUrl: "http://opencode.local",
    fetch: Object.assign(
      async (input: RequestInfo | URL, init?: RequestInit) => {
        const url = new URL(new Request(input, init).url)
        requests.push(url.pathname)

        if (url.pathname === "/api/location")
          return Response.json({
            directory: "/repo",
            project: { id: "project", directory: "/repo", canonical: "/repo" },
          })

        if (url.pathname === "/api/project")
          return Response.json([{ id: "project", canonical: "/repo", time: { created: 1, updated: 1 }, sandboxes: [] }])
        throw new Error(`Unexpected request: ${url.pathname}`)
      },
      { preconnect() {} },
    ),
  })

  const [store, setStore] = createStore<ServerSync["data"]>({
    path: { state: "", config: "", worktree: "", directory: "", home: "" },
    project: [],
    provider_auth: {},
    config: {},
    reload: undefined,
  })

  const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } })

  try {
    await Promise.all([
      queryClient.fetchQuery(loadGlobalConfigQuery(ServerScope.local)),
      queryClient.fetchQuery(loadPathQuery(ServerScope.local, null, api.location)),
      queryClient.fetchQuery(loadProjectsQuery(ServerScope.local, api.project)),
    ])
    requests.length = 0
    await bootstrapGlobal({ serverAPI: api, scope: ServerScope.local, setGlobalStore: setStore, queryClient })
    expect(requests).toEqual([])
    expect(store.project.map((project) => [project.id, project.worktree])).toEqual([["project", "/repo"]])

    setStore("project", (projects) => projects.map((project) => ({ ...project, name: "Renamed" })))
    expect(store.project[0]?.name).toBe("Renamed")
    setStore("project", [])
    expect(store.project).toEqual([])

    await bootstrapGlobal({ serverAPI: api, scope: ServerScope.local, setGlobalStore: setStore, queryClient })
    expect(store.project.map((project) => [project.id, project.worktree])).toEqual([["project", "/repo"]])
    expect(store.config).toEqual({})
    expect(requests).toEqual([])

    // A refetch keeps the inventory a view already loaded for this project.
    queryClient.setQueryData(worktreeInventoryKey(ServerScope.local, "project"), [
      { directory: "/repo" },
      { directory: "/repo/feature", strategy: "git" },
    ])
    await queryClient.invalidateQueries({ queryKey: [ServerScope.local, "project"], refetchType: "none" })
    await bootstrapGlobal({ serverAPI: api, scope: ServerScope.local, setGlobalStore: setStore, queryClient })
    expect(requests).toEqual(["/api/project"])
    expect(store.project[0]?.sandboxes).toEqual(["/repo/feature"])
    expect(store.project[0]?.worktrees).toEqual([
      { directory: "/repo" },
      { directory: "/repo/feature", strategy: "git" },
    ])
  } finally {
    queryClient.clear()
  }
})

test.each(["uncached scope", "invalidated"] as const)(
  "global bootstrap awaits both deferred reads when queries are %s",
  async (mode) => {
    const reads = ["/api/location", "/api/project"].map((path) => ({
      path,
      started: Promise.withResolvers<void>(),
      release: Promise.withResolvers<void>(),
    }))
    const state = { blocking: false, complete: false }
    const api = OpenCode.make({
      baseUrl: "http://opencode.local",
      fetch: Object.assign(
        async (input: RequestInfo | URL, init?: RequestInit) => {
          const path = new URL(new Request(input, init).url).pathname
          const read = reads.find((read) => read.path === path)
          if (!read) throw new Error(`Unexpected request: ${path}`)
          if (state.blocking) {
            read.started.resolve()
            await read.release.promise
          }
          if (path === "/api/location")
            return Response.json({
              directory: "/repo",
              project: { id: "project", directory: "/repo", canonical: "/repo" },
            })
          return Response.json([{
            id: "project",
            canonical: "/repo",
            name: state.blocking ? "Fresh" : "Cached",
            time: { created: 1, updated: 1 },
            sandboxes: [],
          }])
        },
        { preconnect() {} },
      ),
    })
    const queryClient = new QueryClient({ defaultOptions: { queries: { retry: false } } })
    const [store, setGlobalStore] = createStore<ServerSync["data"]>({
      path: { state: "", config: "", worktree: "", directory: "", home: "" },
      project: [],
      provider_auth: {},
      config: {},
      reload: undefined,
    })
    const scope =
      mode === "uncached scope"
        ? ServerScope.fromServerKey(ServerConnection.Key.make("https://remote.example"))
        : ServerScope.local
    await Promise.all([
      queryClient.fetchQuery(loadGlobalConfigQuery(ServerScope.local)),
      queryClient.fetchQuery(loadPathQuery(ServerScope.local, null, api.location)),
      queryClient.fetchQuery(loadProjectsQuery(ServerScope.local, api.project)),
    ])
    if (mode === "invalidated")
      await queryClient.invalidateQueries({ queryKey: [scope], refetchType: "none" })
    state.blocking = true
    const pending = bootstrapGlobal({ serverAPI: api, scope, setGlobalStore, queryClient }).then(() => {
      state.complete = true
    })
    try {
      await Promise.race([Promise.all(reads.map((read) => read.started.promise)), pending])
      expect(state.complete).toBe(false)
      expect(store.project).toEqual([])
      reads.find((read) => read.path === "/api/project")?.release.resolve()
      await queryClient.fetchQuery(loadProjectsQuery(scope, api.project))
      expect(state.complete).toBe(false)
      reads.find((read) => read.path === "/api/location")?.release.resolve()
      await pending
      expect(store.project[0]?.name).toBe("Fresh")
      expect(queryClient.getQueryData(loadPathQuery(scope, null, api.location).queryKey)?.directory).toBe("/repo")
      expect(queryClient.getQueryData(loadProjectsQuery(ServerScope.local, api.project).queryKey)?.[0]?.name).toBe(
        mode === "uncached scope" ? "Cached" : "Fresh",
      )
    } finally {
      reads.forEach((read) => read.release.resolve())
      await pending
      queryClient.clear()
    }
  },
)

// Chromium aborts in-flight loopback requests with ERR_NETWORK_CHANGED when Windows reconfigures an
// adapter; the client wraps that as ClientError("Transport"), which the bootstrap retry must see through.
test("recovers project metadata after the connection to the server is dropped", async () => {
  const body = JSON.stringify([{ id: "project", canonical: "/repo", time: { created: 1, updated: 1 }, sandboxes: [] }])
  let dropped = 0
  const requests: string[] = []

  const server = Bun.listen({
    hostname: "127.0.0.1",
    port: 0,
    socket: {
      open(socket) {
        if (dropped >= 2) return
        dropped += 1
        socket.terminate()
      },
      data(socket, chunk) {
        requests.push(String(chunk).split(" ")[0] ?? "")
        socket.end(
          `HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: ${Buffer.byteLength(body)}\r\naccess-control-allow-origin: *\r\nconnection: close\r\n\r\n${body}`,
        )
      },
    },
  })

  const transport = createServerTransport({ http: { url: `http://127.0.0.1:${server.port}` } })

  try {
    const result = await new QueryClient({ defaultOptions: { queries: { retry: false } } }).fetchQuery(
      loadProjectsQuery(ServerScope.local, transport.api.project),
    )

    expect(dropped).toBe(2)
    // happy-dom's fetch adds a CORS preflight; only the GET is the retried API call.
    expect(requests.filter((method) => method === "GET")).toHaveLength(1)
    expect(result).toMatchObject([{ id: "project", worktree: "/repo" }])
  } finally {
    server.stop(true)
  }
})

describe("query keys", () => {
  test("partitions identical directories by server scope and loads current location metadata", async () => {
    const calls: unknown[] = []

    const location = {
      get: async (input: unknown) => {
        calls.push(input)

        return { directory: "/repo/subpath", project: { id: "project", directory: "/repo" } }
      },
    } as ServerApi["location"]

    const remote = "https://debian.example" as typeof ServerScope.local

    expect([...loadPathQuery(ServerScope.local, "/repo", location).queryKey]).toEqual(["local", "/repo", "path"])
    expect([...loadPathQuery(remote, "/repo", location).queryKey]).toEqual(["https://debian.example", "/repo", "path"])

    const result = await new QueryClient().fetchQuery(loadPathQuery(ServerScope.local, "/repo/subpath", location))

    expect(calls).toEqual([{ location: { directory: "/repo/subpath" } }])
    expect(result).toMatchObject({ directory: "/repo/subpath", worktree: "/repo" })
  })

  test("loads project metadata without enumerating any project's worktrees", async () => {
    const requests: string[] = []

    const api = OpenCode.make({
      baseUrl: "http://localhost:3000",
      fetch: Object.assign(
        async (input: RequestInfo | URL, init?: RequestInit) => {
          const url = new URL(new Request(input, init).url)
          requests.push(url.pathname)

          if (url.pathname !== "/api/project") throw new Error(`Unexpected request: ${url}`)

          return Response.json([
            ...Array.from({ length: 300 }, (_, index) => ({
              id: `historical-${index.toString().padStart(3, "0")}`,
              canonical: `/history/${index}`,
              time: { created: 1, updated: 1 },
              sandboxes: [],
            })),
            { id: "b", canonical: "/b", time: { created: 1, updated: 1 }, sandboxes: [] },
            { id: "a", canonical: "/a", time: { created: 1, updated: 1 }, sandboxes: ["/a/legacy"] },
            { id: "test", canonical: "/tmp/opencode-test-1", time: { created: 1, updated: 1 }, sandboxes: [] },
          ])
        },
        { preconnect() {} },
      ),
    })

    const result = await new QueryClient().fetchQuery(loadProjectsQuery(ServerScope.local, api.project))

    expect(requests).toEqual(["/api/project"])
    expect(result).toHaveLength(302)
    expect(result.slice(0, 2)).toMatchObject([
      { id: "a", worktree: "/a", sandboxes: ["/a/legacy"], worktrees: [{ directory: "/a" }] },
      { id: "b", worktree: "/b", sandboxes: [], worktrees: [{ directory: "/b" }] },
    ])
    expect(result.some((project) => project.id === "test")).toBe(false)
  })
})

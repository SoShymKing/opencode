import { beforeAll, describe, expect, mock, test } from "bun:test"
import { createRoot, getOwner, type Owner } from "solid-js"
import { queryOptions } from "@tanstack/solid-query"
import type { NormalizedProviderListResponse } from "@opencode-ai/session-ui/context"
import type { Agent, Config, LspStatus, Path, Project, ReferenceInfo } from "@opencode-ai/sdk/v2/client"
import type { McpResource, McpServer } from "@opencode-ai/client/promise"
import type { QueryOptionsApi } from "../server-sync"
import { ServerScope } from "@/utils/server-scope"
import type { PathKey } from "@/utils/path-key"

let createChildStoreManager: typeof import("./child-store").createChildStoreManager
const querySingles: Array<() => { queryKey?: readonly unknown[]; enabled?: boolean }> = []
const persist: typeof import("@/utils/persist").persisted = (_target, store) => [
  store[0],
  store[1],
  null,
  Object.assign(() => true, { promise: undefined }),
]

const provider = { all: new Map(), connected: [], default: {} } satisfies NormalizedProviderListResponse

const queryOptionsApi = {
  globalConfig: () =>
    queryOptions<Config, Error, Config, string[]>({
      queryKey: [ServerScope.local, "config"],
      queryFn: async () => ({}),
    }),
  projects: () =>
    queryOptions<Project[], Error, Project[], string[]>({
      queryKey: [ServerScope.local, "project"],
      queryFn: async () => [],
    }),
  providers: (directory: PathKey | null) =>
    queryOptions<NormalizedProviderListResponse, Error, NormalizedProviderListResponse, (string | null)[]>({
      queryKey: [ServerScope.local, directory, "providers"],
      queryFn: async () => provider,
    }),
  path: (directory: PathKey | null) =>
    queryOptions<Path>({
      queryKey: [ServerScope.local, directory, "path"],
      queryFn: async () => ({
        state: "",
        config: "",
        worktree: "",
        directory: directory ?? "",
        home: "",
      }),
    }),
  agents: (directory: PathKey) =>
    queryOptions<Agent[], Error, Agent[], string[]>({
      queryKey: [ServerScope.local, directory, "agents"],
      queryFn: async () => [],
    }),
  mcp: (directory: PathKey) =>
    queryOptions<
      Record<string, McpServer["status"]>,
      Error,
      Record<string, McpServer["status"]>,
      readonly [ServerScope, string, "mcp"]
    >({ queryKey: [ServerScope.local, directory, "mcp"], queryFn: async () => ({}) }),
  mcpResources: (directory: PathKey) =>
    queryOptions<
      Record<string, McpResource>,
      Error,
      Record<string, McpResource>,
      readonly [ServerScope, string, "mcpResources"]
    >({ queryKey: [ServerScope.local, directory, "mcpResources"], queryFn: async () => ({}) }),
  lsp: (directory: PathKey) =>
    queryOptions<LspStatus[], Error, LspStatus[], readonly [ServerScope, string, "lsp"]>({
      queryKey: [ServerScope.local, directory, "lsp"],
      queryFn: async () => [],
    }),
  references: (directory: PathKey) =>
    queryOptions<ReferenceInfo[]>({
      queryKey: [ServerScope.local, directory, "references"],
      queryFn: async () => [],
    }),
  sessions: (directory: PathKey) => ({ queryKey: [ServerScope.local, directory, "loadSessions"] as const }),
} satisfies QueryOptionsApi

function createOwner(callback: (owner: Owner) => void) {
  return createRoot((dispose) => {
    const owner = getOwner()
    if (!owner) throw new Error("owner required")
    callback(owner)

    return dispose
  })
}

beforeAll(async () => {
  mock.module("@tanstack/solid-query", () => ({
    useQuery: (options: () => { queryKey?: readonly unknown[]; enabled?: boolean }) => {
      querySingles.push(options)
      return {
        get isLoading() {
          return options().queryKey?.[2] === "path"
        },
        get data() {
          if (options().queryKey?.[2] === "path") throw new Error("pending path data read")
          if (options().queryKey?.[2] === "mcp") return options().enabled ? { demo: { status: "disabled" } } : undefined
          if (options().queryKey?.[2] === "lsp") return []
          if (options().queryKey?.[2] === "providers") return provider
          return undefined
        },
      }
    },
  }))

  createChildStoreManager = (await import("./child-store")).createChildStoreManager
})

describe("createChildStoreManager", () => {
  test("does not evict the active directory during mark", () => {
    let manager: ReturnType<typeof createChildStoreManager> | undefined
    const dispose = createOwner((owner) => {
      manager = createChildStoreManager({
        owner,
        scope: ServerScope.local,
        persist,
        isBooting: () => false,
        isLoadingSessions: () => false,
        onBootstrap() {},
        onMcp() {},
        onDispose() {},
        translate: (key) => key,
        queryOptions: queryOptionsApi,
        global: { provider },
      })
    })

    try {
      if (!manager) throw new Error("manager required")
      const activeManager = manager
      Array.from({ length: 30 }, (_, index) => `/pinned-${index}`).forEach((directory) => {
        activeManager.ensureChild(directory)
        activeManager.pin(directory)
      })

      const directory = "/active"
      activeManager.ensureChild(directory)
      activeManager.mark(directory)

      expect(activeManager.children[directory]).toBeDefined()
    } finally {
      dispose()
    }
  })

  test("starts new child stores as loading and bootstraps with the directory query key", () => {
    const bootstraps: string[] = []
    let manager: ReturnType<typeof createChildStoreManager> | undefined

    const dispose = createOwner((owner) => {
      manager = createChildStoreManager({
        owner,
        scope: ServerScope.local,
        persist,
        isBooting: () => false,
        isLoadingSessions: () => false,
        onBootstrap(directory) {
          bootstraps.push(directory)
        },
        onMcp() {},
        onDispose() {},
        translate: (key) => key,
        queryOptions: queryOptionsApi,
        global: { provider },
      })
    })

    try {
      if (!manager) throw new Error("manager required")

      const [store] = manager.child("C:\\Users\\JH\\opencode")

      expect(store.status).toBe("loading")
      expect(store.limit).toBe(5)
      expect(bootstraps).toEqual(["C:/Users/JH/opencode"])
    } finally {
      dispose()
    }
  })

  test("provides the requested directory while the path query is pending", () => {
    let manager: ReturnType<typeof createChildStoreManager> | undefined

    const dispose = createOwner((owner) => {
      manager = createChildStoreManager({
        owner,
        scope: ServerScope.local,
        persist,
        isBooting: () => false,
        isLoadingSessions: () => false,
        onBootstrap() {},
        onMcp() {},
        onDispose() {},
        translate: (key) => key,
        queryOptions: queryOptionsApi,
        global: { provider },
      })
    })

    try {
      if (!manager) throw new Error("manager required")

      const [store] = manager.child("/project", { bootstrap: false })

      expect(store.path.directory).toBe("/project")
      expect(store.path.worktree).toBe("")
    } finally {
      dispose()
    }
  })

  test("enables MCP only when requested for the directory", () => {
    let manager: ReturnType<typeof createChildStoreManager> | undefined
    const offset = querySingles.length
    const mcpLoads: string[] = []

    const dispose = createOwner((owner) => {
      manager = createChildStoreManager({
        owner,
        scope: ServerScope.local,
        persist,
        isBooting: () => false,
        isLoadingSessions: () => false,
        onBootstrap() {},
        onMcp(directory) {
          mcpLoads.push(directory)
        },
        onDispose() {},
        translate: (key) => key,
        queryOptions: queryOptionsApi,
        global: { provider },
      })
    })

    try {
      if (!manager) throw new Error("manager required")
      const [store, setStore] = manager.child("/project", { bootstrap: false })
      expect(querySingles.length - offset).toBe(6)
      const query = querySingles[offset + 1]
      const resourceQuery = querySingles[offset + 2]
      if (!query) throw new Error("query required")
      if (!resourceQuery) throw new Error("resource query required")
      expect(query().enabled).toBe(false)
      expect(resourceQuery().enabled).toBe(false)

      setStore("status", "complete")
      manager.child("/project", { bootstrap: false, mcp: true })
      expect(query().enabled).toBe(true)
      expect(resourceQuery().enabled).toBe(true)
      expect(store.mcp).toEqual({ demo: { status: "disabled" } })
      expect(mcpLoads).toEqual(["/project"])

      manager.disableMcp("/project")
      expect(query().enabled).toBe(false)
      expect(manager.mcp("/project")).toBe(false)
    } finally {
      dispose()
    }
  })

  test("keeps non-bootstrapping children passive until a real directory access", () => {
    let manager: ReturnType<typeof createChildStoreManager> | undefined
    const offset = querySingles.length
    const bootstraps: string[] = []

    const dispose = createOwner((owner) => {
      manager = createChildStoreManager({
        owner,
        scope: ServerScope.local,
        persist,
        isBooting: () => false,
        isLoadingSessions: () => false,
        onBootstrap(directory) {
          bootstraps.push(directory)
        },
        onMcp() {},
        onDispose() {},
        translate: (key) => key,
        queryOptions: queryOptionsApi,
        global: { provider },
      })
    })

    try {
      if (!manager) throw new Error("manager required")
      const [store] = manager.child("/project", { bootstrap: false })
      const queries = querySingles.slice(offset)

      expect(queries).toHaveLength(6)
      expect(queries[0]?.().enabled).toBe(false)
      expect(queries[3]?.().enabled).toBe(false)
      expect(queries[4]?.().enabled).toBe(false)
      expect(queries[5]?.().enabled).toBe(false)
      expect(store.path.directory).toBe("/project")
      expect(store.provider_ready).toBe(false)
      expect(store.lsp_ready).toBe(false)
      expect(bootstraps).toEqual([])

      manager.child("/project")
      expect(queries[0]?.().enabled).toBe(true)
      expect(queries[3]?.().enabled).toBe(true)
      expect(queries[4]?.().enabled).toBe(true)
      expect(queries[5]?.().enabled).toBe(true)
      expect(bootstraps).toEqual(["/project"])

      manager.child("/project", { bootstrap: false })
      expect(queries[0]?.().enabled).toBe(true)
    } finally {
      dispose()
    }
  })
})

import { describe, expect, test } from "bun:test"
import { createStore } from "solid-js/store"
import { QueryClient } from "@tanstack/solid-query"
import type { Config, OpencodeClient, Project } from "@opencode-ai/sdk/v2/client"
import { createOpencodeClient } from "@opencode-ai/sdk/v2/client"
import type { AgentApi, CatalogApi, CommandApi, ReferenceApi } from "@opencode-ai/client/promise"
import type { NormalizedProviderListResponse } from "@opencode-ai/session-ui/context"
import {
  bootstrapDirectory,
  loadAgentsQuery,
  loadCommands,
  loadGlobalConfigQuery,
  loadPathQuery,
  loadProjectsQuery,
  loadProvidersQuery,
  loadReferencesQuery,
} from "./bootstrap"
import type { State, VcsCache } from "./types"
import { ServerScope } from "@/utils/server-scope"
import type { ServerApi } from "@/utils/server"
import { createServerSession } from "../server-session"

type ProjectApi = ServerApi["project"]

const provider = { all: new Map(), connected: [], default: {} } satisfies NormalizedProviderListResponse
const api = {
  agent: { list: async () => ({ location: {}, data: [] }) },
  provider: { list: async () => ({ location: {}, data: [] }) },
  model: {
    list: async () => ({ location: {}, data: [] }),
    default: async () => ({ location: {}, data: null }),
  },
  permission: { request: { list: async () => ({ location: {}, data: [] }) } },
  project: {
    list: async () => [],
    current: async () => ({ id: "project", directory: "/project" }),
  },
  question: { request: { list: async () => ({ location: {}, data: [] }) } },
  reference: { list: async () => ({ location: {}, data: [] }) },
  vcs: { get: async () => ({ location: {}, data: {} }) },
} as unknown as ServerApi

function directoryState() {
  return createStore<State>({
    status: "loading",
    agent: [],
    command: [],
    reference: [],
    project: "",
    projectMeta: undefined,
    icon: undefined,
    provider_ready: true,
    provider,
    config: {},
    path: { state: "", config: "", worktree: "/project", directory: "/project", home: "/home" },
    session: [],
    sessionTotal: 0,
    session_status: {},
    session_working(id: string) {
      return this.session_status[id]?.type !== "idle"
    },
    session_diff: {},
    todo: {},
    permission: {},
    question: {},
    mcp_ready: true,
    mcp: {},
    mcp_resource: {},
    lsp_ready: true,
    lsp: [],
    vcs: undefined,
    limit: 5,
    message: {},
    session_message: {},
    part: {},
    part_text_accum_delta: {},
  })
}

describe("bootstrapDirectory", () => {
  test("reconnect bootstrap restores permission and question snapshots while raced live state wins", async () => {
    const waiting = ["/session/status", "/permission", "/question"].map((path) => ({
      path, started: Promise.withResolvers<void>(), response: Promise.withResolvers<Response>(),
    }))
    const sdk = createOpencodeClient({ baseUrl: "http://fixture", fetch: Object.assign(async (input: RequestInfo | URL) => {
      const path = new URL(input instanceof Request ? input.url : String(input)).pathname
      const pending = waiting.find((item) => item.path === path)
      if (pending) {
        pending.started.resolve()
        return pending.response.promise
      }
      if (path === "/provider") return Response.json({ all: [], connected: [], default: {} })
      if (path === "/v2/reference") return Response.json({ data: [] })
      if (path === "/config" || path === "/vcs") return Response.json({})
      return Response.json([])
    }, { preconnect() {} }) })
    const session = createServerSession(sdk)
    for (const id of ["live", "quiet"]) session.remember({
      id, slug: id, projectID: "project", directory: "/project", title: id, version: "1", time: { created: 1, updated: 1 },
    })
    const [store, setStore] = directoryState()
    const loading = bootstrapDirectory({
      directory: "/project", scope: ServerScope.local, mcp: false, sdk, api, store, setStore,
      global: { config: {}, path: store.path, project: [{ id: "project", worktree: "/project" } as Project], provider },
      vcsCache: { setStore() {} } as unknown as VcsCache, loadSessions() {}, translate: (key) => key,
      queryClient: new QueryClient(), session, protocol: Promise.resolve("v1"), awaitTasks: true,
    })
    await Promise.all(waiting.map((item) => item.started.promise))
    session.apply({ type: "session.status", properties: { sessionID: "live", status: { type: "busy", activity: { model: "receiving", streamEventCount: 201 } } } })
    const permission = { id: "perm_live", sessionID: "live", permission: "read", patterns: [], metadata: {}, always: [] }
    const question = { id: "question_live", sessionID: "live", questions: [] }
    session.apply({ type: "permission.asked", properties: permission })
    session.apply({ type: "question.asked", properties: question })
    for (const item of waiting) {
      const payload = item.path === "/session/status"
        ? { live: { type: "busy", activity: { model: "waiting", streamEventCount: 200 } }, quiet: { type: "busy", activity: { model: "receiving", streamEventCount: 200 } } }
        : item.path === "/permission" ? [{ ...permission, id: "perm_quiet", sessionID: "quiet" }]
        : [{ ...question, id: "question_quiet", sessionID: "quiet" }]
      item.response.resolve(Response.json(payload))
    }
    await loading
    expect(session.data.session_status.live).toEqual({ type: "busy", activity: { model: "receiving", streamEventCount: 201 } })
    expect(session.data.session_status.quiet).toEqual({ type: "busy", activity: { model: "receiving", streamEventCount: 200 } })
    expect(session.data.permission.live).toEqual([permission])
    expect(session.data.question.live).toEqual([question])
    expect(session.data.permission.quiet?.map((item) => item.id)).toEqual(["perm_quiet"])
    expect(session.data.question.quiet?.map((item) => item.id)).toEqual(["question_quiet"])
  })

  test("uses legacy MCP endpoints while refreshing a v1 directory", async () => {
    const legacyConfigReads: string[] = []
    const mcpReads: string[] = []
    const [store, setStore] = directoryState()

    await bootstrapDirectory({
      directory: "/project",
      scope: ServerScope.local,
      mcp: true,
      global: {
        config: {} satisfies Config,
        path: { state: "", config: "", worktree: "/project", directory: "/project", home: "/home" },
        project: [{ id: "project", worktree: "/project" } as Project],
        provider,
      },
      sdk: {
        app: { agents: async () => ({ data: [{ name: "build", mode: "primary" }] }) },
        config: {
          get: async () => {
            legacyConfigReads.push("directory")
            return { data: {} }
          },
        },
        session: { status: async () => ({ data: {} }) },
        vcs: { get: async () => ({ data: undefined }) },
        command: {
          list: async () => {
            mcpReads.push("command")
            return { data: [] }
          },
        },
        permission: { list: async () => ({ data: [] }) },
        question: { list: async () => ({ data: [] }) },
        v2: { reference: { list: async () => ({ data: { data: [] } }) } },
        mcp: {
          status: async () => {
            mcpReads.push("status")
            return { data: {} }
          },
        },
        experimental: {
          resource: {
            list: async () => {
              mcpReads.push("resource")
              return { data: {} }
            },
          },
        },
        provider: { list: async () => ({ data: { all: [], connected: [], default: {} } }) },
      } as unknown as OpencodeClient,
      api,
      store,
      setStore,
      vcsCache: { setStore() {} } as unknown as VcsCache,
      loadSessions() {},
      translate: (key) => key,
      queryClient: new QueryClient(),
      protocol: Promise.resolve("v1"),
    })

    expect(store.status).toBe("partial")

    await new Promise((resolve) => setTimeout(resolve, 80))

    expect(store.status).toBe("complete")
    expect(legacyConfigReads).toEqual(["directory"])
    expect(mcpReads.sort()).toEqual(["command", "resource", "status"])
  })

  test("skips legacy config while refreshing a v2 directory", async () => {
    const [store, setStore] = directoryState()

    await bootstrapDirectory({
      directory: "/project",
      scope: ServerScope.local,
      mcp: false,
      global: {
        config: {} satisfies Config,
        path: { state: "", config: "", worktree: "/project", directory: "/project", home: "/home" },
        project: [{ id: "project", worktree: "/project" } as Project],
        provider,
      },
      sdk: {
        config: {
          get: async () => {
            throw new Error("legacy directory config should not be called")
          },
        },
      } as unknown as OpencodeClient,
      api,
      store,
      setStore,
      vcsCache: { setStore() {} } as unknown as VcsCache,
      loadSessions() {},
      translate: (key) => key,
      queryClient: new QueryClient(),
      protocol: Promise.resolve("v2"),
    })

    expect(store.status).toBe("partial")

    await new Promise((resolve) => setTimeout(resolve, 80))

    expect(store.status).toBe("complete")
  })
})

describe("config queries", () => {
  test("skips legacy global config for v2 servers", async () => {
    const sdk = {
      global: {
        config: {
          get: async () => {
            throw new Error("legacy global config should not be called")
          },
        },
      },
    } as unknown as OpencodeClient

    const result = await new QueryClient().fetchQuery(
      loadGlobalConfigQuery(ServerScope.local, sdk, Promise.resolve("v2")),
    )

    expect(result).toEqual({})
  })

  test("loads legacy global config for v1 servers", async () => {
    const calls: string[] = []
    const config = { shell: "zsh" } satisfies Config
    const sdk = {
      global: {
        config: {
          get: async () => {
            calls.push("global")
            return { data: config }
          },
        },
      },
    } as unknown as OpencodeClient

    const result = await new QueryClient().fetchQuery(
      loadGlobalConfigQuery(ServerScope.local, sdk, Promise.resolve("v1")),
    )

    expect(result).toEqual(config)
    expect(calls).toEqual(["global"])
  })
})

describe("query keys", () => {
  test("partitions identical directories by server scope", () => {
    const client = {} as Parameters<typeof loadPathQuery>[2]
    const api = {} as CatalogApi
    const remote = "https://debian.example" as typeof ServerScope.local

    expect([...loadPathQuery(ServerScope.local, "/repo", client).queryKey]).toEqual(["local", "/repo", "path"])
    expect([...loadPathQuery(remote, "/repo", client).queryKey]).toEqual(["https://debian.example", "/repo", "path"])
    expect([...loadProvidersQuery(remote, null, api).queryKey]).toEqual(["https://debian.example", null, "providers"])
  })

  test("loads the current provider and model catalog", async () => {
    const calls: unknown[] = []
    const api = {
      provider: {
        list: async (input: unknown) => {
          calls.push(["provider", input])
          return { location: {}, data: [{ id: "openai", name: "OpenAI", package: "@ai-sdk/openai" }] }
        },
      },
      model: {
        list: async (input: unknown) => {
          calls.push(["model", input])
          return { location: {}, data: [] }
        },
        default: async (input: unknown) => {
          calls.push(["default", input])
          return { location: {}, data: null }
        },
      },
    } as unknown as CatalogApi

    const result = await new QueryClient().fetchQuery(loadProvidersQuery(ServerScope.local, "/repo", api))

    expect(calls).toEqual([
      ["provider", { location: { directory: "/repo" } }],
      ["model", { location: { directory: "/repo" } }],
      ["default", { location: { directory: "/repo" } }],
    ])
    expect(result.connected).toEqual(["openai"])
  })

  test("loads agents from the current location-scoped endpoint", async () => {
    const calls: unknown[] = []
    const api = {
      list: async (input: unknown) => {
        calls.push(input)
        return { location: {}, data: [] }
      },
    } as unknown as AgentApi

    const result = await new QueryClient().fetchQuery(loadAgentsQuery(ServerScope.local, "/repo", api))

    expect(calls).toEqual([{ location: { directory: "/repo" } }])
    expect(result).toEqual([])
  })

  test("loads commands from the current location-scoped endpoint", async () => {
    const calls: unknown[] = []
    const api = {
      list: async (input: unknown) => {
        calls.push(input)
        return {
          location: {},
          data: [{ name: "review", template: "Review files" /* source: "command" as const */ }],
        }
      },
    } as unknown as CommandApi

    const result = await loadCommands("/repo", api)

    expect(calls).toEqual([{ location: { directory: "/repo" } }])
    expect(result).toEqual([{ name: "review", template: "Review files" /* source: "command" */ }])
  })

  test("loads projects from the current endpoint", async () => {
    const api = {
      list: async () => [
        { id: "b", worktree: "/b", time: { created: 1, updated: 1 }, sandboxes: [] },
        { id: "a", worktree: "/a", time: { created: 1, updated: 1 }, sandboxes: [] },
      ],
    } as unknown as ProjectApi

    const result = await new QueryClient().fetchQuery(loadProjectsQuery(ServerScope.local, api))

    expect(result.map((project) => project.id)).toEqual(["a", "b"])
  })

  test("loads references from the current location-scoped endpoint", async () => {
    const calls: unknown[] = []
    const api = {
      list: async (input: unknown) => {
        calls.push(input)
        return { location: {}, data: [{ name: "AGENTS.md", path: "/repo/AGENTS.md", source: "instructions" }] }
      },
    } as unknown as ReferenceApi

    const result = await new QueryClient().fetchQuery(loadReferencesQuery(ServerScope.local, "/repo", api))

    expect(calls).toEqual([{ location: { directory: "/repo" } }])
    expect(result).toHaveLength(1)
  })
})

import { describe, expect, test } from "bun:test"
import { createRoot, createSignal } from "solid-js"
import { createStore } from "solid-js/store"
import {
  createServerProjects,
  migrateCanonicalLocalServerState,
  nextServerAfterRemoval,
  resolveServerList,
  ServerConnection,
} from "./server"
import { ServerScope } from "@/utils/server-scope"

describe("resolveServerList", () => {
  test("lets startup auth_token credentials override a persisted same-url server", () => {
    const list = resolveServerList({
      stored: [{ url: "https://server.example.test" }],
      props: [
        {
          type: "http",
          authToken: true,
          http: {
            url: "https://server.example.test",
            username: "opencode",
            password: "secret",
          },
        },
      ],
    })

    expect(list).toHaveLength(1)
    expect(list[0]?.type).toBe("http")
    expect(list[0]?.http).toEqual({
      url: "https://server.example.test",
      username: "opencode",
      password: "secret",
    })
    expect(list[0]?.type === "http" ? list[0].authToken : false).toBe(true)
    expect(ServerConnection.key(list[0]!) as string).toBe("https://server.example.test")
  })

  test("keeps persisted credentials when startup has no auth_token", () => {
    const list = resolveServerList({
      stored: [
        {
          url: "https://server.example.test",
          username: "opencode",
          password: "saved",
        },
      ],
      props: [{ type: "http", http: { url: "https://server.example.test" } }],
    })

    expect(list).toHaveLength(1)
    expect(list[0]?.type).toBe("http")
    expect(list[0]?.http).toEqual({
      url: "https://server.example.test",
      username: "opencode",
      password: "saved",
    })
    expect(list[0]?.type === "http" ? list[0].authToken : true).toBeUndefined()
  })
})

test("treats WSL sidecars as remote server connections", () => {
  expect(
    ServerConnection.local({
      type: "sidecar",
      variant: "wsl",
      distro: "Debian",
      http: { url: "http://127.0.0.1:4097" },
    }),
  ).toBe(false)
  expect(ServerConnection.local({ type: "sidecar", variant: "base", http: { url: "http://127.0.0.1:4096" } })).toBe(
    true,
  )
  expect(ServerConnection.local({ type: "http", http: { url: "http://localhost:4096" } })).toBe(true)
  expect(ServerConnection.local({ type: "http", http: { url: "https://server.example.test" } })).toBe(false)
})

test("active server removal falls back across built-in and persisted servers", () => {
  const local = { type: "sidecar", variant: "base", http: { url: "http://127.0.0.1:4096" } } as const
  const debian = {
    type: "sidecar",
    variant: "wsl",
    distro: "Debian",
    http: { url: "http://127.0.0.1:4097" },
  } as const

  expect(
    nextServerAfterRemoval(
      [local, debian],
      ServerConnection.Key.make("wsl:Debian"),
      ServerConnection.Key.make("sidecar"),
    ),
  ).toBe(ServerConnection.Key.make("sidecar"))
})

describe("createServerProjects", () => {
  describe.each(["local", "https://one.example", "https://two.example"])("normalized mutations in %s", (server) => {
    test.each(["open", "remove", "close", "expand", "collapse", "move"] as const)(
      "%s finds the stored project when given a lowercase drive variant",
      (operation) => {
        createRoot((dispose) => {
          const first = { worktree: "C:\\Repo\\", expanded: operation === "collapse" }
          const before = { worktree: "/before", expanded: false }
          const after = { worktree: "/after", expanded: true }
          const other = [{ worktree: "c:/Repo", expanded: false }]
          const [store, setStore] = createStore({
            projects: structuredClone({ [server]: [before, first, after], untouched: other }),
            lastProject: { [server]: "C:\\Repo\\", untouched: "c:/Repo" },
            recentlyClosed: { [server]: ["C:/Repo", "/closed"], untouched: ["c:/Repo"] },
          })
          const scope = () => ServerScope.fromServerKey(ServerConnection.Key.make(server))
          const projects = createServerProjects({ scope, store, setStore })

          operation === "move" ? projects.move("c:/Repo", 2) : projects[operation]("c:/Repo")

          const expected = {
            open: [before, first, after],
            remove: [before, after],
            close: [before, after],
            expand: [before, { ...first, expanded: true }, after],
            collapse: [before, { ...first, expanded: false }, after],
            move: [before, after, first],
          }
          const closed = { open: ["/closed"], close: ["c:/Repo", "/closed"] }
          dispose()
          expect(store.projects).toEqual({ [server]: expected[operation], untouched: other })
          expect(store.lastProject).toEqual({ [server]: "C:\\Repo\\", untouched: "c:/Repo" })
          expect(store.recentlyClosed).toEqual({
            [server]: operation === "open" || operation === "close" ? closed[operation] : ["C:/Repo", "/closed"],
            untouched: ["c:/Repo"],
          })
        })
      },
    )
  })

  test("keeps active and explicit server buckets in one reactive store", () => {
    createRoot((dispose) => {
      const [scope] = createSignal(ServerScope.local)
      const [store, setStore] = createStore({ projects: {}, lastProject: {}, recentlyClosed: {} })
      const active = createServerProjects({ scope, store, setStore })
      const remote = createServerProjects({ scope: () => "https://debian.example" as ServerScope, store, setStore })

      remote.open("/repo")
      expect(remote.list()).toEqual([{ worktree: "/repo", expanded: true }])
      expect(active.list()).toEqual([])

      const adopted = createServerProjects({ scope: () => "https://debian.example" as ServerScope, store, setStore })
      expect(adopted.list()).toEqual([{ worktree: "/repo", expanded: true }])

      adopted.close("/repo")
      expect(remote.list()).toEqual([])
      dispose()
    })
  })

  test("tracks recently closed projects and drops them when reopened", () => {
    createRoot((dispose) => {
      const [scope] = createSignal(ServerScope.local)
      const [store, setStore] = createStore({ projects: {}, lastProject: {}, recentlyClosed: {} })
      const projects = createServerProjects({ scope, store, setStore })

      projects.open("/a")
      projects.open("/b")
      projects.close("/a")
      expect(projects.recentlyClosed()).toEqual(["/a"])

      projects.close("/b")
      expect(projects.recentlyClosed()).toEqual(["/b", "/a"])

      projects.open("/a")
      expect(projects.recentlyClosed()).toEqual(["/b"])
      expect(projects.list()).toEqual([{ worktree: "/a", expanded: true }])
      dispose()
    })
  })

  test("remove drops a project without recording it as recently closed", () => {
    createRoot((dispose) => {
      const [scope] = createSignal(ServerScope.local)
      const [store, setStore] = createStore({ projects: {}, lastProject: {}, recentlyClosed: {} })
      const projects = createServerProjects({ scope, store, setStore })

      projects.open("/repo/subdir")
      projects.remove("/repo/subdir")
      expect(projects.list()).toEqual([])
      expect(projects.recentlyClosed()).toEqual([])
      dispose()
    })
  })

  test("retains recently closed history beyond the visible display limit", () => {
    createRoot((dispose) => {
      const [scope] = createSignal(ServerScope.local)
      const [store, setStore] = createStore({ projects: {}, lastProject: {}, recentlyClosed: {} })
      const projects = createServerProjects({ scope, store, setStore })

      // Closing 6 projects keeps all 6 in the store even though only 5 are displayed;
      // this prevents display-filtered entries from evicting still-visible ones.
      for (const dir of ["/1", "/2", "/3", "/4", "/5", "/6"]) {
        projects.open(dir)
        projects.close(dir)
      }
      expect(projects.recentlyClosed()).toEqual(["/6", "/5", "/4", "/3", "/2", "/1"])
      dispose()
    })
  })

  test("caps recently closed history at the store limit", () => {
    createRoot((dispose) => {
      const [scope] = createSignal(ServerScope.local)
      const [store, setStore] = createStore({ projects: {}, lastProject: {}, recentlyClosed: {} })
      const projects = createServerProjects({ scope, store, setStore })

      for (let i = 1; i <= 20; i++) {
        projects.open(`/p${i}`)
        projects.close(`/p${i}`)
      }
      expect(projects.recentlyClosed()).toHaveLength(16)
      expect(projects.recentlyClosed()[0]).toBe("/p20")
      expect(projects.recentlyClosed().at(-1)).toBe("/p5")
      dispose()
    })
  })

  test("dedupes recently closed entries by normalized path", () => {
    createRoot((dispose) => {
      const [scope] = createSignal(ServerScope.local)
      const [store, setStore] = createStore({ projects: {}, lastProject: {}, recentlyClosed: {} })
      const projects = createServerProjects({ scope, store, setStore })

      projects.close("/repo")
      projects.close("/repo/")
      expect(projects.recentlyClosed()).toEqual(["/repo/"])
      dispose()
    })
  })
})

describe("migrateCanonicalLocalServerState", () => {
  test.each([undefined, "local", "https://absent.example", "https://canonical.example"])(
    "restores first entries in every scope when canonical is %s",
    (canonical) => {
      const first = { worktree: "C:\\Repo\\", expanded: false, setting: "first" }
      const middle = { worktree: "/middle", expanded: true }
      const slash = { worktree: "/slash/", expanded: false }
      const tail = { worktree: "/tail", expanded: false }
      const invalid = [null, 7, { expanded: true }, { worktree: 7 }]
      const bucket = [
        first,
        middle,
        { worktree: "c:/Repo", expanded: true },
        slash,
        { worktree: "/slash", expanded: true },
        tail,
        ...invalid,
      ]
      const remoteFirst = { worktree: "c:/Repo", expanded: true, setting: "remote" }
      const canonicalFirst = { ...first, expanded: true, setting: "canonical" }
      const value = {
        projects: {
          local: bucket,
          "https://canonical.example": [
            canonicalFirst,
            ...bucket.slice(1),
            { worktree: "/canonical-only", expanded: true },
          ],
          "https://one.example": [remoteFirst, ...bucket],
          "https://two.example": bucket,
          malformed: { keep: true },
        },
        lastProject: {
          local: "c:/Repo",
          "https://canonical.example": "/canonical-only",
          "https://one.example": "C:/Repo",
        },
      }
      const key = canonical === undefined ? undefined : ServerConnection.Key.make(canonical)
      const restored = migrateCanonicalLocalServerState(value, key)
      const survivors = [first, middle, slash, tail, ...invalid]
      const merged = canonical === "https://canonical.example"
      const expected = {
        projects: {
          local: merged ? [...survivors, ...invalid, { worktree: "/canonical-only", expanded: true }] : survivors,
          ...(!merged
            ? {
                "https://canonical.example": [
                  canonicalFirst,
                  ...survivors.slice(1),
                  { worktree: "/canonical-only", expanded: true },
                ],
              }
            : {}),
          "https://one.example": [remoteFirst, middle, slash, tail, ...invalid],
          "https://two.example": survivors,
          malformed: { keep: true },
        },
        lastProject: merged ? { local: "c:/Repo", "https://one.example": "C:/Repo" } : value.lastProject,
      }

      expect(restored).toEqual(expected)
      expect(migrateCanonicalLocalServerState(restored, key)).toEqual(expected)
    },
  )

  test.each(
    [null, 7, [], { projects: null }, { projects: [] }, { projects: { local: "invalid" } }].map((value) => ({ value })),
  )("preserves invalid records and buckets: %j", ({ value }) => {
    expect(migrateCanonicalLocalServerState(value, ServerConnection.Key.make("https://absent.example"))).toEqual(value)
  })

  test("moves an existing canonical web bucket into local scope", () => {
    expect(
      migrateCanonicalLocalServerState(
        {
          list: [],
          projects: { "https://opencode.example.com": [{ worktree: "/remote", expanded: true }] },
          lastProject: { "https://opencode.example.com": "/remote" },
        },
        ServerConnection.Key.make("https://opencode.example.com"),
      ),
    ).toEqual({
      list: [],
      projects: { local: [{ worktree: "/remote", expanded: true }] },
      lastProject: { local: "/remote" },
    })
  })

  test("preserves existing local state while merging a canonical web bucket", () => {
    expect(
      migrateCanonicalLocalServerState(
        {
          projects: {
            local: [{ worktree: "/local", expanded: false }],
            "https://opencode.example.com": [
              { worktree: "/local", expanded: true },
              { worktree: "/remote", expanded: true },
            ],
          },
          lastProject: { local: "/local", "https://opencode.example.com": "/remote" },
        },
        ServerConnection.Key.make("https://opencode.example.com"),
      ),
    ).toEqual({
      projects: {
        local: [
          { worktree: "/local", expanded: false },
          { worktree: "/remote", expanded: true },
        ],
      },
      lastProject: { local: "/local" },
    })
  })
})

import { describe, expect, test } from "bun:test"
import {
  collectNewSessionDeepLinks,
  collectOpenProjectDeepLinks,
  drainPendingDeepLinks,
  parseDeepLink,
  parseNewSessionDeepLink,
} from "./deep-links"
import { type Session } from "@opencode-ai/sdk/v2/client"
import {
  childSessionOnPath,
  closeHomeProject,
  compareSessionTime,
  displayName,
  effectiveWorkspaceOrder,
  errorMessage,
  hasProjectPermissions,
  homeProjectNavigation,
  homeProjectDirectories,
  homeSessionServerStatus,
  latestRootSession,
  projectForSession,
  roots,
  sortedRootSessions,
  toggleHomeProjectSelection,
} from "./helpers"
import { pathKey, projectPathKey } from "@/utils/path-key"
import { ServerConnection } from "@/context/server"

const serverKey = ServerConnection.Key.make

const session = (input: Partial<Session> & Pick<Session, "id" | "directory">) =>
  ({
    title: "",
    version: "v2",
    parentID: undefined,
    messageCount: 0,
    permissions: { session: {}, share: {} },
    time: { created: 0, updated: 0, archived: undefined },
    ...input,
  }) as Session

describe("layout deep links", () => {
  test("parses open-project deep links", () => {
    expect(parseDeepLink("opencode://open-project?directory=/tmp/demo")).toBe("/tmp/demo")
  })

  test("ignores non-project deep links", () => {
    expect(parseDeepLink("opencode://other?directory=/tmp/demo")).toBeUndefined()
    expect(parseDeepLink("https://example.com")).toBeUndefined()
  })

  test("ignores malformed deep links safely", () => {
    expect(() => parseDeepLink("opencode://open-project/%E0%A4%A%")).not.toThrow()
    expect(parseDeepLink("opencode://open-project/%E0%A4%A%")).toBeUndefined()
  })

  test("parses links when URL.canParse is unavailable", () => {
    const original = Object.getOwnPropertyDescriptor(URL, "canParse")
    Object.defineProperty(URL, "canParse", { configurable: true, value: undefined })
    try {
      expect(parseDeepLink("opencode://open-project?directory=/tmp/demo")).toBe("/tmp/demo")
    } finally {
      if (original) Object.defineProperty(URL, "canParse", original)
      if (!original) Reflect.deleteProperty(URL, "canParse")
    }
  })

  test("ignores open-project deep links without directory", () => {
    expect(parseDeepLink("opencode://open-project")).toBeUndefined()
    expect(parseDeepLink("opencode://open-project?directory=")).toBeUndefined()
  })

  test("collects only valid open-project directories", () => {
    const result = collectOpenProjectDeepLinks([
      "opencode://open-project?directory=/a",
      "opencode://other?directory=/b",
      "opencode://open-project?directory=/c",
    ])
    expect(result).toEqual(["/a", "/c"])
  })

  test("parses new-session deep links with optional prompt", () => {
    expect(parseNewSessionDeepLink("opencode://new-session?directory=/tmp/demo")).toEqual({ directory: "/tmp/demo" })
    expect(parseNewSessionDeepLink("opencode://new-session?directory=/tmp/demo&prompt=hello%20world")).toEqual({
      directory: "/tmp/demo",
      prompt: "hello world",
    })
  })

  test("ignores new-session deep links without directory", () => {
    expect(parseNewSessionDeepLink("opencode://new-session")).toBeUndefined()
    expect(parseNewSessionDeepLink("opencode://new-session?directory=")).toBeUndefined()
  })

  test("collects only valid new-session deep links", () => {
    const result = collectNewSessionDeepLinks([
      "opencode://new-session?directory=/a",
      "opencode://open-project?directory=/b",
      "opencode://new-session?directory=/c&prompt=ship%20it",
    ])
    expect(result).toEqual([{ directory: "/a" }, { directory: "/c", prompt: "ship it" }])
  })

  test("drains global deep links once", () => {
    const target = {
      __OPENCODE__: {
        deepLinks: ["opencode://open-project?directory=/a"],
      },
    } as unknown as Window & { __OPENCODE__?: { deepLinks?: string[] } }

    expect(drainPendingDeepLinks(target)).toEqual(["opencode://open-project?directory=/a"])
    expect(drainPendingDeepLinks(target)).toEqual([])
  })
})

describe("projectForSession", () => {
  test.each([
    ["C:/Repo", "c:/Repo"],
    ["c:/Repo", "C:/Repo"],
  ])("matches worktree %s when session drive case differs: %s", (worktree, directory) => {
    const project = { id: "global", worktree }
    const projects = [{ id: "global", worktree: "C:/Users/JH" }, project]

    const result = projectForSession(session({ id: "drive", directory }), projects)

    expect(result).toBe(project)
  })

  test.each([
    ["C:/Sandbox", "c:/Sandbox"],
    ["c:/Sandbox", "C:/Sandbox"],
  ])("matches sandbox %s when session drive case differs: %s", (sandbox, directory) => {
    const project = { id: "repo", worktree: "C:/Repo", sandboxes: [sandbox] }

    const result = projectForSession(session({ id: "drive-sandbox", directory }), [project])

    expect(result).toBe(project)
  })

  test.each([
    ["C:/Repo", "c:/repo"],
    ["/Repo", "/repo"],
    ["//Server/Share/Repo", "//server/Share/Repo"],
    ["//Server/Share/Repo", "//Server/Share/repo"],
  ])("keeps path case distinct for %s and %s without project ID fallback", (worktree, directory) => {
    const project = { id: "repo", worktree, sandboxes: [worktree] }

    const result = projectForSession(session({ id: "case-distinct", directory }), [project])

    expect(result).toBeUndefined()
  })

  test.each([
    ["/Repo/", "/Repo"],
    ["\\\\Server\\Share\\Repo\\", "//Server/Share/Repo"],
  ])("preserves normalized path matching for %s and %s", (worktree, directory) => {
    const project = { id: "repo", worktree }

    const result = projectForSession(session({ id: "normalized", directory }), [project])

    expect(result).toBe(project)
  })

  test.each(["/repo/beta", "/repo/tango"])("matches exact worktree %s when projects share a repo id", (directory) => {
    const projects = [
      { id: "repo", worktree: "/repo/beta" },
      { id: "repo", worktree: "/repo/tango" },
    ]

    const result = projectForSession(session({ id: "ses_repo", projectID: "repo", directory }), projects)

    expect(result?.worktree).toBe(directory)
  })

  test.each(["/tmp/alpha", "/tmp/bravo"])(
    "matches exact worktree %s when non-Git projects share the global id",
    (directory) => {
      const projects = [
        { id: "global", worktree: "/tmp/alpha" },
        { id: "global", worktree: "/tmp/bravo" },
      ]

      const result = projectForSession(session({ id: "ses_global", projectID: "global", directory }), projects)

      expect(result?.worktree).toBe(directory)
    },
  )

  test("falls back to project id when the session is below the repo directory", () => {
    const project = { id: "repo", worktree: "/repo" }

    const result = projectForSession(session({ id: "ses_nested", projectID: "repo", directory: "/repo/src" }), [
      project,
    ])

    expect(result).toBe(project)
  })

  test("matches a sandbox when the session project id differs", () => {
    const project = { id: "repo", worktree: "/repo", sandboxes: ["/sandbox"] }

    const result = projectForSession(session({ id: "ses_sandbox", projectID: "other", directory: "/sandbox" }), [
      project,
    ])

    expect(result).toBe(project)
  })

  test("returns undefined when neither path nor project id matches", () => {
    const project = { id: "repo", worktree: "/repo", sandboxes: ["/sandbox"] }

    const result = projectForSession(session({ id: "ses_missing", projectID: "other", directory: "/elsewhere" }), [
      project,
    ])

    expect(result).toBeUndefined()
  })

  test("matches normalized Windows worktree instead of another global-id JH project", () => {
    const project = { id: "global", worktree: "C:/Users/JH/demo/" }
    const projects = [project, { id: "global", worktree: "C:/Users/JH" }]

    const result = projectForSession(
      session({ id: "ses_windows", projectID: "global", directory: "C:\\Users\\JH\\demo" }),
      projects,
    )

    expect(result).toBe(project)
  })

  test.each(["/repo", "/sandbox"])("prefers exact path %s over a conflicting explicit byID map", (directory) => {
    const project = { id: "repo", worktree: "/repo", sandboxes: ["/sandbox"] }
    const conflicting = { id: "other", worktree: "/other" }
    const byID = new Map([["other", conflicting]])

    const result = projectForSession(
      session({ id: "ses_explicit", projectID: "other", directory }),
      [project, conflicting],
      byID,
    )

    expect(result).toBe(project)
  })
})

describe("layout workspace helpers", () => {
  test("preserves lowercase drive spelling in storage path keys", () => {
    expect(String(pathKey("c:/Repo"))).toBe("c:/Repo")
  })

  test.each([
    ["c:/Repo", "C:/Repo"],
    ["C:\\Repo\\", "C:/Repo"],
    ["/Repo", "/Repo"],
    ["/repo", "/repo"],
    ["//Server/Share", "//Server/Share"],
  ])("normalizes only Windows drive spelling for project identity: %s", (directory, expected) => {
    expect(String(projectPathKey(directory))).toBe(expected)
  })

  test.each([
    ["C:/Repo", "c:/Repo"],
    ["c:/Repo", "C:/Repo"],
    ["/Repo/", "/Repo"],
    ["\\\\Server\\Share\\Repo\\", "//Server/Share/Repo"],
  ])("filters visible roots for %s when session directory is %s", (directory, equivalent) => {
    const visible = session({ id: "visible", directory: equivalent })
    const sessions = [
      visible,
      session({ id: "child", directory: equivalent, parentID: "visible" }),
      session({ id: "archived", directory: equivalent, time: { created: 1, updated: 1, archived: 1 } }),
      session({ id: "different", directory: `${equivalent}/other` }),
    ]

    const result = roots({ path: { directory }, session: sessions })

    expect(result).toEqual([visible])
  })

  test.each([
    ["C:/Repo", "c:/repo"],
    ["/Repo", "/repo"],
    ["//Server/Share/Repo", "//server/Share/Repo"],
  ])("excludes roots when path case differs beyond the drive: %s and %s", (directory, distinct) => {
    const sessions = [session({ id: "distinct", directory: distinct })]

    const result = roots({ path: { directory }, session: sessions })

    expect(result).toEqual([])
  })

  test("deduplicates drive variants while preserving local spelling, first raw workspace and persisted order", () => {
    const local = "c:\\Repo\\"
    const dirs = ["C:/Repo", "c:\\Alpha\\", "C:/Alpha", "C:/Beta", "c:/Beta", "C:/Gamma"]
    const persisted = ["C:/Repo", "c:/Beta", "C:/Alpha", "c:/Alpha", "C:/missing"]

    const result = effectiveWorkspaceOrder(local, dirs, persisted)

    expect(result).toEqual([local, "C:/Beta", "c:\\Alpha\\", "C:/Gamma"])
  })

  test("deduplicates drive variants in live order without persisted workspaces", () => {
    const dirs = ["c:/Repo", "c:/Alpha", "C:/Alpha", "C:/Beta"]

    const result = effectiveWorkspaceOrder("C:/Repo", dirs)

    expect(result).toEqual(["C:/Repo", "c:/Alpha", "C:/Beta"])
  })

  test("keeps folder, POSIX and UNC case distinct in workspace order", () => {
    const dirs = ["C:/repo", "/Repo", "/repo", "//Server/Share", "//server/Share"]

    const result = effectiveWorkspaceOrder("C:/Repo", dirs)

    expect(result).toEqual(["C:/Repo", ...dirs])
  })

  test.each([
    { selected: "C:/Repo", directory: "c:/Repo", sameServer: true, clears: true },
    { selected: "c:/Repo", directory: "C:/Repo", sameServer: true, clears: true },
    { selected: "C:/Repo", directory: "c:/Repo", sameServer: false, clears: false },
    { selected: "C:/Repo", directory: "c:/repo", sameServer: true, clears: false },
    { selected: "/Repo", directory: "/repo", sameServer: true, clears: false },
    { selected: "//Server/Share", directory: "//server/Share", sameServer: true, clears: false },
  ])("toggles Home selection with path and server identity: %j", (input) => {
    const server = serverKey("https://windows.example")
    const current = {
      server: input.sameServer ? server : serverKey("https://other.example"),
      directory: input.selected,
    }

    const result = toggleHomeProjectSelection(current, server, input.directory)

    expect(result).toEqual(input.clears ? { server } : { server, directory: input.directory })
  })

  test.each([
    { selected: "C:/Repo", directory: "c:/Repo", sameServer: true, clears: true },
    { selected: "c:/Repo", directory: "C:/Repo", sameServer: true, clears: true },
    { selected: "C:/Repo", directory: "c:/Repo", sameServer: false, clears: false },
    { selected: "C:/Repo", directory: "c:/repo", sameServer: true, clears: false },
    { selected: "/Repo", directory: "/repo", sameServer: true, clears: false },
    { selected: "//Server/Share", directory: "//server/Share", sameServer: true, clears: false },
  ])("closes Home selection with path and server identity: %j", (input) => {
    const server = serverKey("https://windows.example")
    const current = {
      server: input.sameServer ? server : serverKey("https://other.example"),
      directory: input.selected,
    }
    const closed: string[] = []

    const result = closeHomeProject(current, server, { close: (directory) => closed.push(directory) }, input.directory)

    expect(closed).toEqual([input.directory])
    expect(result).toEqual(input.clears ? { server } : current)
  })

  test("normalizes trailing slash in workspace key", () => {
    expect(String(pathKey("/tmp/demo///"))).toBe("/tmp/demo")
    expect(String(pathKey("C:\\tmp\\demo\\\\"))).toBe("C:/tmp/demo")
  })

  test("preserves posix and drive roots in workspace key", () => {
    expect(String(pathKey("/"))).toBe("/")
    expect(String(pathKey("///"))).toBe("/")
    expect(String(pathKey("C:\\"))).toBe("C:/")
    expect(String(pathKey("C://"))).toBe("C:/")
    expect(String(pathKey("C:///"))).toBe("C:/")
  })

  test("keeps local first while preserving known order", () => {
    const result = effectiveWorkspaceOrder("/root", ["/root", "/b", "/c"], ["/root", "/c", "/a", "/b"])
    expect(result).toEqual(["/root", "/c", "/b"])
  })

  test("finds the latest root session across workspaces", () => {
    const result = latestRootSession(
      [
        {
          path: { directory: "/root" },
          session: [session({ id: "root", directory: "/root", time: { created: 1, updated: 1, archived: undefined } })],
        },
        {
          path: { directory: "/workspace" },
          session: [
            session({
              id: "workspace",
              directory: "/workspace",
              time: { created: 2, updated: 2, archived: undefined },
            }),
          ],
        },
      ],
      120_000,
    )

    expect(result?.id).toBe("workspace")
  })

  test("sorts recent sessions by persisted update time instead of id", () => {
    const result = sortedRootSessions(
      {
        path: { directory: "/workspace" },
        session: [
          session({ id: "ses_z", directory: "/workspace", time: { created: 1, updated: 2, archived: undefined } }),
          session({ id: "ses_a", directory: "/workspace", time: { created: 1, updated: 3, archived: undefined } }),
        ],
      },
      3,
    )

    expect(result.map((item) => item.id)).toEqual(["ses_a", "ses_z"])
  })

  test("uses id only to break equal session timestamps", () => {
    const sessions = [
      session({ id: "ses_z", directory: "/workspace", time: { created: 1, updated: 2, archived: undefined } }),
      session({ id: "ses_a", directory: "/workspace", time: { created: 1, updated: 2, archived: undefined } }),
    ]

    expect(sessions.sort(compareSessionTime).map((item) => item.id)).toEqual(["ses_a", "ses_z"])
  })

  test("detects project permissions with a filter", () => {
    const result = hasProjectPermissions(
      {
        root: [{ id: "perm-root" }, { id: "perm-hidden" }],
        child: [{ id: "perm-child" }],
      },
      (item) => item.id === "perm-child",
    )

    expect(result).toBe(true)
  })

  test("ignores project permissions filtered out", () => {
    const result = hasProjectPermissions(
      {
        root: [{ id: "perm-root" }],
      },
      () => false,
    )

    expect(result).toBe(false)
  })

  test("ignores archived and child sessions when finding latest root session", () => {
    const result = latestRootSession(
      [
        {
          path: { directory: "/workspace" },
          session: [
            session({
              id: "archived",
              directory: "/workspace",
              time: { created: 10, updated: 10, archived: 10 },
            }),
            session({
              id: "child",
              directory: "/workspace",
              parentID: "parent",
              time: { created: 20, updated: 20, archived: undefined },
            }),
            session({
              id: "root",
              directory: "/workspace",
              time: { created: 30, updated: 30, archived: undefined },
            }),
          ],
        },
      ],
      120_000,
    )

    expect(result?.id).toBe("root")
  })

  test("finds the direct child on the active session path", () => {
    const list = [
      session({ id: "root", directory: "/workspace" }),
      session({ id: "child", directory: "/workspace", parentID: "root" }),
      session({ id: "leaf", directory: "/workspace", parentID: "child" }),
    ]

    expect(childSessionOnPath(list, "root", "leaf")?.id).toBe("child")
    expect(childSessionOnPath(list, "child", "leaf")?.id).toBe("leaf")
    expect(childSessionOnPath(list, "root", "root")).toBeUndefined()
    expect(childSessionOnPath(list, "root", "other")).toBeUndefined()
  })

  test("formats fallback project display name", () => {
    expect(displayName({ worktree: "/tmp/app" })).toBe("app")
    expect(displayName({ worktree: "/tmp/app", name: "My App" })).toBe("My App")
    expect(displayName({ worktree: "/" })).toBe("/")
  })

  test("scopes home project selection by server", () => {
    expect(
      toggleHomeProjectSelection(undefined, serverKey("https://debian.example"), "/home/luke/repos/amazon"),
    ).toEqual({
      server: serverKey("https://debian.example"),
      directory: "/home/luke/repos/amazon",
    })
    expect(
      toggleHomeProjectSelection(
        { server: serverKey("https://windows.example"), directory: "/home/luke/repos/amazon" },
        serverKey("https://debian.example"),
        "/home/luke/repos/amazon",
      ),
    ).toEqual({ server: serverKey("https://debian.example"), directory: "/home/luke/repos/amazon" })
    expect(
      toggleHomeProjectSelection(
        { server: serverKey("https://debian.example"), directory: "/home/luke/repos/amazon" },
        serverKey("https://debian.example"),
        "/home/luke/repos/amazon",
      ),
    ).toEqual({ server: serverKey("https://debian.example") })
  })

  test("closes a home project through its server context", () => {
    const closed: string[] = []

    expect(
      closeHomeProject(
        { server: serverKey("https://windows.example"), directory: "/shared" },
        serverKey("https://debian.example"),
        { close: (directory) => closed.push(directory) },
        "/shared",
      ),
    ).toEqual({ server: serverKey("https://windows.example"), directory: "/shared" })
    expect(closed).toEqual(["/shared"])
    expect(
      closeHomeProject(
        { server: serverKey("https://debian.example"), directory: "/shared" },
        serverKey("https://debian.example"),
        { close: (directory) => closed.push(directory) },
        "/shared",
      ),
    ).toEqual({ server: serverKey("https://debian.example") })
  })

  test("defers home project navigation until its server is active", () => {
    expect(
      homeProjectNavigation(serverKey("sidecar"), serverKey("https://debian.example"), "/YW1hem9u/session"),
    ).toEqual({
      server: serverKey("https://debian.example"),
      href: "/YW1hem9u/session",
    })
    expect(
      homeProjectNavigation(
        serverKey("https://debian.example"),
        serverKey("https://debian.example"),
        "/YW1hem9u/session",
      ),
    ).toEqual({
      href: "/YW1hem9u/session",
    })
  })

  test("preserves picker order when adding multiple projects", () => {
    expect(homeProjectDirectories(["/first", "/second"])).toEqual(["/first", "/second"])
    expect(homeProjectDirectories("/only")).toEqual(["/only"])
    expect(homeProjectDirectories(null)).toEqual([])
  })

  test("hides status derived from an inactive server", () => {
    let reads = 0
    const status = () => {
      reads++
      return { working: true, tint: "red" }
    }
    expect(homeSessionServerStatus(false, status)).toEqual({
      working: false,
      tint: undefined,
    })
    expect(reads).toBe(0)
    expect(homeSessionServerStatus(true, status)).toEqual({
      working: true,
      tint: "red",
    })
    expect(reads).toBe(1)
  })

  test("extracts api error message and fallback", () => {
    expect(errorMessage({ data: { message: "boom" } }, "fallback")).toBe("boom")
    expect(errorMessage(new Error("broken"), "fallback")).toBe("broken")
    expect(errorMessage("unknown", "fallback")).toBe("fallback")
  })
})

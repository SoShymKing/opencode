import { describe, expect, test } from "bun:test"
import type { SessionInfo } from "@opencode/client/promise"
import {
  closeHomeProject,
  errorMessage,
  homeProjectDirectories,
  projectForSession,
  toggleHomeProjectSelection,
} from "./helpers"
import { ServerConnection } from "@/runtime/server/registry"

const serverKey = ServerConnection.Key.make

const session = (input: Partial<SessionInfo> & Pick<SessionInfo, "id"> & { directory: string }) =>
  ({
    projectID: "project",
    title: "",
    cost: 0,
    tokens: { input: 0, output: 0, reasoning: 0, cache: { read: 0, write: 0 } },
    parentID: undefined,
    time: { created: 0, updated: 0, archived: undefined },
    ...input,
    location: { directory: input.directory },
    directory: undefined,
  }) as SessionInfo

type Project = {
  id: string
  worktree: string
  sandboxes: string[]
  worktrees?: { directory: string; strategy?: string }[]
}

const enriched: { name: string; project: Project }[] = [
  {
    name: "matching project id",
    project: {
      id: "project",
      worktree: "/repo",
      sandboxes: ["/workspaces/feature"],
      worktrees: [{ directory: "/repo" }, { directory: "/workspaces/feature", strategy: "git" }],
    },
  },
  { name: "stale project id", project: { id: "updated", worktree: "/repo", sandboxes: ["/workspaces/feature"] } },
]

describe("layout workspace helpers", () => {
  test("closes a selected drive alias using the original directory", () => {
    const server = serverKey("https://windows.example")
    const closed: string[] = []
    expect(
      closeHomeProject(
        { server, directory: "c:/Repo" },
        server,
        { close: (directory) => closed.push(directory) },
        "C:\\Repo",
      ),
    ).toEqual({ server })
    expect(closed).toEqual(["C:\\Repo"])
  })

  test.each([
    { selected: "c:/Repo", directory: "C:\\Repo\\", sameServer: true, clears: true },
    { selected: "C:/Repo", directory: "c:/Repo", sameServer: true, clears: true },
    { selected: "/Repo/", directory: "/Repo", sameServer: true, clears: true },
    { selected: "\\\\Server\\Share\\Repo\\", directory: "//Server/Share/Repo", sameServer: true, clears: true },
    { selected: "c:/Repo", directory: "C:\\Repo", sameServer: false, clears: false },
    { selected: "C:/Repo", directory: "c:/repo", sameServer: true, clears: false },
    { selected: "/Repo", directory: "/repo", sameServer: true, clears: false },
    { selected: "//Server/Share", directory: "//server/Share", sameServer: true, clears: false },
  ])("toggles and closes Home selection with path and server identity: %j", (input) => {
    const server = serverKey("https://windows.example")

    const current = {
      server: input.sameServer ? server : serverKey("https://other.example"),
      directory: input.selected,
    }

    const closed: string[] = []

    expect(toggleHomeProjectSelection(current, server, input.directory)).toEqual(
      input.clears ? { server } : { server, directory: input.directory },
    )
    expect(closeHomeProject(current, server, { close: (directory) => closed.push(directory) }, input.directory)).toEqual(
      input.clears ? { server } : current,
    )
    expect(closed).toEqual([input.directory])
  })

  test.each(enriched)("keeps the enriched project for a nested workspace session ($name)", ({ project }) => {
    expect(
      projectForSession(session({ id: "feature", directory: "/workspaces/feature/packages/app" }), [project]),
    ).toBe(project)
  })

  test.each([
    ["C:/Repo", "c:/Repo"],
    ["c:/Repo", "C:/Repo"],
    ["/Repo/", "/Repo"],
    ["\\\\Server\\Share\\Repo\\", "//Server/Share/Repo"],
  ])("matches normalized project worktree %s for session %s", (worktree, directory) => {
    const project = { id: "global", worktree }
    expect(
      projectForSession(session({ id: "alias", projectID: "global", directory }), [
        project,
        { id: "global", worktree: "/other" },
      ]),
    ).toBe(project)
  })

  test.each([
    ["C:/Sandbox", "c:/Sandbox"],
    ["c:/Sandbox", "C:/Sandbox"],
    ["/sandbox", "/sandbox"],
  ])("matches sandbox %s when session project ID differs", (sandbox, directory) => {
    const project = { id: "repo", worktree: "/repo", sandboxes: [sandbox] }
    expect(projectForSession(session({ id: "sandbox", projectID: "other", directory }), [project])).toBe(project)
  })

  test.each([
    ["C:/Repo", "c:/repo"],
    ["/Repo", "/repo"],
    ["//Server/Share/Repo", "//server/Share/Repo"],
    ["//Server/Share/Repo", "//Server/Share/repo"],
    ["/repo", "/elsewhere"],
  ])("keeps project path case distinct without ID fallback: %s and %s", (worktree, directory) => {
    expect(
      projectForSession(session({ id: "distinct", projectID: "other", directory }), [
        { id: "repo", worktree, sandboxes: [worktree] },
      ]),
    ).toBeUndefined()
  })

  test.each(["repo", "global"])("matches exact worktrees sharing the %s ID", (projectID) => {
    const projects = ["/repo/beta", "/repo/tango"].map((worktree) => ({ id: projectID, worktree }))
    projects.forEach((project) => {
      expect(
        projectForSession(session({ id: "shared", projectID, directory: project.worktree }), projects),
      ).toBe(project)
    })
  })

  test("falls back to project ID for a nested session", () => {
    const project = { id: "repo", worktree: "/repo" }
    expect(projectForSession(session({ id: "nested", projectID: "repo", directory: "/repo/src" }), [project])).toBe(
      project,
    )
  })

  test.each(["/repo", "/sandbox"])("prefers exact project path %s over a conflicting project ID", (directory) => {
    const project = { id: "repo", worktree: "/repo", sandboxes: ["/sandbox"] }
    expect(
      projectForSession(session({ id: "conflict", projectID: "other", directory }), [
        project,
        { id: "other", worktree: "/other" },
      ]),
    ).toBe(project)
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

  test("extracts api error message and fallback", () => {
    expect(errorMessage({ data: { message: "boom" } }, "fallback")).toBe("boom")
    expect(errorMessage(new Error("broken"), "fallback")).toBe("broken")
    expect(errorMessage("unknown", "fallback")).toBe("fallback")
  })

  test("preserves picker order when adding multiple projects", () => {
    expect(homeProjectDirectories(["/first", "/second"])).toEqual(["/first", "/second"])
    expect(homeProjectDirectories("/only")).toEqual(["/only"])
    expect(homeProjectDirectories(null)).toEqual([])
  })
})

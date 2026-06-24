/** @jsxImportSource @opentui/solid */
import { describe, expect, test } from "bun:test"
import { tmpdir } from "../../../fixture/fixture"
import { mount, wait } from "./sync-fixture"
import type { GlobalEvent } from "@opencode-ai/sdk/v2"
import { SESSION_LIST_REQUEST_WINDOW_KEY } from "../../../../src/util/session-list-window"

const DAY = 24 * 60 * 60 * 1000

function expectStartWithin(input: { start: number | undefined; days: number; before: number; after: number }) {
  expect(input.start).toBeDefined()
  if (input.start === undefined) return
  expect(Number.isFinite(input.start)).toBe(true)
  expect(input.start).toBeGreaterThanOrEqual(input.before - input.days * DAY)
  expect(input.start).toBeLessThanOrEqual(input.after - input.days * DAY)
}

function expectSessionListStartWithin(input: { url: URL | undefined; days: number; before: number; after: number }) {
  expect(input.url).toBeDefined()
  if (!input.url) return
  const start = input.url.searchParams.get("start")
  expect(start).toBeTruthy()
  if (!start) return
  expectStartWithin({ start: Number(start), days: input.days, before: input.before, after: input.after })
}

function branchEvent(branch: string, workspace?: string): GlobalEvent {
  return {
    directory: "/tmp/other",
    project: "proj_test",
    workspace,
    payload: {
      id: `evt_vcs_${branch}`,
      type: "vcs.branch.updated",
      properties: { branch },
    },
  }
}

describe("tui sync", () => {
  test("session list requests use the default 30 day window", async () => {
    await using tmp = await tmpdir()
    await Bun.write(`${tmp.path}/kv.json`, "{}")
    const { app, sync, session } = await mount(undefined, tmp.path)

    try {
      const before = Date.now()
      await sync.session.refresh()
      const after = Date.now()

      expectSessionListStartWithin({ url: session.at(-1), days: 30, before, after })
    } finally {
      app.renderer.destroy()
    }
  })

  test("session list requests use the configured day window", async () => {
    await using tmp = await tmpdir()
    await Bun.write(`${tmp.path}/kv.json`, "{}")
    const { app, kv, sync, session } = await mount(undefined, tmp.path)

    try {
      kv.set(SESSION_LIST_REQUEST_WINDOW_KEY, 90)
      const before = Date.now()
      await sync.session.refresh()
      const after = Date.now()

      expectSessionListStartWithin({ url: session.at(-1), days: 90, before, after })
    } finally {
      app.renderer.destroy()
    }
  })

  test("session query exposes the configured request window for search callers", async () => {
    await using tmp = await tmpdir()
    await Bun.write(`${tmp.path}/kv.json`, "{}")
    const { app, kv, sync } = await mount(undefined, tmp.path)

    try {
      kv.set(SESSION_LIST_REQUEST_WINDOW_KEY, 90)
      const before = Date.now()
      const query = sync.session.query()
      const after = Date.now()

      expectStartWithin({ start: query.start, days: 90, before, after })

      kv.set(SESSION_LIST_REQUEST_WINDOW_KEY, "all")
      expect(sync.session.query().start).toBeUndefined()
    } finally {
      app.renderer.destroy()
    }
  })

  test("session list requests can include all sessions", async () => {
    await using tmp = await tmpdir()
    await Bun.write(`${tmp.path}/kv.json`, "{}")
    const { app, kv, sync, session } = await mount(undefined, tmp.path)

    try {
      kv.set(SESSION_LIST_REQUEST_WINDOW_KEY, "all")
      await sync.session.refresh()

      expect(session.at(-1)?.searchParams.get("start")).toBeNull()
    } finally {
      app.renderer.destroy()
    }
  })

  test("session list requests fall back to the default window for invalid settings", async () => {
    await using tmp = await tmpdir()
    await Bun.write(`${tmp.path}/kv.json`, "{}")
    const { app, kv, sync, session } = await mount(undefined, tmp.path)

    try {
      kv.set(SESSION_LIST_REQUEST_WINDOW_KEY, -1)
      const before = Date.now()
      await sync.session.refresh()
      const after = Date.now()

      expectSessionListStartWithin({ url: session.at(-1), days: 30, before, after })
    } finally {
      app.renderer.destroy()
    }
  })

  test("refresh scopes sessions by default and lists project sessions when disabled", async () => {
    await using tmp = await tmpdir()
    await Bun.write(`${tmp.path}/kv.json`, "{}")
    const { app, kv, sync, session } = await mount(undefined, tmp.path)

    try {
      expect(kv.get("session_directory_filter_enabled", true)).toBe(true)
      expect(session.at(-1)?.searchParams.get("scope")).toBeNull()
      expect(session.at(-1)?.searchParams.get("path")).toBe("packages/tui")

      kv.set("session_directory_filter_enabled", false)
      await sync.session.refresh()

      expect(session.at(-1)?.searchParams.get("scope")).toBe("project")
      expect(session.at(-1)?.searchParams.get("path")).toBeNull()
    } finally {
      app.renderer.destroy()
    }
  })

  test("vcs branch updates only apply for the active workspace", async () => {
    await using tmp = await tmpdir()
    await Bun.write(`${tmp.path}/kv.json`, "{}")
    const { app, emit, project, sync } = await mount(undefined, tmp.path)

    try {
      expect(sync.data.vcs?.branch).toBe("main")

      project.workspace.set("ws_a")
      emit(branchEvent("other", "ws_b"))
      await Bun.sleep(30)

      expect(sync.data.vcs?.branch).toBe("main")

      emit(branchEvent("feature", "ws_a"))
      await wait(() => sync.data.vcs?.branch === "feature")

      expect(sync.data.vcs?.branch).toBe("feature")
    } finally {
      app.renderer.destroy()
    }
  })
})

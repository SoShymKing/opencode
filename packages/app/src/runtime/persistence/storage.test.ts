import { afterEach, describe, expect, test } from "bun:test"
import { renderToString } from "solid-js/web"
import { Schema } from "effect"
import { ServerScope } from "@/runtime/server/scope"
import { Persist, persisted, removePersisted } from "./storage"
import { flushPersisted } from "./persist"
import { Persistence } from "./schema"

const platform = {
  platform: "web" as const,
  openExternal() {},
  restart: async () => {},
  notify: async () => {},
}
const valueSchema = Persistence.struct({ value: Schema.Number })
const storage = localStorage

function failingStorage(prefix: string, error: Error) {
  const calls: string[] = []
  Object.defineProperty(globalThis, "localStorage", {
    configurable: true,
    value: {
      get length() {
        return storage.length
      },
      key: (index: number) => storage.key(index),
      clear: () => storage.clear(),
      getItem: (key: string) => storage.getItem(key),
      removeItem: (key: string) => storage.removeItem(key),
      setItem: (key: string, value: string) => {
        calls.push(key)
        if (key.startsWith(prefix)) throw error
        storage.setItem(key, value)
      },
    } satisfies Storage,
  })
  return calls
}

afterEach(() => {
  Object.defineProperty(globalThis, "localStorage", { configurable: true, value: storage })
  localStorage.clear()
})

describe("persist targets", () => {
  test("preserves shipped drive-case workspace keys and their backslash aliases", () => {
    expect(Persist.workspace("C:/Repo", "prompt")).toEqual({
      storage: "opencode.workspace.C--Repo.kw5499.dat",
      workspaceStorageAliases: ["opencode.workspace.C--Repo.geem0.dat"],
      key: "workspace:prompt",
    })
    expect(Persist.workspace("c:/Repo", "prompt")).toEqual({
      storage: "opencode.workspace.c--Repo.1pg8zu5.dat",
      workspaceStorageAliases: ["opencode.workspace.c--Repo.ube63c.dat"],
      key: "workspace:prompt",
    })
    expect(Persist.workspace("/Repo", "prompt").storage).not.toBe(Persist.workspace("/repo", "prompt").storage)
  })

  test("workspace storage names are safe file names and normalize Windows paths", () => {
    const raw = Persist.workspace("C:\\Users\\foo", "vcs")
    const normalized = Persist.workspace("C:/Users/foo", "vcs")

    expect(raw.storage).toMatch(/^opencode\.workspace\.[a-zA-Z0-9._-]+\.dat$/)
    expect(raw.storage).toBe(normalized.storage)
    expect(raw.workspaceStorageAliases).toHaveLength(1)
    expect(raw.workspaceStorageAliases![0]).toMatch(/^opencode\.workspace\.[a-zA-Z0-9._-]+\.dat$/)
    expect(raw.workspaceStorageAliases![0]).not.toBe(raw.storage)
    expect(normalized.workspaceStorageAliases).toEqual(raw.workspaceStorageAliases)
    expect(Persist.workspace("/home/luke/repo", "vcs").workspaceStorageAliases).toBeUndefined()
  })

  test("removes workspace storage aliases and draft storage when removing persisted targets", () => {
    const workspace = Persist.workspace("C:\\Users\\foo", "terminal")
    const draft = Persist.draft("draft-a", "prompt")

    const keys = [
      `${workspace.storage}:${workspace.key}`,
      `${workspace.workspaceStorageAliases![0]}:${workspace.key}`,
      `${draft.storage}:${draft.key}`,
    ]

    keys.forEach((key) => localStorage.setItem(key, '{"value":1}'))

    removePersisted(workspace)
    removePersisted(draft)

    expect(keys.map((key) => localStorage.getItem(key))).toEqual([null, null, null])
  })

  test("draft target isolates storage per draft and namespaces keys", () => {
    const a = Persist.draft("draft-a", "prompt")
    const b = Persist.draft("draft-b", "prompt")

    expect(a.key).toBe("draft:prompt")
    expect(a.storage).not.toBe(b.storage)
    expect(a.storage).not.toBe(Persist.workspace("/home/luke/repo", "prompt").storage)
  })

  test("server workspace target preserves local storage and isolates remote storage", () => {
    const local = Persist.serverWorkspace(ServerScope.local, "/home/luke/repo", "prompt")
    const windows = Persist.serverWorkspace("https://windows.example" as ServerScope, "/home/luke/repo", "prompt")
    const debian = Persist.serverWorkspace("https://debian.example" as ServerScope, "/home/luke/repo", "prompt")

    expect(local).toEqual(Persist.workspace("/home/luke/repo", "prompt"))
    expect(windows.storage).not.toBe(local.storage)
    expect(debian.storage).not.toBe(local.storage)
    expect(debian.storage).not.toBe(windows.storage)
    expect(windows.workspaceStorageAliases).toBeUndefined()
    expect(debian.workspaceStorageAliases).toBeUndefined()
  })

  test("server global target preserves local key and isolates remote keys", () => {
    expect(Persist.serverGlobal(ServerScope.local, "notification")).toEqual(Persist.global("notification"))
    expect(Persist.serverGlobal("https://debian.example" as ServerScope, "notification")).toEqual({
      storage: "opencode.global.dat",
      key: "https://debian.example\0notification",
    })
  })

  test("server global target cannot collide when scope and key contain colons", () => {
    expect(Persist.serverGlobal("a:b" as ServerScope, "c")).not.toEqual(Persist.serverGlobal("a" as ServerScope, "b:c"))
  })
})

describe("persisted storage relocation", () => {
  test("hydrates distinct shipped Windows keys and imports only their own backslash aliases", () => {
    localStorage.setItem("opencode.workspace.C--Repo.kw5499.dat:workspace:prompt", '{"value":2}')
    localStorage.setItem("opencode.workspace.c--Repo.ube63c.dat:workspace:prompt", '{"value":3}')
    renderToString(() => {
      const upper = persisted(Persist.workspace("C:/Repo", "prompt"), valueSchema, { value: 1 }, platform)
      const lower = persisted(Persist.workspace("c:/Repo", "prompt"), valueSchema, { value: 1 }, platform)
      expect([upper[0].value, lower[0].value]).toEqual([2, 3])
      expect(localStorage.getItem("opencode.workspace.c--Repo.1pg8zu5.dat:workspace:prompt")).toBe('{"value":3}')
      expect(localStorage.getItem("opencode.workspace.c--Repo.ube63c.dat:workspace:prompt")).toBeNull()
      return ""
    })
  })

  test("moves direct previous keys into scoped storage and keeps a current value on repeat hydration", () => {
    const target = { ...Persist.global("direct-migration"), previousKey: "legacy.workspace" }
    localStorage.setItem("legacy.workspace", '{"value":2}')
    renderToString(() => {
      expect(persisted(target, valueSchema, { value: 1 }, platform)[0].value).toBe(2)
      expect(localStorage.getItem("opencode.global.dat:direct-migration")).toBe('{"value":2}')
      expect(localStorage.getItem("legacy.workspace")).toBeNull()
      localStorage.setItem("legacy.workspace", '{"value":3}')
      expect(persisted(target, valueSchema, { value: 1 }, platform)[0].value).toBe(2)
      expect(localStorage.getItem("legacy.workspace")).toBe('{"value":3}')
      return ""
    })
  })

  test("failed quota writes do not appear persisted or consume the direct legacy source", () => {
    const target = { storage: "opencode.quota.migration", key: "value", previousKey: "legacy.quota" }
    localStorage.setItem("legacy.quota", '{"value":2}')
    failingStorage("opencode.quota.migration:", new DOMException("quota", "QuotaExceededError"))
    renderToString(() => {
      const [state, setState] = persisted(target, valueSchema, { value: 1 }, platform)
      expect(state.value).toBe(1)
      setState("value", 3)
      flushPersisted()
      expect(persisted(target, valueSchema, { value: 1 }, platform)[0].value).toBe(1)
      expect(localStorage.getItem("opencode.quota.migration:value")).toBeNull()
      expect(localStorage.getItem("legacy.quota")).toBe('{"value":2}')
      return ""
    })
  })

  test("a throwing scope does not disable healthy prefixed or direct storage", () => {
    const writes = failingStorage("opencode.throw.scope:", new Error("storage set failed"))
    renderToString(() => {
      const target = { storage: "opencode.throw.scope", key: "value" }
      const [, setBad] = persisted(target, valueSchema, { value: 1 }, platform)
      setBad("value", 2)
      flushPersisted()
      const calls = writes.length
      setBad("value", 3)
      flushPersisted()
      expect(writes).toHaveLength(calls)
      expect(persisted(target, valueSchema, { value: 1 }, platform)[0].value).toBe(1)
      const [, setGood] = persisted(Persist.global("healthy-scope"), valueSchema, { value: 1 }, platform)
      const [, setDirect] = persisted("healthy-direct", valueSchema, { value: 1 }, platform)
      setGood("value", 4)
      setDirect("value", 5)
      flushPersisted()
      expect(localStorage.getItem("opencode.global.dat:healthy-scope")).toBe('{"value":4}')
      expect(localStorage.getItem("healthy-direct")).toBe('{"value":5}')
      return ""
    })
  })

  test("malformed JSON is discarded before hydration", () => {
    localStorage.setItem("opencode.global.dat:malformed", '{"value":"\\x"}')
    renderToString(() => {
      expect(persisted(Persist.global("malformed"), valueSchema, { value: 1 }, platform)[0].value).toBe(1)
      expect(localStorage.getItem("opencode.global.dat:malformed")).toBeNull()
      return ""
    })
  })
})

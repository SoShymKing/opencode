import { NodeFileSystem } from "@effect/platform-node"
import { Flock } from "@opencode/util/flock"
import { Global } from "@opencode/util/global"
import { Effect, FileSystem, Option, Schema } from "effect"
import { expect, test } from "bun:test"
import { parse } from "jsonc-parser"
import path from "path"
import { Config } from "../src/config"
import { OPENCODE_CHANNEL } from "../src/version"
import { tmpdir } from "./fixture/tmpdir"

function run<A, E>(directory: string, effect: Effect.Effect<A, E, Config.Service>) {
  return Effect.runPromise(
    effect.pipe(
      Effect.provide(Config.layer),
      Effect.provide(Global.layerWith({ config: directory, state: directory })),
      Effect.provide(NodeFileSystem.layer),
    ),
  )
}

test("migrates legacy session windows with existing cli config and preserves scope defaults", async () => {
  for (const item of [
    { value: 7, expected: 7 },
    { value: " 14 ", expected: 14 },
    { value: " ALL ", expected: "all" },
    { value: 0, expected: 30 },
    { value: -1, expected: 30 },
    { value: 1.5, expected: 30 },
    { value: Number.MAX_SAFE_INTEGER, expected: 30 },
    { value: "bad", expected: 30 },
    { value: null, expected: 30 },
  ]) {
    await using directory = await tmpdir()
    await Bun.write(path.join(directory.path, "cli.json"), JSON.stringify({ tabs: { scope: "global" } }))
    const legacy = { session_list_request_window_days: item.value }
    await Bun.write(path.join(directory.path, "kv.json"), JSON.stringify(legacy))
    await run(directory.path, Effect.gen(function* () {
      const service = yield* Config.Service
      yield* service.get()
      yield* service.get()
    }))
    const channel = process.env.OPENCODE_TUI_CHANNEL ?? OPENCODE_CHANNEL
    expect(await Bun.file(path.join(directory.path, channel, "tui/session-list.json")).json()).toEqual({
      allProjects: true,
      session_list_request_window_days: item.expected,
    })
    expect(await Bun.file(path.join(directory.path, "kv.json")).json()).toEqual(legacy)
  }
})

test("migrates session windows on update using inline scope and the effective channel", async () => {
  await using directory = await tmpdir()
  const previous = { channel: process.env.OPENCODE_TUI_CHANNEL, content: process.env.OPENCODE_CLI_CONFIG_CONTENT }
  process.env.OPENCODE_TUI_CHANNEL = "migration-test"
  process.env.OPENCODE_CLI_CONFIG_CONTENT = JSON.stringify({ tabs: { scope: "global" } })
  try {
    await Bun.write(path.join(directory.path, "cli.json"), JSON.stringify({ tabs: { scope: "cwd" } }))
    await Bun.write(path.join(directory.path, "kv.json"), JSON.stringify({ session_list_request_window_days: 90 }))
    await run(directory.path, Effect.gen(function* () {
      const service = yield* Config.Service
      yield* service.update((draft) => { draft.mouse = false })
    }))
    expect(await Bun.file(path.join(directory.path, "migration-test/tui/session-list.json")).json()).toEqual({
      allProjects: true, session_list_request_window_days: 90,
    })
    expect(await Bun.file(path.join(directory.path, OPENCODE_CHANNEL, "tui/session-list.json")).exists()).toBe(false)
    delete process.env.OPENCODE_TUI_CHANNEL
    delete process.env.OPENCODE_CLI_CONFIG_CONTENT
    await run(directory.path, Effect.gen(function* () {
      const service = yield* Config.Service
      yield* service.get()
    }))
    expect(await Bun.file(path.join(directory.path, OPENCODE_CHANNEL, "tui/session-list.json")).json()).toEqual({
      allProjects: false, session_list_request_window_days: 90,
    })
  } finally {
    if (previous.channel === undefined) delete process.env.OPENCODE_TUI_CHANNEL
    else process.env.OPENCODE_TUI_CHANNEL = previous.channel
    if (previous.content === undefined) delete process.env.OPENCODE_CLI_CONFIG_CONTENT
    else process.env.OPENCODE_CLI_CONFIG_CONTENT = previous.content
  }
})

test("does not migrate session-list outside state for an invalid channel", async () => {
  await using directory = await tmpdir()
  const state = path.join(directory.path, "state")
  const previous = process.env.OPENCODE_TUI_CHANNEL
  await Bun.write(path.join(state, "cli.json"), "{}")
  await Bun.write(path.join(state, "kv.json"), JSON.stringify({ session_list_request_window_days: 7 }))
  process.env.OPENCODE_TUI_CHANNEL = "../outside"
  try {
    await run(
      state,
      Effect.gen(function* () {
        const service = yield* Config.Service
        yield* service.get()
      }),
    )
    expect(await Bun.file(path.join(directory.path, "outside", "tui", "session-list.json")).exists()).toBe(false)
  } finally {
    if (previous === undefined) delete process.env.OPENCODE_TUI_CHANNEL
    else process.env.OPENCODE_TUI_CHANNEL = previous
  }
})

test("preserves session-list properties and existing choices across repeated migration", async () => {
  for (const preference of [undefined, 3, "all", null]) {
    await using directory = await tmpdir()
    const file = path.join(directory.path, process.env.OPENCODE_TUI_CHANNEL ?? OPENCODE_CHANNEL, "tui/session-list.json")
    const initial = { allProjects: true, extra: { keep: true }, session_list_request_window_days: preference }
    await Bun.write(file, JSON.stringify(initial))
    await Bun.write(path.join(directory.path, "kv.json"), JSON.stringify({ session_list_request_window_days: 7 }))
    await run(directory.path, Effect.gen(function* () {
      const service = yield* Config.Service
      yield* service.get()
      yield* Effect.promise(() => Bun.write(path.join(directory.path, "kv.json"), JSON.stringify({ session_list_request_window_days: 60 })))
      yield* service.get()
    }))
    expect(await Bun.file(file).json()).toEqual({ ...initial, session_list_request_window_days: preference === undefined ? 7 : preference })
  }
})

test("does not create session-list storage without a legacy window", async () => {
  for (const legacy of [undefined, {}]) {
    await using directory = await tmpdir()
    if (legacy) await Bun.write(path.join(directory.path, "kv.json"), JSON.stringify(legacy))
    await run(directory.path, Effect.gen(function* () {
      const service = yield* Config.Service
      yield* service.get()
    }))
    expect(await Bun.file(path.join(directory.path, process.env.OPENCODE_TUI_CHANNEL ?? OPENCODE_CHANNEL, "tui/session-list.json")).exists()).toBe(false)
  }
})

test("creates session-list storage with the default cwd scope when cli config is missing", async () => {
  await using directory = await tmpdir()
  await Bun.write(path.join(directory.path, "kv.json"), JSON.stringify({ session_list_request_window_days: 7 }))
  await run(
    directory.path,
    Effect.gen(function* () {
      const service = yield* Config.Service
      yield* service.get()
    }),
  )
  const file = path.join(directory.path, process.env.OPENCODE_TUI_CHANNEL ?? OPENCODE_CHANNEL, "tui/session-list.json")
  expect(await Bun.file(file).json()).toEqual({ allProjects: false, session_list_request_window_days: 7 })
})

test("preserves a session-list choice written by the current lock owner", async () => {
  await using directory = await tmpdir()
  const channel = process.env.OPENCODE_TUI_CHANNEL ?? OPENCODE_CHANNEL
  const file = path.join(directory.path, channel, "tui/session-list.json")
  const legacy = path.join(directory.path, "kv.json")
  await Bun.write(path.join(directory.path, "cli.json"), "{}")
  await Bun.write(legacy, JSON.stringify({ session_list_request_window_days: 7 }))
  const reading = Promise.withResolvers<void>()
  const node = await Effect.runPromise(FileSystem.FileSystem.pipe(Effect.provide(NodeFileSystem.layer)))
  const fs = {
    ...node,
    readFileString: (...args: Parameters<typeof node.readFileString>) =>
      node.readFileString(...args).pipe(
        Effect.tap(() => Effect.sync(() => {
          if (args[0] === legacy) reading.resolve()
        })),
      ),
  }
  const held = await Flock.acquire(file, { dir: path.join(directory.path, channel, "locks") })
  const migration = Effect.runPromise(
    Effect.gen(function* () {
      const service = yield* Config.Service
      yield* service.get()
    }).pipe(
      Effect.provide(Config.layer),
      Effect.provide(Global.layerWith({ config: directory.path, state: directory.path })),
      Effect.provideService(FileSystem.FileSystem, fs),
    ),
  )
  try {
    expect(await Promise.race([reading.promise.then(() => "reading"), migration.then(() => "finished")])).toBe("reading")
    await Bun.write(file, JSON.stringify({ allProjects: true, session_list_request_window_days: 120, extra: true }))
  } finally {
    await held.release()
    await migration
  }
  expect(await Bun.file(file).json()).toEqual({ allProjects: true, session_list_request_window_days: 120, extra: true })
})

test("reads session-list inside its native lock before a waiting writer updates it", async () => {
  await using directory = await tmpdir()
  const channel = process.env.OPENCODE_TUI_CHANNEL ?? OPENCODE_CHANNEL
  const file = path.join(directory.path, channel, "tui/session-list.json")
  await Bun.write(path.join(directory.path, "cli.json"), "{}")
  await Bun.write(path.join(directory.path, "kv.json"), JSON.stringify({ session_list_request_window_days: 7 }))
  await Bun.write(file, JSON.stringify({ allProjects: true }))
  const reading = Promise.withResolvers<void>()
  const resume = Promise.withResolvers<void>()
  const waiting = Promise.withResolvers<void>()
  const node = await Effect.runPromise(FileSystem.FileSystem.pipe(Effect.provide(NodeFileSystem.layer)))
  const fs = { ...node, readFileString: (...args: Parameters<typeof node.readFileString>) =>
    args[0] !== file ? node.readFileString(...args) : Effect.gen(function* () {
      reading.resolve()
      yield* Effect.promise(() => resume.promise)
      return yield* node.readFileString(...args)
    }),
  }
  const migration = Effect.runPromise(Effect.gen(function* () {
    const service = yield* Config.Service
    yield* service.get()
  }).pipe(
    Effect.provide(Config.layer),
    Effect.provide(Global.layerWith({ config: directory.path, state: directory.path })),
    Effect.provideService(FileSystem.FileSystem, fs),
  ))
  try {
    expect(await Promise.race([reading.promise.then(() => "reading"), migration.then(() => "finished")])).toBe("reading")
    const writer = Flock.withLock(file, async () => {
      await Bun.write(file, JSON.stringify({ allProjects: false, session_list_request_window_days: "all", extra: true }))
    }, { dir: path.join(directory.path, channel, "locks"), onWait: () => { waiting.resolve() } })
    try {
      expect(await Promise.race([waiting.promise.then(() => "waiting"), writer.then(() => "finished")])).toBe("waiting")
    } finally {
      resume.resolve()
      await Promise.all([migration, writer])
    }
    expect(await Bun.file(file).json()).toEqual({ allProjects: false, session_list_request_window_days: "all", extra: true })
  } finally {
    resume.resolve()
    await migration
  }
})

test("generates reusable keybind schemas and preserves descriptions and numeric constraints", () => {
  const document = Schema.toJsonSchemaDocument(Config.Info)
  expect(document).toHaveProperty(["definitions", "TuiKeybind.BindingValue"])
  expect(document).toHaveProperty(["schema", "properties", "keybinds", "anyOf", 0, "properties", "app.exit"], {
    anyOf: [{ $ref: "#/$defs/TuiKeybind.BindingValue" }, { type: "null" }],
    description: "Exit the application",
  })
  expect(document).toHaveProperty(["schema", "properties", "scroll", "anyOf", 0, "properties", "speed", "anyOf", 0], {
    type: "number",
    minimum: 0.001,
  })
  expect(document).toHaveProperty(
    ["schema", "properties", "attention", "anyOf", 0, "properties", "volume", "anyOf", 0],
    { type: "number", minimum: 0, maximum: 1 },
  )
})

test("includes the published schema when creating cli.json", async () => {
  await using directory = await tmpdir()

  const config = await run(
    directory.path,
    Effect.gen(function* () {
      const service = yield* Config.Service
      return yield* service.update((draft) => {
        draft.animations = false
      })
    }),
  )

  expect(config).toEqual({ $schema: "https://opencode.ai/v2/cli.json", animations: false })
  expect(await Bun.file(path.join(directory.path, "cli.json")).json()).toEqual(config)
})

test("preserves the schema in an existing cli.json", async () => {
  await using directory = await tmpdir()
  const file = path.join(directory.path, "cli.json")
  await Bun.write(file, JSON.stringify({ $schema: "https://opencode.ai/v2/cli.json", animations: true }))

  const config = await run(
    directory.path,
    Effect.gen(function* () {
      const service = yield* Config.Service
      return yield* service.update((draft) => {
        draft.animations = false
      })
    }),
  )

  expect(config).toEqual({ $schema: "https://opencode.ai/v2/cli.json", animations: false })
  expect(await Bun.file(file).json()).toEqual(config)
})

test("merges inline CLI config content over the global config", async () => {
  await using directory = await tmpdir()
  const file = path.join(directory.path, "cli.json")
  const previous = process.env.OPENCODE_CLI_CONFIG_CONTENT
  await Bun.write(
    file,
    JSON.stringify({
      tabs: { mode: "on", scope: "global" },
      keybinds: { "app.exit": "ctrl+q" },
      plugins: ["global"],
      animations: true,
    }),
  )
  process.env.OPENCODE_CLI_CONFIG_CONTENT = JSON.stringify({
    tabs: { enabled: false },
    keybinds: { "help.show": false },
    plugins: ["inline"],
    animations: false,
  })

  try {
    const result = await run(
      directory.path,
      Effect.gen(function* () {
        const service = yield* Config.Service
        const loaded = yield* service.get()
        const updated = yield* service.update((draft) => {
          draft.animations = true
          draft.mouse = false
        })
        return { loaded, updated }
      }),
    )

    expect(result.loaded.tabs).toEqual({ mode: "off", scope: "global" })
    expect(result.loaded.keybinds).toEqual({ "app.exit": "ctrl+q", "help.show": false })
    expect(result.loaded.plugins).toEqual(["inline"])
    expect(result.updated).toMatchObject({ animations: false, mouse: false })
    expect(await Bun.file(file).json()).toMatchObject({ animations: true, mouse: false })
  } finally {
    if (previous === undefined) delete process.env.OPENCODE_CLI_CONFIG_CONTENT
    else process.env.OPENCODE_CLI_CONFIG_CONTENT = previous
  }
})

test("reads the legacy tabs toggle without rewriting it", async () => {
  await using directory = await tmpdir()
  const file = path.join(directory.path, "cli.json")
  await Bun.write(file, JSON.stringify({ tabs: { enabled: false } }))

  const config = await run(
    directory.path,
    Effect.gen(function* () {
      const service = yield* Config.Service
      expect((yield* service.get()).tabs).toEqual({ mode: "off" })
      return yield* service.update((draft) => {
        draft.animations = false
      })
    }),
  )

  expect(config.tabs).toEqual({ mode: "off" })
  expect(await Bun.file(file).json()).toEqual({ tabs: { enabled: false }, animations: false })
})

test("migrates tui and kv config into cli.json", async () => {
  await using directory = await tmpdir()
  await Bun.write(
    path.join(directory.path, "tui.json"),
    JSON.stringify({
      theme: "legacy",
      keybinds: {
        leader: "ctrl+o",
        app_exit: "ctrl+q",
        app_heap_snapshot: "ctrl+h",
        input_paste: { key: "ctrl+v", preventDefault: false },
        session_delete: false,
        "dialog.select.next": "ctrl+n",
      },
      leader_timeout: 500,
      scroll_speed: 2,
      scroll_acceleration: { enabled: true },
      diff_style: "stacked",
      mouse: false,
    }),
  )
  await Bun.write(
    path.join(directory.path, "kv.json"),
    JSON.stringify({
      theme_mode_lock: "light",
      attention_sound_pack: "custom.pack",
      diff_wrap_mode: "none",
      diff_viewer_show_file_tree: false,
      diff_viewer_single_patch: true,
      diff_viewer_view: "split",
      terminal_title_enabled: false,
      file_context_enabled: false,
      paste_summary_enabled: false,
      sidebar: "hide",
      scrollbar_visible: true,
      thinking_mode: "show",
      exploration_grouping: false,
      dismissed_getting_started: true,
      animations_enabled: false,
      skipped_version: "9.9.9",
      which_key_layout: "overlay",
    }),
  )

  const config = await run(
    directory.path,
    Effect.gen(function* () {
      const service = yield* Config.Service
      return yield* service.get()
    }),
  )

  expect(config).toMatchObject({
    $schema: "https://opencode.ai/v2/cli.json",
    theme: { name: "legacy", mode: "light" },
    keybinds: {
      leader: "ctrl+o",
      "app.exit": "ctrl+q",
      "prompt.paste": { key: "ctrl+v", preventDefault: false },
      "session.delete": false,
      "dialog.select.next": "ctrl+n",
    },
    leader: { timeout: 500 },
    scroll: { speed: 2, acceleration: true },
    attention: { sound_pack: "custom.pack" },
    diffs: { wrap: "none", tree: false, single: true, view: "split" },
    terminal: { title: false },
    prompt: { editor: false, paste: "full" },
    session: { sidebar: "hide", scrollbar: true, thinking: "show", grouping: "none" },
    animations: false,
    mouse: false,
  })
  expect(config).not.toHaveProperty("skipped_version")
  expect(config).not.toHaveProperty("which_key")
  expect(config).not.toHaveProperty("hints")
  expect((await Bun.file(path.join(directory.path, "cli.json")).json()).keybinds).toEqual({
    leader: "ctrl+o",
    "app.exit": "ctrl+q",
    "prompt.paste": { key: "ctrl+v", preventDefault: false },
    "session.delete": false,
    "dialog.select.next": "ctrl+n",
  })
  expect(await Bun.file(path.join(directory.path, "cli.json")).exists()).toBe(true)
  expect(await Bun.file(path.join(directory.path, "tui.json")).exists()).toBe(true)
  expect(await Bun.file(path.join(directory.path, "kv.json")).exists()).toBe(true)
})

test("migrates before the first update and does not remigrate afterward", async () => {
  await using directory = await tmpdir()
  await Bun.write(path.join(directory.path, "tui.json"), JSON.stringify({ theme: "legacy" }))

  const config = await run(
    directory.path,
    Effect.gen(function* () {
      const service = yield* Config.Service
      yield* service.update((draft) => {
        draft.animations = false
        draft.mouse = false
      })
      yield* Effect.promise(() =>
        Bun.write(path.join(directory.path, "tui.json"), JSON.stringify({ theme: "changed" })),
      )
      return yield* service.get()
    }),
  )

  expect(config).toEqual({
    $schema: "https://opencode.ai/v2/cli.json",
    theme: { name: "legacy" },
    animations: false,
    mouse: false,
  })
  expect(await Bun.file(path.join(directory.path, "cli.json")).json()).toEqual({
    $schema: "https://opencode.ai/v2/cli.json",
    theme: { name: "legacy" },
    animations: false,
    mouse: false,
  })
})

test("preserves legacy cursor settings", async () => {
  await using directory = await tmpdir()
  await Bun.write(
    path.join(directory.path, "tui.json"),
    JSON.stringify({ cursor: { style: "underline", blinking: false } }),
  )

  const config = await run(
    directory.path,
    Effect.gen(function* () {
      const service = yield* Config.Service
      return yield* service.get()
    }),
  )

  expect(config.cursor).toEqual({ style: "underline", blinking: false })
  expect((await Bun.file(path.join(directory.path, "cli.json")).json()).cursor).toEqual({
    style: "underline",
    blinking: false,
  })
})

test("migrates legacy keybind names in an existing cli.json", async () => {
  await using directory = await tmpdir()
  const file = path.join(directory.path, "cli.json")
  await Bun.write(
    file,
    `{
  // Preserve this comment
  "keybinds": {
    // Session list shortcut
    "session_list": "ctrl+l",
    "app_heap_snapshot": "ctrl+h",
    // Legacy delete shortcut
    "session_delete": "ctrl+d",
    // Canonical delete shortcut
    "session.delete": "ctrl+x",
    "app.heap_snapshot": "ctrl+shift+h"
  }
}
`,
  )

  const config = await run(
    directory.path,
    Effect.gen(function* () {
      const service = yield* Config.Service
      return yield* service.get()
    }),
  )

  expect(config.keybinds).toEqual({
    "session.list": "ctrl+l",
    "session.delete": "ctrl+x",
  })
  const text = await Bun.file(file).text()
  expect(text).toContain("// Preserve this comment")
  expect(text).toContain("// Session list shortcut")
  expect(text).toContain("// Legacy delete shortcut")
  expect(text).toContain("// Canonical delete shortcut")
  expect(parse(text).keybinds).toEqual({
    "session.list": "ctrl+l",
    "session.delete": "ctrl+x",
  })
})

test("migrates copy_on_select in an existing cli.json", async () => {
  for (const item of [
    { legacy: true, expected: "select" },
    { legacy: false, expected: "manual" },
    { legacy: true, copy: "manual", expected: "manual" },
  ] as const) {
    await using directory = await tmpdir()
    const file = path.join(directory.path, "cli.json")
    await Bun.write(
      file,
      `{
  // Preserve this comment
  "terminal": {
    ${item.copy === undefined ? "" : `"copy": ${JSON.stringify(item.copy)},`}
    "copy_on_select": ${item.legacy}
  }
}
`,
    )

    const config = await run(
      directory.path,
      Effect.gen(function* () {
        const service = yield* Config.Service
        return yield* service.get()
      }),
    )

    expect(config.terminal).toEqual({ copy: item.expected })
    const text = await Bun.file(file).text()
    expect(text).toContain("// Preserve this comment")
    expect(text).not.toContain("copy_on_select")
    expect(parse(text).terminal).toEqual({ copy: item.expected })
  }
})

test("uses migrated keybinds when persistence fails", async () => {
  await using directory = await tmpdir()
  const file = path.join(directory.path, "cli.json")
  await Bun.write(file, `{"keybinds":{"session_list":"ctrl+l"}}`)
  const node = await Effect.runPromise(FileSystem.FileSystem.pipe(Effect.provide(NodeFileSystem.layer)))
  const fs = new Proxy(node, {
    get(target, property, receiver) {
      if (property === "rename") return () => Effect.die(new Error("read-only config"))
      // oxlint-disable-next-line no-restricted-globals -- Proxy forwarding requires receiver-aware property access.
      return Reflect.get(target, property, receiver)
    },
  })

  const config = await Effect.runPromise(
    Effect.gen(function* () {
      const service = yield* Config.Service
      return yield* service.get()
    }).pipe(
      Effect.provide(Config.layer),
      Effect.provide(Global.layerWith({ config: directory.path, state: directory.path })),
      Effect.provideService(FileSystem.FileSystem, fs),
    ),
  )

  expect(config.keybinds).toEqual({ "session.list": "ctrl+l" })
  expect(await Bun.file(file).json()).toEqual({ keybinds: { session_list: "ctrl+l" } })
  expect(await Array.fromAsync(new Bun.Glob("*.tmp").scan(directory.path))).toEqual([])
})

test("preserves the effective value when migrating duplicate legacy keybinds", async () => {
  await using directory = await tmpdir()
  const file = path.join(directory.path, "cli.json")
  await Bun.write(file, `{"keybinds":{"session_delete":"ctrl+a","session_delete":"ctrl+b"}}`)

  const config = await run(
    directory.path,
    Effect.gen(function* () {
      const service = yield* Config.Service
      return yield* service.get()
    }),
  )

  expect(config.keybinds).toEqual({ "session.delete": "ctrl+b" })
  expect(parse(await Bun.file(file).text()).keybinds).toEqual({ "session.delete": "ctrl+b" })
})

test("migrates and updates the effective duplicate top-level keybinds", async () => {
  await using directory = await tmpdir()
  const file = path.join(directory.path, "cli.json")
  await Bun.write(file, `{"keybinds":{"session_delete":"first"},"keybinds":{"session_delete":"last"}}`)

  const config = await run(
    directory.path,
    Effect.gen(function* () {
      const service = yield* Config.Service
      expect((yield* service.get()).keybinds).toEqual({ "session.delete": "last" })
      return yield* service.update((draft) => {
        draft.keybinds = { ...draft.keybinds, "session.delete": "changed" }
      })
    }),
  )

  expect(config.keybinds).toEqual({ "session.delete": "changed" })
  expect(parse(await Bun.file(file).text()).keybinds).toEqual({ "session.delete": "changed" })
})

test("serializes migration and updates across processes", async () => {
  await using directory = await tmpdir()
  const file = path.join(directory.path, "cli.json")
  const started = path.join(directory.path, "started")
  const release = path.join(directory.path, "release")
  const migrateReady = path.join(directory.path, "migrate-ready")
  const updateReady = path.join(directory.path, "update-ready")
  await Bun.write(file, `{"keybinds":{"session_delete":"ctrl+d"}}`)
  const worker = path.join(import.meta.dir, "fixture/config-concurrency.ts")
  const migrate = Bun.spawn([process.execPath, worker, "migrate", directory.path, started, release, migrateReady], {
    stdout: "ignore",
    stderr: "pipe",
  })

  try {
    await waitForFile(started, migrate.exited)
    const update = Bun.spawn([process.execPath, worker, "update", directory.path, started, release, updateReady], {
      stdout: "ignore",
      stderr: "pipe",
    })
    try {
      await waitForFile(updateReady, update.exited)
      await Bun.write(release, "")
      const [migrateCode, updateCode] = await Promise.all([migrate.exited, update.exited])
      expect(await new Response(migrate.stderr).text()).toBe("")
      expect(await new Response(update.stderr).text()).toBe("")
      expect([migrateCode, updateCode]).toEqual([0, 0])
      expect(await Bun.file(file).json()).toEqual({ keybinds: { "session.delete": "ctrl+d" }, mouse: false })
    } finally {
      update.kill()
      await update.exited
    }
  } finally {
    await Bun.write(release, "")
    migrate.kill()
    await migrate.exited
  }
})

test("config reads remain interruptible while waiting for the file lock", async () => {
  await using directory = await tmpdir()
  const file = path.join(directory.path, "cli.json")
  const locks = path.join(directory.path, "locks")
  const held = await Flock.acquire(file, { dir: locks })

  try {
    const service = await Effect.runPromise(
      Config.Service.pipe(
        Effect.provide(Config.layer),
        Effect.provide(Global.layerWith({ config: directory.path, state: directory.path })),
        Effect.provide(NodeFileSystem.layer),
      ),
    )
    const result = Effect.runPromise(service.get().pipe(Effect.timeoutOption("50 millis")))
    expect(await Promise.race([result, Bun.sleep(250).then(() => "blocked" as const)])).toEqual(Option.none())
  } finally {
    await held.release()
  }
})

test("updates effective duplicate canonical keybinds", async () => {
  await using directory = await tmpdir()
  const file = path.join(directory.path, "cli.json")
  await Bun.write(
    file,
    `{"keybinds":{"session.delete":"first","session.delete":"last","opencode.settings":"off","opencode.settings":"on"}}`,
  )

  const config = await run(
    directory.path,
    Effect.gen(function* () {
      const service = yield* Config.Service
      expect((yield* service.get()).keybinds).toEqual({ "session.delete": "last", "opencode.settings": "on" })
      return yield* service.update((draft) => {
        draft.keybinds = { ...draft.keybinds, "session.delete": "changed", "opencode.settings": "changed" }
      })
    }),
  )

  expect(config.keybinds).toEqual({ "session.delete": "changed", "opencode.settings": "changed" })
  expect(parse(await Bun.file(file).text()).keybinds).toEqual({
    "session.delete": "changed",
    "opencode.settings": "changed",
  })
})

test("removes orphaned keybinds without deleting trailing comments", async () => {
  await using directory = await tmpdir()
  const file = path.join(directory.path, "cli.json")
  await Bun.write(
    file,
    `{
  "keybinds": {
    "app_heap_snapshot": "ctrl+h" /* Keep legacy explanation */,
    "app.heap_snapshot": "ctrl+shift+h" /* Keep canonical explanation */,
  },
}
`,
  )

  const config = await run(
    directory.path,
    Effect.gen(function* () {
      const service = yield* Config.Service
      return yield* service.get()
    }),
  )

  expect(config.keybinds).toEqual({})
  const text = await Bun.file(file).text()
  expect(text).toContain("/* Keep legacy explanation */")
  expect(text).toContain("/* Keep canonical explanation */")
  expect(parse(text).keybinds).toEqual({})
})

test("updates a config draft while preserving JSONC comments", async () => {
  await using directory = await tmpdir()
  await Bun.write(path.join(directory.path, "cli.json"), '{\n  // Keep this comment\n  "animations": true\n}\n')

  const config = await run(
    directory.path,
    Effect.gen(function* () {
      const service = yield* Config.Service
      return yield* service.update((draft) => {
        draft.prompt = { paste: "compact" }
        draft.mini = {
          thinking: "hide",
          shell_output: "hide",
          turn_summary: "hide",
          splash: "hide",
          work_spinner: "block-low-comet",
          mono: true,
        }
      })
    }),
  )

  expect(config).toEqual({
    animations: true,
    prompt: { paste: "compact" },
    mini: {
      thinking: "hide",
      shell_output: "hide",
      turn_summary: "hide",
      splash: "hide",
      work_spinner: "block-low-comet",
      mono: true,
    },
  })
  expect(await Bun.file(path.join(directory.path, "cli.json")).text()).toContain("// Keep this comment")
})

async function waitForFile(file: string, exited: Promise<number>) {
  const found = await Promise.race([
    (async () => {
      while (!(await Bun.file(file).exists())) await Bun.sleep(10)
      return true
    })(),
    exited.then(() => false),
    Bun.sleep(5000).then(() => false),
  ])
  if (!found) throw new Error(`timed out waiting for ${file}`)
}

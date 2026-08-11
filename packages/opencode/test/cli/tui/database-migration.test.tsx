import { describe, expect, test } from "bun:test"
import type {
  TuiDialogConfirmProps,
  TuiDialogPromptProps,
  TuiKeymap,
  TuiPluginApi,
  TuiPluginMeta,
  TuiToast,
} from "@opencode-ai/plugin/tui"
import { createDatabaseMigrationPlugin, type DatabaseMigrationOperations } from "@/plugin/tui/database-migration"
import { internalTuiPlugins } from "@/plugin/tui/internal"
import { createTuiPluginApi } from "../../fixture/tui-plugin"

type Command = NonNullable<Parameters<TuiKeymap["registerLayer"]>[0]["commands"]>[number]
type Action = { readonly title: string; readonly onSelect?: () => void }

const meta: TuiPluginMeta = {
  id: "internal:database-migration",
  source: "internal",
  spec: "internal:database-migration",
  target: "internal:database-migration",
  state: "same",
  first_time: 0,
  last_time: 0,
  time_changed: 0,
  load_count: 1,
  fingerprint: "internal:database-migration",
}

function required<Value>(value: Value | undefined, message: string): Value {
  if (value === undefined) throw new Error(message)
  return value
}

async function settle() {
  await Promise.resolve()
  await Promise.resolve()
  await Promise.resolve()
}

async function setup(operations: DatabaseMigrationOperations) {
  const commands = new Map<string, Command>()
  const toasts: TuiToast[] = []
  let render: (() => unknown) | undefined
  let actions: Action[] = []
  let selectTitle = ""
  let prompt: TuiDialogPromptProps | undefined
  let confirm: TuiDialogConfirmProps | undefined
  const base = createTuiPluginApi()
  base.keymap.registerLayer = (layer) => {
    layer.commands?.forEach((command) => commands.set(command.name, command))
    return () => {}
  }
  const api: TuiPluginApi = {
    ...base,
    ui: {
      ...base.ui,
      DialogSelect(props) {
        selectTitle = props.title
        actions = props.options.map((option) => ({ title: option.title, onSelect: option.onSelect }))
        return null
      },
      DialogPrompt(props) {
        prompt = props
        return null
      },
      DialogConfirm(props) {
        confirm = props
        return null
      },
      toast(input) {
        toasts.push(input)
      },
      dialog: {
        replace(next) {
          render = next
        },
        clear() {
          render = undefined
        },
        setSize() {},
        get size() {
          return "medium" as const
        },
        get depth() {
          return render ? 1 : 0
        },
        get open() {
          return render !== undefined
        },
      },
    },
  }
  await createDatabaseMigrationPlugin(operations).tui(api, undefined, meta)
  return {
    commands,
    toasts,
    get actions() {
      return actions
    },
    get selectTitle() {
      return selectTitle
    },
    get prompt() {
      return prompt
    },
    get confirm() {
      return confirm
    },
    render() {
      required(render, "expected dialog render")()
    },
  }
}

function operations(input?: Partial<DatabaseMigrationOperations>) {
  return {
    activeDatabase: () => "/data/opencode.db",
    exportDatabase: async () => {},
    stageImport: async () => ({ marker: "/data/opencode.db.replacement.json", staged: "/data/staged.db" }),
    ...input,
  } satisfies DatabaseMigrationOperations
}

function openAction(harness: Awaited<ReturnType<typeof setup>>, title: string) {
  Reflect.apply(required(harness.commands.get("database.migration"), "expected migration command").run, undefined, [])
  harness.render()
  required(
    harness.actions.find((action) => action.title === title),
    `expected ${title} action`,
  ).onSelect?.()
  harness.render()
}

describe("database migration TUI plugin", () => {
  test("is local-only and registers one palette command with two actions", async () => {
    const flags = { experimentalEventSystem: false }
    expect(internalTuiPlugins(flags).some((plugin) => plugin.id === "internal:database-migration")).toBe(false)
    expect(
      internalTuiPlugins(flags, { localDatabase: false }).some((plugin) => plugin.id === "internal:database-migration"),
    ).toBe(false)
    expect(
      internalTuiPlugins(flags, { localDatabase: true }).filter(
        (plugin) => plugin.id === "internal:database-migration",
      ),
    ).toHaveLength(1)

    const harness = await setup(operations())
    const command = required(harness.commands.get("database.migration"), "expected migration command")
    expect({
      name: command.name,
      title: command.title,
      category: command.category,
      namespace: command.namespace,
    }).toEqual({
      name: "database.migration",
      title: "Database migration",
      category: "System",
      namespace: "palette",
    })
    expect(harness.commands).toHaveLength(1)

    Reflect.apply(command.run, undefined, [])
    harness.render()
    expect(harness.selectTitle).toBe("Database migration")
    expect(harness.actions.map((action) => action.title)).toEqual(["Export DB", "Import DB"])
  })

  test("cancelling export and import invokes no core operation", async () => {
    let calls = 0
    const harness = await setup(
      operations({
        exportDatabase: async () => {
          calls += 1
        },
        stageImport: async () => {
          calls += 1
          return { marker: "marker", staged: "staged" }
        },
      }),
    )

    openAction(harness, "Export DB")
    required(harness.prompt, "expected export prompt").onConfirm?.("   ")
    required(harness.prompt, "expected export prompt").onCancel?.()
    openAction(harness, "Import DB")
    required(harness.prompt, "expected import prompt").onConfirm?.("   ")
    required(harness.prompt, "expected import prompt").onConfirm?.("/backup/legacy.db")
    harness.render()
    required(harness.confirm, "expected import confirmation").onCancel?.()

    expect(calls).toBe(0)
  })

  test("exports the active database and reports both artifacts", async () => {
    const exports: Array<{ readonly source: string; readonly destination: string }> = []
    const harness = await setup(operations({ exportDatabase: async (input) => void exports.push(input) }))

    openAction(harness, "Export DB")
    expect(harness.prompt?.value).toBe("/data/opencode.db.legacy-v01.db")
    required(harness.prompt, "expected export prompt").onConfirm?.("  /backup/export.db  ")
    required(harness.prompt, "expected export prompt").onConfirm?.("/backup/duplicate.db")
    await settle()

    expect(exports).toEqual([{ source: "/data/opencode.db", destination: "/backup/export.db" }])
    expect(harness.toasts.at(-1)).toEqual({
      variant: "success",
      message: "Exported /backup/export.db and /backup/export.db.manifest.json",
    })
  })

  test("returns to the action dialog when export fails", async () => {
    const harness = await setup(operations({ exportDatabase: async () => Promise.reject(new Error("export failed")) }))

    openAction(harness, "Export DB")
    required(harness.prompt, "expected export prompt").onConfirm?.("/backup/export.db")
    await settle()
    harness.render()

    expect(harness.toasts.at(-1)?.variant).toBe("error")
    expect(harness.toasts.at(-1)?.message).toContain("export failed")
    expect(harness.selectTitle).toBe("Database migration")
  })

  test("stages import after confirmation and reports restart required", async () => {
    const imports: Array<{ readonly source: string; readonly target: string }> = []
    const harness = await setup(
      operations({ stageImport: async (input) => (imports.push(input), operations().stageImport(input)) }),
    )

    openAction(harness, "Import DB")
    required(harness.prompt, "expected import prompt").onConfirm?.("  /backup/legacy.db  ")
    harness.render()
    expect(harness.confirm?.message).toContain("next restart")
    required(harness.confirm, "expected import confirmation").onConfirm?.()
    await settle()

    expect(imports).toEqual([{ source: "/backup/legacy.db", target: "/data/opencode.db" }])
    expect(harness.toasts.at(-1)?.message).toContain("/data/staged.db")
    expect(harness.toasts.at(-1)?.message).toContain("/data/opencode.db.replacement.json")
    expect(harness.toasts.at(-1)?.message).toContain("Restart required")
  })

  test("returns to the action dialog when import fails", async () => {
    const harness = await setup(operations({ stageImport: async () => Promise.reject(new Error("import failed")) }))

    openAction(harness, "Import DB")
    required(harness.prompt, "expected import prompt").onConfirm?.("/backup/legacy.db")
    harness.render()
    required(harness.confirm, "expected import confirmation").onConfirm?.()
    await settle()
    harness.render()

    expect(harness.toasts.at(-1)?.variant).toBe("error")
    expect(harness.toasts.at(-1)?.message).toContain("import failed")
    expect(harness.selectTitle).toBe("Database migration")
  })
})

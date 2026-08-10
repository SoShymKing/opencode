import { Database } from "@opencode-ai/core/database/database"
import { DatabaseLegacyV01 } from "@opencode-ai/core/database/legacy-v01"
import type { TuiPluginApi } from "@opencode-ai/plugin/tui"
import type { BuiltinTuiPlugin } from "@opencode-ai/tui/builtins"
import { errorMessage } from "@opencode-ai/tui/util/error"
import { Effect } from "effect"
import { createSignal } from "solid-js"

export type DatabaseMigrationOperations = {
  readonly activeDatabase: () => string
  readonly exportDatabase: (input: { readonly source: string; readonly destination: string }) => Promise<void>
  readonly stageImport: (input: {
    readonly source: string
    readonly target: string
  }) => Promise<{ readonly marker: string; readonly staged: string }>
}

const productionOperations: DatabaseMigrationOperations = {
  activeDatabase: Database.path,
  exportDatabase: (input) => Effect.runPromise(DatabaseLegacyV01.exportDatabase(input)),
  stageImport: (input) => Effect.runPromise(DatabaseLegacyV01.stageImport(input)),
}

function showActions(api: TuiPluginApi, operations: DatabaseMigrationOperations) {
  api.ui.dialog.replace(() => (
    <api.ui.DialogSelect
      title="Database migration"
      flat
      options={[
        {
          title: "Export DB",
          value: "export",
          onSelect: () => showExport(api, operations),
        },
        {
          title: "Import DB",
          value: "import",
          onSelect: () => showImport(api, operations),
        },
      ]}
    />
  ))
}

function showExport(api: TuiPluginApi, operations: DatabaseMigrationOperations) {
  api.ui.dialog.replace(() => <ExportDialog api={api} operations={operations} />)
}

function ExportDialog(props: { readonly api: TuiPluginApi; readonly operations: DatabaseMigrationOperations }) {
  const [busy, setBusy] = createSignal(false)
  const source = props.operations.activeDatabase()
  return (
    <props.api.ui.DialogPrompt
      title="Export DB"
      value={`${source}.legacy-v01.db`}
      placeholder="Destination .db path"
      busy={busy()}
      busyText="Exporting database..."
      onCancel={() => props.api.ui.dialog.clear()}
      onConfirm={(raw) => {
        if (busy()) return
        const destination = raw.trim()
        if (!destination) {
          props.api.ui.toast({ variant: "error", message: "Destination path is required" })
          return
        }
        setBusy(true)
        void props.operations
          .exportDatabase({ source, destination })
          .then(
            () => {
              props.api.ui.toast({
                variant: "success",
                message: `Exported ${destination} and ${destination}.manifest.json`,
              })
              props.api.ui.dialog.clear()
            },
            (error) => {
              props.api.ui.toast({ variant: "error", message: errorMessage(error) })
              showActions(props.api, props.operations)
            },
          )
          .finally(() => setBusy(false))
      }}
    />
  )
}

function showImport(api: TuiPluginApi, operations: DatabaseMigrationOperations) {
  api.ui.dialog.replace(() => (
    <api.ui.DialogPrompt
      title="Import DB"
      placeholder="Legacy .db path"
      onCancel={() => api.ui.dialog.clear()}
      onConfirm={(raw) => {
        const source = raw.trim()
        if (!source) {
          api.ui.toast({ variant: "error", message: "Legacy database path is required" })
          return
        }
        showImportConfirmation(api, operations, source)
      }}
    />
  ))
}

function showImportConfirmation(api: TuiPluginApi, operations: DatabaseMigrationOperations, source: string) {
  const target = operations.activeDatabase()
  api.ui.dialog.replace(() => (
    <api.ui.DialogConfirm
      title="Import DB"
      message={`The source DB (${source}) and current DB (${target}) stay untouched. A replacement is staged and activates on the next restart. Close every other OpenCode process before restarting; pre-lease or nonparticipating processes cannot be detected. Process-crash recovery is supported; sudden Windows power loss is not guaranteed.`}
      onCancel={() => api.ui.dialog.clear()}
      onConfirm={() => {
        void operations.stageImport({ source, target }).then(
          (result) => {
            api.ui.toast({
              variant: "success",
              message: `Staged ${result.staged}; marker ${result.marker}. Restart required to activate.`,
            })
            api.ui.dialog.clear()
          },
          (error) => {
            api.ui.toast({ variant: "error", message: errorMessage(error) })
            showActions(api, operations)
          },
        )
      }}
    />
  ))
}

export function createDatabaseMigrationPlugin(
  operations: DatabaseMigrationOperations = productionOperations,
): BuiltinTuiPlugin {
  return {
    id: "internal:database-migration",
    tui: async (api) => {
      api.keymap.registerLayer({
        commands: [
          {
            name: "database.migration",
            title: "Database migration",
            category: "System",
            namespace: "palette",
            run() {
              showActions(api, operations)
            },
          },
        ],
      })
    },
  }
}

export default createDatabaseMigrationPlugin()

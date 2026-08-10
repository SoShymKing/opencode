import { createBuiltinPlugins, type BuiltinTuiPlugin } from "@opencode-ai/tui/builtins"
import type { RuntimeFlags } from "@/effect/runtime-flags"
import DatabaseMigration from "./database-migration"

export type InternalTuiPlugin = BuiltinTuiPlugin

export function internalTuiPlugins(
  flags: Pick<RuntimeFlags.Info, "experimentalEventSystem">,
  options: { readonly localDatabase: boolean } = { localDatabase: false },
): InternalTuiPlugin[] {
  const plugins = createBuiltinPlugins({
    experimentalEventSystem: flags.experimentalEventSystem,
  })
  return options.localDatabase ? [...plugins, DatabaseMigration] : plugins
}

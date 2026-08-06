import type { DatabaseMigration } from "./migration"

export const migrations = (
  await Promise.all([import("./migration/v01_baseline"), import("./migration/v02_session_message_part")])
).map((module) => module.default) satisfies DatabaseMigration.Migration[]

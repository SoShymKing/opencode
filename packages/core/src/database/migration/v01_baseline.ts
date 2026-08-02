import { Effect } from "effect"
import type { DatabaseMigration } from "../migration"

export default {
  id: "v01_baseline",
  up: () => Effect.void,
} satisfies DatabaseMigration.Migration

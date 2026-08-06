import { Database } from "@opencode-ai/core/database/database"
import { Effect, Layer } from "effect"

const filename = process.argv[2]
if (!filename) throw new Error("Database filename is required")

await Effect.runPromise(Effect.scoped(Layer.build(Database.layerFromPath(filename))))

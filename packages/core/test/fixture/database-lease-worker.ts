import { Database } from "@opencode-ai/core/database/database"
import { DatabaseLease } from "@opencode-ai/core/database/lease"
import { Effect } from "effect"

const mode = process.argv[2]
const target = process.argv[3]
if (!target || (mode !== "shared" && mode !== "exclusive" && mode !== "database")) process.exit(2)

const wait = Effect.gen(function* () {
  console.log("READY")
  yield* Effect.promise(() => Bun.stdin.text())
})

const hold =
  mode === "database"
    ? Effect.gen(function* () {
        yield* Database.Service
        yield* wait
      }).pipe(Effect.provide(Database.layerFromPath(target)))
    : Effect.gen(function* () {
        yield* mode === "shared" ? DatabaseLease.shared(target) : DatabaseLease.exclusive(target)
        yield* wait
      })

await Effect.runPromise(hold.pipe(Effect.scoped))

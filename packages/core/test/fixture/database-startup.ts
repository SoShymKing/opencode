import path from "node:path"
import { WsNative } from "@opencode/core/database/ws-native"
import { Effect } from "effect"
import { useDatabase } from "./database-migration"
import { setupWs } from "./database-ws"

export function createStartupDatabase(filename: string, native = false) {
  return useDatabase(filename, (db) => Effect.gen(function* () {
    yield* setupWs(db, { directory: path.dirname(filename), lineage: "v02" })
    yield* db.run("UPDATE session SET title = 'Session'")
    if (native) yield* WsNative.convert(db)
    yield* db.run("PRAGMA wal_checkpoint(TRUNCATE)")
  }))
}

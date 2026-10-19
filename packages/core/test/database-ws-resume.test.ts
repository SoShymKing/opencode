import { expect, test } from "bun:test"
import { readdir } from "node:fs/promises"
import { join } from "node:path"
import { sql } from "drizzle-orm"
import { Effect } from "effect"
import type { SqlClient } from "effect/sql"
import type { Global } from "@opencode/util/global"
import { DatabaseArtifact } from "../src/database/artifact"
import { EffectDrizzleSqlite } from "../src/database/drizzle"
import { sqliteLayer } from "../src/database/sqlite.bun"
import { WsNative } from "../src/database/ws-native"
import { WsNativeFile } from "../src/database/ws-native-file"
import { messageID, richContent, sessionID, setupWs } from "./fixture/database-ws"
import { tempGlobalLayer } from "./fixture/global"
import { tmpdir } from "./fixture/tmpdir"

test("file startup reuses committed WS work when legacy native encoding fails again", async () => {
  await using tmp = await tmpdir()
  const target = join(tmp.path, "source.db")
  await useDatabase(target, (db) => Effect.gen(function* () {
    yield* setupWs(db, { directory: tmp.path, rich: true })
    yield* db.run("UPDATE message SET time_created = 'synthetic-not-a-clock' WHERE id = 'msg_legacy'")
    yield* WsNative.validateSource(db)
  }))
  const before = await DatabaseArtifact.digest(target).pipe(Effect.runPromise)
  const admit = () => WsNativeFile.admit(target).pipe(Effect.provide(tempGlobalLayer), Effect.scoped, Effect.runPromise)
  const stages = async () => (await readdir(tmp.path)).filter((name) => name.startsWith("source.db.native-") && name.endsWith(".db")).sort()

  await expect(admit()).rejects.toThrow("native-encode")
  expect(await DatabaseArtifact.digest(target).pipe(Effect.runPromise)).toEqual(before)
  const retained = await stages()
  expect(retained).toHaveLength(1)
  const staged = join(tmp.path, retained[0] ?? "")
  const first = await prefix(staged)

  await expect(admit()).rejects.toThrow("native-encode")
  expect(await DatabaseArtifact.digest(target).pipe(Effect.runPromise)).toEqual(before)
  const second = await prefix(staged)
  const converted = {
    session: { id: sessionID },
    message: { id: messageID, type: "assistant", seq: 7, inline_content: null },
    parts: { count: richContent.length },
    text: [{ position: 0, text: "first" }, { position: 1, text: "second" }],
  }
  expect({ stages: await stages(), first, second }).toEqual({ stages: retained, first: converted, second: converted })
})

function prefix(filename: string) {
  return useDatabase(filename, (db) => Effect.gen(function* () {
    if (!(yield* db.get("SELECT 1 FROM sqlite_schema WHERE type = 'table' AND name = 'session_v2'"))) return null
    return {
      session: yield* db.get(sql`SELECT id FROM session_v2 WHERE id = ${sessionID}`),
      message: yield* db.get(sql`SELECT id, type, seq, json_type(data, '$.content') AS inline_content FROM session_message WHERE id = ${messageID}`),
      parts: yield* db.get(sql`SELECT count(*) AS count FROM session_message_part WHERE message_id = ${messageID}`),
      text: yield* db.all(sql`SELECT position, json_extract(data, '$.text') AS text FROM session_message_part WHERE message_id = ${messageID} AND type = 'text' ORDER BY position`),
    }
  }))
}

function useDatabase<A, E>(filename: string, use: (db: EffectDrizzleSqlite.EffectSQLiteDatabase) => Effect.Effect<A, E, SqlClient.SqlClient | Global.Service>) {
  return Effect.gen(function* () {
    const db = yield* EffectDrizzleSqlite.makeWithDefaults()
    return yield* use(db)
  }).pipe(Effect.provide(sqliteLayer({ filename })), Effect.provide(tempGlobalLayer), Effect.scoped, Effect.runPromise)
}

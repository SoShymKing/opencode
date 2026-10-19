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
import { WsNativeProgress } from "../src/database/ws-native-progress"
import { Resources } from "../src/database/ws-native-serial"
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

test("legacy checkpoint resume accounts for intentional omissions before filtering committed rows", () =>
  useDatabase(":memory:", (db) => Effect.gen(function* () {
    yield* setupWs(db, { directory: "/fixture", rich: true })
    yield* db.run("UPDATE part SET data = '{\"type\":\"compaction\",\"auto\":false}' WHERE message_id = 'msg_legacy'")
    yield* db.run(`WITH RECURSIVE n(value) AS (SELECT 1 UNION ALL SELECT value + 1 FROM n WHERE value < 128)
      INSERT INTO message SELECT printf('msg_row_%03d', value), session_id, value, value, data FROM message, n WHERE id = 'msg_legacy'`)
    yield* db.run("INSERT INTO message SELECT 'msg_z_bad_clock', session_id, 'synthetic-not-a-clock', time_updated, data FROM message WHERE id = 'msg_legacy'")
    yield* WsNative.prepareStage(db, "legacy-resume-token")

    const first = yield* WsNative.resumeStage(db, "legacy-resume-token").pipe(Effect.result)
    expect(first._tag).toBe("Failure")
    if (first._tag !== "Failure") throw new Error("Expected invalid clock")
    expect(String(first.failure)).toContain("Invalid native-encode stage=schema-decode field=time")
    const checkpoint = yield* WsNativeProgress.verify(db, "legacy-resume-token")
    expect(checkpoint).toMatchObject({ phase: "legacy", session_id: "ses_legacy", seq: 127, message_id: "msg_row_128" })
    const committed = yield* db.all("SELECT * FROM session_message WHERE session_id = 'ses_legacy' ORDER BY seq")
    expect(committed).toHaveLength(128)
    expect(yield* db.get("SELECT id FROM session_message WHERE id = 'msg_legacy'")).toBeUndefined()

    const second = yield* WsNative.resumeStage(db, "legacy-resume-token").pipe(Effect.result)
    expect(second._tag).toBe("Failure")
    if (second._tag !== "Failure") throw new Error("Expected invalid clock on retry")
    expect(String(second.failure)).toContain("Invalid native-encode stage=schema-decode field=time")
    expect(yield* db.all("SELECT * FROM session_message WHERE session_id = 'ses_legacy' ORDER BY seq")).toEqual(committed)
    expect(yield* WsNativeProgress.verify(db, "legacy-resume-token")).toEqual(checkpoint)
  }).pipe(Effect.provideService(Resources, () => ({ cpu: 1, freeBytes: 1024 ** 3 })))))

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

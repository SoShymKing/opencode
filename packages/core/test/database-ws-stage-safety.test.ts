import { expect, test } from "bun:test"
import { sql } from "drizzle-orm"
import { Effect } from "effect"
import { EffectDrizzleSqlite } from "../src/database/drizzle"
import { sqliteLayer } from "../src/database/sqlite.bun"
import { DatabaseWsArchive } from "../src/database/ws-archive"
import { WsNative } from "../src/database/ws-native"
import type { WsNativeProgress } from "../src/database/ws-native-progress"
import { messageID, setupWs } from "./fixture/database-ws"
import { tempGlobalLayer } from "./fixture/global"
import { tmpdir } from "./fixture/tmpdir"

test("rejects a finish checkpoint that skips all archived WS messages", async () => {
  await using temp = await tmpdir()
  await Effect.runPromise(Effect.gen(function* () {
    const db = yield* EffectDrizzleSqlite.makeWithDefaults()
    yield* setupWs(db, { directory: temp.path, rich: false })
    const original = yield* db.all(sql`SELECT * FROM session_message WHERE id = ${messageID}`)
    expect(original).toHaveLength(1)
    const checkpoint = yield* WsNative.prepareStage(db, "synthetic-phase-token")
    const archive = yield* DatabaseWsArchive.read(db, checkpoint.archive_id)
    const source = archive && DatabaseWsArchive.sourceTable(archive, "session_message")
    if (!source) throw new Error("Missing archived fixture messages")
    expect(yield* db.all(sql`SELECT * FROM ${DatabaseWsArchive.identifier(source)}`)).toEqual(original)
    expect(yield* db.all("SELECT * FROM session_message")).toEqual([])
    const progress = yield* db.get<Omit<WsNativeProgress.Checkpoint, "prefix">>("SELECT * FROM _ws_stage")

    yield* db.run("UPDATE _ws_stage SET phase = 'finish'")
    const result = yield* WsNative.resumeStage(db, "synthetic-phase-token").pipe(Effect.exit)

    expect({
      result: result._tag,
      ledger: yield* db.all("SELECT name FROM sqlite_schema WHERE type = 'table' AND name = 'migration'"),
      progress: yield* db.get("SELECT * FROM _ws_stage"),
      messages: yield* db.all("SELECT * FROM session_message"),
      parts: yield* db.all("SELECT * FROM session_message_part"),
      source: yield* db.all(sql`SELECT * FROM ${DatabaseWsArchive.identifier(source)}`),
    }).toEqual({
      result: "Failure",
      ledger: [],
      progress: { ...progress, phase: "finish" },
      messages: [],
      parts: [],
      source: original,
    })
  }).pipe(Effect.provide(sqliteLayer({ filename: ":memory:" })), Effect.provide(tempGlobalLayer), Effect.scoped))
})

import { expect, test } from "bun:test"
import { join } from "node:path"
import { Effect } from "effect"
import { DatabaseArtifact } from "../src/database/artifact"
import { DatabaseReplacement } from "../src/database/replacement"
import { EffectDrizzleSqlite } from "../src/database/drizzle"
import { sqliteLayer } from "../src/database/sqlite.bun"
import { WsNativeStage } from "../src/database/ws-native-stage"
import { setupWs } from "./fixture/database-ws"
import { tempGlobalLayer } from "./fixture/global"
import { tmpdir } from "./fixture/tmpdir"

test("standalone activation rejects a private stage older than the committed preimage", async () => {
  await using temp = await tmpdir()
  const target = join(temp.path, "source.db")
  await Effect.runPromise(Effect.gen(function* () {
    const db = yield* EffectDrizzleSqlite.makeWithDefaults()
    yield* setupWs(db, { directory: temp.path })
  }).pipe(Effect.provide(sqliteLayer({ filename: target })), Effect.scoped))
  const ready = await WsNativeStage.prepare(target, target).pipe(Effect.provide(tempGlobalLayer), Effect.scoped, Effect.runPromise)
  await WsNativeStage.handoff(target).pipe(Effect.runPromise)
  await Effect.runPromise(Effect.gen(function* () {
    const db = yield* EffectDrizzleSqlite.makeWithDefaults()
    yield* db.run("PRAGMA journal_mode = WAL")
    yield* db.run("UPDATE session SET title = 'new committed source'")
    yield* db.run("PRAGMA wal_checkpoint(TRUNCATE)")
    yield* db.run("PRAGMA journal_mode = DELETE")
  }).pipe(Effect.provide(sqliteLayer({ filename: target })), Effect.scoped))
  const before = await DatabaseArtifact.digest(target).pipe(Effect.runPromise)
  await DatabaseArtifact.write(`${target}.replacement.json`, JSON.stringify({
    format: "opencode.database.replacement", version: 2, stageFormat: "native", phase: "prepared", staged: ready, targetPreimage: before,
  })).pipe(Effect.runPromise)
  await expect(DatabaseReplacement.activate(target).pipe(Effect.provide(tempGlobalLayer), Effect.scoped, Effect.runPromise)).rejects.toThrow("replacement target differs from its backup")
  expect(await DatabaseArtifact.digest(target).pipe(Effect.runPromise)).toEqual(before)
  await Effect.runPromise(Effect.gen(function* () {
    const db = yield* EffectDrizzleSqlite.makeWithDefaults()
    expect(yield* db.get("SELECT title FROM session")).toEqual({ title: "new committed source" })
  }).pipe(Effect.provide(sqliteLayer({ filename: target })), Effect.scoped))
  expect(await Bun.file(`${target}.replacement.json`).exists()).toBe(true)
})

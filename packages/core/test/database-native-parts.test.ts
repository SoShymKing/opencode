import { expect, test } from "bun:test"
import { Effect } from "effect"
import { sql } from "drizzle-orm"
import { EffectDrizzleSqlite } from "../src/database/drizzle.js"
import { sqliteLayer } from "../src/database/sqlite.bun.js"
import migration from "../src/database/migration/20261016000000_session_message_part.js"
import { Global } from "@opencode/util/global"
import { DatabaseMigration } from "../src/database/migration.js"
import { migrations } from "../src/database/migration.gen.js"

test("native forward migration preserves mixed order and generates per-kind IDs", async () => {
  await Effect.runPromise(Effect.gen(function* () {
    const db = yield* EffectDrizzleSqlite.makeWithDefaults()
    yield* db.run("PRAGMA foreign_keys = ON")
    yield* db.run("CREATE TABLE session_v2 (id TEXT PRIMARY KEY)")
    yield* db.run("CREATE TABLE session_message (id TEXT PRIMARY KEY, session_id TEXT REFERENCES session_v2(id) ON DELETE CASCADE, seq INTEGER, type TEXT, data TEXT)")
    yield* db.run("INSERT INTO session_v2 VALUES ('ses_native')")
    const data = JSON.stringify({ agent: "build", model: { providerID: "test", id: "model" }, time: { created: 1 }, content: [
      { type: "text", text: "first" },
      { type: "reasoning", text: "reason" },
      { type: "tool", id: "call_tool", name: "read", state: { status: "streaming", input: "{}" }, time: { created: 1 } },
      { type: "text", text: "last" },
    ] })
    yield* db.run(sql`INSERT INTO session_message VALUES ('msg_native', 'ses_native', 0, 'assistant', ${data})`)
    yield* db.run("CREATE TABLE migration (id TEXT PRIMARY KEY, time_completed INTEGER NOT NULL)")
    for (const previous of migrations.filter((item) => item.id !== migration.id))
      yield* db.run(sql`INSERT INTO migration VALUES (${previous.id}, 1)`)
    yield* DatabaseMigration.applyOnly(db, [migration])
    yield* DatabaseMigration.applyOnly(db, [migration])
    expect(yield* db.all(sql`SELECT position, id, type FROM session_message_part ORDER BY position`)).toEqual([
      { position: 0, id: "0", type: "text" },
      { position: 1, id: "0", type: "reasoning" },
      { position: 2, id: "call_tool", type: "tool" },
      { position: 3, id: "1", type: "text" },
    ])
    expect(yield* db.get(sql`SELECT json_type(data, '$.content') AS content FROM session_message`)).toEqual({ content: null })
    expect(yield* db.all(sql`SELECT 1 FROM session_message_part WHERE json_type(data, '$.id') IS NOT NULL OR json_type(data, '$.type') IS NOT NULL`)).toEqual([])
    yield* db.run("DELETE FROM session_v2")
    expect(yield* db.all(sql`SELECT * FROM session_message_part`)).toEqual([])
  }).pipe(
    Effect.provideService(Global.Service, Global.make({ data: process.env.OPENCODE_TEST_HOME })),
    Effect.provide(sqliteLayer({ filename: ":memory:" })), Effect.scoped,
  ))
})

test("invalid native content rolls back both child DDL and parent edits", async () => {
  await Effect.runPromise(Effect.gen(function* () {
    const db = yield* EffectDrizzleSqlite.makeWithDefaults()
    yield* db.run("CREATE TABLE session_message (id TEXT PRIMARY KEY, session_id TEXT, seq INTEGER, type TEXT, data TEXT)")
    yield* db.run("INSERT INTO session_message VALUES ('msg_bad', 'ses_test', 0, 'assistant', '{}')")
    const result = yield* Effect.exit(db.transaction((tx) => migration.up(tx)))
    expect(result._tag).toBe("Failure")
    expect(yield* db.all(sql`SELECT name FROM sqlite_master WHERE name = 'session_message_part'`)).toEqual([])
    expect(yield* db.get(sql`SELECT data FROM session_message`)).toEqual({ data: "{}" })
  }).pipe(
    Effect.provideService(Global.Service, Global.make({ data: process.env.OPENCODE_TEST_HOME })),
    Effect.provide(sqliteLayer({ filename: ":memory:" })), Effect.scoped,
  ))
})

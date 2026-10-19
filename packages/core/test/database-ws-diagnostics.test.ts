import { expect, test } from "bun:test"
import { SqliteClient } from "@effect/sql-sqlite-bun"
import { Effect } from "effect"
import { sql } from "drizzle-orm"
import { openWorker } from "@opencode/core/database/ws-native-bun"
import { EffectDrizzleSqlite } from "@opencode/core/database/drizzle"
import { WsNative } from "@opencode/core/database/ws-native"
import { DatabaseWsArchive } from "@opencode/core/database/ws-archive"
import { setupWs } from "./fixture/database-ws"
import { tempGlobalLayer } from "./fixture/global"

test("native-encode reports only fixed stage and root field through the real worker", () => Effect.runPromise(Effect.gen(function* () {
  const worker = yield* openWorker()
  for (const marker of ["synthetic-secret-one", "synthetic-secret-two"]) {
    for (const field of ["id", "time", "content"] as const) {
      const id = field === "id" ? marker : `msg_${marker}`
      const row = field === "content"
        ? { id, type: "assistant", data: { agent: "build", model: { id: "model", providerID: "test" }, time: { created: 1 },
          content: [{ type: "tool", id: marker, name: "read", time: { created: 1 }, state: {
            status: "completed", input: {}, content: [], metadata: { [marker]: undefined },
          } }] } }
        : { id, type: "user", data: { text: marker, time: { created: field === "time" ? marker : 1 } } }
      const result = yield* worker.run([{ kind: "native-encode", row }]).pipe(Effect.result)
      expect(result._tag).toBe("Failure")
      if (result._tag !== "Failure") throw new Error("Expected diagnostic failure")
      expect(result.failure.messageID).toBe(id)
      expect(result.failure.message).toBe(`Invalid native-encode stage=schema-decode field=${field}`)
      expect(result.failure.message).not.toContain(marker)
      expect(result.failure).not.toHaveProperty("cause")
      expect(result.failure).not.toHaveProperty("issue")
    }
  }
  const valid = yield* worker.run([{ kind: "native-encode", row: { id: "msg_control", type: "user", data: { text: "control", time: { created: 1 } } } }])
  expect(valid[0]?.kind).toBe("native-encode")
}).pipe(Effect.scoped)))

for (const invalid of [false, true]) test(`legacy SQL clock diagnostic preserves source, invalid=${invalid}`, () => Effect.runPromise(Effect.gen(function* () {
  const db = yield* EffectDrizzleSqlite.makeWithDefaults()
  yield* setupWs(db, { directory: "/fixture", rich: true })
  const original = yield* db.get("SELECT data FROM message WHERE id = 'msg_legacy'")
  if (invalid) yield* db.run("UPDATE message SET time_created = 'synthetic-not-a-clock' WHERE id = 'msg_legacy'")
  expect(yield* db.get("SELECT data FROM message WHERE id = 'msg_legacy'")).toEqual(original)
  yield* WsNative.validateSource(db)
  const ddl = yield* db.all("SELECT * FROM sqlite_schema")
  const tables = yield* db.all<{ name: string }>("SELECT name FROM sqlite_schema WHERE type = 'table'")
  const before = new Map<string, unknown[]>()
  for (const table of tables) before.set(table.name, yield* db.all(sql`SELECT * FROM ${DatabaseWsArchive.identifier(table.name)}`))
  const result = yield* WsNative.convert(db).pipe(Effect.result)
  expect(result._tag).toBe(invalid ? "Failure" : "Success")
  if (result._tag === "Failure") {
    expect(result.failure.message).toBe("WsNativeMessageError: Invalid native-encode stage=schema-decode field=time")
    expect(yield* db.all("SELECT * FROM sqlite_schema")).toEqual(ddl)
    for (const [name, rows] of before) expect(yield* db.all(sql`SELECT * FROM ${DatabaseWsArchive.identifier(name)}`)).toEqual(rows)
  }
  if (!invalid) yield* WsNative.validate(db)
}).pipe(Effect.provide(SqliteClient.layer({ filename: ":memory:", disableWAL: true })), Effect.provide(tempGlobalLayer), Effect.scoped)))

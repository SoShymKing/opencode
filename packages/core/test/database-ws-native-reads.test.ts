import { expect, test } from "bun:test"
import { SqliteClient } from "@effect/sql-sqlite-bun"
import { Effect } from "effect"
import { sql } from "drizzle-orm"
import { EffectDrizzleSqlite } from "@opencode/core/database/drizzle"
import { WsNativeScan } from "@opencode/core/database/ws-native-scan"
import { Resources } from "@opencode/core/database/ws-native-serial"
import { WsNativeMessage } from "@opencode/core/database/ws-native-message"

for (const failure of [undefined, "payload", "-7", "9007199254740993", "9223372036854775807"]) test(`native child reads batch admitted IDs and preserve raw rows (${failure ?? "valid"})`, () => Effect.runPromise(Effect.gen(function* () {
  const db = yield* EffectDrizzleSqlite.makeWithDefaults()
  yield* db.run("CREATE TABLE session_message (id TEXT PRIMARY KEY, session_id TEXT, type TEXT, data TEXT)")
  yield* db.run("CREATE TABLE session_message_part (message_id TEXT, position INTEGER, id TEXT, type TEXT, data TEXT)")
  for (let index = 0; index < 260; index++) {
    const id = `msg_${String(index).padStart(3, "0")}`
    yield* db.run(sql`INSERT INTO session_message VALUES (${id}, 'ses_migration', 'assistant', '{"agent":"build","model":{"providerID":"test","id":"model"},"time":{"created":1}}')`)
    for (const position of ["0", "7", "7", "42", "100", "1000"])
      yield* db.run(sql`INSERT INTO session_message_part VALUES (${id}, ${sql.raw(position)}, ${`raw-${position}`}, 'text', '{ "text":"raw\\u0020text" }')`)
  }
  for (let index = 0; index < 140; index++)
    yield* db.run(sql`INSERT INTO session_message_part VALUES ('msg_259', 7, ${`extra-${index}`}, 'text', '{"text":"extra"}')`)
  if (failure) yield* db.run(sql`INSERT INTO session_message_part VALUES ('msg_259', ${sql.raw(failure === "payload" ? "1001" : failure)}, 'bad-late-child', 'text', ${failure === "payload" ? '{}' : '{"text":"valid"}'})`)
  const snapshot = sql`SELECT message_id, CAST(position AS TEXT) AS position, id, type, data FROM session_message_part ORDER BY rowid`
  const before = yield* db.all(snapshot)
  const pages: number[] = []
  const sizes: number[] = []
  const reader: Pick<EffectDrizzleSqlite.EffectSQLiteDatabase, "transaction"> = {
    transaction: (accept, config) => db.transaction((tx) => {
      const all = tx.all.bind(tx)
      tx.all = <T>(query: Parameters<typeof tx.all>[0]) => {
        const result = all<T>(query)
        const compiled = result.getQuery()
        const child = compiled.sql.includes("SELECT message_id AS messageID")
        if (child) pages.push(compiled.params.length)
        const execute = result.execute
        result.execute = () => execute().pipe(Effect.tap((rows) => Effect.sync(() => {
          if (child) sizes.push(rows.length)
          else expect(rows.length).toBeLessThanOrEqual(128)
        })))
        return result
      }
      return accept(tx)
    }, config),
  }
  const result = yield* WsNativeScan.validateNative(reader).pipe(Effect.result)
  expect(result._tag).toBe(failure ? "Failure" : "Success")
  if (result._tag === "Failure") {
    expect(result.failure).toBeInstanceOf(WsNativeMessage.WsNativeMessageError)
    if (result.failure instanceof WsNativeMessage.WsNativeMessageError) expect(result.failure.messageID).toBe("msg_259")
  }
  expect(yield* db.all(snapshot)).toEqual(before)
  expect(pages).toEqual([128, 128, 4])
  expect(sizes).toEqual([768, 768, 164 + Number(Boolean(failure))])
}).pipe(Effect.provideService(Resources, () => ({ cpu: 1, freeBytes: 1024 ** 3 })),
  Effect.provide(SqliteClient.layer({ filename: ":memory:", disableWAL: true })), Effect.scoped)))

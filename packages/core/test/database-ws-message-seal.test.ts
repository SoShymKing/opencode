import { expect, test } from "bun:test"
import { createHash } from "node:crypto"
import { SqliteClient } from "@effect/sql-sqlite-bun"
import { sql } from "drizzle-orm"
import { Effect } from "effect"
import { EffectDrizzleSqlite } from "../src/database/drizzle"
import { WsNativeScan } from "../src/database/ws-native-scan"
import { WsNativeSeal } from "../src/database/ws-native-seal"

for (const fixture of [
  { parts: 0, payload: 0 },
  { parts: 9, payload: 0 },
  { parts: 35, payload: 0 },
  { parts: 9, payload: 600_000 },
  { parts: 35, payload: 20_000 },
]) test(`message seal preserves canonical bytes with ${fixture.parts} parts and ${fixture.payload} payload bytes`, () =>
  Effect.runPromise(Effect.gen(function* () {
    const db = yield* EffectDrizzleSqlite.makeWithDefaults()
    yield* db.run("CREATE TABLE session_message (id TEXT PRIMARY KEY, session_id TEXT, type TEXT, seq INTEGER, time_created INTEGER, time_updated INTEGER, data TEXT)")
    yield* db.run("CREATE TABLE session_message_part (message_id TEXT, position INTEGER, id TEXT, type TEXT, data TEXT, PRIMARY KEY (message_id, position))")
    yield* db.run(sql`INSERT INTO session_message VALUES ('message', 'session', 'assistant', 9223372036854775807, 1.25, NULL, ${'{ "number":1.00e+2,"text":"\uD55C\uAE00\uD83D\uDE00" }'})`)
    for (let index = fixture.parts - 1; index >= 0; index--) {
      yield* db.run(sql`INSERT INTO session_message_part VALUES ('message', ${index * 3}, ${`part-${index}`}, 'text',
        ${fixture.payload && (fixture.payload < 100_000 || index === 4)
          ? sql`zeroblob(${fixture.payload})`
          : index % 3 === 0 ? sql`X'00FF80'`
          : sql`${'{ "number":-0.00e+0,"text":"\u00E9\uD55C\uD83D\uDE00" }\0tail'}`})`)
    }
    yield* db.run("INSERT INTO session_message_part VALUES ('other', 0, 'unrelated', 'text', 'excluded')")

    const reference = createHash("sha256").update("previous-seal")
    for (const selection of [
      { table: "session_message", fields: ["id", "session_id", "type", "seq", "time_created", "time_updated", "data"], where: sql`id = 'message'`, order: sql`id` },
      { table: "session_message_part", fields: ["message_id", "position", "id", "type", "data"], where: sql`message_id = 'message'`, order: sql`position` },
    ]) {
      const canonical = sql`json_array(${sql.join(selection.fields.flatMap((name) => [
        sql`typeof(${sql.identifier(name)})`,
        sql`CASE WHEN typeof(${sql.identifier(name)}) IN ('text', 'blob') THEN hex(${sql.identifier(name)}) ELSE quote(${sql.identifier(name)}) END`,
      ]), sql`, `)})`
      const count = selection.table === "session_message" ? 1 : fixture.parts
      for (let ordinal = 0; ordinal < count; ordinal++) {
        for (let offset = 1; ; offset += WsNativeScan.byteLimit) {
          const part = yield* db.get<{ value: string }>(sql`SELECT substr(${canonical}, ${offset}, ${WsNativeScan.byteLimit}) AS value
            FROM ${sql.identifier(selection.table)} WHERE ${selection.where} ORDER BY ${selection.order} LIMIT 1 OFFSET ${ordinal}`)
          if (!part) throw new Error("Missing reference row")
          reference.update(part.value)
          if (part.value.length < WsNativeScan.byteLimit) break
        }
      }
    }

    const queries: string[] = []
    const values: unknown[] = []
    const get = db.get.bind(db)
    db.get = <T>(query: Parameters<typeof db.get>[0]) => {
      const result = get<T>(query)
      queries.push(result.getQuery().sql)
      const execute = result.execute
      result.execute = () => execute().pipe(Effect.tap((value) => Effect.sync(() => { values.push(value) })))
      return result
    }
    const all = db.all.bind(db)
    db.all = <T>(query: Parameters<typeof db.all>[0]) => {
      const result = all<T>(query)
      queries.push(result.getQuery().sql)
      const execute = result.execute
      result.execute = () => execute().pipe(Effect.tap((rows) => Effect.sync(() => { values.push(...rows) })))
      return result
    }

    const digest = yield* WsNativeSeal.message(db, "message", "previous-seal")

    expect(digest).toBe(reference.digest("hex"))
    for (const value of values) {
      if (typeof value !== "object" || value === null) continue
      for (const scalar of Object.values(value)) {
        if (typeof scalar === "string") expect(Buffer.byteLength(scalar)).toBeLessThanOrEqual(WsNativeScan.byteLimit)
      }
    }
    expect(queries).toHaveLength(fixture.payload === 600_000 ? 17 : fixture.payload ? 6 : 2)
    if (fixture.payload) expect(values[1]).toEqual({ value: null })
    if (!fixture.parts) expect(values[1]).toEqual({ value: "" })
    if (fixture.payload === 600_000) expect(queries.filter((query) => query.includes("hex(substr(CAST(s.\"data\" AS BLOB)"))).toHaveLength(3)
    if (fixture.payload) expect(queries.some((query) => query.includes("AS bytes"))).toBe(true)
  }).pipe(Effect.provide(SqliteClient.layer({ filename: ":memory:", disableWAL: true })), Effect.scoped)))

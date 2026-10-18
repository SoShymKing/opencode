import { expect, test } from "bun:test"
import SQLite from "bun:sqlite"
import { join } from "node:path"
import { Context, Effect, Layer } from "effect"
import { Reactivity } from "effect/reactivity"
import { SqlClient, Statement } from "effect/sql"
import type { SqlError } from "effect/sql/SqlError"
import { Database } from "@opencode/core/database/database"
import { migrations } from "@opencode/core/database/migration.gen"
import { sqliteLayer } from "../src/database/sqlite.bun"
import { Global } from "@opencode/util/global"
import { tmpdir } from "./fixture/tmpdir"

test("bootstrap retries a busy WAL promotion without rerunning migrations", async () => {
  await using tmp = await tmpdir()
  const filename = join(tmp.path, "startup.sqlite")
  using holder = new SQLite(filename)
  holder.run("CREATE TABLE _fixture (id INTEGER)")
  const failures: string[] = []
  const queries: string[] = []
  const client = Layer.effect(SqlClient.SqlClient, Effect.gen(function* () {
    const client = yield* SqlClient.SqlClient
    const connection = yield* client.reserve
    const execute = <A>(query: string, effect: Effect.Effect<A, SqlError>) => Effect.gen(function* () {
      queries.push(query)
      if (query === "PRAGMA journal_mode = WAL" && failures.length === 0) holder.run("BEGIN IMMEDIATE")
      return yield* effect.pipe(Effect.tapError((error) => Effect.sync(() => {
        if (query !== "PRAGMA journal_mode = WAL") return
        expect(error.reason.cause).toMatchObject({ code: "SQLITE_BUSY", errno: 5 })
        failures.push(error.reason._tag)
        holder.run("ROLLBACK")
      })))
    })
    return yield* SqlClient.make({
      acquirer: Effect.succeed({
        ...connection,
        execute: (query, params, transform) => execute(query, connection.execute(query, params, transform)),
        executeRaw: (query, params) => execute(query, connection.executeRaw(query, params)),
      }),
      compiler: Statement.makeCompilerSqlite(),
      spanAttributes: [],
    })
  })).pipe(Layer.provide(sqliteLayer({ filename })), Layer.provide(Reactivity.layer))

  await Effect.runPromise(Effect.gen(function* () {
    const services = yield* Layer.build(Database.layerFromClient.pipe(Layer.provide(client)))
    const database = Context.get(services, Database.Service)
    expect(failures).toEqual(["LockTimeoutError"])
    expect(queries.filter((query) => query === "PRAGMA journal_mode = WAL")).toHaveLength(2)
    expect(queries.filter((query) => query === "begin immediate")).toHaveLength(1)
    expect(yield* database.db.get("PRAGMA journal_mode")).toEqual({ journal_mode: "wal" })
    expect(yield* database.db.get("SELECT count(*) AS count FROM migration")).toEqual({ count: migrations.length })
  }).pipe(Effect.provideService(Global.Service, Global.make({ data: tmp.path })), Effect.scoped))
})

export * as WsNativeProfile from "./ws-native-profile.js"

import { sql } from "drizzle-orm"
import { Effect } from "effect"
import { mkdtemp, rm } from "node:fs/promises"
import { tmpdir } from "node:os"
import path from "node:path"
import { sqliteLayer } from "./sqlite.bun.js"
import { EffectDrizzleSqlite } from "./drizzle.js"
import { DatabaseArtifact } from "./artifact.js"
import { DatabaseLineage } from "./lineage.js"
import { DatabaseMigration } from "./migration.js"
import { DatabaseWsArchive } from "./ws-archive.js"
import { DatabaseLease } from "./lease.js"

type Database = EffectDrizzleSqlite.EffectSQLiteDatabase
const identifier = DatabaseWsArchive.identifier

export function run(target: Database, filename: string | undefined) {
  return Effect.scoped(Effect.gen(function* () {
    if (!filename || !(yield* DatabaseArtifact.exists(filename))) return false
    yield* DatabaseArtifact.requireRegularFile(filename)
    const directory = yield* Effect.acquireRelease(
      Effect.promise(() => mkdtemp(path.join(tmpdir(), "opencode-profile-"))),
      (directory) => Effect.promise(() => rm(directory, { recursive: true, force: true })),
    )
    const staged = path.join(directory, "snapshot.db")
    const recognized = yield* DatabaseLease.shared(filename).pipe(
      Effect.andThen(
        Effect.gen(function* () {
          const source = yield* EffectDrizzleSqlite.makeWithDefaults()
          const state = yield* DatabaseLineage.inspect(source, [...DatabaseMigration.currentMigrationIDs])
          if (!state.ws && !state.previousNative) {
            if (state.tables.has("session") && state.tables.has("session_message"))
              return yield* Effect.fail(new DatabaseLineage.UnsafeMigrationHistoryError([...state.completed], "unrecognized profile schema"))
            return false
          }
          yield* DatabaseArtifact.snapshot(source, staged)
          return true
        }).pipe(Effect.provide(sqliteLayer({ filename, readonly: true, readwrite: false, create: false }))),
      ),
      Effect.scoped,
    )
    if (!recognized) return false
    const digest = yield* DatabaseArtifact.digest(staged)
    const key = `migration.ws-profile.${digest.sha256}`
    if (yield* target.get(sql`SELECT key FROM kv WHERE key = ${key}`)) return true
    const { WsNative } = yield* Effect.promise(() => import("./ws-native.js"))
    yield* Effect.gen(function* () {
      const source = yield* EffectDrizzleSqlite.makeWithDefaults()
      yield* WsNative.validateSource(source)
      yield* WsNative.convert(source)
      yield* WsNative.validate(source)
    }).pipe(Effect.provide(sqliteLayer({ filename: staged })), Effect.scoped)
    yield* merge(target, staged, key)
    return true
  }))
}

export function merge(target: Database, staged: string, key: string) {
  return Effect.scoped(Effect.gen(function* () {
    yield* Effect.acquireRelease(
      target.run(sql`ATTACH DATABASE ${staged} AS ws_profile`),
      () => target.run(sql`DETACH DATABASE ws_profile`).pipe(Effect.orDie),
    )
    yield* target.transaction((tx) => Effect.gen(function* () {
      if (yield* tx.get(sql`SELECT key FROM kv WHERE key = ${key}`)) return
      yield* tx.run(sql`PRAGMA defer_foreign_keys = ON`)
      const objects = yield* tx.all<{ type: string; name: string; tbl_name: string; sql: string | null }>(
        sql`SELECT type, name, tbl_name, sql FROM ws_profile.sqlite_schema ORDER BY type, name`,
      )
      const tables = objects.filter((object) => object.type === "table" && !object.name.startsWith("sqlite_"))
      for (const table of tables) {
        if (!table.name.startsWith("_ws_")) continue
        if (yield* tx.get(sql`SELECT name FROM sqlite_schema WHERE name = ${table.name}`)) continue
        if (table.sql) yield* tx.run(sql.raw(table.sql))
      }
      for (const object of objects) {
        if (!object.sql || !object.tbl_name.startsWith("_ws_") || object.type !== "index") continue
        if (yield* tx.get(sql`SELECT name FROM sqlite_schema WHERE name = ${object.name}`)) continue
        yield* tx.run(sql.raw(object.sql))
      }
      for (const table of tables) {
        if (table.name === "migration") continue
        const columns = yield* tx.all<{ name: string; hidden: number }>(sql`SELECT name, hidden FROM pragma_table_xinfo(${table.name}, 'ws_profile')`)
        const layout = table.name.startsWith("_ws_source_")
          ? yield* tx.get<{ wr: number }>(sql`SELECT wr FROM pragma_table_list WHERE schema = 'ws_profile' AND name = ${table.name}`)
          : undefined
        const rowid = layout?.wr === 0
          ? ["rowid", "_rowid_", "oid"].find((name) => !columns.some((column) => column.name.toLowerCase() === name))
          : undefined
        const names = sql.join([
          ...(rowid ? [identifier(rowid)] : []),
          ...columns.filter((column) => column.hidden === 0).map((column) => identifier(column.name)),
        ], sql`, `)
        const filter = table.name === "account_state"
          ? sql`WHERE NOT EXISTS (SELECT 1 FROM main.account_state)`
          : table.name === "kv" ? sql`WHERE key NOT LIKE 'migration.%'` : sql``
        yield* tx.run(sql`INSERT INTO main.${identifier(table.name)} (${names}) SELECT ${names} FROM ws_profile.${identifier(table.name)} ${filter}`)
      }
      for (const object of objects) {
        if (!object.sql || !object.tbl_name.startsWith("_ws_")) continue
        if (object.type !== "trigger") continue
        if (yield* tx.get(sql`SELECT name FROM sqlite_schema WHERE name = ${object.name}`)) continue
        yield* tx.run(sql.raw(object.sql))
      }
      const violations = yield* tx.all(sql`PRAGMA foreign_key_check`)
      if (violations.length) return yield* Effect.fail(new DatabaseWsArchive.ArchiveError("Imported profile has invalid references"))
      yield* tx.run(sql`INSERT INTO kv (key, value, time_created, time_updated) VALUES (${key}, '{"completed":true}', ${Date.now()}, ${Date.now()})`)
    }), { behavior: "immediate" })
  }))
}

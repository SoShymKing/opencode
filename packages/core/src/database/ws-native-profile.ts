export * as WsNativeProfile from "./ws-native-profile.js"

import { sql } from "drizzle-orm"
import { Effect } from "effect"
import { withPool } from "#ws-native-worker"
import { mkdtemp, rm } from "node:fs/promises"
import { tmpdir } from "node:os"
import { createHash } from "node:crypto"
import path from "node:path"
import { sqliteLayer } from "./sqlite.bun.js"
import { EffectDrizzleSqlite } from "./drizzle.js"
import { DatabaseArtifact } from "./artifact.js"
import { DatabaseLineage } from "./lineage.js"
import { DatabaseMigration } from "./migration.js"
import { DatabaseWsArchive } from "./ws-archive.js"
import { DatabaseLease } from "./lease.js"
import { WsNativeStage } from "./ws-native-stage.js"

type Database = EffectDrizzleSqlite.EffectSQLiteDatabase
const identifier = DatabaseWsArchive.identifier

export function run(target: Database, filename: string | undefined, begin: Effect.Effect<void> = Effect.void) {
  return Effect.scoped(Effect.gen(function* () {
    if (!filename || !(yield* DatabaseArtifact.exists(filename))) return false
    yield* DatabaseArtifact.requireRegularFile(filename)
    yield* DatabaseLease.shared(filename)
    const cached = yield* WsNativeStage.cleanRead(filename, (identity) => target.get(sql`SELECT key FROM kv WHERE key LIKE 'migration.ws-profile.%'
      AND json_valid(value) AND json_extract(value, '$.completed') = 1 AND json_extract(value, '$.sourceIdentity') = ${identity.fingerprint} LIMIT 1`))
    const sourceIdentity = cached?.identity.fingerprint
    const main = yield* target.get<{ file: string }>("SELECT file FROM pragma_database_list WHERE name = 'main'")
    const owner = main?.file ? path.join(path.dirname(main.file), `profile-${createHash("sha256").update(`${main.file}\0${path.resolve(filename)}`).digest("hex").slice(0, 16)}`) : undefined
    if (cached?.value) {
      if (owner && (yield* DatabaseArtifact.exists(`${owner}.ws-stage.json`))) {
        yield* DatabaseLease.exclusive(owner)
        yield* WsNativeStage.retireProfile(target, owner)
      }
      return true
    }
    if (owner && sourceIdentity) {
      const recognized = yield* Effect.gen(function* () {
        const source = yield* EffectDrizzleSqlite.makeWithDefaults()
        return yield* recognize(source)
      }).pipe(Effect.provide(sqliteLayer({ filename, readonly: true, readwrite: false, create: false })), Effect.scoped)
      if (!recognized) return false
      yield* DatabaseLease.exclusive(owner)
      yield* begin
      const proof = yield* WsNativeStage.capture(filename, owner)
      const same = yield* WsNativeStage.cleanRead(filename, (identity) => Effect.succeed(identity.fingerprint === sourceIdentity))
      if (!same?.value || proof.before.fingerprint !== sourceIdentity)
        return yield* Effect.fail(new DatabaseWsArchive.ArchiveError("Profile source changed during preparation"))
      const key = `migration.ws-profile.${proof.marker.snapshot.sha256}`
      if (yield* target.get(sql`SELECT key FROM kv WHERE key = ${key}`)) {
        yield* target.transaction((tx) => tx.run(sql`UPDATE kv SET value = ${JSON.stringify({ completed: true, sourceIdentity })} WHERE key = ${key}`), { behavior: "immediate" })
        yield* WsNativeStage.retireProfile(target, owner)
        return true
      }
      const ready = yield* WsNativeStage.prepare(filename, owner, proof)
      yield* merge(target, path.join(path.dirname(owner), ready.name), key, sourceIdentity)
      yield* WsNativeStage.retireProfile(target, owner)
      return true
    }
    const directory = yield* Effect.acquireRelease(
      Effect.promise(() => mkdtemp(path.join(tmpdir(), "opencode-profile-"))),
      (directory) => Effect.promise(() => rm(directory, { recursive: true, force: true })),
    )
    const staged = path.join(directory, "snapshot.db")
    const recognized = yield* Effect.gen(function* () {
      const source = yield* EffectDrizzleSqlite.makeWithDefaults()
      if (!(yield* recognize(source))) return false
      yield* begin
      yield* DatabaseArtifact.snapshot(source, staged)
      return true
    }).pipe(Effect.provide(sqliteLayer({ filename, readonly: true, readwrite: false, create: false })), Effect.scoped)
    if (!recognized) return false
    if (sourceIdentity) {
      const same = yield* WsNativeStage.cleanRead(filename, (identity) => Effect.succeed(identity.fingerprint === sourceIdentity))
      if (!same?.value) return yield* Effect.fail(new DatabaseWsArchive.ArchiveError("Profile source changed during snapshot"))
    }
    const digest = yield* DatabaseArtifact.digest(staged)
    const key = `migration.ws-profile.${digest.sha256}`
    if (yield* target.get(sql`SELECT key FROM kv WHERE key = ${key}`)) {
      yield* target.transaction((tx) => tx.run(sql`UPDATE kv SET value = ${JSON.stringify({ completed: true, sourceIdentity })} WHERE key = ${key}`), { behavior: "immediate" })
      if (owner) {
        yield* DatabaseLease.exclusive(owner)
        yield* WsNativeStage.retireProfile(target, owner)
      }
      return true
    }
    if (owner) {
      yield* DatabaseLease.exclusive(owner)
      const proof = yield* WsNativeStage.capture(filename, owner)
      if (proof.marker.snapshot.sha256 !== digest.sha256)
        return yield* Effect.fail(new DatabaseWsArchive.ArchiveError("Profile source changed during preparation"))
      const ready = yield* WsNativeStage.prepare(filename, owner, proof)
      yield* merge(target, path.join(path.dirname(owner), ready.name), key)
      yield* WsNativeStage.retireProfile(target, owner)
      return true
    }
    const { WsNative } = yield* Effect.promise(() => import("./ws-native.js"))
    yield* Effect.gen(function* () {
      const source = yield* EffectDrizzleSqlite.makeWithDefaults()
      yield* WsNative.convert(source)
    }).pipe(Effect.provide(sqliteLayer({ filename: staged })), Effect.scoped)
    yield* merge(target, staged, key, sourceIdentity)
    return true
  })).pipe(withPool)
}

function recognize(source: Database) {
  return Effect.gen(function* () {
    const state = yield* DatabaseLineage.inspect(source, [...DatabaseMigration.currentMigrationIDs])
    if (state.ws || state.previousNative) return true
    if (state.tables.has("session") && state.tables.has("session_message"))
      return yield* Effect.fail(new DatabaseLineage.UnsafeMigrationHistoryError([...state.completed], "unrecognized profile schema"))
    return false
  })
}

export function merge(target: Database, staged: string, key: string, sourceIdentity?: string) {
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
      const tables = objects.filter((object) => object.type === "table" && !object.name.startsWith("sqlite_") && object.name !== "_ws_stage")
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
      yield* tx.run(sql`INSERT INTO kv (key, value, time_created, time_updated) VALUES (${key}, ${JSON.stringify({ completed: true, sourceIdentity })}, ${Date.now()}, ${Date.now()})`)
    }), { behavior: "immediate" })
  }))
}

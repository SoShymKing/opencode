import { Database } from "bun:sqlite"
import { readdir } from "node:fs/promises"
import path from "node:path"
import { SqliteClient } from "@effect/sql-sqlite-bun"
import { EffectDrizzleSqlite } from "@opencode-ai/effect-drizzle-sqlite"
import { DatabaseMigration } from "@opencode-ai/core/database/migration"
import sessionMessagePartMigration from "@opencode-ai/core/database/migration/v02_session_message_part"
import { sql } from "drizzle-orm"
import { Effect } from "effect"

const makeDb = EffectDrizzleSqlite.makeWithDefaults()
export type MigrationDatabase = Effect.Success<typeof makeDb>
export const messageData = '{"agent":"build","content":[{"type":"text","text":"unchanged"}]}'
export const messageBytes = Buffer.from(messageData).toString("hex").toUpperCase()

type ExistingOptions = {
  readonly migrationIDs?: readonly string[]
  readonly drizzleIDs?: readonly string[]
  readonly partComplete?: boolean
}

export function useDatabase<A, E>(filename: string, use: (db: MigrationDatabase) => Effect.Effect<A, E>) {
  return Effect.runPromise(
    Effect.gen(function* () {
      return yield* use(yield* makeDb)
    }).pipe(Effect.provide(SqliteClient.layer({ filename })), Effect.scoped),
  )
}

export function setupExisting(db: MigrationDatabase, options: ExistingOptions = {}) {
  return Effect.gen(function* () {
    yield* db.run("PRAGMA journal_mode = WAL")
    yield* db.run("PRAGMA wal_autocheckpoint = 0")
    yield* db.run("PRAGMA foreign_keys = ON")
    yield* db.run(sql`CREATE TABLE session (id text PRIMARY KEY)`)
    yield* db.run(sql`CREATE TABLE session_message (
      id text PRIMARY KEY,
      session_id text NOT NULL REFERENCES session(id) ON DELETE CASCADE,
      type text NOT NULL,
      seq integer NOT NULL,
      time_created integer NOT NULL,
      time_updated integer NOT NULL,
      data text NOT NULL
    )`)
    yield* db.run(sql`CREATE TABLE migration (id TEXT PRIMARY KEY, time_completed INTEGER NOT NULL)`)
    yield* Effect.forEach(
      options.migrationIDs ?? [],
      (id) => db.run(sql`INSERT INTO migration (id, time_completed) VALUES (${id}, 1)`),
      { discard: true },
    )
    if (options.drizzleIDs) {
      yield* db.run(sql`CREATE TABLE __drizzle_migrations (
        id INTEGER PRIMARY KEY,
        hash text NOT NULL,
        created_at numeric,
        name text,
        applied_at text
      )`)
      yield* Effect.forEach(
        options.drizzleIDs,
        (id) => db.run(sql`INSERT INTO __drizzle_migrations (hash, name) VALUES ('hash', ${id})`),
        { discard: true },
      )
    }
    yield* db.run(sql`INSERT INTO session (id) VALUES ('session')`)
    yield* db.run(sql`INSERT INTO session_message (
      id, session_id, type, seq, time_created, time_updated, data
    ) VALUES ('message', 'session', 'assistant', 0, 1, 1, ${messageData})`)
    if (options.partComplete) yield* db.transaction((tx) => sessionMessagePartMigration.up(tx))
  })
}

export async function backupPaths(filename: string) {
  const prefix = `${path.basename(filename)}.backup-`
  return (await readdir(path.dirname(filename)))
    .filter((name) => name.startsWith(prefix) && name.endsWith(".db"))
    .map((name) => path.join(path.dirname(filename), name))
}

export function inspectBackup(filename: string) {
  using database = new Database(filename, { readonly: true })
  return {
    quickCheck: database.query<{ readonly quick_check: string }, []>("PRAGMA quick_check").get()?.quick_check,
    journal: database.query<{ readonly id: string }, []>("SELECT id FROM migration ORDER BY rowid").all(),
    partTable: database
      .query<
        { readonly name: string },
        []
      >("SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'session_message_part'")
      .get(),
    dataBytes: database
      .query<
        { readonly bytes: string },
        []
      >("SELECT hex(CAST(data AS blob)) AS bytes FROM session_message WHERE id = 'message'")
      .get()?.bytes,
  }
}

export function apply(filename: string) {
  return useDatabase(filename, (db) => DatabaseMigration.apply(db, filename))
}

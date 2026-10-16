import { describe, expect, test } from "bun:test"
import { readdir } from "node:fs/promises"
import { $ } from "bun"
import { fileURLToPath } from "url"
import path from "path"
import { SqliteClient } from "@effect/sql-sqlite-bun"
import { EffectDrizzleSqlite } from "@opencode/core/database/drizzle"
import { Deferred, Effect, Fiber, Layer } from "effect"
import { Reactivity } from "effect/reactivity"
import { SqlClient, Statement } from "effect/sql"
import { sql } from "drizzle-orm"
import { DatabaseMigration } from "@opencode/core/database/migration"
import { migrations } from "@opencode/core/database/migration.gen"
import sessionMessagePartMigration from "@opencode/core/database/ws-message-parts"
import nativeMessagePartMigration from "@opencode/core/database/migration/20261016000000_session_message_part"
import { Database } from "@opencode/core/database/database"
import { tmpdir } from "./fixture/tmpdir"
import legacyCredentialsMigration from "@opencode/core/database/migration/20260805200742_import_legacy_credentials"
import worktreeMigration from "@opencode/core/database/migration/20260812213948_worktree"
import previousV2Migration from "@opencode/core/database/migration/20260804233008_loose_psylocke"
import workspaceMigration from "@opencode/core/database/migration/20260808023530_workspace_domain"
import executionClaimsMigration from "@opencode/core/database/migration/20260811161259_execution_claim_attempts"
import sessionInboxMigration from "@opencode/core/database/migration/20260812181746_session_inbox"
import sessionViewedStateMigration from "@opencode/core/database/migration/20260819222447_session_viewed_state"
import azureCliCredentialMigration from "@opencode/core/database/migration/20261007190000_azure_cli_external_credential"
import { Global } from "@opencode/util/global"

const run = <A, E>(
  effect: Effect.Effect<A, E, SqlClient.SqlClient | Global.Service>,
  global = Global.make({ data: path.join(process.cwd(), ".test-data") }),
  filename = ":memory:",
) =>
  Effect.runPromise(
    effect.pipe(
      Effect.provideService(Global.Service, global),
      Effect.provide(SqliteClient.layer({ filename, disableWAL: filename === ":memory:" })),
      Effect.scoped,
    ),
  )

const makeDb = EffectDrizzleSqlite.makeWithDefaults()
const nativeIDs = migrations.map((migration) => migration.id)
const wsData = JSON.stringify({
  agent: "build", model: { providerID: "test", id: "model" },
  content: [{ id: "text-1", type: "text", text: "first" }, { id: "text-2", type: "text", text: "second" }],
  time: { created: 1 },
})
const parentData = JSON.stringify({ agent: "build", model: { providerID: "test", id: "model" }, time: { created: 1 } })
const nativeData = JSON.stringify({
  agent: "build", model: { providerID: "test", id: "model" },
  content: [{ type: "text", text: "first" }, { type: "text", text: "second" }], time: { created: 1 },
})
const wsHistories = [
  { name: "compact baseline", ids: ["v01_baseline"], complete: true },
  { name: "complete union with metadata alias", ids: DatabaseMigration.legacyBaselineMigrationIDs.map((id) =>
    id === DatabaseMigration.legacySessionMetadataMigrationID ? DatabaseMigration.legacySessionMetadataAlias : id), complete: true },
  { name: "old part marker", ids: [...DatabaseMigration.legacyBaselineMigrationIDs, DatabaseMigration.legacyPartMigrationID], complete: true },
  { name: "partial baseline", ids: DatabaseMigration.legacyBaselineMigrationIDs.slice(0, 3), complete: false },
  { name: "part marker without baseline", ids: [DatabaseMigration.legacyPartMigrationID], complete: false },
]

function wsFixture(db: EffectDrizzleSqlite.EffectSQLiteDatabase, ids: readonly string[]) {
  return Effect.gen(function* () {
    yield* db.run(sql`CREATE TABLE session (id TEXT PRIMARY KEY)`)
    yield* db.run(sql`CREATE TABLE session_message (id TEXT PRIMARY KEY, session_id TEXT REFERENCES session(id), type TEXT, seq INTEGER, data TEXT)`)
    yield* db.run(sql`CREATE TABLE migration (id TEXT PRIMARY KEY, time_completed INTEGER NOT NULL)`)
    yield* db.run(sql`CREATE TABLE __drizzle_migrations (name TEXT)`)
    yield* db.run(sql`INSERT INTO session VALUES ('session')`)
    yield* db.run(sql`INSERT INTO session_message VALUES ('msg_migration', 'session', 'assistant', 0, ${wsData})`)
    for (const [index, id] of ids.entries()) {
      yield* index % 2 === 0
        ? db.run(sql`INSERT INTO migration VALUES (${id}, 1)`)
        : db.run(sql`INSERT INTO __drizzle_migrations VALUES (${id})`)
    }
    if (ids.includes(DatabaseMigration.legacyPartMigrationID))
      yield* db.transaction((tx) => sessionMessagePartMigration.up(tx))
  })
}

function nativeFixture(db: EffectDrizzleSqlite.EffectSQLiteDatabase) {
  return Effect.gen(function* () {
    yield* db.run(sql`CREATE TABLE session_v2 (id TEXT PRIMARY KEY)`)
    yield* db.run(sql`CREATE TABLE session_message (id TEXT PRIMARY KEY, session_id TEXT REFERENCES session_v2(id), type TEXT, seq INTEGER, data TEXT)`)
    yield* db.run(sql`CREATE TABLE migration (id TEXT PRIMARY KEY, time_completed INTEGER NOT NULL)`)
    yield* db.run(sql`INSERT INTO session_v2 VALUES ('session')`)
    yield* db.run(sql`INSERT INTO session_message VALUES ('msg_migration', 'session', 'assistant', 0, ${nativeData})`)
    for (const id of nativeIDs.filter((id) => id !== nativeMessagePartMigration.id))
      yield* db.run(sql`INSERT INTO migration VALUES (${id}, 1)`)
  })
}

// A real in-memory SqlClient whose schema inspection signals `arrived` and then
// waits on `gate`. Bootstrap inspects the schema as its first locked statement,
// so a database built over this client parks while holding its migration lock.
const parkedClient = (arrived: Deferred.Deferred<void>, gate: Deferred.Deferred<void>) =>
  Layer.effect(
    SqlClient.SqlClient,
    Effect.gen(function* () {
      const client = yield* SqlClient.SqlClient
      const connection = yield* client.reserve
      const park = <A, E>(query: string, effect: Effect.Effect<A, E>) =>
        query.includes("sqlite_master")
          ? Deferred.succeed(arrived, undefined).pipe(Effect.andThen(Deferred.await(gate)), Effect.andThen(effect))
          : effect
      return yield* SqlClient.make({
        acquirer: Effect.succeed({
          ...connection,
          execute: (query, params, transform) => park(query, connection.execute(query, params, transform)),
          executeRaw: (query, params) => park(query, connection.executeRaw(query, params)),
        }),
        compiler: Statement.makeCompilerSqlite(),
        spanAttributes: [],
      })
    }),
  ).pipe(Layer.provide(SqliteClient.layer({ filename: ":memory:", disableWAL: true })), Layer.provide(Reactivity.layer))

describe("DatabaseMigration", () => {
  test.each(wsHistories)("refuses ws $name before mutation or backup", async (history) => {
    await using tmp = await tmpdir()
    await run(Effect.gen(function* () {
      const db = yield* makeDb
      yield* wsFixture(db, history.ids)
      const journal = yield* db.all(sql`SELECT * FROM migration ORDER BY rowid`)
      const drizzle = yield* db.all(sql`SELECT * FROM __drizzle_migrations ORDER BY rowid`)
      const schema = yield* db.all(sql`SELECT * FROM sqlite_master ORDER BY name`)
      const bytes = yield* db.get(sql`SELECT hex(CAST(data AS blob)) AS bytes FROM session_message`)
      const parts = history.ids.includes(DatabaseMigration.legacyPartMigrationID)
        ? yield* db.all(sql`SELECT * FROM session_message_part ORDER BY position`)
        : []
      for (const attempt of [DatabaseMigration.apply(db, path.join(tmp.path, "ws.sqlite")), DatabaseMigration.applyOnly(db, [nativeMessagePartMigration])]) {
        const error = yield* attempt.pipe(Effect.flip)
        expect(error).toBeInstanceOf(DatabaseMigration.WsUpgradeRequiredError)
        expect(error).toMatchObject({ baselineComplete: history.complete })
        expect(yield* db.all(sql`SELECT * FROM migration ORDER BY rowid`)).toEqual(journal)
        expect(yield* db.all(sql`SELECT * FROM __drizzle_migrations ORDER BY rowid`)).toEqual(drizzle)
        expect(yield* db.all(sql`SELECT * FROM sqlite_master ORDER BY name`)).toEqual(schema)
        expect(yield* db.get(sql`SELECT hex(CAST(data AS blob)) AS bytes FROM session_message`)).toEqual(bytes)
        if (history.ids.includes(DatabaseMigration.legacyPartMigrationID))
          expect(yield* db.all(sql`SELECT * FROM session_message_part ORDER BY position`)).toEqual(parts)
      }
    }), Global.make({ data: tmp.path }), path.join(tmp.path, "ws.sqlite"))
    expect((await readdir(tmp.path)).filter((name) => name.includes(".backup-"))).toEqual([])
  })

  test.each(wsHistories.filter((history) => history.complete))("BLOCKED automatic ws conversion: $name", async (history) => {
    await run(Effect.gen(function* () {
      const db = yield* makeDb
      yield* wsFixture(db, history.ids)
      yield* DatabaseMigration.apply(db)
      expect(yield* db.all(sql`SELECT id FROM migration WHERE id IN ('v01_baseline', 'v02_session_message_part') ORDER BY id`)).toEqual([
        { id: "v01_baseline" }, { id: "v02_session_message_part" },
      ])
      expect(yield* db.get(sql`SELECT data FROM session_message`)).toEqual({ data: parentData })
      expect(yield* db.get(sql`SELECT count(*) AS count FROM session_message_part`)).toEqual({ count: 2 })
    }))
  })

  test("v02 compatibility helper converts embedded assistant content without claiming native upgrade", async () => {
    await run(Effect.gen(function* () {
      const db = yield* makeDb
      yield* wsFixture(db, ["v01_baseline"])
      yield* db.transaction((tx) => sessionMessagePartMigration.up(tx))
      expect(yield* db.get(sql`SELECT data FROM session_message`)).toEqual({ data: parentData })
      expect(yield* db.all(sql`SELECT position, id, type, data FROM session_message_part ORDER BY position`)).toEqual([
        { position: 0, id: "text-1", type: "text", data: '{"text":"first"}' },
        { position: 1, id: "text-2", type: "text", data: '{"text":"second"}' },
      ])
    }))
  })

  test.each([":memory:", "native.sqlite"])("normalizes native history once with the timestamp registry: %s", async (name) => {
    await using tmp = await tmpdir()
    const filename = name === ":memory:" ? name : path.join(tmp.path, name)
    await run(Effect.gen(function* () {
      const db = yield* makeDb
      yield* nativeFixture(db)
      yield* db.run(sql`PRAGMA wal_autocheckpoint = 0`)
      yield* DatabaseMigration.apply(db, filename)
      yield* DatabaseMigration.apply(db, filename)
      expect(yield* db.all(sql`SELECT id FROM migration ORDER BY rowid`)).toEqual(nativeIDs.map((id) => ({ id })))
      expect(yield* db.get(sql`SELECT data FROM session_message`)).toEqual({ data: parentData })
      expect(yield* db.all(sql`SELECT position, id, type, data FROM session_message_part ORDER BY position`)).toEqual([
        { position: 0, id: "0", type: "text", data: '{"text":"first"}' },
        { position: 1, id: "1", type: "text", data: '{"text":"second"}' },
      ])
    }), Global.make({ data: tmp.path }), filename)
    const backups = (await readdir(tmp.path)).filter((name) => name.includes(".backup-"))
    expect(backups).toHaveLength(filename === ":memory:" ? 0 : 1)
    for (const backup of backups) await run(Effect.gen(function* () {
      const db = yield* makeDb
      expect(yield* db.get(sql`PRAGMA quick_check`)).toEqual({ quick_check: "ok" })
      expect(yield* db.get(sql`SELECT hex(CAST(data AS blob)) AS bytes FROM session_message`)).toEqual({
        bytes: Buffer.from(nativeData).toString("hex").toUpperCase(),
      })
      expect(yield* db.get(sql`SELECT json_array_length(data, '$.content') AS count FROM session_message`)).toEqual({ count: 2 })
      expect(yield* db.get(sql`SELECT name FROM sqlite_master WHERE name = 'session_message_part'`)).toBeUndefined()
      expect(yield* db.all(sql`SELECT id FROM migration ORDER BY rowid`)).toEqual(nativeIDs.filter((id) => id !== nativeMessagePartMigration.id).map((id) => ({ id })))
    }), Global.make({ data: tmp.path }), path.join(tmp.path, backup))
  })

  test("retains native backup and rolls back the normalization marker when DDL fails", async () => {
    await using tmp = await tmpdir()
    const filename = path.join(tmp.path, "failure.sqlite")
    await run(Effect.gen(function* () {
      const db = yield* makeDb
      yield* nativeFixture(db)
      yield* db.run(sql`CREATE TABLE session_message_part (id TEXT)`)
      const before = yield* db.all(sql`SELECT * FROM session_message`)
      expect((yield* Effect.exit(DatabaseMigration.apply(db, filename)))._tag).toBe("Failure")
      expect(yield* db.all(sql`SELECT * FROM session_message`)).toEqual(before)
      expect(yield* db.get(sql`SELECT id FROM migration WHERE id = ${nativeMessagePartMigration.id}`)).toBeUndefined()
    }), Global.make({ data: tmp.path }), filename)
    const backups = (await readdir(tmp.path)).filter((name) => name.includes(".backup-"))
    expect(backups).toHaveLength(1)
    for (const backup of backups) await run(Effect.gen(function* () {
      const db = yield* makeDb
      expect(yield* db.get(sql`PRAGMA quick_check`)).toEqual({ quick_check: "ok" })
      expect(yield* db.get(sql`SELECT data FROM session_message`)).toEqual({ data: nativeData })
    }), Global.make({ data: tmp.path }), path.join(tmp.path, backup))
  })

  test("bootstraps the same fresh file concurrently without a backup", async () => {
    await using tmp = await tmpdir()
    const filename = path.join(tmp.path, "embedded.sqlite")

    await Effect.runPromise(
      Effect.all(
        [Database.layer({ path: filename }), Database.layer({ path: filename })].map((layer) =>
          Effect.scoped(Layer.build(layer)),
        ),
        { concurrency: "unbounded" },
      ).pipe(Effect.provideService(Global.Service, Global.make({ data: tmp.path }))),
    )
    expect((await readdir(tmp.path)).filter((name) => name.includes(".backup-"))).toEqual([])
  })

  test("serializes fresh initialization across independent processes", async () => {
    await using tmp = await tmpdir()
    const filename = path.join(tmp.path, "concurrent.sqlite")
    const worker = `
      import { Database } from "@opencode/core/database/database"
      import { Global } from "@opencode/util/global"
      import { Effect, Layer } from "effect"
      await Effect.runPromise(Effect.scoped(Layer.build(Database.layer({ path: process.argv[1] }))).pipe(
        Effect.provideService(Global.Service, Global.make({ data: process.env.OPENCODE_TEST_HOME })),
      ))
    `
    const children = Array.from({ length: 8 }, () => Bun.spawn([process.execPath, "-e", worker, filename], {
      cwd: process.cwd(), stdout: "ignore", stderr: "pipe",
    }))
    const results = await Promise.all(children.map(async (child) => {
      const [exit, stderr] = await Promise.all([child.exited, new Response(child.stderr).text()])
      return { exit, stderr }
    }))
    expect(results).toEqual(Array.from({ length: children.length }, () => ({ exit: 0, stderr: "" })))
    await run(Effect.gen(function* () {
      const db = yield* makeDb
      expect(yield* db.all(sql`SELECT id FROM migration ORDER BY rowid`)).toEqual(nativeIDs.map((id) => ({ id })))
    }), Global.make({ data: tmp.path }), filename)
    expect((await readdir(tmp.path)).filter((name) => name.includes(".backup-"))).toEqual([])
  })

  test("bootstraps distinct databases without waiting on each other's lock", async () => {
    await Effect.runPromise(
      Effect.gen(function* () {
        const arrived = yield* Deferred.make<void>()
        const gate = yield* Deferred.make<void>()
        // Park the first database inside its bootstrap, after it holds its lock.
        const parked = yield* Effect.forkScoped(
          Layer.build(Database.layerFromClient.pipe(Layer.provide(parkedClient(arrived, gate)))),
        )
        yield* Deferred.await(arrived)

        yield* Layer.build(
          Database.layerFromClient.pipe(Layer.provide(SqliteClient.layer({ filename: ":memory:", disableWAL: true }))),
        ).pipe(Effect.timeout("2 seconds"))

        expect(parked.pollUnsafe()).toBeUndefined()
        yield* Deferred.succeed(gate, undefined)
        yield* Fiber.join(parked)
      }).pipe(
        Effect.provideService(Global.Service, Global.make({ data: path.join(process.cwd(), ".test-data") })),
        Effect.scoped,
      ),
    )
  })

  if (process.platform === "linux") {
    test("declared schema has no ungenerated migrations", async () => {
      const result = await $`bun ${fileURLToPath(new URL("../script/migration.ts", import.meta.url))} --check`
        .quiet()
        .nothrow()
      expect(result.exitCode, result.stderr.toString()).toBe(0)
      expect(result.stdout.toString()).toContain("No schema changes, nothing to migrate")
    }, 30_000)
  }

  test("bootstraps the current schema and records the migration registry", async () => {
    await run(
      Effect.gen(function* () {
        const db = yield* makeDb
        yield* DatabaseMigration.apply(db)

        expect(yield* db.get(sql`SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'session_v2'`)).toEqual(
          {
            name: "session_v2",
          },
        )
        expect(
          yield* db.get(sql`SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'session_pending'`),
        ).toEqual({ name: "session_pending" })
        expect(yield* db.get(sql`SELECT count(*) AS count FROM migration`)).toEqual({ count: migrations.length })
        expect(yield* db.get(sql`SELECT name FROM sqlite_master WHERE name = 'session_message_part'`)).toEqual({ name: "session_message_part" })
        expect(nativeIDs.every((id) => /^\d{14}_/.test(id))).toBe(true)
        expect(yield* db.all(sql`SELECT id FROM migration ORDER BY rowid`)).toEqual(nativeIDs.map((id) => ({ id })))
      }),
    )
  })

  test("adds nullable attention state to existing sessions", async () => {
    await run(
      Effect.gen(function* () {
        const db = yield* makeDb
        yield* db.run(sql`CREATE TABLE session_v2 (id text PRIMARY KEY, title text)`)
        yield* db.run(sql`INSERT INTO session_v2 (id, title) VALUES ('ses_existing', 'Existing')`)

        yield* DatabaseMigration.applyOnly(db, [sessionViewedStateMigration])
        yield* DatabaseMigration.applyOnly(db, [sessionViewedStateMigration])

        expect(yield* db.get(sql`SELECT id, title, time_idle, time_viewed, idle_outcome FROM session_v2`)).toEqual({
          id: "ses_existing",
          title: "Existing",
          time_idle: null,
          time_viewed: null,
          idle_outcome: null,
        })
        expect(yield* db.get(sql`SELECT count(*) AS count FROM migration`)).toEqual({ count: 1 })
      }),
    )
  })

  test("bootstraps alongside underscore-prefixed embedder tables", async () => {
    await run(
      Effect.gen(function* () {
        const db = yield* makeDb
        yield* db.run(sql`CREATE TABLE _embedder_state (id text PRIMARY KEY)`)
        yield* DatabaseMigration.apply(db)
        expect(yield* db.get(sql`SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'session_v2'`)).toEqual(
          { name: "session_v2" },
        )
      }),
    )
  })

  test("applies generic migrations once and records their order", async () => {
    await run(
      Effect.gen(function* () {
        const db = yield* makeDb
        yield* db.run(sql`CREATE TABLE session (id text PRIMARY KEY)`)
        const input = [
          {
            id: "first",
            up: (tx: Parameters<Parameters<typeof db.transaction>[0]>[0]) =>
              tx.run(sql`CREATE TABLE applied (id text PRIMARY KEY)`),
          },
          {
            id: "second",
            up: (tx: Parameters<Parameters<typeof db.transaction>[0]>[0]) =>
              tx.run(sql`INSERT INTO applied (id) VALUES ('second')`),
          },
        ]

        yield* DatabaseMigration.applyOnly(db, input)
        yield* DatabaseMigration.applyOnly(db, input)

        expect(yield* db.all(sql`SELECT id FROM applied`)).toEqual([{ id: "second" }])
        expect(yield* db.all(sql`SELECT id FROM migration ORDER BY time_completed, id`)).toEqual([
          { id: "first" },
          { id: "second" },
        ])
      }),
    )
  })

  test("BLOCKED preserves previous V2 state through the current migration lineage", async () => {
    await run(
      Effect.gen(function* () {
        const db = yield* makeDb
        yield* db.run(sql`PRAGMA foreign_keys = ON`)
        yield* db.run(sql`CREATE TABLE migration (id text PRIMARY KEY, time_completed integer NOT NULL)`)
        yield* db.run(sql`
          INSERT INTO migration (id, time_completed)
          VALUES ('20260730195856_optional_session_title', 1)
        `)
        yield* db.run(sql`CREATE TABLE project (id text PRIMARY KEY)`)
        yield* db.run(sql`
          CREATE TABLE project_directory (
            project_id text NOT NULL,
            directory text NOT NULL,
            type text,
            strategy text,
            time_created integer NOT NULL,
            PRIMARY KEY (project_id, directory)
          )
        `)
        yield* db.run(sql`
          CREATE TABLE workspace (
            id text PRIMARY KEY,
            type text NOT NULL,
            name text NOT NULL,
            project_id text NOT NULL,
            time_used integer NOT NULL
          )
        `)
        yield* db.run(sql`
          CREATE TABLE session (
            id text PRIMARY KEY,
            project_id text NOT NULL REFERENCES project(id) ON DELETE CASCADE,
            workspace_id text,
            parent_id text,
            time_suspended integer
          )
        `)
        yield* db.run(sql`CREATE INDEX session_project_idx ON session (project_id)`)
        yield* db.run(sql`CREATE INDEX session_workspace_idx ON session (workspace_id)`)
        yield* db.run(sql`CREATE INDEX session_parent_idx ON session (parent_id)`)
        yield* db.run(
          sql`CREATE INDEX session_time_suspended_idx ON session (time_suspended) WHERE "session"."time_suspended" IS NOT NULL`,
        )
        yield* db.run(sql`
          CREATE TABLE session_message (
            id text PRIMARY KEY,
            session_id text NOT NULL REFERENCES session(id) ON DELETE CASCADE,
            data text NOT NULL
          )
        `)
        yield* db.run(sql`CREATE TABLE message (id text PRIMARY KEY, session_id text NOT NULL)`)
        yield* db.run(sql`
          CREATE TABLE session_pending (
            id text PRIMARY KEY,
            session_id text NOT NULL REFERENCES session(id) ON DELETE CASCADE
          )
        `)
        yield* db.run(sql`CREATE TABLE event_sequence (aggregate_id text PRIMARY KEY, seq integer NOT NULL)`)
        yield* db.run(sql`
          CREATE TABLE event (
            id text PRIMARY KEY,
            aggregate_id text NOT NULL REFERENCES event_sequence(aggregate_id) ON DELETE CASCADE,
            seq integer NOT NULL,
            created integer NOT NULL,
            type text NOT NULL,
            data text NOT NULL
          )
        `)
        yield* db.run(sql`CREATE TABLE data_migration (name text PRIMARY KEY)`)
        yield* db.run(sql`INSERT INTO project VALUES ('project')`)
        yield* db.run(sql`INSERT INTO project_directory VALUES ('project', '/repo', 'main', NULL, 1)`)
        yield* db.run(sql`INSERT INTO session VALUES ('session', 'project', NULL, NULL, NULL)`)
        yield* db.run(sql`INSERT INTO session_message VALUES ('message', 'session', '{"text":"preserved"}')`)
        yield* db.run(sql`INSERT INTO session_pending VALUES ('pending', 'session')`)
        yield* db.run(sql`INSERT INTO event_sequence VALUES ('session', 41)`)
        yield* db.run(sql`INSERT INTO event VALUES ('event', 'session', 41, 1, 'session.text.ended.1', '{}')`)

        yield* DatabaseMigration.applyOnly(db, [
          previousV2Migration,
          workspaceMigration,
          executionClaimsMigration,
          sessionInboxMigration,
          worktreeMigration,
        ])

        expect(yield* db.get(sql`SELECT id, resume_attempts FROM session_v2`)).toEqual({
          id: "session",
          resume_attempts: 0,
        })
        expect(yield* db.get(sql`SELECT id, data FROM session_message`)).toEqual({
          id: "message",
          data: '{"text":"preserved"}',
        })
        expect(yield* db.get(sql`SELECT id FROM session_pending`)).toEqual({ id: "pending" })
        expect(yield* db.get(sql`SELECT seq FROM event_sequence`)).toEqual({ seq: 41 })
        expect(yield* db.get(sql`SELECT id, seq FROM event`)).toEqual({ id: "event", seq: 41 })
        expect(
          yield* db.get(sql`SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'session'`),
        ).toBeUndefined()
        expect(yield* db.get(sql`SELECT directory FROM worktree`)).toEqual({ directory: "/repo" })
        expect(yield* db.all<{ table: string }>(sql`PRAGMA foreign_key_list(session_message)`)).toContainEqual(
          expect.objectContaining({ table: "session_v2" }),
        )
        expect(yield* db.all<{ table: string }>(sql`PRAGMA foreign_key_list(session_pending)`)).toContainEqual(
          expect.objectContaining({ table: "session_v2" }),
        )
      }),
    )
  })

  test("rejects previous V2 databases with V1-only session history", async () => {
    await run(
      Effect.gen(function* () {
        const db = yield* makeDb
        yield* db.run(sql`CREATE TABLE migration (id text PRIMARY KEY, time_completed integer NOT NULL)`)
        yield* db.run(sql`
          INSERT INTO migration (id, time_completed)
          VALUES ('20260730195856_optional_session_title', 1)
        `)
        yield* db.run(sql`CREATE TABLE session (id text PRIMARY KEY)`)
        yield* db.run(sql`CREATE TABLE session_message (id text PRIMARY KEY, session_id text NOT NULL)`)
        yield* db.run(sql`CREATE TABLE message (id text PRIMARY KEY, session_id text NOT NULL)`)
        yield* db.run(sql`INSERT INTO session VALUES ('session')`)
        yield* db.run(sql`INSERT INTO message VALUES ('message', 'session')`)

        expect((yield* Effect.exit(DatabaseMigration.applyOnly(db, [previousV2Migration])))._tag).toBe("Failure")
        expect(yield* db.get(sql`SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'session'`)).toEqual({
          name: "session",
        })
        expect(yield* db.get(sql`SELECT id FROM migration WHERE id = ${previousV2Migration.id}`)).toBeUndefined()
      }),
    )
  })

  test("copies project directories into worktrees without removing the old table", async () => {
    await run(
      Effect.gen(function* () {
        const db = yield* makeDb
        yield* db.run(sql`CREATE TABLE project (id text PRIMARY KEY)`)
        yield* db.run(
          sql`CREATE TABLE project_directory (project_id text NOT NULL, directory text NOT NULL, type text, strategy text, time_created integer NOT NULL, PRIMARY KEY (project_id, directory))`,
        )
        yield* db.run(
          sql`INSERT INTO project_directory (project_id, directory, type, strategy, time_created) VALUES ('project', '/root', 'main', NULL, 1), ('project', '/legacy', 'git_worktree', NULL, 2), ('project', '/strategy', NULL, 'git_worktree', 3), ('project', '/custom', NULL, 'acme/snapshot', 4)`,
        )

        yield* DatabaseMigration.applyOnly(db, [worktreeMigration])

        expect(yield* db.all(sql`SELECT directory, strategy FROM worktree ORDER BY directory`)).toEqual([
          { directory: "/custom", strategy: "acme/snapshot" },
          { directory: "/legacy", strategy: "git" },
          { directory: "/root", strategy: null },
          { directory: "/strategy", strategy: "git" },
        ])
        expect(yield* db.get(sql`SELECT count(*) AS count FROM project_directory`)).toEqual({ count: 4 })
      }),
    )
  })

  test("imports legacy JSON credentials without changing the source file or existing credentials", async () => {
    await using tmp = await tmpdir()
    const source = path.join(tmp.path, "auth.json")
    const content = JSON.stringify({
      openai: { type: "oauth", refresh: "refresh", access: "access", expires: 123, accountId: "account" },
      anthropic: { type: "api", key: "legacy-key", metadata: { region: "us" } },
      google: { type: "api", key: "google-key", metadata: { region: "us" } },
      "github-copilot": { type: "oauth", refresh: "refresh", access: "access", expires: 123 },
      "custom-provider": { type: "api", key: "custom-key" },
      "https://example.com/": { type: "wellknown", key: "TOKEN", token: "wellknown-key" },
      invalid: { type: "unknown" },
    })
    await Bun.write(source, content)

    await run(
      Effect.gen(function* () {
        const db = yield* makeDb
        yield* DatabaseMigration.apply(db)
        const now = Date.now()
        yield* db.run(sql`
          INSERT INTO credential (id, integration_id, label, value, time_created, time_updated)
          VALUES ('existing', 'anthropic', 'Existing', ${JSON.stringify({ type: "key", key: "current-key" })}, ${now}, ${now})
        `)

        yield* db.run(sql`DELETE FROM migration WHERE id = ${legacyCredentialsMigration.id}`)
        yield* DatabaseMigration.applyOnly(db, [legacyCredentialsMigration])
        yield* DatabaseMigration.applyOnly(db, [legacyCredentialsMigration])

        expect(yield* db.all(sql`SELECT integration_id, label, value FROM credential ORDER BY integration_id`)).toEqual(
          [
            {
              integration_id: "anthropic",
              label: "Existing",
              value: JSON.stringify({ type: "key", key: "current-key" }),
            },
            {
              integration_id: "custom-provider",
              label: "API key",
              value: JSON.stringify({ type: "key", key: "custom-key" }),
            },
            {
              integration_id: "github-copilot",
              label: "OAuth",
              value: JSON.stringify({
                type: "oauth",
                methodID: "device",
                refresh: "refresh",
                access: "access",
                expires: 123,
              }),
            },
            {
              integration_id: "google",
              label: "API key",
              value: JSON.stringify({ type: "key", key: "google-key", metadata: { region: "us" } }),
            },
            {
              integration_id: "https://example.com",
              label: "API key",
              value: JSON.stringify({ type: "key", key: "wellknown-key" }),
            },
            {
              integration_id: "openai",
              label: "OAuth",
              value: JSON.stringify({
                type: "oauth",
                methodID: "chatgpt-browser",
                refresh: "refresh",
                access: "access",
                expires: 123,
                metadata: { accountID: "account" },
              }),
            },
          ],
        )
        expect(yield* db.get(sql`SELECT value FROM kv WHERE key = 'wellknown:sources'`)).toEqual({
          value: JSON.stringify(["https://example.com"]),
        })
      }),
      Global.make({ data: tmp.path }),
    )

    expect(await Bun.file(source).text()).toBe(content)
  })

  test("skips legacy credential import when the source file is absent", async () => {
    await using tmp = await tmpdir()

    await run(
      Effect.gen(function* () {
        const db = yield* makeDb
        yield* DatabaseMigration.apply(db)
        yield* db.run(sql`DELETE FROM migration WHERE id = ${legacyCredentialsMigration.id}`)
        yield* DatabaseMigration.applyOnly(db, [legacyCredentialsMigration])

        expect(yield* db.all(sql`SELECT id FROM credential`)).toEqual([])
      }),
      Global.make({ data: tmp.path }),
    )
  })

  test("moves Azure CLI connections saved as OAuth credentials to external credentials", async () => {
    await run(
      Effect.gen(function* () {
        const db = yield* makeDb
        yield* DatabaseMigration.apply(db)
        const now = Date.now()
        const oauth = { type: "oauth", methodID: "azure-cli", access: "token", refresh: "azure-cli", expires: 123 }
        yield* db.run(sql`
          INSERT INTO credential (id, integration_id, label, value, time_created, time_updated) VALUES
            ('cli', 'azure', 'Azure CLI', ${JSON.stringify({ ...oauth, metadata: { resourceName: "my-models" } })}, ${now}, ${now}),
            ('cli-configured', 'azure', 'Azure CLI', ${JSON.stringify(oauth)}, ${now}, ${now}),
            ('key', 'azure', 'API key', ${JSON.stringify({ type: "key", key: "secret" })}, ${now}, ${now}),
            ('other', 'openai', 'OAuth', ${JSON.stringify(oauth)}, ${now}, ${now})
        `)

        yield* db.run(sql`DELETE FROM migration WHERE id = ${azureCliCredentialMigration.id}`)
        yield* DatabaseMigration.applyOnly(db, [azureCliCredentialMigration])

        expect(yield* db.all(sql`SELECT id, value FROM credential ORDER BY id`)).toEqual([
          {
            id: "cli",
            value: JSON.stringify({ type: "external", methodID: "azure-cli", metadata: { resourceName: "my-models" } }),
          },
          { id: "cli-configured", value: JSON.stringify({ type: "external", methodID: "azure-cli" }) },
          { id: "key", value: JSON.stringify({ type: "key", key: "secret" }) },
          { id: "other", value: JSON.stringify(oauth) },
        ])
      }),
    )
  })

  test("rolls back a failed migration without recording it", async () => {
    await run(
      Effect.gen(function* () {
        const db = yield* makeDb
        yield* db.run(sql`CREATE TABLE session (id text PRIMARY KEY)`)
        const migration = {
          id: "failing",
          up: (tx: Parameters<Parameters<typeof db.transaction>[0]>[0]) =>
            Effect.gen(function* () {
              yield* tx.run(sql`CREATE TABLE rolled_back (id text PRIMARY KEY)`)
              yield* Effect.fail(new Error("stop"))
            }),
        }

        expect((yield* Effect.exit(DatabaseMigration.applyOnly(db, [migration])))._tag).toBe("Failure")
        expect(
          yield* db.get(sql`SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'rolled_back'`),
        ).toBeUndefined()
        expect(yield* db.get(sql`SELECT id FROM migration WHERE id = 'failing'`)).toBeUndefined()
      }),
    )
  })

  test("suspends foreign keys outside migrations that rebuild referenced tables", async () => {
    await run(
      Effect.gen(function* () {
        const db = yield* makeDb
        yield* db.run(sql`PRAGMA foreign_keys = ON`)
        yield* db.run(sql`CREATE TABLE session (id text PRIMARY KEY, title text NOT NULL)`)
        yield* db.run(
          sql`CREATE TABLE message (id text PRIMARY KEY, session_id text NOT NULL REFERENCES session(id) ON DELETE CASCADE)`,
        )
        yield* db.run(sql`INSERT INTO session VALUES ('session', 'title')`)
        yield* db.run(sql`INSERT INTO message VALUES ('message', 'session')`)

        yield* DatabaseMigration.applyOnly(db, [
          {
            id: "rebuild",
            foreignKeys: false,
            up: (tx) =>
              Effect.gen(function* () {
                yield* tx.run(sql`CREATE TABLE next_session (id text PRIMARY KEY, title text)`)
                yield* tx.run(sql`INSERT INTO next_session SELECT * FROM session`)
                yield* tx.run(sql`DROP TABLE session`)
                yield* tx.run(sql`ALTER TABLE next_session RENAME TO session`)
              }),
          },
        ])

        expect(yield* db.get(sql`SELECT id FROM message`)).toEqual({ id: "message" })
        expect(yield* db.get<{ foreign_keys: number }>(sql`PRAGMA foreign_keys`)).toEqual({ foreign_keys: 1 })
      }),
    )
  })

  test.each(["name", "timestamp"])("imports a recognized Drizzle %s journal once and rejects later unknown evidence", async (kind) => {
    await run(
      Effect.gen(function* () {
        const db = yield* makeDb
        yield* db.run(
          sql`CREATE TABLE __drizzle_migrations (id INTEGER PRIMARY KEY, hash text NOT NULL, created_at numeric, name text, applied_at TEXT)`,
        )
        yield* db.run(sql`
          INSERT INTO __drizzle_migrations (hash, created_at, name, applied_at)
          VALUES ('hash', ${kind === "timestamp" ? Date.UTC(2026, 9, 10, 18, 4, 56) : null}, ${kind === "name" ? "20261010180456_project_time_archived" : null}, ${new Date().toISOString()})
        `)

        yield* DatabaseMigration.applyOnly(db, [])
        yield* DatabaseMigration.applyOnly(db, [])
        expect(yield* db.all(sql`SELECT id FROM migration`)).toEqual([{ id: "20261010180456_project_time_archived" }])
        const before = yield* db.all(sql`SELECT * FROM migration`)
        yield* kind === "timestamp"
          ? db.run(sql`UPDATE __drizzle_migrations SET created_at = 1`)
          : db.run(sql`UPDATE __drizzle_migrations SET name = 'unknown'`)
        const drizzle = yield* db.all(sql`SELECT * FROM __drizzle_migrations`)
        expect(yield* DatabaseMigration.applyOnly(db, []).pipe(Effect.flip)).toBeInstanceOf(DatabaseMigration.UnsafeMigrationHistoryError)
        expect(yield* db.all(sql`SELECT * FROM migration`)).toEqual(before)
        expect(yield* db.all(sql`SELECT * FROM __drizzle_migrations`)).toEqual(drizzle)
      }),
    )
  })
})

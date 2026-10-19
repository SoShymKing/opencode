import { expect, test } from "bun:test"
import { join } from "node:path"
import { Effect } from "effect"
import { sql } from "drizzle-orm"
import { SQLiteDialect } from "drizzle-orm/sqlite-core"
import { WsNative } from "../src/database/ws-native"
import { WsNativeFile } from "../src/database/ws-native-file"
import { WsNativeStage } from "../src/database/ws-native-stage"
import { DatabaseArtifact } from "../src/database/artifact"
import { DatabaseWsArchive } from "../src/database/ws-archive"
import { EffectDrizzleSqlite } from "../src/database/drizzle"
import { sqliteLayer } from "../src/database/sqlite.bun"
import { setupWs, messageID, sessionID, legacySessionID } from "./fixture/database-ws"
import { tempGlobalLayer } from "./fixture/global"
import { tmpdir } from "./fixture/tmpdir"

test("equal physical WAL evidence cannot authorize a different committed snapshot", async () => {
  await using temp = await tmpdir()
  const filename = join(temp.path, "source.db")
  await Effect.runPromise(Effect.gen(function* () {
    const db = yield* EffectDrizzleSqlite.makeWithDefaults()
    yield* db.run("PRAGMA journal_mode = WAL")
    yield* setupWs(db, { directory: temp.path })
    yield* WsNativeStage.prepare(filename, filename)
    const marker = yield* WsNativeStage.read(filename)
    yield* db.run("UPDATE session SET title = 'later published commit'")
    const current = yield* WsNativeStage.identity(filename)
    yield* DatabaseArtifact.replaceWithText(`${filename}.ws-stage.json`, JSON.stringify({ ...marker, source: { ...marker.source, evidence: current.evidence } }))
    expect((yield* WsNativeStage.prepare(filename, filename).pipe(Effect.exit))._tag).toBe("Failure")
    expect(yield* WsNativeStage.identity(filename)).toEqual(current)
  }).pipe(Effect.provide(sqliteLayer({ filename })), Effect.provide(tempGlobalLayer), Effect.scoped))
})

test("WAL preparation checks the retained snapshot before using a supplied physical proof", async () => {
  await using temp = await tmpdir()
  const filename = join(temp.path, "source.db")
  await Effect.runPromise(Effect.gen(function* () {
    const db = yield* EffectDrizzleSqlite.makeWithDefaults()
    yield* db.run("PRAGMA journal_mode = WAL")
    yield* setupWs(db, { directory: temp.path })
    const proof = yield* WsNativeStage.capture(filename, filename)
    yield* db.run("UPDATE session SET title = 'new committed generation'")
    const current = yield* WsNativeStage.identity(filename)
    expect((yield* WsNativeStage.prepare(filename, filename, {
      ...proof, before: current, marker: { ...proof.marker, source: { ...proof.marker.source, evidence: current.evidence } },
    }).pipe(Effect.exit))._tag).toBe("Failure")
    expect(yield* DatabaseArtifact.exists(join(temp.path, proof.marker.work))).toBe(false)
    expect(yield* WsNativeStage.identity(filename)).toEqual(current)
  }).pipe(Effect.provide(sqliteLayer({ filename })), Effect.provide(tempGlobalLayer), Effect.scoped))
})

test("intentional v1 import can replace a different target snapshot", async () => {
  await using temp = await tmpdir()
  const target = join(temp.path, "target.db")
  const imported = join(temp.path, "import.db")
  for (const filename of [target, imported]) await Effect.runPromise(Effect.gen(function* () {
    const db = yield* EffectDrizzleSqlite.makeWithDefaults()
    yield* setupWs(db, { directory: temp.path })
    yield* db.run(sql`UPDATE session SET title = ${filename === target ? "target preimage" : "intentional import"}`)
  }).pipe(Effect.provide(sqliteLayer({ filename })), Effect.scoped))
  await DatabaseArtifact.write(`${target}.replacement.json`, JSON.stringify({ format: "opencode.database.replacement", version: 1,
    phase: "prepared", staged: { name: "import.db", ...(await DatabaseArtifact.digest(imported).pipe(Effect.runPromise)) },
  })).pipe(Effect.runPromise)
  await WsNativeFile.admit(target).pipe(Effect.provide(tempGlobalLayer), Effect.scoped, Effect.runPromise)
  await Effect.runPromise(Effect.gen(function* () {
    const db = yield* EffectDrizzleSqlite.makeWithDefaults()
    expect(yield* db.get("SELECT title FROM session_v2")).toEqual({ title: "intentional import" })
  }).pipe(Effect.provide(sqliteLayer({ filename: target })), Effect.scoped))
})

test("pending private replacement rejects a newer normalized preimage", async () => {
  await using temp = await tmpdir()
  const filename = join(temp.path, "source.db")
  await Effect.runPromise(Effect.gen(function* () {
    const db = yield* EffectDrizzleSqlite.makeWithDefaults()
    yield* setupWs(db, { directory: temp.path })
  }).pipe(Effect.provide(sqliteLayer({ filename })), Effect.scoped))
  const ready = await WsNativeStage.prepare(filename, filename).pipe(Effect.provide(tempGlobalLayer), Effect.scoped, Effect.runPromise)
  await WsNativeStage.handoff(filename).pipe(Effect.runPromise)
  await Effect.runPromise(Effect.gen(function* () {
    const db = yield* EffectDrizzleSqlite.makeWithDefaults()
    yield* db.run("UPDATE session SET title = 'newly normalized commit'")
  }).pipe(Effect.provide(sqliteLayer({ filename })), Effect.scoped))
  const before = await DatabaseArtifact.digest(filename).pipe(Effect.runPromise)
  await DatabaseArtifact.write(`${filename}.replacement.json`, JSON.stringify({
    format: "opencode.database.replacement", version: 2, stageFormat: "native", phase: "prepared", staged: ready, targetPreimage: before,
  })).pipe(Effect.runPromise)
  await expect(WsNativeFile.admit(filename).pipe(Effect.provide(tempGlobalLayer), Effect.scoped, Effect.runPromise)).rejects.toThrow()
  expect(await DatabaseArtifact.digest(filename).pipe(Effect.runPromise)).toEqual(before)
})

test("prefix verification seeks indexed messages without sorting each page", async () => {
  await Effect.runPromise(Effect.gen(function* () {
    const db = yield* EffectDrizzleSqlite.makeWithDefaults()
    yield* setupWs(db, { directory: "/fixture" })
    yield* WsNative.prepareStage(db, "plan-token")
    yield* WsNative.resumeStage(db, "plan-token")
    const queries: Parameters<typeof db.all>[0][] = []
    const dialect = new SQLiteDialect()
    const execute = db.all.bind(db)
    db.all = (query) => {
      const text = typeof query === "string" ? query : dialect.sqlToQuery(query.getSQL()).sql
      if (text.includes("FROM session_message m") && text.includes("ORDER BY") && text.includes("LIMIT")) queries.push(query)
      return execute(query)
    }
    yield* WsNative.resumeStage(db, "plan-token")
    expect(queries.length).toBeGreaterThan(0)
    for (const query of queries) {
      const statement = typeof query === "string" ? sql.raw(query) : query.getSQL()
      const plan = yield* execute<{ detail: string }>(sql`EXPLAIN QUERY PLAN ${statement}`)
      expect(plan.some((row) => row.detail.includes("TEMP B-TREE"))).toBe(false)
      if (dialect.sqlToQuery(statement).sql.includes(" > ")) {
        expect(plan.some((row) => row.detail.includes("SEARCH m USING"))).toBe(true)
        expect(plan.some((row) => row.detail.startsWith("SCAN m"))).toBe(false)
      }
    }
  }).pipe(Effect.provide(sqliteLayer({ filename: ":memory:" })), Effect.provide(tempGlobalLayer), Effect.scoped))
})

test("archive seals admit bounded bytes and chunk oversized cells", async () => {
  await Effect.runPromise(Effect.gen(function* () {
    const db = yield* EffectDrizzleSqlite.makeWithDefaults()
    yield* setupWs(db, { directory: "/fixture" })
    yield* db.run("CREATE TABLE seal_bytes (id INTEGER PRIMARY KEY, payload BLOB, text TEXT)")
    yield* db.run("INSERT INTO seal_bytes VALUES (1, zeroblob(200000), 'A' || char(0) || 'B'), (2, zeroblob(200000), ''), (3, zeroblob(200000), NULL), (4, zeroblob(600000), 'large')")
    yield* db.run("INSERT INTO seal_bytes VALUES (5, X'00FF', replace(hex(zeroblob(300000)), '00', char(233)) || char(0) || 'tail')")
    yield* db.run("CREATE TABLE nul_keys (id TEXT PRIMARY KEY, bytes BLOB) WITHOUT ROWID")
    yield* db.run("WITH RECURSIVE n(value) AS (SELECT 1 UNION ALL SELECT value + 1 FROM n WHERE value < 129) INSERT INTO nul_keys SELECT printf('%03d', value) || char(0) || 'tail', X'00FF' FROM n")
    let reads = 0
    const observe = (values: readonly unknown[]) => {
      const bytes = values.reduce<number>((total, value) => total + (typeof value === "object" && value !== null && "value" in value && typeof value.value === "string" ? Buffer.byteLength(value.value) : 0), 0)
      expect(bytes).toBeLessThanOrEqual(1024 * 1024)
      reads++
    }
    const execute = db.all.bind(db)
    db.all = <Row>(query: Parameters<typeof db.all>[0]) => {
      const result = execute<Row>(query)
      const run = result.execute
      result.execute = () => run().pipe(Effect.tap((rows) => Effect.sync(() => observe(rows))))
      return result
    }
    const get = db.get.bind(db)
    db.get = <Row>(query: Parameters<typeof db.get>[0]) => {
      const result = get<Row>(query)
      const run = result.execute
      result.execute = () => run().pipe(Effect.tap((value) => Effect.sync(() => observe([value]))))
      return result
    }
    yield* WsNative.prepareStage(db, "bytes-token")
    yield* WsNative.resumeStage(db, "bytes-token")
    expect(reads).toBeGreaterThan(0)
    const state = yield* db.get<{ archive_id: string }>("SELECT archive_id FROM _ws_stage")
    const archive = state && (yield* DatabaseWsArchive.read(db, state.archive_id))
    const table = archive && DatabaseWsArchive.sourceTable(archive, "seal_bytes")
    if (!table) throw new Error("Missing large-cell archive")
    const trigger = yield* db.get<{ sql: string }>(sql`SELECT sql FROM sqlite_schema WHERE name = ${`${table}_update_immutable`}`)
    if (!trigger) throw new Error("Missing archive guard")
    yield* db.run(sql`DROP TRIGGER ${DatabaseWsArchive.identifier(`${table}_update_immutable`)}`)
    yield* db.run(sql`UPDATE ${DatabaseWsArchive.identifier(table)} SET payload = CAST(substr(payload, 1, 599999) || X'FF' AS BLOB) WHERE id = 4`)
    yield* db.run(sql.raw(trigger.sql))
    expect((yield* WsNative.resumeStage(db, "bytes-token").pipe(Effect.exit))._tag).toBe("Failure")
  }).pipe(Effect.provide(sqliteLayer({ filename: ":memory:" })), Effect.provide(tempGlobalLayer), Effect.scoped))
})

test("owned validation proof avoids repeated validation but direct resume still verifies", async () => {
  await Effect.runPromise(Effect.gen(function* () {
    const db = yield* EffectDrizzleSqlite.makeWithDefaults()
    yield* setupWs(db, { directory: "/fixture" })
    let checks = 0
    const execute = db.all.bind(db)
    const dialect = new SQLiteDialect()
    db.all = (query) => {
      const text = typeof query === "string" ? query : dialect.sqlToQuery(query.getSQL()).sql
      if (text === "PRAGMA integrity_check") checks++
      return execute(query)
    }
    const transaction = db.transaction.bind(db)
    db.transaction = (callback, options) => transaction((tx) => {
      const all = tx.all.bind(tx)
      tx.all = (query) => {
        const text = typeof query === "string" ? query : dialect.sqlToQuery(query.getSQL()).sql
        if (text === "PRAGMA integrity_check") checks++
        return all(query)
      }
      return callback(tx)
    }, options)
    const initialize = yield* WsNative.validateStageSource(db)
    expect(checks).toBe(1)
    const checkpoint = yield* initialize(db, "proof-token")
    expect(checks).toBe(1)
    yield* checkpoint.resume
    expect(checks).toBe(2)
    yield* WsNative.resumeStage(db, "proof-token")
    expect(checks).toBe(3)
  }).pipe(Effect.provide(sqliteLayer({ filename: ":memory:" })), Effect.provide(tempGlobalLayer), Effect.scoped))
})

test("private stage retains WS prefix when later native encoding rejects", async () => {
  await using temp = await tmpdir()
  const filename = join(temp.path, "work.db")
  await Effect.runPromise(Effect.gen(function* () {
    const db = yield* EffectDrizzleSqlite.makeWithDefaults()
    yield* setupWs(db, { directory: temp.path, rich: true })
    yield* db.run("UPDATE message SET time_created = 'synthetic-not-a-clock' WHERE id = 'msg_legacy'")
    yield* WsNative.prepareStage(db, "test-token")
    expect((yield* WsNative.resumeStage(db, "test-token").pipe(Effect.exit))._tag).toBe("Failure")
    expect(yield* db.get(sql`SELECT id FROM session_message WHERE id = ${messageID}`)).toEqual({ id: messageID })
    expect(yield* db.get("SELECT phase FROM _ws_stage")).toEqual({ phase: "legacy" })
    yield* db.run("CREATE TRIGGER refuse_replay BEFORE INSERT ON session_message BEGIN SELECT RAISE(ABORT, 'replayed committed prefix'); END")
  }).pipe(Effect.provide(sqliteLayer({ filename })), Effect.provide(tempGlobalLayer), Effect.scoped))
  await Effect.runPromise(Effect.gen(function* () {
    const db = yield* EffectDrizzleSqlite.makeWithDefaults()
    yield* WsNative.prepareStage(db, "test-token")
    expect((yield* WsNative.resumeStage(db, "test-token").pipe(Effect.flip)).message).toContain("native-encode")
    expect(yield* db.get(sql`SELECT id FROM session_v2 WHERE id = ${sessionID}`)).toEqual({ id: sessionID })
  }).pipe(Effect.provide(sqliteLayer({ filename })), Effect.provide(tempGlobalLayer), Effect.scoped))
})

for (const legacy of [false, true]) {
  test(`one large ${legacy ? "legacy" : "WS"} session resumes after the last batch rolls back`, async () => {
    await using temp = await tmpdir()
    const filename = join(temp.path, "work.db")
    await Effect.runPromise(Effect.gen(function* () {
      const db = yield* EffectDrizzleSqlite.makeWithDefaults()
      yield* setupWs(db, { directory: temp.path, rich: legacy })
      if (legacy) {
        yield* db.run(`WITH RECURSIVE n(value) AS (SELECT 1 UNION ALL SELECT value + 1 FROM n WHERE value < 299)
          INSERT INTO message SELECT printf('msg_legacy_%04d', value), session_id, value + 1, value + 2, data FROM message, n WHERE id = 'msg_legacy'`)
      } else {
        yield* db.run("UPDATE session_message SET seq = 0")
        yield* db.run(`WITH RECURSIVE n(value) AS (SELECT 1 UNION ALL SELECT value + 1 FROM n WHERE value < 299)
          INSERT INTO session_message SELECT printf('msg_ws_%04d', value), session_id, type, value, time_created, time_updated, data FROM session_message, n WHERE seq = 0`)
        yield* db.run("UPDATE event_sequence SET seq = 299")
      }
      yield* WsNative.prepareStage(db, "batch-token")
      yield* db.run(sql`CREATE TRIGGER fail_late BEFORE INSERT ON session_message WHEN NEW.session_id = ${sql.raw(`'${legacy ? legacySessionID : sessionID}'`)} AND NEW.seq >= 270
        BEGIN SELECT RAISE(ABORT, 'late batch rejected'); END`)
      expect((yield* WsNative.resumeStage(db, "batch-token").pipe(Effect.exit))._tag).toBe("Failure")
      expect(yield* db.get(sql`SELECT count(*) AS count, max(seq) AS last FROM session_message WHERE session_id = ${legacy ? legacySessionID : sessionID}`)).toEqual({ count: 256, last: 255 })
      yield* db.run("DROP TRIGGER fail_late")
      yield* db.run(sql`CREATE TRIGGER refuse_replay BEFORE INSERT ON session_message WHEN NEW.session_id = ${sql.raw(`'${legacy ? legacySessionID : sessionID}'`)} AND NEW.seq <= 255
        BEGIN SELECT RAISE(ABORT, 'replayed committed prefix'); END`)
    }).pipe(Effect.provide(sqliteLayer({ filename })), Effect.provide(tempGlobalLayer), Effect.scoped))
    await Effect.runPromise(Effect.gen(function* () {
      const db = yield* EffectDrizzleSqlite.makeWithDefaults()
      yield* WsNative.prepareStage(db, "batch-token")
      yield* WsNative.resumeStage(db, "batch-token")
      expect(yield* db.get(sql`SELECT count(*) AS count FROM session_message WHERE session_id = ${legacy ? legacySessionID : sessionID}`)).toEqual({ count: 300 })
      expect(yield* db.get("SELECT phase FROM _ws_stage")).toEqual({ phase: "complete" })
      yield* WsNative.resumeStage(db, "batch-token")
    }).pipe(Effect.provide(sqliteLayer({ filename })), Effect.provide(tempGlobalLayer), Effect.scoped))
  })
}

test("file retry retains the same stage and leaves an invalid source untouched", async () => {
  await using temp = await tmpdir()
  const filename = join(temp.path, "source.db")
  await Effect.runPromise(Effect.gen(function* () {
    const db = yield* EffectDrizzleSqlite.makeWithDefaults()
    yield* setupWs(db, { directory: temp.path, rich: true })
    yield* db.run("UPDATE message SET time_created = 'synthetic-not-a-clock' WHERE id = 'msg_legacy'")
  }).pipe(Effect.provide(sqliteLayer({ filename })), Effect.scoped))
  const before = await DatabaseArtifact.digest(filename).pipe(Effect.runPromise)
  const admit = () => WsNativeFile.admit(filename).pipe(Effect.provide(tempGlobalLayer), Effect.scoped, Effect.runPromise)
  await expect(admit()).rejects.toThrow("native-encode")
  const marker = await WsNativeStage.read(filename).pipe(Effect.runPromise)
  await expect(admit()).rejects.toThrow("native-encode")
  expect(await WsNativeStage.read(filename).pipe(Effect.runPromise)).toEqual(marker)
  expect(await DatabaseArtifact.digest(filename).pipe(Effect.runPromise)).toEqual(before)
})

test("ready stage activates once and final-commit recovery does not replay messages", async () => {
  await using temp = await tmpdir()
  const filename = join(temp.path, "source.db")
  await Effect.runPromise(Effect.gen(function* () {
    const db = yield* EffectDrizzleSqlite.makeWithDefaults()
    yield* setupWs(db, { directory: temp.path })
  }).pipe(Effect.provide(sqliteLayer({ filename })), Effect.scoped))
  const before = await DatabaseArtifact.digest(filename).pipe(Effect.runPromise)
  const prepare = () => WsNativeStage.prepare(filename, filename).pipe(Effect.provide(tempGlobalLayer), Effect.scoped, Effect.runPromise)
  await prepare()
  const marker = await WsNativeStage.read(filename).pipe(Effect.runPromise)
  await DatabaseArtifact.replaceWithText(`${filename}.ws-stage.json`, JSON.stringify({ ...marker, phase: "converting", ready: undefined })).pipe(Effect.runPromise)
  await Effect.runPromise(Effect.gen(function* () {
    const db = yield* EffectDrizzleSqlite.makeWithDefaults()
    yield* db.run("CREATE TRIGGER refuse_replay BEFORE INSERT ON session_message BEGIN SELECT RAISE(ABORT, 'replayed committed prefix'); END")
  }).pipe(Effect.provide(sqliteLayer({ filename: join(temp.path, marker.work) })), Effect.scoped))
  const ready = await prepare()
  expect(await prepare()).toEqual(ready)
  expect(await DatabaseArtifact.digest(filename).pipe(Effect.runPromise)).toEqual(before)
  await WsNativeFile.admit(filename).pipe(Effect.provide(tempGlobalLayer), Effect.scoped, Effect.runPromise)
  await WsNativeFile.admit(filename).pipe(Effect.provide(tempGlobalLayer), Effect.scoped, Effect.runPromise)
  expect(await Bun.file(`${filename}.ws-stage.json`).exists()).toBe(false)
  await Effect.runPromise(Effect.gen(function* () {
    const db = yield* EffectDrizzleSqlite.makeWithDefaults()
    expect(yield* db.get("SELECT count(*) AS count FROM session_message")).toEqual({ count: 1 })
  }).pipe(Effect.provide(sqliteLayer({ filename })), Effect.scoped))
})

for (const mutation of ["cursor", "prefix", "archive", "header", "phase"]) {
  test(`rejects a corrupt ${mutation} in private progress`, async () => {
    await using temp = await tmpdir()
    await Effect.runPromise(Effect.gen(function* () {
      const db = yield* EffectDrizzleSqlite.makeWithDefaults()
      yield* setupWs(db, { directory: temp.path, rich: true })
      yield* db.run("CREATE TABLE exact_values (bytes BLOB, exact INTEGER)")
      yield* db.run("INSERT INTO exact_values (rowid, bytes, exact) VALUES (9223372036854775806, X'00FF', 9223372036854775807)")
      yield* db.run("UPDATE message SET time_created = 'synthetic-not-a-clock' WHERE id = 'msg_legacy'")
      yield* WsNative.prepareStage(db, "seal-token")
      expect((yield* WsNative.resumeStage(db, "seal-token").pipe(Effect.exit))._tag).toBe("Failure")
      yield* WsNative.prepareStage(db, "seal-token")
      if (mutation === "cursor") yield* db.run("UPDATE _ws_stage SET session_id = 'wrong', seq = 4, message_id = 'wrong'")
      if (mutation === "prefix") yield* db.run("UPDATE session_message_part SET data = '{}' WHERE position = 0")
      if (mutation === "header") yield* db.run("UPDATE project SET name = 'corrupt'")
      if (mutation === "phase") yield* db.run("UPDATE _ws_stage SET phase = 'complete'")
      if (mutation === "archive") {
        const state = yield* db.get<{ archive_id: string }>("SELECT archive_id FROM _ws_stage")
        const archive = state && (yield* DatabaseWsArchive.read(db, state.archive_id))
        const table = archive && DatabaseWsArchive.sourceTable(archive, "exact_values")
        if (!table) throw new Error("Missing exact archive fixture")
        const trigger = yield* db.get<{ sql: string }>(sql`SELECT sql FROM sqlite_schema WHERE name = ${`${table}_update_immutable`}`)
        if (!trigger) throw new Error("Missing archive guard")
        yield* db.run(sql`DROP TRIGGER ${DatabaseWsArchive.identifier(`${table}_update_immutable`)}`)
        yield* db.run(sql`UPDATE ${DatabaseWsArchive.identifier(table)} SET exact = 9223372036854775806`)
        yield* db.run(sql.raw(trigger.sql))
      }
      expect((yield* WsNative.resumeStage(db, "seal-token").pipe(Effect.exit))._tag).toBe("Failure")
    }).pipe(Effect.provide(sqliteLayer({ filename: ":memory:" })), Effect.provide(tempGlobalLayer), Effect.scoped))
  })
}

for (const wal of [false, true]) {
  test(`changed ${wal ? "WAL" : "main"} content cannot reuse a ready stage`, async () => {
    await using temp = await tmpdir()
    const filename = join(temp.path, "source.db")
    await Effect.runPromise(Effect.gen(function* () {
      const db = yield* EffectDrizzleSqlite.makeWithDefaults()
      yield* setupWs(db, { directory: temp.path })
      if (wal) yield* db.run("PRAGMA journal_mode = WAL")
      yield* WsNativeStage.prepare(filename, filename)
      const marker = yield* WsNativeStage.read(filename)
      const main = yield* DatabaseArtifact.digest(filename)
      yield* db.run("UPDATE session SET title = 'changed source'")
      if (wal) expect(yield* DatabaseArtifact.digest(filename)).toEqual(main)
      const before = yield* WsNativeStage.identity(filename)
      expect((yield* WsNativeStage.prepare(filename, filename).pipe(Effect.exit))._tag).toBe("Failure")
      expect(yield* WsNativeStage.identity(filename)).toEqual(before)
      expect(yield* WsNativeStage.read(filename)).toEqual(marker)
    }).pipe(Effect.provide(sqliteLayer({ filename })), Effect.provide(tempGlobalLayer), Effect.scoped))
  })
}

for (const interruption of ["handoff", "preimage", "rename"]) {
  test(`recovers the ${interruption} boundary without another conversion`, async () => {
    await using temp = await tmpdir()
    const filename = join(temp.path, "source.db")
    await Effect.runPromise(Effect.gen(function* () {
      const db = yield* EffectDrizzleSqlite.makeWithDefaults()
      yield* setupWs(db, { directory: temp.path })
      yield* db.run("PRAGMA journal_mode = WAL")
    }).pipe(Effect.provide(sqliteLayer({ filename })), Effect.scoped))
    const ready = await WsNativeStage.prepare(filename, filename).pipe(Effect.provide(tempGlobalLayer), Effect.scoped, Effect.runPromise)
    await WsNativeStage.handoff(filename).pipe(Effect.runPromise)
    if (interruption === "preimage") await Effect.runPromise(Effect.gen(function* () {
      const db = yield* EffectDrizzleSqlite.makeWithDefaults()
      yield* db.run("PRAGMA wal_checkpoint(TRUNCATE)")
      yield* db.run("PRAGMA journal_mode = DELETE")
    }).pipe(Effect.provide(sqliteLayer({ filename })), Effect.scoped))
    if (interruption === "rename") await DatabaseArtifact.replace(join(temp.path, ready.name), filename).pipe(Effect.runPromise)
    await WsNativeFile.admit(filename).pipe(Effect.provide(tempGlobalLayer), Effect.scoped, Effect.runPromise)
    expect(await Bun.file(`${filename}.ws-stage.json`).exists()).toBe(false)
    await Effect.runPromise(Effect.gen(function* () {
      const db = yield* EffectDrizzleSqlite.makeWithDefaults()
      expect(yield* db.get("SELECT count(*) AS count FROM session_message")).toEqual({ count: 1 })
      expect(yield* db.get("SELECT 1 FROM sqlite_schema WHERE name = '_ws_stage'")).toBeUndefined()
    }).pipe(Effect.provide(sqliteLayer({ filename })), Effect.scoped))
  })
}

test("unsafe marker paths are rejected without touching the source", async () => {
  await using temp = await tmpdir()
  const filename = join(temp.path, "source.db")
  await Effect.runPromise(Effect.gen(function* () {
    const db = yield* EffectDrizzleSqlite.makeWithDefaults()
    yield* setupWs(db, { directory: temp.path })
  }).pipe(Effect.provide(sqliteLayer({ filename })), Effect.scoped))
  await WsNativeStage.prepare(filename, filename).pipe(Effect.provide(tempGlobalLayer), Effect.scoped, Effect.runPromise)
  const marker = await WsNativeStage.read(filename).pipe(Effect.runPromise)
  const before = await DatabaseArtifact.digest(filename).pipe(Effect.runPromise)
  await DatabaseArtifact.replaceWithText(`${filename}.ws-stage.json`, JSON.stringify({ ...marker, work: "../outside.db" })).pipe(Effect.runPromise)
  await expect(WsNativeFile.admit(filename).pipe(Effect.provide(tempGlobalLayer), Effect.scoped, Effect.runPromise)).rejects.toThrow("Unsafe WS stage marker")
  expect(await DatabaseArtifact.digest(filename).pipe(Effect.runPromise)).toEqual(before)
})

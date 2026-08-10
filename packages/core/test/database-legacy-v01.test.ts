import { describe, expect, test } from "bun:test"
import { Database as SqliteDatabase } from "bun:sqlite"
import { stat } from "node:fs/promises"
import path from "node:path"
import { DatabaseArtifact } from "@opencode-ai/core/database/artifact"
import { DatabaseLegacyV01 } from "@opencode-ai/core/database/legacy-v01"
import { DatabaseMigration } from "@opencode-ai/core/database/migration"
import { Effect, Schema } from "effect"
import {
  assistantContent,
  assistantEnvelope,
  createCurrentDatabase,
  inspectMessages,
  userEnvelope,
} from "./fixture/database-transfer"
import { tmpdir } from "./fixture/tmpdir"

const decodeJson = Schema.decodeUnknownSync(Schema.UnknownFromJsonString)

describe("DatabaseLegacyV01", () => {
  test("exports a lossless legacy-v01 snapshot and manifest without changing the source", async () => {
    await using tmp = await tmpdir()
    const source = path.join(tmp.path, "current.sqlite")
    const destination = path.join(tmp.path, "legacy.sqlite")
    await createCurrentDatabase(source)
    const before = await DatabaseArtifact.digest(source).pipe(Effect.runPromise)

    await DatabaseLegacyV01.exportDatabase({ source, destination }).pipe(Effect.runPromise)

    const after = await DatabaseArtifact.digest(source).pipe(Effect.runPromise)
    const manifest = Schema.decodeUnknownSync(DatabaseLegacyV01.Manifest, { onExcessProperty: "error" })(
      decodeJson(await Bun.file(`${destination}.manifest.json`).text()),
    )
    using legacy = new SqliteDatabase(destination, { readonly: true })
    const messages = legacy
      .query<
        { readonly id: string; readonly type: string; readonly data: string },
        []
      >("SELECT id, type, data FROM session_message ORDER BY seq")
      .all()

    expect(after).toEqual(before)
    expect(manifest).toEqual({
      format: "opencode.database",
      version: "legacy-v01",
      ...(await DatabaseArtifact.digest(destination).pipe(Effect.runPromise)),
    })
    expect(legacy.query("SELECT name FROM sqlite_master WHERE name = 'session_message_part'").get()).toBeNull()
    expect(legacy.query<{ readonly id: string }, []>("SELECT id FROM migration ORDER BY rowid").all()).toEqual(
      DatabaseMigration.legacyBaselineMigrationIDs.map((id) => ({ id })),
    )
    expect(decodeJson(messages[0]?.data ?? "")).toEqual(userEnvelope)
    expect(decodeJson(messages[1]?.data ?? "")).toEqual({ ...assistantEnvelope, content: assistantContent })
    expect(decodeJson(messages[2]?.data ?? "")).toEqual({ ...assistantEnvelope, time: { created: 1 }, content: [] })
  })

  test("exports committed writes from a live WAL database", async () => {
    await using tmp = await tmpdir()
    const source = path.join(tmp.path, "current.sqlite")
    const destination = path.join(tmp.path, "legacy.sqlite")
    const committed = { ...userEnvelope, text: "committed in WAL" }
    await createCurrentDatabase(source)
    using live = new SqliteDatabase(source)
    live.run("PRAGMA journal_mode = WAL")
    live.run("PRAGMA wal_autocheckpoint = 0")
    live.query("UPDATE session_message SET data = ? WHERE id = 'msg_user'").run(JSON.stringify(committed))
    expect((await stat(`${source}-wal`)).size).toBeGreaterThan(0)

    await DatabaseLegacyV01.exportDatabase({ source, destination }).pipe(Effect.runPromise)

    expect(await Bun.file(`${destination}.manifest.json`).exists()).toBe(true)
    expect(await Bun.file(`${destination}-wal`).exists()).toBe(false)
    expect(await Bun.file(`${destination}-shm`).exists()).toBe(false)
    using legacy = new SqliteDatabase(destination, { readonly: true })
    expect(
      decodeJson(
        legacy.query<{ readonly data: string }, []>("SELECT data FROM session_message WHERE id = 'msg_user'").get()
          ?.data ?? "",
      ),
    ).toEqual(committed)
  })

  test("stages a verified inverse conversion without changing the legacy source or current target", async () => {
    await using tmp = await tmpdir()
    const current = path.join(tmp.path, "current.sqlite")
    const legacy = path.join(tmp.path, "legacy.sqlite")
    const target = path.join(tmp.path, "target.sqlite")
    await createCurrentDatabase(current)
    await createCurrentDatabase(target)
    await DatabaseLegacyV01.exportDatabase({ source: current, destination: legacy }).pipe(Effect.runPromise)
    const sourceBefore = await DatabaseArtifact.digest(legacy).pipe(Effect.runPromise)
    const targetBefore = await DatabaseArtifact.digest(target).pipe(Effect.runPromise)

    const result = await DatabaseLegacyV01.stageImport({ source: legacy, target }).pipe(Effect.runPromise)

    expect(await DatabaseArtifact.digest(legacy).pipe(Effect.runPromise)).toEqual(sourceBefore)
    expect(await DatabaseArtifact.digest(target).pipe(Effect.runPromise)).toEqual(targetBefore)
    const marker = Schema.decodeUnknownSync(DatabaseLegacyV01.PendingReplacement, { onExcessProperty: "error" })(
      decodeJson(await Bun.file(result.marker).text()),
    )
    expect(marker).toEqual({
      format: "opencode.database.replacement",
      version: 1,
      phase: "prepared",
      staged: {
        name: path.basename(result.staged),
        ...(await DatabaseArtifact.digest(result.staged).pipe(Effect.runPromise)),
      },
    })
    const staged = inspectMessages(result.staged)
    expect(staged.quickCheck).toBe("ok")
    expect(staged.foreignKeys).toEqual([])
    expect(staged.journal).toEqual([{ id: "v01_baseline" }, { id: "v02_session_message_part" }])
    expect(staged.parts.map((part) => ({ ...part, data: decodeJson(part.data) }))).toEqual(
      assistantContent.map((part, position) => {
        const { id, type, ...data } = part
        return { message_id: "msg_assistant", position, id, type, data }
      }),
    )
    expect(decodeJson(staged.rows[0]?.data ?? "")).toEqual(userEnvelope)
    expect(decodeJson(staged.rows[1]?.data ?? "")).toEqual(assistantEnvelope)
  })

  test("preserves opaque JSON value lexemes through export and import", async () => {
    await using tmp = await tmpdir()
    const current = path.join(tmp.path, "current.sqlite")
    const legacy = path.join(tmp.path, "legacy.sqlite")
    const target = path.join(tmp.path, "target.sqlite")
    await createCurrentDatabase(current)
    await createCurrentDatabase(target)
    const lexemes = ["9007199254740993", "-9007199254740993", "1.2300", "6.022e+23", "\\u96ea"]
    const raw =
      '{"name":"read","state":{"status":"completed","input":{"large":9007199254740993,"negative":-9007199254740993,"decimal":1.2300,"exponent":6.022e+23,"escaped":"\\u96ea"},"structured":{"value":9007199254740993},"content":[]},"time":{"created":1,"completed":2}}'
    using database = new SqliteDatabase(current)
    database.run("UPDATE session_message_part SET data = ? WHERE id = 'tool-call'", [raw])
    database.run("PRAGMA wal_checkpoint(TRUNCATE)")
    database.close()

    await DatabaseLegacyV01.exportDatabase({ source: current, destination: legacy }).pipe(Effect.runPromise)
    const result = await DatabaseLegacyV01.stageImport({ source: legacy, target }).pipe(Effect.runPromise)

    using exported = new SqliteDatabase(legacy, { readonly: true })
    const legacyRaw = exported
      .query<{ readonly data: string }, []>("SELECT data FROM session_message WHERE id = 'msg_assistant'")
      .get()?.data
    const stagedRaw = inspectMessages(result.staged).parts.find((part) => part.id === "tool-call")?.data
    for (const lexeme of lexemes) {
      expect(legacyRaw).toContain(lexeme)
      expect(stagedRaw).toContain(lexeme)
    }
  })
})

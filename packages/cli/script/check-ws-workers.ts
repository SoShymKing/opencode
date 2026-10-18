#!/usr/bin/env bun
import assert from "node:assert/strict"
import { copyFile, mkdir, mkdtemp, readdir, rm } from "node:fs/promises"
import { tmpdir } from "node:os"
import path from "node:path"
import { Database } from "bun:sqlite"
import { Effect } from "effect"
import { Hash } from "@opencode/util/hash"
import { EffectDrizzleSqlite } from "../../core/src/database/drizzle"
import { sqliteLayer } from "../../core/src/database/sqlite.bun"
import { setupWs, sessionID, legacySessionID, messageData } from "../../core/test/fixture/database-ws"
import { isolatedEnv } from "../test/fixture/environment"

const input = process.argv[2]
assert(input, "Usage: bun run script/check-ws-workers.ts <executable>")
const root = await mkdtemp(path.join(tmpdir(), "opencode-ws-workers-"))
const count = 1024
const payload = "worker fixture ".repeat(1024)
const watermark = count * 2 + 99
try {
  const bin = path.join(root, "bin")
  await mkdir(bin)
  const executable = path.join(bin, process.platform === "win32" ? "opencode.exe" : "opencode")
  await copyFile(path.resolve(input), executable)
  for (const malformed of [false, true]) {
    const home = path.join(root, malformed ? "malformed" : "valid")
    await mkdir(home)
    const target = path.join(home, "fixture.db")
    await Effect.gen(function* () {
      const db = yield* EffectDrizzleSqlite.makeWithDefaults()
      yield* setupWs(db, { directory: home })
    }).pipe(Effect.provide(sqliteLayer({ filename: target })), Effect.scoped, Effect.runPromise)
    {
      using db = new Database(target)
      db.transaction(() => {
        const projectID = Hash.fast(`directory:${bin}`)
        db.run("PRAGMA defer_foreign_keys = ON")
        db.run("UPDATE project SET id = ?", [projectID])
        for (const table of ["session", "workspace", "permission", "project_directory"])
          db.run(`UPDATE ${table} SET project_id = ?`, [projectID])
        db.run(`INSERT INTO session (id, project_id, slug, directory, title, version, time_created, time_updated)
          VALUES (?, ?, 'legacy-session', ?, 'Legacy source session', 'fixture', 1, ?)`, [legacySessionID, projectID, bin, count + 2])
        const insert = db.prepare("INSERT INTO session_message VALUES (?, ?, 'assistant', ?, 1, 16, ?)")
        const event = db.prepare("INSERT INTO event VALUES (?, ?, ?, 'session.next.text.ended.1', ?)")
        const message = db.prepare("INSERT INTO message VALUES (?, ?, ?, ?, ?)")
        const part = db.prepare("INSERT INTO part VALUES (?, ?, ?, ?, ?, ?)")
        for (let index = 0; index < count; index++) {
          insert.run(`msg_worker_${index}`, sessionID, 100 + index, JSON.stringify({
            agent: "build", model: { providerID: "test", id: "model" }, time: { created: 1 },
            content: [{ id: "text-1", type: "text", text: `${index}:${payload}` }],
          }))
          event.run(`evt_worker_${index}`, sessionID, count + 100 + index, JSON.stringify({
            sessionID, timestamp: 4, assistantMessageID: "msg_migration", textID: "text-1", text: `${index}:${payload}`,
          }))
          message.run(`msg_legacy_${index}`, legacySessionID, index + 1, index + 1, JSON.stringify({
            role: "user", time: { created: index + 1 }, agent: "build", model: { providerID: "test", modelID: "model" },
          }))
          part.run(`prt_legacy_${index}`, `msg_legacy_${index}`, legacySessionID, index + 1, index + 1,
            JSON.stringify({ type: "text", text: `legacy-${index}:${payload}` }))
        }
        message.run("msg_legacy_assistant", legacySessionID, count + 1, count + 2, JSON.stringify({
          role: "assistant", time: { created: count + 1, completed: count + 2 }, parentID: `msg_legacy_${count - 1}`,
          modelID: "model", providerID: "test", mode: "build", agent: "build", path: { cwd: bin, root: bin },
          cost: 0, tokens: { input: 2, output: 3, reasoning: 0, cache: { read: 0, write: 0 } }, finish: "stop",
        }))
        part.run("prt_legacy_answer", "msg_legacy_assistant", legacySessionID, count + 1, count + 2,
          JSON.stringify({ type: "text", text: "Legacy answer" }))
        part.run("prt_legacy_tool", "msg_legacy_assistant", legacySessionID, count + 1, count + 2, JSON.stringify({
          type: "tool", callID: "legacy-call", tool: "fixture-tool",
          state: { status: "completed", input: {}, output: "Legacy tool result", title: "Fixture", metadata: {}, time: { start: count + 1, end: count + 2 } },
        }))
        db.run("UPDATE event_sequence SET seq = ?", [watermark])
        if (malformed) db.run("UPDATE event SET data = '{}' WHERE id = ?", [`evt_worker_${count - 1}`])
      })()
    }
    assert(Bun.file(target).size < 100 * 1024 * 1024)
    const before = Bun.hash(await Bun.file(target).arrayBuffer())
    const child = Bun.spawn([executable, "session", "list", "--standalone", "--format", "json"], {
      cwd: bin,
      env: isolatedEnv(home, {
        USERPROFILE: home, TEMP: home, TMP: home, TMPDIR: home,
        OPENCODE_DB: target,
        OPENCODE_DISABLE_AUTOUPDATE: "true",
        OPENCODE_DISABLE_PROJECT_CONFIG: "true",
        OPENCODE_CONFIG_PROJECT_DISABLE: "true",
        OPENCODE_DISABLE_CLAUDE_CODE: "true",
        OPENCODE_CONFIG: undefined,
        OPENCODE_SERVER: undefined,
      }),
      stdin: "ignore", stdout: "pipe", stderr: "pipe",
    })
    const timer = setTimeout(() => child.kill(), 120_000)
    const result = await Promise.all([child.exited, new Response(child.stdout).text(), new Response(child.stderr).text()])
      .finally(async () => {
        clearTimeout(timer)
        if (child.exitCode === null) child.kill()
        await child.exited
      })
    if (malformed) {
      assert.notEqual(result[0], 0, result[2])
      assert.equal(Bun.hash(await Bun.file(target).arrayBuffer()), before)
      assert.equal(await Bun.file(`${target}.replacement.json`).exists(), false)
      console.log("PASS malformed source rejected without changing source")
      continue
    }
    assert.equal(result[0], 0, result[2])
    const listed: unknown = JSON.parse(result[1])
    assert(Array.isArray(listed) && listed.some((item) => item.id === sessionID), result[1])
    assert(listed.some((item) => item.id === legacySessionID), result[1])
    using db = new Database(target, { readonly: true })
    assert.deepEqual(db.query("PRAGMA quick_check").all(), [{ quick_check: "ok" }])
    assert.deepEqual(db.query("PRAGMA foreign_key_check").all(), [])
    assert.deepEqual(db.query("SELECT id FROM session_v2 ORDER BY id").all(), [{ id: legacySessionID }, { id: sessionID }])
    assert.deepEqual(db.query("SELECT count(*) AS count FROM session_message").get(), { count: count * 2 + 2 })
    assert.deepEqual(db.query("SELECT json_extract(data, '$.text') AS text FROM session_message_part WHERE message_id = 'msg_worker_1023'").all(), [{ text: `1023:${payload}` }])
    assert.deepEqual(db.query("SELECT id FROM session_message WHERE session_id = ? ORDER BY seq").all(legacySessionID),
      [...Array.from({ length: count }, (_, index) => ({ id: `msg_legacy_${index}` })), { id: "msg_legacy_assistant" }])
    for (const index of [0, 127, 128, count - 1])
      assert.deepEqual(db.query("SELECT json_extract(data, '$.text') AS text FROM session_message WHERE id = ?").get(`msg_legacy_${index}`), { text: `legacy-${index}:${payload}` })
    assert.deepEqual(db.query("SELECT json_extract(data, '$.text') AS text FROM session_message_part WHERE message_id = 'msg_legacy_assistant' AND type = 'text'").all(), [{ text: "Legacy answer" }])
    assert.deepEqual(db.query("SELECT id, json_extract(data, '$.state.content[0].text') AS text FROM session_message_part WHERE message_id = 'msg_legacy_assistant' AND type = 'tool'").all(), [{ id: "legacy-call", text: "Legacy tool result" }])
    assert.deepEqual(db.query("SELECT aggregate_id, seq FROM event_sequence ORDER BY aggregate_id").all(), [
      { aggregate_id: legacySessionID, seq: count }, { aggregate_id: sessionID, seq: watermark },
    ])
    assert.deepEqual(db.query("SELECT aggregate_id, through_seq FROM _ws_import_root ORDER BY aggregate_id").all(), [
      { aggregate_id: legacySessionID, through_seq: count }, { aggregate_id: sessionID, through_seq: watermark },
    ])
    assert.deepEqual(db.query("SELECT count(*) AS count FROM event").get(), { count: 0 })
    const retained = db.query<{ name: string }, []>(`SELECT json_extract(value, '$.retained') AS name
      FROM _ws_archive, json_each(manifest, '$.tables') WHERE json_extract(value, '$.name') = 'session_message'`).get()
    assert(retained && /^_ws_source_[0-9a-f]{32}_\d+$/.test(retained.name))
    assert.deepEqual(db.query(`SELECT data FROM "${retained.name}" WHERE id = 'msg_migration'`).get(), { data: messageData })
    assert.deepEqual(db.query(`SELECT count(*) AS count FROM "${retained.name}"`).get(), { count: count + 1 })
    const backups = (await readdir(home)).filter((name) => name.includes("replacement-backup"))
    assert.equal(backups.length, 1)
    const backup = backups[0]
    assert(backup)
    using source = new Database(path.join(home, backup), { readonly: true })
    assert.deepEqual(source.query("SELECT count(*) AS count FROM session_message").get(), { count: count + 1 })
    for (const [table, rows] of [["session_message", count + 1], ["message", count + 1], ["part", count + 2], ["event", count]] as const) {
      const archived = db.query<{ name: string }, [string]>(`SELECT json_extract(value, '$.retained') AS name
        FROM _ws_archive, json_each(manifest, '$.tables') WHERE json_extract(value, '$.name') = ?`).get(table)
      assert(archived && /^_ws_source_[0-9a-f]{32}_\d+$/.test(archived.name))
      assert.deepEqual(db.query(`SELECT count(*) AS count FROM "${archived.name}"`).get(), { count: rows })
      assert.deepEqual(source.query(`SELECT count(*) AS count FROM "${table}"`).get(), { count: rows })
      assert.deepEqual(db.query(`SELECT * FROM "${archived.name}" ORDER BY id`).all(), source.query(`SELECT * FROM "${table}" ORDER BY id`).all())
    }
    assert.equal(await Bun.file(`${target}.replacement.json`).exists(), false)
    console.log(`PASS standalone executable converted ${count * 2 + 2} messages and archived ${count} events; native content, archive, backup verified`)
  }
} finally {
  await rm(root, { recursive: true, force: true })
}

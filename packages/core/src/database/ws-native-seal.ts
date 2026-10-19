export * as WsNativeSeal from "./ws-native-seal.js"

import { createHash, type Hash } from "node:crypto"
import { sql, type SQL } from "drizzle-orm"
import { Effect } from "effect"
import type { EffectDrizzleSqlite } from "./drizzle.js"
import { DatabaseWsArchive } from "./ws-archive.js"
import { WsNativeScan } from "./ws-native-scan.js"

type Database = EffectDrizzleSqlite.EffectSQLiteDatabase
const identifier = DatabaseWsArchive.identifier
const field = (name: string) => sql`s.${identifier(name)}`
type Selection = { readonly table: string; readonly fields: readonly string[]; readonly where: SQL }

export function messages(db: Database, ids: readonly string[], previous: string) {
  return Effect.gen(function* () {
    let prefix = previous
    const parent = ["id", "session_id", "type", "seq", "time_created", "time_updated", "data"]
    const parts = ["message_id", "position", "id", "type", "data"]
    for (let offset = 0; offset < ids.length; offset += WsNativeScan.rowLimit) {
      const page = ids.slice(offset, offset + WsNativeScan.rowLimit)
      const sizes = new Map((yield* db.all<{ id: string; bytes: number }>(sql`SELECT m.id,
        (SELECT ${size(parent)} FROM session_message s WHERE s.id = m.id) +
        (SELECT COALESCE(SUM(${size(parts)}), 0) FROM session_message_part s WHERE s.message_id = m.id AND s.position > -1) AS bytes
        FROM session_message m WHERE m.id IN (${sql.join(page.map((id) => sql`${id}`), sql`, `)})`)).map((item) => [item.id, item]))
      const ordered: { id: string; bytes: number }[] = []
      for (const id of page) {
        const item = sizes.get(id)
        if (!item) return yield* Effect.fail(new DatabaseWsArchive.ArchiveError("Missing stage seal row"))
        ordered.push(item)
      }
      for (const group of admit(ordered)) {
        if (group[0].bytes > WsNativeScan.byteLimit) {
          prefix = yield* message(db, group[0].id, prefix)
          continue
        }
        const values = new Map((yield* db.all<{ id: string; parent: string; parts: string }>(sql`SELECT m.id,
          (SELECT ${encoded(parent)} FROM session_message s WHERE s.id = m.id) AS parent,
          (SELECT COALESCE(GROUP_CONCAT(value, ''), '') FROM (
            SELECT ${encoded(parts)} AS value FROM session_message_part s WHERE s.message_id = m.id AND s.position > -1 ORDER BY position
          )) AS parts FROM session_message m WHERE m.id IN (${sql.join(group.map((item) => sql`${item.id}`), sql`, `)})`)).map((item) => [item.id, item]))
        for (const item of group) {
          const value = values.get(item.id)
          if (!value) return yield* Effect.fail(new DatabaseWsArchive.ArchiveError("Missing stage seal row"))
          prefix = createHash("sha256").update(prefix).update(value.parent).update(value.parts).digest("hex")
        }
      }
    }
    return prefix
  })
}

export function message(db: Database, id: string, previous: string) {
  return Effect.gen(function* () {
    const hash = createHash("sha256").update(previous)
    const parent = { table: "session_message", fields: ["id", "session_id", "type", "seq", "time_created", "time_updated", "data"], where: sql`s.id = ${id}` }
    yield* row(db, parent, hash)
    const fields = ["message_id", "position", "id", "type", "data"]
    const parts = yield* db.get<{ value: string | null }>(sql`SELECT CASE WHEN COALESCE(SUM(${size(fields)}), 0) <= ${WsNativeScan.byteLimit}
      THEN (SELECT COALESCE(GROUP_CONCAT(value, ''), '') FROM (
        SELECT ${encoded(fields)} AS value FROM session_message_part s WHERE message_id = ${id} AND position > -1 ORDER BY position
      )) ELSE NULL END AS value FROM session_message_part s WHERE message_id = ${id} AND position > -1`)
    if (parts && parts.value !== null) return hash.update(parts.value).digest("hex")
    let position = -1
    while (true) {
      const candidates = yield* db.all<{ position: number; bytes: number }>(sql`SELECT position, ${size(fields)} AS bytes
        FROM session_message_part s WHERE message_id = ${id} AND position > ${position} ORDER BY position LIMIT ${WsNativeScan.rowLimit}`)
      if (!candidates.length) break
      for (const group of admit(candidates)) {
        const where = sql`s.message_id = ${id} AND s.position BETWEEN ${group[0].position} AND ${group[group.length - 1].position}`
        if (group[0].bytes > WsNativeScan.byteLimit) yield* chunks(db, { table: "session_message_part", fields, where }, hash)
        else for (const item of yield* db.all<{ value: string }>(sql`SELECT ${encoded(fields)} AS value FROM session_message_part s WHERE ${where} ORDER BY position`)) hash.update(item.value)
      }
      position = candidates[candidates.length - 1].position
    }
    return hash.digest("hex")
  })
}

export function archive(db: Database, manifest: DatabaseWsArchive.Archive) {
  return Effect.gen(function* () {
    const hash = createHash("sha256").update(JSON.stringify(manifest))
    const tables = yield* db.all<{ name: string }>(sql`SELECT name FROM sqlite_schema WHERE type = 'table'
      AND name NOT IN ('_ws_stage', 'session_message', 'session_message_part') ORDER BY name`)
    for (const table of tables) {
      hash.update(table.name)
      hash.update(JSON.stringify(yield* db.all(sql`SELECT type, name, sql FROM sqlite_schema WHERE tbl_name = ${table.name} ORDER BY type, name`)))
      const columns = yield* db.all<{ name: string; pk: number }>(sql`SELECT name, pk FROM pragma_table_info(${table.name})`)
      const layout = yield* db.get<{ wr: number }>(sql`SELECT wr FROM pragma_table_list WHERE name = ${table.name} AND schema = 'main'`)
      const rowid = layout?.wr === 0 ? ["rowid", "_rowid_", "oid"].find((name) => !columns.some((column) => column.name.toLowerCase() === name)) : undefined
      const keys = rowid ? [rowid] : columns.filter((column) => column.pk).sort((left, right) => left.pk - right.pk).map((column) => column.name)
      if (!keys.length) return yield* Effect.fail(new DatabaseWsArchive.ArchiveError("Stage archive has no exact scan key"))
      const fields = [...(rowid ? [rowid] : []), ...columns.map((column) => column.name)]
      const names = keys.map((_, index) => identifier(`key_${index}`))
      const tuple = sql`(${sql.join(keys.map(field), sql`, `)})`
      const projected = sql.join(keys.map((key, index) => sql`${field(key)} AS ${names[index]}`), sql`, `)
      yield* db.run(sql`CREATE TEMP TABLE _ws_seal_cursor AS SELECT ${projected} FROM ${identifier(table.name)} s WHERE 0`)
      yield* Effect.gen(function* () {
        yield* db.run(sql`CREATE TEMP TABLE _ws_seal_page AS SELECT * FROM _ws_seal_cursor`)
        let started = false
        while (true) {
          yield* db.run(sql`INSERT INTO _ws_seal_page SELECT ${sql.join(keys.map(field), sql`, `)} FROM ${identifier(table.name)} s
            WHERE ${started ? sql`${tuple} > (SELECT ${sql.join(names, sql`, `)} FROM _ws_seal_cursor)` : sql`1`}
            ORDER BY ${sql.join(keys.map(field), sql`, `)} LIMIT ${WsNativeScan.rowLimit}`)
          const join = sql`${tuple} = (${sql.join(names.map((name) => sql`p.${name}`), sql`, `)})`
          const candidates = yield* db.all<{ ordinal: number; bytes: number }>(sql`SELECT p.rowid AS ordinal, ${size(fields)} AS bytes
            FROM _ws_seal_page p CROSS JOIN ${identifier(table.name)} s WHERE ${join} ORDER BY p.rowid`)
          if (!candidates.length) break
          for (const group of admit(candidates)) {
            if (group[0].bytes > WsNativeScan.byteLimit) {
              yield* chunks(db, { table: table.name, fields, where: sql`${tuple} = (SELECT ${sql.join(names, sql`, `)} FROM _ws_seal_page WHERE rowid = ${group[0].ordinal})` }, hash)
              continue
            }
            for (const item of yield* db.all<{ value: string }>(sql`SELECT ${encoded(fields)} AS value FROM _ws_seal_page p
              CROSS JOIN ${identifier(table.name)} s WHERE ${join} AND p.rowid BETWEEN ${group[0].ordinal} AND ${group[group.length - 1].ordinal} ORDER BY p.rowid`)) hash.update(item.value)
          }
          yield* db.run("DELETE FROM _ws_seal_cursor")
          yield* db.run("INSERT INTO _ws_seal_cursor SELECT * FROM _ws_seal_page ORDER BY rowid DESC LIMIT 1")
          yield* db.run("DELETE FROM _ws_seal_page")
          started = true
        }
      }).pipe(Effect.ensuring(Effect.gen(function* () {
        yield* db.run("DROP TABLE IF EXISTS temp._ws_seal_page")
        yield* db.run("DROP TABLE temp._ws_seal_cursor")
      }).pipe(Effect.orDie)))
    }
    return hash.digest("hex")
  })
}

function encoded(fields: readonly string[]) {
  return sql`json_array(${sql.join(fields.flatMap((name) => [sql`typeof(${field(name)})`,
    sql`CASE WHEN typeof(${field(name)}) IN ('text', 'blob') THEN hex(${field(name)}) ELSE quote(${field(name)}) END`]), sql`, `)})`
}

function size(fields: readonly string[]) {
  return sql`2 + ${sql.join(fields.map((name) => sql`length(typeof(${field(name)})) + 8 + CASE WHEN typeof(${field(name)}) IN ('text', 'blob')
    THEN 2 * octet_length(${field(name)}) ELSE length(quote(${field(name)})) END`), sql` + `)}`
}

function* admit<Row extends { bytes: number }>(rows: readonly Row[]) {
  let group: Row[] = []
  let bytes = 0
  for (const item of rows) {
    if (group.length && bytes + item.bytes > WsNativeScan.byteLimit) {
      yield group
      group = []
      bytes = 0
    }
    group.push(item)
    bytes += item.bytes
  }
  if (group.length) yield group
}

function row(db: Database, selection: Selection, hash: Hash) {
  return Effect.gen(function* () {
    const result = yield* db.get<{ value: string | null }>(sql`SELECT CASE WHEN ${size(selection.fields)} <= ${WsNativeScan.byteLimit}
      THEN ${encoded(selection.fields)} ELSE NULL END AS value FROM ${identifier(selection.table)} s WHERE ${selection.where}`)
    if (!result) return yield* Effect.fail(new DatabaseWsArchive.ArchiveError("Missing stage seal row"))
    if (result.value !== null) { hash.update(result.value); return }
    yield* chunks(db, selection, hash)
  })
}

function chunks(db: Database, selection: Selection, hash: Hash) {
  return Effect.gen(function* () {
    hash.update("[")
    for (const [index, name] of selection.fields.entries()) {
      const cell = yield* db.get<{ kind: string; bytes: number; value: string | null }>(sql`SELECT typeof(${field(name)}) AS kind,
        octet_length(${field(name)}) AS bytes, CASE WHEN typeof(${field(name)}) IN ('text', 'blob') THEN NULL ELSE quote(${field(name)}) END AS value
        FROM ${identifier(selection.table)} s WHERE ${selection.where}`)
      if (!cell) return yield* Effect.fail(new DatabaseWsArchive.ArchiveError("Missing stage seal cell"))
      hash.update(`${index ? "," : ""}${JSON.stringify(cell.kind)},`)
      if (cell.value !== null) { hash.update(JSON.stringify(cell.value)); continue }
      hash.update('"')
      for (let offset = 0; offset < cell.bytes; offset += WsNativeScan.byteLimit / 4) {
        const part = yield* db.get<{ value: string }>(sql`SELECT hex(substr(CAST(${field(name)} AS BLOB), ${offset + 1}, ${WsNativeScan.byteLimit / 4})) AS value
          FROM ${identifier(selection.table)} s WHERE ${selection.where}`)
        if (!part) return yield* Effect.fail(new DatabaseWsArchive.ArchiveError("Missing stage seal chunk"))
        hash.update(part.value)
      }
      hash.update('"')
    }
    hash.update("]")
  })
}

export * as WsNativeStage from "./ws-native-stage.js"

import { createHash, randomUUID } from "node:crypto"
import { copyFile, lstat, readFile } from "node:fs/promises"
import { constants } from "node:fs"
import { basename, dirname, join } from "node:path"
import { Effect, Schema } from "effect"
import { sql } from "drizzle-orm"
import type { EffectDrizzleSqlite } from "./drizzle.js"
import { DatabaseArtifact } from "./artifact.js"
import { DatabaseLegacyV01 } from "./legacy-v01.js"
import { DatabaseReplacement } from "./replacement.js"

const Digest = Schema.Struct({ sha256: Schema.String, bytes: Schema.Number })
const Evidence = Schema.Struct({ main: Digest, wal: Schema.NullOr(Digest) })
const Artifact = Schema.Struct({ name: Schema.String, sha256: Schema.String, bytes: Schema.Number })
const Marker = Schema.Struct({
  format: Schema.Literal("opencode.database.ws-stage"), version: Schema.Literal(1), token: Schema.String,
  source: Schema.Struct({ name: Schema.String, evidence: Evidence }), snapshot: Artifact, work: Schema.String,
  phase: Schema.Literals(["snapshot", "converting", "ready", "handoff"]), ready: Schema.optional(Artifact),
})
type Marker = typeof Marker.Type
const failure = (message: string) => Effect.fail(new DatabaseLegacyV01.DatabaseLegacyV01Error(message))

export function identity(source: string) {
  return Effect.gen(function* () {
    yield* DatabaseArtifact.requireRegularFile(source)
    for (const suffix of ["-wal", "-shm", "-journal"])
      if (yield* DatabaseArtifact.exists(`${source}${suffix}`)) yield* DatabaseArtifact.requireRegularFile(`${source}${suffix}`)
    const main = yield* DatabaseArtifact.digest(source)
    const wal = (yield* DatabaseArtifact.exists(`${source}-wal`)) ? yield* DatabaseArtifact.digest(`${source}-wal`) : null
    const evidence = { main, wal: wal?.bytes ? wal : null }
    return { evidence, fingerprint: `v1:${createHash("sha256").update(JSON.stringify(evidence)).digest("hex")}` }
  })
}

export function cleanRead<A, E, R>(source: string, use: (proof: Effect.Success<ReturnType<typeof identity>>) => Effect.Effect<A, E, R>) {
  return Effect.gen(function* () {
    if (!(yield* clean(source))) return
    const context = yield* Effect.context<R>()
    return yield* DatabaseLegacyV01.useDatabase(source, true, (db) => db.transaction((tx) => Effect.gen(function* () {
      yield* tx.get("SELECT name FROM sqlite_schema LIMIT 1")
      if (!(yield* clean(source))) return
      const proof = yield* identity(source)
      if (proof.evidence.wal) return
      const value = yield* use(proof).pipe(Effect.provideContext(context))
      if (!(yield* clean(source))) return
      return { identity: proof, value }
    }), { behavior: "deferred" }))
  })
}

function clean(source: string) {
  return Effect.gen(function* () {
    yield* DatabaseArtifact.requireRegularFile(source)
    for (const suffix of ["-wal", "-shm", "-journal"])
      if (yield* DatabaseArtifact.exists(`${source}${suffix}`)) yield* DatabaseArtifact.requireRegularFile(`${source}${suffix}`)
    if (yield* DatabaseArtifact.exists(`${source}-journal`)) return false
    if (!(yield* DatabaseArtifact.exists(`${source}-wal`))) return true
    const wal = yield* Effect.tryPromise({ try: () => lstat(`${source}-wal`), catch: (cause) => new DatabaseArtifact.Error("stat", { cause }) })
    return wal.size === 0
  })
}

export function read(target: string) {
  return Effect.gen(function* () {
    const filename = `${target}.ws-stage.json`
    yield* DatabaseArtifact.requireRegularFile(filename)
    const text = yield* Effect.tryPromise({ try: () => readFile(filename, "utf8"), catch: (cause) => new DatabaseLegacyV01.DatabaseLegacyV01Error("Unreadable WS stage marker", { cause }) })
    const marker = yield* Schema.decodeUnknownEffect(Schema.fromJsonString(Marker))(text, { onExcessProperty: "error" })
    if (!/^[0-9a-f-]{36}$/.test(marker.token) || marker.snapshot.name !== `${basename(target)}.ws-snapshot-${marker.token}.db` ||
      marker.work !== `${basename(target)}.native-${marker.token}.db` ||
      !marker.source.name || marker.source.name === "." || marker.source.name === ".." || /[\\/:]/.test(marker.source.name) ||
      ((marker.phase === "ready" || marker.phase === "handoff") && !marker.ready) ||
      ((marker.phase === "snapshot" || marker.phase === "converting") && marker.ready !== undefined) ||
      (marker.ready && marker.ready.name !== marker.work)) return yield* failure("Unsafe WS stage marker")
    return marker
  })
}

export function capture(source: string, target: string) {
  return Effect.gen(function* () {
    const { WsNative } = yield* Effect.promise(() => import("./ws-native.js"))
    const before = yield* identity(source)
    const captured = yield* (yield* DatabaseArtifact.exists(`${target}.ws-stage.json`))
      ? read(target).pipe(Effect.map((marker) => ({ marker, initialize: undefined }))) : Effect.gen(function* () {
      const token = randomUUID()
      const snapshot = `${target}.ws-snapshot-${token}.db`
      yield* DatabaseLegacyV01.useDatabase(source, true, (db) => DatabaseArtifact.snapshot(db, snapshot))
      yield* DatabaseReplacement.requireNoSidecars(snapshot)
      const digest = yield* DatabaseArtifact.digest(snapshot)
      if (before.fingerprint !== (yield* identity(source)).fingerprint) return yield* failure("Database source changed during snapshot")
      const initialize = yield* DatabaseLegacyV01.useDatabase(snapshot, true, WsNative.validateStageSource)
      const initial: Marker = {
        format: "opencode.database.ws-stage", version: 1, token,
        source: { name: basename(source), evidence: before.evidence },
        snapshot: { name: basename(snapshot), ...digest }, work: `${basename(target)}.native-${token}.db`, phase: "snapshot",
      }
      yield* DatabaseArtifact.write(`${target}.ws-stage.json`, `${JSON.stringify(initial)}\n`)
      return { marker: initial, initialize }
    })
    const marker = captured.marker
    if (marker.source.name !== basename(source)) return yield* failure("WS stage source differs")
    const snapshot = join(dirname(target), marker.snapshot.name)
    yield* DatabaseArtifact.requireRegularFile(snapshot)
    if (!captured.initialize) yield* DatabaseReplacement.validate(snapshot, marker.snapshot)
    yield* DatabaseReplacement.requireNoSidecars(snapshot)
    const changed = JSON.stringify(marker.source.evidence) !== JSON.stringify(before.evidence)
    if (changed && marker.phase !== "handoff") return yield* failure("Database source changed since WS stage preparation")
    const { WsSnapshotRecovery } = yield* Effect.promise(() => import("./ws-snapshot-recovery.js"))
    const recovered = yield* WsSnapshotRecovery.recover(source, target, { marker, before })
    if (recovered) {
      yield* write(target, recovered)
      return { ...captured, marker: recovered, before }
    }
    if (changed || marker.source.evidence.wal || before.evidence.wal) {
      const { WsNativeFile } = yield* Effect.promise(() => import("./ws-native-file.js"))
      yield* WsNativeFile.requireSameSource(source, snapshot)
    }
    if (before.fingerprint !== (yield* identity(source)).fingerprint) return yield* failure("Database source changed during stage lookup")
    return { ...captured, before }
  })
}

export function prepare(source: string, target: string, captured?: Effect.Success<ReturnType<typeof capture>>) {
  return Effect.gen(function* () {
    const { WsNative } = yield* Effect.promise(() => import("./ws-native.js"))
    const proof = captured ?? (yield* capture(source, target))
    const marker = proof.marker
    const before = proof.before
    const snapshot = join(dirname(target), marker.snapshot.name)
    const work = join(dirname(target), marker.work)
    if (marker.source.evidence.wal || before.evidence.wal) {
      const { WsNativeFile } = yield* Effect.promise(() => import("./ws-native-file.js"))
      yield* WsNativeFile.requireSameSource(source, snapshot)
    }
    if (marker.ready) {
      yield* DatabaseReplacement.validate(work, marker.ready)
      if (before.fingerprint !== (yield* identity(source)).fingerprint) return yield* failure("Database source changed during stage lookup")
      if (marker.source.evidence.wal || before.evidence.wal) {
        const { WsNativeFile } = yield* Effect.promise(() => import("./ws-native-file.js"))
        yield* WsNativeFile.requireSameSource(source, snapshot)
      }
      return marker.ready
    }
    if (!(yield* DatabaseArtifact.exists(work))) {
      if (marker.phase !== "snapshot") return yield* failure("WS stage work is missing")
      const partial = `${work}.${randomUUID()}.partial`
      yield* Effect.tryPromise({ try: () => copyFile(snapshot, partial, constants.COPYFILE_EXCL), catch: (cause) => new DatabaseArtifact.Error("snapshot", { cause }) })
      yield* DatabaseArtifact.syncFile(partial)
      yield* DatabaseArtifact.publish(partial, work)
    }
    yield* DatabaseArtifact.requireRegularFile(work)
    for (const suffix of ["-wal", "-shm", "-journal"])
      if (yield* DatabaseArtifact.exists(`${work}${suffix}`)) yield* DatabaseArtifact.requireRegularFile(`${work}${suffix}`)
    const initialized = yield* DatabaseLegacyV01.useDatabase(work, true, (db) => db.get("SELECT 1 FROM sqlite_schema WHERE name = '_ws_stage'"))
    if (!initialized) yield* DatabaseReplacement.validate(work, marker.snapshot)
    yield* DatabaseLegacyV01.useDatabase(work, false, (db) => Effect.gen(function* () {
      const checkpoint = yield* !initialized && proof.initialize ? proof.initialize(db, marker.token) : WsNative.prepareStage(db, marker.token)
      yield* write(target, { ...marker, phase: "converting" })
      yield* checkpoint.resume
      const rows = yield* db.all<{ busy: number }>("PRAGMA wal_checkpoint(TRUNCATE)")
      if (rows.length !== 1 || rows[0]?.busy !== 0) return yield* failure("WS stage checkpoint is busy")
    }))
    yield* DatabaseReplacement.requireNoSidecars(work)
    yield* DatabaseArtifact.syncFile(work)
    const ready = { name: marker.work, ...(yield* DatabaseArtifact.digest(work)) }
    if (before.fingerprint !== (yield* identity(source)).fingerprint) return yield* failure("database source changed during conversion")
    if (marker.source.evidence.wal || before.evidence.wal) {
      const { WsNativeFile } = yield* Effect.promise(() => import("./ws-native-file.js"))
      yield* WsNativeFile.requireSameSource(source, snapshot)
    }
    yield* write(target, { ...marker, phase: "ready", ready })
    return ready
  })
}

export function retireProfile(target: EffectDrizzleSqlite.EffectSQLiteDatabase, owner: string) {
  return Effect.gen(function* () {
    if (!(yield* DatabaseArtifact.exists(`${owner}.ws-stage.json`))) return
    const marker = yield* read(owner)
    if (!(yield* target.get(sql`SELECT 1 FROM kv WHERE key = ${`migration.ws-profile.${marker.snapshot.sha256}`}
      AND json_valid(value) AND json_extract(value, '$.completed') = 1`))) return yield* failure("Profile stage has no completion proof")
    for (const name of [marker.work, marker.snapshot.name]) {
      const filename = join(dirname(owner), name)
      if (!(yield* DatabaseArtifact.exists(filename))) continue
      yield* DatabaseArtifact.requireRegularFile(filename)
      yield* DatabaseReplacement.requireNoSidecars(filename)
      yield* DatabaseArtifact.requireAbsent(`${filename}-journal`)
      yield* DatabaseArtifact.remove(filename)
    }
    yield* DatabaseArtifact.remove(`${owner}.ws-stage.json`)
  })
}

export function handoff(target: string) {
  return read(target).pipe(Effect.flatMap((marker) => write(target, { ...marker, phase: "handoff" })))
}

export function retire(target: string) {
  return retireOwned(target)
}

export function activateAndRetire(target: string) {
  return Effect.gen(function* () {
    const activated = yield* DatabaseReplacement.activateLocked(target)
    yield* retireOwned(target, activated)
  })
}

function retireOwned(target: string, activated?: DatabaseArtifact.Digest) {
  return Effect.gen(function* () {
    if (!(yield* DatabaseArtifact.exists(`${target}.ws-stage.json`))) return
    const marker = yield* read(target)
    if (!marker.ready) return yield* failure("WS stage has no activation proof")
    if (activated?.sha256 !== marker.ready.sha256 || activated.bytes !== marker.ready.bytes) {
      yield* DatabaseReplacement.validate(target, marker.ready)
    }
    const snapshot = join(dirname(target), marker.snapshot.name)
    if (yield* DatabaseArtifact.exists(snapshot)) {
      yield* DatabaseArtifact.requireRegularFile(snapshot)
      yield* DatabaseReplacement.requireNoSidecars(snapshot)
      yield* DatabaseArtifact.requireAbsent(`${snapshot}-journal`)
      yield* DatabaseArtifact.remove(snapshot)
    }
    yield* DatabaseArtifact.remove(`${target}.ws-stage.json`)
    yield* DatabaseLegacyV01.useDatabase(target, false, (db) => db.run("DROP TABLE _ws_stage"))
  })
}

function write(target: string, marker: Marker) {
  return DatabaseArtifact.replaceWithText(`${target}.ws-stage.json`, `${JSON.stringify(marker)}\n`)
}

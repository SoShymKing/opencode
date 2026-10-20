export * as WsSnapshotRecovery from "./ws-snapshot-recovery.js"

import { randomUUID } from "node:crypto"
import { constants } from "node:fs"
import { copyFile } from "node:fs/promises"
import { basename, dirname, join } from "node:path"
import { Effect } from "effect"
import { DatabaseArtifact } from "./artifact.js"
import { DatabaseLegacyV01 } from "./legacy-v01.js"
import { DatabaseReplacement } from "./replacement.js"
import { WsNativeFile } from "./ws-native-file.js"
import { WsNativeStage } from "./ws-native-stage.js"

export function recover(source: string, target: string, proof: {
  readonly marker: Effect.Success<ReturnType<typeof WsNativeStage.read>>
  readonly before: Effect.Success<ReturnType<typeof WsNativeStage.identity>>
}) {
  return Effect.gen(function* () {
    const marker = proof.marker
    if (marker.phase !== "snapshot") return
    const work = join(dirname(target), marker.work)
    for (const suffix of ["", "-wal", "-shm", "-journal"])
      if (yield* DatabaseArtifact.exists(`${work}${suffix}`)) return
    if (!(yield* DatabaseLegacyV01.useDatabase(source, true, DatabaseArtifact.hasEmptyOrphanSequence))) return
    const snapshot = join(dirname(target), marker.snapshot.name)
    if (yield* DatabaseLegacyV01.useDatabase(snapshot, true, (db) => db.get("SELECT 1 FROM sqlite_schema WHERE name = 'sqlite_sequence'"))) return
    const token = randomUUID()
    const destination = `${target}.ws-snapshot-${token}.db`
    const partial = `${destination}.partial`
    return yield* Effect.acquireUseRelease(
      Effect.tryPromise({
        try: () => copyFile(snapshot, partial, constants.COPYFILE_EXCL),
        catch: (cause) => new DatabaseArtifact.Error("snapshot", { cause }),
      }),
      () => Effect.gen(function* () {
        yield* DatabaseReplacement.validate(partial, marker.snapshot)
        yield* DatabaseLegacyV01.useDatabase(source, true, (db) => DatabaseArtifact.restoreEmptySequence(db, partial))
        yield* DatabaseReplacement.requireNoSidecars(partial)
        yield* WsNativeFile.requireSameSource(source, partial)
        if (proof.before.fingerprint !== (yield* WsNativeStage.identity(source)).fingerprint)
          return yield* Effect.fail(new DatabaseLegacyV01.DatabaseLegacyV01Error("Database source changed during snapshot recovery"))
        const digest = yield* DatabaseArtifact.digest(partial)
        yield* DatabaseArtifact.syncFile(partial)
        yield* DatabaseArtifact.publish(partial, destination)
        return {
          ...marker, token, snapshot: { name: basename(destination), ...digest },
          work: `${basename(target)}.native-${token}.db`,
        }
      }),
      () => DatabaseArtifact.discard(partial),
    )
  })
}

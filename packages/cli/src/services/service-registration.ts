export * as ServiceRegistration from "./service-registration"

import { Service, type Info } from "@opencode/client/effect/service"
import path from "node:path"
import { Effect, FileSystem, Schedule, Schema, Semaphore } from "effect"
import { HttpServer } from "effect/http"
import { NetAddress } from "effect/net"
import { OPENCODE_VERSION } from "../version"

const infoJson = Schema.fromJsonString(Service.Info)
const encodeInfo = Schema.encodeEffect(infoJson)
const decodeInfo = Schema.decodeUnknownEffect(infoJson)

export const acquire = Effect.fnUntraced(function* (options: {
  readonly address: NetAddress.SocketAddress
  readonly password: string
  readonly id: string
  readonly file: string
  readonly shutdown: Effect.Effect<void>
}) {
  const fs = yield* FileSystem.FileSystem
  const lock = yield* Semaphore.make(1)
  let closed = false
  const temp = options.file + "." + options.id + ".tmp"
  yield* fs.makeDirectory(path.dirname(options.file), { recursive: true })
  const info = {
    id: options.id,
    version: OPENCODE_VERSION,
    url: HttpServer.formatAddress(options.address),
    pid: process.pid,
    password: options.password,
  }
  const encoded = yield* encodeInfo(info)
  const current = fs.readFileString(options.file).pipe(Effect.flatMap(decodeInfo))
  const owns = (found: Info) =>
    found.id === info.id &&
    found.version === info.version &&
    found.url === info.url &&
    found.pid === info.pid &&
    found.password === info.password
  yield* fs.writeFileString(temp, encoded, { mode: 0o600 }).pipe(Effect.andThen(fs.rename(temp, options.file)))
  yield* current.pipe(
    Effect.catchCause((cause) =>
      Effect.logWarning("managed service registration check failed; shutting down", {
        cause,
        serviceID: options.id,
        servicePID: process.pid,
        registration: options.file,
      }).pipe(Effect.andThen(Effect.failCause(cause))),
    ),
    Effect.tap((found) =>
      owns(found)
        ? Effect.void
        : Effect.logWarning("managed service registration replaced; shutting down", {
            serviceID: options.id,
            servicePID: process.pid,
            registration: options.file,
            observedServiceID: found.id,
            observedServicePID: found.pid,
            observedVersion: found.version,
            observedURL: found.url,
          }),
    ),
    Effect.filterOrFail(owns),
    Effect.repeat(Schedule.spaced("5 seconds")),
    Effect.ignore,
    Effect.andThen(options.shutdown),
    Effect.forkScoped,
  )
  const setPreparation = (active: boolean) => lock.withPermit(Effect.gen(function* () {
    if (closed) return
    const found = yield* current.pipe(Effect.catch(() => Effect.succeed(undefined)))
    if (!found || !owns(found)) return
    const encoded = yield* encodeInfo({ ...info, ...(active ? { startup: "database-preparation" as const } : {}) })
    yield* fs.writeFileString(temp, encoded, { mode: 0o600 })
    yield* current.pipe(
      Effect.catch(() => Effect.succeed(undefined)),
      Effect.flatMap((latest) => latest && owns(latest) ? fs.rename(temp, options.file) : Effect.void),
    )
  }).pipe(Effect.ensuring(fs.remove(temp).pipe(Effect.ignore)))).pipe(Effect.uninterruptible)
  const cleanup = lock.withPermit(Effect.gen(function* () {
    closed = true
    yield* current.pipe(
      Effect.flatMap((found) => (owns(found) ? fs.remove(options.file) : Effect.void)),
      Effect.ignore,
    )
  })).pipe(Effect.uninterruptible)
  return { cleanup, setPreparation }
})

export const register = (options: Parameters<typeof acquire>[0]) => acquire(options).pipe(Effect.map((owner) => owner.cleanup))

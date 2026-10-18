import { expect, test } from "bun:test"
import { NodeFileSystem } from "@effect/platform-node"
import { Effect, FileSystem } from "effect"
import { NetAddress } from "effect/net"
import path from "node:path"
import { ServiceRegistration } from "../src/services/service-registration"

test("preparation preserves identity and cleanup rejects late updates", async () => {
  await Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem
    const file = path.join(yield* fs.makeTempDirectoryScoped(), "service.json")
    const owner = yield* ServiceRegistration.acquire({
      address: NetAddress.inetAddressFromIpStringUnsafe("127.0.0.1", 4321),
      password: "test-password", id: "test-owner", file, shutdown: Effect.never,
    })
    const original = JSON.parse(yield* fs.readFileString(file))
    yield* owner.setPreparation(true)
    expect(JSON.parse(yield* fs.readFileString(file))).toEqual({ ...original, startup: "database-preparation" })
    yield* owner.setPreparation(false)
    expect(JSON.parse(yield* fs.readFileString(file))).toEqual(original)
    yield* owner.cleanup
    yield* owner.setPreparation(true)
    expect(yield* fs.exists(file)).toBe(false)
  }).pipe(Effect.scoped, Effect.provide(NodeFileSystem.layer), Effect.runPromise)
})

test.each(["id", "version", "url", "pid", "password"])("preparation and cleanup leave a successor with different %s untouched", async (field) => {
  await Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem
    const file = path.join(yield* fs.makeTempDirectoryScoped(), "service.json")
    const owner = yield* ServiceRegistration.acquire({
      address: NetAddress.inetAddressFromIpStringUnsafe("127.0.0.1", 4321),
      password: "test-password", id: "test-owner", file, shutdown: Effect.never,
    })
    const successor = { ...JSON.parse(yield* fs.readFileString(file)), [field]: field === "pid" ? 123 : "successor" }
    yield* fs.writeFileString(file, JSON.stringify(successor))
    yield* owner.setPreparation(true)
    yield* owner.setPreparation(false)
    yield* owner.cleanup
    expect(JSON.parse(yield* fs.readFileString(file))).toEqual(successor)
  }).pipe(Effect.scoped, Effect.provide(NodeFileSystem.layer), Effect.runPromise)
})

test.each(["deleted", "corrupt"])("preparation leaves %s registration for the ownership watcher", async (state) => {
  await Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem
    const file = path.join(yield* fs.makeTempDirectoryScoped(), "service.json")
    const owner = yield* ServiceRegistration.acquire({
      address: NetAddress.inetAddressFromIpStringUnsafe("127.0.0.1", 4321),
      password: "test-password", id: "test-owner", file, shutdown: Effect.never,
    })
    if (state === "deleted") yield* fs.remove(file)
    if (state === "corrupt") yield* fs.writeFileString(file, "{")
    yield* owner.setPreparation(false)
    if (state === "deleted") expect(yield* fs.exists(file)).toBe(false)
    if (state === "corrupt") expect(yield* fs.readFileString(file)).toBe("{")
    yield* owner.cleanup
  }).pipe(Effect.scoped, Effect.provide(NodeFileSystem.layer), Effect.runPromise)
})

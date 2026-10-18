import { expect } from "bun:test"
import path from "node:path"
import { Database } from "@opencode/core/database/database"
import { WsNativeWorkspace } from "@opencode/core/database/ws-native-workspace"
import { EnvironmentService } from "@opencode/core/environment/environment"
import { Location } from "@opencode/core/location"
import { AbsolutePath } from "@opencode/core/schema"
import { Workspace } from "@opencode/core/workspace"
import { WorkspaceDriver } from "@opencode/core/workspace/driver"
import { WorkspaceTable } from "@opencode/core/workspace/sql"
import { LayerNode } from "@opencode/util/effect/layer-node"
import { eq } from "drizzle-orm"
import { Effect, Layer } from "effect"
import { tmpdirScoped } from "./fixture/tmpdir"
import { testEffect } from "./lib/effect"

const it = testEffect(LayerNode.compile(LayerNode.group([Database.node, Workspace.node, WorkspaceDriver.node])))

it.live("connects an imported explicit workspace through the native registry and Environment", () =>
  Effect.gen(function* () {
    const directory = yield* tmpdirScoped()
    const file = path.join(directory.path, "existing.txt")
    yield* Effect.promise(() => Bun.write(file, "existing user file"))
    const workspace = yield* Workspace.Service
    const db = (yield* Database.Service).db
    const workspaceID = Workspace.ID.create()
    yield* db.insert(WorkspaceTable).values({
      id: workspaceID,
      provider: WsNativeWorkspace.Provider,
      binding: { directory: directory.path },
      created_at: 1,
      last_used_at: 1,
    }).run()

    expect((yield* workspace.provision(workspaceID)).binding).toEqual({ directory: directory.path })
    yield* Effect.gen(function* () {
      const location = yield* Location.Service
      const environment = yield* EnvironmentService.Service
      expect(location.workspaceID).toBe(workspaceID)
      expect(new TextDecoder().decode((yield* environment.files.read(file)).bytes)).toBe("existing user file")
    }).pipe(Effect.provide(LayerNode.compile(LayerNode.group([EnvironmentService.node, Location.node]), {
      replacements: [
        Workspace.node.replace(Layer.succeed(Workspace.Service, workspace)),
        Location.node.replace(Location.boundNode({ directory: AbsolutePath.make(directory.path), workspaceID })),
      ],
    })))

    const registry = yield* WorkspaceDriver.RegistryService
    const driver = yield* registry.get(WsNativeWorkspace.Provider)
    yield* driver.suspendForIdle({ workspaceID, binding: { directory: directory.path }, saveBinding: () => Effect.void })
    expect(yield* workspace.destroy(workspaceID)).toEqual({ destroyed: true })
    expect(yield* db.select().from(WorkspaceTable).where(eq(WorkspaceTable.id, workspaceID)).get()).toBeUndefined()
    expect(yield* Effect.promise(() => Bun.file(file).text())).toBe("existing user file")
  }),
)

it.live("rejects missing and invalid imported bindings without provisioning resources", () =>
  Effect.gen(function* () {
    const workspace = yield* Workspace.Service
    const db = (yield* Database.Service).db
    const registry = yield* WorkspaceDriver.RegistryService
    const driver = yield* registry.get(WsNativeWorkspace.Provider)
    const bindings: Array<WorkspaceDriver.Binding | null> = [null, {}, { directory: 42 }, { directory: "relative/path" }]
    for (const binding of bindings) {
      const workspaceID = Workspace.ID.create()
      yield* db.insert(WorkspaceTable).values({
        id: workspaceID,
        provider: WsNativeWorkspace.Provider,
        binding,
        created_at: 1,
        last_used_at: 1,
      }).run()
      const error = yield* (binding === null
        ? workspace.provision(workspaceID).pipe(Effect.flip)
        : driver.connect({ workspaceID, binding, saveBinding: () => Effect.void }).pipe(Effect.flip))
      expect(error).toBeInstanceOf(WorkspaceDriver.Error)
      if (binding === null) expect(error.message).toContain("does not support provisioning")
      yield* workspace.destroy(workspaceID)
    }
  }),
)

it.live("keeps unknown providers unresolved and custom registries exact", () =>
  Effect.gen(function* () {
    const registry = yield* WorkspaceDriver.RegistryService
    expect(yield* registry.get("custom-remote").pipe(Effect.flip)).toEqual(
      new WorkspaceDriver.ProviderNotFound({ provider: "custom-remote" }),
    )
    yield* Effect.gen(function* () {
      const custom = yield* WorkspaceDriver.RegistryService
      expect(yield* custom.get(WsNativeWorkspace.Provider).pipe(Effect.flip)).toEqual(
        new WorkspaceDriver.ProviderNotFound({ provider: WsNativeWorkspace.Provider }),
      )
    }).pipe(Effect.provide(LayerNode.compile(WorkspaceDriver.registryNode({}))))
  }),
)

export * as WsNativeWorkspace from "./ws-native-workspace.js"

import path from "node:path"
import { Effect, Schema } from "effect"
import type { ChildProcessSpawner } from "effect/process/ChildProcessSpawner"
import { makeLocalDriver } from "../environment/local.js"
import { WorkspaceDriver } from "../workspace/driver.js"

export const Provider = "ws-local"

const Binding = Schema.Struct({ directory: Schema.String })

export const make = (spawner: ChildProcessSpawner["Service"]) =>
  WorkspaceDriver.make({
    create: () =>
      Effect.fail(
        new WorkspaceDriver.Error({
          message: "ws-local does not support provisioning; import an existing directory binding",
        }),
      ),
    connect: ({ binding }) =>
      Effect.gen(function* () {
        const value = yield* Schema.decodeUnknownEffect(Binding)(binding).pipe(
          Effect.mapError((cause) => new WorkspaceDriver.Error({ message: "Invalid ws-local binding", cause })),
        )
        if (!path.isAbsolute(value.directory))
          return yield* new WorkspaceDriver.Error({ message: "ws-local binding directory must be absolute" })
        return makeLocalDriver(spawner)
      }),
    suspendForIdle: () => Effect.void,
    // Imported directories remain user-owned: destruction only detaches the logical workspace record.
    destroy: () => Effect.void,
  })

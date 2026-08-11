import { AppNodeBuilder } from "@opencode-ai/core/effect/app-node-builder"
import { LayerNode } from "@opencode-ai/core/effect/layer-node"
import { Database } from "@opencode-ai/core/database/database"
import { EventV2 } from "@opencode-ai/core/event"
import { Project } from "@opencode-ai/core/project"
import { ProjectTable } from "@opencode-ai/core/project/sql"
import { AbsolutePath } from "@opencode-ai/core/schema"
import { SessionProjector } from "@opencode-ai/core/session/projector"
import { SessionV1 } from "@opencode-ai/core/v1/session"
import { MessageV2 } from "@/session/message-v2"
import type { MessageID } from "@/session/schema"
import { isRecord } from "@/util/record"
import { Effect, Exit, Layer, Schema, Scope, Stream } from "effect"
import path from "path"

export type History = Effect.Success<ReturnType<typeof captureHistory>>

export const captureHistory = Effect.fn("test.captureShellHistory")(function* (aggregateID: string) {
  const database = yield* Database.Service
  const events = yield* EventV2.Service
  const latest = yield* EventV2.latestSequence(database.db, aggregateID)
  const payloads = latest < 0
    ? []
    : Array.from(
        yield* events
          .durable({ aggregateID })
          .pipe(Stream.take(latest + 1), Stream.runCollect),
      )
  const serialized: EventV2.SerializedEvent[] = []
  for (const [index, event] of payloads.entries()) {
    if (!event.durable || event.durable.aggregateID !== aggregateID || event.durable.seq !== index)
      return yield* Effect.die(new Error("invalid durable shell history"))
    if (event.durable.version < 1 || !isRecord(event.data))
      return yield* Effect.die(new Error("invalid durable shell event"))
    serialized.push({
      id: event.id,
      type: EventV2.versionedType(event.type, event.durable.version),
      seq: event.durable.seq,
      aggregateID,
      data: event.data,
    })
  }
  if (serialized.length !== latest + 1) return yield* Effect.die(new Error("incomplete durable shell history"))
  return { latest, payloads, serialized }
})

export function toolParts(history: History, callID: string) {
  return history.payloads.flatMap((event) => {
    if (event.type !== SessionV1.Event.PartUpdated.type) return []
    if (!Schema.is(SessionV1.Event.PartUpdated.data)(event.data)) return []
    const part = event.data.part
    return part.type === "tool" && part.callID === callID ? [part] : []
  })
}

export const replayFresh = Effect.fn("test.replayShellHistoryFresh")(function* (input: {
  readonly directory: string
  readonly projectID: Project.ID
  readonly messageID: MessageID
  readonly callID: string
  readonly history: History
}) {
  const targetLayer = AppNodeBuilder.build(
    LayerNode.group([Database.node, EventV2.node, SessionProjector.node]),
    [[Database.node, Database.layerFromPath(path.join(input.directory, `replay-${crypto.randomUUID()}.sqlite`))]],
  )
  const scope = yield* Effect.acquireRelease(Scope.make(), (scope) => Scope.close(scope, Exit.void))
  const context = yield* Layer.buildWithMemoMap(targetLayer, Layer.makeMemoMapUnsafe(), scope)
  return yield* Effect.gen(function* () {
    const database = yield* Database.Service
    const events = yield* EventV2.Service
    yield* database.db
      .insert(ProjectTable)
      .values({ id: input.projectID, worktree: AbsolutePath.make(input.directory), sandboxes: [] })
      .run()
      .pipe(Effect.orDie)
    if ((yield* events.replayAll(input.history.serialized)) !== input.history.serialized[0]?.aggregateID)
      return yield* Effect.die(new Error("fresh shell replay failed"))
    const part = (yield* MessageV2.parts(input.messageID)).find(
      (item): item is SessionV1.ToolPart => item.type === "tool" && item.callID === input.callID,
    )
    if (!part) return yield* Effect.die(new Error("replayed shell tool part unavailable"))
    return part
  }).pipe(Effect.provide(context))
})

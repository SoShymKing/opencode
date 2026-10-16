import { expect, test } from "bun:test"
import { Effect, Layer } from "effect"
import { HttpRouter, HttpServer } from "effect/http"
import { HttpApi, HttpApiBuilder } from "effect/http-api"
import { Session } from "@opencode/core/session"
import { SessionTransfer } from "@opencode/core/session/transfer"
import { Database } from "@opencode/core/database/database"
import { AppNodeBuilder } from "@opencode/core/effect/app-node-builder"
import { SessionStatusEvent } from "@opencode/schema/session-status-event"
import { makeSessionGroup } from "@opencode/protocol/groups/session"
import { Authorization } from "@opencode/protocol/middleware/authorization"
import { SessionLocationMiddleware } from "../src/middleware/session-location"
import { FormLocationMiddleware } from "../src/middleware/form-location"
import { schemaErrorLayer } from "../src/middleware/schema-error"
import { SessionHandler } from "../src/handlers/session"

test("GET /api/session/active uses the native status snapshot and clears absent sessions", async () => {
  const sessionID = Session.ID.make("ses_active_snapshot")
  const snapshot = new Map<Session.ID, SessionStatusEvent.Info>([
    [sessionID, { type: "busy", activity: { model: "receiving", streamEventCount: 200 } }],
  ])
  const server = HttpRouter.toWebHandler(
    HttpApiBuilder.layer(HttpApi.make("server").add(makeSessionGroup(SessionLocationMiddleware, FormLocationMiddleware))).pipe(
      Layer.provide(SessionHandler.pipe(
        Layer.provide(Layer.mock(Session.Service, {
          activeSnapshot: Effect.sync(() => new Map(snapshot)),
          revert: {
            stage: () => Effect.die("unused"),
            clear: () => Effect.die("unused"),
            commit: () => Effect.die("unused"),
          },
        })),
        Layer.provide(Layer.mock(SessionTransfer.Service, {})),
        Layer.provide(Layer.succeed(Authorization, Authorization.of((effect) => effect))),
        Layer.provide(Layer.succeed(SessionLocationMiddleware, SessionLocationMiddleware.of(() => Effect.die("unused")))),
        Layer.provide(Layer.succeed(FormLocationMiddleware, FormLocationMiddleware.of(() => Effect.die("unused")))),
        Layer.provide(schemaErrorLayer),
      )),
      Layer.provide(HttpServer.layerServices),
      Layer.provideMerge(AppNodeBuilder.build(Database.configured({ path: ":memory:" }))),
    ),
    { disableLogger: true },
  )
  await Effect.runPromise(Effect.acquireUseRelease(
    Effect.succeed(server),
    (server) => Effect.promise(async () => {
      const response = await server.handler(new Request("http://localhost/api/session/active"))
      expect(response.status).toBe(200)
      expect(await response.json()).toEqual({ data: {
        [sessionID]: { type: "running", status: { type: "busy", activity: { model: "receiving", streamEventCount: 200 } } },
      } })
      snapshot.set(sessionID, { type: "retry", attempt: 2, message: "Retrying", next: 500 })
      const retry = await server.handler(new Request("http://localhost/api/session/active"))
      expect(await retry.json()).toEqual({ data: {
        [sessionID]: { type: "running", status: { type: "retry", attempt: 2, message: "Retrying", next: 500 } },
      } })
      snapshot.clear()
      const empty = await server.handler(new Request("http://localhost/api/session/active"))
      expect(await empty.json()).toEqual({ data: {} })
    }),
    (server) => Effect.promise(() => server.dispose()),
  ))
})

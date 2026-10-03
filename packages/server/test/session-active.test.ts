import { expect, test } from "bun:test"
import { Effect, Layer } from "effect"
import { HttpRouter, HttpServer } from "effect/unstable/http"
import { HttpApi, HttpApiBuilder } from "effect/unstable/httpapi"
import { SessionV2 } from "@opencode-ai/core/session"
import { SessionStatusEvent } from "@opencode-ai/schema/session-status-event"
import { makeSessionGroup } from "@opencode-ai/protocol/groups/session"
import { Authorization } from "@opencode-ai/protocol/middleware/authorization"
import { SessionLocationMiddleware } from "../src/middleware/session-location"
import { schemaErrorLayer } from "../src/middleware/schema-error"
import { SessionHandler } from "../src/handlers/session"

test("GET /api/session/active uses the native status snapshot and clears absent sessions", async () => {
  const sessionID = SessionV2.ID.make("ses_active_snapshot")
  const snapshot = new Map<SessionV2.ID, SessionStatusEvent.Info>([
    [sessionID, { type: "busy", activity: { model: "receiving", streamEventCount: 200 } }],
  ])
  const server = HttpRouter.toWebHandler(
    HttpApiBuilder.layer(HttpApi.make("server").add(makeSessionGroup(SessionLocationMiddleware))).pipe(
      Layer.provide(SessionHandler.pipe(
        Layer.provide(Layer.mock(SessionV2.Service, {
          activeSnapshot: Effect.sync(() => new Map(snapshot)),
          revert: {
            stage: () => Effect.die("unused"),
            clear: () => Effect.die("unused"),
            commit: () => Effect.die("unused"),
          },
        })),
        Layer.provide(Layer.succeed(Authorization, Authorization.of((effect) => effect))),
        Layer.provide(Layer.succeed(SessionLocationMiddleware, SessionLocationMiddleware.of(() => Effect.die("unused")))),
        Layer.provide(schemaErrorLayer),
      )),
      Layer.provide(HttpServer.layerServices),
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
      snapshot.clear()
      const empty = await server.handler(new Request("http://localhost/api/session/active"))
      expect(await empty.json()).toEqual({ data: {} })
    }),
    (server) => Effect.promise(() => server.dispose()),
  ))
})

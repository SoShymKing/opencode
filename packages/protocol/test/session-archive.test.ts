import { expect, test } from "bun:test"
import { OpenApi } from "effect/http-api"
import { ClientApi } from "../src/client.js"

test("archive and restore expose native POST mutations with no response body", () => {
  const document = OpenApi.fromApi(ClientApi)
  for (const operation of ["archive", "restore"]) {
    const endpoint = document.paths[`/api/session/{sessionID}/${operation}`]?.post
    expect(endpoint?.operationId).toBe(`session.${operation}`)
    expect(endpoint?.responses?.["204"]).toBeDefined()
    expect(endpoint?.requestBody).toBeUndefined()
  }
})

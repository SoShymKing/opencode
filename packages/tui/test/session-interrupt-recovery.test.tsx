import { expect, test } from "bun:test"
import { createAppFixture } from "./fixture/app"
import { directory, json } from "./fixture/tui-client"

test("attached session retries a stalled cancel and accepts one follow-up after settlement", async () => {
  const session = {
    id: "ses_interrupt",
    projectID: "proj_test",
    location: { directory },
    title: "Interrupt recovery",
    agent: "build",
    model: { providerID: "provider", id: "model" },
    cost: 0,
    tokens: { input: 0, output: 0, reasoning: 0, cache: { read: 0, write: 0 } },
    time: { created: 1, updated: 1 },
  }
  const first = Promise.withResolvers<void>()
  const second = Promise.withResolvers<void>()
  const stalled = Promise.withResolvers<Response>()
  const submitted = Promise.withResolvers<void>()
  const bodies: unknown[] = []
  let interrupts = 0
  await using setup = await createAppFixture({
    args: { sessionID: session.id },
    config: { animations: false, tabs: { mode: "off" } },
    fetch: async (url, request) => {
      if (url.pathname === "/api/session") return json({ data: [session], cursor: {} })
      if (url.pathname === "/api/session/active") return json({ data: { [session.id]: { type: "running" } } })
      if (url.pathname === `/api/session/${session.id}`) return json({ data: session })
      if (url.pathname === `/api/session/${session.id}/message`) return json({ data: [], cursor: {} })
      if (url.pathname === `/api/session/${session.id}/inbox`) return json({ data: [] })
      if (url.pathname === "/api/agent")
        return json({
          location: { directory, project: { id: "proj_test", directory, canonical: directory } },
          data: [{ id: "build", mode: "primary", hidden: false, permissions: [] }],
        })
      if (url.pathname === "/api/model")
        return json({
          location: { directory, project: { id: "proj_test", directory, canonical: directory } },
          data: [{ id: "model", providerID: "provider", name: "Model", variants: [] }],
        })
      if (url.pathname === `/api/session/${session.id}/interrupt`) {
        interrupts++
        if (interrupts === 1) {
          first.resolve()
          return stalled.promise
        }
        second.resolve()
        return json({ interrupted: true })
      }
      if (url.pathname === `/api/session/${session.id}/prompt`) {
        bodies.push(await request.json())
        submitted.resolve()
        return json({ data: {} })
      }
      return undefined
    },
  })
  try {
    await setup.ready
    await setup.waitForFrame((frame) => frame.includes("esc interrupt"))
    setup.mockInput.pressEscape()
    await setup.waitForFrame((frame) => frame.includes("again to interrupt"))
    setup.mockInput.pressEscape()
    await first.promise
    setup.mockInput.pressEscape()
    await setup.waitForFrame((frame) => frame.includes("again to interrupt"))
    setup.mockInput.pressEscape()
    await second.promise
    await setup.renderOnce()
    expect(interrupts).toBe(2)
    expect(setup.captureCharFrame()).toContain("esc interrupt")
    setup.events.emit({
      id: "evt_interrupted",
      created: 2,
      type: "session.execution.interrupted",
      durable: { aggregateID: session.id, seq: 1, version: 1 },
      data: { sessionID: session.id, reason: "user" },
    })
    await setup.waitForFrame((frame) => !frame.includes("esc interrupt"))
    setup.mockInput.typeText("Continue after cancel")
    setup.mockInput.pressEnter()
    await submitted.promise
    expect(bodies).toHaveLength(1)
    expect(bodies[0]).toMatchObject({ text: "Continue after cancel" })
  } finally {
    stalled.resolve(json({ interrupted: true }))
  }
})

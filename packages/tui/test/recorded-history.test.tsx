import { expect, test } from "bun:test"
import { testRender } from "@opentui/solid"
import { InputRenderable } from "@opentui/core"
import { ConfigProvider } from "../src/config"
import { ThemeProvider } from "../src/context/theme"
import { RecordedFiles, RecordedShellMessage } from "../src/routes/session/message-parts"
import { createTuiResolvedConfig } from "./fixture/tui-runtime"
import { recordedAssistant, recordedShell, recordedUser } from "./fixture/recorded-history"
import { createAppFixture } from "./fixture/app"
import { directory, json } from "./fixture/tui-client"
import { partPath, type Verbosity } from "../src/routes/session/grouping/session"

test.each([44, 100])("renders static recorded history at width %s", async (width) => {
  const ready = Promise.withResolvers<void>()
  const app = await testRender(() => (
    <ConfigProvider config={createTuiResolvedConfig({})}>
      <ThemeProvider mode="dark" source={{ discover: async () => ({}) }}>
        <box flexDirection="column" ref={() => ready.resolve()}>
          <RecordedShellMessage message={recordedShell()} />
          <RecordedShellMessage message={{ ...recordedShell(2), id: "msg_recorded_completed" }} />
          <RecordedFiles files={recordedUser.recordedFiles} />
        </box>
      </ThemeProvider>
    </ConfigProvider>
  ), { width, height: 20 })
  try {
    await ready.promise
    await app.renderOnce()
    const frame = app.captureCharFrame()
    if (process.env.TUI_HISTORY_QA_DIR)
      await Bun.write(`${process.env.TUI_HISTORY_QA_DIR}/recorded-history-${width}.txt`, frame)
    expect(frame.match(/Recorded shell \(outcome unknown\)/g)).toHaveLength(2)
    expect(frame.match(/\$ printf recorded/g)).toHaveLength(2)
    expect(frame.match(/recorded output/g)).toHaveLength(2)
    expect(frame).toContain("Unavailable file: missing.png")
    expect(frame.replace(/\s+/g, "")).toContain("notaURI:%zz")
    expect(frame).not.toMatch(/running|completed|success|bad title|\u001b|\u0007/)
    expect(app.renderer.liveRequestCount).toBe(0)
  } finally {
    app.renderer.destroy()
  }
})

test.each([44, 100])("renders assistant recorded tools as static facts at width %s", async (width) => {
  const session = {
    id: "ses_recorded_tools", title: "Recorded tools", projectID: "project", location: { directory },
    cost: 0, tokens: { input: 0, output: 0, reasoning: 0, cache: { read: 0, write: 0 } },
    time: { created: 0, updated: 0 },
  }
  const requests: string[] = []
  await using app = await createAppFixture({
    width, height: 80, args: { sessionID: session.id },
    config: { animations: false, tabs: { mode: "off" }, session: { verbosity: width === 44 ? "low" : "medium" } },
    fetch: (url) => {
      requests.push(url.pathname)
      if (url.pathname === "/api/session") return json({ data: [session], cursor: {} })
      if (url.pathname === `/api/session/${session.id}`) return json({ data: session })
      if (url.pathname === `/api/session/${session.id}/message`) return json({ data: [recordedAssistant], cursor: {} })
      if (url.pathname === `/api/session/${session.id}/inbox`) return json({ data: [] })
      return undefined
    },
  })
  await app.ready
  await app.waitForFrame((frame) => frame.includes("savedprogress") && frame.replace(/\s+/g, "").includes("hosted-raw") && frame.includes("saved-error"))
  const frame = app.captureCharFrame()
  for (const text of ["partial-read", "saved-output", "saved-partial", "saved-task", "Unavailable file:", "outcome unknown"])
    expect(frame).toContain(text)
  expect(frame).not.toMatch(/Running command|Preparing read|bad title|\u001b|\u0007/)
  expect(app.renderer.liveRequestCount).toBe(0)
  const row = frame.split("\n").findIndex((line) => line.includes("printf saved-"))
  expect(row).toBeGreaterThanOrEqual(0)
  await app.mockMouse.click(10, row)
  await app.renderOnce()
  expect(requests.some((request) => request.includes("sh_must_not_read") || request.includes("ses_must_not_adopt") || request.includes("/output"))).toBe(false)
  if (process.env.TUI_HISTORY_QA_DIR)
    await Bun.write(`${process.env.TUI_HISTORY_QA_DIR}/recorded-tools-session-${width}.txt`, frame)
})

test.each(["low", "medium", "high"] satisfies Verbosity[])("keeps recorded tools outside native activity groups at %s", (verbosity) => {
  for (const part of recordedAssistant.content) {
    if (part.type !== "tool") continue
    expect(partPath(part, verbosity)).toEqual([])
  }
  expect(partPath({ type: "tool", name: "read" }, verbosity)).toEqual(verbosity === "low" ? ["activity", "exploration"] : ["exploration"])
})

test("keeps native shell output expansion live without recorded provenance", async () => {
  const session = {
    id: "ses_native_control", title: "Native control", projectID: "project", location: { directory },
    cost: 0, tokens: { input: 0, output: 0, reasoning: 0, cache: { read: 0, write: 0 } },
    time: { created: 0, updated: 0 },
  }
  let reads = 0
  await using app = await createAppFixture({
    width: 100, height: 30, args: { sessionID: session.id },
    config: { animations: false, tabs: { mode: "off" } },
    fetch: (url) => {
      if (url.pathname === "/api/session") return json({ data: [session], cursor: {} })
      if (url.pathname === `/api/session/${session.id}`) return json({ data: session })
      if (url.pathname === `/api/session/${session.id}/message`)
        return json({ data: [{ ...recordedAssistant, content: [{
          type: "tool", id: "native-control", name: "bash", time: { created: 3 },
          state: { status: "running", input: { command: "native-control-command" }, metadata: { shellID: "sh_native_control" } },
        }] }], cursor: {} })
      if (url.pathname === `/api/session/${session.id}/inbox`) return json({ data: [] })
      if (url.pathname.includes("sh_native_control") && url.pathname.endsWith("/output")) {
        reads++
        return json({ data: { output: "native-output", cursor: 13, size: 13, truncated: false } })
      }
      return undefined
    },
  })
  await app.ready
  await app.waitForFrame((frame) => frame.includes("native-control-command"))
  const frame = app.captureCharFrame()
  expect(frame).not.toContain("Recorded bash")
  const row = frame.split("\n").findIndex((line) => line.includes("native-control-command"))
  await app.mockMouse.click(10, row)
  await app.waitForFrame((frame) => frame.includes("native-output"))
  expect(reads).toBeGreaterThan(0)
})

test.each([44, 100])("shows recordings and file-only history in the full session at width %s", async (width) => {
  const session = {
    id: "ses_recorded",
    title: "Imported history",
    projectID: "project",
    location: { directory },
    cost: 0,
    tokens: { input: 0, output: 0, reasoning: 0, cache: { read: 0, write: 0 } },
    time: { created: 0, updated: 0 },
  }
  const requests: string[] = []
  await using app = await createAppFixture({
    width,
    height: 40,
    args: { sessionID: session.id },
    config: { animations: false, tabs: { mode: "off" }, keybinds: { "session.timeline": "f6" } },
    fetch: (url) => {
      requests.push(url.pathname)
      if (url.pathname === "/api/session") return json({ data: [session], cursor: {} })
      if (url.pathname === `/api/session/${session.id}`) return json({ data: session })
      if (url.pathname === `/api/session/${session.id}/message`)
        return json({ data: [recordedUser, { ...recordedShell(2), id: "msg_recorded_completed" }, recordedShell()], cursor: {} })
      if (url.pathname === `/api/session/${session.id}/inbox`) return json({ data: [] })
      return undefined
    },
  })
  await app.ready
  await app.waitForFrame((frame) => frame.includes("Unavailable file: missing.png") && (frame.match(/recorded output/g)?.length ?? 0) === 2)
  const frame = app.captureCharFrame()
  expect(frame.match(/Recorded shell \(outcome unknown\)/g)).toHaveLength(2)
  expect(frame.replace(/[\s┃]/g, "")).toContain("notaURI:%zz")
  expect(requests.some((request) => request.includes("/output") || request.includes("not a URI"))).toBe(false)
  if (process.env.TUI_HISTORY_QA_DIR)
    await Bun.write(`${process.env.TUI_HISTORY_QA_DIR}/recorded-session-${width}.txt`, frame)
  app.mockInput.pressKey("F6")
  await app.waitForFrame((frame) => frame.includes("Timeline") && app.renderer.currentFocusedRenderable instanceof InputRenderable)
  app.mockInput.typeText("printf")
  await app.waitForFrame((frame) => frame.includes("printf") && frame.includes("Timeline"))
  app.mockInput.pressEnter()
  await app.waitForFrame((frame) => frame.includes("Message Actions") && frame.includes("Copy"))
  const detail = app.captureCharFrame()
  expect(detail).not.toContain("undo messages and file changes")
  expect(detail).not.toContain("create a new session")
  if (process.env.TUI_HISTORY_QA_DIR)
    await Bun.write(`${process.env.TUI_HISTORY_QA_DIR}/recorded-detail-${width}.txt`, detail)
})

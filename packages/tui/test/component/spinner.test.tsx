/** @jsxImportSource @opentui/solid */
import { expect, mock, test } from "bun:test"
import { mkdir } from "node:fs/promises"
import path from "node:path"
import { testRender } from "@opentui/solid"
import { TuiConfigProvider } from "../../src/config"
import { KVProvider } from "../../src/context/kv"
import { ThemeProvider } from "../../src/context/theme"
import { TestTuiContexts } from "../fixture/tui-environment"
import { tmpdir } from "../fixture/fixture"
import { createTuiResolvedConfig } from "../fixture/tui-runtime"

test("spinner renders when the opentui-spinner solid extension is unavailable", async () => {
  await using tmp = await tmpdir()
  const state = path.join(tmp.path, "state")
  await mkdir(state, { recursive: true })
  await Bun.write(path.join(state, "kv.json"), JSON.stringify({ animations_enabled: true }))
  mock.module("opentui-spinner/solid", () => ({}))

  try {
    const { Spinner } = await import("../../src/component/spinner")
    const app = await testRender(
      () => (
        <TestTuiContexts directory={tmp.path} paths={{ home: tmp.path, state, worktree: tmp.path }}>
          <TuiConfigProvider config={createTuiResolvedConfig()}>
            <KVProvider>
              <ThemeProvider mode="dark">
                <Spinner>Loading</Spinner>
              </ThemeProvider>
            </KVProvider>
          </TuiConfigProvider>
        </TestTuiContexts>
      ),
      { width: 24, height: 3 },
    )

    try {
      expect(await captureSettledFrame(app)).toContain("Loading")
    } finally {
      app.renderer.destroy()
    }
  } finally {
    mock.restore()
  }
})

async function captureSettledFrame(app: Awaited<ReturnType<typeof testRender>>) {
  for (let attempt = 0; attempt < 5; attempt++) {
    await app.renderOnce()
    const frame = app.captureCharFrame()
    if (frame.trim().length > 0) return frame
    await Bun.sleep(25)
  }
  return app.captureCharFrame()
}

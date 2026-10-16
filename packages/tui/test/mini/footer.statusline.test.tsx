/** @jsxImportSource @opentui/solid */
import { expect, test } from "bun:test"
import { ManualClock } from "@opentui/core/testing"
import { testRender } from "@opentui/solid"
import { createSignal } from "solid-js"
import { Keymap } from "../../src/context/keymap"
import { RunFooterView } from "../../src/mini/footer.view"
import { RUN_THEME_FALLBACK } from "../../src/mini/theme"
import { resolveMiniSettings } from "../../src/mini/runtime.boot"
import type { FooterState, FooterSubagentState, FooterView } from "../../src/mini/types"
import { createTuiResolvedConfig } from "../fixture/tui-runtime"

test("running footer keeps interrupt hint beside a one-cell spinner", async () => {
  const app = await renderRunningFooter()

  try {
    const positions: number[] = []
    const gaps: number[] = []

    for (let frame = 0; frame < 3; frame++) {
      await app.renderOnce()
      const statusline = app
        .captureCharFrame()
        .split("\n")
        .find((line) => line.includes("BUILD") && line.includes("esc interrupt"))

      expect(statusline).toBeDefined()
      if (!statusline) throw new Error("Footer statusline not found")
      const modeEnd = statusline.indexOf("BUILD") + "BUILD".length
      const interruptStart = statusline.indexOf("esc interrupt")
      positions.push(interruptStart)
      gaps.push(interruptStart - modeEnd)
      expect(statusline.slice(modeEnd, interruptStart)).not.toMatch(/[■⬝]/)
      expect(app.renderer.root.findDescendantById("mini-work-spinner")?.width).toBe(1)
      app.clock.advance(80)
    }

    expect(new Set(positions).size).toBe(1)
    expect(Math.max(...gaps)).toBeLessThanOrEqual(5)
  } finally {
    app.cleanup()
  }
})

async function renderRunningFooter() {
  const config = createTuiResolvedConfig()
  const [view] = createSignal<FooterView>({ type: "prompt" })
  const [state] = createSignal<FooterState>({
    phase: "running",
    status: "",
    notice: "",
    model: "",
    usage: undefined,
    first: false,
    interrupt: 0,
    exit: 0,
  })
  const [subagents] = createSignal<FooterSubagentState>({ tabs: [], details: {}, permissions: [], forms: [] })
  const clock = new ManualClock()

  function Harness() {
    return (
      <Keymap.Provider config={config}>
        <RunFooterView
          directory={() => "/tmp"}
          findFiles={async () => []}
          agents={() => []}
          references={() => []}
          currentAgent={() => "BUILD"}
          currentAgentID={() => "build"}
          commands={() => []}
          providers={() => undefined}
          currentModel={() => undefined}
          variants={() => []}
          currentVariant={() => undefined}
          state={state}
          view={view}
          subagent={subagents}
          theme={() => RUN_THEME_FALLBACK}
          tuiConfig={config}
          mono={false}
          miniSettings={() => resolveMiniSettings()}
          onSubmit={() => true}
          onPermissionReply={() => {}}
          onFormReply={() => {}}
          onFormCancel={() => {}}
          onCycle={() => {}}
          onInterrupt={() => false}
          onEditorOpen={async () => undefined}
          onInputClear={() => {}}
          onExit={() => {}}
          onModelSelect={() => {}}
          onAgentSelect={() => {}}
          onVariantSelect={() => {}}
          onRows={() => {}}
          onLayout={() => {}}
          onStatus={() => {}}
          onMiniSettingChange={() => {}}
        />
      </Keymap.Provider>
    )
  }

  const app = await testRender(
    () => (
      <box width={100} height={8}>
        <Harness />
      </box>
    ),
    { width: 100, height: 8, kittyKeyboard: true, clock },
  )

  return {
    ...app,
    clock,
    cleanup() {
      app.renderer.currentFocusedRenderable?.blur()
      app.renderer.currentFocusedEditor?.blur()
      app.renderer.destroy()
    },
  }
}

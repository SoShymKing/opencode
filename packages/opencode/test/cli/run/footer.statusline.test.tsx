/** @jsxImportSource @opentui/solid */
import { expect, test } from "bun:test"
import { createDefaultOpenTuiKeymap } from "@opentui/keymap/opentui"
import { testRender, useRenderer } from "@opentui/solid"
import { createSignal } from "solid-js"
import { OpencodeKeymapProvider, registerOpencodeKeymap } from "@opencode-ai/tui/keymap"
import { RunFooterView } from "@/cli/cmd/run/footer.view"
import { RUN_THEME_FALLBACK } from "@/cli/cmd/run/theme"
import type { FooterState, FooterSubagentState, FooterView } from "@/cli/cmd/run/types"
import { createTuiResolvedConfig } from "../../fixture/tui-runtime"

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
      await Bun.sleep(45)
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
    queue: 0,
    model: "gpt-5",
    duration: "",
    usage: "",
    first: false,
    interrupt: 0,
    exit: 0,
  })
  const [subagents] = createSignal<FooterSubagentState>({ tabs: [], details: {}, permissions: [], questions: [] })
  let offKeymap: (() => void) | undefined

  function Harness() {
    const renderer = useRenderer()
    const keymap = createDefaultOpenTuiKeymap(renderer)
    offKeymap = registerOpencodeKeymap(keymap, renderer, config)

    return (
      <OpencodeKeymapProvider keymap={keymap}>
        <RunFooterView
          directory="/tmp"
          findFiles={async () => []}
          agents={() => []}
          resources={() => []}
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
          backgroundSubagents={true}
          agent="opencode"
          onSubmit={() => true}
          onPermissionReply={() => {}}
          onQuestionReply={() => {}}
          onQuestionReject={() => {}}
          onCycle={() => {}}
          onInterrupt={() => false}
          onEditorOpen={async () => undefined}
          onInputClear={() => {}}
          onExit={() => {}}
          onModelSelect={() => {}}
          onVariantSelect={() => {}}
          onRows={() => {}}
          onLayout={() => {}}
          onStatus={() => {}}
          onQueuedRemove={async () => true}
        />
      </OpencodeKeymapProvider>
    )
  }

  const app = await testRender(
    () => (
      <box width={100} height={8}>
        <Harness />
      </box>
    ),
    { width: 100, height: 8, kittyKeyboard: true },
  )

  return {
    ...app,
    cleanup() {
      app.renderer.currentFocusedRenderable?.blur()
      app.renderer.currentFocusedEditor?.blur()
      offKeymap?.()
      app.renderer.destroy()
    },
  }
}

/** @jsxImportSource @opentui/solid */
import { expect, jest, test } from "bun:test"
import { RGBA } from "@opentui/core"
import { getComponentCatalogue, testRender } from "@opentui/solid"
import { ConfigProvider } from "../../src/config"
import { ThemeProvider } from "../../src/context/theme"
import { Spinner, SpinnerIcon, SPINNER_FRAMES } from "../../src/component/spinner"
import { emptyThemeSource } from "../fixture/fixture"
import { createTuiResolvedConfig } from "../fixture/tui-runtime"

test("spinner renders when the opentui-spinner solid extension is unavailable", async () => {
  const catalogue = getComponentCatalogue()
  const registered = catalogue.spinner
  delete catalogue.spinner
  try {
    const app = await testRender(() => (
      <ConfigProvider config={createTuiResolvedConfig({ animations: true })}>
        <ThemeProvider mode="dark" source={emptyThemeSource}>
          <Spinner>Loading</Spinner>
        </ThemeProvider>
      </ConfigProvider>
    ), { width: 24, height: 3 })
    try {
      await app.renderOnce()
      await app.renderOnce()
      expect(app.captureCharFrame()).toContain("Loading")
      expect(SPINNER_FRAMES.some((glyph) => app.captureCharFrame().includes(glyph))).toBe(true)
    } finally {
      app.renderer.destroy()
    }
  } finally {
    if (registered) catalogue.spinner = registered
  }
})

test.each([24, 80])("spinner renders native text at %i columns without an extension renderable", async (width) => {
  for (const animations of [false, true]) {
    for (const shimmer of [false, true]) {
      const app = await testRender(
        () => (
          <ConfigProvider config={createTuiResolvedConfig({ animations })}>
            <ThemeProvider mode="dark" source={emptyThemeSource}>
              <Spinner shimmer={shimmer ? RGBA.fromHex("#ffffff") : undefined}>Loading</Spinner>
            </ThemeProvider>
          </ConfigProvider>
        ),
        { width, height: 3 },
      )
      try {
        await app.renderOnce()
        await app.renderOnce()
        const frame = app.captureCharFrame()
        expect(frame).toContain("Loading")
        expect(animations ? SPINNER_FRAMES.some((glyph) => frame.includes(glyph)) : frame.includes("⋯")).toBe(true)
        expect(frame).not.toMatch(/[■⬝]/)
      } finally {
        app.renderer.destroy()
      }
    }
  }
})

test("prompt spinner frames stay one cell wide across animation and cleanup", async () => {
  jest.useFakeTimers()
  const app = await testRender(() => <SpinnerIcon frames={SPINNER_FRAMES} interval={80} />, { width: 4, height: 1 })
  try {
    const glyphs = new Set<string>()
    for (const expected of SPINNER_FRAMES) {
      await app.renderOnce()
      await app.renderOnce()
      const glyph = app.captureCharFrame().trim()
      expect(glyph).toBe(expected)
      expect(Array.from(glyph)).toHaveLength(1)
      glyphs.add(glyph)
      jest.advanceTimersByTime(80)
    }
    expect(glyphs.size).toBe(SPINNER_FRAMES.length)
  } finally {
    app.renderer.destroy()
    jest.useRealTimers()
  }
})

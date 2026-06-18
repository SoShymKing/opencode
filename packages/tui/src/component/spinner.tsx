import { For, Show, createSignal, onCleanup, onMount } from "solid-js"
import { useTheme } from "../context/theme"
import { useKV } from "../context/kv"
import { useRenderer, type JSX } from "@opentui/solid"
import type { ColorInput, RGBA } from "@opentui/core"
import type { ColorGenerator } from "opentui-spinner"

export const SPINNER_FRAMES = ["⠋", "⠙", "⠹", "⠸", "⠼", "⠴", "⠦", "⠧", "⠇", "⠏"]

export function SpinnerIcon(props: { frames?: readonly string[]; interval?: number; color?: ColorInput | ColorGenerator }) {
  const renderer = useRenderer()
  const frames = () => (props.frames?.length ? props.frames : SPINNER_FRAMES)
  const [frameIndex, setFrameIndex] = createSignal(0)
  onMount(() => {
    const timer = setInterval(() => {
      setFrameIndex((index) => (index + 1) % frames().length)
      renderer.requestRender()
    }, props.interval ?? 80)
    onCleanup(() => clearInterval(timer))
  })
  const frame = () => frames()[frameIndex() % frames().length] ?? ""
  const chars = () => Array.from(frame())
  const color = (charIndex: number) =>
    typeof props.color === "function" ? props.color(frameIndex(), charIndex, frames().length, chars().length) : props.color
  return (
    <text wrapMode="none">
      <For each={chars()}>{(char, index) => <span style={{ fg: color(index()) }}>{char}</span>}</For>
    </text>
  )
}

export function Spinner(props: { children?: JSX.Element; color?: RGBA }) {
  const { theme } = useTheme()
  const kv = useKV()
  const color = () => props.color ?? theme.textMuted
  return (
    <Show when={kv.get("animations_enabled", true)} fallback={<text fg={color()}>⋯ {props.children}</text>}>
      <box flexDirection="row" gap={1}>
        <SpinnerIcon frames={SPINNER_FRAMES} interval={80} color={color()} />
        <Show when={props.children}>
          <text fg={color()}>{props.children}</text>
        </Show>
      </box>
    </Show>
  )
}

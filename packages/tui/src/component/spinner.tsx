import { createEffect, For, Show, createSignal, onCleanup, onMount } from "solid-js"
import { useTheme } from "../context/theme"
import { useConfig } from "../config"
import { useRenderer, type JSX } from "@opentui/solid"
import type { ColorInput, RGBA } from "@opentui/core"
import type { ColorGenerator } from "opentui-spinner"
import { registerOpencodeSpinner } from "./register-spinner"
import { SPINNER_FRAMES } from "./spinner-frames"
import { ShimmerText } from "./shimmer-text"

export { SPINNER_FRAMES } from "./spinner-frames"

registerOpencodeSpinner()

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
    <text wrapMode="none" bg="transparent">
      <For each={chars()}>{(char, index) => <span style={{ fg: color(index()) }}>{char}</span>}</For>
    </text>
  )
}

export function Spinner(props: { children?: JSX.Element; color?: RGBA; shimmer?: RGBA }) {
  const theme = useTheme()
  const config = useConfig().data
  const color = () => props.color ?? theme.text.muted
  const [frame, setFrame] = createSignal(0)
  createEffect(() => {
    if (!(config.animations ?? true) || !props.shimmer) return
    const timer = setInterval(() => setFrame((value) => (value + 1) % SPINNER_FRAMES.length), 80)
    onCleanup(() => clearInterval(timer))
  })
  // A bare spinner beside a long sibling shrinks to a fractional width that rounds to 0, so the
  // glyph paints on top of the row gap. Only a labeled spinner may shrink (to wrap its text).
  return (
    <Show
      when={config.animations ?? true}
      fallback={<text fg={color()}>{props.children ? <>⋯ {props.children}</> : "⋯"}</text>}
    >
      <Show
        when={props.shimmer}
        fallback={
          <box flexDirection="row" gap={1} flexShrink={props.children ? 1 : 0}>
            <box flexShrink={0}>
              <SpinnerIcon frames={SPINNER_FRAMES} interval={80} color={color()} />
            </box>
            <Show when={props.children}>
              <text fg={color()}>{props.children}</text>
            </Show>
          </box>
        }
      >
        {(shimmer) => (
          <ShimmerText fg={color()} shimmer={shimmer()}>
            {SPINNER_FRAMES[frame()]} {props.children}
          </ShimmerText>
        )}
      </Show>
    </Show>
  )
}

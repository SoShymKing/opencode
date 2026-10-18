import { Effect, Layer } from "effect"
import type { DatabasePreparation } from "./preparation.js"

export type Status = { readonly status: "completed" }

export const layer = Layer.empty
export const configured = (_observer?: DatabasePreparation.Observer) => layer
export function status() {
  return Effect.succeed({ status: "completed" } as const)
}
export function run() {
  return Effect.succeed({ status: "completed" } as const)
}

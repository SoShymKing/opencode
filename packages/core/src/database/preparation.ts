export * as DatabasePreparation from "./preparation.js"

import { Effect } from "effect"

export type Observer = (active: boolean) => Promise<void>

export function track<A, E, R>(observer: Observer | undefined, run: (begin: Effect.Effect<void>) => Effect.Effect<A, E, R>) {
  return Effect.suspend(() => {
    let active = false
    const begin = Effect.suspend(() => {
      if (active || !observer) return Effect.void
      active = true
      return Effect.promise(() => observer(true))
    }).pipe(Effect.uninterruptible)
    return run(begin).pipe(Effect.ensuring(Effect.suspend(() =>
      active && observer ? Effect.promise(() => observer(false)) : Effect.void,
    )))
  })
}

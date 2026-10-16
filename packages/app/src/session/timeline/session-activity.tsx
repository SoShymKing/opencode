import type { SessionStatusActivity, SessionStatusTerminal } from "@opencode/client/promise"
import { TextShimmer } from "@opencode/ui/text-shimmer"
import { createEffect, onCleanup, Show } from "solid-js"
import { createStore } from "solid-js/store"
import { useLanguage } from "@/runtime/i18n/language"

export function SessionActivity(props: {
  activity?: SessionStatusActivity
  terminal?: SessionStatusTerminal
  tool?: "pending" | "running"
}) {
  const language = useLanguage()
  const [clock, setClock] = createStore({ now: Date.now() })

  createEffect(() => {
    const received = props.activity?.lastStreamEventAt

    if (received === undefined || props.activity?.model === "none") return
    setClock("now", Date.now())
    const timer = setInterval(() => setClock("now", Date.now()), 1000)
    onCleanup(() => clearInterval(timer))
  })

  return (
    <div role="status" class="flex min-w-0 flex-wrap items-center gap-x-2 gap-y-1">
      <Show when={props.activity}>
        {(activity) => (
          <>
            <TextShimmer
              text={language.activityCount(activity().model, activity().streamEventCount)}
              active={activity().model !== "none"}
            />
            <Show when={activity().model !== "none" && activity().lastStreamEventAt !== undefined}>
              <span class="text-v2-text-text-muted">
                {language.activityReceiptAge(
                  Math.max(0, Math.floor((clock.now - (activity().lastStreamEventAt ?? clock.now)) / 1000)),
                )}
              </span>
            </Show>
            <Show when={props.tool}>
              {(tool) => <span>{language.t(`session.activity.tools.${tool()}`)}</span>}
            </Show>
          </>
        )}
      </Show>
      <Show when={props.terminal}>
        {(terminal) => <span>{language.t(`session.activity.${terminal().reason}`)}</span>}
      </Show>
    </div>
  )
}

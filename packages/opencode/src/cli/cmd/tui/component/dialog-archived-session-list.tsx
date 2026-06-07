import type { Session } from "@opencode-ai/sdk/v2"
import { createMemo, createResource, createSignal, onMount } from "solid-js"
import { useDialog } from "@tui/ui/dialog"
import { DialogSelect, type DialogSelectOption } from "@tui/ui/dialog-select"
import { useSDK } from "@tui/context/sdk"
import { useSync } from "@tui/context/sync"
import { useToast } from "@tui/ui/toast"
import { Locale } from "@/util/locale"
import { errorMessage } from "@/util/error"

const archivedSessionLimit = 200

export function DialogArchivedSessionList() {
  const dialog = useDialog()
  const sdk = useSDK()
  const sync = useSync()
  const toast = useToast()
  const [restoring, setRestoring] = createSignal<string>()

  const [sessions, { refetch }] = createResource(async () => {
    const archived: Session[] = []
    let cursor: number | undefined
    while (archived.length < archivedSessionLimit) {
      const result = await sdk.client.experimental.session.list({
        archived: true,
        roots: true,
        limit: archivedSessionLimit,
        cursor,
      })
      if (result.error) throw result.error
      archived.push(...(result.data ?? []).filter((session) => session.time.archived !== undefined))
      const next = result.response.headers.get("x-next-cursor")
      if (!next) break
      cursor = Number(next)
    }
    return archived
      .toSorted((a, b) => (b.time.archived ?? b.time.updated) - (a.time.archived ?? a.time.updated))
      .slice(0, archivedSessionLimit)
  })

  const options = createMemo<DialogSelectOption<string | undefined>[]>(() => {
    if (sessions.loading) {
      return [{ title: "Loading archived sessions...", value: undefined, category: "Status" }]
    }
    if (sessions.error) {
      return [
        {
          title: "Failed to load archived sessions",
          description: errorMessage(sessions.error),
          value: undefined,
          category: "Status",
        },
      ]
    }
    const list = sessions()
    if (!list?.length) {
      return [{ title: "No archived sessions", value: undefined, category: "Status" }]
    }
    return list.map((session) => option(session, restoring()))
  })

  async function restore(sessionID: string) {
    if (restoring()) return
    setRestoring(sessionID)
    const result = await sdk.client.session.update({ sessionID, time: { archived: null } }).catch((err) => ({
      error: err,
    }))
    if (result.error) {
      toast.show({
        variant: "error",
        title: "Failed to restore session",
        message: errorMessage(result.error),
      })
      setRestoring(undefined)
      return
    }
    toast.show({ variant: "info", message: "Session restored" })
    const refreshed = await Promise.all([refetch(), sync.session.refresh()])
      .then(() => ({ error: undefined }))
      .catch((err) => ({ error: err }))
    setRestoring(undefined)
    if (refreshed.error) {
      toast.show({
        variant: "error",
        title: "Failed to refresh sessions",
        message: errorMessage(refreshed.error),
      })
    }
  }

  onMount(() => {
    dialog.setSize("large")
  })

  return (
    <DialogSelect
      title="Archived sessions"
      options={options()}
      locked={sessions.loading || restoring() !== undefined}
      onSelect={(selected) => {
        if (!selected.value) return
        void restore(selected.value)
      }}
    />
  )
}

function option(session: Session, restoring: string | undefined): DialogSelectOption<string> {
  const archived = session.time.archived ?? session.time.updated
  return {
    title: restoring === session.id ? "Restoring..." : session.title,
    value: session.id,
    category: new Date(archived).toDateString(),
    footer: Locale.time(archived),
  }
}

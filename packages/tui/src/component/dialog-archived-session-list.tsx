import type { SessionInfo } from "@opencode/client"
import { createMemo, createResource, createSignal, onMount } from "solid-js"
import { useDialog } from "../ui/dialog"
import { DialogSelect, type DialogSelectOption } from "../ui/dialog-select"
import { useClient } from "../context/client"
import { useData } from "../context/data"
import { useToast } from "../ui/toast"
import { Locale } from "../util/locale"
import { errorMessage } from "../util/error"
import { listSessionPages } from "../util/session-list-window"
import { withTimestampedFallback } from "@opencode/util/session-title-fallback"

const archivedSessionLimit = 200

export function DialogArchivedSessionList() {
  const dialog = useDialog()
  const client = useClient()
  const data = useData()
  const toast = useToast()
  const [restoring, setRestoring] = createSignal<string>()

  const [sessions, { refetch }] = createResource(async () => {
    const archived = await listSessionPages(
      client.api,
      {
        archived: true,
        parentID: null,
        limit: archivedSessionLimit,
        order: "desc",
      },
      archivedSessionLimit,
    )
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
    const result = await client.api.session.restore({ sessionID }).then(
      () => ({ error: undefined }),
      (error: unknown) => ({ error }),
    )
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
    data.session.invalidate(sessionID)
    const refreshed = await Promise.all([refetch(), data.session.sync(sessionID)])
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

function option(session: SessionInfo, restoring: string | undefined): DialogSelectOption<string> {
  const archived = session.time.archived ?? session.time.updated
  return {
    title: restoring === session.id ? "Restoring..." : withTimestampedFallback(session),
    value: session.id,
    category: new Date(archived).toDateString(),
    footer: Locale.time(archived),
  }
}

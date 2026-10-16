import type { SessionApi, SessionInfo } from "@opencode/client/promise"
import { Dialog, DialogBody, DialogHeader, DialogTitle } from "@opencode/ui/dialog"
import { List } from "@opencode/ui/list"
import { useDialog } from "@opencode/ui/context/dialog"
import { onMount } from "solid-js"
import { createStore } from "solid-js/store"
import { useLanguage } from "@/runtime/i18n/language"
import { errorMessage } from "@/shell/layout/helpers"
import { showToast } from "@/shell/notifications/toast"
import { listAllSessions } from "@/session/list"
import { sessionLabel } from "@/session/title"
import { restoreHomeSession } from "./archive"

export function ArchivedSessionsDialog(props: {
  api: Pick<SessionApi, "list" | "restore" | "get">
  directories?: string[]
  onRestore: (session: SessionInfo) => void
}) {
  const language = useLanguage()
  const dialog = useDialog()

  const [state, setState] = createStore<{
    sessions: SessionInfo[]
    loading: boolean
    restoring?: string
    error?: string
  }>({ sessions: [], loading: true })

  onMount(() => {
    void Promise.all(
      (props.directories ?? [undefined]).map((directory) =>
        listAllSessions(props.api, { directory, archived: true, parentID: null, order: "desc" }),
      ),
    )
      .then((pages) => setState("sessions", [...new Map(pages.flat().map((item) => [item.id, item])).values()]))
      .catch((cause) => setState("error", errorMessage(cause, language.t("common.requestFailed"))))
      .finally(() => setState("loading", false))
  })

  const restore = async (session: SessionInfo | undefined) => {
    if (!session || state.restoring) return
    setState("restoring", session.id)
    await restoreHomeSession(props.api, session.id)
      .then((info) => {
        props.onRestore(info)
        dialog.close()
      })
      .catch((cause) =>
        showToast({
          title: language.t("toast.session.restore.failed.title"),
          description: errorMessage(cause, language.t("common.requestFailed")),
        }),
      )
      .finally(() => setState("restoring", undefined))
  }

  return (
    <Dialog>
      <DialogHeader>
        <DialogTitle>{language.t("command.session.archived.list")}</DialogTitle>
      </DialogHeader>
      <DialogBody>
        <List
          class="flex-1 min-h-0"
          search={{ placeholder: language.t("dialog.archivedSessions.search.placeholder"), autofocus: true }}
          emptyMessage={
            state.loading
              ? language.t("dialog.archivedSessions.loading")
              : state.error ?? language.t("dialog.archivedSessions.empty")
          }
          key={(session) => session.id}
          items={state.sessions}
          filterKeys={["title"]}
          onSelect={restore}
        >
          {(session) => (
            <div class="flex min-w-0 w-full items-center justify-between gap-3 text-start">
              <div class="flex min-w-0 flex-col">
                <span dir="auto" class="truncate">{sessionLabel(session)}</span>
                <span dir="ltr" class="truncate text-v2-text-text-muted">{session.location.directory}</span>
              </div>
              <span class="shrink-0 text-v2-text-text-muted">
                {language.t(
                  state.restoring === session.id ? "dialog.archivedSessions.restoring" : "dialog.archivedSessions.restore",
                )}
              </span>
            </div>
          )}
        </List>
      </DialogBody>
    </Dialog>
  )
}

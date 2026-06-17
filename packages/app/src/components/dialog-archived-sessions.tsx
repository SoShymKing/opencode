import { getFilename } from "@opencode-ai/core/util/path"
import { Dialog } from "@opencode-ai/ui/dialog"
import { Icon } from "@opencode-ai/ui/icon"
import { List } from "@opencode-ai/ui/list"
import { useParams } from "@solidjs/router"
import { createMemo, onMount, Show } from "solid-js"
import { createStore } from "solid-js/store"
import type { GlobalSession } from "@opencode-ai/sdk/v2/client"
import { useLanguage } from "@/context/language"
import { useLayout } from "@/context/layout"
import { useServerSDK } from "@/context/server-sdk"
import { useServerSync } from "@/context/server-sync"
import { displayName } from "@/pages/layout/helpers"
import { decode64 } from "@/utils/base64"
import { pathKey } from "@/utils/path-key"
import { formatServerError } from "@/utils/server-errors"
import { getRelativeTime } from "@/utils/time"
import { showToast } from "@/utils/toast"

type ArchivedGlobalSession = GlobalSession & {
  time: GlobalSession["time"] & { archived: number }
}

type ArchivedSessionItem = {
  id: string
  title: string
  directory: string
  label: string
  archivedLabel: string
  updatedLabel?: string
}

const archivedSessionLimit = 200
const archivedSessionPageLimit = 100

const isArchivedSession = (session: GlobalSession): session is ArchivedGlobalSession =>
  session.time.archived !== undefined

export function DialogArchivedSessions() {
  const params = useParams()
  const language = useLanguage()
  const layout = useLayout()
  const serverSDK = useServerSDK()
  const serverSync = useServerSync()
  const [store, setStore] = createStore({
    sessions: [] as ArchivedSessionItem[],
    loading: true,
    error: undefined as string | undefined,
    restoring: undefined as string | undefined,
  })
  let loadToken = 0

  const projectDirectory = createMemo(() => decode64(params.dir) ?? "")
  const project = createMemo(() => {
    const directory = projectDirectory()
    if (!directory) return
    const key = pathKey(directory)
    return layout.projects
      .list()
      .find((item) => pathKey(item.worktree) === key || item.sandboxes?.some((sandbox) => pathKey(sandbox) === key))
  })
  const workspaceDirectories = createMemo(() => {
    const current = project()
    const directory = projectDirectory()
    if (!current) return directory ? [directory] : []
    return [current.worktree, ...(current.sandboxes ?? [])]
  })

  const relativeTime = (time: number) => getRelativeTime(new Date(time).toISOString(), language.t)

  const label = (session: GlobalSession) => {
    const current = project()
    const projectLabel = session.project?.name ?? (current ? displayName(current) : getFilename(session.directory))
    const kind =
      current && pathKey(session.directory) === pathKey(current.worktree)
        ? language.t("workspace.type.local")
        : language.t("workspace.type.sandbox")
    const [child] = serverSync().child(session.directory, { bootstrap: false })
    const home = serverSync().data.path.home
    const path = home ? session.directory.replace(home, "~") : session.directory
    const name = child.vcs?.branch ?? getFilename(session.directory)
    return `${projectLabel} / ${kind} : ${name || path}`
  }

  const item = (session: ArchivedGlobalSession): ArchivedSessionItem => ({
    id: session.id,
    title: session.title || language.t("command.session.new"),
    directory: session.directory,
    label: label(session),
    archivedLabel: language.t("dialog.archivedSessions.archived", { time: relativeTime(session.time.archived) }),
    updatedLabel:
      session.time.updated === session.time.archived
        ? undefined
        : language.t("dialog.archivedSessions.updated", { time: relativeTime(session.time.updated) }),
  })

  async function fetchArchivedSessions() {
    const directories = workspaceDirectories()
    if (directories.length === 0) return []

    const seenDirectories = new Set<string>()
    const sessionsByKey = new Map<string, ArchivedGlobalSession>()

    for (const directory of directories) {
      const directoryKey = pathKey(directory)
      if (seenDirectories.has(directoryKey)) continue
      seenDirectories.add(directoryKey)

      let cursor: number | undefined
      while (true) {
        const result = await serverSDK().client.experimental.session.list({
          archived: true,
          roots: true,
          directory,
          limit: archivedSessionPageLimit,
          cursor,
        })

        for (const session of (result.data ?? []).filter(isArchivedSession)) {
          sessionsByKey.set(`${pathKey(session.directory)}:${session.id}`, session)
        }

        const next = result.response.headers.get("x-next-cursor")
        if (!next) break
        const nextCursor = Number(next)
        if (!Number.isFinite(nextCursor)) break
        cursor = nextCursor
      }
    }

    return [...sessionsByKey.values()]
      .sort((a, b) => b.time.archived - a.time.archived)
      .slice(0, archivedSessionLimit)
      .map(item)
  }

  function loadArchivedSessions() {
    const token = ++loadToken
    setStore("loading", true)
    setStore("error", undefined)
    return fetchArchivedSessions()
      .then((sessions) => {
        if (token !== loadToken) return
        setStore("sessions", sessions)
      })
      .catch((error: unknown) => {
        if (token !== loadToken) return
        setStore("sessions", [])
        setStore("error", formatServerError(error, language.t))
      })
      .finally(() => {
        if (token !== loadToken) return
        setStore("loading", false)
      })
  }

  function restoreSession(session: ArchivedSessionItem | undefined) {
    if (!session) return
    if (store.restoring) return
    setStore("restoring", session.id)
    void serverSDK().client.session
      .update({ sessionID: session.id, time: { archived: null } })
      .then(() => {
        showToast({ title: language.t("toast.session.restore.success.title") })
        return Promise.all([loadArchivedSessions(), serverSync().project.loadSessions(session.directory)]).catch(
          (error: unknown) => {
            showToast({
              variant: "error",
              title: language.t("common.requestFailed"),
              description: formatServerError(error, language.t),
            })
          },
        )
      })
      .catch((error: unknown) => {
        showToast({
          variant: "error",
          title: language.t("toast.session.restore.failed.title"),
          description: formatServerError(error, language.t),
        })
      })
      .finally(() => setStore("restoring", undefined))
  }

  const emptyMessage = () => {
    if (store.loading) return language.t("dialog.archivedSessions.loading")
    if (store.error) return store.error
    return language.t("dialog.archivedSessions.empty")
  }

  onMount(() => {
    void loadArchivedSessions()
  })

  return (
    <Dialog title={language.t("command.session.archived.list")} class="pt-3 pb-0 !max-h-[480px]" transition>
      <List
        class="flex-1 px-3 min-h-0 [&_[data-slot=list-scroll]]:flex-1 [&_[data-slot=list-scroll]]:min-h-0 [&_[data-slot=list-item]]:min-h-14"
        search={{ placeholder: language.t("dialog.archivedSessions.search.placeholder"), autofocus: true }}
        emptyMessage={emptyMessage()}
        key={(session) => `${session.directory}:${session.id}`}
        items={store.sessions}
        filterKeys={["title", "label", "archivedLabel", "updatedLabel"]}
        onSelect={restoreSession}
      >
        {(session) => (
          <div class="w-full flex items-center justify-between gap-3 rounded-md pl-1">
            <div class="flex items-center gap-x-3 grow min-w-0">
              <Icon name="bubble-5" size="small" class="shrink-0 text-icon-weak" />
              <div class="flex flex-col min-w-0 text-left">
                <div class="flex items-center gap-2 min-w-0">
                  <span class="text-14-regular text-text-strong truncate">{session.title}</span>
                  <span class="text-14-regular text-text-weak truncate">{session.label}</span>
                </div>
                <div class="flex items-center gap-2 min-w-0 text-12-regular text-text-weak">
                  <span class="whitespace-nowrap">{session.archivedLabel}</span>
                  <Show when={session.updatedLabel}>
                    <span class="truncate">{session.updatedLabel}</span>
                  </Show>
                </div>
              </div>
            </div>
            <span class="shrink-0 text-12-medium text-text-subtle">
              {store.restoring === session.id
                ? language.t("dialog.archivedSessions.restoring")
                : language.t("dialog.archivedSessions.restore")}
            </span>
          </div>
        )}
      </List>
    </Dialog>
  )
}

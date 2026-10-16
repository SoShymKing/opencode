import { useGlobal, useServerCtx } from "@/runtime/server/runtime"
import { type HomeProjectSelection, useLayout } from "@/shell/state/layout"
import { ServerConnection, useServers } from "@/runtime/server/registry"
import { useTabs } from "@/shell/tabs/tabs"
import { toggleHomeProjectSelection } from "@/shell/layout/helpers"
import { createEffect, createMemo, startTransition } from "solid-js"
import type { SessionInfo } from "@opencode/client/promise"
import { projectPathKey } from "@/workspaces/path-key"
import { addProjects } from "./projects/add"

export function createHomeController() {
  const layout = useLayout()
  const global = useGlobal()
  const servers = useServers()
  const tabs = useTabs()
  const selection = layout.home.selection

  const focusedServer = createMemo<ServerConnection.Any | undefined>(
    () => servers.visible.find((conn) => ServerConnection.key(conn) === selection().server) ?? servers.visible[0],
  )

  const focusedServerCtx = useServerCtx(focusedServer)
  const focusedSync = () => focusedServerCtx()?.sync
  const projects = createMemo(() => focusedServerCtx()?.projects.list() ?? [])
  const recentlyClosed = createMemo(() => focusedServerCtx()?.projects.recentlyClosed() ?? [])
  const homedir = createMemo(() => focusedSync()?.data.path.home ?? "")

  const selectedProject = createMemo(() => {
    const directory = selection().directory

    if (!directory) return
    const key = projectPathKey(directory)

    return projects().find((project) => projectPathKey(project.worktree) === key)
  })

  const newSessionProject = createMemo(() => {
    const selected = selectedProject()

    if (selected) return selected
    const last = focusedServerCtx()?.projects.last()

    if (!last) return projects()[0]
    const key = projectPathKey(last)

    return projects().find((project) => projectPathKey(project.worktree) === key) ?? projects()[0]
  })

  createEffect(() => {
    const list = servers.visible

    if (list.some((conn) => ServerConnection.key(conn) === selection().server)) return
    const conn = list[0]

    if (conn) setSelection({ server: ServerConnection.key(conn) })
  })
  createEffect(() => {
    const ctx = focusedServerCtx()
    const id = selectedProject()?.id

    if (!ctx || !id || ctx.sdk.connection.status() !== "connected") return
    // Selecting a project is the demand for its worktree inventory: the session filter spans its worktrees.
    void ctx.sync.worktrees.list(id).then(() => ctx.sync.worktrees.refresh(id))
  })
  createEffect(() => {
    // The project list is empty until the server store hydrates; clearing the restored
    // selection against it would persist the loss.
    if (!servers.hydrated()) return
    const current = selection()
    const directory = current.directory

    if (!directory) return
    const conn = servers.visible.find((conn) => ServerConnection.key(conn) === current.server)

    if (!conn) return

    if (
      global
        .ensureServerCtx(conn)
        .projects.list()
        .some((project) => projectPathKey(project.worktree) === projectPathKey(directory))
    )
      return
    setSelection({ server: current.server })
  })

  function setSelection(next: HomeProjectSelection) {
    layout.home.setSelection(next)
  }

  function openProjectNewSession(conn: ServerConnection.Any, directory: string) {
    const ctx = global.ensureServerCtx(conn)
    ctx.projects.open(directory)
    ctx.projects.touch(directory)
    void tabs.newDraft({ server: ServerConnection.key(conn), directory })
  }

  function openProjectSession(conn: ServerConnection.Any, directory: string, session: SessionInfo) {
    const ctx = global.ensureServerCtx(conn)
    void ctx.data.session.message.sync(session.id).catch(() => undefined)
    void startTransition(() => {
      const tab = tabs.addSessionTab({ server: ServerConnection.key(conn), sessionId: session.id })
      tabs.select(tab)
      ctx.data.session.remember(session)
      ctx.projects.open(directory)
      ctx.projects.touch(directory)
    })
  }

  return {
    selection: {
      value: selection,
      set: setSelection,
      focusServer: (conn: ServerConnection.Any) => setSelection({ server: ServerConnection.key(conn) }),
    },
    server: {
      list: () => servers.visible,
      health: (conn: ServerConnection.Any) => global.servers.health[ServerConnection.key(conn)],
      context: (conn: ServerConnection.Any) => global.ensureServerCtx(conn),
      focused: focusedServer,
      focusedContext: focusedServerCtx,
      focusedSync,
    },
    project: {
      list: projects,
      recentlyClosed,
      homedir,
      selected: selectedProject,
      newSession: newSessionProject,
      forServer: (conn: ServerConnection.Any) => global.ensureServerCtx(conn).projects.list(),
      select: (conn: ServerConnection.Any, directory: string) => {
        const key = ServerConnection.key(conn)

        if (global.servers.health[key]?.healthy === false) return

        if (
          !global
            .ensureServerCtx(conn)
            .projects.list()
            .some((project) => projectPathKey(project.worktree) === projectPathKey(directory))
        )
          return
        setSelection(toggleHomeProjectSelection(selection(), key, directory))
      },
      add: (conn: ServerConnection.Any, directories: string[]) => {
        const context = global.ensureServerCtx(conn)

        const directory = addProjects(
          context,
          directories.map(
            (directory) =>
              context.projects.list().find((project) => projectPathKey(project.worktree) === projectPathKey(directory))
                ?.worktree ??
              directory,
          ),
        )

        if (!directory) return
        setSelection({ server: ServerConnection.key(conn), directory })
      },
      openNewSession: () => {
        const conn = focusedServer()
        const project = newSessionProject()

        if (!conn || !project) return
        openProjectNewSession(conn, project.worktree)
      },
      openProjectNewSession,
      openProjectSession,
    },
  }
}

export type HomeController = ReturnType<typeof createHomeController>

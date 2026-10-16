import type { SessionInfo } from "@opencode/client/promise"
import { displayName } from "@opencode/ui/project-avatar"
import { projectPathKey } from "@/workspaces/path-key"
import type { ServerConnection } from "@/runtime/server/registry"
import type { HomeProjectSelection } from "@/shell/state/layout"

export function compareSessionTime(a: SessionInfo, b: SessionInfo) {
  const updated = (b.time.updated ?? b.time.created) - (a.time.updated ?? a.time.created)

  if (updated !== 0) return updated

  return a.id < b.id ? -1 : a.id > b.id ? 1 : 0
}

type ProjectAppearance = {
  name?: string
  worktree: string
  icon?: { color?: string; url?: string; override?: string }
}

function withProjectAppearance<T extends ProjectAppearance>(metadata: T, appearance?: ProjectAppearance) {
  if (!appearance || appearance === metadata) return metadata

  return { ...metadata, name: displayName(appearance), icon: appearance.icon }
}

export function toggleHomeProjectSelection(
  current: HomeProjectSelection | undefined,
  server: ServerConnection.Key,
  directory: string,
): HomeProjectSelection {
  if (current?.server === server && current.directory && projectPathKey(current.directory) === projectPathKey(directory))
    return { server }

  return { server, directory }
}

export function closeHomeProject(
  selected: HomeProjectSelection | undefined,
  server: ServerConnection.Key,
  projects: { close: (directory: string) => void },
  directory: string,
) {
  projects.close(directory)

  if (
    selected?.server === server &&
    selected.directory &&
    projectPathKey(selected.directory) === projectPathKey(directory)
  )
    return { server }

  return selected
}

export function homeProjectDirectories(result: string | string[] | null) {
  if (!result) return []

  return Array.isArray(result) ? result : [result]
}

export function projectForSession<T extends { id?: string; worktree: string; sandboxes?: string[] }>(
  session: SessionInfo,
  projects: T[],
) {
  const directory = projectPathKey(session.location.directory)

  const exact =
    projects.find((project) => projectPathKey(project.worktree) === directory) ??
    projects.find((project) => project.sandboxes?.some((sandbox) => projectPathKey(sandbox) === directory))

  if (exact) return exact

  const matching = projects.filter((project) => project.id === session.projectID)

  if (matching.length === 1) return matching[0]

  return (
    (matching.length ? matching : projects)
      .filter((project) =>
        [project.worktree, ...(project.sandboxes ?? [])].some((root) => {
          const key = projectPathKey(root)

          return directory.startsWith(key.endsWith("/") ? key : `${key}/`)
        }),
      )
      .sort((a, b) => b.worktree.length - a.worktree.length)[0] ?? matching.at(-1)
  )
}

export function resolveProjectForSession<
  T extends { id?: string; worktree: string; sandboxes?: string[] },
  U extends { id?: string; worktree: string; sandboxes?: string[] },
>(session: SessionInfo, opened: T[], stored: U[]) {
  const current = projectForSession(session, opened)

  if (current?.id === session.projectID) {
    const unresolved = opened.find(
      (project) => !project.id && projectPathKey(project.worktree) === projectPathKey(session.location.directory),
    )

    if (!unresolved) return current
    const canonical = projectForSession(session, stored)

    if (canonical?.id === session.projectID && projectPathKey(canonical.worktree) === projectPathKey(unresolved.worktree))
      return unresolved

    return current
  }

  const synced = projectForSession(session, stored)

  if (synced?.id !== session.projectID) return current ?? synced

  if (current && !current.id && projectPathKey(current.worktree) === projectPathKey(session.location.directory))
    return current

  return synced
}

export function resolveSessionDetailsProject<
  T extends ProjectAppearance & { id?: string; sandboxes?: string[] },
  U extends ProjectAppearance & { id?: string; sandboxes?: string[] },
>(session: SessionInfo, opened: T[], stored: U[]) {
  const metadata = projectForSession(session, stored)

  if (!metadata) return

  return withProjectAppearance(metadata, resolveProjectForSession(session, opened, stored))
}

export const errorMessage = (err: unknown, fallback: string) => {
  if (err && typeof err === "object" && "data" in err) {
    const data = (err as { data?: { message?: string } }).data

    if (data?.message) return data.message
  }

  if (err instanceof Error) return err.message

  return fallback
}

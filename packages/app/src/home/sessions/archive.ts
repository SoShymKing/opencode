import { notifySessionTabsRemoved } from "@/shell/titlebar/session-events"
import type { ServerConnection } from "@/runtime/server/registry"
import type { SessionApi, SessionInfo } from "@opencode/client/promise"

type HomeSession = Pick<SessionInfo, "id" | "location" | "time">

export async function archiveHomeSession(input: {
  server: ServerConnection.Key
  session: HomeSession
  api: Pick<SessionApi, "archive">
  remember: (time: SessionInfo["time"]) => void
  onError?: (error: unknown) => void
}) {
  await input
    .api.archive({ sessionID: input.session.id })
    .then(() => {
      input.remember({ ...input.session.time, archived: Date.now() })
      notifySessionTabsRemoved({
        server: input.server,
        directory: input.session.location.directory,
        sessionIDs: [input.session.id],
      })
    })
    .catch((error) => input.onError?.(error))
}

export async function restoreHomeSession(api: Pick<SessionApi, "restore" | "get">, sessionID: string) {
  await api.restore({ sessionID })

  return api.get({ sessionID })
}

import type { TuiPlugin } from "@opencode-ai/plugin/tui"
import type { InternalTuiPlugin } from "../../plugin/internal"
import { DialogArchivedSessionList } from "@tui/component/dialog-archived-session-list"
import { SessionSwitcherDialog } from "./dialog"

const id = "internal:session-switcher"

const tui: TuiPlugin = async (api) => {
  api.keymap.registerLayer({
    priority: 1000,
    commands: [
      {
        name: "session.list",
        title: "Switch session",
        category: "Session",
        namespace: "palette",
        suggested: () => api.state.session.count() > 0,
        slashName: "sessions",
        slashAliases: ["resume", "continue"],
        run() {
          api.ui.dialog.replace(() => <SessionSwitcherDialog />)
        },
      },
      {
        name: "session.archived.list",
        title: "Archived sessions",
        category: "Session",
        namespace: "palette",
        run() {
          api.ui.dialog.replace(() => <DialogArchivedSessionList />)
        },
      },
    ],
  })
}

const plugin: InternalTuiPlugin = {
  id,
  tui,
}

export default plugin

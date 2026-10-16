import type { Database } from "../database/database.js"
import { SessionEvent } from "./event.js"
import { projectContentEvent } from "./projector-content.js"

export type ToolEvent =
  | SessionEvent.Tool.Input.Started
  | SessionEvent.Tool.Input.Ended
  | SessionEvent.Tool.Called
  | SessionEvent.Tool.Success
  | SessionEvent.Tool.Failed

export function projectToolEvent(db: Database.Interface["db"], event: ToolEvent) {
  return projectContentEvent(db, event)
}

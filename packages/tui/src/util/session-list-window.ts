export const SESSION_LIST_REQUEST_WINDOW_KEY = "session_list_request_window_days"

const DAY = 24 * 60 * 60 * 1000
const DEFAULT_SESSION_LIST_REQUEST_WINDOW = 30
const SESSION_LIST_REQUEST_WINDOWS = [7, 30, 90, 365, "all"] as const

export type SessionListRequestWindow = (typeof SESSION_LIST_REQUEST_WINDOWS)[number]

export function sessionListRequestWindow(value: unknown): SessionListRequestWindow {
  switch (value) {
    case 7:
    case 30:
    case 90:
    case 365:
    case "all":
      return value
  }
  return DEFAULT_SESSION_LIST_REQUEST_WINDOW
}

export function sessionListRequestWindowLabel(value: unknown) {
  const window = sessionListRequestWindow(value)
  if (window === "all") return "all sessions"
  return `${window} days`
}

export function nextSessionListRequestWindow(value: unknown) {
  const index = SESSION_LIST_REQUEST_WINDOWS.indexOf(sessionListRequestWindow(value))
  return SESSION_LIST_REQUEST_WINDOWS[index + 1] ?? SESSION_LIST_REQUEST_WINDOWS[0]
}

export function sessionListRequestStart(value: unknown, now = Date.now()) {
  const window = sessionListRequestWindow(value)
  if (window === "all") return
  return now - window * DAY
}

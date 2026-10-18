export const sourceRevisions = { ws: "5573129a75", previousNative: "0fbe489e35~1" } as const
export const sessionID = "ses_migration"
export const messageID = "msg_migration"
export const legacySessionID = "ses_legacy"
export const forkSessionID = "ses_fork"
export const userMessageID = "msg_user_explicit"
export const pendingSteerID = "msg_pending_steer"
export const pendingQueueID = "msg_pending_queue"
export const model = { providerID: "test", id: "model" } as const
export const messageContent = [
  { id: "text-1", type: "text", text: "first" },
  { id: "text-2", type: "text", text: "second" },
] as const
export const messageData = JSON.stringify({ agent: "build", model, content: messageContent, time: { created: 1 } })
export const nativeMessageData = JSON.stringify({
  agent: "build", model, content: [{ type: "text", text: "first" }, { type: "text", text: "second" }], time: { created: 1 },
})
export const normalizedMessageData = JSON.stringify({ agent: "build", model, time: { created: 1 } })
export const messageBytes = Buffer.from(messageData).toString("hex").toUpperCase()
export const nativeMessageBytes = Buffer.from(nativeMessageData).toString("hex").toUpperCase()
export const normalizedMessageBytes = Buffer.from(normalizedMessageData).toString("hex").toUpperCase()
export const messageParts = messageContent.map((part, position) => ({
  position, id: part.id, type: part.type, data: JSON.stringify({ text: part.text }),
}))
export const rawJSON = '{ "large":9007199254740993, "decimal":1.2300, "exponent":1e+09, "negativeZero":-0, "null":null }'
export const rawBlobHex = "00017F80FF004142"
export const baseline = "Baseline policy: preserve the original context.\nUnicode: \uD55C\uAE00 \u03BB"
export const snapshot = JSON.stringify({
  "fixture/policy": { value: "Current policy differs from the baseline", removed: "Policy removed" },
  "fixture/nullable": { value: null },
})
export const attachment = { uri: "data:text/plain;base64,7ZWc6riA", mime: "text/plain", name: "\uD55C\uAE00.txt" } as const
export const prompt = { text: "Review \uD55C\uAE00 \u03BB", files: [attachment], agents: [{ name: "build" }] } as const
export const richContent = [
  ...messageContent,
  { id: "reasoning-1", type: "reasoning", text: "Check the boundary", providerMetadata: { test: { signature: "fake-signature" } }, time: { created: 2, completed: 3 } },
  { id: "pending-call", type: "tool", name: "read", state: { status: "pending", input: '{"path":' }, time: { created: 4 } },
  { id: "running-call", type: "tool", name: "bash", state: { status: "running", input: { command: "printf running" }, structured: { pid: 42 }, content: [{ type: "text", text: "running" }] }, time: { created: 5, ran: 6 } },
  { id: "empty-call", type: "tool", name: "read", state: { status: "completed", input: { path: "empty.txt" }, structured: {}, content: [], result: "" }, time: { created: 7, ran: 8, completed: 9 } },
  { id: "hosted-call", type: "tool", name: "web_search", provider: { executed: true, metadata: { test: { source: "hosted" } }, resultMetadata: { test: { request: "fake-request" } } }, state: { status: "completed", input: { query: "fixture" }, structured: { hits: 1 }, content: [{ type: "text", text: "Found \uD55C\uAE00" }], result: { url: "https://example.invalid/result", score: 0.5 }, attachments: [attachment], outputPaths: ["result.txt"] }, time: { created: 10, ran: 11, completed: 12 } },
  { id: "error-call", type: "tool", name: "read", state: { status: "error", input: { path: "missing.txt" }, structured: { code: "ENOENT" }, content: [], error: { type: "unknown", message: "File not found" }, result: null }, time: { created: 13, ran: 14, completed: 15 } },
] as const
export const richMessageData = JSON.stringify({
  agent: "build", model, content: richContent, time: { created: 1, completed: 16 },
  finish: "tool-calls", cost: 0.125, streamEventCount: 27,
  tokens: { input: 31, output: 17, reasoning: 5, cache: { read: 7, write: 3 } },
  snapshot: { start: "before", end: "after", files: ["result.txt"] },
})
export const wsEvents = [
  { id: "evt_admit_unrelated", seq: 2, type: "session.next.prompt.admitted.1", data: { sessionID, timestamp: 1, messageID: userMessageID, prompt, delivery: "steer" } },
  { id: "evt_promote_unrelated", seq: 4, type: "session.next.prompted.1", data: { sessionID, timestamp: 2, messageID: userMessageID, prompt, delivery: "steer" } },
  { id: "evt_step_unrelated", seq: 7, type: "session.next.step.started.1", data: { sessionID, timestamp: 3, assistantMessageID: messageID, agent: "build", model } },
  { id: "evt_text_unrelated", seq: 10, type: "session.next.text.ended.1", data: { sessionID, timestamp: 4, assistantMessageID: messageID, textID: "text-1", text: "first" } },
  { id: "evt_context_unrelated", seq: 13, type: "session.next.context.updated.1", data: { sessionID, timestamp: 5, messageID: "msg_context_explicit", text: "Current policy differs from the baseline" } },
  { id: "evt_steer_unrelated", seq: 40, type: "session.next.prompt.admitted.1", data: { sessionID, timestamp: 40, messageID: pendingSteerID, prompt: { text: "Steer next" }, delivery: "steer" } },
  { id: "evt_queue_unrelated", seq: 43, type: "session.next.prompt.admitted.1", data: { sessionID, timestamp: 43, messageID: pendingQueueID, prompt: { text: "Queue later" }, delivery: "queue" } },
] as const
export const wsBaselineIDs = [
  "20260127222353_familiar_lady_ursula",
  "20260211171708_add_project_commands",
  "20260213144116_wakeful_the_professor",
  "20260225215848_workspace",
  "20260227213759_add_session_workspace_id",
  "20260228203230_blue_harpoon",
  "20260303231226_add_workspace_fields",
  "20260309230000_move_org_to_state",
  "20260312043431_session_message_cursor",
  "20260323234822_events",
  "20260410174513_workspace-name",
  "20260413175956_chief_energizer",
  "20260423070820_add_icon_url_override",
  "20260427172553_slow_nightmare",
  "20260428004200_add_session_path",
  "20260501142318_next_venus",
  "20260504145000_add_sync_owner",
  "20260507164347_add_workspace_time",
  "20260510033149_session_usage",
  "20260511000411_data_migration_state",
  "20260511173437_session-metadata",
  "20260601010001_normalize_storage_paths",
  "20260601202201_amazing_prowler",
  "20260602002951_lowly_union_jack",
  "20260602182828_add_project_directories",
  "20260603001617_session_message_projection_indexes",
  "20260603040000_session_message_projection_order",
  "20260603141458_session_input_inbox",
  "20260603160727_jittery_ezekiel_stane",
  "20260604172448_event_sourced_session_input",
  "20260605003541_add_session_context_snapshot",
  "20260605042240_add_context_epoch_agent",
  "20260611035744_credential",
  "20260611192811_lush_chimera",
  "20260612174303_project_dir_strategy",
  "20260622142730_simplify_session_context_epoch",
  "20260622170816_reset_v2_session_state",
  "20260622202450_simplify_session_input",
] as const
export const previousNativeIDs = [
  ...wsBaselineIDs,
  "20260702134641_add_session_context_entry",
  "20260703090000_reset_v2_event_rename_sweep",
  "20260703181610_event_created_column",
  "20260703190000_reset_v2_shell_event_payloads",
  "20260703200000_reset_v2_session_events",
  "20260705180000_rename_instructions",
  "20260706223930_add-session-fork",
  "20260707010146_durable_session_inbox",
  "20260707120000_migrate_prelaunch_v2_state",
  "20260709013000_generic_session_input",
  "20260709025533_drop-todo",
  "20260709163752_time_suspended",
  "20260709190621_session_pending_table",
  "20260710025429_instruction_sync",
  "20260716020354_kv",
  "20260722011141_delete_tool_progress_events",
  "20260722170000_canonical_tool_results",
  "20260729022634_session_fork_boundary",
  "20260730195856_optional_session_title",
] as const
export const metadataAlias = "20260530232709_lovely_romulus"
export const historicalPartID = "20260802133449_session_message_part"
export const wsLineages = ["v01", "v02", "expanded", "expanded-alias", "split-alias", "historical-part"] as const
export type WsLineage = typeof wsLineages[number]

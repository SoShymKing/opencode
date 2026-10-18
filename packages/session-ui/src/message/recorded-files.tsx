import type { SessionMessageRecordedFile } from "@opencode/client/promise"
import { For } from "solid-js"
import { useI18n } from "@opencode/ui/context/i18n"
import { AttachmentCard } from "./attachment-card"

export function RecordedFiles(props: { files?: SessionMessageRecordedFile[] }) {
  const i18n = useI18n()

  return (
    <For each={props.files}>
      {(file) => (
        <AttachmentCard title={file.name ?? file.uri} hover={file.uri}>
          {i18n.t("ui.message.recordedFile.unavailable")}
        </AttachmentCard>
      )}
    </For>
  )
}

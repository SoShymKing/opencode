import { SessionMessageTable } from "@opencode/core/session/sql"

type MessageData = (typeof SessionMessageTable.$inferInsert)["data"]

const user = {
  text: "Hello",
  time: { created: 0 },
} satisfies MessageData

const assistant = {
  agent: "build",
  model: { id: "model", providerID: "provider" },
  time: { created: 0 },
} satisfies MessageData

type AssistantData = Extract<MessageData, { agent: string; model: { id: string; providerID: string } }>
type UserData = Extract<MessageData, { text: string }>

const envelopeExcludesContent: "content" extends keyof AssistantData ? false : true = true
const userTextRetainsType: UserData["text"] extends string ? true : false = true
const rejectsNumericText: { text: number; time: { created: number } } extends MessageData ? false : true = true

void user
void assistant
void envelopeExcludesContent
void userTextRetainsType
void rejectsNumericText

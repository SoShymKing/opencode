import { describe, expect } from "bun:test"
import { asc, eq } from "drizzle-orm"
import { Cause, Effect, Exit, Schema } from "effect"
import { Bus } from "@opencode/core/bus"
import { Database } from "@opencode/core/database/database"
import { AppNodeBuilder } from "@opencode/core/effect/app-node-builder"
import { Session } from "@opencode/core/session"
import { SessionExecution } from "@opencode/core/session/execution"
import { SessionProjector } from "@opencode/core/session/projector"
import { SessionTransfer } from "@opencode/core/session/transfer"
import { SessionHistory } from "@opencode/core/session/history"
import { AttachmentQuarantine } from "@opencode/core/session/attachment-quarantine"
import { toLLMMessages } from "@opencode/core/session/runner/to-llm-message"
import { Model } from "@opencode/schema/model"
import { SessionMessagePartTable, SessionMessageTable } from "@opencode/core/session/sql"
import { SessionMessage } from "@opencode/schema/session-message"
import { AbsolutePath } from "@opencode/schema/schema"
import { LayerNode } from "@opencode/util/effect/layer-node"
import { tmpdirScoped } from "./fixture/tmpdir"
import { testEffect } from "./lib/effect"

const it = testEffect(AppNodeBuilder.build(
  LayerNode.group([Database.node, Session.node, SessionTransfer.node, SessionProjector.node]),
  [Bus.node.replace(Bus.configured({ persist: true })), SessionExecution.node.replace(SessionExecution.noopLayer)],
))

describe("SessionTransfer normalized storage", () => {
  it.live("preserves quarantine through transfer and remaps refs across fork batches without future facts", () => Effect.gen(function* () {
    const directory = yield* tmpdirScoped()
    const sessions = yield* Session.Service
    const transfer = yield* SessionTransfer.Service
    const database = yield* Database.Service
    const location = { directory: AbsolutePath.make(directory.path) }
    const original = yield* sessions.create({ location })
    const decode = Schema.decodeUnknownSync(SessionMessage.Info)
    const user = decode({ id: "msg_quarantine_user", type: "user", text: "input", time: { created: 0 },
      files: [{ mime: "image/png", data: "dXNlcg==", source: { type: "inline" } }],
    })
    const tool = decode({ id: "msg_quarantine_tool", type: "assistant", agent: "build", model: { id: "test", providerID: "test" }, time: { created: 501, completed: 502 },
      content: [{ type: "tool", id: "file", name: "read", time: { created: 501, completed: 502 },
        state: { status: "completed", input: {}, content: [{ type: "file", mime: "image/png", uri: "data:image/png;base64,dG9vbA==" }] } }],
    })
    const failure = decode({ id: "msg_quarantine_failure", type: "assistant", agent: "build", model: { id: "test", providerID: "test" }, content: [], time: { created: 503, completed: 504 },
      error: { type: "provider.invalid-file", message: "sanitized", rejectedAttachments: [
        { source: "user", messageID: user.id, fileIndex: 0 },
        { source: "tool", messageID: tool.id, partIndex: 0, contentIndex: 0 },
      ] },
    })
    const messages = [user, ...Array.from({ length: 500 }, (_, index) => decode({
      id: `msg_filler_${index}`, type: "synthetic", text: "history", time: { created: index + 1 },
    })), tool, failure]
    yield* sessions.remove(original.id)
    yield* transfer.import({ data: { info: original, messages }, location })
    expect((yield* transfer.export({ sessionID: original.id })).messages).toEqual(messages)
    const fork = yield* sessions.fork({ sessionID: original.id })
    const forked = (yield* transfer.export({ sessionID: fork.id })).messages
    expect(forked).toHaveLength(messages.length)
    const history = yield* SessionHistory.quarantinedEntries(database.db, fork.id, "latest")
    expect(history.rejectedAttachments.map((ref) => ref.messageID)).toEqual([forked[0].id, forked[501].id])
    const model = Schema.decodeUnknownSync(Model.Ref)({ id: "test", providerID: "test" })
    const outgoing = toLLMMessages(history.entries.map((entry) => entry.message), model, model.providerID, { rejectedAttachments: history.rejectedAttachments })
    expect(AttachmentQuarantine.batch(outgoing)).toEqual({ coverage: "complete", refs: [] })
    const earlier = yield* sessions.fork({ sessionID: original.id, before: failure.id })
    const earlierHistory = yield* SessionHistory.quarantinedEntries(database.db, earlier.id, "latest")
    expect(earlierHistory.rejectedAttachments).toEqual([])
    expect((yield* transfer.export({ sessionID: original.id })).messages).toEqual(messages)
    const invalidID = Session.ID.create()
    const invalid = yield* transfer.import({ data: { info: { ...original, id: invalidID }, messages: [failure] }, location }).pipe(Effect.exit)
    expect(Exit.isFailure(invalid)).toBe(true)
    if (Exit.isFailure(invalid)) expect(Cause.squash(invalid.cause)).toBeInstanceOf(AttachmentQuarantine.Unavailable)
    expect(Exit.isFailure(yield* sessions.get(invalidID).pipe(Effect.exit))).toBe(true)
  }))

  it.live("round-trips native assistant children and cascades them on session removal", () =>
    Effect.gen(function* () {
      const directory = yield* tmpdirScoped()
      const sessions = yield* Session.Service
      const transfer = yield* SessionTransfer.Service
      const database = yield* Database.Service
      const location = { directory: AbsolutePath.make(directory.path) }
      const original = yield* sessions.create({ location })
      const message = Schema.decodeUnknownSync(SessionMessage.Info)({
        id: "msg_transfer_normalized", type: "assistant", agent: "build",
        model: { id: "test", providerID: "test" }, time: { created: 10, completed: 20 },
        content: [
          { type: "text", text: "answer", state: { signature: "opaque" } },
          { type: "reasoning", text: "reasoning", time: { created: 10, completed: 11 } },
          { type: "tool", id: "tool-transfer", name: "read", time: { created: 11, completed: 12 },
            providerState: { signature: "call" }, providerResultState: { result: "state" },
            state: { status: "completed", input: {}, content: [{ type: "text", text: "result" }] } },
        ],
      })
      const data = { info: original, messages: [message] }
      yield* sessions.remove(original.id)

      const imported = yield* transfer.import({ data, location })
      const exported = yield* transfer.export({ sessionID: imported.id })

      expect(exported.messages).toEqual([message])
      const parents = yield* database.db.select().from(SessionMessageTable)
        .where(eq(SessionMessageTable.session_id, imported.id)).all()
      expect(parents).toHaveLength(1)
      expect(parents[0]?.data).not.toHaveProperty("content")
      const children = yield* database.db.select().from(SessionMessagePartTable)
        .where(eq(SessionMessagePartTable.message_id, message.id)).orderBy(asc(SessionMessagePartTable.position)).all()
      expect(children.map((child) => child.type)).toEqual(["text", "reasoning", "tool"])
      yield* sessions.remove(imported.id)
      expect(yield* database.db.select().from(SessionMessagePartTable)
        .where(eq(SessionMessagePartTable.message_id, message.id)).all()).toEqual([])
    }),
  )
})

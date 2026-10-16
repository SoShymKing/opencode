import { Effect } from "effect"
import type { DatabaseMigration } from "../migration.js"
import { normalizeMessages } from "../native-message-parts.js"

const migration: DatabaseMigration.Migration = {
  id: "20261016000000_session_message_part",
  up(tx) {
    return Effect.gen(function* () {
      yield* tx.run(`CREATE TABLE session_message_part (
        message_id text NOT NULL REFERENCES session_message(id) ON DELETE CASCADE,
        position integer NOT NULL,
        id text NOT NULL,
        type text NOT NULL,
        data text NOT NULL CHECK(json_valid(data) AND json_type(data) = 'object'),
        PRIMARY KEY(message_id, position)
      )`)
      yield* tx.run(`CREATE INDEX session_message_part_lookup_idx ON session_message_part (message_id, type, id, position DESC)`)
      yield* normalizeMessages(tx)
    })
  },
}

export default migration

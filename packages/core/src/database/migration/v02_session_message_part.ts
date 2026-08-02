import { Effect } from "effect"
import type { DatabaseMigration } from "../migration"

export default {
  id: "v02_session_message_part",
  up(tx) {
    return Effect.gen(function* () {
      yield* tx.run(`
        CREATE TABLE \`session_message_part\` (
          \`message_id\` text NOT NULL,
          \`position\` integer NOT NULL,
          \`id\` text NOT NULL,
          \`type\` text NOT NULL,
          \`data\` text NOT NULL CHECK(json_valid("data") AND json_type("data") = 'object'),
          PRIMARY KEY(\`message_id\`, \`position\`),
          FOREIGN KEY (\`message_id\`) REFERENCES \`session_message\`(\`id\`) ON UPDATE no action ON DELETE cascade
        );
      `)
      yield* tx.run(
        `CREATE INDEX \`session_message_part_lookup_idx\` ON \`session_message_part\` (\`message_id\`,\`type\`,\`id\`,"position" desc);`,
      )
    })
  },
} satisfies DatabaseMigration.Migration

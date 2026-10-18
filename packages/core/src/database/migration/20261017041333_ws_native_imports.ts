import { Effect } from "effect"
import type { DatabaseMigration } from "../migration.js"

const migration: DatabaseMigration.Migration = {
  id: "20261017041333_ws_native_imports",
  up(tx) {
    return Effect.gen(function* () {
      yield* tx.run(`
        CREATE TABLE \`_ws_archive\` (
          \`id\` text PRIMARY KEY,
          \`manifest\` text NOT NULL
        );
      `)
      yield* tx.run(`
        CREATE TABLE \`_ws_import_root\` (
          \`aggregate_id\` text PRIMARY KEY,
          \`archive_id\` text NOT NULL,
          \`through_seq\` integer NOT NULL,
          \`codec_version\` integer NOT NULL,
          CONSTRAINT \`fk__ws_import_root_archive_id__ws_archive_id_fk\` FOREIGN KEY (\`archive_id\`) REFERENCES \`_ws_archive\`(\`id\`)
        );
      `)
      yield* tx.run(`ALTER TABLE \`instruction_state\` ADD \`frozen_baseline\` text;`)
      yield* tx.run(`ALTER TABLE \`instruction_state\` ADD \`imported_removals\` text;`)
    })
  },
}

export default migration

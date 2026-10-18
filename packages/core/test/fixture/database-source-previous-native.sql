-- Frozen native-P-only DDL: 0fbe489e35~1:packages/core/src/database/schema.gen.ts.
CREATE TABLE `event` (
  `id` text PRIMARY KEY, `aggregate_id` text NOT NULL, `seq` integer NOT NULL, `created` integer NOT NULL,
  `type` text NOT NULL, `data` text NOT NULL,
  CONSTRAINT `fk_event_aggregate_id_event_sequence_aggregate_id_fk` FOREIGN KEY (`aggregate_id`) REFERENCES `event_sequence`(`aggregate_id`) ON DELETE CASCADE
);
CREATE TABLE `kv` (`key` text PRIMARY KEY, `value` text NOT NULL, `time_created` integer NOT NULL, `time_updated` integer NOT NULL);
CREATE TABLE `instruction_blob` (`hash` text PRIMARY KEY, `value` text);
CREATE TABLE `instruction_entry` (
  `session_id` text NOT NULL, `key` text NOT NULL, `value` text, `removed` integer DEFAULT false NOT NULL,
  `time_created` integer NOT NULL, `time_updated` integer NOT NULL,
  CONSTRAINT `instruction_entry_pk` PRIMARY KEY(`session_id`, `key`),
  CONSTRAINT `fk_instruction_entry_session_id_session_id_fk` FOREIGN KEY (`session_id`) REFERENCES `session`(`id`) ON DELETE CASCADE
);
CREATE TABLE `instruction_state` (
  `session_id` text PRIMARY KEY, `epoch_start` integer NOT NULL, `through_seq` integer NOT NULL,
  `initial_values` text NOT NULL, `current_values` text NOT NULL,
  CONSTRAINT `fk_instruction_state_session_id_session_id_fk` FOREIGN KEY (`session_id`) REFERENCES `session`(`id`) ON DELETE CASCADE
);
CREATE TABLE `session_pending` (
  `id` text PRIMARY KEY, `session_id` text NOT NULL, `type` text NOT NULL, `data` text NOT NULL, `delivery` text,
  `admitted_seq` integer NOT NULL, `time_created` integer NOT NULL,
  CONSTRAINT `fk_session_pending_session_id_session_id_fk` FOREIGN KEY (`session_id`) REFERENCES `session`(`id`) ON DELETE CASCADE
);
CREATE TABLE `session` (
  `id` text PRIMARY KEY, `project_id` text NOT NULL, `workspace_id` text, `parent_id` text,
  `fork_session_id` text, `fork_boundary` text, `slug` text NOT NULL, `directory` text NOT NULL, `path` text,
  `title` text, `version` text NOT NULL, `share_url` text, `summary_additions` integer, `summary_deletions` integer,
  `summary_files` integer, `summary_diffs` text, `metadata` text, `cost` real DEFAULT 0 NOT NULL,
  `tokens_input` integer DEFAULT 0 NOT NULL, `tokens_output` integer DEFAULT 0 NOT NULL,
  `tokens_reasoning` integer DEFAULT 0 NOT NULL, `tokens_cache_read` integer DEFAULT 0 NOT NULL,
  `tokens_cache_write` integer DEFAULT 0 NOT NULL, `revert` text, `permission` text, `agent` text, `model` text,
  `time_created` integer NOT NULL, `time_updated` integer NOT NULL, `time_compacting` integer, `time_archived` integer,
  `time_suspended` integer,
  CONSTRAINT `fk_session_project_id_project_id_fk` FOREIGN KEY (`project_id`) REFERENCES `project`(`id`) ON DELETE CASCADE
);
CREATE INDEX `session_pending_session_delivery_seq_idx` ON `session_pending` (`session_id`,`delivery`,`admitted_seq`);
CREATE UNIQUE INDEX `session_pending_session_compaction_idx` ON `session_pending` (`session_id`) WHERE "session_pending"."type" = 'compaction';
CREATE UNIQUE INDEX `session_pending_session_admitted_seq_idx` ON `session_pending` (`session_id`,`admitted_seq`);
CREATE INDEX `session_time_suspended_idx` ON `session` (`time_suspended`) WHERE "session"."time_suspended" is not null;

-- Frozen shared DDL from 5573129a75 and 0fbe489e35~1:
-- packages/core/src/database/schema.gen.ts. Whitespace only is compacted.
CREATE TABLE `workspace` (
  `id` text PRIMARY KEY, `type` text NOT NULL, `name` text DEFAULT '' NOT NULL,
  `branch` text, `directory` text, `extra` text, `project_id` text NOT NULL, `time_used` integer NOT NULL,
  CONSTRAINT `fk_workspace_project_id_project_id_fk` FOREIGN KEY (`project_id`) REFERENCES `project`(`id`) ON DELETE CASCADE
);
CREATE TABLE `data_migration` (`name` text PRIMARY KEY, `time_completed` integer NOT NULL);
CREATE TABLE `account_state` (
  `id` integer PRIMARY KEY, `active_account_id` text, `active_org_id` text,
  CONSTRAINT `fk_account_state_active_account_id_account_id_fk` FOREIGN KEY (`active_account_id`) REFERENCES `account`(`id`) ON DELETE SET NULL
);
CREATE TABLE `account` (
  `id` text PRIMARY KEY, `email` text NOT NULL, `url` text NOT NULL, `access_token` text NOT NULL,
  `refresh_token` text NOT NULL, `token_expiry` integer, `time_created` integer NOT NULL, `time_updated` integer NOT NULL
);
CREATE TABLE `control_account` (
  `email` text NOT NULL, `url` text NOT NULL, `access_token` text NOT NULL, `refresh_token` text NOT NULL,
  `token_expiry` integer, `active` integer NOT NULL, `time_created` integer NOT NULL, `time_updated` integer NOT NULL,
  CONSTRAINT `control_account_pk` PRIMARY KEY(`email`, `url`)
);
CREATE TABLE `credential` (
  `id` text PRIMARY KEY, `integration_id` text, `label` text NOT NULL, `value` text NOT NULL,
  `connector_id` text, `method_id` text, `active` integer, `time_created` integer NOT NULL, `time_updated` integer NOT NULL
);
CREATE TABLE `event_sequence` (`aggregate_id` text PRIMARY KEY, `seq` integer NOT NULL, `owner_id` text);
CREATE TABLE `permission` (
  `id` text PRIMARY KEY, `project_id` text NOT NULL, `action` text NOT NULL, `resource` text NOT NULL,
  `time_created` integer NOT NULL, `time_updated` integer NOT NULL,
  CONSTRAINT `fk_permission_project_id_project_id_fk` FOREIGN KEY (`project_id`) REFERENCES `project`(`id`) ON DELETE CASCADE
);
CREATE TABLE `project_directory` (
  `project_id` text NOT NULL, `directory` text NOT NULL, `type` text, `strategy` text, `time_created` integer NOT NULL,
  CONSTRAINT `project_directory_pk` PRIMARY KEY(`project_id`, `directory`),
  CONSTRAINT `fk_project_directory_project_id_project_id_fk` FOREIGN KEY (`project_id`) REFERENCES `project`(`id`) ON DELETE CASCADE
);
CREATE TABLE `project` (
  `id` text PRIMARY KEY, `worktree` text NOT NULL, `vcs` text, `name` text, `icon_url` text, `icon_url_override` text,
  `icon_color` text, `time_created` integer NOT NULL, `time_updated` integer NOT NULL, `time_initialized` integer,
  `sandboxes` text NOT NULL, `commands` text
);
CREATE TABLE `message` (
  `id` text PRIMARY KEY, `session_id` text NOT NULL, `time_created` integer NOT NULL, `time_updated` integer NOT NULL, `data` text NOT NULL,
  CONSTRAINT `fk_message_session_id_session_id_fk` FOREIGN KEY (`session_id`) REFERENCES `session`(`id`) ON DELETE CASCADE
);
CREATE TABLE `part` (
  `id` text PRIMARY KEY, `message_id` text NOT NULL, `session_id` text NOT NULL,
  `time_created` integer NOT NULL, `time_updated` integer NOT NULL, `data` text NOT NULL,
  CONSTRAINT `fk_part_message_id_message_id_fk` FOREIGN KEY (`message_id`) REFERENCES `message`(`id`) ON DELETE CASCADE
);
CREATE TABLE `session_message` (
  `id` text PRIMARY KEY, `session_id` text NOT NULL, `type` text NOT NULL, `seq` integer NOT NULL,
  `time_created` integer NOT NULL, `time_updated` integer NOT NULL, `data` text NOT NULL,
  CONSTRAINT `fk_session_message_session_id_session_id_fk` FOREIGN KEY (`session_id`) REFERENCES `session`(`id`) ON DELETE CASCADE
);
CREATE TABLE `session_share` (
  `session_id` text PRIMARY KEY, `id` text NOT NULL, `secret` text NOT NULL, `url` text NOT NULL,
  `time_created` integer NOT NULL, `time_updated` integer NOT NULL,
  CONSTRAINT `fk_session_share_session_id_session_id_fk` FOREIGN KEY (`session_id`) REFERENCES `session`(`id`) ON DELETE CASCADE
);
CREATE UNIQUE INDEX `event_aggregate_seq_idx` ON `event` (`aggregate_id`,`seq`);
CREATE INDEX `event_aggregate_type_seq_idx` ON `event` (`aggregate_id`,`type`,`seq`);
CREATE UNIQUE INDEX `permission_project_action_resource_idx` ON `permission` (`project_id`,`action`,`resource`);
CREATE INDEX `message_session_time_created_id_idx` ON `message` (`session_id`,`time_created`,`id`);
CREATE INDEX `part_message_id_id_idx` ON `part` (`message_id`,`id`);
CREATE INDEX `part_session_idx` ON `part` (`session_id`);
CREATE UNIQUE INDEX `session_message_session_seq_idx` ON `session_message` (`session_id`,`seq`);
CREATE INDEX `session_message_session_type_seq_idx` ON `session_message` (`session_id`,`type`,`seq`);
CREATE INDEX `session_message_session_time_created_id_idx` ON `session_message` (`session_id`,`time_created`,`id`);
CREATE INDEX `session_message_time_created_idx` ON `session_message` (`time_created`);
CREATE INDEX `session_project_idx` ON `session` (`project_id`);
CREATE INDEX `session_workspace_idx` ON `session` (`workspace_id`);
CREATE INDEX `session_parent_idx` ON `session` (`parent_id`);

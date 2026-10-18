-- Frozen WS v02 DDL: 5573129a75:packages/core/src/database/schema.gen.ts.
CREATE TABLE `session_message_part` (
  `message_id` text NOT NULL, `position` integer NOT NULL, `id` text NOT NULL, `type` text NOT NULL, `data` text NOT NULL,
  CONSTRAINT `session_message_part_pk` PRIMARY KEY(`message_id`, `position`),
  CONSTRAINT `fk_session_message_part_message_id_session_message_id_fk` FOREIGN KEY (`message_id`) REFERENCES `session_message`(`id`) ON DELETE CASCADE,
  CONSTRAINT "session_message_part_data_json_object" CHECK(json_valid("data") AND json_type("data") = 'object')
);
CREATE INDEX `session_message_part_lookup_idx` ON `session_message_part` (`message_id`,`type`,`id`,"position" desc);

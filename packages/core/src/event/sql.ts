import { sqliteTable, text, integer, index, uniqueIndex } from "drizzle-orm/sqlite-core"
import { Event } from "@opencode/schema/event"

export const WsArchiveTable = sqliteTable("_ws_archive", {
  id: text().primaryKey().notNull(),
  manifest: text().notNull(),
})

export const WsImportRootTable = sqliteTable("_ws_import_root", {
  aggregate_id: text().primaryKey().notNull(),
  archive_id: text().notNull().references(() => WsArchiveTable.id),
  through_seq: integer().notNull(),
  codec_version: integer().notNull(),
})

export const EventSequenceTable = sqliteTable("event_sequence", {
  aggregate_id: text().notNull().primaryKey(),
  seq: integer().notNull(),
  owner_id: text(),
})

export const EventTable = sqliteTable(
  "event",
  {
    id: text().$type<Event.ID>().primaryKey(),
    aggregate_id: text()
      .notNull()
      .references(() => EventSequenceTable.aggregate_id, { onDelete: "cascade" }),
    seq: integer().notNull(),
    created: integer().notNull().default(0),
    type: text().notNull(),
    data: text({ mode: "json" }).$type<Record<string, unknown>>().notNull(),
  },
  (table) => [
    uniqueIndex("event_aggregate_seq_idx").on(table.aggregate_id, table.seq),
    index("event_aggregate_type_seq_idx").on(table.aggregate_id, table.type, table.seq),
  ],
)

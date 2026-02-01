PRAGMA foreign_keys = ON;

BEGIN TRANSACTION;

CREATE TABLE IF NOT EXISTS notification_templates (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  event_key TEXT NOT NULL UNIQUE,
  template_code TEXT NOT NULL,
  enabled INTEGER NOT NULL DEFAULT 1,
  message_format TEXT
);

COMMIT;

PRAGMA foreign_keys = ON;

BEGIN TRANSACTION;

-- users: vendor link for external companies (optional)
ALTER TABLE users ADD COLUMN vendor_id INTEGER;

-- work_orders: outsourcing decision fields
ALTER TABLE work_orders ADD COLUMN outsourcing_mode TEXT NOT NULL DEFAULT 'INHOUSE';
ALTER TABLE work_orders ADD COLUMN vendor_id INTEGER;
ALTER TABLE work_orders ADD COLUMN outsourcing_note TEXT;
ALTER TABLE work_orders ADD COLUMN outsourcing_decided_by INTEGER;
ALTER TABLE work_orders ADD COLUMN outsourcing_decided_at TEXT;

-- notification queue for Kakao or other channels
CREATE TABLE IF NOT EXISTS notification_queue (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  channel TEXT NOT NULL,
  recipient TEXT,
  payload_json TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'PENDING',
  created_at TEXT NOT NULL DEFAULT (datetime('now')),
  sent_at TEXT,
  error TEXT
);

COMMIT;

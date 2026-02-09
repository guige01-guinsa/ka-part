PRAGMA journal_mode=WAL;

CREATE TABLE IF NOT EXISTS vehicles (
  plate TEXT PRIMARY KEY,
  unit TEXT,
  owner_name TEXT,
  status TEXT NOT NULL DEFAULT 'active',
  valid_from TEXT,
  valid_to TEXT,
  note TEXT,
  updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS violations (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  plate TEXT NOT NULL,
  verdict TEXT NOT NULL,
  rule_code TEXT,
  location TEXT,
  memo TEXT,
  inspector TEXT,
  photo_path TEXT,
  lat REAL,
  lng REAL,
  created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE INDEX IF NOT EXISTS idx_violations_plate ON violations(plate);
CREATE INDEX IF NOT EXISTS idx_violations_created_at ON violations(created_at);

CREATE TABLE IF NOT EXISTS users (
  username TEXT PRIMARY KEY,
  pw_hash TEXT NOT NULL,
  role TEXT NOT NULL,
  created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

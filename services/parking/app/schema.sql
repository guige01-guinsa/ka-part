PRAGMA journal_mode=WAL;

CREATE TABLE IF NOT EXISTS vehicles (
  site_code TEXT NOT NULL,
  plate TEXT NOT NULL,
  unit TEXT,
  owner_name TEXT,
  status TEXT NOT NULL DEFAULT 'active',
  valid_from TEXT,
  valid_to TEXT,
  note TEXT,
  updated_at TEXT NOT NULL DEFAULT (datetime('now')),
  PRIMARY KEY (site_code, plate)
);

CREATE TABLE IF NOT EXISTS violations (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  site_code TEXT NOT NULL DEFAULT 'COMMON',
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

CREATE TABLE IF NOT EXISTS users (
  username TEXT PRIMARY KEY,
  pw_hash TEXT NOT NULL,
  role TEXT NOT NULL,
  created_at TEXT NOT NULL DEFAULT (datetime('now'))
);

PRAGMA journal_mode=WAL;
PRAGMA foreign_keys=ON;

CREATE TABLE IF NOT EXISTS sites (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  name TEXT NOT NULL UNIQUE,
  created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS entries (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  site_id INTEGER NOT NULL,
  entry_date TEXT NOT NULL,            -- YYYY-MM-DD
  work_type TEXT NOT NULL DEFAULT '',
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  UNIQUE(site_id, entry_date, work_type),
  FOREIGN KEY(site_id) REFERENCES sites(id) ON DELETE CASCADE
);

-- Each tab stores its fields as key/value rows (flexible schema)
CREATE TABLE IF NOT EXISTS entry_values (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  entry_id INTEGER NOT NULL,
  tab_key TEXT NOT NULL,               -- e.g. home, tr450, tr400, meter, facility
  field_key TEXT NOT NULL,             -- e.g. manager, temp, voltage
  value_text TEXT NOT NULL,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  UNIQUE(entry_id, tab_key, field_key),
  FOREIGN KEY(entry_id) REFERENCES entries(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_entries_date ON entries(entry_date);
CREATE INDEX IF NOT EXISTS idx_entries_site_date_work_type ON entries(site_id, entry_date, work_type);
CREATE INDEX IF NOT EXISTS idx_values_tab ON entry_values(tab_key);

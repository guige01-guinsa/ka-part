-- sql/20260128_inspection_meter.sql
PRAGMA foreign_keys = ON;

-- ============================================================
-- 1) meters
-- ============================================================
CREATE TABLE IF NOT EXISTS meters (
  id            INTEGER PRIMARY KEY AUTOINCREMENT,
  meter_code    TEXT UNIQUE,
  name          TEXT NOT NULL,
  location_id   INTEGER,
  category      TEXT,
  unit          TEXT,
  digits        INTEGER DEFAULT 0,
  note          TEXT DEFAULT '',
  created_at    TEXT DEFAULT (datetime('now')),
  updated_at    TEXT DEFAULT (datetime('now'))
);

ALTER TABLE meters ADD COLUMN meter_code   TEXT;
ALTER TABLE meters ADD COLUMN name        TEXT;
ALTER TABLE meters ADD COLUMN location_id INTEGER;
ALTER TABLE meters ADD COLUMN category    TEXT;
ALTER TABLE meters ADD COLUMN unit        TEXT;
ALTER TABLE meters ADD COLUMN digits      INTEGER DEFAULT 0;
ALTER TABLE meters ADD COLUMN note        TEXT DEFAULT '';
ALTER TABLE meters ADD COLUMN created_at  TEXT;
ALTER TABLE meters ADD COLUMN updated_at  TEXT;

CREATE INDEX IF NOT EXISTS idx_meters_location_id ON meters(location_id);
CREATE INDEX IF NOT EXISTS idx_meters_category    ON meters(category);

-- ============================================================
-- 2) meter_reads
-- ============================================================
CREATE TABLE IF NOT EXISTS meter_reads (
  id            INTEGER PRIMARY KEY AUTOINCREMENT,
  meter_id      INTEGER NOT NULL,
  read_date     TEXT NOT NULL,
  raw_value     TEXT NOT NULL,
  value         REAL,
  reader_login  TEXT,
  note          TEXT DEFAULT '',
  created_at    TEXT DEFAULT (datetime('now')),
  UNIQUE(meter_id, read_date),
  FOREIGN KEY (meter_id) REFERENCES meters(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_meter_reads_meter_date ON meter_reads(meter_id, read_date);
CREATE INDEX IF NOT EXISTS idx_meter_reads_date       ON meter_reads(read_date);

-- ============================================================
-- 3) inspections (legacy helper)
-- ============================================================
CREATE TABLE IF NOT EXISTS inspections (
  id            INTEGER PRIMARY KEY AUTOINCREMENT,
  insp_code     TEXT UNIQUE,
  title         TEXT NOT NULL,
  location_id   INTEGER,
  category      TEXT,
  status        TEXT DEFAULT 'OPEN',
  urgent        INTEGER DEFAULT 0,
  result_note   TEXT DEFAULT '',
  created_at    TEXT DEFAULT (datetime('now')),
  updated_at    TEXT DEFAULT (datetime('now'))
);

ALTER TABLE inspections ADD COLUMN insp_code    TEXT;
ALTER TABLE inspections ADD COLUMN title        TEXT;
ALTER TABLE inspections ADD COLUMN location_id  INTEGER;
ALTER TABLE inspections ADD COLUMN category     TEXT;
ALTER TABLE inspections ADD COLUMN status       TEXT DEFAULT 'OPEN';
ALTER TABLE inspections ADD COLUMN urgent       INTEGER DEFAULT 0;
ALTER TABLE inspections ADD COLUMN result_note  TEXT DEFAULT '';
ALTER TABLE inspections ADD COLUMN created_at   TEXT;
ALTER TABLE inspections ADD COLUMN updated_at   TEXT;

CREATE INDEX IF NOT EXISTS idx_inspections_status  ON inspections(status);
CREATE INDEX IF NOT EXISTS idx_inspections_created ON inspections(created_at);

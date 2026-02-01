-- sql/20260128_inspection_meter.sql
.bail off
PRAGMA foreign_keys = ON;

-- ============================================================
-- 1) meters: 기존 테이블이 있든 없든 "필요 컬럼"을 맞춘다
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

-- 기존 meters가 오래된 스키마일 수 있으니 컬럼 보강 (있으면 에러 나도 계속)
ALTER TABLE meters ADD COLUMN meter_code   TEXT;
ALTER TABLE meters ADD COLUMN name        TEXT;
ALTER TABLE meters ADD COLUMN location_id INTEGER;
ALTER TABLE meters ADD COLUMN category    TEXT;
ALTER TABLE meters ADD COLUMN unit        TEXT;
ALTER TABLE meters ADD COLUMN digits      INTEGER DEFAULT 0;
ALTER TABLE meters ADD COLUMN note        TEXT DEFAULT '';
ALTER TABLE meters ADD COLUMN created_at  TEXT;
ALTER TABLE meters ADD COLUMN updated_at  TEXT;

-- 인덱스는 컬럼 보강 후
CREATE INDEX IF NOT EXISTS idx_meters_location_id ON meters(location_id);
CREATE INDEX IF NOT EXISTS idx_meters_category    ON meters(category);

-- ============================================================
-- 2) meter_reads: 없으면 만들고, 있으면 그대로 둔다
-- ============================================================
CREATE TABLE IF NOT EXISTS meter_reads (
  id            INTEGER PRIMARY KEY AUTOINCREMENT,
  meter_id      INTEGER NOT NULL,
  read_date     TEXT NOT NULL,       -- YYYY-MM-DD
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

-- 샘플 1건 (name 컬럼만은 대부분 존재하므로 최소 가정)
INSERT OR IGNORE INTO meters (id, name) VALUES (1, '전기실 메인 계량기');

-- ============================================================
-- 3) inspections: 기존 테이블이 있든 없든 "status/urgent 등" 보강
-- ============================================================
CREATE TABLE IF NOT EXISTS inspections (
  id            INTEGER PRIMARY KEY AUTOINCREMENT,
  insp_code     TEXT UNIQUE,
  title         TEXT NOT NULL,
  location_id   INTEGER,
  category      TEXT,
  status        TEXT DEFAULT 'OPEN',  -- OPEN/DONE/CANCELED
  urgent        INTEGER DEFAULT 0,
  result_note   TEXT DEFAULT '',
  created_at    TEXT DEFAULT (datetime('now')),
  updated_at    TEXT DEFAULT (datetime('now'))
);

-- 기존 inspections가 예전 스키마면 컬럼 보강
ALTER TABLE inspections ADD COLUMN insp_code    TEXT;
ALTER TABLE inspections ADD COLUMN title        TEXT;
ALTER TABLE inspections ADD COLUMN location_id  INTEGER;
ALTER TABLE inspections ADD COLUMN category     TEXT;
ALTER TABLE inspections ADD COLUMN status       TEXT DEFAULT 'OPEN';
ALTER TABLE inspections ADD COLUMN urgent       INTEGER DEFAULT 0;
ALTER TABLE inspections ADD COLUMN result_note  TEXT DEFAULT '';
ALTER TABLE inspections ADD COLUMN created_at   TEXT;
ALTER TABLE inspections ADD COLUMN updated_at   TEXT;

-- 인덱스는 컬럼 보강 후
CREATE INDEX IF NOT EXISTS idx_inspections_status  ON inspections(status);
CREATE INDEX IF NOT EXISTS idx_inspections_created ON inspections(created_at);

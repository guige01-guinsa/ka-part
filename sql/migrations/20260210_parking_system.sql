PRAGMA foreign_keys = ON;

BEGIN TRANSACTION;

CREATE TABLE IF NOT EXISTS complexes (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  code TEXT NOT NULL UNIQUE,
  name TEXT NOT NULL,
  is_active INTEGER NOT NULL DEFAULT 1,
  created_at TEXT NOT NULL DEFAULT (datetime('now')),
  updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);

CREATE TABLE IF NOT EXISTS user_complexes (
  user_id INTEGER NOT NULL,
  complex_id INTEGER NOT NULL,
  is_primary INTEGER NOT NULL DEFAULT 1,
  created_at TEXT NOT NULL DEFAULT (datetime('now')),
  PRIMARY KEY (user_id, complex_id),
  FOREIGN KEY (user_id) REFERENCES users(id),
  FOREIGN KEY (complex_id) REFERENCES complexes(id)
);

CREATE TABLE IF NOT EXISTS parking_illegal_vehicles (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  complex_id INTEGER NOT NULL,
  plate_number TEXT NOT NULL,
  plate_normalized TEXT NOT NULL,
  reason TEXT NOT NULL DEFAULT '미등록/불법 주차 차량',
  memo TEXT,
  status TEXT NOT NULL DEFAULT 'ACTIVE' CHECK (status IN ('ACTIVE','CLEARED')),
  reported_by_user_id INTEGER,
  created_at TEXT NOT NULL DEFAULT (datetime('now')),
  updated_at TEXT NOT NULL DEFAULT (datetime('now')),
  cleared_at TEXT,
  cleared_by_user_id INTEGER,
  FOREIGN KEY (complex_id) REFERENCES complexes(id),
  FOREIGN KEY (reported_by_user_id) REFERENCES users(id),
  FOREIGN KEY (cleared_by_user_id) REFERENCES users(id)
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_parking_illegal_active_plate
ON parking_illegal_vehicles(complex_id, plate_normalized)
WHERE status='ACTIVE';

CREATE INDEX IF NOT EXISTS ix_parking_illegal_complex_status
ON parking_illegal_vehicles(complex_id, status, updated_at);

CREATE TABLE IF NOT EXISTS parking_scan_logs (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  complex_id INTEGER NOT NULL,
  scanned_by_user_id INTEGER,
  plate_input TEXT,
  plate_normalized TEXT,
  source TEXT NOT NULL DEFAULT 'MANUAL' CHECK (source IN ('MANUAL','OCR')),
  verdict TEXT NOT NULL CHECK (verdict IN ('ILLEGAL','CLEAR','UNKNOWN')),
  illegal_vehicle_id INTEGER,
  scanned_at TEXT NOT NULL DEFAULT (datetime('now')),
  FOREIGN KEY (complex_id) REFERENCES complexes(id),
  FOREIGN KEY (scanned_by_user_id) REFERENCES users(id),
  FOREIGN KEY (illegal_vehicle_id) REFERENCES parking_illegal_vehicles(id)
);

CREATE INDEX IF NOT EXISTS ix_parking_scan_complex_time
ON parking_scan_logs(complex_id, scanned_at DESC);

CREATE INDEX IF NOT EXISTS ix_user_complexes_complex
ON user_complexes(complex_id, is_primary);

INSERT OR IGNORE INTO complexes(code, name, is_active, created_at, updated_at)
VALUES ('KA-DEFAULT', 'ka-part 아파트', 1, datetime('now'), datetime('now'));

INSERT OR IGNORE INTO user_complexes(user_id, complex_id, is_primary, created_at)
SELECT u.id, c.id, 1, datetime('now')
FROM users u
JOIN complexes c ON c.code='KA-DEFAULT';

COMMIT;

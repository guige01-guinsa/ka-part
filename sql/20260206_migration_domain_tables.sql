BEGIN;

CREATE TABLE IF NOT EXISTS home_logs (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  site_name TEXT NOT NULL,
  login_name TEXT,
  manager TEXT,
  work_type TEXT CHECK(work_type IN ('일일','주간','월간','정기','기타일상')) NOT NULL,
  important_work TEXT,
  note TEXT,
  created_at TEXT DEFAULT (datetime('now','localtime'))
);

CREATE TABLE IF NOT EXISTS transformer_450_reads (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  site_name TEXT NOT NULL,
  entry_date TEXT,
  lv1_l1_v REAL, lv1_l1_a REAL, lv1_l1_kw REAL,
  lv1_l2_v REAL, lv1_l2_a REAL, lv1_l2_kw REAL,
  lv1_l3_v REAL, lv1_l3_a REAL, lv1_l3_kw REAL,
  lv1_temp REAL,
  created_at TEXT DEFAULT (datetime('now','localtime')),
  updated_at TEXT
);
CREATE UNIQUE INDEX IF NOT EXISTS ux_tr450_site_date
ON transformer_450_reads(site_name, entry_date);

CREATE TABLE IF NOT EXISTS transformer_400_reads (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  site_name TEXT NOT NULL,
  entry_date TEXT,
  lv2_l1_v REAL, lv2_l1_a REAL, lv2_l1_kw REAL,
  lv2_l2_v REAL, lv2_l2_a REAL, lv2_l2_kw REAL,
  lv2_l3_v REAL, lv2_l3_a REAL, lv2_l3_kw REAL,
  lv2_temp REAL,
  created_at TEXT DEFAULT (datetime('now','localtime')),
  updated_at TEXT
);
CREATE UNIQUE INDEX IF NOT EXISTS ux_tr400_site_date
ON transformer_400_reads(site_name, entry_date);

CREATE TABLE IF NOT EXISTS power_meter_reads (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  site_name TEXT NOT NULL,
  entry_date TEXT,
  aiss_l1_a REAL, aiss_l2_a REAL, aiss_l3_a REAL,
  main_kwh REAL, industry_kwh REAL, street_kwh REAL,
  created_at TEXT DEFAULT (datetime('now','localtime')),
  updated_at TEXT
);
CREATE UNIQUE INDEX IF NOT EXISTS ux_meter_site_date
ON power_meter_reads(site_name, entry_date);

CREATE TABLE IF NOT EXISTS facility_tasks (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  site_name TEXT NOT NULL,
  title TEXT NOT NULL,
  status TEXT CHECK(status IN ('완료','진행중')) NOT NULL,
  content TEXT,
  note TEXT,
  created_at TEXT DEFAULT (datetime('now','localtime'))
);

CREATE TABLE IF NOT EXISTS facility_checks (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  site_name TEXT NOT NULL,
  entry_date TEXT,
  tank_level_1 REAL,
  tank_level_2 REAL,
  hydrant_pressure REAL,
  sp_pump_pressure REAL,
  high_pressure REAL,
  low_pressure REAL,
  office_pressure REAL,
  shop_pressure REAL,
  created_at TEXT DEFAULT (datetime('now','localtime')),
  updated_at TEXT
);
CREATE UNIQUE INDEX IF NOT EXISTS ux_fac_site_date
ON facility_checks(site_name, entry_date);

CREATE TABLE IF NOT EXISTS facility_subtasks (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  site_name TEXT NOT NULL,
  entry_date TEXT,
  domain_key TEXT NOT NULL CHECK(domain_key IN ('fire','mechanical','telecom')),
  task_title TEXT,
  status TEXT,
  criticality TEXT,
  detail TEXT,
  next_due TEXT,
  created_at TEXT DEFAULT (datetime('now','localtime')),
  updated_at TEXT
);
CREATE UNIQUE INDEX IF NOT EXISTS ux_subtasks_site_date_domain
ON facility_subtasks(site_name, entry_date, domain_key);

COMMIT;

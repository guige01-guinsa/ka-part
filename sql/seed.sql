PRAGMA foreign_keys = ON;

BEGIN TRANSACTION;

-- ---------------------------------------------------------
-- 1) 사용자(admin)
-- ---------------------------------------------------------
INSERT OR IGNORE INTO users(name, login, phone, is_active)
VALUES ('관리소장', 'admin', NULL, 1);

-- ---------------------------------------------------------
-- 2) 역할(roles)
-- ---------------------------------------------------------
INSERT OR IGNORE INTO roles(code, name) VALUES ('TECH','시설기사');
INSERT OR IGNORE INTO roles(code, name) VALUES ('LEAD','시설과장');
INSERT OR IGNORE INTO roles(code, name) VALUES ('MANAGER','관리소장');
INSERT OR IGNORE INTO roles(code, name) VALUES ('ACCOUNTING','경리');

-- ---------------------------------------------------------
-- 3) admin에게 MANAGER 부여
-- ---------------------------------------------------------
INSERT OR IGNORE INTO user_roles(user_id, role_id)
SELECT u.id, r.id
FROM users u, roles r
WHERE u.login='admin' AND r.code='MANAGER';

-- ---------------------------------------------------------
-- 4) 기본 분류(categories)
-- ---------------------------------------------------------
INSERT OR IGNORE INTO categories(code, name) VALUES ('ELEC','전기');
INSERT OR IGNORE INTO categories(code, name) VALUES ('FIRE','소방');
INSERT OR IGNORE INTO categories(code, name) VALUES ('ELEV','승강기');
INSERT OR IGNORE INTO categories(code, name) VALUES ('MECH','기계/설비');
INSERT OR IGNORE INTO categories(code, name) VALUES ('ARCH','건축');
INSERT OR IGNORE INTO categories(code, name) VALUES ('COMM','통신');

-- ---------------------------------------------------------
-- 5) 공용부 위치(locations) - 사용자가 준 기준 그대로
-- ---------------------------------------------------------
INSERT OR IGNORE INTO locations(type, code, name, is_active) VALUES ('COMMON','LOC_ELECROOM','전기실',1);
INSERT OR IGNORE INTO locations(type, code, name, is_active) VALUES ('COMMON','LOC_PUMPROOM','펌프실',1);
INSERT OR IGNORE INTO locations(type, code, name, is_active) VALUES ('COMMON','LOC_WATERTANK','저수조실',1);
INSERT OR IGNORE INTO locations(type, code, name, is_active) VALUES ('COMMON','LOC_HEATEX','열교환기실',1);
INSERT OR IGNORE INTO locations(type, code, name, is_active) VALUES ('COMMON','LOC_ELEV_MR','승강기기계실',1);
INSERT OR IGNORE INTO locations(type, code, name, is_active) VALUES ('COMMON','LOC_STORE1','창고1',1);
INSERT OR IGNORE INTO locations(type, code, name, is_active) VALUES ('COMMON','LOC_STORE2','창고2',1);
INSERT OR IGNORE INTO locations(type, code, name, is_active) VALUES ('COMMON','LOC_PARK1','주차장1',1);
INSERT OR IGNORE INTO locations(type, code, name, is_active) VALUES ('COMMON','LOC_PARK2','주차장2',1);


INSERT OR IGNORE INTO roles(code, name) VALUES ('CHIEF','Chief');
INSERT OR IGNORE INTO roles(code, name) VALUES ('FACILITY_MANAGER','Facility Manager');
INSERT OR IGNORE INTO roles(code, name) VALUES ('STAFF','Staff');
INSERT OR IGNORE INTO roles(code, name) VALUES ('RESIDENT','Resident');
INSERT OR IGNORE INTO roles(code, name) VALUES ('VENDOR','Vendor');

COMMIT;

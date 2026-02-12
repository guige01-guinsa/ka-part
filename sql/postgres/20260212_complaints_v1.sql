-- Complaints app schema for PostgreSQL 15+
-- Scope: resident complaints, admin handling, visit policy, audit trail

BEGIN;

-- -------------------------------------------------------------------------
-- Enums
-- -------------------------------------------------------------------------
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'complaint_scope') THEN
        CREATE TYPE complaint_scope AS ENUM ('COMMON', 'PRIVATE', 'EMERGENCY');
    END IF;
END $$;

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'complaint_status') THEN
        CREATE TYPE complaint_status AS ENUM (
            'RECEIVED',
            'TRIAGED',
            'GUIDANCE_SENT',
            'ASSIGNED',
            'IN_PROGRESS',
            'COMPLETED',
            'CLOSED'
        );
    END IF;
END $$;

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'visit_reason') THEN
        CREATE TYPE visit_reason AS ENUM (
            'FIRE_INSPECTION',
            'NEIGHBOR_DAMAGE',
            'EMERGENCY_INFRA'
        );
    END IF;
END $$;

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'resolution_type') THEN
        CREATE TYPE resolution_type AS ENUM (
            'REPAIR',
            'GUIDANCE_ONLY',
            'EXTERNAL_VENDOR'
        );
    END IF;
END $$;

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'work_order_status') THEN
        CREATE TYPE work_order_status AS ENUM (
            'OPEN',
            'DISPATCHED',
            'DONE',
            'CANCELED'
        );
    END IF;
END $$;

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'user_role') THEN
        CREATE TYPE user_role AS ENUM (
            'RESIDENT',
            'STAFF',
            'ADMIN'
        );
    END IF;
END $$;

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'user_status') THEN
        CREATE TYPE user_status AS ENUM (
            'ACTIVE',
            'INACTIVE'
        );
    END IF;
END $$;

-- -------------------------------------------------------------------------
-- Core master tables
-- -------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS complexes (
    id              BIGSERIAL PRIMARY KEY,
    name            VARCHAR(120) NOT NULL,
    address         TEXT NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS buildings (
    id              BIGSERIAL PRIMARY KEY,
    complex_id      BIGINT NOT NULL REFERENCES complexes(id) ON DELETE CASCADE,
    building_no     VARCHAR(20) NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (complex_id, building_no)
);

CREATE TABLE IF NOT EXISTS units (
    id              BIGSERIAL PRIMARY KEY,
    building_id     BIGINT NOT NULL REFERENCES buildings(id) ON DELETE CASCADE,
    unit_no         VARCHAR(20) NOT NULL,
    floor           INTEGER,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (building_id, unit_no)
);

CREATE TABLE IF NOT EXISTS users (
    id              BIGSERIAL PRIMARY KEY,
    role            user_role NOT NULL,
    status          user_status NOT NULL DEFAULT 'ACTIVE',
    name            VARCHAR(80) NOT NULL,
    phone           VARCHAR(30) NOT NULL,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (phone)
);

CREATE TABLE IF NOT EXISTS household_members (
    id              BIGSERIAL PRIMARY KEY,
    unit_id         BIGINT NOT NULL REFERENCES units(id) ON DELETE CASCADE,
    user_id         BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    is_head         BOOLEAN NOT NULL DEFAULT FALSE,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (unit_id, user_id)
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_household_head_per_unit
    ON household_members(unit_id)
    WHERE is_head = TRUE;

CREATE TABLE IF NOT EXISTS complaint_categories (
    id              BIGSERIAL PRIMARY KEY,
    name            VARCHAR(80) NOT NULL,
    scope           complaint_scope NOT NULL,
    is_active       BOOLEAN NOT NULL DEFAULT TRUE,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (name, scope)
);

CREATE TABLE IF NOT EXISTS guidance_templates (
    id              BIGSERIAL PRIMARY KEY,
    title           VARCHAR(120) NOT NULL,
    content         TEXT NOT NULL,
    is_active       BOOLEAN NOT NULL DEFAULT TRUE,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- -------------------------------------------------------------------------
-- Complaint lifecycle
-- -------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS complaints (
    id                  BIGSERIAL PRIMARY KEY,
    ticket_no           VARCHAR(30) NOT NULL UNIQUE,
    unit_id             BIGINT NOT NULL REFERENCES units(id) ON DELETE RESTRICT,
    reporter_user_id    BIGINT NOT NULL REFERENCES users(id) ON DELETE RESTRICT,
    category_id         BIGINT NOT NULL REFERENCES complaint_categories(id) ON DELETE RESTRICT,
    assigned_to_user_id BIGINT REFERENCES users(id) ON DELETE SET NULL,
    guidance_template_id BIGINT REFERENCES guidance_templates(id) ON DELETE SET NULL,

    scope               complaint_scope NOT NULL,
    status              complaint_status NOT NULL DEFAULT 'RECEIVED',
    priority            VARCHAR(10) NOT NULL DEFAULT 'NORMAL',
    resolution_type     resolution_type,
    title               VARCHAR(140) NOT NULL,
    description         TEXT NOT NULL,
    location_detail     VARCHAR(200),

    requires_visit      BOOLEAN NOT NULL DEFAULT FALSE,
    visit_reason        visit_reason,

    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    triaged_at          TIMESTAMPTZ,
    closed_at           TIMESTAMPTZ,
    updated_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),

    CONSTRAINT chk_ticket_no_format CHECK (ticket_no ~ '^C-[0-9]{8}-[0-9]{5}$'),
    CONSTRAINT chk_priority CHECK (priority IN ('LOW', 'NORMAL', 'HIGH', 'URGENT')),
    CONSTRAINT chk_visit_reason_pair CHECK (
        (requires_visit = FALSE AND visit_reason IS NULL)
        OR (requires_visit = TRUE AND visit_reason IS NOT NULL)
    ),
    CONSTRAINT chk_private_guidance_only CHECK (
        scope <> 'PRIVATE' OR resolution_type = 'GUIDANCE_ONLY'
    )
);

CREATE TABLE IF NOT EXISTS complaint_attachments (
    id              BIGSERIAL PRIMARY KEY,
    complaint_id    BIGINT NOT NULL REFERENCES complaints(id) ON DELETE CASCADE,
    file_url        TEXT NOT NULL,
    mime_type       VARCHAR(80) NOT NULL,
    size_bytes      BIGINT NOT NULL CHECK (size_bytes >= 0),
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS complaint_status_history (
    id              BIGSERIAL PRIMARY KEY,
    complaint_id    BIGINT NOT NULL REFERENCES complaints(id) ON DELETE CASCADE,
    from_status     complaint_status,
    to_status       complaint_status NOT NULL,
    changed_by      BIGINT NOT NULL REFERENCES users(id) ON DELETE RESTRICT,
    note            TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS complaint_comments (
    id              BIGSERIAL PRIMARY KEY,
    complaint_id    BIGINT NOT NULL REFERENCES complaints(id) ON DELETE CASCADE,
    user_id         BIGINT NOT NULL REFERENCES users(id) ON DELETE RESTRICT,
    comment         TEXT NOT NULL,
    is_internal     BOOLEAN NOT NULL DEFAULT FALSE,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS work_orders (
    id              BIGSERIAL PRIMARY KEY,
    complaint_id    BIGINT NOT NULL REFERENCES complaints(id) ON DELETE CASCADE,
    assignee_user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE RESTRICT,
    status          work_order_status NOT NULL DEFAULT 'OPEN',
    scheduled_at    TIMESTAMPTZ,
    completed_at    TIMESTAMPTZ,
    result_note     TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS visit_logs (
    id              BIGSERIAL PRIMARY KEY,
    complaint_id    BIGINT NOT NULL REFERENCES complaints(id) ON DELETE CASCADE,
    unit_id         BIGINT NOT NULL REFERENCES units(id) ON DELETE RESTRICT,
    visitor_user_id BIGINT NOT NULL REFERENCES users(id) ON DELETE RESTRICT,
    visit_reason    visit_reason NOT NULL,
    check_in_at     TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    check_out_at    TIMESTAMPTZ,
    result_note     TEXT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT chk_visit_checkout CHECK (check_out_at IS NULL OR check_out_at >= check_in_at)
);

-- -------------------------------------------------------------------------
-- Communication and governance
-- -------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS notices (
    id              BIGSERIAL PRIMARY KEY,
    title           VARCHAR(140) NOT NULL,
    content         TEXT NOT NULL,
    is_pinned       BOOLEAN NOT NULL DEFAULT FALSE,
    published_at    TIMESTAMPTZ,
    author_user_id  BIGINT NOT NULL REFERENCES users(id) ON DELETE RESTRICT,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS faqs (
    id              BIGSERIAL PRIMARY KEY,
    question        VARCHAR(200) NOT NULL,
    answer          TEXT NOT NULL,
    display_order   INTEGER NOT NULL DEFAULT 100,
    is_active       BOOLEAN NOT NULL DEFAULT TRUE,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS notifications (
    id              BIGSERIAL PRIMARY KEY,
    user_id         BIGINT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    type            VARCHAR(40) NOT NULL,
    title           VARCHAR(140) NOT NULL,
    body            TEXT NOT NULL,
    ref_type        VARCHAR(40),
    ref_id          BIGINT,
    read_at         TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS audit_logs (
    id              BIGSERIAL PRIMARY KEY,
    actor_user_id   BIGINT REFERENCES users(id) ON DELETE SET NULL,
    action          VARCHAR(80) NOT NULL,
    target_type     VARCHAR(80) NOT NULL,
    target_id       BIGINT,
    before_json     JSONB,
    after_json      JSONB,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- -------------------------------------------------------------------------
-- Indexes
-- -------------------------------------------------------------------------
CREATE INDEX IF NOT EXISTS idx_complaints_status_created_at
    ON complaints(status, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_complaints_scope_status
    ON complaints(scope, status);

CREATE INDEX IF NOT EXISTS idx_complaints_unit_id
    ON complaints(unit_id);

CREATE INDEX IF NOT EXISTS idx_complaint_history_complaint_id
    ON complaint_status_history(complaint_id, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_work_orders_complaint_id
    ON work_orders(complaint_id);

CREATE INDEX IF NOT EXISTS idx_visit_logs_unit_id
    ON visit_logs(unit_id, check_in_at DESC);

CREATE INDEX IF NOT EXISTS idx_notifications_user_read
    ON notifications(user_id, read_at, created_at DESC);

CREATE INDEX IF NOT EXISTS idx_audit_logs_target
    ON audit_logs(target_type, target_id, created_at DESC);

-- -------------------------------------------------------------------------
-- Triggers and policy guards
-- -------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION set_timestamp_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_complaints_set_updated_at ON complaints;
CREATE TRIGGER trg_complaints_set_updated_at
BEFORE UPDATE ON complaints
FOR EACH ROW
EXECUTE FUNCTION set_timestamp_updated_at();

DROP TRIGGER IF EXISTS trg_notices_set_updated_at ON notices;
CREATE TRIGGER trg_notices_set_updated_at
BEFORE UPDATE ON notices
FOR EACH ROW
EXECUTE FUNCTION set_timestamp_updated_at();

CREATE OR REPLACE FUNCTION enforce_work_order_scope()
RETURNS TRIGGER AS $$
DECLARE
    v_scope complaint_scope;
BEGIN
    SELECT scope INTO v_scope FROM complaints WHERE id = NEW.complaint_id;
    IF v_scope = 'PRIVATE' THEN
        RAISE EXCEPTION USING
            ERRCODE = '23514',
            MESSAGE = 'work_orders are allowed only for COMMON or EMERGENCY complaints';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trg_work_orders_scope_guard ON work_orders;
CREATE TRIGGER trg_work_orders_scope_guard
BEFORE INSERT OR UPDATE ON work_orders
FOR EACH ROW
EXECUTE FUNCTION enforce_work_order_scope();

COMMIT;

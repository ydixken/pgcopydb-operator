-- Fixture schema for the e2e suite (profile v2). Runs as the app user against
-- the app database, so every object lands owned by app: superuser-owned
-- objects break the clone with permission errors when restoring as app.
-- Every statement is idempotent (IF NOT EXISTS / OR REPLACE / exception
-- wrapped) because the seed Job re-runs this file on kept clusters.
-- PG14-clean on purpose: the version matrix (W-E) reuses these fixtures.

-- citext is a trusted extension since PG13, so the database owner may install
-- it without superuser.
CREATE EXTENSION IF NOT EXISTS citext;

-- Custom types: an enum, a domain, and a composite. CREATE TYPE/DOMAIN have
-- no IF NOT EXISTS, hence the exception wrappers.
DO $$ BEGIN
    CREATE TYPE e2e_severity AS ENUM ('debug', 'info', 'warning', 'error', 'critical');
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

DO $$ BEGIN
    CREATE DOMAIN e2e_percent AS numeric CHECK (VALUE >= 0 AND VALUE <= 100);
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

DO $$ BEGIN
    CREATE TYPE e2e_dimensions AS (width_cm numeric, height_cm numeric, depth_cm numeric);
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;

-- customers and orders keep the exact v1 shape: the live-write statements in
-- e2e_test.go insert into orders (customer_id, amount, note) and must
-- survive unchanged.
CREATE TABLE IF NOT EXISTS customers (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    name text NOT NULL,
    created_at timestamptz DEFAULT now()
);

CREATE TABLE IF NOT EXISTS orders (
    id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    customer_id bigint REFERENCES customers(id),
    amount numeric(12,2) NOT NULL,
    note text
);
CREATE INDEX IF NOT EXISTS orders_customer_idx ON orders (customer_id);

-- Second schema; audit.events keeps its v1 shape.
CREATE SCHEMA IF NOT EXISTS audit AUTHORIZATION app;
CREATE TABLE IF NOT EXISTS audit.events (
    id bigserial PRIMARY KEY,
    payload jsonb
);

-- Cross-schema FK: audit -> public.
CREATE TABLE IF NOT EXISTS audit.access_log (
    id bigint PRIMARY KEY,
    customer_id bigint NOT NULL REFERENCES public.customers(id),
    action text NOT NULL,
    occurred_at timestamptz NOT NULL
);

-- citext, domain, arrays, jsonb, and an update trigger.
CREATE TABLE IF NOT EXISTS app_users (
    id bigint PRIMARY KEY,
    email citext NOT NULL UNIQUE,
    display_name text NOT NULL,
    quota_percent e2e_percent NOT NULL DEFAULT 100,
    tags text[] NOT NULL DEFAULT '{}',
    prefs jsonb NOT NULL DEFAULT '{}'::jsonb,
    updated_at timestamptz
);
CREATE INDEX IF NOT EXISTS app_users_tags_gin ON app_users USING gin (tags);
CREATE INDEX IF NOT EXISTS app_users_name_lower_idx ON app_users (lower(display_name));

CREATE OR REPLACE FUNCTION e2e_touch_updated_at() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    NEW.updated_at := now();
    RETURN NEW;
END $$;

CREATE OR REPLACE TRIGGER app_users_touch_updated
    BEFORE UPDATE ON app_users
    FOR EACH ROW EXECUTE FUNCTION e2e_touch_updated_at();

-- RANGE-partitioned by time: 8 monthly partitions plus DEFAULT. The PK must
-- include the partition key; the seed relies on it for ON CONFLICT DO
-- NOTHING. Bounds carry an explicit offset so they are timezone-independent.
CREATE TABLE IF NOT EXISTS events (
    id bigint NOT NULL,
    occurred_at timestamptz NOT NULL,
    customer_id bigint NOT NULL,
    severity e2e_severity NOT NULL,
    source text NOT NULL,
    payload jsonb NOT NULL,
    tags text[] NOT NULL,
    PRIMARY KEY (id, occurred_at)
) PARTITION BY RANGE (occurred_at);

CREATE TABLE IF NOT EXISTS events_2026_01 PARTITION OF events
    FOR VALUES FROM ('2026-01-01 00:00:00+00') TO ('2026-02-01 00:00:00+00');
CREATE TABLE IF NOT EXISTS events_2026_02 PARTITION OF events
    FOR VALUES FROM ('2026-02-01 00:00:00+00') TO ('2026-03-01 00:00:00+00');
CREATE TABLE IF NOT EXISTS events_2026_03 PARTITION OF events
    FOR VALUES FROM ('2026-03-01 00:00:00+00') TO ('2026-04-01 00:00:00+00');
CREATE TABLE IF NOT EXISTS events_2026_04 PARTITION OF events
    FOR VALUES FROM ('2026-04-01 00:00:00+00') TO ('2026-05-01 00:00:00+00');
CREATE TABLE IF NOT EXISTS events_2026_05 PARTITION OF events
    FOR VALUES FROM ('2026-05-01 00:00:00+00') TO ('2026-06-01 00:00:00+00');
CREATE TABLE IF NOT EXISTS events_2026_06 PARTITION OF events
    FOR VALUES FROM ('2026-06-01 00:00:00+00') TO ('2026-07-01 00:00:00+00');
CREATE TABLE IF NOT EXISTS events_2026_07 PARTITION OF events
    FOR VALUES FROM ('2026-07-01 00:00:00+00') TO ('2026-08-01 00:00:00+00');
CREATE TABLE IF NOT EXISTS events_2026_08 PARTITION OF events
    FOR VALUES FROM ('2026-08-01 00:00:00+00') TO ('2026-09-01 00:00:00+00');
CREATE TABLE IF NOT EXISTS events_default PARTITION OF events DEFAULT;

CREATE INDEX IF NOT EXISTS events_customer_time_idx ON events (customer_id, occurred_at);
CREATE INDEX IF NOT EXISTS events_payload_gin ON events USING gin (payload);

-- Arrays, a generated STORED column, a composite-typed column, and a partial
-- index.
CREATE TABLE IF NOT EXISTS readings (
    id bigint PRIMARY KEY,
    sensor text NOT NULL,
    captured_at timestamptz NOT NULL,
    samples double precision[] NOT NULL,
    unit_price numeric(12,4) NOT NULL,
    quantity integer NOT NULL,
    total_price numeric(14,4) GENERATED ALWAYS AS (unit_price * quantity) STORED,
    dimensions e2e_dimensions
);
CREATE INDEX IF NOT EXISTS readings_recent_partial_idx ON readings (captured_at)
    WHERE quantity > 5;

-- TOAST-heavy: STORAGE EXTERNAL disables compression so the ~24KB payloads
-- really occupy TOAST pages instead of compressing away. This table carries
-- most of the fixture volume.
CREATE TABLE IF NOT EXISTS documents (
    id bigint PRIMARY KEY,
    title text NOT NULL,
    body text NOT NULL,
    attachment bytea NOT NULL,
    checksum text NOT NULL
);
ALTER TABLE documents ALTER COLUMN body SET STORAGE EXTERNAL;
ALTER TABLE documents ALTER COLUMN attachment SET STORAGE EXTERNAL;
CREATE INDEX IF NOT EXISTS documents_title_lower_idx ON documents (lower(title));

-- Custom-start sequences; their state must survive the clone.
CREATE SEQUENCE IF NOT EXISTS invoice_number_seq START WITH 720001 INCREMENT BY 10;
CREATE SEQUENCE IF NOT EXISTS ticket_number_seq START WITH 5000 INCREMENT BY 5 MINVALUE 5000;

-- Materialized view; created empty here, refreshed by seed.sql. pg_restore
-- re-populates it on the target with REFRESH, which is what the suite checks.
CREATE MATERIALIZED VIEW IF NOT EXISTS event_daily_counts AS
    SELECT occurred_at::date AS day, severity, count(*) AS n
    FROM events
    GROUP BY 1, 2;

-- Seed bookkeeping. e2e_scaled is the one row-count formula shared with the
-- Go suite (scaled() in e2e_suite_test.go); the two must stay in sync.
CREATE TABLE IF NOT EXISTS e2e_seed (
    profile text NOT NULL,
    scale numeric NOT NULL,
    seeded_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (profile, scale)
);

CREATE OR REPLACE FUNCTION e2e_scaled(n bigint) RETURNS bigint
LANGUAGE sql STABLE PARALLEL SAFE AS $$
    SELECT GREATEST(1, round(n * current_setting('e2e.scale')::numeric))::bigint
$$;

-- Batched inserts with a COMMIT per batch: statement memory and WAL bursts
-- stay bounded, and a crashed seed resumes where it stopped (deterministic
-- ids plus ON CONFLICT DO NOTHING make re-runs converge).
CREATE OR REPLACE PROCEDURE e2e_seed_batches(insert_sql text, total bigint, batch bigint)
LANGUAGE plpgsql AS $$
DECLARE
    b bigint := 1;
BEGIN
    WHILE b <= total LOOP
        EXECUTE format(insert_sql, b, LEAST(b + batch - 1, total));
        COMMIT;
        b := b + batch;
    END LOOP;
END $$;

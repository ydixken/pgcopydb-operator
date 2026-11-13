-- Fixture schema for the e2e suite (base fixture). Runs as the migration role
-- against its database, so every object lands owned by that role: superuser-owned
-- objects break the clone with permission errors when restoring as it.
-- Primary keys and unique constraints only: the non-unique secondary indexes
-- are in finish.sql, built after the data instead of maintained per insert.
-- Every statement is idempotent (IF NOT EXISTS / OR REPLACE / exception
-- wrapped) because the seed Job re-runs this file on kept clusters.
-- PG14-clean on purpose: the version matrix (W-E) reuses these fixtures.

-- citext is a trusted extension since PG13, so the database owner may install
-- it without superuser.
CREATE EXTENSION IF NOT EXISTS citext;

-- Custom types: an enum and a composite. CREATE TYPE has no IF NOT EXISTS,
-- hence the exception wrappers. The e2e_percent domain is in legacy.sql.
DO $$ BEGIN
    CREATE TYPE e2e_severity AS ENUM ('debug', 'info', 'warning', 'error', 'critical');
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

-- Second schema; audit.events keeps its v1 shape.
CREATE SCHEMA IF NOT EXISTS audit AUTHORIZATION CURRENT_USER;
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

-- citext, arrays, jsonb, and an update trigger. legacy.sql adds the UNIQUE
-- on email and retypes quota_percent to its CHECK domain.
CREATE TABLE IF NOT EXISTS app_users (
    id bigint PRIMARY KEY,
    email citext NOT NULL,
    display_name text NOT NULL,
    quota_percent numeric NOT NULL DEFAULT 100,
    tags text[] NOT NULL DEFAULT '{}',
    prefs jsonb NOT NULL DEFAULT '{}'::jsonb,
    updated_at timestamptz
);

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

-- Arrays and a composite-typed column; legacy.sql adds a generated STORED
-- column and a partial index.
CREATE TABLE IF NOT EXISTS readings (
    id bigint PRIMARY KEY,
    sensor text NOT NULL,
    captured_at timestamptz NOT NULL,
    samples double precision[] NOT NULL,
    unit_price numeric(12,4) NOT NULL,
    quantity integer NOT NULL,
    dimensions e2e_dimensions
);

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

-- Custom-start sequences; their state must survive the clone.
CREATE SEQUENCE IF NOT EXISTS invoice_number_seq START WITH 720001 INCREMENT BY 10;
CREATE SEQUENCE IF NOT EXISTS ticket_number_seq START WITH 5000 INCREMENT BY 5 MINVALUE 5000;

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

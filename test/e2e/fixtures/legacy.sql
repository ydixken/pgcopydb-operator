-- Legacy-only fixture stage: the objects the native engine refuses before any
-- database change. run.sh stages it after the loads for the pgcopydb engine
-- only, and the native-refusal spec applies it to a seeded native source.
-- Every statement is idempotent, because both re-run it on kept clusters. It
-- includes no prelude, so the spec can feed it to psql as it is.

\echo legacy-only objects
-- A domain CHECK on a column: native follow refuses table and domain CHECKs.
DO $$ BEGIN
    CREATE DOMAIN e2e_percent AS numeric CHECK (VALUE >= 0 AND VALUE <= 100);
EXCEPTION WHEN duplicate_object THEN NULL;
END $$;
ALTER TABLE app_users ALTER COLUMN quota_percent TYPE e2e_percent;

-- A UNIQUE index on citext uses an extension opclass, which native follow refuses.
DO $$ BEGIN
    ALTER TABLE app_users ADD CONSTRAINT app_users_email_key UNIQUE (email);
EXCEPTION WHEN duplicate_table OR duplicate_object THEN NULL;
END $$;

-- Native follow refuses generated columns. Adding one rewrites readings once.
ALTER TABLE readings ADD COLUMN IF NOT EXISTS
    total_price numeric(14,4) GENERATED ALWAYS AS (unit_price * quantity) STORED;

-- Native follow refuses expression and partial indexes.
CREATE INDEX IF NOT EXISTS app_users_name_lower_idx ON app_users (lower(display_name));
CREATE INDEX IF NOT EXISTS readings_recent_partial_idx ON readings (captured_at)
    WHERE quantity > 5;
CREATE INDEX IF NOT EXISTS documents_title_lower_idx ON documents (lower(title));

-- Native clone refuses materialized views. Created populated after the load;
-- pg_restore repopulates it on the target, which the suite checks.
CREATE MATERIALIZED VIEW IF NOT EXISTS event_daily_counts AS
    SELECT occurred_at::date AS day, severity, count(*) AS n
    FROM events
    GROUP BY 1, 2;

-- Fixture seed stage: everything except the two bulk tables (profile v2).
-- Runs concurrently with the other seed_* stages; see run.sh.
--
-- These are ~7% of the seed together, so they share one stage rather than
-- getting one each. Their order is not arbitrary: orders and audit.access_log
-- both carry an FK to customers, so customers is loaded first. Nothing here
-- references events or documents, which is what lets the three stages run at
-- the same time.
\ir prelude.sql

\echo customers
CALL e2e_seed_batches($sql$
    INSERT INTO customers (id, name)
    OVERRIDING SYSTEM VALUE
    SELECT g, 'customer-' || g
    FROM generate_series(%s, %s) g
    ON CONFLICT (id) DO NOTHING
$sql$, e2e_scaled(50000), 100000);

\echo orders
CALL e2e_seed_batches($sql$
    INSERT INTO orders (id, customer_id, amount, note)
    OVERRIDING SYSTEM VALUE
    SELECT g, (g %% e2e_scaled(50000)) + 1, (g %% 900)::numeric / 7, 'order ' || g
    FROM generate_series(%s, %s) g
    ON CONFLICT (id) DO NOTHING
$sql$, e2e_scaled(200000), 200000);

\echo audit.events
CALL e2e_seed_batches($sql$
    INSERT INTO audit.events (id, payload)
    SELECT g, jsonb_build_object('n', g)
    FROM generate_series(%s, %s) g
    ON CONFLICT (id) DO NOTHING
$sql$, e2e_scaled(1000), 100000);

\echo audit.access_log
CALL e2e_seed_batches($sql$
    INSERT INTO audit.access_log (id, customer_id, action, occurred_at)
    SELECT g, (g %% e2e_scaled(50000)) + 1,
           (ARRAY['read', 'write', 'delete', 'login'])[(g %% 4) + 1],
           timestamptz '2026-01-01 00:00:00+00' + (g %% 15552000) * interval '1 second'
    FROM generate_series(%s, %s) g
    ON CONFLICT (id) DO NOTHING
$sql$, e2e_scaled(100000), 100000);

\echo app_users
CALL e2e_seed_batches($sql$
    INSERT INTO app_users (id, email, display_name, quota_percent, tags, prefs)
    SELECT g, 'User-' || g || '@Example.com', 'User ' || g, g %% 101,
           ARRAY['tier-' || (g %% 7), 'region-' || (g %% 11)],
           jsonb_build_object('theme', CASE WHEN g %% 2 = 0 THEN 'dark' ELSE 'light' END, 'n', g)
    FROM generate_series(%s, %s) g
    ON CONFLICT (id) DO NOTHING
$sql$, e2e_scaled(20000), 100000);

\echo readings
CALL e2e_seed_batches($sql$
    INSERT INTO readings (id, sensor, captured_at, samples, unit_price, quantity, dimensions)
    SELECT g, 'sensor-' || (g %% 500),
           timestamptz '2026-03-01 00:00:00+00' + (g %% 5184000) * interval '1 second',
           (SELECT array_agg(((g + i) %% 1000)::double precision / 7)
              FROM generate_series(1, 16) i),
           ((g %% 9999) + 1)::numeric / 100,
           (g %% 10) + 1,
           ROW(((g %% 50) + 1)::numeric, ((g %% 30) + 1)::numeric,
               ((g %% 20) + 1)::numeric)::e2e_dimensions
    FROM generate_series(%s, %s) g
    ON CONFLICT (id) DO NOTHING
$sql$, e2e_scaled(2500000), 500000);

-- Large objects: 40 at scale 1, 128KB each. No ON CONFLICT for LOs, so top
-- up to the wanted count instead.
\echo large objects
DO $$
DECLARE
    want bigint := e2e_scaled(40);
    have bigint;
BEGIN
    SELECT count(*) INTO have FROM pg_largeobject_metadata;
    WHILE have < want LOOP
        PERFORM lo_from_bytea(0, decode(repeat(md5(have::text), 8192), 'hex'));
        have := have + 1;
    END LOOP;
END $$;

-- Fixture seed for the e2e suite (profile v2). Runs after schema.sql in the
-- same psql session, as the app user, with two variables set by the seed Job:
--   scale    row-count multiplier; 1 targets roughly 12GB of seeded data
--   profile  fixture generation name, bumped when the shape changes
--
-- Idempotency contract: a matching e2e_seed marker means a previous run
-- finished this exact profile and scale, so the whole file short-circuits.
-- Without a marker (fresh cluster or crashed seed) every insert re-runs;
-- deterministic ids and ON CONFLICT DO NOTHING make that converge instead of
-- duplicating. The marker is written last, only after everything succeeded.
-- A kept cluster whose marker MISMATCHES is not handled here: the suite
-- deletes and recreates the cluster before running this Job.

SELECT EXISTS (
    SELECT 1 FROM e2e_seed WHERE profile = :'profile' AND scale = :'scale'::numeric
) AS seeded \gset
\if :seeded
\echo seed: marker matches the requested profile and scale, nothing to do
\quit
\endif

-- psql does not interpolate variables inside quoted strings, so the scale
-- travels into e2e_scaled() (used inside the batched INSERTs) as a session
-- setting.
SELECT set_config('e2e.scale', :'scale', false);

\echo seed: customers
CALL e2e_seed_batches($sql$
    INSERT INTO customers (id, name)
    OVERRIDING SYSTEM VALUE
    SELECT g, 'customer-' || g
    FROM generate_series(%s, %s) g
    ON CONFLICT (id) DO NOTHING
$sql$, e2e_scaled(50000), 100000);

\echo seed: orders
CALL e2e_seed_batches($sql$
    INSERT INTO orders (id, customer_id, amount, note)
    OVERRIDING SYSTEM VALUE
    SELECT g, (g %% e2e_scaled(50000)) + 1, (g %% 900)::numeric / 7, 'order ' || g
    FROM generate_series(%s, %s) g
    ON CONFLICT (id) DO NOTHING
$sql$, e2e_scaled(200000), 200000);

\echo seed: audit.events
CALL e2e_seed_batches($sql$
    INSERT INTO audit.events (id, payload)
    SELECT g, jsonb_build_object('n', g)
    FROM generate_series(%s, %s) g
    ON CONFLICT (id) DO NOTHING
$sql$, e2e_scaled(1000), 100000);

\echo seed: audit.access_log
CALL e2e_seed_batches($sql$
    INSERT INTO audit.access_log (id, customer_id, action, occurred_at)
    SELECT g, (g %% e2e_scaled(50000)) + 1,
           (ARRAY['read', 'write', 'delete', 'login'])[(g %% 4) + 1],
           timestamptz '2026-01-01 00:00:00+00' + (g %% 15552000) * interval '1 second'
    FROM generate_series(%s, %s) g
    ON CONFLICT (id) DO NOTHING
$sql$, e2e_scaled(100000), 100000);

\echo seed: app_users
CALL e2e_seed_batches($sql$
    INSERT INTO app_users (id, email, display_name, quota_percent, tags, prefs)
    SELECT g, 'User-' || g || '@Example.com', 'User ' || g, g %% 101,
           ARRAY['tier-' || (g %% 7), 'region-' || (g %% 11)],
           jsonb_build_object('theme', CASE WHEN g %% 2 = 0 THEN 'dark' ELSE 'light' END, 'n', g)
    FROM generate_series(%s, %s) g
    ON CONFLICT (id) DO NOTHING
$sql$, e2e_scaled(20000), 100000);

-- The bulk row-count load: 8M rows at scale 1, spread over 270 days so all
-- 8 monthly partitions and the DEFAULT one receive data.
\echo seed: events (partitioned)
CALL e2e_seed_batches($sql$
    INSERT INTO events (id, occurred_at, customer_id, severity, source, payload, tags)
    SELECT g,
           timestamptz '2026-01-01 00:00:00+00'
               + (g %% 270) * interval '1 day' + (g %% 86400) * interval '1 second',
           (g %% e2e_scaled(50000)) + 1,
           (ARRAY['debug', 'info', 'warning', 'error', 'critical'])[(g %% 5) + 1]::e2e_severity,
           'generator-' || (g %% 17),
           jsonb_build_object('seq', g, 'src', 'seed', 'even', g %% 2 = 0),
           ARRAY['bucket-' || (g %% 13), 'shard-' || (g %% 7)]
    FROM generate_series(%s, %s) g
    ON CONFLICT (id, occurred_at) DO NOTHING
$sql$, e2e_scaled(8000000), 500000);

\echo seed: readings
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

-- The bulk byte load: ~24KB per row (16KB text + 8KB bytea, both STORAGE
-- EXTERNAL, so nothing compresses away). 350K rows at scale 1 is ~8.5GB of
-- TOAST. Small batches keep each transaction near 120MB.
\echo seed: documents (TOAST)
CALL e2e_seed_batches($sql$
    INSERT INTO documents (id, title, body, attachment, checksum)
    SELECT g, 'Document ' || g,
           repeat(md5(g::text), 500),
           decode(repeat(md5((g * 2654435761)::text), 512), 'hex'),
           md5(g::text)
    FROM generate_series(%s, %s) g
    ON CONFLICT (id) DO NOTHING
$sql$, e2e_scaled(350000), 5000);

-- Large objects: 40 at scale 1, 128KB each. No ON CONFLICT for LOs, so top
-- up to the wanted count instead.
\echo seed: large objects
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

-- The identity/serial sequences did not advance under the explicit-id
-- inserts; put them past the seeded rows so live writes get fresh ids.
SELECT setval(pg_get_serial_sequence('public.customers', 'id'), (SELECT max(id) FROM customers));
SELECT setval(pg_get_serial_sequence('public.orders', 'id'), (SELECT max(id) FROM orders));
SELECT setval(pg_get_serial_sequence('audit.events', 'id'), (SELECT max(id) FROM audit.events));

\echo seed: refreshing event_daily_counts
REFRESH MATERIALIZED VIEW event_daily_counts;

-- Success marker, written last. Old rows go away so a marker always states
-- exactly what the cluster contains.
BEGIN;
DELETE FROM e2e_seed;
INSERT INTO e2e_seed (profile, scale) VALUES (:'profile', :'scale'::numeric);
COMMIT;

\echo seed: done

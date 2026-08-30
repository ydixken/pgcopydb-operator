-- Fixture seed stage: events (profile v2). Runs concurrently with the other
-- seed_* stages; see run.sh.
--
-- The bulk row-count load: 8M rows at scale 1, spread over 270 days so all
-- 8 monthly partitions and the DEFAULT one receive data. This is the phase
-- whose cost is per row rather than per byte, which is why the secondary
-- indexes it feeds are built afterwards by finish.sql.
\ir prelude.sql

\echo events (partitioned)
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

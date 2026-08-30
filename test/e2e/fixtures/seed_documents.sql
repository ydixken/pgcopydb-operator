-- Fixture seed stage: documents (profile v2). Runs concurrently with the other
-- seed_* stages; see run.sh.
--
-- The bulk byte load: ~24KB per row (16KB text + 8KB bytea, both STORAGE
-- EXTERNAL, so nothing compresses away). 350K rows at scale 1 is ~8.5GB of
-- TOAST. Small batches keep each transaction near 120MB. This phase is bound
-- by how fast WAL reaches the volume, so it gains nothing from deferred
-- indexes and everything from having another stage's work overlap its waits.
\ir prelude.sql

\echo documents (TOAST)
CALL e2e_seed_batches($sql$
    INSERT INTO documents (id, title, body, attachment, checksum)
    SELECT g, 'Document ' || g,
           repeat(md5(g::text), 500),
           decode(repeat(md5((g * 2654435761)::text), 512), 'hex'),
           md5(g::text)
    FROM generate_series(%s, %s) g
    ON CONFLICT (id) DO NOTHING
$sql$, e2e_scaled(350000), 5000);

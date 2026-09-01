-- Fixture seed stage: a production-shaped spread of extra tables (optional).
--
-- The v2 fixture is deliberately lopsided: one table carries 73% of the bytes,
-- which is the worst case for table-level parallelism and the reason a clone
-- of it cannot use more than a couple of COPY streams. Real databases are not
-- shaped like that, so this stage adds many tables whose sizes are drawn from
-- a normal distribution and normalised to a requested total.
--
-- Off unless e2e.extra_tables is set. Sizes are deterministic: the generator
-- seeds the RNG, so the same request produces the same layout on every run and
-- two runs remain comparable.
\ir prelude.sql

\echo extra tables (gaussian sizes)

DO $$
DECLARE
    n_tables  int    := current_setting('e2e.extra_tables', true)::int;
    total_mb  bigint := current_setting('e2e.extra_mb', true)::bigint;
    -- Rows of roughly 600 bytes, stored inline, so this stage costs bytes
    -- rather than TOAST round trips: the point here is the table count and
    -- size spread, which seed_documents already covers for TOAST.
    rows_per_mb constant int := 1750;
    mean_mb   numeric;
    sigma     numeric;
    weights   numeric[] := '{}';
    total_w   numeric := 0;
    u1        numeric;
    u2        numeric;
    z         numeric;
    w         numeric;
    mb        int;
    i         int;
BEGIN
    IF n_tables IS NULL OR n_tables < 1 THEN
        RAISE NOTICE 'e2e.extra_tables unset, skipping';
        RETURN;
    END IF;

    mean_mb := total_mb::numeric / n_tables;
    -- Half the mean, so the spread is wide enough to be production-like while
    -- the floor below still keeps every table non-trivial.
    sigma := mean_mb / 2;

    -- Deterministic layout: same request, same sizes, every run.
    PERFORM setseed(0.4242);

    FOR i IN 1..n_tables LOOP
        -- Box-Muller. random() can return 0 and ln(0) is undefined, so the
        -- draw is nudged off the boundary rather than rejected.
        u1 := greatest(random(), 1e-9);
        u2 := random();
        z  := sqrt(-2 * ln(u1)) * cos(2 * pi() * u2);
        w  := greatest(mean_mb + z * sigma, mean_mb * 0.1);
        weights := weights || w;
        total_w := total_w + w;
    END LOOP;

    FOR i IN 1..n_tables LOOP
        -- Normalised so the sizes sum to the requested total whatever the
        -- draw did, and floored at 1MB so no table rounds away to nothing.
        mb := greatest(round(weights[i] / total_w * total_mb)::int, 1);
        EXECUTE format(
            'CREATE TABLE IF NOT EXISTS %I (id bigint PRIMARY KEY, k int, tag text, payload text)',
            format('x_%s', lpad(i::text, 3, '0')));
        EXECUTE format($f$
            INSERT INTO %I (id, k, tag, payload)
            SELECT g, g %% 1000, 'x' || (g %% 17), p.body
            FROM generate_series(1, %s) g,
                 (SELECT repeat(md5('x'), 18) AS body) p
            ON CONFLICT (id) DO NOTHING
        $f$, format('x_%s', lpad(i::text, 3, '0')), mb::bigint * rows_per_mb);
        RAISE NOTICE 'x_% -> % MB', lpad(i::text, 3, '0'), mb;
    END LOOP;
END $$;

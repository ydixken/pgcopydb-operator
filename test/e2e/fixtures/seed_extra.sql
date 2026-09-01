-- Fixture seed stage: a production-shaped spread of extra tables (optional).
--
-- The base fixture is deliberately lopsided: one table carries 73% of the bytes,
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
    -- Every session derives the full layout, then builds its shard.
    shards    int    := current_setting('e2e.extra_shards', true)::int;
    shard     int    := current_setting('e2e.extra_shard', true)::int;
    -- Rows of roughly 600 bytes, stored inline, so this stage costs bytes
    -- rather than TOAST round trips: the point here is the table count and
    -- size spread, which seed_documents already covers for TOAST.
    rows_per_mb constant int := 1750;
    mean_mb   numeric;
    sigma     numeric;
    weights   numeric[] := '{}';
    total_w   numeric := 0;
    cumulative_w numeric := 0;
    remaining_mb bigint;
    allocated_mb bigint := 0;
    next_allocated_mb bigint;
    u1        numeric;
    u2        numeric;
    z         numeric;
    w         numeric;
    mb        bigint;
    i         int;
BEGIN
    IF n_tables IS NULL OR n_tables = 0 THEN
        RAISE NOTICE 'e2e.extra_tables unset, skipping';
        RETURN;
    END IF;
    IF n_tables < 0 THEN
        RAISE EXCEPTION 'e2e.extra_tables must not be negative';
    END IF;
    IF total_mb < n_tables THEN
        RAISE EXCEPTION 'e2e.extra_mb (%) must be at least e2e.extra_tables (%)',
            total_mb, n_tables;
    END IF;
    IF shards < 1 OR shard < 0 OR shard >= shards THEN
        RAISE EXCEPTION 'invalid extra shard % of %', shard, shards;
    END IF;

    remaining_mb := total_mb - n_tables;

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
        -- Cumulative rounding distributes the remainder exactly while the
        -- reserved 1MB keeps every table non-empty.
        cumulative_w := cumulative_w + weights[i];
        next_allocated_mb := round(cumulative_w / total_w * remaining_mb)::bigint;
        mb := 1 + next_allocated_mb - allocated_mb;
        allocated_mb := next_allocated_mb;
        -- Every session computes every size, so sharding cannot change the
        -- deterministic layout.
        CONTINUE WHEN (i % shards) <> shard;
        EXECUTE format(
            'CREATE TABLE IF NOT EXISTS %I (id bigint PRIMARY KEY, k int, tag text, payload text)',
            format('x_%s', lpad(i::text, 3, '0')));
        -- Content and width both vary per row. A constant payload would give
        -- every row the same bytes and the same length, which no real table
        -- has, and would let the target's page layout be unrealistically
        -- uniform. The width swings between roughly 300 and 1100 bytes, so
        -- rows_per_mb above is an average rather than an exact figure.
        EXECUTE format($f$
            INSERT INTO %I (id, k, tag, payload)
            SELECT g, g %% 1000, 'x' || (g %% 17),
                   repeat(md5(g::text || ':%s'), 9 + (g %% 25))
            FROM generate_series(1, %s) g
            ON CONFLICT (id) DO NOTHING
        $f$, format('x_%s', lpad(i::text, 3, '0')), i, mb * rows_per_mb);
        RAISE NOTICE 'x_% -> % MB', lpad(i::text, 3, '0'), mb;
    END LOOP;
    IF allocated_mb + n_tables <> total_mb THEN
        RAISE EXCEPTION 'extra table allocation was % MB, expected % MB',
            allocated_mb + n_tables, total_mb;
    END IF;
END $$;

-- Fixture seed stage (optional): many extra tables on top of the base fixture,
-- whose one table holding 73% of the bytes is the worst case for table-level
-- parallelism. Sizes are normal, or lognormal under a positive e2e.extra_skew
-- so a few giants dominate, normalised to e2e.extra_mb. The RNG is seeded, so
-- a request gives the same layout on every run. Off unless e2e.extra_tables.
\ir prelude.sql

\echo extra tables

DO $$
DECLARE
    n_tables  int    := current_setting('e2e.extra_tables', true)::int;
    total_mb  bigint := current_setting('e2e.extra_mb', true)::bigint;
    -- Every session derives the full layout, then builds its shard.
    shards    int    := current_setting('e2e.extra_shards', true)::int;
    shard     int    := current_setting('e2e.extra_shard', true)::int;
    -- NULL or 0 keeps the normal spread every earlier fixture was built with.
    skew      numeric := current_setting('e2e.extra_skew', true)::numeric;
    -- Rows of roughly 600 bytes, stored inline, so this stage costs bytes
    -- rather than TOAST round trips: the point here is the table count and
    -- size spread, which seed_documents already covers for TOAST.
    rows_per_mb constant int := 1750;
    -- About 128MB per transaction, like seed_documents: a 100GB table never
    -- sits uncommitted and a retry loses one batch at most. The procedure
    -- e2e_seed_batches would need this template's % escaped twice.
    batch_rows constant bigint := 128 * rows_per_mb;
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
    sizes     bigint[] := '{}';
    session_of int[];
    loads     bigint[];
    bins      int;
    bin       int;
    n_rows    bigint;
    first_id  bigint;
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
    IF skew < 0 THEN
        RAISE EXCEPTION 'e2e.extra_skew must not be negative';
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
        IF skew > 0 THEN
            w := mean_mb * exp(skew * z);
        ELSE
            w := greatest(mean_mb + z * sigma, mean_mb * 0.1);
        END IF;
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
        sizes := sizes || mb;
    END LOOP;
    IF allocated_mb + n_tables <> total_mb THEN
        RAISE EXCEPTION 'extra table allocation was % MB, expected % MB',
            allocated_mb + n_tables, total_mb;
    END IF;

    -- Largest table first onto the least-loaded session, ties to the lower
    -- number, so giants spread out and every session derives the same plan.
    -- ponytail: O(tables x sessions), fine for the hundreds x_NNN names imply.
    bins := least(shards, n_tables);
    loads := array_fill(0::bigint, ARRAY[bins]);
    session_of := array_fill(0, ARRAY[n_tables]);
    FOR i IN SELECT t.n FROM unnest(sizes) WITH ORDINALITY AS t(size_mb, n)
             ORDER BY t.size_mb DESC, t.n LOOP
        bin := 1;
        FOR b IN 2..bins LOOP
            IF loads[b] < loads[bin] THEN
                bin := b;
            END IF;
        END LOOP;
        loads[bin] := loads[bin] + sizes[i];
        session_of[i] := bin - 1;
    END LOOP;

    FOR i IN 1..n_tables LOOP
        CONTINUE WHEN session_of[i] <> shard;
        EXECUTE format(
            'CREATE TABLE IF NOT EXISTS %I (id bigint PRIMARY KEY, k int, tag text, payload text)',
            format('x_%s', lpad(i::text, 3, '0')));
        n_rows := sizes[i] * rows_per_mb;
        -- Batches commit in id order, so the highest id present is where a
        -- retried Job resumes instead of starting the table over.
        EXECUTE format('SELECT coalesce(max(id), 0) + 1 FROM %I',
            format('x_%s', lpad(i::text, 3, '0'))) INTO first_id;
        WHILE first_id <= n_rows LOOP
            -- Content and width both vary per row. A constant payload would
            -- give every row the same bytes and length, which no real table
            -- has. The width swings between roughly 300 and 1100 bytes, so
            -- rows_per_mb above is an average rather than an exact figure.
            EXECUTE format($f$
                INSERT INTO %I (id, k, tag, payload)
                SELECT g, g %% 1000, 'x' || (g %% 17),
                       repeat(md5(g::text || ':%s'), 9 + (g %% 25))
                FROM generate_series(%s, %s) g
                ON CONFLICT (id) DO NOTHING
            $f$, format('x_%s', lpad(i::text, 3, '0')), i,
                first_id, least(first_id + batch_rows - 1, n_rows));
            COMMIT;
            first_id := first_id + batch_rows;
        END LOOP;
        RAISE NOTICE 'x_% -> % MB', lpad(i::text, 3, '0'), sizes[i];
    END LOOP;
END $$;

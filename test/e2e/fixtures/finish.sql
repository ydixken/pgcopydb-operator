-- Fixture seed stage: everything that has to follow the concurrent load stages
-- (base fixture). Runs alone; see run.sh.
\ir prelude.sql

-- The non-unique secondary indexes live here rather than in schema.sql so the
-- load pays one bulk build each instead of a row-at-a-time maintenance cost
-- across every insert, which is also what pg_restore does during a real
-- migration. Primary keys and app_users.email stay in schema.sql: the loads
-- use ON CONFLICT, and that needs its unique index to already exist.
\echo indexes
CREATE INDEX IF NOT EXISTS orders_customer_idx ON orders (customer_id);
CREATE INDEX IF NOT EXISTS app_users_tags_gin ON app_users USING gin (tags);
CREATE INDEX IF NOT EXISTS app_users_name_lower_idx ON app_users (lower(display_name));
CREATE INDEX IF NOT EXISTS events_customer_time_idx ON events (customer_id, occurred_at);
CREATE INDEX IF NOT EXISTS events_payload_gin ON events USING gin (payload);
CREATE INDEX IF NOT EXISTS readings_recent_partial_idx ON readings (captured_at)
    WHERE quantity > 5;
CREATE INDEX IF NOT EXISTS documents_title_lower_idx ON documents (lower(title));

-- The identity/serial sequences did not advance under the explicit-id
-- inserts; put them past the seeded rows so live writes get fresh ids.
SELECT setval(pg_get_serial_sequence('public.customers', 'id'), (SELECT max(id) FROM customers));
SELECT setval(pg_get_serial_sequence('public.orders', 'id'), (SELECT max(id) FROM orders));
SELECT setval(pg_get_serial_sequence('audit.events', 'id'), (SELECT max(id) FROM audit.events));

\echo refreshing event_daily_counts
REFRESH MATERIALIZED VIEW event_daily_counts;

-- Success marker, written last. Old rows go away so a marker always states
-- exactly what the cluster contains.
BEGIN;
DELETE FROM e2e_seed;
INSERT INTO e2e_seed (profile, scale) VALUES (:'profile', :'scale'::numeric);
COMMIT;

\echo done

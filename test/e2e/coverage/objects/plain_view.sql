-- coverage: group=clone  expect=identical  schemas=1  min_pg=14
-- @setup
CREATE TABLE ${schema}.items (id int PRIMARY KEY, name text, price numeric(8,2));
INSERT INTO ${schema}.items SELECT g, 'item ' || g, g * 1.5 FROM generate_series(1, 20) g;
CREATE VIEW ${schema}.cheap AS SELECT id, upper(name) AS name FROM ${schema}.items WHERE price < 10;
-- A view on a view, so restore order matters.
CREATE VIEW ${schema}.cheap_count AS SELECT count(*) AS n FROM ${schema}.cheap;

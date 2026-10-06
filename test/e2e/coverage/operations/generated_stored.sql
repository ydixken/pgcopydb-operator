-- coverage: group=follow  expect=identical  schemas=1  min_pg=14
-- @setup
CREATE TABLE ${schema}.t (
  id int PRIMARY KEY,
  price numeric(10,2) NOT NULL,
  qty int NOT NULL,
  total numeric(12,2) GENERATED ALWAYS AS (price * qty) STORED
);
INSERT INTO ${schema}.t (id, price, qty) SELECT g, g * 1.5, g FROM generate_series(1, 20) g;
-- @follow
INSERT INTO ${schema}.t (id, price, qty) VALUES (21, 9.99, 3);
UPDATE ${schema}.t SET qty = qty + 1 WHERE id % 2 = 0;
UPDATE ${schema}.t SET price = 0 WHERE id = 5;

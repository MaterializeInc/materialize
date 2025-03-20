---
title: "Example data: items and orders"
description: "Sample data used in various examples."
disable_list: false
menu:
  main:
    parent: idiomatic-materialize-appendix
    identifier: idiomatic-materialize-appendix-example-orders
---

The following sample data is used in:

- [First value in group
  examples](/transform-data/idiomatic-materialize-sql/first-value/#examples)

- [Last value in group
  examples](/transform-data/idiomatic-materialize-sql/last-value/#examples)

- [Top-K in group
  examples](/transform-data/idiomatic-materialize-sql/top-k/#examples)

```mzsql
CREATE TABLE orders (
    order_id int NOT NULL,
    order_date timestamp NOT NULL,
    item text NOT NULL,
    quantity int NOT NULL
);

INSERT INTO orders VALUES
(1,current_timestamp,'brownie',10),
(1,current_timestamp,'chocolate cake',1),
(1,current_timestamp,'chocolate chip cookie',20),
(1,current_timestamp,'coffee cake',1),
(1,current_timestamp,'fruit tart',4),
(2,current_timestamp + (15 * interval '1 minute'),'chocolate chip cookie',20),
(2,current_timestamp + (15 * interval '1 minute'),'brownie',20),
(3,current_timestamp + (30 * interval '1 minute'),'sugar cookie',10),
(3,current_timestamp + (30 * interval '1 minute'),'donut',36),
(3,current_timestamp + (30 * interval '1 minute'),'chiffon cake',2),
(3,current_timestamp + (30 * interval '1 minute'),'egg tart',6),
(4,current_timestamp + (1 * interval '1 day') + (35 * interval '1 minute'),'cheesecake',1),
(5,current_timestamp + (1 * interval '1 day')+ (35 * interval '1 minute'),'chocolate chip cookie',20),
(5,current_timestamp + (1 * interval '1 day')+ (35 * interval '1 minute'),'brownie',20),
(5,current_timestamp + (1 * interval '1 day')+ (35 * interval '1 minute'),'cheesecake',1),
(5,current_timestamp + (1 * interval '1 day')+ (35 * interval '1 minute'),'cupcake',6),
(5,current_timestamp + (1 * interval '1 day')+ (35 * interval '1 minute'),'chocolate cake',1),
(6,current_timestamp + (1 * interval '2 day'),'chocolate cake',1),
(7,current_timestamp + (1 * interval '2 day')+ (10 * interval '1 minute'),'coffee cake',1),
(7,current_timestamp + (1 * interval '2 day')+ (10 * interval '1 minute'),'egg tart',12),
(8,current_timestamp + (1 * interval '2 day')+ (15 * interval '1 minute'),'chocolate chip cookie',12),
(8,current_timestamp + (1 * interval '2 day')+ (15 * interval '1 minute'),'brownie',12),
(8,current_timestamp + (1 * interval '2 day')+ (15 * interval '1 minute'),'sugar cookie',12),
(8,current_timestamp + (1 * interval '2 day')+ (15 * interval '1 minute'),'donut',12),
(9,current_timestamp + (1 * interval '2 day')+ (30 * interval '1 minute'),'cupcake',6),
(10,current_timestamp + (1 * interval '3 day'),'chiffon cake',1),
(10,current_timestamp + (1 * interval '3 day'),'egg tart',6),
(10,current_timestamp + (1 * interval '3 day'),'fruit tart',6),
(11,current_timestamp + (1 * interval '3 day') + (15 * interval '1 minute'),'cheesecake',1),
(12,current_timestamp + (1 * interval '3 day') + (35 * interval '1 minute'),'brownies',12),
(12,current_timestamp + (1 * interval '3 day') + (35 * interval '1 minute'),'cupcake',12),
(12,current_timestamp + (1 * interval '3 day') + (35 * interval '1 minute'),'chocolate cake',1)
;

CREATE TABLE items(
    item text NOT NULL,
    price numeric(8,4) NOT NULL,
    currency text NOT NULL DEFAULT 'USD'
);

INSERT INTO items VALUES
('brownie',2.25,'USD'),
('cheesecake',40,'USD'),
('chiffon cake',30,'USD'),
('chocolate cake',30,'USD'),
('chocolate chip cookie',2.5,'USD'),
('coffee cake',25,'USD'),
('cupcake',3,'USD'),
('donut',1.25,'USD'),
('egg tart',2.5,'USD'),
('fruit tart',4.5,'USD'),
('sugar cookie',2.5,'USD');

CREATE VIEW orders_view AS
SELECT o.*,i.price,o.quantity * i.price as subtotal
FROM orders as o
JOIN items as i
ON o.item = i.item;

CREATE VIEW orders_daily_totals AS
SELECT date_trunc('day',order_date) AS order_date,
       sum(subtotal) AS daily_total
FROM orders_view
GROUP BY date_trunc('day',order_date);


CREATE TABLE sales_items (
  week_of date NOT NULL,
  items text[]
);

INSERT INTO sales_items VALUES
(date_trunc('week', current_timestamp),ARRAY['brownie','chocolate chip cookie','chocolate cake']),
(date_trunc('week', current_timestamp + (1* interval '7 day')),ARRAY['chocolate chip cookie','donut','cupcake']);
```

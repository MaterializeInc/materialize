---
title: "Example data: items and orders"
description: "Sample data used in various examples."
disable_list: false
menu:
  main:
    parent: idiomatic-materialize-appendix
    identifier: idiomatic-materialize-appendix-example-orders
---

The following sample data is used in the various Idiomaitc Materialize SQL
pages:

```mzsql
CREATE TABLE orders (
    order_id int NOT NULL,
    order_date timestamp NOT NULL,
    item text NOT NULL,
    quantity int NOT NULL,
    status text NOT NULL
);

INSERT INTO orders VALUES
(1,current_timestamp - (1 * interval '3 day') - (35 * interval '1 minute'),'brownies',12, 'Complete'),
(1,current_timestamp - (1 * interval '3 day') - (35 * interval '1 minute'),'cupcake',12, 'Complete'),
(1,current_timestamp - (1 * interval '3 day') - (35 * interval '1 minute'),'chocolate cake',1, 'Complete'),
(2,current_timestamp - (1 * interval '3 day') - (15 * interval '1 minute'),'cheesecake',1, 'Complete'),
(3,current_timestamp - (1 * interval '3 day'),'chiffon cake',1, 'Complete'),
(3,current_timestamp - (1 * interval '3 day'),'egg tart',6, 'Complete'),
(3,current_timestamp - (1 * interval '3 day'),'fruit tart',6, 'Complete'),
(4,current_timestamp - (1 * interval '2 day')- (30 * interval '1 minute'),'cupcake',6, 'Shipped'),
(4,current_timestamp - (1 * interval '2 day')- (30 * interval '1 minute'),'cupcake',6, 'Shipped'),
(5,current_timestamp - (1 * interval '2 day'),'chocolate cake',1, 'Processing'),
(6,current_timestamp,'brownie',10, 'Pending'),
(6,current_timestamp,'chocolate cake',1, 'Pending'),
(7,current_timestamp,'chocolate chip cookie',20, 'Processing'),
(8,current_timestamp,'coffee cake',1, 'Complete'),
(8,current_timestamp,'fruit tart',4, 'Complete'),
(9,current_timestamp + (15 * interval '1 minute'),'chocolate chip cookie',20, 'Pending'),
(9,current_timestamp + (15 * interval '1 minute'),'brownie',20, 'Processing'),
(10,current_timestamp + (30 * interval '1 minute'),'sugar cookie',10, 'Pending'),
(10,current_timestamp + (30 * interval '1 minute'),'donut',36, 'Pending'),
(11,current_timestamp + (30 * interval '1 minute'),'chiffon cake',2, 'Pending'),
(11,current_timestamp + (30 * interval '1 minute'),'egg tart',6, 'Pending'),
(12,current_timestamp + (1 * interval '1 day') + (35 * interval '1 minute'),'cheesecake',1, 'Pending'),
(13,current_timestamp + (1 * interval '1 day')+ (35 * interval '1 minute'),'chocolate chip cookie',20, 'Pending'),
(14,current_timestamp + (1 * interval '1 day')+ (35 * interval '1 minute'),'brownie',20, 'Pending'),
(14,current_timestamp + (1 * interval '1 day')+ (35 * interval '1 minute'),'cheesecake',1, 'Pending'),
(14,current_timestamp + (1 * interval '1 day')+ (35 * interval '1 minute'),'cupcake',6, 'Pending'),
(15,current_timestamp + (1 * interval '1 day')+ (35 * interval '1 minute'),'chocolate cake',1, 'Pending'),
(16,current_timestamp + (1 * interval '2 day'),'chocolate cake',1, 'Pending'),
(17,current_timestamp + (1 * interval '2 day')+ (10 * interval '1 minute'),'coffee cake',1, 'Pending'),
(17,current_timestamp + (1 * interval '2 day')+ (10 * interval '1 minute'),'egg tart',12, 'Pending'),
(18,current_timestamp + (1 * interval '2 day')+ (15 * interval '1 minute'),'chocolate chip cookie',12, 'Pending'),
(18,current_timestamp + (1 * interval '2 day')+ (15 * interval '1 minute'),'brownie',12, 'Pending'),
(18,current_timestamp + (1 * interval '2 day')+ (15 * interval '1 minute'),'sugar cookie',12, 'Pending'),
(18,current_timestamp + (1 * interval '2 day')+ (15 * interval '1 minute'),'donut',12, 'Pending'),
(19,current_timestamp + (1 * interval '2 day')+ (30 * interval '1 minute'),'cupcake',6, 'Pending'),
(20,current_timestamp + (1 * interval '3 day'),'chiffon cake',1, 'Pending'),
(20,current_timestamp + (1 * interval '3 day'),'egg tart',6, 'Pending'),
(20,current_timestamp + (1 * interval '3 day'),'fruit tart',6, 'Pending'),
(21,current_timestamp + (1 * interval '3 day') + (15 * interval '1 minute'),'cheesecake',1, 'Pending'),
(22,current_timestamp + (1 * interval '3 day') + (35 * interval '1 minute'),'brownies',12, 'Pending'),
(22,current_timestamp + (1 * interval '3 day') + (35 * interval '1 minute'),'cupcake',12, 'Pending'),
(22,current_timestamp + (1 * interval '3 day') + (35 * interval '1 minute'),'chocolate cake',1, 'Pending')
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

---
title: "Improve Lookup Speed"
description: "Speed up reads from materialized views using additional indexes"
weight:
menu:
  main:
    parent: 'sql-patterns'
disable_toc: true
---

You can set up an index over a column were filtering by literal values or expressions are common to improve the performance.

```sql
CREATE MATERIALIZED VIEW active_customers AS
    SELECT guid, geo_id, last_active_on
    FROM customer_source
    GROUP BY geo_id;

CREATE INDEX active_customers_idx ON active_customers (guid);

SELECT * FROM active_customers WHERE guid = 'd868a5bf-2430-461d-a665-40418b1125e7';

-- Using expressions
CREATE INDEX active_customers_exp_idx ON active_customers (upper(guid));

SELECT * FROM active_customers WHERE upper(guid) = 'D868A5BF-2430-461D-A665-40418B1125E7';

-- Filter using an expression in one field and a literal in another field
CREATE INDEX active_customers_exp_field_idx ON active_customers (upper(guid), geo_id);

SELECT * FROM active_customers 
WHERE upper(guid) = 'D868A5BF-2430-461D-A665-40418B1125E7' and geo_id = 'ID_8482';
```

Create an index with an expression to improve query performance over a frequent used expression and
avoid building downstream views to apply the function like the one used in the example: `upper()`.
Take into account that aggregations like `count()` are not possible to use as expressions.

## Related pages

-   [`CREATE INDEX`](../../sql/create-index)

---
title: "Manual Materialization"
description: "Use Materialize more efficiently by creating non-materialized views, then indexes."
menu:
  main:
    parent: 'sql-patterns'
---

You can convert a non-materialized view into a materialized view by adding an
index to it.

```sql
CREATE VIEW active_customers AS
    SELECT guid, geo_id, last_active_on
    FROM customer_source
    WHERE last_active_on > now() - INTERVAL '30' DAYS;

CREATE INDEX active_customers_primary_idx ON active_customers (guid);
```

Note that this index is different than the primary index that Materialize would
automatically create if you had used `CREATE MATERIALIZED VIEW`. Indexes that
are automatically created contain an index of all columns in the result set,
unless they contain a unique key. Manually materializing a view can lead to more
efficient reads if the index matches your lookup pattern.

## Related pages

-   [`CREATE INDEX`](../../sql/create-index)

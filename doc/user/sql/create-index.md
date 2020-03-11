---
title: "CREATE INDEX"
description: "`CREATE INDEX` creates an in-memory index on a view."
menu:
  main:
    parent: 'sql'
---

{{< warning >}} This is an advanced feature of Materialized; most users will not
need to manually create indexes to maximize the value Materialize offers, as
running `CREATE MATERIALIZED VIEW` automatically creates all required indexes to
eagerly materialize that view. {{< /warning >}}

`CREATE INDEX` creates an in-memory index on a view.

- For materialized views, this creates additional indexes.
- For non-materialized views, it converts them into materialized views.

## Conceptual framework

Indexes persist some subset of a query's data to memory, which let materialized
views quickly perform reads from materialized views, which includes performing
[`JOIN`](../join)s. For more information, see [API Components:
Indexes](../../overview/api-components#indexes).

### When to create indexes

You might want to create indexes when...

- Using non-primary keys (e.g. foreign keys) as a join condition. In this case,
  users could create an index on the columns in the join condition.
- You want to convert a non-materialized view to a materialized view.

## Syntax

{{< diagram "create-index.html" >}}

Field | Use
------|-----
_index&lowbar;name_ | A name for the index.
_view&lowbar;name_ | The name of the view for which you want to create an index.
_col&lowbar;ref_**...** | The columns to use as the key into the index.

## Details

### Restrictions

- You can only index some subset of columns from the view's embedded `SELECT`
  statement's returned columns. For example, if your view embedded `SELECT a, b,
  c...`, you can only index `{a, b, c}`, even if the source you're reading from
  contains additional columns.

- You cannot exclude any columns from being in the index's "value" set. For
  example, if your view embedded `SELECT a, b, c...`, all indexes will contain
  `{a, b, c}` as their values.

    If you want to create an index that only stores a subset of these columns,
    consider creating another materialized view that uses `SELECT some_subset
    FROM this_view...`.

### Structure

Indexes in Materialize have the following structure for each unique row.

```nofmt
((tuple of indexed columns), (tuple of the row, i.e. stored columns))
```

#### Indexed columns vs. stored columns

Automatically created columns index all of a table's columns, unless Materialize
is provided or can infer a unique key for the result set.

For instance, unique keys can be...

- **Provided** by the schema provided for the source, e.g. through the Confluent
  Schema Registry.
- **Inferred** when the query's results...
  - Contains a `GROUP BY`.
  - Uses results derived from a unique key without adding any additional
    columns.

When creating your own indexes, you can choose the indexed columns.

### Memory footprint

We do not currently have a good "rule of thumb" for understanding the size of
indexes and are working on [a feature to let you see the size each index
consumes](https://github.com/MaterializeInc/materialize/issues/1532).

## Examples

### Optimizing joins with indexes

We can optimize the performance of `JOIN` on two relations by ensuring their
join keys are the leading columns in an index.

```sql
CREATE MATERIALIZED VIEW active_customers AS
    SELECT
        guid, geo_id, last_active_on
    FROM
        customer_source
    WHERE
        last_active_on > now() - INTERVAL '30' DAYS;

CREATE INDEX active_customers_geo_idx
    ON active_customers (geo_id);

CREATE MATERIALIZED VIEW active_customer_per_geo AS
    SELECT
        geo.name, count(*)
    FROM
        geo_regions AS geo
        JOIN active_customers ON
                active_customers.geo_id = geo.id
    GROUP BY
        geo.name;
```

In the above example, the index `active_customers_geo_idx`...

- Helps us because it contains a key that the view `active_customer_per_geo` can
  use to look up values for the join condition (`active_customers.geo`).

    Because this access pattern is more optimal than scanning the entire
    `active_customers` results set, the Materialize optimizer will choose to use
    `active_customers_geo_idx`.

- Obeys our restrictions by containing only a subset of columns in the result
  set.

### Materializing views

You can convert a non-materialized view into a materialized view by adding an
index to it.

```sql
CREATE VIEW active_customers AS
    SELECT
        guid, geo_id, last_active_on
    FROM
        customer_source
    WHERE
        last_active_on > now() - INTERVAL '30' DAYS;

CREATE INDEX active_customers_primary_idx
    ON active_customers (guid);
```

Note that this index is different than the primary index that Materialize would
automatically create if you had used `CREATE MATERIALIZED VIEW`. Indexes that
are automatically created contain an index of all columns in the result set,
unless they contain a unique key. (Remember that indexes store a copy of a
row's indexed columns _and_ a copy of the entire row.)

## Related pages

- [`SHOW INDEX`](../show-index)
- [`DROP INDEX`](../drop-index)

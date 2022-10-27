---
title: "CREATE INDEX"
description: "`CREATE INDEX` creates an in-memory index on a source, view, or materialized view."
menu:
  # This should also have a "non-content entry" under Reference, which is
  # configured in doc/user/config.toml
  main:
    parent: 'commands'
---

`CREATE INDEX` creates an in-memory [index](/overview/key-concepts/#indexes) on a source, view, or materialized
view.

Indexes assemble and maintain a query's results in memory within a [cluster](/overview/key-concepts#clusters),
which provides future queries the data
they need in a format they can immediately use. In particular, creating indexes
can be very helpful for the [`JOIN`](../join) operator, which needs to build
and maintain the appropriate indexes if they do not otherwise exist.

### Usage patterns

You might want to create indexes when...

-   You want to use non-primary keys (e.g. foreign keys) as a join condition. In
    this case, you could create an index on the columns in the join condition.
-   You want to speed up searches filtering by literal values or expressions.

[//]: # "TODO(morsapaes) Point to relevant operational guide on indexes once
this exists."

## Syntax

{{< diagram "create-index.svg" >}}

[//]: # "TODO(morsapaes) This diagram is out of control."

Field | Use
------|-----
**DEFAULT** | Creates a default index that stores all columns in a source, view, or materialized view in memory.
_index&lowbar;name_ | A name for the index.
_obj&lowbar;name_ | The name of the source, view, or materialized view on which you want to create an index.
_cluster_name_ | The [cluster](/sql/create-cluster) to maintain this index. If not specified, defaults to the active cluster.
_method_ | The name of the index method to use. The only supported method is [`arrangement`](/overview/arrangements).
_col&lowbar;expr_**...** | The expressions to use as the key for the index.
_field_ | The name of the option you want to set.
_val_ | The value for the option.

## Details

### Restrictions

-   You can only reference the columns available in the `SELECT` list of the query
    that defines the view. For example, if your view was defined as `SELECT a, b FROM src`, you can only reference columns `a` and `b`, even if `src` contains
    additional columns.

-   You cannot exclude any columns from being in the index's "value" set. For
    example, if your view is defined as `SELECT a, b FROM ...`, all indexes will
    contain `{a, b}` as their values.

    If you want to create an index that only stores a subset of these columns,
    consider creating another materialized view that uses `SELECT some_subset FROM this_view...`.

### Structure

Indexes in Materialize have the following structure for each unique row:

```nofmt
((tuple of indexed expressions), (tuple of the row, i.e. stored columns))
```

#### Indexed expressions vs. stored columns

Automatically created indexes will use all columns as key expressions for the
index, unless Materialize is provided or can infer a unique key for the source
or view.

For instance, unique keys can be...

-   **Provided** by the schema provided for the source, e.g. through the Confluent
    Schema Registry.
-   **Inferred** when the query...
    -   Concludes with a `GROUP BY`.
    -   Uses sources or views that have a unique key without damaging this property.
        For example, joining a view with unique keys against a second, where the join
        constraint uses foreign keys.

When creating your own indexes, you can choose the indexed expressions.

### Memory footprint

The in-memory sizes of indexes are proportional to the current size of the source
or view they represent. The actual amount of memory required depends on several
details related to the rate of compaction and the representation of the types of
data in the source or view. We are working on a feature to let you see the size
each index consumes {{% gh 1532 %}}.

Creating an index may also force the first materialization of a view, which may
cause Materialize to install a dataflow to determine and maintain the results of
the view. This dataflow may have a memory footprint itself, in addition to that
of the index.

## Examples

### Optimizing joins with indexes

You can optimize the performance of `JOIN` on two relations by ensuring their
join keys are the key columns in an index.

```sql
CREATE MATERIALIZED VIEW active_customers AS
    SELECT guid, geo_id, last_active_on
    FROM customer_source
    WHERE last_active_on > now() - INTERVAL '30' DAYS;

CREATE INDEX active_customers_geo_idx ON active_customers (geo_id);

CREATE MATERIALIZED VIEW active_customer_per_geo AS
    SELECT geo.name, count(*)
    FROM geo_regions AS geo
    JOIN active_customers ON active_customers.geo_id = geo.id
    GROUP BY geo.name;
```

In the above example, the index `active_customers_geo_idx`...

-   Helps us because it contains a key that the view `active_customer_per_geo` can
    use to look up values for the join condition (`active_customers.geo_id`).

    Because this index is exactly what the query requires, the Materialize
    optimizer will choose to use `active_customers_geo_idx` rather than build
    and maintain a private copy of the index just for this query.

-   Obeys our restrictions by containing only a subset of columns in the result
    set.

### Speed up filtering with indexes

If you commonly filter by a certain column being equal to a literal value, you can set up an index over that column to speed up your queries:

```sql
CREATE MATERIALIZED VIEW active_customers AS
    SELECT guid, geo_id, last_active_on
    FROM customer_source
    GROUP BY geo_id;

CREATE INDEX active_customers_idx ON active_customers (guid);

-- This should now be very fast!
SELECT * FROM active_customers WHERE guid = 'd868a5bf-2430-461d-a665-40418b1125e7';

-- Using indexed expressions:
CREATE INDEX active_customers_exp_idx ON active_customers (upper(guid));
SELECT * FROM active_customers WHERE upper(guid) = 'D868A5BF-2430-461D-A665-40418B1125E7';

-- Filter using an expression in one field and a literal in another field:
CREATE INDEX active_customers_exp_field_idx ON active_customers (upper(guid), geo_id);
SELECT * FROM active_customers WHERE upper(guid) = 'D868A5BF-2430-461D-A665-40418B1125E7' and geo_id = 'ID_8482';
```

Create an index with an expression to improve query performance over a frequently used expression, and
avoid building downstream views to apply the function like the one used in the example: `upper()`.
Take into account that aggregations like `count()` cannot be used as indexed expressions.

For more details on using indexes to optimize queries, see [Optimization](../../ops/optimization/).

## Related pages

-   [`SHOW INDEXES`](../show-indexes)
-   [`DROP INDEX`](../drop-index)

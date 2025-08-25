---
title: "CREATE INDEX"
description: "`CREATE INDEX` creates an in-memory index on a source, view, or materialized view."
menu:
  # This should also have a "non-content entry" under Reference, which is
  # configured in doc/user/config.toml
  main:
    parent: 'commands'
---

`CREATE INDEX` creates an in-memory [index](/concepts/indexes/) on a source, view, or materialized view.

In Materialize, indexes store query results in memory within a specific [cluster](/concepts/clusters/), and keep these results **incrementally updated** as new data arrives. This ensures that indexed data remains [fresh](/concepts/reaction-time), reflecting the latest changes with minimal latency.

The primary use case for indexes is to accelerate direct queries issued via [`SELECT`](/sql/select/) statements.
By maintaining fresh, up-to-date results in memory, indexes can significantly [optimize query performance](/transform-data/optimization/), reducing both response time and compute load—especially for resource-intensive operations such as joins, aggregations, and repeated subqueries.

Because indexes are scoped to a single cluster, they are most useful for accelerating queries within that cluster. For results that must be shared across clusters or persisted to durable storage, consider using a [materialized view](/sql/create-materialized-view), which also maintains fresh results but is accessible system-wide.

### Usage patterns

#### Indexes on views vs. materialized views

{{% views-indexes/table-usage-pattern-intro %}}
{{% views-indexes/table-usage-pattern %}}

#### Indexes and query optimizations

You might want to create indexes when...

-   You want to use non-primary keys (e.g. foreign keys) as a join condition. In
    this case, you could create an index on the columns in the join condition.
-   You want to speed up searches filtering by literal values or expressions.

{{% views-indexes/index-query-optimization-specific-instances %}}

#### Best practices

{{% views-indexes/index-best-practices %}}

## Syntax

{{< diagram "create-index.svg" >}}

### `with_options`

{{< diagram "with-options-retain-history.svg" >}}

Field | Use
------|-----
**DEFAULT** | Creates a default index using a set of columns that uniquely identify each row. If this set of columns can't be inferred, all columns are used.
_index&lowbar;name_ | A name for the index.
_obj&lowbar;name_ | The name of the source, view, or materialized view on which you want to create an index.
_cluster_name_ | The [cluster](/sql/create-cluster) to maintain this index. If not specified, defaults to the active cluster.
_method_ | The name of the index method to use. The only supported method is [`arrangement`](/overview/arrangements).
_col&lowbar;expr_**...** | The expressions to use as the key for the index.
_retention_period_ | ***Private preview.** This option has known performance or stability issues and is under active development.* <br>Duration for which Materialize retains historical data, which is useful to implement [durable subscriptions](/transform-data/patterns/durable-subscriptions/#history-retention-period). **Note:** Configuring indexes to retain history is not recommended. As an alternative, consider creating a materialized view for your subscription query and configuring the history retention period on the view instead. See [durable subscriptions](/transform-data/patterns/durable-subscriptions/#history-retention-period). <br>Accepts positive [interval](/sql/types/interval/) values (e.g. `'1hr'`). <br>Default: `1s`.

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
data in the source or view.

Creating an index may also force the first materialization of a view, which may
cause Materialize to install a dataflow to determine and maintain the results of
the view. This dataflow may have a memory footprint itself, in addition to that
of the index.

## Examples

### Optimizing joins with indexes

You can optimize the performance of `JOIN` on two relations by ensuring their
join keys are the key columns in an index.

```mzsql
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

```mzsql
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

## Privileges

The privileges required to execute this statement are:

{{< include-md file="shared-content/sql-command-privileges/create-index.md" >}}

## Related pages

-   [`SHOW INDEXES`](../show-indexes)
-   [`DROP INDEX`](../drop-index)

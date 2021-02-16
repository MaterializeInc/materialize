---
title: "CREATE INDEX"
description: "`CREATE INDEX` creates an in-memory index on a source or view."
menu:
  main:
    parent: 'sql'
---

{{< warning >}} This is an advanced feature of Materialized; most users will not
need to manually create indexes to maximize the value Materialize offers, as
running `CREATE MATERIALIZED SOURCE` or `CREATE MATERIALIZED VIEW` automatically
creates an index which will eagerly materialize that source or view. {{< /warning >}}

`CREATE INDEX` creates an in-memory index on a source or view.

- For materialized views, this creates additional indexes.
- For non-materialized views, it converts them into materialized views.

## Conceptual framework

Indexes assemble and maintain in memory a query's results, which can
provide future queries the data they need pre-arranged in a format they can immediately use.
In particular, this can be very helpful for the [`JOIN`](../join) operator which needs
to build and maintain the appropriate indexes if they do not otherwise exist.
For more information, see [API Components: Indexes](/overview/api-components#indexes).

### When to create indexes

You might want to create indexes when...

- You want to use non-primary keys (e.g. foreign keys) as a join condition.
  In this case, you could create an index on the columns in the join condition.
- You want to convert a non-materialized view or source to a materialized view or source.

## Syntax

{{< diagram "create-index.svg" >}}

Field | Use
------|-----
**DEFAULT** | Creates a default index with the same structure as the index automatically created with [**CREATE MATERIALIZED VIEW**](/sql/create-materialized-view) or [**CREATE MATERIALIZED SOURCE**](/sql/create-source). This provides a simple method to convert a non-materialized object to a materialized one.
_index&lowbar;name_ | A name for the index.
_obj&lowbar;name_ | The name of the source or view on which you want to create an index.
_col&lowbar;ref_**...** | The columns to use as the key into the index.
_field_ | The name of an index parameter to set to _val_. See [`ALTER INDEX`](/sql/alter-index) for available parameters.

{{< version-changed v0.7.1 >}}
The `WITH (field = val, ...)` clause was added to allow setting index parameters
when creating the index.
{{</ version-changed >}}

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

Automatically created indexes will use all columns as key columns for the index,
unless Materialize is provided or can infer a unique key for the source or view.

For instance, unique keys can be...

- **Provided** by the schema provided for the source, e.g. through the Confluent
  Schema Registry.
- **Inferred** when the query...
  - Concludes with a `GROUP BY`.
  - Uses sources or views that have a unique key without damaging this property.
    For example, joining a view with unique keys against a second, where the join
    constraint uses foreign keys.

When creating your own indexes, you can choose the indexed columns.

### Memory footprint

The in-memory sizes of indexes are proportional to the current size of the source
or view they represent. The actual amount of memory required depends on several
details related to the rate of compaction and the representation of the types of
data in the source or view. We are working on [a feature to let you see the size
each index consumes](https://github.com/MaterializeInc/materialize/issues/1532).

Creating an index may also force the first materialization of a view, which may
cause Materialize to install a dataflow to determine and maintain the results of
the view. This dataflow may have a memory footprint itself, in addition to that
of the index.

## Examples

### Optimizing joins with indexes

We can optimize the performance of `JOIN` on two relations by ensuring their
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

- Helps us because it contains a key that the view `active_customer_per_geo` can
  use to look up values for the join condition (`active_customers.geo_id`).

    Because this index is exactly what the query requires, the Materialize
    optimizer will choose to use `active_customers_geo_idx` rather than build
    and maintain a private copy of the index just for this query.

- Obeys our restrictions by containing only a subset of columns in the result
  set.

### Materializing views

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
unless they contain a unique key. (Remember that indexes store a copy of a
row's indexed columns _and_ a copy of the entire row.)

## Related pages

- [`SHOW INDEX`](../show-index)
- [`DROP INDEX`](../drop-index)

---
title: "DROP INDEX"
description: "DROP INDEX removes an index"
menu:
  main:
    parent: 'commands'
---

`DROP INDEX` removes an index from Materialize.

## Syntax

```mzsql
DROP INDEX [IF EXISTS] <index_name> [CASCADE|RESTRICT];
```

Syntax element | Description
---------------|------------
**IF EXISTS** | Optional. If specified, do not return an error if the specified index does not exist.
`<index_name>` | Index to drop.
**CASCADE** | Optional. Equivalent to `RESTRICT`. `DROP INDEX` never drops the objects that depend on the index. See [Dependent objects](#dependent-objects).
**RESTRICT** | Optional. Do not remove the index if other objects depend on it. _(Default.)_

## Dependent objects

When Materialize creates a materialized view or another index, its optimizer
may plan the new object to read from an existing index rather than from the
underlying relation. The object then depends on that index, even though the
index does not appear in the object's definition.

`DROP INDEX` fails if any materialized view or index reads from the index, and
the error names the dependent objects. This holds with `CASCADE` too: because
the dependency is invisible in the dependents' definitions, `DROP INDEX` never
drops them. To drop the index, first drop the dependent
objects yourself, then drop the index and recreate the dependents. In
production, use a [blue/green deployment](/developer-tools/dbt/blue-green-deployments/)
to replace them without downtime.

A newer index on a relation can read from an older index on the same relation,
so drop indexes on the same relation newest first.

In-progress `SELECT` and `SUBSCRIBE` statements that read from the index do not
block the drop. The index continues to be maintained until they finish, and the
`DROP INDEX` statement returns a notice saying so.

You can inspect which objects read from an index with
[`mz_internal.mz_compute_dependencies`](/sql/system-catalog/mz_internal/#mz_compute_dependencies).

## Privileges

To execute the `DROP INDEX` statement, you need:

{{% include-headless "/headless/sql-command-privileges/drop-index" %}}

## Examples

### Remove an index

{{< tip >}}

In the **Materialize Console**, you can view existing indexes in the [**Database
object explorer**](/developer-tools/console/data/). Alternatively, you can use the
[`SHOW INDEXES`](/sql/show-indexes) command.

{{< /tip >}}

Using the  `DROP INDEX` commands, the following example drops an index named `q01_geo_idx`.

```mzsql
DROP INDEX q01_geo_idx;
```

If the index `q01_geo_idx` does not exist, the above operation returns an error.

### Remove an index that other objects read from

If a materialized view was planned to read from the index, `DROP INDEX` returns
an error:

```mzsql
DROP INDEX q01_geo_idx;
```
```nofmt
ERROR:  cannot drop index "q01_geo_idx": still depended upon by materialized view "q01_geo_summary"
DETAIL:  The dependent objects are live dataflows that read from index "q01_geo_idx", so the index cannot be dropped while they exist.
HINT:  Drop the dependent objects first, then drop the index and recreate the dependents. To replace them in production without downtime, use a blue/green deployment.
```

Drop the materialized view first, then the index, and recreate the view:

```mzsql
DROP MATERIALIZED VIEW q01_geo_summary;
DROP INDEX q01_geo_idx;
CREATE MATERIALIZED VIEW q01_geo_summary AS ...;
```

### Remove an index without erroring if the index does not exist

You can specify the `IF EXISTS` option so that the `DROP INDEX` command does
not return an error if the index to drop does not exist.

```mzsql
DROP INDEX IF EXISTS q01_geo_idx;
```

## Related pages

- [`CREATE INDEX`](/sql/create-index)
- [`SHOW VIEWS`](/sql/show-views)
- [`SHOW INDEXES`](/sql/show-indexes)
- [`DROP OWNED`](/sql/drop-owned)

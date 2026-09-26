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
**CASCADE** | Optional. If specified, remove the index and its dependent objects.
**RESTRICT** | Optional. Do not remove the index if other objects depend on it. _(Default.)_

## Dependent objects

When Materialize creates a materialized view or another index, its optimizer
may plan the new object to read from an existing index rather than from the
underlying relation. The object then depends on that index, even though the
index does not appear in the object's definition.

`DROP INDEX` without `CASCADE` fails if any materialized view or index reads
from the index. The error names the dependent objects. To drop the index anyway,
either drop and recreate the dependent objects first or use `DROP INDEX ...
CASCADE`, which drops the index together with every object that reads from it
and their own dependents.

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

### Remove an index and the objects that read from it

If a materialized view was planned to read from the index, `DROP INDEX` without
`CASCADE` returns an error:

```mzsql
DROP INDEX q01_geo_idx;
```
```nofmt
ERROR:  cannot drop index "q01_geo_idx": still depended upon by materialized view "q01_geo_summary"
DETAIL:  The dependent objects are live dataflows that read from index "q01_geo_idx", so the index cannot be dropped while they exist.
HINT:  Add CASCADE to the statement to drop the index together with its dependent objects, or drop the dependent objects yourself and recreate them once the index is gone.
```

With `CASCADE`, the index and the materialized view are both dropped:

```mzsql
DROP INDEX q01_geo_idx CASCADE;
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

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
DROP INDEX [IF EXISTS] <index_name> [ 'CASCADE' | 'RESTRICT' ];
```

Option | Description
------|-----
**IF EXISTS** | Do not return an error if the specified index does not exist.
**CASCADE** | Remove the index and its dependent objects.
**RESTRICT** |  Remove the index. _(Default.)_

{{< note >}}

Since indexes do not have dependent objects, `DROP INDEX`, `DROP INDEX
RESTRICT`, and `DROP INDEX CASCADE` are equivalent.

{{< /note >}}

## Privileges

To execute the `DROP INDEX` statement, you need:

{{< include-md file="shared-content/sql-command-privileges/drop-index.md" >}}

## Examples

### Remove an index

{{< tip >}}

In the **Materialize Console**, you can view existing indexes in the [**Database
object explorer**](/console/data/). Alternatively, you can use the
[`SHOW INDEXES`](/sql/show-indexes) command.

{{< /tip >}}

Using the  `DROP INDEX` commands, the following example drops an index named `q01_geo_idx`.

```mzsql
DROP INDEX q01_geo_idx;
```

If the index `q01_geo_idx` does not exist, the above operation returns an error.

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

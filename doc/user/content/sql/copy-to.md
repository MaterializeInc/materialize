---
title: "COPY TO"
description: "`COPY TO` outputs a query via the COPY protocol."
menu:
    main:
        parent: "commands"
---

`COPY TO` sends rows using the [Postgres COPY protocol](https://www.postgresql.org/docs/current/sql-copy.html).

## Syntax

{{< diagram "copy-to.svg" >}}

Field | Use
------|-----
_query_ | The [`SELECT`](/sql/select) or [`SUBSCRIBE`](/sql/subscribe) query to send
_field_ | The name of the option you want to set.
_val_ | The value for the option.

### `WITH` options

Name | Value type | Default value | Description
----------------------------|--------|--------|--------
`FORMAT` | `TEXT`,`BINARY` | `TEXT` | Sets the output formatting method.

## Example

### Copying a view

```sql
COPY (SELECT * FROM some_view) TO STDOUT;
```

### Subscribing to a view with binary output

```sql
COPY (SUBSCRIBE some_view) TO STDOUT WITH (FORMAT binary);
```

## Privileges

{{< alpha />}}

The privileges required to execute this statement are:

- `USAGE` privileges on the schemas that all relations in the query are contained in.
- `SELECT` privileges on all relations in the query.
    - NOTE: if any item is a view, then the view owner must also have the necessary privileges to
      execute the view definition.
- `USAGE` privileges on all types used in the query.
- `USAGE` privileges on the active cluster.

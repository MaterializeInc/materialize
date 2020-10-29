---
title: "COPY TO"
description: "`COPY TO` outputs a query via the COPY protocol."
menu:
    main:
        parent: "sql"
---

`COPY TO` sends rows using the [Postgres COPY protocol](https://www.postgresql.org/docs/current/sql-copy.html).

## Syntax

{{< diagram "copy-to.svg" >}}

Field | Use
------|-----
_query_ | The [`SELECT`](/sql/select) or [`TAIL`](/sql/tail) query to send
_option_ |

Name | Value
-----|-------
`FORMAT` | `TEXT` for text output (the default)

## Example

### Copying a view

```sql
COPY (SELECT * FROM some_view) TO STDOUT
```

### Tailing a view

```sql
COPY (TAIL some_view) TO STDOUT
```

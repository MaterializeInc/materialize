---
title: "COPY FROM"
description: "`COPY FROM` copies data into a table using the COPY protocol."
menu:
    main:
        parent: "sql"
---

`COPY FROM` copies data into a table using the [Postgres COPY protocol](https://www.postgresql.org/docs/current/sql-copy.html).

## Syntax

{{< diagram "copy-from.svg" >}}

Field | Use
------|-----
_name_| The name of the table to be copied.

Rows are expected in `text` format, one per line, with columns separated by a tab character. `\N` (backslash-N) represents a null collumn value.

## Example


```sql
COPY t FROM STDIN
```

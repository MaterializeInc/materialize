---
title: "COPY FROM"
description: "`COPY FROM` copies data into a table using the COPY protocol."
menu:
    main:
        parent: "sql"
---

`COPY FROM` copies data into a table using the [Postgres `COPY` protocol](https://www.postgresql.org/docs/current/sql-copy.html).

## Syntax

{{< diagram "copy-from.svg" >}}

Field       | Use
------------|-----
_table_name_| The name of the table to be copied.
_column_    | An optional list of columns to be copied. If no column list is specified, all columns of the table will be copied.

Supported `option` values:

Name | Default value | Description
-----|---------------|------------
`DELIMITER` | tab character | Specifies the character that separates columns within each row (line) of the file.
`NULL` | `\N` (backslash-N) | Specifies the string that represents a null value.

Rows are expected in `text` format, one per line, with columns separated by the
`DELIMITER` character.

## Example

```sql
COPY t FROM STDIN WITH (DELIMITER '|')
```

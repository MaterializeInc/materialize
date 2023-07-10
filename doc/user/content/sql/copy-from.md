---
title: "COPY FROM"
description: "`COPY FROM` copies data into a table using the COPY protocol."
menu:
    main:
        parent: "commands"
---

`COPY FROM` copies data into a table using the [Postgres `COPY` protocol][pg-copy-from].

## Syntax

{{< diagram "copy-from.svg" >}}

Field | Use
------|-----
_table_name_ | Copy values to this table.
**(**_column_...**)** | Correlate the inserted rows' columns to _table_name_'s columns by ordinal position, i.e. the first column of the row to insert is correlated to the first named column. <br/><br/>Without a column list, all columns must have data provided, and will be referenced using their order in the table. With a partial column list, all unreferenced columns will receive their default value.
_field_ | The name of the option you want to set.
_val_ | The value for the option.

### `WITH` options

The following options are valid within the `WITH` clause.

Name | Value type | Default value | Description
-----|-----------------|---------------|------------
`FORMAT` | `TEXT`, `CSV` | `TEXT` | Sets the input formatting method. For more information see [Text formatting](#text-formatting), [CSV formatting](#csv-formatting).
`DELIMITER` | Single-quoted one-byte character | Format-dependent | Overrides the format's default column delimiter.
`NULL` | Single-quoted strings | Format-dependent | Specifies the string that represents a _NULL_ value.
`QUOTE` | Single-quoted one-byte character | `"` | Specifies the character to signal a quoted string, which may contain the `DELIMITER` value (without beginning new columns). To include the `QUOTE` character itself in column, wrap the column's value in the `QUOTE` character and prefix all instance of the value you want to literally interpret with the `ESCAPE` value. _`FORMAT CSV` only_
`ESCAPE` | Single-quoted strings | `QUOTE`'s value | Specifies the character to allow instances of the `QUOTE` character to be parsed literally as part of a column's value. _`FORMAT CSV` only_
`HEADER`  | `boolean`   | `boolean`  | Specifies that the file contains a header line with the names of each column in the file. The first line is ignored on input.  _`FORMAT CSV` only._

Note that `DELIMITER` and `QUOTE` must use distinct values.

## Details

### Text formatting

As described in the **Text Format** section of [PostgreSQL's documentation][pg-copy-from].

### CSV formatting

As described in the **CSV Format** section of [PostgreSQL's documentation][pg-copy-from]
except that:

- More than one layer of escaped quote characters returns the wrong result.
  {{% gh 9074 %}}

- Quote characters must immediately follow a delimiter to be treated as
  expected. {{% gh 9075 %}}

- Single-column rows containing quoted end-of-data markers (e.g. `"\."`) will be
  treated as end-of-data markers despite being quoted. In PostgreSQL, this data
  would be escaped and would not terminate the data processing.

- Quoted null strings will be parsed as nulls, despite being quoted. In
  PostgreSQL, this data would be escaped.

  To ensure proper null handling, we recommend specifying a unique string for
  null values, and ensuring it is never quoted.

- Unterminated quotes are allowed, i.e. they do not generate errors. In
  PostgreSQL, all open unescaped quotation punctuation must have a matching
  piece of unescaped quotation punctuation or it generates an error.

## Example

```sql
COPY t FROM STDIN WITH (DELIMITER '|');
```

```sql
COPY t FROM STDIN (FORMAT CSV);
```

```sql
COPY t FROM STDIN (DELIMITER '|');
```

## Privileges

{{< alpha />}}

The privileges required to execute this statement are:

- `USAGE` privileges on the schema containing the table.
- `INSERT` privileges on the table.

[pg-copy-from]: https://www.postgresql.org/docs/14/sql-copy.html

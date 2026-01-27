# COPY FROM
`COPY FROM` copies data into a table using the COPY protocol.
`COPY FROM` copies data into a table using the [Postgres `COPY` protocol][pg-copy-from].

## Syntax



```mzsql
COPY [INTO] <table_name> [ ( <column> [, ...] ) ] FROM STDIN
[[WITH] ( <option1> [=] <val1> [, ...] ] )]
;

```

| Syntax element | Description |
| --- | --- |
| `<table_name>` | Name of an existing table to copy data into.  |
| `( <column> [, ...] )` | If specified, correlate the inserted rows' columns to `<table_name>`'s columns by ordinal position, i.e. the first column of the row to insert is correlated to the first named column. If not specified, all columns must have data provided, and will be referenced using their order in the table. With a partial column list, all unreferenced columns will receive their default value.  |
| `[WITH] ( <option1> [=] <val1> [, ...] )` | The following `<options>` are supported for the `COPY FROM` operation: \| Name \|  Description \| \|------\|---------------\| \| `FORMAT` \|  Sets the input formatting method. Valid input formats are `TEXT` and `CSV`. For more information see [Text formatting](#text-formatting) and [CSV formatting](#csv-formatting).<br><br> Default: `TEXT`. \| `DELIMITER` \| A single-quoted one-byte character to use as the column delimiter. Must be different from `QUOTE`.<br><br> Default: A tab character in `TEXT`  format, a comma in `CSV` format. \| `NULL`  \| A single-quoted string that represents a _NULL_ value.<br><br> Default: `\N` (backslash-N) in text format, an unquoted empty string in CSV format. \| `QUOTE` \| _For `FORMAT CSV` only._ A single-quoted one-byte character that specifies the character to signal a quoted string, which may contain the `DELIMITER` value (without beginning new columns). To include the `QUOTE` character itself in column, wrap the column's value in the `QUOTE` character and prefix all instance of the value you want to literally interpret with the `ESCAPE` value. Must be different from `DELIMITER`.<br><br> Default: `"`. \| `ESCAPE` \| _For `FORMAT CSV` only._ A single-quoted string that specifies the character to allow instances of the `QUOTE` character to be parsed literally as part of a column's value. <br><br> Default: `QUOTE`'s value. \| `HEADER`  \| _For `FORMAT CSV` only._ A boolean that specifies that the file contains a header line with the names of each column in the file. The first line is ignored on input. <br><br> Default: `false`.  |


## Details

### Text formatting

As described in the **Text Format** section of [PostgreSQL's documentation][pg-copy-from].

### CSV formatting

As described in the **CSV Format** section of [PostgreSQL's documentation][pg-copy-from]
except that:

- More than one layer of escaped quote characters returns the wrong result.

- Quote characters must immediately follow a delimiter to be treated as
  expected.

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

```mzsql
COPY t FROM STDIN WITH (DELIMITER '|');
```

```mzsql
COPY t FROM STDIN (FORMAT CSV);
```

```mzsql
COPY t FROM STDIN (DELIMITER '|');
```

## Privileges

The privileges required to execute this statement are:

- `USAGE` privileges on the schema containing the table.
- `INSERT` privileges on the table.

[pg-copy-from]: https://www.postgresql.org/docs/14/sql-copy.html

## Limits

You can only copy up to 1 GiB of data at a time. If you need this limit increased, please [chat with our team](http://materialize.com/convert-account/).

---
title: "COPY FROM"
description: "`COPY FROM` copies data into a table using the COPY protocol."
menu:
    main:
        parent: "commands"
---

`COPY FROM` copies data into a table using the [Postgres `COPY` protocol][pg-copy-from].

## Syntax

{{% include-syntax file="examples/copy_from" example="syntax" %}}

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

{{< include-md file="shared-content/sql-command-privileges/copy-from.md" >}}

[pg-copy-from]: https://www.postgresql.org/docs/14/sql-copy.html

## Limits

You can only copy up to 1 GiB of data at a time. If you need this limit increased, please [chat with our team](http://materialize.com/convert-account/).

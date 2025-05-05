---
title: "CREATE TABLE"
description: "`CREATE TABLE` creates a table that is persisted in durable storage."
pagerank: 40
menu:
  # This should also have a "non-content entry" under Reference, which is
  # configured in doc/user/config.toml
  main:
    parent: 'commands'
---

`CREATE TABLE` defines a table that is persisted in durable storage. In
Materialize, you can create:

- User-populated tables. User-populated tables can be written to (i.e.,
  [`INSERT`]/[`UPDATE`]/[`DELETE`]) by the user.

- [Source-populated](/concepts/sources/) tables. Source-populated tables cannot
  be written to by the user; they are populated through data ingestion from a
  source.

Tables can be joined with other tables, materialized views, and views. Tables in
Materialize are similar to tables in standard relational databases: they consist
of rows and columns where the columns are fixed when the table is created.

## Syntax

{{< tabs >}}

{{< tab "User-populated tables" >}}

To create a table that users can write to (i.e., perform
[`INSERT`](/sql/insert/)/[`UPDATE`](/sql/update/)/[`DELETE`](/sql/delete/)
operations):

```mzsql
CREATE [TEMP|TEMPORARY] TABLE <table_name> (
  <column_name> <column_type> [NOT NULL][DEFAULT <default_expr>]
  [, ...]
)
[WITH (
  RETAIN HISTORY [=] FOR <duration>
)]
;
```

{{% yaml-table data="syntax_options/create_table_options_user_populated" %}}

{{</ tab >}}

{{< tab "Source-populated tables (DB source)" >}}

To create a table from a [source](/sql/create-source/), where the source maps to
an external database system:

{{< note >}}

Users cannot write to source-populated tables; i.e., users cannot perform
[`INSERT`](/sql/insert/)/[`UPDATE`](/sql/update/)/[`DELETE`](/sql/delete/)
operations on source-populated tables.

{{</ note >}}

```mzsql
CREATE TABLE <table_name> FROM SOURCE <source_name> (REFERENCE <ref_object>)
[WITH (
    TEXT COLUMNS (<fq_column_name> [, ...])
  | EXCLUDE COLUMNS (<fq_column_name> [, ...])
  [, ...]
)]
;
```

{{% yaml-table data="syntax_options/create_table_options_source_populated_db" %}}

{{</ tab >}}


{{</ tabs >}}

## Details

### Table names and column names

Names for tables and column(s) must follow the [naming
guidelines](/sql/identifiers/#naming-restrictions).

### Known limitations

Tables do not currently support:

- Primary keys
- Unique constraints
- Check constraints

See also the known limitations for [`INSERT`](../insert#known-limitations),
[`UPDATE`](../update#known-limitations), and [`DELETE`](../delete#known-limitations).

### Temporary tables

The `TEMP`/`TEMPORARY` keyword creates a temporary table. Temporary tables are
automatically dropped at the end of the SQL session and are not visible to other
connections. They are always created in the special `mz_temp` schema.

Temporary tables may depend upon other temporary database objects, but non-temporary
tables may not depend on temporary objects.

## Examples

### Creating a table

You can create a table `t` with the following statement:

```mzsql
CREATE TABLE t (a int, b text NOT NULL);
```

Once a table is created, you can inspect the table with various `SHOW` commands.

```mzsql
SHOW TABLES;
TABLES
------
t

SHOW COLUMNS IN t;
name       nullable  type
-------------------------
a          true      int4
b          false     text
```

## Privileges

The privileges required to execute this statement are:

- `CREATE` privileges on the containing schema.
- `USAGE` privileges on all types used in the table definition.
- `USAGE` privileges on the schemas that all types in the statement are contained in.

## Related pages

- [`INSERT`](../insert)
- [`CREATE SOURCE`](/sql/create-source/)
- [`DROP TABLE`](../drop-table)

[`INSERT`]: /sql/insert/
[`UPDATE`]: /sql/update/
[`DELETE`]: /sql/delete/

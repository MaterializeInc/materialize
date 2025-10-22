---
title: "CREATE TABLE: Read-write table"
description: "Reference page for `CREATE TABLE`. `CREATE TABLE` creates a table that is persisted in durable storage."
menu:
  # This should also have a "non-content entry" under Reference, which is
  # configured in doc/user/config.toml
  main:
    parent: 'create-table'
    name: "User-populated table"
---

`CREATE TABLE` defines a table that is persisted in durable storage.

In Materialize, you can create read-write tables. Read-write tables are
user-populated; i.e., users can read and write to the tables ([`INSERT`],
[`SELECT`], [`UPDATE`], [`DELETE`]).

Tables in Materialize are similar to tables in standard relational databases:
they consist of rows and columns where the columns are fixed when the table is
created.

Tables can be joined with other tables, materialized views, and views; and you
can create views/materialized views/indexes on tables.

## Syntax

{{% include-example file="examples/create_table/example_user_populated_table"
 example="syntax" %}}

{{% include-example file="examples/create_table/example_user_populated_table"
 example="syntax-options" %}}

## Details

### Table names and column names

Names for tables and column(s) must follow the [naming
guidelines](/sql/identifiers/#naming-restrictions).

<a name="supported-db-source-types"></a>

### Known limitations

Tables do not currently support:

- Primary keys
- Unique constraints
- Check constraints

See also the known limitations for [`INSERT`](/sql/insert#known-limitations),
[`UPDATE`](/sql/update#known-limitations), and [`DELETE`](/sql/delete#known-limitations).

### Required privileges

The privileges required to execute this statement are:

{{< include-md file="shared-content/sql-command-privileges/create-table.md" >}}

## Examples

### Create a table (User-populated)

{{% include-example file="examples/create_table/example_user_populated_table"
 example="create-table" %}}

Once a user-populated table is created, you can perform CRUD
(Create/Read/Update/Write) operations on it.

{{% include-example file="examples/create_table/example_user_populated_table"
 example="write-to-table" %}}

{{% include-example file="examples/create_table/example_user_populated_table"
 example="read-from-table" %}}

## Related pages

- [`INSERT`]
- [`CREATE SOURCE`](/sql/create-source/)
- [`DROP TABLE`](/sql/drop-table)
- [Ingest data](/ingest-data/)

[`INSERT`]: /sql/insert/
[`SELECT`]: /sql/select/
[`UPDATE`]: /sql/update/
[`DELETE`]: /sql/delete/

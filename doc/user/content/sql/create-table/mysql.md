---
title: "CREATE TABLE: MySQL"
description: "Reference page for `CREATE TABLE`. `CREATE TABLE` creates a table that is persisted in durable storage."
menu:
  # This should also have a "non-content entry" under Reference, which is
  # configured in doc/user/config.toml
  main:
    parent: 'create-table'
    name: "MySQL"
---

{{< source-versioning-disambiguation is_new=true other_ref="[old reference page](/sql/create-source-v1/mysql/)" include_blurb=true >}}

`CREATE TABLE` defines a table that is persisted in durable storage.

In Materialize, you can create [source-populated](/concepts/sources/) tables.
Source-populated tables are read-only tables; they cannot be written to by the
user. These tables are populated by [data ingestion from a source](/ingest-data/).

Tables in Materialize are similar to tables in standard relational databases:
they consist of rows and columns where the columns are fixed when the table is
created.

Tables can be joined with other tables, materialized views, and views; and you
can create views/materialized views/indexes on tables.

## Syntax

{{% include-example file="examples/create_table/example_mysql_table"
 example="syntax" %}}

{{% include-example file="examples/create_table/example_mysql_table"
 example="syntax-options" %}}

## Details

### Table names and column names

Names for tables and column(s) must follow the [naming
guidelines](/sql/identifiers/#naming-restrictions).

<a name="supported-db-source-types"></a>

### Supported data types

{{< include-md file="shared-content/mysql-supported-types.md" >}}

{{< include-md file="shared-content/mysql-unsupported-type-handling.md" >}}

### Source-populated tables and snapshotting

{{< include-md file="shared-content/create-table-from-source-snapshotting.md"
>}}

### Required privileges

The privileges required to execute this statement are:

{{< include-md file="shared-content/sql-command-privileges/create-table.md" >}}

## Examples

### Create a table (MySQL Source)

{{< note >}}

The example assumes you have configured your upstream MySQL 5.7+ (i.e.,
enabled GTID-based binlog replication, created the
replication user, and updated the network configuration as needed).

For details about configuring your upstream system, see the [MySQL
integration guides](/ingest-data/mysql/#supported-versions-and-services).

{{</ note >}}

{{% include-example file="examples/create_table/example_mysql_table"
 example="create-table" %}}

{{< include-md file="shared-content/create-table-from-source-readonly.md" >}}

{{< include-md file="shared-content/create-table-from-source-snapshotting.md"
>}}

Once the snapshotting process completes, you can query the table:

{{% include-example file="examples/create_table/example_mysql_table"
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

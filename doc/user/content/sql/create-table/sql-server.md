---
title: "CREATE TABLE: SQL Server source table"
description: "Create a read-only table from a SQL Server source (new syntax)."
menu:
  main:
    parent: 'create-table'
    name: "SQL Server source table"
    identifier: 'create-table-sql-server'
    weight: 40
---

In Materialize, you can create read-only tables from [SQL Server sources created
using the new syntax](/sql/create-source/sql-server-v2/).

{{< note >}}
You must be on **v26.14.1+** to use the syntax.
{{< /note >}}

## Syntax

{{< note >}}
{{% include-headless "/headless/create-table-from-source-readonly" %}}
{{< /note >}}

{{% include-syntax file="examples/create_table_sql_server"
example="syntax" %}}

## Details

### DDL transaction block

For performance, when issuing multiple `CREATE TABLE FROM SOURCE...` statements,
use within a [transaction block](/sql/begin/#ddl-only-transactions).

### Source-populated tables and snapshotting

{{% include-headless "/headless/create-table-from-source-snapshotting"
%}}

### Supported data types

{{% include-headless "/headless/sql-server-supported-types" %}}

{{% include-headless "/headless/sql-server-unsupported-type-handling" %}}

### Handling table schema changes

The use of `CREATE SOURCE` (new syntax) with `CREATE TABLE FROM SOURCE` allows
for the handling of the upstream DDL changes, specifically adding or dropping
columns in the upstream tables, without downtime. For details, see [SQL Server:
Handling upstream schema changes with zero
downtime](/ingest-data/sql-server/source-versioning/).

## Privileges

The privileges required to execute this statement are:

{{% include-headless "/headless/sql-command-privileges/create-table" %}}

## Examples

### Create a table

{{% include-example file="examples/create_table_sql_server"
 example="create-table" %}}

{{% include-example file="examples/create_table_sql_server"
 example="show-tables" %}}

{{% include-example file="examples/create_table_sql_server"
 example="show-columns" %}}

{{% include-example file="examples/create_table_sql_server"
 example="read-from-table" %}}

## Related pages

- [`CREATE SOURCE: SQL Server (New Syntax)`](/sql/create-source/sql-server-v2/)
- [`DROP TABLE`](/sql/drop-table)

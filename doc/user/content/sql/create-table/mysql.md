---
title: "CREATE TABLE: MySQL source table"
description: "Create a read-only table from a MySQL source (new syntax)."
menu:
  main:
    parent: 'create-table'
    name: "MySQL source table"
    identifier: 'create-table-mysql'
    weight: 30
---

In Materialize, you can create read-only tables from [MySQL sources created
using the new syntax](/sql/create-source/mysql-v2/).

{{< note >}}

{{% include-example file="examples/create_table_mysql"
example="syntax-version-requirement" %}}
{{< /note >}}

## Syntax

{{< note >}}
{{% include-headless "/headless/create-table-from-source-readonly" %}}
{{< /note >}}

{{% include-syntax file="examples/create_table_mysql"
example="syntax" %}}

## Details

### DDL transaction block

For performance, when issuing multiple `CREATE TABLE FROM SOURCE...` statements,
use within a [transaction block](/sql/begin/#ddl-only-transactions).

### Source-populated tables and snapshotting

{{% include-headless "/headless/create-table-from-source-snapshotting" %}}

### Supported data types

{{% include-from-yaml data="mysql_source_details" name="mysql-supported-types" %}}

{{% include-from-yaml data="mysql_source_details" name="mysql-unsupported-types" %}}

### Handling table schema changes

The use of `CREATE SOURCE` (new syntax) with `CREATE TABLE FROM SOURCE` allows
for the handling of the upstream DDL changes, specifically adding or dropping
columns in the upstream tables, without downtime. For details, see [MySQL:
Handling upstream schema changes with zero
downtime](/ingest-data/mysql/source-versioning/).

## Privileges

The privileges required to execute this statement are:

{{% include-headless "/headless/sql-command-privileges/create-table" %}}

## Examples

### Create a table

{{% include-example file="examples/create_table_mysql"
 example="create-table" %}}

## Related pages

- [`CREATE SOURCE: MySQL (New Syntax)`](/sql/create-source/mysql-v2/)
- [`DROP TABLE`](/sql/drop-table)

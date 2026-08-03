---
title: "CREATE TABLE: PostgreSQL source table"
description: "Create a read-only table from a PostgreSQL source (new syntax)."
menu:
  main:
    parent: 'create-table'
    name: "PostgreSQL source table"
    identifier: 'create-table-postgres'
    weight: 20
---

In Materialize, you can create read-only tables from [PostgreSQL sources created
using the new syntax](/sql/create-source/postgres-v2/).

{{< note >}}
{{% include-example file="examples/create_table_postgres"
example="syntax-version-requirement" %}}
{{< /note >}}

## Syntax

{{< note >}}
{{% include-headless "/headless/create-table-from-source-readonly" %}}
{{< /note >}}

{{% include-syntax file="examples/create_table_postgres"
example="syntax" %}}

## Details

### DDL transaction block

For performance, when issuing multiple `CREATE TABLE FROM SOURCE...` statements,
use within a [transaction block](/sql/begin/#ddl-only-transactions).

### Source-populated tables and snapshotting

{{% include-headless "/headless/create-table-from-source-snapshotting" %}}

### Supported data types

{{% include-from-yaml data="postgres_source_details" name="postgres-supported-types" %}}

{{% include-from-yaml data="postgres_source_details" name="postgres-unsupported-types" %}}

### Handling table schema changes

The use of `CREATE SOURCE` (new syntax) with `CREATE TABLE FROM SOURCE` allows
for the handling of the upstream DDL changes, specifically adding or dropping
columns in the upstream tables, without downtime. For details, see [PostgreSQL:
Handling upstream schema changes with zero
downtime](/ingest-data/postgres/source-versioning/).

See also [Handling upstream operations](#handling-upstream-operations) for
additional upstream operation considerations.

### Inherited tables

{{% include-from-yaml data="postgres_source_details"
name="postgres-inherited-tables" %}}

{{% include-from-yaml data="postgres_source_details"
name="postgres-inherited-tables-action" %}}

## Handling upstream operations

{{% upstream-schema-change-behavior connector="postgres" %}}

## Privileges

The privileges required to execute this statement are:

{{% include-headless "/headless/sql-command-privileges/create-table" %}}

## Examples

### Create a table

{{< note >}}

{{% include-example file="examples/create_table_postgres"
example="syntax-version-requirement" %}}

The example assumes you have configured your upstream PostgreSQL 11+ (i.e.,
enabled logical replication, created the publication for the various tables and
replication user, and updated the network configuration).

For details about configuring your upstream system, see the [PostgreSQL
integration guides](/ingest-data/postgres/#supported-versions-and-services).

{{</ note >}}

{{% include-example file="examples/create_table_postgres"
 example="create-table" %}}

{{% include-example file="examples/create_table_postgres"
 example="read-from-table" %}}

## Related pages

- [`CREATE SOURCE: PostgreSQL (New Syntax)`](/sql/create-source/postgres-v2/)
- [`DROP TABLE`](/sql/drop-table)

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

`CREATE TABLE` defines a table that is persisted in durable storage.

In Materialize, you can create:
- Read-write tables. With read-write tables, users can read ([`SELECT`]) and
  write to the tables ([`INSERT`], [`UPDATE`], [`DELETE`]).

-  *Private Preview*. Read-only tables from [PostgreSQL sources (new
  syntax)](/sql/create-source/postgres-v2/). Users cannot be write ([`INSERT`],
  [`UPDATE`], [`DELETE`]) to these tables. These tables are populated by [data
  ingestion from a source](/ingest-data/postgres/).

Tables in Materialize are similar to tables in standard relational databases:
they consist of rows and columns where the columns are fixed when the table is
created.

Tables can be joined with other tables, materialized views, views, and
subsources; and you can create views/materialized views/indexes on tables.


[//]: # "TODO(morsapaes) Bring back When to use a table? once there's more
clarity around best practices."

## Syntax

{{< tabs >}}
{{< tab "Read-write table" >}}
### Read-write table

{{% include-example file="examples/create_table/example_user_populated_table" example="syntax" %}}

{{% include-example file="examples/create_table/example_user_populated_table" example="syntax-options" %}}

{{< /tab >}}
{{< tab "PostgreSQL source table" >}}
### PostgreSQL source table

{{< private-preview />}}
{{% include-example file="examples/create_table/example_postgres_table" example="syntax" %}}

{{% include-example file="examples/create_table/example_postgres_table" example="syntax-options" %}}
{{< /tab >}}

{{< /tabs >}}


## Read-write tables

### Table names and column names

Names for tables and column(s) must follow the [naming
guidelines](/sql/identifiers/#naming-restrictions).

### Known limitations

Tables do not currently support:

- Primary keys
- Unique constraints
- Check constraints

See also the known limitations for [`INSERT`](/sql/insert#known-limitations),
[`UPDATE`](/sql/update#known-limitations), and [`DELETE`](/sql/delete#known-limitations).

## PostgreSQL source tables

{{< private-preview />}}

### Table names and column names

Names for tables and column(s) must follow the [naming
guidelines](/sql/identifiers/#naming-restrictions).

<a name="supported-db-source-types"></a>

### Read-only tables

{{< include-md file="shared-content/create-table-from-source-readonly.md" >}}

### Source-populated tables and snapshotting

{{< include-md file="shared-content/create-table-from-source-snapshotting.md"
>}}

### Supported data types

{{% include-from-yaml data="postgres_source_details" name="postgres-supported-types" %}}

{{% include-from-yaml data="postgres_source_details" name="postgres-unsupported-types" %}}

### Handling table schema changes

The use of [`CREATE SOURCE`](/sql/create-source/postgres-v2/) with `CREATE TABLE
FROM SOURCE` allows for the handling of the upstream DDL changes, specifically
adding or dropping columns, without downtime.

#### Incompatible schema changes

All other schema changes to upstream tables (such as changing types) will set
the corresponding subsource into an error state, which prevents you from reading
from the source.

To handle incompatible schema changes, use [`DROP
SOURCE`](/sql/alter-source/#context) and [`ALTER SOURCE...ADD
SUBSOURCE`](/sql/alter-source/) to first drop the affected subsource, and then
add the table back to the source. When you add the subsource, it will have the
updated schema from the corresponding upstream table.

### Upstream table truncation restrictions

{{% include-from-yaml data="postgres_source_details"
name="postgres-truncation-restriction" %}}

### Inherited tables

{{% include-from-yaml data="postgres_source_details"
name="postgres-inherited-tables" %}}

## Privileges

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

### Create a table (PostgreSQL source)

{{< private-preview />}}

{{< note >}}

The example assumes you have configured your upstream PostgreSQL 11+ (i.e.,
enabled logical replication, created the publication for the various tables and
replication user, and updated the network configuration).

For details about configuring your upstream system, see the [PostgreSQL
integration guides](/ingest-data/postgres/#supported-versions-and-services).

{{</ note >}}

{{% include-example file="examples/create_table/example_postgres_table"
 example="create-table" %}}

{{< include-md file="shared-content/create-table-from-source-readonly.md" >}}

{{< include-md file="shared-content/create-table-from-source-snapshotting.md"
>}}

{{% include-example file="examples/create_table/example_postgres_table"
 example="read-from-table" %}}


## Related pages

- [`INSERT`]
- [`DROP TABLE`](/sql/drop-table)

[`INSERT`]: /sql/insert/
[`SELECT`]: /sql/select/
[`UPDATE`]: /sql/update/
[`DELETE`]: /sql/delete/

---
title: "CREATE SOURCE: SQL Server"
description: "Connecting Materialize to a SQL Server database for Change Data Capture (CDC)."
pagerank: 40
menu:
  main:
    parent: 'create-source'
    identifier: cs_sql-server-v2
    name: SQL Server (New Syntax)
    weight: 24
---

{{< private-preview />}}

{{< source-versioning-disambiguation is_new=true
other_ref="[old reference page](/sql/create-source/sql-server/)" include_blurb=true >}}

## Prerequisites

{{% create-source/intro %}}
Materialize supports SQL Server (2016+) as a real-time data source. To connect to a
SQL Server database, you first need to tweak its configuration to enable [Change Data
Capture](https://learn.microsoft.com/en-us/sql/relational-databases/track-changes/enable-and-disable-change-data-capture-sql-server)
and [`SNAPSHOT` transaction isolation](https://learn.microsoft.com/en-us/dotnet/framework/data/adonet/sql/snapshot-isolation-in-sql-server)
for the database that you would like to replicate. Then [create a connection](#prerequisite-creating-a-connection-to-sql-server)
in Materialize that specifies access and authentication parameters.
{{% /create-source/intro %}}

## Syntax

{{% include-syntax file="examples/create_source_sql_server" example="syntax" %}}

## Ingesting data

After a source is created, you can create tables from the source
upstream SQL Server database that have [Change Data Capture enabled](https://learn.microsoft.com/en-us/sql/relational-databases/track-changes/about-change-data-capture-sql-server).
You can create multiple tables that reference the same table in the source.

See [`CREATE TABLE FROM SOURCE`](/sql/create-table/) for details.

#### Handling table schema changes

The use of the `CREATE SOURCE` with the new [`CREATE TABLE FROM
SOURCE`](/sql/create-table/) allows for the handling of certain upstream DDL
changes without downtime.

See [Guide: Handle upstream schema changes with zero downtime](/ingest-data/sql-server/source-versioning/) for details.

#### Supported types

With the new syntax, after a SQL Server source is created, you [`CREATE TABLE
FROM SOURCE`](/sql/create-table/) to create a corresponding table in
Matererialize and start ingesting data.

{{< include-md file="shared-content/sql-server-supported-types.md" >}}

For more information, including strategies for handling unsupported types,
see [`CREATE TABLE FROM SOURCE`](/sql/create-table/).

### Monitoring source progress

[//]: # "TODO(morsapaes) Replace this section with guidance using the new
progress metrics in mz_source_statistics + console monitoring, when available
(also for PostgreSQL)."

By default, SQL Server sources expose progress metadata as a subsource that you
can use to monitor source **ingestion progress**. The name of the progress
subsource can be specified when creating a source using the `EXPOSE PROGRESS
AS` clause; otherwise, it will be named `<src_name>_progress`.

The following metadata is available for each source as a progress subsource:

Field     | Type                          | Details
----------|-------------------------------|--------------
`lsn`     | [`bytea`](/sql/types/bytea/)  | The upper-bound [Log Sequence Number](https://learn.microsoft.com/en-us/sql/relational-databases/sql-server-transaction-log-architecture-and-management-guide) replicated thus far into Materialize.


And can be queried using:

```mzsql
SELECT lsn
FROM <src_name>_progress;
```

The reported `lsn` should increase as Materialize consumes **new** CDC events
from the upstream SQL Server database. For more details on monitoring source
ingestion progress and debugging related issues, see [Troubleshooting](/ops/troubleshooting/).

## Example

{{< important >}}
Before creating a SQL Server source, you must enable Change Data Capture and
`SNAPSHOT` transaction isolation in the upstream database.
{{</ important >}}

### Creating a source {#create-source-example}

#### Prerequisite: Creating a connection to SQL Server

First, you must create a connection to your SQL Server database. A connection describes how to connect and authenticate to an external system you
want Materialize to read data from.

Once created, a connection is **reusable** across multiple `CREATE SOURCE`
statements. For more details on creating connections, check the
[`CREATE CONNECTION`](/sql/create-connection/#sql-server) documentation page.

```mzsql
CREATE SECRET sqlserver_pass AS '<SQL_SERVER_PASSWORD>';

CREATE CONNECTION sqlserver_connection TO SQL SERVER (
    HOST 'instance.foo000.us-west-1.rds.amazonaws.com',
    PORT 1433,
    USER 'materialize',
    PASSWORD SECRET sqlserver_pass,
    DATABASE '<DATABASE_NAME>'
);
```

If your SQL Server instance is not exposed to the public internet, you can
[tunnel the connection](/sql/create-connection/#network-security-connections)
through and SSH bastion host.

{{< tabs tabID="1" >}}
{{< tab "SSH tunnel">}}
```mzsql
CREATE CONNECTION ssh_connection TO SSH TUNNEL (
    HOST 'bastion-host',
    PORT 22,
    USER 'materialize',
    DATABASE '<DATABASE_NAME>'
);
```

```mzsql
CREATE CONNECTION sqlserver_connection TO SQL SERVER (
    HOST 'instance.foo000.us-west-1.rds.amazonaws.com',
    SSH TUNNEL ssh_connection,
    DATABASE '<DATABASE_NAME>'
);
```

For step-by-step instructions on creating SSH tunnel connections and configuring
an SSH bastion server to accept connections from Materialize, check
[this guide](/ops/network-security/ssh-tunnel/).

{{< /tab >}}
{{< /tabs >}}

#### Creating the source in Materialize

You **must** enable Change Data Capture, see [Enable Change Data Capture SQL Server Instructions](/ingest-data/sql-server/self-hosted/#a-configure-sql-server).

Once CDC is enabled for all of the tables you wish to create subsources for, you can create a `SOURCE` in
Materialize to begin replicating data!

_Create source from the connection we just created_

```mzsql
CREATE SOURCE mz_source
    FROM SQL SERVER CONNECTION sqlserver_connection;
```

After a source is created, you can create a table from the source, referencing specific table(s).

_Creates a table in Materialize from the upstream table dbo.items_
```mzsql
CREATE TABLE items FROM SOURCE mz_source(REFERENCE dbo.items);
```

## Related pages

- [`CREATE SECRET`](/sql/create-secret)
- [`CREATE CONNECTION`](/sql/create-connection)
- [`CREATE SOURCE`](../)

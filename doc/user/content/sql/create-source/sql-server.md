---
title: "CREATE SOURCE: SQL Server"
description: "Connecting Materialize to a SQL Server database for Change Data Capture (CDC)."
pagerank: 40
menu:
  main:
    parent: 'create-source'
    identifier: cs_sql-server
    name: SQL Server
    weight: 20
---

{{< private-preview />}}

{{% create-source/intro %}}
Materialize supports SQL Server (2016+) as a real-time data source. To connect to a
SQL Server database, you first need to tweak its configuration to enable [Change Data
Capture](https://learn.microsoft.com/en-us/sql/relational-databases/track-changes/enable-and-disable-change-data-capture-sql-server)
and [`SNAPSHOT` transaction isolation](https://learn.microsoft.com/en-us/dotnet/framework/data/adonet/sql/snapshot-isolation-in-sql-server)
for the database that you would like to replicate. Then [create a connection](#creating-a-connection)
in Materialize that specifies access and authentication parameters.
{{% /create-source/intro %}}

## Syntax

{{< diagram "create-source-sql-server.svg" >}}

### `with_options`

{{< diagram "with-options-retain-history.svg" >}}

Field | Use
------|-----
_src_name_  | The name for the source.
**IF NOT EXISTS**  | Do nothing (except issuing a notice) if a source with the same name already exists. _Default._
**IN CLUSTER** _cluster_name_ | The [cluster](/sql/create-cluster) to maintain this source.
**CONNECTION** _connection_name_ | The name of the SQL Server connection to use in the source. For details on creating connections, check the [`CREATE CONNECTION`](/sql/create-connection/#sql-server) documentation page.
**FOR ALL TABLES** | Create subsources for all tables with CDC enabled in all schemas upstream.
**FOR TABLES (** _table_list_ **)** | Create subsources for specific tables upstream. Requires fully-qualified table names (`<schema>.<table>`).
**RETAIN HISTORY FOR** <br>_retention_period_ | ***Private preview.** This option has known performance or stability issues and is under active development.* Duration for which Materialize retains historical data, which is useful to implement [durable subscriptions](/transform-data/patterns/durable-subscriptions/#history-retention-period). Accepts positive [interval](/sql/types/interval/) values (e.g. `'1hr'`). Default: `1s`.

## Creating a source

Materialize ingests the CDC stream for all (or a specific set of) tables in your
upstream SQL Server database that have [Change Data Capture enabled](https://learn.microsoft.com/en-us/sql/relational-databases/track-changes/about-change-data-capture-sql-server).

```mzsql
CREATE SOURCE mz_source
  FROM SQL SERVER CONNECTION sql_server_connection
  FOR ALL TABLES;
```

When you define a source, Materialize will automatically:

1. Create a **subsource** for each capture instance upstream, and perform an
   initial, snapshot-based sync of the associated tables before it starts
   ingesting change events.

    ```mzsql
    SHOW SOURCES;
    ```

    ```nofmt
             name         |   type     |  cluster  |
    ----------------------+------------+------------
     mz_source            | sql-server |
     mz_source_progress   | progress   |
     table_1              | subsource  |
     table_2              | subsource  |
    ```

1. Incrementally update any materialized or indexed views that depend on the
   source as change events stream in, as a result of `INSERT`, `UPDATE` and
   `DELETE` operations in the upstream SQL Server database.

It's important to note that the schema metadata is captured when the source is
initially created, and is validated against the upstream schema upon restart.
If you create new tables upstream after creating a SQL Server source and want to
replicate them to Materialize, the source must be dropped and recreated.

##### SQL Server schemas

`CREATE SOURCE` will attempt to create each upstream table in the same schema as
the source. This may lead to naming collisions if, for example, you are
replicating `schema1.table_1` and `schema2.table_1`. Use the `FOR TABLES`
clause to provide aliases for each upstream table, in such cases, or to specify
an alternative destination schema in Materialize.

```mzsql
CREATE SOURCE mz_source
  FROM SQL SERVER CONNECTION sql_server_connection
  FOR TABLES (schema1.table_1 AS s1_table_1, schema2.table_1 AS s2_table_1);
```

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

## Known limitations

{{% include-md file="shared-content/sql-server-considerations.md" %}}

## Examples

{{< important >}}
Before creating a SQL Server source, you must enable Change Data Capture and
`SNAPSHOT` transaction isolation in the upstream database.
{{</ important >}}

### Creating a connection

A connection describes how to connect and authenticate to an external system you
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
    PASSWORD SECRET sqlserver_pass
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
    USER 'materialize'
);
```

```mzsql
CREATE CONNECTION sqlserver_connection TO SQL SERVER (
    HOST 'instance.foo000.us-west-1.rds.amazonaws.com',
    SSH TUNNEL ssh_connection
);
```

For step-by-step instructions on creating SSH tunnel connections and configuring
an SSH bastion server to accept connections from Materialize, check
[this guide](/ops/network-security/ssh-tunnel/).

{{< /tab >}}
{{< /tabs >}}

### Creating a source {#create-source-example}

Once Change Data Capture and `SNAPSHOT` transaction isolation are enabled for
the database in your SQL Server instance, you also need to enable [Change Data
capture for the specific tables](https://learn.microsoft.com/en-us/sql/relational-databases/system-stored-procedures/sys-sp-cdc-enable-table-transact-sql) you want to replicate.

```sql
-- In SQL Server.
EXEC sys.sp_cdc_enable_table
  @source_schema = '<SCHEMA_NAME>',
  @source_name = '<TABLE_NAME>',
  @role_name = '<ROLE_FROM_MZ_CONNECTION>',
  @supports_net_changes = 0;
```

Once CDC is enabled for all of the relevant tables, you can create a `SOURCE` in
Materialize to begin replicating data!

_Create subsources for all tables in SQL Server_

```mzsql
CREATE SOURCE mz_source
    FROM SQL SERVER CONNECTION sqlserver_connection
    FOR ALL TABLES;
```

_Create subsources for specific tables in SQL Server_

```mzsql
CREATE SOURCE mz_source
  FROM SQL SERVER CONNECTION sqlserver_connection
  FOR TABLES (mydb.table_1, mydb.table_2 AS alias_table_2);
```

#### Handling unsupported types

If you're replicating tables that use [data types unsupported](#supported-types)
by SQL Server's CDC feature, use the `EXCLUDE COLUMNS` option to exclude them from
replication. This option expects the upstream fully-qualified names of the
replicated table and column (i.e. as defined in your SQL Server database).

```mzsql
CREATE SOURCE mz_source
  FROM SQL SERVER CONNECTION sqlserver_connection (
    EXCLUDE COLUMNS (mydb.table_1.column_of_unsupported_type)
  )
  FOR ALL TABLES;
```

### Handling errors and schema changes

{{< include-md file="shared-content/schema-changes-in-progress.md" >}}

To handle upstream [schema changes](#schema-changes) or errored subsources, use
the [`DROP SOURCE`](/sql/alter-source/#context) syntax to drop the affected
subsource, and then [`ALTER SOURCE...ADD SUBSOURCE`](/sql/alter-source/) to add
the subsource back to the source.

```mzsql
-- List all subsources in mz_source
SHOW SUBSOURCES ON mz_source;

-- Get rid of an outdated or errored subsource
DROP SOURCE table_1;

-- Start ingesting the table with the updated schema or fix
ALTER SOURCE mz_source ADD SUBSOURCE table_1;
```

## Related pages

- [`CREATE SECRET`](/sql/create-secret)
- [`CREATE CONNECTION`](/sql/create-connection)
- [`CREATE SOURCE`](../)

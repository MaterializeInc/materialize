---
title: "CREATE SOURCE: PostgreSQL"
description: "Connecting Materialize to a PostgreSQL database for Change Data Capture (CDC)."
pagerank: 40
menu:
  main:
    parent: 'create-source'
    identifier: cs_postgres
    name: PostgreSQL
    weight: 20
aliases:
  - /sql/create-source/postgresql
---

{{% create-source/intro %}}
Materialize supports PostgreSQL (11+) as a data source. To connect to a
PostgreSQL instance, you first need to [create a connection](#creating-a-connection)
that specifies access and authentication parameters.
Once created, a connection is **reusable** across multiple `CREATE SOURCE`
statements.
{{% /create-source/intro %}}

{{< warning >}}
Before creating a PostgreSQL source, you must set up logical replication in the
upstream database. For step-by-step instructions, see the integration guide for
your PostgreSQL service: [AlloyDB](/ingest-data/postgres-alloydb/),
[Amazon RDS](/ingest-data/postgres-amazon-rds/),
[Amazon Aurora](/ingest-data/postgres-amazon-aurora/),
[Azure DB](/ingest-data/postgres-azure-db/),
[Google Cloud SQL](/ingest-data/postgres-google-cloud-sql/),
[Self-hosted](/ingest-data/postgres-self-hosted/).
{{< /warning >}}

## Syntax

{{< diagram "create-source-postgres.svg" >}}

### `with_options`

{{< diagram "with-options-retain-history.svg" >}}

Field | Use
------|-----
_src_name_  | The name for the source.
**IF NOT EXISTS**  | Do nothing (except issuing a notice) if a source with the same name already exists. _Default._
**IN CLUSTER** _cluster_name_ | The [cluster](/sql/create-cluster) to maintain this source.
**CONNECTION** _connection_name_ | The name of the PostgreSQL connection to use in the source. For details on creating connections, check the [`CREATE CONNECTION`](/sql/create-connection/#postgresql) documentation page.
**FOR ALL TABLES** | Create subsources for all tables in the publication.
**FOR SCHEMAS (** _schema_list_ **)** | Create subsources for specific schemas in the publication.
**FOR TABLES (** _table_list_ **)** | Create subsources for specific tables in the publication.
**EXPOSE PROGRESS AS** _progress_subsource_name_ | The name of the progress collection for the source. If this is not specified, the progress collection will be named `<src_name>_progress`. For more information, see [Monitoring source progress](#monitoring-source-progress).
**RETAIN HISTORY FOR** <br>_retention_period_ | ***Private preview.** This option has known performance or stability issues and is under active development.* Duration for which Materialize retains historical data, which is useful to implement [durable subscriptions](/transform-data/patterns/durable-subscriptions/#history-retention-period). Accepts positive [interval](/sql/types/interval/) values (e.g. `'1hr'`). Default: `1s`.

### `CONNECTION` options

Field                                | Value     | Description
-------------------------------------|-----------|-------------------------------------
`PUBLICATION`                        | `text`    | **Required.** The PostgreSQL [publication](https://www.postgresql.org/docs/current/logical-replication-publication.html) (the replication data set containing the tables to be streamed to Materialize).
`TEXT COLUMNS`                       | A list of names | Decode data as `text` for specific columns that contain PostgreSQL types that are unsupported in Materialize.

## Features

### Change data capture

This source uses PostgreSQL's native replication protocol to continually ingest
changes resulting from `INSERT`, `UPDATE` and `DELETE` operations in the
upstream database — a process also known as _change data capture_.

For this reason, you must configure the upstream PostgreSQL database to support
logical replication before creating a source in Materialize. For step-by-step
instructions, see the integration guide for your PostgreSQL service:
[AlloyDB](/ingest-data/postgres-alloydb/),
[Amazon RDS](/ingest-data/postgres-amazon-rds/),
[Amazon Aurora](/ingest-data/postgres-amazon-aurora/),
[Azure DB](/ingest-data/postgres-azure-db/),
[Google Cloud SQL](/ingest-data/postgres-google-cloud-sql/),
[Self-hosted](/ingest-data/postgres-self-hosted/).

#### Creating a source

To avoid creating multiple replication slots in the upstream PostgreSQL database
and minimize the required bandwidth, Materialize ingests the raw replication
stream data for some specific set of tables in your publication.

```mzsql
CREATE SOURCE mz_source
  FROM POSTGRES CONNECTION pg_connection (PUBLICATION 'mz_source')
  FOR ALL TABLES;
```

When you define a source, Materialize will automatically:

1. Create a **replication slot** in the upstream PostgreSQL database (see
   [PostgreSQL replication slots](#postgresql-replication-slots)).

    The name of the replication slot created by Materialize is prefixed with
    `materialize_` for easy identification, and can be looked up in
    `mz_internal.mz_postgres_sources`.

    ```mzsql
    SELECT id, replication_slot FROM mz_internal.mz_postgres_sources;
    ```

    ```
       id   |             replication_slot
    --------+----------------------------------------------
     u8     | materialize_7f8a72d0bf2a4b6e9ebc4e61ba769b71
    ```
1. Create a **subsource** for each original table in the publication.

    ```mzsql
    SHOW SOURCES;
    ```

    ```nofmt
             name         |   type
    ----------------------+-----------
     mz_source            | postgres
     mz_source_progress   | progress
     table_1              | subsource
     table_2              | subsource
    ```

    And perform an initial, snapshot-based sync of the tables in the publication
    before it starts ingesting change events.

1. Incrementally update any materialized or indexed views that depend on the
source as change events stream in, as a result of `INSERT`, `UPDATE` and
`DELETE` operations in the upstream PostgreSQL database.

It's important to note that the schema metadata is captured when the source is
initially created, and is validated against the upstream schema upon restart.
If you create new tables upstream after creating a PostgreSQL source and want to
replicate them to Materialize, the source must be dropped and recreated.

##### PostgreSQL replication slots

Each source ingests the raw replication stream data for all tables in the
specified publication using **a single** replication slot. This allows you to
minimize the performance impact on the upstream database, as well as reuse the
same source across multiple materializations.

{{< warning >}}
Make sure to delete any replication slots if you stop using Materialize, or if
either the Materialize or PostgreSQL instances crash. To look up the name of
the replication slot created for each source, use `mz_internal.mz_postgres_sources`.
{{< /warning >}}

If you delete all objects that depend on a source without also dropping the
source, the upstream replication slot will linger and continue to accumulate
data so that the source can resume in the future. To avoid unbounded disk space
usage, make sure to use [`DROP SOURCE`](/sql/drop-source/) or manually delete
the replication slot.

For PostgreSQL 13+, it is recommended that you set a reasonable value for
[`max_slot_wal_keep_size`](https://www.postgresql.org/docs/13/runtime-config-replication.html#GUC-MAX-SLOT-WAL-KEEP-SIZE)
to limit the amount of storage used by replication slots.

##### PostgreSQL schemas

`CREATE SOURCE` will attempt to create each upstream table in the same schema as
the source. This may lead to naming collisions if, for example, you are
replicating `schema1.table_1` and `schema2.table_1`. Use the `FOR TABLES`
clause to provide aliases for each upstream table, in such cases, or to specify
an alternative destination schema in Materialize.

```mzsql
CREATE SOURCE mz_source
  FROM POSTGRES CONNECTION pg_connection (PUBLICATION 'mz_source')
  FOR TABLES (schema1.table_1 AS s1_table_1, schema2_table_1 AS s2_table_1);
```

### Monitoring source progress

By default, PostgreSQL sources expose progress metadata as a subsource that you
can use to monitor source **ingestion progress**. The name of the progress
subsource can be specified when creating a source using the `EXPOSE PROGRESS
AS` clause; otherwise, it will be named `<src_name>_progress`.

The following metadata is available for each source as a progress subsource:

Field          | Type                                     | Meaning
---------------|------------------------------------------|--------
`lsn`          | [`uint8`](/sql/types/uint/#uint8-info)   | The last Log Sequence Number (LSN) consumed from the upstream PostgreSQL replication stream.

And can be queried using:

```mzsql
SELECT lsn
FROM <src_name>_progress;
```

The reported LSN should increase as Materialize consumes **new** WAL records
from the upstream PostgreSQL database. For more details on monitoring source
ingestion progress and debugging related issues, see [Troubleshooting](/ops/troubleshooting/).

## Known limitations

{{% include-md file="shared-content/postgres-considerations.md" %}}

## Examples

{{< important >}}
Before creating a PostgreSQL source, you must set up logical replication in the
upstream database. For step-by-step instructions, see the integration guide for
your PostgreSQL service: [AlloyDB](/ingest-data/postgres-alloydb/),
[Amazon RDS](/ingest-data/postgres-amazon-rds/),
[Amazon Aurora](/ingest-data/postgres-amazon-aurora/),
[Azure DB](/ingest-data/postgres-azure-db/),
[Google Cloud SQL](/ingest-data/postgres-google-cloud-sql/),
[Self-hosted](/ingest-data/postgres-self-hosted/).
{{< /important >}}

### Creating a connection

A connection describes how to connect and authenticate to an external system you
want Materialize to read data from.

Once created, a connection is **reusable** across multiple `CREATE SOURCE`
statements. For more details on creating connections, check the
[`CREATE CONNECTION`](/sql/create-connection/#postgresql) documentation page.

```mzsql
CREATE SECRET pgpass AS '<POSTGRES_PASSWORD>';

CREATE CONNECTION pg_connection TO POSTGRES (
    HOST 'instance.foo000.us-west-1.rds.amazonaws.com',
    PORT 5432,
    USER 'postgres',
    PASSWORD SECRET pgpass,
    SSL MODE 'require',
    DATABASE 'postgres'
);
```

If your PostgreSQL server is not exposed to the public internet, you can
[tunnel the connection](/sql/create-connection/#network-security-connections)
through an AWS PrivateLink service or an SSH bastion host.

{{< tabs tabID="1" >}}
{{< tab "AWS PrivateLink">}}

```mzsql
CREATE CONNECTION privatelink_svc TO AWS PRIVATELINK (
    SERVICE NAME 'com.amazonaws.vpce.us-east-1.vpce-svc-0e123abc123198abc',
    AVAILABILITY ZONES ('use1-az1', 'use1-az4')
);
```

```mzsql
CREATE SECRET pgpass AS '<POSTGRES_PASSWORD>';

CREATE CONNECTION pg_connection TO POSTGRES (
    HOST 'instance.foo000.us-west-1.rds.amazonaws.com',
    PORT 5432,
    USER 'postgres',
    PASSWORD SECRET pgpass,
    AWS PRIVATELINK privatelink_svc,
    DATABASE 'postgres'
);
```

For step-by-step instructions on creating AWS PrivateLink connections and
configuring an AWS PrivateLink service to accept connections from Materialize,
check [this guide](/ops/network-security/privatelink/).

{{< /tab >}}
{{< tab "SSH tunnel">}}
```mzsql
CREATE CONNECTION ssh_connection TO SSH TUNNEL (
    HOST 'bastion-host',
    PORT 22,
    USER 'materialize',
);
```

```mzsql
CREATE CONNECTION pg_connection TO POSTGRES (
    HOST 'instance.foo000.us-west-1.rds.amazonaws.com',
    PORT 5432,
    SSH TUNNEL ssh_connection,
    DATABASE 'postgres'
);
```

For step-by-step instructions on creating SSH tunnel connections and configuring
an SSH bastion server to accept connections from Materialize, check
[this guide](/ops/network-security/ssh-tunnel/).

{{< /tab >}}
{{< /tabs >}}

### Creating a source {#create-source-example}

_Create subsources for all tables included in the PostgreSQL publication_

```mzsql
CREATE SOURCE mz_source
    FROM POSTGRES CONNECTION pg_connection (PUBLICATION 'mz_source')
    FOR ALL TABLES;
```

_Create subsources for all tables from specific schemas included in the
 PostgreSQL publication_

```mzsql
CREATE SOURCE mz_source
  FROM POSTGRES CONNECTION pg_connection (PUBLICATION 'mz_source')
  FOR SCHEMAS (public, project);
```

_Create subsources for specific tables included in the PostgreSQL publication_

```mzsql
CREATE SOURCE mz_source
  FROM POSTGRES CONNECTION pg_connection (PUBLICATION 'mz_source')
  FOR TABLES (table_1, table_2 AS alias_table_2);
```

#### Handling unsupported types

If the publication contains tables that use [data types](/sql/types/)
unsupported by Materialize, use the `TEXT COLUMNS` option to decode data as
`text` for the affected columns. This option expects the upstream names of the
replicated table and column (i.e. as defined in your PostgreSQL database).

```mzsql
CREATE SOURCE mz_source
  FROM POSTGRES CONNECTION pg_connection (
    PUBLICATION 'mz_source',
    TEXT COLUMNS (upstream_table_name.column_of_unsupported_type)
  ) FOR ALL TABLES;
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
#### Adding subsources

When adding subsources to a PostgreSQL source, Materialize opens a temporary
replication slot to snapshot the new subsources' current states. After
completing the snapshot, the table will be kept up-to-date, like all other
tables in the publication.

#### Dropping subsources

Dropping a subsource prevents Materialize from ingesting any data from it, in
addition to dropping any state that Materialize previously had for the table.

## Related pages

- [`CREATE SECRET`](/sql/create-secret)
- [`CREATE CONNECTION`](/sql/create-connection)
- [`CREATE SOURCE`](../)
- PostgreSQL integration guides:
  - [AlloyDB](/ingest-data/postgres-alloydb/)
  - [Amazon RDS](/ingest-data/postgres-amazon-rds/)
  - [Amazon Aurora](/ingest-data/postgres-amazon-aurora/)
  - [Azure DB](/ingest-data/postgres-azure-db/)
  - [Google Cloud SQL](/ingest-data/postgres-google-cloud-sql/)
  - [Self-hosted](/ingest-data/postgres-self-hosted/)

[`enum`]: https://www.postgresql.org/docs/current/datatype-enum.html
[`money`]: https://www.postgresql.org/docs/current/datatype-money.html

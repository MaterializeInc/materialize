---
title: "CREATE SOURCE: MySQL"
description: "Connecting Materialize to a MySQL database for Change Data Capture (CDC)."
pagerank: 40
menu:
  main:
    parent: 'create-source'
    identifier: cs_mysql
    name: MySQL
    weight: 20
---

{{% create-source/intro %}}
Materialize supports MySQL (5.7+) as a real-time data source. To connect to a
MySQL database, you first need to tweak its configuration to enable
[GTID-based binary log (binlog) replication](#change-data-capture), and then
[create a connection](#creating-a-connection) in Materialize that specifies
access and authentication parameters.
{{% /create-source/intro %}}

## Syntax

{{< note >}}
Although `schema` and `database` are [synonyms in MySQL](https://dev.mysql.com/doc/refman/8.0/en/glossary.html#glos_schema),
the MySQL source documentation and syntax **standardize on `schema`** as the
preferred keyword.
{{< /note >}}

{{< diagram "create-source-mysql.svg" >}}

### `with_options`

{{< diagram "with-options-retain-history.svg" >}}

Field | Use
------|-----
_src_name_  | The name for the source.
**IF NOT EXISTS**  | Do nothing (except issuing a notice) if a source with the same name already exists. _Default._
**IN CLUSTER** _cluster_name_ | The [cluster](/sql/create-cluster) to maintain this source.
**CONNECTION** _connection_name_ | The name of the MySQL connection to use in the source. For details on creating connections, check the [`CREATE CONNECTION`](/sql/create-connection/#mysql) documentation page.
**FOR ALL TABLES** | Create subsources for all tables in all schemas upstream. The [`mysql` system schema](https://dev.mysql.com/doc/refman/8.3/en/system-schema.html) is ignored.
**FOR SCHEMAS (** _schema_list_ **)** | Create subsources for specific schemas upstream.
**FOR TABLES (** _table_list_ **)** | Create subsources for specific tables upstream. Requires fully-qualified table names (`<schema>.<table>`).
**EXPOSE PROGRESS AS** _progress_subsource_name_ | The name of the progress collection for the source. If this is not specified, the progress collection will be named `<src_name>_progress`. For more information, see [Monitoring source progress](#monitoring-source-progress).
**RETAIN HISTORY FOR** <br>_retention_period_ | ***Private preview.** This option has known performance or stability issues and is under active development.* Duration for which Materialize retains historical data, which is useful to implement [durable subscriptions](/transform-data/patterns/durable-subscriptions/#history-retention-period). Accepts positive [interval](/sql/types/interval/) values (e.g. `'1hr'`). Default: `1s`.

### `CONNECTION` options

Field             | Value                           | Description
------------------|---------------------------------|-------------------------------------
`EXCLUDE COLUMNS` | A list of fully-qualified names | Exclude specific columns that cannot be decoded or should not be included in the subsources created in Materialize.
`TEXT COLUMNS`    | A list of fully-qualified names | Decode data as `text` for specific columns that contain MySQL types that are [unsupported in Materialize](#supported-types).

## Features

### Change data capture

{{< note >}}
For step-by-step instructions on enabling GTID-based binlog replication for your
MySQL service, see the integration guides:
[Amazon RDS](/ingest-data/mysql/amazon-rds/),
[Amazon Aurora](/ingest-data/mysql/amazon-aurora/),
[Azure DB](/ingest-data/mysql/azure-db/),
[Google Cloud SQL](/ingest-data/mysql/google-cloud-sql/),
[Self-hosted](/ingest-data/mysql/self-hosted/).
{{< /note >}}

The source uses MySQL's binlog replication protocol to **continually ingest
changes** resulting from `INSERT`, `UPDATE` and `DELETE` operations in the
upstream database. This process is known as _change data capture_.

The replication method used is based on [global transaction identifiers (GTIDs)](https://dev.mysql.com/doc/refman/8.0/en/replication-gtids.html),
and guarantees **transactional consistency** — any operation inside a MySQL
transaction is assigned the same timestamp in Materialize, which means that the
source will never show partial results based on partially replicated
transactions.

Before creating a source in Materialize, you **must** configure the upstream
MySQL database for GTID-based binlog replication. This requires the following
configuration changes:

<br>

Configuration parameter          | Value  | Details
---------------------------------|--------| -------------------------------
`log_bin`                        | `ON`   |
`binlog_format`                  | `ROW`  | This configuration is [deprecated as of MySQL 8.0.34](https://dev.mysql.com/doc/refman/8.0/en/replication-options-binary-log.html#sysvar_binlog_format). Newer versions of MySQL default to row-based logging.
`binlog_row_image`               | `FULL` |
`gtid_mode`                      | `ON`   |
`enforce_gtid_consistency`       | `ON`   |
`replica_preserve_commit_order`  | `ON`   | Only required when connecting Materialize to a read-replica for replication, rather than the primary server.

If you're running MySQL using a managed service, further configuration changes
might be required. For step-by-step instructions on enabling GTID-based binlog
replication for your MySQL service, see the integration guides.

#### Binlog retention

{{< warning >}}
If Materialize tries to resume replication and finds GTID gaps due to missing
binlog files, the source enters an errored state and you have to drop and
recreate it.
{{< /warning >}}

By default, MySQL retains binlog files for **30 days** (i.e., 2592000 seconds)
before automatically removing them. This is configurable via the
[`binlog_expire_logs_seconds`](https://dev.mysql.com/doc/mysql-replication-excerpt/8.0/en/replication-options-binary-log.html#sysvar_binlog_expire_logs_seconds)
system variable. We recommend using the default value for this configuration in
order to not compromise Materialize's ability to resume replication in case of
failures or restarts.

In some MySQL managed services, binlog expiration can be overriden by a
service-specific configuration parameter. It's important that you double-check
if such a configuration exists, and ensure it's set to the maximum interval
available.

As an example, [Amazon RDS for MySQL](/ingest-data/mysql/amazon-rds/) has its
own configuration parameter for binlog retention ([`binlog retention hours`](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/mysql-stored-proc-configuring.html#mysql_rds_set_configuration-usage-notes.binlog-retention-hours))
that overrides `binlog_expire_logs_seconds` and is set to `NULL` by default.

#### Creating a source

Materialize ingests the raw replication stream data for all (or a specific set
of) tables in your upstream MySQL database.

```mzsql
CREATE SOURCE mz_source
  FROM MYSQL CONNECTION mysql_connection
  FOR ALL TABLES;
```

When you define a source, Materialize will automatically:

1. Create a **subsource** for each original table upstream, and perform an
   initial, snapshot-based sync of the tables before it starts ingesting change
   events.

    ```mzsql
    SHOW SOURCES;
    ```

    ```nofmt
             name         |   type    |  cluster  |
    ----------------------+-----------+------------
     mz_source            | mysql     |
     mz_source_progress   | progress  |
     table_1              | subsource |
     table_2              | subsource |
    ```

1. Incrementally update any materialized or indexed views that depend on the
   source as change events stream in, as a result of `INSERT`, `UPDATE` and
   `DELETE` operations in the upstream MySQL database.

It's important to note that the schema metadata is captured when the source is
initially created, and is validated against the upstream schema upon restart.
If you create new tables upstream after creating a MySQL source and want to
replicate them to Materialize, the source must be dropped and recreated.

##### MySQL schemas

`CREATE SOURCE` will attempt to create each upstream table in the same schema as
the source. This may lead to naming collisions if, for example, you are
replicating `schema1.table_1` and `schema2.table_1`. Use the `FOR TABLES`
clause to provide aliases for each upstream table, in such cases, or to specify
an alternative destination schema in Materialize.

```mzsql
CREATE SOURCE mz_source
  FROM MYSQL CONNECTION mysql_connection
  FOR TABLES (schema1.table_1 AS s1_table_1, schema2.table_1 AS s2_table_1);
```

### Monitoring source progress

[//]: # "TODO(morsapaes) Replace this section with guidance using the new
progress metrics in mz_source_statistics + console monitoring, when available
(also for PostgreSQL)."

By default, MySQL sources expose progress metadata as a subsource that you
can use to monitor source **ingestion progress**. The name of the progress
subsource can be specified when creating a source using the `EXPOSE PROGRESS
AS` clause; otherwise, it will be named `<src_name>_progress`.

The following metadata is available for each source as a progress subsource:

Field              | Type                                                    | Details
-------------------|---------------------------------------------------------|--------------
`source_id_lower`  | [`uuid`](/sql/types/uuid/)  | The lower-bound GTID `source_id` of the GTIDs covered by this range.
`source_id_upper`  | [`uuid`](/sql/types/uuid/)  | The upper-bound GTID `source_id` of the GTIDs covered by this range.
`transaction_id`   | [`uint8`](/sql/types/uint/#uint8-info)                  | The `transaction_id` of the next GTID possible from the GTID `source_id`s covered by this range.

And can be queried using:

```mzsql
SELECT transaction_id
FROM <src_name>_progress;
```

Progress metadata is represented as a [GTID set](https://dev.mysql.com/doc/refman/8.0/en/replication-gtids-concepts.html)
of future possible GTIDs, which is similar to the [`gtid_executed`](https://dev.mysql.com/doc/refman/8.0/en/replication-options-gtids.html#sysvar_gtid_executed)
system variable on a MySQL replica. The reported `transaction_id` should
increase as Materialize consumes **new** binlog records from the upstream MySQL
database. For more details on monitoring source ingestion progress and
debugging related issues, see [Troubleshooting](/ops/troubleshooting/).

## Known limitations

{{< include-md file="shared-content/mysql-considerations.md" >}}

## Examples

{{< important >}}
Before creating a MySQL source, you must enable GTID-based binlog replication in the
upstream database. For step-by-step instructions, see the integration guide for
your MySQL service: [Amazon RDS](/ingest-data/mysql/amazon-rds/),
[Amazon Aurora](/ingest-data/mysql/amazon-aurora/),
[Azure DB](/ingest-data/mysql/azure-db/),
[Google Cloud SQL](/ingest-data/mysql/google-cloud-sql/),
[Self-hosted](/ingest-data/mysql/self-hosted/).
{{< /important >}}

### Creating a connection

A connection describes how to connect and authenticate to an external system you
want Materialize to read data from.

Once created, a connection is **reusable** across multiple `CREATE SOURCE`
statements. For more details on creating connections, check the
[`CREATE CONNECTION`](/sql/create-connection/#mysql) documentation page.

```mzsql
CREATE SECRET mysqlpass AS '<MYSQL_PASSWORD>';

CREATE CONNECTION mysql_connection TO MYSQL (
    HOST 'instance.foo000.us-west-1.rds.amazonaws.com',
    PORT 3306,
    USER 'materialize',
    PASSWORD SECRET mysqlpass
);
```

If your MySQL server is not exposed to the public internet, you can
[tunnel the connection](/sql/create-connection/#network-security-connections)
through an AWS PrivateLink service or an SSH bastion host SSH bastion host.

{{< tabs tabID="1" >}}
{{< tab "AWS PrivateLink">}}

```mzsql
CREATE CONNECTION privatelink_svc TO AWS PRIVATELINK (
   SERVICE NAME 'com.amazonaws.vpce.us-east-1.vpce-svc-0e123abc123198abc',
   AVAILABILITY ZONES ('use1-az1', 'use1-az4')
);

CREATE CONNECTION mysql_connection TO MYSQL (
    HOST 'instance.foo000.us-west-1.rds.amazonaws.com',
    PORT 3306,
    USER 'root',
    PASSWORD SECRET mysqlpass,
    AWS PRIVATELINK privatelink_svc
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
    USER 'materialize'
);
```

```mzsql
CREATE CONNECTION mysql_connection TO MYSQL (
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

_Create subsources for all tables in MySQL_

```mzsql
CREATE SOURCE mz_source
    FROM MYSQL CONNECTION mysql_connection
    FOR ALL TABLES;
```

_Create subsources for all tables from specific schemas in MySQL_

```mzsql
CREATE SOURCE mz_source
  FROM MYSQL CONNECTION mysql_connection
  FOR SCHEMAS (mydb, project);
```

_Create subsources for specific tables in MySQL_

```mzsql
CREATE SOURCE mz_source
  FROM MYSQL CONNECTION mysql_connection
  FOR TABLES (mydb.table_1, mydb.table_2 AS alias_table_2);
```

#### Handling unsupported types

If you're replicating tables that use [data types unsupported](#supported-types)
by Materialize, use the `TEXT COLUMNS` option to decode data as `text` for the
affected columns. This option expects the upstream fully-qualified names of the
replicated table and column (i.e. as defined in your MySQL database).

```mzsql
CREATE SOURCE mz_source
  FROM MYSQL CONNECTION mysql_connection (
    TEXT COLUMNS (mydb.table_1.column_of_unsupported_type)
  )
  FOR ALL TABLES;
```

#### Excluding columns

MySQL doesn't provide a way to filter out columns from the replication stream.
To exclude specific upstream columns from being ingested, use the `EXCLUDE
COLUMNS` option.

```mzsql
CREATE SOURCE mz_source
  FROM MYSQL CONNECTION mysql_connection (
    EXCLUDE COLUMNS (mydb.table_1.column_to_ignore)
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
- MySQL integration guides:
  - [Amazon RDS](/ingest-data/mysql/amazon-rds/)
  - [Amazon Aurora](/ingest-data/mysql/amazon-aurora/)
  - [Azure DB](/ingest-data/mysql/azure-db/)
  - [Google Cloud SQL](/ingest-data/mysql/google-cloud-sql/)
  - [Self-hosted](/ingest-data/mysql/self-hosted/)

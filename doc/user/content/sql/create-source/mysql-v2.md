---
title: "CREATE SOURCE: MySQL (New Syntax)"
description: "Connecting Materialize to a MySQL database for Change Data Capture (CDC)."
pagerank: 40
menu:
  main:
    parent: 'create-source'
    identifier: cs_mysql-v2
    name: MySQL (New Syntax)
    weight: 21
---

{{< private-preview />}}

{{< source-versioning-disambiguation is_new=true
other_ref="[old reference page](/sql/create-source/mysql/)" include_blurb=true >}}

## Prerequisites

{{% create-source/intro %}}
Materialize supports MySQL (5.7+) as a real-time data source. To connect to a
MySQL database, you first need to tweak its configuration to enable
[GTID-based binary log (binlog) replication](#change-data-capture) and set
[`binlog_row_metadata=FULL`](#change-data-capture), and then
[create a connection](#prerequisite-creating-a-connection-to-mysql) in
Materialize that specifies access and authentication parameters.
{{% /create-source/intro %}}

## Syntax

{{% include-syntax file="examples/create_source_mysql" example="syntax" %}}

## Ingesting data

After a source is created, you can create tables from the source referencing
upstream MySQL tables that have GTID-based binlog replication enabled. You can
create multiple tables that reference the same upstream table.

See [`CREATE TABLE FROM SOURCE`](/sql/create-table/) for details.

#### Handling table schema changes

The use of `CREATE SOURCE` with the new [`CREATE TABLE FROM
SOURCE`](/sql/create-table/) allows for the handling of certain upstream DDL
changes without downtime.

See [`CREATE TABLE FROM SOURCE`](/sql/create-table/#handling-table-schema-changes) for details.

#### Supported types

With the new syntax, after a MySQL source is created, you [`CREATE TABLE FROM
SOURCE`](/sql/create-table/) to create a corresponding table in Materialize and
start ingesting data.

{{% include-from-yaml data="mysql_source_details"
name="mysql-supported-types" %}}

{{% include-from-yaml data="mysql_source_details"
name="mysql-unsupported-types" %}}

For more information, including strategies for handling unsupported types,
see [`CREATE TABLE FROM SOURCE`](/sql/create-table/).

#### Upstream table truncation restrictions

{{% include-from-yaml data="mysql_source_details"
name="mysql-truncation-restriction" %}}

For additional considerations, see also [`CREATE TABLE`](/sql/create-table/).

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

The replication method used is based on [global transaction identifiers
(GTIDs)](https://dev.mysql.com/doc/refman/8.0/en/replication-gtids.html), and
guarantees **transactional consistency** — any operation inside a MySQL
transaction is assigned the same timestamp in Materialize, which means that the
source will never show partial results based on partially replicated
transactions.

Before creating a source in Materialize, you **must** configure the upstream
MySQL database for GTID-based binlog replication:

{{% mysql-direct/ingesting-data/mysql-configs %}}

In addition, the new `CREATE TABLE FROM SOURCE` syntax requires full row
metadata in the binlog. You must set the following system variable on the
upstream MySQL server:

```sql
SET GLOBAL binlog_row_metadata = FULL;
```

If you're running MySQL using a managed service, additional configuration
changes might be required. For step-by-step instructions on enabling GTID-based
binlog replication for your MySQL service, see the integration guides.

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

In some MySQL managed services, binlog expiration can be overridden by a
service-specific configuration parameter. It's important that you double-check
if such a configuration exists, and ensure it's set to the maximum interval
available.

As an example, [Amazon RDS for MySQL](/ingest-data/mysql/amazon-rds/) has its
own configuration parameter for binlog retention ([`binlog retention hours`](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/mysql-stored-proc-configuring.html#mysql_rds_set_configuration-usage-notes.binlog-retention-hours))
that overrides `binlog_expire_logs_seconds` and is set to `NULL` by default.

### Monitoring source progress

[//]: # "TODO(morsapaes) Replace this section with guidance using the new
progress metrics in mz_source_statistics + console monitoring, when available
(also for PostgreSQL)."

By default, MySQL sources expose progress metadata as a subsource that you can
use to monitor source **ingestion progress**. The name of the progress subsource
can be specified when creating a source using the `EXPOSE PROGRESS AS` clause;
otherwise, it will be named `<src_name>_progress`.

The following metadata is available for each source as a progress subsource:

Field              | Type                                                    | Details
-------------------|---------------------------------------------------------|--------------
`source_id_lower`  | [`uuid`](/sql/types/uuid/)                              | The lower-bound GTID `source_id` of the GTIDs covered by this range.
`source_id_upper`  | [`uuid`](/sql/types/uuid/)                              | The upper-bound GTID `source_id` of the GTIDs covered by this range.
`transaction_id`   | [`uint8`](/sql/types/uint/#uint8-info)                  | The `transaction_id` of the next GTID possible from the GTID `source_id`s covered by this range.

And can be queried using:

```mzsql
SELECT transaction_id
FROM <src_name>_progress;
```

Progress metadata is represented as a [GTID set](https://dev.mysql.com/doc/refman/8.0/en/replication-gtids-concepts.html)
of future possible GTIDs, which is similar to the
[`gtid_executed`](https://dev.mysql.com/doc/refman/8.0/en/replication-options-gtids.html#sysvar_gtid_executed)
system variable on a MySQL replica. The reported `transaction_id` should
increase as Materialize consumes **new** binlog records from the upstream MySQL
database. For more details on monitoring source ingestion progress and debugging
related issues, see [Troubleshooting](/ops/troubleshooting/).

## Known limitations

{{% include-headless "/headless/mysql-considerations" %}}

## Example

{{< important >}}
Before creating a MySQL source, you must enable GTID-based binlog replication in
the upstream database. For step-by-step instructions, see the integration guide
for your MySQL service: [Amazon RDS](/ingest-data/mysql/amazon-rds/),
[Amazon Aurora](/ingest-data/mysql/amazon-aurora/),
[Azure DB](/ingest-data/mysql/azure-db/),
[Google Cloud SQL](/ingest-data/mysql/google-cloud-sql/),
[Self-hosted](/ingest-data/mysql/self-hosted/).
{{< /important >}}

### Creating a source {#create-source-example}

#### Prerequisite: Creating a connection to MySQL

First, you must create a connection to your MySQL database. A connection
describes how to connect and authenticate to an external system you want
Materialize to read data from.

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

If your MySQL server is not exposed to the public internet, you can [tunnel the
connection](/sql/create-connection/#network-security-connections) through an
AWS PrivateLink service (Materialize Cloud) or an SSH bastion host.

{{< tabs tabID="1" >}}
{{< tab "AWS PrivateLink (Materialize Cloud)">}}

{{< include-md file="shared-content/aws-privatelink-cloud-only-note.md" >}}

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

#### Creating the source in Materialize

You **must** enable GTID-based binlog replication before creating the source.
See [Change data capture](#change-data-capture) for configuration details.

Once replication is configured, create a `SOURCE` in Materialize:

```mzsql
CREATE SOURCE mz_source
    FROM MYSQL CONNECTION mysql_connection;
```

After a source is created, you can create a table from the source, referencing
specific upstream table(s).

_Creates a table in Materialize from the upstream table `mydb.orders`_

```mzsql
CREATE TABLE orders FROM SOURCE mz_source (REFERENCE mydb.orders);
```

## Related pages

- [`CREATE TABLE`](/sql/create-table/)
- [`CREATE SECRET`](/sql/create-secret)
- [`CREATE CONNECTION`](/sql/create-connection)
- [`CREATE SOURCE`](../)
- MySQL integration guides:
  - [Amazon RDS](/ingest-data/mysql/amazon-rds/)
  - [Amazon Aurora](/ingest-data/mysql/amazon-aurora/)
  - [Azure DB](/ingest-data/mysql/azure-db/)
  - [Google Cloud SQL](/ingest-data/mysql/google-cloud-sql/)
  - [Self-hosted](/ingest-data/mysql/self-hosted/)

---
title: "CREATE SOURCE: MySQL (New Syntax)"
description: "Connecting Materialize to a MySQL database for Change Data Capture (CDC)."
pagerank: 40
menu:
    main:
        parent: "create-source"
        identifier: cs_mysql-v2
        name: MySQL (New Syntax)
        weight: 15
---

{{< public-preview />}}

{{< source-versioning-disambiguation is_new=true
other_ref="[old reference page](/sql/create-source/mysql/)" include_blurb=true >}}

{{% create-source-intro external_source="MySQL" version="8.0.1+"
create_table="/sql/create-table/" %}}

## Prerequisites

{{% include-from-yaml data="mysql_source_details" name="mysql-source-prereq" %}}

## Syntax

{{% include-syntax file="examples/create_source_mysql" example="syntax" %}}

## Ingesting data

After a source is created, you can create tables from the source referencing
upstream MySQL tables that have [GTID-based binlog replication
enabled](#change-data-capture) (Note: `binlog_row_metadata=FULL` is required to
use the new syntax). You can create multiple tables that reference the same
upstream table. See [`CREATE TABLE FROM SOURCE`](/sql/create-table/) for
details.

### Handling table schema changes

The use of `CREATE SOURCE` with the new [`CREATE TABLE FROM
SOURCE`](/sql/create-table/) allows for the handling of certain upstream DDL
changes, specifically adding or dropping columns in the upstream tables, without
downtime.

See [Guide: Handle upstream schema
changes](/ingest-data/mysql/source-versioning/) for details.

### Supported types

With the new syntax, after a MySQL source is created, you [`CREATE TABLE FROM
SOURCE`](/sql/create-table/) to create a corresponding table in Materialize and
start ingesting data.

{{% include-from-yaml data="mysql_source_details"
name="mysql-supported-types" %}}

{{% include-from-yaml data="mysql_source_details"
name="mysql-unsupported-types" %}}

For more information, including strategies for handling unsupported types,
see [`CREATE TABLE FROM SOURCE`](/sql/create-table/).

### Upstream table truncation restrictions

{{% include-from-yaml data="mysql_source_details"
name="mysql-truncation-restriction" %}}

For additional considerations, see also [`CREATE TABLE`](/sql/create-table/).

### Change data capture

{{< note >}}

For step-by-step instructions on enabling GTID-based binlog replication for your
MySQL service, see the integration guides:
{{% include-headless "headless/mysql-ingest-guides" %}}

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

{{< tip >}}

For `binlog_row_metadata`, using `SET GLOBAL binlog_row_metadata = FULL;` does
not persist across MySQL server restarts. To make
the setting durable, use `SET PERSIST` (MySQL 8.0.11+) or set
`binlog_row_metadata=FULL` in the server's configuration file. On managed
services, set the variable through the service's parameter configuration
instead.
{{< /tip >}}

If you're running MySQL using a managed service, additional configuration
changes might be required. To enable GTID-based binlog replication for your
MySQL service, see the integration guides.

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

By default, MySQL sources expose progress metadata as a subsource that you can
use to monitor source **ingestion progress**. The name of the progress subsource
can be specified when creating a source using the `EXPOSE PROGRESS AS` clause;
otherwise, it will be named `<src_name>_progress`.

The following metadata is available for each source as a progress subsource:

| Field             | Type                                   | Details                                                                                          |
| ----------------- | -------------------------------------- | ------------------------------------------------------------------------------------------------ |
| `source_id_lower` | [`uuid`](/sql/types/uuid/)             | The lower-bound GTID `source_id` of the GTIDs covered by this range.                             |
| `source_id_upper` | [`uuid`](/sql/types/uuid/)             | The upper-bound GTID `source_id` of the GTIDs covered by this range.                             |
| `transaction_id`  | [`uint8`](/sql/types/uint/#uint8-info) | The `transaction_id` of the next GTID possible from the GTID `source_id`s covered by this range. |

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
database. For more information, see [Troubleshooting](/ops/troubleshooting/).

## Example

{{< important >}}

Before creating a MySQL source, you must enable [GTID-based binary log (binlog)
replication](#change-data-capture), including setting
[`binlog_row_metadata=FULL`](#change-data-capture) to use the new syntax.

{{< /important >}}

### Prerequisites

{{% include-from-yaml data="mysql_source_details" name="mysql-source-prereq" %}}

For details, see the [MySQL integration
guides](/ingest-data/mysql/#integration-guides).

### Create a source

{{% include-example file="examples/create_source_mysql"
 example="create-source" %}}

{{% include-example file="examples/create_source_mysql"
 example="create-table" %}}

## Related pages

- [`CREATE TABLE`](/sql/create-table/)
- [`CREATE SECRET`](/sql/create-secret)
- [`CREATE CONNECTION`](/sql/create-connection)
- [`CREATE SOURCE`](../)
- [MySQL integration guides](/ingest-data/mysql/#integration-guides)

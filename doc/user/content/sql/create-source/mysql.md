---
title: "CREATE SOURCE: MySQL"
description: "Connecting Materialize to a MySQL database."
menu:
  main:
    parent: 'create-source'
    name: "MySQL"
    identifier: 'create-source-mysql'
---

{{< source-versioning-disambiguation is_new=true
other_ref="[old reference page](/sql/create-source-v1/mysql/)" include_blurb=true >}}

{{% create-source-intro external_source="MySQL" version="5.7+"
create_table="/sql/create-table/mysql/" %}}

## Prerequisites

{{< include-md file="shared-content/mysql-source-prereq.md" >}}

## Syntax

{{% include-example file="examples/create_source/example_mysql_source"
 example="syntax" %}}

{{% include-example file="examples/create_source/example_mysql_source"
 example="syntax-options" %}}

## Details

### Change data capture

The source uses MySQL's binlog replication protocol to **continually ingest
changes** resulting from `INSERT`, `UPDATE` and `DELETE` operations in the
upstream database. This process is known as _change data capture_.

The replication method used is based on [global transaction identifiers (GTIDs)](https://dev.mysql.com/doc/refman/8.0/en/replication-gtids.html),
and guarantees **transactional consistency** — any operation inside a MySQL
transaction is assigned the same timestamp in Materialize, which means that the
source will never show partial results based on partially replicated
transactions.

Before creating a source in Materialize, you **must** configure the upstream
MySQL database for GTID-based binlog replication. Ensure the upstream MySQL
database has been configured for GTID-based binlog replication:

{{% mysql-direct/ingesting-data/mysql-configs %}}

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

In some MySQL managed services, binlog expiration can be overriden by a
service-specific configuration parameter. It's important that you double-check
if such a configuration exists, and ensure it's set to the maximum interval
available.

As an example, [Amazon RDS for MySQL](/ingest-data/mysql/amazon-rds/) has its
own configuration parameter for binlog retention ([`binlog retention hours`](https://docs.aws.amazon.com/AmazonRDS/latest/UserGuide/mysql-stored-proc-configuring.html#mysql_rds_set_configuration-usage-notes.binlog-retention-hours))
that overrides `binlog_expire_logs_seconds` and is set to `NULL` by default.

### Schema metadata

It's important to note that the schema metadata is captured when the source is
initially created, and is validated against the upstream schema upon restart.
If you create new tables upstream after creating a MySQL source and want to
replicate them to Materialize, the source must be dropped and recreated.


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

{{< include-md file="shared-content/mysql-considerations-v2.md" >}}

## Examples

### Prerequisites

{{< include-md file="shared-content/mysql-source-prereq.md" >}}

### Create a source {#create-source-example}

{{% include-example file="examples/create_source/example_mysql_source"
 example="create-source" %}}

 {{% include-example file="examples/create_source/example_mysql_source"
 example="create-table" %}}

## Related pages

- [`CREATE SECRET`](/sql/create-secret)
- [`CREATE CONNECTION`](/sql/create-connection)
- [`CREATE TABLE`]
- [MySQL integration guides](/ingest-data/mysql/)

[`CREATE TABLE`]: /sql/create-table/mysql/
[external_source]: MySQL
[version]: 5.7+

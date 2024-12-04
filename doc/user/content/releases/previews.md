---
title: "Preview features"
description: ""
disable_list: true
draft: true
menu:
  main:
    parent: releases-previews
    weight: 50
    identifier: previews
---

The features listed below are available as [private/public
preview](https://materialize.com/preview-terms/). Public preview features are
enabled by default; private previews can be enabled for your region(s). If you
would like to enable a private preview feature for your region(s), contact your
Materialize support.

{{< note >}}

- The features are under **active development** and may have stability or
  performance issues.

- The features are **not** subject to our backwards compatibility guarantees.

{{</ note >}}

## Private Preview

{{< note >}}

If you would like to enable a private preview feature for your region(s),
contact your Materialize support.

{{</ note >}}


### Graceful cluster resizing

For clusters that do not contain sources or sinks, Materialize supports altering
the cluster size with no downtime (i.e., graceful cluster resizing).

For more information, see [Graceful cluster resizing](/sql/alter-cluster/#graceful-cluster-resizing).

### Real-time recency

Materialize offers a form of "end-to-end linearizability" known as real-time
recency, where the results are guaranteed to contain all visible data according
to physical time.

For more information, see [Real-time
recency](/get-started/isolation-level/#real-time-recency).

### Refresh strategies for materialized views

Materialize supports the configuration of refresh strategies for your
materialized views. For instance, you might have data that does not require
up-to-the-second freshness or can be accessed using different patterns to
optimize for performance and cost (e.g., hot vs. cold data). To support these
use cases, you can configure the refresh strategy of a materialized view.

For more information, see [Refresh
strategies](/sql/create-materialized-view/#refresh-strategies).

### Scheduled clusters

To support refresh strategies for materialized views[(Private
Preview)](#refresh-strategies-for-materialized-views), Materialize supports
scheduling clusters to automatically turn on and off. Scheduled clusters can
only contain materialized views configured with a [non-default refresh
strategy](/sql/create-materialized-view/#refresh-strategies).

For more information, see:

- [Scheduling](/sql/create-cluster/#scheduling).

- [`CREATE CLUSTER`](/sql/create-cluster).
- [`ALTER CLUSTER`](/sql/alter-cluster).

### Support for Fivetran

Materialize adds support for Fivetran to sync data into Materialize.

For more information, see [Fivetran](/ingest-data/fivetran/).

### SUBSCRIBE: ENVELOPE DEBEZIUM

Materialize adds the `ENVELOPE DEBEZIUM` option to the
[`SUBSCRIBE`](/sql/subscribe/) command.  `ENVELOPE DEBEZIUM` modifies the
`SUBSCRIBE` output to support upserts that use a [Debezium-style diff
envelope](/sql/subscribe/#envelope-debezium).

For more information, see [`SUBSCRIBE`: `ENVELOPE
DEBEZIUM`](/sql/subscribe/#envelope-debezium).

### SUBSCRIBE: WITHIN TIMESTAMP ORDER BY

Materialize adds the `WITHIN TIMESTAMP ORDER BY` option to the
[`SUBSCRIBE`](/sql/subscribe/) command. `WITHIN TIMESTAMP ORDER BY` modifies the
output ordering of `SUBSCRIBE`.

For more information, see [`SUBSCRIBE`: `WITHIN TIMESTAMP ORDER
BY`](/sql/subscribe/#within-timestamp-order-by).

### Sink headers

Materialize adds the `HEADERS` option to the [`CREATE
SINK`](/sql/create-sink/kafka/#headers) command to support adding additional
headers  to each message emitted by the sink.

For more information, see [`CREATE SINK`: `PARTITION
BY`](/sql/create-sink/kafka/#headers).

### Sink partitioning strategy

Materialize adds the `PARTITION BY` option to the [`CREATE
SINK`](/sql/create-sink/kafka/#partitioning) command to support  custom
partitioning strategy.

For more information, see [`CREATE SINK`: `PARTITION
BY`](/sql/create-sink/kafka/#partitioning).

### Value decoding error handling

Materialize adds the `VALUE DECODING ERRORS = INLINE` option to configure the
source to continue ingesting data in the presence of value decoding errors.

For more information, see [`CREATE SOURCE`: `VALUE DECODING
ERRORS`](/sql/create-source/kafka/#value-decoding-errors).

### Retention period

Materialize adds history retention period configuration for its objects.

For more information, see:

- [`CREATE INDEX`:`retention_period`](/sql/create-index/#with_options)
- [`CREATE
  INDEX`:`retention_period`](/sql/create-materialized-view/#with_options)
- [`CREATE INDEX`:`retention_period`](/sql/create-table/#with_options)
- [`CREATE SOURCE: Kafka`:
  `retention_period`](/sql/create-source/kafka/#with_options)
- [`CREATE SOURCE: MySQL`:
  `retention_period`](/sql/create-source/mysql/#with_options)
- [`CREATE SOURCE: PostgreSQL`:
  `retention_period`](/sql/create-source/postgres/#with_options)

## Public preview

### Bulk exports to object storage

Bulk exports allow you to periodically dump results computed in Materialize to
object storage. This is useful to perform tasks like periodic **backups for
auditing**, and additional downstream processing in **analytical data
warehouses** like Snowflake, Databricks or BigQuery.

| Object storage service                      | Supported? |
| ------------------------------------------- |:----------:|
| Amazon S3                                   | ✓          |
| Google Cloud Storage                        | ✓          |
| Azure Blob Storage                          |            |

For more information on bulk exports to object storage, see the
[reference documentation](/sql/copy-to/#copy-to-s3) and the warehouse-specific
integration guides:

| Cloud data warehouse service                | Integration guide                                                                     |
| ------------------------------------------- | ------------------------------------------------------------------------------------- |
| Snowflake                                   | [Snowflake via object storage](https://materialize.com/docs/serve-results/snowflake/) |
| BigQuery                                    | Coming soon!                                                                          |
| Databricks                                  | Coming soon!                                                                          |

### Health Dashboard

[Materialize Console](https://console.materialize.com/) introduces an
**Environment Overview** page under **Monitoring**. The **Environment Overview**
page displays a health dashboard that provides the status for each cluster in
your environment.

![Image of the Health Dashboard in the Materialize Console Environment
Overview](/images/health-dashboard.png "Health Dashboard in the  Materialize
Console Environment Overview")

### AWS Connection for IAM authentication

Materialize supports the use of an AWS Connection to perform:

- IAM authentication with an Amazon MSK cluster.

- Bulk exports to Amazon S3. See [Bulk exports to object
  storage](#bulk-exports-to-object-storage).

For more information, see [`CREATE Connection:
AWS`](/sql/create-connection/#aws).

### Various mz_internal tables

- `mz_recent_activity_log`: Contains a log of the SQL statements that have been
  issued to Materialize in the last three days, along with various metadata
  about them.

- `mz_notices`: Contains a list of currently active notices emitted by the
  system.

- `mz_notices_redacted`: Contains a redacted list of currently active optimizer
  notices emitted by the system.

{{< warning >}}

The objects in the `mz_internal` schema are not part of Materialize's stable
interface. Backwards-incompatible changes to these objects may be made at any
time.

{{< /warning >}}

For more information, see [mz_internal](/sql/system-catalog/mz_internal/).

### EXPLAIN FILTER PUSHDOWN

Materialize adds the [`EXPLAIN FILTER PUSHDOWN`](/sql/explain-filter-pushdown/)
command to display the filter pushdown statistics for `SELECT` statements and
materialized views.

For more information, see [`EXPLAIN FILTER
PUSHDOWN`](/sql/explain-filter-pushdown/).

### Recursive CTE

Materialize adds the `WITH MUTUALLY RECURSIVE` option to support recursive CTEs.

For more information, see [`WITH MUTUALLY
RECURSIVE`](/sql/select/recursive-ctes/).

## Private previews enabled by default

The features listed below are in preview but enabled by default.

### Websocket API

Materialize provides an interactive WebSocket API endpoint. This feature is
enabled by default.

For more information, see [Websocket API](/integrations/websocket-api/).

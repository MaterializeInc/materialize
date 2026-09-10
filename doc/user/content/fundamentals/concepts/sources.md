---
title: Sources
description: Learn about sources in Materialize.
menu:
  main:
    parent: concepts
    weight: 30
    identifier: 'concepts-sources'
aliases:
  - /get-started/key-concepts/#sources
  - /self-managed/v25.2/concepts/sources/
  - /concepts/sources/
---

## Overview

{{% include-headless "/headless/source-definition" %}}

## Supported external systems

Materialize supports ingesting data from the following external systems:

{{% include-headless "/headless/ingest-connectors-table" %}}

## Creating a source

### Prerequisites

{{% include-headless "/headless/source-upstream-prereq" %}}

### CREATE SOURCE syntax

To create a source, you use the [`CREATE SOURCE`](/sql/create-source/) syntax.
There are two versions of the syntax:

- *Recommended.* The new [`CREATE SOURCE`](/sql/create-source/#new-syntax)
  syntax, used with [`CREATE TABLE ... FROM SOURCE`](/sql/create-table/). The
  new syntax allows Materialize to handle certain upstream schema changes,
  specifically adding or dropping columns, **without** downtime.

- The legacy [`CREATE SOURCE ... FOR <ALL
  TABLES|TABLES|SCHEMAS>`](/sql/create-source/#legacy-syntax) syntax, which
  creates a source and its subsources. *Subsource* is the legacy term for the
  read-only tables created from a source. With the legacy `CREATE SOURCE ...
  FOR ...` syntax, the subsources are automatically created when the `CREATE
  SOURCE ...` command is issued.

### Tables and subsources

A source makes external data available in Materialize through:

- The [tables](/sql/create-table/) created from it, when using the new
  `CREATE SOURCE` syntax.

- The subsources, when using the legacy `CREATE SOURCE` syntax.

Both the tables and subsources created from a source are **read-only**.
Materialize populates them by ingesting changes from the upstream system, and
you cannot insert, update, or delete their data directly.

## Snapshotting

When you create a table from a source (or, with the legacy syntax, when the
subsources are created), Materialize [snapshots](/fundamentals/concepts/snapshotting/) the
data currently available in the upstream system for that table.

{{% include-headless "/headless/ingestion/snapshotting-queries" %}}

See [Snapshotting](/fundamentals/concepts/snapshotting/) for more information.

## Hydration

{{% include-from-yaml data="hydration-details" name="definition" %}}

{{% include-from-yaml data="hydration-details" name="sources-summary" %}}

See [Hydration](/fundamentals/concepts/hydration/) for more information.

## Lifecycle of a source

After you run `CREATE TABLE ... FROM SOURCE` for a table (or a cluster
replica hosting the source restarts), the table moves through a sequence of
states before it's continuously serving up-to-date data. Knowing which state a
table is in tells you whether it's making progress or is stuck.

All source types (Kafka, PostgreSQL, MySQL, SQL Server, and load generators)
report through the same
[`mz_source_statuses`](/sql/system-catalog/mz_internal/#mz_source_statuses)
mechanism and go through the same states below. The examples monitor a `bids`
table created from an `AUCTION` load-generator source named `auction_house`:

```mzsql
CREATE SOURCE auction_house FROM LOAD GENERATOR AUCTION (TICK INTERVAL '1s');
CREATE TABLE bids FROM SOURCE auction_house (REFERENCE bids);
```

Substitute your own source and table names.

### Created

Immediately after creation, before the table has reported any status, `status`
reads `created`. This is a placeholder for "no status yet reported" rather
than a state the table lingers in, so it's rarely observed in practice.

### Paused

A table with no cluster replica to run on reports `paused`:

```mzsql
SELECT o.name, s.status, s.details
FROM mz_internal.mz_source_statuses s
JOIN mz_objects o ON o.id = s.id
WHERE o.name = 'bids';
```

```none
 name |  status  |                             details
------+----------+-------------------------------------------------------------------
 bids | paused   | {"hints":["There is currently no replica running this source"]}
```

To resolve, [increase the replication
factor](/sql/alter-cluster/#replication-factor-1) of the cluster hosting the
source. A table also reports `paused`, with a different hint, when the
specific replica that was running it is dropped (for example, during a
[cluster resize](/sql/alter-cluster/#resizing)):

```none
 name |  status  |                           details
------+----------+-----------------------------------------------------------------
 bids | paused   | {"hints":["The replica running this source has been dropped"]}
```

### Starting

Once a replica is available, the table connects to the upstream system and
initializes:

```none
 name |  status
------+----------
 bids | starting
```

If a table stays in `starting` for more than a few minutes, see
[Troubleshooting: Why isn't my source ingesting
data?](/ingest-data/troubleshooting/#why-isnt-my-source-ingesting-data).

### Running: snapshotting

Once connected, the table [snapshots](/fundamentals/concepts/snapshotting/) the
data already available in the upstream system. `status` reports `running`
throughout both snapshotting and steady-state ingestion; to tell them apart,
check `snapshot_committed` in
[`mz_source_statistics`](/sql/system-catalog/mz_internal/#mz_source_statistics),
as described in [Monitoring the snapshotting
progress](/ingest-data/monitoring-data-ingestion/#monitoring-the-snapshotting-progress).

While a table is snapshotting, it cannot serve queries: queries block until
snapshotting completes. See [Snapshotting](/fundamentals/concepts/snapshotting/)
for snapshot duration and upstream impact, which vary by source type: CDC
sources (PostgreSQL, MySQL, SQL Server) must retain their change log until the
snapshot completes, while Kafka sources have no equivalent retention
requirement.

### Running: steady state

Once the snapshot is committed, the table continually ingests changes from the
upstream system. `status` remains `running`; to confirm the table has caught
up and is tracking the upstream system with low lag, see [Monitoring data
lag](/ingest-data/monitoring-data-ingestion/#monitoring-data-lag).

{{< note >}}
A cluster replica restart or resize triggers [hydration](/fundamentals/concepts/hydration/) for the table. For Kafka **upsert** sources, this rebuilds the table's internal upsert index from storage; for other source types, hydration is negligible or not applicable. See [Hydration](/fundamentals/concepts/hydration/).
{{< /note >}}

### Stalled

A configuration or connectivity issue reports `stalled`, with the specific
problem in the `error` column and, often, a suggested fix in `details`:

```mzsql
SELECT o.name, s.status, s.error, s.details
FROM mz_internal.mz_source_statuses s
JOIN mz_objects o ON o.id = s.id
WHERE o.name = 'bids';
```

See [Troubleshooting data ingestion](/ingest-data/troubleshooting/) for causes
that apply to any source type, and the CDC-specific troubleshooting guides for
[PostgreSQL](/ingest-data/postgres/troubleshooting/) and
[MySQL](/ingest-data/mysql/troubleshooting/) for replication-slot, WAL, and
GTID issues unique to those connectors.

### Dropped

Dropping a table (or its source) is terminal: once dropped, the table no
longer appears in `mz_source_statuses`, but its final `dropped` status remains
in `mz_source_status_history`:

```mzsql
SELECT status FROM mz_internal.mz_source_status_history
WHERE source_id = '<TABLE_ID>' AND status = 'dropped';
```

```none
 status
---------
 dropped
```

### Full history

To see every transition a table has gone through, useful when debugging why a
table is in its current state, query
[`mz_source_status_history`](/sql/system-catalog/mz_internal/#mz_source_status_history)
ordered by time:

```mzsql
SELECT occurred_at, status, error
FROM mz_internal.mz_source_status_history h
JOIN mz_objects o ON o.id = h.source_id
WHERE o.name = 'bids'
ORDER BY occurred_at;
```

## Sources and clusters

Sources require compute resources in Materialize. That is, sources must be
associated with a [cluster](/fundamentals/concepts/clusters/). If possible, dedicate a
cluster just for sources.

See also [Operational guidelines](/clusters/operational-guidelines/).

## Related pages

- [`CREATE SOURCE`](/sql/create-source)
- [`CREATE TABLE`](/sql/create-table)
- [Snapshotting](/fundamentals/concepts/snapshotting/)
- [Hydration](/fundamentals/concepts/hydration/)
- [Monitoring data ingestion](/ingest-data/monitoring-data-ingestion/)
- [Troubleshooting data ingestion](/ingest-data/troubleshooting/)

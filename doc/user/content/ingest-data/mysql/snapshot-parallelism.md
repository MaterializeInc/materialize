---
title: "Snapshot parallelism"
description: "How Materialize parallelizes the initial snapshot of MySQL tables, and how to get the most out of it."
menu:
  main:
    parent: "mysql"
    name: "Snapshot parallelism"
    identifier: "mysql-snapshot-parallelism"
    weight: 70
---

{{< private-preview />}}

When you create a [MySQL source](/sql/create-source/mysql-v2/), Materialize
performs an initial, snapshot-based sync of the selected tables before it
starts ingesting change events from the binlog. For large tables, this
snapshot dominates the time until the source becomes healthy and your queries
return up-to-date results.

To speed this up, Materialize can split the snapshot of a single table across
all the workers of the cluster hosting the source. Each worker reads a
disjoint range of the table's primary key space in parallel, instead of one
worker reading the whole table on its own.

## How it works

Before reading a table, Materialize samples the table's primary key to find
boundary keys that divide it into ranges of roughly equal size, using
inexpensive index probes and optimizer row estimates. Each worker then reads
only its assigned key range, within the same consistent snapshot of the
upstream database. The results are identical to a single-worker snapshot,
including transactional consistency: parallelism changes only how fast the
snapshot completes.

Sampling adds a small number of point queries per table before the snapshot
starts. These queries use the primary key index and do not scan the table.
Their number is capped in proportion to the table's estimated size, so
sampling stays negligible next to the snapshot itself.

## Requirements

A table's snapshot is parallelized when all of the following hold:

- The table has a **single-column primary key** of a **string type**
  (`CHAR`, `VARCHAR`, or `TEXT`). Composite and numeric primary keys are not
  yet supported.
- The table is large enough to be worth splitting. Small tables are read by a
  single worker, where parallelism would add overhead without benefit.

Tables that don't meet these requirements are still snapshot correctly, each
by a single worker. Different tables are always processed concurrently,
independent of this feature.

## Upstream considerations

- **Connection count.** During snapshotting, Materialize opens one connection
  per worker reading a key range, plus a small number of coordination
  connections. If your MySQL server or connection pooler enforces a low
  [`max_connections`](https://dev.mysql.com/doc/refman/8.0/en/server-system-variables.html#sysvar_max_connections)
  limit, account for this burst when sizing it. After the snapshot completes,
  the source drops back to a single replication connection.

- **Statistics freshness.** Range boundaries are placed using the MySQL
  optimizer's row estimates. Stale statistics don't affect correctness, but
  can skew how evenly work divides across workers. Running
  [`ANALYZE TABLE`](https://dev.mysql.com/doc/refman/8.0/en/analyze-table.html)
  on very large tables before creating the source can improve balance.

- **Read load.** A parallel snapshot reads the same total data as a serial
  one, but over a shorter window, so expect proportionally higher read
  throughput on the upstream database (or read replica) while it runs.

## Observability

The progress of an ongoing snapshot is visible in the
[`mz_internal.mz_source_statistics`](/reference/system-catalog/mz_internal/#mz_source_statistics)
system catalog view, including the number of rows read so far relative to the
estimated total.

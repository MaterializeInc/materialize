---
title: "Snapshot parallelism"
description: "How Materialize splits the snapshot of a single MySQL table across the workers of a cluster."
menu:
  main:
    parent: "mysql"
    name: "Snapshot parallelism"
    identifier: "mysql-snapshot-parallelism"
    weight: 70
---

When you create a [MySQL source](/sql/create-source/mysql-v2/), Materialize
performs an initial, snapshot-based sync of the selected tables before it
starts ingesting change events from the binlog. For large tables, this
snapshot dominates the time until the source becomes healthy.

How snapshot work is spread across the workers of a cluster, and what that
means for the upstream database, is covered in
[Snapshotting](/concepts/snapshotting/#parallelism). This page covers what is
specific to MySQL: Materialize can split the read of a **single table**
across all the workers of the cluster, so that even a source dominated by one
very large table benefits from a larger cluster.

## Which tables are split

The snapshot of an individual table is split across workers when all of the
following hold:

- The table has a **single-column primary key**. Composite primary keys are
  not supported.
- The primary key column is of type **`CHAR` or `VARCHAR`**, with a declared
  length of **at most 768 characters**. Other types, including numeric keys,
  are not supported.
- The primary key column uses the **`utf8mb4` character set** with the
  **`utf8mb4_bin` collation**.
- The table is **large enough to be worth splitting**. Small tables are read
  by a single worker, where splitting would add overhead without benefit.

How evenly the split lands also depends on the distribution of the key
values. See [How a table is partitioned](#how-a-table-is-partitioned).

Tables that don't meet these requirements, or whose boundary sampling fails
for any reason, still snapshot correctly: each is read in full by a single
worker, and different tables are still read concurrently.

## How a table is partitioned

Materialize partitions a table by the unique leading characters of its
primary keys. Before reading the table, it probes the primary key index to
discover key prefixes and uses the MySQL optimizer's row estimates to gauge
how many rows fall under each one, extending prefixes until it finds
boundaries that divide the table into roughly even ranges. The probes are
inexpensive point lookups, capped in proportion to the table's estimated
size, so this sampling phase stays negligible next to the snapshot itself.
Each worker then reads only its assigned range, within the same consistent
snapshot of the upstream database, so the result is identical to a
single-worker snapshot, only faster.

Because partitioning is based on key prefixes and optimizer estimates, how
evenly the work divides depends on the shape of your keys:

- **Evenly distributed keys partition well.** Keys whose leading characters
  spread rows uniformly, such as UUIDs, hashes, or other randomized
  identifiers, produce well-balanced ranges.

- **Skewed keys partition less evenly.** If a large share of the table's rows
  sort under a few common prefixes, some ranges end up with more rows than
  others, and the workers assigned to them finish later.

- **The probe budget can run out.** If finding even boundaries would require
  examining very many distinct prefixes, Materialize stops probing and uses
  the coarser boundaries found so far, which can also leave ranges uneven.

Uneven partitioning is never incorrect. It only reduces the speedup, since
the snapshot finishes when the busiest worker finishes.

## MySQL-specific upstream considerations

- **Connection count.** While the snapshot is being set up, Materialize
  briefly holds up to two connections per worker, plus one. Once reading is
  underway, this settles to one connection per worker reading a range, plus
  one coordination connection. After the snapshot completes, the source drops
  back to a single replication connection. If your MySQL server or connection
  pooler enforces a low
  [`max_connections`](https://dev.mysql.com/doc/refman/8.0/en/server-system-variables.html#sysvar_max_connections)
  limit, account for this burst when sizing it.

- **Statistics freshness.** Range boundaries are placed using the MySQL
  optimizer's row estimates. Stale statistics don't affect correctness, but
  can skew how evenly work divides across workers. Running
  [`ANALYZE TABLE`](https://dev.mysql.com/doc/refman/8.0/en/analyze-table.html)
  on very large tables before creating the source can improve balance.

For general guidance on read load, IOPS, and other upstream impact, which is
not specific to MySQL, see [Is the upstream database
overloaded?](/ingest-data/troubleshooting/#is-the-upstream-database-overloaded)

## Observability

The progress of an ongoing snapshot is visible in the
[`mz_internal.mz_source_statistics`](/reference/system-catalog/mz_internal/#mz_source_statistics)
system catalog view: `snapshot_records_known` is the estimated total size of
the snapshot and `snapshot_records_staged` is how much of it has been read so
far.

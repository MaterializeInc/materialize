---
headless: true
---

Materialize can parallelize snapshotting across the workers of the cluster
hosting the source.

- **PostgreSQL sources** are parallelized by table, i.e., different tables
  are read concurrently by different workers. On PostgreSQL 14 and later,
  Materialize additionally attempts to partition each table's read across
  workers. Tables that cannot be partitioned fall back to a single worker.

- **MySQL sources** are parallelized by table, i.e., different tables are
  read concurrently by different workers. For tables that meet certain
  requirements, Materialize can additionally partition the table's read
  across workers {{< private-preview-inline />}}. See [MySQL snapshot
  parallelism](/ingest-data/mysql/snapshot-parallelism/).

- **Kafka sources** are parallelized by topic partition, with partitions
  distributed across workers, so parallelism is bounded by the topic's
  partition count.

- **SQL Server sources** are not parallelized: a single worker reads all
  tables.

The degree of snapshot parallelism depends on the number of workers. A
cluster's [size](/sql/create-cluster/#available-sizes) determines its number
of workers, so a larger cluster can shorten the snapshot, to the extent the
work parallelizes and the upstream database keeps up. The volume read from
the upstream database is unchanged, it is compressed into a shorter window
of more concurrent queries and connections. To determine whether
snapshotting is overloading the upstream database, and for ways to mitigate
the load, see [Is the upstream database
overloaded?](/ingest-data/troubleshooting/#is-the-upstream-database-overloaded)

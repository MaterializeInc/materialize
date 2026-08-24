---
headless: true
---

Materialize parallelizes snapshotting across the workers of the cluster
hosting the source. For PostgreSQL and MySQL sources, work is distributed by
table, with different tables read concurrently by different workers.
PostgreSQL sources additionally partition every table, splitting its read
across workers (on PostgreSQL 14 and later). MySQL sources partition tables
that meet certain requirements. See [MySQL snapshot
parallelism](/ingest-data/mysql/snapshot-parallelism/). Kafka sources are
parallelized by topic partition, with partitions distributed across workers,
so parallelism is bounded by the topic's partition count. SQL Server sources
are not parallelized: a single worker reads all tables.

A cluster's [size](/sql/create-cluster/#available-sizes) determines its
number of workers, so a larger cluster shortens the snapshot. The volume
read from the upstream database is unchanged, it is compressed into a
shorter window of more concurrent queries and connections. To tell whether
the upstream database is struggling under this load, and for options if it
is, see [Is the upstream database
overloaded?](/ingest-data/troubleshooting/#is-the-upstream-database-overloaded)

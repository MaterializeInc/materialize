---
title: "Upsert and Debezium on DISK Replicas"
description: "How to size and manage large UPSERT and DEBEZIUM sources using DISK replicas."
menu:
  main:
    parent: "kafka"
    name: "Upsert and DEBEZIUM on `DISK` Replicas"
    weight: 11
---

{{< note >}}
This guide discusses a feature (namely, `DISK` replicas), that is currently only available as a preview. Please reach out to your Materialize
representative for more information.
{{</ note >}}

This guide goes through how to run and manage large `UPSERT` or `DEBEZIUM` Kafka sources on `DISK` replicas.

### Overview

Kafka sources that use `ENVELOPE UPSERT` or `ENVELOPE DEBEZIUM` require the entire _working set_ to be stored somewhere, so they
can produce retractions when keys are updated. The _working set_ is sum total of _current_ values for

By default, Materialize uses an in-memory map to store the _working set_. However, sources that are scheduled to run on clusters whose
replicas are configured to have an attached _disk_ (in reality its an ssd, but we call it a disk) using [_RocksDB_](https://rocksdb.org/).

Using `DISK` replicas for these sources can reduce memory usage, and allow sources to be scheduled on smaller and cheaper replicas.

### Sizing

Materialize tries to ensure that any set of sources whose _working set_'s fit on a replica's attached disk can successfully run, but there are 2
important parts of a source lifecycle that use additional resources, where care must be

#### Hydration

The first is _hydration_, when Materialize the large backlog of Kafka messages as it catches up with the end of the topic. Kafka sometimes
allows Materialize to consume messages faster than they can be arranged into a _working set_ in RocksDB. Instead, they will be buffered into
memory. In this case, a larger replica may temporarily be required

{{< note >}}
In the future this buffering limitation may be removed.
{{</ note >}}

To choose an initial size for _hydration_, please run:
```sql
select size, memory_bytes * processes / 1024 / 1024 / 1024 as disk_size_gib from mz_internal.mz_cluster_replica_sizes;
```
and choose the smallest size is _larger_ than your _estimated working set size_.

#### Rehydration

The second case is _rehydration_, when Materialize repopulates the _working set_ state in the replica when the replica is restarted.
This typically only happens during maintenance windows.

_Rehydration_ uses additional cpu and memory resources than steady-state processing. This is primarily due to processing within RocksDB itself,
as well as (bounded) buffering of data being processed. It's important to ensure that your chosen replica size is sufficient for _rehydration_,
and the below guide goes through this!

### Guide

1. #### Choose a Kafka guide

The [`DEBEZIUM`](/ingest-data/cdc-postgres-kafka-debezium/), [MSK](/ingest-data/amazon-msk/), [Confluent Cloud](/ingest-data/confluent-cloud/),
[Upstash](/ingest-data/upstash-kafka/), [Redpanda](/ingest-data/redpanda/), and [Redpanda Cloud](/ingest-data/redpanda-cloud/) guides are good starts.

2. #### Alter the final `CREATE SOURCE` command

Create a cluster with the _initial_ size chosen above, with a single replica:

```sql
CREATE CLUSTER src_cluster REPLICAS (r (SIZE '<chosen_size>', DISK));
```

and start the source in that cluster:

```sql
CREATE SOURCE src_name IN CLUSTER src_cluster ... -- Note that there is no `WITH (SIZE = ...)`
```

3. #### Wait for hydration

##### Waiting for hydration

{{< note >}}
In the future, additional tables in the `mz_catalog` table will simplify this process.
{{</ note >}}

You can periodically run:
```sql
EXPLAIN TIMESTAMP FOR SELECT * from src_name;
```

The output will look something like this:
```
                                 Timestamp
---------------------------------------------------------------------------
                 query timestamp: 1692134262972 (2023-08-15 21:17:42.972) +
           oracle read timestamp: 1692134262972 (2023-08-15 21:17:42.972) +
 largest not in advance of upper: 1692134264000 (2023-08-15 21:17:44.000) +
                           upper:[1692134264001 (2023-08-15 21:17:44.001)]+
                           since:[1692134262972 (2023-08-15 21:17:42.972)]+
         can respond immediately: true                                    +
                        timeline: Some(EpochMilliseconds)                 +
               session wall time: 1692134263145 (2023-08-15 21:17:43.145) +
                                                                          +
 source materialize.public.s (u4, storage):                               +
                   read frontier:[1692134262972 (2023-08-15 21:17:42.972)]+
                  write frontier:[1692134264001 (2023-08-15 21:17:44.001)]+
```

When the `query timestamp` and the `write frontier` of the source (at the bottom) are
equal or within 1 second, the source is caught up.

Additionally, you can run:
```sql
SELECT * from src_

##### Determining resource usage

The following query will give you roughly minute-granularity information about how much memory, cpu,
and disk space your source is using as it hydrates.
```sql
SELECT u.process_id, u.cpu_percent, u.memory_percent, u.disk_percent
  FROM mz_internal.mz_cluster_replica_utilization u
  JOIN mz_cluster_replicas cr ON cr.id = u.replica_id
  JOIN mz_clusters c ON c.id = cr.cluster_id
  WHERE c.name = 'src_cluster';
```

To view how much real user data we have ingested from your source (with minute granularity), use:
```sql
SELECT u.envelope_state_bytes / 1024 / 1024 / 1024 as ingested_working_set_gib FROM mz_sources s
  JOIN mz_internal.mz_source_statistics u ON s.id = u.id
  WHERE s.name = 'src_name';
```

If you have a rough estimate of the working-set size of your source, periodically running this query can give you an estimate
of how much progress Materialize has made ingesting your source.


##### Problems during hydration

You can use:
```sql
SELECT s.process_id, s.status, s.reason, s.updated_at
  FROM mz_internal.mz_cluster_replica_statuses s
  JOIN mz_cluster_replicas cr ON cr.id = s.replica_id
  JOIN mz_clusters c ON c.id = cr.cluster_id
  WHERE c.name = 'materialize_public_s'
```

to inspect the status of your replica. If you see problems, like an `oom-killed` (out of memory) reason, you may
need to adjust your replica size for hydration.


4. #### Choosing a size and testing rehydration

##### Choosing a size

After hydration is finished, check the final size of your working set using:
```sql
SELECT u.envelope_state_bytes / 1024 / 1024 / 1024 as ingested_working_set_gib FROM mz_sources s
  JOIN mz_internal.mz_source_statistics u ON s.id = u.id
  WHERE s.name = 'src_name';
```
again.

Compare this size to the sizes available to you:
```sql
select size, disk_bytes * processes / 1024 / 1024 / 1024 as disk_size_gib from mz_internal.mz_cluster_replica_sizes;
```

**The smallest size that fits your working set should work. However, larger sizes will rehydrate faster, and have more room for growth.**

##### Testing rehydration

You can run the following commands to recreate your replica with a different (smaller size):
```sql
DROP CLUSTER REPLICA src_cluster.r;
CREATE CLUSTER REPLICA src_cluster.r SIZE '<chosen size>', DISK;
```

This set of commands can be run as many times as you want while testing. Every time you do this, you can repeat step 3 to check
how long rehydration takes to perform. You can additionally check the status of your replica, to see if anything has gone wrong:

```sql
SELECT s.process_id, s.status, s.reason, s.updated_at
  FROM mz_internal.mz_cluster_replica_statuses s
  JOIN mz_cluster_replicas cr ON cr.id = s.replica_id
  JOIN mz_clusters c ON c.id = cr.cluster_id
  WHERE c.name = 'materialize_public_s'
```


### Working with multiple sources

Clusters backed by `DISK` replicas also work with more than 1 source, even if the sources have varied sizes. The process for hydrating
and rehydrating a replca

You can determine the individual sizes of all the sources with this query:
```sql
SELECT s.name, u.envelope_state_bytes / 1024 / 1024 / 1024 as ingested_working_set_gib FROM mz_sources s
  JOIN mz_internal.mz_source_statistics u ON s.id = u.id
  WHERE s.name IN ('src1', 'src2');
```

Or use this query to get a total size you can use to determine the replica size.

```sql
SELECT SUM(u.envelope_state_bytes) / 1024 / 1024 / 1024 as ingested_working_set_gib FROM mz_sources s
  JOIN mz_internal.mz_source_statistics u ON s.id = u.id
  WHERE s.name IN ('src1', 'src2');
```

We also recommend checking the status (described in [Testing rehydration](#testing-rehydration), as large numbers

---
title: "Choosing Sizes for Source Clusters"
description: "How to choose and test cluster replica sizes for sources."
menu:
  main:
    parent: "sizing"
    name: "Choosing Sizes for Source Clusters"
    weight: 1
---

{{< note >}}
This guide discusses limitations within Materialize that may be lifted in the future. It will be kept up to date.
{{</ note >}}

This guide goes through how to properly choose a size for clusters that are running sources.

## Basics

### Clusters vs replicas
As discussed [here](/sql/create-cluster/#conceptual-framework), _clusters_ are logical groupings of some number of _replicas_.
Currently, clusters that host sources can never have more than 1 replica. Because of this, this guide uses the word _cluster_ and _replica_
interchangeably, except when it matters.

Additionally this guide recommends and assumes the following:

- You host many sources on the same cluster, using `IN CLUSTER`, instead of `WITH (SIZE = <size>)`
- You are using [Managed clusters](/sql/create-cluster/#managed-clusters)

If you are using `WITH (SIZE = <size>)`, the sources will need to recreated on a new cluster, using `IN CLUSTER` instead. This guide
will help you choose the correct size for that cluster in [this section](#consolidating-existing-sources)

If you are not using managed clusters, we recommend you run:
```
ALTER CLUSTER <your_source_cluster_name> SET (MANAGED);
```
to change your cluster into a managed one.

### Available sizes
The following query will give the TOTAL number of cores, memory, and disk allocation for the various cluster replica sizes Materialize
offers. These are not guarantees, but they are good starting points for choosing and iterating on your source cluster configuration:
```sql
SELECT
  size,
  processes as number_of_processes,
  cpu_nano_cores * processes / 1000000000 as total_cpu_cores,
  memory_bytes * processes / 1024 / 1024 / 1024 as total_memory_gib,
  disk_bytes * processes / 1024 / 1024 / 1024 as total_disk_gib
FROM mz_internal.mz_cluster_replica_sizes
ORDER BY cpu_core_total ASC;
```

Note that replicas that are not [Disk-attached replicas](/sql/create-cluster-replica/#disk-attached-replicas) do not have any
disk available.

### Inspecting resource utilization
Understanding the resource utilization of your cluster is important when determining whether a specific size performs sufficiently well
for your workload. While this data is exposed in the `mz_internal.mz_cluster_replica_metrics` table, we recommend using the _clusters_ tab
in the Materialize Console. This will give you historical resource utilization.

{{< note >}}
Disk usage is not currently available on that tab, but will be added. You can use:
```sql
SELECT
  crs.process_id,
  crs.disk_percent
FROM mz_internal.mz_cluster_replica_utilization crs
  JOIN mz_cluster_replicas cr ON crs.replica_id = cr.id
  JOIN mz_clusters c ON c.id = cr.cluster_id
WHERE c.name = '<cluster_name>';
```
to get the current disk utilization for a replica in the meantime.
{{</ note >}}

This tab will also show you on the graph if a cluster replica has OOMed (out-of-memory'd). This is useful when determining whether or not
a specific size has enough memory to support your workload.

### Phases
There are 3 primary phases a source cluster can be in:

- _Hydration_: When first creating a source (i.e. running a `CREATE SOURCE` statement), Materialize must process historical data
    in the upstream source.
    - This is sometimes referred to as "snapshotting" or "reading a snapshot" for Postgres sources.
    - For Kafka sources, this involves reading the entire topic from the beginning of its retention period.
- _Rehydration_: During maintenance windows (or in some cases, errors that require restarting a cluster), Materialize may need to _rehydrate_
    all the data for sources on each cluster into some local state store.
    - This is done for Upsert and Debezium Kafka sources, but is skipped otherwise. See [Upsert-like sources](#upsert-like-sources) for
    more information.
- _Steady-state_: After _hydration_ or _rehydration_, Materialize processes new data as it appears in the upstream services.

_Hydration_ and _rehydration_ use more memory and CPU than _steady-state_, and are discussed below.

### Workloads
There are 2 primary workloads that sources currently fall under in Materialize:

- Upsert-like: `ENVELOPE UPSERT` and `ENVELOPE DEBEZIUM` Kafka sources.
- Append-only: all other sources, including Postgres sources.

These workloads perform significantly differently, and are covered in separate sections below.


## Upsert-like sources
Upsert and Debezium sources are required to store a _local_ version of ALL active key-value pairs the source's working set to work.
this is because when a key is _updated_, the source must be able to produce a [CDC](https://en.wikipedia.org/wiki/Change_data_capture)-style
_retraction_ for the previous value. This is a consequence of how data is stored in Materialize and is fundamental to making computations
on top of sources efficient.

The _local state_ for Upsert and Debezium sources defaults to in-memory, but [Disk-attached replicas](/sql/create-cluster-replica/#disk-attached-replicas)
move that state onto an attached disk. See (TODO: link to https://github.com/MaterializeInc/materialize/pull/21438 when its merged) for details on how to
configure that feature for clusters.

### Steady-state
In the steady-state, upsert-like sources require enough space in their _local state_ to store the entire _working set_. The _working set_
is the set of all active keys and their current values.

#### Determining your steady-state size
The first step in creating a source cluster for 1 or more upsert-like sources is to estimate the sum total _working set_ size for all
sources. This estimate does not have to be perfect.

After determining this estimate, choose the smallest size from [Available sizes](#available-sizes) whose `total_memory_gib` is larger than
the estimate (use `total_disk_gib` if using disk-attached replicas). This is your `steady_state_size`.

### Hydration and starting your sources
{{< note >}}
The limitation discussed in this section may be removed in the future.
{{</ note >}}

There is currently a limitation in Materialize where _hydration_ uses additional memory (even when using disk-attached replicas) then
both _steady-state_ and _rehydration_.

#### Determining your hydration size
The amount of memory required to hydrate a source is _roughly_ equal to the total size of its Kafka topic. You can total this size for
ALL sources you are going to run on your source cluster, and compare it to `total_memory_gib` in [Available sizes](#available-sizes) to
determine your `hydration_size`, which may be larger than your `steady_state_size`.

After doing so, create your cluster:
```sql
CREATE CLUSTER src_cluster MANAGED, REPLICATION FACTOR = 1, SIZE = <hydration_size>
```

#### Starting hydration
You can then run all your `CREATE SOURCE .. IN CLUSTER src_cluster` statements to start the hydration process for all your sources.

#### Waiting for hydration
There is currently not a good way of determining whether or not hydration is finished. You can run:
```sql
SELECT
    s.id,
    s.name,
    SUM(mss.bytes_received) AS bytes_received,
    SUM(mss.messages_received) AS messages_received,
    SUM(mss.updates_staged) AS updates_staged,
    SUM(mss.updates_committed) AS updates_committed
FROM mz_internal.mz_source_statistics mss
  JOIN mz_sources s ON s.id = mss.id
  JOIN mz_clusters c ON c.id = s.cluster_id
WHERE c.name = 'src_cluster'
GROUP BY s.id, s.name
```

To determine how many Kafka messages (and how many bytes worth) Materialize has read, `messages_received` and `bytes_received` respectively. However,
its currently not possible to easily determine whether or not _hydration_ is finished. You can wait for the memory usage of your cluster to reduce
and level-out using the guidance in [Inspecting resource utilization](#inspecting-resource-utilization).

{{< note >}}
In the future there may be additional system tables that simplify determining if hydration is finished.
{{</ note >}}

If you see OOM's happening, you may need to increase your `hydration_size`, and use:
```sql
ALTER CLUSTER src_cluster SET SIZE = <new_hydration_size>
```
or, wait for some sources to hydrate before `CREATE`-ing others.


### Shrinking and testing rehydration
After _hydration_ is finished, you may run:
```sql
ALTER CLUSTER src_cluster SET SIZE = <steady_state_size>
```

to shrink your cluster to its steady state size. This also ensures that your cluster can _rehydrate_ properly,
which typically uses more CPU and memory than _steady_state_ (even for disk-attached replicas). You can
use [Inspecting resource utilization](#inspecting-resource-utilization) to see the resource utilization of
_rehydration_, and use:
```sql
SELECT
  bool_and(mss.rehydration_finished IS NOT NULL)
FROM mz_internal.mz_source_statistics mss
  JOIN mz_sources s ON s.id = mss.id
  JOIN mz_clusters c ON c.id = s.cluster_id
WHERE c.name = 'src_cluster';
```
to determine when the process is finished.


{{< note >}}
If your `hydration_size` and `steady_state_size` are the same, we still recommend you test _rehydration_, by running the following:
```sql
ALTER CLUSTER src_cluster SET (REPLICATION FACTOR = 0);
ALTER CLUSTER src_cluster SET (REPLICATION FACTOR = 1);
```
{{</ note >}}

### Inspecting your working set
You can run:
```sql
SELECT
  SUM(mss.envelope_state_count) ingested_working_set_total_keys
  SUM(mss.envelope_state_bytes) / 1024 / 1024 / 1024 as ingested_working_set_gib
FROM mz_internal.mz_source_statistics mss
  JOIN mz_sources s ON s.id = mss.id
  JOIN mz_clusters c ON c.id = s.cluster_id
WHERE c.name = '<your cluster name>';
```

to determine the total number of keys and size of the _working sets_ of your upsert-like sources. If you want it broken
out by source, run this instead:
```sql
SELECT
  s.id,
  s.name,
  SUM(mss.envelope_state_count) ingested_working_set_total_keys
  SUM(mss.envelope_state_bytes) / 1024 / 1024 / 1024 as ingested_working_set_gib
FROM mz_internal.mz_source_statistics mss
  JOIN mz_sources s ON s.id = mss.id
  JOIN mz_clusters c ON c.id = s.cluster_id
WHERE c.name = '<your cluster name>'
GROUP BY s.id, s.name;
```


## Append-only sources
Postgres sources and non-Upsert/Debezium Kafka sources are _append-only_, which means they _rehydrate_ effectively immediately.

### Steady-state
In the steady-state, append-only sources need enough CPU and memory to keep up with the upstream service.

#### Determining your steady-state size
It is hard to estimate the minimal size required for a cluster of append-only sources. For "small" postgres instances and Kafka topics,
it is reasonable to start with the smallest size available in [Available sizes](#available-sizes) for each source. When running more than one
source, multiply the `total_cpu_cores` by the number of sources, and choose a size that has that many cores (or more).

For larger Postgres instances, you may want to start with the number of cores used by the Postgres instance itself.
For large Kafka topics, you may want to use the same number of cores used by another consumer of the topic.
If you are unsure if your instances/topics are large or small, assume they are small for now!

The size chosen in this section is your `steady_state_size`.

### Hydration and starting your sources
{{< note >}}
The limitation discussed in this section may be removed in the future.
{{</ note >}}

There is currently a limitation in Materialize where _hydration_ uses additional memory than _steady-state_.

#### Determining your hydration size
The amount of memory required to hydrate a source is _roughly_ equal to the total size of the postgres instance/Kafka topic. You can total this size for
ALL sources you are going to run on your source cluster, and compare it to `total_memory_gib` in [Available sizes](#available-sizes) to
determine your `hydration_size`, which may be larger than your `steady_state_size`. If this calculated `hydration_size` is prohibitively large, you
can try a smaller size and see if it works in the next section.

After doing so, create your cluster:
```sql
CREATE CLUSTER src_cluster MANAGED, REPLICATION FACTOR = 1, SIZE = <hydration_size>
```

#### Starting hydration
You can then run all your `CREATE SOURCE .. IN CLUSTER src_cluster` statements to start the hydration process for all your sources.

#### Waiting for hydration
There is currently not a good way of determining whether or not hydration for Kafka sources is finished. You can run:
```sql
SELECT
    s.id,
    s.name,
    SUM(mss.bytes_received) AS bytes_received,
    SUM(mss.messages_received) AS messages_received,
    SUM(mss.updates_staged) AS updates_staged,
    SUM(mss.updates_committed) AS updates_committed
FROM mz_internal.mz_source_statistics mss
  JOIN mz_sources s ON s.id = mss.id
  JOIN mz_clusters c ON c.id = s.cluster_id
WHERE c.name = 'src_cluster'
GROUP BY s.id, s.name
```

To determine how many Kafka messages (and how many bytes worth) Materialize has read, `messages_received` and `bytes_received`
respectively. However, its currently not possible to easily determine whether or not _hydration_ is finished. You can wait for the memory usage
of your cluster to reduce and level-out using the guidance in [Inspecting resource utilization](#inspecting-resource-utilization).

For Postgres sources, you can use the guidance [here](/manage/troubleshooting/#has-my-source-ingested-its-initial-snapshot) to determine
if _hydration_ is finished.

When _hydrating_ your sources on your source cluster, you will want to use the guidance in [Inspecting resource utilization](#inspecting-resource-utilization)
to ensure your cluster is not OOMing.

{{< note >}}
In the future there may be additional system tables that simplify determining if hydration is finished.
{{</ note >}}

If you see OOM's happening, you may need to increase your `hydration_size`, and use:
```sql
ALTER CLUSTER src_cluster SET SIZE = <new_hydration_size>
```
or, wait for some sources to hydrate before `CREATE`-ing others.


### Shrinking and testing steady-state
After _hydration_ is finished, you may run:
```sql
ALTER CLUSTER src_cluster SET SIZE = <steady_state_size>
```

Additionally, you can view the CPU and memory utilization graphs described in [Inspecting resource utilization](#inspecting-resource-utilization)
to determine how much you can reduce the CPU and memory size of your cluster. In general, append-only sources can be shrunk to a size
that fits their _steady-state_ CPU and memory utilization.

{{< note >}}
In the future there may be additional system tables that clarify if a source is keeping up with its upstream service.
{{</ note >}}


## Mixing workloads
If you wish to run sources with different style workloads (upsert-like vs. append-only) on the same cluster, the above advice should remain
relevant. A couple extra pieces of advice:

- When determining your initial `steady_state_size` and `hydration_size`, you will need estimate the resource utilization of
[upsert-like](#steady-state) and [append-only](#steady-state-1) separately, and sum them.
- Because you have at least one upsert-like workload, you may need to adjust your cluster's size during _hydration_, as
[discussed here](#hydration-and-starting-your-sources).
- We recommend explicitly testing _rehydration_, as [discussed here](#shrinking-and-testing-rehydration)

## Consolidating existing sources
If you have existing sources scheduled on separate clusters (or are using `WITH (SIZE = <size>)` on your sources), and wish to consolidate them on
a single cluster, the advice in this guide remains relevant. However, its easier to determine the `steady_state_size` and `hydration_size` of your various sources.

- For append-only sources, you can use the console to determine the actual memory and CPU utilization of each source,
following the guidance [here](#inspecting-resource-utilization), to determine your `steady_state_size`.
- For upsert-like sources, you can run the following to determine the actual size of your working set, which can be used to determine how much memory
(or disk space for disk-attached replicas) you need:
```sql
SELECT
  SUM(mss.envelope_state_bytes) / 1024 / 1024 / 1024 as ingested_working_set_gib
FROM mz_internal.mz_source_statistics mss
  JOIN mz_sources s ON s.id = mss.id
WHERE s.name = '<your source name>';
```
This can be used to determined your `steady_state_size`, as discussed [here](#determining-your-steady-state-size).
- Your `hydration_size` can be determined by totaling the sizes used when you first hydrated these existing sources in the first place.

Once these sizes are chosen, you can use `DROP SOURCE <source_name>` to tear down the existing sources, and recreate them in your source cluster,
using `CREATE SOURCE ... IN CLUSTER src_cluster ...`. `SHOW CREATE SOURCE <source_name>` can be used to recover the DDL used to create your source,
if need be.

{{< note >}}
In the future there may be additional `ALTER SOURCE` commands that simplify this process.
{{</ note >}}

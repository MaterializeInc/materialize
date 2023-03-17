---
title: "mz_internal"
description: "mz_internal is a system catalog schema which exposes internal metadata about Materialize. This schema is not part of Materialize's stable interface."
menu:
  main:
    parent: 'system-catalog'
    weight: 4
---

The following sections describe the available objects in the `mz_internal`
schema.

{{< warning >}}
The objects in the `mz_internal` schema are not part of Materialize's stable interface.
Backwards-incompatible changes to these tables may be made at any time.
{{< /warning >}}

{{< warning >}}
`SELECT` statements may reference these objects, but creating views that
reference these objects is not allowed.
{{< /warning >}}

### `mz_arrangement_sharing`

The `mz_arrangement_sharing` view describes how many times each [arrangement]
in the system is used.

Field         | Type       | Meaning
--------------|------------|--------
`operator_id` | [`bigint`] | The ID of the operator that created the arrangement. Corresponds to [`mz_dataflow_operators.id`](#mz_dataflow_operators).
`worker_id`   | [`bigint`] | The ID of the worker thread hosting the arrangement.
`count`       | [`bigint`] | The number of operators that share the arrangement.

### `mz_arrangement_sizes`

The `mz_arrangement_sizes` view describes the size of each [arrangement] in
the system.

Field         | Type       | Meaning
--------------|------------|--------
`operator_id` | [`bigint`] | The ID of the operator that created the arrangement. Corresponds to [`mz_dataflow_operators.id`](#mz_dataflow_operators).
`worker_id`   | [`bigint`] | The ID of the worker thread hosting the arrangement.
`records`     | [`bigint`] | The number of records in the arrangement.
`batches`     | [`bigint`] | The number of batches in the arrangement.

### `mz_cluster_replica_metrics`

The `mz_cluster_replica_metrics` table gives the last known CPU and RAM utilization statistics
for all processes of all extant cluster replicas.

At this time, we do not make any guarantees about the exactness or freshness of these numbers.

Field              | Type       | Meaning
-------------------|------------|--------
`replica_id`       | [`bigint`] | The ID of a cluster replica.
`process_id`       | [`bigint`] | An identifier of a compute process within a replica.
`cpu_nano_cores`   | [`bigint`] | Approximate CPU usage, in billionths of a vCPU core.
`memory_bytes`     | [`bigint`] | Approximate RAM usage, in bytes.

### `mz_cluster_replica_sizes`

The `mz_cluster_replica_sizes` table contains a mapping of logical sizes
(e.g. "xlarge") to physical sizes (number of processes, and CPU and memory allocations per process).

{{< warning >}}
The values in this table may change at any time, and users should not rely on
them for any kind of capacity planning.
{{< /warning >}}

| Field            | Type      | Meaning                                                       |
|------------------|-----------|---------------------------------------------------------------|
| `size`           | [`text`]  | The human-readable replica size.                              |
| `processes`      | [`uint8`] | The number of processes in the replica.                       |
| `workers`        | [`uint8`] | The number of Timely Dataflow workers per process.            |
| `cpu_nano_cores` | [`uint8`] | The CPU allocation per process, in billionths of a vCPU core. |
| `memory_bytes`   | [`uint8`] | The RAM allocation per process, in billionths of a vCPU core. |


### `mz_cluster_links`

The `mz_cluster_links` table contains a row for each cluster that is linked to a
source or sink. When present, the lifetime of the specified cluster is tied to
the lifetime of the specified source or sink: the cluster cannot be dropped
without dropping the linked source or sink, and dropping the linked source or
sink will also drop the cluster. There is at most one row per cluster.

{{< note >}}
The concept of a linked cluster is not user-facing, and is intentionally undocumented. Linked clusters are meant to preserve the soon-to-be legacy interface for sizing sources and sinks.
{{< /note >}}

| Field            | Type      | Meaning                                                       |
|------------------|-----------|---------------------------------------------------------------|
| `cluster_id`     | [`text`]  | The ID of the cluster. Corresponds to [`mz_clusters.id`](/sql/system-catalog/mz_catalog/#mz_clusters).  |
| `object_id`      | [`text`]  | The ID of the source or sink. Corresponds to [`mz_objects.id`](/sql/system-catalog/mz_catalog/#mz_clusters).  |



### `mz_cluster_replica_statuses`

The `mz_cluster_replica_statuses` table contains a row describing the status
of each process in each cluster replica in the system.

Field               | Type                          | Meaning
--------------------|-------------------------------|--------
`replica_id`        | [`uint8`]                     | Materialize's unique ID for the cluster replica.
`process_id`        | [`uint8`]                     | The ID of the process within the cluster replica.
`status`            | [`text`]                      | The status of the cluster replica: `ready` or `not-ready`.
`updated_at`        | [`timestamp with time zone`]  | The time at which the status was last updated.

### `mz_cluster_replica_utilization`

The `mz_cluster_replica_utilization` view gives the last known CPU and RAM utilization statistics
for all processes of all extant cluster replicas, as a percentage of the total resource allocation.

At this time, we do not make any guarantees about the exactness or freshness of these numbers.

| Field            | Type      | Meaning                                                    |
|------------------|-----------|------------------------------------------------------------|
| `replica_id`     | [`uint8`] | The ID of a cluster replica.                               |
| `process_id`     | [`uint8`] | An identifier of a compute process within a replica.       |
| `cpu_percent`    | [`uint8`] | Approximate CPU usage, in percent of the total allocation. |
| `memory_percent` | [`uint8`] | Approximate RAM usage, in percent of the total allocation. |

### `mz_dataflows`

The `mz_dataflows` view describes the [dataflows][dataflow] in the system.

Field      | Type       | Meaning
-----------|------------|--------
`id`       | [`bigint`] | The ID of the dataflow.
`worker_id`| [`bigint`] | The ID of the worker thread hosting the dataflow.
`local_id` | [`bigint`] | The scope-local index of the dataflow.
`name`     | [`text`]   | The internal name of the dataflow.

### `mz_dataflow_addresses`

The `mz_dataflow_addresses` source describes how the dataflow channels
and operators in the system are nested into scopes.

Field       | Type            | Meaning
------------|-----------------|--------
`id`        | [`bigint`]      | The ID of the channel or operator. Corresponds to [`mz_dataflow_channels.id`](#mz_dataflow_channels) or [`mz_dataflow_operators.id`](#mz_dataflow_operators).
`worker_id` | [`bigint`]      | The ID of the worker thread hosting the channel or operator.
`address`   | [`bigint list`] | A list of scope-local indexes indicating the path from the root to this channel or operator.

### `mz_dataflow_operator_parents`

The `mz_dataflow_operator_parents` view describes how operators are
nested into scopes, by relating operators to their parent operators.

| Field       | Type       | Meaning                                                                                     |
|-------------|------------|---------------------------------------------------------------------------------------------|
| `id`        | [`bigint`] | The ID of the operator. Corresponds to [`mz_dataflow_operators.id`](#mz_dataflow_operators) |
| `parent_id` | [`bigint`] | The ID of the operator's parent operator.                                                   |
| `worker_id` | [`bigint`] | The ID of the worker thread hosting the operators.                                          |

### `mz_dataflow_channels`

The `mz_dataflow_channels` source describes the communication channels between
[dataflow] operators. A communication channel connects one of the outputs of a
source operator to one of the inputs of a target operator.

Field           | Type       | Meaning
----------------|------------|--------
`id`            | [`bigint`] | The ID of the channel.
`worker_id`     | [`bigint`] | The ID of the worker thread hosting the channel.
`from_index`    | [`bigint`] | The scope-local index of the source operator. Corresponds to an address in [`mz_dataflow_addresses.address`](#mz_dataflow_addresses).
`from_port`      | [`bigint`] | The source operator's output port.
`to_index`      | [`bigint`] | The scope-local index of the target operator. Corresponds to an address in [`mz_dataflow_addresses.address`](#mz_dataflow_addresses).
`to_port`       | [`bigint`] | The target operator's input port.

### `mz_dataflow_channel_operators`

The `mz_dataflow_channel_operators` view associates
[channels](#mz_dataflow_channels) with the operators that are their endpoints.

| Field              | Type       | Meaning                                          |
|--------------------|------------|--------------------------------------------------|
| `id`               | [`bigint`] | The ID of the channel.                           |
| `worker_id`        | [`bigint`] | The ID of the worker thread hosting the channel. |
| `from_operator_id` | [`bigint`] | The ID of the source of the channel.             |
| `to_operator_id`   | [`bigint`] | The ID of the target of the channel.             |


### `mz_dataflow_operators`

The `mz_dataflow_operators` source describes the dataflow operators in the
system.

Field       | Type       | Meaning
------------|------------|--------
`id`        | [`bigint`] | The ID of the operator.
`worker_id` | [`bigint`] | The ID of the worker thread hosting the operator.
`name`      | [`text`]   | The name of the operator.

### `mz_dataflow_operator_dataflows`

The `mz_dataflow_operator_dataflows` view describes the [dataflow] to which each
dataflow operator belongs.

Field           | Type       | Meaning
----------------|------------|--------
`id`            | [`bigint`] | The ID of the operator.
`name`          | [`text`]   | The internal name of the operator.
`worker_id`     | [`bigint`] | The ID of the worker thread hosting the operator.
`dataflow_id`   | [`bigint`] | The ID of the dataflow hosting the operator.
`dataflow_name` | [`text`]   | The name of the dataflow hosting the operator.

### `mz_compute_exports`

The `mz_compute_exports` source describes the dataflows created by indexes, materialized views, and subscriptions in the system.

Field         | Type       | Meaning
--------------|------------|--------
`export_id`   | [`text`]   | The ID of the index, materialized view, or subscription that created the dataflow. Corresponds to [`mz_catalog.mz_indexes.id`](../mz_catalog#mz_indexes), [`mz_catalog.mz_materialized_views.id`](../mz_catalog#mz_materialized_views), or [`mz_internal.mz_subscriptions`](#mz_subscriptions).
`worker_id`   | [`bigint`] | The ID of the worker thread hosting the corresponding [dataflow].
`dataflow_id` | [`bigint`] | The ID of the [dataflow]. Corresponds to [`mz_dataflows.local_id`](#mz_dataflows).

### `mz_compute_frontiers`

The `mz_compute_frontiers` view describes the frontier for each
[dataflow] in the system across all workers. The frontier describes the earliest
timestamp at which the output of the dataflow may change; data prior to that
timestamp is sealed.

For per-worker frontier information, see
[`mz_worker_compute_frontiers`](#mz_worker_compute_frontiers).

Field        | Type       | Meaning
-------------|------------|--------
`export_id ` | [`text`]   | The ID of the index, materialized view, or subscription that created the dataflow. Corresponds to [`mz_compute_exports.export_id`](#mz_compute_exports).
`time`       | [`mz_timestamp`] | The next timestamp at which the output may change.

### `mz_cluster_replica_frontiers`

The `mz_cluster_replica_frontiers` table describes the frontiers of each [dataflow] in the system.
[`mz_compute_frontiers`](#mz_compute_frontiers) is similar to this table, but `mz_compute_frontiers`
exposes the frontiers known to the compute replicas while `mz_cluster_replica_frontiers` contains
the frontiers the coordinator is aware of.

At this time, we do not make any guarantees about the freshness of these numbers.

Field        | Type       | Meaning
-------------|------------|--------
`replica_id` | [`bigint`] | The ID of a cluster replica.
`export_id ` | [`text`]   | The ID of the index, materialized view, or subscription that created the dataflow. Corresponds to [`mz_compute_exports.export_id`](#mz_compute_exports).
`time`       | [`mz_timestamp`] | The next timestamp at which the output may change.

### `mz_compute_import_frontiers`

The `mz_compute_import_frontiers` view describes the frontiers for every
source object used in a [dataflow] in the system across all workers. The frontier
describes the earliest timestamp at which the output of the source instantiation
at the dataflow layer may change; data prior to that timestamp is sealed.

For per-worker frontier information, see
[`mz_worker_compute_import_frontiers`](#mz_worker_compute_import_frontiers).

Field       | Type       | Meaning
------------|------------|--------
`export_id` | [`text`]   | The ID of the index, materialized view, or subscription that created the dataflow. Corresponds to [`mz_compute_exports.export_id`](#mz_compute_exports).
`import_id` | [`text`]   | The ID of the input source object for the dataflow. Corresponds to either [`mz_catalog.mz_sources.id`](../mz_catalog#mz_sources) or [`mz_catalog.mz_tables.id`](../mz_catalog#mz_tables) or [`mz_compute_exports.export_id`](#mz_compute_exports).
`time`      | [`mz_timestamp`] | The next timestamp at which the source instantiation may change.

### `mz_message_counts`

The `mz_message_counts` view describes the messages sent and received over the
dataflow channels in the system.

Field             | Type       | Meaning
------------------|------------|--------
`channel_id`      | [`bigint`] | The ID of the channel. Corresponds to [`mz_dataflow_channels.id`](#mz_dataflow_channels).
`from_worker_id`  | [`bigint`] | The ID of the worker thread sending the message.
`to_worker_id`    | [`bigint`] | The ID of the worker thread receiving the message.
`sent`            | [`bigint`] | The number of messages sent.
`received`        | [`bigint`] | The number of messages received.

### `mz_active_peeks`

The `mz_active_peeks` source describes all read queries ("peeks") that are
pending in the dataflow layer.

Field      | Type       | Meaning
-----------|------------|--------
`id`       | [`uuid`]   | The ID of the peek request.
`worker_id`| [`bigint`] | The ID of the worker thread servicing the peek.
`index_id` | [`text`]   | The ID of the index the peek is targeting.
`time`     | [`mz_timestamp`] | The timestamp the peek has requested.

### `mz_subscriptions`

The `mz_subscriptions` table describes all active [`SUBSCRIBE`](/sql/subscribe)
operations in the system.

Field                   | Type                         | Meaning
------------------------|------------------------------|--------
`id`                    | [`text`]                     | The ID of the subscription.
`user`                  | [`text`]                     | The user who started the subscription.
`cluster_id`            | [`text`]                     | The ID of the cluster on which the subscription is running. Corresponds to [`mz_clusters.id`](../mz_catalog/#mz_clusters).
`created_at`            | [`timestamp with time zone`] | The time at which the subscription was created.
`referenced_object_ids` | [`text list`]                | The IDs of objects referenced by the subscription. Corresponds to [`mz_objects.id`](../mz_catalog/#mz_objects)

### `mz_object_dependencies`

The `mz_object_dependencies` table describes the dependency structure between
all database objects in the system.

Field                  | Type       | Meaning
-----------------------|------------|--------
`object_id`            | [`text`]   | The ID of the dependent object. Corresponds to [`mz_objects.id`](../mz_catalog/#mz_objects).
`referenced_object_id` | [`text`]   | The ID of the referenced object. Corresponds to [`mz_objects.id`](../mz_catalog/#mz_objects).

### `mz_peek_durations_histogram`

The `mz_peek_durations_histogram` view describes a histogram of the duration in
nanoseconds of read queries ("peeks") in the dataflow layer.

Field         | Type       | Meaning
--------------|------------|--------
`worker_id`   | [`bigint`] | The ID of the worker thread servicing the peek.
`duration_ns` | [`bigint`] | The upper bound of the bucket in nanoseconds.
`count`       | [`bigint`] | The (noncumulative) count of peeks in this bucket.

### `mz_postgres_sources`

The `mz_postgres_sources` table contains a row for each PostgreSQL source in the
system.

Field              | Type           | Meaning
-------------------|----------------|--------
`id`               | [`text`]       | The ID of the source. Corresponds to [`mz_catalog.mz_sources.id`](../mz_catalog#mz_sources).
`replication_slot` | [`text`]       | The name of the replication slot in the PostgreSQL database that Materialize will create and stream data from.

### `mz_records_per_dataflow`

The `mz_records_per_dataflow` view describes the number of records in each
[dataflow] on each worker in the system.

For the same information aggregated across all workers, see
[`mz_records_per_dataflow_global`](#mz_records_per_dataflow_global).

Field       | Type        | Meaning
------------|-------------|--------
`id`        | [`bigint`]  | The ID of the dataflow. Corresponds to [`mz_dataflows.id`](#mz_dataflows).
`name`      | [`text`]    | The internal name of the dataflow.
`worker_id` | [`bigint`]  | The ID of the worker thread hosting the dataflow.
`records`   | [`numeric`] | The number of records in the dataflow.

### `mz_records_per_dataflow_global`

The `mz_records_per_dataflow_global` view describes the number of records in each
[dataflow] in the system.

For the same information broken down across workers, see
[`mz_records_per_dataflow`](#mz_records_per_dataflow).

Field     | Type        | Meaning
----------|-------------|--------
`id`      | [`bigint`]  | The ID of the dataflow. Corresponds to [`mz_dataflows.id`](#mz_dataflows).
`name`    | [`text`]    | The internal name of the dataflow.
`records` | [`numeric`] | The number of records in the dataflow.

### `mz_records_per_dataflow_operator`

The `mz_records_per_dataflow_operator` view describes the number of records in
each [dataflow] operator in the system.

Field         | Type        | Meaning
--------------|-------------|--------
`id`          | [`bigint`]  | The ID of the operator. Corresponds to [`mz_dataflow_operators.id`](#mz_dataflow_operators).
`name`        | [`text`]    | The internal name of the dataflow.
`worker_id`   | [`bigint`]  | The ID of the worker thread hosting the dataflow.
`dataflow_id` | [`bigint`]  | The ID of the dataflow. Corresponds to [`mz_dataflows.id`](#mz_dataflows).
`records`     | [`numeric`] | The number of records in the dataflow.

### `mz_scheduling_elapsed`

The `mz_scheduling_elapsed` view describes the total amount of time spent in
each [dataflow] operator.

Field        | Type       | Meaning
-------------|------------|--------
`id`         | [`bigint`] | The ID of the operator. Corresponds to [`mz_dataflow_operators.id`](#mz_dataflow_operators).
`worker_id`  | [`bigint`] | The ID of the worker thread hosting the operator.
`elapsed_ns` | [`bigint`] | The total elapsed time spent in the operator in nanoseconds.

### `mz_compute_operator_durations_histogram`

The `mz_compute_operator_durations_histogram` view describes a histogram of the
duration in nanoseconds of each invocation for each [dataflow] operator.

Field         | Type       | Meaning
--------------|------------|--------
`id`          | [`bigint`] | The ID of the operator. Corresponds to [`mz_dataflow_operators.id`](#mz_dataflow_operators).
`worker_id`   | [`bigint`] | The ID of the worker thread hosting the operator.
`duration_ns` | [`bigint`] | The upper bound of the duration bucket in nanoseconds.
`count`       | [`bigint`] | The number of recordings in the bucket.

### `mz_scheduling_parks_histogram`

The `mz_scheduling_parks_histogram` view describes a histogram of [dataflow] worker
park events. A park event occurs when a worker has no outstanding work.

Field          | Type       | Meaning
---------------|------------| -------
`worker_id`    | [`bigint`] | The ID of the worker thread.
`slept_for_ns` | [`bigint`] | The actual length of the park event in nanoseconds.
`requested_ns` | [`bigint`] | The requested length of the park event in nanoseconds.
`count`        | [`bigint`] | The number of park events in this bucket.

### `mz_worker_compute_delays_histogram`

The `mz_worker_compute_delays_histogram` view describes
a histogram of wall-clock delay in nanoseconds between observations of
source object frontier advancements at the dataflow layer and the
advancements of the corresponding [dataflow] frontiers.

Field       | Type       | Meaning
------------|------------|--------
`export_id` | [`text`]   | The ID of the index, materialized view, or subscription that created the dataflow. Corresponds to [`mz_compute_exports.export_id`](#mz_compute_exports).
`import_id` | [`text`]   | The ID of the input source object for the dataflow. Corresponds to either [`mz_catalog.mz_sources.id`](../mz_catalog#mz_sources) or [`mz_catalog.mz_tables.id`](../mz_catalog#mz_tables) or [`mz_catalog.mz_materialized_views.id`](../mz_catalog#mz_materialized_views).
`worker_id` | [`bigint`] | The ID of the worker thread hosting the dataflow.
`delay_ns`  | [`bigint`] | The upper bound of the bucket in nanoseconds.
`count`     | [`bigint`] | The (noncumulative) count of delay measurements in this bucket.

### `mz_worker_compute_dependencies`

The `mz_worker_compute_dependencies` source describes the dependency structure
between each [dataflow] and the sources of their data. To create a complete dependency
structure, the `import_id` column includes other dataflows.

Field      | Type       | Meaning
-----------|------------|--------
`export_id`| [`text`]   | The ID of the object that created the dataflow. Corresponds to [`mz_compute_exports.export_id`](#mz_compute_exports).
`import_id`| [`text`]   | The ID of the source object for the dataflow. Corresponds to [`mz_catalog.mz_sources.id`](../mz_catalog#mz_sources) or [`mz_catalog.mz_tables.id`](../mz_catalog#mz_tables) or [`mz_compute_exports.export_id`](#mz_compute_exports).
`worker_id`| [`bigint`] | The ID of the worker thread hosting the dataflow.

### `mz_worker_compute_frontiers`

The `mz_worker_compute_frontiers` source describes each worker's
frontier for each [dataflow] in the system. The frontier describes the earliest
timestamp at which the output of the dataflow may change; data prior to that
timestamp is sealed.

For frontier information aggregated across all workers, see
[`mz_compute_frontiers`](#mz_compute_frontiers).

Field       | Type       | Meaning
------------|------------|--------
`export_id` | [`text`]   | The ID of the index, materialized view, or subscription that created the dataflow. Corresponds to [`mz_compute_exports.export_id`](#mz_compute_exports).
`worker_id` | [`bigint`] | The ID of the worker thread hosting the dataflow.
`time`      | [`mz_timestamp`] | The next timestamp at which the dataflow may change.

### `mz_worker_compute_import_frontiers`

The `mz_worker_compute_import_frontiers` source describes the frontiers that
each worker is aware of for every source object used in a [dataflow] in the system. The
frontier describes the earliest timestamp at which the output of the source instantiation
at the dataflow layer may change; data prior to that timestamp is sealed.

For frontier information aggregated across all workers, see
[`mz_compute_import_frontiers`](#mz_compute_import_frontiers).

Field       | Type       | Meaning
------------|------------|--------
`export_id` | [`text`]   | The ID of the index, materialized view, or subscription that created the dataflow. Corresponds to [`mz_compute_exports.export_id`](#mz_compute_exports).
`source_id` | [`text`]   | The ID of the input source object for the dataflow. Corresponds to either [`mz_catalog.mz_sources.id`](../mz_catalog#mz_sources) or [`mz_catalog.mz_tables.id`](../mz_catalog#mz_tables) or [`mz_compute_exports.export_id`](#mz_compute_exports).
`worker_id` | [`bigint`] | The ID of the worker thread hosting the dataflow.
`time`      | [`mz_timestamp`] | The next timestamp at which the dataflow may change.

### `mz_source_statistics`

The `mz_source_statistics` table contains statistics for each worker thread of
each source in the system.

Materialize does not make any guarantees about the exactness or freshness of
these statistics. They are occasionally reset to zero as internal components of
the system are restarted.

Field                 | Type         | Meaning
----------------------|--------------|--------
`id`                  | [`text`]     | The ID of the source. Corresponds to [`mz_catalog.mz_sources.id`](../mz_catalog#mz_sources).
`worker_id`           | [`bigint`]   | The ID of the worker thread.
`snapshot_committed`  | [`boolean`]  | Whether the worker has committed the initial snapshot for a source.
`messages_received`   | [`bigint`]   | The number of messages the worker has received from the external system. Messages are counted in a source type-specific manner. Messages do not correspond directly to updates: some messages produce multiple updates, while other messages may be coalesced into a single update.
`updates_staged`      | [`bigint`]   | The number of updates (insertions plus deletions) the worker has written but not yet committed to the storage layer.
`updates_committed`   | [`bigint`]   | The number of updates (insertions plus deletions) the worker has committed to the storage layer.
`bytes_received`      | [`bigint`]   | The number of bytes the worker has read from the external system. Bytes are counted in a source type-specific manner and may or may not include protocol overhead.

### `mz_sink_statistics`

The `mz_sink_statistics` table contains statistics for each worker thread of
each sink in the system.

Materialize does not make any guarantees about the exactness or freshness of
these statistics. They are occasionally reset to zero as internal components of
the system are restarted.

Field                 | Type         | Meaning
----------------------|--------------|--------
`id`                  | [`text`]     | The ID of the source. Corresponds to [`mz_catalog.mz_sources.id`](../mz_catalog#mz_sources).
`worker_id`           | [`bigint`]   | The ID of the worker thread.
`messages_staged`     | [`bigint`]   | The number of messages staged but possibly not committed to the sink.
`messaged_commited`   | [`bigint`]   | The number of messages committed to the sink.
`bytes_staged`        | [`bigint`]   | The number of bytes staged but possibly not committed to the sink. This counts both keys and values, if applicable.
`bytes_committed`     | [`bigint`]   | The number of bytes committed to the sink. This counts both keys and values, if applicable.

### `mz_source_statuses`

The `mz_source_statuses` view provides the current state for each source in the
system, including potential error messages and additional metadata helpful for
debugging.

Field                   | Type                          | Meaning
------------------------|-------------------------------|--------
`id`                    | [`text`]                      | The ID of the source. Corresponds to [`mz_catalog.mz_sources.id`](../mz_catalog#mz_sources).
`name`                  | [`text`]                      | The name of the source.
`type`                  | [`text`]                      | The type of the source.
`last_status_change_at` | [`timestamp with time zone`]  | Wall-clock timestamp of the source status change.
`status`                | [`text`]                      | The status of the source: one of `created`, `starting`, `running`, `stalled`, `failed`, or `dropped`.
`error`                 | [`text`]                      | If the source is in an error state, the error message.
`details`               | [`jsonb`]                     | Additional metadata provided by the source. In case of error, may contain a `hint` field with helpful suggestions.

### `mz_source_status_history`

The `mz_source_status_history` table contains a row describing the status of the
historical state for each source in the system, including potential error
messages and additional metadata helpful for debugging.

Field         | Type                          | Meaning
--------------|-------------------------------|--------
`occurred_at` | [`timestamp with time zone`]  | Wall-clock timestamp of the source status change.
`source_id`   | [`text`]                      | The ID of the source. Corresponds to [`mz_catalog.mz_sources.id`](../mz_catalog#mz_sources).
`status`      | [`text`]                      | The status of the source: one of `created`, `starting`, `running`, `stalled`, `failed`, or `dropped`.
`error`       | [`text`]                      | If the source is in an error state, the error message.
`details`     | [`jsonb`]                     | Additional metadata provided by the source. In case of error, may contain a `hint` field with helpful suggestions.

### `mz_sink_statuses`

The `mz_sink_statuses` view provides the current state for each sink in the
system, including potential error messages and additional metadata helpful for
debugging.

Field                   | Type                          | Meaning
------------------------|-------------------------------|--------
`id`                    | [`text`]                      | The ID of the sink. Corresponds to [`mz_catalog.mz_sinks.id`](../mz_catalog#mz_sinks).
`name`                  | [`text`]                      | The name of the sink.
`type`                  | [`text`]                      | The type of the sink.
`last_status_change_at` | [`timestamp with time zone`]  | Wall-clock timestamp of the sink status change.
`status`                | [`text`]                      | The status of the sink: one of `created`, `starting`, `running`, `stalled`, `failed`, or `dropped`.
`error`                 | [`text`]                      | If the sink is in an error state, the error message.
`details`               | [`jsonb`]                     | Additional metadata provided by the sink. In case of error, may contain a `hint` field with helpful suggestions.

### `mz_sink_status_history`

The `mz_sink_status_history` table contains rows describing the
history of changes to the status of each sink in the system, including potential error
messages and additional metadata helpful for debugging.

Field         | Type                          | Meaning
--------------|-------------------------------|--------
`occurred_at` | [`timestamp with time zone`]  | Wall-clock timestamp of the sink status change.
`sink_id`     | [`text`]                      | The ID of the sink. Corresponds to [`mz_catalog.mz_sinks.id`](../mz_catalog#mz_sinks).
`status`      | [`text`]                      | The status of the sink: one of `created`, `starting`, `running`, `stalled`, `failed`, or `dropped`.
`error`       | [`text`]                      | If the sink is in an error state, the error message.
`details`     | [`jsonb`]                     | Additional metadata provided by the sink. In case of error, may contain a `hint` field with helpful suggestions.


[`bigint`]: /sql/types/bigint
[`bigint list`]: /sql/types/list
[`boolean`]: /sql/types/boolean
[`jsonb`]: /sql/types/jsonb
[`mz_timestamp`]: /sql/types/mz_timestamp
[`numeric`]: /sql/types/numeric
[`text`]: /sql/types/text
[`text list`]: /sql/types/list
[`uuid`]: /sql/types/uuid
[`uint8`]: /sql/types/uint8
[`timestamp with time zone`]: /sql/types/timestamp
[arrangement]: /overview/arrangements/#arrangements
[dataflow]: /overview/arrangements/#dataflows

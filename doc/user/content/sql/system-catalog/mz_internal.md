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


## System Relations

### `mz_cluster_replica_metrics`

The `mz_cluster_replica_metrics` table gives the last known CPU and RAM utilization statistics
for all processes of all extant cluster replicas.

At this time, we do not make any guarantees about the exactness or freshness of these numbers.

<!-- RELATION_SPEC mz_internal.mz_cluster_replica_metrics -->
| Field               | Type         | Meaning                                              |
| ------------------- | ------------ | --------                                             |
| `replica_id`        | [`text`]     | The ID of a cluster replica.                         |
| `process_id`        | [`uint8`]    | An identifier of a compute process within a replica. |
| `cpu_nano_cores`    | [`uint8`]    | Approximate CPU usage, in billionths of a vCPU core. |
| `memory_bytes`      | [`uint8`]    | Approximate RAM usage, in bytes.                     |
| `disk_bytes`        | [`uint8`]    | Currently null. Reserved for later use.              |

### `mz_cluster_replica_sizes`

The `mz_cluster_replica_sizes` table contains a mapping of logical sizes
(e.g. "xlarge") to physical sizes (number of processes, and CPU and memory allocations per process).

{{< warning >}}
The values in this table may change at any time, and users should not rely on
them for any kind of capacity planning.
{{< /warning >}}

<!-- RELATION_SPEC mz_internal.mz_cluster_replica_sizes -->
| Field                  | Type        | Meaning                                                       |
|------------------------|-------------|---------------------------------------------------------------|
| `size`                 | [`text`]    | The human-readable replica size.                              |
| `processes`            | [`uint8`]   | The number of processes in the replica.                       |
| `workers`              | [`uint8`]   | The number of Timely Dataflow workers per process.            |
| `cpu_nano_cores`       | [`uint8`]   | The CPU allocation per process, in billionths of a vCPU core. |
| `memory_bytes`         | [`uint8`]   | The RAM allocation per process, in billionths of a vCPU core. |
| `disk_bytes`           | [`uint8`]   | Currently null. Reserved for later use.                       |
| `credits_per_hour`     | [`numeric`] | The number of compute credits consumed per hour.              |

### `mz_cluster_links`

The `mz_cluster_links` table contains a row for each cluster that is linked to a
source or sink. When present, the lifetime of the specified cluster is tied to
the lifetime of the specified source or sink: the cluster cannot be dropped
without dropping the linked source or sink, and dropping the linked source or
sink will also drop the cluster. There is at most one row per cluster.

{{< note >}}
The concept of a linked cluster is not user-facing, and is intentionally undocumented. Linked clusters are meant to preserve the soon-to-be legacy interface for sizing sources and sinks.
{{< /note >}}

<!-- RELATION_SPEC mz_internal.mz_cluster_links -->
| Field        | Type     | Meaning                                                                                                      |
|--------------|----------|--------------------------------------------------------------------------------------------------------------|
| `cluster_id` | [`text`] | The ID of the cluster. Corresponds to [`mz_clusters.id`](/sql/system-catalog/mz_catalog/#mz_clusters).       |
| `object_id`  | [`text`] | The ID of the source or sink. Corresponds to [`mz_objects.id`](/sql/system-catalog/mz_catalog/#mz_clusters). |



### `mz_cluster_replica_statuses`

The `mz_cluster_replica_statuses` table contains a row describing the status
of each process in each cluster replica in the system.

<!-- RELATION_SPEC mz_internal.mz_cluster_replica_statuses -->
| Field                | Type                            | Meaning                                                                      |
| -------------------- | ------------------------------- | --------                                                                     |
| `replica_id`         | [`text`]                        | Materialize's unique ID for the cluster replica.                             |
| `process_id`         | [`uint8`]                       | The ID of the process within the cluster replica.                            |
| `status`             | [`text`]                        | The status of the cluster replica: `ready` or `not-ready`.                   |
| `reason`             | [`text`]                        | If the cluster replica is in a `not-ready` state, the reason (if available). |
| `updated_at`         | [`timestamp with time zone`]    | The time at which the status was last updated.                               |

### `mz_cluster_replica_utilization`

The `mz_cluster_replica_utilization` view gives the last known CPU and RAM utilization statistics
for all processes of all extant cluster replicas, as a percentage of the total resource allocation.

At this time, we do not make any guarantees about the exactness or freshness of these numbers.

<!-- RELATION_SPEC mz_internal.mz_cluster_replica_utilization -->
| Field            | Type                 | Meaning                                                    |
|------------------|----------------------|------------------------------------------------------------|
| `replica_id`     | [`text`]             | The ID of a cluster replica.                               |
| `process_id`     | [`uint8`]            | An identifier of a compute process within a replica.       |
| `cpu_percent`    | [`double precision`] | Approximate CPU usage, in percent of the total allocation. |
| `memory_percent` | [`double precision`] | Approximate RAM usage, in percent of the total allocation. |
| `disk_percent`   | [`double precision`] | Currently null. Reserved for later use.                    |

### `mz_cluster_replica_frontiers`

The `mz_cluster_replica_frontiers` table describes the frontiers of each [dataflow] in the system.
[`mz_compute_frontiers`](#mz_compute_frontiers) is similar to this table, but `mz_compute_frontiers`
exposes the frontiers known to the compute replicas while `mz_cluster_replica_frontiers` contains
the frontiers the coordinator is aware of.

At this time, we do not make any guarantees about the freshness of these numbers.

<!-- RELATION_SPEC mz_internal.mz_cluster_replica_frontiers -->
| Field         | Type             | Meaning                                                                                                                                                  |
| ------------- | ------------     | --------                                                                                                                                                 |
| `replica_id`  | [`text`]         | The ID of a cluster replica.                                                                                                                             |
| `export_id `  | [`text`]         | The ID of the index, materialized view, or subscription that created the dataflow. Corresponds to [`mz_compute_exports.export_id`](#mz_compute_exports). |
| `time`        | [`mz_timestamp`] | The next timestamp at which the output may change.                                                                                                       |

### `mz_cluster_replica_heartbeats`

The `mz_cluster_replica_heartbeats` table gives the last known heartbeat of all
extant cluster replicas.

<!-- RELATION_SPEC mz_internal.mz_cluster_replica_heartbeats -->
| Field             | Type                           | Meaning                                   |
| ----------------- | ------------------------------ | --------                                  |
| `replica_id`      | [`text`]                       | The ID of a cluster replica.              |
| `last_heartbeat`  | [`timestamp with time zone`]   | The time of the replica's last heartbeat. |

### `mz_cluster_replica_history`

The `mz_cluster_replica_history` view contains information about the timespan of
each replica, including the times at which it was created and dropped
(if applicable).

<!-- RELATION_SPEC mz_internal.mz_cluster_replica_history -->
| Field                 | Type                         | Meaning                                                                                                                                   |
|-----------------------|------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------|
| `internal_replica_id` | [`text`]                     | An internal identifier of a cluster replica. Guaranteed to be unique, but not guaranteed to correspond to any user-facing replica ID.     |
| `size`                | [`text`]                     | The size of the cluster replica. Corresponds to [`mz_cluster_replica_sizes.size`](#mz_cluster_replica_sizes).                             |
| `cluster_name`        | [`text`]                     | The name of the cluster associated with the replica.                                                                                      |
| `replica_name`        | [`text`]                     | The name of the replica.                                                                                                                  |
| `created_at`          | [`timestamp with time zone`] | The time at which the replica was created.                                                                                                |
| `dropped_at`          | [`timestamp with time zone`] | The time at which the replica was dropped, or `NULL` if it still exists.                                                                  |
| `credits_per_hour`    | [`numeric`]                  | The number of compute credits consumed per hour. Corresponds to [`mz_cluster_replica_sizes.credits_per_hour`](#mz_cluster_replica_sizes). |

### `mz_kafka_sources`

The `mz_kafka_sources` table contains a row for each Kafka source in the system.

<!-- RELATION_SPEC mz_internal.mz_kafka_sources -->
| Field                  | Type           | Meaning                                                                                                   |
|------------------------|----------------|-----------------------------------------------------------------------------------------------------------|
| `id`                   | [`text`]       | The ID of the Kafka source. Corresponds to [`mz_catalog.mz_sources.id`](../mz_catalog#mz_sources).        |
| `group_id_base`        | [`text`]       | The prefix of the group ID that Materialize will use when consuming data for the Kafka source.            |

### `mz_object_dependencies`

The `mz_object_dependencies` table describes the dependency structure between
all database objects in the system.

<!-- RELATION_SPEC mz_internal.mz_object_dependencies -->
| Field                   | Type         | Meaning                                                                                       |
| ----------------------- | ------------ | --------                                                                                      |
| `object_id`             | [`text`]     | The ID of the dependent object. Corresponds to [`mz_objects.id`](../mz_catalog/#mz_objects).  |
| `referenced_object_id`  | [`text`]     | The ID of the referenced object. Corresponds to [`mz_objects.id`](../mz_catalog/#mz_objects). |

### `mz_object_transitive_dependencies`

The `mz_object_transitive_dependencies` table describes the transitive dependency structure between
all database objects in the system.
This table is the transitive closure of [`mz_object_dependencies`](#mz_object_dependencies).

<!-- RELATION_SPEC mz_internal.mz_object_transitive_dependencies -->
| Field                   | Type         | Meaning                                                                                                               |
| ----------------------- | ------------ | --------                                                                                                              |
| `id`             | [`text`]     | The ID of the dependent object. Corresponds to [`mz_objects.id`](../mz_catalog/#mz_objects).                          |
| `referenced_object_id`  | [`text`]     | The ID of the (possibly transitively) referenced object. Corresponds to [`mz_objects.id`](../mz_catalog/#mz_objects). |

### `mz_postgres_sources`

The `mz_postgres_sources` table contains a row for each PostgreSQL source in the
system.

<!-- RELATION_SPEC mz_internal.mz_postgres_sources -->
| Field               | Type             | Meaning                                                                                                        |
| ------------------- | ---------------- | --------                                                                                                       |
| `id`                | [`text`]         | The ID of the source. Corresponds to [`mz_catalog.mz_sources.id`](../mz_catalog#mz_sources).                   |
| `replication_slot`  | [`text`]         | The name of the replication slot in the PostgreSQL database that Materialize will create and stream data from. |

### `mz_sessions`

The `mz_sessions` table contains a row for each active session in the system.

<!-- RELATION_SPEC mz_internal.mz_sessions -->
| Field           | Type                           | Meaning                                                                                                                   |
| --------------- | ------------------------------ | --------                                                                                                                  |
| `id`            | [`uint4`]                      | The ID of the session.                                                                                                    |
| `role_id`       | [`text`]                       | The role ID of the role that the session is logged in as. Corresponds to [`mz_catalog.mz_roles`](../mz_catalog#mz_roles). |
| `connected_at`  | [`timestamp with time zone`]   | The time at which the session connected to the system.                                                                    |

### `mz_sink_statistics`

The `mz_sink_statistics` table contains statistics for each worker thread of
each sink in the system.

Materialize does not make any guarantees about the exactness or freshness of
these statistics. They are occasionally reset to zero as internal components of
the system are restarted.

<!-- RELATION_SPEC mz_internal.mz_sink_statistics -->
| Field                | Type      | Meaning                                                                                                             |
|----------------------|-----------| --------                                                                                                            |
| `id`                 | [`text`]  | The ID of the source. Corresponds to [`mz_catalog.mz_sources.id`](../mz_catalog#mz_sources).                        |
| `worker_id`          | [`uint8`] | The ID of the worker thread.                                                                                        |
| `messages_staged`    | [`uint8`] | The number of messages staged but possibly not committed to the sink.                                               |
| `messages_committed` | [`uint8`] | The number of messages committed to the sink.                                                                       |
| `bytes_staged`       | [`uint8`] | The number of bytes staged but possibly not committed to the sink. This counts both keys and values, if applicable. |
| `bytes_committed`    | [`uint8`] | The number of bytes committed to the sink. This counts both keys and values, if applicable.                         |

### `mz_sink_statuses`

The `mz_sink_statuses` view provides the current state for each sink in the
system, including potential error messages and additional metadata helpful for
debugging.

<!-- RELATION_SPEC mz_internal.mz_sink_statuses -->
| Field                    | Type                            | Meaning                                                                                                          |
| ------------------------ | ------------------------------- | --------                                                                                                         |
| `id`                     | [`text`]                        | The ID of the sink. Corresponds to [`mz_catalog.mz_sinks.id`](../mz_catalog#mz_sinks).                           |
| `name`                   | [`text`]                        | The name of the sink.                                                                                            |
| `type`                   | [`text`]                        | The type of the sink.                                                                                            |
| `last_status_change_at`  | [`timestamp with time zone`]    | Wall-clock timestamp of the sink status change.                                                                  |
| `status`                 | [`text`]                        | The status of the sink: one of `created`, `starting`, `running`, `stalled`, `failed`, or `dropped`.              |
| `error`                  | [`text`]                        | If the sink is in an error state, the error message.                                                             |
| `details`                | [`jsonb`]                       | Additional metadata provided by the sink. In case of error, may contain a `hint` field with helpful suggestions. |

### `mz_sink_status_history`

The `mz_sink_status_history` table contains rows describing the
history of changes to the status of each sink in the system, including potential error
messages and additional metadata helpful for debugging.

<!-- RELATION_SPEC mz_internal.mz_sink_status_history -->
| Field          | Type                            | Meaning                                                                                                          |
| -------------- | ------------------------------- | --------                                                                                                         |
| `occurred_at`  | [`timestamp with time zone`]    | Wall-clock timestamp of the sink status change.                                                                  |
| `sink_id`      | [`text`]                        | The ID of the sink. Corresponds to [`mz_catalog.mz_sinks.id`](../mz_catalog#mz_sinks).                           |
| `status`       | [`text`]                        | The status of the sink: one of `created`, `starting`, `running`, `stalled`, `failed`, or `dropped`.              |
| `error`        | [`text`]                        | If the sink is in an error state, the error message.                                                             |
| `details`      | [`jsonb`]                       | Additional metadata provided by the sink. In case of error, may contain a `hint` field with helpful suggestions. |

### `mz_source_statistics`

The `mz_source_statistics` table contains statistics for each worker thread of
each source in the system.

Materialize does not make any guarantees about the exactness or freshness of
these statistics. They are occasionally reset to zero as internal components of
the system are restarted.

<!-- RELATION_SPEC mz_internal.mz_source_statistics -->
| Field                  | Type        | Meaning                                                                                                                                                                                                                                                                             |
| ---------------------- |-------------| --------                                                                                                                                                                                                                                                                            |
| `id`                   | [`text`]    | The ID of the source. Corresponds to [`mz_catalog.mz_sources.id`](../mz_catalog#mz_sources).                                                                                                                                                                                        |
| `worker_id`            | [`uint8`]   | The ID of the worker thread.                                                                                                                                                                                                                                                        |
| `snapshot_committed`   | [`boolean`] | Whether the worker has committed the initial snapshot for a source.                                                                                                                                                                                                                 |
| `messages_received`    | [`uint8`]   | The number of messages the worker has received from the external system. Messages are counted in a source type-specific manner. Messages do not correspond directly to updates: some messages produce multiple updates, while other messages may be coalesced into a single update. |
| `updates_staged`       | [`uint8`]   | The number of updates (insertions plus deletions) the worker has written but not yet committed to the storage layer.                                                                                                                                                                |
| `updates_committed`    | [`uint8`]   | The number of updates (insertions plus deletions) the worker has committed to the storage layer.                                                                                                                                                                                    |
| `bytes_received`       | [`uint8`]   | The number of bytes the worker has read from the external system. Bytes are counted in a source type-specific manner and may or may not include protocol overhead.                                                                                                                  |
| `envelope_state_bytes` | [`uint8`]   | The number of bytes stored in the source envelope state.                                                                       |
| `envelope_state_count` | [`uint8`]   | The number of individual records stored in the source envelope state.                                                                                                                                                                                                               |

### `mz_source_statuses`

The `mz_source_statuses` view provides the current state for each source in the
system, including potential error messages and additional metadata helpful for
debugging.

<!-- RELATION_SPEC mz_internal.mz_source_statuses -->
| Field                    | Type                            | Meaning                                                                                                            |
| ------------------------ | ------------------------------- | --------                                                                                                           |
| `id`                     | [`text`]                        | The ID of the source. Corresponds to [`mz_catalog.mz_sources.id`](../mz_catalog#mz_sources).                       |
| `name`                   | [`text`]                        | The name of the source.                                                                                            |
| `type`                   | [`text`]                        | The type of the source.                                                                                            |
| `last_status_change_at`  | [`timestamp with time zone`]    | Wall-clock timestamp of the source status change.                                                                  |
| `status`                 | [`text`]                        | The status of the source: one of `created`, `starting`, `running`, `stalled`, `failed`, or `dropped`.              |
| `error`                  | [`text`]                        | If the source is in an error state, the error message.                                                             |
| `details`                | [`jsonb`]                       | Additional metadata provided by the source. In case of error, may contain a `hint` field with helpful suggestions. |

### `mz_source_status_history`

The `mz_source_status_history` table contains a row describing the status of the
historical state for each source in the system, including potential error
messages and additional metadata helpful for debugging.

<!-- RELATION_SPEC mz_internal.mz_source_status_history -->
| Field          | Type                            | Meaning                                                                                                            |
| -------------- | ------------------------------- | --------                                                                                                           |
| `occurred_at`  | [`timestamp with time zone`]    | Wall-clock timestamp of the source status change.                                                                  |
| `source_id`    | [`text`]                        | The ID of the source. Corresponds to [`mz_catalog.mz_sources.id`](../mz_catalog#mz_sources).                       |
| `status`       | [`text`]                        | The status of the source: one of `created`, `starting`, `running`, `stalled`, `failed`, or `dropped`.              |
| `error`        | [`text`]                        | If the source is in an error state, the error message.                                                             |
| `details`      | [`jsonb`]                       | Additional metadata provided by the source. In case of error, may contain a `hint` field with helpful suggestions. |

### `mz_subscriptions`

The `mz_subscriptions` table describes all active [`SUBSCRIBE`](/sql/subscribe)
operations in the system.

<!-- RELATION_SPEC mz_internal.mz_subscriptions -->
| Field                    | Type                         | Meaning                                                                                                                    |
| ------------------------ |------------------------------| --------                                                                                                                   |
| `id`                     | [`text`]                     | The ID of the subscription.                                                                                                |
| `session_id`             | [`uint4`]                    | The ID of the session that runs the subscription. Corresponds to [`mz_sessions.id`](#mz_sessions).                         |
| `cluster_id`             | [`text`]                     | The ID of the cluster on which the subscription is running. Corresponds to [`mz_clusters.id`](../mz_catalog/#mz_clusters). |
| `created_at`             | [`timestamp with time zone`] | The time at which the subscription was created.                                                                            |
| `referenced_object_ids`  | [`text list`]                | The IDs of objects referenced by the subscription. Corresponds to [`mz_objects.id`](../mz_catalog/#mz_objects)             |


## Replica Introspection Relations

This section lists the available replica introspection relations.

Introspection relations are maintained by independently collecting internal logging information within each of the replicas of a cluster.
Thus, in a multi-replica cluster, queries to these relations need to be directed to a specific replica by issuing the command `SET cluster_replica = <replica_name>`.
Note that once this command is issued, all subsequent `SELECT` queries, for introspection relations or not, will be directed to the targeted replica.
Replica targeting can be cancelled by issuing the command `RESET cluster_replica`.

For each of the below introspection relations, there exists also a variant with a `_per_worker` name suffix.
Per-worker relations expose the same data as their global counterparts, but have an extra `worker_id` column that splits the information by Timely Dataflow worker.

### `mz_active_peeks`

The `mz_active_peeks` view describes all read queries ("peeks") that are pending in the [dataflow] layer.

<!-- RELATION_SPEC mz_internal.mz_active_peeks -->
| Field       | Type               | Meaning                                                                                                           |
| ----------- | ------------------ | --------                                                                                                          |
| `id`        | [`uuid`]           | The ID of the peek request.                                                                                       |
| `index_id`  | [`text`]           | The ID of the index the peek is targeting. Corresponds to [`mz_catalog.mz_indexes.id`](../mz_catalog#mz_indexes). |
| `time`      | [`mz_timestamp`]   | The timestamp the peek has requested.                                                                             |

<!-- RELATION_SPEC_UNDOCUMENTED mz_internal.mz_active_peeks_per_worker -->

### `mz_arrangement_sharing`

The `mz_arrangement_sharing` view describes how many times each [arrangement] in the system is used.

<!-- RELATION_SPEC mz_internal.mz_arrangement_sharing -->
| Field          | Type       | Meaning                                                                                                                   |
| -------------- |------------| --------                                                                                                                  |
| `operator_id`  | [`uint8`]  | The ID of the operator that created the arrangement. Corresponds to [`mz_dataflow_operators.id`](#mz_dataflow_operators). |
| `count`        | [`bigint`] | The number of operators that share the arrangement.                                                                       |

<!-- RELATION_SPEC_UNDOCUMENTED mz_internal.mz_arrangement_sharing_per_worker -->
<!-- RELATION_SPEC_UNDOCUMENTED mz_internal.mz_arrangement_sharing_raw -->

### `mz_arrangement_sizes`

The `mz_arrangement_sizes` view describes the size of each [arrangement] in the system.

<!-- RELATION_SPEC mz_internal.mz_arrangement_sizes -->
| Field          | Type        | Meaning                                                                                                                   |
| -------------- |-------------| --------                                                                                                                  |
| `operator_id`  | [`uint8`]   | The ID of the operator that created the arrangement. Corresponds to [`mz_dataflow_operators.id`](#mz_dataflow_operators). |
| `records`      | [`numeric`] | The number of records in the arrangement.                                                                                 |
| `batches`      | [`numeric`] | The number of batches in the arrangement.                                                                                 |

<!-- RELATION_SPEC_UNDOCUMENTED mz_internal.mz_arrangement_sizes_per_worker -->

### `mz_compute_delays_histogram`

The `mz_compute_delays_histogram` view describes a histogram of the wall-clock delay in nanoseconds between observations of import frontier advancements of a [dataflow] and the advancements of the corresponding export frontiers.

<!-- RELATION_SPEC mz_internal.mz_compute_delays_histogram -->
| Field        | Type        | Meaning                                                                                                                                                                                                                                              |
| ------------ |-------------| --------                                                                                                                                                                                                                                             |
| `export_id`  | [`text`]    | The ID of the dataflow export. Corresponds to [`mz_compute_exports.export_id`](#mz_compute_exports).                                                                                                                                                 |
| `import_id`  | [`text`]    | The ID of the dataflow import. Corresponds to either [`mz_catalog.mz_sources.id`](../mz_catalog#mz_sources) or [`mz_catalog.mz_tables.id`](../mz_catalog#mz_tables) or [`mz_catalog.mz_materialized_views.id`](../mz_catalog#mz_materialized_views). |
| `delay_ns`   | [`uint8`]   | The upper bound of the bucket in nanoseconds.                                                                                                                                                                                                        |
| `count`      | [`numeric`] | The (noncumulative) count of delay measurements in this bucket.                                                                                                                                                                                      |

<!-- RELATION_SPEC_UNDOCUMENTED mz_internal.mz_compute_delays_histogram_per_worker -->
<!-- RELATION_SPEC_UNDOCUMENTED mz_internal.mz_compute_delays_histogram_raw -->

### `mz_compute_dependencies`

The `mz_compute_dependencies` view describes the dependency structure between each [dataflow] and the sources of its data.

<!-- RELATION_SPEC mz_internal.mz_compute_dependencies -->
| Field        | Type         | Meaning                                                                                                                                                                                                                |
| ------------ | ------------ | --------                                                                                                                                                                                                               |
| `export_id`  | [`text`]     | The ID of the dataflow export. Corresponds to [`mz_compute_exports.export_id`](#mz_compute_exports).                                                                                                                   |
| `import_id`  | [`text`]     | The ID of the dataflow import. Corresponds to [`mz_catalog.mz_sources.id`](../mz_catalog#mz_sources) or [`mz_catalog.mz_tables.id`](../mz_catalog#mz_tables) or [`mz_compute_exports.export_id`](#mz_compute_exports). |

<!-- RELATION_SPEC_UNDOCUMENTED mz_internal.mz_compute_dependencies_per_worker -->

### `mz_compute_exports`

The `mz_compute_exports` view describes the objects exported by [dataflows][dataflow] in the system.

<!-- RELATION_SPEC mz_internal.mz_compute_exports -->
| Field          | Type      | Meaning                                                                                                                                                                                                                                                                                        |
| -------------- |-----------| --------                                                                                                                                                                                                                                                                                       |
| `export_id`    | [`text`]  | The ID of the index, materialized view, or subscription exported by the dataflow. Corresponds to [`mz_catalog.mz_indexes.id`](../mz_catalog#mz_indexes), [`mz_catalog.mz_materialized_views.id`](../mz_catalog#mz_materialized_views), or [`mz_internal.mz_subscriptions`](#mz_subscriptions). |
| `dataflow_id`  | [`uint8`] | The ID of the dataflow. Corresponds to [`mz_dataflows.id`](#mz_dataflows).                                                                                                                                                                                                               |

<!-- RELATION_SPEC_UNDOCUMENTED mz_internal.mz_compute_exports_per_worker -->

### `mz_compute_frontiers`

The `mz_compute_frontiers` view describes the frontier of each [dataflow] export in the system.
The frontier describes the earliest timestamp at which the output of the dataflow may change; data prior to that timestamp is sealed.

<!-- RELATION_SPEC mz_internal.mz_compute_frontiers -->
| Field        | Type               | Meaning                                                                                              |
| ------------ | ------------------ | --------                                                                                             |
| `export_id`  | [`text`]           | The ID of the dataflow export. Corresponds to [`mz_compute_exports.export_id`](#mz_compute_exports). |
| `time`       | [`mz_timestamp`]   | The next timestamp at which the dataflow output may change.                                          |

<!-- RELATION_SPEC_UNDOCUMENTED mz_internal.mz_compute_frontiers_per_worker -->

### `mz_compute_import_frontiers`

The `mz_compute_import_frontiers` view describes the frontiers of each [dataflow] import in the system.
The frontier describes the earliest timestamp at which the input into the dataflow may change; data prior to that timestamp is sealed.

<!-- RELATION_SPEC mz_internal.mz_compute_import_frontiers -->
| Field        | Type               | Meaning                                                                                                                                                                                                                |
| ------------ | ------------------ | --------                                                                                                                                                                                                               |
| `export_id`  | [`text`]           | The ID of the dataflow export. Corresponds to [`mz_compute_exports.export_id`](#mz_compute_exports).                                                                                                                   |
| `import_id`  | [`text`]           | The ID of the dataflow import. Corresponds to [`mz_catalog.mz_sources.id`](../mz_catalog#mz_sources) or [`mz_catalog.mz_tables.id`](../mz_catalog#mz_tables) or [`mz_compute_exports.export_id`](#mz_compute_exports). |
| `time`       | [`mz_timestamp`]   | The next timestamp at which the dataflow input may change.                                                                                                                                                             |

<!-- RELATION_SPEC_UNDOCUMENTED mz_internal.mz_compute_import_frontiers_per_worker -->

### `mz_compute_operator_durations_histogram`

The `mz_compute_operator_durations_histogram` view describes a histogram of the duration in nanoseconds of each invocation for each [dataflow] operator.

<!-- RELATION_SPEC mz_internal.mz_compute_operator_durations_histogram -->
| Field          | Type        | Meaning                                                                                      |
| -------------- |-------------| --------                                                                                     |
| `id`           | [`uint8`]   | The ID of the operator. Corresponds to [`mz_dataflow_operators.id`](#mz_dataflow_operators). |
| `duration_ns`  | [`uint8`]   | The upper bound of the duration bucket in nanoseconds.                                       |
| `count`        | [`numeric`] | The (noncumulative) count of invocations in the bucket.                                      |

<!-- RELATION_SPEC_UNDOCUMENTED mz_internal.mz_compute_operator_durations_histogram_per_worker -->
<!-- RELATION_SPEC_UNDOCUMENTED mz_internal.mz_compute_operator_durations_histogram_raw -->

### `mz_dataflows`

The `mz_dataflows` view describes the [dataflows][dataflow] in the system.

<!-- RELATION_SPEC mz_internal.mz_dataflows -->
| Field       | Type      | Meaning                                |
| ----------- |-----------| --------                               |
| `id`        | [`uint8`] | The ID of the dataflow.                |
| `name`      | [`text`]  | The internal name of the dataflow.     |

<!-- RELATION_SPEC_UNDOCUMENTED mz_internal.mz_dataflows_per_worker -->

### `mz_dataflow_addresses`

The `mz_dataflow_addresses` view describes how the [dataflow] channels and operators in the system are nested into scopes.

<!-- RELATION_SPEC mz_internal.mz_dataflow_addresses -->
| Field        | Type            | Meaning                                                                                                                                                       |
| ------------ |-----------------| --------                                                                                                                                                      |
| `id`         | [`uint8`]       | The ID of the channel or operator. Corresponds to [`mz_dataflow_channels.id`](#mz_dataflow_channels) or [`mz_dataflow_operators.id`](#mz_dataflow_operators). |
| `address`    | [`bigint list`] | A list of scope-local indexes indicating the path from the root to this channel or operator.                                                                  |

<!-- RELATION_SPEC_UNDOCUMENTED mz_internal.mz_dataflow_addresses_per_worker -->

### `mz_dataflow_channels`

The `mz_dataflow_channels` view describes the communication channels between [dataflow] operators.
A communication channel connects one of the outputs of a source operator to one of the inputs of a target operator.

<!-- RELATION_SPEC mz_internal.mz_dataflow_channels -->
| Field            | Type      | Meaning                                                                                                                 |
| ---------------- |-----------| --------                                                                                                                |
| `id`             | [`uint8`] | The ID of the channel.                                                                                                  |
| `from_index`     | [`uint8`] | The scope-local index of the source operator. Corresponds to [`mz_dataflow_addresses.address`](#mz_dataflow_addresses). |
| `from_port`      | [`uint8`] | The source operator's output port.                                                                                      |
| `to_index`       | [`uint8`] | The scope-local index of the target operator. Corresponds to [`mz_dataflow_addresses.address`](#mz_dataflow_addresses). |
| `to_port`        | [`uint8`] | The target operator's input port.                                                                                       |

<!-- RELATION_SPEC_UNDOCUMENTED mz_internal.mz_dataflow_channels_per_worker -->

### `mz_dataflow_channel_operators`

The `mz_dataflow_channel_operators` view associates [dataflow] channels with the operators that are their endpoints.

<!-- RELATION_SPEC mz_internal.mz_dataflow_channel_operators -->
| Field                   | Type           | Meaning                                                                                                             |
|-------------------------|----------------|---------------------------------------------------------------------------------------------------------------------|
| `id`                    | [`uint8`]      | The ID of the channel. Corresponds to [`mz_dataflow_channels.id`](#mz_dataflow_channels).                           |
| `from_operator_id`      | [`uint8`]      | The ID of the source of the channel. Corresponds to [`mz_dataflow_operators.id`](#mz_dataflow_operators).           |
| `from_operator_address` | [`uint8 list`] | The address of the source of the channel. Corresponds to [`mz_dataflow_addresses.address`](#mz_dataflow_addresses). |
| `to_operator_id`        | [`uint8`]      | The ID of the target of the channel. Corresponds to [`mz_dataflow_operators.id`](#mz_dataflow_operators).           |
| `to_operator_address`   | [`uint8 list`] | The address of the target of the channel. Corresponds to [`mz_dataflow_addresses.address`](#mz_dataflow_addresses). |

<!-- RELATION_SPEC_UNDOCUMENTED mz_internal.mz_dataflow_channel_operators_per_worker -->

### `mz_dataflow_operators`

The `mz_dataflow_operators` view describes the [dataflow] operators in the system.

<!-- RELATION_SPEC mz_internal.mz_dataflow_operators -->
| Field        | Type      | Meaning                            |
| ------------ |-----------| --------                           |
| `id`         | [`uint8`] | The ID of the operator.            |
| `name`       | [`text`]  | The internal name of the operator. |

<!-- RELATION_SPEC_UNDOCUMENTED mz_internal.mz_dataflow_operators_per_worker -->

### `mz_dataflow_operator_dataflows`

The `mz_dataflow_operator_dataflows` view describes the [dataflow] to which each operator belongs.

<!-- RELATION_SPEC mz_internal.mz_dataflow_operator_dataflows -->
| Field            | Type      | Meaning                                                                                         |
| ---------------- |-----------| --------                                                                                        |
| `id`             | [`uint8`] | The ID of the operator. Corresponds to [`mz_dataflow_operators.id`](#mz_dataflow_operators).    |
| `name`           | [`text`]  | The internal name of the operator.                                                              |
| `dataflow_id`    | [`uint8`] | The ID of the dataflow hosting the operator. Corresponds to [`mz_dataflows.id`](#mz_dataflows). |
| `dataflow_name`  | [`text`]  | The internal name of the dataflow hosting the operator.                                         |

<!-- RELATION_SPEC_UNDOCUMENTED mz_internal.mz_dataflow_operator_dataflows_per_worker -->

### `mz_dataflow_operator_parents`

The `mz_dataflow_operator_parents` view describes how [dataflow] operators are nested into scopes, by relating operators to their parent operators.

<!-- RELATION_SPEC mz_internal.mz_dataflow_operator_parents -->
| Field        | Type      | Meaning                                                                                                        |
| ------------ |-----------| --------                                                                                                       |
| `id`         | [`uint8`] | The ID of the operator. Corresponds to [`mz_dataflow_operators.id`](#mz_dataflow_operators).                   |
| `parent_id`  | [`uint8`] | The ID of the operator's parent operator. Corresponds to [`mz_dataflow_operators.id`](#mz_dataflow_operators). |

<!-- RELATION_SPEC_UNDOCUMENTED mz_internal.mz_dataflow_operator_parents_per_worker -->

### `mz_dataflow_arrangement_sizes`

The `mz_dataflow_arrangement_sizes` view describes how many records and batches
are contained in operators under each dataflow.

<!-- RELATION_SPEC mz_internal.mz_dataflow_arrangement_sizes -->
| Field     | Type        | Meaning                                                                      |
|-----------|-------------|------------------------------------------------------------------------------|
| `id`      | [`uint8`]   | The ID of the [dataflow]. Corresponds to [`mz_dataflows.id`](#mz_dataflows). |
| `name`    | [`text`]    | The name of the object (e.g., index) maintained by the dataflow.             |
| `records` | [`numeric`] | The number of records in all arrangements in the dataflow.                   |
| `batches` | [`numeric`] | The number of batches in all arrangements in the dataflow.                   |

### `mz_message_counts`

The `mz_message_counts` view describes the messages sent and received over the [dataflow] channels in the system.

<!-- RELATION_SPEC mz_internal.mz_message_counts -->
| Field              | Type        | Meaning                                                                                   |
| ------------------ |-------------| --------                                                                                  |
| `channel_id`       | [`uint8`]   | The ID of the channel. Corresponds to [`mz_dataflow_channels.id`](#mz_dataflow_channels). |
| `sent`             | [`numeric`] | The number of messages sent.                                                              |
| `received`         | [`numeric`] | The number of messages received.                                                          |

<!-- RELATION_SPEC_UNDOCUMENTED mz_internal.mz_message_counts_per_worker -->
<!-- RELATION_SPEC_UNDOCUMENTED mz_internal.mz_message_counts_received_raw -->
<!-- RELATION_SPEC_UNDOCUMENTED mz_internal.mz_message_counts_sent_raw -->

### `mz_peek_durations_histogram`

The `mz_peek_durations_histogram` view describes a histogram of the duration in nanoseconds of read queries ("peeks") in the [dataflow] layer.

<!-- RELATION_SPEC mz_internal.mz_peek_durations_histogram -->
| Field          | Type        | Meaning                                            |
| -------------- |-------------| --------                                           |
| `duration_ns`  | [`uint8`]   | The upper bound of the bucket in nanoseconds.      |
| `count`        | [`numeric`] | The (noncumulative) count of peeks in this bucket. |

<!-- RELATION_SPEC_UNDOCUMENTED mz_internal.mz_peek_durations_histogram_per_worker -->
<!-- RELATION_SPEC_UNDOCUMENTED mz_internal.mz_peek_durations_histogram_raw -->

### `mz_records_per_dataflow`

The `mz_records_per_dataflow` view describes the number of records in each [dataflow].

<!-- RELATION_SPEC mz_internal.mz_records_per_dataflow -->
| Field        | Type        | Meaning                                                                    |
| ------------ |-------------| --------                                                                   |
| `id`         | [`uint8`]   | The ID of the dataflow. Corresponds to [`mz_dataflows.id`](#mz_dataflows). |
| `name`       | [`text`]    | The internal name of the dataflow.                                         |
| `records`    | [`numeric`] | The number of records in the dataflow.                                     |

<!-- RELATION_SPEC_UNDOCUMENTED mz_internal.mz_records_per_dataflow_per_worker -->

### `mz_records_per_dataflow_operator`

The `mz_records_per_dataflow_operator` view describes the number of records in each [dataflow] operator in the system.

<!-- RELATION_SPEC mz_internal.mz_records_per_dataflow_operator -->
| Field          | Type        | Meaning                                                                                      |
| -------------- |-------------| --------                                                                                     |
| `id`           | [`uint8`]   | The ID of the operator. Corresponds to [`mz_dataflow_operators.id`](#mz_dataflow_operators). |
| `name`         | [`text`]    | The internal name of the operator.                                                           |
| `dataflow_id`  | [`uint8`]   | The ID of the dataflow. Corresponds to [`mz_dataflows.id`](#mz_dataflows).                   |
| `records`      | [`numeric`] | The number of records in the operator.                                                       |

<!-- RELATION_SPEC_UNDOCUMENTED mz_internal.mz_records_per_dataflow_operator_per_worker -->

### `mz_scheduling_elapsed`

The `mz_scheduling_elapsed` view describes the total amount of time spent in each [dataflow] operator.

<!-- RELATION_SPEC mz_internal.mz_scheduling_elapsed -->
| Field         | Type        | Meaning                                                                                      |
| ------------- |-------------| --------                                                                                     |
| `id`          | [`uint8`]   | The ID of the operator. Corresponds to [`mz_dataflow_operators.id`](#mz_dataflow_operators). |
| `elapsed_ns`  | [`numeric`] | The total elapsed time spent in the operator in nanoseconds.                                 |

<!-- RELATION_SPEC_UNDOCUMENTED mz_internal.mz_scheduling_elapsed_per_worker -->
<!-- RELATION_SPEC_UNDOCUMENTED mz_internal.mz_scheduling_elapsed_raw -->

### `mz_scheduling_parks_histogram`

The `mz_scheduling_parks_histogram` view describes a histogram of [dataflow] worker park events. A park event occurs when a worker has no outstanding work.

<!-- RELATION_SPEC mz_internal.mz_scheduling_parks_histogram -->
| Field           | Type        | Meaning                                                  |
| --------------- |-------------| -------                                                  |
| `slept_for_ns`  | [`uint8`]   | The actual length of the park event in nanoseconds.      |
| `requested_ns`  | [`uint8`]   | The requested length of the park event in nanoseconds.   |
| `count`         | [`numeric`] | The (noncumulative) count of park events in this bucket. |

<!-- RELATION_SPEC_UNDOCUMENTED mz_internal.mz_scheduling_parks_histogram_per_worker -->
<!-- RELATION_SPEC_UNDOCUMENTED mz_internal.mz_scheduling_parks_histogram_raw -->

[`bigint`]: /sql/types/bigint
[`bigint list`]: /sql/types/list
[`boolean`]: /sql/types/boolean
[`double precision`]: /sql/types/double-precision
[`jsonb`]: /sql/types/jsonb
[`mz_timestamp`]: /sql/types/mz_timestamp
[`numeric`]: /sql/types/numeric
[`text`]: /sql/types/text
[`text list`]: /sql/types/list
[`uuid`]: /sql/types/uuid
[`uint4`]: /sql/types/uint4
[`uint8`]: /sql/types/uint8
[`timestamp with time zone`]: /sql/types/timestamp
[arrangement]: /get-started/arrangements/#arrangements
[dataflow]: /get-started/arrangements/#dataflows

<!-- RELATION_SPEC_UNDOCUMENTED mz_internal.mz_aggregates -->
<!-- RELATION_SPEC_UNDOCUMENTED mz_internal.mz_arrangement_batches_raw -->
<!-- RELATION_SPEC_UNDOCUMENTED mz_internal.mz_arrangement_records_raw -->
<!-- RELATION_SPEC_UNDOCUMENTED mz_internal.mz_dataflow_operator_reachability -->
<!-- RELATION_SPEC_UNDOCUMENTED mz_internal.mz_dataflow_operator_reachability_per_worker -->
<!-- RELATION_SPEC_UNDOCUMENTED mz_internal.mz_dataflow_operator_reachability_raw -->
<!-- RELATION_SPEC_UNDOCUMENTED mz_internal.mz_show_cluster_replicas -->
<!-- RELATION_SPEC_UNDOCUMENTED mz_internal.mz_show_indexes -->
<!-- RELATION_SPEC_UNDOCUMENTED mz_internal.mz_show_materialized_views -->
<!-- RELATION_SPEC_UNDOCUMENTED mz_internal.mz_storage_shards -->
<!-- RELATION_SPEC_UNDOCUMENTED mz_internal.mz_storage_usage_by_shard -->
<!-- RELATION_SPEC_UNDOCUMENTED mz_internal.mz_view_foreign_keys -->
<!-- RELATION_SPEC_UNDOCUMENTED mz_internal.mz_view_keys -->

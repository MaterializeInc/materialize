---
title: "mz_introspection"
description: "mz_introspection is a system catalog schema which contains replica introspection relations. This schema is not part of Materialize's stable interface."
menu:
  main:
    parent: 'system-catalog'
    weight: 4
---

The following sections describe the available objects in the `mz_introspection`
schema.

{{< warning >}}
The objects in the `mz_introspection` schema are not part of Materialize's stable interface.
Backwards-incompatible changes to these objects may be made at any time.
{{< /warning >}}

{{< warning >}}
`SELECT` statements may reference these objects, but creating views that
reference these objects is not allowed.
{{< /warning >}}

Introspection relations are maintained by independently collecting internal logging information within each of the replicas of a cluster.
Thus, in a multi-replica cluster, queries to these relations need to be directed to a specific replica by issuing the command `SET cluster_replica = <replica_name>`.
Note that once this command is issued, all subsequent `SELECT` queries, for introspection relations or not, will be directed to the targeted replica.
Replica targeting can be cancelled by issuing the command `RESET cluster_replica`.

For each of the below introspection relations, there exists also a variant with a `_per_worker` name suffix.
Per-worker relations expose the same data as their global counterparts, but have an extra `worker_id` column that splits the information by Timely Dataflow worker.

## `mz_active_peeks`

The `mz_active_peeks` view describes all read queries ("peeks") that are pending in the [dataflow] layer.

<!-- RELATION_SPEC mz_introspection.mz_active_peeks -->
| Field       | Type             | Meaning                                                                                                                                                                                                                                                                                                               |
|-------------|------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `id`        | [`uuid`]         | The ID of the peek request.                                                                                                                                                                                                                                                                                           |
| `object_id` | [`text`]         | The ID of the collection the peek is targeting. Corresponds to [`mz_catalog.mz_indexes.id`](../mz_catalog#mz_indexes), [`mz_catalog.mz_materialized_views.id`](../mz_catalog#mz_materialized_views), [`mz_catalog.mz_sources.id`](../mz_catalog#mz_sources), or [`mz_catalog.mz_tables.id`](../mz_catalog#mz_tables). |
| `type`      | [`text`]         | The type of the corresponding peek: `index` if targeting an index or temporary dataflow; `persist` for a source, materialized view, or table.                                                                                                                                                                         |
| `time`      | [`mz_timestamp`] | The timestamp the peek has requested.                                                                                                                                                                                                                                                                                 |

<!-- RELATION_SPEC_UNDOCUMENTED mz_introspection.mz_active_peeks_per_worker -->

## `mz_arrangement_sharing`

The `mz_arrangement_sharing` view describes how many times each [arrangement] in the system is used.

<!-- RELATION_SPEC mz_introspection.mz_arrangement_sharing -->
| Field          | Type       | Meaning                                                                                                                   |
| -------------- |------------| --------                                                                                                                  |
| `operator_id`  | [`uint8`]  | The ID of the operator that created the arrangement. Corresponds to [`mz_dataflow_operators.id`](#mz_dataflow_operators). |
| `count`        | [`bigint`] | The number of operators that share the arrangement.                                                                       |

<!-- RELATION_SPEC_UNDOCUMENTED mz_introspection.mz_arrangement_sharing_per_worker -->
<!-- RELATION_SPEC_UNDOCUMENTED mz_introspection.mz_arrangement_sharing_raw -->

## `mz_arrangement_sizes`

The `mz_arrangement_sizes` view describes the size of each [arrangement] in the system.

The size, capacity, and allocations are an approximation, which may underestimate the actual size in memory.
Specifically, reductions can use more memory than we show here.

<!-- RELATION_SPEC mz_introspection.mz_arrangement_sizes -->
| Field         | Type       | Meaning                                                                                                                   |
|---------------|------------| --------                                                                                                                  |
| `operator_id` | [`uint8`]  | The ID of the operator that created the arrangement. Corresponds to [`mz_dataflow_operators.id`](#mz_dataflow_operators). |
| `records`     | [`bigint`] | The number of records in the arrangement.                                                                                 |
| `batches`     | [`bigint`] | The number of batches in the arrangement.                                                                                 |
| `size`        | [`bigint`] | The utilized size in bytes of the arrangement.                                                                            |
| `capacity`    | [`bigint`] | The capacity in bytes of the arrangement. Can be larger than the size.                                                    |
| `allocations` | [`bigint`] | The number of separate memory allocations backing the arrangement.                                                        |

<!-- RELATION_SPEC_UNDOCUMENTED mz_introspection.mz_arrangement_sizes_per_worker -->
<!-- RELATION_SPEC_UNDOCUMENTED mz_introspection.mz_arrangement_records_raw -->
<!-- RELATION_SPEC_UNDOCUMENTED mz_introspection.mz_arrangement_batcher_allocations_raw -->
<!-- RELATION_SPEC_UNDOCUMENTED mz_introspection.mz_arrangement_batcher_capacity_raw -->
<!-- RELATION_SPEC_UNDOCUMENTED mz_introspection.mz_arrangement_batcher_records_raw -->
<!-- RELATION_SPEC_UNDOCUMENTED mz_introspection.mz_arrangement_batcher_size_raw -->
<!-- RELATION_SPEC_UNDOCUMENTED mz_introspection.mz_arrangement_batches_raw -->
<!-- RELATION_SPEC_UNDOCUMENTED mz_introspection.mz_arrangement_heap_allocations_raw -->
<!-- RELATION_SPEC_UNDOCUMENTED mz_introspection.mz_arrangement_heap_capacity_raw -->
<!-- RELATION_SPEC_UNDOCUMENTED mz_introspection.mz_arrangement_heap_size_raw -->

## `mz_compute_error_counts`

The `mz_compute_error_counts` view describes the counts of errors in objects exported by [dataflows][dataflow] in the system.

Dataflow exports that don't have any errors are not included in this view.

<!-- RELATION_SPEC mz_introspection.mz_compute_error_counts -->
| Field        | Type        | Meaning                                                                                              |
| ------------ |-------------| --------                                                                                             |
| `export_id`  | [`text`]    | The ID of the dataflow export. Corresponds to [`mz_compute_exports.export_id`](#mz_compute_exports). |
| `count`      | [`numeric`] | The count of errors present in this dataflow export.                                                 |

<!-- RELATION_SPEC_UNDOCUMENTED mz_introspection.mz_compute_error_counts_per_worker -->
<!-- RELATION_SPEC_UNDOCUMENTED mz_introspection.mz_compute_error_counts_raw -->

## `mz_compute_exports`

The `mz_compute_exports` view describes the objects exported by [dataflows][dataflow] in the system.

<!-- RELATION_SPEC mz_introspection.mz_compute_exports -->
| Field          | Type      | Meaning                                                                                                                                                                                                                                                                                        |
| -------------- |-----------| --------                                                                                                                                                                                                                                                                                       |
| `export_id`    | [`text`]  | The ID of the index, materialized view, or subscription exported by the dataflow. Corresponds to [`mz_catalog.mz_indexes.id`](../mz_catalog#mz_indexes), [`mz_catalog.mz_materialized_views.id`](../mz_catalog#mz_materialized_views), or [`mz_internal.mz_subscriptions`](../mz_internal#mz_subscriptions). |
| `dataflow_id`  | [`uint8`] | The ID of the dataflow. Corresponds to [`mz_dataflows.id`](#mz_dataflows).                                                                                                                                                                                                               |

<!-- RELATION_SPEC_UNDOCUMENTED mz_introspection.mz_compute_exports_per_worker -->

## `mz_compute_frontiers`

The `mz_compute_frontiers` view describes the frontier of each [dataflow] export in the system.
The frontier describes the earliest timestamp at which the output of the dataflow may change; data prior to that timestamp is sealed.

<!-- RELATION_SPEC mz_introspection.mz_compute_frontiers -->
| Field        | Type               | Meaning                                                                                              |
| ------------ | ------------------ | --------                                                                                             |
| `export_id`  | [`text`]           | The ID of the dataflow export. Corresponds to [`mz_compute_exports.export_id`](#mz_compute_exports). |
| `time`       | [`mz_timestamp`]   | The next timestamp at which the dataflow output may change.                                          |

<!-- RELATION_SPEC_UNDOCUMENTED mz_introspection.mz_compute_frontiers_per_worker -->

## `mz_compute_import_frontiers`

The `mz_compute_import_frontiers` view describes the frontiers of each [dataflow] import in the system.
The frontier describes the earliest timestamp at which the input into the dataflow may change; data prior to that timestamp is sealed.

<!-- RELATION_SPEC mz_introspection.mz_compute_import_frontiers -->
| Field        | Type               | Meaning                                                                                                                                                                                                                |
| ------------ | ------------------ | --------                                                                                                                                                                                                               |
| `export_id`  | [`text`]           | The ID of the dataflow export. Corresponds to [`mz_compute_exports.export_id`](#mz_compute_exports).                                                                                                                   |
| `import_id`  | [`text`]           | The ID of the dataflow import. Corresponds to [`mz_catalog.mz_sources.id`](../mz_catalog#mz_sources) or [`mz_catalog.mz_tables.id`](../mz_catalog#mz_tables) or [`mz_compute_exports.export_id`](#mz_compute_exports). |
| `time`       | [`mz_timestamp`]   | The next timestamp at which the dataflow input may change.                                                                                                                                                             |

<!-- RELATION_SPEC_UNDOCUMENTED mz_introspection.mz_compute_import_frontiers_per_worker -->

## `mz_compute_operator_durations_histogram`

The `mz_compute_operator_durations_histogram` view describes a histogram of the duration in nanoseconds of each invocation for each [dataflow] operator.

<!-- RELATION_SPEC mz_introspection.mz_compute_operator_durations_histogram -->
| Field          | Type        | Meaning                                                                                      |
| -------------- |-------------| --------                                                                                     |
| `id`           | [`uint8`]   | The ID of the operator. Corresponds to [`mz_dataflow_operators.id`](#mz_dataflow_operators). |
| `duration_ns`  | [`uint8`]   | The upper bound of the duration bucket in nanoseconds.                                       |
| `count`        | [`numeric`] | The (noncumulative) count of invocations in the bucket.                                      |

<!-- RELATION_SPEC_UNDOCUMENTED mz_introspection.mz_compute_operator_durations_histogram_per_worker -->
<!-- RELATION_SPEC_UNDOCUMENTED mz_introspection.mz_compute_operator_durations_histogram_raw -->

## `mz_dataflows`

The `mz_dataflows` view describes the [dataflows][dataflow] in the system.

<!-- RELATION_SPEC mz_introspection.mz_dataflows -->
| Field       | Type      | Meaning                                |
| ----------- |-----------| --------                               |
| `id`        | [`uint8`] | The ID of the dataflow.                |
| `name`      | [`text`]  | The internal name of the dataflow.     |

<!-- RELATION_SPEC_UNDOCUMENTED mz_introspection.mz_dataflows_per_worker -->

## `mz_dataflow_addresses`

The `mz_dataflow_addresses` view describes how the [dataflow] channels and operators in the system are nested into scopes.

<!-- RELATION_SPEC mz_introspection.mz_dataflow_addresses -->
| Field        | Type            | Meaning                                                                                                                                                       |
| ------------ |-----------------| --------                                                                                                                                                      |
| `id`         | [`uint8`]       | The ID of the channel or operator. Corresponds to [`mz_dataflow_channels.id`](#mz_dataflow_channels) or [`mz_dataflow_operators.id`](#mz_dataflow_operators). |
| `address`    | [`bigint list`] | A list of scope-local indexes indicating the path from the root to this channel or operator.                                                                  |

<!-- RELATION_SPEC_UNDOCUMENTED mz_introspection.mz_dataflow_addresses_per_worker -->

## `mz_dataflow_arrangement_sizes`

The `mz_dataflow_arrangement_sizes` view describes the size of arrangements per
operators under each dataflow.

<!-- RELATION_SPEC mz_introspection.mz_dataflow_arrangement_sizes -->
| Field         | Type       | Meaning                                                                      |
|---------------|------------|------------------------------------------------------------------------------|
| `id`          | [`uint8`]  | The ID of the [dataflow]. Corresponds to [`mz_dataflows.id`](#mz_dataflows). |
| `name`        | [`text`]   | The name of the [dataflow].                                                  |
| `records`     | [`bigint`] | The number of records in all arrangements in the dataflow.                   |
| `batches`     | [`bigint`] | The number of batches in all arrangements in the dataflow.                   |
| `size`        | [`bigint`] | The utilized size in bytes of the arrangements.                              |
| `capacity`    | [`bigint`] | The capacity in bytes of the arrangements. Can be larger than the size.      |
| `allocations` | [`bigint`] | The number of separate memory allocations backing the arrangements.          |

## `mz_dataflow_channels`

The `mz_dataflow_channels` view describes the communication channels between [dataflow] operators.
A communication channel connects one of the outputs of a source operator to one of the inputs of a target operator.

<!-- RELATION_SPEC mz_introspection.mz_dataflow_channels -->
| Field            | Type      | Meaning                                                                                                                 |
| ---------------- |-----------| --------                                                                                                                |
| `id`             | [`uint8`] | The ID of the channel.                                                                                                  |
| `from_index`     | [`uint8`] | The scope-local index of the source operator. Corresponds to [`mz_dataflow_addresses.address`](#mz_dataflow_addresses). |
| `from_port`      | [`uint8`] | The source operator's output port.                                                                                      |
| `to_index`       | [`uint8`] | The scope-local index of the target operator. Corresponds to [`mz_dataflow_addresses.address`](#mz_dataflow_addresses). |
| `to_port`        | [`uint8`] | The target operator's input port.                                                                                       |
| `type`           | [`text`]  | The container type of the channel.                                                                                      |

<!-- RELATION_SPEC_UNDOCUMENTED mz_introspection.mz_dataflow_channels_per_worker -->

## `mz_dataflow_channel_operators`

The `mz_dataflow_channel_operators` view associates [dataflow] channels with the operators that are their endpoints.

<!-- RELATION_SPEC mz_introspection.mz_dataflow_channel_operators -->
| Field                   | Type           | Meaning                                                                                                             |
|-------------------------|----------------|---------------------------------------------------------------------------------------------------------------------|
| `id`                    | [`uint8`]      | The ID of the channel. Corresponds to [`mz_dataflow_channels.id`](#mz_dataflow_channels).                           |
| `from_operator_id`      | [`uint8`]      | The ID of the source of the channel. Corresponds to [`mz_dataflow_operators.id`](#mz_dataflow_operators).           |
| `from_operator_address` | [`uint8 list`] | The address of the source of the channel. Corresponds to [`mz_dataflow_addresses.address`](#mz_dataflow_addresses). |
| `to_operator_id`        | [`uint8`]      | The ID of the target of the channel. Corresponds to [`mz_dataflow_operators.id`](#mz_dataflow_operators).           |
| `to_operator_address`   | [`uint8 list`] | The address of the target of the channel. Corresponds to [`mz_dataflow_addresses.address`](#mz_dataflow_addresses). |
| `type`                  | [`text`]  | The container type of the channel.                                                                                       |

<!-- RELATION_SPEC_UNDOCUMENTED mz_introspection.mz_dataflow_channel_operators_per_worker -->

## `mz_dataflow_global_ids`

The `mz_dataflow_global_ids` view associates [dataflow] ids with global ids (ids of the form `u8` or `t5`).

<!-- RELATION_SPEC mz_introspection.mz_dataflow_global_ids -->

| Field        | Type      | Meaning                                    |
|------------- | -------   | --------                                   |
| `id`         | [`uint8`] | The dataflow ID.                           |
| `global_id`  | [`text`]  | A global ID associated with that dataflow. |

<!-- RELATION_SPEC_UNDOCUMENTED mz_introspection.mz_compute_dataflow_global_ids_per_worker -->

## `mz_dataflow_operators`

The `mz_dataflow_operators` view describes the [dataflow] operators in the system.

<!-- RELATION_SPEC mz_introspection.mz_dataflow_operators -->
| Field        | Type      | Meaning                            |
| ------------ |-----------| --------                           |
| `id`         | [`uint8`] | The ID of the operator.            |
| `name`       | [`text`]  | The internal name of the operator. |

<!-- RELATION_SPEC_UNDOCUMENTED mz_introspection.mz_dataflow_operators_per_worker -->

## `mz_dataflow_operator_dataflows`

The `mz_dataflow_operator_dataflows` view describes the [dataflow] to which each operator belongs.

<!-- RELATION_SPEC mz_introspection.mz_dataflow_operator_dataflows -->
| Field            | Type      | Meaning                                                                                         |
| ---------------- |-----------| --------                                                                                        |
| `id`             | [`uint8`] | The ID of the operator. Corresponds to [`mz_dataflow_operators.id`](#mz_dataflow_operators).    |
| `name`           | [`text`]  | The internal name of the operator.                                                              |
| `dataflow_id`    | [`uint8`] | The ID of the dataflow hosting the operator. Corresponds to [`mz_dataflows.id`](#mz_dataflows). |
| `dataflow_name`  | [`text`]  | The internal name of the dataflow hosting the operator.                                         |

<!-- RELATION_SPEC_UNDOCUMENTED mz_introspection.mz_dataflow_operator_dataflows_per_worker -->

## `mz_dataflow_operator_parents`

The `mz_dataflow_operator_parents` view describes how [dataflow] operators are nested into scopes, by relating operators to their parent operators.

<!-- RELATION_SPEC mz_introspection.mz_dataflow_operator_parents -->
| Field        | Type      | Meaning                                                                                                        |
| ------------ |-----------| --------                                                                                                       |
| `id`         | [`uint8`] | The ID of the operator. Corresponds to [`mz_dataflow_operators.id`](#mz_dataflow_operators).                   |
| `parent_id`  | [`uint8`] | The ID of the operator's parent operator. Corresponds to [`mz_dataflow_operators.id`](#mz_dataflow_operators). |

<!-- RELATION_SPEC_UNDOCUMENTED mz_introspection.mz_dataflow_operator_parents_per_worker -->

## `mz_dataflow_shutdown_durations_histogram`

The `mz_dataflow_shutdown_durations_histogram` view describes a histogram of the time in nanoseconds required to fully shut down dropped [dataflows][dataflow].

<!-- RELATION_SPEC mz_introspection.mz_dataflow_shutdown_durations_histogram -->
| Field          | Type        | Meaning                                                |
| -------------- |-------------| --------                                               |
| `duration_ns`  | [`uint8`]   | The upper bound of the bucket in nanoseconds.          |
| `count`        | [`numeric`] | The (noncumulative) count of dataflows in this bucket. |

<!-- RELATION_SPEC_UNDOCUMENTED mz_introspection.mz_dataflow_shutdown_durations_histogram_per_worker -->
<!-- RELATION_SPEC_UNDOCUMENTED mz_introspection.mz_dataflow_shutdown_durations_histogram_raw -->

## `mz_expected_group_size_advice`

The `mz_expected_group_size_advice` view provides advice on opportunities to set [query hints].
Query hints are applicable to dataflows maintaining [`MIN`], [`MAX`], or [Top K] query patterns.
The maintainance of these query patterns is implemented inside an operator scope, called a region,
through a hierarchical scheme for either aggregation or Top K computations.

<!-- RELATION_SPEC mz_introspection.mz_expected_group_size_advice -->
| Field           | Type                 | Meaning                                                                                                   |
|-----------------|----------------------|-----------------------------------------------------------------------------------------------------------|
| `dataflow_id`   | [`uint8`]            | The ID of the [dataflow]. Corresponds to [`mz_dataflows.id`](#mz_dataflows).                              |
| `dataflow_name` | [`text`]             | The internal name of the dataflow hosting the min/max aggregation or Top K.                               |
| `region_id`     | [`uint8`]            | The ID of the root operator scope. Corresponds to [`mz_dataflow_operators.id`](#mz_dataflow_operators).   |
| `region_name`   | [`text`]             | The internal name of the root operator scope for the min/max aggregation or Top K.                        |
| `levels`        | [`bigint`]           | The number of levels in the hierarchical scheme implemented by the region.                                |
| `to_cut`        | [`bigint`]           | The number of levels that can be eliminated (cut) from the region's hierarchy.                            |
| `savings`       | [`numeric`]          | A conservative estimate of the amount of memory in bytes to be saved by applying the hint.                |
| `hint`          | [`double precision`] | The hint value that will eliminate `to_cut` levels from the region's hierarchy.                           |

## `mz_mappable_objects`

The `mz_mappable_objects` identifies indexes (and their underlying views) and materialized views which can be debugged using the [`mz_lir_mapping`](#mz_lir_mapping) view.

<!-- RELATION_SPEC mz_introspection.mz_mappable_objects -->
| Field        | Type      | Meaning
| ------------ | --------  | -----------
| `name`       | [`text`]  | The name of the object.
| `global_id`  | [`text`]  | The global ID of the object.

See [Which part of my query runs slowly or uses a lot of memory?](/transform-data/troubleshooting/#which-part-of-my-query-runs-slowly-or-uses-a-lot-of-memory) for examples of debugging with `mz_mappable_objects` and `mz_lir_mapping`.

## `mz_lir_mapping`

The `mz_lir_mapping` view describes the low-level internal representation (LIR) plan that corresponds to global ids of indexes (and their underlying views) and materialized views.
You can find a list of all debuggable objects in [`mz_mappable_objects`](#mz_mappable_objects).
LIR is a higher-level representation than dataflows; this view is used for profiling and debugging indices and materialized views.
Note that LIR is not a stable interface and may change at any time.
In particular, you should not attempt to parse `operator` descriptions.
LIR nodes are implemented by zero or more dataflow operators with sequential ids.
We use the range `[operator_id_start, operator_id_end)` to record this information.
If an LIR node was implemented without any dataflow operators, `operator_id_start` will be equal to `operator_id_end`.

<!-- RELATION_SPEC mz_introspection.mz_lir_mapping -->
| Field               | Type      | Meaning
| ---------           | --------  | -----------
| `global_id`         | [`text`]  | The global ID.
| `lir_id`            | [`uint8`] | The LIR node ID.
| `operator`          | [`text`]  | The LIR operator, in the format `OperatorName INPUTS [OPTIONS]`.
| `parent_lir_id`     | [`uint8`] | The parent of this LIR node. May be `NULL`.
| `nesting`           | [`uint2`] | The nesting level of this LIR node.
| `operator_id_start` | [`uint8`] | The first dataflow operator ID implementing this LIR operator (inclusive).
| `operator_id_end`   | [`uint8`] | The first dataflow operator ID _after_ this LIR operator (exclusive).

<!-- RELATION_SPEC_UNDOCUMENTED mz_introspection.mz_compute_lir_mapping_per_worker -->

## `mz_message_counts`

The `mz_message_counts` view describes the messages and message batches sent and received over the [dataflow] channels in the system.
It distinguishes between individual records (`sent`, `received`) and batches of records (`batch_sent`, `batch_sent`).

<!-- RELATION_SPEC mz_introspection.mz_message_counts -->
| Field              | Type        | Meaning                                                                                   |
| ------------------ |-------------| --------                                                                                  |
| `channel_id`       | [`uint8`]   | The ID of the channel. Corresponds to [`mz_dataflow_channels.id`](#mz_dataflow_channels). |
| `sent`             | [`numeric`] | The number of messages sent.                                                              |
| `received`         | [`numeric`] | The number of messages received.                                                          |
| `batch_sent`       | [`numeric`] | The number of batches sent.                                                               |
| `batch_received`   | [`numeric`] | The number of batches received.                                                           |

<!-- RELATION_SPEC_UNDOCUMENTED mz_introspection.mz_message_counts_per_worker -->
<!-- RELATION_SPEC_UNDOCUMENTED mz_introspection.mz_message_batch_counts_received_raw -->
<!-- RELATION_SPEC_UNDOCUMENTED mz_introspection.mz_message_batch_counts_sent_raw -->
<!-- RELATION_SPEC_UNDOCUMENTED mz_introspection.mz_message_counts_received_raw -->
<!-- RELATION_SPEC_UNDOCUMENTED mz_introspection.mz_message_counts_sent_raw -->

## `mz_peek_durations_histogram`

The `mz_peek_durations_histogram` view describes a histogram of the duration in nanoseconds of read queries ("peeks") in the [dataflow] layer.

<!-- RELATION_SPEC mz_introspection.mz_peek_durations_histogram -->
| Field         | Type        | Meaning                                            |
|---------------|-------------|----------------------------------------------------|
| `type`        | [`text`]    | The peek variant: `index` or `persist`.            |
| `duration_ns` | [`uint8`]   | The upper bound of the bucket in nanoseconds.      |
| `count`       | [`numeric`] | The (noncumulative) count of peeks in this bucket. |

<!-- RELATION_SPEC_UNDOCUMENTED mz_introspection.mz_peek_durations_histogram_per_worker -->
<!-- RELATION_SPEC_UNDOCUMENTED mz_introspection.mz_peek_durations_histogram_raw -->

## `mz_records_per_dataflow`

The `mz_records_per_dataflow` view describes the number of records in each [dataflow].

<!-- RELATION_SPEC mz_introspection.mz_records_per_dataflow -->
| Field         | Type       | Meaning                                                                    |
| ------------  |------------| --------                                                                   |
| `id`          | [`uint8`]  | The ID of the dataflow. Corresponds to [`mz_dataflows.id`](#mz_dataflows). |
| `name`        | [`text`]   | The internal name of the dataflow.                                         |
| `records`     | [`bigint`] | The number of records in the dataflow.                                     |
| `batches`     | [`bigint`] | The number of batches in the dataflow.                                     |
| `size`        | [`bigint`] | The utilized size in bytes of the arrangements.                            |
| `capacity`    | [`bigint`] | The capacity in bytes of the arrangements. Can be larger than the size.    |
| `allocations` | [`bigint`] | The number of separate memory allocations backing the arrangements.        |

<!-- RELATION_SPEC_UNDOCUMENTED mz_introspection.mz_records_per_dataflow_per_worker -->

## `mz_records_per_dataflow_operator`

The `mz_records_per_dataflow_operator` view describes the number of records in each [dataflow] operator in the system.

<!-- RELATION_SPEC mz_introspection.mz_records_per_dataflow_operator -->
| Field          | Type       | Meaning                                                                                      |
| -------------- |------------| --------                                                                                     |
| `id`           | [`uint8`]  | The ID of the operator. Corresponds to [`mz_dataflow_operators.id`](#mz_dataflow_operators). |
| `name`         | [`text`]   | The internal name of the operator.                                                           |
| `dataflow_id`  | [`uint8`]  | The ID of the dataflow. Corresponds to [`mz_dataflows.id`](#mz_dataflows).                   |
| `records`      | [`bigint`] | The number of records in the operator.                                                       |
| `batches`      | [`bigint`] | The number of batches in the dataflow.                                                       |
| `size`         | [`bigint`] | The utilized size in bytes of the arrangement.                                               |
| `capacity`     | [`bigint`] | The capacity in bytes of the arrangement. Can be larger than the size.                       |
| `allocations`  | [`bigint`] | The number of separate memory allocations backing the arrangement.                           |

<!-- RELATION_SPEC_UNDOCUMENTED mz_introspection.mz_records_per_dataflow_operator_per_worker -->

## `mz_scheduling_elapsed`

The `mz_scheduling_elapsed` view describes the total amount of time spent in each [dataflow] operator.

<!-- RELATION_SPEC mz_introspection.mz_scheduling_elapsed -->
| Field         | Type        | Meaning                                                                                      |
| ------------- |-------------| --------                                                                                     |
| `id`          | [`uint8`]   | The ID of the operator. Corresponds to [`mz_dataflow_operators.id`](#mz_dataflow_operators). |
| `elapsed_ns`  | [`numeric`] | The total elapsed time spent in the operator in nanoseconds.                                 |

<!-- RELATION_SPEC_UNDOCUMENTED mz_introspection.mz_scheduling_elapsed_per_worker -->
<!-- RELATION_SPEC_UNDOCUMENTED mz_introspection.mz_scheduling_elapsed_raw -->

## `mz_scheduling_parks_histogram`

The `mz_scheduling_parks_histogram` view describes a histogram of [dataflow] worker park events. A park event occurs when a worker has no outstanding work.

<!-- RELATION_SPEC mz_introspection.mz_scheduling_parks_histogram -->
| Field           | Type        | Meaning                                                  |
| --------------- |-------------| -------                                                  |
| `slept_for_ns`  | [`uint8`]   | The actual length of the park event in nanoseconds.      |
| `requested_ns`  | [`uint8`]   | The requested length of the park event in nanoseconds.   |
| `count`         | [`numeric`] | The (noncumulative) count of park events in this bucket. |

<!-- RELATION_SPEC_UNDOCUMENTED mz_introspection.mz_scheduling_parks_histogram_per_worker -->
<!-- RELATION_SPEC_UNDOCUMENTED mz_introspection.mz_scheduling_parks_histogram_raw -->

[`bigint`]: /sql/types/bigint
[`bigint list`]: /sql/types/list
[`double precision`]: /sql/types/double-precision
[`mz_timestamp`]: /sql/types/mz_timestamp
[`numeric`]: /sql/types/numeric
[`text`]: /sql/types/text
[`uuid`]: /sql/types/uuid
[`uint2`]: /sql/types/uint2
[`uint8`]: /sql/types/uint8
[`uint8 list`]: /sql/types/list
[arrangement]: /get-started/arrangements/#arrangements
[dataflow]: /get-started/arrangements/#dataflows
[`MIN`]: /sql/functions/#min
[`MAX`]: /sql/functions/#max
[Top K]: /transform-data/patterns/top-k
[query hints]: /sql/select/#query-hints

<!-- RELATION_SPEC_UNDOCUMENTED mz_introspection.mz_compute_hydration_times_per_worker -->
<!-- RELATION_SPEC_UNDOCUMENTED mz_introspection.mz_compute_operator_hydration_statuses_per_worker -->
<!-- RELATION_SPEC_UNDOCUMENTED mz_introspection.mz_dataflow_operator_reachability -->
<!-- RELATION_SPEC_UNDOCUMENTED mz_introspection.mz_dataflow_operator_reachability_per_worker -->
<!-- RELATION_SPEC_UNDOCUMENTED mz_introspection.mz_dataflow_operator_reachability_raw -->

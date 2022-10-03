---
title: "mz_internal"
description: "mz_internal is a system catalog that presents metadata about Materialize in an unstable format that is likely to change."
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

### `mz_arrangement_sharing`

The `mz_arrangement_sharing` source describes how many times each [arrangement]
in the system is used.

Field         | Type       | Meaning
--------------|------------|--------
`operator_id` | [`bigint`] | The ID of the operator that created the arrangement. Corresponds to [`mz_dataflow_operators.id`](#mz_dataflow_operators).
`worker_id`   | [`bigint`] | The ID of the worker thread hosting the arrangement.
`count`       | [`bigint`] | The number of operators that share the arrangement.

### `mz_arrangement_sizes`

The `mz_arrangement_sizes` source describes the size of each [arrangement] in
the system.

Field         | Type       | Meaning
--------------|------------|--------
`operator_id` | [`bigint`] | The ID of the operator that created the arrangement. Corresponds to [`mz_dataflow_operators.id`](#mz_dataflow_operators).
`worker_id`   | [`bigint`] | The ID of the worker thread hosting the arrangement.
`records`     | [`bigint`] | The number of records in the arrangement.
`batches`     | [`bigint`] | The number of batches in the arrangement.

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

The `mz_compute_exports` source describes the dataflows created by indexes and materialized views in the system.

Field       | Type       | Meaning
------------|------------|--------
`export_id` | [`text`]   | The ID of the index or materialized view that created the dataflow. Corresponds to [`mz_catalog.mz_indexes.id`](../mz_catalog#mz_indexes) or [`mz_catalog.mz_materialized_views.id`](../mz_catalog#mz_materialized_views).
`worker_id` | [`bigint`] | The ID of the worker thread hosting the corresponding [dataflow].

### `mz_compute_frontiers`

The `mz_compute_frontiers` view describes the frontier for each
[dataflow] in the system across all workers. The frontier describes the earliest
timestamp at which the output of the dataflow may change; data prior to that
timestamp is sealed.

For per-worker frontier information, see
[`mz_worker_compute_frontiers`](#mz_worker_compute_frontiers).

Field        | Type       | Meaning
-------------|------------|--------
`export_id ` | [`text`]   | The ID of the index or materialized view that created the dataflow. Corresponds to [`mz_compute_exports.export_id`](#mz_compute_exports).
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
`export_id` | [`text`]   | The ID of the index or materialized view that created the dataflow. Corresponds to [`mz_compute_exports.export_id`](#mz_compute_exports).
`import_id` | [`text`]   | The ID of the input source object for the dataflow. Corresponds to either [`mz_catalog.mz_sources.id`](../mz_catalog#mz_sources) or [`mz_catalog.mz_tables.id`](../mz_catalog#mz_tables) or [`mz_catalog.mz_materialized_views.id`](../mz_catalog#mz_materialized_views).
`time`      | [`mz_timestamp`] | The next timestamp at which the source instantiation may change.

### `mz_message_counts`

The `mz_message_counts` source describes the messages sent and received over the
dataflow channels in the system.

Field             | Type       | Meaning
------------------|------------|--------
`channel_id`      | [`bigint`] | The ID of the channel. Corresponds to [`mz_dataflow_channels.id`](#mz_dataflow_channels).
`from_worker_id`  | [`bigint`] | The ID of the worker thread sending the message.
`to_worker_id`    | [`bigint`] | The ID of the worker thread receiving the message.
`sent`            | [`bigint`] | The number of messages sent.
`received`        | [`bigint`] | The number of messages received.

### `mz_peek_active`

The `mz_peek_active` source describes all read queries ("peeks") that are
pending in the dataflow layer.

Field      | Type       | Meaning
-----------|------------|--------
`id`       | [`uuid`]   | The ID of the peek request.
`worker_id`| [`bigint`] | The ID of the worker thread servicing the peek.
`index_id` | [`text`]   | The ID of the index the peek is targeting.
`time`     | [`mz_timestamp`] | The timestamp the peek has requested.

### `mz_peek_durations`

The `mz_peek_durations` source describes a histogram of the duration of read
queries ("peeks") in the dataflow layer.

Field         | Type       | Meaning
--------------|------------|--------
`worker_id`   | [`bigint`] | The ID of the worker thread servicing the peek.
`duration_ns` | [`bigint`] | The upper bound of the bucket in nanoseconds.
`count`       | [`bigint`] | The (noncumulative) count of peeks in this bucket.

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

The `mz_scheduling_elapsed` source describes the total amount of time spent in
each [dataflow] operator.

Field        | Type       | Meaning
-------------|------------|--------
`id`         | [`bigint`] | The ID of the operator. Corresponds to [`mz_dataflow_operators.id`](#mz_dataflow_operators).
`worker_id`  | [`bigint`] | The ID of the worker thread hosting the operator.
`elapsed_ns` | [`bigint`] | The total elapsed time spent in the operator in nanoseconds.

### `mz_scheduling_histogram`

The `mz_scheduling_histogram` source stores a histogram describing the
duration of each invocation for each [dataflow] operator.

Field         | Type       | Meaning
--------------|------------|--------
`id`          | [`bigint`] | The ID of the operator. Corresponds to [`mz_dataflow_operators.id`](#mz_dataflow_operators).
`worker_id`   | [`bigint`] | The ID of the worker thread hosting the operator.
`duration_ns` | [`bigint`] | The upper bound of the bucket.
`count`       | [`bigint`] | The number of recordings in the bucket.

### `mz_scheduling_parks`

The `mz_scheduling_parks` source stores a histogram describing [dataflow] worker
park events. A park event occurs when a worker has no outstanding work.

Field       | Type       | Meaning
------------|------------| -------
`worker_id` | [`bigint`] | The ID of the worker thread.
`slept_for` | [`bigint`] | The actual length of the park event.
`requested` | [`bigint`] | The requested length of the park event.
`count`     | [`bigint`] | The number of park events in this bucket.

### `mz_worker_compute_delays`

The `mz_worker_compute_delays` source provides, for each worker,
a histogram of wall-clock delays between observations of source object frontier
advancements at the dataflow layer and the advancements of the corresponding
[dataflow] frontiers.

Field       | Type       | Meaning
------------|------------|--------
`export_id` | [`text`]   | The ID of the index or materialized view that created the dataflow. Corresponds to [`mz_compute_exports.export_id`](#mz_compute_exports).
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
`export_id` | [`text`]   | The ID of the index or materialized view that created the dataflow. Corresponds to [`mz_compute_exports.export_id`](#mz_compute_exports).
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
`export_id` | [`text`]   | The ID of the index or materialized view that created the dataflow. Corresponds to [`mz_compute_exports.export_id`](#mz_compute_exports).
`source_id` | [`text`]   | The ID of the input source object for the dataflow. Corresponds to either [`mz_catalog.mz_sources.id`](../mz_catalog#mz_sources) or [`mz_catalog.mz_tables.id`](../mz_catalog#mz_tables) or [`mz_catalog.mz_materialized_views.id`](../mz_catalog#mz_materialized_views).
`worker_id` | [`bigint`] | The ID of the worker thread hosting the dataflow.
`time`      | [`mz_timestamp`] | The next timestamp at which the dataflow may change.

[`bigint`]: /sql/types/bigint
[`bigint list`]: /sql/types/list
[`boolean`]: /sql/types/boolean
[`bytea`]: /sql/types/bytea
[`double precision`]: /sql/types/double-precision
[`jsonb`]: /sql/types/jsonb
[`mz_timestamp`]: /sql/types/mz_timestamp
[`numeric`]: /sql/types/numeric
[`oid`]: /sql/types/oid
[`text`]: /sql/types/text
[`timestamp`]: /sql/types/timestamp
[`timestamp with time zone`]: /sql/types/timestamp
[`uuid`]: /sql/types/uuid
[gh-issue]: https://github.com/MaterializeInc/materialize/issues/new?labels=C-feature&template=feature.md
[oid]: /sql/types/oid
[`text array`]: /sql/types/array
[arrangement]: /overview/arrangements/#arrangements
[dataflow]: /overview/arrangements/#dataflows
[`record`]: /sql/types/record
[librdkafka]: https://github.com/edenhill/librdkafka/tree/v{{< librdkafka-version >}}
[`STATISTICS.md`]: https://github.com/edenhill/librdkafka/tree/v{{< librdkafka-version >}}/STATISTICS.md

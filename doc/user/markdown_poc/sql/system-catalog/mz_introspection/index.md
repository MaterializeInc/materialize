<div class="content" role="main">

<img
src="data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIGNsYXNzPSJpb25pY29uIiB2aWV3Ym94PSIwIDAgNTEyIDUxMiI+CiAgICAgICAgICAgIDx0aXRsZT5BcnJvdyBQb2ludGluZyB0byB0aGUgbGVmdDwvdGl0bGU+CiAgICAgICAgICAgIDxwYXRoIGZpbGw9Im5vbmUiIHN0cm9rZT0iY3VycmVudENvbG9yIiBzdHJva2UtbGluZWNhcD0icm91bmQiIHN0cm9rZS1saW5lam9pbj0icm91bmQiIHN0cm9rZS13aWR0aD0iNDgiIGQ9Ik0zMjggMTEyTDE4NCAyNTZsMTQ0IDE0NCIgLz4KICAgICAgICAgIDwvc3ZnPg=="
class="ionicon" /> All Topics

<div>

<div class="breadcrumb">

[Home](/docs/)  /  [SQL commands](/docs/sql/)  /  [System
catalog](/docs/sql/system-catalog/)

</div>

# mz_introspection

The following sections describe the available objects in the
`mz_introspection` schema.

<div class="warning">

**WARNING!** The objects in the `mz_introspection` schema are not part
of Materialize’s stable interface. Backwards-incompatible changes to
these objects may be made at any time.

</div>

<div class="warning">

**WARNING!** `SELECT` statements may reference these objects, but
creating views that reference these objects is not allowed.

</div>

Introspection relations are maintained by independently collecting
internal logging information within each of the replicas of a cluster.
Thus, in a multi-replica cluster, queries to these relations need to be
directed to a specific replica by issuing the command
`SET cluster_replica = <replica_name>`. Note that once this command is
issued, all subsequent `SELECT` queries, for introspection relations or
not, will be directed to the targeted replica. Replica targeting can be
cancelled by issuing the command `RESET cluster_replica`.

For each of the below introspection relations, there exists also a
variant with a `_per_worker` name suffix. Per-worker relations expose
the same data as their global counterparts, but have an extra
`worker_id` column that splits the information by Timely Dataflow
worker.

## `mz_active_peeks`

The `mz_active_peeks` view describes all read queries (“peeks”) that are
pending in the [dataflow](/docs/get-started/arrangements/#dataflows)
layer.

| Field | Type | Meaning |
|----|----|----|
| `id` | [`uuid`](/docs/sql/types/uuid) | The ID of the peek request. |
| `object_id` | [`text`](/docs/sql/types/text) | The ID of the collection the peek is targeting. Corresponds to [`mz_catalog.mz_indexes.id`](../mz_catalog#mz_indexes), [`mz_catalog.mz_materialized_views.id`](../mz_catalog#mz_materialized_views), [`mz_catalog.mz_sources.id`](../mz_catalog#mz_sources), or [`mz_catalog.mz_tables.id`](../mz_catalog#mz_tables). |
| `type` | [`text`](/docs/sql/types/text) | The type of the corresponding peek: `index` if targeting an index or temporary dataflow; `persist` for a source, materialized view, or table. |
| `time` | [`mz_timestamp`](/docs/sql/types/mz_timestamp) | The timestamp the peek has requested. |

## `mz_arrangement_sharing`

The `mz_arrangement_sharing` view describes how many times each
[arrangement](/docs/get-started/arrangements/#arrangements) in the
system is used.

| Field | Type | Meaning |
|----|----|----|
| `operator_id` | [`uint8`](/docs/sql/types/uint8) | The ID of the operator that created the arrangement. Corresponds to [`mz_dataflow_operators.id`](#mz_dataflow_operators). |
| `count` | [`bigint`](/docs/sql/types/bigint) | The number of operators that share the arrangement. |

## `mz_arrangement_sizes`

The `mz_arrangement_sizes` view describes the size of each
[arrangement](/docs/get-started/arrangements/#arrangements) in the
system.

The size, capacity, and allocations are an approximation, which may
underestimate the actual size in memory. Specifically, reductions can
use more memory than we show here.

| Field | Type | Meaning |
|----|----|----|
| `operator_id` | [`uint8`](/docs/sql/types/uint8) | The ID of the operator that created the arrangement. Corresponds to [`mz_dataflow_operators.id`](#mz_dataflow_operators). |
| `records` | [`bigint`](/docs/sql/types/bigint) | The number of records in the arrangement. |
| `batches` | [`bigint`](/docs/sql/types/bigint) | The number of batches in the arrangement. |
| `size` | [`bigint`](/docs/sql/types/bigint) | The utilized size in bytes of the arrangement. |
| `capacity` | [`bigint`](/docs/sql/types/bigint) | The capacity in bytes of the arrangement. Can be larger than the size. |
| `allocations` | [`bigint`](/docs/sql/types/bigint) | The number of separate memory allocations backing the arrangement. |

## `mz_compute_error_counts`

The `mz_compute_error_counts` view describes the counts of errors in
objects exported by
[dataflows](/docs/get-started/arrangements/#dataflows) in the system.

Dataflow exports that don’t have any errors are not included in this
view.

| Field | Type | Meaning |
|----|----|----|
| `export_id` | [`text`](/docs/sql/types/text) | The ID of the dataflow export. Corresponds to [`mz_compute_exports.export_id`](#mz_compute_exports). |
| `count` | [`numeric`](/docs/sql/types/numeric) | The count of errors present in this dataflow export. |

## `mz_compute_exports`

The `mz_compute_exports` view describes the objects exported by
[dataflows](/docs/get-started/arrangements/#dataflows) in the system.

| Field | Type | Meaning |
|----|----|----|
| `export_id` | [`text`](/docs/sql/types/text) | The ID of the index, materialized view, or subscription exported by the dataflow. Corresponds to [`mz_catalog.mz_indexes.id`](../mz_catalog#mz_indexes), [`mz_catalog.mz_materialized_views.id`](../mz_catalog#mz_materialized_views), or [`mz_internal.mz_subscriptions`](../mz_internal#mz_subscriptions). |
| `dataflow_id` | [`uint8`](/docs/sql/types/uint8) | The ID of the dataflow. Corresponds to [`mz_dataflows.id`](#mz_dataflows). |

## `mz_compute_frontiers`

The `mz_compute_frontiers` view describes the frontier of each
[dataflow](/docs/get-started/arrangements/#dataflows) export in the
system. The frontier describes the earliest timestamp at which the
output of the dataflow may change; data prior to that timestamp is
sealed.

| Field | Type | Meaning |
|----|----|----|
| `export_id` | [`text`](/docs/sql/types/text) | The ID of the dataflow export. Corresponds to [`mz_compute_exports.export_id`](#mz_compute_exports). |
| `time` | [`mz_timestamp`](/docs/sql/types/mz_timestamp) | The next timestamp at which the dataflow output may change. |

## `mz_compute_import_frontiers`

The `mz_compute_import_frontiers` view describes the frontiers of each
[dataflow](/docs/get-started/arrangements/#dataflows) import in the
system. The frontier describes the earliest timestamp at which the input
into the dataflow may change; data prior to that timestamp is sealed.

| Field | Type | Meaning |
|----|----|----|
| `export_id` | [`text`](/docs/sql/types/text) | The ID of the dataflow export. Corresponds to [`mz_compute_exports.export_id`](#mz_compute_exports). |
| `import_id` | [`text`](/docs/sql/types/text) | The ID of the dataflow import. Corresponds to [`mz_catalog.mz_sources.id`](../mz_catalog#mz_sources) or [`mz_catalog.mz_tables.id`](../mz_catalog#mz_tables) or [`mz_compute_exports.export_id`](#mz_compute_exports). |
| `time` | [`mz_timestamp`](/docs/sql/types/mz_timestamp) | The next timestamp at which the dataflow input may change. |

## `mz_compute_operator_durations_histogram`

The `mz_compute_operator_durations_histogram` view describes a histogram
of the duration in nanoseconds of each invocation for each
[dataflow](/docs/get-started/arrangements/#dataflows) operator.

| Field | Type | Meaning |
|----|----|----|
| `id` | [`uint8`](/docs/sql/types/uint8) | The ID of the operator. Corresponds to [`mz_dataflow_operators.id`](#mz_dataflow_operators). |
| `duration_ns` | [`uint8`](/docs/sql/types/uint8) | The upper bound of the duration bucket in nanoseconds. |
| `count` | [`numeric`](/docs/sql/types/numeric) | The (noncumulative) count of invocations in the bucket. |

## `mz_dataflows`

The `mz_dataflows` view describes the
[dataflows](/docs/get-started/arrangements/#dataflows) in the system.

| Field  | Type                             | Meaning                            |
|--------|----------------------------------|------------------------------------|
| `id`   | [`uint8`](/docs/sql/types/uint8) | The ID of the dataflow.            |
| `name` | [`text`](/docs/sql/types/text)   | The internal name of the dataflow. |

## `mz_dataflow_addresses`

The `mz_dataflow_addresses` view describes how the
[dataflow](/docs/get-started/arrangements/#dataflows) channels and
operators in the system are nested into scopes.

| Field | Type | Meaning |
|----|----|----|
| `id` | [`uint8`](/docs/sql/types/uint8) | The ID of the channel or operator. Corresponds to [`mz_dataflow_channels.id`](#mz_dataflow_channels) or [`mz_dataflow_operators.id`](#mz_dataflow_operators). |
| `address` | [`bigint list`](/docs/sql/types/list) | A list of scope-local indexes indicating the path from the root to this channel or operator. |

## `mz_dataflow_arrangement_sizes`

The `mz_dataflow_arrangement_sizes` view describes the size of
arrangements per operators under each dataflow.

| Field | Type | Meaning |
|----|----|----|
| `id` | [`uint8`](/docs/sql/types/uint8) | The ID of the [dataflow](/docs/get-started/arrangements/#dataflows). Corresponds to [`mz_dataflows.id`](#mz_dataflows). |
| `name` | [`text`](/docs/sql/types/text) | The name of the [dataflow](/docs/get-started/arrangements/#dataflows). |
| `records` | [`bigint`](/docs/sql/types/bigint) | The number of records in all arrangements in the dataflow. |
| `batches` | [`bigint`](/docs/sql/types/bigint) | The number of batches in all arrangements in the dataflow. |
| `size` | [`bigint`](/docs/sql/types/bigint) | The utilized size in bytes of the arrangements. |
| `capacity` | [`bigint`](/docs/sql/types/bigint) | The capacity in bytes of the arrangements. Can be larger than the size. |
| `allocations` | [`bigint`](/docs/sql/types/bigint) | The number of separate memory allocations backing the arrangements. |

## `mz_dataflow_channels`

The `mz_dataflow_channels` view describes the communication channels
between [dataflow](/docs/get-started/arrangements/#dataflows) operators.
A communication channel connects one of the outputs of a source operator
to one of the inputs of a target operator.

| Field | Type | Meaning |
|----|----|----|
| `id` | [`uint8`](/docs/sql/types/uint8) | The ID of the channel. |
| `from_index` | [`uint8`](/docs/sql/types/uint8) | The scope-local index of the source operator. Corresponds to [`mz_dataflow_addresses.address`](#mz_dataflow_addresses). |
| `from_port` | [`uint8`](/docs/sql/types/uint8) | The source operator’s output port. |
| `to_index` | [`uint8`](/docs/sql/types/uint8) | The scope-local index of the target operator. Corresponds to [`mz_dataflow_addresses.address`](#mz_dataflow_addresses). |
| `to_port` | [`uint8`](/docs/sql/types/uint8) | The target operator’s input port. |
| `type` | [`text`](/docs/sql/types/text) | The container type of the channel. |

## `mz_dataflow_channel_operators`

The `mz_dataflow_channel_operators` view associates
[dataflow](/docs/get-started/arrangements/#dataflows) channels with the
operators that are their endpoints.

| Field | Type | Meaning |
|----|----|----|
| `id` | [`uint8`](/docs/sql/types/uint8) | The ID of the channel. Corresponds to [`mz_dataflow_channels.id`](#mz_dataflow_channels). |
| `from_operator_id` | [`uint8`](/docs/sql/types/uint8) | The ID of the source of the channel. Corresponds to [`mz_dataflow_operators.id`](#mz_dataflow_operators). |
| `from_operator_address` | [`uint8 list`](/docs/sql/types/list) | The address of the source of the channel. Corresponds to [`mz_dataflow_addresses.address`](#mz_dataflow_addresses). |
| `to_operator_id` | [`uint8`](/docs/sql/types/uint8) | The ID of the target of the channel. Corresponds to [`mz_dataflow_operators.id`](#mz_dataflow_operators). |
| `to_operator_address` | [`uint8 list`](/docs/sql/types/list) | The address of the target of the channel. Corresponds to [`mz_dataflow_addresses.address`](#mz_dataflow_addresses). |
| `type` | [`text`](/docs/sql/types/text) | The container type of the channel. |

## `mz_dataflow_global_ids`

The `mz_dataflow_global_ids` view associates
[dataflow](/docs/get-started/arrangements/#dataflows) ids with global
ids (ids of the form `u8` or `t5`).

| Field | Type | Meaning |
|----|----|----|
| `id` | [`uint8`](/docs/sql/types/uint8) | The dataflow ID. |
| `global_id` | [`text`](/docs/sql/types/text) | A global ID associated with that dataflow. |

## `mz_dataflow_operators`

The `mz_dataflow_operators` view describes the
[dataflow](/docs/get-started/arrangements/#dataflows) operators in the
system.

| Field  | Type                             | Meaning                            |
|--------|----------------------------------|------------------------------------|
| `id`   | [`uint8`](/docs/sql/types/uint8) | The ID of the operator.            |
| `name` | [`text`](/docs/sql/types/text)   | The internal name of the operator. |

## `mz_dataflow_operator_dataflows`

The `mz_dataflow_operator_dataflows` view describes the
[dataflow](/docs/get-started/arrangements/#dataflows) to which each
operator belongs.

| Field | Type | Meaning |
|----|----|----|
| `id` | [`uint8`](/docs/sql/types/uint8) | The ID of the operator. Corresponds to [`mz_dataflow_operators.id`](#mz_dataflow_operators). |
| `name` | [`text`](/docs/sql/types/text) | The internal name of the operator. |
| `dataflow_id` | [`uint8`](/docs/sql/types/uint8) | The ID of the dataflow hosting the operator. Corresponds to [`mz_dataflows.id`](#mz_dataflows). |
| `dataflow_name` | [`text`](/docs/sql/types/text) | The internal name of the dataflow hosting the operator. |

## `mz_dataflow_operator_parents`

The `mz_dataflow_operator_parents` view describes how
[dataflow](/docs/get-started/arrangements/#dataflows) operators are
nested into scopes, by relating operators to their parent operators.

| Field | Type | Meaning |
|----|----|----|
| `id` | [`uint8`](/docs/sql/types/uint8) | The ID of the operator. Corresponds to [`mz_dataflow_operators.id`](#mz_dataflow_operators). |
| `parent_id` | [`uint8`](/docs/sql/types/uint8) | The ID of the operator’s parent operator. Corresponds to [`mz_dataflow_operators.id`](#mz_dataflow_operators). |

## `mz_dataflow_shutdown_durations_histogram`

The `mz_dataflow_shutdown_durations_histogram` view describes a
histogram of the time in nanoseconds required to fully shut down dropped
[dataflows](/docs/get-started/arrangements/#dataflows).

| Field | Type | Meaning |
|----|----|----|
| `duration_ns` | [`uint8`](/docs/sql/types/uint8) | The upper bound of the bucket in nanoseconds. |
| `count` | [`numeric`](/docs/sql/types/numeric) | The (noncumulative) count of dataflows in this bucket. |

## `mz_expected_group_size_advice`

The `mz_expected_group_size_advice` view provides advice on
opportunities to set [query hints](/docs/sql/select/#query-hints). Query
hints are applicable to dataflows maintaining
[`MIN`](/docs/sql/functions/#min), [`MAX`](/docs/sql/functions/#max), or
[Top K](/docs/transform-data/patterns/top-k) query patterns. The
maintainance of these query patterns is implemented inside an operator
scope, called a region, through a hierarchical scheme for either
aggregation or Top K computations.

| Field | Type | Meaning |
|----|----|----|
| `dataflow_id` | [`uint8`](/docs/sql/types/uint8) | The ID of the [dataflow](/docs/get-started/arrangements/#dataflows). Corresponds to [`mz_dataflows.id`](#mz_dataflows). |
| `dataflow_name` | [`text`](/docs/sql/types/text) | The internal name of the dataflow hosting the min/max aggregation or Top K. |
| `region_id` | [`uint8`](/docs/sql/types/uint8) | The ID of the root operator scope. Corresponds to [`mz_dataflow_operators.id`](#mz_dataflow_operators). |
| `region_name` | [`text`](/docs/sql/types/text) | The internal name of the root operator scope for the min/max aggregation or Top K. |
| `levels` | [`bigint`](/docs/sql/types/bigint) | The number of levels in the hierarchical scheme implemented by the region. |
| `to_cut` | [`bigint`](/docs/sql/types/bigint) | The number of levels that can be eliminated (cut) from the region’s hierarchy. |
| `savings` | [`numeric`](/docs/sql/types/numeric) | A conservative estimate of the amount of memory in bytes to be saved by applying the hint. |
| `hint` | [`double precision`](/docs/sql/types/double-precision) | The hint value that will eliminate `to_cut` levels from the region’s hierarchy. |

## `mz_mappable_objects`

The `mz_mappable_objects` identifies indexes (and their underlying
views) and materialized views which can be debugged using the
[`mz_lir_mapping`](#mz_lir_mapping) view.

| Field       | Type                           | Meaning                      |
|-------------|--------------------------------|------------------------------|
| `name`      | [`text`](/docs/sql/types/text) | The name of the object.      |
| `global_id` | [`text`](/docs/sql/types/text) | The global ID of the object. |

See [Which part of my query runs slowly or uses a lot of
memory?](/docs/transform-data/troubleshooting/#which-part-of-my-query-runs-slowly-or-uses-a-lot-of-memory)
for examples of debugging with `mz_mappable_objects` and
`mz_lir_mapping`.

## `mz_lir_mapping`

The `mz_lir_mapping` view describes the low-level internal
representation (LIR) plan that corresponds to global ids of indexes (and
their underlying views) and materialized views. You can find a list of
all debuggable objects in [`mz_mappable_objects`](#mz_mappable_objects).
LIR is a higher-level representation than dataflows; this view is used
for profiling and debugging indices and materialized views. Note that
LIR is not a stable interface and may change at any time. In particular,
you should not attempt to parse `operator` descriptions. LIR nodes are
implemented by zero or more dataflow operators with sequential ids. We
use the range `[operator_id_start, operator_id_end)` to record this
information. If an LIR node was implemented without any dataflow
operators, `operator_id_start` will be equal to `operator_id_end`.

| Field | Type | Meaning |
|----|----|----|
| global_id | [`text`](/docs/sql/types/text) | The global ID. |
| lir_id | [`uint8`](/docs/sql/types/uint8) | The LIR node ID. |
| operator | [`text`](/docs/sql/types/text) | The LIR operator, in the format `OperatorName INPUTS [OPTIONS]`. |
| parent_lir_id | [`uint8`](/docs/sql/types/uint8) | The parent of this LIR node. May be `NULL`. |
| nesting | [`uint2`](/docs/sql/types/uint2) | The nesting level of this LIR node. |
| operator_id_start | [`uint8`](/docs/sql/types/uint8) | The first dataflow operator ID implementing this LIR operator (inclusive). |
| operator_id_end | [`uint8`](/docs/sql/types/uint8) | The first dataflow operator ID *after* this LIR operator (exclusive). |

## `mz_message_counts`

The `mz_message_counts` view describes the messages and message batches
sent and received over the
[dataflow](/docs/get-started/arrangements/#dataflows) channels in the
system. It distinguishes between individual records (`sent`, `received`)
and batches of records (`batch_sent`, `batch_sent`).

| Field | Type | Meaning |
|----|----|----|
| `channel_id` | [`uint8`](/docs/sql/types/uint8) | The ID of the channel. Corresponds to [`mz_dataflow_channels.id`](#mz_dataflow_channels). |
| `sent` | [`numeric`](/docs/sql/types/numeric) | The number of messages sent. |
| `received` | [`numeric`](/docs/sql/types/numeric) | The number of messages received. |
| `batch_sent` | [`numeric`](/docs/sql/types/numeric) | The number of batches sent. |
| `batch_received` | [`numeric`](/docs/sql/types/numeric) | The number of batches received. |

## `mz_peek_durations_histogram`

The `mz_peek_durations_histogram` view describes a histogram of the
duration in nanoseconds of read queries (“peeks”) in the
[dataflow](/docs/get-started/arrangements/#dataflows) layer.

| Field | Type | Meaning |
|----|----|----|
| `type` | [`text`](/docs/sql/types/text) | The peek variant: `index` or `persist`. |
| `duration_ns` | [`uint8`](/docs/sql/types/uint8) | The upper bound of the bucket in nanoseconds. |
| `count` | [`numeric`](/docs/sql/types/numeric) | The (noncumulative) count of peeks in this bucket. |

## `mz_records_per_dataflow`

The `mz_records_per_dataflow` view describes the number of records in
each [dataflow](/docs/get-started/arrangements/#dataflows).

| Field | Type | Meaning |
|----|----|----|
| `id` | [`uint8`](/docs/sql/types/uint8) | The ID of the dataflow. Corresponds to [`mz_dataflows.id`](#mz_dataflows). |
| `name` | [`text`](/docs/sql/types/text) | The internal name of the dataflow. |
| `records` | [`bigint`](/docs/sql/types/bigint) | The number of records in the dataflow. |
| `batches` | [`bigint`](/docs/sql/types/bigint) | The number of batches in the dataflow. |
| `size` | [`bigint`](/docs/sql/types/bigint) | The utilized size in bytes of the arrangements. |
| `capacity` | [`bigint`](/docs/sql/types/bigint) | The capacity in bytes of the arrangements. Can be larger than the size. |
| `allocations` | [`bigint`](/docs/sql/types/bigint) | The number of separate memory allocations backing the arrangements. |

## `mz_records_per_dataflow_operator`

The `mz_records_per_dataflow_operator` view describes the number of
records in each [dataflow](/docs/get-started/arrangements/#dataflows)
operator in the system.

| Field | Type | Meaning |
|----|----|----|
| `id` | [`uint8`](/docs/sql/types/uint8) | The ID of the operator. Corresponds to [`mz_dataflow_operators.id`](#mz_dataflow_operators). |
| `name` | [`text`](/docs/sql/types/text) | The internal name of the operator. |
| `dataflow_id` | [`uint8`](/docs/sql/types/uint8) | The ID of the dataflow. Corresponds to [`mz_dataflows.id`](#mz_dataflows). |
| `records` | [`bigint`](/docs/sql/types/bigint) | The number of records in the operator. |
| `batches` | [`bigint`](/docs/sql/types/bigint) | The number of batches in the dataflow. |
| `size` | [`bigint`](/docs/sql/types/bigint) | The utilized size in bytes of the arrangement. |
| `capacity` | [`bigint`](/docs/sql/types/bigint) | The capacity in bytes of the arrangement. Can be larger than the size. |
| `allocations` | [`bigint`](/docs/sql/types/bigint) | The number of separate memory allocations backing the arrangement. |

## `mz_scheduling_elapsed`

The `mz_scheduling_elapsed` view describes the total amount of time
spent in each [dataflow](/docs/get-started/arrangements/#dataflows)
operator.

| Field | Type | Meaning |
|----|----|----|
| `id` | [`uint8`](/docs/sql/types/uint8) | The ID of the operator. Corresponds to [`mz_dataflow_operators.id`](#mz_dataflow_operators). |
| `elapsed_ns` | [`numeric`](/docs/sql/types/numeric) | The total elapsed time spent in the operator in nanoseconds. |

## `mz_scheduling_parks_histogram`

The `mz_scheduling_parks_histogram` view describes a histogram of
[dataflow](/docs/get-started/arrangements/#dataflows) worker park
events. A park event occurs when a worker has no outstanding work.

| Field | Type | Meaning |
|----|----|----|
| `slept_for_ns` | [`uint8`](/docs/sql/types/uint8) | The actual length of the park event in nanoseconds. |
| `requested_ns` | [`uint8`](/docs/sql/types/uint8) | The requested length of the park event in nanoseconds. |
| `count` | [`numeric`](/docs/sql/types/numeric) | The (noncumulative) count of park events in this bucket. |

</div>

<a href="#top" class="back-to-top">Back to top ↑</a>

<div class="theme-switcher">

<img
src="data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIGNsYXNzPSJzeXN0ZW0iIHZpZXdib3g9IjAgMCA1MTIgNTEyIj4KICAgICAgICA8dGl0bGU+U3lzdGVtIFRoZW1lPC90aXRsZT4KICAgICAgICA8cGF0aCBkPSJNMjU2IDE3NmE4MCA4MCAwIDEwODAgODAgODAuMjQgODAuMjQgMCAwMC04MC04MHptMTcyLjcyIDgwYTE2NS41MyAxNjUuNTMgMCAwMS0xLjY0IDIyLjM0bDQ4LjY5IDM4LjEyYTExLjU5IDExLjU5IDAgMDEyLjYzIDE0Ljc4bC00Ni4wNiA3OS41MmExMS42NCAxMS42NCAwIDAxLTE0LjE0IDQuOTNsLTU3LjI1LTIzYTE3Ni41NiAxNzYuNTYgMCAwMS0zOC44MiAyMi42N2wtOC41NiA2MC43OGExMS45MyAxMS45MyAwIDAxLTExLjUxIDkuODZoLTkyLjEyYTEyIDEyIDAgMDEtMTEuNTEtOS41M2wtOC41Ni02MC43OEExNjkuMyAxNjkuMyAwIDAxMTUxLjA1IDM5M0w5My44IDQxNmExMS42NCAxMS42NCAwIDAxLTE0LjE0LTQuOTJMMzMuNiAzMzEuNTdhMTEuNTkgMTEuNTkgMCAwMTIuNjMtMTQuNzhsNDguNjktMzguMTJBMTc0LjU4IDE3NC41OCAwIDAxODMuMjggMjU2YTE2NS41MyAxNjUuNTMgMCAwMTEuNjQtMjIuMzRsLTQ4LjY5LTM4LjEyYTExLjU5IDExLjU5IDAgMDEtMi42My0xNC43OGw0Ni4wNi03OS41MmExMS42NCAxMS42NCAwIDAxMTQuMTQtNC45M2w1Ny4yNSAyM2ExNzYuNTYgMTc2LjU2IDAgMDEzOC44Mi0yMi42N2w4LjU2LTYwLjc4QTExLjkzIDExLjkzIDAgMDEyMDkuOTQgMjZoOTIuMTJhMTIgMTIgMCAwMTExLjUxIDkuNTNsOC41NiA2MC43OEExNjkuMyAxNjkuMyAwIDAxMzYxIDExOWw1Ny4yLTIzYTExLjY0IDExLjY0IDAgMDExNC4xNCA0LjkybDQ2LjA2IDc5LjUyYTExLjU5IDExLjU5IDAgMDEtMi42MyAxNC43OGwtNDguNjkgMzguMTJhMTc0LjU4IDE3NC41OCAwIDAxMS42NCAyMi42NnoiIC8+CiAgICAgIDwvc3ZnPg=="
class="system" />

<img
src="data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIGNsYXNzPSJzdW4iIHZpZXdib3g9IjAgMCA1MTIgNTEyIj4KICAgICAgICA8dGl0bGU+TGlnaHQgVGhlbWU8L3RpdGxlPgogICAgICAgIDxwYXRoIGQ9Ik0yMzQgMjZoNDR2OTJoLTQ0ek0yMzQgMzk0aDQ0djkyaC00NHpNMzM4LjAyNSAxNDIuODU3bDY1LjA1NC02NS4wNTQgMzEuMTEzIDMxLjExMy02NS4wNTQgNjUuMDU0ek03Ny44MTUgNDAzLjA3NGw2NS4wNTQtNjUuMDU0IDMxLjExMyAzMS4xMTMtNjUuMDU0IDY1LjA1NHpNMzk0IDIzNGg5MnY0NGgtOTJ6TTI2IDIzNGg5MnY0NEgyNnpNMzM4LjAyOSAzNjkuMTRsMzEuMTEyLTMxLjExMyA2NS4wNTQgNjUuMDU0LTMxLjExMiAzMS4xMTJ6TTc3LjgwMiAxMDguOTJsMzEuMTEzLTMxLjExMyA2NS4wNTQgNjUuMDU0LTMxLjExMyAzMS4xMTJ6TTI1NiAzNThhMTAyIDEwMiAwIDExMTAyLTEwMiAxMDIuMTIgMTAyLjEyIDAgMDEtMTAyIDEwMnoiIC8+CiAgICAgIDwvc3ZnPg=="
class="sun" />

<img
src="data:image/svg+xml;base64,PHN2ZyB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIGNsYXNzPSJtb29uIiB2aWV3Ym94PSIwIDAgNTEyIDUxMiI+CiAgICAgICAgPHRpdGxlPkRhcmsgVGhlbWU8L3RpdGxlPgogICAgICAgIDxwYXRoIGQ9Ik0xNTIuNjIgMTI2Ljc3YzAtMzMgNC44NS02Ni4zNSAxNy4yMy05NC43N0M4Ny41NCA2Ny44MyAzMiAxNTEuODkgMzIgMjQ3LjM4IDMyIDM3NS44NSAxMzYuMTUgNDgwIDI2NC42MiA0ODBjOTUuNDkgMCAxNzkuNTUtNTUuNTQgMjE1LjM4LTEzNy44NS0yOC40MiAxMi4zOC02MS44IDE3LjIzLTk0Ljc3IDE3LjIzLTEyOC40NyAwLTIzMi42MS0xMDQuMTQtMjMyLjYxLTIzMi42MXoiIC8+CiAgICAgIDwvc3ZnPg=="
class="moon" />

</div>

<div>

<a
href="//github.com/MaterializeInc/materialize/edit/main/doc/user/content/sql/system-catalog/mz_introspection.md"
class="btn-ghost"><img
src="data:image/svg+xml;base64,PHN2ZyB3aWR0aD0iMTgiIGhlaWdodD0iMTgiIHZpZXdib3g9IjAgMCAyMyAyMyIgZmlsbD0iY3VycmVudENvbG9yIiB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciPgogICAgICAgIDxwYXRoIGQ9Ik0yMC44OTQ1IDExLjQ5NjhDMjAuODk0NSAxMC4yMzk0IDIwLjYxNTEgOS4wNTE5IDIwLjEyNjEgNy44NjQzN0MxOS42MzcxIDYuNzQ2NjkgMTguOTM4NSA1LjY5ODg4IDE4LjE3MDEgNC45MzA0N0MxNy40MDE3IDQuMTYyMDcgMTYuMzUzOSAzLjQ2MzUgMTUuMjM2MiAyLjk3NDUyQzE0LjExODUgMi40ODU1MyAxMi44NjExIDIuMjA2MTMgMTEuNjAzOCAyLjIwNjEzQzEwLjM0NjQgMi4yMDYxMyA5LjE1ODg0IDIuNDg1NTMgNy45NzEzIDIuOTc0NTJDNi44NTM2MiAzLjQ2MzUgNS44MDU3OSA0LjE2MjA3IDUuMDM3MzggNC45MzA0N0M0LjI2ODk4IDUuNjk4ODggMy41NzA0NCA2Ljc0NjY5IDMuMDgxNDUgNy44NjQzN0MyLjU5MjQ3IDguOTgyMDUgMi4zMTMwNCAxMC4yMzk0IDIuMzEzMDQgMTEuNDk2OEMyLjMxMzA0IDEzLjUyMjYgMi45NDE3NCAxNS4zMzg5IDQuMTI5MjggMTcuMDE1NEM1LjMxNjgxIDE4LjY5MTkgNi45MjM0NyAxOS44MDk2IDguODA5NTYgMjAuMzY4NFYxNy45MjM1QzguMjUwNzIgMTcuOTkzNCA3Ljk3MTI5IDE3Ljk5MzMgNy44MzE1OCAxNy45OTMzQzYuNzgzNzYgMTcuOTkzMyA2LjAxNTM1IDE3LjUwNDQgNS41OTYyMiAxNi41MjY0QzUuNDU2NTEgMTYuMTc3MSA1LjI0Njk1IDE1LjgyNzggNS4wMzczOCAxNS42MTgzQzQuOTY3NTMgMTUuNTQ4NCA0Ljg5NzY4IDE1LjQ3ODYgNC43NTc5NyAxNS4zMzg5QzQuNjE4MjYgMTUuMTk5MiA0LjQ3ODU0IDE1LjEyOTMgNC4zMzg4MyAxNC45ODk2QzQuMTk5MTIgMTQuODQ5OSA0LjEyOTI4IDE0Ljc4IDQuMTI5MjggMTQuNzhDNC4xMjkyOCAxNC42NDAzIDQuMjY4OTggMTQuNjQwMyA0LjU0ODQgMTQuNjQwM0M0LjgyNzgyIDE0LjY0MDMgNS4xMDcyNCAxNC43MTAyIDUuMzE2ODEgMTQuODQ5OUM1LjUyNjM3IDE0Ljk4OTYgNS43MzU5NCAxNS4xMjkzIDUuODc1NjUgMTUuMzM4OUM2LjAxNTM2IDE1LjU0ODQgNi4xNTUwNyAxNS43NTggNi4zNjQ2MyAxNS45Njc2QzYuNTA0MzQgMTYuMTc3MSA2LjcxMzkxIDE2LjMxNjggNi45MjM0OCAxNi40NTY1QzcuMTMzMDQgMTYuNTk2MyA3LjQxMjQ2IDE2LjY2NjEgNy43NjE3MyAxNi42NjYxQzguMTgwODYgMTYuNjY2MSA4LjUzMDE0IDE2LjU5NjMgOC45NDkyNyAxNi40NTY1QzkuMDg4OTggMTUuODk3NyA5LjQzODI1IDE1LjQ3ODYgOS44NTczOCAxNS4xMjkzQzguMjUwNzIgMTQuOTg5NiA3LjA2MzE4IDE0LjU3MDUgNi4yOTQ3NyAxMy45NDE4QzUuNTI2MzcgMTMuMzEzMSA1LjEwNzI0IDEyLjE5NTQgNS4xMDcyNCAxMC42NTg2QzUuMTA3MjQgOS41NDA4OSA1LjQ1NjUyIDguNTYyOTQgNi4xNTUwNyA3Ljc5NDUzQzYuMDE1MzYgNy4zNzU0IDUuOTQ1NSA2Ljk1NjI2IDUuOTQ1NSA2LjUzNzEzQzUuOTQ1NSA1Ljk3ODI5IDYuMDg1MjEgNS40MTk0NiA2LjM2NDYzIDQuOTMwNDdDNi45MjM0NyA0LjkzMDQ3IDcuNDEyNDUgNS4wMDAzMiA3LjgzMTU4IDUuMjA5ODlDOC4yNTA3MSA1LjQxOTQ1IDguNzM5NyA1LjY5ODg2IDkuMjk4NTQgNi4xMTc5OUMxMC4wNjY5IDUuOTc4MjggMTAuODM1NCA1LjgzODU4IDExLjc0MzUgNS44Mzg1OEMxMi41MTE5IDUuODM4NTggMTMuMjgwMyA1LjkwODQ1IDEzLjk3ODggNi4wNDgxNkMxNC41Mzc3IDUuNjI5MDMgMTUuMDI2NyA1LjM0OTYgMTUuNDQ1OCA1LjIwOTg5QzE1Ljg2NDkgNS4wMDAzMiAxNi4zNTM5IDQuOTMwNDcgMTYuOTEyNyA0LjkzMDQ3QzE3LjE5MjIgNS40MTk0NiAxNy4zMzE5IDUuOTc4MjkgMTcuMzMxOSA2LjUzNzEzQzE3LjMzMTkgNi45NTYyNiAxNy4yNjIgNy4zNzU0IDE3LjEyMjMgNy43MjQ2N0MxNy44MjA5IDguNDkzMDggMTguMTcwMSA5LjQ3MTA1IDE4LjE3MDEgMTAuNTg4N0MxOC4xNzAxIDEyLjEyNTUgMTcuNzUxIDEzLjE3MzQgMTYuOTgyNiAxMy44NzE5QzE2LjIxNDIgMTQuNTcwNSAxNS4wMjY2IDE0LjkxOTcgMTMuNDIgMTUuMDU5NEMxNC4xMTg1IDE1LjU0ODQgMTQuMzk4IDE2LjE3NzEgMTQuMzk4IDE2Ljk0NTVWMjAuMjI4N0MxNi4zNTM5IDE5LjYgMTcuODkwNyAxOC40ODIzIDE5LjA3ODIgMTYuODc1N0MyMC4yNjU4IDE1LjMzODkgMjAuODk0NSAxMy41MjI2IDIwLjg5NDUgMTEuNDk2OFpNMjIuNzEwNyAxMS40OTY4QzIyLjcxMDcgMTMuNTIyNiAyMi4yMjE3IDE1LjQwODcgMjEuMjQzOCAxNy4wODUyQzIwLjI2NTggMTguODMxNiAxOC44Njg3IDIwLjE1ODggMTcuMTkyMiAyMS4xMzY4QzE1LjQ0NTggMjIuMTE0OCAxMy42Mjk2IDIyLjYwMzggMTEuNjAzOCAyMi42MDM4QzkuNTc3OTYgMjIuNjAzOCA3LjY5MTg4IDIyLjExNDggNi4wMTUzNiAyMS4xMzY4QzQuMjY4OTggMjAuMTU4OCAyLjk0MTc0IDE4Ljc2MTggMS45NjM3NyAxNy4wODUyQzAuOTg1Nzk2IDE1LjMzODkgMC40OTY4MDcgMTMuNTIyNiAwLjQ5NjgwNyAxMS40OTY4QzAuNDk2ODA3IDkuNDcxMDQgMC45ODU3OTYgNy41ODQ5NiAxLjk2Mzc3IDUuOTA4NDRDMi45NDE3NCA0LjE2MjA2IDQuMzM4ODQgMi44MzQ4MyA2LjAxNTM2IDEuODU2ODZDNy43NjE3MyAwLjg3ODg4NiA5LjU3Nzk2IDAuMzg5ODk3IDExLjYwMzggMC4zODk4OTdDMTMuNjI5NiAwLjM4OTg5NyAxNS41MTU2IDAuODc4ODg2IDE3LjE5MjIgMS44NTY4NkMxOC45Mzg1IDIuODM0ODMgMjAuMjY1OCA0LjIzMTkyIDIxLjI0MzggNS45MDg0NEMyMi4yMjE3IDcuNTg0OTYgMjIuNzEwNyA5LjQ3MTA0IDIyLjcxMDcgMTEuNDk2OFoiIC8+CiAgICAgIDwvc3ZnPg==" />
Edit this page</a>

</div>

<div class="footer-links">

[Home](https://materialize.com) [Status](https://status.materialize.com)
[GitHub](https://github.com/MaterializeInc/materialize)
[Blog](https://materialize.com/blog)
[Contact](https://materialize.com/contact)

Cookie Preferences

[Privacy Policy](https://materialize.com/privacy-policy/)

</div>

© 2025 Materialize Inc.

</div>

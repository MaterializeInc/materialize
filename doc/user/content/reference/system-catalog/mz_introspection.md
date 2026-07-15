---
title: "mz_introspection"
description: "mz_introspection is a system catalog schema which contains replica introspection relations. This schema is not part of Materialize's stable interface."
menu:
  main:
    parent: 'system-catalog'
    weight: 4
aliases:
  - /sql/system-catalog/mz_introspection/
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

<!-- RELATION_SPEC mz_introspection.mz_active_peeks FROM_YAML -->
{{< catalog-relation schema="mz_introspection" name="mz_active_peeks" >}}

## `mz_arrangement_sharing`

<!-- RELATION_SPEC mz_introspection.mz_arrangement_sharing FROM_YAML -->
{{< catalog-relation schema="mz_introspection" name="mz_arrangement_sharing" >}}

<!-- RELATION_SPEC_UNDOCUMENTED mz_introspection.mz_arrangement_sharing_per_worker -->
<!-- RELATION_SPEC_UNDOCUMENTED mz_introspection.mz_arrangement_sharing_raw -->

## `mz_arrangement_sizes`

<!-- RELATION_SPEC mz_introspection.mz_arrangement_sizes FROM_YAML -->
{{< catalog-relation schema="mz_introspection" name="mz_arrangement_sizes" >}}

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

<!-- RELATION_SPEC mz_introspection.mz_compute_error_counts FROM_YAML -->
{{< catalog-relation schema="mz_introspection" name="mz_compute_error_counts" >}}

<!-- RELATION_SPEC_UNDOCUMENTED mz_introspection.mz_compute_error_counts_per_worker -->
<!-- RELATION_SPEC_UNDOCUMENTED mz_introspection.mz_compute_error_counts_raw -->

## `mz_compute_exports`

<!-- RELATION_SPEC mz_introspection.mz_compute_exports FROM_YAML -->
{{< catalog-relation schema="mz_introspection" name="mz_compute_exports" >}}

<!-- RELATION_SPEC_UNDOCUMENTED mz_introspection.mz_compute_exports_per_worker -->

## `mz_compute_frontiers`

<!-- RELATION_SPEC mz_introspection.mz_compute_frontiers FROM_YAML -->
{{< catalog-relation schema="mz_introspection" name="mz_compute_frontiers" >}}

<!-- RELATION_SPEC_UNDOCUMENTED mz_introspection.mz_compute_frontiers_per_worker -->

## `mz_compute_import_frontiers`

<!-- RELATION_SPEC mz_introspection.mz_compute_import_frontiers FROM_YAML -->
{{< catalog-relation schema="mz_introspection" name="mz_compute_import_frontiers" >}}

<!-- RELATION_SPEC_UNDOCUMENTED mz_introspection.mz_compute_import_frontiers_per_worker -->

## `mz_compute_operator_durations_histogram`

<!-- RELATION_SPEC mz_introspection.mz_compute_operator_durations_histogram FROM_YAML -->
{{< catalog-relation schema="mz_introspection" name="mz_compute_operator_durations_histogram" >}}

<!-- RELATION_SPEC_UNDOCUMENTED mz_introspection.mz_compute_operator_durations_histogram_per_worker -->
<!-- RELATION_SPEC_UNDOCUMENTED mz_introspection.mz_compute_operator_durations_histogram_raw -->

## `mz_cluster_prometheus_metrics`

The `mz_cluster_prometheus_metrics` source exposes Prometheus metrics collected from each cluster replica process's internal metrics registry.
Metrics are scraped periodically and presented as rows.
Histograms are flattened into separate bucket, sum, and count rows.
Summaries are flattened into separate quantile, sum, and count rows.

<!-- RELATION_SPEC mz_introspection.mz_cluster_prometheus_metrics NO_COMMENTS -->
| Field         | Type                   | Meaning                                                              |
|---------------|------------------------|----------------------------------------------------------------------|
| `process_id`  | [`uint8`]              | The ID of the process that collected the metric.                     |
| `metric_name` | [`text`]               | The name of the Prometheus metric.                                   |
| `metric_type` | [`text`]               | The type of the metric: `counter`, `gauge`, `histogram`, or `summary`. |
| `labels`      | [`map`]                | The label key-value pairs associated with the metric.                |
| `value`       | [`double precision`]   | The numeric value of the metric.                                     |
| `help`        | [`text`]               | The help string describing the metric.                               |

## `mz_dataflows`

<!-- RELATION_SPEC mz_introspection.mz_dataflows FROM_YAML -->
{{< catalog-relation schema="mz_introspection" name="mz_dataflows" >}}

<!-- RELATION_SPEC_UNDOCUMENTED mz_introspection.mz_dataflows_per_worker -->

## `mz_dataflow_addresses`

<!-- RELATION_SPEC mz_introspection.mz_dataflow_addresses FROM_YAML -->
{{< catalog-relation schema="mz_introspection" name="mz_dataflow_addresses" >}}

<!-- RELATION_SPEC_UNDOCUMENTED mz_introspection.mz_dataflow_addresses_per_worker -->

## `mz_dataflow_arrangement_sizes`

<!-- RELATION_SPEC mz_introspection.mz_dataflow_arrangement_sizes FROM_YAML -->
{{< catalog-relation schema="mz_introspection" name="mz_dataflow_arrangement_sizes" >}}

## `mz_dataflow_channels`

<!-- RELATION_SPEC mz_introspection.mz_dataflow_channels FROM_YAML -->
{{< catalog-relation schema="mz_introspection" name="mz_dataflow_channels" >}}

<!-- RELATION_SPEC_UNDOCUMENTED mz_introspection.mz_dataflow_channels_per_worker -->

## `mz_dataflow_channel_operators`

<!-- RELATION_SPEC mz_introspection.mz_dataflow_channel_operators FROM_YAML -->
{{< catalog-relation schema="mz_introspection" name="mz_dataflow_channel_operators" >}}

<!-- RELATION_SPEC_UNDOCUMENTED mz_introspection.mz_dataflow_channel_operators_per_worker -->

## `mz_dataflow_global_ids`

<!-- RELATION_SPEC mz_introspection.mz_dataflow_global_ids FROM_YAML -->
{{< catalog-relation schema="mz_introspection" name="mz_dataflow_global_ids" >}}

<!-- RELATION_SPEC_UNDOCUMENTED mz_introspection.mz_compute_dataflow_global_ids_per_worker -->

## `mz_dataflow_operators`

<!-- RELATION_SPEC mz_introspection.mz_dataflow_operators FROM_YAML -->
{{< catalog-relation schema="mz_introspection" name="mz_dataflow_operators" >}}

<!-- RELATION_SPEC_UNDOCUMENTED mz_introspection.mz_dataflow_operators_per_worker -->

## `mz_dataflow_operator_dataflows`

<!-- RELATION_SPEC mz_introspection.mz_dataflow_operator_dataflows FROM_YAML -->
{{< catalog-relation schema="mz_introspection" name="mz_dataflow_operator_dataflows" >}}

<!-- RELATION_SPEC_UNDOCUMENTED mz_introspection.mz_dataflow_operator_dataflows_per_worker -->

## `mz_dataflow_operator_parents`

<!-- RELATION_SPEC mz_introspection.mz_dataflow_operator_parents FROM_YAML -->
{{< catalog-relation schema="mz_introspection" name="mz_dataflow_operator_parents" >}}

<!-- RELATION_SPEC_UNDOCUMENTED mz_introspection.mz_dataflow_operator_parents_per_worker -->

## `mz_expected_group_size_advice`

<!-- RELATION_SPEC mz_introspection.mz_expected_group_size_advice FROM_YAML -->
{{< catalog-relation schema="mz_introspection" name="mz_expected_group_size_advice" >}}

## `mz_mappable_objects`

<!-- RELATION_SPEC mz_introspection.mz_mappable_objects FROM_YAML -->
{{< catalog-relation schema="mz_introspection" name="mz_mappable_objects" >}}

See [Which part of my query runs slowly or uses a lot of memory?](/transform-data/troubleshooting/#which-part-of-my-query-runs-slowly-or-uses-a-lot-of-memory) for examples of debugging with `mz_mappable_objects` and `mz_lir_mapping`.

## `mz_lir_mapping`

<!-- RELATION_SPEC mz_introspection.mz_lir_mapping FROM_YAML -->
{{< catalog-relation schema="mz_introspection" name="mz_lir_mapping" >}}

<!-- RELATION_SPEC_UNDOCUMENTED mz_introspection.mz_compute_lir_mapping_per_worker -->

## `mz_message_counts`

<!-- RELATION_SPEC mz_introspection.mz_message_counts FROM_YAML -->
{{< catalog-relation schema="mz_introspection" name="mz_message_counts" >}}

<!-- RELATION_SPEC_UNDOCUMENTED mz_introspection.mz_message_counts_per_worker -->
<!-- RELATION_SPEC_UNDOCUMENTED mz_introspection.mz_message_batch_counts_received_raw -->
<!-- RELATION_SPEC_UNDOCUMENTED mz_introspection.mz_message_batch_counts_sent_raw -->
<!-- RELATION_SPEC_UNDOCUMENTED mz_introspection.mz_message_counts_received_raw -->
<!-- RELATION_SPEC_UNDOCUMENTED mz_introspection.mz_message_counts_sent_raw -->

## `mz_peek_durations_histogram`

<!-- RELATION_SPEC mz_introspection.mz_peek_durations_histogram FROM_YAML -->
{{< catalog-relation schema="mz_introspection" name="mz_peek_durations_histogram" >}}

<!-- RELATION_SPEC_UNDOCUMENTED mz_introspection.mz_peek_durations_histogram_per_worker -->
<!-- RELATION_SPEC_UNDOCUMENTED mz_introspection.mz_peek_durations_histogram_raw -->

## `mz_records_per_dataflow`

<!-- RELATION_SPEC mz_introspection.mz_records_per_dataflow FROM_YAML -->
{{< catalog-relation schema="mz_introspection" name="mz_records_per_dataflow" >}}

<!-- RELATION_SPEC_UNDOCUMENTED mz_introspection.mz_records_per_dataflow_per_worker -->

## `mz_records_per_dataflow_operator`

<!-- RELATION_SPEC mz_introspection.mz_records_per_dataflow_operator FROM_YAML -->
{{< catalog-relation schema="mz_introspection" name="mz_records_per_dataflow_operator" >}}

<!-- RELATION_SPEC_UNDOCUMENTED mz_introspection.mz_records_per_dataflow_operator_per_worker -->

## `mz_scheduling_elapsed`

<!-- RELATION_SPEC mz_introspection.mz_scheduling_elapsed FROM_YAML -->
{{< catalog-relation schema="mz_introspection" name="mz_scheduling_elapsed" >}}

<!-- RELATION_SPEC_UNDOCUMENTED mz_introspection.mz_scheduling_elapsed_per_worker -->
<!-- RELATION_SPEC_UNDOCUMENTED mz_introspection.mz_scheduling_elapsed_raw -->

## `mz_scheduling_parks_histogram`

<!-- RELATION_SPEC mz_introspection.mz_scheduling_parks_histogram FROM_YAML -->
{{< catalog-relation schema="mz_introspection" name="mz_scheduling_parks_histogram" >}}

<!-- RELATION_SPEC_UNDOCUMENTED mz_introspection.mz_scheduling_parks_histogram_per_worker -->
<!-- RELATION_SPEC_UNDOCUMENTED mz_introspection.mz_scheduling_parks_histogram_raw -->

[`bigint`]: /sql/types/bigint
[`bigint list`]: /sql/types/list
[`double precision`]: /sql/types/double-precision
[`map`]: /sql/types/map
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

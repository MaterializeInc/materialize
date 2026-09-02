# Compute metric reference

Dashboard `compute-overview`, UID `f248986d-81c6-42a6-817b-00cd7759d808`. The rows that matter for sign-off are `By process` and `By cluster`, each inspected twice, once across all clusters and once restricted to system clusters. The `By replica`, `By worker`, and `By collection` rows require a narrowed variable and belong to the drill-down in step 7 of the skill, not to the sweep.

## Selectors

Process-level panels read cAdvisor and kube-state metrics and select on the pod name:

```
pod=~".*cluster-<cluster_id>-replica-<replica_id>-.+", container="clusterd"
```

Cluster-level panels split into two families with different label names for the same concepts.

| Family | Cluster label | Replica label |
|---|---|---|
| Controller, protocol, peeks, and `v2_mz_*` replica metrics | `instance_id` | `replica_id` |
| `mz_arrangement_*`, `mz_dataflow_replica_*`, `mz_subscribe_*`, `mz_cluster_*` | `cluster_environmentd_materialize_cloud_cluster_id` | `cluster_environmentd_materialize_cloud_replica_id` |

System clusters are `s` followed by digits in either family, and `.*cluster-s[0-9]+-replica-.*` in the pod name.

## Roster

### By process

| Metric | Type | Notes |
|---|---|---|
| `container_cpu_usage_seconds_total` | counter | Needs `cpu="total"`. Divide by `container_spec_cpu_quota` for percent of limit. |
| `container_memory_working_set_bytes` | gauge | Resident plus active file. The primary memory signal. |
| `container_memory_rss` | gauge | Tracks working set closely; a divergence between them is itself a finding. |
| `container_memory_swap` | gauge | Limit is `container_spec_memory_swap_limit_bytes`. Grows with process age on swap-enabled nodes. |
| `mz_memory_limiter_memory_usage_bytes` | gauge | Memory plus swap, the quantity the limiter enforces. Divide by `mz_memory_limiter_memory_limit_bytes`. |
| `mz_metrics_libc_ru_maxrss_bytes` | gauge, monotone per process | Peak resident set. Resets to zero on restart, so it measures the current generation only. |
| `kubelet_volume_stats_used_bytes` | gauge | Scratch disk. Limit is `kubelet_volume_stats_capacity_bytes`. |
| `container_start_time_seconds` | gauge | Source of the uptime panel, and the cheapest way to establish restart age. |
| `kube_pod_container_status_restarts_total` | counter | Resets when a pod is replaced, which every upgrade does. |
| `kube_pod_container_status_last_terminated_exitcode` | gauge | `== 137` is an OOM kill. Counting matching series beats summing restart increases. |
| `container_network_receive_bytes_total`, `container_network_transmit_bytes_total` | counter | Per pod, with no `container` label. |
| `mz_metrics_libc_ru_minflt_total`, `mz_metrics_libc_ru_majflt_total` | counter | Major faults spike hard during rehydration and while paging in from swap. |
| `mz_metrics_libc_ru_utime_seconds_total`, `mz_metrics_libc_ru_stime_seconds_total` | counter | Process-reported CPU. Cross-checks the cAdvisor CPU series, and the two should move together. |

### By cluster

| Metric | Type | Notes |
|---|---|---|
| `mz_compute_controller_replica_count`, `_collection_count`, `_peek_count`, `_subscribe_count` | gauge | Controller-side inventory. Collection count tracks catalog growth, so a slow rise is expected. |
| `mz_compute_controller_command_queue_size` | gauge | Sustained depth means the controller is not draining. |
| `mz_compute_controller_response_send_count`, `_recv_count` | counter | Response queue depth, as the difference of the two. There is no depth gauge, because the response channel is an `instrumented_unbounded_channel` (`src/ore/src/channel.rs`), which takes a send and a receive counter and exports nothing else. Contrast `command_queue_size`, a real gauge that the command path increments and decrements directly. |
| `mz_compute_commands_total`, `mz_compute_responses_total` | counter | Protocol volume. Doubles for one bucket during a zero-downtime upgrade. |
| `mz_compute_command_message_bytes_total`, `mz_compute_response_message_bytes_total` | counter | Protocol bytes. Worth checking when a change touches command encoding. |
| `mz_compute_controller_history_command_count`, `_history_dataflow_count` | gauge | Controller-side command history, which should be reduced and not grow without bound. |
| `mz_compute_replica_history_command_count`, `_history_dataflow_count` | gauge | Replica-side equivalent. |
| `mz_compute_peeks_total` | counter | Label `result`. Successes are `rows` and `rows_stashed`; anything else is an error or a cancellation. |
| `mz_compute_peek_duration_seconds_bucket` | histogram | Quantiles need `sum by (le)` after any namespace join. |
| `v2_mz_dataflow_elapsed_seconds_total` | counter | Compute time. Strongly workload-shaped, and often the noisiest series on the dashboard. |
| `mz_arrangement_maintenance_seconds_total` | counter | Merge and compaction work. Uses the `cluster_environmentd_*` labels. |
| `v2_mz_compute_replica_park_duration_seconds_total` | counter | Time parked, so a fall here alongside a CPU rise localizes new work to the dataflow loop. |
| `v2_mz_arrangement_count`, `_record_count`, `_batch_count`, `_size_bytes` | gauge | Bimodal, see hazards. |
| `v2_mz_hint_group_size_potential_savings_bytes` | gauge | Missing group-size hints. Informational, and not release-sensitive. |
| `v2_mz_orphan_dataflow_count` | gauge | Non-zero means a dataflow outlived its collection, which is a genuine bug signal. |
| `v2_mz_dataflow_error_count` | gauge | Erroring dataflows. Some staging environments carry a permanent floor. |
| `mz_cluster_handle_command_duration_seconds_bucket` | histogram | Replica-side command handling latency. |
| `mz_dataflow_replica_expiration_timestamp_seconds`, `_expiration_remaining_seconds` | gauge | Replica expiration. Panels filter `> 0` and `!= 0` because the metric is exported as zero when unset. |
| `mz_subscribe_snapshots_skipped_total` | counter | Subscribe snapshot optimization hit rate. The panel appends `> 0` to hide inactive replicas. |

## Hazards and invariants

Each entry states a property that holds at any fleet size, followed by the measurement it came from. The property is what survives a release. The measurement is dated, describes whatever fleet existed when it was taken, and is recorded only so the property is not mistaken for a guess.

**OOM kills, dataflow errors, and orphan dataflows are absent rather than zero when healthy.** Production canary returned no series at all for `v2_mz_dataflow_error_count` and `v2_mz_orphan_dataflow_count` across a full week. That is the healthy case, and it is indistinguishable from a renamed metric unless the metric is confirmed to exist elsewhere.

**`v2_mz_orphan_dataflow_count` above zero is always a bug, never a load effect.**

**Working set falls at every upgrade.** In production canary us-east-1 the sum fell from about 290 GB to about 236 GB at the v26.37.0 rollout with no change in the code that mattered, purely because arrangements were rebuilt fresh. Judge memory by the slope within a release, not the step across one.

**Arrangement gauges are bimodal.** `v2_mz_arrangement_record_count` and `v2_mz_arrangement_size_bytes` swing by a factor of three to ten as periodic dataflows rebuild. In August 2026 staging us-east-1 alternated between 0.72e9 and 2.86e9 records with no release involvement. Compare low state against low state, because spike heights are not comparable.

**Peak resident set and swap are restart-sensitive.** Both reset or decay at an upgrade, so a level drop across the boundary is the restart and not the release. Peak resident set therefore describes the current generation only.

**Arrangement maintenance ramps after a restart.** Measured at 0.020 s/s one day after an upgrade and 0.030 s/s three days later on the same release, so an apparent increase across a boundary can be nothing more than a difference in age.

**Compute time and park time are complementary.** A CPU rise with a park fall localizes new work to the dataflow loop, while a CPU rise with park flat points outside it.

**Response queue depth is a difference of two counters, so it only holds while neither has reset.** Both reset when a pod is replaced, which every upgrade does, and a scrape that catches one reset and not the other yields a wild value. Staging us-east-1 read -761 in one bucket and +5371 in another over a window where every other bucket sat within one of zero. Read a single implausible bucket as a reset artifact, and judge the panel by whether it returns to zero rather than by any one sample.

## Known noise classes

Some environments are unhealthy independently of the release, and their contribution is constant across the boundary rather than absent. Check for these first, because they can dominate a fleet aggregate.

* Staging carries persistently crashlooping replicas. Their restart rate is high and flat, and flat means not release-related.
* Some staging environments carry permanently erroring dataflows, likewise flat.
* Where such an environment masks everything else, exclude it with the dashboard variables, as the panel instructions suggest.

## Order of magnitude

Recorded 2026-08 for scope-checking a query, not for comparison. If a result sits an order of magnitude away from these, suspect a mis-scoped selector rather than a regression. Derive the actual baseline from the before-window of your own run.

* Production canary, two environments: a few cores of clusterd CPU, hundreds of GB of working set summed, single-digit peeks per second.
* Staging, about fifteen environments: a few cores, low hundreds of GB, tens of peeks per second.
* System clusters account for most of staging's dataflow time and roughly a quarter of production canary's CPU.

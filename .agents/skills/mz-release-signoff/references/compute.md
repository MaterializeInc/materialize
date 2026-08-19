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
| `mz_compute_controller_response_queue_size` | gauge | Exists directly, but the cluster-level panel instead computes `mz_compute_controller_response_send_count - mz_compute_controller_response_recv_count`. |
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

## Hazards

**Arrangement gauges are bimodal.** `v2_mz_arrangement_record_count` and `v2_mz_arrangement_size_bytes` swing by a factor of three to ten as periodic dataflows rebuild. In August 2026 staging us-east-1 alternated between 0.72e9 and 2.86e9 records with no release involvement. Compare low state against low state.

**Peak resident set and swap are restart-sensitive.** Both reset or decay at an upgrade, so a level drop across the boundary is the restart and not the release.

**Working set falls at every upgrade.** In prod canary us-east-1 the sum fell from about 290 GB to about 236 GB at the v26.37.0 rollout with no version change in the code that mattered, purely because arrangements were rebuilt fresh. Judge memory by the slope within a release, not the step across one.

**Error and orphan gauges are absent when zero.** Prod canary returned no series at all for `v2_mz_dataflow_error_count` and `v2_mz_orphan_dataflow_count` across a full week. That is the healthy case, and it is indistinguishable from a renamed metric unless the metric is confirmed to exist elsewhere.

**Arrangement maintenance ramps after a restart.** Measured at 0.020 s/s one day after an upgrade and 0.030 s/s three days later on the same release, so an apparent increase across a boundary can be nothing more than a difference in age.

## Measured baselines

Measured 2026-08-19 for v26.38.0-rc.3 against v26.37.0, six hour buckets, summed across the selected namespaces. These are reference values for calibration, not thresholds. Fleet size changes will move all of them.

### Production canary, all clusters

| Metric | us-east-1, 2 envs | eu-west-1, 2 envs | us-west-2, 1 env |
|---|---|---|---|
| clusterd processes | 30 to 35 | | |
| CPU, cores | 3.76 to 4.12 | 0.65 to 0.68 | 0.25 to 0.26 |
| Working set | 236 to 257 GB | 10.8 to 13.4 GB | 8.8 to 11.8 GB |
| Peak RSS sum | 360 to 376 GB | 24 to 30 GB | 19 to 27 GB |
| Swap sum | 232 to 248 GB | | |
| Capacity, percent of limit, max | 0.46, 0.61 at upgrade | | |
| Restarts, OOM kills | 0, 0 | 0, 0 | 0, 0 |
| Minor faults | 105k to 170k per s | | |
| Major faults | 10 to 42 per s, 25k during rehydration | | |
| utime, stime | 3.36 to 3.52, 0.38 to 0.46 | | |
| Peeks per s | 7.1 to 8.6 | 3.10 | 1.927 |
| Failed peeks per s | 0.007 to 0.016 | | |
| Peek p99 | 4.2 to 7.0 s | 0.91 to 0.96 s | 0.79 to 0.93 s |
| Dataflow elapsed per s | 1.6 to 4.4 | 0.33 to 1.64 | 0.09 to 0.77 |
| Arrangement maintenance per s | 0.077 to 0.082 | 0.019 to 0.022 | 0.006 to 0.010 |
| Park per s | 115 to 116 | | |
| Arrangement records, base | 2.24e9 | | |
| Arrangement size, base | 203 GB | 740 to 770 MB | 495 to 720 MB |
| Arrangement size, spikes | 0.81 to 2.72 TB | | |
| Commands, responses per s | 1620 to 1790, 1790 to 1930 | 750 to 890 | 500 to 680 |
| Controller collections | 1348 to 1365 | | |
| Dataflow errors, orphans | absent | absent | absent |

### Production canary us-east-1, system clusters only

| Metric | Value |
|---|---|
| CPU, cores | 0.95 to 1.09 |
| Working set | 17 to 41 GB |
| Peeks per s | 2.63 to 2.75 |
| Peek p99 | 0.66 to 0.81 s |
| Arrangement maintenance per s | 0.030 |
| Arrangement size, base | 2.6 GB |

### Staging

| Metric | us-east-1, 15 envs | us-east-1 system only | eu-west-1, 4 envs |
|---|---|---|---|
| clusterd processes | about 190 | | |
| CPU, cores | 3.27 to 3.41 | 2.59 to 2.72 | 1.19 to 1.25 |
| Working set | 168 to 183 GB | 30 to 40 GB | 9.6 to 15.2 GB |
| Peak RSS sum | 69 to 85 GB | 45 to 53 GB | 15 to 22 GB |
| Capacity, percent of limit | avg 0.05, max 0.58, spikes 0.91 | | |
| Restarts per 6h | about 90, pre-existing crashloopers | | 0 |
| Major faults | 1300 to 1500 per s | | |
| Peeks per s | 25.87 | 19.33 | 5.45 |
| Failed peeks per s | 0.080 | | |
| Peek p99 | 0.92 to 0.94 s | 0.83 to 0.88 s | 0.91 to 0.95 s |
| Dataflow elapsed per s | 1.8 to 9.3, sawtooth | | 0.77 to 6.3 |
| Arrangement maintenance per s | 0.115 | 0.104 to 0.108 | 0.032 to 0.046 |
| Park per s | 73 to 75 | | |
| Arrangement records, base | 0.72e9, high state 2.86e9 | 105e6 | |
| Arrangement size, base | 39 GB, high state 155 GB | 6.3 GB | 1.22 to 1.25 GB |
| Commands, responses per s | 6680 to 7360, 6170 to 6480 | 4750 to 5060 | 1280 to 1530 |
| Controller collections | 6130 to 6190 | | |
| Dataflow errors | absent | | 50 to 400, pre-existing |

### Calibration reference

Staging us-east-1 fleet clusterd CPU across three weeks to 2026-08-19, twelve hour buckets: 5.55, 4.09, 4.86, 2.83, 3.37 cores. Any step below roughly 10% in staging is inside this spread and carries no information on its own.

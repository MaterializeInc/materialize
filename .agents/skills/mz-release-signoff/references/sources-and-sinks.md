# Sources and Sinks metric reference

Two dashboards. `storage-overview`, UID `e6dc7745-7d35-4968-a23d-689883a984bb`, is the sweep. `storage-upsert-sources`, UID `ac2de0ab-4a35-48b5-93aa-7e645569debb`, is a drill-down that requires a specific pod and source, and its own panel titles say so. Run the sweep first and only open the upsert dashboard when a source-level signal needs explaining.

## Variables

The two dashboards disagree on naming, and neither matches the compute dashboard.

| Dashboard | Environment variable | Others |
|---|---|---|
| `storage-overview` | `$env` | `$cluster`, `$replica`, `$object_id`, `$pod` |
| `storage-upsert-sources` | `$namespace` | `$pod`, `$source` |

Within `storage-overview` the environment matcher is itself inconsistent: some panels use `namespace=~"$env"` and others `namespace=~".*$env.*"`. The behaviour is the same for a full namespace value, so this is untidy rather than wrong.

## This dashboard has its own sign-off panel

`storage-overview` carries a `Signing off on Releases` text panel with instructions that differ from the compute one. Read it each run. As of 2026-08-19 it says to check all three `cloud-staging` datasources plus the production sandbox environment on all three `cloud-production` datasources, to include at least one day before the canary release, and to look for discrepancies in source and sink counts, new replicas with short uptime, regressed memory and CPU, and drastic changes in source, sink, and command statistics.

Two corrections to that panel. It points at `MaterializeInc/cloud/.github/ISSUE_TEMPLATE/03-release.md` for the canary environment list, and that file no longer exists; the canonical list now lives in `MaterializeInc/release/templates/issue.md`, in the `bin/deploy upgrade production` invocation. It also names only the sandbox environment, while the release bot links both sandbox and analytics for us-east-1, and Prometheus shows both running the release candidate.

## Roster

### Storage objects

| Metric | Type | Notes |
|---|---|---|
| `v2_mz_storage_objects` | gauge | Labels `id`, `type` (`source` or `sink`), `object_type`, `envelope_type`, `networking_type`, `cluster_id`, `replica_id`. `replica_id="none"` means the object has no replicas, which is how the active panels filter. |

Counting these needs the dashboard's inner aggregation, or multi-replica objects are counted once per replica:

```promql
sum(group by (id, namespace) (v2_mz_storage_objects{namespace=~"...", type="source"}))
```

The panel comments attribute this guard to a past double-counting bug, so keep it even when it looks redundant.

### Source statistics

| Metric | Type | Notes |
|---|---|---|
| `mz_source_messages_received`, `mz_source_bytes_received` | counter | Upstream read volume. |
| `mz_source_updates_staged`, `mz_source_updates_committed` | counter | Persist sink progress. Staged above committed is normal transiently, and a persistent gap is workload-dependent rather than automatically a fault. |
| `mz_source_offset_known`, `mz_source_offset_committed` | gauge | Upstream lag. Summing across sources gives a crude fleet lag proxy only. |
| `mz_source_progress` | gauge, milliseconds | A frontier timestamp, not a counter. See hazards. |
| `mz_source_snapshot_records_known`, `mz_source_snapshot_records_staged` | gauge | Snapshot progress, the ratio being the percentage panel. |
| `mz_source_rehydration_latency_ms` | gauge | An alternative to the upsert-specific rehydration metric, per the panel comment. |

Label naming is not consistent: the rate panels filter `parent_source_id` while the gauge panels filter `source_id`. Copy whichever the panel uses.

### Upsert and envelope state

| Metric | Type | Notes |
|---|---|---|
| `mz_source_records_indexed`, `mz_source_bytes_indexed` | gauge | Envelope state size. Their ratio is the average record size panel. |
| `mz_storage_upsert_deletes_total`, `mz_source_envelope_state_tombstones` | counter, gauge | |
| `mz_storage_upsert_state_rehydration_latency` | gauge, milliseconds | Last rehydration duration. See hazards. |
| `mz_storage_upsert_state_rehydration_total`, `_rehydration_updates` | counter | Records and updates replayed. Their ratio exposes retraction-heavy state. |
| `mz_storage_upsert_state_rocksdb_autospill_in_use` | removed | Deleted along with the autospill upsert backend, so the percentage panel that divides `sum` by `count` renders empty and reads as no worker spilling. The surviving members of the family are `_rehydration_latency`, `_rehydration_total`, and `_rehydration_updates`. |
| `mz_storage_upsert_backpressure_emitted_bytes`, `_retired_bytes`, `_last_backpressured_bytes` | counter, gauge | Backpressure. |
| `mz_storage_upsert_merge_snapshot_latency_bucket` | histogram | |
| `mz_storage_upsert_merge_snapshot_updates_total`, `_inserts_total`, `_deletes_total` | counter | |
| `mz_storage_rocksdb_multi_get_latency_bucket`, `_multi_put_latency_bucket` | histogram | Panels plot p95, p99, and p999. |
| `mz_storage_rocksdb_multi_get_count_total`, `_size_total`, `_result_bytes_total`, `_result_count_total`, and the `multi_put` equivalents | counter | Batch sizes are the size over the count. |
| `mz_persist_shard_update_count` | gauge, by `shard` and `name` | Filtered by `name="$source"`, so it joins a source to its persist shard. |

### Sink statistics

| Metric | Type | Notes |
|---|---|---|
| `mz_sink_messages_staged`, `mz_sink_messages_committed` | counter | |
| `mz_sink_bytes_staged`, `mz_sink_bytes_committed` | counter | |
| `mz_sink_oustanding_progress_records` | gauge | The metric name carries a spelling error, `oustanding`. `mz_sink_consumed_progress_records` also exists and is not on the dashboard. |
| `mz_sink_partition_count` | gauge | The panel aggregates with `max` and no namespace filter beyond the grouping. |

### Shard finalization

`mz_shard_finalization_outstanding` and `mz_shard_finalization_pending_commit` are gauges; `mz_shard_finalization_op_started`, `_op_succeeded`, and `_op_failed` are counters. The started and succeeded panels wrap `rate` in `max by (namespace, pod)`, which is unusual but harmless.

### Controller protocol

The two panels each carry an `or` fallback that straddles a metric rename:

```promql
sum by (namespace) (rate(mz_storage_messages_sent_bytes_count{...}[...]))
or
sum by (namespace) (rate(mz_storage_commands_total{...}[...]))
```

Only the second arm resolves today. `mz_storage_messages_sent_bytes_count`, `_received_bytes_count`, `_sent_bytes_sum`, and `_received_bytes_sum` no longer exist. The live names are `mz_storage_commands_total`, `mz_storage_responses_total`, `mz_storage_command_message_bytes_total`, and `mz_storage_response_message_bytes_total`. Do not read the `or` as a bug; it is a deliberate hedge so the panel keeps working across the version boundary, and it is worth imitating.

### Basic health

This row selects every container in the matched pods, via `container!="POD", container!=""` and a `$pod` prefix, rather than just `clusterd`. It is therefore not comparable to the compute dashboard's process row, which pins `container="clusterd"`.

`Container Max RSS Memory Usage` has two series. The `mz_metrics_libc_ru_maxrss` one, multiplied by 1024 to convert from KiB, refers to a metric that no longer exists; the `mz_metrics_libc_ru_maxrss_bytes` one works.

## Hazards and invariants

Each entry states a property that holds at any fleet size, followed by the measurement it came from. The property is what survives a release. The measurement is dated, describes whatever fleet existed when it was taken, and is recorded only so the property is not mistaken for a guess.

**`mz_source_progress` is a frontier timestamp, so its rate is a health check, not a throughput.** The metric holds a millisecond epoch, so a healthy advancing frontier yields a rate of exactly 1000 per series. Production canary measured a flat 87000, meaning 87 series advancing at wall-clock rate. A value below the expected multiple of 1000 means some frontiers are stalled, and the multi-hundred-million spikes seen in every upgrade bucket are frontier reinitialization, not progress. Judge this panel by counting series, never by the absolute number.

**Rehydration latency is a last-value gauge.** `mz_storage_upsert_state_rehydration_latency` holds the most recent rehydration's duration and does not decay, so it forms a staircase with one step per upgrade. That makes it the most useful single release signal here, because each step is a direct measurement of the new version rehydrating. A flat line between upgrades carries no information.

**Upsert state is absent from staging.** `mz_source_records_indexed` and `mz_source_bytes_indexed` summed to exactly zero across staging us-east-1 for the whole window, and the RocksDB and rehydration series were missing entirely. The upsert dashboard can only be verified from the production sandbox, which is why the panel instructions emphasize it.

**Staged and committed update rates differ by environment class.** Production canary measured them equal at about 145 per second. Staging measured 9.59 staged against 6.60 committed, a persistent 45% gap that was stable across the boundary. Compare each stack against its own history rather than expecting the two counters to track.

**Object counts must be aggregated with the inner `group by (id, ...)` guard.** Without it, multi-replica objects are counted once per replica.

**Shard finalization failures should be zero.** Outstanding and pending counts spike at upgrades and then drain.

**The version panel is known broken.** `storage-overview` titles its version panel `Materialize Version (currently broken?)`. Use the compute dashboard's version panel, or the query in the skill's step 1, to establish boundaries.

## Order of magnitude

Recorded 2026-08 for scope-checking only.

* Production canary carries tens of sources and a handful of sinks. Staging carries a similar number of sources and very few sinks.
* Upstream read rates are order a hundred messages per second in production canary and order ten in staging.
* Envelope state in the production sandbox is order a billion records and a hundred GB. Single-digit GB means the selector is wrong.

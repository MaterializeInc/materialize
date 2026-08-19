# Persist metric reference

Dashboard `persist`, UID `m3U1U6ZVk`. It carries 375 panel targets across roughly 230 distinct `mz_persist_*` metrics, which is far more than a release sweep should touch. The dashboard solves this itself: the `Top-Level` row includes a panel literally titled `should be small`, whose series are a curated list of counters that ought to sit near zero. That panel plus the throughput, timings, and cmds panels beside it are the sweep. Everything else, in particular the `By Shard` and `Compaction state` rows, is drill-down.

## Variables

`$env` for the namespace and `$pod`, matching the adapter dashboard rather than the compute one.

Persist reports its own build independently of `mz_version`: `mz_persist_metadata_seconds` carries a `version` label, and the `# processes by version` panel counts by it. This is a useful cross-check that a rollout actually reached the persist clients.

## The sweep subset

### Should be small

Every one of these is a counter whose rate should be at or near zero. Treat any of them departing from its measured baseline as a finding, because unlike the throughput metrics they have no legitimate reason to grow with load.

`mz_persist_blob_failures`, `mz_persist_consensus_failures`, `mz_persist_state_update_state_slow_path`, `mz_persist_lease_timeout_read`, `mz_persist_compaction_noop`, `mz_persist_compaction_failed`, `mz_persist_compaction_dropped`, `mz_persist_external_blob_delete_noop_count`, `mz_persist_external_failed_count`, `mz_persist_cmd_failed_count`, `mz_persist_pushdown_parts_mismatched_stats_count`, `mz_persist_schema_cache_fetch_state_count`, `mz_persist_shard_unconsolidated_snapshot`, `mz_txn_placeholder_schema_apply`, and `mz_persist_columnar_op_count{op="validation", result="invalid"}`.

The columnar validation series appears twice on the panel, once as `mz_persist_columnar_validation_count{result="invalid"}` and once as `mz_persist_columnar_op_count{op="validation", result="invalid"}`. Only the second resolved in production us-east-1.

Worth adding to the sweep even though the panel omits them: `mz_persist_compaction_timed_out`, `mz_persist_s3_operation_timeouts`, `mz_persist_pubsub_client_grpc_error_count`, `mz_txn_op_errored_count`, and `mz_txn_op_retry_count`.

### Volume and timings

| Metric | Type | Notes |
|---|---|---|
| `mz_persist_user_bytes`, `mz_persist_user_goodbytes` | counter | Physical and logical write volume. Goodbytes exceeds bytes because bytes are encoded and compressed. |
| `mz_persist_cmd_started_count`, `_succeeded_count`, `_failed_count` | counter | State machine command volume. |
| `mz_persist_cmd_cas_mismatch_count` | counter | Compare-and-set contention on shard state. Superlinear in writer concurrency, so it is the most sensitive contention signal on the dashboard. Always normalize by `cmd_started_count`. |
| `mz_persist_cmd_seconds` | counter of seconds | Time in state machine commands. |
| `mz_persist_encode_seconds`, `mz_persist_decode_seconds` | counter of seconds | Codec cost. Decode rises sharply during rehydration. |
| `mz_persist_external_seconds`, `_started_count`, `_succeeded_count`, `_failed_count` | counter | Blob and consensus calls. External time dominates every other timing series. |
| `mz_persist_external_rtt_latency`, `mz_persist_external_op_latency_bucket` | gauge, histogram | Per-operation latency, split by `op`. |
| `mz_persist_compaction_seconds`, `_requested`, `_applied`, `_bytes`, `_goodbytes` | counter | Compaction. Requested minus applied equals the noop and dropped counts. |
| `mz_persist_gc_seconds`, `_started`, `_finished`, `_noop`, `_skipped`, `_merged_reqs` | counter | Garbage collection. |
| `mz_persist_retry_retries_count`, `_started_count`, `_finished_count`, `_sleep_seconds` | counter | The panel excludes `op="next_listen_batch"`, which has its own panel because it retries by design. |
| `mz_persist_shard_upper` | gauge, per shard | Counting distinct `shard` labels gives the shard count. |
| `mz_persist_read_batch_part_bytes`, `_count` | counter, by `op` | The `op="unindexed"` slice is the `unindexed reads` panel. Rises by roughly 3x during rehydration. |
| `mz_persist_pushdown_parts_filtered_count`, `_fetched_count`, `_inline_count`, `_faked_count` and their `_bytes` variants | counter | The ratio panels divide one by the sum of all four. |
| `mz_persist_blob_cache_hits_bytes`, `_hits_blobs`, `_evictions` | counter | |
| `mz_txn_batch_unapplied_count`, `_unapplied_min_ts`, `_commit_count`, `_commit_bytes` | gauge, counter | Txn-shard backlog. |
| `mz_txn_op_started_count`, `_succeeded_count`, `_retry_count`, `_errored_count` | counter, by `op` | |

### Everything else

The remaining rows are for drill-down once the sweep flags something: `By Shard` for per-shard state, `Compaction` and `Compaction state` for compaction internals, `GC`, `External`, `Retries`, `Codec`, `Audit`, `Postgres/Consensus`, `PubSub Server` and `PubSub Client`, `Schema/Structured`, `Rehydration`, and `Txns`. Per-shard panels are keyed by `shard` and often by `name`, which is how a source or collection is joined to its shard, for example `mz_persist_shard_update_count{name="$source"}`.

## Hazards

**The panel named `compaction write amp` is a compression ratio, not an amplification.** It computes `mz_persist_compaction_bytes / mz_persist_compaction_goodbytes`, and production canary measured 0.14 to 0.24. Values below one are the healthy case and mean compaction is compressing. A rise toward one is the bad direction, which is the opposite of what the name suggests.

**`mz_persist_pushdown_parts_faked_*` needs the `or up * 0` guard.** The dashboard writes `rate(...) or (up{...} * 0)` because the metric is absent when nothing is faked, and without the guard the whole ratio goes empty rather than to zero. Imitate this whenever a ratio's numerator can vanish.

**CAS mismatch must be normalized.** The raw rate moves with command volume, which moves with fleet size. Divide by `mz_persist_cmd_started_count` before comparing across a boundary; the normalized ratio is stable to three significant figures within a release, which makes it an unusually sharp instrument.

**GC and decode move in opposite directions during an upgrade.** `mz_persist_gc_finished` dips, because a restarting process stops collecting, while `mz_persist_decode_seconds` and unindexed read bytes spike, because state is being refetched. Both are upgrade artifacts and neither is a finding.

**Two writers contend during a zero-downtime upgrade.** CAS mismatch rose roughly six-fold in production canary during each upgrade bucket, from about 0.4 to about 2.5 per second, then returned. This is the two generations writing the same shards and is expected.

## Measured baselines

Measured 2026-08-19 for v26.38.0-rc.3 against v26.37.0, twelve hour buckets, summed across the selected namespaces. Boundaries are production canary 2026-08-15, staging 2026-08-14, with a second `rc` on 2026-08-18.

| Metric | Production canary us-east-1, 2 envs | Staging us-east-1, 16 envs |
|---|---|---|
| User bytes per s | 252 to 318 kB | 305 to 306 kB |
| User goodbytes per s | 732 to 852 kB | |
| Commands started per s | 537 to 590 | 1345 before, 1417 after, see findings |
| Commands failed per s | 0 | 0, one bucket at 5e-5 |
| CAS mismatch per s | 0.30 to 0.44, 2.5 during upgrades | 8.2 before, 14.1 after, 12.2 on rc.3 |
| CAS mismatch per command | 0.00070 to 0.00076 | 0.00608 before, 0.00990 after, 0.00853 on rc.3 |
| Command seconds per s | 4.1 to 4.9 | 11.8 to 14.4 |
| External seconds per s | 7.0 to 8.7 | 19.4 to 25.1 |
| External calls started per s | 636 to 802 | |
| Encode seconds per s | 0.020 to 0.025 | |
| Decode seconds per s | 0.17 to 0.21, 0.48 to 0.53 during upgrades | |
| Compaction seconds per s | 0.57 to 0.72 | 1.61 to 1.83 |
| Compaction requested per s | 12.6 to 13.9 | |
| Compaction applied per s | 12.6 to 13.8 | 37.0 before, 37.6 after |
| Compaction write amp, that is compression | 0.14 to 0.24 | |
| Compaction noop per s | 0.048 to 0.072 | |
| GC seconds per s | 0.28 to 0.33 | 0.71 to 0.79 |
| GC finished per s | 6.6, dipping to 5.9 during upgrades | 13.26 before, 13.52 after |
| Shards | 1451 to 1470 | 1709 to 1711 |
| Unindexed read bytes per s | 28 to 37 MB, 103 to 112 MB during upgrades | 1.0 to 1.8 MB |
| Pushdown filtered fraction | 0.064 to 0.28 | |
| Blob cache hit bytes per s | 2.2 to 3.1 MB | |
| Lease timeout reads per s | 0.0002 to 0.0006 | 0.0094 to 0.0123 |
| Txn batches unapplied | about 1.0 to 1.2 | 0.07 to 0.57 |
| Blob, consensus, compaction, external failures | 0 | 0 |
| PubSub gRPC errors per s | 0 | 0, one bucket at 8e-4 |
| Txn op retries and errors per s | 0 | |

## Open finding from the characterization run

Staging us-east-1 stepped on CAS mismatch per persist command exactly at the release boundary, and the step is not explained by volume or fleet composition. The ratio held 0.00608 to 0.00629 for the three days before, jumped to 0.00990 in the first bucket after the v26.38.0-rc.1 upgrade, held between 0.00988 and 0.01009 for the next four days, then fell to 0.00853 with rc.3 and held there. Commands started stepped alongside it from 1345 to 1417 per second, about 5%.

Three things make this worth a Persist owner's attention rather than dismissal. The step is normalized per command, so it survives the calibration test in the skill's step 6. It is sharp, landing inside the upgrade bucket and flat on both sides, which drift cannot produce. And rc.3 recovered about a third of it, which points at a specific change rather than at the environment.

Against it being a release blocker: no failure counter moved, command and external latency were flat, and production canary showed no step at all with its normalized ratio slightly falling, from 0.00076 to 0.00070. The plausible reading is more frequent state writes raising contention superlinearly in a dense multi-environment stack, which staging is and the two-environment canary is not.

Recheck this ratio next release. If it stays elevated, the question for Persist is which v26.38 change increased state-write frequency by about 5%.

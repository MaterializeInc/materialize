# Persist metric reference

Dashboard `persist`, UID `m3U1U6ZVk`. It carries 375 panel targets across roughly 230 distinct `mz_persist_*` metrics, which is far more than a release sweep should touch. The dashboard solves this itself: the `Top-Level` row includes a panel literally titled `should be small`, whose series are a curated list of counters that ought to sit near zero. That panel plus the throughput, timings, and cmds panels beside it are the sweep. Everything else, in particular the `By Shard` and `Compaction state` rows, is drill-down.

## Variables

`$env` for the namespace and `$pod`, matching the adapter dashboard rather than the compute one.

Persist reports its own build independently of `mz_version`: `mz_persist_metadata_seconds` carries a `version` label, and the `# processes by version` panel counts by it. This is a useful cross-check that a rollout actually reached the persist clients.

## The sweep subset

### Should be small

Every one of these is a counter whose rate should be at or near zero. Treat any of them departing from its measured baseline as a finding, because unlike the throughput metrics they have no legitimate reason to grow with load.

`mz_persist_blob_failures`, `mz_persist_consensus_failures`, `mz_persist_state_update_state_slow_path`, `mz_persist_lease_timeout_read`, `mz_persist_compaction_noop`, `mz_persist_compaction_failed`, `mz_persist_compaction_dropped`, `mz_persist_external_blob_delete_noop_count`, `mz_persist_external_failed_count`, `mz_persist_cmd_failed_count`, `mz_persist_pushdown_parts_mismatched_stats_count`, `mz_persist_schema_cache_fetch_state_count`, `mz_persist_shard_unconsolidated_snapshot`, and `mz_persist_columnar_op_count{op="validation", result="invalid"}`.

Two of the panel's sixteen arms are dead and are deliberately left out of the list above, because an arm that cannot resolve contributes a permanent zero and makes the panel look healthier than it is. `mz_persist_columnar_validation_count{result="invalid"}` duplicates the columnar validation series that `mz_persist_columnar_op_count{op="validation", result="invalid"}` already carries, and only the second resolves. `mz_txn_placeholder_schema_apply` resolves nowhere. Neither name has ever appeared in the Materialize source tree, in any release from v26.36.0 through v26.39.0-rc.3 or in the history behind them, so both are panel-authored rather than renamed. Sweep the fourteen that resolve and treat the panel's own count as wrong by two.

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

## Hazards and invariants

Each entry states a property that holds at any fleet size, followed by the measurement it came from. The property is what survives a release. The measurement is dated, describes whatever fleet existed when it was taken, and is recorded only so the property is not mistaken for a guess.

**Every metric in the `should be small` list has no legitimate reason to grow with load.** Any of them departing from zero is a finding rather than a scaling effect.

**CAS mismatch must be normalized by `mz_persist_cmd_started_count`.** The raw rate moves with command volume, which moves with fleet size, while the normalized ratio is stable to three significant figures within a release. That stability is what makes it an unusually sharp instrument.

**Compaction requested minus applied equals the noop plus dropped counts.** If it does not, one of the three is broken.

**The panel named `compaction write amp` is a compression ratio, not an amplification.** It computes `mz_persist_compaction_bytes / mz_persist_compaction_goodbytes`, and production canary measured 0.14 to 0.24. Values below one are the healthy case and mean compaction is compressing. A rise toward one is the bad direction, which is the opposite of what the name suggests.

**`mz_persist_pushdown_parts_faked_*` needs the `or up * 0` guard.** The dashboard writes `rate(...) or (up{...} * 0)` because the metric is absent when nothing is faked, and without the guard the whole ratio goes empty rather than to zero. Imitate this whenever a ratio's numerator can vanish.

**Two writers contend during a zero-downtime upgrade.** CAS mismatch rose roughly six-fold in production canary during each upgrade bucket, from about 0.4 to about 2.5 per second, then returned. This is the two generations writing the same shards and is expected.

**GC and decode move in opposite directions during an upgrade.** `mz_persist_gc_finished` dips, because a restarting process stops collecting, while `mz_persist_decode_seconds` and unindexed read bytes spike, because state is being refetched. Both are upgrade artifacts and neither is a finding.

**Persist reports its own build independently of `mz_version`.** `mz_persist_metadata_seconds` carries a `version` label, and the `# processes by version` panel counts by it. Use it to confirm a rollout reached the persist clients.

**Counting distinct `shard` labels on `mz_persist_shard_upper` is how the shard count is obtained.** There is no shard-count gauge.

## Order of magnitude

Recorded 2026-08 for scope-checking only.

* External time dominates every other timing series, by roughly a factor of two over command time and an order of magnitude over compaction.
* Command rates are hundreds per second for a handful of environments and low thousands across a staging fleet.
* Shard counts are order a thousand per stack.
* Normalized CAS mismatch is order 1e-3 in a two-environment canary and order 1e-2 in a dense staging fleet, so the two stacks are not comparable to each other.

## Open finding from the characterization run

Staging us-east-1 stepped on CAS mismatch per persist command exactly at the release boundary, and the step is not explained by volume or fleet composition. The ratio held 0.00608 to 0.00629 for the three days before, jumped to 0.00990 in the first bucket after the v26.38.0-rc.1 upgrade, held between 0.00988 and 0.01009 for the next four days, then fell to 0.00853 with rc.3 and held there. Commands started stepped alongside it from 1345 to 1417 per second, about 5%.

Three things make this worth a Persist owner's attention rather than dismissal. The step is normalized per command, so it survives the calibration test in the skill's step 6. It is sharp, landing inside the upgrade bucket and flat on both sides, which drift cannot produce. And rc.3 recovered about a third of it, which points at a specific change rather than at the environment.

Against it being a release blocker: no failure counter moved, command and external latency were flat, and production canary showed no step at all with its normalized ratio slightly falling, from 0.00076 to 0.00070. The plausible reading is more frequent state writes raising contention superlinearly in a dense multi-environment stack, which staging is and the two-environment canary is not.

Recheck this ratio next release. If it stays elevated, the question for Persist is which v26.38 change increased state-write frequency by about 5%.

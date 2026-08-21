# Adapter metric reference

Dashboard `environmentd-health`, UID `mR1Kg1d4z`. All metrics come from `environmentd`, one process per environment, so there is no cluster or replica dimension and no system-cluster split. During a zero-downtime upgrade two `environmentd` pods run at once and every sum doubles for one bucket.

## Variables

This dashboard names its environment variable `$env`, not `$namespace`, and also exposes `$pod`. The release bot sets `var-namespace` and `var-env` on every link for exactly this reason. When reproducing a panel expression, substitute `$env` with the namespace regex.

## Roster

### Durable catalog

| Metric | Type | Notes |
|---|---|---|
| `mz_catalog_transaction_commit_latency_seconds_{bucket,count,sum}` | histogram | Catalog write latency. |
| `mz_catalog_transaction_commits`, `mz_catalog_transactions_started` | counter | Commit volume, very low in steady state. |
| `mz_catalog_sync_latency_seconds_{bucket,count,sum}` | histogram | Catalog read-side sync latency. |
| `mz_catalog_syncs` | counter | Sync volume. |
| `mz_catalog_collection_entries` | gauge, by `collection` | Catalog size. Grows monotonically with catalog contents, so a slow rise is expected and a fall is worth explaining. |
| `mz_catalog_snapshot_latency_seconds_*`, `mz_catalog_snapshot_seconds_*`, `mz_catalog_snapshots_taken` | histogram, counter | Snapshots are taken at boot, so these are only non-empty around a restart. |
| `mz_catalog_transact_seconds_*`, `mz_catalog_transact_phase_seconds_*` | histogram | Finer breakdown than the commit latency histogram. |
| `mz_catalog_allocate_id_seconds_*` | histogram | ID allocation. |
| `mz_catalog_snapshot_cache`, `mz_catalog_snapshot_consolidations`, `mz_catalog_snapshot_max_entries` | counter, gauge | Snapshot cache behaviour. |
| `mz_catalog_arc_strong_count`, `mz_catalog_arc_weak_count` | gauge | Catalog handle counts, useful for leak hunting. |
| `v2_mz_catalog_items` | gauge | Item count, an alternative to collection entries. |

### Adapter and coordinator

| Metric | Type | Notes |
|---|---|---|
| `v2_mz_envd_up` | gauge | One per healthy `environmentd`. The cheapest liveness check, and it should equal the environment count. |
| `mz_start_time_environmentd` | gauge, milliseconds | Startup duration. Divide by 1000. Resets only on restart, so it reports the current generation's boot. |
| `mz_connection_status` | counter, by `status` and `source` | Upstream connection outcomes. |
| `mz_determine_timestamp` | counter, by `respond_immediately` and `isolation_level` | Timestamp selection volume. A fall in `respond_immediately` means more queries are waiting on a timestamp. |
| `mz_time_to_first_row_seconds_bucket` | histogram, by `isolation_level` | The label values are `strict serializable` and `serializable`, with a space. Their distributions differ by roughly a factor of five, so never aggregate across the label. |
| `mz_row_set_finishing_seconds_{bucket,sum,count}` | histogram | Row-set finishing. The panel title notes the top bucket is 16s. |
| `mz_linearize_message_seconds_bucket` | histogram, by `immediately_handled` | Read linearization. Sub-millisecond in steady state. |
| `mz_slow_message_handling_{bucket,sum,count}` | histogram, by `message_kind` | Coordinator message handling. The `_sum` rate is the coordinator's busy time and the best single coordinator-load signal. |
| `mz_coord_queue_busy_seconds_{bucket,count}` | histogram | Coordinator queue. The `> 1s` panel subtracts the `le="1"` bucket increase from the count increase. |
| `mz_append_table_duration_seconds_{bucket,sum,count}` | histogram | Table append latency. |
| `mz_query_total` | counter, by `session_type` and `statement_type` | `session_type` is `user` or `system`. System queries dominate by an order of magnitude. |
| `mz_active_sessions` | gauge, by `session_type` | Session counts. |

### HTTP and auth

| Metric | Type | Notes |
|---|---|---|
| `mz_http_requests_total` | counter, by `path` and `status` | Panels split webhook paths (`/api/webhook.*`) from the rest, because webhook volume swamps everything else. |
| `mz_http_request_duration_seconds_bucket` | histogram | Same split. |
| `mz_auth_request_count` | counter, by `path`, `status`, `mz_context_org_name` | Frontegg. Status values are strings such as `401 Unauthorized`, not bare codes. |
| `mz_auth_request_duration_seconds_bucket` | histogram, by `path` | |
| `mz_auth_refresh_tasks_active` | gauge | |

### Infrastructure

Container metrics select `container="environmentd"`. CPU percent divides by `container_spec_cpu_quota / container_spec_cpu_period`, and memory percent by `container_spec_memory_limit_bytes`.

`crdb_dedicated_sys_cpu_combined_percent_normalized` and its `_maximum` variant carry no namespace label. They describe the whole regional CockroachDB cluster, so they are shared across all environments in the region and cannot be attributed to the release under test.

## Broken panels, as of 2026-08-19

These render empty and are not evidence of a healthy system. Verified against the metric catalogue in production us-east-1.

* `Avg Transaction Commit Latency` and `Avg Transaction Commit Latency by Env` divide `mz_catalog_transaction_commit_latency_seconds` by `mz_catalog_transaction_commits`. The first name does not exist, because the metric is a histogram exposing only `_bucket`, `_count`, and `_sum`. Use `rate(..._sum) / rate(..._count)` instead.
* `Avg Sync Latency` and `Avg Sync Latency By Env` have the same defect for `mz_catalog_sync_latency_seconds`.
* `Stash (CRDB) Query Latencies` reads `mz_query_latency_bucket`, which no longer exists. The catalog moved off the stash, so the panel has no replacement.
* `Swap Usage (bytes)` plots `container_spec_swap_limit_bytes` as its limit series. The real name is `container_spec_memory_swap_limit_bytes`. The usage series is fine.

## Hazards

**Two p99 panels are pinned by bucket resolution.** `p99 Slow Coordinator Messages` reported 0.0001276 s across eight days in staging with five-digit stability, and `Coordinator Table Append Latencies` p99 reported 0.12673 s in production with the same rigidity, both while the underlying counters advanced normally. The quantile is landing inside one wide bucket, so interpolation returns the bucket boundary and the panel cannot move. Read `rate(_sum) / rate(_count)` instead for these two, and treat an implausibly constant quantile anywhere as a bucket artifact rather than as stability.

**Snapshot latency is NaN except around restarts.** `mz_catalog_snapshot_latency_seconds` only records at boot, so an average over a steady-state window divides by zero. Non-NaN values in an upgrade bucket are the expected case, not a finding.

**`environmentd` does not swap.** Swap usage measured exactly zero across the whole window in production canary. A non-zero value there is itself the finding.

**Serializable and strict serializable time-to-first-row are not comparable.** In production canary the p99 ran about 1.2 s for strict serializable and about 6.5 s for serializable over the same window. Compare each against its own history.

## Invariants

* `v2_mz_envd_up` should equal the environment count. It is the cheapest liveness check on the dashboard.
* `environmentd` does not swap. A non-zero swap reading is itself the finding.
* Catalog collection entries grow monotonically with catalog contents. A fall needs explaining.
* Catalog snapshot latency is recorded at boot only, so it is NaN over a steady-state window and non-NaN in an upgrade bucket. Neither is a finding.
* Serializable and strict serializable time-to-first-row differ by roughly a factor of five. Never aggregate across the `isolation_level` label, and compare each against its own history.
* System queries outnumber user queries by an order of magnitude, and by far more in staging, where most environments are idle apart from introspection. Staging user-query numbers are not a workload signal.
* Coordinator busy time, the `_sum` rate of `mz_slow_message_handling`, is the best single coordinator-load signal, because the message rate alone hides how expensive each message was.
* `crdb_dedicated_*` metrics describe the whole regional CockroachDB cluster and cannot be attributed to the release under test.

## Order of magnitude

Recorded 2026-08 for scope-checking only. Derive the real baseline from your own before-window.

* `environmentd` CPU is a fraction of a core per environment, and working set is low single-digit GB per environment.
* Coordinator message rate is thousands per second per environment, and coordinator busy time a few hundredths of a second per second.
* Linearize and coordinator-message quantiles are sub-millisecond. Time to first row is order one second for strict serializable and several seconds for serializable.
* Catalog commits are rare, well under one per second, so their latency average is noisy by construction.

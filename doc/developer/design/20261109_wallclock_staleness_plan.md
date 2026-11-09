# Wall-clock staleness implementation plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Record per-object histograms of wall-clock staleness, charged per sample at its worst case, and report freshness in milliseconds, behind two feature flags.

**Architecture:** Pure logic in `mz_storage_types::freshness` (buckets, reflected time, recorder, reporter), a shared per-frontier state in `mz_storage_client::freshness`, and hooks in both controllers at frontier advances, maintenance ticks and collection lifecycle events. Rows go to new `mz_internal` builtin sources through the existing introspection append path.

**Tech Stack:** Rust, Materialize controllers, builtin catalog, testdrive, sqllogictest, mzcompose.

**Spec:** `doc/developer/design/20261109_wallclock_staleness.md` (PR #40586). Neither the spec nor this plan goes into the implementation stack.

**Old code:** The superseded stack is at commit `8b83a7e33e` (branches `freshness/*` on `upstream`). "Port from X" means start from `git show 8b83a7e33e:<path>` or the named commit and remove what the spec drops. The reuse map is in the session notes, and the names below are the ones it found.

## Global Constraints

* Flags `enable_wallclock_freshness_reporting` and `enable_wallclock_freshness_recording`, default `false`, on in CI through `get_minimal_system_parameters` in `misc/python/materialize/mzcompose/__init__.py`.
* Dyncfgs `wallclock_freshness_buckets_per_octave` (default 4), `wallclock_freshness_period_interval` (default 60 s), `wallclock_freshness_histogram_retention_interval` (default 30 days). No virtual arrival interval and no restart policy.
* Bucket k covers staleness in (2^((k - 1) / b), 2^(k / b)] ms, and staleness of at most 1 ms is bucket 0.
* Rows carry `measure = 'end_to_end'` only.
* Count unit is milliseconds of wall-clock time. Every full period of a recording sums to the period length.
* Instants are `mz_repr::Timestamp` (milliseconds since the Unix epoch) in every Rust interface, never a bare `u64`. Spans such as staleness and period length are `std::time::Duration`. Only the per-bucket count is a plain `u64` of milliseconds, because it becomes a diff. SQL columns stay `timestamp with time zone`, which holds milliseconds exactly.
* Staleness is `now.saturating_sub(reflected)`, never negative.
* No `as` conversions (`mz_ore::cast::CastFrom`/`CastLossy`), no `std::collections::HashMap`, no `unsafe`.
* Large test modules in `<module>/tests.rs` with `#[cfg(test)] mod tests;`.
* Comments follow the repository's comment rules: no em-dashes, no structuring semicolons, contracts in doc comments, reasoning inline.
* Before each commit: `bin/fmt`, `CARGO_INCREMENTAL=0 cargo check -p <crates touched> --all-targets`. Commit trailers as in the session's attribution.
* Do not hand-edit `*.snap` or SLT expectations, regenerate them with the rewrite modes the `mz-test` skill names.

## Review Focus

1. A frontier that lies ahead of the controller's wall clock (clusterd clock ahead, or a REFRESH view): staleness reads 0, no underflow, no panic. Test in Task 3.
2. The period dyncfg changes in the middle of a period: the open period is cut at that moment, nothing is counted twice, and later periods align to the new grid. Test in Task 3.
3. The controller's wall clock steps backwards: the sample is dropped and the next one starts at the new time. Test in Task 3.
4. The recording flag is turned off and on again: the recording restarts at the moment it turns on, without a sample spanning the off time. Test in Task 6.
5. A replica that is removed while a period is open emits its partial period once, and a rehydrated replica keeps its non-introspection recordings. Test in Task 7.

---

## Layer 1: `storage-types: wall-clock staleness model`

### Task 1: Buckets and the module

**Files:**
- Create: `src/storage-types/src/freshness.rs`, `src/storage-types/src/freshness/histogram.rs`, `src/storage-types/src/freshness/tests.rs`
- Modify: `src/storage-types/src/lib.rs`

**Interfaces:**
- Produces: `pub struct BucketScheme` with `new(per_octave: u32) -> Option<Self>`, `per_octave(&self) -> u32`, `bucket(&self, staleness: Duration) -> i32` (whole milliseconds, rounded up), `upper_bound_ms(&self, k: i32) -> f64`.

- [ ] **Step 1:** Port `BucketScheme` from `8b83a7e33e:src/storage-types/src/freshness/histogram.rs` without `add_span`/`add_span_with`, and its tests from `8b83a7e33e:src/storage-types/src/freshness/tests.rs` (`bucket_*`). Rename "latency" to "staleness" in docs.
- [ ] **Step 2:** Add a test `bucket_bounds_are_inclusive_above`: for b in [1, 2, 4, 8] and k in 1..=64, `bucket(floor(upper_bound_ms(k))) == k` where that bound is an integer above 1, and `bucket(0) == 0`, `bucket(1) == 0`, `bucket(2) == b`.
- [ ] **Step 3:** Run `cargo nextest run -p mz-storage-types freshness`. Expected: PASS.
- [ ] **Step 4:** Commit `storage-types: exponential staleness buckets`.

### Task 2: Reflected time

**Files:**
- Create: `src/storage-types/src/freshness/reflected.rs`
- Test: `src/storage-types/src/freshness/tests.rs`

**Interfaces:**
- Consumes: `TimeDependence` from `src/storage-types/src/time_dependence.rs` (unchanged).
- Produces: `pub fn reflected_time(dependence: &TimeDependence, frontier: Timestamp) -> Option<Timestamp>`.

Semantics from the spec: `None` if `frontier` is 0, or if a schedule has no refresh at or before its bound. Otherwise `reflected_at(dependence, frontier - 1)`, where a node first rounds the bound down to `last_refresh_at_or_before(schedule, bound)` if it has a schedule, and then returns the bound if `dependence` is empty, else the minimum over children at the rounded bound.

- [ ] **Step 1:** Write tests: wall clock gives `F - 1`, frontier 0 gives `None`, a `REFRESH EVERY '1 minute'` node at frontier `next_refresh` gives the last refresh, a node with children `[refresh 60 s, refresh 7 s]` gives the minimum of both, and a refresh before its first refresh gives `None`.
- [ ] **Step 2:** Write a proptest `reflected_time_matches_oracle`, modeled on `last_reflected_matches_oracle` in the old tests (l.257): random trees of depth up to 3 with `REFRESH EVERY` schedules of 1 to 120 s and random aligns, frontiers up to 10 minutes. The oracle scans t downward from `F - 1` and returns the first t for which every root-to-leaf path's refresh rounding of t, applied innermost first with `round_up_timestamp`, stays below `F`. Expected equality.
- [ ] **Step 3:** Run the tests, expect FAIL, implement `reflected_time`, porting `last_refresh_at_or_before` from `8b83a7e33e:src/storage-types/src/freshness.rs` l.612 with its saturation NOTE, run again, expect PASS.
- [ ] **Step 4:** Commit `storage-types: invert time dependence to the reflected time`.

### Task 3: Recorder and reporter

**Files:**
- Create: `src/storage-types/src/freshness/recorder.rs`, `src/storage-types/src/freshness/reporter.rs`
- Test: `src/storage-types/src/freshness/tests.rs`

**Interfaces:**
- Consumes: `BucketScheme`.
- Produces:
  - `pub struct RecorderConfig { pub scheme: BucketScheme, pub period: Duration, pub retention: Duration }`
  - `pub struct PeriodRow { pub period_start: Timestamp, pub period_end: Timestamp, pub buckets_per_octave: u32, pub bucket: i32, pub count_ms: u64 }`
  - `pub struct StalenessRecorder` with `new(start: Timestamp, reflected: Timestamp) -> Self`, `observe(&mut self, now: Timestamp, reflected: Timestamp, config: &RecorderConfig) -> Vec<PeriodRow>`, `tick(&mut self, now: Timestamp, config: &RecorderConfig) -> Vec<PeriodRow>`, `finish(&mut self, now: Timestamp, config: &RecorderConfig) -> Vec<PeriodRow>`.
  - `pub struct StalenessReporter` with `new(start: Timestamp, reflected: Timestamp) -> Self`, `observe(&mut self, now: Timestamp, reflected: Timestamp)`, `read(&mut self, now: Timestamp) -> Duration`.

Recorder semantics: it holds the open sample's start `a` and reflected time `r`, and one open period of bucket counts. `observe` closes the sample at `now` and starts the next one with the new `r`. `tick` closes the sample at the last period boundary at or before `now`, if one was passed, and continues it from there with the same `r`. A sample `[a, b)` is split at period boundaries, and each part `[x, y)` adds `y - x` to the bucket of `y.saturating_sub(r)`. Periods are half-open on the grid of `period` from the epoch, except that the first period starts at `start` and a period-length change cuts the open period at the current sample's end. A period's rows are returned once `now` reaches its end. `now <= a` drops the sample and sets `a = now`. `finish` closes the sample at `now` and returns the partial period. Port the period bookkeeping (grid, cut, ordering assert) from `PeriodBook` in `8b83a7e33e:src/storage-types/src/freshness/period.rs`, without `CATCH_UP_MS`, `DAY_MS` and the arrival high-water mark.

Reporter semantics: `read` returns the maximum of the worst cases of samples closed since the last `read`, the worst case of the last closed sample, and `now - r`, then resets the first.

- [ ] **Step 1:** Write tests:
  - `steady_one_second_source`: start 0, observe at 1050, 2050, ..., 60050 with `r` = previous tick (0, 1000, ...). The period `[0, 60000)` has all its count in `bucket(1050)` and sums to 60000.
  - `stall_charges_each_period_at_its_end`: `r` fixed at 0, ticks at 60000, 120000, 180000. Rows are 60000 ms each in `bucket(60000)`, `bucket(120000)`, `bucket(180000)`.
  - `frontier_ahead_of_clock_reads_zero`: `r = now + 500`, the count lands in bucket 0.
  - `clock_steps_back_drops_sample`: observe at 1000, then 900, then 2000. The total count is `2000 - 900`.
  - `period_change_cuts_open_period`: switch `period` from 60 s to 10 s at 30000. Rows `[0, 30000)`, then `[30000, 40000)`, `[40000, 50000)`, and no overlap.
  - `finish_emits_partial_period`: finish at 30000 gives one period `[0, 30000)` summing to 30000, and later calls return nothing.
  - Proptest `mass_is_conserved`: random sequences of observe, tick and clock values. The emitted counts per full period equal the period in milliseconds, and the total equals the finish time minus the start, minus dropped backwards steps.
  - `reporter_keeps_peak_between_reads`: two samples with worst cases 1200 and 1050 between reads, `read` returns 1200, the next `read` without samples returns `max(1050, now - r)`.
- [ ] **Step 2:** Run, expect FAIL. Implement. Run `cargo nextest run -p mz-storage-types freshness`, expect PASS.
- [ ] **Step 3:** Commit `storage-types: record and report wall-clock staleness`.

## Layer 2: `catalog: staleness histogram relations and configuration`

### Task 4: Relations, flags and dyncfgs

**Files:**
- Modify: `src/catalog/src/builtin.rs`, `src/catalog/src/builtin/mz_internal.rs`, `src/pgrepr-consts/src/oid.rs`, `src/storage-client/src/healthcheck.rs`, `src/storage-client/src/controller.rs` (`IntrospectionType`), `src/storage-controller/src/collection_mgmt.rs`, `src/controller-types/src/dyncfgs.rs`, `src/storage-types/src/dyncfgs.rs`, `src/sql/system-parameters.json`, `misc/python/materialize/mzcompose/__init__.py`, `misc/python/materialize/parallel_workload/action.py`, `test/launchdarkly-flag-consistency/mzcompose.py`, `doc/user/content/sql/system-catalog/mz_internal.md`, and the expectation files listed in Step 3.

**Interfaces:**
- Produces: builtins `MZ_WALLCLOCK_GLOBAL_FRESHNESS_HISTOGRAM_RAW`, `MZ_WALLCLOCK_GLOBAL_FRESHNESS_HISTOGRAM`, `MZ_WALLCLOCK_REPLICA_FRESHNESS_HISTOGRAM_RAW`, `MZ_WALLCLOCK_REPLICA_FRESHNESS_HISTOGRAM`, `MZ_WALLCLOCK_FRESHNESS_PROGRESS` (OIDs 17141 to 17145), `IntrospectionType::{WallclockFreshnessHistogram, ReplicaWallclockFreshnessHistogram}`, `WALLCLOCK_FRESHNESS_HISTOGRAM_RAW_DESC`, `REPLICA_WALLCLOCK_FRESHNESS_HISTOGRAM_RAW_DESC`, dyncfgs `ENABLE_WALLCLOCK_FRESHNESS_REPORTING`, `ENABLE_WALLCLOCK_FRESHNESS_RECORDING`, `WALLCLOCK_FRESHNESS_BUCKETS_PER_OCTAVE`, `WALLCLOCK_FRESHNESS_PERIOD_INTERVAL`, `WALLCLOCK_FRESHNESS_HISTOGRAM_RETENTION_INTERVAL`.

- [ ] **Step 1:** Port commit `7126b70546` and the follow-ups `f77f8b53a0`, `d0c7b97c2e`, `8943af6604` from the old `freshness/catalog` branch. Remove `WALLCLOCK_FRESHNESS_VIRTUAL_ARRIVAL_INTERVAL` and `WALLCLOCK_FRESHNESS_RESTART_POLICY` everywhere they appear (dyncfgs, JSON, mzcompose, parallel workload, LaunchDarkly list). Rewrite the column comments: `count` is milliseconds of wall-clock time, `bucket` indexes staleness, `measure` is `end_to_end`. Remove all `local` and `skew` text. Keep the retention arms in `partially_truncate_metrics_history` and the append arms in the write task.
- [ ] **Step 2:** Run `CARGO_INCREMENTAL=0 cargo check --workspace --all-targets` and `bin/lint-test-flags`. Expected: clean.
- [ ] **Step 3:** Regenerate expectations with the rewrite modes from the `mz-test` skill: `test/sqllogictest/oid.slt`, `information_schema_tables.slt`, `autogenerated/mz_internal.slt`, `catalog_server_explain.slt`, `mz_catalog_server_index_accounting.slt`, `test/testdrive/catalog.td`, `test/workload-replay/objects.txt`, `test/workload-replay/system_catalog_identifiers.txt`. Diff against commit `49246a7c18` to confirm the same names.
- [ ] **Step 4:** Commit `catalog: staleness histogram relations and configuration`.

## Layer 3: `controllers: freshness dependence`

### Task 5: Freshness dependence for every collection

**Files:**
- Modify: `src/storage-client/src/storage_collections.rs`, `src/compute-client/src/controller.rs`, `src/compute-client/src/controller/instance.rs`, `src/compute-client/src/as_of_selection.rs` (mock)
- Test: `src/storage-client/src/storage_collections.rs` test module, `src/compute-client/src/controller/tests.rs`

**Interfaces:**
- Produces: `StorageCollections::determine_freshness_dependence(&self, id: GlobalId) -> Result<Option<TimeDependence>, TimeDependenceError>`, `Instance::get_freshness_dependence(&self, id: GlobalId) -> Result<Option<TimeDependence>, CollectionMissing>`, a `freshness_dependence: Option<TimeDependence>` field next to `time_dependence` in both collection states, and `ComputeController::determine_freshness_dependence(&self, instance_id, dataflow, used_imports) -> Result<Option<TimeDependence>, TimeDependenceError>`.

The freshness dependence equals the time dependence except that `LoadGenerator(_)` ingestions are `Some(TimeDependence::default())`, and ingestion exports inherit their ingestion's freshness dependence. The compute side mirrors `determine_time_dependence` (controller.rs l.1171) with the freshness getters. Log collections get `Some(TimeDependence::default())`. `TimeDependence` and dataflow expiration stay unchanged.

- [ ] **Step 1:** Write tests: a load generator source has time dependence `None` and freshness dependence `Some(default)`; an index on a view over a load generator has freshness dependence `Some(default)` and time dependence `None`; a Kafka source has both equal.
- [ ] **Step 2:** Run, expect FAIL, implement, run `cargo nextest run -p mz-storage-client -p mz-compute-client freshness_dependence`, expect PASS.
- [ ] **Step 3:** Commit `controllers: track a freshness dependence that counts load generators as wall clock`.

## Layer 4: `compute: record staleness histograms`

### Task 6: Shared per-frontier state

**Files:**
- Create: `src/storage-client/src/freshness.rs`, `src/storage-client/src/freshness/tests.rs`
- Modify: `src/storage-client/src/lib.rs`

**Interfaces:**
- Consumes: `reflected_time`, `StalenessRecorder`, `StalenessReporter`, `RecorderConfig`, `PeriodRow`, the dyncfgs.
- Produces:
  - `pub fn recorder_config(dyncfg: &ConfigSet) -> Option<RecorderConfig>` (`None` when recording is off, ported from the old l.683 without `interval_ms`).
  - `pub fn pack_rows(rows: Vec<PeriodRow>, collection_id: GlobalId, replica_id: Option<ReplicaId>, labels: &BTreeMap<&'static str, String>) -> Vec<(Row, Diff)>`, ported from the old l.706 with `measure` fixed to `"end_to_end"`.
  - `pub struct FreshnessState` with `new(dependence: TimeDependence, start: Timestamp, frontier: &Antichain<Timestamp>, config: Option<&RecorderConfig>) -> Self`, `observe(&mut self, now: Timestamp, frontier: &Antichain<Timestamp>, config: Option<&RecorderConfig>) -> Vec<PeriodRow>`, `tick(&mut self, now: Timestamp, config: Option<&RecorderConfig>) -> Vec<PeriodRow>`, `finish(&mut self, now: Timestamp, config: Option<&RecorderConfig>) -> Vec<PeriodRow>`, `reported_lag(&mut self, now: Timestamp) -> Duration`.

`FreshnessState` always keeps a reporter and keeps a recorder only while `config` is `Some`. A recorder is created at the first call with `Some` config, starting at `start` on the first call and at `now` on later ones, and dropped at the first call with `None`. `reflected_time` returning `None` maps to the recording's start. An empty frontier calls `finish` and stops recording, and the reporter reads 0 from then on.

- [ ] **Step 1:** Write tests: `flag_off_then_on_restarts_at_now` (Review Focus 4), `empty_frontier_finishes_once`, `refresh_view_reads_last_refresh`, `pack_rows_layout` (column order of both descs, count as diff).
- [ ] **Step 2:** Run, expect FAIL, implement, run `cargo nextest run -p mz-storage-client freshness`, expect PASS.
- [ ] **Step 3:** Commit `storage-client: per-frontier staleness state`.

### Task 7: Compute controller hooks

**Files:**
- Modify: `src/compute-client/src/controller.rs`, `src/compute-client/src/controller/instance.rs`, `src/compute-client/src/controller/instance_client.rs`, `src/compute-client/src/controller/introspection.rs`, `src/compute-client/src/as_of_selection.rs` (mock stubs)
- Create: `src/compute-client/src/controller/instance/tests.rs` (move the existing inline tests first, as old commit `a982ac3833` did)

**Interfaces:**
- Consumes: `FreshnessState`, `recorder_config`, `pack_rows`, `get_freshness_dependence`.
- Produces: `CollectionState::freshness: Option<FreshnessState>` (global), `ReplicaCollectionState::freshness: Option<FreshnessState>`, `boot: Timestamp` on `ComputeController` passed to `Instance::new`.

Wiring, with the old locations as guides (`8b83a7e33e:src/compute-client/src/controller/instance.rs`):
* Global observe in `maybe_update_global_write_frontier` (old l.2189), replica observe in `handle_frontiers_response` (old l.2596).
* A new tick over all states in `maintain`, after the config refresh, delivering rows.
* Rows are packed with `pack_rows` and delivered by a `deliver_freshness_rows` that picks the introspection type by `replica_id` (old l.2329).
* Start time: `boot` for collections added before `initialized`, `now` after. Recording off while `read_only`. Transient collections are skipped.
* Drop: `finish` in `drop_collections`. Replica removal: `finish` for that replica's states. Rehydration: non-log states carry over, log states finish and start again at `now`.
* Log collections: no global state, per-replica states only, started at replica creation.

- [ ] **Step 1:** Port `FreshnessHarness` from the old instance tests (l.629-830) with its fake clock and `Recorded` decoding. Write tests: `global_and_replica_rows_per_period`, `log_collection_has_no_global_rows`, `drop_emits_partial_period_once`, `replica_removal_emits_partial_period` and `rehydration_keeps_non_log_recordings` (Review Focus 5), `restart_starts_at_boot`.
- [ ] **Step 2:** Run, expect FAIL, implement, run `cargo nextest run -p mz-compute-client freshness`, expect PASS.
- [ ] **Step 3:** Commit `compute: record staleness histograms`.

## Layer 5: `storage: record staleness histograms`

### Task 8: Storage controller hooks

**Files:**
- Modify: `src/storage-controller/src/lib.rs`, `src/storage-controller/src/collection_mgmt.rs`

**Interfaces:**
- Consumes: `FreshnessState`, `recorder_config`, `pack_rows`, `determine_freshness_dependence`.

Wiring, with `8b83a7e33e:src/storage-controller/src/lib.rs` as guide: observe in `update_write_frontier` (old l.3051-3103). Collections without a cluster (`CollectionStateExtra::None`, tables and storage-managed collections) observe in `refresh_wallclock_lag` (old l.3974). Tick in the same periodic path as `refresh_wallclock_lag`. Append through `append_freshness_updates` (old l.3209). Start at `boot` before initialization, at creation after. Finish in `drop_sources_unvalidated` and `drop_sinks_unvalidated`. Only the storage controller writes global rows of storage-owned collections, and materialized views stay with compute.

- [ ] **Step 1:** Write tests in the storage controller's test module where one exists, otherwise cover it in Task 10's testdrive: a table's rows sum to 60000 per full period.
- [ ] **Step 2:** Implement, run `CARGO_INCREMENTAL=0 cargo check -p mz-storage-controller --all-targets`, then the tests.
- [ ] **Step 3:** Commit `storage: record staleness histograms`.

## Layer 6: `controllers: report freshness in milliseconds`

### Task 9: Reporting

**Files:**
- Modify: `src/storage-client/src/controller.rs` (`WallclockLag`), `src/compute-client/src/controller/instance.rs`, `src/storage-controller/src/lib.rs`, `src/cluster-client/src/metrics.rs`, `src/cluster-client/src/lib.rs`
- Test: `src/storage-client/src/freshness/tests.rs`, `test/testdrive/wallclock-lag.td`

**Interfaces:**
- Consumes: `FreshnessState::reported_lag`.

Port the `WallclockLag::Millis(u64)` change with `from_duration`, `histogram_bucket` and `metric_seconds` from old commit `e4b15cdf56` and its tests. Under the reporting flag, `refresh_wallclock_lag` in both controllers uses `reported_lag(now)` for the per-minute history and the Prometheus metric. The existing global histogram keeps its 1 Hz frontier lag. With the flag off every value is as today.

- [ ] **Step 1:** Write tests: `reporting_off_is_unchanged`, `reporting_on_reads_sample_peak` (a 1 s source reads about 1 s, not a sawtooth).
- [ ] **Step 2:** Run, expect FAIL, implement, expect PASS. Update `test/testdrive/wallclock-lag.td` from its old version: REFRESH EVERY views read up to their period under the flag.
- [ ] **Step 3:** Commit `controllers: report freshness in milliseconds`.

## Layer 7: `test: end-to-end staleness recording`

### Task 10: Testdrive and restart

**Files:**
- Create: `test/testdrive/wallclock-freshness.td`
- Modify: `test/restart/mzcompose.py`

- [ ] **Step 1:** Port `test/testdrive/wallclock-freshness.td` from the old stack. Keep the checks that full periods sum to 60000 and periods are minute-aligned, drop `local` and `skew`. Add a load generator source and assert it has rows. Add a table and a REFRESH EVERY view.
- [ ] **Step 2:** Port `workflow_wallclock_freshness_restart_policy` as `workflow_wallclock_freshness_restart`, hole only: after a restart the progress view's `covered_until` shows a gap no longer than one period plus the downtime, and periods after it are contiguous.
- [ ] **Step 3:** Run both with the commands from the `mz-test` skill. Expected: PASS.
- [ ] **Step 4:** Commit `test: end-to-end staleness recording`.

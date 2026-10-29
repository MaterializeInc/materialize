# Tick-aware freshness implementation plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Report freshness in milliseconds as max(D, S) and record an arrival-weighted latency histogram per object and period in new `mz_internal` relations.

**Architecture:** Pure logic (freshness tree, assignment bound, bucket scheme, period recorder) lives in a new `mz_storage_types::freshness` module that both controllers use. The compute and storage controllers build a freshness tree per collection, feed every write frontier advance into a recorder, and append final period rows through new introspection types. Reporting swaps the 1s ceiling of `WallclockLagFn` for max(D, S) behind a flag.

**Tech Stack:** Rust, the compute and storage controllers, persist-backed introspection collections, dyncfgs, sqllogictest and testdrive.

**Spec:** `doc/developer/design/20261028_tick_aware_freshness.md` (PR #40320). The spec and this plan stay on the design branch and are not part of the implementation stack.

## Global constraints

* Base every branch on `upstream/main`. Stack the PRs in task order, one PR per task group marked below, each under about 500 changed lines.
* Never use `std::collections::HashMap`/`HashSet`, never use `as` conversions (use `mz_ore::cast::CastFrom`/`CastLossy`), no `unsafe`.
* Large test modules go in `<module>/tests.rs` declared with `#[cfg(test)] mod tests;`.
* Run `bin/fmt` and `CARGO_INCREMENTAL=0 cargo check -p <crate> --all-targets` before every commit. Use the `mz-test` skill before running tests and the `mz-feature-flags` skill before adding flags.
* Comment rules from `CLAUDE.md`: no em-dashes or structuring semicolons, doc comments state the contract, reasoning goes inline at the decision point.
* `DataflowDescription::time_dependence` and `TimeDependence` stay byte-identical. The freshness tree is a separate, controller-side structure.
* Latency and timestamps are integer milliseconds. Every arrival is counted exactly once per recorder, and every emitted row is final.
* Defaults: buckets per octave 4 (power of two), virtual arrival interval 1 ms, period 1 minute, retention 30 days, restart policy `hole`. Both feature flags are on in CI and off in production.

## Review focus

1. `ALTER SOURCE ... TIMESTAMP INTERVAL` between two advances: no arrival is counted twice and none is lost (test in Task 4).
2. A replica that reconnects with a regressed write frontier: the global recorder ignores the regression, the replica recorder does not re-cover recorded arrivals (test in Task 4).
3. A REFRESH EVERY view before its first refresh and after its last `REFRESH AT`: `last_reflected` returns `None` and nothing is recorded, no panic (test in Task 1).
4. The controller observes an advance before the last window ends (clock skew): every window still counts its full length, and latency is at least the window length (test in Task 3).
5. A 1000 ms x 1001 ms join stalled for a day: one advance costs time proportional to touched buckets, not windows (test in Task 3).

---

## PR 1: freshness model (Tasks 1 to 2)

### Task 1: Freshness tree and `last_reflected`

**Files:**
- Create: `src/storage-types/src/freshness.rs`
- Create: `src/storage-types/src/freshness/tests.rs`
- Modify: `src/storage-types/src/lib.rs` (add `pub mod freshness;`)

**Interfaces:**
- Produces:

```rust
pub enum Ticker {
    /// Ticks at multiples of `interval_ms` since the epoch.
    Aligned { interval_ms: u64 },
    /// Ticks every `interval_ms` at an unknown phase.
    Unaligned { interval_ms: u64 },
    /// Follows wall clock continuously.
    Continuous,
}
pub enum FreshnessLeaf { Ingestion(GlobalId), Fixed(Ticker) }
pub enum FreshnessTree {
    Leaf(FreshnessLeaf),
    Min(Vec<FreshnessTree>),
    Refresh { schedule: RefreshSchedule, input: Box<FreshnessTree> },
}
pub trait TickerLookup { fn ingestion_ticker(&self, id: GlobalId) -> Option<Ticker>; }
impl FreshnessTree {
    pub fn merge(inputs: Vec<FreshnessTree>, schedule: Option<&RefreshSchedule>) -> Option<FreshnessTree>;
    pub fn resolve(&self, lookup: &dyn TickerLookup) -> Option<ResolvedTree>;
}
pub struct ResolvedTree { /* tickers fixed at resolve time */ }
impl ResolvedTree {
    /// The largest arrival time whose updates are all visible below `frontier`.
    pub fn last_reflected(&self, frontier: Timestamp) -> Option<u64>;
    /// The smallest window boundary at or after `z` (the union of all aligned and unaligned leaf grids).
    pub fn next_boundary(&self, z: u64) -> u64;
    /// The tickers of all leaves, in tree order, used as the cache key for D.
    pub fn tickers(&self) -> Vec<Ticker>;
}
```

`merge` keeps distinct leaves (dedup equal leaves), returns `None` for no inputs, and unwraps a single input without schedule. `resolve` returns `None` if any ingestion leaf is unknown. Window boundaries of `Unaligned` leaves use the epoch-aligned grid of their interval, and `Continuous` leaves make every ms a boundary.

- [ ] **Step 1: Write the failing tests** in `freshness/tests.rs`:
  * `last_reflected_join_7_11`: for leaves `Aligned{7000}` and `Aligned{11000}` under `Min`, output frontiers 11001, 21001, 22001 and 28001 give `Some(7000)`, `Some(11000)`, `Some(21000)` and `Some(22000)`, the spec's worked example.
  * `last_reflected_refresh_before_first`: `Refresh{every 60s, aligned_to 0}` over `Aligned{1000}` returns `None` for a frontier below the first refresh.
  * `last_reflected_matches_oracle`: proptest over random trees (1 to 4 leaves, intervals 1..=50, optional refresh) and frontiers; compare with a brute-force oracle that scans arrivals t ascending and checks visibility leaf by leaf.
  * `next_boundary_union_of_grids`: `next_boundary(15000)` for 7s and 11s leaves is 21000, `next_boundary(22000)` is 22000.
- [ ] **Step 2:** Run `CARGO_INCREMENTAL=0 cargo test -p mz-storage-types --lib freshness`. Expected: compile failure, types missing.
- [ ] **Step 3: Implement** the types and methods above. `last_reflected` follows the spike (`time_dependence.rs` on branch `tick-aware-lag-spike`): bound = frontier - 1, round down to the last complete refresh with `round_down_timestamp_m1`, floor to each leaf's grid, minimum over leaves.
- [ ] **Step 4:** Run the tests. Expected: PASS.
- [ ] **Step 5: Commit** `storage-types: freshness tree and last_reflected`.

### Task 2: Assignment bound D

**Files:**
- Modify: `src/storage-types/src/freshness.rs`
- Modify: `src/storage-types/src/freshness/tests.rs`

**Interfaces:**
- Consumes: `ResolvedTree` from Task 1.
- Produces: `impl ResolvedTree { pub fn assignment_bound(&self) -> u64 }`, D in ms, never below the true worst case.

Closed form for a `Min` of aligned leaves without refresh: $\max_{k \ne m} (i_k + i_m - \gcd(i_k, i_m))$, or $i_1$ for one leaf. An unaligned leaf u contributes $i + u$ against aligned i, and $u_1 + u_2$ against another unaligned leaf. Continuous leaves contribute 0. For a `Refresh` node, scan the change points of the expected frontier over one hyperperiod of all intervals and refresh periods, including the points `c + u - 1` for each unaligned period u, without a time cap. If the hyperperiod exceeds 10^6 change points, return the conservative bound `D(input) + refresh period` instead.

- [ ] **Step 1: Write the failing tests:**
  * `bound_join_closed_form`: 7 s x 11 s is 17000, 1 s x 7 s is 7000, a single 100 ms leaf is 100, and 6 s x 10 s x 15 s is 20000 (the pairwise maximum, from 10 + 15 - 5).
  * `bound_aligned_unaligned`: `Aligned{7000}` with `Unaligned{1000}` is 8000.
  * `bound_refresh`: `Refresh{60s}` over `Aligned{1000}` is 60000, over 7s x 11s is 77000, `Refresh{every 30 days}` over `Aligned{1000}` is at least 30 days.
  * `bound_matches_oracle`: proptest, aligned leaves with intervals 1..=30 and optional refresh period 1..=60, against a brute-force maximum of visibility minus arrival over two hyperperiods.
- [ ] **Step 2:** Run them. Expected: FAIL.
- [ ] **Step 3: Implement** `assignment_bound`.
- [ ] **Step 4:** Run them. Expected: PASS.
- [ ] **Step 5: Commit** `storage-types: assignment bound of a freshness tree`.

## PR 2: histogram recording logic (Tasks 3 to 4)

### Task 3: Bucket scheme and window mass

**Files:**
- Create: `src/storage-types/src/freshness/histogram.rs`
- Modify: `src/storage-types/src/freshness.rs` (add `pub mod histogram;`), `src/storage-types/src/freshness/tests.rs`

**Interfaces:**
- Consumes: `ResolvedTree::next_boundary`.
- Produces:

```rust
pub struct BucketScheme { per_octave: u32 }
impl BucketScheme {
    /// `None` unless `per_octave` is a power of two in 1..=256.
    pub fn new(per_octave: u32) -> Option<Self>;
    pub fn per_octave(&self) -> u32;
    /// Bucket `k` covers latencies in (2^((k-1)/b), 2^(k/b)] ms.
    pub fn bucket(&self, latency_ms: u64) -> i32;
    pub fn upper_bound_ms(&self, k: i32) -> f64;
}
/// Adds the virtual arrivals in `(from, to]` that became visible at `advance_ms` to `counts`,
/// keyed by bucket. Arrivals sit at multiples of `interval_ms`.
pub fn add_span(tree: &ResolvedTree, scheme: &BucketScheme, interval_ms: u64,
    from: u64, to: u64, advance_ms: u64, counts: &mut BTreeMap<i32, u64>);
```

`add_span` walks buckets, not windows. With `A = max(advance_ms, to)` (the clock-skew clamp), the windows whose worst-case latency falls in bucket $(\ell, h]$ start in $[A - h, A - \ell)$, and their arrival time is $\beta(A - \ell) - \beta(A - h)$ clamped to $(from, to]$, where $\beta$ is `next_boundary`. Count the multiples of `interval_ms` in that range. Latency 0 cannot occur, since A is at least the window end.

- [ ] **Step 1: Write the failing tests:**
  * `bucket_boundaries`: with b = 4, `bucket(1) == 0`, `bucket(2) == 4`, `bucket(1024) == 40`, `bucket(1025) == 41`, and `upper_bound_ms(k - 1) < x <= upper_bound_ms(k)` for `k = bucket(x)` and x in 1..10_000.
  * `span_conserves_counts`: for random trees and spans, the sum of counts equals the number of multiples of `interval_ms` in `(from, to]`, for intervals 1, 7 and 10.
  * `span_matches_window_walk`: proptest comparing `add_span` with a reference that enumerates windows one by one.
  * `span_join_7_11_hyperperiod`: summing the spec's 77 s hyperperiod advances reproduces the distribution 7 at 7 s, 4 at 11 s, 6 at 12 s, 8 at 13 s, 31 at 14 s, 5 at 15 s, 7 at 16 s, 9 at 17 s (seconds of arrivals, checked with a scheme fine enough to separate them, b = 256).
  * `span_clock_skew`: `advance_ms < to` still counts every arrival with latency at least its window length.
  * `span_long_stall_is_cheap`: a 1000 ms x 1001 ms join over a span of one day completes and evaluates `next_boundary` at most twice per touched bucket, and a day spans at most 4 * 28 buckets (count calls through a test-only counter on `ResolvedTree`).
- [ ] **Step 2:** Run them. Expected: FAIL.
- [ ] **Step 3: Implement** `BucketScheme` and `add_span`.
- [ ] **Step 4:** Run them. Expected: PASS.
- [ ] **Step 5: Commit** `storage-types: exponential latency buckets and span accounting`.

### Task 4: Period recorder

**Files:**
- Create: `src/storage-types/src/freshness/recorder.rs`
- Modify: `src/storage-types/src/freshness/histogram.rs` or `freshness.rs` (module wiring), `src/storage-types/src/freshness/tests.rs`

**Interfaces:**
- Consumes: `add_span`, `BucketScheme`, `ResolvedTree`.
- Produces:

```rust
pub struct RecorderConfig { pub scheme: BucketScheme, pub interval_ms: u64, pub period_ms: u64 }
pub struct PeriodRow { pub period_start_ms: u64, pub period_end_ms: u64, pub buckets_per_octave: u32, pub bucket: i32, pub count: u64 }
pub struct FreshnessRecorder { /* high-water mark, open periods, finished flag */ }
impl FreshnessRecorder {
    /// Starts recording arrivals after `start_ms`: the object's creation time, a restart seed, or the current frontier.
    pub fn new(start_ms: u64) -> Self;
    /// Records a write frontier observed at `now_ms` and returns the rows of periods that closed.
    pub fn observe(&mut self, tree: &ResolvedTree, frontier: &Antichain<Timestamp>, now_ms: u64, config: &RecorderConfig) -> Vec<PeriodRow>;
}
```

`observe` computes `lr = last_reflected(frontier)`, records `(high_water, lr]` when `lr > high_water`, splitting at period boundaries so each arrival lands in the period it arrived in, then sets `high_water = lr`. It emits every open period whose end is at most `high_water`, once. An empty frontier emits all open periods up to `high_water` and finishes the recorder. A regressed frontier (`lr <= high_water`) records nothing. Open periods remember the scheme they were opened with, so a change of buckets per octave applies from the next period.

- [ ] **Step 1: Write the failing tests:**
  * `rows_are_final_and_conserved`: drive a 1 s source healthy for 5 minutes with advances every second; every emitted period has counts summing to 60_000 and is emitted once.
  * `stall_charged_to_arrival_periods`: a 90 s stall spanning two period boundaries emits those periods only after recovery, each with full mass, latencies falling from about 91 s.
  * `regress_does_not_double_count`: frontier goes 100_001, 50_001, 120_001; total counts equal 120_000 minus the start.
  * `alter_interval_does_not_double_count`: switch the ingestion ticker from 1 s to 11 s between two advances (re-resolve the tree); total counts stay equal to covered time.
  * `empty_frontier_finishes`: after an empty frontier, `observe` returns nothing more.
  * `scheme_change_applies_next_period`.
- [ ] **Step 2:** Run them. Expected: FAIL.
- [ ] **Step 3: Implement** `FreshnessRecorder`.
- [ ] **Step 4:** Run them. Expected: PASS.
- [ ] **Step 5: Commit** `storage-types: period recorder for freshness histograms`.

## PR 3: relations and configuration (Task 5)

### Task 5: Introspection relations, dyncfgs and flags

**Files:**
- Modify: `src/storage-client/src/controller.rs` (`IntrospectionType`: add `WallclockFreshnessHistogram`, `ReplicaWallclockFreshnessHistogram`)
- Modify: `src/storage-client/src/healthcheck.rs` (descs `WALLCLOCK_FRESHNESS_HISTOGRAM_RAW_DESC`, `REPLICA_WALLCLOCK_FRESHNESS_HISTOGRAM_RAW_DESC`)
- Modify: `src/catalog/src/builtin/mz_internal.rs`, `src/catalog/src/builtin.rs` (raw sources, `count` views, progress view), `src/pgrepr-consts/src/oid.rs` (new OIDs)
- Modify: `src/storage-controller/src/collection_mgmt.rs` (register as append-only metrics histories, truncate on `period_start`)
- Modify: `src/storage-types/src/dyncfgs.rs` (retention), `src/controller-types/src/dyncfgs.rs` (flags and recorder parameters)
- Modify: `src/compute-client/src/controller/introspection.rs` (senders)
- Test: `test/sqllogictest/autogenerated/mz_internal.slt`, `test/sqllogictest/oid.slt`, `test/sqllogictest/information_schema_tables.slt`, `test/testdrive/catalog.td`, `doc/user/content/sql/system-catalog/mz_internal.md`

**Interfaces:**
- Produces:
  * Raw relations `mz_internal.mz_wallclock_global_freshness_histogram_raw` with columns `period_start timestamptz`, `period_end timestamptz`, `object_id text`, `measure text` (`'end_to_end'` or `'local'`, see Tasks 11 and 12), `buckets_per_octave uint4`, `bucket int4`, `labels jsonb`; the update diff is the count. Views `mz_internal.mz_wallclock_global_freshness_histogram` adding `count` by `GROUP BY` with `sum` over the diff (as the existing `count(*)` view does). The replica variants add `replica_id text`.
  * View `mz_internal.mz_wallclock_freshness_progress` (`object_id`, `replica_id` nullable, `covered_until` = max `period_end`), unioning the global (null replica) and replica histograms.
  * Dyncfgs in `controller-types`: `enable_wallclock_freshness_reporting` (bool), `enable_wallclock_freshness_recording` (bool), `wallclock_freshness_buckets_per_octave` (u32, 4), `wallclock_freshness_virtual_arrival_interval` (Duration, 1 ms), `wallclock_freshness_period_interval` (Duration, 60 s), `wallclock_freshness_restart_policy` (String, `"hole"`). In `storage-types`: `wallclock_freshness_histogram_retention_interval` (Duration, 30 days). Flag defaults per the `mz-feature-flags` skill: off in code, on in CI.

- [ ] **Step 1:** Add the relations and dyncfgs, then rewrite the catalog tests listed above with the commands from the `mz-test` skill.
- [ ] **Step 2:** Run `bin/sqllogictest --optimized -- test/sqllogictest/autogenerated/mz_internal.slt test/sqllogictest/oid.slt` and `bin/lint-test-flags` (via `mz-feature-flags`). Expected: PASS.
- [ ] **Step 3: Commit** `catalog: freshness histogram relations and configuration`.

## PR 4: tickers and trees in the controllers (Task 6)

### Task 6: Live tickers and per-collection freshness trees

**Files:**
- Modify: `src/storage-client/src/storage_collections.rs` (keep `timestamp_interval` per ingestion, update on alter, implement `TickerLookup`, build `FreshnessTree` per collection next to `time_dependence`)
- Modify: `src/storage-client/src/storage_collections.rs` trait `StorageCollections` (add `fn freshness_tree(&self, id: GlobalId) -> Option<FreshnessTree>` and `fn ticker_lookup(&self) -> Arc<dyn TickerLookup + Send + Sync>`)
- Modify: `src/compute-client/src/controller.rs` (`determine_freshness_tree` next to `determine_time_dependence`, mirroring its import walk), `src/compute-client/src/controller/instance.rs` (store the tree on `CollectionState`, introspection collections get `Fixed(Aligned{introspection interval})`)

**Interfaces:**
- Consumes: `FreshnessTree`, `TickerLookup`, `Ticker`.
- Produces: `CollectionState::freshness_tree: Option<FreshnessTree>` in the compute controller, `StorageCollections::freshness_tree(id)` for storage collections. Leaves: ingestions and load generators `Ingestion(id)`; tables, webhooks and storage-managed collections `Fixed(Unaligned{1000})`; compute introspection `Fixed(Aligned{logging interval})`; anything else `Fixed(Continuous)`.

- [ ] **Step 1: Write the failing tests:** `freshness_tree_follows_alter` in `storage_collections` tests: after `alter_ingestion_source_desc` with a new interval, `ticker_lookup().ingestion_ticker(id)` returns the new interval. A compute controller test that a dataflow joining two ingestions gets a `Min` of two `Ingestion` leaves while its `time_dependence` stays equal to the value before this change.
- [ ] **Step 2:** Run them with `CARGO_INCREMENTAL=0 cargo test -p mz-storage-client -p mz-compute-client --lib freshness`. Expected: FAIL.
- [ ] **Step 3: Implement.**
- [ ] **Step 4:** Run them. Expected: PASS.
- [ ] **Step 5: Commit** `controllers: freshness trees with live tick intervals`.

## PR 5: recording (Tasks 7 to 8)

### Task 7: Compute controller recording

**Files:**
- Modify: `src/compute-client/src/controller/instance.rs`

**Interfaces:**
- Consumes: `FreshnessRecorder`, `RecorderConfig`, `ResolvedTree`, Task 5 introspection senders and dyncfgs, Task 6 trees.
- Produces: rows in both freshness histograms for compute collections.

Keep one `FreshnessRecorder` per collection, driven from the global write frontier in `maybe_update_global_write_frontier`, and one per replica collection, driven in the `Frontiers` response handler. Start recorders at the collection's creation time (replica: the replica collection's creation time), or for a controller restart, at the current frontier (policy `hole`). Cache `ResolvedTree` and D per collection keyed on `ResolvedTree::tickers()`. Convert `PeriodRow`s to introspection updates with the workload-class labels the existing histogram uses. Record only when `enable_wallclock_freshness_recording` is set and the controller is not read-only. Drop recorders with their collections.

- [ ] **Step 1: Write the failing test:** an instance-level test that drives frontier responses for a collection with a 1 s ingestion leaf over three minutes and asserts the sent histogram updates sum to 60_000 per closed period, plus a regressing replica frontier that does not change the global rows.
- [ ] **Step 2:** Run it. Expected: FAIL.
- [ ] **Step 3: Implement.**
- [ ] **Step 4:** Run it. Expected: PASS.
- [ ] **Step 5: Commit** `compute: record freshness histograms`.

### Task 8: Storage controller recording

**Files:**
- Modify: `src/storage-controller/src/lib.rs` (`update_write_frontier` for ingestions and exports, `refresh_wallclock_lag` for collections whose frontiers only `storage_collections` reports, such as tables and webhooks)

**Interfaces:**
- Consumes: as Task 7, with `StorageCollections::freshness_tree`.
- Produces: rows in the global freshness histogram for storage-owned collections. Materialized views stay with the compute controller, so each object has exactly one writer.

- [ ] **Step 1: Write the failing test:** testdrive `test/testdrive/wallclock-freshness.td` with a table and a load generator `TIMESTAMP INTERVAL '1s'`: every closed period after the first full one sums to 60_000 per object, and the progress view advances.
- [ ] **Step 2:** Run `bin/mzcompose --find testdrive run default -- wallclock-freshness.td`. Expected: FAIL.
- [ ] **Step 3: Implement.**
- [ ] **Step 4:** Run it. Expected: PASS.
- [ ] **Step 5: Commit** `storage: record freshness histograms`.

## PR 6: reporting (Task 9)

### Task 9: max(D, S) in milliseconds

**Files:**
- Modify: `src/storage-client/src/controller.rs` (`WallclockLag`: store milliseconds, keep `Undefined`)
- Modify: `src/cluster-client/src/lib.rs` (`WallclockLagFn`: compute the 1s ceiling only when reporting is off)
- Modify: `src/compute-client/src/controller/instance.rs`, `src/storage-controller/src/lib.rs` (`refresh_wallclock_lag`: under `enable_wallclock_freshness_reporting`, lag = max(D, now - last_reflected(frontier)) for collections with a resolved tree)
- Test: `test/testdrive/wallclock-lag.td`

**Interfaces:**
- Consumes: Task 2 D, Task 1 `last_reflected`, Task 6 trees.
- Produces: `WallclockLag::Millis(u64)` with `to_interval_datum` at ms precision. The existing global histogram keeps power-of-two seconds via `millis.div_ceil(1000).next_power_of_two()`. The Prometheus metric observes seconds as `f64`.

- [ ] **Step 1: Write the failing tests:** a unit test that with the flag off every reported value is a whole number of seconds and equals today's; testdrive cases in `wallclock-lag.td` with the flag on: a REFRESH EVERY '1 minute' view reports a lag of at least 59 s, a 1 s source below 2 s.
- [ ] **Step 2:** Run them. Expected: FAIL.
- [ ] **Step 3: Implement.**
- [ ] **Step 4:** Run them. Expected: PASS.
- [ ] **Step 5: Commit** `controllers: report freshness as max(D, S) in milliseconds`.

## PR 7: restart charge policy (Task 10)

### Task 10: `charge` restart policy

**Files:**
- Modify: `src/storage-controller/src/collection_mgmt.rs` (while truncating the freshness histograms at write-task start, collect the maximum `period_end` per object and replica)
- Modify: `src/storage-controller/src/lib.rs`, `src/compute-client/src/controller/instance.rs` (seed recorders from that map when the policy is `charge`, fall back to `hole` for objects without a seed)

**Interfaces:**
- Consumes: Task 5 relations, Tasks 7 and 8 recorders.
- Produces: `fn freshness_seeds(&self) -> BTreeMap<(GlobalId, Option<ReplicaId>), u64>` on the storage controller, available before recorders start.

- [ ] **Step 1: Write the failing test:** testdrive or mzcompose test that restarts environmentd with policy `charge` and checks that the periods across the restart are contiguous and sum to 60_000 each, and with `hole` that a gap appears in the progress view instead.
- [ ] **Step 2:** Run it. Expected: FAIL.
- [ ] **Step 3: Implement.**
- [ ] **Step 4:** Run it. Expected: PASS.
- [ ] **Step 5: Commit** `controllers: charge restart policy for freshness histograms`.

## PR 2b: local delay logic (Task 11, executed after Task 4)

### Task 11: Local delay recorder

Local delay attributes latency to hops. For an object C, the local delay of an arrival is the time C made it visible minus the time the slowest direct input of C had made it visible. A source has no inputs, so its local delay equals its end-to-end latency. Local delay uses the same virtual arrivals, windows, periods and finality rules as the end-to-end recorder, so its counts per period equal the end-to-end counts.

**Files:**
- Create: `src/storage-types/src/freshness/local.rs`
- Modify: `src/storage-types/src/freshness.rs` (module wiring), `src/storage-types/src/freshness/tests.rs`

**Interfaces:**
- Consumes: `ResolvedTree`, `BucketScheme`, `RecorderConfig`, `PeriodRow`, the period splitting of `FreshnessRecorder`.
- Produces:

```rust
/// Observation times of a collection's write frontier advances, kept while a consumer may still need them.
pub struct FrontierLog { /* (frontier, observed_ms), ascending */ }
impl FrontierLog {
    pub fn record(&mut self, frontier: Timestamp, observed_ms: u64);
    /// The earliest observation time at which the frontier exceeded `ts`, if observed.
    pub fn passed_at(&self, ts: Timestamp) -> Option<u64>;
    pub fn prune_through(&mut self, ts: Timestamp);
}
pub struct LocalDelayRecorder { /* like FreshnessRecorder */ }
impl LocalDelayRecorder {
    pub fn new(start_ms: u64) -> Self;
    /// Records an advance of the object to `frontier` at `now_ms`, given its direct inputs' logs.
    pub fn observe(&mut self, tree: &ResolvedTree, frontier: &Antichain<Timestamp>, now_ms: u64,
        inputs: &[&FrontierLog], config: &RecorderConfig) -> Vec<PeriodRow>;
}
```

All arrivals in one window share their output timestamp $T = \max_k \lceil e \rceil_{i_k}$ for window end $e$. Their ready time is the maximum over inputs of `passed_at(T)`, and their local delay is `now_ms` minus the ready time, at least 0. Group windows by input log segments, so work per advance is proportional to the input advances consumed, each consumed once. An input whose log no longer covers `T` (pruned or never observed) counts as ready at the object's own previous advance time, which understates and never overstates local delay. With no inputs, local delay equals the end-to-end latency.

- [ ] **Step 1: Write the failing tests:**
  * `local_chain_attributes_hops`: A (1 s source) -> B -> C, where B adds 3 s and C adds 0: C's local rows sit in the bucket of about 0 ms, B's in the bucket of 3000 ms, and C's end-to-end rows include both.
  * `local_counts_match_end_to_end`: for random advances, per period the local counts sum to the end-to-end counts of the same object.
  * `local_join_uses_slowest_input`.
  * `frontier_log_passed_at`: earliest observation strictly above `ts`, `None` before.
  * `local_missing_input_entry_understates`.
- [ ] **Step 2:** Run `CARGO_INCREMENTAL=0 cargo test -p mz-storage-types --lib freshness`. Expected: FAIL.
- [ ] **Step 3: Implement.**
- [ ] **Step 4:** Run them. Expected: PASS.
- [ ] **Step 5: Commit** `storage-types: local delay recorder for freshness attribution`.

## PR 5b: local delay recording (Task 12, executed after Task 8)

### Task 12: Record local delay in both controllers

**Files:**
- Modify: `src/compute-client/src/controller/instance.rs`, `src/storage-controller/src/lib.rs`

**Interfaces:**
- Consumes: Task 11 `FrontierLog`, `LocalDelayRecorder`; Task 5 relations with `measure`; Tasks 7 and 8 recorders.
- Produces: rows with `measure = 'local'` next to `measure = 'end_to_end'` (which Tasks 7 and 8 write) in the global and per-replica histograms.

Keep a `FrontierLog` per collection in each controller, recorded at the same point the end-to-end recorder observes advances. A compute collection's direct inputs are its dataflow's imports (indexes on the same instance and storage collections). The compute controller reads storage inputs' logs through `StorageCollections` (add `fn frontier_log(&self, id: GlobalId) -> Option<FrontierLog>` returning a clone of the recent entries, or an equivalent accessor), so a storage input's observation time is when its controller saw it. Prune a log through the minimum frontier its consumers have recorded.

- [ ] **Step 1: Write the failing test:** testdrive in `test/testdrive/wallclock-freshness.td`: a load generator with `TIMESTAMP INTERVAL '1s'`, a view over it and an index on the view. After two periods, the index's `local` rows sit below 1 s, the source's `local` rows equal its `end_to_end` rows, and per period the `local` and `end_to_end` counts of each object match.
- [ ] **Step 2:** Run `bin/mzcompose --find testdrive run default -- wallclock-freshness.td`. Expected: FAIL.
- [ ] **Step 3: Implement.**
- [ ] **Step 4:** Run it. Expected: PASS.
- [ ] **Step 5: Commit** `controllers: record local delay for freshness attribution`.

# Bounded staleness isolation

* Associated: TBD (open issue link to be added before merge)

## The problem

Materialize today offers two practical isolation levels for SELECT traffic: serializable and strict serializable.

* Strict serializable picks the freshest oracle-linearized timestamp across all queried objects.
  This guarantees real-time recency but forces the query to wait until every input collection has caught up to that timestamp.
  In practice this commonly costs ~1s of latency, governed by the timestamp tick rate, even when much fresher data already exists at a slightly lower timestamp.

* Serializable picks the freshest timestamp available without waiting.
  It avoids the wait but offers no upper bound on staleness.
  When an input collection lags --- for example a slow source or a stalled materialized view --- the chosen timestamp can drift arbitrarily far behind the freshest known state of the system.

Customers consistently want the third point on this curve: a bound on staleness rather than a guarantee of perfect freshness.
A typical request is "give me data no older than five seconds, but do not block to get newer data."
This is well known as bounded staleness in the literature and in production systems such as Spanner.

## Success criteria

A solution is successful when:

1. A user can configure a session to read at a freshness ceiling of `D` (e.g. five seconds) and have queries served at any timestamp `T` satisfying `R(now) - T <= D`, where `R` is whichever of wall clock or oracle `read_ts` the deployment is configured to anchor against, without waiting on input frontiers.
2. When the freshness ceiling cannot be met --- because some input collection lags by more than `D` --- the query returns a clear, actionable error that fires in the coordinator before any peek is dispatched, so the failure mode is independent of the cluster the query would have run on.
3. The default-anchor steady-state path does not pay the per-query timestamp-oracle round-trip that strict serializable pays.
4. The new isolation level is opt-in at two levels: the operator must enable a master feature flag, and individual sessions then opt in by `SET transaction_isolation`.
5. The design preserves all existing strict-serializable correctness invariants documented in `guide-adapter.md`.

## Out of scope
* A wait-with-timeout variant of the failure mode.
  We expect to add a sibling behavior later, gated on a separate session variable, that waits up to a deadline before erroring.
  This document specifies error-only behavior.
* Bounded staleness on timelines other than `EpochMilliseconds`.
  Both anchor variants rely on timeline timestamps tracking wall-clock time (the wall-clock variant directly; the oracle variant indirectly, through the oracle's wall-clock cap).
  Other timelines are out of scope until that property is re-derived for them.
* Cross-session linearizability.
  Two sessions running under bounded staleness may observe each other's writes at different timestamps within the bound.
  This is the explicit trade-off and matches the existing serializable contract.
* Interaction with `real_time_recency`.
  The two settings are contradictory; combining them errors at session-variable validation time.

## Solution proposal

### High level

Introduce an isolation level `bounded staleness <duration>` carrying an associated `Duration` payload.
Under this isolation level, the timestamp selector picks the largest `T` in the closed interval `[anchor - D, largest_not_in_advance_of_upper]`, where `anchor` is selected by an operator-controlled dyncfg:

* `wall_clock` (default): `anchor = NowFn()`. No oracle round-trip on the query path. Correct for single-`environmentd` deployments and for multi-node deployments where the inter-node clock skew is acceptably small (NTP-synced clusters in practice).
* `oracle_read_ts`: `anchor = oracle.read_ts()`. Pays one oracle round-trip per query. Correct under any multi-node topology because it linearizes against the authoritative shared oracle.

If the interval is empty, the query errors with `BoundedStalenessExceeded` (`SQLSTATE 40001`).
The whole feature is gated behind a master feature flag; until an operator enables it, `SET transaction_isolation = 'bounded staleness <D>'` errors at planning time.

### Surface area

In `src/sql/src/session/vars/value.rs`:

```rust
pub enum IsolationLevel {
    ReadUncommitted,
    ReadCommitted,
    RepeatableRead,
    Serializable,
    StrongSessionSerializable,
    StrictSerializable,
    BoundedStaleness(Duration),
}
```

`Duration` is `Copy`, so the enum stays `Copy`.

* `parse`: accept strings of the form `bounded staleness <duration>`, where `<duration>` is a humantime-style spec (`5s`, `500ms`, `1m30s`).
  Reject `D = 0`; reject negative durations; reject `D` larger than 1 hour.
* `as_str` returns the user-facing rendition (`"bounded staleness 5s"`) as `Cow<'static, str>`; round-trips through `SHOW transaction_isolation` and `Value::parse`.
* `as_variant_str` returns the unit-cardinality variant identifier (`"bounded staleness"`, no duration) as `&'static str`.
  This is used for Prometheus labels; using `as_str` there would create one bucket per duration value and blow up label cardinality.
* `valid_values` lists the literal pattern `"bounded staleness <duration>"` for the error string.

The literal strings (`"bounded staleness"`, `"strict serializable"`, ...) live as associated `const`s on `IsolationLevel` so that `as_str`, `as_variant_str`, `valid_values`, and `parse` all reference one source of truth.

`TRANSACTION_ISOLATION` itself does not change shape; it already stores an `IsolationLevel`.

### Feature flags

* **Master flag** (`enable_bounded_staleness_isolation`, feature-flag table in `src/sql/src/session/vars/definitions.rs`).
  Default on.
  `plan_set_variable` in `src/sql/src/plan/statement/scl.rs` parses the value being assigned; if it parses to `BoundedStaleness(_)`, the planner calls `scx.require_feature_flag(&vars::ENABLE_BOUNDED_STALENESS_ISOLATION)`, which errors when the flag is off and otherwise lets the SET proceed.
* **Anchor toggle** (`bounded_staleness_use_oracle_anchor`, dyncfg in `src/adapter-types/src/dyncfgs.rs`).
  Default `false` (wall-clock anchor).
  Set to `true` to switch to oracle anchor.

The anchor toggle is a dyncfg rather than a session variable because the choice is operator-policy, not user-policy: a single deployment uses one anchor across all sessions, and switching mid-flight should propagate process-wide.

### Anchor flag plumbing

`needs_linearized_read_ts` is a free function on the `TimestampProvider` trait with no system-vars handle, so it cannot consult the dyncfg's `ConfigSet` at the call site without threading a parameter through every caller.
Instead the anchor flag is mirrored into a process-wide `AtomicBool` (`BOUNDED_STALENESS_USE_ORACLE_ANCHOR_FLAG` in `src/adapter/src/coord/timestamp_selection.rs`), refreshed by `refresh_bounded_staleness_use_oracle_anchor` from `Coordinator::update_controller_config` in `src/adapter/src/coord/ddl.rs`.

`update_controller_config` already runs whenever a dyncfg-class system var changes (the broader catch-all for adapter dyncfg propagation), so the mirror is updated automatically in the same path that propagates dyncfg changes to the controllers.
Reads in `needs_linearized_read_ts` are a single relaxed atomic load.

### Timestamp selection

In `src/adapter/src/coord/timestamp_selection.rs`:

* `needs_linearized_read_ts` returns `true` for `BoundedStaleness(_)` iff the static mirror reports oracle anchor.
* `oracle_read_ts` therefore fetches the oracle exactly when the operator has selected oracle anchor, and returns `None` otherwise.
* `determine_timestamp_via_constraints`, when the isolation level is `BoundedStaleness(D)`:
  1. Computes the freshness anchor: `anchor = oracle_read_ts.unwrap_or_else(|| Timestamp::from(NowFn()))`.
  2. Pushes a lower-bound constraint `Antichain::from_elem(anchor - D)` with reason `IsolationLevel(BoundedStaleness(D))`.
  3. Pushes a *hard upper-bound constraint* `Antichain::from_elem(largest_not_in_advance_of_upper)` with the same reason.

The hard upper bound is essential.
Without it, a candidate forced above the inputs' upper by the lower bound would be sent to compute and block waiting for the upper to advance, making the failure mode cluster-shape-dependent.
With it, the existing feasibility check in the constraint solver fires `BoundedStalenessExceeded` deterministically in the coordinator before any peek dispatch.

The within-session monotonicity machinery (the prior read timestamp acting as a lower bound) is unchanged and continues to apply to bounded-staleness reads.

### Feasibility check and error path

After candidate selection, if no `T` exists in `[lower, upper]`, emit `AdapterError::BoundedStalenessExceeded { bound, gap_ms, slowest_input }`.
The `Display` impl produces:

```
ERROR: cannot serve query under bounded staleness {D}; freshest available
       timestamp is {gap_ms}ms older than the bound[; slowest input: {id}]
```

`SQLSTATE`: `T_R_SERIALIZATION_FAILURE` (`40001`).
This matches what application retry loops already handle.

`gap_ms` is computed from the constraint solver's `lower_bound().into_option().map_or(0, u64::from) - upper_bound().into_option().map_or(0, u64::from)`.
`slowest_input` is currently `None` --- determining the specific lagging input requires more bookkeeping than the constraint API exposes today; left for follow-up.

### Freshness math

Let `R(t)` denote the oracle's `read_ts` value at instant `t`.
The user-visible contract under `BoundedStaleness(D)` is `R(now) - T <= D`.

In the `EpochMilliseconds` timeline:

* The oracle is monotone: `R(t_2) >= R(t_1)` for `t_2 >= t_1`.
* The oracle is wall-clock-capped on each `environmentd`: `R(t) <= NowFn_local()`.

**Wall-clock anchor.** Picking `T >= NowFn() - D` gives `R(now) - T <= NowFn() - (NowFn() - D) = D` on a single node.
On multiple nodes the oracle reflects the maximum clock across all nodes that have ticked or applied writes, so the bound degrades by the inter-node clock skew: a node whose local clock lags by `ε` can serve `T` such that `R(now) - T <= D + ε`.
NTP-synced clusters keep `ε` in single-digit ms, but it is non-zero.

**Oracle anchor.** Picking `T >= R(now) - D` gives `R(now) - T <= D` exactly, regardless of topology.
The cost is a per-query oracle round-trip (the same one strict serializable pays).

The dependence of the wall-clock variant on the wall-clock-capped-timeline property is recorded as a correctness invariant in `guide-adapter.md`.
If a future timeline does not satisfy it, both variants of bounded staleness must be redesigned for that timeline; the current implementation rejects bounded-staleness queries whose timeline is not `EpochMilliseconds`.

### Interactions

* `AS OF`: when `AS OF` is explicit, the user has chosen `T`; bounded staleness adds no constraint.
* `real_time_recency`: rejected at session-variable validation time when both are set.
* Mixed read/write transactions: `INSERT`, `UPDATE`, `DELETE`, and `COPY FROM` all reject under bounded staleness with a clear error.
* `StrongSessionSerializable`: orthogonal; no interaction.
* Within-session monotonicity: maintained the same way as today.
* Cross-session: no linearizability guarantee. Two sessions can observe data at timestamps separated by up to `2D`.
* Multi-timeline queries: rejected at planning time when the query touches a non-`EpochMilliseconds` timeline.

### File map

* `src/sql/src/session/vars/value.rs` --- enum variant, parser, formatter, `as_str`, `as_variant_str`, `valid_values`.
* `src/sql/src/session/vars/definitions.rs` --- `enable_bounded_staleness_isolation` feature flag.
* `src/sql/src/plan/statement/scl.rs` --- gate the SET on the feature flag.
* `src/adapter-types/src/dyncfgs.rs` --- `bounded_staleness_use_oracle_anchor` dyncfg.
* `src/adapter/src/coord/timestamp_selection.rs` --- `needs_linearized_read_ts`, the bounded-staleness lower- and upper-bound constraints, the static anchor-flag mirror, the `BoundedStalenessExceeded` emit path.
* `src/adapter/src/coord/ddl.rs` --- `update_controller_config` calls `refresh_bounded_staleness_use_oracle_anchor` so the static tracks the dyncfg.
* `src/adapter/src/coord/sequencer/inner.rs` plus `src/adapter/src/coord/sequencer/inner/copy_from.rs` --- write-path rejection.
* `src/adapter/src/error.rs` --- `BoundedStalenessExceeded` variant.
* `doc/developer/guide-adapter.md` --- invariant covering the wall-clock-capped-timeline assumption.
* `misc/python/materialize/mzcompose/__init__.py` --- register the dyncfg with `get_variable_system_parameters` (System Parameters: Random covers both anchor branches).
* `misc/python/materialize/parallel_workload/action.py` --- register the dyncfg with `FlipFlagsAction` (parallel-workload flips it during stress runs).

### Testing

* SLT (`test/sqllogictest/bounded_staleness.slt`): parsing, formatting, round-tripping, rejection of zero/negative/out-of-range/unparseable durations, mutual-exclusion with `real_time_recency`, write rejection. Enables the master feature flag at the top.
* Testdrive (`test/testdrive/bounded-staleness.td`): happy-path SELECT under `bounded staleness 5s`; deterministic error-path scenario (build an MV on a dedicated cluster, drop replication factor to 0, sleep past the bound, verify the error fires). Enables the master feature flag at the top.
* The error path now bails in the coordinator before peek dispatch (because of the hard upper-bound constraint), so the test does not need a separate serving cluster.

## Minimal viable prototype

A working prototype implements:

1. The enum variant, parser, and formatter, with no behavioral effect yet.
2. The constraint logic in `determine_timestamp_via_constraints`, gated behind the new isolation level only, with the wall-clock anchor.
3. The error path, exercised against a synthetic stalled MV.
4. The master feature flag and the anchor-toggle dyncfg.
5. The oracle-anchor branch.

We can validate end to end with a customer-representative dashboard query against a continuously updating source, comparing tail latencies under strict serializable, serializable, `bounded staleness 5s` (wall-clock anchor), and `bounded staleness 5s` (oracle anchor).

## Alternatives

### Wall-clock-only anchor (no toggle)

The original design proposed only the wall-clock anchor.
Single-node correct, fast, simple.
The multi-node concern was raised in review: under horizontal scale-out, the oracle reflects the max clock across nodes, so a slow-clock node serves `T` outside the `D` contract by the inter-node skew.

We addressed this by introducing the anchor toggle rather than picking one variant outright.
The wall-clock anchor remains the default for two reasons: (1) the current production deployment is single-`environmentd`, so the multi-node concern is not yet active; (2) the wall-clock path's no-oracle-round-trip property is a meaningful performance win that is otherwise unavailable.
Operators preparing for multi-node can flip the dyncfg.

### Two session variables instead of a parameterised isolation level

Add a unit variant `BoundedStaleness` plus a separate `max_staleness_ms` session variable.
Smaller parser change, but introduces an inconsistency window and splits one concept across two settings.
Rejected for SQL ergonomics.

### Standalone `max_staleness_ms` overlay on serializable

Keep the existing isolation levels and add a separate session variable that overlays a staleness ceiling.
This conflates a contract change with a knob.
Rejected for clarity.

### Wait-with-timeout instead of error

Pick `T` at the staleness floor and block on input frontiers up to `D`, then either return or error.
This silently degrades to strict-serializable-with-cap behavior and breaks the freshness contract during the wait.
Error-on-infeasible is the primary semantic; wait-with-timeout is a future, opt-in extension behind a separate session variable.

### Caching `read_ts` everywhere (the rejected `peek_read_ts_fast` path)

Use a cached oracle value for *all* isolation levels, not just bounded staleness.
This was attempted and rejected for strict serializable in `guide-adapter.md`; the strict-serializable contract requires the timestamp to be determined during the query's real-time interval.
Bounded staleness as designed here does not require it for the wall-clock anchor; for the oracle anchor we make a fresh oracle call per query (caching is a possible future optimization).

### Oracle-anchor caching today

The oracle-anchor branch could cache `read_ts` and refresh on a schedule, rather than calling the oracle per query.
Done correctly --- with `T >= max(cached_anchor, NowFn() - cache_age) - D` to absorb the staleness of the cache itself --- this collapses to the wall-clock floor whenever the oracle lags wall clock.
We considered this during the original design and rejected it because the wall-clock floor was simpler and at least as fresh.
For a multi-node deployment that uses oracle anchor, caching is a worthwhile future optimization, but the contract math has to be redone for each cache implementation.

## Open questions

* What is the right default upper bound on `D`?
  Currently 1 hour.
  Suggest revisiting after early customer feedback.
* `slowest_input` in the `BoundedStalenessExceeded` error is currently `None`.
  Plumbing the per-input upper antichain into the constraint solver would give us a more actionable message.
* Should bounded staleness be permitted inside a multi-statement read transaction?
  The within-session monotonicity argument suggests yes, but the design has not been stress-tested for that case.
* Should we add a `reason` label to the existing `determine_timestamp` metric for `bounded staleness` so we can compare the timestamp gap against the strict-serializable counterfactual, similar to the existing `timestamp_difference_for_strict_serializable_ms`?
  A `timestamp_difference_for_bounded_staleness_ms` metric is already wired in.
* Future caching for the oracle-anchor branch: when (if ever) does the per-query oracle round-trip become a bottleneck worth solving?

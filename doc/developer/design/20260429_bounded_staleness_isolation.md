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

1. A user can configure a session to read at a freshness ceiling of `D` (e.g. five seconds) and have queries served at any timestamp `T` satisfying `oracle.read_ts(now) - T <= D` without waiting on input frontiers.
2. When the freshness ceiling cannot be met --- because some input collection lags by more than `D` --- the query returns a clear, actionable error that fires in the coordinator before any peek is dispatched, so the failure mode is independent of the cluster the query would have run on.
3. The new isolation level is opt-in at two levels: the operator must enable a master feature flag, and individual sessions then opt in by `SET transaction_isolation`.
4. The design preserves all existing strict-serializable correctness invariants documented in `guide-adapter.md`.

## Out of scope

* Bounded staleness for read-then-write transactions.
  Writes still require a linearized oracle write timestamp, and mixing bounded staleness with writes raises questions about commit ordering that this design does not address.
  An attempt to issue a write inside a bounded-staleness transaction errors.
* A wait-with-timeout variant of the failure mode.
  We expect to add a sibling behavior later, gated on a separate session variable, that waits up to a deadline before erroring.
  This document specifies error-only behavior.
* Bounded staleness on timelines other than `EpochMilliseconds`.
  The freshness math is currently scoped to a single timeline.
  Other timelines are out of scope until that property is re-derived for them.
* Cross-session linearizability.
  Two sessions running under bounded staleness may observe each other's writes at different timestamps within the bound.
  This is the explicit trade-off and matches the existing serializable contract.
* Interaction with `real_time_recency`.
  The two settings are contradictory; combining them errors at session-variable validation time.

## Solution proposal

### High level

Introduce an isolation level `bounded staleness <duration>` carrying an associated `Duration` payload.
Under this isolation level, the timestamp selector picks the largest `T` in the closed interval `[oracle.read_ts(now) - D, largest_not_in_advance_of_upper]`.
Each query makes one oracle round-trip to obtain the anchor (the same one strict serializable already pays); the oracle is the only anchor that stays correct across crashes, restarts, clock changes, and a future multi-`environmentd` deployment.

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
  Reject `D = 0` and negative durations; large bounds are accepted (they just behave like serializable when input frontiers stay within `D`, which is fine).
* `as_str` returns the user-facing rendition (`"bounded staleness 5s"`) as `Cow<'static, str>`; round-trips through `SHOW transaction_isolation` and `Value::parse`.
* `as_variant_str` returns the unit-cardinality variant identifier (`"bounded staleness"`, no duration) as `&'static str`.
  This is used for Prometheus labels; using `as_str` there would create one bucket per duration value and blow up label cardinality.
* `valid_values` lists the literal pattern `"bounded staleness <duration>"` for the error string.

The literal strings (`"bounded staleness"`, `"strict serializable"`, ...) live as associated `const`s on `IsolationLevel` so that `as_str`, `as_variant_str`, `valid_values`, and `parse` all reference one source of truth.

`TRANSACTION_ISOLATION` itself does not change shape; it already stores an `IsolationLevel`.

### Feature flag

`enable_bounded_staleness_isolation` (feature-flag table in `src/sql/src/session/vars/definitions.rs`), default off.
`plan_set_variable` in `src/sql/src/plan/statement/scl.rs` parses the value being assigned; if it parses to `BoundedStaleness(_)`, the planner calls `scx.require_feature_flag(&vars::ENABLE_BOUNDED_STALENESS_ISOLATION)`, which errors when the flag is off and otherwise lets the SET proceed.

### Timestamp selection

In `src/adapter/src/coord/timestamp_selection.rs`:

* `needs_linearized_read_ts` returns `true` for `BoundedStaleness(_)`, so `oracle_read_ts` is fetched on the query path (one oracle round-trip per query, same as strict serializable).
* `determine_timestamp_via_constraints`, when the isolation level is `BoundedStaleness(D)`:
  1. Reads `anchor = oracle_read_ts` (always populated by construction; an `expect` documents the invariant).
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

The oracle is monotone (`R(t_2) >= R(t_1)` for `t_2 >= t_1`) and persisted, so it survives crashes, restarts, and arbitrary clock changes.
We fetch `R(now)` once at query start and pick `T >= R(now) - D`, which gives `R(now) - T <= D` exactly.
This is correct on a single `environmentd` and remains correct when the system grows to multiple `environmentd`s sharing one authoritative oracle: every node sees the same `R`.

We considered a wall-clock anchor (`T >= NowFn() - D`, no oracle round-trip) during initial design.
It is unsafe under crashes/restarts (a fresh `NowFn()` after a restart can regress past previously-served timestamps if the wall clock has been adjusted), and degrades in a multi-`environmentd` deployment by the inter-node clock skew.
We rejected it; see [Alternatives](#alternatives).

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
* `src/adapter/src/coord/timestamp_selection.rs` --- `needs_linearized_read_ts`, the bounded-staleness lower- and upper-bound constraints, the `BoundedStalenessExceeded` emit path.
* `src/adapter/src/coord/sequencer/inner.rs` plus `src/adapter/src/coord/sequencer/inner/copy_from.rs` --- write-path rejection.
* `src/adapter/src/error.rs` --- `BoundedStalenessExceeded` variant.
* `doc/developer/guide-adapter.md` --- invariant on bounded staleness's interaction with the timestamp oracle.

### Testing

* SLT (`test/sqllogictest/bounded_staleness.slt`): parsing, formatting, round-tripping, rejection of zero/negative/out-of-range/unparseable durations, mutual-exclusion with `real_time_recency`, write rejection. Enables the master feature flag at the top.
* Testdrive (`test/testdrive/bounded-staleness.td`): happy-path SELECT under `bounded staleness 5s`; deterministic error-path scenario (build an MV on a dedicated cluster, drop replication factor to 0, sleep past the bound, verify the error fires). Enables the master feature flag at the top.
* The error path now bails in the coordinator before peek dispatch (because of the hard upper-bound constraint), so the test does not need a separate serving cluster.

## Minimal viable prototype

A working prototype implements:

1. The enum variant, parser, and formatter, with no behavioral effect yet.
2. The constraint logic in `determine_timestamp_via_constraints`, gated behind the new isolation level only.
3. The error path, exercised against a synthetic stalled MV.
4. The master feature flag.

We can validate end to end with a customer-representative dashboard query against a continuously updating source, comparing tail latencies under strict serializable, serializable, and `bounded staleness 5s`.

## Alternatives

### Wall-clock anchor

An earlier draft of this design picked `T >= NowFn() - D` to skip the per-query oracle round-trip.

This is unsafe.
On restart, `NowFn()` may regress (NTP step backward, container migration, etc.); a query running before the regress can have served `T` past the post-regress floor, breaking monotonicity.
In a future multi-`environmentd` deployment the picture is worse: the shared oracle reflects the max clock across all nodes, so a slow-clock node serves `T` outside the `D` contract by the inter-node skew.

The oracle is monotone, persisted, and shared; anchoring against it sidesteps both problems.
The cost --- one oracle round-trip per query --- is the same one strict serializable already pays, and is small relative to the latency budget that motivated bounded staleness in the first place.
A future optimization could cache `read_ts` and refresh it asynchronously, which is sound under bounded staleness's relaxed contract; we leave that for later.

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

### Caching `read_ts` for all isolation levels (the rejected `peek_read_ts_fast` path)

Use a cached oracle value for *every* isolation level, not just bounded staleness.
This was attempted and rejected for strict serializable in `guide-adapter.md`; the strict-serializable contract requires the timestamp to be determined during the query's real-time interval.
Bounded staleness's relaxed contract makes oracle-read caching legitimate for *its* path; we defer that work but flag it as the natural follow-up if the per-query oracle round-trip becomes a bottleneck.

## Open questions

* `slowest_input` in the `BoundedStalenessExceeded` error is currently `None`.
  Plumbing the per-input upper antichain into the constraint solver would give us a more actionable message.
* Should bounded staleness be permitted inside a multi-statement read transaction?
  The within-session monotonicity argument suggests yes, but the design has not been stress-tested for that case.
* When (if ever) does the per-query oracle round-trip become a bottleneck worth caching?
  A `timestamp_difference_for_bounded_staleness_ms` metric is already wired in to help spot the regime where caching would pay off.

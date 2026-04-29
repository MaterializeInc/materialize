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

1. A user can configure a session to read at a freshness ceiling of `D` (e.g. five seconds) and have queries served at any timestamp `T` satisfying `now_oracle_read_ts - T <= D`, without waiting on input frontiers.
2. When the freshness ceiling cannot be met --- because some input collection lags by more than `D` --- the query returns a clear, actionable error rather than blocking.
3. Steady-state queries under bounded staleness do not pay the per-query timestamp-oracle round-trip that strict serializable pays.
4. The new isolation level is opt-in and has no behavioral effect on sessions that do not select it.
5. The design preserves all existing strict-serializable correctness invariants documented in `guide-adapter.md`.

## Out of scope

* Bounded staleness for read-then-write transactions.
  Writes still require a linearized oracle write timestamp, and mixing bounded staleness with writes raises questions about commit ordering that this design does not address.
  An attempt to issue a write inside a bounded-staleness transaction errors.
* A wait-with-timeout variant of the failure mode.
  We expect to add a sibling behavior later, gated on a separate session variable, that waits up to a deadline before erroring.
  This document specifies error-only behavior.
* Bounded staleness on timelines other than `EpochMillis`.
  The freshness math relies on timeline timestamps being capped by wall-clock time.
  Other timelines are out of scope until that property is re-derived for them.
* Cross-session linearizability.
  Two sessions running under bounded staleness may observe each other's writes at different timestamps within the bound.
  This is the explicit trade-off and matches the existing serializable contract.
* Interaction with `real_time_recency`.
  The two settings are contradictory; combining them errors at session-variable validation time.

## Solution proposal

### High level

Introduce an isolation level `bounded staleness <duration>` carrying an associated `Duration` payload.
Under this isolation level, the timestamp selector picks any `T` in the closed interval `[now_ms - D, min(upper_i) - 1]`, where `now_ms` is the current wall-clock epoch in milliseconds.
If the interval is empty, the query errors.
The picker chooses the largest `T` in the interval, i.e. the freshest data available without waiting.

The query path makes no call into the timestamp oracle.
The oracle's `read_ts` is not consulted, cached, or anchored against.
The freshness contract is satisfied by the wall-clock floor alone, because in the `EpochMillis` timeline the oracle's `read_ts` is bounded above by `now_ms`; see the freshness math below.

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
  Reject `D = 0`: under that bound the contract collapses to strict serializable, but without the oracle-linearized timestamp that strict serializable requires; users wanting that should set `strict serializable`.
  Reject negative durations.
  Reject `D` larger than a configurable maximum (default 1 hour) so that misconfiguration does not silently degrade to unbounded serializable.
* `as_str` / `format`: round-trip as `bounded staleness 5s`.
* `valid_values`: include the literal pattern `bounded staleness <duration>` in the error string, since the value is parameterised.

`TRANSACTION_ISOLATION` itself does not change shape.
It already stores an `IsolationLevel`.

### Timestamp selection

In `src/adapter/src/coord/timestamp_selection.rs`:

* `needs_linearized_read_ts` returns `false` for `BoundedStaleness`.
* `oracle_read_ts` returns `None` for `BoundedStaleness`.
* `determine_timestamp_for` adds, when the isolation level is `BoundedStaleness(D)`:
  ```
  // `coord.catalog().config().now` is the existing NowFn already
  // threaded through the coordinator.
  let now_ms = mz_repr::Timestamp::from((coord.catalog().config().now)());
  let lower = now_ms.saturating_sub(D.as_millis() as u64);
  constraints.lower.push((Antichain::from_elem(lower),
                          Reason::IsolationLevel(BoundedStaleness(D))));
  ```
  No oracle constraint is added, in contrast to `StrictSerializable`.
  `NowFn` is the same source the timestamp oracle ticks against, so wall-clock skew between the lower-bound check and the oracle's notion of "now" is zero by construction.

The existing upper-bound logic --- combining `since` and `upper` of the input collections to find the latest no-wait timestamp --- runs unchanged.
The existing within-session monotonicity machinery (the prior read timestamp acting as a lower bound) is also unchanged and continues to apply.

### Feasibility check and error path

After candidate selection, if no `T` exists in `[lower, upper_max]` --- i.e. the input frontier is below the staleness floor --- emit:

```
ERROR: cannot serve query under bounded staleness {D};
       freshest available timestamp is {min_upper - 1},
       which is {Δ}ms older than the {D}ms staleness bound;
       slowest input: {coll_id}
```

SQLSTATE: `40001` (`serialization_failure`).
This matches what application retry loops already handle, and the semantics fit: "transient failure under the chosen isolation level; retry may succeed."

### Freshness math

Let `R(t)` denote the oracle's `read_ts` value at instant `t`.
The user-visible contract under `BoundedStaleness(D)` is `R(now) - T <= D`.

In the `EpochMillis` timeline:

* The oracle is monotone: `R(t_2) >= R(t_1)` for `t_2 >= t_1`.
* The oracle is wall-clock-capped: `R(t) <= wall_clock_ms_at(t)`.

Therefore at any instant `now`, `R(now) <= now_ms`.
Picking `T >= now_ms - D` gives `R(now) - T <= now_ms - (now_ms - D) = D`.
The contract holds.

When the oracle lags wall clock --- for example immediately after a tick when the oracle has not yet caught up --- the chosen `T` may be older than `R(now) - D`, i.e. we serve fresher data than the contract strictly requires.
This is benign: it always satisfies the contract and is strictly more user-friendly.

A new invariant must be added to `guide-adapter.md` recording the dependence on the wall-clock-capped timeline property.
If a future timeline does not satisfy it, bounded staleness must be redesigned for that timeline, and the parser should reject bounded-staleness sessions whose queries cross into such a timeline.

### Interactions

* `AS OF`: when `AS OF` is explicit, the user has chosen `T`; bounded staleness adds no constraint.
  This matches existing behavior.
* `real_time_recency`: rejected at session-variable validation time when both are set.
* Mixed read/write transactions: a `BEGIN` under bounded staleness errors on the first DML statement.
* `StrongSessionSerializable`: orthogonal; no interaction.
* Within-session monotonicity: maintained the same way as today, by lower-bounding the timestamp by the maximum prior read timestamp in the session.
  Bounded staleness inherits this from the existing constraint logic.
* Cross-session: no linearizability guarantee.
  Two sessions can observe data at timestamps separated by up to `2D`.
* Multi-timeline queries: if the query touches a non-`EpochMillis` timeline, bounded staleness is rejected at planning time.

### File map

* `src/sql/src/session/vars/value.rs`: enum variant, parser, formatter, validation.
* `src/sql/src/session/vars/definitions.rs`: optional system var for the maximum permitted `D`.
* `src/adapter/src/coord/timestamp_selection.rs`: `needs_linearized_read_ts`, `oracle_read_ts`, `determine_timestamp_for` extensions; new error variant.
* `src/adapter/src/error.rs`: new `AdapterError` variant for the bounded-staleness failure case.
* `doc/developer/guide-adapter.md`: new invariant covering the wall-clock-capped-timeline assumption.

### Testing

* SLT coverage of parsing, formatting, and round-tripping the new isolation level value.
* SLT coverage of the legal-but-no-data-yet case (small `D`, freshly created collection): expect the staleness error.
* Testdrive coverage of the steady-state path: load data, set bounded staleness with `D = 5s`, verify queries return without contacting the oracle (assert via metrics) and within latency bounds well under one tick.
* Testdrive coverage of the slow-input case: artificially stall an input frontier and verify the staleness error fires.
* Adapter-level unit tests over `determine_timestamp_for` for the new constraint.
* Property-style coverage: random `D`, random input frontiers, assert that returned `T` always satisfies the contract whenever a `T` is returned.

## Minimal viable prototype

A working prototype implements:

1. The enum variant, parser, and formatter, with no behavioral effect yet.
   This validates the SQL surface against psql, drivers, and round-tripping through `SHOW TRANSACTION_ISOLATION`.
2. The constraint logic in `determine_timestamp_for`, gated behind the new isolation level only.
   With this in place a session can run `SET TRANSACTION_ISOLATION TO 'bounded staleness 5s'` and observe the desired latency profile.
3. The error path, exercised against a synthetic stalled source.

We can validate end to end with a customer-representative dashboard query against a continuously updating source, comparing tail latencies under strict serializable, serializable, and `bounded staleness 5s`.

## Alternatives

### Oracle-anchored floor with a cached anchor

The first version of this design used `T >= cached_oracle_anchor - (D - cache_age)`, where the anchor was a recently observed value of `oracle.read_ts()` and `cache_age` was the wall-clock age of that observation.
The motivation was to bound `T` directly against the oracle's notion of "freshest known timestamp" rather than against wall clock.

This approach is unsound as written.
The math `R(now) - T <= D` holds only if `R(now) <= A + cache_age`, which assumes the oracle was caught up to wall clock at observation time.
The oracle in practice can lag wall clock by up to one tick (default 1s).
When the cache is refreshed at instant `t_c` while the oracle lags wall clock by `Δ`, the chosen `T` can violate the contract by `Δ`.

A correct oracle-anchored variant would have to either keep the cache so fresh that `Δ` is negligible (defeating the perf motivation, since refresh frequency must approach the oracle tick rate or finer), or tighten the lower bound to `T >= max(A, now_ms - cache_age) - (D - cache_age)`, which simplifies to `T >= now_ms - D` --- the wall-clock floor.

The wall-clock floor is therefore both simpler and at least as fresh.
It is preferred.

### Two session variables instead of a parameterised isolation level

Add a unit variant `BoundedStaleness` plus a separate `max_staleness_ms` session variable.
Smaller parser change, but introduces an inconsistency window where one variable is set and the other is not, requires a default-or-error policy, and splits one concept across two settings.
Rejected for SQL ergonomics.

### Standalone `max_staleness_ms` overlay on serializable

Keep the existing isolation levels and add a separate session variable that, when set, overlays a staleness ceiling.
This conflates a contract change with a knob and does not give the user a single name for the consistency model in use.
Rejected for clarity.

### Wait-with-timeout instead of error

Pick `T` at the staleness floor `now_ms - D` and block on input frontiers up to `D`, then either return or error.
This silently degrades to strict-serializable-with-cap behavior and breaks the freshness contract during the wait.
We choose error-on-infeasible as the primary semantic and leave wait-with-timeout as a future, opt-in extension behind a separate session variable.

### Caching `read_ts` everywhere (the rejected `peek_read_ts_fast` path)

Use a cached oracle value for *all* isolation levels, not just bounded staleness.
This was attempted and rejected for strict serializable in `guide-adapter.md`; the strict-serializable contract requires the timestamp to be determined during the query's real-time interval.
Bounded staleness as designed here does not need the cache at all, so this alternative is moot.

## Open questions

* What is the right default upper bound on `D`?
  One hour, ten minutes, or unbounded?
  Suggest one hour, on the grounds that very large bounds make the feature behaviorally indistinguishable from serializable and probably indicate a configuration error.
* SQLSTATE choice for the staleness-violation error: `40001` (proposed) or a new code?
  `40001` integrates with existing retry loops; a new code is more precise but invisible to clients.
* Should bounded staleness be permitted inside a multi-statement read transaction?
  The within-session monotonicity argument suggests yes, but the design has not been stress-tested for that case.
* Should we add a `reason` label to the existing `determine_timestamp` metric for `bounded staleness` so we can compare the timestamp gap against the strict-serializable counterfactual, similar to the existing `timestamp_difference_for_strict_serializable_ms`?
* Wall-clock skew: the freshness math assumes the local wall clock is reasonably close to true time.
  Do we need explicit handling for clocks that go backwards (NTP step), or is the existing within-session monotonicity floor enough?

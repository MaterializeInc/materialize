# Temporal bucketing inside the merge batcher

- Associated:
  [materialize#37589](https://github.com/MaterializeInc/materialize/pull/37589) (implementation),
  [materialize#36427](https://github.com/MaterializeInc/materialize/pull/36427) (earlier draft),
  [materialize#36644](https://github.com/MaterializeInc/materialize/pull/36644) (operator coverage in the lowering),
  [20210426_temporal_filters](./20210426_temporal_filters.md)

## Summary

Move temporal bucketing from a standalone dataflow operator into the merge
batcher that every arrangement already contains. Each batcher decides at
every seal (the frontier advance at which the batcher emits newly ready
updates), based on the data it actually holds, whether to park far-future
updates in a bucket chain. This replaces the lowering's static analysis
of where bucketing *may* be needed with a free runtime check of where it
*is* needed. It applies uniformly to every arrangement that dataflow
rendering builds on a totally ordered timestamp, and it lets us later
delete the standalone operator and its plan-level plumbing. A feature
flag switches between the old and new mechanism during rollout.

## The Problem

Temporal filters produce updates whose timestamps lie days or weeks in the
future (a row's scheduled retraction at `ts + window`). The plain merge
batcher re-merges and re-extracts everything it holds at every seal, so an
update sitting at distance `d` from the frontier is re-walked `d / tick`
times before it matures. The whole future tail is thus re-walked at
every tick for the entire retention window, which makes arrangement
maintenance the dominant cost of temporal workloads.

The current mitigation is the `temporal_bucket` operator: a separate
operator that holds far-future updates in a `BucketChain` (buckets of
geometrically growing time spans, amortized `O(log distance)` per record)
and releases them shortly before they mature. Because it is a separate
operator, the MIR-to-LIR lowering must decide where to insert it, by
propagating a static may-analysis ("may this collection carry future
updates?") through the plan.

The main cost is complexity, smeared through the lowering. The analysis
must find every merge batcher (its invariant is "bucketing sits
immediately upstream of every batcher that may see future updates"), it
needs absorption conventions so bucket operators do not stack, and every
new rendering feature must reason about the invariant again.
materialize#36644 fixed missed sites at Reduce, TopK, and consolidating
Unions. The plumbing (`has_future_updates` propagation,
`ArrangementStrategy`, EXPLAIN annotations) exists at the plan level to
serve what is a runtime property of the data.

The static placement also has performance costs in both directions:

- The operator cannot run where the analysis cannot see future updates.
  Updates run ahead of a batcher's frontier without any temporal filter:
  multi-input operators and worker skew deliver data early, all the more
  when a dataflow lags. Those batchers pay the plain re-merging cost.
- Where the operator does run, it duplicates machinery the arrangement
  already has: it exchanges the data (a second shuffle on top of the
  arrangement's own exchange) and holds it in its own bucket chains, and
  matured updates then enter the arrangement's batcher to be merged again.
  Relatedly, the operator's hydration cost depends on whether it is
  scheduled before or after the snapshot arrives, while a batcher, sealed
  only when the frontier moves, is deterministic.

## The Design

### The observation is free

The plain batcher's seal already partitions every held update against the
seal's `upper` (that is what its extract does, and where its `kept` updates
come from). Deciding "does this batcher hold far-future data right now?"
therefore costs one extra comparison inside a loop that already compares
every record against the upper. The batcher can make the bucketing decision
empirically, per seal, per arrangement instance, at no extra cost.

Three consequences, one per cost above:

1. The complexity is localized. Everything about bucketing lives inside the
   merge batcher instead of being smeared across the lowering: there is no
   site to find and nothing to absorb (each batcher holds only its own
   data, which it must hold anyway), and the plan-level plumbing can be
   deleted.
2. Coverage is decided by the data, not the plan. Future updates are parked
   wherever they actually show up, including the skew-delivered kind no
   static analysis can see, and a batcher whose future tail drains reverts
   to plain behavior automatically.
3. The duplicated machinery disappears: no second exchange, no second set
   of chains, and the hydration cost no longer depends on when an operator
   happens to be scheduled.

### The batcher

`TemporalBucketingMergeBatcher` (`mz_timely_util::merge_batcher`) is a
drop-in implementation of differential's `Batcher` trait. It is generic
over a `TemporalMerger`, the chain-merging engine: differential's `Merger`
role plus a three-way, bounds-tracking extract. In sketch:

```text
trait TemporalMerger {
    type Time;
    type Chunk;   // chain entries, possibly offloaded
    type Output;  // the container pushed into and emitted by the batcher
    fn absorb(Output) -> Chunk;
    fn materialize(Chunk) -> Output;
    fn merge(chain, chain) -> chain;  // consolidating
    fn extract_time_partitioned(chain, split_lo, split_hi, track_before)
        -> { before, within, beyond: chains with attained time bounds };
}
```

Two implementations exist,
one over the paged column chains of the `ColumnMergeBatcher` family and
one wrapping our columnation-based `ColInternalMerger`. Both engines'
record-walking loops live in this repository rather than in the
differential-dataflow dependency, so the three-way extract needs no fork
or vendoring, and the temporal logic itself exists exactly once.

The paged engine is the primary target, but the columnation adapter is
what carries the rollout: it is the default engine everywhere and the only
one with builders for every arrangement family (see "Rollout and
alternatives").

The batcher's state is the plain batcher's flat chains plus a
`BucketChain` of `MergeBucket`s for far-future data. The `BucketChain` is
the same shared module the operator uses (`mz_timely_util::temporal`).

At every seal, one extract walk partitions the merged flat chains three
ways against the seal's `upper` and `upper + θ`, where θ is a configurable
near/far threshold (next section):

- ready (`t < upper`): emitted, exactly like the plain batcher,
- near (`upper <= t < upper + θ`): returned to the flat chains, exactly
  like the plain batcher's `kept` handling,
- far (`upper + θ <= t`): routed into the bucket chain.

The seal then peels matured buckets (entirely below `upper`), merges them
into the emitted output, and lets the chain restore its shape under a fuel
budget. When the batcher holds no far-future data, the bucket chain is
never touched and the seal is the plain batcher's code plus one branch.
Parity with plain in that case is a benchmarked requirement, not a
best-effort goal.

### The near/far threshold θ

θ (dyncfg `compute_temporal_bucketing_summary`, default 2s, shared with the
operator) exists because data slightly ahead of the frontier is normal and
cheap for the plain batcher: in-flight updates race a tick or two ahead,
and multi-input dataflows see worker and input skew. A record at distance
`d` costs about `d / tick` flat re-merges, so `θ / tick` is the maximum
number of rescans we accept before bucketing takes over. Sharing the
config with the operator also shares the behavior: the operator releases
within-summary data immediately, so under either mechanism such data ends
up in the arrangement's plain flat chains.

The knob is forgiving, since both misconfiguration directions cost a small
constant per record. It is read once per batcher from a process-global
installed by `apply_worker_config`, so a config change takes effect on
newly built batchers without a restart.

### Bucket bounds

Every bucket carries the `(min, max)` times of its contents, established
during the extract walks that move the data anyway. Bounds exist if and
only if the bucket holds data. There is deliberately no unknown state with
a scan fallback: a fallback would turn a bounds-maintenance bug into
silent performance degradation, while without one such bugs surface as
invariant violations, caught by the test-only validator, the protocol
proptest, and the frontier guard below.

Importantly, splits whose midpoint falls outside a bucket's bounds move
the bucket wholesale without touching records. This is the load-bearing
performance detail: it is what keeps large frontier jumps cheap, most
importantly a batcher's first seal after hydration (see "The first seal"
under Performance).

Consolidation inside a bucket can cancel the records that attained a
bound, so bounds are conservative (possibly wider than the data) but
always within the bucket's time range, and a bucket whose contents cancel
entirely drops its content outright.

### Frontier semantics and the arrange protocol

Differential's arrange operator downgrades its capabilities to
`batcher.frontier()` after each seal and uses the capability's time as the
hint from which downstream trace imports mint their capabilities. The
contract this imposes: the reported frontier must never fall below a
sealed upper, else the arrange holds a capability below the trace upper
and the import panics.

The batcher meets the contract by construction. It reports the minimum of
the near data's lower bound (at least `upper` by classification) and the
lowest non-empty bucket's bound (at least the bucket's range start, which
peeling keeps at or above `upper`).

A proptest simulates this protocol end to end, with updates that can
cancel, and a cheap runtime guard soft-panics with full bucket state if
the invariant is ever violated.

### Wiring: uniform coverage

When `enable_compute_temporal_bucketing_batcher` is on, every merge
batcher in dataflow rendering that runs on a totally ordered timestamp is
a temporal-bucketing batcher, and
the `temporal_bucket` operator is suppressed at all of its insertion sites,
ignoring the lowering's `ArrangementStrategy`. There is deliberately no
reasoning about which sites need bucketing. The fast path makes unnecessary
coverage free, so uniform coverage replaces the per-site analysis, which
was the point of the exercise.

Batcher selection dispatches per render timestamp through a
`MaybeTemporalArrange` trait (mirroring `MaybeBucketByTime`):
`mz_repr::Timestamp` honors the flag, the iterative-scope
`Product<Timestamp, PointStamp>` ignores it, since partially ordered
timestamps cannot be bucketed by a totally ordered chain. Covered sites are
the ArrangeBy ok and err arrangements, all Reduce input arrangements and
the monotonic consolidation, all TopK arrangements and consolidations,
Threshold, the linear join stages, consolidating Unions, LetRec
consolidations, and index exports. Delta joins build no batchers of their
own: both their lookup side and their update streams read the input
arrangements, so covering
those covers delta joins transitively, including the half-join operators'
internal buffers, which only ever see updates the input arrangements have
released. Introspection (logging) dataflows keep plain batchers: they are
built outside the rendering path and their updates carry no future
timestamps.

## Performance

There are three data regimes, and each is compared against what would
actually run there today. Parity or better across all of them is what
makes running the batcher everywhere safe.

1. No future data (almost every batcher): the baseline is the plain
   batcher, the incumbent at these sites. The requirement is parity, and
   criterion benchmarks measure a temporal-to-plain runtime ratio of
   0.98-0.99, at or marginally better than parity.
2. Data within θ of the frontier: the threshold split makes the batcher
   behave identically to plain, which is also how the operator treats such
   data. The operator's summary and the batcher's θ are the same config,
   `compute_temporal_bucketing_summary`, so parity holds against either
   comparator by construction.
3. Genuine far-future data, split by what produces it:
   - Temporal-filter workloads: the baseline is the operator, today's
     mechanism for exactly these. Measured: parity (the temporal-filter
     feature benchmarks), with the operator's duplicated machinery (the
     second exchange, the second set of chains, the scheduling-dependent
     hydration cost) removed structurally.
   - Future data the planner cannot see (worker skew, multi-input
     operators, lagging dataflows): the baseline is the plain batcher,
     the only thing that runs there. Criterion benchmarks measure
     steady-state gains of more than an order of magnitude. A dataflow
     lagging by L can accumulate skew-delivered data up to L ahead of its
     frontier, and the plain batcher re-walks all of it at every seal.
     The batcher parks the beyond-θ part at a constant number of touches
     independent of L.

### The first seal

A batcher's first seal after hydration absorbs the entire future tail,
call it F records, at once, and the bucket chain then bisects from the
whole time domain down to the frontier, so it is natural to worry that
this one seal touches each record many times. We have both theoretical
arguments that the cost stays a small multiple of F and simulations of
the bucket chain's mechanics that confirm them across data distributions.
Three things bound the seal:

- `suppress_early_progress` holds back progress on source imports until
  the input passes the as_of, so arranges see no intermediate frontiers.
  The snapshot ships in the same single seal, without a detour through the
  bucket chain, and only the genuine future tail enters the chain.
- The attained bucket bounds let every bisection step whose midpoint
  misses the data move the bucket wholesale, without touching records.
  Only splits that genuinely cut through data walk it. In simulation this
  is the difference between roughly 3 F and roughly 34 F record-touches.
- Within the seal, only the peel share (about 1.1 F) runs synchronously.
  The restore share is fuel-capped and trickles out over subsequent seals.

For an interval-shaped tail the bisection is a halving cascade: each
genuine cut splits off a part that the seal does not touch again, so the
touches form a geometric series summing to about 2 F. Simulations across
data shapes (uniform tails over various window sizes, growing and
shrinking tables, batch-load spikes, random frontier alignments) confirm
2-4 F record-touches in total, recouped within seconds against the plain
batcher's F per tick.

The adversarial ceiling is
`O(F log F)`, reachable only by distributions that place equal mass at
every distance scale from the frontier. The peel runs unfueled, so the
ceiling also bounds the synchronous share of a single seal, but the same
reasoning applies there too: the threshold split excises the realistic
routes to it (in the worst simulated case, nearly all of
that mass sits within θ of the frontier and never enters the chain). The
ceiling is also not a new asymptotic class: it is the bucket chain's
designed amortized `O(log distance)` per-record lifetime cost,
front-loaded into one seal.

## Observability

The batcher counts records touched by bucket-chain operations: a cumulative
counter plus a per-seal high-water mark recorded with that seal's held
count (never an average, which would dilute the one expensive hydration
seal across thousands of cheap ones). A `tracing::debug!` line reports any
seal that performed bucket-chain work. Bucket contents flow into
`BatcherEvent` logging so arrangement-size introspection does not
under-report exactly the workloads that hold weeks of retractions.

## Correctness and testing

- Unit tests plus a model-based proptest: emitted batches must equal the
  not-yet-emitted updates below each upper, consolidated, and the reported
  frontier must match a reference.
- A protocol proptest simulating the arrange operator's capability handling
  against a downstream trace import, with cancelling updates. This caught a
  real bug (a fully cancelled bucket retaining stale bounds, reported as a
  frontier below the seal upper, panicking trace imports). The
  drop-content-outright rule under "Bucket bounds" closes exactly this
  case.
- An expensive `validate_bounds_invariant()` walk (bounds contain exactly
  the bucket's data, counts consistent, no empty content) called from tests
  only, never on production paths.
- Criterion benchmarks for parity (regimes 1 and 2) and wins (regime 3).
- End-to-end sqllogictests, and CI runs entirely in batcher mode via the
  minimal system parameters. The flag-off production configurations (the
  standalone operator, and plain batchers) keep deterministic coverage
  through a dedicated sqllogictest that is deleted with the flags.
  Parameter-randomized CI runs additionally flip the flag.

## Rollout and alternatives

`enable_compute_temporal_bucketing_batcher` (default off) selects the new
mechanism at dataflow build time and takes precedence over the operator
flag. Off means bit-for-bit today's behavior. The #36644 plumbing stays
untouched during the transition and is deleted in a later cleanup PR
(operator, `ArrangementStrategy`, `has_future_updates` propagation, EXPLAIN
annotations). EXPLAIN loses its bucketing annotations. The replacement is
runtime introspection of where bucketing actually engaged, which is more
truthful than where the planner guessed.

Of the two engines, the paged one is the primary target: it composes with
`enable_column_paged_batcher` (with both flags on, the affected
arrangements run temporal bucketing over paged chains), and the far-future
tail is the ideal spill candidate, since it is touched again only when
buckets split or mature. The columnation adapter is nonetheless what
carries the rollout: the columnation engine is the default everywhere and
CI runs it nearly exclusively, and paged builders exist only for the
row-row and val-row arrangement families, so uniform coverage of every
batcher site is only expressible on the columnation engine today. The
adapter is a small delegation wrapper plus that engine's extract walk, and
it is deleted together with the engine.

Alternatives considered:

- Keep the operator and extend the lowering's coverage. Rejected: the
  coverage invariant is exactly the recurring cost we want to remove, and
  the operator cannot adapt to the data at runtime.
- Per-site selection of which batchers get the temporal variant. Rejected:
  it recreates the same shielding analysis inside rendering that we are
  removing from the lowering.
- Dataflow-granularity fallback (one scan of the finalized dataflow for
  temporal predicates, one bool into rendering) remains available if
  always-on turns out too aggressive, and still deletes the lowering
  complexity.

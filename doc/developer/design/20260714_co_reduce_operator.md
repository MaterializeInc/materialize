# Design: `co_reduce2`, a two-input reduce over co-arranged inputs

## Goal

Generalize the fused `SetDifference` operator (`src/compute/src/render/set_difference.rs`,
CLU-168) into `co_reduce2`: a reduce over two co-arranged inputs, with distinct trace
types, with the per-key computation supplied as a closure.
Naiad calls this shape `CoGroupBy`.
`SetDifference` is a two-input caller whose closure computes the thresholded net.

A variadic reduce over N homogeneous inputs was the original target for this design.
That scope was descoped to the two-input, heterogeneous-trace `co_reduce2` that shipped.
See "Future work: variadic-N `co_reduce`" at the end for the deferred generalization.

## Where it lives

`mz-timely-util` (`src/timely-util/`).
The crate already depends on `timely` and `differential-dataflow`, carries no
`mz-repr` dependency, and hosts exactly this class of generic timely+dd machinery
(`operator.rs` extension traits, the columnar merge-batchers, `antichain.rs`,
`order.rs`, `pact.rs`).
Authoring `co_reduce2` there keeps it Row-agnostic and on par with differential's own
`reduce`.
Materialize's `RowRowSpine` satisfies the trait bounds without the crate learning
about `Row`.

The alternatives are worse for this purpose.
Timely alone has no arrangement or trace concept, so it is the wrong layer.
`differential-dataflow` proper or `differential-dogs3` would work but add an external
release cycle.
Lift to one of those later only if a non-Materialize consumer appears.

## What was generic vs Materialize-specific

The original fused operator mixed a generic reduce-core with Row/compute wiring.
The extraction split along that seam.

| Piece | Home |
| --- | --- |
| Two-input consumption, per-input frontier tracking, `upper_limit = meet`, read-at-own-frontier, output `TraceAgent`, per-capability builders, `seal`, input/output compaction, `pending`/`CapabilitySet` interesting-times, fueling | `mz-timely-util` (`co_reduce.rs`) |
| `KeyWork`, `own_current_key`, `seek_owned_key`, `stage_times`, `read_key_values` | `mz-timely-util`, generic over `Cursor` |
| `compute_key`/`emit_deltas` per-key synthetic-time closure evaluation | `mz-timely-util`. The arithmetic (base plus diff, subtract minus diff, threshold) is the caller's closure |
| `RowRowSpine`/`RowRowAgent`/`RowRowBuilder`, `Row`, `Diff`, error handling, `MzArrange`, `log_arrangement_size`, `differential/default_exert_logic` lookup | compute |
| `Context`, `CollectionBundle`, `ArrangementFlavor`, the `Local`/`Trace` arm demux, `arm_arrangement` | compute |

## Signature

The shipped signature (`src/timely-util/src/co_reduce.rs`):

```rust
pub fn co_reduce2<'scope, T, Tr1, Tr2, K, V, V2, R2, Bu, Out, L>(
    input0: Arranged<'scope, Tr1>,
    input1: Arranged<'scope, Tr2>,
    name: &str,
    fuel: usize,
    logic: L,
) -> Arranged<'scope, TraceAgent<Out>>
where
    T: Timestamp + Lattice + Ord,
    Tr1: TraceReader<Time = T> + Clone + 'static,
    // `Tr2` shares the key GAT with `Tr1` so their keys compare, and its diff is pinned
    // to `Tr1::Diff` so both inputs feed the closure as the one input diff type `D`.
    Tr2: for<'a> TraceReader<Key<'a> = Tr1::Key<'a>, Time = T, Diff = Tr1::Diff> + Clone + 'static,
    Tr1::Diff: Semigroup + Clone + 'static,
    Tr1::KeyContainer: BatchContainer<Owned = K>,
    Tr1::ValContainer: BatchContainer<Owned = V>,
    Tr2::KeyContainer: BatchContainer<Owned = K>,
    Tr2::ValContainer: BatchContainer<Owned = V>,
    K: Ord + Clone + 'static,
    V: Ord + Clone + 'static,
    V2: Ord + Clone + 'static,
    R2: Abelian + Clone + 'static,
    Out: for<'a> Trace<Key<'a> = Tr1::Key<'a>, ValOwn = V2, Time = T, Diff = R2> + 'static,
    Out::KeyContainer: BatchContainer<Owned = K>,
    Out::ValContainer: BatchContainer<Owned = V2>,
    Bu: Builder<Time = T, Output = Out::Batch> + 'static,
    Bu::Input: Default + PushInto<((K, V2), T, R2)>,
    L: FnMut(&K, &[&[(V, Tr1::Diff)]], &mut Vec<(V2, R2)>) + 'static,
```

For each key present in either input, at every interesting time `t`, `logic` runs with
the two consolidated `(value, diff)` multisets of updates with time `<= t`: `input0`'s
slice first, `input1`'s second.
`logic` writes the desired output `(value, diff)` multiset at `t`.
The operator diffs desired against prior output per value and emits the minimal
updates into one output arrangement.
An output time finalizes only once it is complete in both inputs, i.e.
`<=`-covered by the meet of the two input frontiers.

`Tr1` and `Tr2` may be distinct trace types.
They unify only on the key GAT (so their keys compare), the owned key `K` and value
`V` (so the closure sees homogeneous `(V, D)` slices), and the diff `D` (pinned to
`Tr1::Diff`).
This lets a caller pass two different arrangement flavors directly, for example a
local arrangement and an entered trace, taking each by monomorphization instead of
forcing both onto one trace type.

There is no `G: Scope` type parameter.
The scope is read off `input0.stream.scope()` inside the function body.

## The two forked pieces, generalized

1. **Value-collapse to value-aware.**
   The pre-extraction `set_difference` fused operator emitted a single empty value
   and collapsed each input to a per-time running count, so its per-key computation
   never grouped by value.
   `co_reduce2` retains values: per key, per input, a `Vec<(V, T, D)>`.
   At each interesting time it consolidates each input's `(V, D)` as-of that time
   (sum diffs of entries with `ti <= t`, group by `V`, drop zeros) and hands the
   closure `&[&[(V, D)]]`.
   This re-derives, for two inputs, what differential's private `ValueHistory` does
   for one.
   The collapsed single-empty-value operator is the special case of it.

2. **Baked-in arithmetic to closure.**
   The old `net = sum(base) - sum(subtract)` and threshold are now the closure body
   (`set_difference_threshold` in `set_difference.rs`).
   The operator keeps ownership of the interesting-times worklist, synthetic
   join-closure, capability assignment, and desired-vs-current diffing.

## Negation in the closure

Negation is not an operator feature.
The operator reads both inputs plainly (no sign flip) and passes each input's
`(V, D)` slice separately.
`SetDifference`'s closure, with `input0 = base` and `input1 = subtract`:

```rust
fn set_difference_threshold(_key: &Row, inputs: &[&[(Row, Diff)]], out: &mut Vec<(Row, Diff)>) {
    let sum = |s: &[(Row, Diff)]| s.iter().map(|(_v, d)| *d).sum::<Diff>();
    let net = sum(inputs[0]) - sum(inputs[1]);
    if net.is_positive() {
        out.push((Row::default(), net));
    }
}
```

The operator does not know which input is the minuend.
This is simpler than the pre-extraction code, which flipped the subtract diffs while
reading them.
Value/collision closures (PIVOT's collision-resolve, replace) would compose with this
one: one closure over structure, one over arithmetic.

## Fueling

The pre-extraction operator processed every dirty key in one activation: an
O(dirty-set-size)-per-round cost with no bound on the work done in a single
activation.
`co_reduce2` bounds that with a `fuel: usize` parameter and an
accumulate-then-ship-on-drain scheme.

A round finalizes the interval `[processed, upper_limit)` over the dirty-key set
staged in `pending`.
Each activation processes at most `fuel` keys from that set (`Round::remaining`, a
`BTreeMap` drained in ascending key order).
Per-key output accumulates into per-capability `Builder`s (`Round::builders`) rather
than being shipped immediately.
Only once `remaining` is fully drained does the operator build, ship, and seal the
round's batches.
While it is not drained, the operator re-activates itself (`reactivate.activate()`)
and resumes the same round on the next activation, holding the round's output
capabilities so the output frontier stays at the round's lower limit, and deferring
input/output compaction.

This guarantees downstream never observes a partially emitted round as complete: a
round is atomic from the outside regardless of how many activations it takes
internally.
Bounding keys per activation caps CPU per activation without changing output
correctness or frontier semantics.
`SetDifference` wires this to a fixed constant, `SET_DIFFERENCE_FUEL`
(`set_difference.rs`).
See the `TODO` on that constant for the plan to make it configurable.

## Migration (as it played out)

1. Landed `co_reduce2` in `mz-timely-util` with unit tests (`co_reduce.rs`) covering
   incremental correctness (retraction, reappearance, net-zero, multiplicity,
   multi-value-per-key) and fueled/unfueled equivalence.
2. Reimplemented the fused set-difference operator as a `co_reduce2` call with the
   threshold closure.
   The compute-side `render_set_difference` (bundle demux, error arrange,
   `arm_arrangement`, output `CollectionBundle`) stayed unchanged.
   Only its inner operator call changed.
   `outer_join.slt` stayed byte-identical.
3. Added the fueling seam (`fuel: usize` plus the `Round` accumulate-then-drain
   scheme) in the same operator, ahead of any large-dirty-set caller needing it.
4. Future callers, still open: PIVOT (triple-store to wide row, collapses a wide
   delta join into one reduce), some `DISTINCT` forms, same-key variadic joins
   without inter-stage re-arrangement.
   These motivate the variadic-N generalization below.

## Resolved against differential-dataflow 0.24 source

### Output builder / trace generality (resolved)

The `Arranged`-based `reduce_trace` (`reduce.rs:34`) was the template, not the
`Collection`-based `reduce_core` (which hardcodes `Bu::Input = Vec<..>` and is
incompatible with `RowRowBuilder`, whose `Input` is a `TimelyStack` columnation
stack).
Its output bounds:

```rust
Out: for<'a> Trace<Key<'a> = Tr::Key<'a>, ValOwn = V2, Time = Tr::Time, Diff = R2> + 'static,
Bu:  Builder<Time = Tr::Time, Output = Out::Batch, Input: Default>,
```

No output `Batcher` parameter exists on the reduce path.
Reduce builds batches directly with a `Builder`.
The `Batcher` bounds in the arrange operators are unrelated.
`Batch: Batch` rides in free on the `Trace` supertrait.
Every output call the shipped operator makes (`Trace::new`, `TraceAgent::new`,
`Builder::new/push/done`, `TraceWriter::insert/seal/exert`, the `TraceReader`
compaction/`cursor_through` calls) is generic surface, nothing inherent to
`RowRowSpine`.
So `Out = RowRowSpine<T, Diff>`, `Bu = RowRowBuilder<T, Diff>` reproduces the fused
operator's return type `Arranged<TraceAgent<RowRowSpine<T, Diff>>> = Arranged<RowRowAgent>`,
matching `co_reduce2`'s actual output bounds shown above.

### Interesting-time synthesis (resolved: always synthesize)

Differential has no linearity fast-path.
`HistoryReplayer::compute` (`reduce.rs:558-575`) synthesizes the join-closure
unconditionally for every in-region time, joining it with all batch and current
times, regardless of the user logic (`L` is opaque).
The only gate is the `interesting` flag (`reduce.rs:444-486`), which decides whether
`logic` is *called* at a time, driven by presence of updates, not by any property of
`L`.
`co_reduce2`'s `compute_key` partner-join is a faithful fork of this.

For a totally ordered timestamp, synthesis is already a no-op (a join of comparable
times is one of them, already interesting: the `synth == t` guard in `compute_key`).
It only bites for multi-dimensional lattice times (product/nested timestamps in
iterative scopes).

Decision: `co_reduce2` **always synthesizes**, matching differential.
Simplest, unconditionally correct, free for single-dimensional time.
A `linear` opt-out to skip synthesis is a legitimate future optimization but must be
an explicit, off-by-default caller opt-in, never inferred.
Skipping synthesis under a non-linear closure yields silent wrong results: the
output multiplicity at an un-evaluated join time is stale and nothing downstream
flags it.
It only earns its keep on multi-dimensional lattice timestamps under a provably
linear closure.

## Builder-input filling (decided: inlined push)

`co_reduce2` fills the output builder itself, via
`Bu::Input: Default + PushInto<((K, V2), T, R2)>`.
The public signature stays `logic`-only (one closure), matching the pre-extraction
fused operator.
This bakes the `((key, val), time, diff)` input layout into `co_reduce2`, which
covers every foreseeable Materialize caller (all use `RowRowBuilder`, exactly this
layout).

The push-closure form `reduce_trace` exposes (a second parameter
`P: FnMut(&mut Bu::Input, K, &mut Vec<(V2, Tr::Time, R2)>)`, requiring only
`Bu::Input: Default`) is more general but adds a second closure to the public
signature.
No Materialize caller needs it.
If a builder whose input is not `((key, val), t, r)` ever appears, add `P` then.

## Future work: variadic-N `co_reduce`

The original goal for this design was a variadic `co_reduce` over N homogeneous
arranged inputs sharing one trace type `Tr`, in the shape of Naiad's `CoGroupBy`.
That generalization did not ship.
`co_reduce2` covers `SetDifference`'s two heterogeneous-trace inputs, which was
enough for CLU-168, and remains the only caller.
The N-input generalization is deferred, not abandoned.
PIVOT and same-key variadic joins (see Migration step 4) are the motivating future
callers.

Sketch of the extension, unchanged from the original design:

* Replace `binary_frontier` with a hand-built `timely::OperatorBuilder` that calls
  `new_input` in a loop over a `Vec<Arranged<G, Tr>>`.
* The per-input frontiers become one `Vec<Antichain<T>>`.
  `upper_limit` is the meet across all of them, not just two.
* `stage_times` runs per input, as it already does for two.
* The self-difference safety argument (acquire each cursor sequentially,
  `cursor_through` returns owned storage, no two trace borrows held at once) extends
  unchanged to N sequential acquisitions.
* Homogeneity (all inputs share one `Tr`) is the one place this shape is less
  general than `co_reduce2`, which takes two distinct trace types.
  Heterogeneous-N would need boxed or erased cursors and is not needed by any known
  caller.

# Design: `co_reduce`, a variadic reduce over co-arranged inputs

## Goal

Generalize the fused `SetDifference` operator (`src/compute/src/render/set_difference.rs`,
CLU-168) into `co_reduce`: a reduce over N homogeneous arranged inputs sharing a key
type, with the per-key computation supplied as a closure.
Naiad calls this shape `CoGroupBy`.
`SetDifference` becomes a two-input caller whose closure computes the thresholded net.

## Where it lives

`mz-timely-util` (`src/timely-util/`).
The crate already depends on `timely` and `differential-dataflow`, carries no
`mz-repr` dependency, and hosts exactly this class of generic timely+dd machinery
(`operator.rs` extension traits, the columnar merge-batchers, `antichain.rs`,
`order.rs`, `pact.rs`).
Authoring `co_reduce` there keeps it Row-agnostic and on par with differential's own
`reduce`.
Materialize's `RowRowSpine` satisfies the trait bounds without the crate learning
about `Row`.

The alternatives are worse for this purpose.
Timely alone has no arrangement or trace concept, so it is the wrong layer.
`differential-dataflow` proper or `differential-dogs3` would work but add an external
release cycle.
Lift to one of those later only if a non-Materialize consumer appears.

## What is already generic vs Materialize-specific

The current operator mixes a fully generic reduce-core with Row/compute wiring.
The extraction is that seam.

| Piece (current location in `set_difference.rs`) | Home |
| --- | --- |
| `set_difference_core` operator body: two-input consumption, per-input frontier tracking, `upper_limit = meet`, read-at-own-frontier, output `TraceAgent`, per-capability builders, `seal`, input/output compaction, `pending`/`CapabilitySet` interesting-times | `mz-timely-util` (generalized to N inputs) |
| `KeyWork`, `read_current_key`, `own_current_key`, `stage_times`, `seek_owned_key` | `mz-timely-util` (generic over `Cursor`) |
| `compute_key` per-key net + synthetic-time closure | `mz-timely-util`, but the arithmetic (base `+diff`, subtract `-diff`, threshold) becomes the user closure |
| `RowRowSpine`/`RowRowAgent`/`RowRowBuilder`, `Row`, `Diff`, error handling, `MzArrange`, `log_arrangement_size`, `differential/default_exert_logic` lookup | compute |
| `Context`, `CollectionBundle`, `ArrangementFlavor`, the `Local`/`Trace` arm demux, `arm_arrangement` | compute |

## Signature

Follows differential's `reduce_core`/`reduce_abelian`, but takes a `Vec` of inputs
rather than one, and is a free function rather than a method (it has no single
receiver).

```rust
/// Variadic reduce over N homogeneous arranged inputs sharing a key type.
///
/// For each key, `logic` runs at every interesting time with, per input, the
/// consolidated `(value, diff)` multiset accumulated up to that time (input `i`'s
/// slice is `inputs[i]`). It writes the desired output `(value, diff)` multiset for
/// that time. The operator diffs the desired output against the prior output and
/// emits the minimal set of updates into a single output arrangement.
///
/// An output time is finalized only once it is complete in every input (the meet of
/// the per-input frontiers). Each input is read at its own frontier, so a
/// `cursor_through` never straddles a batch even when one input runs ahead.
pub fn co_reduce<G, Tr, K, V, V2, R2, Bu, Out, L>(
    inputs: Vec<Arranged<G, Tr>>,
    name: &str,
    logic: L,
) -> Arranged<G, TraceAgent<Out>>
where
    G: Scope<Timestamp = Tr::Time>,
    Tr: TraceReader + Clone + 'static,
    Tr::Time: Lattice + Ord,                       // Ord = a linear extension for the worklist
    Tr::Diff: Semigroup + Clone,
    Tr::KeyContainer: BatchContainer<Owned = K>,   // K = Row for Materialize
    K: Ord + Clone + 'static,
    for<'a> Tr::Val<'a>: IntoOwned<'a, Owned = V>,
    V: Ord + Clone + 'static,
    V2: Ord + Clone + 'static,
    R2: Semigroup + Clone,
    Out: Trace<Time = Tr::Time, Diff = R2> + 'static,
    for<'a> Out::Key<'a>: /* built from K */,
    for<'a> Out::Val<'a>: /* built from V2 */,
    Bu: Builder<Time = Tr::Time, Output = Out::Batch>,
    L: FnMut(&K, &[&[(V, Tr::Diff)]], &mut Vec<(V2, R2)>) + 'static,
```

The exact `IntoOwned`/`Builder` bounds match whatever `reduce_core` uses on the
pinned differential version; the shape above is the contract, not the final
where-clause.

Homogeneity (all inputs `Tr`) is the one place `co_reduce` is less general than
`join`, which takes two distinct trace types.
Heterogeneous-N would require boxed/erased cursors and is not needed by any
Materialize caller.
Document it as the boundary.

## The three forked pieces, generalized

1. **Two inputs to N.**
   Replace `binary_frontier` with a hand-built `timely::OperatorBuilder` that calls
   `new_input` in a loop over `inputs`.
   `upper_base`/`upper_subtract` become `Vec<Antichain<T>>`, one per input;
   `upper_limit` is the meet across all of them.
   `stage_times` runs per input.
   The self-difference safety argument (acquire each cursor sequentially,
   `cursor_through` returns owned storage, no two trace borrows held at once)
   extends unchanged to N sequential acquisitions.

2. **Value-collapse to value-aware.**
   `set_difference` emits a single empty value and collapses each input to a
   per-time running count, so `compute_key` never groups by value.
   `co_reduce` must retain values: per key, per input, a `Vec<(V, T, Diff)>`.
   At each interesting time it consolidates each input's `(V, Diff)` as-of that time
   (sum diffs of entries with `ti <= t`, group by `V`, drop zeros) and hands the
   closure `&[&[(V, Diff)]]`.
   This re-derives, for N inputs, what differential's private `ValueHistory` does for
   one; the collapse in `set_difference` is the single-empty-value special case of it.

3. **Baked-in arithmetic to closure.**
   `compute_key`'s `net = sum(base) - sum(subtract)` and threshold become the
   closure body.
   The operator keeps ownership of the interesting-times worklist, synthetic
   join-closure, capability assignment, and desired-vs-current diffing.

## Negation in the closure

Negation stops being an operator feature.
The operator reads every input plainly (no sign flip) and passes each input's
`(V, Diff)` slice separately.
`SetDifference`'s closure, with inputs `[base, subtract]`:

```rust
|_key, inputs: &[&[((), Diff)]], out: &mut Vec<((), Diff)>| {
    let sum = |slice: &[((), Diff)]| slice.iter().map(|(_v, d)| *d).sum::<Diff>();
    let net = sum(inputs[0]) - sum(inputs[1]);   // subtract is inputs[1]
    if net.is_positive() {
        out.push(((), net));
    }
}
```

The operator no longer knows which input is the minuend.
This is simpler than today's code, which flips the subtract diffs during
`read_current_key` (the `negate` parameter disappears).
Value/collision closures (PIVOT's collision-resolve, replace) compose with this one:
one closure over structure, one over arithmetic.

## Fueling seam

Today the per-key loop processes every dirty key in one activation.
That is the known O(n^2)-per-round / no-fueling limit (large dirty sets stall the
worker).
`mz_join_core` is the reference for the fix and it is generic, not Row-specific, so
it belongs with the operator in `mz-timely-util`:

* Thread a `fuel: &mut isize` budget through the per-key loop, decrementing per unit
  of work (per key, or per `(time, value)` evaluated).
* When `fuel` hits zero, stop draining `pending`, retain the unprocessed keys in
  `next_pending`, keep their capabilities, and re-activate via the operator's
  `Activator` so the next invocation resumes.
* The output `seal`/compaction still advance only to `upper_limit`, so a
  partially-processed round is correct: unshipped keys stay pending and their
  capabilities are retained below `upper_limit`.

The budget size comes from the same `differential/default_exert_logic` /
`ExertionLogic` config the operator already reads; expose it as a parameter so
compute can wire the Materialize default and `mz-timely-util` stays policy-free.

## Migration

1. Land `co_reduce` in `mz-timely-util` with a Row-typed unit test that reproduces
   the set-difference closure and checks incremental correctness (retraction,
   reappearance, net-zero, multiplicity) against a reference.
2. Reimplement `set_difference_core` as a `co_reduce` call with the threshold
   closure.
   Keep the compute-side `render_set_difference` (bundle demux, error arrange,
   `arm_arrangement`, output `CollectionBundle`) unchanged; only its inner operator
   call changes.
   `outer_join.slt` must stay byte-identical.
3. Add the fueling seam and a large-dirty-set test.
4. Future callers: PIVOT (triple-store to wide row; collapses a wide delta join into
   one `co_reduce`), some `DISTINCT` forms, same-key variadic joins without
   inter-stage re-arrangement.

## Resolved against differential-dataflow 0.24 source

### Output builder / trace generality (resolved)

The `Arranged`-based `reduce_trace` (`reduce.rs:34`) is the template, not the
`Collection`-based `reduce_core` (which hardcodes `Bu::Input = Vec<..>` and is
incompatible with `RowRowBuilder`, whose `Input` is a `TimelyStack` columnation
stack).
Its output bounds are:

```rust
Out: for<'a> Trace<Key<'a> = Tr::Key<'a>, ValOwn = V2, Time = Tr::Time, Diff = R2> + 'static,
Bu:  Builder<Time = Tr::Time, Output = Out::Batch, Input: Default>,
```

No output `Batcher` parameter exists on the reduce path (reduce builds batches
directly with a `Builder`; the `Batcher` bounds in the arrange operators are
unrelated).
`Batch: Batch` rides in free on the `Trace` supertrait.
Every output call `set_difference_core` makes (`Trace::new`, `TraceAgent::new`,
`Builder::new/push/done`, `TraceWriter::insert/seal/exert`, the `TraceReader`
compaction/`cursor_through` calls) is generic surface, nothing is inherent to
`RowRowSpine`.
So `Out = RowRowSpine<T, Diff>`, `Bu = RowRowBuilder<T, Diff>` reproduces today's
return type `Arranged<TraceAgent<RowRowSpine<T, Diff>>> = Arranged<RowRowAgent>`.

The where-clause `co_reduce` declares:

```rust
pub fn co_reduce<'scope, G, Tr, K, V2, R2, Bu, Out, L>(
    inputs: Vec<Arranged<'scope, Tr>>,
    name: &str,
    logic: L,
) -> Arranged<'scope, TraceAgent<Out>>
where
    G: Scope<Timestamp = Tr::Time>,
    Tr: TraceReader + Clone + 'static,
    Tr::Time: Lattice,
    Tr::KeyContainer: BatchContainer<Owned = K>,   // K = Row for Materialize
    Out: for<'a> Trace<Key<'a> = Tr::Key<'a>, ValOwn = V2, Time = Tr::Time, Diff = R2> + 'static,
    Bu: Builder<Time = Tr::Time, Output = Out::Batch, Input: Default + PushInto<((K, V2), Tr::Time, R2)>>,
    V2: Data,
    R2: Semigroup,   // + Abelian only for the reduce_abelian output-negation convenience
    L: FnMut(&K, &[&[(Tr::Val<'_>, Tr::Diff)]], &mut Vec<(V2, R2)>),
```

The one genuinely open decision is how the builder input is filled (see below); it
is a signature-shape trade, not a correctness constraint.

### Interesting-time synthesis (resolved: always synthesize)

Differential has NO linearity fast-path.
`HistoryReplayer::compute` (`reduce.rs:558-575`) synthesizes the join-closure
unconditionally for every in-region time, joining it with all batch and current
times, regardless of the user logic (`L` is opaque).
The only gate is the `interesting` flag (`reduce.rs:444-486`), which decides whether
`logic` is *called* at a time, driven by presence of updates, not by any property of
`L`.
`set_difference_core`'s `compute_key` partner-join (`set_difference.rs:408-422`) is a
faithful fork.

For a totally ordered timestamp, synthesis is already a no-op (a join of comparable
times is one of them, already interesting: `set_difference.rs:414` `synth == t`
guard).
It only bites for multi-dimensional lattice times (product/nested timestamps in
iterative scopes).

Decision: `co_reduce` **always synthesizes**, matching differential.
Simplest, unconditionally correct, free for single-dimensional time.
A `linear` opt-out to skip synthesis is a legitimate FUTURE optimization but must be
an explicit, off-by-default caller opt-in, never inferred: skipping synthesis under a
non-linear closure yields SILENT wrong results (the output multiplicity at an
un-evaluated join time is stale and nothing downstream flags it).
It only earns its keep on multi-dimensional lattice timestamps under a provably
linear closure.

## Builder-input filling (decided: inlined push)

`co_reduce` fills the output builder itself, adding
`Bu::Input: Default + PushInto<((K, V2), Tr::Time, R2)>`.
The public signature stays `logic`-only (one closure), matching `set_difference_core`
today.
This bakes the `((key, val), time, diff)` input layout into `co_reduce`, which covers
every foreseeable Materialize caller (all use `RowRowBuilder`, exactly this layout).

The push-closure form `reduce_trace` exposes (a second parameter
`P: FnMut(&mut Bu::Input, K, &mut Vec<(V2, Tr::Time, R2)>)`, requiring only
`Bu::Input: Default`) is more general but adds a second closure to the public
signature.
No Materialize caller needs it.
If a builder whose input is not `((key, val), t, r)` ever appears, add `P` then.

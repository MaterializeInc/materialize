// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License in the LICENSE file at the
// root of this repository, or online at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! A reduce over two co-arranged inputs.
//!
//! [`co_reduce2`] takes two arrangements that agree on key type but may carry distinct
//! trace types, and a per-key `logic` closure, and produces a single output arrangement.
//! It generalizes a fused two-input set-difference operator along two axes: a caller
//! closure instead of baked-in arithmetic, and value-aware output accounting instead of
//! a single collapsed empty value. The two inputs feed the closure as homogeneous
//! `(value, diff)` multisets, so they share the input value type `V` and diff type `D`,
//! while their trace types stay independent. That lets the operator take two distinct
//! arrangement flavors (for example a local agent versus an entered trace) by
//! monomorphization, the way `mz_join_core` and `set_difference_core` do.
//!
//! The operator finalizes an output time only once it is complete in both inputs (the
//! meet of the two per-input frontiers). It reads each input at its OWN frontier, not at
//! the meet: `cursor_through(meet)` would panic with "upper straddles batch" whenever
//! the meet fell inside a batch of an input whose frontier ran ahead. Reading each
//! input at its own frontier is straddle-free, and reading data beyond the meet is
//! harmless because it cannot affect the output at any time below the meet.
//!
//! Interesting times: because `logic` may be non-linear (its output can change at a
//! join of input times where no input has a literal update), the operator seeds
//! interesting times from batch and pending times and repeatedly joins each processed
//! time with every input time to reach the full join-closure. For a totally ordered
//! timestamp this synthesis is a no-op.
//!
//! Fuel: a round finalizes the interval `[processed, upper_limit)` over the dirty-key set
//! staged in `pending`. That set can be arbitrarily large, so the operator bounds the
//! number of keys it processes per activation by `fuel` and yields the timely worker
//! rather than draining the whole set in one shot. A round in progress is finalized
//! atomically: the operator accumulates processed keys' updates into per-capability
//! builders and only builds, ships, and seals the round's batches once its dirty set
//! fully drains. Until then it holds the round's output capabilities (so the output
//! frontier stays at `processed`), leaves `processed` unadvanced, defers compaction, and
//! re-activates itself. This guarantees downstream never observes a partially emitted
//! round as complete. Bounding keys per activation caps CPU per activation without
//! changing output correctness or frontier semantics.

use std::cmp::{Ordering, Reverse};
use std::collections::{BTreeMap, BTreeSet, BinaryHeap};
use std::rc::Rc;

use differential_dataflow::consolidation::consolidate;
use differential_dataflow::difference::{Abelian, Semigroup};
use differential_dataflow::lattice::{Lattice, antichain_join_into};
use differential_dataflow::operators::arrange::{Arranged, TraceAgent};
use differential_dataflow::trace::implementations::BatchContainer;
use differential_dataflow::trace::{
    BatchReader, Builder, Cursor, Description, ExertionLogic, Trace, TraceReader,
};
use timely::PartialOrder;
use timely::container::PushInto;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::{Capability, CapabilitySet, Operator};
use timely::progress::{Antichain, Timestamp};

/// Owns the cursor's current key.
fn own_current_key<C, K>(cursor: &C, storage: &C::Storage) -> K
where
    C: Cursor,
    C::KeyContainer: BatchContainer<Owned = K>,
{
    <C::KeyContainer as BatchContainer>::into_owned(cursor.get_key(storage).expect("key exists"))
}

/// Advances the cursor forward to `target`, returning whether it is present.
///
/// Keys are visited in ascending order across a round, so advancing each source cursor
/// forward to successive targets is monotone. On a match the cursor is left positioned
/// at the key (values at their start); on a miss it stops at the first key greater than
/// `target`.
fn seek_owned_key<C, K>(cursor: &mut C, storage: &C::Storage, target: &K) -> bool
where
    C: Cursor,
    C::KeyContainer: BatchContainer<Owned = K>,
    K: Ord,
{
    while cursor.key_valid(storage) {
        match own_current_key::<C, K>(cursor, storage).cmp(target) {
            Ordering::Less => cursor.step_key(storage),
            Ordering::Equal => return true,
            Ordering::Greater => return false,
        }
    }
    false
}

/// Records every update time of every key in the cursor into `pending`.
///
/// Staged times survive rounds in which the combined frontier does not advance, so a
/// batch that arrives while another input lags is not lost. Diffs and values are not
/// staged: the per-key history is re-read from the input traces at process time.
fn stage_times<C, K>(cursor: &mut C, storage: &C::Storage, pending: &mut BTreeMap<K, Vec<C::Time>>)
where
    C: Cursor,
    C::KeyContainer: BatchContainer<Owned = K>,
    K: Ord + Clone,
    C::Time: Clone,
{
    while cursor.key_valid(storage) {
        let key = own_current_key::<C, K>(cursor, storage);
        let times = pending.entry(key).or_default();
        while cursor.val_valid(storage) {
            cursor.map_times(storage, |t, _d| times.push(C::owned_time(t)));
            cursor.step_val(storage);
        }
        cursor.step_key(storage);
    }
}

/// Reads all `(value, time, diff)` updates of the cursor's current key, owning values.
///
/// The cursor must be positioned at the key. The value cursor is advanced to its end.
/// Unlike a threshold reduce, values are retained: the closure sees per-input value
/// multisets, so it can key output on input values.
fn read_key_values<C, V>(cursor: &mut C, storage: &C::Storage, out: &mut Vec<(V, C::Time, C::Diff)>)
where
    C: Cursor,
    C::ValContainer: BatchContainer<Owned = V>,
    V: Clone,
    C::Time: Clone,
    C::Diff: Clone,
{
    while cursor.val_valid(storage) {
        let val: V = <C::ValContainer as BatchContainer>::into_owned(cursor.val(storage));
        cursor.map_times(storage, |t, d| {
            out.push((val.clone(), C::owned_time(t), C::owned_diff(d)));
        });
        cursor.step_val(storage);
    }
}

/// Per-key state assembled during a round.
struct KeyWork<T, V, D, V2, R2> {
    /// Per input: full `(value, time, diff)` history read from that input's trace.
    inputs: Vec<Vec<(V, T, D)>>,
    /// Prior output `(value, time, diff)` read from the output trace.
    prior: Vec<(V2, T, R2)>,
    /// In-region interesting seed times (batch and pending times below the round upper).
    seeds: Vec<T>,
}

/// Evaluates one key over the round's interesting times.
///
/// At each interesting time `t`, consolidates each input's `(value, diff)` accumulated
/// up to `t`, calls `logic` to get the desired output multiset at `t`, and emits the
/// per-value delta against the current output (prior plus already-produced). Synthesizes
/// join-closure times so a non-linear closure is evaluated wherever its output can
/// change. Times at or beyond `upper_limit` are deferred into `new_interesting`.
#[allow(clippy::too_many_arguments)]
fn compute_key<T, K, V, D, V2, R2, L>(
    key: &K,
    kw: &KeyWork<T, V, D, V2, R2>,
    upper_limit: &Antichain<T>,
    logic: &mut L,
    cap_times: &[T],
    cap_updates: &mut [Vec<(V2, T, R2)>],
    new_interesting: &mut Vec<T>,
) where
    T: Lattice + Ord + Clone,
    V: Ord + Clone,
    D: Semigroup + Clone,
    V2: Ord + Clone,
    R2: Abelian + Clone,
    L: FnMut(&K, &[&[(V, D)]], &mut Vec<(V2, R2)>),
{
    // Partners for synthetic-time joins: every input time and prior-output time. Both
    // lie in the join-closure of input times; including prior is redundant but harmless.
    let mut partners: Vec<T> = Vec::new();
    for hist in &kw.inputs {
        partners.extend(hist.iter().map(|(_v, t, _d)| t.clone()));
    }
    partners.extend(kw.prior.iter().map(|(_v, t, _d)| t.clone()));
    partners.extend(kw.seeds.iter().cloned());
    partners.sort();
    partners.dedup();

    let mut queued: BTreeSet<T> = BTreeSet::new();
    let mut worklist: BinaryHeap<Reverse<T>> = BinaryHeap::new();
    for t in &kw.seeds {
        if upper_limit.less_equal(t) {
            new_interesting.push(t.clone());
        } else if queued.insert(t.clone()) {
            worklist.push(Reverse(t.clone()));
        }
    }

    // Output produced this round for this key, diffed against.
    let mut produced: Vec<(V2, T, R2)> = Vec::new();
    // Reusable buffers.
    let mut asof: Vec<Vec<(V, D)>> = (0..kw.inputs.len()).map(|_| Vec::new()).collect();
    let mut desired: Vec<(V2, R2)> = Vec::new();

    while let Some(Reverse(t)) = worklist.pop() {
        // Per-input consolidated (value, diff) as of `t`.
        for (i, hist) in kw.inputs.iter().enumerate() {
            asof[i].clear();
            for (v, ti, d) in hist {
                if PartialOrder::less_equal(ti, &t) {
                    asof[i].push((v.clone(), d.clone()));
                }
            }
            consolidate(&mut asof[i]);
        }
        let slices: Vec<&[(V, D)]> = asof.iter().map(|v| v.as_slice()).collect();

        desired.clear();
        logic(key, &slices, &mut desired);
        consolidate(&mut desired);

        // Current output as of `t`, per value: prior + produced with time <= t.
        let mut current: Vec<(V2, R2)> = Vec::new();
        for (v, ti, d) in kw.prior.iter().chain(produced.iter()) {
            if PartialOrder::less_equal(ti, &t) {
                current.push((v.clone(), d.clone()));
            }
        }
        consolidate(&mut current);

        // Emit per-value delta = desired - current. Both are consolidated and sorted by
        // value, so merge-walk them.
        emit_deltas(
            &desired,
            &current,
            &t,
            cap_times,
            cap_updates,
            &mut produced,
        );

        // Synthesize joins of `t` with every partner not already <= t.
        for tp in &partners {
            if PartialOrder::less_equal(tp, &t) {
                continue;
            }
            let synth = t.join(tp);
            if synth == t {
                continue;
            }
            if upper_limit.less_equal(&synth) {
                new_interesting.push(synth);
            } else if queued.insert(synth.clone()) {
                worklist.push(Reverse(synth));
            }
        }
    }
}

/// Merge-walks `desired` and `current` (both consolidated, sorted by value) and pushes
/// `desired - current` per value into `produced` and the covering capability's updates.
fn emit_deltas<T, V2, R2>(
    desired: &[(V2, R2)],
    current: &[(V2, R2)],
    t: &T,
    cap_times: &[T],
    cap_updates: &mut [Vec<(V2, T, R2)>],
    produced: &mut Vec<(V2, T, R2)>,
) where
    T: Lattice + Ord + Clone,
    V2: Ord + Clone,
    R2: Abelian + Clone,
{
    let mut di = 0;
    let mut ci = 0;
    let mut push = |v: &V2, delta: R2, produced: &mut Vec<(V2, T, R2)>| {
        if delta.is_zero() {
            return;
        }
        produced.push((v.clone(), t.clone(), delta.clone()));
        // Latest capability covering `t`. One must exist: `t` is reachable from a batch
        // or a pending time, both of which retain a capability at or below their time.
        let idx = cap_times
            .iter()
            .enumerate()
            .rev()
            .find(|(_, ct)| PartialOrder::less_equal(*ct, t))
            .map(|(i, _)| i)
            .expect("a capability covers every produced time");
        cap_updates[idx].push((v.clone(), t.clone(), delta));
    };
    while di < desired.len() && ci < current.len() {
        match desired[di].0.cmp(&current[ci].0) {
            Ordering::Less => {
                push(&desired[di].0, desired[di].1.clone(), produced);
                di += 1;
            }
            Ordering::Greater => {
                let mut neg = current[ci].1.clone();
                neg.negate();
                push(&current[ci].0, neg, produced);
                ci += 1;
            }
            Ordering::Equal => {
                let mut delta = desired[di].1.clone();
                let mut neg = current[ci].1.clone();
                neg.negate();
                delta.plus_equals(&neg);
                push(&desired[di].0, delta, produced);
                di += 1;
                ci += 1;
            }
        }
    }
    while di < desired.len() {
        push(&desired[di].0, desired[di].1.clone(), produced);
        di += 1;
    }
    while ci < current.len() {
        let mut neg = current[ci].1.clone();
        neg.negate();
        push(&current[ci].0, neg, produced);
        ci += 1;
    }
}

/// State of a round whose dirty set did not fully drain within one activation's fuel.
///
/// Persisting this across activations lets the operator bound per-activation work while
/// still finalizing the round atomically. The round never seals `upper_limit` until
/// `remaining` is empty, so downstream never observes a partially emitted round as
/// complete. The held `caps` keep the output frontier at `lower_limit` while work remains.
struct Round<T, K, V2, R2, Bu>
where
    T: Timestamp,
{
    /// Frozen finalization target. Sealed only once `remaining` drains.
    upper_limit: Antichain<T>,
    /// Frozen lower bound. The round's output batches start here.
    lower_limit: Antichain<T>,
    /// Frozen per-input read frontiers. Re-acquired cursors read at these each activation.
    /// They stay valid across activations because the round defers compaction until it
    /// finalizes.
    read_base: Antichain<T>,
    read_subtract: Antichain<T>,
    /// Capabilities held for the round, index-aligned with `builders` and `cap_updates`.
    /// Holding them pins the output frontier at `lower_limit` until the round finalizes.
    caps: Vec<Capability<T>>,
    /// Times of `caps`, precomputed for `emit_deltas`'s covering-capability lookup.
    cap_times: Vec<T>,
    /// Per-capability output builders, accumulated across activations.
    builders: Vec<Bu>,
    /// Per-capability update scratch, drained into `builders` per processed key.
    cap_updates: Vec<Vec<(V2, T, R2)>>,
    /// Dirty keys not yet processed this round, ascending. Values are in-region seeds.
    remaining: BTreeMap<K, Vec<T>>,
    /// Interesting times deferred to a later round (carried plus synthetic), by key.
    deferred: BTreeMap<K, Vec<T>>,
}

/// A reduce over two co-arranged inputs.
///
/// For each key present in either input, at every interesting time `t`, `logic` is called
/// with the two consolidated `(value, diff)` multisets of the inputs' updates with time
/// `<= t`: slice `[0]` is `base`, slice `[1]` is `subtract`. `logic` writes the desired
/// output `(value, diff)` multiset at `t` into its `out` argument. The operator diffs
/// desired against prior output per value and emits the minimal updates into one output
/// arrangement. An output time is finalized only once it is complete in both inputs, i.e.
/// `<=`-covered by the meet of the two input frontiers.
///
/// The two inputs may carry distinct trace types (`Tr1`, `Tr2`), so callers can pass two
/// different arrangement flavors directly without unifying them. They must agree on key
/// GAT, owned key `K`, owned value `V`, and diff `D` (`Tr1::Diff`), since the closure sees
/// both as homogeneous `(V, D)` slices.
///
/// `fuel` bounds the number of dirty keys processed per activation. When a round's dirty
/// set exceeds `fuel`, the operator processes `fuel` keys, re-activates itself, and
/// resumes the same round on the next activation, yielding the timely worker in between.
/// Fueling does not change output correctness or frontier semantics: a round's output is
/// sealed only once its dirty set fully drains.
pub fn co_reduce2<'scope, T, Tr1, Tr2, K, V, V2, R2, Bu, Out, L>(
    base: Arranged<'scope, Tr1>,
    subtract: Arranged<'scope, Tr2>,
    name: &str,
    fuel: usize,
    logic: L,
) -> Arranged<'scope, TraceAgent<Out>>
where
    T: Timestamp + Lattice + Ord,
    Tr1: TraceReader<Time = T> + Clone + 'static,
    // `Tr2` shares the key GAT with `Tr1` so their keys compare, and its diff is pinned to
    // `Tr1::Diff` so both inputs feed the closure as the one input diff type `D`.
    Tr2: for<'a> TraceReader<Key<'a> = Tr1::Key<'a>, Time = T, Diff = Tr1::Diff> + Clone + 'static,
    Tr1::Diff: Semigroup + Clone + 'static,
    Tr1::KeyContainer: BatchContainer<Owned = K>,
    Tr1::ValContainer: BatchContainer<Owned = V>,
    // The key GAT equate does not equate the owned-key container type, so bound `Tr2`'s
    // key and value containers to the same owned types explicitly.
    Tr2::KeyContainer: BatchContainer<Owned = K>,
    Tr2::ValContainer: BatchContainer<Owned = V>,
    K: Ord + Clone + 'static,
    V: Ord + Clone + 'static,
    V2: Ord + Clone + 'static,
    R2: Abelian + Clone + 'static,
    Out: for<'a> Trace<Key<'a> = Tr1::Key<'a>, ValOwn = V2, Time = T, Diff = R2> + 'static,
    Out::KeyContainer: BatchContainer<Owned = K>,
    Out::ValContainer: BatchContainer<Owned = V2>,
    // `Bu::Input` is only required to be default-constructible and to accept output tuples.
    // This admits builders whose input is not a `Vec` (for example a columnar stack), which
    // a `Vec`-typed bound would reject.
    Bu: Builder<Time = T, Output = Out::Batch> + 'static,
    Bu::Input: Default + PushInto<((K, V2), T, R2)>,
    L: FnMut(&K, &[&[(V, Tr1::Diff)]], &mut Vec<(V2, R2)>) + 'static,
{
    let scope = base.stream.scope();

    // A round must make progress, so process at least one key per activation. A zero
    // budget would otherwise defer forever without draining the dirty set.
    let fuel = fuel.max(1);

    // Clones of both input traces, held for the operator's lifetime: each round re-reads
    // both inputs through their own frontiers.
    let mut base_trace = base.trace.clone();
    let mut subtract_trace = subtract.trace.clone();

    let mut result_trace = None;
    let stream = {
        let result_trace = &mut result_trace;
        base.stream.binary_frontier(
            subtract.stream,
            Pipeline,
            Pipeline,
            name,
            move |_capability, operator_info| {
                let mut logic = logic;
                let logger = scope
                    .worker()
                    .logger_for::<differential_dataflow::logging::DifferentialEventBuilder>(
                        "differential/arrange",
                    )
                    .map(Into::into);

                let activator = Some(scope.activator_for(Rc::clone(&operator_info.address)));
                // Separate activator the operator uses to re-schedule itself when a round
                // defers work under fuel.
                let reactivate = scope.activator_for(Rc::clone(&operator_info.address));
                let mut empty = Out::new(operator_info.clone(), logger.clone(), activator);
                if let Some(exert_logic) = scope
                    .worker()
                    .config()
                    .get::<ExertionLogic>("differential/default_exert_logic")
                    .cloned()
                {
                    empty.set_exert_logic(exert_logic);
                }
                let (mut output_reader, mut output_writer) =
                    TraceAgent::new(empty, operator_info, logger);
                *result_trace = Some(output_reader.clone());

                // Persistent per-input received frontiers. Each advances monotonically and
                // combines batch uppers, `advance_upper`, and the input frontier.
                let mut upper_base = Antichain::from_elem(T::minimum());
                let mut upper_subtract = Antichain::from_elem(T::minimum());
                // The last processed frontier. Next round's lower limit.
                let mut processed = Antichain::from_elem(T::minimum());

                // Retained capabilities and staged interesting times, keyed by output key.
                let mut capabilities = CapabilitySet::<T>::new();
                let mut pending: BTreeMap<K, Vec<T>> = BTreeMap::new();

                // In-progress round whose dirty set did not fully drain under fuel. While
                // `Some`, the operator resumes it before considering any newer interval.
                let mut round: Option<Round<T, K, V2, R2, Bu>> = None;

                move |(input1, frontier1), (input2, frontier2), output| {
                    // 1. Drain both inputs. Stage each batch's update times as interesting
                    //    seeds in `pending`, capture output capabilities, and record the
                    //    batch upper. Diffs and values are re-read from the input traces at
                    //    process time, not staged.
                    input1.for_each(|cap, data: &mut Vec<Tr1::Batch>| {
                        capabilities.insert(cap.retain(0));
                        for batch in data.drain(..) {
                            upper_base.clone_from(batch.upper());
                            let mut cursor = batch.cursor();
                            stage_times(&mut cursor, &batch, &mut pending);
                        }
                    });
                    input2.for_each(|cap, data: &mut Vec<Tr2::Batch>| {
                        capabilities.insert(cap.retain(0));
                        for batch in data.drain(..) {
                            upper_subtract.clone_from(batch.upper());
                            let mut cursor = batch.cursor();
                            stage_times(&mut cursor, &batch, &mut pending);
                        }
                    });

                    // Advance each received frontier through empty regions the trace knows
                    // about, then incorporate the input frontier guarantee. Each `upper_*`
                    // stays batch-aligned to its input, which makes
                    // `cursor_through(upper_*)` straddle-free.
                    base_trace.advance_upper(&mut upper_base);
                    subtract_trace.advance_upper(&mut upper_subtract);
                    {
                        let mut joined = Antichain::new();
                        antichain_join_into(
                            &upper_base.borrow()[..],
                            &frontier1.frontier()[..],
                            &mut joined,
                        );
                        upper_base = joined;
                    }
                    {
                        let mut joined = Antichain::new();
                        antichain_join_into(
                            &upper_subtract.borrow()[..],
                            &frontier2.frontier()[..],
                            &mut joined,
                        );
                        upper_subtract = joined;
                    }

                    // Step A: start a round if none is in progress. A round in progress
                    // freezes its finalization target, so it is resumed toward that target
                    // before any newer interval is considered.
                    if round.is_none() {
                        // The processed frontier is the meet of the two received frontiers:
                        // an output time is final only when it is final in both inputs.
                        // Inputs are *read* at their own frontiers (`upper_base`,
                        // `upper_subtract`), not at the meet, to avoid a straddling
                        // `cursor_through`. Reading data beyond the meet is harmless: it
                        // cannot affect the output at any time below the meet.
                        let upper_limit = upper_base.meet(&upper_subtract);
                        let lower_limit = processed.clone();

                        // Retire `[lower_limit, upper_limit)` when it is non-empty.
                        if upper_limit != lower_limit {
                            if capabilities
                                .iter()
                                .any(|c| !upper_limit.less_equal(c.time()))
                            {
                                // Partition the staged dirty set into this round's in-region
                                // seeds (`remaining`, processed under fuel) and beyond-upper
                                // times carried to a later round (`deferred`).
                                let mut remaining: BTreeMap<K, Vec<T>> = BTreeMap::new();
                                let mut deferred: BTreeMap<K, Vec<T>> = BTreeMap::new();
                                for (key, times) in std::mem::take(&mut pending) {
                                    let mut seeds = Vec::new();
                                    let mut carried = Vec::new();
                                    for t in times {
                                        if upper_limit.less_equal(&t) {
                                            carried.push(t);
                                        } else {
                                            seeds.push(t);
                                        }
                                    }
                                    if !seeds.is_empty() {
                                        remaining.insert(key.clone(), seeds);
                                    }
                                    if !carried.is_empty() {
                                        deferred.insert(key, carried);
                                    }
                                }

                                // Snapshot the capabilities covering the round's dirty set.
                                // The clones pin the output frontier at `lower_limit` until
                                // the round finalizes, independent of new input arriving in
                                // `capabilities` meanwhile.
                                let caps: Vec<Capability<T>> =
                                    capabilities.iter().cloned().collect();
                                let cap_times: Vec<T> =
                                    caps.iter().map(|c| c.time().clone()).collect();
                                let builders: Vec<Bu> =
                                    (0..cap_times.len()).map(|_| Bu::new()).collect();
                                let cap_updates: Vec<Vec<(V2, T, R2)>> =
                                    (0..cap_times.len()).map(|_| Vec::new()).collect();

                                round = Some(Round {
                                    upper_limit,
                                    lower_limit,
                                    read_base: upper_base.clone(),
                                    read_subtract: upper_subtract.clone(),
                                    caps,
                                    cap_times,
                                    builders,
                                    cap_updates,
                                    remaining,
                                    deferred,
                                });
                            } else {
                                // Empty region: nothing to output, but progress must still be
                                // reflected. Seal and compact to the meet, advance
                                // `processed`.
                                output_writer.seal(upper_limit.clone());
                                base_trace.set_logical_compaction(upper_limit.borrow());
                                base_trace.set_physical_compaction(upper_base.borrow());
                                subtract_trace.set_logical_compaction(upper_limit.borrow());
                                subtract_trace.set_physical_compaction(upper_subtract.borrow());
                                output_reader.set_logical_compaction(upper_limit.borrow());
                                output_reader.set_physical_compaction(upper_limit.borrow());
                                processed = upper_limit;
                            }
                        }
                    }

                    // Step B: drive the in-progress round, processing at most `fuel` keys.
                    if let Some(mut r) = round.take() {
                        if !r.remaining.is_empty() {
                            // Acquire each input cursor at the round's frozen read frontier
                            // and the output cursor at its frozen lower limit. Each
                            // `cursor_through` returns owned storage, so no two trace borrows
                            // are held at once, which keeps self-referencing inputs (aliasing
                            // one trace) safe. Re-acquiring per activation is valid because
                            // the round defers compaction until it finalizes, so the frozen
                            // frontiers stay readable.
                            let (mut base_src, base_src_stor) = base_trace
                                .cursor_through(r.read_base.borrow())
                                .expect("failed to acquire base cursor");
                            let (mut sub_src, sub_src_stor) = subtract_trace
                                .cursor_through(r.read_subtract.borrow())
                                .expect("failed to acquire subtract cursor");
                            let (mut out_cur, out_stor) = output_reader
                                .cursor_through(r.lower_limit.borrow())
                                .expect("failed to acquire output cursor");

                            // Drain up to `fuel` dirty keys. `remaining` is drained ascending
                            // (a `BTreeMap`), so both this activation's seeks and the builder
                            // pushes across activations stay in ascending key order.
                            let mut processed_keys = 0usize;
                            while processed_keys < fuel {
                                let Some((key, seeds)) = r.remaining.pop_first() else {
                                    break;
                                };
                                processed_keys += 1;

                                let mut kw = KeyWork {
                                    inputs: vec![Vec::new(); 2],
                                    prior: Vec::new(),
                                    seeds,
                                };
                                if seek_owned_key(&mut base_src, &base_src_stor, &key) {
                                    read_key_values(
                                        &mut base_src,
                                        &base_src_stor,
                                        &mut kw.inputs[0],
                                    );
                                }
                                if seek_owned_key(&mut sub_src, &sub_src_stor, &key) {
                                    read_key_values(&mut sub_src, &sub_src_stor, &mut kw.inputs[1]);
                                }
                                if seek_owned_key(&mut out_cur, &out_stor, &key) {
                                    read_key_values(&mut out_cur, &out_stor, &mut kw.prior);
                                }

                                // Carried times are already staged in `deferred`; `compute_key`
                                // appends any beyond-upper synthetic times.
                                let mut new_interesting = Vec::new();
                                compute_key(
                                    &key,
                                    &kw,
                                    &r.upper_limit,
                                    &mut logic,
                                    &r.cap_times,
                                    &mut r.cap_updates,
                                    &mut new_interesting,
                                );

                                // Push this key's updates into each capability's builder,
                                // preserving ascending key order across the builder.
                                for i in 0..r.cap_times.len() {
                                    if r.cap_updates[i].is_empty() {
                                        continue;
                                    }
                                    // The builder assumes value-sorted input per key, but the
                                    // updates arrive time-ordered (ascending time, then value
                                    // within a time). A stable sort by value reorders them
                                    // into (value, time) order while preserving the time
                                    // order within equal values.
                                    r.cap_updates[i].sort_by(|a, b| a.0.cmp(&b.0));
                                    let mut builder_buffer = <Bu::Input>::default();
                                    for (v2, time, r2) in r.cap_updates[i].drain(..) {
                                        builder_buffer.push_into(((key.clone(), v2), time, r2));
                                    }
                                    r.builders[i].push(&mut builder_buffer);
                                }

                                if !new_interesting.is_empty() {
                                    new_interesting.sort();
                                    new_interesting.dedup();
                                    r.deferred.entry(key).or_default().extend(new_interesting);
                                }
                            }
                        }

                        if r.remaining.is_empty() {
                            // The round's dirty set fully drained. Only now is it safe to
                            // build, ship, and seal: every dirty key's output below
                            // `upper_limit` is present. Build and ship one batch per
                            // capability. Only one capability may accompany each message, so
                            // each batch's upper folds in the times of the later capabilities.
                            let mut output_lower = r.lower_limit.clone();
                            let caps = r.caps;
                            for (index, builder) in r.builders.drain(..).enumerate() {
                                let mut output_upper = r.upper_limit.clone();
                                for capability in &caps[index + 1..] {
                                    output_upper.insert(capability.time().clone());
                                }
                                if output_upper != output_lower {
                                    let description = Description::new(
                                        output_lower.clone(),
                                        output_upper.clone(),
                                        Antichain::from_elem(T::minimum()),
                                    );
                                    let batch = builder.done(description);
                                    output.session(&caps[index]).give(batch.clone());
                                    output_writer.insert(batch, Some(caps[index].time().clone()));
                                    output_lower = output_upper;
                                }
                            }

                            // Fold the round's deferred times back into `pending`, which may
                            // already hold times staged from input that arrived during the
                            // round.
                            for (key, mut times) in std::mem::take(&mut r.deferred) {
                                let entry = pending.entry(key).or_default();
                                entry.append(&mut times);
                                entry.sort();
                                entry.dedup();
                            }

                            // Reflect observed progress. The output is sealed and compacted to
                            // the meet; each input trace is compacted physically to the
                            // round's read frontier (so a later `cursor_through` stays valid)
                            // and logically to the meet (times below the meet are final).
                            output_writer.seal(r.upper_limit.clone());
                            base_trace.set_logical_compaction(r.upper_limit.borrow());
                            base_trace.set_physical_compaction(r.read_base.borrow());
                            subtract_trace.set_logical_compaction(r.upper_limit.borrow());
                            subtract_trace.set_physical_compaction(r.read_subtract.borrow());
                            output_reader.set_logical_compaction(r.upper_limit.borrow());
                            output_reader.set_physical_compaction(r.upper_limit.borrow());
                            processed = r.upper_limit;

                            // Downgrade the persistent capabilities to the frontier of the
                            // remaining pending times. Dropping the round then releases its
                            // held capability clones, letting the output frontier advance.
                            let mut frontier = Antichain::new();
                            for times in pending.values() {
                                for t in times {
                                    frontier.insert(t.clone());
                                }
                            }
                            capabilities.downgrade(frontier.iter());

                            // If newer input advanced the received frontiers during the round,
                            // a further interval is now retirable. Re-activate to start it.
                            if upper_base.meet(&upper_subtract) != processed {
                                reactivate.activate();
                            }
                        } else {
                            // Fuel exhausted with work left. Keep the round intact and
                            // re-activate to resume it. Do NOT seal or advance `processed`:
                            // the deferred keys still owe output below `upper_limit`.
                            round = Some(r);
                            reactivate.activate();
                        }
                    }

                    output_writer.exert();
                }
            },
        )
    };

    Arranged {
        stream,
        trace: result_trace.unwrap(),
    }
}

#[cfg(test)]
mod tests {
    // The tests drive the operator with differential's stock `arrange_by_key`, which the
    // repo lint discourages in favor of the compute-crate `MzArrange` wrapper. This crate
    // has no such wrapper, and the stock arrangement is exactly what these unit tests need.
    #![allow(clippy::disallowed_methods)]

    use std::sync::{Arc, Mutex};

    use super::*;
    use differential_dataflow::AsCollection;
    use differential_dataflow::input::Input;
    use differential_dataflow::trace::implementations::ord_neu::{OrdValSpine, RcOrdValBuilder};
    use timely::dataflow::operators::capture::Extract;
    use timely::dataflow::operators::{Capture, Inspect, Probe, ToStream};

    type Spine = OrdValSpine<u64, u64, u64, isize>;
    type Builder_ = RcOrdValBuilder<u64, u64, u64, isize>;

    /// Net-of-two closure: input 0 positive, input 1 negated, keep positive residual on
    /// the empty output value `0u64`.
    fn net_positive(_k: &u64, inputs: &[&[(u64, isize)]], out: &mut Vec<(u64, isize)>) {
        let sum = |s: &[(u64, isize)]| s.iter().map(|(_v, d)| *d).sum::<isize>();
        let net = sum(inputs[0]) - sum(inputs[1]);
        if net > 0 {
            out.push((0u64, net));
        }
    }

    #[mz_ore::test]
    fn co_reduce_sum_two_inputs_static() {
        let captured = timely::execute_directly(move |worker| {
            let (mut in0, mut in1, cap) = worker.dataflow(|scope| {
                let (h0, c0) = scope.new_collection::<u64, isize>();
                let (h1, c1) = scope.new_collection::<u64, isize>();
                let a0 = c0.map(|k| (k, k)).arrange_by_key();
                let a1 = c1.map(|k| (k, k)).arrange_by_key();
                let out = co_reduce2::<
                    u64,
                    TraceAgent<Spine>,
                    TraceAgent<Spine>,
                    u64,
                    u64,
                    u64,
                    isize,
                    Builder_,
                    Spine,
                    _,
                >(a0, a1, "test", usize::MAX, net_positive);
                let cap = out.as_collection(|k, _v| *k).inner.capture();
                (h0, h1, cap)
            });
            // input 0: keys 1,2,3 each once; input 1: key 2 once, key 3 twice.
            // net: 1 -> +1 (keep), 2 -> 0 (drop), 3 -> -1 (drop). Output: {1}.
            in0.insert(1);
            in0.insert(2);
            in0.insert(3);
            in1.insert(2);
            in1.insert(3);
            in1.insert(3);
            in0.close();
            in1.close();
            cap
        });
        let mut rows: Vec<(u64, u64, isize)> = captured
            .extract()
            .into_iter()
            .flat_map(|(_t, data)| data)
            .map(|(k, _t, r)| (k, 0u64, r))
            .collect();
        rows.sort();
        assert_eq!(rows, vec![(1u64, 0u64, 1isize)]);
    }

    #[mz_ore::test]
    fn co_reduce_incremental() {
        let captured = timely::execute_directly(move |worker| {
            let (mut in0, mut in1, probe, cap) = worker.dataflow(|scope| {
                let (h0, c0) = scope.new_collection::<u64, isize>();
                let (h1, c1) = scope.new_collection::<u64, isize>();
                let a0 = c0.map(|k| (k, k)).arrange_by_key();
                let a1 = c1.map(|k| (k, k)).arrange_by_key();
                let out = co_reduce2::<
                    u64,
                    TraceAgent<Spine>,
                    TraceAgent<Spine>,
                    u64,
                    u64,
                    u64,
                    isize,
                    Builder_,
                    Spine,
                    _,
                >(a0, a1, "test", usize::MAX, net_positive);
                let coll = out.as_collection(|k, _v| *k);
                let (probe, probed) = coll.inner.probe();
                let cap = probed.capture();
                (h0, h1, probe, cap)
            });

            // Round 1 (time 0): in0={1,1}, in1={1}. net(1) = 2 - 1 = +1 -> output +1.
            in0.insert(1);
            in0.insert(1);
            in1.insert(1);
            in0.advance_to(1);
            in1.advance_to(1);
            in0.flush();
            in1.flush();
            worker.step_while(|| probe.less_than(in0.time()));

            // Round 2 (time 1): retract one in0 1. net(1) = 1 - 1 = 0 -> retract -> -1.
            in0.remove(1);
            in0.advance_to(2);
            in1.advance_to(2);
            in0.flush();
            in1.flush();
            worker.step_while(|| probe.less_than(in0.time()));

            // Round 3 (time 2): insert in0 1 once. net(1) = 2 - 1 = +1 -> output +1.
            in0.insert(1);
            in0.advance_to(3);
            in1.advance_to(3);
            in0.flush();
            in1.flush();
            worker.step_while(|| probe.less_than(in0.time()));

            in0.close();
            in1.close();
            cap
        });

        let stream: Vec<(u64, u64, isize)> = captured
            .extract()
            .into_iter()
            .flat_map(|(t, data)| data.into_iter().map(move |(k, _t, r)| (k, t, r)))
            .collect();

        // The round-2 retraction must appear in the change stream.
        assert!(
            stream.iter().any(|(k, _t, r)| *k == 1 && *r == -1),
            "expected a retraction of key 1 in the change stream, got {stream:?}"
        );

        // Consolidating over time leaves the net multiplicity +1 for key 1.
        let mut consolidated: BTreeMap<u64, isize> = BTreeMap::new();
        for (k, _t, r) in &stream {
            *consolidated.entry(*k).or_default() += *r;
        }
        consolidated.retain(|_k, r| *r != 0);
        assert_eq!(
            consolidated.into_iter().collect::<Vec<_>>(),
            vec![(1u64, 1isize)]
        );
    }

    /// Value-aware closure: output the maximum present value across all inputs, with diff
    /// `+1`. A collapsed empty-value operator could not express this.
    fn max_present(_k: &u64, inputs: &[&[(u64, isize)]], out: &mut Vec<(u64, isize)>) {
        let mut best: Option<u64> = None;
        for slice in inputs {
            for (v, d) in *slice {
                if *d > 0 {
                    best = Some(best.map_or(*v, |b| b.max(*v)));
                }
            }
        }
        if let Some(v) = best {
            out.push((v, 1));
        }
    }

    #[mz_ore::test]
    fn co_reduce_value_aware() {
        let captured = timely::execute_directly(move |worker| {
            let (mut in0, mut in1, cap) = worker.dataflow(|scope| {
                let (h0, c0) = scope.new_collection::<(u64, u64), isize>();
                let (h1, c1) = scope.new_collection::<(u64, u64), isize>();
                let a0 = c0.arrange_by_key();
                let a1 = c1.arrange_by_key();
                let out = co_reduce2::<
                    u64,
                    TraceAgent<Spine>,
                    TraceAgent<Spine>,
                    u64,
                    u64,
                    u64,
                    isize,
                    Builder_,
                    Spine,
                    _,
                >(a0, a1, "test", usize::MAX, max_present);
                // Retain the output value: the closure keys output on input values.
                let cap = out.as_collection(|k, v| (*k, *v)).inner.capture();
                (h0, h1, cap)
            });
            // key 1: in0 value 10, in1 value 20, both +1. max = 20 -> ((1, 20), +1).
            in0.insert((1, 10));
            in1.insert((1, 20));
            in0.close();
            in1.close();
            cap
        });
        let mut rows: Vec<((u64, u64), isize)> = captured
            .extract()
            .into_iter()
            .flat_map(|(_t, data)| data)
            .map(|(kv, _t, r)| (kv, r))
            .collect();
        rows.sort();
        assert_eq!(rows, vec![((1u64, 20u64), 1isize)]);
    }

    /// Echoes every input `(value, diff)` unchanged, so one key emits every distinct
    /// input value as its own output value.
    fn echo_values(_k: &u64, inputs: &[&[(u64, isize)]], out: &mut Vec<(u64, isize)>) {
        for slice in inputs {
            for (v, d) in *slice {
                out.push((*v, *d));
            }
        }
    }

    #[mz_ore::test]
    fn co_reduce_multi_value_per_key() {
        // Read the output arrangement back through its batch stream. A linear scan of a
        // batch (or `as_collection` + external consolidation) always recovers the correct
        // multiset even from a value-unsorted batch, because updates are only ever combined
        // when values compare equal, never dropped. So the corruption is invisible to a
        // summed readback. It is visible only in the batch's stored value order, which
        // downstream value seeks (binary search) and spine merges rely on being ascending.
        //
        // Batches are `Rc`-backed (neither `Send` nor `Ord`), so they cannot leave the
        // worker via `capture`/`extract`. Instead an `inspect_batch` walks each batch's
        // cursor in place and records, per key, its `(value, time, diff)` updates in stored
        // order into a shared buffer.
        type Recorded = Vec<(u64, Vec<(u64, u64, isize)>)>;
        let recorded: Arc<Mutex<Recorded>> = Arc::new(Mutex::new(Vec::new()));
        let sink = Arc::clone(&recorded);
        timely::execute_directly(move |worker| {
            worker.dataflow::<u64, _, _>(|scope| {
                // Key 1 gains a new distinct value at each of three times, in non-ascending
                // value order. Feeding all updates as one stream message makes
                // `arrange_by_key` form a single batch spanning times 0, 1, 2 under one
                // capability, so all three times retire in one round under that capability.
                // `emit_deltas` appends them to the same per-key buffer time-major
                // (30@t0, 10@t1, 20@t2), i.e. value-unsorted. Without the value sort the
                // builder writes the vals in that order, violating the batch invariant.
                let a0 = vec![
                    ((1u64, 30u64), 0, 1isize),
                    ((1u64, 10u64), 1, 1isize),
                    ((1u64, 20u64), 2, 1isize),
                ]
                .to_stream(scope)
                .as_collection()
                .arrange_by_key();
                // `co_reduce2` is two-input; the second arm carries no updates, so
                // `echo_values` echoes only `a0` and the stored-order guard still holds.
                let a1 = Vec::<((u64, u64), u64, isize)>::new()
                    .to_stream(scope)
                    .as_collection()
                    .arrange_by_key();
                let out = co_reduce2::<
                    u64,
                    TraceAgent<Spine>,
                    TraceAgent<Spine>,
                    u64,
                    u64,
                    u64,
                    isize,
                    Builder_,
                    Spine,
                    _,
                >(a0, a1, "test", usize::MAX, echo_values);
                out.stream.inspect_batch(move |_t, batches| {
                    let mut recorded = sink.lock().expect("lock poisoned");
                    for batch in batches {
                        let mut cursor = batch.cursor();
                        while cursor.key_valid(batch) {
                            let key = own_current_key::<_, u64>(&cursor, batch);
                            let mut updates: Vec<(u64, u64, isize)> = Vec::new();
                            read_key_values(&mut cursor, batch, &mut updates);
                            recorded.push((key, updates));
                            cursor.step_key(batch);
                        }
                    }
                });
            });
        });

        let recorded = Arc::try_unwrap(recorded)
            .expect("no outstanding references")
            .into_inner()
            .expect("lock poisoned");

        // The batch invariant: within a batch each key's stored values are strictly
        // ascending. The pre-fix code stores them time-major (30, 10, 20), failing this.
        for (key, updates) in &recorded {
            let mut vals_order: Vec<u64> = Vec::new();
            for (v, _t, _d) in updates {
                if vals_order.last() != Some(v) {
                    vals_order.push(*v);
                }
            }
            let mut sorted = vals_order.clone();
            sorted.sort();
            assert_eq!(
                vals_order, sorted,
                "key {key} stored values are not ascending: {vals_order:?}"
            );
        }

        // The multiset is correct regardless (see the note above), asserted for completeness.
        let mut multiset: BTreeMap<(u64, u64), isize> = BTreeMap::new();
        for (key, updates) in &recorded {
            for (v, _t, d) in updates {
                *multiset.entry((*key, *v)).or_default() += d;
            }
        }
        multiset.retain(|_kv, r| *r != 0);
        assert_eq!(
            multiset.into_iter().collect::<Vec<_>>(),
            vec![((1, 10), 1), ((1, 20), 1), ((1, 30), 1)]
        );
    }

    /// Builds a one-round dataflow with `keys` keys, all present once in input 0 only (net
    /// `+1` each), drives the single worker to completion counting worker steps, and returns
    /// the consolidated output multiset and the step count.
    ///
    /// The step count is the yielding witness. `co_reduce2` is the only operator that
    /// re-activates itself here (via its fuel path), so extra steps under a small fuel
    /// isolate its yielding. We count worker steps rather than the operator's own closure
    /// invocations because the closure is internal to `co_reduce2` and cannot be hooked
    /// without changing its signature.
    fn run_fueled_round(fuel: usize, keys: u64) -> (Vec<(u64, isize)>, usize) {
        let (captured, steps) = timely::execute_directly(move |worker| {
            let (mut in0, in1, cap) = worker.dataflow(|scope| {
                let (h0, c0) = scope.new_collection::<u64, isize>();
                let (h1, c1) = scope.new_collection::<u64, isize>();
                let a0 = c0.map(|k| (k, k)).arrange_by_key();
                let a1 = c1.map(|k| (k, k)).arrange_by_key();
                let out = co_reduce2::<
                    u64,
                    TraceAgent<Spine>,
                    TraceAgent<Spine>,
                    u64,
                    u64,
                    u64,
                    isize,
                    Builder_,
                    Spine,
                    _,
                >(a0, a1, "test", fuel, net_positive);
                let cap = out.as_collection(|k, _v| *k).inner.capture();
                (h0, h1, cap)
            });

            for k in 0..keys {
                in0.insert(k);
            }
            in0.close();
            in1.close();

            // Drive to completion, counting steps. Each `co_reduce2` self-reactivation
            // under fuel adds a step.
            let mut steps = 0usize;
            while worker.step() {
                steps += 1;
            }
            (cap, steps)
        });

        let mut consolidated: BTreeMap<u64, isize> = BTreeMap::new();
        for (_t, data) in captured.extract() {
            for (k, _t, r) in data {
                *consolidated.entry(k).or_default() += r;
            }
        }
        consolidated.retain(|_k, r| *r != 0);
        (consolidated.into_iter().collect(), steps)
    }

    #[mz_ore::test]
    fn co_reduce_fuels() {
        // Stage a dirty set far larger than the fuel budget in one round. Assert both that
        // the final output is correct and complete and that the operator yielded (took more
        // steps than the same round drained in one shot). The complete-output assertion
        // guards the invariant that a round is never sealed while any dirty key still owes
        // output: a premature seal would drop the deferred keys' `+1`s from the result.
        const KEYS: u64 = 8_000;
        const SMALL_FUEL: usize = 100;

        let expected: Vec<(u64, isize)> = (0..KEYS).map(|k| (k, 1isize)).collect();

        // Small fuel: many activations. Large fuel: the whole round drains in one activation.
        let (out_small, steps_small) = run_fueled_round(SMALL_FUEL, KEYS);
        let (out_large, steps_large) = run_fueled_round(usize::MAX, KEYS);

        // (1) Output is correct and complete under both budgets: fueling shed no work.
        assert_eq!(out_small, expected, "small-fuel output incomplete or wrong");
        assert_eq!(out_large, expected, "large-fuel output incomplete or wrong");

        // (2) The small-fuel run took strictly more worker steps, proving `co_reduce2`
        // re-activated itself to yield rather than draining the whole dirty set in one shot.
        assert!(
            steps_small > steps_large,
            "expected fueling to add worker steps: small={steps_small}, large={steps_large}"
        );
        assert!(
            steps_small > 1,
            "expected the fueled operator to take more than one activation, took {steps_small}"
        );
    }

    #[mz_ore::test]
    fn co_reduce_fuels_retraction() {
        // `co_reduce_fuels` only covers insertion under small fuel. Retraction is the case
        // that actually stresses the fuel/seal interaction: a round must not seal while any
        // fuel-deferred key still owes a retraction, or the stale `+1` from a prior round
        // would survive in the output. Stage a dirty set (KEYS/2) far larger than the fuel
        // budget for the retracting round, so it spans many activations before it seals.
        const KEYS: u64 = 8_000;
        const SMALL_FUEL: usize = 100;

        let stream: Vec<(u64, u64, isize)> = {
            let captured = timely::execute_directly(move |worker| {
                let (mut in0, mut in1, probe, cap) = worker.dataflow(|scope| {
                    let (h0, c0) = scope.new_collection::<u64, isize>();
                    let (h1, c1) = scope.new_collection::<u64, isize>();
                    let a0 = c0.map(|k| (k, k)).arrange_by_key();
                    let a1 = c1.map(|k| (k, k)).arrange_by_key();
                    let out = co_reduce2::<
                        u64,
                        TraceAgent<Spine>,
                        TraceAgent<Spine>,
                        u64,
                        u64,
                        u64,
                        isize,
                        Builder_,
                        Spine,
                        _,
                    >(a0, a1, "test", SMALL_FUEL, net_positive);
                    let coll = out.as_collection(|k, _v| *k);
                    let (probe, probed) = coll.inner.probe();
                    let cap = probed.capture();
                    (h0, h1, probe, cap)
                });

                // Round 1 (time 0): every key present once in input 0 only. net(k) = 1 for
                // all KEYS keys -> output +1 each.
                for k in 0..KEYS {
                    in0.insert(k);
                }
                in0.advance_to(1);
                in1.advance_to(1);
                in0.flush();
                in1.flush();
                worker.step_while(|| probe.less_than(in0.time()));

                // Round 2 (time 1): retract the first half of the keys by matching them on
                // input 1, netting those keys to 0 -> output -1 each. The other half is
                // untouched and stays at +1.
                for k in 0..KEYS / 2 {
                    in1.insert(k);
                }
                in0.advance_to(2);
                in1.advance_to(2);
                in0.flush();
                in1.flush();
                worker.step_while(|| probe.less_than(in0.time()));

                in0.close();
                in1.close();
                cap
            });

            captured
                .extract()
                .into_iter()
                .flat_map(|(t, data)| data.into_iter().map(move |(k, _t, r)| (k, t, r)))
                .collect()
        };

        // Every retracted key must appear as a negative diff in the change stream, not merely
        // be absent from the final consolidated output: fuel must retract explicitly, not
        // just fail to re-emit.
        let mut retracted: BTreeSet<u64> = BTreeSet::new();
        for (k, _t, r) in &stream {
            if *k < KEYS / 2 && *r < 0 {
                retracted.insert(*k);
            }
        }
        assert_eq!(
            retracted.len(),
            usize::try_from(KEYS / 2).expect("KEYS/2 fits in usize"),
            "expected every retracted key to appear as a negative diff in the change stream"
        );

        // Consolidating over time leaves exactly the surviving keys, each once.
        let mut consolidated: BTreeMap<u64, isize> = BTreeMap::new();
        for (k, _t, r) in &stream {
            *consolidated.entry(*k).or_default() += r;
        }
        consolidated.retain(|_k, r| *r != 0);
        let expected: Vec<(u64, isize)> = (KEYS / 2..KEYS).map(|k| (k, 1isize)).collect();
        assert_eq!(
            consolidated.into_iter().collect::<Vec<_>>(),
            expected,
            "final output must be exactly the surviving keys"
        );
    }
}

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

//! A variadic reduce over N homogeneous co-arranged inputs.
//!
//! [`co_reduce`] takes N arrangements sharing one trace type and key, and a per-key
//! `logic` closure, and produces a single output arrangement. It generalizes a fused
//! two-input set-difference operator along three axes: N inputs instead of two, a
//! caller closure instead of baked-in arithmetic, and value-aware output accounting
//! instead of a single collapsed empty value.
//!
//! The operator finalizes an output time only once it is complete in every input (the
//! meet of the per-input frontiers). It reads each input at its OWN frontier, not at
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
use timely::container::CapacityContainerBuilder;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::CapabilitySet;
use timely::dataflow::operators::generic::OutputBuilder;
use timely::dataflow::operators::generic::builder_rc::OperatorBuilder;
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

/// A variadic reduce over N homogeneous co-arranged inputs.
///
/// For each key present in any input, at every interesting time `t`, `logic` is called
/// with, per input `i`, the consolidated `(value, diff)` multiset of that input's updates
/// with time `<= t` (slice `inputs[i]`, in the order the `inputs` vec was passed). `logic`
/// writes the desired output `(value, diff)` multiset at `t` into its `out` argument. The
/// operator diffs desired against prior output per value and emits the minimal updates into
/// one output arrangement. An output time is finalized only once it is complete in every
/// input, i.e. `<=`-covered by the meet of all input frontiers.
#[allow(clippy::too_many_arguments)]
pub fn co_reduce<'scope, T, Tr, K, V, V2, R2, Bu, Out, L>(
    inputs: Vec<Arranged<'scope, Tr>>,
    name: &str,
    logic: L,
) -> Arranged<'scope, TraceAgent<Out>>
where
    T: Timestamp + Lattice + Ord,
    Tr: TraceReader<Time = T> + Clone + 'static,
    Tr::KeyContainer: BatchContainer<Owned = K>,
    Tr::ValContainer: BatchContainer<Owned = V>,
    K: Ord + Clone + 'static,
    V: Ord + Clone + 'static,
    V2: Ord + Clone + 'static,
    Tr::Diff: Semigroup + Clone + 'static,
    R2: Abelian + Clone + 'static,
    Out: for<'a> Trace<Key<'a> = Tr::Key<'a>, ValOwn = V2, Time = T, Diff = R2> + 'static,
    Out::KeyContainer: BatchContainer<Owned = K>,
    Out::ValContainer: BatchContainer<Owned = V2>,
    Bu: Builder<Time = T, Input = Vec<((K, V2), T, R2)>, Output = Out::Batch> + 'static,
    L: FnMut(&K, &[&[(V, Tr::Diff)]], &mut Vec<(V2, R2)>) + 'static,
{
    let scope = inputs[0].stream.scope();
    let num_inputs = inputs.len();

    // Clones of every input trace, held for the operator's lifetime: each round re-reads
    // every input through its own frontier.
    let mut traces: Vec<Tr> = inputs.iter().map(|a| a.trace.clone()).collect();

    let mut builder = OperatorBuilder::new(name.to_owned(), scope.clone());
    let operator_info = builder.operator_info();

    // The output must exist before the inputs so each input's connection to output port 0
    // is registered, which is what lets `cap.retain(0)` mint an output capability.
    let (output, stream) = builder.new_output::<Vec<Out::Batch>>();
    let mut output = OutputBuilder::<T, CapacityContainerBuilder<Vec<Out::Batch>>>::from(output);
    let mut input_handles: Vec<_> = inputs
        .iter()
        .map(|a| builder.new_input(a.stream.clone(), Pipeline))
        .collect();

    let mut result_trace = None;
    {
        let result_trace = &mut result_trace;
        let mut logic = logic;
        builder.build(move |_capabilities| {
            let logger = scope
                .worker()
                .logger_for::<differential_dataflow::logging::DifferentialEventBuilder>(
                    "differential/arrange",
                )
                .map(Into::into);

            let activator = Some(scope.activator_for(Rc::clone(&operator_info.address)));
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
            let mut uppers: Vec<Antichain<T>> = (0..num_inputs)
                .map(|_| Antichain::from_elem(T::minimum()))
                .collect();
            // The last processed frontier. Next round's lower limit.
            let mut processed = Antichain::from_elem(T::minimum());

            // Retained capabilities and staged interesting times, keyed by output key.
            let mut capabilities = CapabilitySet::<T>::new();
            let mut pending: BTreeMap<K, Vec<T>> = BTreeMap::new();

            move |frontiers: &[timely::progress::frontier::MutableAntichain<T>]| {
                let mut output = output.activate();

                // 1. Drain every input. Stage each batch's update times as interesting
                //    seeds in `pending`, capture output capabilities, and record the batch
                //    upper. Diffs and values are re-read from the input traces at process
                //    time, not staged.
                for i in 0..num_inputs {
                    input_handles[i].for_each(|cap, data: &mut Vec<Tr::Batch>| {
                        capabilities.insert(cap.retain(0));
                        for batch in data.drain(..) {
                            uppers[i].clone_from(batch.upper());
                            let mut cursor = batch.cursor();
                            stage_times(&mut cursor, &batch, &mut pending);
                        }
                    });
                }

                // Advance each received frontier through empty regions the trace knows
                // about, then incorporate the input frontier guarantee. Each `uppers[i]`
                // stays batch-aligned to input `i`, which makes `cursor_through(uppers[i])`
                // straddle-free.
                for i in 0..num_inputs {
                    traces[i].advance_upper(&mut uppers[i]);
                    let mut joined = Antichain::new();
                    antichain_join_into(
                        &uppers[i].borrow()[..],
                        &frontiers[i].frontier()[..],
                        &mut joined,
                    );
                    uppers[i] = joined;
                }

                // The processed frontier is the meet of the per-input received frontiers:
                // an output time is final only when it is final in every input. Inputs are
                // *read* at their own frontiers (`uppers[i]`), not at the meet, to avoid a
                // straddling `cursor_through`. Reading data beyond the meet is harmless: it
                // cannot affect the output at any time below the meet.
                let mut upper_limit = uppers[0].clone();
                for u in &uppers[1..] {
                    upper_limit = upper_limit.meet(u);
                }
                let lower_limit = processed.clone();

                // Retire `[lower_limit, upper_limit)` when it is non-empty.
                if upper_limit != lower_limit {
                    // Only compute if we hold a capability inside the interval; else there
                    // is nothing to output (and we could not transmit it).
                    if capabilities
                        .iter()
                        .any(|c| !upper_limit.less_equal(c.time()))
                    {
                        // Acquire each input cursor at its own frontier and the output
                        // cursor at the prior processed frontier. Each `cursor_through`
                        // returns owned storage, so no two trace borrows are held at once,
                        // which keeps self-referencing inputs (aliasing one trace) safe.
                        let mut cursors: Vec<(Tr::Cursor, Tr::Storage)> =
                            Vec::with_capacity(num_inputs);
                        for i in 0..num_inputs {
                            cursors.push(
                                traces[i]
                                    .cursor_through(uppers[i].borrow())
                                    .expect("failed to acquire input cursor"),
                            );
                        }
                        let (mut out_cur, out_stor) = output_reader
                            .cursor_through(lower_limit.borrow())
                            .expect("failed to acquire output cursor");

                        // Per-capability output builders and update buffers.
                        let cap_times: Vec<T> =
                            capabilities.iter().map(|c| c.time().clone()).collect();
                        let mut builders: Vec<Bu> =
                            (0..cap_times.len()).map(|_| Bu::new()).collect();
                        let mut cap_updates: Vec<Vec<(V2, T, R2)>> =
                            (0..cap_times.len()).map(|_| Vec::new()).collect();
                        let mut builder_buffer: Vec<((K, V2), T, R2)> = Vec::new();

                        // Dirty keys are the staged keys, iterated in ascending order so the
                        // source and output cursors advance monotonically. A key whose
                        // staged times are all beyond `upper_limit` stays pending.
                        let mut next_pending: BTreeMap<K, Vec<T>> = BTreeMap::new();
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
                            if seeds.is_empty() {
                                if !carried.is_empty() {
                                    next_pending.insert(key, carried);
                                }
                                continue;
                            }

                            let mut kw = KeyWork {
                                inputs: (0..num_inputs).map(|_| Vec::new()).collect(),
                                prior: Vec::new(),
                                seeds,
                            };
                            for i in 0..num_inputs {
                                let (cursor, storage) = &mut cursors[i];
                                if seek_owned_key(cursor, storage, &key) {
                                    read_key_values(cursor, storage, &mut kw.inputs[i]);
                                }
                            }
                            if seek_owned_key(&mut out_cur, &out_stor, &key) {
                                read_key_values(&mut out_cur, &out_stor, &mut kw.prior);
                            }

                            // Carried (beyond-upper) times remain interesting next round;
                            // `compute_key` appends any deferred synthetic times.
                            let mut new_interesting = carried;
                            compute_key(
                                &key,
                                &kw,
                                &upper_limit,
                                &mut logic,
                                &cap_times,
                                &mut cap_updates,
                                &mut new_interesting,
                            );

                            // Push this key's updates into each capability's builder,
                            // preserving ascending key order across the builder.
                            for i in 0..cap_times.len() {
                                if cap_updates[i].is_empty() {
                                    continue;
                                }
                                builder_buffer.clear();
                                for (v2, time, r2) in cap_updates[i].drain(..) {
                                    builder_buffer.push(((key.clone(), v2), time, r2));
                                }
                                builders[i].push(&mut builder_buffer);
                            }

                            if !new_interesting.is_empty() {
                                new_interesting.sort();
                                new_interesting.dedup();
                                next_pending.insert(key, new_interesting);
                            }
                        }

                        // Build and ship one batch per capability. Only one capability may
                        // accompany each message, so each batch's upper folds in the times
                        // of the later capabilities.
                        let mut output_lower = lower_limit.clone();
                        for (index, builder) in builders.drain(..).enumerate() {
                            let mut output_upper = upper_limit.clone();
                            for capability in &capabilities[index + 1..] {
                                output_upper.insert(capability.time().clone());
                            }
                            if output_upper != output_lower {
                                let description = Description::new(
                                    output_lower.clone(),
                                    output_upper.clone(),
                                    Antichain::from_elem(T::minimum()),
                                );
                                let batch = builder.done(description);
                                output.session(&capabilities[index]).give(batch.clone());
                                output_writer
                                    .insert(batch, Some(capabilities[index].time().clone()));
                                output_lower = output_upper;
                            }
                        }

                        // Downgrade capabilities to the frontier of remaining pending times.
                        pending = next_pending;
                        let mut frontier = Antichain::new();
                        for times in pending.values() {
                            for t in times {
                                frontier.insert(t.clone());
                            }
                        }
                        capabilities.downgrade(frontier.iter());
                    }

                    // Reflect observed progress. The output is sealed and compacted to the
                    // meet; each input trace is compacted physically to its own frontier (so
                    // next round's `cursor_through(uppers[i])` stays valid) and logically to
                    // the meet (times below the meet are final).
                    output_writer.seal(upper_limit.clone());
                    for i in 0..num_inputs {
                        traces[i].set_logical_compaction(upper_limit.borrow());
                        traces[i].set_physical_compaction(uppers[i].borrow());
                    }
                    output_reader.set_logical_compaction(upper_limit.borrow());
                    output_reader.set_physical_compaction(upper_limit.borrow());
                    processed = upper_limit;
                }

                output_writer.exert();
            }
        });
    }

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

    use super::*;
    use differential_dataflow::input::Input;
    use differential_dataflow::trace::implementations::ord_neu::{OrdValSpine, RcOrdValBuilder};
    use timely::dataflow::operators::capture::Extract;
    use timely::dataflow::operators::{Capture, Probe};

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
                let out =
                    co_reduce::<u64, TraceAgent<Spine>, u64, u64, u64, isize, Builder_, Spine, _>(
                        vec![a0, a1],
                        "test",
                        net_positive,
                    );
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
                let out =
                    co_reduce::<u64, TraceAgent<Spine>, u64, u64, u64, isize, Builder_, Spine, _>(
                        vec![a0, a1],
                        "test",
                        net_positive,
                    );
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
                let out =
                    co_reduce::<u64, TraceAgent<Spine>, u64, u64, u64, isize, Builder_, Spine, _>(
                        vec![a0, a1],
                        "test",
                        max_present,
                    );
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

    /// Net of three inputs: `d0 + d1 - d2`, keep positive residual on the empty value.
    fn net3_positive(_k: &u64, inputs: &[&[(u64, isize)]], out: &mut Vec<(u64, isize)>) {
        let sum = |s: &[(u64, isize)]| s.iter().map(|(_v, d)| *d).sum::<isize>();
        let net = sum(inputs[0]) + sum(inputs[1]) - sum(inputs[2]);
        if net > 0 {
            out.push((0u64, net));
        }
    }

    #[mz_ore::test]
    fn co_reduce_three_inputs() {
        let captured = timely::execute_directly(move |worker| {
            let (mut in0, mut in1, mut in2, cap) = worker.dataflow(|scope| {
                let (h0, c0) = scope.new_collection::<u64, isize>();
                let (h1, c1) = scope.new_collection::<u64, isize>();
                let (h2, c2) = scope.new_collection::<u64, isize>();
                let a0 = c0.map(|k| (k, k)).arrange_by_key();
                let a1 = c1.map(|k| (k, k)).arrange_by_key();
                let a2 = c2.map(|k| (k, k)).arrange_by_key();
                let out =
                    co_reduce::<u64, TraceAgent<Spine>, u64, u64, u64, isize, Builder_, Spine, _>(
                        vec![a0, a1, a2],
                        "test",
                        net3_positive,
                    );
                let cap = out.as_collection(|k, _v| *k).inner.capture();
                (h0, h1, h2, cap)
            });
            // key 1: in0 +1, in1 +1, in2 +1. net = 1 + 1 - 1 = +1 -> {(1, 0, +1)}.
            // key 2: in0 +1, in2 +1 twice. net = 1 + 0 - 2 = -1 -> dropped.
            in0.insert(1);
            in1.insert(1);
            in2.insert(1);
            in0.insert(2);
            in2.insert(2);
            in2.insert(2);
            in0.close();
            in1.close();
            in2.close();
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
}

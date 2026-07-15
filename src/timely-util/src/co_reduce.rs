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
//! while their trace types stay independent. Callers pass two arrangement flavors
//! (for example a local agent versus an entered trace) directly, and the operator
//! is monomorphized per pair.
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

use std::collections::BTreeMap;
use std::rc::Rc;

use differential_dataflow::difference::{Abelian, Semigroup};
use differential_dataflow::lattice::{Lattice, antichain_join_into};
use differential_dataflow::operators::arrange::{Arranged, TraceAgent};
use differential_dataflow::trace::implementations::BatchContainer;
use differential_dataflow::trace::{
    BatchCursor, BatchDiff, BatchReader, Builder, Cursor, Description, ExertionLogic, Navigable,
    Trace, TraceReader,
};
use timely::container::PushInto;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::{Capability, CapabilitySet, Operator};
use timely::progress::{Antichain, Timestamp};

mod cursor;

#[cfg(test)]
mod tests;

use cursor::{KeyWork, compute_key, read_key_values, seek_owned_key, stage_times};

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
    read_input0: Antichain<T>,
    read_input1: Antichain<T>,
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
/// `<= t`: slice `[0]` is `input0`, slice `[1]` is `input1`. `logic` writes the desired
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
    input0: Arranged<'scope, Tr1>,
    input1: Arranged<'scope, Tr2>,
    name: &str,
    fuel: usize,
    logic: L,
) -> Arranged<'scope, TraceAgent<Out>>
where
    T: Timestamp + Lattice + Ord,
    // The inputs are read only through their batch cursors, so all key, value, and diff
    // opinions are stated on `BatchCursor<Tr>` rather than on the trace itself, which in this
    // differential version carries only a time opinion. Both batches must be `Navigable` so the
    // operator can build cursors over them.
    Tr1: TraceReader<Time = T> + Clone + 'static,
    Tr1::Batch: Navigable,
    Tr2: TraceReader<Time = T> + Clone + 'static,
    Tr2::Batch: Navigable,
    BatchCursor<Tr1>: Cursor<Time = T>,
    <BatchCursor<Tr1> as Cursor>::Diff: Semigroup + Clone + 'static,
    <BatchCursor<Tr1> as Cursor>::KeyContainer: BatchContainer<Owned = K>,
    <BatchCursor<Tr1> as Cursor>::ValContainer: BatchContainer<Owned = V>,
    // `Tr2`'s diff is pinned to `Tr1`'s so both inputs feed the closure as the one input diff
    // type, and its key and value containers carry the same owned types.
    BatchCursor<Tr2>: Cursor<Time = T, Diff = BatchDiff<Tr1>>,
    <BatchCursor<Tr2> as Cursor>::KeyContainer: BatchContainer<Owned = K>,
    <BatchCursor<Tr2> as Cursor>::ValContainer: BatchContainer<Owned = V>,
    K: Ord + Clone + 'static,
    V: Ord + Clone + 'static,
    V2: Ord + Clone + 'static,
    R2: Abelian + Clone + 'static,
    Out: Trace<Time = T> + 'static,
    Out::Batch: Navigable,
    BatchCursor<Out>: Cursor<Time = T, Diff = R2, ValOwn = V2>,
    <BatchCursor<Out> as Cursor>::KeyContainer: BatchContainer<Owned = K>,
    <BatchCursor<Out> as Cursor>::ValContainer: BatchContainer<Owned = V2>,
    // `Bu::Input` is only required to be default-constructible and to accept output tuples.
    // This admits builders whose input is not a `Vec` (for example a columnar stack), which
    // a `Vec`-typed bound would reject.
    Bu: Builder<Time = T, Output = Out::Batch> + 'static,
    Bu::Input: Default + PushInto<((K, V2), T, R2)>,
    L: FnMut(&K, &[&[(V, BatchDiff<Tr1>)]], &mut Vec<(V2, R2)>) + 'static,
{
    let scope = input0.stream.scope();

    // A round must make progress, so process at least one key per activation. A zero
    // budget would otherwise defer forever without draining the dirty set.
    let fuel = fuel.max(1);

    // Clones of both input traces, held for the operator's lifetime: each round re-reads
    // both inputs through their own frontiers.
    let mut input0_trace = input0.trace.clone();
    let mut input1_trace = input1.trace.clone();

    let mut result_trace = None;
    let stream = {
        let result_trace = &mut result_trace;
        input0.stream.binary_frontier(
            input1.stream,
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
                let mut upper_input0 = Antichain::from_elem(T::minimum());
                let mut upper_input1 = Antichain::from_elem(T::minimum());
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
                            upper_input0.clone_from(batch.upper());
                            let mut cursor = batch.cursor();
                            stage_times(&mut cursor, &batch, &mut pending);
                        }
                    });
                    input2.for_each(|cap, data: &mut Vec<Tr2::Batch>| {
                        capabilities.insert(cap.retain(0));
                        for batch in data.drain(..) {
                            upper_input1.clone_from(batch.upper());
                            let mut cursor = batch.cursor();
                            stage_times(&mut cursor, &batch, &mut pending);
                        }
                    });

                    // Advance each received frontier through empty regions the trace knows
                    // about, then incorporate the input frontier guarantee. Each `upper_*`
                    // stays batch-aligned to its input, which makes
                    // `cursor_through(upper_*)` straddle-free.
                    input0_trace.advance_upper(&mut upper_input0);
                    input1_trace.advance_upper(&mut upper_input1);
                    {
                        let mut joined = Antichain::new();
                        antichain_join_into(
                            &upper_input0.borrow()[..],
                            &frontier1.frontier()[..],
                            &mut joined,
                        );
                        upper_input0 = joined;
                    }
                    {
                        let mut joined = Antichain::new();
                        antichain_join_into(
                            &upper_input1.borrow()[..],
                            &frontier2.frontier()[..],
                            &mut joined,
                        );
                        upper_input1 = joined;
                    }

                    // Step A: start a round if none is in progress. A round in progress
                    // freezes its finalization target, so it is resumed toward that target
                    // before any newer interval is considered.
                    if round.is_none() {
                        // The processed frontier is the meet of the two received frontiers:
                        // an output time is final only when it is final in both inputs.
                        // Inputs are *read* at their own frontiers (`upper_input0`,
                        // `upper_input1`), not at the meet, to avoid a straddling
                        // `cursor_through`. Reading data beyond the meet is harmless: it
                        // cannot affect the output at any time below the meet.
                        let upper_limit = upper_input0.meet(&upper_input1);
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
                                    read_input0: upper_input0.clone(),
                                    read_input1: upper_input1.clone(),
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
                                input0_trace.set_logical_compaction(upper_limit.borrow());
                                input0_trace.set_physical_compaction(upper_input0.borrow());
                                input1_trace.set_logical_compaction(upper_limit.borrow());
                                input1_trace.set_physical_compaction(upper_input1.borrow());
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
                            //
                            // NOTE: re-acquiring cursors and re-seeking every key on every
                            // activation makes each `seek_owned_key` restart from the cursor's
                            // initial position, so total seek work across a fuel-split round is
                            // O(N^2 / fuel) in the round's dirty-key count N, rather than O(N)
                            // for a single-activation round. This is dormant when the caller's
                            // fuel budget drains a round's whole dirty set in one activation, as
                            // production callers do today. If a smaller budget ever makes this
                            // bite, persist the cursors in `Round` instead of re-acquiring them
                            // here.
                            let (mut input0_src, input0_src_stor) = input0_trace
                                .cursor_through(r.read_input0.borrow())
                                .expect("failed to acquire input0 cursor");
                            let (mut input1_src, input1_src_stor) = input1_trace
                                .cursor_through(r.read_input1.borrow())
                                .expect("failed to acquire input1 cursor");
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
                                if seek_owned_key(&mut input0_src, &input0_src_stor, &key) {
                                    read_key_values(
                                        &mut input0_src,
                                        &input0_src_stor,
                                        &mut kw.inputs[0],
                                    );
                                }
                                if seek_owned_key(&mut input1_src, &input1_src_stor, &key) {
                                    read_key_values(
                                        &mut input1_src,
                                        &input1_src_stor,
                                        &mut kw.inputs[1],
                                    );
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
                            input0_trace.set_logical_compaction(r.upper_limit.borrow());
                            input0_trace.set_physical_compaction(r.read_input0.borrow());
                            input1_trace.set_logical_compaction(r.upper_limit.borrow());
                            input1_trace.set_physical_compaction(r.read_input1.borrow());
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
                            if upper_input0.meet(&upper_input1) != processed {
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

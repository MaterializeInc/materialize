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
//! The operator is split into a driver ([`co_reduce2_with_tactic`]) that owns frontiers,
//! capabilities, and trace maintenance, and a [`CoReduceTactic`] that owns per-key
//! computation. [`co_reduce2`] runs the conventional [`CursorTactic`].
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
//! atomically: the tactic accumulates processed keys' updates into per-held-time
//! builders and only builds, ships, and seals the round's batches once its dirty set
//! fully drains. Until then it holds the round's output capabilities (so the output
//! frontier stays at `processed`), leaves `processed` unadvanced, defers compaction, and
//! re-activates itself. This guarantees downstream never observes a partially emitted
//! round as complete. Bounding keys per activation caps CPU per activation without
//! changing output correctness or frontier semantics.

use std::rc::Rc;

use differential_dataflow::difference::{Abelian, Semigroup};
use differential_dataflow::lattice::{Lattice, antichain_join_into};
use differential_dataflow::operators::arrange::{Arranged, TraceAgent};
use differential_dataflow::trace::implementations::BatchContainer;
use differential_dataflow::trace::{
    BatchCursor, BatchDiff, BatchReader, Builder, Cursor, ExertionLogic, Navigable, Trace,
    TraceReader,
};
use timely::container::PushInto;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::{CapabilitySet, Operator};
use timely::progress::{Antichain, Timestamp};

mod cursor;

#[cfg(test)]
mod tests;

pub use cursor::CursorTactic;

/// A per-round engine for a two-input key-wise reduction.
///
/// The interface trades only in structural information: batch handles, frontiers, and
/// time-tagged output batches. Key and value opinions live inside implementations,
/// which state them as bounds on the batches' cursors.
///
/// # Contract
///
/// The driver ([`co_reduce2_with_tactic`]) relies on the following.
///
/// * **Ordered, tiling output.** `finish`'s `(time, batch)` pairs are in ascending order
///   and their descriptions tile `[lower, upper)`: the first batch's lower is `lower`,
///   each batch's upper is the next batch's lower, and the last batch's upper is `upper`.
///   Zero-width sub-intervals are skipped.
/// * **Shipped at a held time.** Each returned `time` is an element of the `held`
///   antichain passed to `begin`. The driver mints a capability at it.
/// * **Progress.** A `step` call with `fuel >= 1` must process at least one key or
///   return true, else the driver's reactivation loop livelocks with capabilities held.
/// * **Frontier bounds withheld work, and collapses to empty when there is none.**
///   `finish`'s returned frontier must be at-or-below every time the tactic withholds
///   for later rounds, and MUST be the empty antichain when nothing is withheld, else
///   capabilities are held forever and recursive scopes deadlock.
pub trait CoReduceTactic<B0, B1, BOut>
where
    B0: BatchReader,
    B1: BatchReader<Time = B0::Time>,
    BOut: BatchReader<Time = B0::Time>,
{
    /// Freeze a round over `[lower, upper)`.
    ///
    /// `history0`/`history1`: ALL batches of that input through its own (batch-aligned)
    /// read frontier, for cursor-building over full per-key histories. `new0`/`new1`:
    /// the batches that arrived since the last round, a subset of the corresponding
    /// history's content, used solely to stage `(key, time)` pairs into the tactic's
    /// internal pending set. The views overlap by design. `output`: prior output batches
    /// through `lower`. `held`: the times the driver holds capabilities for.
    #[allow(clippy::too_many_arguments)]
    fn begin(
        &mut self,
        history0: Vec<B0>,
        new0: Vec<B0>,
        history1: Vec<B1>,
        new1: Vec<B1>,
        output: Vec<BOut>,
        lower: &Antichain<B0::Time>,
        upper: &Antichain<B0::Time>,
        held: &Antichain<B0::Time>,
    );

    /// Process up to `fuel` dirty keys of the in-flight round. Returns true when the
    /// round's dirty set has drained.
    fn step(&mut self, fuel: usize) -> bool;

    /// Finalize the round: the round's output batches, each tagged with the held time
    /// to ship it at, plus the frontier of times the tactic withholds.
    fn finish(&mut self) -> (Vec<(B0::Time, BOut)>, Antichain<B0::Time>);
}

/// Drives a two-input key-wise reduction using a supplied [`CoReduceTactic`].
///
/// The driver does the dataflow plumbing: per-input received frontiers, the meet that
/// finalizes output times, capabilities, the fuel loop, output trace maintenance, and
/// compaction. It requires only `TraceReader` of its inputs and `Trace` of its output,
/// never `Navigable`. Building cursors over the batches it hands to `begin` is the
/// tactic's concern.
///
/// An output time is finalized only once it is complete in both inputs, at the meet of
/// the two per-input frontiers. Each input is read at its OWN frontier, not at the
/// meet: a cut at the meet can straddle a batch of an input whose frontier ran ahead.
/// Reading data beyond the meet is harmless because it cannot affect the output at any
/// time below the meet.
///
/// `fuel` bounds the keys the tactic processes per activation. The driver clamps it to
/// at least one and forwards it to every `step` call.
pub fn co_reduce2_with_tactic<'scope, T, Tr0, Tr1, Out, TAC>(
    input0: Arranged<'scope, Tr0>,
    input1: Arranged<'scope, Tr1>,
    name: &str,
    fuel: usize,
    tactic: TAC,
) -> Arranged<'scope, TraceAgent<Out>>
where
    T: Timestamp + Lattice + Ord,
    Tr0: TraceReader<Time = T> + Clone + 'static,
    Tr1: TraceReader<Time = T> + Clone + 'static,
    Out: Trace<Time = T> + 'static,
    TAC: CoReduceTactic<Tr0::Batch, Tr1::Batch, Out::Batch> + 'static,
{
    let scope = input0.stream.scope();

    // A round must make progress, so the tactic sees at least one key of budget.
    let fuel = fuel.max(1);

    // Clones of both input traces, held for the operator's lifetime: each round reads
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
                let mut tactic = tactic;
                let logger = scope
                    .worker()
                    .logger_for::<differential_dataflow::logging::DifferentialEventBuilder>(
                        "differential/arrange",
                    )
                    .map(Into::into);

                let activator = Some(scope.activator_for(Rc::clone(&operator_info.address)));
                // Separate activator the operator uses to re-schedule itself while a
                // round defers work under fuel.
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

                // Persistent per-input received frontiers. Each advances monotonically
                // and combines batch uppers, `advance_upper`, and the input frontier.
                let mut upper_input0 = Antichain::from_elem(T::minimum());
                let mut upper_input1 = Antichain::from_elem(T::minimum());
                // The last processed frontier. Next round's lower limit.
                let mut processed = Antichain::from_elem(T::minimum());

                let mut capabilities = CapabilitySet::<T>::new();

                // Batches drained but not yet handed to a round. A batch on one input
                // need not advance the meet, so its handles wait here, possibly across
                // activations, until the next round starts. `queued_times` tracks the
                // capability times covering them: the round-end downgrade folds these
                // in, since the tactic knows nothing of queued batches and its returned
                // frontier alone would release the capabilities their output needs.
                let mut queued0: Vec<Tr0::Batch> = Vec::new();
                let mut queued1: Vec<Tr1::Batch> = Vec::new();
                let mut queued_times: Antichain<T> = Antichain::new();

                // Metadata of the in-flight round, if any. The tactic holds the round's
                // working state. The driver keeps only what sealing and compaction need.
                // While a round is in flight the driver never downgrades capabilities,
                // which pins the output frontier at the round's lower limit.
                let mut round: Option<(Antichain<T>, Antichain<T>, Antichain<T>, Antichain<T>)> =
                    None;

                move |(input1_handle, frontier1), (input2_handle, frontier2), output| {
                    // Drain both inputs: capture capabilities, record batch uppers, and
                    // queue the handles for the next round.
                    input1_handle.for_each(|cap, data: &mut Vec<Tr0::Batch>| {
                        queued_times.insert(cap.time().clone());
                        capabilities.insert(cap.retain(0));
                        for batch in data.drain(..) {
                            upper_input0.clone_from(batch.upper());
                            queued0.push(batch);
                        }
                    });
                    input2_handle.for_each(|cap, data: &mut Vec<Tr1::Batch>| {
                        queued_times.insert(cap.time().clone());
                        capabilities.insert(cap.retain(0));
                        for batch in data.drain(..) {
                            upper_input1.clone_from(batch.upper());
                            queued1.push(batch);
                        }
                    });

                    // Advance each received frontier through empty regions the trace
                    // knows about, then incorporate the input frontier guarantee. Each
                    // `upper_*` stays batch-aligned to its input, which makes
                    // `batches_through(upper_*)` a clean cut.
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

                    // Start a round if none is in flight. A round in flight freezes its
                    // finalization target and is driven to completion before any newer
                    // interval is considered.
                    if round.is_none() {
                        let upper_limit = upper_input0.meet(&upper_input1);
                        if upper_limit != processed {
                            if capabilities
                                .iter()
                                .any(|c| !upper_limit.less_equal(c.time()))
                            {
                                let history0 = input0_trace
                                    .batches_through(upper_input0.borrow())
                                    .expect("failed to acquire input0 batches");
                                let history1 = input1_trace
                                    .batches_through(upper_input1.borrow())
                                    .expect("failed to acquire input1 batches");
                                let output_batches = output_reader
                                    .batches_through(processed.borrow())
                                    .expect("failed to acquire output batches");
                                let held: Antichain<T> =
                                    capabilities.iter().map(|c| c.time().clone()).collect();
                                tactic.begin(
                                    history0,
                                    std::mem::take(&mut queued0),
                                    history1,
                                    std::mem::take(&mut queued1),
                                    output_batches,
                                    &processed,
                                    &upper_limit,
                                    &held,
                                );
                                queued_times = Antichain::new();
                                round = Some((
                                    processed.clone(),
                                    upper_limit,
                                    upper_input0.clone(),
                                    upper_input1.clone(),
                                ));
                            } else {
                                // Empty region: nothing to output, but progress must
                                // still be reflected. Queued batches are not consumed,
                                // as their capability times are at or beyond the meet.
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

                    // Drive the in-flight round.
                    if let Some((lower, upper, read0, read1)) = round.take() {
                        if tactic.step(fuel) {
                            let (produced, tactic_frontier) = tactic.finish();

                            // Contract checks (see CoReduceTactic). Cheap, debug-only.
                            debug_assert!(
                                produced
                                    .iter()
                                    .all(|(time, _)| capabilities
                                        .iter()
                                        .any(|c| c.time().less_equal(time))),
                                "CoReduceTactic::finish shipped a batch at an uncovered time",
                            );
                            debug_assert!(
                                {
                                    let mut edge = lower.clone();
                                    let abutting = produced.iter().all(|(_, batch)| {
                                        let matches = batch.description().lower() == &edge;
                                        edge = batch.description().upper().clone();
                                        matches
                                    });
                                    abutting && (produced.is_empty() || edge == upper)
                                },
                                "CoReduceTactic::finish output must be ordered and tile [lower, upper)",
                            );

                            for (time, batch) in produced {
                                let cap = capabilities.delayed(&time);
                                output.session(&cap).give(batch.clone());
                                output_writer.insert(batch, Some(time));
                            }

                            // Downgrade to the tactic's withheld frontier joined with
                            // the times covering still-queued batches.
                            let mut target = tactic_frontier;
                            for time in queued_times.elements() {
                                target.insert(time.clone());
                            }
                            capabilities.downgrade(target.iter());

                            // Reflect observed progress. The output is sealed and
                            // compacted to the meet. Each input trace is compacted
                            // physically to the round's read frontier and logically to
                            // the meet.
                            output_writer.seal(upper.clone());
                            input0_trace.set_logical_compaction(upper.borrow());
                            input0_trace.set_physical_compaction(read0.borrow());
                            input1_trace.set_logical_compaction(upper.borrow());
                            input1_trace.set_physical_compaction(read1.borrow());
                            output_reader.set_logical_compaction(upper.borrow());
                            output_reader.set_physical_compaction(upper.borrow());
                            processed = upper;

                            // If newer input advanced the received frontiers during the
                            // round, a further interval is now retirable.
                            if upper_input0.meet(&upper_input1) != processed {
                                reactivate.activate();
                            }
                        } else {
                            // Fuel exhausted with work left. Keep the round and resume
                            // it on the next activation. Do NOT seal or advance
                            // `processed`: unprocessed keys still owe output below the
                            // round's upper.
                            round = Some((lower, upper, read0, read1));
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
///
/// `logic` must produce the empty output multiset when both input accumulations are
/// empty. The operator evaluates `logic` at times where all diffs have cancelled, and a
/// nonempty result there is unsound.
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
    co_reduce2_with_tactic(
        input0,
        input1,
        name,
        fuel,
        cursor::CursorTactic::<Tr1::Batch, Tr2::Batch, Out::Batch, K, V, V2, R2, Bu, L>::new(logic),
    )
}

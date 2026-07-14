// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Rendering of the fused `SetDifference` operator.
//!
//! `SetDifference { base, subtract, key }` computes, per `key`, the thresholded
//! net multiplicity `net = sum_v count_base(key, v) - sum_v count_subtract(key, v)`,
//! emitting `(key, (), net)` whenever `net > 0`. This reproduces
//! `Threshold(Union(Negate(subtract), base))` exactly: the anti-side `Threshold`
//! keys on the entire difference row, so per-key accounting equals per-row
//! accounting. The operator ignores value contents (it only sums diffs), so the
//! two input arrangements may carry different `thinning`.
//!
//! The point of the fused operator is to consume the two already-existing input
//! arrangements directly and produce the one output arrangement, without the
//! intermediate `ArrangeBy` (trace T) that the reconstructed
//! `Threshold(ArrangeBy(Union(..)))` dataflow would build.
//!
//! It is a `binary_frontier` operator that forks two differential internals:
//!
//!  * The two-input consumption skeleton of [`mz_join_core`], including the
//!    `binary_frontier` setup and per-input received-batch frontier tracking.
//!  * The output-trace machinery of differential's `reduce_trace`: an output
//!    `TraceAgent`, per-capability batch builders, `seal`, compaction, and the
//!    persisted `pending` / `CapabilitySet` interesting-times bookkeeping.
//!
//! The private `HistoryReplayer` and `ValueHistory` of `reduce_trace` are not
//! reachable, so the per-key net walk is reimplemented here. Because the output
//! value is always the empty row, differential's per-value history collapses to
//! a per-key time-indexed running count, which is much simpler.
//!
//! [`mz_join_core`]: crate::render::join::mz_join_core

use std::cmp::{Ordering, Reverse};
use std::collections::{BTreeMap, BTreeSet, BinaryHeap};
use std::rc::Rc;

use differential_dataflow::consolidation::consolidate;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::arrange::{Arranged, TraceAgent};
use differential_dataflow::trace::implementations::BatchContainer;
use differential_dataflow::trace::{
    BatchReader, Builder, Cursor, Description, ExertionLogic, Trace, TraceReader,
};
use mz_compute_types::plan::scalar::{LirScalarExpr, mfp_mir_to_lir_plan};
use mz_compute_types::plan::{ArrangementStrategy, AvailableCollections};
use mz_expr::MapFilterProject;
use mz_repr::{Diff, Row};
use mz_timely_util::columnation::ColumnationChunker;
use timely::PartialOrder;
use timely::container::PushInto;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::{CapabilitySet, Operator};
use timely::progress::Antichain;

use crate::extensions::arrange::{ArrangementSize, KeyCollection, MzArrange};
use crate::render::context::{ArrangementFlavor, CollectionBundle, Context};
use crate::render::{MaybeBucketByTime, RenderTimestamp};
use crate::typedefs::{ErrBatcher, ErrBuilder, RowRowAgent, RowRowEnter, RowRowSpine};
use mz_row_spine::RowRowBuilder;

/// One input arm, read through its existing arrangement keyed by the arm's own key.
///
/// The recognizer rewrites each arm so its rendered bundle exposes an existing arrangement
/// directly (`Get::PassArrangements` or an `ArrangeBy`), stripping any projection that would
/// otherwise render the arm as a raw collection. So the arm's arrangement lookup hits and no
/// per-arm re-arrangement is built, which is what lets the fusion eliminate the shared
/// intermediate arrangement (trace T) over the union without reintroducing one per arm.
enum ArmArrangement<'scope, T: RenderTimestamp> {
    Local(differential_dataflow::operators::arrange::Arranged<'scope, RowRowAgent<T, Diff>>),
    Trace(
        differential_dataflow::operators::arrange::Arranged<
            'scope,
            RowRowEnter<mz_repr::Timestamp, Diff, T>,
        >,
    ),
}

impl<'scope, T: RenderTimestamp + MaybeBucketByTime> Context<'scope, T> {
    /// Renders a `SetDifference` node into a single output arrangement.
    ///
    /// `base` must be available arranged on `base_key` and `subtract` on
    /// `subtract_key`, which the recognizer guarantees. Those two keys carry the
    /// same key datums as the output key (`ensure_arrangement.0`), so reading each
    /// arm through its own arrangement produces aligned keys. The output is a
    /// `Local` arrangement keyed by the output key with empty (`thinning=()`)
    /// values, matching the `Threshold` output this node replaces.
    pub(crate) fn render_set_difference(
        &self,
        base: CollectionBundle<'scope, T>,
        subtract: CollectionBundle<'scope, T>,
        base_key: Vec<LirScalarExpr>,
        subtract_key: Vec<LirScalarExpr>,
        ensure_arrangement: (Vec<LirScalarExpr>, Vec<usize>, Vec<usize>),
    ) -> CollectionBundle<'scope, T> {
        let key = ensure_arrangement.0.clone();

        // Errors from both inputs are converted to collections, concatenated, and
        // arranged once, mirroring the reduce error pattern in `render/reduce.rs`.
        let mut err_collections = Vec::with_capacity(2);

        // Read each arm through its own existing arrangement (`base_key` /
        // `subtract_key`). Their key datums align with the output key, so the
        // per-key net accounting co-iterates correctly across arms.
        let base_arm =
            self.arm_arrangement(base, &base_key, &ensure_arrangement, &mut err_collections);
        let sub_arm = self.arm_arrangement(
            subtract,
            &subtract_key,
            &ensure_arrangement,
            &mut err_collections,
        );

        // Demultiplex the four Local/Trace combinations. `set_difference_core` is
        // generic over the two trace types; both variants read `Row` keys and
        // ignore values.
        let oks = match (base_arm, sub_arm) {
            (ArmArrangement::Local(base_oks), ArmArrangement::Local(sub_oks)) => {
                set_difference_core(base_oks, sub_oks)
            }
            (ArmArrangement::Local(base_oks), ArmArrangement::Trace(sub_oks)) => {
                set_difference_core(base_oks, sub_oks)
            }
            (ArmArrangement::Trace(base_oks), ArmArrangement::Local(sub_oks)) => {
                set_difference_core(base_oks, sub_oks)
            }
            (ArmArrangement::Trace(base_oks), ArmArrangement::Trace(sub_oks)) => {
                set_difference_core(base_oks, sub_oks)
            }
        };

        let errs = differential_dataflow::collection::concatenate(self.scope, err_collections);
        let errs: KeyCollection<_, _, _> = errs.into();
        let errs = errs.mz_arrange::<ColumnationChunker<_>, ErrBatcher<_, _>, ErrBuilder<_, _>, _>(
            "Arrange SetDifference err",
        );

        CollectionBundle::from_expressions(key, ArrangementFlavor::Local(oks, errs))
    }

    /// Reads one input arm through its existing arrangement keyed by `arm_key`,
    /// pushing the arm's error collection into `errs`.
    ///
    /// The recognizer guarantees each recognized arm exposes an arrangement on its
    /// own key, so the `arm.arrangement(arm_key)` lookup hits. The `None` branch is
    /// a defensive fallback: it arranges the arm on the output `ensure_arrangement`
    /// once, mirroring the `ArrangeBy` the lowering would otherwise insert. The
    /// arrangement value is empty (`thinning=()`), which the operator ignores.
    fn arm_arrangement(
        &self,
        arm: CollectionBundle<'scope, T>,
        arm_key: &[LirScalarExpr],
        ensure_arrangement: &(Vec<LirScalarExpr>, Vec<usize>, Vec<usize>),
        errs: &mut Vec<
            differential_dataflow::VecCollection<
                'scope,
                T,
                crate::render::errors::DataflowErrorSer,
                Diff,
            >,
        >,
    ) -> ArmArrangement<'scope, T> {
        match arm.arrangement(arm_key) {
            Some(ArrangementFlavor::Local(oks, arm_errs)) => {
                errs.push(arm_errs.as_collection(|k, _v| k.clone()));
                ArmArrangement::Local(oks)
            }
            Some(ArrangementFlavor::Trace(_, oks, arm_errs)) => {
                errs.push(arm_errs.as_collection(|k, _v| k.clone()));
                ArmArrangement::Trace(oks)
            }
            None => {
                let arity = ensure_arrangement.0.len();
                let arranged = arm.ensure_collections(
                    AvailableCollections::new_arranged(vec![ensure_arrangement.clone()]),
                    None,
                    mfp_mir_to_lir_plan(MapFilterProject::new(arity)),
                    self.as_of_frontier.clone(),
                    self.until.clone(),
                    &self.config_set,
                    ArrangementStrategy::Direct,
                );
                match arranged
                    .arrangement(&ensure_arrangement.0)
                    .expect("arrangement just built on the output key")
                {
                    ArrangementFlavor::Local(oks, arm_errs) => {
                        errs.push(arm_errs.as_collection(|k, _v| k.clone()));
                        ArmArrangement::Local(oks)
                    }
                    ArrangementFlavor::Trace(..) => {
                        unreachable!("a freshly built arrangement is always `Local`")
                    }
                }
            }
        }
    }
}

/// Per-key bookkeeping assembled during a processing round.
struct KeyWork<T> {
    /// Signed input deltas: `base` contributes `+diff`, `subtract` contributes
    /// `-diff`. Merged across both inputs' source traces and incoming batches.
    deltas: Vec<(T, Diff)>,
    /// Previously produced output deltas for this key (from the output trace).
    /// Values are collapsed away; only `(time, diff)` matters for the running
    /// output multiplicity of the single empty value.
    prior: Vec<(T, Diff)>,
    /// In-region interesting seed times: batch times plus pending times that are
    /// not beyond the round's upper. Drives which times we must (re-)evaluate.
    seeds: Vec<T>,
}

impl<T> KeyWork<T> {
    fn new() -> Self {
        Self {
            deltas: Vec::new(),
            prior: Vec::new(),
            seeds: Vec::new(),
        }
    }
}

/// Reads all `(time, diff)` updates of the cursor's *current* key.
///
/// The cursor must already be positioned at the key (`get_key` returns it). The
/// value cursor is advanced to its end, so the key cursor is left on the same
/// key with values exhausted. `negate` flips the diff sign (for the `subtract`
/// input). When `record_seeds` is set, each time is also pushed to `seeds`
/// (used only while reading incoming batches, whose times seed interesting-time
/// evaluation).
fn read_current_key<C>(
    cursor: &mut C,
    storage: &C::Storage,
    negate: bool,
    out: &mut Vec<(C::Time, Diff)>,
    seeds: &mut Vec<C::Time>,
    record_seeds: bool,
) where
    C: Cursor<Diff = Diff>,
{
    while cursor.get_val(storage).is_some() {
        cursor.map_times(storage, |t, d| {
            let time = C::owned_time(t);
            let mut diff = C::owned_diff(d);
            if negate {
                diff = -diff;
            }
            if record_seeds {
                seeds.push(time.clone());
            }
            out.push((time, diff));
        });
        cursor.step_val(storage);
    }
}

/// Owns the cursor's current key as a `Row`.
fn own_current_key<C>(cursor: &C, storage: &C::Storage) -> Row
where
    C: Cursor,
    C::KeyContainer: BatchContainer<Owned = Row>,
{
    <C::KeyContainer as BatchContainer>::into_owned(cursor.get_key(storage).expect("key exists"))
}

/// Records every update time of every key in the cursor into `pending`.
///
/// Used to stage incoming batch times as interesting seeds. Times accumulate in
/// `pending` and survive rounds in which the combined frontier does not advance,
/// so a batch that arrives while the other input lags is not lost. Diffs are not
/// staged: the per-key net is recomputed from the input traces at process time.
fn stage_times<C>(cursor: &mut C, storage: &C::Storage, pending: &mut BTreeMap<Row, Vec<C::Time>>)
where
    C: Cursor,
    C::KeyContainer: BatchContainer<Owned = Row>,
{
    while cursor.key_valid(storage) {
        let key = own_current_key(cursor, storage);
        let times = pending.entry(key).or_default();
        while cursor.val_valid(storage) {
            cursor.map_times(storage, |t, _d| times.push(C::owned_time(t)));
            cursor.step_val(storage);
        }
        cursor.step_key(storage);
    }
}

/// Advances the cursor forward to `target`, returning whether it is present.
///
/// Keys are visited in ascending order across a round, so advancing each source
/// cursor forward to successive targets is monotone. On a match the cursor is
/// left positioned at the key (values at their start); on a miss it stops at the
/// first key greater than `target`, ready for the next (larger) target.
fn seek_owned_key<C>(cursor: &mut C, storage: &C::Storage, target: &Row) -> bool
where
    C: Cursor,
    C::KeyContainer: BatchContainer<Owned = Row>,
{
    while cursor.key_valid(storage) {
        match own_current_key(cursor, storage).cmp(target) {
            Ordering::Less => cursor.step_key(storage),
            Ordering::Equal => return true,
            Ordering::Greater => return false,
        }
    }
    false
}

/// Evaluates the thresholded net for one key over the round's interesting times.
///
/// Walks interesting times in a linear extension of the time order (ascending by
/// `Ord`, which refines `PartialOrder`), maintaining the running net and diffing
/// against previously produced output to emit the minimal set of output updates.
///
/// Synthetic interesting times: the threshold is non-linear in the net, so the
/// output can change at any join of input times, even where neither input has a
/// literal update. Seeding from `seeds` (batch + pending times) and repeatedly
/// joining each processed time with every input time reaches the full needed
/// join-closure. Joins that land at or beyond `upper_limit` cannot be finalized
/// this round and are deferred into `new_interesting` (they become pending). For
/// a totally ordered time this synthesis is a no-op, since a join is one of its
/// operands and is already an interesting time.
fn compute_key<T: RenderTimestamp>(
    kw: &mut KeyWork<T>,
    upper_limit: &Antichain<T>,
    empty_val: &Row,
    cap_times: &[T],
    cap_updates: &mut [Vec<(Row, T, Diff)>],
    new_interesting: &mut Vec<T>,
) {
    // Consolidate signed input and prior output by time.
    consolidate(&mut kw.deltas);
    consolidate(&mut kw.prior);

    // Times we join against to synthesize interesting times. Input and prior
    // output times both lie in the join-closure of input times; including prior
    // is redundant but harmless.
    let mut partners: Vec<T> = Vec::new();
    partners.extend(kw.deltas.iter().map(|(t, _)| t.clone()));
    partners.extend(kw.prior.iter().map(|(t, _)| t.clone()));
    partners.extend(kw.seeds.iter().cloned());
    partners.sort();
    partners.dedup();

    // Worklist of in-region interesting times, popped in ascending order.
    let mut queued: BTreeSet<T> = BTreeSet::new();
    let mut worklist: BinaryHeap<Reverse<T>> = BinaryHeap::new();
    for t in &kw.seeds {
        // Seeds are already in-region (callers filter batch/pending times), but
        // guard defensively so a beyond-upper seed is deferred, not processed.
        if upper_limit.less_equal(t) {
            new_interesting.push(t.clone());
        } else if queued.insert(t.clone()) {
            worklist.push(Reverse(t.clone()));
        }
    }

    // Output deltas produced this round for this key, used to diff against.
    let mut produced: Vec<(T, Diff)> = Vec::new();

    while let Some(Reverse(t)) = worklist.pop() {
        // Net input multiplicity as of `t` = sum of all input diffs at times <= t.
        let mut net = Diff::ZERO;
        for (ti, di) in &kw.deltas {
            if PartialOrder::less_equal(ti, &t) {
                net += *di;
            }
        }
        // The threshold: keep the positive residual multiplicity, else nothing.
        let desired = if net.is_positive() { net } else { Diff::ZERO };

        // Current output multiplicity as of `t` = prior + already-produced.
        let mut current = Diff::ZERO;
        for (ti, di) in &kw.prior {
            if PartialOrder::less_equal(ti, &t) {
                current += *di;
            }
        }
        for (ti, di) in &produced {
            if PartialOrder::less_equal(ti, &t) {
                current += *di;
            }
        }

        let delta = desired - current;
        if !delta.is_zero() {
            produced.push((t.clone(), delta));
            // Assign to the latest capability that covers `t`. Such a capability
            // must exist: `t` is reachable from a batch or a pending time, both of
            // which retain a capability at or below their time.
            let idx = cap_times
                .iter()
                .enumerate()
                .rev()
                .find(|(_, ct)| PartialOrder::less_equal(*ct, &t))
                .map(|(i, _)| i)
                .expect("a capability covers every produced time");
            cap_updates[idx].push((empty_val.clone(), t.clone(), delta));
        }

        // Synthesize joins of `t` with every input time not already <= t.
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

/// The fused set-difference operator over the two input `oks` arrangements.
///
/// Produces a `Local` output arrangement keyed by the difference key with empty
/// values. See the module docs for the overall shape.
fn set_difference_core<'scope, T, Tr1, Tr2>(
    base: Arranged<'scope, Tr1>,
    subtract: Arranged<'scope, Tr2>,
) -> Arranged<'scope, RowRowAgent<T, Diff>>
where
    T: RenderTimestamp,
    Tr1: TraceReader<Time = T, Diff = Diff> + Clone + 'static,
    Tr1::KeyContainer: BatchContainer<Owned = Row>,
    Tr2: for<'a> TraceReader<Key<'a> = Tr1::Key<'a>, Time = T, Diff = Diff> + Clone + 'static,
    Tr2::KeyContainer: BatchContainer<Owned = Row>,
{
    let scope = base.stream.scope();
    // Hold clones of both input traces for the operator's lifetime. We never
    // drop them (unlike the join, which may): we always need to re-read both
    // inputs through a shared frontier `P` each round.
    let mut base_trace = base.trace.clone();
    let mut subtract_trace = subtract.trace.clone();

    let mut result_trace = None;

    // The output `TraceAgent` reader is created inside the operator constructor,
    // so we thread a `&mut` out through it (as `reduce_trace` does) rather than
    // move the `Option` into the `'static` per-activation closure.
    let stream = {
        let result_trace = &mut result_trace;
        base.stream.binary_frontier(
            subtract.stream,
            Pipeline,
            Pipeline,
            "SetDifference",
            move |_capability, operator_info| {
                let logger = scope
                    .worker()
                    .logger_for::<differential_dataflow::logging::DifferentialEventBuilder>(
                        "differential/arrange",
                    )
                    .map(Into::into);

                let activator =
                    Some(scope.activator_for(std::rc::Rc::clone(&operator_info.address)));
                let mut empty = <RowRowSpine<T, Diff> as Trace>::new(
                    operator_info.clone(),
                    logger.clone(),
                    activator,
                );
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

                // Empty output value shared by every produced record (`thinning=()`).
                let empty_val = Row::default();

                // Persistent per-input received frontiers. Each advances monotonically
                // and combines batch uppers, `advance_upper`, and the input frontier,
                // exactly as `reduce_trace` computes its single `upper_limit`.
                let mut upper_base = Antichain::from_elem(T::minimum());
                let mut upper_subtract = Antichain::from_elem(T::minimum());
                // The last processed frontier `P`. Next round's lower limit.
                let mut processed = Antichain::from_elem(T::minimum());

                // Retained capabilities and outstanding synthetic/deferred interesting
                // times, keyed by row. The capability set is downgraded only to the
                // frontier of pending times, so we never release a capability we still
                // need to output at.
                let mut capabilities = CapabilitySet::<T>::new();
                let mut pending: BTreeMap<Row, Vec<T>> = BTreeMap::new();

                move |(input1, frontier1), (input2, frontier2), output| {
                    // 1. Drain both inputs. Stage each batch's update times as
                    //    interesting seeds in `pending` (accumulating across rounds),
                    //    capture output capabilities, and advance the received
                    //    frontiers. Diffs are not staged: the per-key net is read from
                    //    the input traces at process time.
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

                    // Advance received frontiers through empty regions the traces know
                    // about but that arrive as no batch, then incorporate the input
                    // frontier guarantees (as `reduce_trace` does). Each `upper_i` stays
                    // batch-aligned to input `i`, and is >= every batch upper of input
                    // `i`, which is what makes `cursor_through(upper_i)` straddle-free.
                    base_trace.advance_upper(&mut upper_base);
                    subtract_trace.advance_upper(&mut upper_subtract);
                    {
                        let mut joined = Antichain::new();
                        differential_dataflow::lattice::antichain_join_into(
                            &upper_base.borrow()[..],
                            &frontier1.frontier()[..],
                            &mut joined,
                        );
                        upper_base = joined;
                    }
                    {
                        let mut joined = Antichain::new();
                        differential_dataflow::lattice::antichain_join_into(
                            &upper_subtract.borrow()[..],
                            &frontier2.frontier()[..],
                            &mut joined,
                        );
                        upper_subtract = joined;
                    }

                    // The combined processed frontier `P` is the meet of the two
                    // received frontiers: an output time is final only when it is final
                    // in *both* inputs. `P.less_equal(t)` iff `upper_base.less_equal(t)
                    // || upper_subtract.less_equal(t)`. `P` alone governs which output
                    // times we finalize this round, the output `seal`, and the output
                    // trace compaction.
                    //
                    // The two inputs are *read* at their own frontiers (`upper_base`,
                    // `upper_subtract`), not at `P`. `cursor_through(P)` would panic with
                    // "upper straddles batch" whenever `P` fell inside a batch of the
                    // input whose frontier ran ahead. Reading each input at its own
                    // frontier is straddle-free, and reading data beyond `P` is harmless:
                    // it cannot affect the net at any time below `P`.
                    let upper_limit = upper_base.meet(&upper_subtract);
                    let lower_limit = processed.clone();

                    // Retire `[lower_limit, upper_limit)` when it is non-empty.
                    if upper_limit != lower_limit {
                        // Only compute if we hold a capability inside the interval; else
                        // there is nothing to output (and we could not transmit it).
                        if capabilities
                            .iter()
                            .any(|c| !upper_limit.less_equal(c.time()))
                        {
                            // Acquire input cursors at each input's own frontier and the
                            // output cursor at the prior `P`. We acquire each sequentially
                            // and `cursor_through` returns owned storage, so no two trace
                            // borrows are held at once. This is what makes self-difference
                            // (both handles aliasing one trace) safe.
                            let (mut base_src, base_src_stor) = base_trace
                                .cursor_through(upper_base.borrow())
                                .expect("failed to acquire base cursor");
                            let (mut sub_src, sub_src_stor) = subtract_trace
                                .cursor_through(upper_subtract.borrow())
                                .expect("failed to acquire subtract cursor");
                            let (mut out_cur, out_stor) = output_reader
                                .cursor_through(lower_limit.borrow())
                                .expect("failed to acquire output cursor");

                            // Per-capability output builders and reusable update buffers.
                            let cap_times: Vec<T> =
                                capabilities.iter().map(|c| c.time().clone()).collect();
                            let mut builders: Vec<RowRowBuilder<T, Diff>> =
                                (0..cap_times.len()).map(|_| Builder::new()).collect();
                            let mut cap_updates: Vec<Vec<(Row, T, Diff)>> =
                                (0..cap_times.len()).map(|_| Vec::new()).collect();
                            let mut builder_buffer =
                                <RowRowBuilder<T, Diff> as Builder>::Input::default();
                            let mut dummy_seeds: Vec<T> = Vec::new();

                            // Dirty keys are the staged (batch/pending) keys, iterated in
                            // ascending order so the source and output cursors advance
                            // monotonically. A key whose staged times are all beyond `P`
                            // is not evaluated this round but stays pending.
                            let mut next_pending: BTreeMap<Row, Vec<T>> = BTreeMap::new();
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

                                // Read the key's full signed net history from both input
                                // traces (base `+diff`, subtract `-diff`) and its prior
                                // output. Data beyond `P` is included but only affects
                                // times beyond `P`, which we do not finalize here.
                                let mut kw = KeyWork::new();
                                kw.seeds = seeds;
                                if seek_owned_key(&mut base_src, &base_src_stor, &key) {
                                    read_current_key(
                                        &mut base_src,
                                        &base_src_stor,
                                        false,
                                        &mut kw.deltas,
                                        &mut dummy_seeds,
                                        false,
                                    );
                                }
                                if seek_owned_key(&mut sub_src, &sub_src_stor, &key) {
                                    read_current_key(
                                        &mut sub_src,
                                        &sub_src_stor,
                                        true,
                                        &mut kw.deltas,
                                        &mut dummy_seeds,
                                        false,
                                    );
                                }
                                if seek_owned_key(&mut out_cur, &out_stor, &key) {
                                    read_current_key(
                                        &mut out_cur,
                                        &out_stor,
                                        false,
                                        &mut kw.prior,
                                        &mut dummy_seeds,
                                        false,
                                    );
                                }

                                // Carried (beyond-`P`) times remain interesting next round;
                                // `compute_key` appends any deferred synthetic times.
                                let mut new_interesting = carried;
                                compute_key(
                                    &mut kw,
                                    &upper_limit,
                                    &empty_val,
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
                                    for (val, time, diff) in cap_updates[i].drain(..) {
                                        builder_buffer.push_into(((key.clone(), val), time, diff));
                                    }
                                    builders[i].push(&mut builder_buffer);
                                }

                                if !new_interesting.is_empty() {
                                    new_interesting.sort();
                                    new_interesting.dedup();
                                    next_pending.insert(key, new_interesting);
                                }
                            }

                            // Build and ship one batch per capability. Only one
                            // capability may accompany each message, so each batch's
                            // upper folds in the times of the later capabilities.
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
                                    output.session(&capabilities[index]).give(Rc::clone(&batch));
                                    output_writer
                                        .insert(batch, Some(capabilities[index].time().clone()));
                                    output_lower = output_upper;
                                }
                            }

                            // Downgrade capabilities to the frontier of remaining pending
                            // times. Times below `upper_limit` were shipped this round; we
                            // only need to retain capabilities for deferred (pending) work.
                            pending = next_pending;
                            let mut frontier = Antichain::new();
                            for times in pending.values() {
                                for t in times {
                                    frontier.insert(t.clone());
                                }
                            }
                            capabilities.downgrade(frontier.iter());
                        }

                        // Reflect observed progress. The output is sealed and compacted
                        // to `P`; each input trace is compacted physically to its own
                        // frontier (so next round's `cursor_through(upper_i)` stays
                        // valid) and logically to `P` (times below `P` are final).
                        output_writer.seal(upper_limit.clone());
                        base_trace.set_logical_compaction(upper_limit.borrow());
                        base_trace.set_physical_compaction(upper_base.borrow());
                        subtract_trace.set_logical_compaction(upper_limit.borrow());
                        subtract_trace.set_physical_compaction(upper_subtract.borrow());
                        output_reader.set_logical_compaction(upper_limit.borrow());
                        output_reader.set_physical_compaction(upper_limit.borrow());
                        processed = upper_limit;
                    }

                    // Exert trace maintenance if requested.
                    output_writer.exert();
                }
            },
        )
    };

    Arranged {
        stream,
        trace: result_trace.unwrap(),
    }
    .log_arrangement_size()
}

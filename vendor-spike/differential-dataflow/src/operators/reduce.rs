//! Applies a reduction function on records grouped by key.
//!
//! The `reduce` operator acts on `(key, val)` data.
//! Records with the same key are grouped together, and a user-supplied reduction function is applied
//! to the key and the list of values.
//! The function is expected to populate a list of output values.
//!
//! The output can change at times that are joins of input times, not only at input times themselves,
//! and the operator must determine at which times to re-evaluate the reduction. A machine-checked
//! account of which times suffice lives in `formal/Differential/Coverage.lean`.

use crate::Data;

use std::marker::PhantomData;

use timely::progress::frontier::Antichain;
use timely::progress::Timestamp;
use timely::dataflow::operators::Operator;
use timely::dataflow::operators::CapabilitySet;
use timely::dataflow::channels::pact::Pipeline;

use crate::operators::arrange::{Arranged, TraceAgent};
use crate::trace::{BatchCursor, BatchDiff, BatchKey, BatchReader, BatchVal, BatchValOwn, Builder, Cursor, Description, ExertionLogic, Navigable, Trace, TraceReader};
use crate::trace::cursor::cursor_list;
use crate::trace::implementations::containers::BatchContainer;

/// Sort and deduplicate a list. Shared by the cursor and reference tactics (via their
/// `use super::*`) and the proxy tactic (`crate::operators::reduce::sort_dedup`), which
/// each previously carried an identical copy.
#[inline(never)]
pub(crate) fn sort_dedup<T: Ord>(list: &mut Vec<T>) {
    list.dedup();
    list.sort();
    list.dedup();
}

/// A type that resolves a key-wise reduction over batches arriving on the input.
///
/// Unlike join, reduce does not suspend: its output is at most linear in its input, so a single
/// `retire` runs the whole `[lower, upper)` interval to completion rather than yielding under a fuel
/// budget.
pub trait ReduceTactic<B1: BatchReader, B2: BatchReader<Time = B1::Time>> {
    /// Retire the interval `[lower, upper)`, producing the output batches it informs.
    ///
    /// It is presented with the pre-existing input batches and output batches (those before `lower`),
    /// the new input batches, and `held`: the times the operator currently holds capabilities for. It
    /// reasons only about times, returning the output batches to ship — each tagged with the time at
    /// which to ship it — and the new frontier of interesting times for the operator to hold.
    ///
    /// # Contract
    ///
    /// The driver ([`reduce_with_tactic`]) relies on the following; the first two are cheap to check
    /// and are `debug_assert!`ed there.
    ///
    /// * **Ordered, tiling output.** The returned `(time, batch)` pairs are in ascending order and
    ///   their descriptions *tile* `[lower, upper)`: the first batch's lower is `lower`, each batch's
    ///   upper is the next batch's lower, and the last batch's upper is `upper` — no gaps, no overlaps.
    ///   Sub-intervals with no updates are skipped; the next batch's lower simply picks up where the
    ///   last left off. Producing *in order* is a requirement, not a convenience — it is what lets the
    ///   driver check the tiling with a single linear scan.
    /// * **Shipped at a held time.** Each batch's `time` tag is an element of `held`; the driver mints
    ///   a capability at it, which is only valid for a held time.
    /// * **Frontier bounds withheld work, and collapses to empty when there is none.** The returned
    ///   frontier must be at-or-below every time the tactic defers, so the driver knows what is safe to
    ///   release. In particular, with no work to defer it must be the *empty* antichain. Derive it from
    ///   the actual withheld set rather than constructing it and this holds for free; returning a
    ///   non-empty frontier with nothing pending holds capabilities forever and **deadlocks recursive
    ///   scopes**. (Not driver-checkable — the withheld set is tactic-internal — so tactics self-enforce.)
    fn retire(
        &mut self,
        source_batches: Vec<B1>,
        output_batches: Vec<B2>,
        input_batches: Vec<B1>,
        lower: &Antichain<B1::Time>,
        upper: &Antichain<B1::Time>,
        held: &Antichain<B1::Time>,
    ) -> (Vec<(B1::Time, B2)>, Antichain<B1::Time>);
}

/// A key-wise reduction of values in an input trace.
///
/// This method exists to provide reduce functionality without opinions about qualifying trace types.
///
/// The `logic` closure is expected to take a key, accumulated input, and tentative accumulated output,
/// and populate its final argument with whatever it feels to be appopriate updates. The behavior and
/// correctness of the implementation rely on this making sense, and e.g. ideally the updates would if
/// applied to the tentative output bring it in line with some function applied to the input.
///
/// The `push` closure is expected to clear its first argument, then populate it with the key and drain
/// the value updates, as appropriate for the container. It is critical that it clear the container as
/// the operator has no ability to do this otherwise, and failing to do so represents a leak from one
/// key's computation to another, and will likely introduce non-determinism.
pub fn reduce_trace<'scope, Tr1, Bu, Tr2, KC, L, P>(trace: Arranged<'scope, Tr1>, name: &str, logic: L, push: P) -> Arranged<'scope, TraceAgent<Tr2>>
where
    Tr1: TraceReader<Batch: Navigable> + 'static,
    Tr2: Trace<Batch: Navigable, Time = Tr1::Time> + 'static,
    KC: BatchContainer,
    BatchCursor<Tr1>: Cursor<Time = Tr1::Time, KeyContainer = KC>,
    for<'a> BatchCursor<Tr1>: Cursor<Key<'a> = KC::ReadItem<'a>>,
    for<'a> BatchCursor<Tr2>: Cursor<Key<'a> = KC::ReadItem<'a>, ValOwn: Data, Time = Tr2::Time>,
    Bu: Builder<Time=Tr2::Time, Output = Tr2::Batch, Input: Default> + 'static,
    L: FnMut(KC::ReadItem<'_>, &[(BatchVal<'_, Tr1>, BatchDiff<Tr1>)], &mut Vec<(BatchValOwn<Tr2>, BatchDiff<Tr2>)>, &mut Vec<(BatchValOwn<Tr2>, BatchDiff<Tr2>)>)+'static,
    P: FnMut(&mut Bu::Input, KC::ReadItem<'_>, &mut Vec<(BatchValOwn<Tr2>, Tr2::Time, BatchDiff<Tr2>)>) + 'static,
{
    reduce_with_tactic(trace, name, cursors::CursorTactic::<Tr1::Batch, Tr2::Batch, Bu, L, P>::new(logic, push))
}

// The model-derived reference tactic and its entry point live in `mod reference`; re-exported here
// (doc-hidden) as the sole public handle for its differential and oracle tests.
#[doc(hidden)]
pub use reference::reduce_trace_reference;

/// Drives a key-wise reduction using a supplied [`ReduceTactic`].
///
/// This is the general reduce operator: it does the dataflow plumbing (frontiers, capabilities, output
/// trace maintenance) and routes the per-interval work through the tactic. It requires only
/// `TraceReader` of its input and `Trace` of its output, never `Navigable`: it extracts batches via
/// `batches_through`, and building cursors over them (if that is how the reduce proceeds) is the
/// tactic's concern.
pub fn reduce_with_tactic<'scope, Tr1, Tr2, T>(trace: Arranged<'scope, Tr1>, name: &str, mut tactic: T) -> Arranged<'scope, TraceAgent<Tr2>>
where
    Tr1: TraceReader + 'static,
    Tr2: Trace<Time = Tr1::Time> + 'static,
    T: ReduceTactic<Tr1::Batch, Tr2::Batch> + 'static,
{
    let mut result_trace = None;

    // fabricate a data-parallel operator using the `unary_notify` pattern.
    let stream = {

        let mut source_trace = trace.trace;
        let result_trace = &mut result_trace;
        let scope = trace.stream.scope();
        trace.stream.unary_frontier(Pipeline, name, move |_capability, operator_info| {

            // Acquire a logger for arrange events.
            let logger = scope.worker().logger_for::<crate::logging::DifferentialEventBuilder>("differential/arrange").map(Into::into);

            let activator = Some(scope.activator_for(std::rc::Rc::clone(&operator_info.address)));
            let mut empty = Tr2::new(operator_info.clone(), logger.clone(), activator);
            // If there is default exert logic set, install it.
            if let Some(exert_logic) = scope.worker().config().get::<ExertionLogic>("differential/default_exert_logic").cloned() {
                empty.set_exert_logic(exert_logic);
            }

            let (mut output_reader, mut output_writer) = TraceAgent::new(empty, operator_info, logger);

            *result_trace = Some(output_reader.clone());

            // Capabilities for the lower envelope of the interesting times the operator holds.
            let mut capabilities = CapabilitySet::<Tr1::Time>::default();

            // Upper and lower frontiers for the pending input and output batches to process.
            let mut upper_limit = Antichain::from_elem(<Tr1::Time as Timestamp>::minimum());
            let mut lower_limit = Antichain::from_elem(<Tr1::Time as Timestamp>::minimum());

            move |(input, frontier), output| {

                // The operator receives input batches, which it treats as contiguous and will collect and
                // then process as one batch. It captures the input frontier from the batches, from the upstream
                // trace, and from the input frontier, and retires the work through that interval.
                //
                // Reduce may retain capabilities and need to perform work and produce output at times that
                // may not be seen in its input. The standard example is that updates at `(0, 1)` and `(1, 0)`
                // may result in outputs at `(1, 1)` as well, even with no input at that time.

                let mut batch_storage = Vec::new();

                // Downgrade previous upper limit to be current lower limit.
                lower_limit.clear();
                lower_limit.extend(upper_limit.borrow().iter().cloned());

                // Drain input batches in order, capturing capabilities and the last upper.
                input.for_each(|capability, batches| {
                    capabilities.insert(capability.retain(0));
                    for batch in batches.drain(..) {
                        upper_limit.clone_from(batch.upper());
                        batch_storage.push(batch);
                    }
                });

                // Pull in any subsequent empty batches we believe to exist.
                source_trace.advance_upper(&mut upper_limit);
                // Incorporate the input frontier guarantees as well.
                let mut joined = Antichain::new();
                crate::lattice::antichain_join_into(&upper_limit.borrow()[..], &frontier.frontier()[..], &mut joined);
                upper_limit = joined;

                // We plan to retire the interval [lower_limit, upper_limit), which should be non-empty to proceed.
                if upper_limit != lower_limit {

                    // Acquire the pre-existing input and output batches preceding the interval. Batch handles
                    // are cheap to clone, so we fetch them whether or not the tactic finds work to do.
                    let source_batches = source_trace.batches_through(lower_limit.borrow()).expect("failed to acquire source batches");
                    let output_batches = output_reader.batches_through(lower_limit.borrow()).expect("failed to acquire output batches");

                    // The times the operator currently holds capabilities for, as an antichain.
                    let held: Antichain<Tr1::Time> = capabilities.iter().map(|c| c.time().clone()).collect();

                    // Retire the interval. The tactic reasons only about times: it returns output batches
                    // each tagged with the time to ship it at, and the new frontier of interesting times.
                    let (produced, new_frontier) = tactic.retire(source_batches, output_batches, batch_storage, &lower_limit, &upper_limit, &held);

                    // Contract checks (see `ReduceTactic::retire`). Cheap, debug-only.
                    debug_assert!(
                        produced.iter().all(|(time, _)| held.elements().contains(time)),
                        "ReduceTactic::retire shipped a batch at a time not held as a capability",
                    );
                    debug_assert!(
                        {
                            // Ordered output makes tiling a single linear scan: each description's lower
                            // must meet the previous upper (starting at `lower_limit`), ending at `upper_limit`.
                            let mut edge = lower_limit.clone();
                            let abutting = produced.iter().all(|(_, batch)| {
                                let matches = batch.description().lower() == &edge;
                                edge.clone_from(batch.description().upper());
                                matches
                            });
                            abutting && (produced.is_empty() || edge == upper_limit)
                        },
                        "ReduceTactic::retire output must be ordered and tile [lower, upper)",
                    );

                    // Ship each batch at a capability minted from the set at its time, and commit it to the
                    // output trace. The times are elements of `held`, so they stay valid until we downgrade.
                    for (time, batch) in produced {
                        let capability = capabilities.delayed(&time);
                        output.session(&capability).give(batch.clone());
                        output_writer.insert(batch, Some(time));
                    }

                    // Downgrade to the frontier the tactic handed back (a no-op when it found no work).
                    capabilities.downgrade(new_frontier);

                    // ensure that observed progress is reflected in the output.
                    output_writer.seal(upper_limit.clone());

                    // We only anticipate future times in advance of `upper_limit`.
                    source_trace.set_logical_compaction(upper_limit.borrow());
                    output_reader.set_logical_compaction(upper_limit.borrow());

                    // We will only slice the data between future batches.
                    source_trace.set_physical_compaction(upper_limit.borrow());
                    output_reader.set_physical_compaction(upper_limit.borrow());
                }

                // Exert trace maintenance if we have been so requested.
                output_writer.exert();
            }
        }
    )
    };

    Arranged { stream, trace: result_trace.unwrap() }
}

/// The conventional cursor-based [`ReduceTactic`].
///
/// It builds a [`CursorList`](crate::trace::cursor::CursorList) over the input, output, and new-batch
/// updates and replays them together per key, applying `logic` and shaping output with `push`. It holds
/// the outstanding synthetic interesting `(key, time)` moments across activations, and reasons only
/// about times: capabilities are the driver's concern.
mod cursors {

    use super::*;

    /// The conventional cursor-based [`ReduceTactic`].
    pub struct CursorTactic<B1, B2, Bu, L, P>
    where
        B1: BatchReader + Navigable,
        B2: BatchReader<Time = B1::Time> + Navigable,
        B1::Cursor: Cursor<Time = B1::Time>,
        for<'a> B2::Cursor: Cursor<Key<'a> = <B1::Cursor as Cursor>::Key<'a>, ValOwn: Data, Time = B1::Time>,
    {
        logic: L,
        push: P,
        // Outstanding `(key, time)` synthetic interesting moments, sorted by `(key, time)`, and the
        // buffers into which we assemble the next round's moments.
        pending_keys: <B1::Cursor as Cursor>::KeyContainer,
        pending_time: <B1::Cursor as Cursor>::TimeContainer,
        next_pending_keys: <B1::Cursor as Cursor>::KeyContainer,
        next_pending_time: <B1::Cursor as Cursor>::TimeContainer,
        // Buffers reused across activations.
        interesting_times: Vec<B1::Time>,
        new_interesting_times: Vec<B1::Time>,
        // Output batches may need to be built piecemeal, and these temp storage help there.
        output_upper: Antichain<B1::Time>,
        output_lower: Antichain<B1::Time>,
        _marker: PhantomData<(B2, Bu)>,
    }

    impl<B1, B2, Bu, L, P> CursorTactic<B1, B2, Bu, L, P>
    where
        B1: BatchReader + Navigable,
        B2: BatchReader<Time = B1::Time> + Navigable,
        B1::Cursor: Cursor<Time = B1::Time>,
        for<'a> B2::Cursor: Cursor<Key<'a> = <B1::Cursor as Cursor>::Key<'a>, ValOwn: Data, Time = B1::Time>,
    {
        /// Construct a tactic that applies `logic` to each key and shapes output with `push`.
        pub fn new(logic: L, push: P) -> Self {
            CursorTactic {
                logic,
                push,
                pending_keys: <B1::Cursor as Cursor>::KeyContainer::with_capacity(0),
                pending_time: <B1::Cursor as Cursor>::TimeContainer::with_capacity(0),
                next_pending_keys: <B1::Cursor as Cursor>::KeyContainer::with_capacity(0),
                next_pending_time: <B1::Cursor as Cursor>::TimeContainer::with_capacity(0),
                interesting_times: Vec::new(),
                new_interesting_times: Vec::new(),
                output_upper: Antichain::from_elem(<B1::Time as Timestamp>::minimum()),
                output_lower: Antichain::from_elem(<B1::Time as Timestamp>::minimum()),
                _marker: PhantomData,
            }
        }
    }

    impl<B1, B2, Bu, L, P> ReduceTactic<B1, B2> for CursorTactic<B1, B2, Bu, L, P>
    where
        B1: BatchReader + Navigable,
        B2: BatchReader<Time = B1::Time> + Navigable,
        B1::Cursor: Cursor<Time = B1::Time>,
        for<'a> B2::Cursor: Cursor<Key<'a> = <B1::Cursor as Cursor>::Key<'a>, ValOwn: Data, Time = B1::Time>,
        Bu: Builder<Time = B1::Time, Output = B2, Input: Default>,
        L: FnMut(<B1::Cursor as Cursor>::Key<'_>, &[(<B1::Cursor as Cursor>::Val<'_>, <B1::Cursor as Cursor>::Diff)], &mut Vec<(<B2::Cursor as Cursor>::ValOwn, <B2::Cursor as Cursor>::Diff)>, &mut Vec<(<B2::Cursor as Cursor>::ValOwn, <B2::Cursor as Cursor>::Diff)>),
        P: FnMut(&mut Bu::Input, <B1::Cursor as Cursor>::Key<'_>, &mut Vec<(<B2::Cursor as Cursor>::ValOwn, B1::Time, <B2::Cursor as Cursor>::Diff)>),
    {
        fn retire(
            &mut self,
            source_batches: Vec<B1>,
            output_batches: Vec<B2>,
            input_batches: Vec<B1>,
            lower: &Antichain<B1::Time>,
            upper: &Antichain<B1::Time>,
            held: &Antichain<B1::Time>,
        ) -> (Vec<(B1::Time, B2)>, Antichain<B1::Time>)
        {
            let mut produced = Vec::new();

            // We have compute needs only if we hold a time in the interval [lower, upper); otherwise we
            // could not transmit outputs even if they were (incorrectly) non-zero, and we leave the held
            // times unchanged.
            if held.elements().iter().any(|time| !upper.less_equal(time)) {

                // cursors for navigating input, output, and new-batch updates.
                let (mut source_cursor, ref source_storage) = cursor_list(source_batches);
                let (mut output_cursor, ref output_storage) = cursor_list(output_batches);
                let (mut batch_cursor, ref batch_storage) = cursor_list(input_batches);

                // Prepare an output buffer and builder for each held time.
                // TODO: It would be better if all updates went into one batch, but timely dataflow prevents
                //       this as long as it requires that there is only one capability for each message.
                let mut buffers = Vec::<(B1::Time, Vec<(<B2::Cursor as Cursor>::ValOwn, B1::Time, <B2::Cursor as Cursor>::Diff)>)>::new();
                let mut builders = Vec::new();
                for time in held.elements().iter() {
                    buffers.push((time.clone(), Vec::new()));
                    builders.push(Bu::new());
                }
                // Temporary staging for output building.
                let mut buffer = Bu::Input::default();

                // Reuseable state for performing the computation.
                let mut thinker = history_replay::HistoryReplayer::new();

                // March through the keys we must work on, merging `batch_cursor` and pending keys.
                // The interesting moments need to be in the interval to prompt work.
                let mut pending_pos = 0;
                while batch_cursor.key_valid(batch_storage) || pending_pos < self.pending_keys.len() {

                    // Determine the next key we will work on; could be synthetic, could be from a batch.
                    let key1 = self.pending_keys.get(pending_pos);
                    let key2 = batch_cursor.get_key(batch_storage);
                    let key = match (key1, key2) {
                        (Some(key1), Some(key2)) => ::std::cmp::min(key1, key2),
                        (Some(key1), None)       => key1,
                        (None, Some(key2))       => key2,
                        (None, None)             => unreachable!(),
                    };

                    // Populate `interesting_times` with interesting times not beyond `upper`.
                    // TODO: This could just be `pending_time` and indexes within `lower .. upper`.
                    let prior_pos = pending_pos;
                    self.interesting_times.clear();
                    while self.pending_keys.get(pending_pos) == Some(key) {
                        let owned_time = <B1::Cursor as Cursor>::owned_time(self.pending_time.index(pending_pos));
                        if !upper.less_equal(&owned_time) { self.interesting_times.push(owned_time); }
                        pending_pos += 1;
                    }

                    // tidy up times, removing redundancy.
                    sort_dedup(&mut self.interesting_times);

                    // If there are new updates, or pending times, we must investigate!
                    if batch_cursor.get_key(batch_storage) == Some(key) || !self.interesting_times.is_empty() {

                        // do the per-key computation.
                        thinker.compute(
                            key,
                            (&mut source_cursor, source_storage),
                            (&mut output_cursor, output_storage),
                            (&mut batch_cursor, batch_storage),
                            &self.interesting_times,
                            &mut self.logic,
                            upper,
                            &mut buffers[..],
                            &mut self.new_interesting_times,
                        );

                        // Advance the cursor if this key, so that the loop's validity check registers the work as done.
                        if batch_cursor.get_key(batch_storage) == Some(key) { batch_cursor.step_key(batch_storage); }

                        // Merge novel pending times with any prior pending times we did not process.
                        // TODO: This could be a merge, not a sort_dedup, because both lists should be sorted.
                        for pos in prior_pos .. pending_pos {
                            let owned_time = <B1::Cursor as Cursor>::owned_time(self.pending_time.index(pos));
                            if upper.less_equal(&owned_time) { self.new_interesting_times.push(owned_time); }
                        }
                        sort_dedup(&mut self.new_interesting_times);
                        for time in self.new_interesting_times.drain(..) {
                            self.next_pending_keys.push_ref(key);
                            self.next_pending_time.push_own(&time);
                        }

                        // Sort each buffer by value and move into the corresponding builder.
                        // TODO: This makes assumptions about at least one of (i) the stability of `sort_by`,
                        //       (ii) that the buffers are time-ordered, and (iii) that the builders accept
                        //       arbitrarily ordered times.
                        for index in 0 .. buffers.len() {
                            buffers[index].1.sort_by(|x,y| x.0.cmp(&y.0));
                            (self.push)(&mut buffer, key, &mut buffers[index].1);
                            buffers[index].1.clear();
                            builders[index].push(&mut buffer);

                        }
                    }
                    else {
                        // copy over the pending key and times.
                        for pos in prior_pos .. pending_pos {
                            self.next_pending_keys.push_ref(self.pending_keys.index(pos));
                            self.next_pending_time.push_ref(self.pending_time.index(pos));
                        }
                    }
                }
                // Drop to avoid lifetime issues that would lock `pending_{keys, time}`.
                drop(thinker);

                // We start sealing output batches from the lower limit (previous upper limit).
                // In principle, we could update `lower` itself, and it should arrive at `upper` by the
                // end of the process.
                self.output_lower.clear();
                self.output_lower.extend(lower.borrow().iter().cloned());

                // build each batch (because only one capability per message).
                for (index, builder) in builders.drain(..).enumerate() {

                    // Form the upper limit of the next batch, which includes all times greater
                    // than the input batch, or the held times from i + 1 onward.
                    self.output_upper.clear();
                    self.output_upper.extend(upper.borrow().iter().cloned());
                    for time in &held.elements()[index + 1 ..] {
                        self.output_upper.insert_ref(time);
                    }

                    if self.output_upper.borrow() != self.output_lower.borrow() {

                        let description = Description::new(self.output_lower.clone(), self.output_upper.clone(), Antichain::from_elem(<B1::Time as Timestamp>::minimum()));
                        let batch = builder.done(description);

                        // hand the batch back to the driver to ship and commit, tagged with its time.
                        produced.push((held.elements()[index].clone(), batch));

                        self.output_lower.clear();
                        self.output_lower.extend(self.output_upper.borrow().iter().cloned());
                    }
                }
                // This should be true, as the final iteration introduces no held times, and
                // uses exactly `upper` to determine the upper bound. Good to check though.
                assert!(self.output_upper.borrow() == upper.borrow());

                // Refresh pending keys and times.
                self.pending_keys.clear(); std::mem::swap(&mut self.next_pending_keys, &mut self.pending_keys);
                self.pending_time.clear(); std::mem::swap(&mut self.next_pending_time, &mut self.pending_time);

                // Compute the new frontier of interesting times for the operator to hold.
                let mut frontier = Antichain::<B1::Time>::new();
                let mut owned_time = <B1::Time as Timestamp>::minimum();
                for pos in 0 .. self.pending_time.len() {
                    <B1::Cursor as Cursor>::clone_time_onto(self.pending_time.index(pos), &mut owned_time);
                    frontier.insert_ref(&owned_time);
                }

                (produced, frontier)
            }
            else {
                // No work: leave the held times unchanged, so the driver's downgrade is a no-op.
                (produced, held.clone())
            }
        }
    }


    /// Implementation based on replaying historical and new updates together.
    mod history_replay {

        use timely::progress::Antichain;

        use crate::lattice::Lattice;
        use crate::trace::Cursor;
        use crate::operators::ValueHistory;

        use crate::operators::reduce::sort_dedup;

        /// The `HistoryReplayer` is a compute strategy based on moving through existing inputs, interesting times, etc in
        /// time order, maintaining consolidated representations of updates with respect to future interesting times.
        pub struct HistoryReplayer<V1, V2, V, T, D1, D2> {
            input_history: ValueHistory<V1, T, D1>,
            output_history: ValueHistory<V2, T, D2>,
            batch_history: ValueHistory<V1, T, D1>,
            input_buffer: Vec<(V1, D1)>,
            output_buffer: Vec<(V, D2)>,
            update_buffer: Vec<(V, D2)>,
            output_produced: Vec<((V, T), D2)>,
            synth_times: Vec<T>,
            meets: Vec<T>,
            times_current: Vec<T>,
            temporary: Vec<T>,
        }

        impl<V1, V2, V, T, D1, D2> HistoryReplayer<V1, V2, V, T, D1, D2>
        where
            V1: Copy + Ord,
            V2: Copy + Ord,
            V: Clone + Ord,
            T: Ord + Clone + Lattice,
            D1: Clone + crate::difference::Semigroup,
            D2: Clone + crate::difference::Semigroup,
        {
            pub fn new() -> Self {
                HistoryReplayer {
                    input_history: ValueHistory::new(),
                    output_history: ValueHistory::new(),
                    batch_history: ValueHistory::new(),
                    input_buffer: Vec::new(),
                    output_buffer: Vec::new(),
                    update_buffer: Vec::new(),
                    output_produced: Vec::new(),
                    synth_times: Vec::new(),
                    meets: Vec::new(),
                    times_current: Vec::new(),
                    temporary: Vec::new(),
                }
            }
            #[inline(never)]
            pub fn compute<'a, K, C1, C2, C3, L>(
                &mut self,
                key: K,
                (source_cursor, source_storage): (&mut C1, &'a C1::Storage),
                (output_cursor, output_storage): (&mut C2, &'a C2::Storage),
                (batch_cursor, batch_storage): (&mut C3, &'a C3::Storage),
                times: &Vec<T>,
                logic: &mut L,
                upper_limit: &Antichain<T>,
                outputs: &mut [(T, Vec<(V, T, D2)>)],
                new_interesting: &mut Vec<T>)
            where
                C1: Cursor<Key<'a> = K, Val<'a> = V1, Time = T, Diff = D1>,
                C2: Cursor<Key<'a> = K, Val<'a> = V2, ValOwn = V, Time = T, Diff = D2>,
                C3: Cursor<Key<'a> = K, Val<'a> = V1, Time = T, Diff = D1>,
                K: Copy + Ord,
                L: FnMut(K, &[(V1, D1)], &mut Vec<(V, D2)>, &mut Vec<(V, D2)>),
            {

                // The work we need to perform is at times defined principally by the contents of `batch_cursor`
                // and `times`, respectively "new work we just received" and "old times we were warned about".
                //
                // Our first step is to identify these times, so that we can use them to restrict the amount of
                // information we need to recover from `input` and `output`; as all times of interest will have
                // some time from `batch_cursor` or `times`, we can compute their meet and advance all other
                // loaded times by performing the lattice `join` with this value.

                // Load the batch contents.
                let mut batch_replay = self.batch_history.replay_key(batch_cursor, batch_storage, key, None);

                // We determine the meet of times we must reconsider (those from `batch` and `times`). This meet
                // can be used to advance other historical times, which may consolidate their representation. As
                // a first step, we determine the meets of each *suffix* of `times`, which we will use as we play
                // history forward.

                self.meets.clear();
                self.meets.extend(times.iter().cloned());
                for index in (1 .. self.meets.len()).rev() {
                    self.meets[index-1] = self.meets[index-1].meet(&self.meets[index]);
                }

                // Determine the meet of times in `batch` and `times`.
                let mut meet = None;
                update_meet(&mut meet, self.meets.get(0));
                update_meet(&mut meet, batch_replay.meet());

                // Having determined the meet, we can load the input and output histories, where we
                // advance all times by joining them with `meet`. The resulting times are more compact
                // and guaranteed to accumulate identically for times greater or equal to `meet`.

                // Load the input and output histories.
                let mut input_replay =
                self.input_history.replay_key(source_cursor, source_storage, key, meet.as_ref());
                let mut output_replay =
                self.output_history.replay_key(output_cursor, output_storage, key, meet.as_ref());

                self.synth_times.clear();
                self.times_current.clear();
                self.output_produced.clear();

                // The frontier of times we may still consider.
                // Derived from frontiers of our update histories, supplied times, and synthetic times.

                let mut times_slice = &times[..];
                let mut meets_slice = &self.meets[..];

                // We have candidate times from `batch` and `times`, as well as times identified by either
                // `input` or `output`. Finally, we may have synthetic times produced as the join of times
                // we consider in the course of evaluation. As long as any of these times exist, we need to
                // keep examining times.
                while let Some(next_time) = [   batch_replay.time(),
                                                times_slice.first(),
                                                input_replay.time(),
                                                output_replay.time(),
                                                self.synth_times.last(),
                                            ].into_iter().flatten().min().cloned() {

                    // Advance input and output history replayers. This marks applicable updates as active.
                    input_replay.step_while_time_is(&next_time);
                    output_replay.step_while_time_is(&next_time);

                    // One of our goals is to determine if `next_time` is "interesting", meaning whether we
                    // have any evidence that we should re-evaluate the user logic at this time. For a time
                    // to be "interesting" it would need to be the join of times that include either a time
                    // from `batch`, `times`, or `synth`. Neither `input` nor `output` times are sufficient.

                    // Advance batch history, and capture whether an update exists at `next_time`.
                    let mut interesting = batch_replay.step_while_time_is(&next_time);
                    if interesting { if let Some(meet) = meet.as_ref() { batch_replay.advance_buffer_by(meet); } }

                    // advance both `synth_times` and `times_slice`, marking this time interesting if in either.
                    while self.synth_times.last() == Some(&next_time) {
                        // We don't know enough about `next_time` to avoid putting it in to `times_current`.
                        // TODO: If we knew that the time derived from a canceled batch update, we could remove the time.
                        self.times_current.push(self.synth_times.pop().expect("failed to pop from synth_times")); // <-- TODO: this could be a min-heap.
                        interesting = true;
                    }
                    while times_slice.first() == Some(&next_time) {
                        // We know nothing about why we were warned about `next_time`, and must include it to scare future times.
                        self.times_current.push(times_slice[0].clone());
                        times_slice = &times_slice[1..];
                        meets_slice = &meets_slice[1..];
                        interesting = true;
                    }

                    // Times could also be interesting if an interesting time is less than them, as they would join
                    // and become the time itself. They may not equal the current time because whatever frontier we
                    // are tracking may not have advanced far enough.
                    // TODO: `batch_history` may or may not be super compact at this point, and so this check might
                    //       yield false positives if not sufficiently compact. Maybe we should look into this and see.
                    interesting = interesting || batch_replay.buffer().iter().any(|&((_, ref t),_)| t.less_equal(&next_time));
                    interesting = interesting || self.times_current.iter().any(|t| t.less_equal(&next_time));

                    // We should only process times that are not in advance of `upper_limit`.
                    //
                    // We have no particular guarantee that known times will not be in advance of `upper_limit`.
                    // We may have the guarantee that synthetic times will not be, as we test against the limit
                    // before we add the time to `synth_times`.
                    if !upper_limit.less_equal(&next_time) {

                        // DETERMINATION (times only). Determine synthetic interesting times.
                        //
                        // Synthetic interesting times are produced differently for interesting and uninteresting
                        // times. An uninteresting time must join with an interesting time to become interesting,
                        // which means joins with `self.batch_history` and  `self.times_current`. I think we can
                        // skip `self.synth_times` as we haven't gotten to them yet, but we will and they will be
                        // joined against everything.

                        // Any time, even uninteresting times, must be joined with the current accumulation of
                        // batch times as well as the current accumulation of `times_current`.
                        self.temporary.extend(batch_replay.buffer().iter().map(|((_,time),_)| time).filter(|time| !time.less_equal(&next_time)).map(|time| time.join(&next_time)));
                        self.temporary.extend(self.times_current.iter().filter(|time| !time.less_equal(&next_time)).map(|time| time.join(&next_time)));

                        // An interesting time additionally joins with `input` and `output` history and this round's
                        // produced output: it carries the seed, so those joins stay interesting (an uninteresting
                        // time does not, as `input`/`output` times are not themselves seeds). We advance the buffers
                        // by `meet` first, exactly as evaluation reads them below; by join preservation the advanced
                        // and unadvanced times spawn the same synthetics, so this matches the pre-split behavior.
                        if interesting {
                            if let Some(meet) = meet.as_ref() { input_replay.advance_buffer_by(meet) };
                            if let Some(meet) = meet.as_ref() { output_replay.advance_buffer_by(meet) };
                            self.temporary.extend(input_replay.buffer().iter().map(|((_,time),_)| time).filter(|time| !time.less_equal(&next_time)).map(|time| time.join(&next_time)));
                            self.temporary.extend(output_replay.buffer().iter().map(|((_,time),_)| time).filter(|time| !time.less_equal(&next_time)).map(|time| time.join(&next_time)));
                            self.temporary.extend(self.output_produced.iter().map(|((_,time),_)| time).filter(|time| !time.less_equal(&next_time)).map(|time| time.join(&next_time)));
                        }
                        sort_dedup(&mut self.temporary);

                        // Introduce synthetic times, and re-organize if we add any.
                        let synth_len = self.synth_times.len();
                        for time in self.temporary.drain(..) {
                            // We can either service `join` now, or must delay for the future.
                            if upper_limit.less_equal(&time) {
                                debug_assert!(outputs.iter().any(|(t,_)| t.less_equal(&time)));
                                new_interesting.push(time);
                            }
                            else {
                                self.synth_times.push(time);
                            }
                        }
                        if self.synth_times.len() > synth_len {
                            self.synth_times.sort_by(|x,y| y.cmp(x));
                            self.synth_times.dedup();
                        }

                        // EVALUATION (values only).
                        // We should re-evaluate the computation if this is an interesting time.
                        // If the time is uninteresting (and our logic is sound) it is not possible for there to be
                        // output produced. This sounds like a good test to have for debug builds!
                        if interesting {

                            // Assemble the input collection at `next_time`. (`self.input_buffer` cleared just after use).
                            // The buffers were advanced by `meet` in the determination step above.
                            debug_assert!(self.input_buffer.is_empty());
                            for ((value, time), diff) in input_replay.buffer().iter() {
                                if time.less_equal(&next_time) { self.input_buffer.push((*value, diff.clone())); }
                            }
                            for ((value, time), diff) in batch_replay.buffer().iter() {
                                if time.less_equal(&next_time) { self.input_buffer.push((*value, diff.clone())); }
                            }
                            crate::consolidation::consolidate(&mut self.input_buffer);

                            // Assemble the output collection at `next_time`. (`self.output_buffer` cleared just after use).
                            for ((value, time), diff) in output_replay.buffer().iter() {
                                if time.less_equal(&next_time) { self.output_buffer.push((C2::owned_val(*value), diff.clone())); }
                            }
                            for ((value, time), diff) in self.output_produced.iter() {
                                if time.less_equal(&next_time) { self.output_buffer.push(((*value).to_owned(), diff.clone())); }
                            }
                            crate::consolidation::consolidate(&mut self.output_buffer);

                            // Apply user logic if non-empty input or output and see what happens!
                            if !self.input_buffer.is_empty() || !self.output_buffer.is_empty() {
                                logic(key, &self.input_buffer[..], &mut self.output_buffer, &mut self.update_buffer);
                                self.input_buffer.clear();
                                self.output_buffer.clear();

                                // Having subtracted output updates from user output, consolidate the results to determine
                                // if there is anything worth reporting. Note: this also orders the results by value, so
                                // that could make the above merging plan even easier.
                                //
                                // Stash produced updates into both capability-indexed buffers and `output_produced`.
                                // The two locations are important, in that we will compact `output_produced` as we move
                                // through times, but we cannot compact the output buffers because we need their actual
                                // times.
                                crate::consolidation::consolidate(&mut self.update_buffer);
                                if !self.update_buffer.is_empty() {

                                    // We *should* be able to find a capability for `next_time`. Any thing else would
                                    // indicate a logical error somewhere along the way; either we release a capability
                                    // we should have kept, or we have computed the output incorrectly (or both!)
                                    let idx = outputs.iter().rev().position(|(time, _)| time.less_equal(&next_time));
                                    let idx = outputs.len() - idx.expect("failed to find index") - 1;
                                    for (val, diff) in self.update_buffer.drain(..) {
                                        self.output_produced.push(((val.clone(), next_time.clone()), diff.clone()));
                                        outputs[idx].1.push((val, next_time.clone(), diff));
                                    }

                                    // Advance times in `self.output_produced` and consolidate the representation.
                                    // NOTE: We only do this when we add records; it could be that there are situations
                                    //       where we want to consolidate even without changes (because an initially
                                    //       large collection can now be collapsed).
                                    if let Some(meet) = meet.as_ref() { for entry in &mut self.output_produced { (entry.0).1.join_assign(meet); } }
                                    crate::consolidation::consolidate(&mut self.output_produced);
                                }
                            }
                        }
                    }
                    else if interesting {
                        // We cannot process `next_time` now, and must delay it.
                        //
                        // I think we are probably only here because of an uninteresting time declared interesting,
                        // as initial interesting times are filtered to be in interval, and synthetic times are also
                        // filtered before introducing them to `self.synth_times`.
                        new_interesting.push(next_time.clone());
                        debug_assert!(outputs.iter().any(|(t,_)| t.less_equal(&next_time)))
                    }

                    // Update `meet` to track the meet of each source of times.
                    meet = None;
                    update_meet(&mut meet, batch_replay.meet());
                    update_meet(&mut meet, input_replay.meet());
                    update_meet(&mut meet, output_replay.meet());
                    for time in self.synth_times.iter() { update_meet(&mut meet, Some(time)); }
                    update_meet(&mut meet, meets_slice.first());

                    // Update `times_current` by the frontier.
                    if let Some(meet) = meet.as_ref() {
                        for time in self.times_current.iter_mut() {
                            *time = time.join(meet);
                        }
                    }

                    sort_dedup(&mut self.times_current);
                }

                // Normalize the representation of `new_interesting`, deduplicating and ordering.
                sort_dedup(new_interesting);
            }
        }

        /// Updates an optional meet by an optional time.
        fn update_meet<T: Lattice+Clone>(meet: &mut Option<T>, other: Option<&T>) {
            if let Some(time) = other {
                if let Some(meet) = meet.as_mut() { meet.meet_assign(time); }
                else { *meet = Some(time.clone()); }
            }
        }
    }
}

/// A second [`ReduceTactic`], written directly from the incremental model in
/// `formal/Differential/Model.lean`.
///
/// Per key it runs two phases over one cursor walk. Phase 1 (determination) computes the
/// interesting times as the truncated join-closure over {input, output, seeds} — the model's
/// `Reached` — advancing by meets so the synthetic set stays bounded. Phase 2 (application) walks
/// exactly those times in order, maintaining tight input/output accumulations by meets, and emits
/// the corrections — the model's `emit_correct`. Determination never consults the output produced
/// this round (it finishes first), so this tactic embodies the proven algorithm exactly and is the
/// clean subject for differential testing against [`cursors::CursorTactic`].
pub(crate) mod reference {

    use super::*;
    use crate::lattice::Lattice;
    use crate::operators::ValueHistory;

    /// Drives a key-wise reduction with the model-derived [`ReferenceTactic`], the analogue of the
    /// default [`super::reduce_trace`]. Same result contract; intended for differential testing of the
    /// two tactics against each other. Re-exported (doc-hidden) from the parent module as the sole
    /// public handle: the reference tactic is a testing and demonstration oracle, not a stable entry
    /// point to build on.
    pub fn reduce_trace_reference<'scope, Tr1, Bu, Tr2, L, P>(trace: Arranged<'scope, Tr1>, name: &str, logic: L, push: P) -> Arranged<'scope, TraceAgent<Tr2>>
    where
        Tr1: TraceReader<Batch: Navigable> + 'static,
        Tr2: Trace<Batch: Navigable, Time = Tr1::Time> + 'static,
        BatchCursor<Tr1>: Cursor<Time = Tr1::Time>,
        for<'a> BatchCursor<Tr2>: Cursor<Key<'a> = BatchKey<'a, Tr1>, ValOwn: Data, Time = Tr2::Time>,
        Bu: Builder<Time=Tr2::Time, Output = Tr2::Batch, Input: Default> + 'static,
        L: FnMut(BatchKey<'_, Tr1>, &[(BatchVal<'_, Tr1>, BatchDiff<Tr1>)], &mut Vec<(BatchValOwn<Tr2>, BatchDiff<Tr2>)>, &mut Vec<(BatchValOwn<Tr2>, BatchDiff<Tr2>)>)+'static,
        P: FnMut(&mut Bu::Input, BatchKey<'_, Tr1>, &mut Vec<(BatchValOwn<Tr2>, Tr2::Time, BatchDiff<Tr2>)>) + 'static,
    {
        reduce_with_tactic(trace, name, ReferenceTactic::<Tr1::Batch, Tr2::Batch, Bu, L, P>::new(logic, push))
    }


    /// Updates an optional meet by an optional time.
    fn update_meet<T: Lattice+Clone>(meet: &mut Option<T>, other: Option<&T>) {
        if let Some(time) = other {
            if let Some(meet) = meet.as_mut() { meet.meet_assign(time); }
            else { *meet = Some(time.clone()); }
        }
    }

    /// The model-derived [`ReduceTactic`]. Structurally a twin of [`cursors::CursorTactic`]; only the
    /// per-key engine differs.
    pub struct ReferenceTactic<B1, B2, Bu, L, P>
    where
        B1: BatchReader + Navigable,
        B2: BatchReader<Time = B1::Time> + Navigable,
        B1::Cursor: Cursor<Time = B1::Time>,
        for<'a> B2::Cursor: Cursor<Key<'a> = <B1::Cursor as Cursor>::Key<'a>, ValOwn: Data, Time = B1::Time>,
    {
        logic: L,
        push: P,
        pending_keys: <B1::Cursor as Cursor>::KeyContainer,
        pending_time: <B1::Cursor as Cursor>::TimeContainer,
        next_pending_keys: <B1::Cursor as Cursor>::KeyContainer,
        next_pending_time: <B1::Cursor as Cursor>::TimeContainer,
        interesting_times: Vec<B1::Time>,
        new_interesting_times: Vec<B1::Time>,
        output_upper: Antichain<B1::Time>,
        output_lower: Antichain<B1::Time>,
        _marker: PhantomData<(B2, Bu)>,
    }

    impl<B1, B2, Bu, L, P> ReferenceTactic<B1, B2, Bu, L, P>
    where
        B1: BatchReader + Navigable,
        B2: BatchReader<Time = B1::Time> + Navigable,
        B1::Cursor: Cursor<Time = B1::Time>,
        for<'a> B2::Cursor: Cursor<Key<'a> = <B1::Cursor as Cursor>::Key<'a>, ValOwn: Data, Time = B1::Time>,
    {
        /// Construct a tactic that applies `logic` to each key and shapes output with `push`.
        pub fn new(logic: L, push: P) -> Self {
            ReferenceTactic {
                logic,
                push,
                pending_keys: <B1::Cursor as Cursor>::KeyContainer::with_capacity(0),
                pending_time: <B1::Cursor as Cursor>::TimeContainer::with_capacity(0),
                next_pending_keys: <B1::Cursor as Cursor>::KeyContainer::with_capacity(0),
                next_pending_time: <B1::Cursor as Cursor>::TimeContainer::with_capacity(0),
                interesting_times: Vec::new(),
                new_interesting_times: Vec::new(),
                output_upper: Antichain::from_elem(<B1::Time as Timestamp>::minimum()),
                output_lower: Antichain::from_elem(<B1::Time as Timestamp>::minimum()),
                _marker: PhantomData,
            }
        }
    }

    impl<B1, B2, Bu, L, P> ReduceTactic<B1, B2> for ReferenceTactic<B1, B2, Bu, L, P>
    where
        B1: BatchReader + Navigable,
        B2: BatchReader<Time = B1::Time> + Navigable,
        B1::Cursor: Cursor<Time = B1::Time>,
        for<'a> B2::Cursor: Cursor<Key<'a> = <B1::Cursor as Cursor>::Key<'a>, ValOwn: Data, Time = B1::Time>,
        Bu: Builder<Time = B1::Time, Output = B2, Input: Default>,
        L: FnMut(<B1::Cursor as Cursor>::Key<'_>, &[(<B1::Cursor as Cursor>::Val<'_>, <B1::Cursor as Cursor>::Diff)], &mut Vec<(<B2::Cursor as Cursor>::ValOwn, <B2::Cursor as Cursor>::Diff)>, &mut Vec<(<B2::Cursor as Cursor>::ValOwn, <B2::Cursor as Cursor>::Diff)>),
        P: FnMut(&mut Bu::Input, <B1::Cursor as Cursor>::Key<'_>, &mut Vec<(<B2::Cursor as Cursor>::ValOwn, B1::Time, <B2::Cursor as Cursor>::Diff)>),
    {
        fn retire(
            &mut self,
            source_batches: Vec<B1>,
            output_batches: Vec<B2>,
            input_batches: Vec<B1>,
            lower: &Antichain<B1::Time>,
            upper: &Antichain<B1::Time>,
            held: &Antichain<B1::Time>,
        ) -> (Vec<(B1::Time, B2)>, Antichain<B1::Time>)
        {
            let mut produced = Vec::new();

            if held.elements().iter().any(|time| !upper.less_equal(time)) {

                let (mut source_cursor, ref source_storage) = cursor_list(source_batches);
                let (mut output_cursor, ref output_storage) = cursor_list(output_batches);
                let (mut batch_cursor, ref batch_storage) = cursor_list(input_batches);

                let mut buffers = Vec::<(B1::Time, Vec<(<B2::Cursor as Cursor>::ValOwn, B1::Time, <B2::Cursor as Cursor>::Diff)>)>::new();
                let mut builders = Vec::new();
                for time in held.elements().iter() {
                    buffers.push((time.clone(), Vec::new()));
                    builders.push(Bu::new());
                }
                let mut buffer = Bu::Input::default();

                // Reuseable state for performing the computation.
                let mut thinker = ReferenceThinker::new();

                let mut pending_pos = 0;
                while batch_cursor.key_valid(batch_storage) || pending_pos < self.pending_keys.len() {

                    let key1 = self.pending_keys.get(pending_pos);
                    let key2 = batch_cursor.get_key(batch_storage);
                    let key = match (key1, key2) {
                        (Some(key1), Some(key2)) => ::std::cmp::min(key1, key2),
                        (Some(key1), None)       => key1,
                        (None, Some(key2))       => key2,
                        (None, None)             => unreachable!(),
                    };

                    let prior_pos = pending_pos;
                    self.interesting_times.clear();
                    while self.pending_keys.get(pending_pos) == Some(key) {
                        let owned_time = <B1::Cursor as Cursor>::owned_time(self.pending_time.index(pending_pos));
                        if !upper.less_equal(&owned_time) { self.interesting_times.push(owned_time); }
                        pending_pos += 1;
                    }

                    sort_dedup(&mut self.interesting_times);

                    if batch_cursor.get_key(batch_storage) == Some(key) || !self.interesting_times.is_empty() {

                        thinker.compute(
                            key,
                            (&mut source_cursor, source_storage),
                            (&mut output_cursor, output_storage),
                            (&mut batch_cursor, batch_storage),
                            &self.interesting_times,
                            &mut self.logic,
                            upper,
                            &mut buffers[..],
                            &mut self.new_interesting_times,
                        );

                        if batch_cursor.get_key(batch_storage) == Some(key) { batch_cursor.step_key(batch_storage); }

                        for pos in prior_pos .. pending_pos {
                            let owned_time = <B1::Cursor as Cursor>::owned_time(self.pending_time.index(pos));
                            if upper.less_equal(&owned_time) { self.new_interesting_times.push(owned_time); }
                        }
                        sort_dedup(&mut self.new_interesting_times);
                        for time in self.new_interesting_times.drain(..) {
                            self.next_pending_keys.push_ref(key);
                            self.next_pending_time.push_own(&time);
                        }

                        for index in 0 .. buffers.len() {
                            buffers[index].1.sort_by(|x,y| x.0.cmp(&y.0));
                            (self.push)(&mut buffer, key, &mut buffers[index].1);
                            buffers[index].1.clear();
                            builders[index].push(&mut buffer);

                        }
                    }
                    else {
                        for pos in prior_pos .. pending_pos {
                            self.next_pending_keys.push_ref(self.pending_keys.index(pos));
                            self.next_pending_time.push_ref(self.pending_time.index(pos));
                        }
                    }
                }
                drop(thinker);

                self.output_lower.clear();
                self.output_lower.extend(lower.borrow().iter().cloned());

                for (index, builder) in builders.drain(..).enumerate() {

                    self.output_upper.clear();
                    self.output_upper.extend(upper.borrow().iter().cloned());
                    for time in &held.elements()[index + 1 ..] {
                        self.output_upper.insert_ref(time);
                    }

                    if self.output_upper.borrow() != self.output_lower.borrow() {

                        let description = Description::new(self.output_lower.clone(), self.output_upper.clone(), Antichain::from_elem(<B1::Time as Timestamp>::minimum()));
                        let batch = builder.done(description);

                        produced.push((held.elements()[index].clone(), batch));

                        self.output_lower.clear();
                        self.output_lower.extend(self.output_upper.borrow().iter().cloned());
                    }
                }
                assert!(self.output_upper.borrow() == upper.borrow());

                self.pending_keys.clear(); std::mem::swap(&mut self.next_pending_keys, &mut self.pending_keys);
                self.pending_time.clear(); std::mem::swap(&mut self.next_pending_time, &mut self.pending_time);

                let mut frontier = Antichain::<B1::Time>::new();
                let mut owned_time = <B1::Time as Timestamp>::minimum();
                for pos in 0 .. self.pending_time.len() {
                    <B1::Cursor as Cursor>::clone_time_onto(self.pending_time.index(pos), &mut owned_time);
                    frontier.insert_ref(&owned_time);
                }

                (produced, frontier)
            }
            else {
                (produced, held.clone())
            }
        }
    }

    /// The two-phase per-key engine.
    ///
    /// Phase 1 (determination) reads the input/output/seed *times* and closes them into `active`
    /// (the interesting times) and the pended set — Model.lean's `Reached`, directly. Phase 2
    /// (application) walks the same, still-loaded histories for *values* and emits corrections.
    pub struct ReferenceThinker<V1, V2, V, T, D1, D2> {
        input_history: ValueHistory<V1, T, D1>,
        output_history: ValueHistory<V2, T, D2>,
        batch_history: ValueHistory<V1, T, D1>,
        input_buffer: Vec<(V1, D1)>,
        output_buffer: Vec<(V, D2)>,
        update_buffer: Vec<(V, D2)>,
        output_produced: Vec<((V, T), D2)>,
        // Phase 1 (the compacted closure): synthetic reached times still to visit, the reached times
        // in play as join partners, scratch for the joins, and suffix-meets of the supplied times.
        synth_times: Vec<T>,
        times_current: Vec<T>,
        temporary: Vec<T>,
        meets: Vec<T>,
        // Reusable time-only buffers for phase 1's `TimeReplay` walks. Pooled here (rather than in
        // `ValueHistory`) so the reference tactic pays for them and the standard value walk does not.
        batch_times: Vec<T>,
        input_times: Vec<T>,
        output_times: Vec<T>,
        // The interesting (in-band reached) times, handed from phase 1 to phase 2.
        active: Vec<T>,
    }

    impl<V1, V2, V, T, D1, D2> ReferenceThinker<V1, V2, V, T, D1, D2>
    where
        V1: Copy + Ord,
        V2: Copy + Ord,
        V: Clone + Ord,
        T: Ord + Clone + Lattice + 'static,
        D1: Clone + crate::difference::Semigroup,
        D2: Clone + crate::difference::Semigroup,
    {
        pub fn new() -> Self {
            ReferenceThinker {
                input_history: ValueHistory::new(),
                output_history: ValueHistory::new(),
                batch_history: ValueHistory::new(),
                input_buffer: Vec::new(),
                output_buffer: Vec::new(),
                update_buffer: Vec::new(),
                output_produced: Vec::new(),
                synth_times: Vec::new(),
                times_current: Vec::new(),
                temporary: Vec::new(),
                meets: Vec::new(),
                batch_times: Vec::new(),
                input_times: Vec::new(),
                output_times: Vec::new(),
                active: Vec::new(),
            }
        }

        #[inline(never)]
        pub fn compute<'a, K, C1, C2, C3, L>(
            &mut self,
            key: K,
            (source_cursor, source_storage): (&mut C1, &'a C1::Storage),
            (output_cursor, output_storage): (&mut C2, &'a C2::Storage),
            (batch_cursor, batch_storage): (&mut C3, &'a C3::Storage),
            times: &Vec<T>,
            logic: &mut L,
            upper_limit: &Antichain<T>,
            outputs: &mut [(T, Vec<(V, T, D2)>)],
            new_interesting: &mut Vec<T>)
        where
            C1: Cursor<Key<'a> = K, Val<'a> = V1, Time = T, Diff = D1>,
            C2: Cursor<Key<'a> = K, Val<'a> = V2, ValOwn = V, Time = T, Diff = D2>,
            C3: Cursor<Key<'a> = K, Val<'a> = V1, Time = T, Diff = D1>,
            K: Copy + Ord,
            L: FnMut(K, &[(V1, D1)], &mut Vec<(V, D2)>, &mut Vec<(V, D2)>),
        {
            // ================== PHASE 1 — DETERMINATION (`Reached`, compacted) ==================
            // The interesting times are Model.lean's `Reached` — but computed the non-quadratic way.
            // Walk the input/output/seed times in increasing order; a time is *reached* only via a
            // seed (a batch update, a due pending time, or a synthetic join of earlier reached times);
            // a reached in-band time joins against the live partners to spawn more reached times, and
            // a join beyond `upper` is pended. The live partner sets are kept an antichain by
            // `advance_buffer_by(meet)` — coincident times collapse under the running meet — so this is
            // the closure without the all-pairs blow-up. Time-only: `TimeReplay` reads the histories
            // without touching values or stepping the underlying `history`, so phase 2 can walk them.
            {
                // Suffix-meets of the supplied (due pending) times, consumed as we pass them.
                self.meets.clear();
                self.meets.extend(times.iter().cloned());
                for index in (1 .. self.meets.len()).rev() {
                    self.meets[index-1] = self.meets[index-1].meet(&self.meets[index]);
                }

                // Build each history, then read it time-only (leaving it intact for phase 2).
                drop(self.batch_history.replay_key(batch_cursor, batch_storage, key, None));
                drop(self.input_history.replay_key(source_cursor, source_storage, key, None));
                drop(self.output_history.replay_key(output_cursor, output_storage, key, None));
                let mut batch_replay = self.batch_history.replay_times(&mut self.batch_times);
                let mut input_replay = self.input_history.replay_times(&mut self.input_times);
                let mut output_replay = self.output_history.replay_times(&mut self.output_times);

                self.synth_times.clear();
                self.times_current.clear();
                self.temporary.clear();
                self.active.clear();

                let mut times_slice = &times[..];
                let mut meets_slice = &self.meets[..];
                let mut meet: Option<T> = None;

                while let Some(next_time) = [   batch_replay.time(),
                                                times_slice.first(),
                                                input_replay.time(),
                                                output_replay.time(),
                                                self.synth_times.last(),
                                            ].into_iter().flatten().min().cloned() {

                    input_replay.step_while_time_is(&next_time);
                    output_replay.step_while_time_is(&next_time);

                    // Reached via a seed: a batch update, a due pending time, or a synthetic join.
                    // (Input/output times alone are not reached — they are only join partners.)
                    let mut interesting = batch_replay.step_while_time_is(&next_time);
                    if interesting { if let Some(m) = meet.as_ref() { batch_replay.advance_buffer_by(m); } }
                    while self.synth_times.last() == Some(&next_time) {
                        self.times_current.push(self.synth_times.pop().unwrap());
                        interesting = true;
                    }
                    while times_slice.first() == Some(&next_time) {
                        self.times_current.push(next_time.clone());
                        times_slice = &times_slice[1..];
                        meets_slice = &meets_slice[1..];
                        interesting = true;
                    }
                    // Absorb: a time at or above a reached time is itself reached.
                    interesting = interesting
                        || batch_replay.buffer().iter().any(|t| t.less_equal(&next_time))
                        || self.times_current.iter().any(|t| t.less_equal(&next_time));

                    if !upper_limit.less_equal(&next_time) {
                        // A reached in-band time is `active`; join it against the live partners —
                        // input/output (`joinBase`) and reached-so-far (`joinActive`) — for new times.
                        if interesting {
                            self.active.push(next_time.clone());
                            if let Some(m) = meet.as_ref() { input_replay.advance_buffer_by(m); output_replay.advance_buffer_by(m); }
                            self.temporary.extend(input_replay.buffer().iter().filter(|t| !t.less_equal(&next_time)).map(|t| t.join(&next_time)));
                            self.temporary.extend(output_replay.buffer().iter().filter(|t| !t.less_equal(&next_time)).map(|t| t.join(&next_time)));
                        }
                        self.temporary.extend(batch_replay.buffer().iter().filter(|t| !t.less_equal(&next_time)).map(|t| t.join(&next_time)));
                        self.temporary.extend(self.times_current.iter().filter(|t| !t.less_equal(&next_time)).map(|t| t.join(&next_time)));
                        sort_dedup(&mut self.temporary);

                        let synth_len = self.synth_times.len();
                        for time in self.temporary.drain(..) {
                            if upper_limit.less_equal(&time) { new_interesting.push(time); }  // pended
                            else { self.synth_times.push(time); }                             // reached, later
                        }
                        if self.synth_times.len() > synth_len {
                            self.synth_times.sort_by(|x,y| y.cmp(x));
                            self.synth_times.dedup();
                        }
                    }
                    else if interesting {
                        new_interesting.push(next_time.clone());
                    }

                    // Running meet (a lower bound on every time still to visit); compact the reached
                    // partners `times_current` with it.
                    meet = None;
                    update_meet(&mut meet, batch_replay.meet());
                    update_meet(&mut meet, input_replay.meet());
                    update_meet(&mut meet, output_replay.meet());
                    for time in self.synth_times.iter() { update_meet(&mut meet, Some(time)); }
                    update_meet(&mut meet, meets_slice.first());
                    if let Some(m) = meet.as_ref() {
                        for time in self.times_current.iter_mut() { *time = time.join(m); }
                    }
                    sort_dedup(&mut self.times_current);
                }

                sort_dedup(&mut self.active);
            }

            // ===================== PHASE 2 — APPLICATION (`emit_correct`) =====================
            // Walk `self.active` in order over the SAME per-key edits (via `replay`, no cursor), keep
            // the input/output accumulations tight by advancing to the meet of the times still to be
            // produced, apply `logic`, and emit corrections.
            {
                self.meets.clear();
                self.meets.extend(self.active.iter().cloned());
                for index in (1 .. self.meets.len()).rev() {
                    self.meets[index-1] = self.meets[index-1].meet(&self.meets[index]);
                }

                // Walk the histories loaded (and left intact) by phase 1 — no cursor re-read, no
                // rebuild, no re-sort; just a fresh walk of the same sorted `history` for values.
                let mut batch_replay = self.batch_history.walk();
                let mut input_replay = self.input_history.walk();
                let mut output_replay = self.output_history.walk();

                self.output_produced.clear();

                for index in 0 .. self.active.len() {
                    let next_time = self.active[index].clone();
                    let meet = self.meets[index].clone();

                    // Phase 2 visits only the active times, so at each we must catch up the histories to
                    // include every edit that will contribute to the accumulation at `next_time` (edits
                    // at non-active times count too). `history` is sorted by the total `Ord` and `step`
                    // pops the least, so we step the `Ord`-prefix `t <= next_time`, NOT the partial
                    // `t.less_equal(next_time)`: the partial order interleaves with the sort, so an
                    // `Ord`-earlier time incomparable to `next_time` would halt a `less_equal` walk early
                    // and strand later edits that *are* `less_equal(next_time)`. Stepping the `Ord`-prefix
                    // takes a superset; the `less_equal` filter below then selects the true `<= next_time`
                    // edits. (Assembling with `less_equal` while stepping with `<=` is what the value-aware
                    // cursor does via its full time-order frontier walk.)
                    while input_replay.time().map_or(false, |t| *t <= next_time) { input_replay.step(); }
                    while batch_replay.time().map_or(false, |t| *t <= next_time) { batch_replay.step(); }
                    while output_replay.time().map_or(false, |t| *t <= next_time) { output_replay.step(); }
                    input_replay.advance_buffer_by(&meet);
                    batch_replay.advance_buffer_by(&meet);
                    output_replay.advance_buffer_by(&meet);

                    debug_assert!(self.input_buffer.is_empty());
                    for ((value, time), diff) in input_replay.buffer().iter() {
                        if time.less_equal(&next_time) { self.input_buffer.push((*value, diff.clone())); }
                    }
                    for ((value, time), diff) in batch_replay.buffer().iter() {
                        if time.less_equal(&next_time) { self.input_buffer.push((*value, diff.clone())); }
                    }
                    crate::consolidation::consolidate(&mut self.input_buffer);

                    for ((value, time), diff) in output_replay.buffer().iter() {
                        if time.less_equal(&next_time) { self.output_buffer.push((C2::owned_val(*value), diff.clone())); }
                    }
                    for ((value, time), diff) in self.output_produced.iter() {
                        if time.less_equal(&next_time) { self.output_buffer.push((value.clone(), diff.clone())); }
                    }
                    crate::consolidation::consolidate(&mut self.output_buffer);

                    if !self.input_buffer.is_empty() || !self.output_buffer.is_empty() {
                        logic(key, &self.input_buffer[..], &mut self.output_buffer, &mut self.update_buffer);
                        self.input_buffer.clear();
                        self.output_buffer.clear();

                        crate::consolidation::consolidate(&mut self.update_buffer);
                        if !self.update_buffer.is_empty() {

                            let idx = outputs.iter().rev().position(|(time, _)| time.less_equal(&next_time));
                            let idx = outputs.len() - idx.expect("failed to find index") - 1;
                            for (val, diff) in self.update_buffer.drain(..) {
                                self.output_produced.push(((val.clone(), next_time.clone()), diff.clone()));
                                outputs[idx].1.push((val, next_time.clone(), diff));
                            }

                            for entry in &mut self.output_produced { (entry.0).1.join_assign(&meet); }
                            crate::consolidation::consolidate(&mut self.output_produced);
                        }
                    }
                }
            }
        }
    }
}

// Copyright (c) 2015 Frank McSherry
// SPDX-License-Identifier: MIT
// See LICENSE in this directory.

//! An append-only collection of update batches.
//!
//! The `Spine` is a general-purpose trace implementation based on collection and merging
//! immutable batches of updates. It is generic with respect to the batch type, and can be
//! instantiated for any implementor of [`SpineBatch`].
//!
//! ## Design
//!
//! This spine is represented as a list of layers, where each element in the list is either
//!
//!   1. MergeState::Vacant  empty
//!   2. MergeState::Single  a single span
//!   3. MergeState::Double  a pair of spans
//!
//! Each "span" has the option to be `None`, indicating a non-span that nonetheless acts
//! as a number of updates proportionate to the level at which it exists (for bookkeeping).
//!
//! Each of the spans at layer i contains at most 2^i elements. The sequence of spans
//! should have the upper bound of one match the lower bound of the next. Spans may carry
//! no updates, with matching upper and lower bounds, as a bookkeeping mechanism.
//!
//! Each batch at layer i is treated as if it contains exactly 2^i elements, even though it
//! may actually contain fewer elements. This allows us to decouple the physical representation
//! from logical amounts of effort invested in each batch. It allows us to begin compaction and
//! to reduce the number of updates, without compromising our ability to continue to move
//! updates along the spine. We are explicitly making the trade-off that while some batches
//! might compact at lower levels, we want to treat them as if they contained their full set of
//! updates for accounting reasons (to apply work to higher levels).
//!
//! We maintain the invariant that for any in-progress merge at level k there should be fewer
//! than 2^k records at levels lower than k. That is, even if we were to apply an unbounded
//! amount of effort to those records, we would not have enough records to prompt a merge into
//! the in-progress merge. Ideally, we maintain the extended invariant that for any in-progress
//! merge at level k, the remaining effort required (number of records minus applied effort) is
//! less than the number of records that would need to be added to reach 2^k records in layers
//! below.
//!
//! ## Mathematics
//!
//! When a merge is initiated, there should be a non-negative *deficit* of updates before the layers
//! below could plausibly produce a new batch for the currently merging layer. We must determine a
//! factor of proportionality, so that newly arrived updates provide at least that amount of "fuel"
//! towards the merging layer, so that the merge completes before lower levels invade.
//!
//! ### Deficit:
//!
//! A new merge is initiated only in response to the completion of a prior merge, or the introduction
//! of new records from outside. The latter case is special, and will maintain our invariant trivially,
//! so we will focus on the former case.
//!
//! When a merge at level k completes, assuming we have maintained our invariant then there should be
//! fewer than 2^k records at lower levels. The newly created merge at level k+1 will require up to
//! 2^k+2 units of work, and should not expect a new batch until strictly more than 2^k records are
//! added. This means that a factor of proportionality of four should be sufficient to ensure that
//! the merge completes before a new merge is initiated.
//!
//! When new records get introduced, we will need to roll up any batches at lower levels, which we
//! treat as the introduction of records. Each of these virtual records introduced should either be
//! accounted for the fuel it should contribute, as it results in the promotion of batches closer to
//! in-progress merges.
//!
//! ### Fuel sharing
//!
//! We like the idea of applying fuel preferentially to merges at *lower* levels, under the idea that
//! they are easier to complete, and we benefit from fewer total merges in progress. This does delay
//! the completion of merges at higher levels, and may not obviously be a total win. If we choose to
//! do this, we should make sure that we correctly account for completed merges at low layers: they
//! should still extract fuel from new updates even though they have completed, at least until they
//! have paid back any "debt" to higher layers by continuing to provide fuel as updates arrive.

use std::collections::VecDeque;
use std::task::{Context, Poll, Waker};

use differential_dataflow_next::lattice::Lattice;
use differential_dataflow_next::logging::Logger;
use differential_dataflow_next::trace::asynchronous::{Batch as SpineBatch, MergeStatus, Merger};
use differential_dataflow_next::trace::{Description, ExertionLogic, Span};
use mz_ore::cast::CastFrom;

use ::timely_next::dataflow::operators::generic::OperatorInfo;
use ::timely_next::order::PartialOrder;
use ::timely_next::progress::{Antichain, frontier::AntichainRef};

/// The number of updates in a span: its batch's length, or zero when it carries none.
///
/// This is how the spine asks whether a span has updates. `Span::has_updates` answers the
/// weaker question of whether a batch is *present*, which agrees with this only for
/// producers that report absence when they built nothing; the spine can measure instead,
/// and does.
fn span_len<B: SpineBatch>(span: &Span<B::Time, B>) -> usize {
    span.inner.as_ref().map(|b| b.len()).unwrap_or(0)
}

// A continuation of the existing introduction algorithm. No action owns a
// published span: results move directly between levels before a poll can yield.
enum Maintenance {
    Fuel {
        level: usize,
        grant: isize,
        remaining: isize,
    },
    RollUp {
        target: usize,
        next: Option<usize>,
        remaining: isize,
    },
    Insert {
        level: usize,
        pending: bool,
    },
    FuseEmpty {
        level: usize,
        remaining: isize,
    },
    Tidy,
}

/// An append-only collection of update tuples.
///
/// A spine maintains a small number of immutable collections of update tuples, merging the collections when
/// two have similar sizes. In this way, it allows the addition of more tuples, which may then be merged with
/// other immutable collections.
pub struct Spine<B: SpineBatch> {
    operator: OperatorInfo,
    logger: Option<Logger>,
    logical_frontier: Antichain<B::Time>, // Times after which the trace must accumulate correctly.
    physical_frontier: Antichain<B::Time>, // Times after which the trace must be able to subset its inputs.
    merging: Vec<MergeState<B>>,           // Several possibly shared collections of updates.
    pending: Vec<Span<B::Time, B>>,        // Spans at times in advance of `frontier`.
    upper: Antichain<B::Time>,
    effort: usize,
    activator: Option<timely_next::scheduling::activate::Activator>,
    /// Parameters to `exert_logic`, containing tuples of `(index, count, length)`.
    exert_logic_param: Vec<(usize, usize, usize)>,
    /// Logic to indicate whether and how many records we should introduce in the absence of actual updates.
    exert_logic: Option<ExertionLogic>,
    maintenance: VecDeque<Maintenance>,
    waker: Option<Waker>,
}

impl<B: SpineBatch + Clone + 'static> Spine<B> {
    /// Return published spans through a clean cut at or beyond physical compaction.
    pub fn spans_through(&self, upper: AntichainRef<B::Time>) -> Option<Vec<Span<B::Time, B>>> {
        // If `upper` is the minimum frontier, we can return an empty cursor.
        // This can happen with operators that are written to expect the ability to acquire cursors
        // for their prior frontiers, and which start at `[T::minimum()]`, such as `Reduce`, sadly.
        if upper.less_equal(&<B::Time as timely_next::progress::Timestamp>::minimum()) {
            return Some(Vec::new());
        }

        // The supplied `upper` should have the property that for each of our
        // batch `lower` and `upper` frontiers, the supplied upper is comparable
        // to the frontier; it should not be incomparable, because the frontiers
        // that we created form a total order. If it is, there is a bug.
        //
        // We should acquire a cursor including all batches whose upper is less
        // or equal to the supplied upper, excluding all batches whose lower is
        // greater or equal to the supplied upper, and if a batch straddles the
        // supplied upper it had better be empty.

        // We shouldn't grab a cursor into a closed trace, right?
        assert!(self.logical_frontier.borrow().len() > 0);

        // Check that `upper` is greater or equal to `self.physical_frontier`.
        // Otherwise, the cut could be in `self.merging` and it is user error anyhow.
        assert!(PartialOrder::less_equal(
            &self.physical_frontier.borrow(),
            &upper
        ));

        let mut storage = Vec::new();

        for merge_state in self.merging.iter().rev() {
            match merge_state {
                MergeState::Double(variant) => match variant {
                    MergeVariant::InProgress(batch1, batch2, _, _) => {
                        if span_len(batch1) > 0 {
                            storage.push(batch1.clone());
                        }
                        if span_len(batch2) > 0 {
                            storage.push(batch2.clone());
                        }
                    }
                    MergeVariant::Complete(Some((batch, _))) => {
                        if span_len(batch) > 0 {
                            storage.push(batch.clone());
                        }
                    }
                    MergeVariant::Complete(None) => {}
                },
                MergeState::Single(Some(batch)) => {
                    if span_len(batch) > 0 {
                        storage.push(batch.clone());
                    }
                }
                MergeState::Single(None) => {}
                MergeState::Vacant => {}
            }
        }

        for batch in self.pending.iter() {
            if span_len(batch) > 0 {
                // For a non-empty `batch`, it is a catastrophic error if `upper`
                // requires some-but-not-all of the updates in the batch. We can
                // determine this from `upper` and the lower and upper bounds of
                // the batch itself.
                //
                // TODO: It is not clear if this is the 100% correct logic, due
                // to the possible non-total-orderedness of the frontiers.

                let include_lower = PartialOrder::less_equal(&batch.lower().borrow(), &upper);
                let include_upper = PartialOrder::less_equal(&batch.upper().borrow(), &upper);

                if include_lower != include_upper && upper != batch.lower().borrow() {
                    panic!("`spans_through`: `upper` straddles batch");
                }

                // include pending batches
                if include_upper {
                    storage.push(batch.clone());
                }
            }
        }

        Some(storage)
    }
    #[inline]
    /// Advance the frontier through which update timestamps may be compacted.
    pub fn set_logical_compaction(&mut self, frontier: AntichainRef<B::Time>) {
        self.logical_frontier.clear();
        self.logical_frontier.extend(frontier.iter().cloned());
    }
    #[inline]
    /// Return the current logical compaction frontier.
    pub fn get_logical_compaction(&self) -> AntichainRef<'_, B::Time> {
        self.logical_frontier.borrow()
    }
    #[inline]
    /// Advance the frontier through which published batches may be merged.
    pub fn set_physical_compaction(&mut self, frontier: AntichainRef<'_, B::Time>) {
        // We should never request to rewind the frontier.
        debug_assert!(
            PartialOrder::less_equal(&self.physical_frontier.borrow(), &frontier),
            "FAIL\tthrough frontier !<= new frontier {:?} {:?}\n",
            self.physical_frontier,
            frontier
        );
        self.physical_frontier.clear();
        self.physical_frontier.extend(frontier.iter().cloned());
        self.activate();
    }
    #[inline]
    /// Return the current physical compaction frontier.
    pub fn get_physical_compaction(&self) -> AntichainRef<'_, B::Time> {
        self.physical_frontier.borrow()
    }

    #[inline]
    pub fn map_spans<F: FnMut(&Span<B::Time, B>)>(&self, mut f: F) {
        for batch in self.merging.iter().rev() {
            match batch {
                MergeState::Double(MergeVariant::InProgress(batch1, batch2, _, _)) => {
                    f(batch1);
                    f(batch2);
                }
                MergeState::Double(MergeVariant::Complete(Some((batch, _)))) => f(batch),
                MergeState::Single(Some(batch)) => f(batch),
                _ => {}
            }
        }
        for batch in self.pending.iter() {
            f(batch);
        }
    }
}

impl<B: SpineBatch + Clone + 'static> Spine<B> {
    /// Construct an empty spine with the default effort multiplier.
    pub fn new(
        info: ::timely_next::dataflow::operators::generic::OperatorInfo,
        logging: Option<differential_dataflow_next::logging::Logger>,
        activator: Option<timely_next::scheduling::activate::Activator>,
    ) -> Self {
        Self::with_effort(1, info, logging, activator)
    }

    /// Apply some amount of effort to trace maintenance.
    ///
    /// Whether and how much effort to apply is determined by `self.exert_logic`, a closure the user can set.
    pub fn exert(&mut self) {
        self.exert_inner(true);
    }

    /// Apply policy-funded effort to active merges without forcing separate batches together.
    ///
    /// Pending introductions retain their insertion-funded work. Call `exert` when
    /// input drains to satisfy the configured optional consolidation policy.
    pub fn exert_merges(&mut self) {
        self.exert_inner(false);
    }

    fn exert_inner(&mut self, allow_consolidation: bool) {
        // Finish the old grant before asking policy for another one.
        if !self.drive_maintenance() {
            return;
        }
        self.consider_merges();
        if !self.maintenance.is_empty() {
            return;
        }
        self.tidy_layers();
        if let Some(effort) = self.exert_effort() {
            let active_merge = self.merging.iter().any(|b| b.is_double());
            if !active_merge && !allow_consolidation {
                return;
            }
            crate::columnar::chunk::metrics::record(
                crate::columnar::chunk::metrics::Stage::OptionalExert,
                effort,
                0,
            );
            if active_merge {
                self.queue_fuel(effort.cast_signed());
            } else {
                let level = usize::cast_from(effort.next_power_of_two().trailing_zeros());
                self.queue_introduction(level, false);
            }
            if self.drive_maintenance() {
                self.activate();
            }
        }
    }

    /// Resume queued maintenance without requesting another exertion allowance.
    ///
    /// Completion wakes the owner so it can accept input before starting further
    /// maintenance. Incomplete work retains its fuel and waits for the read waker.
    pub fn resume_maintenance(&mut self) {
        if self.drive_maintenance() {
            self.activate();
        }
    }

    /// Install the owning operator's wakeup before any maintenance can return pending.
    pub fn set_waker(&mut self, waker: Waker) {
        self.waker = Some(waker);
    }
    /// Whether an earlier maintenance continuation still needs to finish.
    pub fn maintenance_pending(&self) -> bool {
        !self.maintenance.is_empty()
    }

    /// Set the policy that grants maintenance fuel in the absence of new updates.
    pub fn set_exert_logic(&mut self, logic: ExertionLogic) {
        self.exert_logic = Some(logic);
    }

    // Ideally, this method acts as insertion of `span`, even if we are not yet able to begin
    // merging its batch. This means it is a good time to perform amortized work proportional
    // to the size of that batch.
    /// Append a span whose lower frontier equals the current upper frontier.
    pub fn insert(&mut self, span: Span<B::Time, B>) {
        // Log the introduction of a batch.
        self.logger.as_ref().map(|l| {
            l.log(differential_dataflow_next::logging::BatchEvent {
                operator: self.operator.global_id,
                length: span_len(&span),
            })
        });

        assert!(span.lower() != span.upper());
        assert_eq!(span.lower(), &self.upper);

        self.upper.clone_from(span.upper());

        // TODO: Consolidate or discard spans with no updates.
        self.pending.push(span);
        self.activate();
    }

    /// Completes the trace with a final empty batch.
    pub fn close(&mut self) {
        if !self.upper.borrow().is_empty() {
            self.insert(Span::empty(self.upper.clone(), Antichain::new()));
        }
    }
}

// Drop implementation allows us to log batch drops, to zero out maintained totals.
impl<B: SpineBatch> Drop for Spine<B> {
    fn drop(&mut self) {
        self.drop_batches();
    }
}

impl<B: SpineBatch> Spine<B> {
    /// Drops and logs batches. Used in `set_logical_compaction` and drop.
    fn drop_batches(&mut self) {
        if let Some(logger) = &self.logger {
            for batch in self.merging.drain(..) {
                match batch {
                    MergeState::Single(Some(batch)) => {
                        logger.log(differential_dataflow_next::logging::DropEvent {
                            operator: self.operator.global_id,
                            length: span_len(&batch),
                        });
                    }
                    MergeState::Double(MergeVariant::InProgress(batch1, batch2, _, _)) => {
                        logger.log(differential_dataflow_next::logging::DropEvent {
                            operator: self.operator.global_id,
                            length: span_len(&batch1),
                        });
                        logger.log(differential_dataflow_next::logging::DropEvent {
                            operator: self.operator.global_id,
                            length: span_len(&batch2),
                        });
                    }
                    MergeState::Double(MergeVariant::Complete(Some((batch, _)))) => {
                        logger.log(differential_dataflow_next::logging::DropEvent {
                            operator: self.operator.global_id,
                            length: span_len(&batch),
                        });
                    }
                    _ => {}
                }
            }
            for batch in self.pending.drain(..) {
                logger.log(differential_dataflow_next::logging::DropEvent {
                    operator: self.operator.global_id,
                    length: span_len(&batch),
                });
            }
        }
    }
}

impl<B: SpineBatch> Spine<B> {
    fn activate(&self) {
        if let Some(activator) = &self.activator {
            activator.activate();
        } else if let Some(waker) = &self.waker {
            waker.wake_by_ref();
        }
    }

    /// Determine the amount of effort we should exert in the absence of updates.
    ///
    /// This method prepares an iterator over batches, including the level, count, and length of each layer.
    /// It supplies this to `self.exert_logic`, who produces the response of the amount of exertion to apply.
    fn exert_effort(&mut self) -> Option<usize> {
        self.exert_logic.as_ref().and_then(|exert_logic| {
            self.exert_logic_param.clear();
            self.exert_logic_param
                .extend(
                    self.merging
                        .iter()
                        .enumerate()
                        .rev()
                        .map(|(index, batch)| match batch {
                            MergeState::Vacant => (index, 0, 0),
                            MergeState::Single(_) => (index, 1, batch.len()),
                            MergeState::Double(_) => (index, 2, batch.len()),
                        }),
                );

            (exert_logic)(&self.exert_logic_param[..])
        })
    }

    /// Allocates a fueled `Spine` with a specified effort multiplier.
    ///
    /// This trace will merge batches progressively, with each inserted batch applying a multiple
    /// of the batch's length in effort to each merge. The `effort` parameter is that multiplier.
    /// This value should be at least one for the merging to happen; a value of zero is not helpful.
    pub fn with_effort(
        mut effort: usize,
        operator: OperatorInfo,
        logger: Option<differential_dataflow_next::logging::Logger>,
        activator: Option<timely_next::scheduling::activate::Activator>,
    ) -> Self {
        // Zero effort is .. not smart.
        if effort == 0 {
            effort = 1;
        }

        Spine {
            operator,
            logger,
            logical_frontier: Antichain::from_elem(
                <B::Time as timely_next::progress::Timestamp>::minimum(),
            ),
            physical_frontier: Antichain::from_elem(
                <B::Time as timely_next::progress::Timestamp>::minimum(),
            ),
            merging: Vec::new(),
            pending: Vec::new(),
            upper: Antichain::from_elem(<B::Time as timely_next::progress::Timestamp>::minimum()),
            effort,
            activator,
            exert_logic_param: Vec::default(),
            exert_logic: None,
            maintenance: VecDeque::new(),
            waker: None,
        }
    }

    /// Migrate data from `self.pending` into `self.merging`.
    ///
    /// This method reflects on the bookmarks held by others that may prevent merging, and in the
    /// case that new batches can be introduced to the pile of mergeable batches, it gets on that.
    #[inline(never)]
    fn consider_merges(&mut self) {
        if !self.drive_maintenance() {
            return;
        }
        while !self.pending.is_empty()
            && PartialOrder::less_equal(self.pending[0].upper(), &self.physical_frontier)
        {
            if span_len(&self.pending[0]) == 0 {
                if let Some(level) = self.merging.iter().position(|m| !m.is_vacant()) {
                    if self.merging[level].is_single() && self.merging[level].len() == 0 {
                        let batch = self.pending.remove(0);
                        self.insert_at(Some(batch), level);
                        self.maintenance.push_back(Maintenance::FuseEmpty {
                            level,
                            remaining: isize::MAX,
                        });
                        if !self.drive_maintenance() {
                            return;
                        }
                        continue;
                    }
                }
            }
            let level = usize::cast_from(
                span_len(&self.pending[0])
                    .next_power_of_two()
                    .trailing_zeros(),
            );
            self.queue_introduction(level, true);
            if !self.drive_maintenance() {
                return;
            }
        }
        if self.exert_effort().is_some() {
            self.activate();
        }
    }

    fn queue_fuel(&mut self, grant: isize) {
        self.maintenance.push_back(Maintenance::Fuel {
            level: 0,
            grant,
            remaining: grant,
        });
    }

    fn queue_introduction(&mut self, level: usize, pending: bool) {
        if !pending {
            crate::columnar::chunk::metrics::record(
                crate::columnar::chunk::metrics::Stage::VirtualIntroduction,
                1usize << level,
                0,
            );
        }
        assert!(self.maintenance.is_empty());
        // Preserve the fueled spine's virtual update accounting, independent of
        // cancellation and of how many times a read wakes its continuation.
        self.queue_fuel(((8usize << level) * self.effort).cast_signed());
        self.maintenance.push_back(Maintenance::RollUp {
            target: level,
            next: None,
            remaining: isize::MAX,
        });
        self.maintenance
            .push_back(Maintenance::Insert { level, pending });
        self.maintenance.push_back(Maintenance::Tidy);
    }

    fn drive_maintenance(&mut self) -> bool {
        let waker = self.waker.clone();
        let mut cx = Context::from_waker(waker.as_ref().unwrap_or(Waker::noop()));
        while let Some(mut action) = self.maintenance.pop_front() {
            let ready = match &mut action {
                Maintenance::Fuel {
                    level,
                    grant,
                    remaining,
                } => {
                    let mut ready = true;
                    while *level < self.merging.len() {
                        if self.merging[*level]
                            .poll_work(&mut cx, remaining)
                            .is_pending()
                        {
                            ready = false;
                            break;
                        }
                        if self.merging[*level].is_complete() {
                            let complete = self.complete_at(*level);
                            self.insert_at(complete, *level + 1);
                        }
                        *level += 1;
                        *remaining = *grant;
                    }
                    ready
                }
                Maintenance::RollUp {
                    target,
                    next,
                    remaining,
                } => {
                    while self.merging.len() <= *target {
                        self.merging.push(MergeState::Vacant);
                    }
                    if next.is_none() && self.merging[..*target].iter().any(|m| !m.is_vacant()) {
                        self.insert_at(None, 0);
                        *next = Some(0);
                    }
                    let mut ready = true;
                    if let Some(level) = next {
                        while *level < *target
                            || (*level == *target && self.merging[*target].is_double())
                        {
                            if self.merging[*level]
                                .poll_work(&mut cx, remaining)
                                .is_pending()
                            {
                                ready = false;
                                break;
                            }
                            // Extraction and installation cannot be separated by a yield.
                            let complete = self.complete_at(*level);
                            self.insert_at(complete, *level + 1);
                            *level += 1;
                            *remaining = isize::MAX;
                        }
                    }
                    ready
                }
                Maintenance::FuseEmpty { level, remaining } => {
                    if self.merging[*level]
                        .poll_work(&mut cx, remaining)
                        .is_pending()
                    {
                        false
                    } else {
                        let merged = self.complete_at(*level);
                        self.merging[*level] = MergeState::Single(merged);
                        true
                    }
                }
                Maintenance::Insert { level, pending } => {
                    let batch = if *pending {
                        Some(self.pending.remove(0))
                    } else {
                        None
                    };
                    self.insert_at(batch, *level);
                    true
                }
                Maintenance::Tidy => {
                    self.tidy_layers();
                    true
                }
            };
            if !ready {
                assert!(
                    waker.is_some(),
                    "pending trace maintenance requires a waker"
                );
                self.maintenance.push_front(action);
                return false;
            }
        }
        true
    }

    /// Inserts a batch at a specific location.
    ///
    /// This is a non-public internal method that can panic if we try and insert into a
    /// layer which already contains two batches (and is still in the process of merging).
    fn insert_at(&mut self, batch: Option<Span<B::Time, B>>, index: usize) {
        // Ensure the spine is large enough.
        while self.merging.len() <= index {
            self.merging.push(MergeState::Vacant);
        }

        // Insert the batch at the location.
        match self.merging[index].take() {
            MergeState::Vacant => {
                self.merging[index] = MergeState::Single(batch);
            }
            MergeState::Single(old) => {
                // Log the initiation of a merge.
                self.logger.as_ref().map(|l| {
                    l.log(differential_dataflow_next::logging::MergeEvent {
                        operator: self.operator.global_id,
                        scale: index,
                        length1: old.as_ref().map(span_len).unwrap_or(0),
                        length2: batch.as_ref().map(span_len).unwrap_or(0),
                        complete: None,
                    })
                });
                let compaction_frontier = self.logical_frontier.borrow();
                self.merging[index] = MergeState::begin_merge(old, batch, compaction_frontier);
            }
            MergeState::Double(_) => {
                panic!("Attempted to insert batch into incomplete merge!")
            }
        };
    }

    /// Extract a completed layer, preserving merge-completion logging.
    fn complete_at(&mut self, index: usize) -> Option<Span<B::Time, B>> {
        if let Some((merged, inputs)) = self.merging[index].complete() {
            if let Some((input1, input2)) = inputs {
                // Log the completion of a merge from existing parts.
                self.logger.as_ref().map(|l| {
                    l.log(differential_dataflow_next::logging::MergeEvent {
                        operator: self.operator.global_id,
                        scale: index,
                        length1: span_len(&input1),
                        length2: span_len(&input2),
                        complete: Some(span_len(&merged)),
                    })
                });
            }
            Some(merged)
        } else {
            None
        }
    }

    /// Attempts to draw down large layers to size appropriate layers.
    fn tidy_layers(&mut self) {
        // If the largest layer is complete (not merging), we can attempt
        // to draw it down to the next layer. This is permitted if we can
        // maintain our invariant that below each merge there are at most
        // half the records that would be required to invade the merge.
        if !self.merging.is_empty() {
            let mut length = self.merging.len();
            if self.merging[length - 1].is_single() {
                // To move a batch down, we require that it contain few
                // enough records that the lower level is appropriate,
                // and that moving the batch would not create a merge
                // violating our invariant.

                let appropriate_level = usize::cast_from(
                    self.merging[length - 1]
                        .len()
                        .next_power_of_two()
                        .trailing_zeros(),
                );

                // Continue only as far as is appropriate
                while appropriate_level < length - 1 {
                    match self.merging[length - 2].take() {
                        // Vacant or structurally empty batches can be absorbed.
                        MergeState::Vacant | MergeState::Single(None) => {
                            self.merging.remove(length - 2);
                            length = self.merging.len();
                        }
                        // Single batches may initiate a merge, if sizes are
                        // within bounds, but terminate the loop either way.
                        MergeState::Single(Some(batch)) => {
                            // Determine the number of records that might lead
                            // to a merge. Importantly, this is not the number
                            // of actual records, but the sum of upper bounds
                            // based on indices.
                            let mut smaller = 0;
                            for (index, batch) in self.merging[..(length - 2)].iter().enumerate() {
                                match batch {
                                    MergeState::Vacant => {}
                                    MergeState::Single(_) => {
                                        smaller += 1 << index;
                                    }
                                    MergeState::Double(_) => {
                                        smaller += 2 << index;
                                    }
                                }
                            }

                            if smaller <= (1 << length) / 8 {
                                self.merging.remove(length - 2);
                                self.insert_at(Some(batch), length - 2);
                            } else {
                                self.merging[length - 2] = MergeState::Single(Some(batch));
                            }
                            return;
                        }
                        // If a merge is in progress there is nothing to do.
                        MergeState::Double(state) => {
                            self.merging[length - 2] = MergeState::Double(state);
                            return;
                        }
                    }
                }
            }
        }
    }
}

/// Describes the state of a layer.
///
/// A layer can be empty, contain a single span, or contain a pair of spans
/// that are in the process of merging into a span for the next layer.
///
/// Note the two distinct kinds of nothing here: `Single(None)` is a *structural*
/// absence, a stand-in for virtual updates with no description at all, used for
/// fuel bookkeeping; a present span carrying no batch is a *recorded* update-free
/// interval of time, with a real description.
enum MergeState<B: SpineBatch> {
    /// An empty layer, containing no updates.
    Vacant,
    /// A layer containing a single span.
    ///
    /// The `None` variant is used to represent a structurally empty layer present
    /// to ensure the progress of maintenance work.
    Single(Option<Span<B::Time, B>>),
    /// A layer containing two spans, in the process of merging.
    Double(MergeVariant<B>),
}

impl<B: SpineBatch> MergeState<B> {
    /// The number of actual updates contained in the level.
    fn len(&self) -> usize {
        match self {
            MergeState::Single(Some(b)) => span_len(b),
            MergeState::Double(MergeVariant::InProgress(b1, b2, _, _)) => {
                span_len(b1) + span_len(b2)
            }
            MergeState::Double(MergeVariant::Complete(Some((b, _)))) => span_len(b),
            _ => 0,
        }
    }

    /// True only for the MergeState::Vacant variant.
    fn is_vacant(&self) -> bool {
        if let MergeState::Vacant = self {
            true
        } else {
            false
        }
    }

    /// True only for the MergeState::Single variant.
    fn is_single(&self) -> bool {
        if let MergeState::Single(_) = self {
            true
        } else {
            false
        }
    }

    /// True only for the MergeState::Double variant.
    fn is_double(&self) -> bool {
        if let MergeState::Double(_) = self {
            true
        } else {
            false
        }
    }

    /// Extract a completed merge or a single span.
    ///
    /// The result is either a batch, if there is a non-trivial batch to return
    /// or `None` if there is no meaningful batch to return. This does not distinguish
    /// between Vacant entries and structurally empty batches, which should be done
    /// with the `is_complete()` method.
    ///
    /// There is the additional option of input batches.
    fn complete(
        &mut self,
    ) -> Option<(
        Span<B::Time, B>,
        Option<(Span<B::Time, B>, Span<B::Time, B>)>,
    )> {
        match std::mem::replace(self, MergeState::Vacant) {
            MergeState::Vacant => None,
            MergeState::Single(batch) => batch.map(|b| (b, None)),
            MergeState::Double(variant) => variant.complete(),
        }
    }

    /// True iff the layer is a complete merge, ready for extraction.
    fn is_complete(&self) -> bool {
        if let MergeState::Double(MergeVariant::Complete(_)) = self {
            true
        } else {
            false
        }
    }

    /// Performs a bounded amount of work towards a merge.
    ///
    /// If the merge completes, the resulting batch is returned.
    /// If a batch is returned, it is the obligation of the caller
    /// to correctly install the result.
    fn poll_work(&mut self, cx: &mut Context<'_>, fuel: &mut isize) -> Poll<()> {
        if let MergeState::Double(layer) = self {
            layer.poll_work(cx, fuel)
        } else {
            Poll::Ready(())
        }
    }

    /// Extract the merge state, typically temporarily.
    fn take(&mut self) -> Self {
        std::mem::replace(self, MergeState::Vacant)
    }

    /// Initiates the merge of an "old" batch with a "new" batch.
    ///
    /// The upper frontier of the old batch should match the lower
    /// frontier of the new batch, with the resulting batch describing
    /// their composed interval, from the lower frontier of the old
    /// batch to the upper frontier of the new batch.
    ///
    /// Either batch may be `None` which corresponds to a structurally
    /// empty batch whose upper and lower frontiers are equal. This
    /// option exists purely for bookkeeping purposes, and no computation
    /// is performed to merge the two batches.
    fn begin_merge(
        batch1: Option<Span<B::Time, B>>,
        batch2: Option<Span<B::Time, B>>,
        compaction_frontier: AntichainRef<B::Time>,
    ) -> MergeState<B> {
        let variant = match (batch1, batch2) {
            (Some(batch1), Some(batch2)) => {
                assert!(batch1.upper() == batch2.lower());
                let since = batch1
                    .desc
                    .since()
                    .join(batch2.desc.since())
                    .join(&compaction_frontier.to_owned());
                let description =
                    Description::new(batch1.lower().clone(), batch2.upper().clone(), since);
                match (&batch1.inner, &batch2.inner) {
                    (Some(source1), Some(source2)) => {
                        crate::columnar::chunk::metrics::record(
                            crate::columnar::chunk::metrics::Stage::TraceMerge,
                            source1.len() + source2.len(),
                            0,
                        );
                        let merger = B::Merger::new(source1, source2, description.since().borrow());
                        MergeVariant::InProgress(batch1, batch2, description, merger)
                    }
                    // With at most one side carrying updates there is nothing to merge, and the
                    // surviving updates' times are not advanced by the compaction frontier.
                    _ => {
                        let inner = batch1.inner.or(batch2.inner);
                        MergeVariant::Complete(Some((Span::new(description, inner), None)))
                    }
                }
            }
            (None, Some(x)) => MergeVariant::Complete(Some((x, None))),
            (Some(x), None) => MergeVariant::Complete(Some((x, None))),
            (None, None) => MergeVariant::Complete(None),
        };

        MergeState::Double(variant)
    }
}

enum MergeVariant<B: SpineBatch> {
    /// Describes an actual in-progress merge between two non-trivial batches.
    ///
    /// Beyond the two source batches, this records the description their merge
    /// will bear (their composed interval, with the compaction frontier as its
    /// `since`) and the merger itself.
    InProgress(
        Span<B::Time, B>,
        Span<B::Time, B>,
        Description<B::Time>,
        B::Merger,
    ),
    /// A merge that requires no further work. May or may not represent a non-trivial batch.
    Complete(
        Option<(
            Span<B::Time, B>,
            Option<(Span<B::Time, B>, Span<B::Time, B>)>,
        )>,
    ),
}

impl<B: SpineBatch> MergeVariant<B> {
    /// Extract a completed batch, unless structurally empty.
    ///
    /// The result is either `None`, for structurally empty batches,
    /// or a batch and optionally input batches from which it derived.
    fn complete(
        self,
    ) -> Option<(
        Span<B::Time, B>,
        Option<(Span<B::Time, B>, Span<B::Time, B>)>,
    )> {
        if let MergeVariant::Complete(batch) = self {
            batch
        } else {
            panic!("Failed to complete a merge!");
        }
    }

    /// Applies some amount of work, potentially completing the merge.
    ///
    /// In case the work completes, the source batches are returned.
    /// This allows the caller to manage the released resources.
    fn poll_work(&mut self, cx: &mut Context<'_>, fuel: &mut isize) -> Poll<()> {
        if let MergeVariant::InProgress(b1, b2, _, merge) = self {
            match merge.poll_work(
                b1.inner.as_ref().unwrap(),
                b2.inner.as_ref().unwrap(),
                cx,
                fuel,
            ) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(MergeStatus::InProgress) => return Poll::Ready(()),
                Poll::Ready(MergeStatus::Complete) => {}
            }
            if let MergeVariant::InProgress(b1, b2, description, merge) =
                std::mem::replace(self, MergeVariant::Complete(None))
            {
                let inner = merge.done();
                *self =
                    MergeVariant::Complete(Some((Span::new(description, inner), Some((b1, b2)))));
            }
        }
        Poll::Ready(())
    }
}

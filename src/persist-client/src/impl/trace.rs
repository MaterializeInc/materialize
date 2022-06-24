// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! An append-only collection of compactable update batches. The Spine below is
//! a fork of Differential Dataflow's [Spine] with minimal modifications. The
//! original Spine code is designed for incremental (via "fuel"ing) synchronous
//! merge of in-memory batches. Persist doesn't want compaction to block
//! incoming writes and, in fact, may in the future elect to push the work of
//! compaction onto another machine entirely via RPC. As a result, we abuse the
//! Spine code as follows:
//!
//! [Spine]: differential_dataflow::trace::implementations::spine_fueled::Spine
//!
//! - The normal Spine works in terms of [Batch] impls. A `Batch` is added to
//!   the Spine. As progress is made, the Spine will merge two batches together
//!   by: constructing a [Batch::Merger], giving it bits of fuel to
//!   incrementally perform the merge (which spreads out the work, keeping
//!   latencies even), and then once it's done fueling extracting the new single
//!   output `Batch` and discarding the inputs.
//! - Persist instead represents a batch of blob data with a [HollowBatch]
//!   pointer which contains the normal `Batch` metadata plus the keys necessary
//!   to retrieve the updates.
//! - [SpineBatch] wraps `HollowBatch` and has a [FuelingMerge] companion
//!   (analogous to `Batch::Merger`) that allows us to represent a merge as it
//!   is fueling. Normally, this would represent real incremental compaction
//!   progress, but in persist, it's simply a bookkeeping mechanism. Once fully
//!   fueled, the `FuelingMerge` is turned into a [SpineBatch::Fueled] variant,
//!   which to the Spine is indistinguishable from a merged batch. At this
//!   point, it is eligible for asynchronous compaction and a `FueledMergeReq`
//!   is generated.
//! - At any later point, this request may be answered via
//!   [Trace::apply_merge_res]. This internally replaces the
//!   `SpineBatch::Fueled` with a `SpineBatch::Merged`, which has no effect on
//!   the `Spine` but replaces the metadata in persist's state to point at the
//!   new batch.
//! - `SpineBatch` is explictly allowed to accumulate a list of `HollowBatch`s.
//!   This decouples compaction from Spine progress and also allows us to reduce
//!   write amplification by merging `N` batches at once where `N` can be
//!   greater than 2.
//!
//! [Batch]: differential_dataflow::trace::Batch
//! [Batch::Merger]: differential_dataflow::trace::Batch::Merger

use std::fmt::Debug;

use differential_dataflow::lattice::Lattice;
use differential_dataflow::trace::Description;
use timely::progress::frontier::AntichainRef;
use timely::progress::{Antichain, Timestamp};
use timely::PartialOrder;

use crate::r#impl::state::HollowBatch;

#[derive(Debug, Clone, PartialEq)]
pub struct FueledMergeReq<T> {
    pub desc: Description<T>,
    pub inputs: Vec<HollowBatch<T>>,
}

#[derive(Debug)]
pub struct FueledMergeRes<T> {
    pub output: HollowBatch<T>,
}

/// An append-only collection of compactable update batches.
///
/// In an effort to keep our fork of Spine as close as possible to the original,
/// we push as many changes as possible into this wrapper.
#[derive(Debug, Clone)]
pub struct Trace<T> {
    spine: Spine<T>,
    merge_reqs: Vec<FueledMergeReq<T>>,
}

impl<T: Timestamp + Lattice> Default for Trace<T> {
    fn default() -> Self {
        Self {
            spine: Spine::new(),
            merge_reqs: Vec::new(),
        }
    }
}

impl<T: Timestamp + Lattice> Trace<T> {
    pub fn since(&self) -> &Antichain<T> {
        &self.spine.since
    }

    pub fn upper(&self) -> &Antichain<T> {
        &self.spine.upper
    }

    pub fn downgrade_since(&mut self, since: Antichain<T>) {
        self.spine.since = since;
    }

    pub fn push_batch(&mut self, batch: HollowBatch<T>) {
        let mut merge_reqs = Vec::new();
        self.spine
            .insert(SpineBatch::Merged(batch), &mut merge_reqs);
        // Spine::roll_up (internally used by insert) clears all batches out of
        // levels below a target by walking up from level 0 and merging each
        // level into the next (providing the necessary fuel). In practice, this
        // means we'll get a series of requests like `(a, b), (a, b, c), ...`.
        // It's a waste to do all of these (we'll throw away the results), so we
        // filter out any that are entirely covered by some other request.
        let mut merge_reqs = Self::remove_redundant_merge_reqs(merge_reqs);
        self.merge_reqs.append(&mut merge_reqs);
    }

    pub fn map_batches<F: FnMut(&HollowBatch<T>)>(&self, mut f: F) {
        self.spine.map_batches(move |b| match b {
            SpineBatch::Merged(b) => f(b),
            SpineBatch::Fueled { parts, .. } => {
                for b in parts.iter() {
                    f(b);
                }
            }
        })
    }

    pub fn take_merge_reqs(&mut self) -> Vec<FueledMergeReq<T>> {
        std::mem::take(&mut self.merge_reqs)
    }

    pub fn apply_merge_res(&mut self, res: &FueledMergeRes<T>) -> bool {
        let mut applied = false;
        for batch in self.spine.merging.iter_mut().rev() {
            if applied {
                break;
            }
            match batch {
                MergeState::Double(MergeVariant::InProgress(batch1, batch2, m)) => {
                    if batch1.maybe_replace(res) {
                        applied = true;
                        // There's a second copy here as m.b1, which is what
                        // actually gets used when FuelingMerge::done gets
                        // called, so update this one too.
                        assert!(m.b1.maybe_replace(res));
                    }
                    if batch2.maybe_replace(res) {
                        applied = true;
                        // There's a second copy here as m.b2, which is what
                        // actually gets used when FuelingMerge::done gets
                        // called, so update this one too.
                        assert!(m.b2.maybe_replace(res));
                    }
                }
                MergeState::Double(MergeVariant::Complete(Some((batch, _)))) => {
                    applied = applied || batch.maybe_replace(res);
                }
                MergeState::Single(Some(batch)) => {
                    applied = applied || batch.maybe_replace(res);
                }
                _ => {}
            }
        }

        applied
    }

    #[cfg(test)]
    pub fn num_spine_batches(&self) -> usize {
        let mut ret = 0;
        self.spine.map_batches(|_| ret += 1);
        ret
    }

    #[cfg(test)]
    pub fn num_hollow_batches(&self) -> usize {
        let mut ret = 0;
        self.map_batches(|_| ret += 1);
        ret
    }

    #[cfg(test)]
    pub fn num_updates(&self) -> usize {
        let mut ret = 0;
        self.map_batches(|b| ret += b.len);
        ret
    }

    // This is only called with the results of one `insert` and so the length of
    // `merge_reqs` is bounded by the number of levels in the spine (or possibly
    // some small constant multiple?). The number of levels is logarithmic in
    // number of updates in the spine, so this number should stay very small. As
    // a result, we simply use the naive O(n^2) algorithm here instead of doing
    // anything fancy with e.g. interval trees.
    fn remove_redundant_merge_reqs(
        mut merge_reqs: Vec<FueledMergeReq<T>>,
    ) -> Vec<FueledMergeReq<T>> {
        // Returns true if b0 covers b1, false otherwise.
        fn covers<T: PartialOrder>(b0: &FueledMergeReq<T>, b1: &FueledMergeReq<T>) -> bool {
            PartialOrder::less_equal(b0.desc.lower(), b1.desc.lower())
                && PartialOrder::less_equal(b1.desc.upper(), b0.desc.upper())
                && b0.desc.since() == b1.desc.since()
        }

        let mut ret = Vec::<FueledMergeReq<T>>::with_capacity(merge_reqs.len());
        // In practice, merge_reqs will come in sorted such that the "large"
        // requests are later. Take advantage of this by processing back to
        // front.
        while let Some(merge_req) = merge_reqs.pop() {
            let covered = ret.iter().any(|r| covers(r, &merge_req));
            if !covered {
                // Now check if anything we've already staged is covered by this
                // new req. In practice, the merge_reqs come in sorted and so
                // this `retain` is a no-op.
                ret.retain(|r| !covers(&merge_req, r));
                ret.push(merge_req);
            }
        }
        ret
    }
}

#[derive(Debug, Clone)]
enum SpineBatch<T> {
    Merged(HollowBatch<T>),
    Fueled {
        desc: Description<T>,
        parts: Vec<HollowBatch<T>>,
    },
}

impl<T: Timestamp + Lattice> SpineBatch<T> {
    pub fn lower(&self) -> &Antichain<T> {
        self.desc().lower()
    }

    pub fn upper(&self) -> &Antichain<T> {
        self.desc().upper()
    }

    fn desc(&self) -> &Description<T> {
        match self {
            SpineBatch::Merged(HollowBatch { desc, .. }) => desc,
            SpineBatch::Fueled { desc, .. } => desc,
        }
    }

    pub fn len(&self) -> usize {
        match self {
            SpineBatch::Merged(HollowBatch { len, .. }) => *len,
            // NB: This is an upper bound on len, we won't know for sure until
            // we compact it.
            SpineBatch::Fueled { parts, .. } => parts.iter().map(|b| b.len).sum(),
        }
    }

    pub fn empty(lower: Antichain<T>, upper: Antichain<T>, since: Antichain<T>) -> Self {
        SpineBatch::Merged(HollowBatch {
            desc: Description::new(lower, upper, since),
            keys: vec![],
            len: 0,
        })
    }

    pub fn begin_merge(
        b1: &Self,
        b2: &Self,
        compaction_frontier: Option<AntichainRef<T>>,
    ) -> FuelingMerge<T> {
        let mut since = b1.desc().since().join(b2.desc().since());
        if let Some(compaction_frontier) = compaction_frontier {
            since = since.join(&compaction_frontier.to_owned());
        }
        FuelingMerge {
            b1: b1.clone(),
            b2: b2.clone(),
            since,
            progress: 0,
        }
    }

    fn maybe_replace(&mut self, res: &FueledMergeRes<T>) -> bool {
        // We could perhaps be more lenient here and swap out a subset of the
        // parts of a Fueled if they happen to match this Res. E.g. if the res
        // is for `[1,3)` and the parts are `[0,1),[1,2),[2,3),[3,4)`, we could
        // swap out the middle two parts for this res. This situation could
        // happen if we generate a merge req and then, before we get a res,
        // merge that batch again. See if this matters in practice before we add
        // the complexity.
        if self.desc() != &res.output.desc {
            return false;
        }
        // Spine internally has an invariant about a batch being at some level
        // or higher based on the len. We could end up violating this invariant
        // if we increased the length of the batch. Compaction should never
        // increase the length, so we'll only hit this assert if there's a bug
        // somewhere.
        assert!(self.len() >= res.output.len);

        *self = SpineBatch::Merged(res.output.clone());
        true
    }
}

#[derive(Debug, Clone)]
pub struct FuelingMerge<T> {
    b1: SpineBatch<T>,
    b2: SpineBatch<T>,
    since: Antichain<T>,
    progress: usize,
}

impl<T: Timestamp + Lattice> FuelingMerge<T> {
    /// Perform some amount of work, decrementing `fuel`.
    ///
    /// If `fuel` is non-zero after the call, the merging is complete and one
    /// should call `done` to extract the merged results.
    fn work(&mut self, _: &SpineBatch<T>, _: &SpineBatch<T>, fuel: &mut isize) {
        let remaining = self.b1.len() + self.b2.len() - self.progress;
        #[allow(clippy::cast_sign_loss)]
        let used = std::cmp::min(*fuel as usize, remaining);
        self.progress += used;
        *fuel -= used as isize;
    }

    /// Extracts merged results.
    ///
    /// This method should only be called after `work` has been called and has
    /// not brought `fuel` to zero. Otherwise, the merge is still in progress.
    fn done(self, merge_reqs: &mut Vec<FueledMergeReq<T>>) -> SpineBatch<T> {
        let lower = self.b1.desc().lower().clone();
        let upper = self.b2.desc().upper().clone();
        let since = self.since;

        // Special case empty batches.
        if self.b1.len() == 0 && self.b2.len() == 0 {
            return SpineBatch::empty(lower, upper, since);
        }

        let desc = Description::new(lower, upper, since);

        let mut merged_parts = Vec::new();
        let mut append_parts = |b| match b {
            SpineBatch::Merged(b) => merged_parts.push(b),
            SpineBatch::Fueled { parts, .. } => merged_parts.extend_from_slice(&parts),
        };
        append_parts(self.b1);
        append_parts(self.b2);

        merge_reqs.push(FueledMergeReq {
            desc: desc.clone(),
            inputs: merged_parts.clone(),
        });

        SpineBatch::Fueled {
            desc,
            parts: merged_parts,
        }
    }
}

/// An append-only collection of update batches.
///
/// The `Spine` is a general-purpose trace implementation based on collection
/// and merging immutable batches of updates. It is generic with respect to the
/// batch type, and can be instantiated for any implementor of `trace::Batch`.
///
/// ## Design
///
/// This spine is represented as a list of layers, where each element in the
/// list is either
///
///   1. MergeState::Vacant  empty
///   2. MergeState::Single  a single batch
///   3. MergeState::Double  a pair of batches
///
/// Each "batch" has the option to be `None`, indicating a non-batch that
/// nonetheless acts as a number of updates proportionate to the level at which
/// it exists (for bookkeeping).
///
/// Each of the batches at layer i contains at most 2^i elements. The sequence
/// of batches should have the upper bound of one match the lower bound of the
/// next. Batches may be logically empty, with matching upper and lower bounds,
/// as a bookkeeping mechanism.
///
/// Each batch at layer i is treated as if it contains exactly 2^i elements,
/// even though it may actually contain fewer elements. This allows us to
/// decouple the physical representation from logical amounts of effort invested
/// in each batch. It allows us to begin compaction and to reduce the number of
/// updates, without compromising our ability to continue to move updates along
/// the spine. We are explicitly making the trade-off that while some batches
/// might compact at lower levels, we want to treat them as if they contained
/// their full set of updates for accounting reasons (to apply work to higher
/// levels).
///
/// We maintain the invariant that for any in-progress merge at level k there
/// should be fewer than 2^k records at levels lower than k. That is, even if we
/// were to apply an unbounded amount of effort to those records, we would not
/// have enough records to prompt a merge into the in-progress merge. Ideally,
/// we maintain the extended invariant that for any in-progress merge at level
/// k, the remaining effort required (number of records minus applied effort) is
/// less than the number of records that would need to be added to reach 2^k
/// records in layers below.
///
/// ## Mathematics
///
/// When a merge is initiated, there should be a non-negative *deficit* of
/// updates before the layers below could plausibly produce a new batch for the
/// currently merging layer. We must determine a factor of proportionality, so
/// that newly arrived updates provide at least that amount of "fuel" towards
/// the merging layer, so that the merge completes before lower levels invade.
///
/// ### Deficit:
///
/// A new merge is initiated only in response to the completion of a prior
/// merge, or the introduction of new records from outside. The latter case is
/// special, and will maintain our invariant trivially, so we will focus on the
/// former case.
///
/// When a merge at level k completes, assuming we have maintained our invariant
/// then there should be fewer than 2^k records at lower levels. The newly
/// created merge at level k+1 will require up to 2^k+2 units of work, and
/// should not expect a new batch until strictly more than 2^k records are
/// added. This means that a factor of proportionality of four should be
/// sufficient to ensure that the merge completes before a new merge is
/// initiated.
///
/// When new records get introduced, we will need to roll up any batches at
/// lower levels, which we treat as the introduction of records. Each of these
/// virtual records introduced should either be accounted for the fuel it should
/// contribute, as it results in the promotion of batches closer to in-progress
/// merges.
///
/// ### Fuel sharing
///
/// We like the idea of applying fuel preferentially to merges at *lower*
/// levels, under the idea that they are easier to complete, and we benefit from
/// fewer total merges in progress. This does delay the completion of merges at
/// higher levels, and may not obviously be a total win. If we choose to do
/// this, we should make sure that we correctly account for completed merges at
/// low layers: they should still extract fuel from new updates even though they
/// have completed, at least until they have paid back any "debt" to higher
/// layers by continuing to provide fuel as updates arrive.
#[derive(Debug, Clone)]
struct Spine<T> {
    effort: usize,
    since: Antichain<T>,
    upper: Antichain<T>,
    merging: Vec<MergeState<T>>,
}

impl<T: Timestamp + Lattice> Spine<T> {
    /// Allocates a fueled `Spine`.
    ///
    /// This trace will merge batches progressively, with each inserted batch
    /// applying a multiple of the batch's length in effort to each merge. The
    /// `effort` parameter is that multiplier. This value should be at least one
    /// for the merging to happen; a value of zero is not helpful.
    pub fn new() -> Self {
        Spine {
            effort: 1,
            since: Antichain::from_elem(T::minimum()),
            upper: Antichain::from_elem(T::minimum()),
            merging: Vec::new(),
        }
    }

    // Ideally, this method acts as insertion of `batch`, even if we are not yet
    // able to begin merging the batch. This means it is a good time to perform
    // amortized work proportional to the size of batch.
    pub fn insert(&mut self, batch: SpineBatch<T>, merge_reqs: &mut Vec<FueledMergeReq<T>>) {
        assert!(batch.lower() != batch.upper());
        assert_eq!(batch.lower(), &self.upper);

        self.upper.clone_from(batch.upper());

        // If `batch` and the most recently inserted batch are both empty,
        // we can just fuse them. We can also replace a structurally empty
        // batch with this empty batch, preserving the apparent record count
        // but now with non-trivial lower and upper bounds.
        if batch.len() == 0 {
            if let Some(position) = self.merging.iter().position(|m| !m.is_vacant()) {
                if self.merging[position].is_single() && self.merging[position].len() == 0 {
                    self.insert_at(Some(batch), position);
                    let merged = self.complete_at(position, merge_reqs);
                    self.merging[position] = MergeState::Single(merged);
                    return;
                }
            }
        }

        // Normal insertion for the batch.
        let index = batch.len().next_power_of_two();
        self.introduce_batch(Some(batch), index.trailing_zeros() as usize, merge_reqs);
    }

    /// Apply some amount of effort to trace maintenance.
    ///
    /// The units of effort are updates, and the method should be thought of as
    /// analogous to inserting as many empty updates, where the trace is
    /// permitted to perform proportionate work.
    #[allow(dead_code)]
    pub fn exert(&mut self, effort: &mut isize, merge_reqs: &mut Vec<FueledMergeReq<T>>) {
        // If there is work to be done, ...
        self.tidy_layers();
        if !self.reduced() {
            // If any merges exist, we can directly call `apply_fuel`.
            if self.merging.iter().any(|b| b.is_double()) {
                self.apply_fuel(effort, merge_reqs);
            }
            // Otherwise, we'll need to introduce fake updates to move merges
            // along.
            else {
                // Introduce an empty batch with roughly *effort number of
                // virtual updates.
                #[allow(clippy::cast_sign_loss)]
                let level = (*effort as usize).next_power_of_two().trailing_zeros() as usize;
                self.introduce_batch(None, level, merge_reqs);
            }
        }
    }

    pub fn map_batches<F: FnMut(&SpineBatch<T>)>(&self, mut f: F) {
        for batch in self.merging.iter().rev() {
            match batch {
                MergeState::Double(MergeVariant::InProgress(batch1, batch2, _)) => {
                    f(batch1);
                    f(batch2);
                }
                MergeState::Double(MergeVariant::Complete(Some((batch, _)))) => f(batch),
                MergeState::Single(Some(batch)) => f(batch),
                _ => {}
            }
        }
    }

    /// True iff there is at most one non-empty batch in `self.merging`.
    ///
    /// When true, there is no maintenance work to perform in the trace, other
    /// than compaction. We do not yet have logic in place to determine if
    /// compaction would improve a trace, so for now we are ignoring that.
    fn reduced(&self) -> bool {
        let mut non_empty = 0;
        for index in 0..self.merging.len() {
            if self.merging[index].is_double() {
                return false;
            }
            if self.merging[index].len() > 0 {
                non_empty += 1;
            }
            if non_empty > 1 {
                return false;
            }
        }
        true
    }

    /// Describes the merge progress of layers in the trace.
    ///
    /// Intended for diagnostics rather than public consumption.
    #[allow(dead_code)]
    fn describe(&self) -> Vec<(usize, usize)> {
        self.merging
            .iter()
            .map(|b| match b {
                MergeState::Vacant => (0, 0),
                x @ MergeState::Single(_) => (1, x.len()),
                x @ MergeState::Double(_) => (2, x.len()),
            })
            .collect()
    }

    /// Introduces a batch at an indicated level.
    ///
    /// The level indication is often related to the size of the batch, but it
    /// can also be used to artificially fuel the computation by supplying empty
    /// batches at non-trivial indices, to move merges along.
    fn introduce_batch(
        &mut self,
        batch: Option<SpineBatch<T>>,
        batch_index: usize,
        merge_reqs: &mut Vec<FueledMergeReq<T>>,
    ) {
        // Step 0.  Determine an amount of fuel to use for the computation.
        //
        //          Fuel is used to drive maintenance of the data structure,
        //          and in particular are used to make progress through merges
        //          that are in progress. The amount of fuel to use should be
        //          proportional to the number of records introduced, so that
        //          we are guaranteed to complete all merges before they are
        //          required as arguments to merges again.
        //
        //          The fuel use policy is negotiable, in that we might aim
        //          to use relatively less when we can, so that we return
        //          control promptly, or we might account more work to larger
        //          batches. Not clear to me which are best, of if there
        //          should be a configuration knob controlling this.

        // The amount of fuel to use is proportional to 2^batch_index, scaled by
        // a factor of self.effort which determines how eager we are in
        // performing maintenance work. We need to ensure that each merge in
        // progress receives fuel for each introduced batch, and so multiply by
        // that as well.
        if batch_index > 32 {
            println!("Large batch index: {}", batch_index);
        }

        // We believe that eight units of fuel is sufficient for each introduced
        // record, accounted as four for each record, and a potential four more
        // for each virtual record associated with promoting existing smaller
        // batches. We could try and make this be less, or be scaled to merges
        // based on their deficit at time of instantiation. For now, we remain
        // conservative.
        let mut fuel = 8 << batch_index;
        // Scale up by the effort parameter, which is calibrated to one as the
        // minimum amount of effort.
        fuel *= self.effort;
        // Convert to an `isize` so we can observe any fuel shortfall.
        let mut fuel = fuel as isize;

        // Step 1.  Apply fuel to each in-progress merge.
        //
        //          Before we can introduce new updates, we must apply any
        //          fuel to in-progress merges, as this fuel is what ensures
        //          that the merges will be complete by the time we insert
        //          the updates.
        self.apply_fuel(&mut fuel, merge_reqs);

        // Step 2.  We must ensure the invariant that adjacent layers do not
        //          contain two batches will be satisfied when we insert the
        //          batch. We forcibly completing all merges at layers lower
        //          than and including `batch_index`, so that the new batch is
        //          inserted into an empty layer.
        //
        //          We could relax this to "strictly less than `batch_index`"
        //          if the layer above has only a single batch in it, which
        //          seems not implausible if it has been the focus of effort.
        //
        //          This should be interpreted as the introduction of some
        //          volume of fake updates, and we will need to fuel merges
        //          by a proportional amount to ensure that they are not
        //          surprised later on. The number of fake updates should
        //          correspond to the deficit for the layer, which perhaps
        //          we should track explicitly.
        self.roll_up(batch_index, merge_reqs);

        // Step 3. This insertion should be into an empty layer. It is a logical
        //         error otherwise, as we may be violating our invariant, from
        //         which all wonderment derives.
        self.insert_at(batch, batch_index);

        // Step 4. Tidy the largest layers.
        //
        //         It is important that we not tidy only smaller layers,
        //         as their ascension is what ensures the merging and
        //         eventual compaction of the largest layers.
        self.tidy_layers();
    }

    /// Ensures that an insertion at layer `index` will succeed.
    ///
    /// This method is subject to the constraint that all existing batches
    /// should occur at higher levels, which requires it to "roll up" batches
    /// present at lower levels before the method is called. In doing this, we
    /// should not introduce more virtual records than 2^index, as that is the
    /// amount of excess fuel we have budgeted for completing merges.
    fn roll_up(&mut self, index: usize, merge_reqs: &mut Vec<FueledMergeReq<T>>) {
        // Ensure entries sufficient for `index`.
        while self.merging.len() <= index {
            self.merging.push(MergeState::Vacant);
        }

        // We only need to roll up if there are non-vacant layers.
        if self.merging[..index].iter().any(|m| !m.is_vacant()) {
            // Collect and merge all batches at layers up to but not including
            // `index`.
            let mut merged = None;
            for i in 0..index {
                self.insert_at(merged, i);
                merged = self.complete_at(i, merge_reqs);
            }

            // The merged results should be introduced at level `index`, which
            // should be ready to absorb them (possibly creating a new merge at
            // the time).
            self.insert_at(merged, index);

            // If the insertion results in a merge, we should complete it to
            // ensure the upcoming insertion at `index` does not panic.
            if self.merging[index].is_double() {
                let merged = self.complete_at(index, merge_reqs);
                self.insert_at(merged, index + 1);
            }
        }
    }

    /// Applies an amount of fuel to merges in progress.
    ///
    /// The supplied `fuel` is for each in progress merge, and if we want to
    /// spend the fuel non-uniformly (e.g. prioritizing merges at low layers) we
    /// could do so in order to maintain fewer batches on average (at the risk
    /// of completing merges of large batches later, but tbh probably not much
    /// later).
    pub fn apply_fuel(&mut self, fuel: &mut isize, merge_reqs: &mut Vec<FueledMergeReq<T>>) {
        // For the moment our strategy is to apply fuel independently to each
        // merge in progress, rather than prioritizing small merges. This sounds
        // like a great idea, but we need better accounting in place to ensure
        // that merges that borrow against later layers but then complete still
        // "acquire" fuel to pay back their debts.
        for index in 0..self.merging.len() {
            // Give each level independent fuel, for now.
            let mut fuel = *fuel;
            // Pass along various logging stuffs, in case we need to report
            // success.
            self.merging[index].work(&mut fuel, merge_reqs);
            // `fuel` could have a deficit at this point, meaning we over-spent
            // when we took a merge step. We could ignore this, or maintain the
            // deficit and account future fuel against it before spending again.
            // It isn't clear why that would be especially helpful to do; we
            // might want to avoid overspends at multiple layers in the same
            // invocation (to limit latencies), but there is probably a rich
            // policy space here.

            // If a merge completes, we can immediately merge it in to the next
            // level, which is "guaranteed" to be complete at this point, by our
            // fueling discipline.
            if self.merging[index].is_complete() {
                let complete = self.complete_at(index, merge_reqs);
                self.insert_at(complete, index + 1);
            }
        }
    }

    /// Inserts a batch at a specific location.
    ///
    /// This is a non-public internal method that can panic if we try and insert
    /// into a layer which already contains two batches (and is still in the
    /// process of merging).
    fn insert_at(&mut self, batch: Option<SpineBatch<T>>, index: usize) {
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
                let compaction_frontier = Some(self.since.borrow());
                self.merging[index] = MergeState::begin_merge(old, batch, compaction_frontier);
            }
            MergeState::Double(_) => {
                panic!("Attempted to insert batch into incomplete merge!")
            }
        };
    }

    /// Completes and extracts what ever is at layer `index`.
    fn complete_at(
        &mut self,
        index: usize,
        merge_reqs: &mut Vec<FueledMergeReq<T>>,
    ) -> Option<SpineBatch<T>> {
        if let Some((merged, _)) = self.merging[index].complete(merge_reqs) {
            Some(merged)
        } else {
            None
        }
    }

    /// Attempts to draw down large layers to size appropriate layers.
    fn tidy_layers(&mut self) {
        // If the largest layer is complete (not merging), we can attempt to
        // draw it down to the next layer. This is permitted if we can maintain
        // our invariant that below each merge there are at most half the
        // records that would be required to invade the merge.
        if !self.merging.is_empty() {
            let mut length = self.merging.len();
            if self.merging[length - 1].is_single() {
                // To move a batch down, we require that it contain few enough
                // records that the lower level is appropriate, and that moving
                // the batch would not create a merge violating our invariant.

                let appropriate_level = self.merging[length - 1]
                    .len()
                    .next_power_of_two()
                    .trailing_zeros() as usize;

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
/// A layer can be empty, contain a single batch, or contain a pair of batches
/// that are in the process of merging into a batch for the next layer.
#[derive(Debug, Clone)]
enum MergeState<T> {
    /// An empty layer, containing no updates.
    Vacant,
    /// A layer containing a single batch.
    ///
    /// The `None` variant is used to represent a structurally empty batch
    /// present to ensure the progress of maintenance work.
    Single(Option<SpineBatch<T>>),
    /// A layer containing two batches, in the process of merging.
    Double(MergeVariant<T>),
}

impl<T: Timestamp + Lattice> MergeState<T> {
    /// The number of actual updates contained in the level.
    fn len(&self) -> usize {
        match self {
            MergeState::Single(Some(b)) => b.len(),
            MergeState::Double(MergeVariant::InProgress(b1, b2, _)) => b1.len() + b2.len(),
            MergeState::Double(MergeVariant::Complete(Some((b, _)))) => b.len(),
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

    /// Immediately complete any merge.
    ///
    /// The result is either a batch, if there is a non-trivial batch to return
    /// or `None` if there is no meaningful batch to return. This does not
    /// distinguish between Vacant entries and structurally empty batches, which
    /// should be done with the `is_complete()` method.
    ///
    /// There is the additional option of input batches.
    fn complete(
        &mut self,
        merge_reqs: &mut Vec<FueledMergeReq<T>>,
    ) -> Option<(SpineBatch<T>, Option<(SpineBatch<T>, SpineBatch<T>)>)> {
        match std::mem::replace(self, MergeState::Vacant) {
            MergeState::Vacant => None,
            MergeState::Single(batch) => batch.map(|b| (b, None)),
            MergeState::Double(variant) => variant.complete(merge_reqs),
        }
    }

    /// True iff the layer is a complete merge, ready for extraction.
    fn is_complete(&mut self) -> bool {
        if let MergeState::Double(MergeVariant::Complete(_)) = self {
            true
        } else {
            false
        }
    }

    /// Performs a bounded amount of work towards a merge.
    ///
    /// If the merge completes, the resulting batch is returned. If a batch is
    /// returned, it is the obligation of the caller to correctly install the
    /// result.
    fn work(&mut self, fuel: &mut isize, merge_reqs: &mut Vec<FueledMergeReq<T>>) {
        // We only perform work for merges in progress.
        if let MergeState::Double(layer) = self {
            layer.work(fuel, merge_reqs)
        }
    }

    /// Extract the merge state, typically temporarily.
    fn take(&mut self) -> Self {
        std::mem::replace(self, MergeState::Vacant)
    }

    /// Initiates the merge of an "old" batch with a "new" batch.
    ///
    /// The upper frontier of the old batch should match the lower frontier of
    /// the new batch, with the resulting batch describing their composed
    /// interval, from the lower frontier of the old batch to the upper frontier
    /// of the new batch.
    ///
    /// Either batch may be `None` which corresponds to a structurally empty
    /// batch whose upper and lower frontiers are equal. This option exists
    /// purely for bookkeeping purposes, and no computation is performed to
    /// merge the two batches.
    fn begin_merge(
        batch1: Option<SpineBatch<T>>,
        batch2: Option<SpineBatch<T>>,
        compaction_frontier: Option<AntichainRef<T>>,
    ) -> MergeState<T> {
        let variant = match (batch1, batch2) {
            (Some(batch1), Some(batch2)) => {
                assert!(batch1.upper() == batch2.lower());
                let begin_merge = SpineBatch::begin_merge(&batch1, &batch2, compaction_frontier);
                MergeVariant::InProgress(batch1, batch2, begin_merge)
            }
            (None, Some(x)) => MergeVariant::Complete(Some((x, None))),
            (Some(x), None) => MergeVariant::Complete(Some((x, None))),
            (None, None) => MergeVariant::Complete(None),
        };

        MergeState::Double(variant)
    }
}

#[derive(Debug, Clone)]
enum MergeVariant<T> {
    /// Describes an actual in-progress merge between two non-trivial batches.
    InProgress(SpineBatch<T>, SpineBatch<T>, FuelingMerge<T>),
    /// A merge that requires no further work. May or may not represent a
    /// non-trivial batch.
    Complete(Option<(SpineBatch<T>, Option<(SpineBatch<T>, SpineBatch<T>)>)>),
}

impl<T: Timestamp + Lattice> MergeVariant<T> {
    /// Completes and extracts the batch, unless structurally empty.
    ///
    /// The result is either `None`, for structurally empty batches, or a batch
    /// and optionally input batches from which it derived.
    fn complete(
        mut self,
        merge_reqs: &mut Vec<FueledMergeReq<T>>,
    ) -> Option<(SpineBatch<T>, Option<(SpineBatch<T>, SpineBatch<T>)>)> {
        let mut fuel = isize::max_value();
        self.work(&mut fuel, merge_reqs);
        if let MergeVariant::Complete(batch) = self {
            batch
        } else {
            panic!("Failed to complete a merge!");
        }
    }

    /// Applies some amount of work, potentially completing the merge.
    ///
    /// In case the work completes, the source batches are returned. This allows
    /// the caller to manage the released resources.
    fn work(&mut self, fuel: &mut isize, merge_reqs: &mut Vec<FueledMergeReq<T>>) {
        let variant = std::mem::replace(self, MergeVariant::Complete(None));
        if let MergeVariant::InProgress(b1, b2, mut merge) = variant {
            merge.work(&b1, &b2, fuel);
            if *fuel > 0 {
                *self = MergeVariant::Complete(Some((merge.done(merge_reqs), Some((b1, b2)))));
            } else {
                *self = MergeVariant::InProgress(b1, b2, merge);
            }
        } else {
            *self = variant;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn trace_datadriven() {
        fn parse_batch(x: &str) -> HollowBatch<u64> {
            let parts = x.split(' ').collect::<Vec<_>>();
            assert!(
                parts.len() >= 4,
                "usage: insert <lower> <upper> <since> <len> <keys>"
            );
            let (lower, upper, since, len, keys) =
                (parts[0], parts[1], parts[2], parts[3], &parts[4..]);
            let lower = lower.parse().expect("invalid lower");
            let upper = upper.parse().expect("invalid upper");
            let since = since.parse().expect("invalid since");
            let len = len.parse().expect("invalid len");
            HollowBatch {
                desc: Description::new(
                    Antichain::from_elem(lower),
                    Antichain::from_elem(upper),
                    Antichain::from_elem(since),
                ),
                len,
                keys: keys.iter().map(|x| (*x).to_owned()).collect(),
            }
        }

        datadriven::walk("tests/trace", |f| {
            let mut trace = Trace::default();

            f.run(move |tc| -> String {
                match tc.directive.as_str() {
                    "since-upper" => {
                        assert!(tc.input.is_empty());
                        format!(
                            "{:?}{:?}\n",
                            trace.since().elements(),
                            trace.upper().elements()
                        )
                    }
                    "batches" => {
                        assert!(tc.input.is_empty());
                        let mut s = String::new();
                        trace.spine.map_batches(|b| {
                            let b = match b {
                                SpineBatch::Merged(HollowBatch { desc, len, keys }) => format!(
                                    "{:?}{:?}{:?} {}{}\n",
                                    desc.lower().elements(),
                                    desc.upper().elements(),
                                    desc.since().elements(),
                                    len,
                                    keys.iter()
                                        .map(|x| format!(" {}", x))
                                        .collect::<Vec<_>>()
                                        .join(""),
                                ),
                                SpineBatch::Fueled { desc, parts } => format!(
                                    "{:?}{:?}{:?} {}/{}{}\n",
                                    desc.lower().elements(),
                                    desc.upper().elements(),
                                    desc.since().elements(),
                                    parts.len(),
                                    parts.iter().map(|x| x.len).sum::<usize>(),
                                    parts
                                        .iter()
                                        .flat_map(|x| x.keys.iter())
                                        .map(|x| format!(" {}", x))
                                        .collect::<Vec<_>>()
                                        .join("")
                                ),
                            };
                            s.push_str(&b);
                        });
                        s
                    }
                    "insert" => {
                        for x in tc.input.trim().split('\n') {
                            trace.push_batch(parse_batch(x));
                        }
                        "ok\n".to_owned()
                    }
                    "downgrade-since" => {
                        let since = tc.input.trim().parse().expect("invalid since");
                        trace.downgrade_since(Antichain::from_elem(since));
                        "ok\n".to_owned()
                    }
                    "take-merge-reqs" => {
                        assert!(tc.input.is_empty());
                        let mut s = String::new();
                        for merge_req in trace.take_merge_reqs() {
                            s.push_str(&format!(
                                "{:?}{:?}{:?} {}\n",
                                merge_req.desc.lower().elements(),
                                merge_req.desc.upper().elements(),
                                merge_req.desc.since().elements(),
                                merge_req
                                    .inputs
                                    .iter()
                                    .flat_map(|x| x.keys.iter())
                                    .cloned()
                                    .collect::<Vec<_>>()
                                    .join(" ")
                            ));
                        }
                        if s.is_empty() {
                            s.push_str("<empty>\n");
                        }
                        s
                    }
                    "apply-merge-res" => {
                        let res = FueledMergeRes {
                            output: parse_batch(&tc.input.trim()),
                        };
                        if trace.apply_merge_res(&res) {
                            "applied\n".into()
                        } else {
                            "no-op\n".into()
                        }
                    }
                    _ => panic!("unknown directive {:?}", tc),
                }
            })
        });
    }

    #[test]
    fn remove_redundant_merge_reqs() {
        fn req(lower: u64, upper: u64) -> FueledMergeReq<u64> {
            FueledMergeReq {
                desc: Description::new(
                    Antichain::from_elem(lower),
                    Antichain::from_elem(upper),
                    Antichain::new(),
                ),
                inputs: vec![],
            }
        }

        // Empty
        assert_eq!(Trace::<u64>::remove_redundant_merge_reqs(vec![]), vec![]);

        // Single
        assert_eq!(
            Trace::remove_redundant_merge_reqs(vec![req(0, 1)]),
            vec![req(0, 1)]
        );

        // Duplicate
        assert_eq!(
            Trace::remove_redundant_merge_reqs(vec![req(0, 1), req(0, 1)]),
            vec![req(0, 1)]
        );

        // Nothing covered
        assert_eq!(
            Trace::remove_redundant_merge_reqs(vec![req(0, 1), req(1, 2)]),
            vec![req(1, 2), req(0, 1)]
        );

        // Covered
        assert_eq!(
            Trace::remove_redundant_merge_reqs(vec![req(1, 2), req(0, 3)]),
            vec![req(0, 3)]
        );

        // Covered, lower equal
        assert_eq!(
            Trace::remove_redundant_merge_reqs(vec![req(0, 2), req(0, 3)]),
            vec![req(0, 3)]
        );

        // Covered, upper equal
        assert_eq!(
            Trace::remove_redundant_merge_reqs(vec![req(1, 3), req(0, 3)]),
            vec![req(0, 3)]
        );

        // Covered, unexpected order (doesn't happen in practice)
        assert_eq!(
            Trace::remove_redundant_merge_reqs(vec![req(0, 3), req(1, 2)]),
            vec![req(0, 3)]
        );

        // Partially overlapping
        assert_eq!(
            Trace::remove_redundant_merge_reqs(vec![req(0, 2), req(1, 3)]),
            vec![req(1, 3), req(0, 2)]
        );

        // Partially overlapping, the other order
        assert_eq!(
            Trace::remove_redundant_merge_reqs(vec![req(1, 3), req(0, 2)]),
            vec![req(0, 2), req(1, 3)]
        );

        // Different sinces (doesn't happen in practice)
        let req015 = FueledMergeReq {
            desc: Description::new(
                Antichain::from_elem(0),
                Antichain::from_elem(1),
                Antichain::from_elem(5),
            ),
            inputs: vec![],
        };
        assert_eq!(
            Trace::remove_redundant_merge_reqs(vec![req(0, 1), req015.clone()]),
            vec![req015, req(0, 1)]
        );
    }
}

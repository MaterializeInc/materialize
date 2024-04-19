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

use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::fmt::Debug;
use std::sync::Arc;

use differential_dataflow::lattice::Lattice;
use differential_dataflow::trace::Description;
use mz_ore::cast::CastFrom;
#[allow(unused_imports)] // False positive.
use mz_ore::fmt::FormatBuffer;
use serde::{Serialize, Serializer};
use timely::progress::frontier::AntichainRef;
use timely::progress::{Antichain, Timestamp};
use timely::PartialOrder;

use crate::internal::state::HollowBatch;

#[derive(Debug, Clone, PartialEq)]
pub struct FueledMergeReq<T> {
    pub desc: Description<T>,
    pub inputs: Vec<IdHollowBatch<T>>,
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
    pub(crate) roundtrip_structure: bool,
}

#[cfg(any(test, debug_assertions))]
impl<T: PartialEq> PartialEq for Trace<T> {
    fn eq(&self, other: &Self) -> bool {
        // Deconstruct self and other so we get a compile failure if new fields
        // are added.
        let Trace {
            spine: _,
            roundtrip_structure: _,
        } = self;
        let Trace {
            spine: _,
            roundtrip_structure: _,
        } = other;

        // Intentionally use HollowBatches for this comparison so we ignore
        // differences in spine layers.
        self.batches().eq(other.batches())
    }
}

impl<T: Timestamp + Lattice> Default for Trace<T> {
    fn default() -> Self {
        Self {
            spine: Spine::new(),
            roundtrip_structure: false,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ThinSpineBatch<T> {
    pub(crate) level: usize,
    pub(crate) desc: Description<T>,
    pub(crate) parts: Vec<SpineId>,
}

/// This is a "flattened" representation of a Trace. Goals:
/// - small updates to the trace should result in small differences in the `FlatTrace`;
/// - two `FlatTrace`s should be efficient to diff;
/// - converting to and from a `Trace` should be relatively straightforward.
///
/// These goals are all somewhat in tension, and the space of possible representations is pretty
/// large. See individual fields for comments on some of the tradeoffs.
#[derive(Clone, Debug)]
pub struct FlatTrace<T> {
    pub(crate) since: Antichain<T>,
    /// Hollow batches without an associated ID. If this flattened trace contains spine batches,
    /// we can figure out which legacy batch belongs in which spine batch by comparing the `desc`s.
    /// Previously, we serialized a trace as just this list of batches. Keeping this data around
    /// helps ensure backwards compatibility. In the near future, we may still keep some batches
    /// here to help minimize the size of diffs -- rewriting all the hollow batches in a shard
    /// can be prohibitively expensive. Eventually, we'd like to remove this in favour of the
    /// collection below.
    pub(crate) legacy_batches: BTreeMap<Arc<HollowBatch<T>>, ()>,
    /// Hollow batches _with_ an associated ID. Spine batches can reference these hollow batches
    /// by id directly.
    pub(crate) hollow_batches: BTreeMap<SpineId, Arc<HollowBatch<T>>>,
    /// Spine batches stored by ID. We reference hollow batches by ID, instead of inlining them,
    /// to make differential updates smaller when two batches merge together. We also store the
    /// level on the batch, instead of mapping from level to a list of batches... the level of a
    /// spine batch doesn't change over time, but the list of batches at a particular level does.
    pub(crate) spine_batches: BTreeMap<SpineId, ThinSpineBatch<T>>,
    /// In-progress merges. We store this by spine id instead of level to prepare for some possible
    /// generalizations to spine (merging N of M batches at a level). This is also a natural place
    /// to store incremental merge progress in the future.
    pub(crate) fueling_merges: BTreeMap<SpineId, FuelingMerge<T>>,
}

impl<T: Timestamp + Lattice> Trace<T> {
    pub(crate) fn flatten(&self) -> FlatTrace<T> {
        let since = self.spine.since.clone();
        let mut legacy_batches = BTreeMap::new();
        // Since we store all batches in the legacy-batch collection for the moment,
        // this doesn't need to be mutable.
        let hollow_batches = BTreeMap::new();
        let mut spine_batches = BTreeMap::new();
        let mut fueling_merges = BTreeMap::new();

        let mut push_hollow_batch = |id_batch: &IdHollowBatch<T>| {
            let id = id_batch.id;
            let batch = Arc::clone(&id_batch.batch);
            legacy_batches.insert(batch, ());
            id
        };

        let mut push_spine_batch = |level: usize, batch: &SpineBatch<T>| {
            let (id, spine_batch) = match batch {
                SpineBatch::Merged(id_batch) => (
                    id_batch.id,
                    ThinSpineBatch {
                        level,
                        desc: id_batch.batch.desc.clone(),
                        parts: vec![push_hollow_batch(id_batch)],
                    },
                ),
                SpineBatch::Fueled {
                    id,
                    desc,
                    parts,
                    len: _,
                } => (
                    *id,
                    ThinSpineBatch {
                        level,
                        desc: desc.clone(),
                        parts: parts.into_iter().map(&mut push_hollow_batch).collect(),
                    },
                ),
            };
            spine_batches.insert(id, spine_batch);
        };

        for (level, state) in self.spine.merging.iter().enumerate() {
            match state {
                MergeState::Vacant => {}
                MergeState::Single(batch) => push_spine_batch(level, batch),
                MergeState::Double(left, right, merge) => {
                    push_spine_batch(level, left);
                    push_spine_batch(level, right);
                    let merge_id = SpineId(left.id().0, right.id().1);
                    fueling_merges.insert(merge_id, merge.clone());
                }
            }
        }

        if !self.roundtrip_structure {
            assert!(hollow_batches.is_empty());
            spine_batches.clear();
            fueling_merges.clear();
        }

        FlatTrace {
            since,
            legacy_batches,
            hollow_batches,
            spine_batches,
            fueling_merges,
        }
    }
    pub(crate) fn unflatten(value: FlatTrace<T>) -> Result<Self, String> {
        let FlatTrace {
            since,
            legacy_batches,
            mut hollow_batches,
            spine_batches,
            mut fueling_merges,
        } = value;

        // If the flattened representation has spine batches, we know to preserve the structure for
        // this trace.
        // Note that for empty spines, roundtrip_structure will default to false. This is done for
        // backwards-compatability.
        let roundtrip_structure = !spine_batches.is_empty();

        // We need to look up legacy batches somehow, but we don't have a spine id for them.
        // Instead, we rely on the fact that the spine must store them in antichain order.
        // Our timestamp type may not be totally ordered, so we need to implement our own comparator
        // here. Persist's invariants ensure that all the frontiers we're comparing are comparable,
        // though.
        let compare_chains = |left: &Antichain<T>, right: &Antichain<T>| {
            if PartialOrder::less_than(left, right) {
                Ordering::Less
            } else if PartialOrder::less_than(right, left) {
                Ordering::Greater
            } else {
                Ordering::Equal
            }
        };
        let mut legacy_batches: Vec<_> = legacy_batches.into_iter().map(|(k, _)| k).collect();
        legacy_batches.sort_by(|a, b| compare_chains(a.desc.lower(), b.desc.lower()).reverse());

        let mut pop_batch = |id: SpineId, spine_desc: &Description<T>| -> Result<_, String> {
            let batch = hollow_batches
                .remove(&id)
                .or_else(|| legacy_batches.pop())
                .ok_or_else(|| format!("missing referenced hollow batch {id:?}"))?;

            if !PartialOrder::less_equal(spine_desc.lower(), batch.desc.lower())
                || !PartialOrder::less_equal(batch.desc.upper(), spine_desc.upper())
            {
                return Err(format!(
                    "hollow batch desc {:?} did not fall within spine batch desc {:?}",
                    batch.desc, spine_desc
                ));
            }

            Ok(IdHollowBatch { id, batch })
        };

        let (upper, next_id) = if let Some((id, batch)) = spine_batches.last_key_value() {
            (batch.desc.upper().clone(), id.1)
        } else {
            (Antichain::from_elem(T::minimum()), 0)
        };
        let levels = spine_batches
            .first_key_value()
            .map(|(_, batch)| batch.level + 1)
            .unwrap_or(0);
        let mut merging = vec![MergeState::Vacant; levels];
        for (id, mut batch) in spine_batches {
            let level = batch.level;
            let batch = if batch.parts.len() == 1 {
                let id = batch.parts.pop().expect("popping from nonempty vec");
                SpineBatch::Merged(pop_batch(id, &batch.desc)?)
            } else {
                let parts = batch
                    .parts
                    .into_iter()
                    .map(|id| pop_batch(id, &batch.desc))
                    .collect::<Result<Vec<_>, _>>()?;
                let len = parts.iter().map(|p| (*p).batch.len).sum();
                SpineBatch::Fueled {
                    id,
                    desc: batch.desc,
                    parts,
                    len,
                }
            };

            let state = std::mem::replace(&mut merging[level], MergeState::Vacant);
            let state = match state {
                MergeState::Vacant => MergeState::Single(batch),
                MergeState::Single(single) => {
                    let merge_id = SpineId(single.id().0, batch.id().1);
                    let merge = fueling_merges
                        .remove(&merge_id)
                        .ok_or_else(|| format!("Expected merge at level {level}"))?;
                    MergeState::Double(single, batch, merge)
                }
                _ => Err(format!("Too many batches at level {level}"))?,
            };

            merging[level] = state;
        }

        let mut trace = Trace {
            spine: Spine {
                effort: 1,
                next_id,
                since,
                upper,
                merging,
            },
            roundtrip_structure,
        };

        fn check_empty(name: &str, len: usize) -> Result<(), String> {
            if len != 0 {
                Err(format!("{len} {name} left after reconstructing spine"))
            } else {
                Ok(())
            }
        }

        if roundtrip_structure {
            check_empty("legacy batches", legacy_batches.len())?;
        } else {
            // If the structure wasn't actually serialized, we may have legacy batches left over.
            for batch in legacy_batches.into_iter().rev() {
                trace.push_batch_no_merge_reqs(Arc::unwrap_or_clone(batch));
            }
        }
        check_empty("hollow batches", hollow_batches.len())?;
        check_empty("merges", fueling_merges.len())?;

        debug_assert_eq!(trace.validate(), Ok(()), "{:?}", trace);

        Ok(trace)
    }
}

#[derive(Clone, Debug, Default)]
pub(crate) struct SpineMetrics {
    pub compact_batches: u64,
    pub compacting_batches: u64,
    pub noncompact_batches: u64,
}

impl<T> Trace<T> {
    pub fn since(&self) -> &Antichain<T> {
        &self.spine.since
    }

    pub fn upper(&self) -> &Antichain<T> {
        &self.spine.upper
    }

    pub fn map_batches<'a, F: FnMut(&'a HollowBatch<T>)>(&'a self, mut f: F) {
        for batch in self.batches() {
            f(batch);
        }
    }

    pub fn batches(&self) -> impl Iterator<Item = &HollowBatch<T>> {
        self.spine
            .spine_batches()
            .flat_map(|b| match b {
                SpineBatch::Merged(b) => std::slice::from_ref(b),
                SpineBatch::Fueled { parts, .. } => parts.as_slice(),
            })
            .map(|b| &*b.batch)
    }

    pub fn num_spine_batches(&self) -> usize {
        self.spine.spine_batches().count()
    }

    #[cfg(test)]
    pub fn num_hollow_batches(&self) -> usize {
        self.batches().count()
    }

    #[cfg(test)]
    pub fn num_updates(&self) -> usize {
        self.batches().map(|b| b.len).sum()
    }
}

impl<T: Timestamp + Lattice> Trace<T> {
    pub fn downgrade_since(&mut self, since: &Antichain<T>) {
        self.spine.since.clone_from(since);
    }

    #[must_use]
    pub fn push_batch(&mut self, batch: HollowBatch<T>) -> Vec<FueledMergeReq<T>> {
        let mut merge_reqs = Vec::new();
        self.spine.insert(
            batch,
            &mut SpineLog::Enabled {
                merge_reqs: &mut merge_reqs,
            },
        );
        debug_assert_eq!(self.spine.validate(), Ok(()), "{:?}", self);
        // Spine::roll_up (internally used by insert) clears all batches out of
        // levels below a target by walking up from level 0 and merging each
        // level into the next (providing the necessary fuel). In practice, this
        // means we'll get a series of requests like `(a, b), (a, b, c), ...`.
        // It's a waste to do all of these (we'll throw away the results), so we
        // filter out any that are entirely covered by some other request.
        Self::remove_redundant_merge_reqs(merge_reqs)
    }

    /// The same as [Self::push_batch] but without the `FueledMergeReq`s, which
    /// account for a surprising amount of cpu in prod. #18368
    pub(crate) fn push_batch_no_merge_reqs(&mut self, batch: HollowBatch<T>) {
        self.spine.insert(batch, &mut SpineLog::Disabled);
    }

    /// Validates invariants.
    ///
    /// See `Spine::validate` for details.
    pub fn validate(&self) -> Result<(), String> {
        self.spine.validate()
    }

    pub fn apply_merge_res(&mut self, res: &FueledMergeRes<T>) -> ApplyMergeResult {
        for batch in self.spine.merging.iter_mut().rev() {
            match batch {
                MergeState::Double(batch1, batch2, _) => {
                    let result = batch1.maybe_replace(res);
                    if result.matched() {
                        return result;
                    }
                    let result = batch2.maybe_replace(res);
                    if result.matched() {
                        return result;
                    }
                }
                MergeState::Single(batch) => {
                    let result = batch.maybe_replace(res);
                    if result.matched() {
                        return result;
                    }
                }
                _ => {}
            }
        }
        ApplyMergeResult::NotAppliedNoMatch
    }

    pub(crate) fn all_fueled_merge_reqs(&self) -> Vec<FueledMergeReq<T>> {
        self.spine
            .spine_batches()
            .filter_map(|b| match b {
                SpineBatch::Merged(_) => None, // No-op.
                SpineBatch::Fueled { desc, parts, .. } => Some(FueledMergeReq {
                    desc: desc.clone(),
                    inputs: parts.clone(),
                }),
            })
            .collect()
    }

    // This is only called with the results of one `insert` and so the length of
    // `merge_reqs` is bounded by the number of levels in the spine (or possibly
    // some small constant multiple?). The number of levels is logarithmic in the
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

    pub fn spine_metrics(&self) -> SpineMetrics {
        let mut metrics = SpineMetrics::default();
        for batch in self.spine.spine_batches() {
            if batch.is_compact() {
                metrics.compact_batches += 1;
            } else if batch.is_merging() {
                metrics.compacting_batches += 1;
            } else {
                metrics.noncompact_batches += 1;
            }
        }
        metrics
    }
}

/// A log of what transitively happened during a Spine operation: e.g.
/// FueledMergeReqs were generated.
enum SpineLog<'a, T> {
    Enabled {
        merge_reqs: &'a mut Vec<FueledMergeReq<T>>,
    },
    Disabled,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct SpineId(pub usize, pub usize);

impl Serialize for SpineId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let SpineId(lo, hi) = self;
        serializer.serialize_str(&format!("{lo}-{hi}"))
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct IdHollowBatch<T> {
    pub id: SpineId,
    pub batch: Arc<HollowBatch<T>>,
}

#[derive(Debug, Clone, PartialEq)]
pub(crate) enum SpineBatch<T> {
    Merged(IdHollowBatch<T>),
    Fueled {
        id: SpineId,
        desc: Description<T>,
        parts: Vec<IdHollowBatch<T>>,
        // A cached version of parts.iter().map(|x| x.len).sum()
        len: usize,
    },
}

#[derive(Debug, Copy, Clone)]
pub enum ApplyMergeResult {
    AppliedExact,
    AppliedSubset,
    NotAppliedNoMatch,
    NotAppliedInvalidSince,
    NotAppliedTooManyUpdates,
}

impl ApplyMergeResult {
    pub fn applied(&self) -> bool {
        match self {
            ApplyMergeResult::AppliedExact | ApplyMergeResult::AppliedSubset => true,
            _ => false,
        }
    }
    pub fn matched(&self) -> bool {
        match self {
            ApplyMergeResult::AppliedExact
            | ApplyMergeResult::AppliedSubset
            | ApplyMergeResult::NotAppliedTooManyUpdates => true,
            _ => false,
        }
    }
}

impl<T: Timestamp + Lattice> SpineBatch<T> {
    pub fn lower(&self) -> &Antichain<T> {
        self.desc().lower()
    }

    pub fn upper(&self) -> &Antichain<T> {
        self.desc().upper()
    }

    fn id(&self) -> SpineId {
        match self {
            SpineBatch::Merged(b) => b.id,
            SpineBatch::Fueled { id, parts, .. } => {
                debug_assert_eq!(parts.first().map(|x| x.id.0), Some(id.0));
                debug_assert_eq!(parts.last().map(|x| x.id.1), Some(id.1));
                *id
            }
        }
    }

    pub fn is_compact(&self) -> bool {
        // This definition is extremely likely to change, but for now, we consider a batch
        // "compact" if it's a merged batch of a single run.
        match self {
            SpineBatch::Merged(b) => b.batch.runs.is_empty(),
            SpineBatch::Fueled { .. } => false,
        }
    }

    pub fn is_merging(&self) -> bool {
        // We can't currently tell if a fueled merge is in progress or dropped on the floor,
        // but for now we assume that it is active.
        match self {
            SpineBatch::Merged(_) => false,
            SpineBatch::Fueled { .. } => true,
        }
    }

    fn desc(&self) -> &Description<T> {
        match self {
            SpineBatch::Merged(b) => &b.batch.desc,
            SpineBatch::Fueled { desc, .. } => desc,
        }
    }

    pub fn len(&self) -> usize {
        match self {
            SpineBatch::Merged(b) => b.batch.len,
            // NB: This is an upper bound on len, we won't know for sure until
            // we compact it.
            SpineBatch::Fueled { len, parts, .. } => {
                // Sanity check the cached len value in debug mode, to hopefully
                // find any bugs with its maintenance.
                debug_assert_eq!(*len, parts.iter().map(|x| x.batch.len).sum::<usize>());
                *len
            }
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn empty(
        id: SpineId,
        lower: Antichain<T>,
        upper: Antichain<T>,
        since: Antichain<T>,
    ) -> Self {
        SpineBatch::Merged(IdHollowBatch {
            id,
            batch: Arc::new(HollowBatch {
                desc: Description::new(lower, upper, since),
                parts: vec![],
                len: 0,
                runs: vec![],
            }),
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
        let remaining_work = b1.len() + b2.len();
        FuelingMerge {
            since,
            remaining_work,
        }
    }

    // TODO: Roundtrip the SpineId through FueledMergeReq/FueledMergeRes?
    fn maybe_replace(&mut self, res: &FueledMergeRes<T>) -> ApplyMergeResult {
        // The spine's and merge res's sinces don't need to match (which could occur if Spine
        // has been reloaded from state due to compare_and_set mismatch), but if so, the Spine
        // since must be in advance of the merge res since.
        if !PartialOrder::less_equal(res.output.desc.since(), self.desc().since()) {
            return ApplyMergeResult::NotAppliedInvalidSince;
        }

        // If our merge result exactly matches a spine batch, we can swap it in directly
        let exact_match = res.output.desc.lower() == self.desc().lower()
            && res.output.desc.upper() == self.desc().upper();
        if exact_match {
            // Spine internally has an invariant about a batch being at some level
            // or higher based on the len. We could end up violating this invariant
            // if we increased the length of the batch.
            //
            // A res output with length greater than the existing spine batch implies
            // a compaction has already been applied to this range, and with a higher
            // rate of consolidation than this one. This could happen as a result of
            // compaction's memory bound limiting the amount of consolidation possible.
            if res.output.len > self.len() {
                return ApplyMergeResult::NotAppliedTooManyUpdates;
            }
            *self = SpineBatch::Merged(IdHollowBatch {
                id: self.id(),
                batch: Arc::new(res.output.clone()),
            });
            return ApplyMergeResult::AppliedExact;
        }

        // It is possible the structure of the spine has changed since the merge res
        // was created, such that it no longer exactly matches the description of a
        // spine batch. This can happen if another merge has happened in the interim,
        // or if spine needed to be rebuilt from state.
        //
        // When this occurs, we can still attempt to slot the merge res in to replace
        // the parts of a fueled merge. e.g. if the res is for `[1,3)` and the parts
        // are `[0,1),[1,2),[2,3),[3,4)`, we can swap out the middle two parts for res.
        match self {
            SpineBatch::Fueled {
                id,
                parts,
                desc,
                len: _,
            } => {
                // first, determine if a subset of parts can be cleanly replaced by the merge res
                let mut lower = None;
                let mut upper = None;
                for (i, batch) in parts.iter().enumerate() {
                    if batch.batch.desc.lower() == res.output.desc.lower() {
                        lower = Some((i, batch.id.0));
                    }
                    if batch.batch.desc.upper() == res.output.desc.upper() {
                        upper = Some((i, batch.id.1));
                    }
                    if lower.is_some() && upper.is_some() {
                        break;
                    }
                }
                // next, replace parts with the merge res batch if we can
                match (lower, upper) {
                    (Some((lower, id_lower)), Some((upper, id_upper))) => {
                        let mut new_parts = vec![];
                        new_parts.extend_from_slice(&parts[..lower]);
                        new_parts.push(IdHollowBatch {
                            id: SpineId(id_lower, id_upper),
                            batch: Arc::new(res.output.clone()),
                        });
                        new_parts.extend_from_slice(&parts[upper + 1..]);
                        let new_spine_batch = SpineBatch::Fueled {
                            id: *id,
                            desc: desc.to_owned(),
                            len: new_parts.iter().map(|x| x.batch.len).sum(),
                            parts: new_parts,
                        };
                        if new_spine_batch.len() > self.len() {
                            return ApplyMergeResult::NotAppliedTooManyUpdates;
                        }
                        *self = new_spine_batch;
                        ApplyMergeResult::AppliedSubset
                    }
                    _ => ApplyMergeResult::NotAppliedNoMatch,
                }
            }
            _ => ApplyMergeResult::NotAppliedNoMatch,
        }
    }

    #[cfg(test)]
    fn describe(&self, extended: bool) -> String {
        match (extended, self) {
            (false, SpineBatch::Merged(x)) => format!(
                "[{}-{}]{:?}{:?}{}",
                x.id.0,
                x.id.1,
                x.batch.desc.lower().elements(),
                x.batch.desc.upper().elements(),
                x.batch.len
            ),
            (
                false,
                SpineBatch::Fueled {
                    id,
                    parts,
                    desc,
                    len,
                },
            ) => format!(
                "[{}-{}]{:?}{:?}{}/{}",
                id.0,
                id.1,
                desc.lower().elements(),
                desc.upper().elements(),
                parts.len(),
                len
            ),
            (true, SpineBatch::Merged(b)) => format!(
                "[{}-{}]{:?}{:?}{:?} {}{}",
                b.id.0,
                b.id.1,
                b.batch.desc.lower().elements(),
                b.batch.desc.upper().elements(),
                b.batch.desc.since().elements(),
                b.batch.len,
                b.batch
                    .parts
                    .iter()
                    .map(|x| format!(" {}", x.printable_name()))
                    .collect::<Vec<_>>()
                    .join(""),
            ),
            (
                true,
                SpineBatch::Fueled {
                    id,
                    desc,
                    parts,
                    len,
                },
            ) => {
                format!(
                    "[{}-{}]{:?}{:?}{:?} {}/{}{}",
                    id.0,
                    id.1,
                    desc.lower().elements(),
                    desc.upper().elements(),
                    desc.since().elements(),
                    parts.len(),
                    len,
                    parts
                        .iter()
                        .flat_map(|x| x.batch.parts.iter())
                        .map(|x| format!(" {}", x.printable_name()))
                        .collect::<Vec<_>>()
                        .join("")
                )
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct FuelingMerge<T> {
    pub(crate) since: Antichain<T>,
    pub(crate) remaining_work: usize,
}

impl<T: Timestamp + Lattice> FuelingMerge<T> {
    /// Perform some amount of work, decrementing `fuel`.
    ///
    /// If `fuel` is non-zero after the call, the merging is complete and one
    /// should call `done` to extract the merged results.
    // TODO(benesch): rewrite to avoid usage of `as`.
    #[allow(clippy::as_conversions)]
    fn work(&mut self, _: &SpineBatch<T>, _: &SpineBatch<T>, fuel: &mut isize) {
        let used = std::cmp::min(*fuel as usize, self.remaining_work);
        self.remaining_work = self.remaining_work.saturating_sub(used);
        *fuel -= used as isize;
    }

    /// Extracts merged results.
    ///
    /// This method should only be called after `work` has been called and has
    /// not brought `fuel` to zero. Otherwise, the merge is still in progress.
    fn done(
        self,
        b1: SpineBatch<T>,
        b2: SpineBatch<T>,
        log: &mut SpineLog<'_, T>,
    ) -> SpineBatch<T> {
        let id = SpineId(b1.id().0, b2.id().1);
        assert!(id.0 < id.1);
        let lower = b1.desc().lower().clone();
        let upper = b2.desc().upper().clone();
        let since = self.since;

        // Special case empty batches.
        if b1.is_empty() && b2.is_empty() {
            return SpineBatch::empty(id, lower, upper, since);
        }

        let desc = Description::new(lower, upper, since);
        let len = b1.len() + b2.len();

        // Pre-size the merged_parts Vec. Benchmarking has shown that, at least
        // in the worst case, the double iteration is absolutely worth having
        // merged_parts pre-sized.
        let mut merged_parts_len = 0;
        for b in [&b1, &b2] {
            match b {
                SpineBatch::Merged(_) => merged_parts_len += 1,
                SpineBatch::Fueled { parts, .. } => merged_parts_len += parts.len(),
            }
        }
        let mut merged_parts = Vec::with_capacity(merged_parts_len);
        for b in [b1, b2] {
            match b {
                SpineBatch::Merged(b) => merged_parts.push(b),
                SpineBatch::Fueled { mut parts, .. } => merged_parts.append(&mut parts),
            }
        }
        // Sanity check the pre-size code.
        debug_assert_eq!(merged_parts.len(), merged_parts_len);

        if let SpineLog::Enabled { merge_reqs } = log {
            merge_reqs.push(FueledMergeReq {
                desc: desc.clone(),
                inputs: merged_parts.clone(),
            });
        }

        SpineBatch::Fueled {
            id,
            desc,
            len,
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
    next_id: usize,
    since: Antichain<T>,
    upper: Antichain<T>,
    merging: Vec<MergeState<T>>,
}

impl<T> Spine<T> {
    pub fn spine_batches(&self) -> impl Iterator<Item = &SpineBatch<T>> {
        self.merging.iter().rev().flat_map(|m| match m {
            MergeState::Vacant => None.into_iter().chain(None.into_iter()),
            MergeState::Single(a) => Some(a).into_iter().chain(None.into_iter()),
            MergeState::Double(a, b, _) => Some(a).into_iter().chain(Some(b).into_iter()),
        })
    }
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
            next_id: 0,
            since: Antichain::from_elem(T::minimum()),
            upper: Antichain::from_elem(T::minimum()),
            merging: Vec::new(),
        }
    }

    // Ideally, this method acts as insertion of `batch`, even if we are not yet
    // able to begin merging the batch. This means it is a good time to perform
    // amortized work proportional to the size of batch.
    pub fn insert(&mut self, batch: HollowBatch<T>, log: &mut SpineLog<'_, T>) {
        assert!(batch.desc.lower() != batch.desc.upper());
        assert_eq!(batch.desc.lower(), &self.upper);

        let id = {
            let id = self.next_id;
            self.next_id += 1;
            SpineId(id, self.next_id)
        };
        let batch = SpineBatch::Merged(IdHollowBatch {
            id,
            batch: Arc::new(batch),
        });

        self.upper.clone_from(batch.upper());

        // If `batch` and the most recently inserted batch are both empty,
        // we can just fuse them.
        if batch.is_empty() {
            if let Some(position) = self.merging.iter().position(|m| !m.is_vacant()) {
                if self.merging[position].is_single() && self.merging[position].is_empty() {
                    self.insert_at(batch, position);
                    // Since we just inserted a batch, we should always have work to complete...
                    // but otherwise we just leave this layer vacant.
                    if let Some(merged) = self.complete_at(position, log) {
                        self.merging[position] = MergeState::Single(merged);
                    }
                    return;
                }
            }
        }

        // Normal insertion for the batch.
        let index = batch.len().next_power_of_two();
        self.introduce_batch(batch, usize::cast_from(index.trailing_zeros()), log);
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
                x @ MergeState::Double(..) => (2, x.len()),
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
        batch: SpineBatch<T>,
        batch_index: usize,
        log: &mut SpineLog<'_, T>,
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
        // TODO(benesch): avoid dangerous usage of `as`.
        #[allow(clippy::as_conversions)]
        let fuel = fuel as isize;

        // Step 1.  Apply fuel to each in-progress merge.
        //
        //          Before we can introduce new updates, we must apply any
        //          fuel to in-progress merges, as this fuel is what ensures
        //          that the merges will be complete by the time we insert
        //          the updates.
        self.apply_fuel(&fuel, log);

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
        self.roll_up(batch_index, log);

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
    fn roll_up(&mut self, index: usize, log: &mut SpineLog<'_, T>) {
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
                if let Some(merged) = merged.take() {
                    self.insert_at(merged, i);
                }
                merged = self.complete_at(i, log);
            }

            // The merged results should be introduced at level `index`, which
            // should be ready to absorb them (possibly creating a new merge at
            // the time).
            if let Some(merged) = merged {
                self.insert_at(merged, index);
            }

            // If the insertion results in a merge, we should complete it to
            // ensure the upcoming insertion at `index` does not panic.
            if self.merging[index].is_double() {
                let merged = self.complete_at(index, log).expect("double batch");
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
    pub fn apply_fuel(&mut self, fuel: &isize, log: &mut SpineLog<'_, T>) {
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
            self.merging[index].work(&mut fuel);
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
                let complete = self.complete_at(index, log).expect("complete batch");
                self.insert_at(complete, index + 1);
            }
        }
    }

    /// Inserts a batch at a specific location.
    ///
    /// This is a non-public internal method that can panic if we try and insert
    /// into a layer which already contains two batches (and is still in the
    /// process of merging).
    fn insert_at(&mut self, batch: SpineBatch<T>, index: usize) {
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
            MergeState::Double(..) => {
                panic!("Attempted to insert batch into incomplete merge!")
            }
        };
    }

    /// Completes and extracts what ever is at layer `index`, leaving this layer vacant.
    fn complete_at(&mut self, index: usize, log: &mut SpineLog<'_, T>) -> Option<SpineBatch<T>> {
        self.merging[index].complete(log)
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

                let appropriate_level = usize::cast_from(
                    self.merging[length - 1]
                        .len()
                        .next_power_of_two()
                        .trailing_zeros(),
                );

                // Continue only as far as is appropriate
                while appropriate_level < length - 1 {
                    match self.merging[length - 2].take() {
                        // Vacant batches can be absorbed.
                        MergeState::Vacant => {
                            self.merging.remove(length - 2);
                            length = self.merging.len();
                        }
                        // Single batches may initiate a merge, if sizes are
                        // within bounds, but terminate the loop either way.
                        MergeState::Single(batch) => {
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
                                    MergeState::Double(..) => {
                                        smaller += 2 << index;
                                    }
                                }
                            }

                            if smaller <= (1 << length) / 8 {
                                self.merging.remove(length - 2);
                                self.insert_at(batch, length - 2);
                            } else {
                                self.merging[length - 2] = MergeState::Single(batch);
                            }
                            return;
                        }
                        // If a merge is in progress there is nothing to do.
                        MergeState::Double(a, b, fuel) => {
                            self.merging[length - 2] = MergeState::Double(a, b, fuel);
                            return;
                        }
                    }
                }
            }
        }
    }

    /// Checks invariants:
    /// - The lowers and uppers of all batches "line up".
    /// - The lower of the "minimum" batch is `antichain[T::minimum]`.
    /// - The upper of the "maximum" batch is `== self.upper`.
    /// - The since of each batch is `less_equal self.since`.
    /// - The `SpineIds` all "line up" and cover from `0` to `self.next_id`.
    /// - TODO: Verify fuel and level invariants.
    fn validate(&self) -> Result<(), String> {
        let mut id = SpineId(0, 0);
        let mut frontier = Antichain::from_elem(T::minimum());
        for x in self.merging.iter().rev() {
            let batches = match x {
                MergeState::Vacant => vec![],
                MergeState::Single(x) => {
                    vec![x]
                }
                MergeState::Double(x0, x1, _m) => {
                    // TODO: Anything we can validate about remaining_work? It'd
                    // be nice to assert that it's bigger than the len of the
                    // two batches, but apply_merge_res might swap those lengths
                    // out from under us.
                    vec![x0, x1]
                }
            };
            for batch in batches {
                if batch.id().0 != id.1 {
                    return Err(format!(
                        "batch id {:?} does not match the previous id {:?}: {:?}",
                        batch.id(),
                        id,
                        self
                    ));
                }
                id = batch.id();
                if batch.desc().lower() != &frontier {
                    return Err(format!(
                        "batch lower {:?} does not match the previous upper {:?}: {:?}",
                        batch.desc().lower(),
                        frontier,
                        self
                    ));
                }
                frontier.clone_from(batch.desc().upper());
                if !PartialOrder::less_equal(batch.desc().since(), &self.since) {
                    return Err(format!(
                        "since of batch {:?} past the spine since {:?}: {:?}",
                        batch.desc().since(),
                        self.since,
                        self
                    ));
                }
            }
        }
        if self.next_id != id.1 {
            return Err(format!(
                "spine next_id {:?} does not match the last batch's id {:?}: {:?}",
                self.next_id, id, self
            ));
        }
        if self.upper != frontier {
            return Err(format!(
                "spine upper {:?} does not match the last batch's upper {:?}: {:?}",
                self.upper, frontier, self
            ));
        }
        Ok(())
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
    Single(SpineBatch<T>),
    /// A layer containing two batches, in the process of merging.
    Double(SpineBatch<T>, SpineBatch<T>, FuelingMerge<T>),
}

impl<T: Timestamp + Lattice> MergeState<T> {
    /// The number of actual updates contained in the level.
    fn len(&self) -> usize {
        match self {
            MergeState::Single(b) => b.len(),
            MergeState::Double(b1, b2, _) => b1.len() + b2.len(),
            _ => 0,
        }
    }

    /// True if this merge state contains no updates.
    fn is_empty(&self) -> bool {
        match self {
            MergeState::Single(b) => b.is_empty(),
            MergeState::Double(b1, b2, _) => b1.is_empty() && b2.is_empty(),
            _ => true,
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
        if let MergeState::Double(_, _, _) = self {
            true
        } else {
            false
        }
    }

    /// Immediately complete any merge.
    ///
    /// The result is either a batch, if there is a non-trivial batch to return
    /// or `None` if there is no meaningful batch to return.
    ///
    /// There is the additional option of input batches.
    fn complete(&mut self, log: &mut SpineLog<'_, T>) -> Option<SpineBatch<T>> {
        match std::mem::replace(self, MergeState::Vacant) {
            MergeState::Vacant => None,
            MergeState::Single(batch) => Some(batch),
            MergeState::Double(a, b, merge) => Some(merge.done(a, b, log)),
        }
    }

    /// True iff the layer is a complete merge, ready for extraction.
    fn is_complete(&self) -> bool {
        match self {
            MergeState::Double(_, _, work) => work.remaining_work == 0,
            _ => false,
        }
    }

    /// Performs a bounded amount of work towards a merge.
    fn work(&mut self, fuel: &mut isize) {
        // We only perform work for merges in progress.
        if let MergeState::Double(b1, b2, merge) = self {
            merge.work(b1, b2, fuel);
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
    fn begin_merge(
        batch1: SpineBatch<T>,
        batch2: SpineBatch<T>,
        compaction_frontier: Option<AntichainRef<T>>,
    ) -> MergeState<T> {
        assert_eq!(batch1.upper(), batch2.lower());
        let begin_merge = SpineBatch::begin_merge(&batch1, &batch2, compaction_frontier);
        MergeState::Double(batch1, batch2, begin_merge)
    }
}

#[cfg(test)]
pub mod datadriven {
    use crate::internal::datadriven::DirectiveArgs;

    use super::*;

    /// Shared state for a single [crate::internal::trace] [datadriven::TestFile].
    #[derive(Debug, Default)]
    pub struct TraceState {
        pub trace: Trace<u64>,
        pub merge_reqs: Vec<FueledMergeReq<u64>>,
    }

    pub fn since_upper(
        datadriven: &TraceState,
        _args: DirectiveArgs,
    ) -> Result<String, anyhow::Error> {
        Ok(format!(
            "{:?}{:?}\n",
            datadriven.trace.since().elements(),
            datadriven.trace.upper().elements()
        ))
    }

    pub fn batches(datadriven: &TraceState, _args: DirectiveArgs) -> Result<String, anyhow::Error> {
        let mut s = String::new();
        for b in datadriven.trace.spine.spine_batches() {
            s.push_str(b.describe(true).as_str());
            s.push('\n');
        }
        Ok(s)
    }

    pub fn insert(
        datadriven: &mut TraceState,
        args: DirectiveArgs,
    ) -> Result<String, anyhow::Error> {
        for x in args
            .input
            .trim()
            .split('\n')
            .map(DirectiveArgs::parse_hollow_batch)
        {
            datadriven
                .merge_reqs
                .append(&mut datadriven.trace.push_batch(x));
        }
        Ok("ok\n".to_owned())
    }

    pub fn downgrade_since(
        datadriven: &mut TraceState,
        args: DirectiveArgs,
    ) -> Result<String, anyhow::Error> {
        let since = args.expect("since");
        datadriven
            .trace
            .downgrade_since(&Antichain::from_elem(since));
        Ok("ok\n".to_owned())
    }

    pub fn take_merge_req(
        datadriven: &mut TraceState,
        _args: DirectiveArgs,
    ) -> Result<String, anyhow::Error> {
        let mut s = String::new();
        for merge_req in std::mem::take(&mut datadriven.merge_reqs) {
            write!(
                s,
                "{:?}{:?}{:?} {}\n",
                merge_req.desc.lower().elements(),
                merge_req.desc.upper().elements(),
                merge_req.desc.since().elements(),
                merge_req
                    .inputs
                    .iter()
                    .flat_map(|x| x.batch.parts.iter())
                    .map(|x| x.printable_name())
                    .collect::<Vec<_>>()
                    .join(" ")
            );
        }
        Ok(s)
    }

    pub fn apply_merge_res(
        datadriven: &mut TraceState,
        args: DirectiveArgs,
    ) -> Result<String, anyhow::Error> {
        let res = FueledMergeRes {
            output: DirectiveArgs::parse_hollow_batch(args.input),
        };
        match datadriven.trace.apply_merge_res(&res) {
            ApplyMergeResult::AppliedExact => Ok("applied exact\n".into()),
            ApplyMergeResult::AppliedSubset => Ok("applied subset\n".into()),
            ApplyMergeResult::NotAppliedNoMatch => Ok("no-op\n".into()),
            ApplyMergeResult::NotAppliedInvalidSince => Ok("no-op invalid since\n".into()),
            ApplyMergeResult::NotAppliedTooManyUpdates => Ok("no-op too many updates\n".into()),
        }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use std::ops::Range;

    use proptest::prelude::*;

    use crate::internal::state::tests::any_hollow_batch;

    use super::*;

    pub fn any_trace<T: Arbitrary + Timestamp + Lattice>(
        num_batches: Range<usize>,
    ) -> impl Strategy<Value = Trace<T>> {
        Strategy::prop_map(
            (
                any::<Option<T>>(),
                proptest::collection::vec(any_hollow_batch::<T>(), num_batches),
                any::<bool>(),
            ),
            |(since, mut batches, roundtrip_structure)| {
                let mut trace = Trace::<T>::default();
                trace.downgrade_since(&since.map_or_else(Antichain::new, Antichain::from_elem));

                // Fix up the arbitrary HollowBatches so the lowers and uppers
                // align.
                batches.sort_by(|x, y| x.desc.upper().elements().cmp(y.desc.upper().elements()));
                let mut lower = Antichain::from_elem(T::minimum());
                for mut batch in batches {
                    // Overall trace since has to be past each batch's since.
                    if PartialOrder::less_than(trace.since(), batch.desc.since()) {
                        trace.downgrade_since(batch.desc.since());
                    }
                    batch.desc = Description::new(
                        lower.clone(),
                        batch.desc.upper().clone(),
                        batch.desc.since().clone(),
                    );
                    lower = batch.desc.upper().clone();
                    let _merge_req = trace.push_batch(batch);
                }
                trace.roundtrip_structure = roundtrip_structure;
                trace
            },
        )
    }

    #[mz_ore::test]
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

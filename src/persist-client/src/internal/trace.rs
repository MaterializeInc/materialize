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
//!   fueled, the `FuelingMerge` is turned into a fueled [SpineBatch],
//!   which to the Spine is indistinguishable from a merged batch. At this
//!   point, it is eligible for asynchronous compaction and a `FueledMergeReq`
//!   is generated.
//! - At any later point, this request may be answered via
//!   [Trace::apply_merge_res_checked] or [Trace::apply_merge_res_unchecked].
//!   This internally replaces the`SpineBatch`, which has no
//!   effect on the structure of `Spine` but replaces the metadata
//!   in persist's state to point at the new batch.
//! - `SpineBatch` is explictly allowed to accumulate a list of `HollowBatch`s.
//!   This decouples compaction from Spine progress and also allows us to reduce
//!   write amplification by merging `N` batches at once where `N` can be
//!   greater than 2.
//!
//! [Batch]: differential_dataflow::trace::Batch
//! [Batch::Merger]: differential_dataflow::trace::Batch::Merger

use arrayvec::ArrayVec;
use differential_dataflow::difference::Semigroup;
use itertools::Itertools;
use mz_persist::metrics::ColumnarMetrics;
use mz_persist_types::Codec64;
use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::fmt::Debug;
use std::mem;
use std::ops::Range;
use std::sync::Arc;
use tracing::warn;

use crate::internal::paths::WriterKey;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::trace::Description;
use mz_ore::cast::CastFrom;
#[allow(unused_imports)] // False positive.
use mz_ore::fmt::FormatBuffer;
use serde::{Serialize, Serializer};
use timely::PartialOrder;
use timely::progress::frontier::AntichainRef;
use timely::progress::{Antichain, Timestamp};

use crate::internal::state::{HollowBatch, RunId};

use super::state::RunPart;

#[derive(Debug, Clone, PartialEq)]
pub struct FueledMergeReq<T> {
    pub id: SpineId,
    pub desc: Description<T>,
    pub inputs: Vec<IdHollowBatch<T>>,
}

#[derive(Debug)]
pub struct FueledMergeRes<T> {
    pub output: HollowBatch<T>,
    pub inputs: Vec<RunLocation>,
    pub new_active_compaction: Option<ActiveCompaction>,
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
            roundtrip_structure: true,
        }
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct ThinSpineBatch<T> {
    pub(crate) level: usize,
    pub(crate) desc: Description<T>,
    pub(crate) parts: Vec<SpineId>,
    /// NB: this exists to validate legacy batch bounds during the migration;
    /// it can be deleted once the roundtrip_structure flag is permanently rolled out.
    pub(crate) descs: Vec<Description<T>>,
}

impl<T: PartialEq> PartialEq for ThinSpineBatch<T> {
    fn eq(&self, other: &Self) -> bool {
        // Ignore the temporary descs vector when comparing for equality.
        (self.level, &self.desc, &self.parts).eq(&(other.level, &other.desc, &other.parts))
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub struct ThinMerge<T> {
    pub(crate) since: Antichain<T>,
    pub(crate) remaining_work: usize,
    pub(crate) active_compaction: Option<ActiveCompaction>,
}

impl<T: Clone> ThinMerge<T> {
    fn fueling(merge: &FuelingMerge<T>) -> Self {
        ThinMerge {
            since: merge.since.clone(),
            remaining_work: merge.remaining_work,
            active_compaction: None,
        }
    }

    fn fueled(batch: &SpineBatch<T>) -> Self {
        ThinMerge {
            since: batch.desc.since().clone(),
            remaining_work: 0,
            active_compaction: batch.active_compaction.clone(),
        }
    }
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
    pub(crate) merges: BTreeMap<SpineId, ThinMerge<T>>,
}

impl<T: Timestamp + Lattice> Trace<T> {
    pub(crate) fn flatten(&self) -> FlatTrace<T> {
        let since = self.spine.since.clone();
        let mut legacy_batches = BTreeMap::new();
        let mut hollow_batches = BTreeMap::new();
        let mut spine_batches = BTreeMap::new();
        let mut merges = BTreeMap::new();

        let mut push_spine_batch = |level: usize, batch: &SpineBatch<T>| {
            let id = batch.id();
            let desc = batch.desc.clone();
            let mut parts = Vec::with_capacity(batch.parts.len());
            let mut descs = Vec::with_capacity(batch.parts.len());
            for IdHollowBatch { id, batch } in &batch.parts {
                parts.push(*id);
                descs.push(batch.desc.clone());
                // Ideally, we'd like to put all batches in the hollow_batches collection, since
                // tracking the spine id reduces ambiguity and makes diffing cheaper. However,
                // we currently keep most batches in the legacy collection for backwards
                // compatibility.
                // As an exception, we add batches with empty time ranges to hollow_batches:
                // they're otherwise not guaranteed to be unique, and since we only started writing
                // them down recently there's no backwards compatibility risk.
                if batch.desc.lower() == batch.desc.upper() {
                    hollow_batches.insert(*id, Arc::clone(batch));
                    assert_eq!(
                        hollow_batches.get(id).map(|b| b.desc.clone()),
                        descs.last().cloned()
                    );
                } else {
                    legacy_batches.insert(Arc::clone(batch), ());
                    assert_eq!(batch.desc, descs.last().unwrap().clone());
                }
            }

            let spine_batch = ThinSpineBatch {
                level,
                desc,
                parts,
                descs,
            };
            spine_batches.insert(id, spine_batch);
        };

        for (level, state) in self.spine.merging.iter().enumerate() {
            for batch in &state.batches {
                push_spine_batch(level, batch);
                if let Some(c) = &batch.active_compaction {
                    let previous = merges.insert(batch.id, ThinMerge::fueled(batch));
                    assert!(
                        previous.is_none(),
                        "recording a compaction for a batch that already exists! (level={level}, id={:?}, compaction={c:?})",
                        batch.id,
                    )
                }
            }
            if let Some(IdFuelingMerge { id, merge }) = state.merge.as_ref() {
                let previous = merges.insert(*id, ThinMerge::fueling(merge));
                assert!(
                    previous.is_none(),
                    "fueling a merge for a batch that already exists! (level={level}, id={id:?}, merge={merge:?})"
                )
            }
        }

        if !self.roundtrip_structure {
            assert!(hollow_batches.is_empty());
            spine_batches.clear();
            merges.clear();
        }

        FlatTrace {
            since,
            legacy_batches,
            hollow_batches,
            spine_batches,
            merges,
        }
    }
    pub(crate) fn unflatten(value: FlatTrace<T>) -> Result<Self, String> {
        let flat_trace_clone = value.clone();
        let FlatTrace {
            since,
            legacy_batches,
            mut hollow_batches,
            spine_batches,
            mut merges,
        } = value;

        // If the flattened representation has spine batches (or is empty)
        // we know to preserve the structure for this trace.
        let roundtrip_structure = !spine_batches.is_empty() || legacy_batches.is_empty();

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

        let mut pop_batch =
            |id: SpineId, expected_desc: Option<&Description<T>>| -> Result<_, String> {
                if let Some(batch) = hollow_batches.remove(&id) {
                    if let Some(desc) = expected_desc {
                        assert_eq!(*desc, batch.desc);
                    }
                    return Ok(IdHollowBatch { id, batch });
                }
                let mut batch = legacy_batches
                    .pop()
                    .ok_or_else(|| format!("missing referenced hollow batch {id:?}"))?;

                let Some(expected_desc) = expected_desc else {
                    return Ok(IdHollowBatch { id, batch });
                };

                if expected_desc.lower() != batch.desc.lower() {
                    return Err(format!(
                        "hollow batch lower {:?} did not match expected lower {:?}",
                        batch.desc.lower().elements(),
                        expected_desc.lower().elements()
                    ));
                }

                // Empty legacy batches are not deterministic: different nodes may split them up
                // in different ways. For now, we rearrange them such to match the spine data.
                if batch.parts.is_empty() && batch.run_splits.is_empty() && batch.len == 0 {
                    let mut new_upper = batch.desc.upper().clone();

                    // While our current batch is too small, and there's another empty batch
                    // in the list, roll it in.
                    while PartialOrder::less_than(&new_upper, expected_desc.upper()) {
                        let Some(next_batch) = legacy_batches.pop() else {
                            break;
                        };
                        if next_batch.is_empty() {
                            new_upper.clone_from(next_batch.desc.upper());
                        } else {
                            legacy_batches.push(next_batch);
                            break;
                        }
                    }

                    // If our current batch is too large, split it by the expected upper
                    // and preserve the remainder.
                    if PartialOrder::less_than(expected_desc.upper(), &new_upper) {
                        legacy_batches.push(Arc::new(HollowBatch::empty(Description::new(
                            expected_desc.upper().clone(),
                            new_upper.clone(),
                            batch.desc.since().clone(),
                        ))));
                        new_upper.clone_from(expected_desc.upper());
                    }
                    batch = Arc::new(HollowBatch::empty(Description::new(
                        batch.desc.lower().clone(),
                        new_upper,
                        batch.desc.since().clone(),
                    )))
                }

                if expected_desc.upper() != batch.desc.upper() {
                    return Err(format!(
                        "hollow batch upper {:?} did not match expected upper {:?}",
                        batch.desc.upper().elements(),
                        expected_desc.upper().elements()
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
        let mut merging = vec![MergeState::default(); levels];
        for (id, batch) in spine_batches {
            let level = batch.level;

            let parts = batch
                .parts
                .into_iter()
                .zip(batch.descs.iter().map(Some).chain(std::iter::repeat(None)))
                .map(|(id, desc)| pop_batch(id, desc))
                .collect::<Result<Vec<_>, _>>()?;
            let len = parts.iter().map(|p| (*p).batch.len).sum();
            let active_compaction = merges.remove(&id).and_then(|m| m.active_compaction);
            let batch = SpineBatch {
                id,
                desc: batch.desc,
                parts,
                active_compaction,
                len,
            };

            let state = &mut merging[level];

            state.push_batch(batch);
            if let Some(id) = state.id() {
                if let Some(merge) = merges.remove(&id) {
                    state.merge = Some(IdFuelingMerge {
                        id,
                        merge: FuelingMerge {
                            since: merge.since,
                            remaining_work: merge.remaining_work,
                        },
                    })
                }
            }
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
        check_empty("merges", merges.len())?;

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
            .flat_map(|b| b.parts.as_slice())
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

    pub fn claim_compaction(&mut self, id: SpineId, compaction: ActiveCompaction) {
        // TODO: we ought to be able to look up the id for a batch by binary searching the levels.
        // In the meantime, search backwards, since most compactions are for recent batches.
        for batch in self.spine.spine_batches_mut().rev() {
            if batch.id == id {
                batch.active_compaction = Some(compaction);
                break;
            }
        }
    }

    /// The same as [Self::push_batch] but without the `FueledMergeReq`s, which
    /// account for a surprising amount of cpu in prod. database-issues#5411
    pub(crate) fn push_batch_no_merge_reqs(&mut self, batch: HollowBatch<T>) {
        self.spine.insert(batch, &mut SpineLog::Disabled);
    }

    /// Apply some amount of effort to trace maintenance.
    ///
    /// The units of effort are updates, and the method should be thought of as
    /// analogous to inserting as many empty updates, where the trace is
    /// permitted to perform proportionate work.
    ///
    /// Returns true if this did work and false if it left the spine unchanged.
    #[must_use]
    pub fn exert(&mut self, fuel: usize) -> (Vec<FueledMergeReq<T>>, bool) {
        let mut merge_reqs = Vec::new();
        let did_work = self.spine.exert(
            fuel,
            &mut SpineLog::Enabled {
                merge_reqs: &mut merge_reqs,
            },
        );
        debug_assert_eq!(self.spine.validate(), Ok(()), "{:?}", self);
        // See the comment in [Self::push_batch].
        let merge_reqs = Self::remove_redundant_merge_reqs(merge_reqs);
        (merge_reqs, did_work)
    }

    /// Validates invariants.
    ///
    /// See `Spine::validate` for details.
    pub fn validate(&self) -> Result<(), String> {
        self.spine.validate()
    }

    /// Obtain all fueled merge reqs that either have no active compaction, or the previous
    /// compaction was started at or before the threshold time, in order from oldest to newest.
    pub(crate) fn fueled_merge_reqs_before_ms(
        &self,
        threshold_ms: u64,
        threshold_writer: Option<WriterKey>,
    ) -> impl Iterator<Item = FueledMergeReq<T>> + '_ {
        self.spine
            .spine_batches()
            .filter(move |b| {
                let noncompact = !b.is_compact();
                let old_writer = threshold_writer.as_ref().map_or(false, |min_writer| {
                    b.parts.iter().any(|b| {
                        b.batch
                            .parts
                            .iter()
                            .any(|p| p.writer_key().map_or(false, |writer| writer < *min_writer))
                    })
                });
                noncompact || old_writer
            })
            .filter(move |b| {
                // Either there's no active compaction, or the last active compaction
                // is not after the timeout timestamp.
                b.active_compaction
                    .as_ref()
                    .map_or(true, move |c| c.start_ms <= threshold_ms)
            })
            .map(|b| FueledMergeReq {
                id: b.id,
                desc: b.desc.clone(),
                inputs: b.parts.clone(),
            })
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
            // TODO: can we relax or remove this since check?
            b0.id.covers(b1.id) && b0.desc.since() == b1.desc.since()
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

impl<T: Timestamp + Lattice + Codec64> Trace<T> {
    pub fn apply_merge_res_checked<D: Codec64 + Semigroup + PartialEq>(
        &mut self,
        res: &FueledMergeRes<T>,
        metrics: &ColumnarMetrics,
    ) -> ApplyMergeResult {
        for batch in self.spine.spine_batches_mut().rev() {
            let result = batch.maybe_replace_checked::<D>(res, metrics);
            if result.matched() {
                return result;
            }
        }
        ApplyMergeResult::NotAppliedNoMatch
    }

    pub fn apply_merge_res_unchecked(&mut self, res: &FueledMergeRes<T>) -> ApplyMergeResult {
        for batch in self.spine.spine_batches_mut().rev() {
            let result = batch.maybe_replace_unchecked(res);
            if result.matched() {
                return result;
            }
        }
        ApplyMergeResult::NotAppliedNoMatch
    }

    pub fn apply_tombstone_merge(&mut self, res: &FueledMergeRes<T>) -> ApplyMergeResult {
        for batch in self.spine.spine_batches_mut().rev() {
            let result = batch.maybe_replace_with_tombstone(res);
            if result.matched() {
                return result;
            }
        }
        ApplyMergeResult::NotAppliedNoMatch
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
/// A RunId uniquely identifies a run within a hollow batch.
/// It is a pair of `SpineId` and an index within that batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct RunLocation(pub SpineId, pub Option<RunId>);

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
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

impl SpineId {
    fn covers(self, other: SpineId) -> bool {
        self.0 <= other.0 && other.1 <= self.1
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct IdHollowBatch<T> {
    pub id: SpineId,
    pub batch: Arc<HollowBatch<T>>,
}

#[derive(Debug, Clone, Eq, PartialEq, Serialize)]
pub struct ActiveCompaction {
    pub start_ms: u64,
}

#[derive(Debug, Clone, PartialEq)]
struct SpineBatch<T> {
    id: SpineId,
    desc: Description<T>,
    parts: Vec<IdHollowBatch<T>>,
    active_compaction: Option<ActiveCompaction>,
    // A cached version of parts.iter().map(|x| x.len).sum()
    len: usize,
}

impl<T> SpineBatch<T> {
    fn merged(batch: IdHollowBatch<T>, active_compaction: Option<ActiveCompaction>) -> Self
    where
        T: Clone,
    {
        Self {
            id: batch.id,
            desc: batch.batch.desc.clone(),
            len: batch.batch.len,
            parts: vec![batch],
            active_compaction,
        }
    }
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
        debug_assert_eq!(self.parts.first().map(|x| x.id.0), Some(self.id.0));
        debug_assert_eq!(self.parts.last().map(|x| x.id.1), Some(self.id.1));
        self.id
    }

    pub fn is_compact(&self) -> bool {
        // A compact batch has at most one run.
        // This check used to be if there was at most one hollow batch with at most one run,
        // but that was a bit too strict since introducing incremental compaction.
        // Incremental compaction can result in a batch with a single run, but multiple empty
        // hollow batches, which we still consider compact. As levels are merged, we
        // will eventually clean up the empty hollow batches.
        self.parts.iter().all(|p| p.batch.run_splits.is_empty())
    }

    pub fn is_merging(&self) -> bool {
        self.active_compaction.is_some()
    }

    fn desc(&self) -> &Description<T> {
        &self.desc
    }

    pub fn len(&self) -> usize {
        // NB: This is an upper bound on len for a non-compact batch; we won't know for sure until
        // we compact it.
        debug_assert_eq!(
            self.len,
            self.parts.iter().map(|x| x.batch.len).sum::<usize>()
        );
        self.len
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
        SpineBatch::merged(
            IdHollowBatch {
                id,
                batch: Arc::new(HollowBatch::empty(Description::new(lower, upper, since))),
            },
            None,
        )
    }

    pub fn begin_merge(
        bs: &[Self],
        compaction_frontier: Option<AntichainRef<T>>,
    ) -> Option<IdFuelingMerge<T>> {
        let from = bs.first()?.id().0;
        let until = bs.last()?.id().1;
        let id = SpineId(from, until);
        let mut sinces = bs.iter().map(|b| b.desc().since());
        let mut since = sinces.next()?.clone();
        for b in bs {
            since.join_assign(b.desc().since())
        }
        if let Some(compaction_frontier) = compaction_frontier {
            since.join_assign(&compaction_frontier.to_owned());
        }
        let remaining_work = bs.iter().map(|x| x.len()).sum();
        Some(IdFuelingMerge {
            id,
            merge: FuelingMerge {
                since,
                remaining_work,
            },
        })
    }

    #[cfg(test)]
    fn describe(&self, extended: bool) -> String {
        let SpineBatch {
            id,
            parts,
            desc,
            active_compaction,
            len,
        } = self;
        let compaction = match active_compaction {
            None => "".to_owned(),
            Some(c) => format!(" (c@{})", c.start_ms),
        };
        match extended {
            false => format!(
                "[{}-{}]{:?}{:?}{}/{}{compaction}",
                id.0,
                id.1,
                desc.lower().elements(),
                desc.upper().elements(),
                parts.len(),
                len
            ),
            true => {
                format!(
                    "[{}-{}]{:?}{:?}{:?} {}/{}{}{compaction}",
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

impl<T: Timestamp + Lattice + Codec64> SpineBatch<T> {
    fn diffs_sum<'a, D: Semigroup + Codec64>(
        parts: impl Iterator<Item = &'a RunPart<T>>,
        metrics: &ColumnarMetrics,
    ) -> Option<D> {
        parts
            .map(|p| p.diffs_sum::<D>(metrics))
            .reduce(|a, b| match (a, b) {
                (Some(mut a), Some(b)) => {
                    a.plus_equals(&b);
                    Some(a)
                }
                _ => None,
            })
            .flatten()
    }

    fn diffs_sum_for_runs<D: Semigroup + Codec64>(
        batch: &HollowBatch<T>,
        run_ids: &[RunId],
        metrics: &ColumnarMetrics,
    ) -> Option<D> {
        if run_ids.is_empty() {
            return None;
        }

        let mut parts = Vec::new();
        for &run_id in run_ids {
            for (i, meta) in batch.run_meta.iter().enumerate() {
                if meta.id == Some(run_id) {
                    let start = if i == 0 { 0 } else { batch.run_splits[i - 1] };
                    let end = batch
                        .run_splits
                        .get(i)
                        .copied()
                        .unwrap_or(batch.parts.len());
                    parts.extend_from_slice(&batch.parts[start..end]);
                }
            }
        }

        Self::diffs_sum(parts.iter(), metrics)
    }

    fn construct_batch_with_runs_replaced(
        original: &HollowBatch<T>,
        run_ids: &[RunId],
        replacement: &HollowBatch<T>,
    ) -> Result<HollowBatch<T>, ApplyMergeResult> {
        if run_ids.is_empty() {
            return Err(ApplyMergeResult::NotAppliedNoMatch);
        }

        assert!(
            replacement.run_meta.len() <= 1,
            "replacement must have exactly 0 or 1 runs"
        );

        let mut run_ids = run_ids.to_vec();

        // This is a defensive check to ensure that the run IDs are in the same order
        // as they appear in the original batch. There isn't currently anywhere that
        // guarantees this, but it is expected that the run IDs will be in order.
        run_ids.sort_by(|a, b| {
            original
                .run_meta
                .iter()
                .position(|m| m.id == Some(*a))
                .unwrap_or(usize::MAX)
                .cmp(
                    &original
                        .run_meta
                        .iter()
                        .position(|m| m.id == Some(*b))
                        .unwrap_or(usize::MAX),
                )
        });

        let start_id = run_ids[0];
        let end_id = *run_ids.last().unwrap();

        // 0. Find the indices of the runs in the original batch.
        let mut start_run = 0;
        let mut end_run = 0;
        let mut found_start = false;
        let mut found_end = false;
        for (i, meta) in original.run_meta.iter().enumerate() {
            if meta.id == Some(start_id) {
                found_start = true;
                start_run = i;
            }
            if meta.id == Some(end_id) {
                found_end = true;
                end_run = i;
            }
        }

        if !found_start || !found_end {
            warn!(
                "Failed to find run IDs in original batch: start_id={start_id:?}, end_id={end_id:?}"
            );
            warn!("original batch: {original:#?}");
            warn!("replacement batch: {replacement:#?}");
            return Err(ApplyMergeResult::NotAppliedNoMatch);
        }

        let replaced_runs_num_updates = original
            .run_meta
            .iter()
            .filter(|meta| run_ids.contains(&meta.id.expect("id should be present at this point")))
            .filter_map(|meta| meta.len)
            .sum::<usize>();

        // 1. Determine the parts to replace.
        let start_part = if start_run == 0 {
            0
        } else {
            original.run_splits[start_run - 1]
        };
        let end_part = if end_run < original.run_splits.len() {
            original.run_splits[end_run]
        } else {
            original.parts.len()
        };

        // 2. Replace parts
        let mut parts = Vec::new();
        parts.extend_from_slice(&original.parts[..start_part]);
        parts.extend_from_slice(&replacement.parts);
        parts.extend_from_slice(&original.parts[end_part..]);

        // 3. Replace run_meta
        let mut run_meta = Vec::new();
        run_meta.extend_from_slice(&original.run_meta[..start_run]);
        run_meta.extend_from_slice(&replacement.run_meta);
        run_meta.extend_from_slice(&original.run_meta[end_run + 1..]);

        // 4. Rebuild run_splits
        let mut run_splits = Vec::with_capacity(run_meta.len());
        let replaced_start = if start_run == 0 {
            0
        } else {
            original.run_splits[start_run - 1]
        };
        let replaced_end = if end_run < original.run_splits.len() {
            original.run_splits[end_run]
        } else {
            original.parts.len()
        };
        let replaced_len = replaced_end - replaced_start;
        let replacement_len = replacement.parts.len();

        let prefix = &original.run_splits[..start_run];
        run_splits.extend_from_slice(prefix);

        let replacement_idx = start_run;
        let replacement_is_last = replacement_idx + replacement.run_meta.len() == run_meta.len();

        // If we deleted runs, and the replacement is the last run,
        // we can remove the last split.
        if replacement.run_meta.is_empty() && replacement_is_last {
            run_splits.pop();
        }

        // Only push the replacement split if it's not the final run and the replacement
        // has runs to add.
        if !replacement.run_meta.is_empty() && !replacement_is_last {
            run_splits.push(replaced_start + replacement_len);
        }

        // 5. Adjust suffix splits
        if end_run + 1 < original.run_splits.len() {
            for &split in &original.run_splits[(end_run + 1)..] {
                let adjusted = split - replaced_len + replacement_len;
                run_splits.push(adjusted);
            }
        }

        assert_eq!(
            run_splits.len(),
            run_meta.len().saturating_sub(1),
            "run_splits must have one fewer element than run_meta"
        );

        let desc = replacement.desc.clone();

        warn!(
            "recalculating len: original len={}, replaced runs len={} replacement len={}",
            original.len, replaced_runs_num_updates, replacement.len,
        );
        let len = original.len - replaced_runs_num_updates + replacement.len;

        Ok(HollowBatch {
            desc: desc.clone(),
            len,
            parts,
            run_meta,
            run_splits,
        })
    }

    fn maybe_replace_checked<D>(
        &mut self,
        res: &FueledMergeRes<T>,
        metrics: &ColumnarMetrics,
    ) -> ApplyMergeResult
    where
        D: Semigroup + Codec64 + PartialEq + Debug,
    {
        // The spine's and merge res's sinces don't need to match (which could occur if Spine
        // has been reloaded from state due to compare_and_set mismatch), but if so, the Spine
        // since must be in advance of the merge res since.
        if !PartialOrder::less_equal(res.output.desc.since(), self.desc().since()) {
            return ApplyMergeResult::NotAppliedInvalidSince;
        }

        let new_diffs_sum = Self::diffs_sum(res.output.parts.iter(), metrics);

        let inputs = res.inputs.clone();

        let inputs = inputs
            .into_iter()
            .sorted()
            .chunk_by(|RunLocation(spine, _)| spine.clone())
            .into_iter()
            .map(|(id, batch)| (id, batch.collect::<Vec<_>>()))
            .collect::<BTreeMap<_, _>>();

        // The merge result can replace either:
        // 1. Specific runs within a single HollowBatch, or
        // 2. One or more complete contiguous HollowBatches
        //
        // Example SpineBatch with parts [A, B, C] where each part has runs:
        // Part A: runs [0, 1, 2, 3]
        // Part B: runs [0, 1, 2]
        // Part C: runs [0, 1]
        //
        // Valid replacements:
        // - Replace runs [1,2] from part A only (partial batch replacement)
        // - Replace all of part B and part C (complete batch replacement)
        // - Replace all of parts A, B, and C (complete batch replacement)
        //
        // Invalid replacements:
        // - Replace run [1] from part A and run [0] from part B (non-contiguous)
        // - Replace runs [1,2] from part A and part B entirely (mixed partial/complete)

        let mut range = Vec::new();
        for (spine_id, _) in inputs.iter() {
            let part = self
                .parts
                .iter()
                .enumerate()
                .find(|(_, p)| p.id == *spine_id);
            let Some((i, _)) = part else {
                return ApplyMergeResult::NotAppliedNoMatch;
            };
            range.push(i);
        }

        range.sort_unstable();
        let is_contiguous = range.windows(2).all(|w| {
            let [a, b] = [w[0], w[1]];
            let skipped = &self.parts[a + 1..b];
            skipped.iter().all(|p| p.batch.runs().next().is_none())
        });
        assert!(
            is_contiguous,
            "parts to replace are not contiguous: {:?}",
            range
        );

        // This is the range of hollow batches that we will replace.
        let min = *range.iter().min().unwrap();
        let max = *range.iter().max().unwrap();
        let replacement_range = min..max + 1;
        let num_batches = self.parts.len();

        let res = if range.len() == 1 {
            // We only need to replace a single part. Here we still care about the run_indices
            // because we only want to replace the runs that are in the merge result.
            let batch = &self.parts[range[0]];
            let batch = &batch.batch;
            let run_ids = inputs
                .values()
                .next()
                .unwrap()
                .iter()
                .filter_map(|id| id.1)
                .collect::<Vec<_>>();

            // backwards compatibility: if the run_ids are empty, we assume we want to replace all runs
            let old_batch_diff_sum = Self::diffs_sum::<D>(batch.parts.iter(), metrics);
            let old_diffs_sum = if run_ids.is_empty() {
                old_batch_diff_sum.clone()
            } else {
                // If we have run_ids, we need to compute the diffs sum for those runs only
                Self::diffs_sum_for_runs::<D>(batch, &run_ids, metrics)
            };

            if let (Some(old_diffs_sum), Some(new_diffs_sum)) = (old_diffs_sum, new_diffs_sum) {
                if old_diffs_sum != new_diffs_sum {
                    warn!(
                        "merge res diffs sum ({:?}) did not match spine batch diffs sum ({:?})",
                        new_diffs_sum, old_diffs_sum
                    );
                    warn!("merge res: {res:#?}");
                    warn!("spine batch: {batch:#?}");
                    warn!("run_ids: {run_ids:?}");
                    warn!("replacement_range: {replacement_range:?}");
                }
                assert_eq!(
                    old_diffs_sum, new_diffs_sum,
                    "merge res diffs sum ({:?}) did not match spine batch diffs sum ({:?})",
                    new_diffs_sum, old_diffs_sum
                );
            }

            let parts = &self.parts[replacement_range.clone()];
            let id = SpineId(parts.first().unwrap().id.0, parts.last().unwrap().id.1);

            match Self::construct_batch_with_runs_replaced(batch, &run_ids, &res.output) {
                Ok(new_batch) => {
                    let new_batch_diff_sum = Self::diffs_sum::<D>(new_batch.parts.iter(), metrics);
                    if let (Some(old_diffs_sum), Some(new_diffs_sum)) =
                        (old_batch_diff_sum, new_batch_diff_sum)
                    {
                        assert_eq!(
                            old_diffs_sum, new_diffs_sum,
                            "merge res diffs sum ({:?}) did not match spine batch diffs sum ({:?})",
                            new_diffs_sum, old_diffs_sum
                        );
                    }
                    self.perform_subset_replacement(
                        &new_batch,
                        id,
                        replacement_range,
                        res.new_active_compaction.clone(),
                    )
                }
                Err(err) => err,
            }
        } else {
            // We need to replace a range of parts. Here we don't care about the run_indices
            // because we must be replacing the entire part(s)
            let old_diffs_sum = Self::diffs_sum::<D>(
                self.parts[replacement_range.clone()]
                    .iter()
                    .flat_map(|p| p.batch.parts.iter()),
                metrics,
            );

            if let (Some(old_diffs_sum), Some(new_diffs_sum)) = (old_diffs_sum, new_diffs_sum) {
                assert_eq!(
                    old_diffs_sum, new_diffs_sum,
                    "merge res diffs sum ({:?}) did not match spine batch diffs sum ({:?})",
                    new_diffs_sum, old_diffs_sum
                );
            }

            let parts = &self.parts[replacement_range.clone()];
            let id = SpineId(parts.first().unwrap().id.0, parts.last().unwrap().id.1);
            self.perform_subset_replacement(
                &res.output,
                id,
                replacement_range,
                res.new_active_compaction.clone(),
            )
        };
        let num_batches_after = self.parts.len();
        assert!(
            num_batches_after <= num_batches,
            "replacing parts should not increase the number of batches"
        );
        res
    }

    fn maybe_replace_with_tombstone(&mut self, res: &FueledMergeRes<T>) -> ApplyMergeResult {
        assert!(
            res.output.parts.is_empty(),
            "merge res for tombstone must have no parts"
        );
        let exact_match = res.output.desc.lower() == self.desc().lower()
            && res.output.desc.upper() == self.desc().upper();

        if exact_match {
            *self = SpineBatch::merged(
                IdHollowBatch {
                    id: self.id(),
                    batch: Arc::new(res.output.clone()),
                },
                None,
            );
            return ApplyMergeResult::AppliedExact;
        }

        if let Some((id, range)) = self.find_replacement_range(res) {
            self.perform_subset_replacement(&res.output, id, range, None)
        } else {
            ApplyMergeResult::NotAppliedNoMatch
        }
    }

    /// Unchecked variant that skips diff sum assertions
    fn maybe_replace_unchecked(&mut self, res: &FueledMergeRes<T>) -> ApplyMergeResult {
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

            *self = SpineBatch::merged(
                IdHollowBatch {
                    id: self.id(),
                    batch: Arc::new(res.output.clone()),
                },
                res.new_active_compaction.clone(),
            );
            return ApplyMergeResult::AppliedExact;
        }

        // Try subset replacement
        if let Some((id, range)) = self.find_replacement_range(res) {
            self.perform_subset_replacement(
                &res.output,
                id,
                range,
                res.new_active_compaction.clone(),
            )
        } else {
            ApplyMergeResult::NotAppliedNoMatch
        }
    }

    /// Find the range of parts that can be replaced by the merge result
    fn find_replacement_range(&self, res: &FueledMergeRes<T>) -> Option<(SpineId, Range<usize>)> {
        // It is possible the structure of the spine has changed since the merge res
        // was created, such that it no longer exactly matches the description of a
        // spine batch. This can happen if another merge has happened in the interim,
        // or if spine needed to be rebuilt from state.
        //
        // When this occurs, we can still attempt to slot the merge res in to replace
        // the parts of a fueled merge. e.g. if the res is for `[1,3)` and the parts
        // are `[0,1),[1,2),[2,3),[3,4)`, we can swap out the middle two parts for res.

        let mut lower = None;
        let mut upper = None;

        for (i, batch) in self.parts.iter().enumerate() {
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

        match (lower, upper) {
            (Some((lower_idx, id_lower)), Some((upper_idx, id_upper))) => {
                Some((SpineId(id_lower, id_upper), lower_idx..(upper_idx + 1)))
            }
            _ => None,
        }
    }

    /// Perform the actual subset replacement
    fn perform_subset_replacement(
        &mut self,
        res: &HollowBatch<T>,
        spine_id: SpineId,
        range: Range<usize>,
        new_active_compaction: Option<ActiveCompaction>,
    ) -> ApplyMergeResult {
        let SpineBatch {
            id,
            parts,
            desc,
            active_compaction: _,
            len: _,
        } = self;

        let mut new_parts = vec![];
        new_parts.extend_from_slice(&parts[..range.start]);
        new_parts.push(IdHollowBatch {
            id: spine_id,
            batch: Arc::new(res.clone()),
        });
        new_parts.extend_from_slice(&parts[range.end..]);

        let new_spine_batch = SpineBatch {
            id: *id,
            desc: desc.to_owned(),
            len: new_parts.iter().map(|x| x.batch.len).sum(),
            parts: new_parts,
            active_compaction: new_active_compaction,
        };

        if new_spine_batch.len() > self.len() {
            return ApplyMergeResult::NotAppliedTooManyUpdates;
        }

        *self = new_spine_batch;
        ApplyMergeResult::AppliedSubset
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct FuelingMerge<T> {
    pub(crate) since: Antichain<T>,
    pub(crate) remaining_work: usize,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct IdFuelingMerge<T> {
    id: SpineId,
    merge: FuelingMerge<T>,
}

impl<T: Timestamp + Lattice> FuelingMerge<T> {
    /// Perform some amount of work, decrementing `fuel`.
    ///
    /// If `fuel` is non-zero after the call, the merging is complete and one
    /// should call `done` to extract the merged results.
    // TODO(benesch): rewrite to avoid usage of `as`.
    #[allow(clippy::as_conversions)]
    fn work(&mut self, _: &[SpineBatch<T>], fuel: &mut isize) {
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
        bs: ArrayVec<SpineBatch<T>, BATCHES_PER_LEVEL>,
        log: &mut SpineLog<'_, T>,
    ) -> Option<SpineBatch<T>> {
        let first = bs.first()?;
        let last = bs.last()?;
        let id = SpineId(first.id().0, last.id().1);
        assert!(id.0 < id.1);
        let lower = first.desc().lower().clone();
        let upper = last.desc().upper().clone();
        let since = self.since;

        // Special case empty batches.
        if bs.iter().all(SpineBatch::is_empty) {
            return Some(SpineBatch::empty(id, lower, upper, since));
        }

        let desc = Description::new(lower, upper, since);
        let len = bs.iter().map(SpineBatch::len).sum();

        // Pre-size the merged_parts Vec. Benchmarking has shown that, at least
        // in the worst case, the double iteration is absolutely worth having
        // merged_parts pre-sized.
        let mut merged_parts_len = 0;
        for b in &bs {
            merged_parts_len += b.parts.len();
        }
        let mut merged_parts = Vec::with_capacity(merged_parts_len);
        for b in bs {
            merged_parts.extend(b.parts)
        }
        // Sanity check the pre-size code.
        debug_assert_eq!(merged_parts.len(), merged_parts_len);

        if let SpineLog::Enabled { merge_reqs } = log {
            merge_reqs.push(FueledMergeReq {
                id,
                desc: desc.clone(),
                inputs: merged_parts.clone(),
            });
        }

        Some(SpineBatch {
            id,
            desc,
            len,
            parts: merged_parts,
            active_compaction: None,
        })
    }
}

/// The maximum number of batches per level in the spine.
/// In practice, we probably want a larger max and a configurable soft cap, but using a
/// stack-friendly data structure and keeping this number low makes this safer during the
/// initial rollout.
const BATCHES_PER_LEVEL: usize = 2;

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
    /// All batches in the spine, oldest to newest.
    pub fn spine_batches(&self) -> impl Iterator<Item = &SpineBatch<T>> {
        self.merging.iter().rev().flat_map(|m| &m.batches)
    }

    /// All (mutable) batches in the spine, oldest to newest.
    pub fn spine_batches_mut(&mut self) -> impl DoubleEndedIterator<Item = &mut SpineBatch<T>> {
        self.merging.iter_mut().rev().flat_map(|m| &mut m.batches)
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

    /// Apply some amount of effort to trace maintenance.
    ///
    /// The units of effort are updates, and the method should be thought of as
    /// analogous to inserting as many empty updates, where the trace is
    /// permitted to perform proportionate work.
    ///
    /// Returns true if this did work and false if it left the spine unchanged.
    fn exert(&mut self, effort: usize, log: &mut SpineLog<'_, T>) -> bool {
        self.tidy_layers();
        if self.reduced() {
            return false;
        }

        if self.merging.iter().any(|b| b.merge.is_some()) {
            let fuel = isize::try_from(effort).unwrap_or(isize::MAX);
            // If any merges exist, we can directly call `apply_fuel`.
            self.apply_fuel(&fuel, log);
        } else {
            // Otherwise, we'll need to introduce fake updates to move merges
            // along.

            // Introduce an empty batch with roughly *effort number of virtual updates.
            let level = usize::cast_from(effort.next_power_of_two().trailing_zeros());
            let id = self.next_id();
            self.introduce_batch(
                SpineBatch::empty(
                    id,
                    self.upper.clone(),
                    self.upper.clone(),
                    self.since.clone(),
                ),
                level,
                log,
            );
        }
        true
    }

    pub fn next_id(&mut self) -> SpineId {
        let id = self.next_id;
        self.next_id += 1;
        SpineId(id, self.next_id)
    }

    // Ideally, this method acts as insertion of `batch`, even if we are not yet
    // able to begin merging the batch. This means it is a good time to perform
    // amortized work proportional to the size of batch.
    pub fn insert(&mut self, batch: HollowBatch<T>, log: &mut SpineLog<'_, T>) {
        assert!(batch.desc.lower() != batch.desc.upper());
        assert_eq!(batch.desc.lower(), &self.upper);

        let id = self.next_id();
        let batch = SpineBatch::merged(
            IdHollowBatch {
                id,
                batch: Arc::new(batch),
            },
            None,
        );

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
                        self.merging[position] = MergeState::single(merged);
                    }
                    return;
                }
            }
        }

        // Normal insertion for the batch.
        let index = batch.len().next_power_of_two();
        self.introduce_batch(batch, usize::cast_from(index.trailing_zeros()), log);
    }

    /// True iff there is at most one HollowBatch in `self.merging`.
    ///
    /// When true, there is no maintenance work to perform in the trace, other
    /// than compaction. We do not yet have logic in place to determine if
    /// compaction would improve a trace, so for now we are ignoring that.
    fn reduced(&self) -> bool {
        self.spine_batches()
            .flat_map(|b| b.parts.as_slice())
            .count()
            < 2
    }

    /// Describes the merge progress of layers in the trace.
    ///
    /// Intended for diagnostics rather than public consumption.
    #[allow(dead_code)]
    fn describe(&self) -> Vec<(usize, usize)> {
        self.merging
            .iter()
            .map(|b| (b.batches.len(), b.len()))
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
            self.merging.push(MergeState::default());
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
            if self.merging[index].is_full() {
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
            self.merging.push(MergeState::default());
        }

        // Insert the batch at the location.
        let merging = &mut self.merging[index];
        merging.push_batch(batch);
        if merging.batches.is_full() {
            let compaction_frontier = Some(self.since.borrow());
            merging.merge = SpineBatch::begin_merge(&merging.batches[..], compaction_frontier)
        }
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
                    let current = &mut self.merging[length - 2];
                    if current.is_vacant() {
                        // Vacant batches can be absorbed.
                        self.merging.remove(length - 2);
                        length = self.merging.len();
                    } else {
                        if !current.is_full() {
                            // Single batches may initiate a merge, if sizes are
                            // within bounds, but terminate the loop either way.

                            // Determine the number of records that might lead
                            // to a merge. Importantly, this is not the number
                            // of actual records, but the sum of upper bounds
                            // based on indices.
                            let mut smaller = 0;
                            for (index, batch) in self.merging[..(length - 2)].iter().enumerate() {
                                smaller += batch.batches.len() << index;
                            }

                            if smaller <= (1 << length) / 8 {
                                // Remove the batch under consideration (shifting the deeper batches up a level),
                                // then merge in the single batch at the current level.
                                let state = self.merging.remove(length - 2);
                                assert_eq!(state.batches.len(), 1);
                                for batch in state.batches {
                                    self.insert_at(batch, length - 2);
                                }
                            }
                        }
                        break;
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
            if x.is_full() != x.merge.is_some() {
                return Err(format!(
                    "all (and only) full batches should have fueling merges (full={}, merge={:?})",
                    x.is_full(),
                    x.merge,
                ));
            }

            if let Some(m) = &x.merge {
                if !x.is_full() {
                    return Err(format!(
                        "merge should only exist for full batches (len={:?}, merge={:?})",
                        x.batches.len(),
                        m.id,
                    ));
                }
                if x.id() != Some(m.id) {
                    return Err(format!(
                        "merge id should match the range of the batch ids (batch={:?}, merge={:?})",
                        x.id(),
                        m.id,
                    ));
                }
            }

            // TODO: Anything we can validate about x.merge? It'd
            // be nice to assert that it's bigger than the len of the
            // two batches, but apply_merge_res might swap those lengths
            // out from under us.
            for batch in &x.batches {
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
struct MergeState<T> {
    batches: ArrayVec<SpineBatch<T>, BATCHES_PER_LEVEL>,
    merge: Option<IdFuelingMerge<T>>,
}

impl<T> Default for MergeState<T> {
    fn default() -> Self {
        Self {
            batches: ArrayVec::new(),
            merge: None,
        }
    }
}

impl<T: Timestamp + Lattice> MergeState<T> {
    /// An id that covers all the batches in the given merge state, assuming there are any.
    fn id(&self) -> Option<SpineId> {
        if let (Some(first), Some(last)) = (self.batches.first(), self.batches.last()) {
            Some(SpineId(first.id().0, last.id().1))
        } else {
            None
        }
    }

    /// A new single-batch merge state.
    fn single(batch: SpineBatch<T>) -> Self {
        let mut state = Self::default();
        state.push_batch(batch);
        state
    }

    /// Push a new batch at this level, checking invariants.
    fn push_batch(&mut self, batch: SpineBatch<T>) {
        if let Some(last) = self.batches.last() {
            assert_eq!(last.id().1, batch.id().0);
            assert_eq!(last.upper(), batch.lower());
        }
        assert!(
            self.merge.is_none(),
            "Attempted to insert batch into incomplete merge! (batch={:?}, batch_count={})",
            batch.id,
            self.batches.len(),
        );
        self.batches
            .try_push(batch)
            .expect("Attempted to insert batch into full layer!");
    }

    /// The number of actual updates contained in the level.
    fn len(&self) -> usize {
        self.batches.iter().map(SpineBatch::len).sum()
    }

    /// True if this merge state contains no updates.
    fn is_empty(&self) -> bool {
        self.batches.iter().all(SpineBatch::is_empty)
    }

    /// True if this level contains no batches.
    fn is_vacant(&self) -> bool {
        self.batches.is_empty()
    }

    /// True only for a single-batch state.
    fn is_single(&self) -> bool {
        self.batches.len() == 1
    }

    /// True if this merge cannot hold any more batches.
    /// (i.e. for a binary merge tree, true if this layer holds two batches.)
    fn is_full(&self) -> bool {
        self.batches.is_full()
    }

    /// Immediately complete any merge.
    ///
    /// The result is either a batch, if there is a non-trivial batch to return
    /// or `None` if there is no meaningful batch to return.
    ///
    /// There is the additional option of input batches.
    fn complete(&mut self, log: &mut SpineLog<'_, T>) -> Option<SpineBatch<T>> {
        let mut this = mem::take(self);
        if this.batches.len() <= 1 {
            this.batches.pop()
        } else {
            // Merge the remaining batches, regardless of whether we have a fully fueled merge.
            let id_merge = this
                .merge
                .or_else(|| SpineBatch::begin_merge(&self.batches[..], None))?;
            id_merge.merge.done(this.batches, log)
        }
    }

    /// True iff the layer is a complete merge, ready for extraction.
    fn is_complete(&self) -> bool {
        match &self.merge {
            Some(IdFuelingMerge { merge, .. }) => merge.remaining_work == 0,
            None => false,
        }
    }

    /// Performs a bounded amount of work towards a merge.
    fn work(&mut self, fuel: &mut isize) {
        // We only perform work for merges in progress.
        if let Some(IdFuelingMerge { merge, .. }) = &mut self.merge {
            merge.work(&self.batches[..], fuel)
        }
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
            inputs: vec![],
            new_active_compaction: None,
        };
        match datadriven.trace.apply_merge_res_unchecked(&res) {
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
    use semver::Version;

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
                any::<u64>(),
            ),
            |(since, mut batches, roundtrip_structure, timeout_ms)| {
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
                    lower.clone_from(batch.desc.upper());
                    let _merge_req = trace.push_batch(batch);
                }
                let reqs: Vec<_> = trace
                    .fueled_merge_reqs_before_ms(timeout_ms, None)
                    .collect();
                for req in reqs {
                    trace.claim_compaction(req.id, ActiveCompaction { start_ms: 0 })
                }
                trace.roundtrip_structure = roundtrip_structure;
                trace
            },
        )
    }

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // proptest is too heavy for miri!
    fn test_roundtrips() {
        fn check(trace: Trace<i64>) {
            trace.validate().unwrap();
            let flat = trace.flatten();
            let unflat = Trace::unflatten(flat).unwrap();
            assert_eq!(trace, unflat);
        }

        proptest!(|(trace in any_trace::<i64>(1..10))| { check(trace) })
    }

    #[mz_ore::test]
    fn fueled_merge_reqs() {
        let mut trace: Trace<u64> = Trace::default();
        let fueled_reqs = trace.push_batch(crate::internal::state::tests::hollow(
            0,
            10,
            &["n0011500/p3122e2a1-a0c7-429f-87aa-1019bf4f5f86"],
            1000,
        ));

        assert!(fueled_reqs.is_empty());
        assert_eq!(
            trace.fueled_merge_reqs_before_ms(u64::MAX, None).count(),
            0,
            "no merge reqs when not filtering by version"
        );
        assert_eq!(
            trace
                .fueled_merge_reqs_before_ms(
                    u64::MAX,
                    Some(WriterKey::for_version(&Version::new(0, 50, 0)))
                )
                .count(),
            0,
            "zero batches are older than a past version"
        );
        assert_eq!(
            trace
                .fueled_merge_reqs_before_ms(
                    u64::MAX,
                    Some(WriterKey::for_version(&Version::new(99, 99, 0)))
                )
                .count(),
            1,
            "one batch is older than a future version"
        );
    }

    #[mz_ore::test]
    fn remove_redundant_merge_reqs() {
        fn req(lower: u64, upper: u64) -> FueledMergeReq<u64> {
            FueledMergeReq {
                id: SpineId(usize::cast_from(lower), usize::cast_from(upper)),
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
            id: SpineId(0, 1),
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

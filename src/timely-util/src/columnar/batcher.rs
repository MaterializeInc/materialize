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

//! Types for consolidating, merging, and extracting columnar update collections.

use std::collections::VecDeque;
use std::marker::PhantomData;

use crate::columnation::ColumnationStack;
use columnar::Container as _;
use columnar::Push as _;
use columnar::{Clear, Columnar, Index, Len};
use columnation::Columnation;
use differential_dataflow::difference::Semigroup;
use differential_dataflow::trace::implementations::merge_batcher::Merger;
use timely::Accountable;
use timely::Container;
use timely::PartialOrder;
use timely::container::{ContainerBuilder, PushInto, SizableContainer};
use timely::progress::frontier::{Antichain, AntichainRef};

use crate::columnar::Column;

/// A chunker to transform input data into sorted columns.
#[derive(Default)]
pub struct Chunker<C> {
    /// Buffer into which we'll consolidate.
    ///
    /// Also the buffer where we'll stage responses to `extract` and `finish`.
    /// When these calls return, the buffer is available for reuse.
    target: C,
    /// Consolidated buffers ready to go.
    ready: VecDeque<C>,
}

impl<C: Container + Clone + 'static> ContainerBuilder for Chunker<C> {
    type Container = C;

    fn extract(&mut self) -> Option<&mut Self::Container> {
        if let Some(ready) = self.ready.pop_front() {
            self.target = ready;
            Some(&mut self.target)
        } else {
            None
        }
    }

    fn finish(&mut self) -> Option<&mut Self::Container> {
        self.extract()
    }
}

impl<'a, D, T, R> PushInto<&'a mut Column<(D, T, R)>> for Chunker<ColumnationStack<(D, T, R)>>
where
    D: Columnar + Columnation,
    for<'b> columnar::Ref<'b, D>: Ord + Copy,
    T: Columnar + Columnation,
    for<'b> columnar::Ref<'b, T>: Ord + Copy,
    R: Columnar + Columnation + Semigroup + for<'b> Semigroup<columnar::Ref<'b, R>>,
    for<'b> columnar::Ref<'b, R>: Ord,
{
    fn push_into(&mut self, container: &'a mut Column<(D, T, R)>) {
        // Sort input data
        // TODO: consider `Vec<usize>` that we retain, containing indexes.
        let borrowed = container.borrow();
        let mut permutation = Vec::with_capacity(borrowed.len());
        Extend::extend(&mut permutation, borrowed.into_index_iter());
        permutation.sort();

        self.target.clear();
        // Iterate over the data, accumulating diffs for like keys.
        let mut iter = permutation.drain(..);
        if let Some((data, time, diff)) = iter.next() {
            let mut owned_data = D::into_owned(data);
            let mut owned_time = T::into_owned(time);

            let mut prev_data = data;
            let mut prev_time = time;
            let mut prev_diff = <R as Columnar>::into_owned(diff);

            for (data, time, diff) in iter {
                if (&prev_data, &prev_time) == (&data, &time) {
                    prev_diff.plus_equals(&diff);
                } else {
                    if !prev_diff.is_zero() {
                        D::copy_from(&mut owned_data, prev_data);
                        T::copy_from(&mut owned_time, prev_time);
                        let tuple = (owned_data, owned_time, prev_diff);
                        self.target.push_into(&tuple);
                        (owned_data, owned_time, prev_diff) = tuple;
                    }
                    prev_data = data;
                    prev_time = time;
                    R::copy_from(&mut prev_diff, diff);
                }
            }

            if !prev_diff.is_zero() {
                D::copy_from(&mut owned_data, prev_data);
                T::copy_from(&mut owned_time, prev_time);
                let tuple = (owned_data, owned_time, prev_diff);
                self.target.push_into(&tuple);
            }
        }

        if !self.target.is_empty() {
            self.ready.push_back(std::mem::take(&mut self.target));
        }
    }
}

/// A chunker that consolidates `Column<(D, T, R)>` updates into sorted `Column`
/// chunks, without round-tripping through columnation.
///
/// Drop-in counterpart to [`Chunker`] for the merge-batcher path: same control
/// flow (sort borrowed refs, fold equal `(data, time)` runs, drop zero diffs),
/// but the consolidated output stays in [`Column`].
pub struct ColumnChunker<U: Columnar> {
    /// Container we consolidate into and present to extract/finish callers.
    /// Always `Column::Typed` between calls so we can push into it.
    target: Column<U>,
    /// Sorted, consolidated chunks pending extraction.
    ready: VecDeque<Column<U>>,
}

// Manual impl rather than `#[derive(Default)]`: the derive would synthesize
// `impl<U: Columnar + Default>`, but `Column<U>: Default` only requires
// `U: Columnar`, and adding a spurious `U: Default` bound would propagate
// through every `ContainerBuilder for ColumnChunker<U>` impl.
impl<U: Columnar> Default for ColumnChunker<U> {
    fn default() -> Self {
        Self {
            target: Column::default(),
            ready: VecDeque::new(),
        }
    }
}

impl<U: Columnar> ContainerBuilder for ColumnChunker<U>
where
    U::Container: Clone + 'static,
{
    type Container = Column<U>;

    fn extract(&mut self) -> Option<&mut Self::Container> {
        if let Some(ready) = self.ready.pop_front() {
            self.target = ready;
            Some(&mut self.target)
        } else {
            None
        }
    }

    fn finish(&mut self) -> Option<&mut Self::Container> {
        self.extract()
    }
}

impl<'a, D, T, R> PushInto<&'a mut Column<(D, T, R)>> for ColumnChunker<(D, T, R)>
where
    D: Columnar,
    for<'b> columnar::Ref<'b, D>: Copy + Ord,
    T: Columnar,
    for<'b> columnar::Ref<'b, T>: Copy + Ord,
    R: Columnar + Default + Semigroup + for<'b> Semigroup<columnar::Ref<'b, R>>,
    for<'b> columnar::Ref<'b, R>: Ord,
    for<'b> <D as Columnar>::Container: columnar::Push<columnar::Ref<'b, D>>,
    for<'b> <T as Columnar>::Container: columnar::Push<columnar::Ref<'b, T>>,
    for<'b> <R as Columnar>::Container: columnar::Push<&'b R>,
{
    fn push_into(&mut self, container: &'a mut Column<(D, T, R)>) {
        // Reset target to an empty owned container. If it's already `Typed`
        // (steady state, possibly recycling a chunk just handed back via
        // `extract`), clear in place to reuse buffer allocations. Otherwise
        // start fresh — the bytes/align variants don't support push.
        match &mut self.target {
            Column::Typed(c) => c.clear(),
            Column::Bytes(_) | Column::Align(_) => {
                self.target = Column::Typed(Default::default());
            }
        }

        // Sort input by columnar ref order.
        let borrowed = container.borrow();
        let mut permutation = Vec::with_capacity(borrowed.len());
        Extend::extend(&mut permutation, borrowed.into_index_iter());
        permutation.sort();

        // Sweep sorted refs, accumulating diffs over equal `(data, time)`
        // pairs and pushing non-zero results to the target's leaves. Refs
        // from the input borrow are valid through the sweep, so D and T
        // are pushed directly via each leaf's `Push<Ref<_>>` impl. Only R
        // needs an owned scratch since it carries the consolidated sum.
        {
            let Column::Typed(target_c) = &mut self.target else {
                unreachable!("target reset to Typed above");
            };
            let (target_d, target_t, target_r) = target_c;

            let mut iter = permutation.drain(..);
            if let Some((data, time, diff)) = iter.next() {
                let mut prev_data = data;
                let mut prev_time = time;
                let mut prev_diff = <R as Columnar>::into_owned(diff);

                for (data, time, diff) in iter {
                    if (&prev_data, &prev_time) == (&data, &time) {
                        prev_diff.plus_equals(&diff);
                    } else {
                        if !prev_diff.is_zero() {
                            target_d.push(prev_data);
                            target_t.push(prev_time);
                            target_r.push(&prev_diff);
                        }
                        prev_data = data;
                        prev_time = time;
                        R::copy_from(&mut prev_diff, diff);
                    }
                }

                if !prev_diff.is_zero() {
                    target_d.push(prev_data);
                    target_t.push(prev_time);
                    target_r.push(&prev_diff);
                }
            }
        }

        if self.target.borrow().len() > 0 {
            let chunk = std::mem::replace(&mut self.target, Column::Typed(Default::default()));
            self.ready.push_back(chunk);
        }
    }
}

/// Advance `*lower` past every position in `[*lower, upper)` where `cmp`
/// returns true.
///
/// On return, `*lower` is the first index `>= initial *lower` where `cmp`
/// returns false, or `upper` if `cmp` holds through the end.
///
/// Takes the predicate as `FnMut(usize) -> bool` rather than a value-bearing
/// closure so callers can index whichever subset of the input columns they
/// actually need to compare — for the merger's `(d, t)`-keyed sort, this lets
/// each probe touch only the D and T leaf views, skipping the diff column.
///
/// Compared to a linear scan, this is `O(log K)` for a run of length `K`
/// satisfying `cmp` — useful when one side of a sorted merge has long runs
/// dominated by the other side.
fn gallop(upper: usize, lower: &mut usize, mut cmp: impl FnMut(usize) -> bool) {
    // If `cmp` is already false at `*lower`, the run is empty — nothing to do.
    if *lower < upper && cmp(*lower) {
        // Phase 1 (overshoot): advance by exponentially growing steps as long
        // as `cmp` holds. After this loop, `*lower` is the last position we
        // confirmed satisfies `cmp`, and `*lower + step` either falls off the
        // end or fails `cmp`. The boundary is somewhere in `(*lower, *lower +
        // step]`.
        let mut step = 1;
        while *lower + step < upper && cmp(*lower + step) {
            *lower += step;
            step <<= 1;
        }

        // Phase 2 (binary descent): halve `step` and probe `*lower + step`,
        // accepting the advance only when `cmp` still holds. This narrows the
        // search range by half each iteration, settling on the largest index
        // still satisfying `cmp`.
        step >>= 1;
        while step > 0 {
            if *lower + step < upper && cmp(*lower + step) {
                *lower += step;
            }
            step >>= 1;
        }

        // `*lower` now points at the last index where `cmp` holds; the caller
        // wants the first index where it doesn't, so step past it.
        *lower += 1;
    }
}

/// Counterpart to `ColInternalMerger` (which merges `ColumnationStack` chunks).
/// Drives the merge batcher with [`Column`]-shaped chunks, no columnation
/// detour, by way of the inherent `merge_from` / `extract` methods on
/// `Column<(D, T, R)>` below.
pub struct ColumnMerger<D, T, R> {
    _marker: PhantomData<(D, T, R)>,
}

impl<D, T, R> Default for ColumnMerger<D, T, R> {
    fn default() -> Self {
        Self {
            _marker: PhantomData,
        }
    }
}

/// Per-chunk merge and extract for [`Column`]-shaped sorted chunks.
///
/// These are the building blocks that [`Merger for ColumnMerger`] orchestrates
/// over chains of chunks. They're inherent methods rather than a trait impl
/// so the merger can call them without going through any wrapper indirection.
impl<D, T, R> Column<(D, T, R)>
where
    D: Columnar + Default,
    for<'a> columnar::Ref<'a, D>: Copy + Ord,
    T: Columnar + Default + Clone + PartialOrder,
    for<'a> columnar::Ref<'a, T>: Copy + Ord,
    R: Columnar + Default + Semigroup + for<'a> Semigroup<columnar::Ref<'a, R>>,
    for<'a> <(D, T, R) as Columnar>::Container: columnar::Push<&'a (D, T, R)>,
    for<'a> <D as Columnar>::Container: columnar::Push<columnar::Ref<'a, D>>,
    for<'a> <D as Columnar>::Container: columnar::Push<&'a D>,
    for<'a> <T as Columnar>::Container: columnar::Push<columnar::Ref<'a, T>>,
    for<'a> <T as Columnar>::Container: columnar::Push<&'a T>,
    for<'a> <R as Columnar>::Container: columnar::Push<columnar::Ref<'a, R>>,
    for<'a> <R as Columnar>::Container: columnar::Push<&'a R>,
{
    /// Merge items from sorted inputs into `self`, advancing positions.
    ///
    /// Mirrors the dispatch shape used by the merge-batcher framework:
    /// - **0**: no-op
    /// - **1**: bulk copy (or swap, if `self` is empty and `*pos == 0`)
    /// - **2**: merge two sorted streams, with diff consolidation on equal
    ///   `(data, time)` keys and gallop bulk-copy of long single-side runs.
    ///
    /// Returns `true` if the merge stopped because the amortized ship-threshold
    /// check inside the inner loop fired (the caller should ship `self` before
    /// the next call). Returns `false` if the merge stopped because at least
    /// one input was exhausted at its position (the caller should refill that
    /// side; `self` may still be at capacity from accumulation across short
    /// calls and the caller should also check `at_capacity` in that case).
    ///
    /// The 0- and 1-input dispatches always return `false`: 0 does no work,
    /// 1 is a bulk copy or swap that runs to completion.
    #[must_use]
    pub fn merge_from(&mut self, others: &mut [Self], positions: &mut [usize]) -> bool {
        match others.len() {
            0 => false,
            1 => {
                let other = &mut others[0];
                let pos = &mut positions[0];
                // If `self` is empty and `*pos == 0`, we can bulk swap in the other chunk.
                if self.is_empty() && *pos == 0 {
                    std::mem::swap(self, other);
                    return false;
                }
                // Otherwise, bulk copy the remaining data from `other[*pos..]` into `self`.
                let Column::Typed(self_c) = self else {
                    unreachable!("merger chunks are always Column::Typed");
                };
                let src_c = other.borrow();
                self_c.extend_from_self(src_c, *pos..other.borrow().len());
                *pos = other.borrow().len();
                false
            }
            2 => {
                let (left, right) = others.split_at(1);
                let (left_pos, right_pos) = positions.split_at_mut(1);
                let left_borrow = left[0].borrow();
                let right_borrow = right[0].borrow();

                let Column::Typed(self_c) = self else {
                    unreachable!("merger chunks are always Column::Typed");
                };

                // Split the input borrows into per-leaf views.
                //
                // A columnar tuple `Borrow::Ref` is recursive: indexing the
                // tuple borrow walks every leaf (and reconstructs the nested
                // ref tuple) regardless of which leaves the caller actually
                // reads. Indexing each leaf view directly cuts probe-path
                // work to the columns we consult — for the merge step
                // that's the `(D, T)` key, with the diff column read only
                // when we push.
                let l_d = left_borrow.0;
                let l_t = left_borrow.1;
                let l_r = left_borrow.2;
                let r_d = right_borrow.0;
                let r_t = right_borrow.1;
                let r_r = right_borrow.2;
                let upper_l = l_d.len();
                let upper_r = r_d.len();

                // Mirror the split on the output container. Tuple
                // containers split into per-leaf containers, which lets us
                // address each leaf independently — both gallop bulk-copies
                // and single-record pushes resolve to a primitive operation
                // per leaf. The leaves stay length-synchronized as long as
                // every record path pushes exactly one element to each.
                let (sd, st, sr) = self_c;

                // Pre-size each output leaf for the worst-case merge
                // (no consolidation): `len(left) + len(right)` records.
                // `reserve_for` walks each input's `as_bytes`, which is
                // accurate for variable-length leaves (where reserving a
                // record count wouldn't size the byte buffer correctly).
                //
                // Gated by record count: above a few hundred thousand
                // records the input bound over-reserves any time
                // consolidation is heavy, and the framework's outer
                // ship-threshold check yields us before we'd use the
                // headroom. For inputs past that point, geometric grow
                // is bounded by 2× the actual output and avoids
                // committing pages we'd never touch.
                const RESERVE_RECORD_THRESHOLD: usize = 1_000_000;
                if upper_l + upper_r <= RESERVE_RECORD_THRESHOLD {
                    use columnar::Container as _;
                    let inputs = [left_borrow, right_borrow];
                    sd.reserve_for(inputs.iter().map(|b| b.0));
                    st.reserve_for(inputs.iter().map(|b| b.1));
                    sr.reserve_for(inputs.iter().map(|b| b.2));
                }

                let mut stash = R::default();

                // Mid-merge ship-threshold check, matching the heuristic
                // used by `Column::at_capacity` and `ColumnBuilder`. The
                // tuple `(sd.borrow(), st.borrow(), sr.borrow())` chains
                // its leaves' `as_bytes` iterators, so passing it to
                // `at_serialized_capacity` reuses the canonical
                // `indexed::length_in_words` formula without needing the
                // parent borrow we destructured.
                //
                // The check walks every leaf slice once per call, which
                // is non-trivial on variable-length leaves; the caller
                // runs it every `THRESHOLD_PERIOD_MASK + 1` iterations
                // rather than per-iter. The ship threshold is ~65 K
                // records, so overshooting by ~1 K records before the
                // check fires has no practical impact — the framework's
                // outer `at_capacity` check sees the oversize chunk and
                // ships it regardless.
                let at_ship_threshold =
                    |sd: &D::Container, st: &T::Container, sr: &R::Container| {
                        use columnar::Borrow as _;
                        crate::columnar::at_serialized_capacity(&(
                            sd.borrow(),
                            st.borrow(),
                            sr.borrow(),
                        ))
                    };
                const THRESHOLD_PERIOD_MASK: u32 = 1023;
                let mut iter: u32 = 0;
                let mut yielded = false;

                while left_pos[0] < upper_l && right_pos[0] < upper_r {
                    let d1 = l_d.get(left_pos[0]);
                    let t1 = l_t.get(left_pos[0]);
                    let d2 = r_d.get(right_pos[0]);
                    let t2 = r_t.get(right_pos[0]);
                    match (d1, t1).cmp(&(d2, t2)) {
                        std::cmp::Ordering::Less => {
                            // Common case (interleaved data): single-record
                            // advance. Skip the gallop call entirely — its
                            // setup plus the first cmp probe is more
                            // expensive than just pushing this record and
                            // re-entering the outer loop. Galloping is only
                            // worthwhile when there's an actual run, which
                            // we detect with the peek check below.
                            sd.push(d1);
                            st.push(t1);
                            sr.push(l_r.get(left_pos[0]));
                            left_pos[0] += 1;
                            // Long-run case: peek at the next record; if
                            // it's still strictly less than `(d2, t2)`,
                            // we have a run worth galloping (and bulk-
                            // copying).
                            if left_pos[0] < upper_l
                                && (l_d.get(left_pos[0]), l_t.get(left_pos[0])) < (d2, t2)
                            {
                                let start = left_pos[0];
                                gallop(upper_l, &mut left_pos[0], |i| {
                                    (l_d.get(i), l_t.get(i)) < (d2, t2)
                                });
                                // Per-leaf bulk copy of the run. Each
                                // call resolves to an `extend_from_slice`
                                // on its leaf (recursively for nested
                                // leaves), which the compiler can
                                // autovectorize.
                                sd.extend_from_self(l_d, start..left_pos[0]);
                                st.extend_from_self(l_t, start..left_pos[0]);
                                sr.extend_from_self(l_r, start..left_pos[0]);
                            }
                        }
                        std::cmp::Ordering::Greater => {
                            // Symmetric on the right side.
                            sd.push(d2);
                            st.push(t2);
                            sr.push(r_r.get(right_pos[0]));
                            right_pos[0] += 1;
                            if right_pos[0] < upper_r
                                && (r_d.get(right_pos[0]), r_t.get(right_pos[0])) < (d1, t1)
                            {
                                let start = right_pos[0];
                                gallop(upper_r, &mut right_pos[0], |i| {
                                    (r_d.get(i), r_t.get(i)) < (d1, t1)
                                });
                                sd.extend_from_self(r_d, start..right_pos[0]);
                                st.extend_from_self(r_t, start..right_pos[0]);
                                sr.extend_from_self(r_r, start..right_pos[0]);
                            }
                        }
                        std::cmp::Ordering::Equal => {
                            let r1 = l_r.get(left_pos[0]);
                            let r2 = r_r.get(right_pos[0]);
                            R::copy_from(&mut stash, r1);
                            stash.plus_equals(&r2);
                            if !stash.is_zero() {
                                sd.push(d1);
                                st.push(t1);
                                sr.push(&stash);
                            }
                            left_pos[0] += 1;
                            right_pos[0] += 1;
                        }
                    }

                    // Amortized ship-threshold check; see comment above
                    // `at_ship_threshold` for rationale.
                    iter = iter.wrapping_add(1);
                    if iter & THRESHOLD_PERIOD_MASK == 0 && at_ship_threshold(sd, st, sr) {
                        yielded = true;
                        break;
                    }
                }
                yielded
            }
            // `Merger::merge` only ever calls `merge_from` with 0/1/2-input
            // slices (k-way merge isn't part of the merge-batcher contract).
            // Defensive guard: if someone bumps that, this will panic
            // immediately rather than silently produce wrong output.
            n => unreachable!("merge_from called with {n} inputs; expected 0, 1, or 2"),
        }
    }

    /// Partition records starting at `*position` into `keep` (times beyond
    /// `upper`, retained for the next round) and `ship` (times not beyond
    /// `upper`, sealed into the output batch). Updates `frontier` with the
    /// times of kept records.
    ///
    /// The caller invokes `extract` repeatedly until `*position >= self.len()`,
    /// swapping out a full output buffer between calls. This shape exists
    /// because the framework only checks `at_capacity()` between calls, so
    /// without an inner-loop yield a single call could quietly produce
    /// oversized output chunks.
    pub fn extract(
        &mut self,
        position: &mut usize,
        upper: AntichainRef<T>,
        frontier: &mut Antichain<T>,
        keep: &mut Self,
        ship: &mut Self,
    ) {
        let Column::Typed(keep_c) = keep else {
            unreachable!("merger chunks are always Column::Typed");
        };
        let Column::Typed(ship_c) = ship else {
            unreachable!("merger chunks are always Column::Typed");
        };

        let self_view = self.borrow();
        let len = self_view.len();

        let mut owned_t = T::default();
        // Yield to the framework when either output buffer reaches the
        // ship threshold, so it can ship a full chunk and hand back a
        // fresh one. Required by the merger's extract contract: the
        // framework only checks `at_capacity` between calls, so without
        // an inner-loop yield a single call can fill an output well past
        // threshold.
        use columnar::Borrow as _;
        while *position < len
            && !crate::columnar::at_serialized_capacity(&keep_c.borrow())
            && !crate::columnar::at_serialized_capacity(&ship_c.borrow())
        {
            let (_, time, _) = self_view.get(*position);
            T::copy_from(&mut owned_t, time);
            if upper.less_equal(&owned_t) {
                // `insert_with` only clones when the time isn't already
                // present in the antichain.
                frontier.insert_with(&owned_t, |t| t.clone());
                keep_c.extend_from_self(self_view, *position..*position + 1);
            } else {
                ship_c.extend_from_self(self_view, *position..*position + 1);
            }
            *position += 1;
        }
    }
}

/// `Merger` impl driving [`MergeBatcher`] over [`Column`]-shaped chunks.
///
/// `merge` walks two sorted chains of chunks in lockstep, calling
/// `Column::merge_from` to consume up to one ship-threshold's worth of input
/// per pass and shipping `result` to `output` whenever it crosses
/// `at_capacity`. Exhausted input chunks are reset and pushed to `stash` for
/// reuse. The drain phase appends remaining full chunks to `output`
/// directly, with no per-element copy.
///
/// `extract` walks each chunk via `Column::extract`, partitioning records
/// into `kept` (times beyond `upper`) and `ship` (sealed into the output
/// batch); both grow chunk-by-chunk under the same `at_capacity` ship
/// signal.
///
/// [`MergeBatcher`]: differential_dataflow::trace::implementations::merge_batcher::MergeBatcher
impl<D, T, R> Merger for ColumnMerger<D, T, R>
where
    D: Columnar + Default + 'static,
    for<'a> columnar::Ref<'a, D>: Copy + Ord,
    T: Columnar + Default + Clone + Ord + PartialOrder + 'static,
    for<'a> columnar::Ref<'a, T>: Copy + Ord,
    R: Columnar + Default + Semigroup + for<'a> Semigroup<columnar::Ref<'a, R>> + 'static,
    for<'a> <(D, T, R) as Columnar>::Container: columnar::Push<&'a (D, T, R)>,
    for<'a> <D as Columnar>::Container: columnar::Push<columnar::Ref<'a, D>>,
    for<'a> <D as Columnar>::Container: columnar::Push<&'a D>,
    for<'a> <T as Columnar>::Container: columnar::Push<columnar::Ref<'a, T>>,
    for<'a> <T as Columnar>::Container: columnar::Push<&'a T>,
    for<'a> <R as Columnar>::Container: columnar::Push<columnar::Ref<'a, R>>,
    for<'a> <R as Columnar>::Container: columnar::Push<&'a R>,
{
    type Time = T;
    type Chunk = Column<(D, T, R)>;

    fn merge(
        &mut self,
        list1: Vec<Self::Chunk>,
        list2: Vec<Self::Chunk>,
        output: &mut Vec<Self::Chunk>,
        stash: &mut Vec<Self::Chunk>,
    ) {
        let mut list1 = list1.into_iter();
        let mut list2 = list2.into_iter();

        let mut heads = [
            list1.next().unwrap_or_default(),
            list2.next().unwrap_or_default(),
        ];
        let mut positions = [0usize, 0usize];

        let mut result = empty_chunk(stash);

        // Main merge loop: both sides have data.
        loop {
            let upper_l = heads[0].borrow().len();
            let upper_r = heads[1].borrow().len();
            if positions[0] >= upper_l || positions[1] >= upper_r {
                break;
            }

            // Whole-chunk passthrough fast path. When one head's tail (from
            // its current position) is sortable-before the other head's
            // current record, the entire tail can be appended to `output`
            // without per-record compares or per-leaf byte copies.
            //
            // Two probes (one record from each side) settle this — when it
            // fires, it skips an entire `merge_from` invocation, including
            // its gallop bulk-copies, and replaces the byte-level extend
            // with a `mem::replace` of the head into `output`.
            //
            // Restricted to `positions[i] == 0` so we can hand the head off
            // wholesale; partial-tail passthrough would require a 1-input
            // `merge_from` to materialize the tail into a new chunk, which
            // is what gallop already handles inside the merge loop.
            let lhs_passthrough = positions[0] == 0 && upper_l > 0 && {
                let lhs = heads[0].borrow();
                let rhs = heads[1].borrow();
                let last_l = (lhs.0.get(upper_l - 1), lhs.1.get(upper_l - 1));
                let cur_r = (rhs.0.get(positions[1]), rhs.1.get(positions[1]));
                last_l < cur_r
            };
            if lhs_passthrough {
                if !result.is_empty() {
                    output.push(std::mem::take(&mut result));
                    result = empty_chunk(stash);
                }
                let head = std::mem::replace(&mut heads[0], list1.next().unwrap_or_default());
                output.push(head);
                positions[0] = 0;
                continue;
            }

            let rhs_passthrough = positions[1] == 0 && upper_r > 0 && {
                let lhs = heads[0].borrow();
                let rhs = heads[1].borrow();
                let last_r = (rhs.0.get(upper_r - 1), rhs.1.get(upper_r - 1));
                let cur_l = (lhs.0.get(positions[0]), lhs.1.get(positions[0]));
                last_r < cur_l
            };
            if rhs_passthrough {
                if !result.is_empty() {
                    output.push(std::mem::take(&mut result));
                    result = empty_chunk(stash);
                }
                let head = std::mem::replace(&mut heads[1], list2.next().unwrap_or_default());
                output.push(head);
                positions[1] = 0;
                continue;
            }

            // Per-record merge. `merge_from` returns `true` when its inner
            // amortized ship-threshold check fires — short-circuit the
            // outer `at_capacity` walk in that case.
            let yielded = result.merge_from(&mut heads, &mut positions);

            if positions[0] >= heads[0].borrow().len() {
                let old = std::mem::replace(&mut heads[0], list1.next().unwrap_or_default());
                recycle_chunk(old, stash);
                positions[0] = 0;
            }
            if positions[1] >= heads[1].borrow().len() {
                let old = std::mem::replace(&mut heads[1], list2.next().unwrap_or_default());
                recycle_chunk(old, stash);
                positions[1] = 0;
            }
            if yielded || result.at_capacity() {
                output.push(std::mem::take(&mut result));
                result = empty_chunk(stash);
            }
        }

        // Drain remaining from each side: copy partial head, then append
        // full chunks directly to output (no per-element copy).
        drain_side(
            &mut heads[0],
            &mut positions[0],
            &mut list1,
            &mut result,
            output,
            stash,
        );
        drain_side(
            &mut heads[1],
            &mut positions[1],
            &mut list2,
            &mut result,
            output,
            stash,
        );
        if !result.is_empty() {
            output.push(result);
        }
    }

    fn extract(
        &mut self,
        merged: Vec<Self::Chunk>,
        upper: AntichainRef<Self::Time>,
        frontier: &mut Antichain<Self::Time>,
        ship: &mut Vec<Self::Chunk>,
        kept: &mut Vec<Self::Chunk>,
        stash: &mut Vec<Self::Chunk>,
    ) {
        let mut keep = empty_chunk(stash);
        let mut ready = empty_chunk(stash);

        for mut buffer in merged {
            let mut position = 0;
            let len = buffer.borrow().len();
            while position < len {
                buffer.extract(&mut position, upper, frontier, &mut keep, &mut ready);
                if keep.at_capacity() {
                    kept.push(std::mem::take(&mut keep));
                    keep = empty_chunk(stash);
                }
                if ready.at_capacity() {
                    ship.push(std::mem::take(&mut ready));
                    ready = empty_chunk(stash);
                }
            }
            recycle_chunk(buffer, stash);
        }
        if !keep.is_empty() {
            kept.push(keep);
        }
        if !ready.is_empty() {
            ship.push(ready);
        }
    }

    fn account(chunk: &Self::Chunk) -> (usize, usize, usize, usize) {
        use timely::dataflow::channels::ContainerBytes;
        let records = usize::try_from(chunk.record_count()).expect("record_count is non-negative");
        // Serialized footprint stands in for both `size` and `capacity`: the
        // chunk owns one logical allocation worth of leaf storage, and we
        // ship/recycle the whole thing rather than tracking per-leaf
        // capacities. Treating `size == capacity` matches how the framework
        // accounts already-shipped chunks (no slack to absorb).
        let bytes = chunk.length_in_bytes();
        (records, bytes, bytes, 1)
    }
}

/// Pop a chunk from `stash` or allocate a fresh one. Stashed chunks are
/// already cleared via `recycle_chunk`, so they're ready for push.
#[inline]
fn empty_chunk<C: Columnar>(stash: &mut Vec<Column<C>>) -> Column<C> {
    stash.pop().unwrap_or_default()
}

/// Reset `chunk` to an empty `Typed` and push it to `stash` for reuse.
///
/// Chunks recycled here come from the merger and chunker, both of which
/// produce `Typed`; only the typed allocations are worth caching for reuse.
/// `Bytes` / `Align` chunks have no typed-side allocation to preserve, so we
/// simply drop them — `empty_chunk` will produce a fresh default just as
/// cheaply, and pushing them onto `stash` would only displace useful
/// recycled allocations.
#[inline]
fn recycle_chunk<C: Columnar>(mut chunk: Column<C>, stash: &mut Vec<Column<C>>) {
    if let Column::Typed(c) = &mut chunk {
        c.clear();
        stash.push(chunk);
    }
}

/// Drain remaining items from one side into `result` / `output`.
///
/// Copies the partially-consumed head into `result` via `merge_from`'s 1-input
/// path, then appends remaining full chunks directly to `output` without
/// per-element copy.
fn drain_side<D, T, R>(
    head: &mut Column<(D, T, R)>,
    pos: &mut usize,
    list: &mut std::vec::IntoIter<Column<(D, T, R)>>,
    result: &mut Column<(D, T, R)>,
    output: &mut Vec<Column<(D, T, R)>>,
    stash: &mut Vec<Column<(D, T, R)>>,
) where
    D: Columnar + Default,
    for<'a> columnar::Ref<'a, D>: Copy + Ord,
    T: Columnar + Default + Clone + PartialOrder,
    for<'a> columnar::Ref<'a, T>: Copy + Ord,
    R: Columnar + Default + Semigroup + for<'a> Semigroup<columnar::Ref<'a, R>>,
    for<'a> <(D, T, R) as Columnar>::Container: columnar::Push<&'a (D, T, R)>,
    for<'a> <D as Columnar>::Container: columnar::Push<columnar::Ref<'a, D>>,
    for<'a> <D as Columnar>::Container: columnar::Push<&'a D>,
    for<'a> <T as Columnar>::Container: columnar::Push<columnar::Ref<'a, T>>,
    for<'a> <T as Columnar>::Container: columnar::Push<&'a T>,
    for<'a> <R as Columnar>::Container: columnar::Push<columnar::Ref<'a, R>>,
    for<'a> <R as Columnar>::Container: columnar::Push<&'a R>,
{
    if *pos < head.borrow().len() {
        // 1-input dispatch — bulk copy that runs to completion; the yield
        // signal is unused.
        let _ = result.merge_from(std::slice::from_mut(head), std::slice::from_mut(pos));
    }
    if !result.is_empty() {
        output.push(std::mem::take(result));
        *result = empty_chunk(stash);
    }
    Extend::extend(output, list);
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Drive a single `push_into` call with `inputs` and collect the
    /// consolidated output (if any) as owned tuples.
    fn run_chunker<D, T, R>(inputs: &[(D, T, R)]) -> Vec<(D, T, R)>
    where
        D: Columnar + Clone,
        for<'a> columnar::Ref<'a, D>: Copy + Ord,
        T: Columnar + Clone,
        for<'a> columnar::Ref<'a, T>: Copy + Ord,
        R: Columnar + Clone + Default + Semigroup + for<'a> Semigroup<columnar::Ref<'a, R>>,
        for<'a> columnar::Ref<'a, R>: Ord,
        <(D, T, R) as Columnar>::Container: Clone,
        for<'a> <(D, T, R) as Columnar>::Container: columnar::Push<&'a (D, T, R)>,
        <(D, T, R) as Columnar>::Container: columnar::Push<(D, T, R)>,
        for<'a> <D as Columnar>::Container: columnar::Push<columnar::Ref<'a, D>>,
        for<'a> <T as Columnar>::Container: columnar::Push<columnar::Ref<'a, T>>,
        for<'a> <R as Columnar>::Container: columnar::Push<&'a R>,
    {
        let mut input: Column<(D, T, R)> = Default::default();
        for tuple in inputs.iter().cloned() {
            input.push_into(tuple);
        }

        let mut chunker: ColumnChunker<(D, T, R)> = Default::default();
        chunker.push_into(&mut input);

        let mut out = Vec::new();
        while let Some(chunk) = chunker.extract() {
            for (d, t, r) in chunk.borrow().into_index_iter() {
                out.push((D::into_owned(d), T::into_owned(t), R::into_owned(r)));
            }
        }
        out
    }

    #[mz_ore::test]
    fn empty_input_yields_no_chunk() {
        let mut chunker: ColumnChunker<(u64, u64, i64)> = Default::default();
        let mut input: Column<(u64, u64, i64)> = Default::default();
        chunker.push_into(&mut input);
        assert!(chunker.extract().is_none());
        assert!(chunker.finish().is_none());
    }

    #[mz_ore::test]
    fn unsorted_input_is_sorted() {
        let out = run_chunker(&[(3u64, 0u64, 1i64), (1u64, 0u64, 1i64), (2u64, 0u64, 1i64)]);
        assert_eq!(out, vec![(1, 0, 1), (2, 0, 1), (3, 0, 1)]);
    }

    #[mz_ore::test]
    fn duplicate_keys_consolidate() {
        let out = run_chunker(&[(1u64, 0u64, 1i64), (1u64, 0u64, 2i64), (1u64, 0u64, -1i64)]);
        assert_eq!(out, vec![(1, 0, 2)]);
    }

    #[mz_ore::test]
    fn diffs_summing_to_zero_are_dropped() {
        let out = run_chunker(&[(1u64, 0u64, 1i64), (1u64, 0u64, -1i64)]);
        assert!(out.is_empty());
    }

    #[mz_ore::test]
    fn mixed_consolidation() {
        // (1, 0): 1 + 2 + (-3) = 0  -> dropped
        // (2, 0): 1            = 1  -> kept
        // (1, 1): 5            = 5  -> kept (different time from the (1, 0) group)
        let out = run_chunker(&[
            (1u64, 0u64, 1i64),
            (2u64, 0u64, 1i64),
            (1u64, 0u64, 2i64),
            (1u64, 1u64, 5i64),
            (1u64, 0u64, -3i64),
        ]);
        assert_eq!(out, vec![(1, 1, 5), (2, 0, 1)]);
    }

    #[mz_ore::test]
    fn key_val_tuple_data() {
        // Exercise the actual val-batcher shape: `D = (K, V)`.
        let out = run_chunker(&[
            ((1u64, 10u64), 0u64, 1i64),
            ((1u64, 10u64), 0u64, 1i64),
            ((1u64, 11u64), 0u64, 1i64),
            ((2u64, 10u64), 0u64, 1i64),
        ]);
        assert_eq!(
            out,
            vec![((1, 10), 0, 2), ((1, 11), 0, 1), ((2, 10), 0, 1),]
        );
    }

    #[mz_ore::test]
    fn buffer_reuse_across_calls() {
        // Two sequential push_into calls; second runs after extract returned
        // the first chunk, exercising the in-place clear path.
        let mut input1: Column<(u64, u64, i64)> = Default::default();
        input1.push_into((1u64, 0u64, 1i64));
        input1.push_into((2u64, 0u64, 1i64));

        let mut input2: Column<(u64, u64, i64)> = Default::default();
        input2.push_into((3u64, 0u64, 1i64));
        input2.push_into((1u64, 0u64, 1i64));

        let mut chunker: ColumnChunker<(u64, u64, i64)> = Default::default();
        chunker.push_into(&mut input1);

        // Hand back the first chunk via extract, simulating the merge batcher
        // taking ownership of the &mut and then returning.
        {
            let _ = chunker.extract().expect("first chunk");
        }

        chunker.push_into(&mut input2);

        let chunk = chunker.extract().expect("second chunk");
        let collected: Vec<_> = chunk
            .borrow()
            .into_index_iter()
            .map(|(d, t, r)| (u64::into_owned(d), u64::into_owned(t), i64::into_owned(r)))
            .collect();
        assert_eq!(collected, vec![(1, 0, 1), (3, 0, 1)]);
    }

    /// Build a `Column<((u64, u64), u64, i64)>` from a slice of tuples.
    fn col(rows: &[((u64, u64), u64, i64)]) -> Column<((u64, u64), u64, i64)> {
        let mut c: Column<((u64, u64), u64, i64)> = Default::default();
        for &t in rows {
            c.push_into(t);
        }
        c
    }

    fn collect_chunks(chunks: &[Column<((u64, u64), u64, i64)>]) -> Vec<((u64, u64), u64, i64)> {
        chunks
            .iter()
            .flat_map(|c| {
                c.borrow().into_index_iter().map(|((k, v), t, r)| {
                    (
                        (u64::into_owned(k), u64::into_owned(v)),
                        u64::into_owned(t),
                        i64::into_owned(r),
                    )
                })
            })
            .collect()
    }

    /// Disjoint-range chains exercise the whole-chunk passthrough fast path:
    /// every chunk in chain1 is sortable-before every chunk in chain2, so
    /// each outer-loop iteration should hand a chunk straight to `output`
    /// without recursing through the per-record merge.
    #[mz_ore::test]
    fn merger_disjoint_chains_passthrough() {
        let chain1 = vec![
            col(&[((0, 0), 0, 1), ((1, 0), 0, 1)]),
            col(&[((2, 0), 0, 1), ((3, 0), 0, 1)]),
        ];
        let chain2 = vec![
            col(&[((10, 0), 0, 1), ((11, 0), 0, 1)]),
            col(&[((12, 0), 0, 1), ((13, 0), 0, 1)]),
        ];

        let mut merger: ColumnMerger<(u64, u64), u64, i64> = Default::default();
        let mut output = Vec::new();
        let mut stash = Vec::new();
        Merger::merge(&mut merger, chain1, chain2, &mut output, &mut stash);

        let collected = collect_chunks(&output);
        let expected: Vec<_> = (0..4u64)
            .map(|d| ((d, 0u64), 0u64, 1i64))
            .chain((10..14u64).map(|d| ((d, 0u64), 0u64, 1i64)))
            .collect();
        assert_eq!(collected, expected);
    }

    /// Interleaved chains never satisfy the passthrough condition; each
    /// outer iteration falls through to `merge_from`. Same correctness
    /// expectation, exercises the non-passthrough path under
    /// `Merger::merge`.
    #[mz_ore::test]
    fn merger_interleaved_chains() {
        // Even keys on one chain, odd on the other; chunks alternate so the
        // per-record path is the only viable route.
        let chain1 = vec![
            col(&[((0, 0), 0, 1), ((2, 0), 0, 1)]),
            col(&[((4, 0), 0, 1), ((6, 0), 0, 1)]),
        ];
        let chain2 = vec![
            col(&[((1, 0), 0, 1), ((3, 0), 0, 1)]),
            col(&[((5, 0), 0, 1), ((7, 0), 0, 1)]),
        ];

        let mut merger: ColumnMerger<(u64, u64), u64, i64> = Default::default();
        let mut output = Vec::new();
        let mut stash = Vec::new();
        Merger::merge(&mut merger, chain1, chain2, &mut output, &mut stash);

        let collected = collect_chunks(&output);
        let expected: Vec<_> = (0..8u64).map(|d| ((d, 0u64), 0u64, 1i64)).collect();
        assert_eq!(collected, expected);
    }

    /// Passthrough must consolidate adjacent equal keys at chunk
    /// boundaries — i.e., must NOT fire when `chain1`'s last record's
    /// `(d, t)` equals `chain2`'s first.
    #[mz_ore::test]
    fn merger_passthrough_respects_equal_boundary() {
        // chain1's last == chain2's first key: equal-key consolidation
        // must kick in (sum of diffs would be 2). If passthrough fired
        // erroneously, both records would land in different output chunks
        // unconsolidated.
        let chain1 = vec![col(&[((0, 0), 0, 1), ((5, 0), 0, 1)])];
        let chain2 = vec![col(&[((5, 0), 0, 1), ((10, 0), 0, 1)])];

        let mut merger: ColumnMerger<(u64, u64), u64, i64> = Default::default();
        let mut output = Vec::new();
        let mut stash = Vec::new();
        Merger::merge(&mut merger, chain1, chain2, &mut output, &mut stash);

        let collected = collect_chunks(&output);
        assert_eq!(
            collected,
            vec![((0, 0), 0, 1), ((5, 0), 0, 2), ((10, 0), 0, 1)]
        );
    }
}

#[cfg(test)]
mod proptests {
    //! Property tests for `Column::merge_from` and `Column::extract`.
    //!
    //! Strategy: generate sorted+consolidated inputs (the merger's input
    //! contract), drive `merge_from` / `extract` the same way the framework
    //! would, and compare against a brute-force reference impl.
    //!
    //! Test types are `D = (u64, u64)`, `T = u64`, `R = i64` drawn from small
    //! ranges so that equal-key collisions are common and the consolidation
    //! path actually runs.
    use super::*;
    use mz_ore::cast::CastFrom;
    use proptest::prelude::*;
    use timely::progress::frontier::Antichain;

    type Tuple = ((u64, u64), u64, i64);

    /// Reference consolidation: sort by `(data, time)`, sum diffs over equal
    /// pairs, drop zeros.
    fn consolidate(mut v: Vec<Tuple>) -> Vec<Tuple> {
        v.sort();
        let mut out: Vec<Tuple> = Vec::new();
        for (d, t, r) in v {
            if let Some(last) = out.last_mut() {
                if last.0 == d && last.1 == t {
                    last.2 += r;
                    continue;
                }
            }
            out.push((d, t, r));
        }
        out.retain(|x| x.2 != 0);
        out
    }

    /// Strategy for sorted+consolidated input lists. Ranges are small to
    /// encourage equal-key collisions.
    fn arb_consolidated() -> impl Strategy<Value = Vec<Tuple>> {
        prop::collection::vec(((0u64..5, 0u64..5), 0u64..3, -3i64..=3i64), 0..30)
            .prop_map(consolidate)
    }

    fn build_column(v: &[Tuple]) -> Column<Tuple> {
        let mut col: Column<Tuple> = Default::default();
        for tup in v {
            col.push_into(*tup);
        }
        col
    }

    fn collect_column(col: &Column<Tuple>) -> Vec<Tuple> {
        col.borrow()
            .into_index_iter()
            .map(|((k, v), t, r)| {
                (
                    (u64::into_owned(k), u64::into_owned(v)),
                    u64::into_owned(t),
                    i64::into_owned(r),
                )
            })
            .collect()
    }

    /// Drive a 2-way merge the same way `Merger::merge` would: a 2-input
    /// call until one side exhausts, then a 1-input drain for whichever
    /// side still has data.
    fn drive_merge(left: Column<Tuple>, right: Column<Tuple>) -> Column<Tuple> {
        let mut self_col: Column<Tuple> = Default::default();
        let mut others = [left, right];
        let mut positions = [0usize, 0];
        let _ = self_col.merge_from(&mut others, &mut positions);

        let [left_done, right_done] = others;
        let [left_pos, right_pos] = positions;

        if left_pos < left_done.borrow().len() {
            let mut tail = [left_done];
            let mut p = [left_pos];
            let _ = self_col.merge_from(&mut tail, &mut p);
        } else if right_pos < right_done.borrow().len() {
            let mut tail = [right_done];
            let mut p = [right_pos];
            let _ = self_col.merge_from(&mut tail, &mut p);
        }

        self_col
    }

    proptest! {
        /// `merge_from` with two sorted+consolidated inputs equals the
        /// reference consolidate(union).
        #[mz_ore::test]
        #[cfg_attr(miri, ignore)]
        fn merge_from_equals_consolidated_union(
            a in arb_consolidated(),
            b in arb_consolidated(),
        ) {
            let merged = drive_merge(build_column(&a), build_column(&b));

            let mut union = a.clone();
            Extend::extend(&mut union, b.iter().copied());
            let expected = consolidate(union);

            prop_assert_eq!(collect_column(&merged), expected);
        }

        /// `merge_from` 1-input bulk-copy from a non-zero position equals
        /// `other[*pos..]`.
        #[mz_ore::test]
        #[cfg_attr(miri, ignore)]
        fn merge_from_one_input_drains_tail(
            data in arb_consolidated(),
            pos_frac in 0u32..=100,
        ) {
            // Cap at len so we always have a valid position.
            let len = data.len();
            let start_pos = if len == 0 { 0 } else {
                (usize::cast_from(pos_frac) * len) / 101
            };

            // Self starts non-empty so we exercise the bulk-copy path, not the
            // empty-self swap shortcut.
            let mut self_col: Column<Tuple> = Default::default();
            let sentinel: Tuple = ((u64::MAX, u64::MAX), 0, 1);
            self_col.push_into(sentinel);

            let mut others = [build_column(&data)];
            let mut positions = [start_pos];
            let _ = self_col.merge_from(&mut others, &mut positions);

            let mut expected = vec![sentinel];
            Extend::extend(&mut expected, data[start_pos..].iter().copied());

            prop_assert_eq!(collect_column(&self_col), expected);
            prop_assert_eq!(positions[0], len);
        }

        /// `merge_from` 1-input swap shortcut: empty self + pos=0 should
        /// produce a column equal to the input.
        #[mz_ore::test]
        #[cfg_attr(miri, ignore)]
        fn merge_from_empty_self_swap(data in arb_consolidated()) {
            let mut self_col: Column<Tuple> = Default::default();
            let mut others = [build_column(&data)];
            let mut positions = [0usize];
            let _ = self_col.merge_from(&mut others, &mut positions);

            prop_assert_eq!(collect_column(&self_col), data);
        }

        /// `extract` partitions correctly:
        ///   - keep ∪ ship multiset-equals self
        ///   - upper.less_equal(t) for every kept time
        ///   - !upper.less_equal(t) for every shipped time
        ///   - frontier covers every kept time
        #[mz_ore::test]
        #[cfg_attr(miri, ignore)]
        fn extract_partitions_by_frontier(
            data in arb_consolidated(),
            upper_time in 0u64..=4,
        ) {
            let mut self_col = build_column(&data);
            let upper = Antichain::from_elem(upper_time);
            let mut frontier: Antichain<u64> = Antichain::new();
            let mut keep: Column<Tuple> = Default::default();
            let mut ship: Column<Tuple> = Default::default();
            let mut position = 0;

            self_col.extract(
                &mut position,
                upper.borrow(),
                &mut frontier,
                &mut keep,
                &mut ship,
            );

            // Single call drains the input (we removed the at_capacity yield).
            prop_assert_eq!(position, data.len());

            let kept = collect_column(&keep);
            let shipped = collect_column(&ship);

            // Partition predicate: kept times >= upper, shipped times < upper.
            for (_, t, _) in &kept {
                prop_assert!(
                    upper.borrow().less_equal(t),
                    "kept time {} should satisfy upper.less_equal", t,
                );
            }
            for (_, t, _) in &shipped {
                prop_assert!(
                    !upper.borrow().less_equal(t),
                    "shipped time {} should NOT satisfy upper.less_equal", t,
                );
            }

            // Union (multiset) equals input.
            let mut union = kept.clone();
            Extend::extend(&mut union, shipped.iter().copied());
            union.sort();
            let mut expected_sorted = data.clone();
            expected_sorted.sort();
            prop_assert_eq!(union, expected_sorted);

            // Frontier dominates every kept time.
            for (_, t, _) in &kept {
                prop_assert!(
                    frontier.less_equal(t),
                    "frontier should dominate kept time {}", t,
                );
            }
        }

        /// Empty input → no work, frontier untouched, position = 0.
        #[mz_ore::test]
        #[cfg_attr(miri, ignore)]
        fn extract_empty_input(upper_time in 0u64..=4) {
            let mut self_col: Column<Tuple> = Default::default();
            let upper = Antichain::from_elem(upper_time);
            let mut frontier: Antichain<u64> = Antichain::new();
            let mut keep: Column<Tuple> = Default::default();
            let mut ship: Column<Tuple> = Default::default();
            let mut position = 0;

            self_col.extract(
                &mut position,
                upper.borrow(),
                &mut frontier,
                &mut keep,
                &mut ship,
            );

            prop_assert_eq!(position, 0);
            prop_assert!(collect_column(&keep).is_empty());
            prop_assert!(collect_column(&ship).is_empty());
            prop_assert!(frontier.elements().is_empty());
        }
    }
}

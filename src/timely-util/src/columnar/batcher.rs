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

use crate::columnation::ColumnationStack;
use columnar::Container as _;
use columnar::Push as _;
use columnar::{Clear, Columnar, Index, Len};
use columnation::Columnation;
use differential_dataflow::difference::Semigroup;
use differential_dataflow::trace::implementations::merge_batcher::InternalMerger;
use differential_dataflow::trace::implementations::merge_batcher::container::InternalMerge;
use timely::Accountable;
use timely::Container;
use timely::PartialOrder;
use timely::container::{ContainerBuilder, PushInto};
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
    // C2: Container + for<'b> PushInto<&'b (D, T, R)>,
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
    R: Columnar + Semigroup + for<'b> Semigroup<columnar::Ref<'b, R>>,
    for<'b> columnar::Ref<'b, R>: Ord,
    for<'b> <(D, T, R) as Columnar>::Container: columnar::Push<&'b (D, T, R)>,
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

        // Sweep sorted refs, accumulating diffs for like (data, time) runs and
        // pushing non-zero results into target. Reuses owned scratch slots
        // across pushes by destructuring the tuple back after each push.
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
/// detour, by way of [`InternalMerge`] below.
pub type ColumnMerger<D, T, R> = InternalMerger<Column<(D, T, R)>>;

/// `InternalMerge` for [`Column`]-shaped sorted chunks.
impl<D, T, R> InternalMerge for Column<(D, T, R)>
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
    type TimeOwned = T;

    fn len(&self) -> usize {
        self.borrow().len()
    }

    fn clear(&mut self) {
        match self {
            Column::Typed(c) => c.clear(),
            Column::Bytes(_) | Column::Align(_) => {
                *self = Column::Typed(Default::default());
            }
        }
    }

    fn merge_from(&mut self, others: &mut [Self], positions: &mut [usize]) {
        match others.len() {
            0 => {}
            1 => {
                let other = &mut others[0];
                let pos = &mut positions[0];
                // If `self` is empty and `*pos == 0`, we can bulk swap in the other chunk.
                if self.is_empty() && *pos == 0 {
                    std::mem::swap(self, other);
                    return;
                }
                // Otherwise, bulk copy the remaining data from `other[*pos..]` into `self`.
                let Column::Typed(self_c) = self else {
                    unreachable!("merger chunks are always Column::Typed");
                };
                let src_c = other.borrow();
                self_c.extend_from_self(src_c, *pos..other.borrow().len());
                *pos = other.borrow().len();
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
                // The columnar tuple `Borrow::Ref` is recursive: for record
                // shape `((K, V), T, R)` the full ref is `((&K, &V), &T, &R)`,
                // built by indexing each leaf and constructing the nested
                // tuple. Going through `left_borrow.get(i)` therefore touches
                // every leaf — four bounds-checked array indices plus two
                // tuple constructions — even when the merge step only needs
                // the `(D, T)` key. Indexing each leaf view directly cuts the
                // work to the columns we actually consult and lets the diff
                // column stay out of the cache on probe paths.
                let l_d = left_borrow.0;
                let l_t = left_borrow.1;
                let l_r = left_borrow.2;
                let r_d = right_borrow.0;
                let r_t = right_borrow.1;
                let r_r = right_borrow.2;
                let upper_l = l_d.len();
                let upper_r = r_d.len();

                // Hoist the same split on the output. `(D, T, R)::Container`
                // is `(D::Container, T::Container, R::Container)`, so we can
                // address each leaf independently and let the compiler treat
                // each leaf-extend as a primitive bulk copy. The leaves stay
                // length-synchronized as long as every record path pushes
                // exactly one element to each.
                let (sd, st, sr) = self_c;

                let mut owned_d = D::default();
                let mut owned_t = T::default();
                let mut stash = R::default();

                // Yield mid-merge once we hit the ship threshold, so the
                // driver can swap in a fresh chunk and ship the full one.
                // The check matches `Column::at_capacity` and `ColumnBuilder`:
                // serialized size within 10% of the next 2 MiB boundary.
                //
                // We aggregate across the three leaf borrows by chaining
                // their `as_bytes` iterators, which is what `length_in_words`
                // walks; this matches what calling `at_serialized_capacity`
                // on the parent borrow would compute, but avoids reborrowing
                // the parent (which we've split into `sd`/`st`/`sr`).
                //
                // Calling this in the loop condition turned out to be
                // cheaper than tick-amortizing it: the cold-path branch is
                // well-predicted (always false on chunks below the ship
                // threshold), the leaf-length sums inline cleanly, and any
                // counter / labeled-break shape we tried added more inner
                // loop overhead than it saved.
                let at_ship_threshold = |sd: &D::Container, st: &T::Container, sr: &R::Container| {
                    use columnar::AsBytes as _;
                    use columnar::Borrow as _;
                    let words = 1
                        + sd.borrow().as_bytes().map(|(_a, b)| 1 + b.len().div_ceil(8)).sum::<usize>()
                        + st.borrow().as_bytes().map(|(_a, b)| 1 + b.len().div_ceil(8)).sum::<usize>()
                        + sr.borrow().as_bytes().map(|(_a, b)| 1 + b.len().div_ceil(8)).sum::<usize>();
                    let round = (words + ((1 << 18) - 1)) & !((1 << 18) - 1);
                    round - words < round / 10
                };

                while left_pos[0] < upper_l
                    && right_pos[0] < upper_r
                    && !at_ship_threshold(sd, st, sr)
                {
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
                            // worthwhile when there's an actual run, which we
                            // detect with the peek check below.
                            sd.push(d1);
                            st.push(t1);
                            sr.push(l_r.get(left_pos[0]));
                            left_pos[0] += 1;
                            // Long-run case: peek at the next record; if it's
                            // still strictly less than `(d2, t2)`, we have a
                            // run worth galloping (and bulk-copying).
                            if left_pos[0] < upper_l
                                && (l_d.get(left_pos[0]), l_t.get(left_pos[0])) < (d2, t2)
                            {
                                let start = left_pos[0];
                                gallop(upper_l, &mut left_pos[0], |i| {
                                    (l_d.get(i), l_t.get(i)) < (d2, t2)
                                });
                                // Per-leaf bulk copy of the run. Each call
                                // resolves to a primitive `extend_from_slice`
                                // on its leaf array (or recurses one level
                                // for tuple D = (K, V)), which the compiler
                                // can autovectorize.
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
                                D::copy_from(&mut owned_d, d1);
                                T::copy_from(&mut owned_t, t1);
                                sd.push(&owned_d);
                                st.push(&owned_t);
                                sr.push(&stash);
                            }
                            left_pos[0] += 1;
                            right_pos[0] += 1;
                        }
                    }
                }
            }
            n => unimplemented!(
                "Column-shaped k-way sorted merge with diff consolidation: {n} inputs"
            ),
        }
    }

    fn extract(
        &mut self,
        position: &mut usize,
        upper: AntichainRef<Self::TimeOwned>,
        frontier: &mut Antichain<Self::TimeOwned>,
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

        while *position < len {
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
        R: Columnar + Clone + Semigroup + for<'a> Semigroup<columnar::Ref<'a, R>>,
        for<'a> columnar::Ref<'a, R>: Ord,
        <(D, T, R) as Columnar>::Container: Clone,
        for<'a> <(D, T, R) as Columnar>::Container: columnar::Push<&'a (D, T, R)>,
        <(D, T, R) as Columnar>::Container: columnar::Push<(D, T, R)>,
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
}

#[cfg(test)]
mod proptests {
    //! Property tests for `InternalMerge for Column<(D, T, R)>`.
    //!
    //! Strategy: generate sorted+consolidated inputs (the merger's input
    //! contract), drive `merge_from` / `extract` the same way the framework
    //! would, and compare against a brute-force reference impl.
    //!
    //! Test types are `D = (u64, u64)`, `T = u64`, `R = i64` drawn from small
    //! ranges so that equal-key collisions are common and the consolidation
    //! path actually runs.
    use super::*;
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

    /// Drive a 2-way merge the same way `InternalMerger::merge` would: a
    /// 2-input call until one side exhausts, then a 1-input drain for
    /// whichever side still has data.
    fn drive_merge(left: Column<Tuple>, right: Column<Tuple>) -> Column<Tuple> {
        let mut self_col: Column<Tuple> = Default::default();
        let mut others = [left, right];
        let mut positions = [0usize, 0];
        InternalMerge::merge_from(&mut self_col, &mut others, &mut positions);

        let [left_done, right_done] = others;
        let [left_pos, right_pos] = positions;

        if left_pos < left_done.borrow().len() {
            let mut tail = [left_done];
            let mut p = [left_pos];
            InternalMerge::merge_from(&mut self_col, &mut tail, &mut p);
        } else if right_pos < right_done.borrow().len() {
            let mut tail = [right_done];
            let mut p = [right_pos];
            InternalMerge::merge_from(&mut self_col, &mut tail, &mut p);
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
                ((pos_frac as usize) * len) / 101
            };

            // Self starts non-empty so we exercise the bulk-copy path, not the
            // empty-self swap shortcut.
            let mut self_col: Column<Tuple> = Default::default();
            let sentinel: Tuple = ((u64::MAX, u64::MAX), 0, 1);
            self_col.push_into(sentinel);

            let mut others = [build_column(&data)];
            let mut positions = [start_pos];
            InternalMerge::merge_from(&mut self_col, &mut others, &mut positions);

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
            InternalMerge::merge_from(&mut self_col, &mut others, &mut positions);

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

            InternalMerge::extract(
                &mut self_col,
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

            InternalMerge::extract(
                &mut self_col,
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

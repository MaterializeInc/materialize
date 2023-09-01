// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Code for iterating through one or more parts, including streaming consolidation.

use std::cmp::Reverse;
use std::collections::binary_heap::PeekMut;
use std::collections::{BinaryHeap, VecDeque};
use std::marker::PhantomData;
use std::sync::Arc;

use differential_dataflow::consolidation::consolidate_updates;
use differential_dataflow::difference::Semigroup;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::trace::Description;
use futures_util::stream::FuturesUnordered;
use futures_util::StreamExt;
use mz_persist::location::Blob;
use mz_persist_types::Codec64;
use timely::progress::Timestamp;
use tokio::task::JoinHandle;
use tracing::{debug_span, Instrument};

use crate::fetch::{fetch_batch_part, Cursor, EncodedPart, FetchBatchFilter};
use crate::internal::metrics::{BatchPartReadMetrics, ReadMetrics, ShardMetrics};
use crate::internal::paths::PartialBatchKey;
use crate::internal::state::HollowBatchPart;
use crate::metrics::Metrics;
use crate::ShardId;

type Tuple<T, D> = ((Vec<u8>, Vec<u8>), T, D);
type TupleRef<'a, T, D> = (&'a [u8], &'a [u8], T, D);

fn borrow_tuple<T: Clone, D: Clone>(((k, v), t, d): &Tuple<T, D>) -> TupleRef<T, D> {
    (k.as_slice(), v.as_slice(), t.clone(), d.clone())
}

fn clone_tuple<T, D>((k, v, t, d): TupleRef<T, D>) -> Tuple<T, D> {
    ((k.to_vec(), v.to_vec()), t, d)
}

/// The data needed to fetch a batch part, bundled up to make it easy
/// to send between threads.
#[derive(Debug)]
pub(crate) struct FetchData<T> {
    shard_id: ShardId,
    blob: Arc<dyn Blob + Send + Sync>,
    metrics: Arc<Metrics>,
    read_metrics: fn(&BatchPartReadMetrics) -> &ReadMetrics,
    shard_metrics: Arc<ShardMetrics>,
    part_key: PartialBatchKey,
    part_desc: Description<T>,
}

impl<T: Codec64 + Timestamp + Lattice> FetchData<T> {
    async fn fetch(self) -> anyhow::Result<EncodedPart<T>> {
        let Self {
            shard_id,
            blob,
            metrics,
            read_metrics,
            shard_metrics,
            part_key,
            part_desc,
        } = self;
        metrics.compaction.parts_prefetched.inc();
        fetch_batch_part(
            &shard_id,
            &*blob,
            &metrics,
            &shard_metrics,
            read_metrics(&metrics.read),
            &part_key,
            &part_desc,
        )
        .await
    }
}

#[derive(Debug)]
pub(crate) enum ConsolidationPart<T, D> {
    Queued {
        data: Option<FetchData<T>>,
    },
    Prefetched {
        handle: JoinHandle<anyhow::Result<EncodedPart<T>>>,
        metrics: Arc<Metrics>,
    },
    Encoded {
        part: EncodedPart<T>,
        cursor: Cursor,
    },
    Sorted {
        data: Vec<((Vec<u8>, Vec<u8>), T, D)>,
        index: usize,
    },
}

impl<'a, T: Timestamp + Codec64 + Lattice, D: Codec64 + Semigroup> ConsolidationPart<T, D> {
    pub(crate) fn from_encoded(part: EncodedPart<T>, filter: &'a FetchBatchFilter<T>) -> Self {
        let mut cursor = Cursor::default();
        if part.maybe_unconsolidated() {
            Self::from_iter(ConsolidationPartIter::encoded(&part, &mut cursor, filter))
        } else {
            ConsolidationPart::Encoded { part, cursor }
        }
    }

    pub(crate) fn from_iter(data: impl IntoIterator<Item = TupleRef<'a, T, D>>) -> Self
    where
        D: Semigroup,
    {
        let mut data: Vec<_> = data.into_iter().map(clone_tuple).collect();
        consolidate_updates(&mut data);
        Self::Sorted { data, index: 0 }
    }

    async fn join(&mut self, filter: &'a FetchBatchFilter<T>) -> anyhow::Result<()> {
        match self {
            ConsolidationPart::Queued { data } => {
                let data = data.take().expect("unfetched");
                data.metrics.compaction.parts_waited.inc();
                *self = Self::from_encoded(data.fetch().await?, filter);
            }
            ConsolidationPart::Prefetched { handle, metrics } => {
                metrics.compaction.parts_waited.inc();
                *self = Self::from_encoded(handle.await??, filter);
            }
            ConsolidationPart::Encoded { .. } | ConsolidationPart::Sorted { .. } => {}
        }
        Ok(())
    }

    /// This requires a mutable pointer because the cursor may need to scan ahead to find the next
    /// valid record.
    pub(crate) fn is_empty(&mut self) -> bool {
        match self {
            ConsolidationPart::Encoded { part, cursor, .. } => cursor.peek(part).is_none(),
            ConsolidationPart::Sorted { data, index } => data.len() <= *index,
            ConsolidationPart::Queued { .. } | ConsolidationPart::Prefetched { .. } => false,
        }
    }
}

/// A tool for incrementally consolidating a persist shard.
///
/// The naive way to consolidate a Persist shard would be to fetch every part, then consolidate
/// the whole thing. We'd like to improve on that in two ways:
/// - Concurrency: we'd like to be able to start consolidating and returning results before every
///   part is fetched. (And continue fetching while we're doing other work.)
/// - Memory usage: we'd like to limit the number of parts we have in memory at once, dropping
///   parts that are fully consolidated and fetching parts just a little before they're needed.
///
/// This interface supports this by consolidating in multiple steps. Each call to [Self::next]
/// will do some housekeeping work -- prefetching needed parts, dropping any unneeded parts -- and
/// return  an iterator over a consolidated subset of the data. To read an entire dataset, the
/// client should call `next` until it returns `None`, which signals all data has been returned...
/// but it's also free to abandon the instance at any time if it eg. only needs a few entries.
#[derive(Debug)]
pub(crate) struct Consolidator<T, D> {
    runs: Vec<VecDeque<(ConsolidationPart<T, D>, usize)>>,
    filter: FetchBatchFilter<T>,
    budget: usize,
    // NB: this is the tricky part!
    // One hazard of streaming consolidation is that we may start consolidating a particular KVT,
    // but not be able to finish, because some other part that might also contain the same KVT
    // (and thus consolidate) may not have been fetched yet. Let's call such a KVT an "incomplete"
    // tuple. These two fields are used to "carry over" incomplete tuples between calls
    // to `next`: `initial_state` is used to pass an incomplete tuple to the next round of
    // consolidation, and `drop_stash` is used to stow any incomplete tuple when the consolidating
    // iter is dropped.
    initial_state: Option<Tuple<T, D>>,
    drop_stash: Option<Tuple<T, D>>,
}

impl<T: Timestamp + Codec64 + Lattice, D: Codec64 + Semigroup> Consolidator<T, D> {
    /// Create a new [Self] instance with the given prefetch budget. This budget is a "soft limit"
    /// on the size of the parts that the consolidator will fetch... we'll try and stay below the
    /// limit, but may burst above it if that's necessary to make progress.
    pub fn new(filter: FetchBatchFilter<T>, prefetch_budget_bytes: usize) -> Self {
        Self {
            runs: vec![],
            filter,
            budget: prefetch_budget_bytes,
            initial_state: None,
            drop_stash: None,
        }
    }

    /// Add another run of data to be consolidated.
    ///
    /// To ensure consolidation, every tuple in this run should be larger than any tuple already
    /// returned from the iterator. At the moment, this invariant is not checked. The simplest way
    /// to ensure this is to enqueue every run before any calls to next.
    // TODO(bkirwi): enforce this invariant, either by forcing all runs to be pre-registered or with an assert.
    // TODO(bkirwi): try moving some of these params into the constructor when the dust settles.
    pub fn enqueue_run<'a>(
        &mut self,
        shard_id: ShardId,
        blob: &Arc<dyn Blob + Send + Sync>,
        metrics: &Arc<Metrics>,
        read_metrics: fn(&BatchPartReadMetrics) -> &ReadMetrics,
        shard_metrics: &Arc<ShardMetrics>,
        desc: &Description<T>,
        parts: impl IntoIterator<Item = &'a HollowBatchPart>,
    ) {
        let run = parts
            .into_iter()
            .map(|part: &HollowBatchPart| {
                let c_part = ConsolidationPart::Queued {
                    data: Some(FetchData {
                        shard_id,
                        blob: Arc::clone(blob),
                        metrics: Arc::clone(metrics),
                        read_metrics,
                        shard_metrics: Arc::clone(shard_metrics),
                        part_key: part.key.clone(),
                        part_desc: desc.clone(),
                    }),
                };
                let size = part.encoded_size_bytes;
                (c_part, size)
            })
            .collect();
        self.runs.push(run);
    }

    /// Tidy up: discard any empty parts, and discard any runs that have no parts left.
    fn trim(&mut self) {
        self.runs.retain_mut(|run| {
            while run.front_mut().map_or(false, |(part, _)| part.is_empty()) {
                run.pop_front();
            }
            !run.is_empty()
        });

        // Some budget may have just been freed up: start prefetching.
        self.start_prefetches();
    }

    /// Return an iterator over the next consolidated chunk of output, if there's any left.
    ///
    /// Requirement: at least the first part of each run should be fetched and nonempty.
    fn iter(&mut self) -> Option<ConsolidatingIter<T, D>> {
        // At this point, the `initial_state` of the previously-returned iterator has either been
        // fully consolidated and returned, or put back into `drop_stash`. In either case, this is
        // safe to overwrite.
        self.initial_state = self.drop_stash.take();
        if self.initial_state.is_none() && self.runs.is_empty() {
            return None;
        }

        let mut iter = ConsolidatingIter::new(
            self.initial_state.as_ref().map(borrow_tuple),
            &mut self.drop_stash,
        );

        for run in &mut self.runs {
            let last_in_run = run.len() < 2;
            if let Some((part, _)) = run.front_mut() {
                let part_iter = match part {
                    ConsolidationPart::Encoded { part, cursor } => {
                        ConsolidationPartIter::encoded(part, cursor, &self.filter)
                    }
                    ConsolidationPart::Sorted { data, index } => {
                        ConsolidationPartIter::Sorted { data, index }
                    }
                    ConsolidationPart::Queued { .. } | ConsolidationPart::Prefetched { .. } => {
                        // Unreachable from the public API: we always join on the first part of every
                        // run in `next`, so we should never encounter an unfetched part here.
                        // TODO: when we have lower bounds on keys in the stats, we could insert a
                        // placeholder here instead.
                        panic!("trying to create an interator from an unfetched part!")
                    }
                };
                iter.push(part_iter, last_in_run);
            }
        }

        Some(iter)
    }

    /// Wait until the next part in every run is available, then return an iterator over the next
    /// consolidated chunk of output. If this method returns `None`, that all the data has been
    /// exhausted and the full consolidated dataset has been returned.
    pub(crate) async fn next(&mut self) -> Option<ConsolidatingIter<T, D>> {
        self.trim();
        let futures: FuturesUnordered<_> = self
            .runs
            .iter_mut()
            .map(|run| async {
                run.front_mut()
                    .expect("trimmed run should be nonempty")
                    .0
                    .join(&self.filter)
                    .await
                    .expect("fetching data to succeed")
            })
            .collect();
        let () = futures.collect().await;
        self.iter()
    }

    /// The size of the data that we _might_ be holding concurrently in memory. While this is
    /// normally kept less than the budget, it may burst over it temporarily, since we need at
    /// least one part in every run to continue making progress.
    fn live_bytes(&self) -> usize {
        self.runs
            .iter()
            .flat_map(|run| {
                run.iter().map(|(part, size)| match part {
                    ConsolidationPart::Queued { .. } => 0,
                    ConsolidationPart::Prefetched { .. }
                    | ConsolidationPart::Encoded { .. }
                    | ConsolidationPart::Sorted { .. } => *size,
                })
            })
            .sum()
    }

    /// Returns None if the budget was exhausted, or Some(remaining_bytes) if it is not.
    pub(crate) fn start_prefetches(&mut self) -> Option<usize> {
        let mut prefetch_budget_bytes = self.budget;

        let mut check_budget = |size| {
            // Subtract the amount from the budget, returning None if the budget is exhausted.
            prefetch_budget_bytes
                .checked_sub(size)
                .map(|remaining| prefetch_budget_bytes = remaining)
        };

        // First account for how much budget has already been used
        let live_bytes = self.live_bytes();
        check_budget(live_bytes)?;
        // Iterate through parts in a certain order (attempting to match the
        // order in which they'll be fetched), prefetching until we run out of
        // budget.
        //
        // The order used here is the first part of each run, then the second, etc.
        // There's a bunch of heuristics we could use here, but we'd get it exactly
        // correct if we stored on HollowBatchPart the actual kv bounds of data
        // contained in each part and go in sorted order of that. This information
        // would also be useful for pushing MFP down into persist reads, so it seems
        // like we might want to do it at some point. As a result, don't think too
        // hard about this heuristic at first.
        let max_run_len = self.runs.iter().map(|x| x.len()).max().unwrap_or_default();
        for idx in 0..max_run_len {
            for run in self.runs.iter_mut() {
                let Some((c_part, size)) = run.get_mut(idx) else {
                    continue;
                };

                if let ConsolidationPart::Queued { data } = c_part {
                    check_budget(*size)?;
                    let data = data.take().expect("unfetched");
                    let metrics = Arc::clone(&data.metrics);
                    let span = debug_span!("compaction::prefetch");
                    let handle = mz_ore::task::spawn(
                        || "persist::compaction::prefetch",
                        async move { data.fetch().await }.instrument(span),
                    );
                    *c_part = ConsolidationPart::Prefetched { handle, metrics };
                }
            }
        }

        Some(prefetch_budget_bytes)
    }
}

/// The mutable references in this iterator (the [Cursor] for an encoded part, or the index for a
/// sorted one) point to state that outlives this iterator, and we take care not to advance them
/// too eagerly.
/// In particular, we only advance the cursor past a tuple when that tuple has been returned from
/// a call to `next`.
#[derive(Debug)]
pub(crate) enum ConsolidationPartIter<'a, T: Timestamp, D> {
    Encoded {
        part: &'a EncodedPart<T>,
        cursor: &'a mut Cursor,
        filter: &'a FetchBatchFilter<T>,
        // The tuple that would be returned by the next call to `cursor.peek`, with the timestamp
        // advanced to the as-of.
        next: Option<TupleRef<'a, T, D>>,
    },
    Sorted {
        data: &'a [((Vec<u8>, Vec<u8>), T, D)],
        index: &'a mut usize,
    },
}

impl<'a, T: Timestamp + Codec64 + Lattice, D: Codec64> ConsolidationPartIter<'a, T, D> {
    fn encoded(
        part: &'a EncodedPart<T>,
        cursor: &'a mut Cursor,
        filter: &'a FetchBatchFilter<T>,
    ) -> ConsolidationPartIter<'a, T, D> {
        let mut iter = Self::Encoded {
            part,
            cursor,
            filter,
            next: None,
        };
        iter.next(); // Advance to the next valid entry in the part
        iter
    }
    fn peek(&self) -> Option<TupleRef<'a, T, D>> {
        match self {
            Self::Encoded { next, .. } => next.clone(),
            Self::Sorted { data, index } => Some(borrow_tuple(data.get(**index)?)),
        }
    }
}

impl<'a, T: Timestamp + Codec64 + Lattice, D: Codec64> Iterator
    for ConsolidationPartIter<'a, T, D>
{
    type Item = TupleRef<'a, T, D>;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            ConsolidationPartIter::Encoded {
                part,
                cursor,
                filter,
                next,
            } => {
                let out = next.take();
                if out.is_some() {
                    cursor.advance(part);
                }
                *next = loop {
                    match cursor.peek(part) {
                        None => break None,
                        Some((k, v, mut t, d)) => {
                            if filter.filter_ts(&mut t) {
                                break Some((k, v, t, D::decode(d)));
                            }
                        }
                    }
                    cursor.advance(part);
                };
                out
            }
            ConsolidationPartIter::Sorted { data, index } => {
                let tuple = data.get(**index)?;
                **index += 1;
                Some(borrow_tuple(tuple))
            }
        }
    }
}

/// This is used as a max-heap entry: the ordering of the fields is important!
#[derive(Debug, Ord, PartialOrd, Eq, PartialEq)]
struct PartRef<'a, T: Timestamp, D> {
    /// The smallest KVT that might be emitted from this run in the future.
    /// This is reverse-sorted: Nones will sort largest (and be popped first on the heap)
    /// and smaller keys will be popped before larger keys.
    next_kvt: Reverse<Option<(&'a [u8], &'a [u8], T)>>,
    /// The index of the corresponding iterator.
    index: usize,
    /// Whether / not the iterator for the part is the last in its run, or whether there may be
    /// iterators for the same part in the future.
    last_in_run: bool,
    _phantom: PhantomData<D>,
}

impl<'a, T: Timestamp + Codec64 + Lattice, D: Codec64 + Semigroup> PartRef<'a, T, D> {
    fn update_peek(&mut self, iter: &mut ConsolidationPartIter<'a, T, D>) {
        let peek = iter.peek();
        self.next_kvt = Reverse(peek.map(|(k, v, t, _)| (k, v, t)));
    }

    fn pop(&mut self, from: &mut [ConsolidationPartIter<'a, T, D>]) -> Option<TupleRef<'a, T, D>> {
        let iter = &mut from[self.index];
        let popped = iter.next();
        self.update_peek(iter);
        popped
    }
}

#[derive(Debug)]
pub(crate) struct ConsolidatingIter<'a, T: Timestamp, D> {
    parts: Vec<ConsolidationPartIter<'a, T, D>>,
    heap: BinaryHeap<PartRef<'a, T, D>>,
    state: Option<TupleRef<'a, T, D>>,
    drop_stash: &'a mut Option<Tuple<T, D>>,
}

impl<'a, T, D> ConsolidatingIter<'a, T, D>
where
    T: Timestamp + Codec64 + Lattice,
    D: Codec64 + Semigroup,
{
    pub fn new(
        init_state: Option<TupleRef<'a, T, D>>,
        drop_stash: &'a mut Option<Tuple<T, D>>,
    ) -> Self {
        Self {
            parts: vec![],
            heap: BinaryHeap::new(),
            state: init_state,
            drop_stash,
        }
    }

    fn push(&mut self, mut iter: ConsolidationPartIter<'a, T, D>, last_in_run: bool) {
        let mut part_ref = PartRef {
            next_kvt: Reverse(None),
            index: self.parts.len(),
            last_in_run,
            _phantom: Default::default(),
        };
        part_ref.update_peek(&mut iter);
        self.parts.push(iter);
        self.heap.push(part_ref);
    }

    /// Attempt to consolidate as much into the current state as possible.
    fn consolidate(&mut self) -> Option<TupleRef<'a, T, D>> {
        loop {
            let Some(mut part) = self.heap.peek_mut() else {
                break;
            };
            match part.next_kvt.0.as_ref() {
                None => {
                    if part.last_in_run {
                        PeekMut::pop(part);
                    } else {
                        // NB: this is the only case this method exits without returning the current state:
                        // there may be more instances of the KVT in a later part of the same run.
                        return None;
                    }
                }
                Some((k1, v1, t1)) => match &mut self.state {
                    None => {
                        self.state = part.pop(&mut self.parts);
                    }
                    Some((k0, v0, t0, d0)) => {
                        if (*k0, *v0, &*t0) == (*k1, *v1, t1) {
                            let (_, _, _, d1) = part
                                .pop(&mut self.parts)
                                .expect("popping from a non-empty iterator");
                            d0.plus_equals(&d1);
                        } else {
                            break;
                        }
                    }
                },
            }
        }
        self.state.take()
    }
}

impl<'a, T, D> Iterator for ConsolidatingIter<'a, T, D>
where
    T: Timestamp + Codec64 + Lattice,
    D: Codec64 + Semigroup,
{
    type Item = TupleRef<'a, T, D>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match self.consolidate() {
                Some((_, _, _, d)) if d.is_zero() => continue,
                other => break other,
            }
        }
    }
}

impl<'a, T: Timestamp, D> Drop for ConsolidatingIter<'a, T, D> {
    fn drop(&mut self) {
        // Make sure to stash any incomplete state in a place where we'll pick it up on the next run.
        // See the comment on `Consolidator` for more on why this is necessary.
        *self.drop_stash = self.state.take().map(clone_tuple);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Arc;

    use differential_dataflow::trace::Description;
    use proptest::collection::vec;
    use proptest::prelude::*;
    use timely::progress::Antichain;

    use mz_ore::metrics::MetricsRegistry;
    use mz_persist::location::Blob;
    use mz_persist::mem::{MemBlob, MemBlobConfig};

    use crate::cfg::PersistConfig;
    use crate::internal::paths::PartialBatchKey;
    use crate::internal::state::HollowBatchPart;
    use crate::metrics::Metrics;
    use crate::ShardId;

    #[mz_ore::test]
    fn consolidation() {
        // Check that output consolidated via this logic matches output consolidated via timely's!
        type Part = Vec<((Vec<u8>, Vec<u8>), u64, i64)>;

        fn check(parts: Vec<(Part, usize)>) {
            let original = {
                let mut rows = parts
                    .iter()
                    .flat_map(|(p, _)| p.clone())
                    .collect::<Vec<_>>();
                consolidate_updates(&mut rows);
                rows
            };
            let streaming = {
                // Toy compaction loop!
                let mut consolidator = Consolidator {
                    // Generated runs of data that are sorted, but not necessarily consolidated.
                    // This is because timestamp-advancement may cause us to have duplicate KVTs,
                    // including those that span runs.
                    runs: parts
                        .into_iter()
                        .map(|(mut part, cut)| {
                            part.sort();
                            let part_2 = part.split_off(cut.min(part.len()));
                            [part, part_2]
                                .into_iter()
                                .map(|part| {
                                    (
                                        ConsolidationPart::from_iter(part.iter().map(borrow_tuple)),
                                        0,
                                    )
                                })
                                .collect::<VecDeque<_>>()
                        })
                        .collect::<Vec<_>>(),
                    filter: FetchBatchFilter::Compaction {
                        since: Antichain::from_elem(0),
                    },
                    budget: 0,
                    initial_state: None,
                    drop_stash: None,
                };

                let mut out = vec![];
                loop {
                    consolidator.trim();
                    let Some(iter) = consolidator.iter() else {
                        break;
                    };
                    out.extend(iter.map(clone_tuple));
                }
                out
            };

            assert_eq!(original, streaming);
        }

        // Restricting the ranges to help make sure we have frequent collisions
        let key_gen = (0..4usize).prop_map(|i| i.to_string().into_bytes()).boxed();
        let part_gen = vec(
            ((key_gen.clone(), key_gen.clone()), 0..10u64, -3..=3i64),
            0..10,
        );
        let run_gen = vec((part_gen, 0..10usize), 0..5);
        proptest!(|(state in run_gen)| {
            check(state)
        });
    }

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)] // unsupported operation: returning ready events from epoll_wait is not yet implemented
    async fn prefetches() {
        fn check(budget: usize, runs: Vec<Vec<usize>>) {
            let desc = Description::new(
                Antichain::from_elem(0u64),
                Antichain::new(),
                Antichain::from_elem(0u64),
            );

            let total_size: usize = runs.iter().flat_map(|run| run.iter().map(|p| *p)).sum();

            let shard_id = ShardId::new();
            let blob: Arc<dyn Blob + Send + Sync> =
                Arc::new(MemBlob::open(MemBlobConfig::default()));
            let metrics = Arc::new(Metrics::new(
                &PersistConfig::new_for_tests(),
                &MetricsRegistry::new(),
            ));
            let shard_metrics = metrics.shards.shard(&shard_id, "");

            let mut consolidator: Consolidator<u64, i64> = Consolidator::new(
                FetchBatchFilter::Compaction {
                    since: desc.since().clone(),
                },
                budget,
            );

            for run in runs {
                let parts: Vec<_> = run
                    .into_iter()
                    .map(|encoded_size_bytes| HollowBatchPart {
                        key: PartialBatchKey("".into()),
                        encoded_size_bytes,
                        key_lower: vec![],
                        stats: None,
                    })
                    .collect();
                consolidator.enqueue_run(
                    shard_id,
                    &blob,
                    &metrics,
                    |m| &m.batch_fetcher,
                    &shard_metrics,
                    &desc,
                    &parts,
                )
            }

            // No matter what, the budget should be respected.
            let remaining = consolidator.start_prefetches();
            let live_bytes = consolidator.live_bytes();
            assert!(live_bytes <= budget, "budget should be respected");
            match remaining {
                None => assert!(live_bytes < total_size, "not all parts fetched"),
                Some(remaining) => assert_eq!(
                    live_bytes + remaining,
                    budget,
                    "remaining should match budget"
                ),
            }

            // If we up the budget to match the total size, we should prefetch everything.
            consolidator.budget = total_size;
            assert!(consolidator.start_prefetches() == Some(0));
        }

        let run_gen = vec(vec(0..20usize, 0..5usize), 0..5usize);
        proptest!(|(budget in 0..20usize, state in run_gen)| {
            check(budget, state)
        });
    }
}

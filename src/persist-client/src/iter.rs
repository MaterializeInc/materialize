// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Code for iterating through one or more parts, including streaming consolidation.

use anyhow::anyhow;
use arrow::array::{Array, AsArray, Int64Array, Int64Builder};
use std::cmp::{Ordering, Reverse};
use std::collections::binary_heap::PeekMut;
use std::collections::{BinaryHeap, VecDeque};
use std::fmt::Debug;
use std::marker::PhantomData;
use std::mem;
use std::sync::Arc;

use differential_dataflow::difference::Semigroup;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::trace::Description;
use futures_util::stream::FuturesUnordered;
use futures_util::StreamExt;
use mz_dyncfg::Config;
use mz_ore::task::JoinHandle;
use mz_persist::indexed::columnar::{ColumnarRecords, ColumnarRecordsStructuredExt};
use mz_persist::indexed::encoding::BlobTraceUpdates;
use mz_persist::location::Blob;
use mz_persist::metrics::ColumnarMetrics;
use mz_persist_types::Codec64;
use semver::Version;
use timely::progress::Timestamp;
use tracing::{debug_span, Instrument};

use crate::fetch::{EncodedPart, FetchBatchFilter};
use crate::internal::metrics::{ReadMetrics, ShardMetrics};
use crate::internal::paths::WriterKey;
use crate::internal::state::BatchPart;
use crate::metrics::Metrics;
use crate::ShardId;

/// Versions prior to this had bugs in consolidation, or used a different sort. However,
/// we can assume that consolidated parts at this version or higher were consolidated
/// according to the current definition.
pub const MINIMUM_CONSOLIDATED_VERSION: Version = Version::new(0, 67, 0);

type KV<'a> = (&'a [u8], &'a [u8]);

/// The data needed to fetch a batch part, bundled up to make it easy
/// to send between threads.
#[derive(Debug)]
pub(crate) enum FetchData<T> {
    Unfetched {
        part_desc: Description<T>,
        part: BatchPart<T>,
    },
    AlreadyFetched,
}

impl<T: Codec64 + Timestamp + Lattice> FetchData<T> {
    /// Returns true iff we were using a different ordering for data or timestamps
    /// when the part was created. This means parts or runs may not be ordered according to
    /// our modern definition, even if the metadata indicates they've been compacted before.
    fn wrong_sort(&self) -> bool {
        let min_version = WriterKey::for_version(&MINIMUM_CONSOLIDATED_VERSION);
        match self {
            FetchData::Unfetched { part, .. } => match part.writer_key() {
                // Old hollow parts may have used a different sort
                Some(key) => key < min_version,
                // Inline parts are all recent enough to have been sorted using the latest ordering,
                // if they're sorted at all.
                None => false,
            },
            FetchData::AlreadyFetched => false,
        }
    }

    fn take(&mut self) -> Self {
        mem::replace(self, FetchData::AlreadyFetched)
    }

    fn key_lower(&self) -> &[u8] {
        match self {
            FetchData::Unfetched { part, .. } => part.key_lower(),
            FetchData::AlreadyFetched => &[],
        }
    }

    async fn fetch(
        self,
        shard_id: ShardId,
        blob: &dyn Blob,
        metrics: &Metrics,
        shard_metrics: &ShardMetrics,
        read_metrics: &ReadMetrics,
    ) -> anyhow::Result<EncodedPart<T>> {
        match self {
            FetchData::Unfetched {
                part_desc, part, ..
            } => EncodedPart::fetch(
                &shard_id,
                &*blob,
                metrics,
                shard_metrics,
                read_metrics,
                &part_desc,
                &part,
            )
            .await
            .map_err(|blob_key| anyhow!("missing unleased key {blob_key}")),
            FetchData::AlreadyFetched => Err(anyhow!("attempt to fetch an already-fetched part")),
        }
    }
}

/// Indices into a part. For most parts, all we need is a single index to the current entry...
/// but for parts that have never been consolidated, this would return entries in the "wrong"
/// order, and it's expensive to re-sort the columnar data. Instead, we sort a list of indices
/// and then use this helper to hand them out in the correct order.
#[derive(Debug, Ord, PartialOrd, Eq, PartialEq, Default)]
struct PartIndices {
    sorted_indices: VecDeque<usize>,
    next_index: usize,
}

impl PartIndices {
    fn index(&self) -> usize {
        self.sorted_indices
            .front()
            .copied()
            .unwrap_or(self.next_index)
    }

    fn inc(&mut self) {
        if self.sorted_indices.pop_front().is_none() {
            self.next_index += 1;
        }
    }
}

#[derive(Debug)]
enum ConsolidationPart<T, D> {
    Queued {
        data: FetchData<T>,
    },
    Prefetched {
        handle: JoinHandle<anyhow::Result<EncodedPart<T>>>,
        wrong_sort: bool,
        key_lower: Vec<u8>,
    },
    Encoded {
        part: Columnar<T, D>,
        cursor: PartIndices,
    },
}

#[derive(Debug)]
struct Columnar<T, D> {
    updates: BlobTraceUpdates,
    _phantom: PhantomData<fn() -> (T, D)>,
}

impl<T: Codec64, D: Codec64> Columnar<T, D> {
    fn len(&self) -> usize {
        self.updates.records().len()
    }

    fn get(&self, index: usize) -> Option<(KV, T, D)> {
        let ((k, v), t, d) = self.updates.records().get(index)?;
        Some(((k, v), T::decode(t), D::decode(d)))
    }

    fn interleave(
        parts: &[&Columnar<T, D>],
        indices: &[Indices],
        timestamps: Int64Array,
        diffs: Int64Array,
    ) -> Self {
        let mut arrays: Vec<&dyn Array> = Vec::with_capacity(parts.len());
        let mut interleave = |get_array: fn(&BlobTraceUpdates) -> &dyn Array| {
            for part in parts {
                arrays.push(get_array(&part.updates));
            }
            let out =
                ::arrow::compute::interleave(arrays.as_slice(), indices).expect("valid references");
            arrays.clear();
            out
        };
        let keys = interleave(|u| u.records().keys()).as_binary().clone();
        let vals = interleave(|u| u.records().vals()).as_binary().clone();
        let records = ColumnarRecords::new(keys, vals, timestamps, diffs);
        let keep_ext = parts.iter().all(|e| e.updates.structured().is_some());
        let updates = if keep_ext {
            let key = interleave(|u| &u.structured().unwrap().key);
            let val = interleave(|u| &u.structured().unwrap().val);
            BlobTraceUpdates::Both(records, ColumnarRecordsStructuredExt { key, val })
        } else {
            BlobTraceUpdates::Row(records)
        };

        Columnar {
            updates,
            _phantom: Default::default(),
        }
    }
}

impl<T: Timestamp + Codec64 + Lattice, D: Codec64> ConsolidationPart<T, D> {
    pub(crate) fn from_encoded(
        part: EncodedPart<T>,
        force_reconsolidation: bool,
        metrics: &ColumnarMetrics,
    ) -> Self {
        let reconsolidate = part.maybe_unconsolidated() || force_reconsolidation;
        let updates: Columnar<T, D> = Columnar {
            updates: part.normalize(metrics),
            _phantom: Default::default(),
        };
        let cursor = if reconsolidate {
            let len = updates.len();
            let mut indices: Vec<_> = (0..len).collect();

            indices.sort_by_key(|i| updates.get(*i).map(|(kv, t, _d)| (kv, t)));

            PartIndices {
                sorted_indices: indices.into(),
                next_index: len,
            }
        } else {
            PartIndices::default()
        };

        ConsolidationPart::Encoded {
            part: updates,
            cursor,
        }
    }

    fn kvt_lower(&self) -> Option<(KV, T)> {
        match self {
            ConsolidationPart::Queued { data } => Some(((data.key_lower(), &[]), T::minimum())),
            ConsolidationPart::Prefetched { key_lower, .. } => {
                Some(((key_lower.as_slice(), &[]), T::minimum()))
            }
            ConsolidationPart::Encoded { part, cursor } => {
                let (kv, t, _d) = part.get(cursor.index())?;
                Some((kv, t))
            }
        }
    }

    /// This requires a mutable pointer because the cursor may need to scan ahead to find the next
    /// valid record.
    pub(crate) fn is_empty(&self) -> bool {
        match self {
            ConsolidationPart::Encoded { part, cursor, .. } => cursor.index() >= part.len(),
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
    context: String,
    shard_id: ShardId,
    blob: Arc<dyn Blob>,
    metrics: Arc<Metrics>,
    shard_metrics: Arc<ShardMetrics>,
    read_metrics: Arc<ReadMetrics>,
    runs: Vec<VecDeque<(ConsolidationPart<T, D>, usize)>>,
    filter: FetchBatchFilter<T>,
    budget: usize,
    split_old_runs: bool,
    // NB: this is the tricky part!
    // One hazard of streaming consolidation is that we may start consolidating a particular KVT,
    // but not be able to finish, because some other part that might also contain the same KVT
    // may not have been fetched yet. The `drop_stash` gives us somewhere
    // to store the streaming iterator's work-in-progress state between runs.
    drop_stash: Option<Columnar<T, D>>,
}

pub(crate) const SPLIT_OLD_RUNS: Config<bool> = Config::new(
    "persist_split_old_runs",
    true,
    "If set, split up runs that were written by older versions of Materialize and may not truly be consolidated."
);

impl<T, D> Consolidator<T, D>
where
    T: Timestamp + Codec64 + Lattice,
    D: Codec64 + Semigroup + Ord,
{
    /// Create a new [Self] instance with the given prefetch budget. This budget is a "soft limit"
    /// on the size of the parts that the consolidator will fetch... we'll try and stay below the
    /// limit, but may burst above it if that's necessary to make progress.
    pub fn new(
        context: String,
        shard_id: ShardId,
        blob: Arc<dyn Blob>,
        metrics: Arc<Metrics>,
        shard_metrics: Arc<ShardMetrics>,
        read_metrics: ReadMetrics,
        filter: FetchBatchFilter<T>,
        prefetch_budget_bytes: usize,
        split_old_runs: bool,
    ) -> Self {
        Self {
            context,
            metrics,
            shard_id,
            blob,
            read_metrics: Arc::new(read_metrics),
            shard_metrics,
            runs: vec![],
            filter,
            budget: prefetch_budget_bytes,
            split_old_runs,
            drop_stash: None,
        }
    }

    /// Add another run of data to be consolidated.
    ///
    /// To ensure consolidation, every tuple in this run should be larger than any tuple already
    /// returned from the iterator. At the moment, this invariant is not checked. The simplest way
    /// to ensure this is to enqueue every run before any calls to next.
    // TODO(bkirwi): enforce this invariant, either by forcing all runs to be pre-registered or with an assert.
    pub fn enqueue_run(
        &mut self,
        desc: &Description<T>,
        parts: impl IntoIterator<Item = BatchPart<T>>,
    ) {
        let run = parts
            .into_iter()
            .map(|part| {
                let bytes = part.encoded_size_bytes();
                let c_part = match part {
                    BatchPart::Inline {
                        updates,
                        ts_rewrite,
                        ..
                    } => {
                        let part = EncodedPart::from_inline(
                            &*self.metrics,
                            (*self.read_metrics).clone(),
                            desc.clone(),
                            &updates,
                            ts_rewrite.as_ref(),
                        );
                        ConsolidationPart::from_encoded(part, true, &self.metrics.columnar)
                    }
                    part => ConsolidationPart::Queued {
                        data: FetchData::Unfetched {
                            part_desc: desc.clone(),
                            part,
                        },
                    },
                };
                (c_part, bytes)
            })
            .collect();
        self.push_run(run);
    }

    fn push_run(&mut self, run: VecDeque<(ConsolidationPart<T, D>, usize)>) {
        // Normally unconsolidated parts are in their own run, but we can end up with unconsolidated
        // runs if we change our sort order or have bugs, for example. Defend against this by
        // splitting up a run if it contains possibly-unconsolidated parts.
        let wrong_sort = run.iter().any(|(p, _)| match p {
            ConsolidationPart::Queued { data } => data.wrong_sort(),
            ConsolidationPart::Prefetched { wrong_sort, .. } => *wrong_sort,
            ConsolidationPart::Encoded { .. } => false,
        });

        if wrong_sort {
            self.metrics.consolidation.wrong_sort.inc();
        }

        if run.len() > 1 && wrong_sort && self.split_old_runs {
            for part in run {
                self.runs.push(VecDeque::from([part]));
            }
        } else {
            self.runs.push(run);
        }
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
        // If an incompletely-consolidated part has been stashed by the last iterator,
        // push that into state as a new run.
        // One might worry about the list of runs growing indefinitely, if we're adding a new
        // run to the list every iteration... but since this part has the smallest tuples
        // of any run, it should be fully processed by the next consolidation step.
        if let Some(part) = self.drop_stash.take() {
            self.runs.push(VecDeque::from_iter([(
                ConsolidationPart::Encoded {
                    part,
                    cursor: PartIndices::default(),
                },
                0,
            )]));
        }

        if self.runs.is_empty() {
            return None;
        }

        let mut iter = ConsolidatingIter::new(&self.context, &self.filter, &mut self.drop_stash);

        for run in &mut self.runs {
            let last_in_run = run.len() < 2;
            if let Some((part, _)) = run.front_mut() {
                match part {
                    ConsolidationPart::Encoded { part, cursor } => {
                        iter.push(part, cursor, last_in_run);
                    }
                    other @ ConsolidationPart::Queued { .. }
                    | other @ ConsolidationPart::Prefetched { .. } => {
                        // We don't want the iterator to return anything at or above this bound,
                        // since it might require data that we haven't fetched yet.
                        if let Some(bound) = other.kvt_lower() {
                            iter.push_upper(bound);
                        }
                    }
                };
            }
        }

        Some(iter)
    }

    /// We don't need to have fetched every part to make progress, but we do at least need
    /// to have fetched _some_ parts: in particular, parts at the beginning of their runs
    /// which may include the smallest remaining KVT.
    ///
    /// Returns success when we've successfully fetched enough parts to be able to make progress.
    async fn unblock_progress(&mut self) -> anyhow::Result<()> {
        if self.runs.is_empty() {
            return Ok(());
        }
        self.runs
            .sort_by(|a, b| a[0].0.kvt_lower().cmp(&b[0].0.kvt_lower()));

        let run = &self.runs[0];
        let min_lower = run[0].0.kvt_lower();
        let first_larger = self
            .runs
            .iter()
            .position(|q| q[0].0.kvt_lower() > min_lower)
            .unwrap_or(self.runs.len());

        let mut ready_futures: FuturesUnordered<_> = self.runs[0..first_larger]
            .iter_mut()
            .map(|run| async {
                let part = &mut run.front_mut().expect("trimmed run should be nonempty").0;
                match part {
                    ConsolidationPart::Queued { data } => {
                        self.metrics.compaction.parts_waited.inc();
                        self.metrics.consolidation.parts_fetched.inc();
                        let wrong_sort = data.wrong_sort();
                        *part = ConsolidationPart::from_encoded(
                            data.take()
                                .fetch(
                                    self.shard_id,
                                    &*self.blob,
                                    &*self.metrics,
                                    &*self.shard_metrics,
                                    &self.read_metrics,
                                )
                                .await?,
                            wrong_sort,
                            &self.metrics.columnar,
                        );
                    }
                    ConsolidationPart::Prefetched {
                        handle, wrong_sort, ..
                    } => {
                        if handle.is_finished() {
                            self.metrics.compaction.parts_prefetched.inc();
                        } else {
                            self.metrics.compaction.parts_waited.inc()
                        }
                        self.metrics.consolidation.parts_fetched.inc();
                        *part = ConsolidationPart::from_encoded(
                            handle.await??,
                            *wrong_sort,
                            &self.metrics.columnar,
                        );
                    }
                    ConsolidationPart::Encoded { .. } => {}
                }
                Ok::<_, anyhow::Error>(true)
            })
            .collect();

        // Wait for all the needed parts to be fetched, and assert that there's at least one.
        let mut total_ready = 0;
        while let Some(awaited) = ready_futures.next().await {
            if awaited? {
                total_ready += 1;
            }
        }
        assert!(
            total_ready > 0,
            "at least one part should be fetched and ready to go"
        );

        Ok(())
    }

    /// Wait until data is available, then return an iterator over the next
    /// consolidated chunk of output. If this method returns `None`, that all the data has been
    /// exhausted and the full consolidated dataset has been returned.
    pub(crate) async fn next(
        &mut self,
    ) -> anyhow::Result<Option<impl Iterator<Item = (KV, T, D)>>> {
        self.trim();
        self.unblock_progress().await?;
        Ok(self.iter().map(|i| i.map(|(_idx, kv, t, d)| (kv, t, d))))
    }

    /// Wait until data is available, then return an iterator over the next
    /// consolidated chunk of output. If this method returns `None`, that all the data has been
    /// exhausted and the full consolidated dataset has been returned.
    #[allow(dead_code)]
    pub(crate) async fn next_chunk(
        &mut self,
        max_len: usize,
    ) -> anyhow::Result<Option<BlobTraceUpdates>> {
        self.trim();
        self.unblock_progress().await?;
        let chunk = self
            .iter()
            .map(|mut i| i.next_chunk(max_len).updates.clone());
        Ok(chunk)
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
                    ConsolidationPart::Prefetched { .. } | ConsolidationPart::Encoded { .. } => {
                        *size
                    }
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
                if let Some((c_part, size)) = run.get_mut(idx) {
                    let data = match c_part {
                        ConsolidationPart::Queued { data } => {
                            check_budget(*size)?;
                            data.take()
                        }
                        _ => continue,
                    };
                    let key_lower = data.key_lower().to_vec();
                    let wrong_sort = data.wrong_sort();
                    let span = debug_span!("compaction::prefetch");
                    let handle = mz_ore::task::spawn(|| "persist::compaction::prefetch", {
                        let shard_id = self.shard_id;
                        let blob = Arc::clone(&self.blob);
                        let metrics = Arc::clone(&self.metrics);
                        let shard_metrics = Arc::clone(&self.shard_metrics);
                        let read_metrics = Arc::clone(&self.read_metrics);
                        async move {
                            data.fetch(shard_id, &*blob, &*metrics, &*shard_metrics, &*read_metrics)
                                .instrument(span)
                                .await
                        }
                    });
                    *c_part = ConsolidationPart::Prefetched {
                        handle,
                        wrong_sort,
                        key_lower,
                    };
                }
            }
        }

        Some(prefetch_budget_bytes)
    }
}

impl<T, D> Drop for Consolidator<T, D> {
    fn drop(&mut self) {
        for run in &self.runs {
            for (part, _) in run {
                match part {
                    ConsolidationPart::Queued { .. } => {
                        self.metrics.consolidation.parts_skipped.inc();
                    }
                    ConsolidationPart::Prefetched { .. } => {
                        self.metrics.consolidation.parts_wasted.inc();
                    }
                    _ => {}
                }
            }
        }
    }
}

/// A pair of indices, referencing a specific row in a specific part.
/// In the consolidating iterator, this is used to track the coordinates of some part that
/// holds a particular K and V.
type Indices = (usize, usize);

/// This is used as a max-heap entry: the ordering of the fields is important!
#[derive(Debug, Ord, PartialOrd, Eq, PartialEq)]
struct PartRef<'a, T: Timestamp, D> {
    /// The smallest KVT that might be emitted from this run in the future.
    /// This is reverse-sorted: Nones will sort largest (and be popped first on the heap)
    /// and smaller keys will be popped before larger keys.
    next_kvt: Reverse<Option<(KV<'a>, T, D)>>,
    /// The index of the corresponding part within the [ConsolidatingIter]'s list of parts.
    part_index: usize,
    /// The index of the next row within that part.
    /// This is a mutable pointer to long-lived state; we must only advance this index once
    /// we've rolled any rows before this index into our state.
    row_index: &'a mut PartIndices,
    /// Whether / not the iterator for the part is the last in its run, or whether there may be
    /// iterators for the same part in the future.
    last_in_run: bool,
    _phantom: PhantomData<D>,
}

impl<'a, T: Timestamp + Codec64 + Lattice, D: Codec64 + Semigroup> PartRef<'a, T, D> {
    fn update_peek(&mut self, part: &'a Columnar<T, D>, filter: &FetchBatchFilter<T>) {
        let mut peek = part.get(self.row_index.index());
        while let Some((_kv, t, _d)) = &mut peek {
            let keep = filter.filter_ts(t);
            if keep {
                break;
            } else {
                self.row_index.inc();
                peek = part.get(self.row_index.index());
            }
        }
        self.next_kvt = Reverse(peek);
    }

    fn pop(
        &mut self,
        from: &[&'a Columnar<T, D>],
        filter: &FetchBatchFilter<T>,
    ) -> Option<(Indices, KV<'a>, T, D)> {
        let part = &from[self.part_index];
        let Reverse(popped) = mem::take(&mut self.next_kvt);
        let indices = (self.part_index, self.row_index.index());
        self.row_index.inc();
        self.update_peek(part, filter);
        let (kv, t, d) = popped?;
        Some((indices, kv, t, d))
    }
}

#[derive(Debug)]
pub(crate) struct ConsolidatingIter<'a, T: Timestamp + Codec64, D: Codec64> {
    context: &'a str,
    filter: &'a FetchBatchFilter<T>,
    parts: Vec<&'a Columnar<T, D>>,
    heap: BinaryHeap<PartRef<'a, T, D>>,
    upper_bound: Option<(KV<'a>, T)>,
    state: Option<(Indices, KV<'a>, T, D)>,
    drop_stash: &'a mut Option<Columnar<T, D>>,
}

impl<'a, T, D> ConsolidatingIter<'a, T, D>
where
    T: Timestamp + Codec64 + Lattice,
    D: Codec64 + Semigroup + Ord,
{
    fn new(
        context: &'a str,
        filter: &'a FetchBatchFilter<T>,
        drop_stash: &'a mut Option<Columnar<T, D>>,
    ) -> Self {
        Self {
            context,
            filter,
            parts: vec![],
            heap: BinaryHeap::new(),
            upper_bound: None,
            state: None,
            drop_stash,
        }
    }

    fn push(&mut self, iter: &'a Columnar<T, D>, index: &'a mut PartIndices, last_in_run: bool) {
        let mut part_ref = PartRef {
            next_kvt: Reverse(None),
            part_index: self.parts.len(),
            row_index: index,
            last_in_run,
            _phantom: Default::default(),
        };
        part_ref.update_peek(iter, self.filter);
        self.parts.push(iter);
        self.heap.push(part_ref);
    }

    /// Set an upper bound based on the stats from an unfetched part. If there's already
    /// an upper bound set, keep the most conservative / smallest one.
    fn push_upper(&mut self, upper: (KV<'a>, T)) {
        let update_bound = self
            .upper_bound
            .as_ref()
            .map_or(true, |existing| *existing > upper);
        if update_bound {
            self.upper_bound = Some(upper);
        }
    }

    /// Attempt to consolidate as much into the current state as possible.
    fn consolidate(&mut self) -> Option<(Indices, KV<'a>, T, D)> {
        loop {
            let Some(mut part) = self.heap.peek_mut() else {
                break;
            };
            if let Some((kv1, t1, _)) = part.next_kvt.0.as_ref() {
                if let Some((idx0, kv0, t0, d0)) = &mut self.state {
                    let consolidates = match (*kv0, &*t0).cmp(&(*kv1, t1)) {
                        Ordering::Less => false,
                        Ordering::Equal => true,
                        Ordering::Greater => {
                            // Don't want to log the entire KV, but it's interesting to know
                            // whether it's KVs going backwards or 'just' timestamps.
                            panic!(
                                "data arrived at the consolidator out of order ({}, kvs equal? {}, {t0:?}, {t1:?})",
                                self.context,
                                (*kv0) == (*kv1)
                            );
                        }
                    };
                    if consolidates {
                        let (idx1, _, _, d1) = part
                            .pop(&self.parts, self.filter)
                            .expect("popping from a non-empty iterator");
                        d0.plus_equals(&d1);
                        *idx0 = idx1;
                    } else {
                        break;
                    }
                } else {
                    // Don't start consolidating a new KVT that's past our provided upper bound,
                    // since that data may also live in some unfetched part.
                    if let Some((kv0, t0)) = &self.upper_bound {
                        if (kv0, t0) <= (kv1, t1) {
                            return None;
                        }
                    }

                    self.state = part.pop(&self.parts, self.filter);
                }
            } else {
                if part.last_in_run {
                    PeekMut::pop(part);
                } else {
                    // There may be more instances of the KVT in a later part of the same run;
                    // exit without returning the current state.
                    return None;
                }
            }
        }

        self.state.take()
    }

    fn next_chunk(&mut self, max_len: usize) -> Columnar<T, D> {
        let mut indices = vec![];
        let mut timestamps = Int64Builder::new();
        let mut diffs = Int64Builder::new();
        while let Some((idx, _, t, d)) = self.next() {
            indices.push(idx);
            timestamps.append_value(i64::from_le_bytes(T::encode(&t)));
            diffs.append_value(i64::from_le_bytes(D::encode(&d)));
            if indices.len() >= max_len {
                break;
            }
        }

        Columnar::interleave(
            &self.parts,
            indices.as_slice(),
            Int64Builder::finish(&mut timestamps),
            Int64Builder::finish(&mut diffs),
        )
    }
}

impl<'a, T, D> Iterator for ConsolidatingIter<'a, T, D>
where
    T: Timestamp + Codec64 + Lattice,
    D: Codec64 + Semigroup + Ord,
{
    type Item = (Indices, KV<'a>, T, D);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            match self.consolidate() {
                Some((_, _, _, d)) if d.is_zero() => continue,
                other => break other,
            }
        }
    }
}

impl<'a, T: Timestamp + Codec64, D: Codec64> Drop for ConsolidatingIter<'a, T, D> {
    fn drop(&mut self) {
        // Make sure to stash any incomplete state in a place where we'll pick it up on the next run.
        // See the comment on `Consolidator` for more on why this is necessary.
        if let Some((idx, _, t, d)) = self.state.take() {
            let part = Columnar::interleave(
                &self.parts,
                &[idx],
                vec![i64::from_le_bytes(t.encode())].into(),
                vec![i64::from_le_bytes(d.encode())].into(),
            );
            *self.drop_stash = Some(part);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Arc;

    use differential_dataflow::consolidation::consolidate_updates;
    use differential_dataflow::trace::Description;
    use mz_ore::metrics::MetricsRegistry;
    use mz_persist::indexed::columnar::ColumnarRecordsBuilder;
    use mz_persist::indexed::encoding::{BlobTraceBatchPart, BlobTraceUpdates};
    use mz_persist::location::Blob;
    use mz_persist::mem::{MemBlob, MemBlobConfig};
    use proptest::collection::vec;
    use proptest::prelude::*;
    use timely::progress::Antichain;

    use crate::cfg::PersistConfig;
    use crate::internal::paths::PartialBatchKey;
    use crate::internal::state::HollowBatchPart;
    use crate::metrics::Metrics;
    use crate::ShardId;

    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // too slow
    fn consolidation() {
        // Check that output consolidated via this logic matches output consolidated via timely's!
        type Part = Vec<((Vec<u8>, Vec<u8>), u64, i64)>;

        fn check(metrics: &Arc<Metrics>, parts: Vec<(Part, usize)>) {
            let original = {
                let mut rows = parts
                    .iter()
                    .flat_map(|(p, _)| p.clone())
                    .collect::<Vec<_>>();
                consolidate_updates(&mut rows);
                rows
            };
            let filter = FetchBatchFilter::Compaction {
                since: Antichain::from_elem(0),
            };
            let desc = Description::new(
                Antichain::from_elem(0),
                Antichain::new(),
                Antichain::from_elem(0),
            );
            let streaming = {
                // Toy compaction loop!
                let mut consolidator = Consolidator {
                    context: "test".to_string(),
                    shard_id: ShardId::new(),
                    blob: Arc::new(MemBlob::open(MemBlobConfig::default())),
                    metrics: Arc::clone(metrics),
                    shard_metrics: metrics.shards.shard(&ShardId::new(), "test"),
                    read_metrics: Arc::new(metrics.read.snapshot.clone()),
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
                                    let mut records = ColumnarRecordsBuilder::default();
                                    for ((k, v), t, d) in &part {
                                        assert!(records.push((
                                            (k, v),
                                            u64::encode(t),
                                            i64::encode(d)
                                        )));
                                    }
                                    let part = EncodedPart::new(
                                        metrics.read.snapshot.clone(),
                                        desc.clone(),
                                        "part",
                                        None,
                                        BlobTraceBatchPart {
                                            desc: desc.clone(),
                                            index: 0,
                                            updates: BlobTraceUpdates::Row(
                                                records.finish(&metrics.columnar),
                                            ),
                                        },
                                    );
                                    (
                                        ConsolidationPart::from_encoded(
                                            part,
                                            true,
                                            &metrics.columnar,
                                        ),
                                        0,
                                    )
                                })
                                .collect::<VecDeque<_>>()
                        })
                        .collect::<Vec<_>>(),
                    filter,
                    budget: 0,
                    split_old_runs: true,
                    drop_stash: None,
                };

                let mut out = vec![];
                loop {
                    consolidator.trim();
                    let Some(iter) = consolidator.iter() else {
                        break;
                    };
                    out.extend(iter.map(|(_, (k, v), t, d)| ((k.to_vec(), v.to_vec()), t, d)));
                }
                out
            };

            assert_eq!(original, streaming);
        }

        let metrics = Arc::new(Metrics::new(
            &PersistConfig::new_for_tests(),
            &MetricsRegistry::new(),
        ));

        // Restricting the ranges to help make sure we have frequent collisions
        let key_gen = (0..4usize).prop_map(|i| i.to_string().into_bytes()).boxed();
        let part_gen = vec(
            ((key_gen.clone(), key_gen.clone()), 0..10u64, -3..=3i64),
            0..10,
        );
        let run_gen = vec((part_gen, 0..10usize), 0..5);
        proptest!(|(state in run_gen)| {
            check(&metrics, state)
        });
    }

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)] // unsupported operation: returning ready events from epoll_wait is not yet implemented
    async fn prefetches() {
        fn check(budget: usize, runs: Vec<Vec<usize>>, prefetch_all: bool) {
            let desc = Description::new(
                Antichain::from_elem(0u64),
                Antichain::new(),
                Antichain::from_elem(0u64),
            );

            let total_size: usize = runs.iter().flat_map(|run| run.iter().map(|p| *p)).sum();

            let shard_id = ShardId::new();
            let blob: Arc<dyn Blob> = Arc::new(MemBlob::open(MemBlobConfig::default()));
            let metrics = Arc::new(Metrics::new(
                &PersistConfig::new_for_tests(),
                &MetricsRegistry::new(),
            ));
            let shard_metrics = metrics.shards.shard(&shard_id, "");

            let mut consolidator: Consolidator<u64, i64> = Consolidator::new(
                "test".to_string(),
                shard_id,
                blob,
                Arc::clone(&metrics),
                shard_metrics,
                metrics.read.batch_fetcher.clone(),
                FetchBatchFilter::Compaction {
                    since: desc.since().clone(),
                },
                budget,
                false,
            );

            for run in runs {
                let parts: Vec<_> = run
                    .into_iter()
                    .map(|encoded_size_bytes| {
                        BatchPart::Hollow(HollowBatchPart {
                            key: PartialBatchKey(
                                "n0000000/p00000000-0000-0000-0000-000000000000".into(),
                            ),
                            encoded_size_bytes,
                            key_lower: vec![],
                            stats: None,
                            ts_rewrite: None,
                            diffs_sum: None,
                            format: None,
                            schema_id: None,
                        })
                    })
                    .collect();
                consolidator.enqueue_run(&desc, parts)
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

            if prefetch_all {
                // If we up the budget to match the total size, we should prefetch everything.
                consolidator.budget = total_size;
                assert!(consolidator.start_prefetches() == Some(0));
            } else {
                // Let the consolidator drop without fetching everything to check the Drop
                // impl works when not all parts are prefetched.
            }
        }

        let run_gen = vec(vec(0..20usize, 0..5usize), 0..5usize);
        proptest!(|(budget in 0..20usize, state in run_gen, prefetch_all in any::<bool>())| {
            check(budget, state, prefetch_all)
        });
    }
}

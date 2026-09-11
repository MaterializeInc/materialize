// Copyright Materialize, Inc. and contributors. All rights reserved.
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

//! Columnar storage operations for native Differential's chunk machinery.
//!
//! [`PoolChunk`] adapts the existing synchronous columnar kernels. Its poll methods
//! first load any required pool bodies through [`ReadState`], then call those kernels
//! on the worker. Disjoint ranges can pass through using resident metadata.
//! Differential retains the merge phase and queues across each pending call.

use std::collections::VecDeque;
use std::rc::Rc;
use std::sync::Arc;
use std::task::{Context, Poll, ready};

use columnar::Columnar;
use differential_dataflow::difference::Semigroup;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::trace::chunk::Chunk as MzChunk;
use differential_dataflow_next::trace::chunk::{Chunk as NativeChunk, asynchronous::AsyncChunk};
use futures_util::FutureExt;
use futures_util::future::LocalBoxFuture;
use timely::dataflow::channels::ContainerBytes;
use timely::progress::Timestamp;
use tokio::sync::OwnedSemaphorePermit;

use super::super::merge::ReadBudget;
use super::super::{Column, ColumnChunk};
use super::timestamp::{Time, to_mz, to_native};

/// A columnar chunk with shared admission for native Differential merge reads.
pub struct PoolChunk<D: Columnar, T: Columnar, R: Columnar> {
    pub(super) chunk: ColumnChunk<D, T, R>,
    pub(super) budget: ReadBudget,
}

impl<D: Columnar, T: Columnar, R: Columnar> PoolChunk<D, T, R> {
    fn take_columns(chunks: &mut VecDeque<Self>) -> VecDeque<ColumnChunk<D, T, R>> {
        chunks.drain(..).map(|chunk| chunk.chunk).collect()
    }

    fn extend_columns(
        output: &mut VecDeque<Self>,
        columns: VecDeque<ColumnChunk<D, T, R>>,
        budget: &ReadBudget,
    ) {
        output.extend(columns.into_iter().map(|chunk| Self {
            chunk,
            budget: budget.clone(),
        }));
    }
}

impl<D: Columnar, T: Columnar, R: Columnar> Clone for PoolChunk<D, T, R> {
    fn clone(&self) -> Self {
        Self {
            chunk: self.chunk.clone(),
            budget: self.budget.clone(),
        }
    }
}

impl<D: Columnar, T: Columnar, R: Columnar> Default for PoolChunk<D, T, R> {
    fn default() -> Self {
        Self {
            chunk: ColumnChunk::Resident(Rc::new(Column::default()), 0),
            // DD uses Default to replace a chunk after moving it into a run.
            // This empty placeholder is never read and carries no input admission.
            budget: ReadBudget::new(1),
        }
    }
}

/// Pending reads for a native merge or extraction.
///
/// Admission covers one chunk operation's decoded inputs and is released after
/// its kernel consumes them. Keeping admission across fuel turns can deadlock:
/// a paused merger can hold the bytes needed by the next merger in the spine.
/// Typed output and compaction carry are outside this input budget.
pub struct ReadState<D: Columnar, T: Columnar, R: Columnar> {
    budget: Option<ReadBudget>,
    required_bytes: usize,
    pending: Option<LocalBoxFuture<'static, LoadedInputs<D, T, R>>>,
}

impl<D: Columnar, T: Columnar, R: Columnar> Default for ReadState<D, T, R> {
    fn default() -> Self {
        Self {
            budget: None,
            required_bytes: 0,
            pending: None,
        }
    }
}

// Field order releases abandoned buffers before their admission.
struct LoadedInputs<D: Columnar, T: Columnar, R: Columnar> {
    chunks: Vec<ColumnChunk<D, T, R>>,
    _reservation: Arc<OwnedSemaphorePermit>,
}

impl<D, T, R> ReadState<D, T, R>
where
    D: Columnar + 'static,
    T: Columnar + 'static,
    R: Columnar + 'static,
{
    /// Load the supplied chunks in order, resuming the same read after `Pending`.
    ///
    /// The caller must keep the inputs unchanged until this returns `Ready`.
    fn poll_load(
        &mut self,
        chunks: Vec<ColumnChunk<D, T, R>>,
        cx: &mut Context<'_>,
    ) -> Poll<LoadedInputs<D, T, R>> {
        if self.pending.is_none() {
            let budget = self.budget.clone().expect("operation has input admission");
            self.pending =
                Some(Self::load_inputs(chunks, budget, self.required_bytes).boxed_local());
        }
        let loaded = ready!(self.pending.as_mut().unwrap().as_mut().poll(cx));
        self.pending = None;
        Poll::Ready(loaded)
    }

    async fn load_inputs(
        chunks: Vec<ColumnChunk<D, T, R>>,
        budget: ReadBudget,
        required_bytes: usize,
    ) -> LoadedInputs<D, T, R> {
        let reservation = budget.reserve(required_bytes).await;
        let reads = chunks
            .into_iter()
            .map(|chunk| Self::load_chunk(chunk, Arc::clone(&reservation)));
        let chunks = futures_util::future::join_all(reads).await;
        LoadedInputs {
            chunks,
            _reservation: reservation,
        }
    }

    async fn load_chunk(
        chunk: ColumnChunk<D, T, R>,
        reservation: Arc<OwnedSemaphorePermit>,
    ) -> ColumnChunk<D, T, R> {
        match chunk {
            ColumnChunk::Spilled(body, depth) => {
                let handle = Arc::clone(&body.handle);
                // NOTE: Dropping the operator cannot cancel a submitted blocking
                // read. This owned task keeps input admission until that read ends.
                let task = mz_ore::task::spawn(|| "native_merge_read", async move {
                    let words = handle.read_async().await;
                    (words, reservation)
                });
                let (words, _reservation) = task.await;
                ColumnChunk::Resident(Rc::new(Column::Align(words)), depth)
            }
            chunk @ ColumnChunk::Resident(..) => chunk,
        }
    }
}

impl<D, T, R> NativeChunk for PoolChunk<D, T, R>
where
    D: Columnar + 'static,
    for<'a> columnar::Ref<'a, D>: Copy + Ord,
    T: Columnar + Default + Timestamp + Lattice + Ord,
    for<'a> columnar::Ref<'a, T>: Copy + Ord,
    R: Columnar + Default + Semigroup + for<'a> Semigroup<columnar::Ref<'a, R>> + 'static,
{
    type Time = Time<T>;
    const TARGET: usize = <ColumnChunk<D, T, R> as MzChunk>::TARGET;

    fn len(&self) -> usize {
        self.chunk.len()
    }

    fn merge(left: &mut VecDeque<Self>, right: &mut VecDeque<Self>, output: &mut VecDeque<Self>) {
        let budget = left
            .front()
            .or_else(|| right.front())
            .unwrap()
            .budget
            .clone();
        let mut left_columns = Self::take_columns(left);
        let mut right_columns = Self::take_columns(right);
        let mut merged = VecDeque::new();
        ColumnChunk::merge(&mut left_columns, &mut right_columns, &mut merged);
        Self::extend_columns(left, left_columns, &budget);
        Self::extend_columns(right, right_columns, &budget);
        Self::extend_columns(output, merged, &budget);
    }

    fn advance(
        input: &mut VecDeque<Self>,
        frontier: timely_next::progress::frontier::AntichainRef<Self::Time>,
        done: bool,
        output: &mut VecDeque<Self>,
    ) {
        let Some(budget) = input.front().map(|chunk| chunk.budget.clone()) else {
            return;
        };
        let mut columns = Self::take_columns(input);
        let mut advanced = VecDeque::new();
        let frontier = to_mz(frontier);
        ColumnChunk::advance(&mut columns, frontier.borrow(), done, &mut advanced);
        Self::extend_columns(input, columns, &budget);
        Self::extend_columns(output, advanced, &budget);
    }

    fn extract(
        input: &mut VecDeque<Self>,
        frontier: timely_next::progress::frontier::AntichainRef<Self::Time>,
        residual: &mut timely_next::progress::Antichain<Self::Time>,
        keep: &mut VecDeque<Self>,
        ship: &mut VecDeque<Self>,
    ) {
        let Some(budget) = input.front().map(|chunk| chunk.budget.clone()) else {
            return;
        };
        let mut columns = Self::take_columns(input);
        let mut kept = VecDeque::new();
        let mut shipped = VecDeque::new();
        let frontier = to_mz(frontier);
        let mut remaining = to_mz(residual.borrow());
        ColumnChunk::extract(
            &mut columns,
            frontier.borrow(),
            &mut remaining,
            &mut kept,
            &mut shipped,
        );
        *residual = to_native(remaining.borrow());
        Self::extend_columns(input, columns, &budget);
        Self::extend_columns(keep, kept, &budget);
        Self::extend_columns(ship, shipped, &budget);
    }

    fn settle(input: &mut VecDeque<Self>, done: bool, output: &mut VecDeque<Self>) {
        let Some(budget) = input.front().map(|chunk| chunk.budget.clone()) else {
            return;
        };
        let mut columns = Self::take_columns(input);
        let mut settled = VecDeque::new();
        ColumnChunk::settle(&mut columns, done, &mut settled);
        Self::extend_columns(input, columns, &budget);
        Self::extend_columns(output, settled, &budget);
    }
}

impl<D, T, R> AsyncChunk for PoolChunk<D, T, R>
where
    D: Columnar + 'static,
    for<'a> columnar::Ref<'a, D>: Copy + Ord,
    T: Columnar + Default + Timestamp + Lattice + Ord,
    for<'a> columnar::Ref<'a, T>: Copy + Ord,
    R: Columnar + Default + Semigroup + for<'a> Semigroup<columnar::Ref<'a, R>> + 'static,
{
    type Pending = ReadState<D, T, R>;

    fn pending_for(left: &[Self], right: &[Self]) -> Self::Pending {
        let largest_chunk_bytes = |run: &[Self]| {
            run.iter()
                .map(|chunk| match &chunk.chunk {
                    ColumnChunk::Resident(column, _) => column.length_in_bytes(),
                    ColumnChunk::Spilled(body, _) => body.bytes,
                })
                .max()
                .unwrap_or(0)
        };
        ReadState {
            budget: left
                .first()
                .or_else(|| right.first())
                .map(|chunk| chunk.budget.clone()),
            required_bytes: largest_chunk_bytes(left).saturating_add(largest_chunk_bytes(right)),
            ..ReadState::default()
        }
    }

    fn poll_merge(
        io: &mut Self::Pending,
        cx: &mut Context<'_>,
        left: &mut VecDeque<Self>,
        right: &mut VecDeque<Self>,
        output: &mut VecDeque<Self>,
    ) -> Poll<()> {
        let input_rows =
            left.front().unwrap().chunk.records() + right.front().unwrap().chunk.records();
        let (left_first, left_last) = left.front().unwrap().chunk.data_span();
        let (right_first, right_last) = right.front().unwrap().chunk.data_span();
        let disjoint = if super::super::rr::<D>(left_last) < super::super::rr::<D>(right_first) {
            Some(&mut *left)
        } else if super::super::rr::<D>(right_last) < super::super::rr::<D>(left_first) {
            Some(&mut *right)
        } else {
            None
        };
        if let Some(disjoint) = disjoint {
            super::super::metrics::record(super::super::metrics::Stage::Merge, input_rows, 0);
            // The synchronous survivor path may migrate codecs by reading the body.
            // Keep this metadata-only path cold, while retaining merge-depth accounting.
            let mut chunk = disjoint.pop_front().unwrap();
            match &mut chunk.chunk {
                ColumnChunk::Resident(_, depth) | ColumnChunk::Spilled(_, depth) => {
                    *depth = depth.saturating_add(1)
                }
            }
            output.push_back(chunk);
            return Poll::Ready(());
        }
        let mut loaded = ready!(io.poll_load(
            vec![
                left.front().unwrap().chunk.clone(),
                right.front().unwrap().chunk.clone()
            ],
            cx
        ));
        let mut chunks = loaded.chunks.drain(..);
        let decoded = [chunks.next().unwrap(), chunks.next().unwrap()];
        drop(chunks);
        let budget = left.front().unwrap().budget.clone();
        let mut left_columns = Self::take_columns(left);
        let mut right_columns = Self::take_columns(right);
        let mut merged = VecDeque::new();
        ColumnChunk::merge_with_loaded(
            &mut left_columns,
            &mut right_columns,
            &mut merged,
            Some(decoded),
        );
        Self::extend_columns(left, left_columns, &budget);
        Self::extend_columns(right, right_columns, &budget);
        Self::extend_columns(output, merged, &budget);
        drop(loaded);
        Poll::Ready(())
    }

    fn poll_advance(
        io: &mut Self::Pending,
        cx: &mut Context<'_>,
        input: &mut VecDeque<Self>,
        frontier: timely_next::progress::frontier::AntichainRef<Self::Time>,
        done: bool,
        out: &mut VecDeque<Self>,
    ) -> Poll<()> {
        let read = if input
            .iter()
            .any(|c| matches!(c.chunk, ColumnChunk::Spilled(..)))
        {
            let mut loaded =
                ready!(io.poll_load(input.iter().map(|c| c.chunk.clone()).collect(), cx));
            assert_eq!(input.len(), loaded.chunks.len());
            for (index, chunk) in loaded.chunks.drain(..).enumerate() {
                input[index].chunk = chunk;
            }
            Some(loaded)
        } else {
            None
        };
        Self::advance(input, frontier, done, out);
        drop(read);
        Poll::Ready(())
    }

    fn poll_extract(
        io: &mut Self::Pending,
        cx: &mut Context<'_>,
        input: &mut VecDeque<Self>,
        frontier: timely_next::progress::frontier::AntichainRef<Self::Time>,
        residual: &mut timely_next::progress::Antichain<Self::Time>,
        keep: &mut VecDeque<Self>,
        ship: &mut VecDeque<Self>,
    ) -> Poll<()> {
        let mut read = None;
        if let Some(chunk) = input.front() {
            let (low, high) = chunk.chunk.chunk_time_bounds();
            let upper = to_mz(frontier);
            let entirely_before = high.iter().all(|time| !upper.less_equal(time));
            let entirely_after = low.iter().all(|time| upper.less_equal(time));
            let straddles_frontier = !entirely_before && !entirely_after;
            if straddles_frontier && matches!(chunk.chunk, ColumnChunk::Spilled(..)) {
                let mut loaded = ready!(io.poll_load(vec![chunk.chunk.clone()], cx));
                input.front_mut().unwrap().chunk = loaded.chunks.pop().unwrap();
                read = Some(loaded);
            }
        }
        Self::extract(input, frontier, residual, keep, ship);
        drop(read);
        Poll::Ready(())
    }
}

/// Feeds already sorted chunks to Differential's batcher without sorting them again.
pub struct PreparedChunker<D: Columnar, T: Columnar, R: Columnar> {
    queued: VecDeque<PoolChunk<D, T, R>>,
    ready: PoolChunk<D, T, R>,
}

impl<D: Columnar, T: Columnar, R: Columnar> Default for PreparedChunker<D, T, R> {
    fn default() -> Self {
        Self {
            queued: VecDeque::new(),
            ready: PoolChunk::default(),
        }
    }
}

impl<D: Columnar, T: Columnar, R: Columnar> timely_next::container::ContainerBuilder
    for PreparedChunker<D, T, R>
{
    type Container = PoolChunk<D, T, R>;

    fn extract(&mut self) -> Option<&mut Self::Container> {
        self.ready = self.queued.pop_front()?;
        Some(&mut self.ready)
    }

    fn finish(&mut self) -> Option<&mut Self::Container> {
        self.extract()
    }
}

impl<D: Columnar, T: Columnar, R: Columnar>
    timely_next::container::PushInto<&mut Vec<PoolChunk<D, T, R>>> for PreparedChunker<D, T, R>
{
    fn push_into(&mut self, input: &mut Vec<PoolChunk<D, T, R>>) {
        self.queued.extend(input.drain(..));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use differential_dataflow_next::trace::asynchronous::{MergeStatus, Merger};
    use differential_dataflow_next::trace::chunk::ChunkBatch;
    use differential_dataflow_next::trace::chunk::asynchronous::ChunkBatchMerger;
    use mz_ore::pool::Pool;
    use timely::container::PushInto;

    type Update = ((u64, Vec<u8>), u64, i64);
    type TestChunk = PoolChunk<(u64, Vec<u8>), u64, i64>;

    fn chunk(updates: &[Update], pool: &Pool, budget: &ReadBudget) -> TestChunk {
        let mut column = Column::default();
        for update in updates {
            column.push_into(update);
        }
        TestChunk {
            chunk: ColumnChunk::spill_body(column, pool, 1),
            budget: budget.clone(),
        }
    }

    #[mz_ore::test]
    fn a_fuel_yield_does_not_hold_read_admission() {
        let pool = Pool::new().unwrap();
        pool.set_spill_threads(0);
        pool.set_budget(0);
        tokio::runtime::Runtime::new().unwrap().block_on(async {
            let budget = ReadBudget::new(1 << 20);
            let batch = |offset: u64| {
                ChunkBatch::new(
                    (0..4)
                        .map(|part| {
                            let rows: Vec<_> = (0..8)
                                .map(|row| ((part * 16 + row * 2 + offset, vec![7; 128]), 0, 1))
                                .collect();
                            chunk(&rows, &pool, &budget)
                        })
                        .collect(),
                )
            };
            let left = batch(0);
            let right = batch(1);
            let frontier = timely_next::progress::Antichain::from_elem(Time(0));
            let mut merger = ChunkBatchMerger::new(&left, &right, frontier.borrow());
            let mut fuel = 1;
            let status =
                futures_util::future::poll_fn(|cx| merger.poll_work(&left, &right, cx, &mut fuel))
                    .await;
            assert!(matches!(status, MergeStatus::InProgress));
            assert!(pool.stats().async_reads > 0);
            assert_eq!(
                budget.reserved_bytes(),
                0,
                "another merger must be able to acquire admission before this one resumes"
            );
        });
    }

    #[mz_ore::test]
    fn loaded_merge_restores_an_untouched_pool_body() {
        let pool = Pool::new().unwrap();
        pool.set_spill_threads(0);
        pool.set_budget(0);
        tokio::runtime::Runtime::new().unwrap().block_on(async {
            let budget = ReadBudget::new(1 << 20);
            // Equal data spans require inspecting times. Only the left input is
            // consumed, so the right input must retain its original pool handle.
            let mut left = VecDeque::from([chunk(&[((1, vec![7; 128]), 0, 1)], &pool, &budget)]);
            let mut right = VecDeque::from([chunk(&[((1, vec![7; 128]), 1, 1)], &pool, &budget)]);
            let ColumnChunk::Spilled(original, _) = right[0].chunk.clone() else {
                unreachable!();
            };
            let mut io = TestChunk::pending_for(left.make_contiguous(), right.make_contiguous());
            let mut output = VecDeque::new();
            futures_util::future::poll_fn(|cx| {
                TestChunk::poll_merge(&mut io, cx, &mut left, &mut right, &mut output)
            })
            .await;
            assert!(left.is_empty());
            assert_eq!(output.iter().map(NativeChunk::len).sum::<usize>(), 1);
            let ColumnChunk::Spilled(survivor, depth) = &right[0].chunk else {
                panic!("untouched input must not retain a decoded read buffer");
            };
            assert!(Rc::ptr_eq(&original, survivor));
            assert_eq!(*depth, 2);
            assert_eq!(budget.reserved_bytes(), 0);
        });
    }

    #[mz_ore::test]
    fn trace_callbacks_leave_reads_to_the_owning_driver() {
        use differential_dataflow::trace::{Description, Trace, TraceReader};
        let pool = Pool::new().unwrap();
        pool.set_spill_threads(0);
        pool.set_budget(0);
        tokio::runtime::Runtime::new().unwrap().block_on(async {
            let budget = ReadBudget::new(1 << 20);
            let info = timely::dataflow::operators::generic::OperatorInfo::new(0, 0, Rc::from([0]));
            let mut trace = super::super::Spine::with_budget(info, budget.clone());
            for time in 0..3 {
                trace.insert(Rc::new(differential_dataflow::trace::chunk::ChunkBatch {
                    chunks: vec![chunk(&[((1, vec![7; 128]), time, 1)], &pool, &budget).chunk],
                    description: Description::new(
                        timely::progress::Antichain::from_elem(time),
                        timely::progress::Antichain::from_elem(time + 1),
                        timely::progress::Antichain::from_elem(0),
                    ),
                }));
            }
            let frontier = timely::progress::Antichain::from_elem(3);
            trace.set_logical_compaction(frontier.borrow());
            trace.set_physical_compaction(frontier.borrow());
            trace.exert();
            assert_eq!(pool.stats().async_reads, 0);
            assert_eq!(
                budget.reserved_bytes(),
                0,
                "callbacks must not acquire admission while the arranger can be awaiting its batcher"
            );
            super::super::maintain(&trace.state, &trace.notify).await;
            assert!(pool.stats().async_reads > 0);
            assert_eq!(budget.reserved_bytes(), 0);
        });
    }
}

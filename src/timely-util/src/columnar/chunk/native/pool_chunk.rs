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

use super::vendor;
use columnar::Columnar;
use differential_dataflow::difference::Semigroup;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::trace::chunk::Chunk as MzChunk;
use futures_util::FutureExt;
use futures_util::future::LocalBoxFuture;
use timely::dataflow::channels::ContainerBytes;
use timely::progress::Timestamp;
use tokio::sync::OwnedSemaphorePermit;

use super::super::merge::ReadBudget;
use super::super::{Column, ColumnChunk};

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

/// Read state retained for one native merge or extraction, across all its chunk operations.
///
/// The reservation lasts until this state is dropped. It covers the largest input
/// pair in the original runs, so progressing to another pair never requires more
/// admission while retaining an earlier reservation. Output and compaction carry
/// are outside this input budget.
pub struct ReadState<D: Columnar, T: Columnar, R: Columnar> {
    budget: Option<ReadBudget>,
    required_bytes: usize,
    reservation: Option<Arc<OwnedSemaphorePermit>>,
    pending: Option<LocalBoxFuture<'static, LoadedInputs<D, T, R>>>,
}
impl<D: Columnar, T: Columnar, R: Columnar> Default for ReadState<D, T, R> {
    fn default() -> Self {
        Self {
            budget: None,
            required_bytes: 0,
            reservation: None,
            pending: None,
        }
    }
}
struct LoadedInputs<D: Columnar, T: Columnar, R: Columnar> {
    chunks: Vec<ColumnChunk<D, T, R>>,
    reservation: Arc<OwnedSemaphorePermit>,
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
    ) -> Poll<Vec<ColumnChunk<D, T, R>>> {
        if self.pending.is_none() {
            let budget = self.budget.clone().expect("operation has input admission");
            self.pending = Some(
                Self::load_inputs(
                    chunks,
                    budget,
                    self.required_bytes,
                    self.reservation.clone(),
                )
                .boxed_local(),
            );
        }
        let loaded = ready!(self.pending.as_mut().unwrap().as_mut().poll(cx));
        self.pending = None;
        self.reservation = Some(loaded.reservation);
        Poll::Ready(loaded.chunks)
    }

    async fn load_inputs(
        chunks: Vec<ColumnChunk<D, T, R>>,
        budget: ReadBudget,
        required_bytes: usize,
        reservation: Option<Arc<OwnedSemaphorePermit>>,
    ) -> LoadedInputs<D, T, R> {
        let reservation = match reservation {
            Some(reservation) => reservation,
            None => budget.reserve(required_bytes).await,
        };
        let reads = chunks
            .into_iter()
            .map(|chunk| Self::load_chunk(chunk, Arc::clone(&reservation)));
        let chunks = futures_util::future::join_all(reads).await;
        LoadedInputs {
            chunks,
            reservation,
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

impl<D, T, R> vendor::chunk::Chunk for PoolChunk<D, T, R>
where
    D: Columnar + 'static,
    for<'a> columnar::Ref<'a, D>: Copy + Ord,
    T: Columnar + Default + Timestamp + Lattice + Ord,
    for<'a> columnar::Ref<'a, T>: Copy + Ord,
    R: Columnar + Default + Semigroup + for<'a> Semigroup<columnar::Ref<'a, R>> + 'static,
{
    type Time = T;
    type Pending = ReadState<D, T, R>;
    const TARGET: usize = <ColumnChunk<D, T, R> as MzChunk>::TARGET;
    fn len(&self) -> usize {
        self.chunk.len()
    }
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
        frontier: timely::progress::frontier::AntichainRef<Self::Time>,
        done: bool,
        output: &mut VecDeque<Self>,
    ) {
        let Some(budget) = input.front().map(|chunk| chunk.budget.clone()) else {
            return;
        };
        let mut columns = Self::take_columns(input);
        let mut advanced = VecDeque::new();
        let frontier = frontier.to_owned();
        ColumnChunk::advance(&mut columns, frontier.borrow(), done, &mut advanced);
        Self::extend_columns(input, columns, &budget);
        Self::extend_columns(output, advanced, &budget);
    }

    fn extract(
        input: &mut VecDeque<Self>,
        frontier: timely::progress::frontier::AntichainRef<Self::Time>,
        residual: &mut timely::progress::Antichain<Self::Time>,
        keep: &mut VecDeque<Self>,
        ship: &mut VecDeque<Self>,
    ) {
        let Some(budget) = input.front().map(|chunk| chunk.budget.clone()) else {
            return;
        };
        let mut columns = Self::take_columns(input);
        let mut kept = VecDeque::new();
        let mut shipped = VecDeque::new();
        let frontier = frontier.to_owned();
        let mut remaining = residual.clone();
        ColumnChunk::extract(
            &mut columns,
            frontier.borrow(),
            &mut remaining,
            &mut kept,
            &mut shipped,
        );
        *residual = remaining;
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

    fn poll_merge(
        io: &mut Self::Pending,
        cx: &mut Context<'_>,
        left: &mut VecDeque<Self>,
        right: &mut VecDeque<Self>,
        output: &mut VecDeque<Self>,
    ) -> Poll<()> {
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
        let loaded = ready!(io.poll_load(
            vec![
                left.front().unwrap().chunk.clone(),
                right.front().unwrap().chunk.clone()
            ],
            cx
        ));
        let mut loaded = loaded.into_iter();
        left.front_mut().unwrap().chunk = loaded.next().unwrap();
        right.front_mut().unwrap().chunk = loaded.next().unwrap();
        Self::merge(left, right, output);
        Poll::Ready(())
    }
    fn poll_advance(
        io: &mut Self::Pending,
        cx: &mut Context<'_>,
        input: &mut VecDeque<Self>,
        frontier: timely::progress::frontier::AntichainRef<Self::Time>,
        done: bool,
        out: &mut VecDeque<Self>,
    ) -> Poll<()> {
        if input
            .iter()
            .any(|c| matches!(c.chunk, ColumnChunk::Spilled(..)))
        {
            let loaded = ready!(io.poll_load(input.iter().map(|c| c.chunk.clone()).collect(), cx));
            assert_eq!(input.len(), loaded.len());
            for (index, chunk) in loaded.into_iter().enumerate() {
                input[index].chunk = chunk;
            }
        }
        Self::advance(input, frontier, done, out);
        Poll::Ready(())
    }
    fn poll_extract(
        io: &mut Self::Pending,
        cx: &mut Context<'_>,
        input: &mut VecDeque<Self>,
        frontier: timely::progress::frontier::AntichainRef<Self::Time>,
        residual: &mut timely::progress::Antichain<Self::Time>,
        keep: &mut VecDeque<Self>,
        ship: &mut VecDeque<Self>,
    ) -> Poll<()> {
        if let Some(chunk) = input.front() {
            let (low, high) = chunk.chunk.chunk_time_bounds();
            let upper = frontier.to_owned();
            let entirely_before = high.iter().all(|time| !upper.less_equal(time));
            let entirely_after = low.iter().all(|time| upper.less_equal(time));
            let straddles_frontier = !entirely_before && !entirely_after;
            if straddles_frontier && matches!(chunk.chunk, ColumnChunk::Spilled(..)) {
                let loaded = ready!(io.poll_load(vec![chunk.chunk.clone()], cx));
                input.front_mut().unwrap().chunk = loaded.into_iter().next().unwrap();
            }
        }
        Self::extract(input, frontier, residual, keep, ship);
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
impl<D: Columnar, T: Columnar, R: Columnar> timely::container::ContainerBuilder
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
    timely::container::PushInto<&mut Vec<PoolChunk<D, T, R>>> for PreparedChunker<D, T, R>
{
    fn push_into(&mut self, input: &mut Vec<PoolChunk<D, T, R>>) {
        self.queued.extend(input.drain(..));
    }
}

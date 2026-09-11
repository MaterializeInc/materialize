// Copyright Materialize, Inc. and contributors. All rights reserved.
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

//! MZ trace interfaces over Differential's pollable batcher and a vendored spine.
//!
//! [`Batcher`] accepts columnar input and seals it at a frontier. [`Spine`] exposes
//! published batches through DD's trace traits. DD supplies the chunk, batcher,
//! and merger contracts. The vendored spine owns scheduling and publication across
//! suspension. [`timestamp`] bridges the two DD/Timely versions at this boundary.
//!
//! [`pool_chunk`] supplies columnar kernels and buffer-pool reads.
//! [`super::asynchronous::arrange`] owns the Timely operator and calls [`maintain`]
//! when compaction or read completion makes work available. Batch conversion clones
//! chunk handles, preserving shared ownership of their bodies.
//!
//! Trace callbacks queue maintenance. Only the owning driver polls storage, so a
//! reader cannot acquire admission for work that the owner must finish while it
//! is already waiting for admission in its batcher.

use std::cell::RefCell;
use std::collections::VecDeque;
use std::rc::Rc;
use std::sync::Arc;
use std::task::{Wake, Waker};

use columnar::Columnar;
use differential_dataflow::difference::Semigroup;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::trace::chunk::ChunkBatch;
use differential_dataflow::trace::{Description, ExertionLogic, Trace, TraceReader};
use differential_dataflow_next::trace::Span;
use differential_dataflow_next::trace::chunk::{
    ChunkBatch as NativeChunkBatch,
    asynchronous::{AsyncChunk, ChunkBatcher},
};
use timely::progress::{Antichain, Timestamp, frontier::AntichainRef};
use tokio::sync::Notify;

use super::ColumnChunk;
use super::merge::ReadBudget;
use pool_chunk::{PoolChunk, PreparedChunker};
use timestamp::{Time, to_mz, to_native};

mod pool_chunk;
mod timestamp;
mod vendor;

type NativeBatcher<D, T, R> = ChunkBatcher<PreparedChunker<D, T, R>, PoolChunk<D, T, R>>;
type NativeSpine<D, T, R> = vendor::spine::Spine<Rc<NativeChunkBatch<PoolChunk<D, T, R>>>>;
type NativeSpan<D, T, R> = Span<Time<T>, Rc<NativeChunkBatch<PoolChunk<D, T, R>>>>;

/// Columnar stash backed by Differential's pollable merge batcher.
///
/// Complete each `push` or `seal` before starting another operation. Cancelling
/// either future requires dropping the batcher: DD may already own its input and
/// retain an unfinished operation that must not be mistaken for the next one.
pub struct Batcher<D: Columnar, T: Columnar, R: Columnar>
where
    PoolChunk<D, T, R>: AsyncChunk,
{
    inner: NativeBatcher<D, T, R>,
    /// Lower frontier for the next batch description.
    lower: Antichain<T>,
    frontier: Antichain<T>,
    budget: ReadBudget,
}

impl<D, T, R> Batcher<D, T, R>
where
    D: Columnar + 'static,
    for<'a> columnar::Ref<'a, D>: Copy + Ord,
    T: Columnar + Default + Timestamp + Lattice + Ord,
    for<'a> columnar::Ref<'a, T>: Copy + Ord,
    R: Columnar + Default + Semigroup + for<'a> Semigroup<columnar::Ref<'a, R>> + 'static,
{
    /// Construct a batcher with shared admission for decoded merge inputs.
    pub fn new(budget: ReadBudget) -> Self {
        Self {
            inner: NativeBatcher::new(None, 0),
            lower: Antichain::from_elem(T::minimum()),
            frontier: Antichain::new(),
            budget,
        }
    }

    /// Lower bound on updates retained by the most recent seal.
    pub fn frontier(&self) -> AntichainRef<'_, T> {
        self.frontier.borrow()
    }

    /// Insert a sorted, consolidated chunk.
    pub async fn push(&mut self, chunk: ColumnChunk<D, T, R>) {
        use differential_dataflow_next::batcher::asynchronous::Batcher as _;
        use differential_dataflow_next::trace::chunk::Chunk as _;
        let mut input = VecDeque::from([PoolChunk {
            chunk,
            budget: self.budget.clone(),
        }]);
        super::metrics::record(
            super::metrics::Stage::InitialSettle,
            input.iter().map(|c| c.chunk.records()).sum(),
            0,
        );
        let mut settled = VecDeque::new();
        PoolChunk::settle(&mut input, true, &mut settled);
        let mut chunks: Vec<_> = settled.into();
        futures_util::future::poll_fn(|cx| self.inner.poll_insert(&mut chunks, cx)).await;
    }

    /// Extract updates before `upper`, retaining later updates for another seal.
    pub async fn seal(
        &mut self,
        upper: Antichain<T>,
    ) -> (Vec<ColumnChunk<D, T, R>>, Description<T>) {
        use differential_dataflow_next::batcher::asynchronous::Batcher as _;
        let target = to_native(upper.borrow());
        let (batch, retained) =
            futures_util::future::poll_fn(|cx| self.inner.poll_extract(target.borrow(), cx)).await;
        self.frontier = to_mz(retained.borrow());
        let lower = std::mem::replace(&mut self.lower, upper.clone());
        (
            batch
                .into_iter()
                .flat_map(|b| b.chunks)
                .map(|c| c.chunk)
                .collect(),
            Description::new(lower, upper, Antichain::from_elem(T::minimum())),
        )
    }
}

/// MZ's trace interface over the vendored Differential fueled spine.
///
/// The arranger and trace readers share `state` on one Timely worker. Compaction
/// frontiers are cached so the reader trait can return
/// references without borrowing the native spine through its `RefCell`.
pub struct Spine<D: Columnar, T: Columnar, R: Columnar>
where
    PoolChunk<D, T, R>: AsyncChunk,
{
    pub(super) state: Rc<RefCell<NativeSpine<D, T, R>>>,
    pub(super) notify: Arc<Notify>,
    logical: Antichain<T>,
    physical: Antichain<T>,
    budget: ReadBudget,
}

impl<D, T, R> Spine<D, T, R>
where
    D: Columnar + 'static,
    for<'a> columnar::Ref<'a, D>: Copy + Ord,
    T: Columnar + Default + Timestamp + Lattice + Ord,
    for<'a> columnar::Ref<'a, T>: Copy + Ord,
    R: Columnar + Default + Semigroup + for<'a> Semigroup<columnar::Ref<'a, R>> + 'static,
{
    /// Construct a trace using the caller's shared decoded-input budget.
    pub(super) fn with_budget(
        info: timely::dataflow::operators::generic::OperatorInfo,
        budget: ReadBudget,
    ) -> Self {
        let notify = Arc::new(Notify::new());
        let info = timely_next::dataflow::operators::generic::OperatorInfo::new(
            info.local_id,
            info.global_id,
            info.address,
        );
        let mut native = NativeSpine::new(info, None, None);
        native.set_waker(Waker::from(Arc::new(NotifyWake(Arc::clone(&notify)))));
        Self {
            state: Rc::new(RefCell::new(native)),
            notify,
            logical: Antichain::from_elem(T::minimum()),
            physical: Antichain::from_elem(T::minimum()),
            budget,
        }
    }

    fn to_mz_batch(span: &NativeSpan<D, T, R>) -> Rc<ChunkBatch<ColumnChunk<D, T, R>>> {
        let chunks = span
            .inner
            .iter()
            .flat_map(|batch| batch.chunks.iter())
            .map(|chunk| chunk.chunk.clone())
            .collect();
        let description = Description::new(
            to_mz(span.desc.lower().borrow()),
            to_mz(span.desc.upper().borrow()),
            to_mz(span.desc.since().borrow()),
        );
        Rc::new(ChunkBatch {
            chunks,
            description,
        })
    }

    fn to_native_span(&self, batch: &ChunkBatch<ColumnChunk<D, T, R>>) -> NativeSpan<D, T, R> {
        let description = differential_dataflow_next::trace::Description::new(
            to_native(batch.description.lower().borrow()),
            to_native(batch.description.upper().borrow()),
            to_native(batch.description.since().borrow()),
        );
        let inner = if batch.chunks.is_empty() {
            None
        } else {
            let chunks = batch
                .chunks
                .iter()
                .map(|chunk| PoolChunk {
                    chunk: chunk.clone(),
                    budget: self.budget.clone(),
                })
                .collect();
            Some(Rc::new(NativeChunkBatch::new(chunks)))
        };
        Span::new(description, inner)
    }
}

impl<D, T, R> TraceReader for Spine<D, T, R>
where
    D: Columnar + 'static,
    for<'a> columnar::Ref<'a, D>: Copy + Ord,
    T: Columnar + Default + Timestamp + Lattice + Ord,
    for<'a> columnar::Ref<'a, T>: Copy + Ord,
    R: Columnar + Default + Semigroup + for<'a> Semigroup<columnar::Ref<'a, R>> + 'static,
{
    type Time = T;
    type Batch = Rc<ChunkBatch<ColumnChunk<D, T, R>>>;

    fn batches_through(&mut self, upper: AntichainRef<T>) -> Option<Vec<Self::Batch>> {
        if !timely::PartialOrder::less_equal(&self.physical.borrow(), &upper) {
            return None;
        }
        let upper = to_native(upper);
        self.state
            .borrow_mut()
            .spans_through(upper.borrow())
            .map(|spans| spans.iter().map(Self::to_mz_batch).collect())
    }

    fn set_logical_compaction(&mut self, frontier: AntichainRef<T>) {
        assert!(timely::PartialOrder::less_equal(
            &self.logical.borrow(),
            &frontier
        ));
        self.logical = frontier.to_owned();
        self.state
            .borrow_mut()
            .set_logical_compaction(to_native(frontier).borrow());
        self.notify.notify_one();
    }

    fn get_logical_compaction(&mut self) -> AntichainRef<'_, T> {
        self.logical.borrow()
    }

    fn set_physical_compaction(&mut self, frontier: AntichainRef<T>) {
        assert!(timely::PartialOrder::less_equal(
            &self.physical.borrow(),
            &frontier
        ));
        self.physical = frontier.to_owned();
        self.state
            .borrow_mut()
            .set_physical_compaction(to_native(frontier).borrow());
        self.notify.notify_one();
    }

    fn get_physical_compaction(&mut self) -> AntichainRef<'_, T> {
        self.physical.borrow()
    }

    fn map_batches<F: FnMut(&Self::Batch)>(&self, mut f: F) {
        self.state
            .borrow()
            .map_spans(|span| f(&Self::to_mz_batch(span)));
    }
}

impl<D, T, R> Trace for Spine<D, T, R>
where
    D: Columnar + 'static,
    for<'a> columnar::Ref<'a, D>: Copy + Ord,
    T: Columnar + Default + Timestamp + Lattice + Ord,
    for<'a> columnar::Ref<'a, T>: Copy + Ord,
    R: Columnar + Default + Semigroup + for<'a> Semigroup<columnar::Ref<'a, R>> + 'static,
{
    fn new(
        info: timely::dataflow::operators::generic::OperatorInfo,
        _logging: Option<differential_dataflow::logging::Logger>,
        _activator: Option<timely::scheduling::Activator>,
    ) -> Self {
        Self::with_budget(info, ReadBudget::new(256 << 20))
    }

    fn exert(&mut self) {
        self.notify.notify_one();
    }

    fn set_exert_logic(&mut self, logic: ExertionLogic) {
        self.state.borrow_mut().set_exert_logic(logic);
    }

    fn insert(&mut self, batch: Self::Batch) {
        let span = self.to_native_span(&batch);
        self.state.borrow_mut().insert(span);
        self.notify.notify_one();
    }

    fn close(&mut self) {
        self.state.borrow_mut().close();
        self.notify.notify_one();
    }
}

struct NotifyWake(Arc<Notify>);
impl Wake for NotifyWake {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.0.notify_one();
    }
}

/// Apply one exertion turn, yielding while its maintenance waits for I/O.
///
/// This does not force all batches to compact. DD's exertion policy determines
/// the work allowance. The caller must use the notification installed by
/// `Spine::with_budget` and poll this future on the owning Timely worker.
/// Set `allow_consolidation` when input drains so policy can also initiate merges
/// between separate batches. Otherwise only active merges receive optional fuel.
pub(super) async fn maintain<B>(
    state: &RefCell<vendor::spine::Spine<B>>,
    notify: &Notify,
    allow_consolidation: bool,
) where
    B: differential_dataflow_next::trace::asynchronous::Batch + Clone + 'static,
{
    if allow_consolidation {
        state.borrow_mut().exert();
    } else {
        state.borrow_mut().exert_merges();
    }
    while state.borrow().maintenance_pending() {
        // No trace borrow may cross this await. Reader compaction can change the
        // same spine while a read is pending, and its wakeup uses this notification.
        notify.notified().await;
        // A read completion resumes the existing allowance. Calling `exert` here
        // would grant more fuel and could keep the arranger from accepting input
        // until an entire merge finishes.
        state.borrow_mut().resume_maintenance();
    }
}

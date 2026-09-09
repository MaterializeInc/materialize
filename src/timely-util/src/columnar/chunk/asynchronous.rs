// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

//! Asynchronous columnar batching and trace compaction.
//!
//! Runs follow a geometric merge policy. Each merge yields between output
//! chunks and awaits pool reads, while settlement and timestamp advancement
//! run on the Timely worker. Trace readers retain the published input batches
//! until their replacement is complete. No shared-state borrow crosses an await.
//!
//! The arranger currently requires totally ordered timestamps. Its trace still
//! uses `TraceAgent` to aggregate reader compaction holds. It is a separate
//! driver from Differential's fueled spine, whose synchronous completion
//! contract cannot represent a pending read.

use std::cell::RefCell;
use std::collections::VecDeque;
use std::rc::Rc;
use std::sync::Arc;

use columnar::Columnar;
use differential_dataflow::difference::Semigroup;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::logging::Logger;
use differential_dataflow::operators::arrange::{Arranged, TraceAgent};
use differential_dataflow::trace::chunk::{Chunk, ChunkBatch};
use differential_dataflow::trace::{
    Batch, BatchReader, Description, ExertionLogic, Trace, TraceReader,
};
use timely::PartialOrder;
use timely::container::{CapacityContainerBuilder, ContainerBuilder, PushInto};
use timely::dataflow::Stream;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::generic::OperatorInfo;
use timely::order::TotalOrder;
use timely::progress::Timestamp;
use timely::progress::frontier::{Antichain, AntichainRef};
use tokio::sync::Notify;

use super::merge::{Merge, ReadBudget, Step};
use super::{ChunkChunker, Column, ColumnChunk};
use crate::builder_async::{Event, OperatorBuilder, PressOnDropButton};

type Run<D, T, R> = VecDeque<ColumnChunk<D, T, R>>;
type BatchRef<D, T, R> = Rc<ChunkBatch<ColumnChunk<D, T, R>>>;

#[derive(Clone)]
struct Weighted<B> {
    data: B,
    weight: usize,
}

impl<B> std::ops::Deref for Weighted<B> {
    type Target = B;
    fn deref(&self) -> &B {
        &self.data
    }
}

/// Geometrically merged runs with asynchronous insertion and sealing.
///
/// A cancelled mutation discards its in-progress work. Callers must retain
/// capabilities until the operation completes, or shut down the dataflow.
pub struct Batcher<D: Columnar, T: Columnar, R: Columnar> {
    runs: Vec<Weighted<Run<D, T, R>>>,
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
    /// Construct an empty batcher using shared merge-input admission.
    /// The budget must fit the largest input pair, including indivisible rows.
    pub fn new(budget: ReadBudget) -> Self {
        Self {
            runs: Vec::new(),
            lower: Antichain::from_elem(T::minimum()),
            frontier: Antichain::new(),
            budget,
        }
    }

    /// Lower bound on times remaining after the most recent seal.
    pub fn frontier(&self) -> AntichainRef<'_, T> {
        self.frontier.borrow()
    }

    /// Insert a sorted, consolidated chunk and restore geometric run sizes.
    pub async fn push(&mut self, chunk: ColumnChunk<D, T, R>) {
        let mut run = VecDeque::from([chunk]);
        let mut settled = VecDeque::new();
        ColumnChunk::settle(&mut run, true, &mut settled);
        self.runs.push(Weighted {
            weight: weight(&settled),
            data: settled,
        });
        while self.runs.len() > 1 {
            let n = self.runs.len();
            if self.runs[n - 1].weight < self.runs[n - 2].weight / 2 {
                break;
            }
            self.merge_last().await;
        }
    }

    async fn merge_last(&mut self) {
        let right = self.runs.pop().expect("right run");
        let left = self.runs.pop().expect("left run");
        let merged = merge_runs(left.data, right.data, None, self.budget.clone()).await;
        self.runs.push(Weighted {
            weight: weight(&merged),
            data: merged,
        });
    }

    /// Extract updates strictly before `upper`, retaining later updates.
    /// Unlike an arrangement writer, callers may push older times back after a seal.
    pub async fn seal(
        &mut self,
        upper: Antichain<T>,
    ) -> (Vec<ColumnChunk<D, T, R>>, Description<T>) {
        while self.runs.len() > 1 {
            self.merge_last().await;
        }
        let mut input = self.runs.pop().map(|r| r.data).unwrap_or_default();
        let mut keep = VecDeque::new();
        let mut retained = VecDeque::new();
        let mut ship = VecDeque::new();
        let mut output = VecDeque::new();
        self.frontier.clear();
        while let Some(mut chunk) = input.pop_front() {
            let (lower, high) = chunk.chunk_time_bounds();
            let split = !high.iter().all(|t| !upper.less_equal(t))
                && !lower.iter().all(|t| upper.less_equal(t));
            // Keep partition input admission until extraction has consumed it.
            let mut reader = split.then(|| {
                Merge::new(
                    VecDeque::from([chunk.clone()]),
                    VecDeque::new(),
                    self.budget.clone(),
                )
                .expect("chunk fits read budget")
            });
            if let Some(reader) = &mut reader {
                chunk = reader.read_output(chunk).await;
            }
            let mut one = VecDeque::from([chunk]);
            ColumnChunk::extract(
                &mut one,
                upper.borrow(),
                &mut self.frontier,
                &mut keep,
                &mut ship,
            );
            drop(reader);
            let mut settled = VecDeque::new();
            ColumnChunk::settle(&mut keep, false, &mut settled);
            retained.extend(settled);
            ColumnChunk::settle(&mut ship, false, &mut output);
            tokio::task::yield_now().await;
        }
        let mut settled = VecDeque::new();
        ColumnChunk::settle(&mut keep, true, &mut settled);
        retained.extend(settled);
        if !retained.is_empty() {
            self.runs.push(Weighted {
                weight: weight(&retained),
                data: retained,
            });
        }
        ColumnChunk::settle(&mut ship, true, &mut output);
        let lower = std::mem::replace(&mut self.lower, upper.clone());
        (
            output.into(),
            Description::new(lower, upper, Antichain::from_elem(T::minimum())),
        )
    }
}

fn weight<D: Columnar, T: Columnar, R: Columnar>(run: &Run<D, T, R>) -> usize
where
    ColumnChunk<D, T, R>: Chunk,
{
    run.iter().map(Chunk::len).sum::<usize>().max(1)
}

async fn merge_runs<D, T, R>(
    left: Run<D, T, R>,
    right: Run<D, T, R>,
    since: Option<Antichain<T>>,
    budget: ReadBudget,
) -> Run<D, T, R>
where
    D: Columnar + 'static,
    for<'a> columnar::Ref<'a, D>: Copy + Ord,
    T: Columnar + Default + Timestamp + Lattice + Ord,
    for<'a> columnar::Ref<'a, T>: Copy + Ord,
    R: Columnar + Default + Semigroup + for<'a> Semigroup<columnar::Ref<'a, R>> + 'static,
{
    let mut merge = Merge::new(left, right, budget).expect("chunk pair fits read budget");
    let mut advancing = VecDeque::new();
    let mut settling = VecDeque::new();
    let mut output = VecDeque::new();
    loop {
        let done = match merge.step().await {
            Step::Output(chunk) => {
                if let Some(since) = &since {
                    advancing.push_back(merge.read_output(chunk).await);
                    ColumnChunk::advance(&mut advancing, since.borrow(), false, &mut settling);
                } else {
                    settling.push_back(chunk);
                }
                false
            }
            Step::Progress => false,
            Step::Complete => true,
        };
        if done && let Some(since) = &since {
            ColumnChunk::advance(&mut advancing, since.borrow(), true, &mut settling);
        }
        ColumnChunk::settle(&mut settling, done, &mut output);
        if done {
            return output;
        }
        tokio::task::yield_now().await;
    }
}

struct State<C: Chunk> {
    batches: Vec<Weighted<Rc<ChunkBatch<C>>>>,
    logical: Antichain<C::Time>,
    physical: Antichain<C::Time>,
}

/// A trace whose synchronous methods publish batches and request asynchronous work.
/// Construct through [`arrange`] so the maintenance driver is installed.
pub struct Spine<D: Columnar, T: Columnar, R: Columnar>
where
    ColumnChunk<D, T, R>: Chunk,
{
    state: Rc<RefCell<State<ColumnChunk<D, T, R>>>>,
    notify: Arc<Notify>,
    logical: Antichain<T>,
    physical: Antichain<T>,
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
    type Batch = BatchRef<D, T, R>;
    fn batches_through(&mut self, upper: AntichainRef<T>) -> Option<Vec<Self::Batch>> {
        let state = self.state.borrow();
        if upper.is_empty() {
            return Some(state.batches.iter().map(|b| Rc::clone(&b.data)).collect());
        }
        if !PartialOrder::less_equal(&self.physical.borrow(), &upper) {
            return None;
        }
        if upper == Antichain::from_elem(T::minimum()).borrow() {
            return Some(Vec::new());
        }
        let end = state
            .batches
            .iter()
            .position(|b| b.upper().borrow() == upper)?;
        Some(
            state.batches[..=end]
                .iter()
                .map(|b| Rc::clone(&b.data))
                .collect(),
        )
    }
    fn set_logical_compaction(&mut self, frontier: AntichainRef<T>) {
        assert!(PartialOrder::less_equal(&self.logical.borrow(), &frontier));
        self.logical = frontier.to_owned();
        self.state.borrow_mut().logical.clone_from(&self.logical);
        self.notify.notify_one();
    }
    fn get_logical_compaction(&mut self) -> AntichainRef<'_, T> {
        self.logical.borrow()
    }
    fn set_physical_compaction(&mut self, frontier: AntichainRef<T>) {
        assert!(PartialOrder::less_equal(&self.physical.borrow(), &frontier));
        self.physical = frontier.to_owned();
        self.state.borrow_mut().physical.clone_from(&self.physical);
        self.notify.notify_one();
    }
    fn get_physical_compaction(&mut self) -> AntichainRef<'_, T> {
        self.physical.borrow()
    }
    fn map_batches<F: FnMut(&Self::Batch)>(&self, f: F) {
        self.state
            .borrow()
            .batches
            .iter()
            .map(|b| &b.data)
            .for_each(f);
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
        _info: OperatorInfo,
        _logging: Option<Logger>,
        _activator: Option<timely::scheduling::Activator>,
    ) -> Self {
        let minimum = Antichain::from_elem(T::minimum());
        Self {
            state: Rc::new(RefCell::new(State {
                batches: Vec::new(),
                logical: minimum.clone(),
                physical: minimum.clone(),
            })),
            notify: Arc::new(Notify::new()),
            logical: minimum.clone(),
            physical: minimum,
        }
    }
    fn exert(&mut self) {
        self.notify.notify_one();
    }
    // This driver completes geometric merges incrementally, independently of fueled-spine policy.
    fn set_exert_logic(&mut self, _logic: ExertionLogic) {}
    fn insert(&mut self, batch: Self::Batch) {
        let mut state = self.state.borrow_mut();
        let lower = state
            .batches
            .last()
            .map(|b| b.upper().clone())
            .unwrap_or_else(|| Antichain::from_elem(T::minimum()));
        assert!(PartialOrder::less_equal(&lower, batch.lower()));
        if &lower != batch.lower() {
            state.batches.push(Weighted {
                weight: 1,
                data: Rc::new(ChunkBatch::empty(lower, batch.lower().clone())),
            });
        }
        state.batches.push(Weighted {
            weight: batch.len().max(1),
            data: batch,
        });
        self.notify.notify_one();
    }
    fn close(&mut self) {
        let mut upper = Antichain::new();
        self.read_upper(&mut upper);
        if !upper.is_empty() {
            self.insert(Rc::new(ChunkBatch::empty(upper, Antichain::new())));
        }
    }
}

async fn maintain<D, T, R>(state: &RefCell<State<ColumnChunk<D, T, R>>>, budget: ReadBudget)
where
    D: Columnar + 'static,
    for<'a> columnar::Ref<'a, D>: Copy + Ord,
    T: Columnar + Default + Timestamp + Lattice + Ord,
    for<'a> columnar::Ref<'a, T>: Copy + Ord,
    R: Columnar + Default + Semigroup + for<'a> Semigroup<columnar::Ref<'a, R>> + 'static,
{
    loop {
        let work = {
            let state = state.borrow();
            (1..state.batches.len())
                .rev()
                .find(|&i| {
                    PartialOrder::less_equal(state.batches[i].upper(), &state.physical)
                        && state.batches[i].weight >= state.batches[i - 1].weight / 2
                })
                .map(|i| {
                    (
                        i,
                        state.batches[i - 1].clone(),
                        state.batches[i].clone(),
                        state.logical.clone(),
                    )
                })
        };
        let Some((i, left, right, since)) = work else {
            return;
        };
        let chunks = merge_runs(
            left.chunks.clone().into(),
            right.chunks.clone().into(),
            Some(since.clone()),
            budget.clone(),
        )
        .await;
        let batch = Rc::new(ChunkBatch {
            chunks: chunks.into(),
            description: Description::new(left.lower().clone(), right.upper().clone(), since),
        });
        // Only this driver replaces batches. Readers can advance holds while
        // it awaits, but those frontiers cannot regress past the captured since.
        state.borrow_mut().batches.splice(
            i - 1..=i,
            [Weighted {
                weight: batch.len().max(1),
                data: batch,
            }],
        );
    }
}

/// Arrange a local stream using asynchronous batch and trace merges.
///
/// The caller owns the shutdown token. The input must already have the desired
/// worker partitioning. All clones of `budget` share decoded-input admission.
pub fn arrange<'scope, D, T, R>(
    stream: Stream<'scope, T, Column<(D, T, R)>>,
    budget: ReadBudget,
    name: &str,
) -> (
    Arranged<'scope, TraceAgent<Spine<D, T, R>>>,
    PressOnDropButton,
)
where
    for<'a> ChunkChunker<D, T, R>: PushInto<&'a mut Column<(D, T, R)>>,
    D: Columnar + 'static,
    for<'a> columnar::Ref<'a, D>: Copy + Ord,
    T: Columnar + Default + Timestamp + Lattice + TotalOrder,
    for<'a> columnar::Ref<'a, T>: Copy + Ord,
    R: Columnar + Default + Semigroup + for<'a> Semigroup<columnar::Ref<'a, R>> + 'static,
{
    let mut builder = OperatorBuilder::new(name.to_owned(), stream.scope());
    let info = builder.operator_info();
    let trace = Spine::<D, T, R>::new(info.clone(), None, None);
    let state = Rc::clone(&trace.state);
    let notify = Arc::clone(&trace.notify);
    let (trace, mut writer) = TraceAgent::new(trace, info, None);
    let (output, result) = builder.new_output::<CapacityContainerBuilder<Vec<BatchRef<D, T, R>>>>();
    let mut input = builder.new_input_for(stream, Pipeline, &output);
    let button = builder.build(move |caps| async move {
        drop(caps);
        let mut cap: Option<timely::dataflow::operators::Capability<T>> = None;
        let mut batcher = Batcher::new(budget.clone());
        let mut chunker = ChunkChunker::<D, T, R>::default();
        let mut upper = Antichain::from_elem(T::minimum());
        loop {
            let event = tokio::select! {
                _ = input.ready(), if !upper.is_empty() => input.next_sync(),
                _ = notify.notified() => None,
            };
            if let Some(event) = event {
                match event {
                    Event::Data(time, mut data) => {
                        if cap.as_ref().is_none_or(|old| time.time() < old.time()) {
                            cap = Some(time);
                        }
                        chunker.push_into(&mut data);
                        while let Some(chunk) = chunker.extract() {
                            batcher.push(std::mem::take(chunk)).await;
                        }
                    }
                    Event::Progress(next) => {
                        while let Some(chunk) = chunker.finish() {
                            batcher.push(std::mem::take(chunk)).await;
                        }
                        if next != upper {
                            let (chunks, description) = batcher.seal(next.clone()).await;
                            let batch = Rc::new(ChunkBatch {
                                chunks,
                                description,
                            });
                            writer
                                .insert(Rc::clone(&batch), cap.as_ref().map(|c| c.time().clone()));
                            if let Some(time) = &cap {
                                output.give(time, batch);
                            }
                            if let Some(t) = batcher.frontier().first() {
                                cap.as_mut()
                                    .expect("buffered data has a capability")
                                    .downgrade(t);
                            } else {
                                cap = None;
                            }
                            upper = next;
                        }
                    }
                }
            }
            maintain(&state, budget.clone()).await;
        }
    });
    (
        Arranged {
            stream: result,
            trace,
        },
        button.press_on_drop(),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use columnar::{Index, Len};
    use differential_dataflow::trace::Batcher as SyncBatcher;
    use differential_dataflow::trace::chunk::ChunkBatcher;
    use mz_ore::pool::Pool;

    type TestChunk = ColumnChunk<(u64, Vec<u8>), u64, i64>;
    type Update = ((u64, Vec<u8>), u64, i64);

    fn chunk(rows: &[Update], pool: &Pool) -> TestChunk {
        let mut column = Column::default();
        for row in rows {
            column.push_into(row);
        }
        TestChunk::spill_body(column, pool, 1)
    }

    fn collect(chunks: impl IntoIterator<Item = TestChunk>) -> Vec<Update> {
        let mut rows = Vec::new();
        for chunk in chunks {
            let column = chunk.into_column();
            let view = column.borrow();
            for i in 0..view.len() {
                rows.push(<Update as Columnar>::into_owned(view.get(i)));
            }
        }
        rows
    }

    #[mz_ore::test]
    fn batcher_matches_synchronous_seals_with_spills_and_restash() {
        let pool = Pool::new().unwrap();
        pool.set_spill_threads(0);
        pool.set_budget(0);
        super::super::with_spill_override(pool.clone(), || {
            tokio::runtime::Runtime::new().unwrap().block_on(async {
                let budget = ReadBudget::new(1 << 20);
                let mut asynchronous = Batcher::new(budget.clone());
                let mut synchronous = ChunkBatcher::<TestChunk>::new(None, 0);
                for round in 0..12 {
                    let mut rows = Vec::new();
                    for i in 0..80 {
                        rows.push((
                            (i % 23, vec![42; 128]),
                            (i + round) % 7,
                            if (i + round) % 3 == 0 { -1 } else { 1 },
                        ));
                    }
                    differential_dataflow::consolidation::consolidate_updates(&mut rows);
                    let chunk = chunk(&rows, &pool);
                    synchronous.push_into(chunk.clone());
                    asynchronous.push(chunk).await;
                    if round % 3 == 2 {
                        let upper = Antichain::from_elem(round / 3 + 1);
                        let (actual, _) = asynchronous.seal(upper.clone()).await;
                        let (expected, _) = synchronous.seal(upper);
                        assert_eq!(collect(actual), collect(expected));
                        assert_eq!(asynchronous.frontier(), synchronous.frontier());
                    }
                }
                assert_eq!(
                    collect(asynchronous.seal(Antichain::new()).await.0),
                    collect(synchronous.seal(Antichain::new()).0)
                );
                assert_eq!(budget.reserved_bytes(), 0);
                assert!(pool.stats().async_reads > 0);
            });
        });
    }

    #[mz_ore::test]
    fn trace_compaction_respects_all_readers_and_retains_published_batches() {
        let pool = Pool::new().unwrap();
        pool.set_spill_threads(0);
        pool.set_budget(0);
        tokio::runtime::Runtime::new().unwrap().block_on(async {
            let info = OperatorInfo::new(0, 0, Rc::from([0]));
            let spine = Spine::<(u64, Vec<u8>), u64, i64>::new(info.clone(), None, None);
            let state = Rc::clone(&spine.state);
            let (mut reader, mut writer) = TraceAgent::new(spine, info, None);
            let mut hold = reader.clone();
            for time in 0..2 {
                writer.insert(
                    Rc::new(ChunkBatch {
                        chunks: vec![chunk(
                            &[((1, vec![7; 2048]), time, if time == 0 { 1 } else { -1 })],
                            &pool,
                        )],
                        description: Description::new(
                            Antichain::from_elem(time),
                            Antichain::from_elem(time + 1),
                            Antichain::from_elem(0),
                        ),
                    }),
                    Some(time),
                );
            }
            let snapshot = reader.batches_through(Antichain::new().borrow()).unwrap();
            let frontier = Antichain::from_elem(2);
            reader.set_logical_compaction(frontier.borrow());
            reader.set_physical_compaction(frontier.borrow());
            let budget = ReadBudget::new(1 << 20);
            maintain(&state, budget.clone()).await;
            assert_eq!(
                state.borrow().batches.len(),
                2,
                "another reader still holds the original cut"
            );
            hold.set_logical_compaction(frontier.borrow());
            hold.set_physical_compaction(frontier.borrow());
            let mut maintenance = Box::pin(maintain(&state, budget.clone()));
            assert!(futures_util::poll!(&mut maintenance).is_pending());
            assert_eq!(
                reader
                    .batches_through(Antichain::new().borrow())
                    .unwrap()
                    .len(),
                2,
                "pending compaction must keep both inputs visible"
            );
            maintenance.await;
            let merged = reader.batches_through(Antichain::new().borrow()).unwrap();
            assert_eq!(merged.len(), 1);
            assert_eq!(
                merged[0].len(),
                0,
                "advancement cancels opposite diffs across batches"
            );
            assert_eq!(merged[0].upper(), &frontier);
            assert!(
                reader
                    .batches_through(Antichain::from_elem(1).borrow())
                    .is_none()
            );
            assert_eq!(
                snapshot.iter().map(|b| b.len()).sum::<usize>(),
                2,
                "published snapshots survive replacement"
            );
            assert!(pool.stats().async_reads >= 2);
            assert_eq!(budget.reserved_bytes(), 0);
        });
    }
}

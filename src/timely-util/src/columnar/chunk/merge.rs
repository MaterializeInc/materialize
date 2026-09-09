// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

//! Resumable merging of columnar runs with asynchronous input reads.
//!
//! A merge retains its two decoded fronts and positions across steps. Input
//! admission reserves the largest possible pair before either read starts, so
//! replacing an exhausted front never waits for memory while holding the other.
//! Blocking reads retain the reservation through cancellation. Resident bounds
//! let disjoint fronts pass through without reads or compression migration.
//!
//! This driver does not advance timestamps or settle output. Its caller must
//! retain the appropriate frontier/capability, consume output incrementally,
//! and yield between steps. Differential's synchronous `Chunk::merge` and trace
//! drivers do not call this interface. Output allocations, resident input
//! storage, and metadata are outside decoded-input admission.

use std::collections::VecDeque;
use std::fmt;
use std::sync::Arc;

use columnar::{Columnar, Container, Len};
use differential_dataflow::difference::Semigroup;
use differential_dataflow::lattice::Lattice;
use mz_ore::pool::ChunkHandle;
use mz_ore::task::JoinHandle;
use timely::dataflow::channels::ContainerBytes;
use timely::progress::Timestamp;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

use super::{Column, ColumnChunk, rr};

/// Shared admission for the serialized size of active merge input pairs.
///
/// Clone this budget across operators to bound their aggregate decoded inputs.
/// Reservations last until a merge finishes or is dropped, including any reads
/// still running after cancellation. Output and typed scratch are not charged.
#[derive(Clone)]
pub struct ReadBudget {
    bytes: Arc<Semaphore>,
    capacity: u32,
}

impl ReadBudget {
    /// Construct a positive decoded-input budget in bytes.
    pub fn new(bytes: u32) -> Self {
        assert!(bytes > 0, "merge read budget must be positive");
        Self {
            bytes: Arc::new(Semaphore::new(
                usize::try_from(bytes).expect("u32 fits usize"),
            )),
            capacity: bytes,
        }
    }

    /// Bytes currently reserved, including cancelled reads still in flight.
    pub fn reserved_bytes(&self) -> usize {
        usize::try_from(self.capacity).expect("u32 fits usize") - self.bytes.available_permits()
    }
}

/// A run contains an input pair larger than the shared read budget.
#[derive(Debug)]
pub struct InsufficientBudget {
    /// Largest input pair's serialized size.
    pub required: usize,
    /// Configured budget in bytes.
    pub available: u32,
}

impl fmt::Display for InsufficientBudget {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "merge requires {} input bytes, budget is {}",
            self.required, self.available
        )
    }
}

impl std::error::Error for InsufficientBudget {}

/// One completed unit of merge work.
pub enum Step<C> {
    /// A sorted, consolidated prefix that the caller can consume or settle.
    Output(C),
    /// Inputs advanced without output, for example through cancellation.
    Progress,
    /// Both runs are exhausted and all output has been returned.
    Complete,
}

/// A two-run merge retaining decoded fronts across output and scheduler yields.
///
/// Each input must be sorted and consolidated across chunk boundaries. `step`
/// may await I/O, and performs at most one column merge call before returning.
/// Cancelling a `step` future leaves the merge resumable. Dropping the merge
/// discards its work, while submitted reads retain their storage and admission.
pub struct Merge<D: Columnar, T: Columnar, R: Columnar> {
    inputs: [VecDeque<ColumnChunk<D, T, R>>; 2],
    columns: [Column<(D, T, R)>; 2],
    positions: [usize; 2],
    loaded: [bool; 2],
    depths: [u8; 2],
    ready: VecDeque<ColumnChunk<D, T, R>>,
    budget: ReadBudget,
    required: u32,
    reservation: Option<Arc<OwnedSemaphorePermit>>,
    pending: Option<JoinHandle<ReadResult>>,
}

struct ReadResult {
    buffers: [Option<Vec<u64>>; 2],
    // Keep completed, unconsumed buffers charged too. Field order releases
    // buffers before admission when an abandoned task's result is dropped.
    _reservation: Arc<OwnedSemaphorePermit>,
}

impl<D, T, R> Merge<D, T, R>
where
    D: Columnar,
    for<'a> columnar::Ref<'a, D>: Copy + Ord,
    T: Columnar + Default + Timestamp + Lattice + Ord,
    for<'a> columnar::Ref<'a, T>: Copy + Ord,
    R: Columnar + Default + Semigroup + for<'a> Semigroup<columnar::Ref<'a, R>>,
{
    /// Own two runs, rejecting a read working set that could never be admitted.
    ///
    /// Sizing inspects resident metadata only. No reads or reservations occur
    /// until `step` encounters overlapping fronts.
    pub fn new(
        left: VecDeque<ColumnChunk<D, T, R>>,
        right: VecDeque<ColumnChunk<D, T, R>>,
        budget: ReadBudget,
    ) -> Result<Self, InsufficientBudget> {
        let inputs = [left, right];
        let required = inputs.iter().fold(0usize, |sum, run| {
            sum.saturating_add(
                run.iter()
                    .map(|chunk| match chunk {
                        ColumnChunk::Resident(column, _) => column.length_in_bytes(),
                        ColumnChunk::Spilled(body, _) => body.bytes,
                    })
                    .max()
                    .unwrap_or(0),
            )
        });
        let required = u32::try_from(required)
            .ok()
            .filter(|n| *n <= budget.capacity)
            .ok_or(InsufficientBudget {
                required,
                available: budget.capacity,
            })?;
        Ok(Self {
            inputs,
            columns: [Column::default(), Column::default()],
            positions: [0; 2],
            loaded: [false; 2],
            depths: [0; 2],
            ready: VecDeque::new(),
            budget,
            required,
            reservation: None,
            pending: None,
        })
    }

    /// Produce one output chunk or yield after one bounded merge operation.
    ///
    /// Requires a Tokio runtime when an input body is pool-backed. The caller
    /// must yield between ready steps to share its worker with other operators.
    pub async fn step(&mut self) -> Step<ColumnChunk<D, T, R>> {
        if let Some(chunk) = self.ready.pop_front() {
            return Step::Output(chunk);
        }
        self.finish_reads().await;
        self.retire_inputs();
        if self.loaded == [false; 2] {
            let passthrough = match (self.inputs[0].front(), self.inputs[1].front()) {
                (None, None) => {
                    self.reservation = None;
                    return Step::Complete;
                }
                (Some(_), None) => Some((0, false)),
                (None, Some(_)) => Some((1, false)),
                (Some(a), Some(b)) => {
                    let (af, al) = a.data_span();
                    let (bf, bl) = b.data_span();
                    if rr::<D>(al) < rr::<D>(bf) {
                        Some((0, true))
                    } else if rr::<D>(bl) < rr::<D>(af) {
                        Some((1, true))
                    } else {
                        None
                    }
                }
            };
            if let Some((side, age)) = passthrough {
                let mut chunk = self.inputs[side].pop_front().expect("observed front");
                // Aging must not invoke survive_merge's synchronous codec migration.
                // Settlement can apply that policy after the caller accepts output.
                if age {
                    match &mut chunk {
                        ColumnChunk::Resident(_, depth) | ColumnChunk::Spilled(_, depth) => {
                            *depth = depth.saturating_add(1);
                        }
                    }
                }
                return Step::Output(chunk);
            }
        }

        if self.reservation.is_none() {
            self.reservation = Some(Arc::new(
                Arc::clone(&self.budget.bytes)
                    .acquire_many_owned(self.required)
                    .await
                    .expect("read budget remains open"),
            ));
        }
        let mut handles = [None, None];
        for (side, handle) in handles.iter_mut().enumerate() {
            if !self.loaded[side]
                && let Some(chunk) = self.inputs[side].pop_front()
            {
                self.depths[side] = chunk.depth();
                match chunk {
                    ColumnChunk::Spilled(body, _) => *handle = Some(Arc::clone(&body.handle)),
                    resident @ ColumnChunk::Resident(..) => {
                        self.columns[side] = resident.into_column();
                        self.loaded[side] = true;
                    }
                }
            }
        }
        if handles.iter().any(Option::is_some) {
            let reservation = Arc::clone(self.reservation.as_ref().expect("admitted inputs"));
            self.pending = Some(mz_ore::task::spawn(|| "column_merge_reads", async move {
                // The task is detached on cancellation, retaining admission until
                // both read_async calls (including their blocking jobs) finish.
                let [a, b] = handles;
                let (a, b) = tokio::join!(read(a), read(b));
                ReadResult {
                    buffers: [a, b],
                    _reservation: reservation,
                }
            }));
            self.finish_reads().await;
        }

        let mut output = Column::default();
        let depth = if self.loaded == [true; 2] {
            let _yielded = output.merge_from(&mut self.columns, &mut self.positions);
            self.depths[0].max(self.depths[1]).saturating_add(1)
        } else {
            let side = usize::from(!self.loaded[0]);
            let view = self.columns[side].borrow();
            let mut remaining = <(D, T, R) as Columnar>::Container::default();
            remaining.extend_from_self(view, self.positions[side]..view.len());
            self.positions[side] = view.len();
            output = Column::Typed(remaining);
            self.depths[side]
        };
        self.retire_inputs();
        ColumnChunk::push_bounded(output, depth, &mut self.ready);
        self.ready.pop_front().map_or(Step::Progress, Step::Output)
    }

    async fn finish_reads(&mut self) {
        if let Some(task) = &mut self.pending {
            let ReadResult { buffers, .. } = task.await;
            self.pending = None;
            for (side, words) in buffers.into_iter().enumerate() {
                if let Some(words) = words {
                    self.columns[side] = Column::Align(words);
                    self.loaded[side] = true;
                }
            }
        }
    }

    fn retire_inputs(&mut self) {
        for side in 0..2 {
            if self.loaded[side] && self.positions[side] == self.columns[side].borrow().len() {
                self.columns[side] = Column::default();
                self.positions[side] = 0;
                self.loaded[side] = false;
            }
        }
    }
}

async fn read(handle: Option<Arc<ChunkHandle>>) -> Option<Vec<u64>> {
    match handle {
        Some(handle) => Some(handle.read_async().await),
        None => None,
    }
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::rc::Rc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Condvar, Mutex};
    use std::time::{Duration, Instant};

    use columnar::{Index, Push};
    use differential_dataflow::consolidation::consolidate_updates;
    use mz_ore::pool::{ExtentCodec, Pool};
    use timely::container::PushInto;

    use super::super::{LZ4_CODEC, SpilledBody, spill_column};
    use super::*;

    type Update = ((u64, String), u64, i64);
    type TestChunk = ColumnChunk<(u64, String), u64, i64>;

    fn chunk(rows: &[Update], spill: Option<(&Pool, &'static dyn ExtentCodec)>) -> TestChunk {
        let mut column = Column::default();
        for row in rows {
            column.push_into(row);
        }
        let Some((pool, codec)) = spill else {
            return TestChunk::from_column(column);
        };
        let bytes = column.length_in_bytes();
        let (time_lower, time_upper) = TestChunk::time_bounds(&column);
        let mut fences = <(u64, String) as Columnar>::Container::default();
        let view = column.borrow();
        fences.push(view.0.get(0));
        fences.push(view.0.get(rows.len() - 1));
        let handle = spill_column(
            column,
            pool,
            bytes,
            mz_ore::pool::ChunkHints { depth: 1 },
            codec,
        );
        pool.set_spill_threads(0);
        pool.set_budget(0);
        TestChunk::Spilled(
            Rc::new(SpilledBody {
                bytes,
                records: rows.len(),
                fences,
                time_lower,
                time_upper: time_upper.into(),
                compressed: true,
                handle: Arc::new(handle),
            }),
            1,
        )
    }

    fn collect(chunk: TestChunk) -> Vec<Update> {
        let column = chunk.into_column();
        let view = column.borrow();
        (0..view.len())
            .map(|i| Update::into_owned(view.get(i)))
            .collect()
    }

    fn rows(keys: impl Iterator<Item = u64>, width: usize) -> Vec<Update> {
        keys.map(|k| ((k, "x".repeat(width)), 0, 1)).collect()
    }

    async fn finish(merge: &mut Merge<(u64, String), u64, i64>) -> Vec<Update> {
        let mut result = Vec::new();
        loop {
            match merge.step().await {
                Step::Output(chunk) => Extend::extend(&mut result, collect(chunk)),
                Step::Progress => {}
                Step::Complete => return result,
            }
            tokio::task::yield_now().await;
        }
    }

    #[mz_ore::test(tokio::test)]
    async fn retains_large_front_across_refills_and_output_yields() {
        let pool = Pool::new().unwrap();
        let left = rows((0..1600).step_by(2), 1900);
        let right = rows((1..1600).step_by(2), 1900);
        let left = VecDeque::from([chunk(&left, Some((&pool, &LZ4_CODEC)))]);
        let right = right
            .chunks(100)
            .map(|r| chunk(r, Some((&pool, &LZ4_CODEC))))
            .collect();
        let budget = ReadBudget::new(4 << 20);
        let mut merge = Merge::new(left, right, budget.clone()).unwrap();
        assert_eq!(finish(&mut merge).await, rows(0..1600, 1900));
        assert_eq!(
            pool.stats().async_reads,
            9,
            "each input is read exactly once"
        );
        assert_eq!(budget.reserved_bytes(), 0);
        assert_eq!(pool.stats().live_chunks, 0);
    }

    #[mz_ore::test(tokio::test)]
    async fn disjoint_runs_and_empty_side_need_no_reads() {
        let pool = Pool::new().unwrap();
        for right in [vec![], rows(10..20, 16)] {
            let left = VecDeque::from([chunk(&rows(0..10, 16), Some((&pool, &LZ4_CODEC)))]);
            let right = if right.is_empty() {
                VecDeque::new()
            } else {
                VecDeque::from([chunk(&right, Some((&pool, &LZ4_CODEC)))])
            };
            let budget = ReadBudget::new(1 << 20);
            let mut merge = Merge::new(left, right, budget.clone()).unwrap();
            loop {
                match merge.step().await {
                    Step::Output(chunk) => assert!(matches!(chunk, TestChunk::Spilled(..))),
                    Step::Complete => break,
                    Step::Progress => panic!("disjoint inputs must pass through"),
                }
                assert_eq!(budget.reserved_bytes(), 0);
            }
        }
        assert_eq!(pool.stats().async_reads, 0);
    }

    #[mz_ore::test(tokio::test)]
    async fn output_size_yields_keep_both_decoded_fronts() {
        let pool = Pool::new().unwrap();
        let left = chunk(&rows((0..3000).step_by(2), 1900), Some((&pool, &LZ4_CODEC)));
        let right = chunk(&rows((1..3000).step_by(2), 1900), Some((&pool, &LZ4_CODEC)));
        let mut merge = Merge::new(
            VecDeque::from([left]),
            VecDeque::from([right]),
            ReadBudget::new(8 << 20),
        )
        .unwrap();
        let mut output = Vec::new();
        let mut chunks = 0;
        loop {
            match merge.step().await {
                Step::Output(chunk) => {
                    let TestChunk::Resident(column, _) = &chunk else {
                        panic!("new merge output must be resident");
                    };
                    assert!(column.length_in_bytes() <= super::super::COMMIT_BYTES);
                    chunks += 1;
                    Extend::extend(&mut output, collect(chunk));
                }
                Step::Progress => {}
                Step::Complete => break,
            }
        }
        assert!(chunks >= 3);
        assert_eq!(output, rows(0..3000, 1900));
        assert_eq!(pool.stats().async_reads, 2);
    }

    #[mz_ore::test(tokio::test)]
    async fn matches_consolidation_across_times_chunks_and_storage_modes() {
        let pool = Pool::new().unwrap();
        for seed in 0..12u64 {
            let mut inputs: [Vec<Update>; 2] = std::array::from_fn(|side| {
                (0..160u64)
                    .map(|i| {
                        (
                            ((i * 13 + seed) % 23, "value".into()),
                            (i + seed) % 5,
                            if (i + u64::try_from(side).unwrap()) % 3 == 0 {
                                -1
                            } else {
                                1
                            },
                        )
                    })
                    .collect()
            });
            let mut expected: Vec<_> = inputs.iter().flatten().cloned().collect();
            consolidate_updates(&mut expected);
            for input in &mut inputs {
                consolidate_updates(input);
            }
            let [left, right] = inputs.map(|input| {
                input
                    .chunks(7)
                    .enumerate()
                    .map(|(i, rows)| chunk(rows, (i % 2 == 0).then_some((&pool, &LZ4_CODEC))))
                    .collect()
            });
            let mut merge = Merge::new(left, right, ReadBudget::new(1 << 20)).unwrap();
            assert_eq!(finish(&mut merge).await, expected, "seed {seed}");
        }
    }

    #[mz_ore::test]
    fn rejects_unadmittable_pair_before_reading() {
        let pool = Pool::new().unwrap();
        let input = chunk(&rows(0..10, 100), Some((&pool, &LZ4_CODEC)));
        let result = Merge::new(
            VecDeque::from([input.clone()]),
            VecDeque::from([input]),
            ReadBudget::new(1),
        );
        assert!(matches!(
            result,
            Err(InsufficientBudget { available: 1, .. })
        ));
        assert_eq!(pool.stats().async_reads, 0);
    }

    #[derive(Debug, Default)]
    struct Gate {
        entered: AtomicUsize,
        open: Mutex<bool>,
        ready: Condvar,
    }

    impl Gate {
        fn release(&self) {
            *self.open.lock().unwrap() = true;
            self.ready.notify_all();
        }
    }

    impl ExtentCodec for Gate {
        fn encode(&self, body: &[u8], out: &mut Vec<u8>) {
            LZ4_CODEC.encode(body, out);
        }
        fn decode(&self, stored: &[u8], body: &mut [u8]) {
            self.entered.fetch_add(1, Ordering::SeqCst);
            let mut open = self.open.lock().unwrap();
            while !*open {
                open = self.ready.wait(open).unwrap();
            }
            LZ4_CODEC.decode(stored, body);
        }
    }

    struct ReleaseOnDrop(&'static Gate);
    impl Drop for ReleaseOnDrop {
        fn drop(&mut self) {
            self.0.release();
        }
    }

    async fn wait_for(mut predicate: impl FnMut() -> bool) {
        tokio::time::timeout(Duration::from_secs(10), async {
            while !predicate() {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await
        .expect("condition did not become true");
    }

    fn blocked_merge(
        pool: &Pool,
        gate: &'static Gate,
        budget: ReadBudget,
    ) -> Merge<(u64, String), u64, i64> {
        Merge::new(
            VecDeque::from([chunk(&rows((0..20).step_by(2), 100), Some((pool, gate)))]),
            VecDeque::from([chunk(&rows((1..20).step_by(2), 100), Some((pool, gate)))]),
            budget,
        )
        .unwrap()
    }

    #[mz_ore::test(tokio::test)]
    async fn cancellation_retains_admission_until_both_reads_finish() {
        let pool = Pool::new().unwrap();
        let gate: &'static Gate = Box::leak(Box::new(Gate::default()));
        let _release = ReleaseOnDrop(gate);
        let budget = ReadBudget::new(1 << 20);
        let mut merge = blocked_merge(&pool, gate, budget.clone());
        let expected = usize::try_from(merge.required).unwrap();
        let mut step = Box::pin(merge.step());
        assert!(futures_util::poll!(&mut step).is_pending());
        wait_for(|| gate.entered.load(Ordering::SeqCst) == 2).await;
        drop(step);
        drop(merge);
        assert_eq!(budget.reserved_bytes(), expected);
        assert_eq!(pool.stats().live_chunks, 2);
        gate.release();
        wait_for(|| budget.reserved_bytes() == 0 && pool.stats().live_chunks == 0).await;
    }

    #[mz_ore::test(tokio::test)]
    async fn cancelled_step_can_resume_without_resubmitting_reads() {
        let pool = Pool::new().unwrap();
        let gate: &'static Gate = Box::leak(Box::new(Gate::default()));
        let _release = ReleaseOnDrop(gate);
        let budget = ReadBudget::new(1 << 20);
        let mut merge = blocked_merge(&pool, gate, budget.clone());
        let mut step = Box::pin(merge.step());
        assert!(futures_util::poll!(&mut step).is_pending());
        wait_for(|| gate.entered.load(Ordering::SeqCst) == 2).await;
        drop(step);
        gate.release();
        assert_eq!(finish(&mut merge).await, rows(0..20, 100));
        assert_eq!(pool.stats().async_reads, 2);
        assert_eq!(budget.reserved_bytes(), 0);
    }

    #[mz_ore::test(tokio::test)]
    async fn concurrent_merges_admit_whole_pairs_and_make_progress() {
        let pool = Pool::new().unwrap();
        let gate: &'static Gate = Box::leak(Box::new(Gate::default()));
        let _release = ReleaseOnDrop(gate);
        let mut first = blocked_merge(&pool, gate, ReadBudget::new(1 << 20));
        let budget = ReadBudget::new(first.required);
        first.budget = budget.clone();
        let mut second = Merge::new(
            VecDeque::from([chunk(
                &rows((0..20).step_by(2), 100),
                Some((&pool, &LZ4_CODEC)),
            )]),
            VecDeque::from([chunk(
                &rows((1..20).step_by(2), 100),
                Some((&pool, &LZ4_CODEC)),
            )]),
            budget.clone(),
        )
        .unwrap();
        let mut first_step = Box::pin(first.step());
        assert!(futures_util::poll!(&mut first_step).is_pending());
        wait_for(|| gate.entered.load(Ordering::SeqCst) == 2).await;
        drop(first_step);
        let mut second_step = Box::pin(second.step());
        assert!(futures_util::poll!(&mut second_step).is_pending());
        assert_eq!(
            pool.stats().async_reads,
            2,
            "second pair must wait for admission"
        );
        assert_eq!(
            budget.reserved_bytes(),
            usize::try_from(budget.capacity).unwrap()
        );
        drop(second_step);
        gate.release();
        assert_eq!(finish(&mut first).await, rows(0..20, 100));
        assert_eq!(finish(&mut second).await, rows(0..20, 100));
        assert_eq!(pool.stats().async_reads, 4);
        assert_eq!(budget.reserved_bytes(), 0);
    }

    #[mz_ore::test]
    fn live_operator_yields_during_reads_and_holds_its_frontier() {
        use crate::builder_async::{Event, OperatorBuilder};
        use futures_util::StreamExt;
        use timely::container::CapacityContainerBuilder;
        use timely::dataflow::channels::pact::Pipeline;
        use timely::dataflow::operators::vec::Input;
        use timely::dataflow::operators::{Inspect, Probe};

        let runtime = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .unwrap();
        let _runtime = runtime.enter();
        let pool = Pool::new().unwrap();
        let gate: &'static Gate = Box::leak(Box::new(Gate::default()));
        let _release = ReleaseOnDrop(gate);
        timely::execute_directly(move |worker| {
            let mut merge = blocked_merge(&pool, gate, ReadBudget::new(1 << 20));
            let output_rows = Rc::new(RefCell::new(Vec::new()));
            let heartbeat = Rc::new(RefCell::new(false));
            let probe = timely::dataflow::operators::probe::Handle::new();
            let (mut input, mut other) = worker.dataflow::<u64, _, _>(|scope| {
                let (input, stream) = scope.new_input::<u64>();
                let (other, other_stream) = scope.new_input::<u64>();
                let seen = Rc::clone(&heartbeat);
                other_stream.inspect(move |_| *seen.borrow_mut() = true);
                let mut builder = OperatorBuilder::new("Resumable merge".into(), scope.clone());
                let (output, stream_out) =
                    builder.new_output::<CapacityContainerBuilder<Vec<Update>>>();
                let mut input_events = builder.new_input_for(stream, Pipeline, &output);
                builder.build(move |caps| async move {
                    drop(caps);
                    while let Some(event) = input_events.next().await {
                        if let Event::Data(cap, _) = event {
                            loop {
                                match merge.step().await {
                                    Step::Output(chunk) => {
                                        for row in collect(chunk) {
                                            output.give(&cap, row);
                                        }
                                    }
                                    Step::Progress => {}
                                    Step::Complete => break,
                                }
                                tokio::task::yield_now().await;
                            }
                        }
                    }
                });
                let rows = Rc::clone(&output_rows);
                stream_out
                    .inspect(move |row| rows.borrow_mut().push(row.clone()))
                    .probe_with(&probe);
                (input, other)
            });
            input.send(0);
            input.advance_to(1);
            let deadline = Instant::now() + Duration::from_secs(10);
            while gate.entered.load(Ordering::SeqCst) < 2 {
                assert!(
                    Instant::now() < deadline,
                    "merge never submitted both reads"
                );
                worker.step();
                std::thread::yield_now();
            }
            other.send(0);
            other.advance_to(1);
            while !*heartbeat.borrow() {
                assert!(
                    Instant::now() < deadline,
                    "unrelated operator could not progress"
                );
                worker.step();
            }
            assert!(
                probe.less_than(&1),
                "pending merge must retain its capability"
            );
            assert!(output_rows.borrow().is_empty());
            gate.release();
            while probe.less_than(&1) {
                assert!(
                    Instant::now() < deadline,
                    "read completion did not wake the merge"
                );
                worker.step();
                std::thread::yield_now();
            }
            assert_eq!(*output_rows.borrow(), rows(0..20, 100));
        });
    }
}

// Copyright Materialize, Inc. and contributors. All rights reserved.
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

//! Worker integration for Differential's native pollable batching and compaction.
//!
//! Capabilities cover pending insertion and sealing. Differential owns merge
//! levels, fuel accounting, publication, and compaction. Pool reads wake this
//! operator, which resumes maintenance on its Timely worker.

use super::merge::ReadBudget;
use super::native::maintain;
pub use super::native::{Batcher, Spine};
use super::{ChunkChunker, Column, ColumnChunk};
use crate::builder_async::{Event, OperatorBuilder, PressOnDropButton};
use columnar::Columnar;
use differential_dataflow::difference::Semigroup;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::operators::arrange::{Arranged, TraceAgent};
use differential_dataflow::trace::chunk::ChunkBatch;
use differential_dataflow::trace::{ExertionLogic, Trace};
use std::rc::Rc;
use std::sync::Arc;
use timely::container::{CapacityContainerBuilder, ContainerBuilder, PushInto};
use timely::dataflow::Stream;
use timely::dataflow::channels::pact::Pipeline;
use timely::order::TotalOrder;
use timely::progress::{Timestamp, frontier::Antichain};
type BatchRef<D, T, R> = Rc<ChunkBatch<ColumnChunk<D, T, R>>>;

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
    let mut trace = Spine::<D, T, R>::with_budget(info.clone(), budget.clone());
    if let Some(logic) = stream
        .scope()
        .worker()
        .config()
        .get::<ExertionLogic>("differential/default_exert_logic")
        .cloned()
    {
        trace.set_exert_logic(logic);
    }
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
        const INPUT_EVENTS_PER_TURN: usize = 32;
        loop {
            let mut event = tokio::select! {
                biased;
                _ = input.ready(), if !upper.is_empty() => input.next_sync(),
                _ = notify.notified() => None,
            };
            let mut next_upper = None;
            // Extra exertion can introduce virtual batches and change subsequent
            // merge work. Amortize it over queued input, with a finite scheduling
            // quantum so other operators still get a turn. The latest observed
            // frontier suffices for sealing, including data read after it.
            for index in 0..INPUT_EVENTS_PER_TURN {
                match event.take() {
                    Some(Event::Data(time, mut data)) => {
                        super::metrics::record(
                            super::metrics::Stage::AsyncInput,
                            columnar::Len::len(&data.borrow()),
                            0,
                        );
                        if cap.as_ref().is_none_or(|old| time.time() < old.time()) {
                            cap = Some(time);
                        }
                        chunker.push_into(&mut data);
                        while let Some(chunk) = chunker.extract() {
                            batcher.push(std::mem::take(chunk)).await;
                        }
                    }
                    Some(Event::Progress(next)) => next_upper = Some(next),
                    None => break,
                }
                if index + 1 < INPUT_EVENTS_PER_TURN {
                    event = input.next_sync();
                }
            }
            if let Some(next) = next_upper {
                while let Some(chunk) = chunker.finish() {
                    batcher.push(std::mem::take(chunk)).await;
                }
                if next != upper {
                    let (chunks, description) = batcher.seal(next.clone()).await;
                    let batch = Rc::new(ChunkBatch {
                        chunks,
                        description,
                    });
                    writer.insert(Rc::clone(&batch), cap.as_ref().map(|c| c.time().clone()));
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
            maintain(&state, &notify).await;
            tokio::task::yield_now().await;
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
mod operator_tests;

#[cfg(test)]
mod tests {
    use super::*;
    use columnar::{Index, Len};
    use differential_dataflow::trace::Batcher as SyncBatcher;
    use differential_dataflow::trace::chunk::ChunkBatcher;
    use differential_dataflow::trace::{BatchReader, Description, TraceReader};
    use mz_ore::pool::Pool;
    use timely::dataflow::operators::generic::OperatorInfo;

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
            let budget = ReadBudget::new(1 << 20);
            let mut spine =
                Spine::<(u64, Vec<u8>), u64, i64>::with_budget(info.clone(), budget.clone());
            spine.set_exert_logic(Arc::new(|levels| {
                (levels.iter().map(|(_, n, _)| n).sum::<usize>() > 1).then_some(100)
            }));
            let notify = Arc::clone(&spine.notify);
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

            maintain(&state, &notify).await;
            assert_eq!(
                reader
                    .batches_through(Antichain::new().borrow())
                    .unwrap()
                    .len(),
                2,
                "another reader still holds the original cut"
            );
            hold.set_logical_compaction(frontier.borrow());
            hold.set_physical_compaction(frontier.borrow());
            let mut maintenance = Box::pin(maintain(&state, &notify));
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
            assert!(
                merged.is_empty(),
                "advancement cancels opposite diffs across batches"
            );
            let mut upper = Antichain::new();
            reader.read_upper(&mut upper);
            assert_eq!(upper, frontier);
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

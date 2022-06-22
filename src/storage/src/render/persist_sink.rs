// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::any::Any;
use std::rc::Rc;
use std::sync::Arc;

use differential_dataflow::{lattice::Lattice, Collection, Hashable};
use timely::dataflow::channels::pact::Exchange;
use timely::dataflow::operators::generic::builder_rc::OperatorBuilder;
use timely::dataflow::Scope;
use timely::progress::frontier::Antichain;
use timely::progress::Timestamp as _;
use timely::PartialOrder;

use mz_dataflow_types::client::controller::storage::CollectionMetadata;
use mz_dataflow_types::sources::SourceData;
use mz_dataflow_types::DataflowError;
use mz_persist_client::batch::BatchBuilder;
use mz_repr::{Diff, GlobalId, Row, Timestamp};

use mz_timely_util::operators_async_ext::OperatorBuilderExt;

use crate::storage_state::StorageState;

enum SinkState<K, V, T: mz_persist_types::Codec64 + timely::progress::Timestamp + Lattice, D> {
    Empty,
    NewBatch {
        batch: BatchBuilder<K, V, T, D>,
        batch_upper: Antichain<T>,
        fresh: bool,
    },
    ClosingBatchAndNewBatch {
        closing_batch: BatchBuilder<K, V, T, D>,
        final_batch_upper: Antichain<T>,
        batch: BatchBuilder<K, V, T, D>,
        batch_upper: Antichain<T>,
        fresh: bool,
    },
}

impl<K, V, T: mz_persist_types::Codec64 + timely::progress::Timestamp + Lattice, D>
    SinkState<K, V, T, D>
{
    fn batch(self) -> (BatchBuilder<K, V, T, D>, Antichain<T>) {
        match self {
            SinkState::NewBatch {
                batch, batch_upper, ..
            } => (batch, batch_upper),
            SinkState::ClosingBatchAndNewBatch {
                batch, batch_upper, ..
            } => (batch, batch_upper),
            SinkState::Empty => panic!(),
        }
    }

    fn closed_batch(
        self,
    ) -> (
        BatchBuilder<K, V, T, D>,
        Antichain<T>,
        BatchBuilder<K, V, T, D>,
        Antichain<T>,
    ) {
        match self {
            SinkState::ClosingBatchAndNewBatch {
                batch,
                batch_upper,
                closing_batch,
                final_batch_upper,
                ..
            } => (batch, batch_upper, closing_batch, final_batch_upper),
            _ => panic!(),
        }
    }
}

pub fn render<G>(
    scope: &mut G,
    src_id: GlobalId,
    storage_metadata: CollectionMetadata,
    source_data: Collection<G, Result<Row, DataflowError>, Diff>,
    storage_state: &mut StorageState,
    token: Rc<dyn Any>,
) where
    G: Scope<Timestamp = Timestamp>,
{
    let operator_name = format!("persist_sink({})", storage_metadata.persist_shard);
    let mut persist_op = OperatorBuilder::new(operator_name, scope.clone());

    // We want exactly one worker (in the cluster) to send all the data to persist. It's fine
    // if other workers from replicated clusters write the same data, though. In the real
    // implementation, we would use a storage client that transparently handles writing to
    // multiple shards. One shard would then only be written to by one worker but we get
    // parallelism from the sharding.
    // TODO(aljoscha): Storage must learn to keep track of collections, which consist of
    // multiple persist shards. Then we should set it up such that each worker can write to one
    // shard.
    let hashed_id = src_id.hashed();
    let active_write_worker = (hashed_id as usize) % scope.peers() == scope.index();

    let mut input = persist_op.new_input(&source_data.inner, Exchange::new(move |_| hashed_id));

    let shared_frontier = Rc::clone(&storage_state.source_uppers[&src_id]);

    let weak_token = Rc::downgrade(&token);

    let persist_clients = Arc::clone(&storage_state.persist_clients);
    persist_op.build_async(
        scope.clone(),
        move |mut capabilities, frontiers, scheduler| async move {
            capabilities.clear();
            let mut buffer = Vec::new();

            let mut sink_state = SinkState::Empty;

            let mut write = persist_clients
                .lock()
                .await
                .open(storage_metadata.persist_location)
                .await
                .expect("could not open persist client")
                .open_writer::<SourceData, (), Timestamp, Diff>(storage_metadata.persist_shard)
                .await
                .expect("could not open persist shard");

            while scheduler.notified().await {
                let input_frontier = frontiers.borrow()[0].clone();

                if !active_write_worker
                    || weak_token.upgrade().is_none()
                    || shared_frontier.borrow().is_empty()
                {
                    return;
                }

                while let Some((_cap, data)) = input.next() {
                    data.swap(&mut buffer);

                    // TODO: come up with a better default batch size here
                    // (100 was chosen arbitrarily), and avoid having to make a batch
                    // per-timestamp.
                    for (row, ts, diff) in buffer.drain(..) {
                        let (batch, upper) = match &mut sink_state {
                            SinkState::Empty => {
                                sink_state = SinkState::NewBatch {
                                    // TODO: the lower has to be the min because we don't know
                                    // what the minimum ts of data we will see is. In the future,
                                    // this lower should be declared in `finish` instead.
                                    batch: write
                                        .builder(100, Antichain::from_elem(Timestamp::minimum())),
                                    batch_upper: Antichain::from_elem(Timestamp::minimum()),
                                    fresh: true,
                                };

                                if let SinkState::NewBatch {
                                    batch,
                                    batch_upper,
                                    fresh,
                                } = &mut sink_state
                                {
                                    *fresh = false;
                                    (batch, batch_upper)
                                } else {
                                    unreachable!()
                                }
                            }
                            SinkState::NewBatch {
                                batch,
                                batch_upper,
                                fresh,
                            } => {
                                *fresh = false;
                                (batch, batch_upper)
                            }
                            SinkState::ClosingBatchAndNewBatch {
                                closing_batch,
                                final_batch_upper,
                                batch,
                                batch_upper,
                                fresh,
                            } => {
                                if PartialOrder::less_than(
                                    &Antichain::from_elem(ts),
                                    final_batch_upper,
                                ) {
                                    (closing_batch, final_batch_upper)
                                } else {
                                    *fresh = false;
                                    (batch, batch_upper)
                                }
                            }
                        };

                        batch
                            .add(&SourceData(row), &(), &ts, &diff)
                            .await
                            .expect("invalid usage");
                        // ANTICHAIN
                        *upper = Lattice::join(&Antichain::from_elem(ts + 1), upper);
                    }
                }

                // See if any timestamps are done!
                // If the frontier has advanced, we need to finalize data being written to persist
                if PartialOrder::less_than(&*shared_frontier.borrow(), &input_frontier) {
                    let possible_batch = match &mut sink_state {
                        SinkState::Empty | SinkState::NewBatch { fresh: true, .. } => {
                            // TODO ADVANCE EMPTY
                            (None, None)
                        }
                        SinkState::NewBatch { .. } => {
                            // if our batch is already past the frontier, then we change the
                            // sink state only, otherwise we return the current batch to be
                            // finalized and closed.
                            let current_batch =
                                std::mem::replace(&mut sink_state, SinkState::Empty);
                            let (batch, batch_upper) = current_batch.batch();

                            if PartialOrder::less_than(&input_frontier, &batch_upper) {
                                sink_state = SinkState::ClosingBatchAndNewBatch {
                                    closing_batch: batch,
                                    final_batch_upper: batch_upper,
                                    batch: write
                                        .builder(100, Antichain::from_elem(Timestamp::minimum())),
                                    batch_upper: Antichain::from_elem(Timestamp::minimum()),
                                    fresh: true,
                                };
                                (None, None)
                            } else {
                                (Some((batch, batch_upper)), None)
                            }
                        }
                        SinkState::ClosingBatchAndNewBatch { fresh, .. } => {
                            // if our batch is already past the frontier, then we change the
                            // sink state only, otherwise we return the current batch to be
                            // finalized and closed.

                            let fresh = *fresh;
                            let current_batch =
                                std::mem::replace(&mut sink_state, SinkState::Empty);

                            let (batch, batch_upper, closing_batch, final_batch_upper) =
                                current_batch.closed_batch();

                            if !fresh && PartialOrder::less_equal(&batch_upper, &input_frontier) {
                                // If the frontier is pass the batch_upper, then its pass the
                                // previous batch as well
                                sink_state = SinkState::Empty;
                                (
                                    Some((batch, batch_upper)),
                                    Some((closing_batch, final_batch_upper)),
                                )
                            } else if PartialOrder::less_equal(&final_batch_upper, &input_frontier)
                            {
                                sink_state = SinkState::NewBatch {
                                    batch,
                                    batch_upper,
                                    fresh,
                                };
                                (None, Some((closing_batch, final_batch_upper)))
                            } else {
                                // NEITHER is ahead, we just continue ahead

                                sink_state = SinkState::ClosingBatchAndNewBatch {
                                    closing_batch,
                                    final_batch_upper,
                                    batch,
                                    batch_upper,
                                    fresh,
                                };
                                (None, None)
                            }
                        }
                    };

                    match possible_batch {
                        (None, None) => {
                            // We always append, even in case we don't have any updates, because appending
                            // also advances the frontier.
                            let expected_upper = shared_frontier.borrow().clone();
                            write
                                .compare_and_append(
                                    Vec::<((SourceData, ()), Timestamp, Diff)>::new(),
                                    expected_upper,
                                    input_frontier.clone(),
                                )
                                .await
                                .expect("cannot append updates")
                                .expect("cannot append updates")
                                .expect("invalid/outdated upper");

                            // advance our stashed frontier
                            *shared_frontier.borrow_mut() = input_frontier.clone();
                        }
                        (None, Some((batch, _batch_upper))) => {
                            let mut batch = batch
                                .finish(input_frontier.clone())
                                .await
                                .expect("invalid usage");
                            let expected_upper = shared_frontier.borrow().clone();
                            write
                                .compare_and_append_batch(
                                    &mut batch,
                                    expected_upper,
                                    input_frontier.clone(),
                                )
                                .await
                                .expect("cannot append updates")
                                .expect("cannot append updates")
                                .expect("invalid/outdated upper");

                            // advance our stashed frontier
                            *shared_frontier.borrow_mut() = input_frontier.clone();
                        }
                        (Some((batch, _batch_upper)), closing_batch) => {
                            // TODO(aljoscha): Figure out how errors from this should be reported.
                            //
                            let expected_upper =
                                if let Some((closing_batch, final_batch_upper)) = closing_batch {
                                    let mut batch = closing_batch
                                        .finish(final_batch_upper.clone())
                                        .await
                                        .expect("invalid usage");
                                    let expected_upper = shared_frontier.borrow().clone();
                                    write
                                        .compare_and_append_batch(
                                            &mut batch,
                                            expected_upper,
                                            final_batch_upper.clone(),
                                        )
                                        .await
                                        .expect("cannot append updates")
                                        .expect("cannot append updates")
                                        .expect("invalid/outdated upper");

                                    final_batch_upper
                                } else {
                                    shared_frontier.borrow().clone()
                                };

                            let mut batch = batch
                                .finish(input_frontier.clone())
                                .await
                                .expect("invalid usage");
                            write
                                .compare_and_append_batch(
                                    &mut batch,
                                    expected_upper,
                                    input_frontier.clone(),
                                )
                                .await
                                .expect("cannot append updates")
                                .expect("cannot append updates")
                                .expect("invalid/outdated upper");

                            // advance our stashed frontier
                            *shared_frontier.borrow_mut() = input_frontier.clone();
                        }
                    }
                } else {
                    // We cannot have updates without the frontier advancing
                    // TODO HOW TO DO THIS CHECK
                    //assert!(finalized_timestamps.is_empty());
                }
            }
        },
    )
}

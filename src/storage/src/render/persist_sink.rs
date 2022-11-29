// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::any::Any;
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Duration;

use differential_dataflow::{Collection, Hashable};
use timely::dataflow::channels::pact::Exchange;
use timely::dataflow::operators::generic::builder_rc::OperatorBuilder;
use timely::dataflow::Scope;
use timely::progress::frontier::Antichain;
use timely::progress::Timestamp as _;
use timely::PartialOrder;
use tracing::trace;

use mz_repr::{Diff, GlobalId, Row, Timestamp};
use mz_storage_client::controller::CollectionMetadata;
use mz_storage_client::types::errors::DataflowError;
use mz_storage_client::types::sources::SourceData;
use mz_timely_util::activator::LimitingActivator;
use mz_timely_util::operators_async_ext::OperatorBuilderExt;

use crate::source::types::SourcePersistSinkMetrics;
use crate::storage_state::StorageState;

struct BatchBuilderAndCounts<K, V, T, D>
where
    T: timely::progress::Timestamp
        + differential_dataflow::lattice::Lattice
        + mz_persist_types::Codec64,
{
    builder: mz_persist_client::batch::BatchBuilder<K, V, T, D>,
    inserts: u64,
    retractions: u64,
    error_inserts: u64,
    error_retractions: u64,
}

impl<K, V, T, D> BatchBuilderAndCounts<K, V, T, D>
where
    T: timely::progress::Timestamp
        + differential_dataflow::lattice::Lattice
        + mz_persist_types::Codec64,
{
    fn new(bb: mz_persist_client::batch::BatchBuilder<K, V, T, D>) -> Self {
        BatchBuilderAndCounts {
            builder: bb,
            inserts: 0,
            retractions: 0,
            error_inserts: 0,
            error_retractions: 0,
        }
    }
}

pub fn render<G>(
    scope: &mut G,
    src_id: GlobalId,
    metadata: CollectionMetadata,
    source_data: Collection<G, Result<Row, DataflowError>, Diff>,
    storage_state: &mut StorageState,
    metrics: SourcePersistSinkMetrics,
    token: Rc<dyn Any>,
) where
    G: Scope<Timestamp = Timestamp>,
{
    let operator_name = format!("persist_sink({})", metadata.data_shard);
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

    let activator = scope.activator_for(&persist_op.operator_info().address[..]);
    let mut activator = LimitingActivator::new(activator);

    let mut input = persist_op.new_input(&source_data.inner, Exchange::new(move |_| hashed_id));

    let current_upper = Rc::clone(&storage_state.source_uppers[&src_id]);
    if !active_write_worker {
        // This worker is not writing, so make sure it's "taken out" of the
        // calculation by advancing to the empty frontier.
        current_upper.borrow_mut().clear();
    }

    let weak_token = Rc::downgrade(&token);

    let persist_clients = Arc::clone(&storage_state.persist_clients);
    persist_op.build_async(
        scope.clone(),
        move |mut capabilities, frontiers, scheduler| async move {
            capabilities.clear();
            let mut buffer = Vec::new();
            let mut stashed_batches = HashMap::new();

            let mut write = persist_clients
                .lock()
                .await
                .open(metadata.persist_location)
                .await
                .expect("could not open persist client")
                .open_writer::<SourceData, (), Timestamp, Diff>(
                    metadata.data_shard,
                    &format!("persist_sink::storage {}", src_id),
                )
                .await
                .expect("could not open persist shard");

            // Initialize this sink's `upper` to the `upper` of the persist shard we are writing
            // to. Data from the source not beyond this time will be dropped, as it has already
            // been persisted.
            // In the future, sources will avoid passing through data not beyond this upper
            if active_write_worker {
                // VERY IMPORTANT: Only the active write worker must change the
                // shared upper. All other workers have already cleared this
                // upper above.
                current_upper.borrow_mut().clone_from(write.upper());
            }

            while scheduler.notified().await {
                let input_upper = frontiers.borrow()[0].clone();

                if weak_token.upgrade().is_none() || current_upper.borrow().is_empty() {
                    return;
                }

                if !active_write_worker {
                    // We cannot simply return because that would block the
                    // frontier from advancing for the one active write worker.
                    continue;
                }

                // Make sure that our write handle lease does not expire!
                write.maybe_heartbeat_writer().await;
                trace!(
                    "storage persist_sink {}/{}: writer heartbeating write handle",
                    src_id,
                    metadata.data_shard
                );
                // NOTE: We schedule ourselves to make sure that we keep
                // heartbeating even in cases where there are no changes in our
                // inputs (updates or frontier changes).
                activator.activate_after(Duration::from_secs(60));

                while let Some((_cap, data)) = input.next() {
                    data.swap(&mut buffer);

                    // TODO: come up with a better default batch size here
                    // (100 was chosen arbitrarily), and avoid having to make a batch
                    // per-timestamp.
                    for (row, ts, diff) in buffer.drain(..) {
                        if write.upper().less_equal(&ts) {
                            let builder = stashed_batches.entry(ts).or_insert_with(|| {
                                BatchBuilderAndCounts::new(write.builder(
                                    // TODO: the lower has to be the min because we don't know
                                    // what the minimum ts of data we will see is. In the future,
                                    // this lower should be declared in `finish` instead.
                                    100,
                                    Antichain::from_elem(Timestamp::minimum()),
                                ))
                            });

                            let is_value = row.is_ok();
                            builder
                                .builder
                                .add(&SourceData(row), &(), &ts, &diff)
                                .await
                                .expect("invalid usage");

                            // Note that we assume `diff` is either +1 or -1 here, being anything
                            // else is a logic bug we can't handle at the metric layer. We also
                            // assume this addition doesn't overflow.
                            match (is_value, diff.is_positive()) {
                                (true, true) => builder.inserts += diff.unsigned_abs(),
                                (true, false) => builder.retractions += diff.unsigned_abs(),
                                (false, true) => builder.error_inserts += diff.unsigned_abs(),
                                (false, false) => builder.error_retractions += diff.unsigned_abs(),
                            }
                        }
                    }
                }

                // See if any timestamps are done!
                // TODO(guswynn/petrosagg): remove this additional allocation
                let mut finalized_timestamps: Vec<_> = stashed_batches
                    .keys()
                    .filter(|ts| !input_upper.less_equal(ts))
                    .copied()
                    .collect();
                finalized_timestamps.sort_unstable();

                // If the frontier has advanced, we need to finalize data being written to persist
                if PartialOrder::less_than(&*current_upper.borrow(), &input_upper) {
                    // We always append, even in case we don't have any updates, because appending
                    // also advances the frontier.
                    if finalized_timestamps.is_empty() {
                        let expected_upper = current_upper.borrow().clone();
                        write
                            .append(
                                Vec::<((SourceData, ()), Timestamp, Diff)>::new(),
                                expected_upper,
                                input_upper.clone(),
                            )
                            .await
                            .expect("cannot append updates")
                            .expect("invalid/outdated upper");

                        metrics
                            .progress
                            .set(mz_persist_client::metrics::encode_ts_metric(&input_upper));

                        // advance our stashed frontier
                        *current_upper.borrow_mut() = input_upper.clone();
                        // wait for more data or a new input frontier
                        continue;
                    }

                    // `current_upper` tracks the last known upper
                    let mut expected_upper = current_upper.borrow().clone();
                    let finalized_batch_count = finalized_timestamps.len();

                    for (i, ts) in finalized_timestamps.into_iter().enumerate() {
                        // TODO(aljoscha): Figure out how errors from this should be reported.

                        // Set the upper to the upper of the batch (which is 1 past the ts it
                        // manages) OR the new frontier if we are appending the final batch
                        let new_upper = if i == finalized_batch_count - 1 {
                            input_upper.clone()
                        } else {
                            Antichain::from_elem(ts.step_forward())
                        };

                        let batch_builder = stashed_batches
                            .remove(&ts)
                            .expect("batch for timestamp to still be there");

                        let mut batch = batch_builder
                            .builder
                            .finish(new_upper.clone())
                            .await
                            .expect("invalid usage");

                        write
                            .compare_and_append_batch(
                                &mut [&mut batch],
                                expected_upper,
                                new_upper.clone(),
                            )
                            .await
                            .expect("cannot append updates")
                            .expect("cannot append updates")
                            .expect("invalid/outdated upper");

                        metrics.processed_batches.inc();
                        metrics.row_inserts.inc_by(batch_builder.inserts);
                        metrics.row_retractions.inc_by(batch_builder.retractions);
                        metrics.error_inserts.inc_by(batch_builder.error_inserts);
                        metrics
                            .error_retractions
                            .inc_by(batch_builder.error_retractions);
                        metrics
                            .progress
                            .set(mz_persist_client::metrics::encode_ts_metric(&new_upper));

                        // next `expected_upper` is the one we just successfully appended
                        expected_upper = new_upper;
                    }
                    // advance our stashed frontier
                    *current_upper.borrow_mut() = input_upper.clone();
                } else {
                    // We cannot have updates without the frontier advancing
                    assert!(finalized_timestamps.is_empty());
                }
            }
        },
    )
}

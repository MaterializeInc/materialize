// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::any::Any;
use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;

use differential_dataflow::consolidation::consolidate_updates;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::{Collection, Hashable};
use futures::{pin_mut, StreamExt};
use timely::dataflow::channels::pact::Exchange;
use timely::dataflow::operators::generic::builder_rc::OperatorBuilder;
use timely::dataflow::Scope;
use timely::progress::Antichain;
use timely::progress::Timestamp as TimelyTimestamp;
use timely::PartialOrder;

use mz_persist_client::read::ListenEvent;
use mz_persist_client::write::WriteHandle;
use mz_persist_client::Upper;
use mz_repr::{Diff, GlobalId, Row, Timestamp};
use mz_storage::controller::CollectionMetadata;
use mz_storage::types::errors::DataflowError;
use mz_storage::types::sinks::{PersistSinkConnection, SinkDesc};
use mz_storage::types::sources::SourceData;
use mz_timely_util::operators_async_ext::OperatorBuilderExt;

use crate::compute_state::ComputeState;
use crate::render::sinks::SinkRender;

impl<G> SinkRender<G> for PersistSinkConnection<CollectionMetadata>
where
    G: Scope<Timestamp = Timestamp>,
{
    fn uses_keys(&self) -> bool {
        false
    }

    fn get_key_indices(&self) -> Option<&[usize]> {
        None
    }

    fn get_relation_key_indices(&self) -> Option<&[usize]> {
        None
    }

    fn render_continuous_sink(
        &self,
        compute_state: &mut ComputeState,
        _sink: &SinkDesc<CollectionMetadata>,
        sink_id: GlobalId,
        sinked_collection: Collection<G, (Option<Row>, Option<Row>), Diff>,
        err_collection: Collection<G, DataflowError, Diff>,
    ) -> Option<Rc<dyn Any>>
    where
        G: Scope<Timestamp = Timestamp>,
    {
        let collection = sinked_collection
            .map(|(key, value)| {
                assert!(key.is_none(), "persist_source does not support keys");
                let row = value.expect("persist_source must have values");
                Ok(row)
            })
            .concat(&err_collection.map(Err));

        persist_sink(sink_id, &self.storage_metadata, collection, compute_state)
    }
}

pub(crate) fn persist_sink<G>(
    sink_id: GlobalId,
    target: &CollectionMetadata,
    collection: Collection<G, Result<Row, DataflowError>, Diff>,
    compute_state: &mut ComputeState,
) -> Option<Rc<dyn Any>>
where
    G: Scope<Timestamp = Timestamp>,
{
    let scope = collection.scope();

    let persist_clients = Arc::clone(&compute_state.persist_clients);
    let persist_location = target.persist_location.clone();
    let shard_id = target.data_shard;

    let mut op = OperatorBuilder::new("persist_sink".into(), scope.clone());

    // TODO(mcsherry): this is shardable, eventually. But for now use a single writer.
    let hashed_id = sink_id.hashed();
    let active_worker = (hashed_id as usize) % scope.peers() == scope.index();
    let mut input = op.new_input(&collection.inner, Exchange::new(move |_| hashed_id));

    // Dropping this token signals that the operator should shut down cleanly.
    let token = Rc::new(());
    let token_weak = Rc::downgrade(&token);

    // Only the active_write_worker will ever produce data so all other workers have
    // an empty frontier. It's necessary to insert all of these into `compute_state.
    // sink_write_frontier` below so we properly clear out default frontiers of
    // non-active workers.
    let shared_frontier = Rc::new(RefCell::new(if active_worker {
        Antichain::from_elem(TimelyTimestamp::minimum())
    } else {
        Antichain::new()
    }));

    compute_state
        .sink_write_frontiers
        .insert(sink_id, Rc::clone(&shared_frontier));

    op.build_async(
        scope,
        move |_capabilities, frontiers, scheduler| async move {
            if !active_worker {
                return;
            }

            // TODO(aljoscha): We need to figure out what to do with error results from these calls.
            let persist_client = persist_clients
                .lock()
                .await
                .open(persist_location)
                .await
                .expect("could not open persist client");

            let (mut write, read) = persist_client
                .open::<SourceData, (), Timestamp, Diff>(shard_id)
                .await
                .expect("could not open persist shard");

            // Invariant: since <= read_frontier <= upper
            advance_upper_by(&mut write, read.since()).await;

            let upper_ts = match write.upper().get(0) {
                Some(ts) => ts,
                // Shard is already closed and cannot be written to anymore.
                None => return,
            };
            let mut read_ts = upper_ts.saturating_sub(1);
            read_ts.advance_by(read.since().borrow());
            let mut read_frontier = Antichain::from_elem(read_ts);

            // Share that we have finished processing all times less than the read frontier.
            // Advancing the sink upper communicates to the storage controller that it is
            // permitted to compact our target storage collection up to the new upper. So we
            // must be careful to not advance the sink upper beyond our read frontier.
            shared_frontier.borrow_mut().clone_from(&read_frontier);

            let as_of = read_frontier.clone();
            let shard_read = async_stream::stream!({
                if !active_worker {
                    return;
                }

                let mut snapshot = read
                    .snapshot(as_of.clone())
                    .await
                    .expect("cannot serve requested as_of");

                while let Some(next) = snapshot.next().await {
                    yield ListenEvent::Updates(next);
                }

                let mut listen = read
                    .listen(as_of)
                    .await
                    .expect("cannot serve requested as_of");

                loop {
                    for event in listen.next().await {
                        yield event;
                    }
                }
            });
            pin_mut!(shard_read);

            let mut buffer = Vec::new();
            // Contains the diff between the input collection and persist shard contents,
            // reflecting the updates we would like to write to the shard in order to "correct" it
            // to track the input collection.
            let mut correction = Vec::new();

            while scheduler.notified().await {
                if token_weak.upgrade().is_none() || shared_frontier.borrow().is_empty() {
                    return;
                }

                // Extract input rows as positive contributions to `correction`.
                input.for_each(|_cap, data| {
                    data.swap(&mut buffer);
                    correction.append(&mut buffer);
                });

                let input_frontier = frontiers.borrow()[0].clone();
                let mut write_frontier = write.upper().clone();

                loop {
                    // Catch up with reading.
                    while PartialOrder::less_than(&read_frontier, &write_frontier) {
                        match shard_read.next().await.unwrap() {
                            ListenEvent::Progress(upper) => read_frontier = upper,
                            ListenEvent::Updates(updates) => {
                                // Extract shard rows as negative contributions to `correction`.
                                let updates = updates.into_iter().map(|((key, _), time, diff)| {
                                    let data = key.expect("decoding failed");
                                    (data.0, time, -diff)
                                });
                                correction.extend(updates);
                            }
                        }
                    }

                    shared_frontier.borrow_mut().clone_from(&read_frontier);

                    if PartialOrder::less_equal(&input_frontier, &write_frontier) {
                        // We cannot write anything new. Need to wait for input progress.
                        break;
                    }

                    // Advance all updates to the write frontier.
                    for (_, time, _) in correction.iter_mut() {
                        time.advance_by(write_frontier.borrow());
                    }

                    consolidate_updates(&mut correction);

                    let to_append = correction
                        .iter()
                        .filter(|(_, time, _)| !input_frontier.less_equal(time))
                        .map(|(data, time, diff)| ((SourceData(data.clone()), ()), time, diff));

                    let result = write
                        .compare_and_append(
                            to_append,
                            write_frontier.clone(),
                            input_frontier.clone(),
                        )
                        .await
                        .expect("external durability failure")
                        .expect("invalid usage");

                    write_frontier = match result {
                        Ok(()) => input_frontier.clone(),
                        Err(Upper(upper)) => upper,
                    };
                }
            }
        },
    );

    Some(token)
}

async fn advance_upper_by(
    write: &mut WriteHandle<SourceData, (), Timestamp, Diff>,
    frontier: &Antichain<Timestamp>,
) {
    let empty_updates: &[((SourceData, ()), Timestamp, Diff)] = &[];
    while PartialOrder::less_than(write.upper(), frontier) {
        write
            .compare_and_append(empty_updates, write.upper().clone(), frontier.clone())
            .await
            .expect("external durability failure")
            .expect("invalid usage")
            .ok();
    }
}

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
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::Arc;

use differential_dataflow::consolidation::consolidate_updates;
use differential_dataflow::{Collection, Hashable};
use mz_persist_client::{PersistClient, ShardId};
use timely::dataflow::channels::pact::Exchange;
use timely::dataflow::operators::generic::builder_rc::OperatorBuilder;
use timely::dataflow::Scope;
use timely::progress::Antichain;
use timely::progress::Timestamp as TimelyTimestamp;
use timely::PartialOrder;

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
        let ok_collection = sinked_collection.map(|(key, value)| {
            assert!(key.is_none(), "persist_source does not support keys");
            value.expect("persist_source must have values")
        });

        persist_sink(
            sink_id,
            &self.storage_metadata,
            ok_collection,
            err_collection,
            compute_state,
            false,
        )
    }
}

pub(crate) fn persist_sink<G>(
    sink_id: GlobalId,
    target: &CollectionMetadata,
    ok_collection: Collection<G, Row, Diff>,
    err_collection: Collection<G, DataflowError, Diff>,
    compute_state: &mut ComputeState,
    truncate: bool,
) -> Option<Rc<dyn Any>>
where
    G: Scope<Timestamp = Timestamp>,
{
    let scope = ok_collection.scope();

    let persist_clients = Arc::clone(&compute_state.persist_clients);
    let persist_location = target.persist_location.clone();
    let shard_id = target.data_shard;

    let operator_name = format!("persist_sink({})", shard_id);
    let mut persist_op = OperatorBuilder::new(operator_name, scope.clone());

    // We want exactly one worker (in the cluster) to send all the data to persist. It's fine
    // if other workers from replicated clusters write the same data, though. In the real
    // implementation, we would use a storage client that transparently handles writing to
    // multiple shards. One shard would then only be written to by one worker but we get
    // parallelism from the sharding.
    // TODO(aljoscha): Storage must learn to keep track of collections, which consist of
    // multiple persist shards. Then we should set it up such that each worker can write to one
    // shard.
    let hashed_id = sink_id.hashed();
    let active_write_worker = (hashed_id as usize) % scope.peers() == scope.index();

    let mut input = persist_op.new_input(&ok_collection.inner, Exchange::new(move |_| hashed_id));
    let mut err_input =
        persist_op.new_input(&err_collection.inner, Exchange::new(move |_| hashed_id));

    let token = Rc::new(());
    let token_weak = Rc::downgrade(&token);

    // Only the active_write_worker will ever produce data so all other workers have
    // an empty frontier. It's necessary to insert all of these into `compute_state.
    // sink_write_frontier` below so we properly clear out default frontiers of
    // non-active workers.
    let shared_frontier = Rc::new(RefCell::new(if active_write_worker {
        Antichain::from_elem(TimelyTimestamp::minimum())
    } else {
        Antichain::new()
    }));

    compute_state
        .sink_write_frontiers
        .insert(sink_id, Rc::clone(&shared_frontier));

    // NOTE(aljoscha): It might be better to roll our own async operator that deals with
    // handling multiple in-flight write futures, similar to how the persist operators in
    // `operators/stream.rs` do it. That would allow us to have multiple write requests in
    // flight concurrently.
    persist_op.build_async(
        scope,
        move |_capabilities, frontiers, scheduler| async move {
            let mut buffer = Vec::new();
            let mut err_buffer = Vec::new();
            let mut stash: HashMap<_, Vec<_>> = HashMap::new();

            // TODO(aljoscha): We need to figure out what to do with error results from these calls.
            let persist_client = persist_clients
                .lock()
                .await
                .open(persist_location)
                .await
                .expect("could not open persist client");

            if truncate && active_write_worker {
                truncate_persist_shard(shard_id, &persist_client).await;
            }

            let mut write = persist_client
                .open_writer::<SourceData, (), Timestamp, Diff>(shard_id)
                .await
                .expect("could not open persist shard");

            while scheduler.notified().await {
                let mut input_frontier = Antichain::new();
                for frontier in frontiers.borrow().clone() {
                    input_frontier.extend(frontier);
                }

                if !active_write_worker
                    || token_weak.upgrade().is_none()
                    || shared_frontier.borrow().is_empty()
                {
                    return;
                }

                input.for_each(|_cap, data| {
                    data.swap(&mut buffer);

                    for (row, ts, diff) in buffer.drain(..) {
                        stash
                            .entry(ts)
                            .or_default()
                            .push(((SourceData(Ok(row)), ()), ts, diff));
                    }
                });

                err_input.for_each(|_cap, data| {
                    data.swap(&mut err_buffer);

                    for (error, ts, diff) in err_buffer.drain(..) {
                        stash
                            .entry(ts)
                            .or_default()
                            .push(((SourceData(Err(error)), ()), ts, diff));
                    }
                });

                let mut updates = stash
                    .iter()
                    .filter(|(ts, _updates)| !input_frontier.less_equal(ts))
                    .flat_map(|(_ts, updates)| updates.iter());

                if PartialOrder::less_than(&*shared_frontier.borrow(), &input_frontier) {
                    // We always append, even in case we don't have any updates, because appending
                    // also advances the frontier.
                    let lower = shared_frontier.borrow().clone();
                    // TODO(aljoscha): Figure out how errors from this should be reported.
                    write
                        .append(updates, lower, input_frontier.clone())
                        .await
                        .expect("cannot append updates")
                        .expect("invalid/outdated upper");

                    *shared_frontier.borrow_mut() = input_frontier.clone();
                } else {
                    // We cannot have updates without the frontier advancing.
                    assert!(updates.next().is_none());
                }

                stash.retain(|ts, _updates| input_frontier.less_equal(ts));
            }
        },
    );

    Some(token)
}

async fn truncate_persist_shard(shard_id: ShardId, persist_client: &PersistClient) {
    let (mut write, read) = persist_client
        .open::<SourceData, (), Timestamp, Diff>(shard_id)
        .await
        .expect("could not open persist shard");

    let upper = write.upper().clone();
    let upper_ts = upper[0];
    if let Some(ts) = upper_ts.checked_sub(1) {
        let as_of = Antichain::from_elem(ts);

        let mut snapshot_iter = read
            .snapshot(as_of)
            .await
            .expect("cannot serve requested as_of");

        let mut updates = Vec::new();
        while let Some(next) = snapshot_iter.next().await {
            updates.extend(next);
        }
        snapshot_iter.expire().await;

        consolidate_updates(&mut updates);

        let retractions = updates
            .into_iter()
            .map(|((k, v), _ts, diff)| ((k.unwrap(), v.unwrap()), upper_ts, diff * -1));

        let new_upper = Antichain::from_elem(upper_ts + 1);
        write
            .compare_and_append(retractions, upper, new_upper)
            .await
            .expect("external durability failure")
            .expect("invalid usage")
            .expect("unexpected upper");
    }

    write.expire().await;
    read.expire().await;
}

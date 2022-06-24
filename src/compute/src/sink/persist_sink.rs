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

use differential_dataflow::{Collection, Hashable};
use timely::dataflow::channels::pact::Exchange;
use timely::dataflow::operators::generic::builder_rc::OperatorBuilder;
use timely::dataflow::Scope;
use timely::progress::Antichain;
use timely::progress::Timestamp as TimelyTimestamp;
use timely::PartialOrder;

use mz_dataflow_types::client::controller::storage::CollectionMetadata;
use mz_dataflow_types::sinks::{PersistSinkConnection, SinkDesc};
use mz_repr::{Diff, GlobalId, Row, Timestamp};
use mz_timely_util::operators_async_ext::OperatorBuilderExt;

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
        compute_state: &mut crate::compute_state::ComputeState,
        _sink: &SinkDesc<CollectionMetadata>,
        sink_id: GlobalId,
        sinked_collection: Collection<G, (Option<Row>, Option<Row>), Diff>,
    ) -> Option<Rc<dyn Any>>
    where
        G: Scope<Timestamp = Timestamp>,
    {
        let scope = sinked_collection.scope();

        let persist_clients = Arc::clone(&compute_state.persist_clients);
        let persist_location = self.storage_metadata.persist_location.clone();
        let shard_id = self.storage_metadata.persist_shard;

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

        let mut input =
            persist_op.new_input(&sinked_collection.inner, Exchange::new(move |_| hashed_id));

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
                let mut stash: HashMap<_, Vec<_>> = HashMap::new();

                // TODO(aljoscha): We need to figure out what to do with error results from these calls.
                let mut write = persist_clients
                    .lock()
                    .await
                    .open(persist_location)
                    .await
                    .expect("could not open persist client")
                    .open_writer::<Row, Row, Timestamp, Diff>(shard_id)
                    .await
                    .expect("could not open persist shard");

                while scheduler.notified().await {
                    let input_frontier = frontiers.borrow()[0].clone();

                    if !active_write_worker
                        || token_weak.upgrade().is_none()
                        || shared_frontier.borrow().is_empty()
                    {
                        return;
                    }

                    input.for_each(|_cap, data| {
                        data.swap(&mut buffer);

                        for ((key, value), ts, diff) in buffer.drain(..) {
                            let key = key.unwrap_or_default();
                            let value = value.unwrap_or_default();
                            stash.entry(ts).or_default().push(((key, value), ts, diff));
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
}

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

use differential_dataflow::{Collection, Hashable};
use timely::dataflow::channels::pact::Exchange;
use timely::dataflow::operators::generic::builder_rc::OperatorBuilder;
use timely::dataflow::Scope;
use timely::progress::Antichain;
use timely::progress::Timestamp as TimelyTimestamp;

use mz_dataflow_types::sinks::{PersistSinkConnection, SinkDesc};
use mz_persist_client::PersistLocation;
use mz_repr::{Diff, GlobalId, Row, Timestamp};
use mz_timely_util::operators_async_ext::OperatorBuilderExt;

use crate::render::sinks::SinkRender;

impl<G> SinkRender<G> for PersistSinkConnection
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
        _sink: &SinkDesc,
        sink_id: GlobalId,
        sinked_collection: Collection<G, (Option<Row>, Option<Row>), Diff>,
    ) -> Option<Rc<dyn Any>>
    where
        G: Scope<Timestamp = Timestamp>,
    {
        let scope = sinked_collection.scope();
        let operator_name = format!("persist_sink({})", self.shard_id);
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

        let persist_location = PersistLocation {
            consensus_uri: self.consensus_uri.clone(),
            blob_uri: self.blob_uri.clone(),
        };
        let shard_id = self.shard_id.clone();

        let token = Rc::new(());
        let token_weak = Rc::downgrade(&token);

        // Only the active_write_worker will ever produce data so all other workers have
        // an empty frontier.  It's necessary to insert all of these into `storage_state.
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
                let mut stash: HashMap<Timestamp, Vec<((Row, Row), Timestamp, Diff)>> =
                    HashMap::new();

                // Keep an empty Row that we can re-use when we are replacing an Option with an empty
                // Row. Our output type is `(Row, Row)` instead of `(Option<Row>, Option<Row>)`.
                let empty_row = Row::default();

                // TODO(aljoscha): We need to figure out what to do with error results from these calls.
                let persist_client = persist_location
                    .open()
                    .await
                    .expect("cannot open persist client");

                let mut write = persist_client
                    .open_writer::<Row, Row, Timestamp, Diff>(shard_id)
                    .await
                    .expect("could not open persist shard");

                while scheduler.notified().await {
                    let frontier = frontiers.borrow()[0].clone();

                    if !active_write_worker || token_weak.upgrade().is_none() {
                        return;
                    }

                    input.for_each(|_cap, data| {
                        data.swap(&mut buffer);

                        let mapped_updates = buffer.drain(..).map(|((key, value), ts, diff)| {
                            let key = key.unwrap_or_else(|| empty_row.clone());
                            let value = value.unwrap_or_else(|| empty_row.clone());
                            ((key, value), ts, diff)
                        });

                        for update in mapped_updates {
                            stash.entry(update.1).or_default().push(update);
                        }
                    });

                    let updates = stash
                        .iter()
                        .filter(|(ts, _updates)| !frontier.less_equal(ts))
                        .flat_map(|(_ts, updates)| updates.iter())
                        .map(|((key, value), ts, diff)| ((key, value), ts, diff));

                    // We always append, even in case we don't have any updates, because appending
                    // also advances the frontier.
                    // TODO(aljoscha): Figure out how errors from this should be reported.
                    write
                        .append(updates, write.upper().clone(), frontier.clone())
                        .await
                        .expect("cannot append updates")
                        .expect("invalid/outdated upper");

                    stash.retain(|ts, _updates| frontier.less_equal(ts));

                    let mut shared_frontier = shared_frontier.borrow_mut();
                    shared_frontier.clear();
                    shared_frontier.extend(frontier.iter().cloned());
                }
            },
        );

        Some(token)
    }
}

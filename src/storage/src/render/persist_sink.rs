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

use differential_dataflow::{Collection, Hashable};
use timely::dataflow::channels::pact::Exchange;
use timely::dataflow::operators::generic::builder_rc::OperatorBuilder;
use timely::dataflow::Scope;
use timely::PartialOrder;

use mz_dataflow_types::client::controller::storage::CollectionMetadata;
use mz_dataflow_types::sources::SourceData;
use mz_dataflow_types::DataflowError;
use mz_repr::{Diff, GlobalId, Row, Timestamp};
use mz_timely_util::operators_async_ext::OperatorBuilderExt;

use crate::storage_state::StorageState;

pub fn render<G>(
    scope: &mut G,
    src_id: GlobalId,
    storage_metadata: CollectionMetadata,
    source_data: Collection<G, Result<Row, DataflowError>, Diff>,
    storage_state: &mut StorageState,
    token: Arc<dyn Any + Send + Sync>,
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

    let weak_token = Arc::downgrade(&token);

    persist_op.build_async(
        scope.clone(),
        move |mut capabilities, frontiers, scheduler| async move {
            capabilities.clear();
            let mut buffer = Vec::new();
            let mut stash: HashMap<_, Vec<_>> = HashMap::new();

            let mut write = crate::persist_cache::open_writer::<
                SourceData,
                (),
                mz_repr::Timestamp,
                mz_repr::Diff,
            >(
                storage_metadata.persist_location,
                storage_metadata.persist_shard,
            )
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

                    for (row, ts, diff) in buffer.drain(..) {
                        stash
                            .entry(ts)
                            .or_default()
                            .push((SourceData(row), ts, diff));
                    }
                }

                let empty = Vec::new();
                let mut updates = stash
                    .iter()
                    .flat_map(|(ts, updates)| {
                        if !input_frontier.less_equal(ts) {
                            updates.iter()
                        } else {
                            empty.iter()
                        }
                    })
                    .map(|&(ref row, ref ts, ref diff)| ((row, &()), ts, diff));

                if PartialOrder::less_than(&*shared_frontier.borrow(), &input_frontier) {
                    // We always append, even in case we don't have any updates, because appending
                    // also advances the frontier.
                    // TODO(aljoscha): Figure out how errors from this should be reported.
                    let expected_upper = shared_frontier.borrow().clone();
                    write
                        .compare_and_append(updates, expected_upper, input_frontier.clone())
                        .await
                        .expect("cannot append updates")
                        .expect("cannot append updates")
                        .expect("invalid/outdated upper");

                    *shared_frontier.borrow_mut() = input_frontier.clone();
                } else {
                    // We cannot have updates without the frontier advancing
                    assert!(updates.next().is_none());
                }

                stash.retain(|ts, _updates| input_frontier.less_equal(ts));
            }
        },
    )
}

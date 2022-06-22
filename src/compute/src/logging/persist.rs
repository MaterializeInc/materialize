// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::HashMap;

use differential_dataflow::Collection;
use timely::dataflow::channels::pact::Exchange;
use timely::dataflow::operators::generic::builder_rc::OperatorBuilder;
use timely::dataflow::Scope;

use mz_dataflow_types::client::controller::storage::CollectionMetadata;
use mz_repr::{Diff, Row, Timestamp};
use mz_timely_util::operators_async_ext::OperatorBuilderExt;

// TODO(teskje): remove code duplication with `PersistSinkConnector`
pub(crate) fn persist_sink<G>(
    target: &CollectionMetadata,
    collection: &Collection<G, (Row, Row), Diff>,
) where
    G: Scope<Timestamp = Timestamp>,
{
    let scope = collection.scope();
    // TODO(teskje): vary active_worker_index, to distribute work for multiple sinks
    let active_worker_index = 0;

    let write = if active_worker_index == scope.index() {
        let shard_id = target.persist_shard;
        let persist_client =
            futures_executor::block_on(target.persist_location.open()).expect("cannot open client");
        let (write, read) =
            futures_executor::block_on(persist_client.open::<Row, Row, Timestamp, Diff>(shard_id))
                .expect("could not open persist shard");

        // TODO(teskje): use `open_write`
        futures_executor::block_on(read.expire());
        Some(write)
    } else {
        None
    };

    let mut sink = OperatorBuilder::new("Logging Persist Sink".into(), scope.clone());
    let mut input = sink.new_input(
        &collection.inner,
        Exchange::new(move |_| active_worker_index as u64),
    );

    sink.build_async(
        scope,
        move |_capabilities, frontiers, scheduler| async move {
            let mut write = match write {
                Some(w) => w,
                None => return,
            };

            let mut buffer = Vec::new();
            let mut stash = HashMap::<_, Vec<_>>::new();

            while scheduler.notified().await {
                let frontier = &frontiers.borrow()[0];

                input.for_each(|_cap, data| {
                    data.swap(&mut buffer);
                    for update in buffer.drain(..) {
                        let ts = update.1;
                        stash.entry(ts).or_default().push(update);
                    }
                });

                let updates = stash
                    .iter()
                    .filter(|(ts, _updates)| !frontier.less_equal(ts))
                    .flat_map(|(_ts, updates)| updates.iter());

                write
                    .append(updates, write.upper().clone(), frontier.clone())
                    .await
                    .expect("cannot append updates")
                    .expect("cannot append updates");

                stash.retain(|ts, _updates| frontier.less_equal(ts));
            }
        },
    );

    // TODO(teskje): return token
}

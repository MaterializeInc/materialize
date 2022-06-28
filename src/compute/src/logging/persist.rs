// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
use std::sync::Arc;

use differential_dataflow::Collection;
use mz_dataflow_types::sources::SourceData;
use mz_persist_client::cache::PersistClientCache;
use mz_persist_client::write::WriteHandle;
use timely::dataflow::channels::pact::Exchange;
use timely::dataflow::operators::generic::builder_rc::OperatorBuilder;
use timely::dataflow::Scope;
use timely::PartialOrder;

use mz_dataflow_types::client::controller::storage::CollectionMetadata;
use mz_repr::{Diff, Row, Timestamp};
use mz_timely_util::operators_async_ext::OperatorBuilderExt;
use timely::progress::Antichain;
use timely::progress::Timestamp as TimelyTimestamp;
use tokio::sync::Mutex;

// TODO(teskje): remove code duplication with `PersistSinkConnection::render_continous_sink`
pub(crate) fn persist_sink<G>(
    target: &CollectionMetadata,
    persist_clients: Arc<Mutex<PersistClientCache>>,
    collection: &Collection<G, (Row, Row), Diff>,
) where
    G: Scope<Timestamp = Timestamp>,
{
    let scope = collection.scope();
    // TODO(teskje): vary active_worker_index, to distribute work for multiple sinks
    let active_worker_index = 0;

    let write: Option<WriteHandle<_, _, _, _>> = if active_worker_index == scope.index() {
        let shard_id = target.persist_shard;
        let location = target.persist_location.clone();
        let persist_client =
            futures_executor::block_on(async { persist_clients.lock().await.open(location).await });

        let persist_client = persist_client.expect("Successful connection");

        let (write, read) = futures_executor::block_on(
            persist_client.open::<SourceData, (), Timestamp, Diff>(shard_id),
        )
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

    let shared_frontier = Rc::new(RefCell::new(Antichain::from_elem(
        TimelyTimestamp::minimum(),
    )));

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
                input.for_each(|_cap, data| {
                    data.swap(&mut buffer);
                    for ((key, _value), ts, diff) in buffer.drain(..) {
                        tracing::trace!(
                            "Persisting introspection update {:?}",
                            ((&key, &_value), ts, diff)
                        );
                        stash
                            .entry(ts)
                            .or_default()
                            .push(((SourceData(Ok(key)), ()), ts, diff));
                    }
                });

                let input_frontier = &frontiers.borrow()[0].clone();
                let mut updates = stash
                    .iter()
                    .filter(|(ts, _updates)| !input_frontier.less_equal(ts))
                    .flat_map(|(_ts, updates)| updates.iter());

                if PartialOrder::less_than(&*shared_frontier.borrow(), &input_frontier) {
                    let lower = shared_frontier.borrow().clone();

                    write
                        .append(updates, lower, input_frontier.clone())
                        .await
                        .expect("cannot append updates")
                        .expect("cannot append updates");

                    *shared_frontier.borrow_mut() = input_frontier.clone();
                } else {
                    assert!(updates.next().is_none());
                }

                stash.retain(|ts, _updates| input_frontier.less_equal(ts));
            }
        },
    );

    // TODO(teskje): return token
}

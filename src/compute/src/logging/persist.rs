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

use differential_dataflow::Collection;
use mz_persist_client::write::WriteHandle;
use mz_repr::GlobalId;
use mz_storage::controller::CollectionMetadata;
use mz_storage::types::sources::SourceData;
use timely::dataflow::channels::pact::Exchange;
use timely::dataflow::operators::generic::builder_rc::OperatorBuilder;
use timely::dataflow::Scope;
use timely::PartialOrder;

use mz_repr::{Diff, Row, Timestamp};
use mz_timely_util::operators_async_ext::OperatorBuilderExt;
use timely::progress::Antichain;
use timely::progress::Timestamp as TimelyTimestamp;

use crate::compute_state::ComputeState;

// TODO(teskje): remove code duplication with `PersistSinkConnection::render_continous_sink`
pub(crate) fn persist_sink<G>(
    target_id: GlobalId,
    target: &CollectionMetadata,
    compute_state: &mut ComputeState,
    collection: &Collection<G, Row, Diff>,
) where
    G: Scope<Timestamp = Timestamp>,
{
    let scope = collection.scope();
    // TODO(teskje): vary active_worker_index, to distribute work for multiple sinks
    let active_worker_index = 0;

    let location = target.persist_location.clone();

    let handles: Option<(WriteHandle<_, _, _, _>, _)> = if active_worker_index == scope.index() {
        let shard_id = target.data_shard;
        let persist_client = futures_executor::block_on(async {
            compute_state
                .persist_clients
                .lock()
                .await
                .open(location)
                .await
        });

        let persist_client = persist_client.expect("Successful connection");

        let (write, read) = futures_executor::block_on(
            persist_client.open::<SourceData, (), Timestamp, Diff>(shard_id),
        )
        .expect("could not open persist shard");

        Some((write, read))
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

    compute_state
        .reported_frontiers
        .insert(target_id, Antichain::from_elem(0));

    compute_state
        .sink_write_frontiers
        .insert(target_id, Rc::clone(&shared_frontier));

    sink.build_async(
        scope,
        move |_capabilities, frontiers, scheduler| async move {
            let (mut write, read) = match handles {
                Some(w) => w,
                None => return,
            };

            // Delete existing data in shard, by reading at recent_upper - 1
            // and writing -1s at recent_upper.
            let recent_upper = write.fetch_recent_upper().await;
            // If this is true, we can obtain a snapshot. Otherwise, we assume the shard is empty
            if recent_upper[0] > 0 {
                let mut initial_flush: Vec<((SourceData, _), _, i64)> = vec![];
                // Flush the shard
                let since = Antichain::from_elem(recent_upper[0] - 1);
                match read.snapshot(since).await {
                    Ok(mut x) => {
                        while let Some(result_vec) = x.next().await {
                            for result in result_vec {
                                let sd = result.0 .0.unwrap();
                                let mult = result.2;
                                initial_flush.push(((sd, ()), recent_upper[0], -mult));
                            }
                        }
                    }
                    Err(_err) => {
                        tracing::warn!("Could not flush logging shard!");
                    }
                };

                // Use smallest possible new_upper
                let new_upper = Antichain::from_elem(recent_upper[0] + 1);
                write
                    .append(initial_flush.into_iter(), recent_upper, new_upper)
                    .await
                    .expect("cannot append updates (1) ")
                    .expect("cannot append updates (2)");
            }

            let mut buffer = Vec::new();
            let mut stash = HashMap::<_, Vec<_>>::new();

            while scheduler.notified().await {
                input.for_each(|_cap, data| {
                    data.swap(&mut buffer);
                    for (key, ts, diff) in buffer.drain(..) {
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
}

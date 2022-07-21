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

use differential_dataflow::AsCollection;
use differential_dataflow::Collection;
use differential_dataflow::Hashable;
use timely::dataflow::channels::pact::Exchange;
use timely::dataflow::operators::generic::builder_rc::OperatorBuilder;
use timely::dataflow::Scope;
use timely::progress::Antichain;
use timely::progress::Timestamp as TimelyTimestamp;
use timely::PartialOrder;

use mz_repr::GlobalId;
use mz_repr::{Diff, Row, Timestamp};
use mz_storage::controller::CollectionMetadata;
use mz_storage::types::errors::DataflowError;
use mz_storage::types::sinks::SinkAsOf;
use mz_storage::types::sources::SourceData;
use mz_timely_util::operators_async_ext::OperatorBuilderExt;

use crate::compute_state::ComputeState;

pub(crate) fn persist_sink<G>(
    target_id: GlobalId,
    target: &CollectionMetadata,
    compute_state: &mut ComputeState,
    desired_collection: &Collection<G, Row, Diff>,
) where
    G: Scope<Timestamp = Timestamp>,
{
    let scope = desired_collection.scope();
    let as_of: SinkAsOf = SinkAsOf {
        frontier: Antichain::from_elem(0),
        strict: false,
    };

    // To create the diffs we also need to read from persist.
    let (ok_stream, err_stream, token) = mz_storage::source::persist_source::persist_source(
        &scope,
        Arc::clone(&compute_state.persist_clients),
        target.clone(),
        as_of.frontier.clone(),
    );
    let persist_collection = ok_stream
        .as_collection()
        .map(Ok)
        .concat(&err_stream.as_collection().map(Err));

    let token = Rc::new((
        install_desired_into_persist(
            target,
            compute_state,
            as_of,
            target_id,
            desired_collection.map(Ok),
            persist_collection,
        ),
        token,
    ));

    // Report frontier of collection back to coord
    compute_state
        .reported_frontiers
        .insert(target_id, Antichain::from_elem(0));

    // We don't allow these dataflows to be dropped, so the tokens could
    // be stored anywhere.
    compute_state.sink_tokens.insert(
        target_id,
        crate::compute_state::SinkToken {
            token: Box::new(token),
            is_tail: false,
        },
    );
}

//TODO(lh): Replace this with persist_sink::install_desired_sink once #13740 is merged.
fn install_desired_into_persist<G>(
    target: &CollectionMetadata,
    compute_state: &mut crate::compute_state::ComputeState,
    sink_as_of: SinkAsOf,
    sink_id: GlobalId,
    desired_collection: Collection<G, Result<Row, DataflowError>, Diff>,
    persist_collection: Collection<G, Result<Row, DataflowError>, Diff>,
) -> Option<Rc<dyn Any>>
where
    G: Scope<Timestamp = Timestamp>,
{
    let scope = desired_collection.scope();

    let persist_clients = Arc::clone(&compute_state.persist_clients);
    let persist_location = target.persist_location.clone();
    let shard_id = target.data_shard;

    let operator_name = format!("persist_sink({})", shard_id);
    let mut persist_op = OperatorBuilder::new(operator_name, scope.clone());

    // Only attempt to write from this frontier onward, as our data are not necessarily
    // correct for times not greater or equal to this frontier. Ignore as_of strictness.
    let mut write_lower_bound = sink_as_of.frontier;

    // TODO(mcsherry): this is shardable, eventually. But for now use a single writer.
    let hashed_id = sink_id.hashed();
    // TODO(lh): Figure out why this is necessary for introspection sources.
    let active_write_worker = scope.index() == 0;
    let mut desired_input =
        persist_op.new_input(&desired_collection.inner, Exchange::new(move |_| hashed_id));
    let mut persist_input =
        persist_op.new_input(&persist_collection.inner, Exchange::new(move |_| hashed_id));

    // Dropping this token signals that the operator should shut down cleanly.
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

    // This operator accepts the current and desired update streams for a `persist` shard.
    // It attempts to write out updates, starting from the current's upper frontier, that
    // will cause the changes of desired to be committed to persist.

    persist_op.build_async(
        scope,
        move |_capabilities, frontiers, scheduler| async move {
            let mut buffer = Vec::new();

            // Contains `desired - persist`, reflecting the updates we would like to commit
            // to `persist` in order to "correct" it to track `desired`.
            let mut correction = Vec::new();

            // TODO(aljoscha): We need to figure out what to do with error results from these calls.
            let mut write = persist_clients
                .lock()
                .await
                .open(persist_location)
                .await
                .expect("could not open persist client")
                .open_writer::<SourceData, (), Timestamp, Diff>(shard_id)
                .await
                .expect("could not open persist shard");

            while scheduler.notified().await {
                if !active_write_worker
                    || token_weak.upgrade().is_none()
                    // FRANK: I don't understand this case.
                    || shared_frontier.borrow().is_empty()
                {
                    return;
                }

                // Extract desired rows as positive contributions to `correction`.
                desired_input.for_each(|_cap, data| {
                    data.swap(&mut buffer);
                    correction.append(&mut buffer);
                });

                // Extract persist rows as negative contributions to `correction`.
                persist_input.for_each(|_cap, data| {
                    data.swap(&mut buffer);
                    correction.extend(buffer.drain(..).map(|(d,t,r)| (d,t,-r)));
                });

                // Capture current frontiers.
                let frontiers = frontiers.borrow().clone();
                let desired_frontier = &frontiers[0];
                let persist_frontier = &frontiers[1];

                // We should only attempt a commit to an interval at least our lower bound.
                if PartialOrder::less_equal(&write_lower_bound, persist_frontier) {

                    // We may have the opportunity to commit updates.
                    if PartialOrder::less_than(persist_frontier, desired_frontier) {

                        // Advance all updates to `persist`'s frontier.
                        for (_, time, _) in correction.iter_mut() {
                            use differential_dataflow::lattice::Lattice;
                            time.advance_by(persist_frontier.borrow());
                        }
                        // Consolidate updates within.
                        differential_dataflow::consolidation::consolidate_updates(&mut correction);

                        let to_append =
                        correction
                            .iter()
                            .filter(|(_, time, _)| persist_frontier.less_equal(time) && !desired_frontier.less_equal(time))
                            .map(|(data, time, diff)| ((SourceData(data.clone()), ()), time, diff));

                        let result =
                        write
                            .compare_and_append(to_append, persist_frontier.clone(), desired_frontier.clone())
                            .await
                            .expect("Indeterminate")    // TODO: What does this error mean?
                            .expect("Invalid usage")    // TODO: What does this error mean?
                            ;

                        // If the result is `Ok`, we will eventually see the appended data return through `persist_input`,
                        // which will remove the results from `correction`; we should not do this directly ourselves.
                        // In either case, we have new information about the next frontier we should attempt to commit from.
                        match result {
                            Ok(()) => {
                                write_lower_bound.clone_from(desired_frontier);
                            }
                            Err(mz_persist_client::Upper(frontier)) => {
                                write_lower_bound.clone_from(&frontier);
                            }
                        }
                    } else {
                        write_lower_bound.clone_from(&persist_frontier);
                    }
                }

                // Confirm that we only require updates from our lower bound onward.
                shared_frontier.borrow_mut().clone_from(&write_lower_bound);
            }
        },
    );

    Some(token)
}

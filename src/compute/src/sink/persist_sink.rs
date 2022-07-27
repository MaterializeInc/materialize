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
use differential_dataflow::{Collection, Hashable};
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
        sink: &SinkDesc<CollectionMetadata>,
        sink_id: GlobalId,
        sinked_collection: Collection<G, (Option<Row>, Option<Row>), Diff>,
        err_collection: Collection<G, DataflowError, Diff>,
    ) -> Option<Rc<dyn Any>>
    where
        G: Scope<Timestamp = Timestamp>,
    {
        let desired_collection = sinked_collection
            .map(|(key, val)| {
                assert!(key.is_none(), "persist_source does not support keys");
                let row = val.expect("persist_source must have values");
                Ok(row)
            })
            .concat(&err_collection.map(Err));

        persist_sink(
            sink_id,
            &self.storage_metadata,
            desired_collection,
            sink.as_of.frontier.clone(),
            compute_state,
        )
    }
}

pub(crate) fn persist_sink<G>(
    sink_id: GlobalId,
    target: &CollectionMetadata,
    desired_collection: Collection<G, Result<Row, DataflowError>, Diff>,
    as_of: Antichain<Timestamp>,
    compute_state: &mut ComputeState,
) -> Option<Rc<dyn Any>>
where
    G: Scope<Timestamp = Timestamp>,
{
    // There is no guarantee that `as_of` is beyond the persist shard's since. If it isn't,
    // instantiating a `persist_source` with it would panic. So instead we leave it to
    // `persist_source` to select an appropriate `as_of`. We only care about times beyond the
    // current shard upper anyway.
    let source_as_of = None;
    let (ok_stream, err_stream, token) = mz_storage::source::persist_source::persist_source(
        &desired_collection.scope(),
        sink_id.clone(),
        Arc::clone(&compute_state.persist_clients),
        target.clone(),
        source_as_of,
    );
    use differential_dataflow::AsCollection;
    let persist_collection = ok_stream
        .as_collection()
        .map(Ok)
        .concat(&err_stream.as_collection().map(Err));

    Some(Rc::new((
        install_desired_into_persist(
            sink_id,
            target,
            desired_collection,
            persist_collection,
            as_of,
            compute_state,
        ),
        token,
    )))
}

fn install_desired_into_persist<G>(
    sink_id: GlobalId,
    target: &CollectionMetadata,
    desired_collection: Collection<G, Result<Row, DataflowError>, Diff>,
    persist_collection: Collection<G, Result<Row, DataflowError>, Diff>,
    _as_of: Antichain<Timestamp>,
    compute_state: &mut crate::compute_state::ComputeState,
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

    // We might want a lower bound on when we should start writing, especially as the
    // dataflow is only valid from an `as_of` onward. However, if the sink's shard's
    // `upper` is not initially beyond that frontier we should do *something*.
    // Writing here allows the sink to advance the shard using the empty collection,
    // but it should perhaps be considered a bug when this happens, as we are writing
    // "junk data" which is a serious smell.
    let mut write_lower_bound = Antichain::from_elem(TimelyTimestamp::minimum());

    // TODO(mcsherry): this is shardable, eventually. But for now use a single writer.
    let hashed_id = sink_id.hashed();
    let active_write_worker = (hashed_id as usize) % scope.peers() == scope.index();
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
            // to `persist` in order to "correct" it to track `desired`. This collection is
            // only modified by updates received from either the `desired` or `persist` inputs.
            let mut correction = Vec::new();

            // TODO(aljoscha): We need to figure out what to do with error results from these calls.
            let persist_client = persist_clients
                .lock()
                .await
                .open(persist_location)
                .await
                .expect("could not open persist client");

            let mut write = persist_client
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
                        consolidate_updates(&mut correction);

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

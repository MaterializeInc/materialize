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

use differential_dataflow::Collection;
use timely::dataflow::Scope;
use timely::progress::Antichain;
use timely::progress::Timestamp as TimelyTimestamp;

use mz_compute_client::sinks::{ComputeSinkDesc, PersistSinkConnection};
use mz_repr::{Diff, GlobalId, Row, Timestamp};
use mz_storage::controller::CollectionMetadata;
use mz_storage::types::errors::DataflowError;

use crate::compute_state::ComputeState;
use crate::render::sinks::SinkRender;

impl<G> SinkRender<G> for PersistSinkConnection<CollectionMetadata>
where
    G: Scope<Timestamp = Timestamp>,
{
    fn render_continuous_sink(
        &self,
        compute_state: &mut ComputeState,
        sink: &ComputeSinkDesc<CollectionMetadata>,
        sink_id: GlobalId,
        sinked_collection: Collection<G, Row, Diff>,
        err_collection: Collection<G, DataflowError, Diff>,
    ) -> Option<Rc<dyn Any>>
    where
        G: Scope<Timestamp = Timestamp>,
    {
        let desired_collection = sinked_collection.map(Ok).concat(&err_collection.map(Err));

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
        sink_id,
        Arc::clone(&compute_state.persist_clients),
        target.clone(),
        source_as_of,
        true,             /* include snapshot */
        Antichain::new(), // we want all updates
        None,             // no MFP
        // Copy the logic in DeltaJoin/Get/Join to start.
        |_timer, count| count > 1_000_000,
    );
    use differential_dataflow::AsCollection;
    let persist_collection = ok_stream
        .as_collection()
        .map(Ok)
        .concat(&err_stream.as_collection().map(Err));

    let persist_clients = Arc::clone(&compute_state.persist_clients);

    // Initialize shared frontier tracking.
    let shared_frontier = Rc::new(RefCell::new(Antichain::from_elem(Timestamp::minimum())));

    compute_state
        .sink_write_frontiers
        .insert(sink_id, Rc::clone(&shared_frontier));

    let mut scope = desired_collection.inner.scope();

    Some(Rc::new((
        mz_storage::render::persist_sink::install_desired_into_persist(
            &mut scope,
            sink_id,
            target,
            desired_collection,
            persist_collection,
            as_of,
            persist_clients,
            shared_frontier,
        ),
        token,
    )))
}

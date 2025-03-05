// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::any::Any;
use std::rc::Rc;

use differential_dataflow::Collection;
use mz_compute_types::sinks::{ComputeSinkDesc, MaterializedViewSinkConnection};
use mz_repr::{Diff, GlobalId, Row, Timestamp};
use mz_storage_types::controller::CollectionMetadata;
use mz_storage_types::errors::DataflowError;
use timely::dataflow::operators::probe;
use timely::dataflow::Scope;
use timely::progress::Antichain;

use crate::compute_state::ComputeState;
use crate::render::sinks::SinkRender;
use crate::render::StartSignal;
use crate::sink::materialized_view_v2;
use crate::sink::refresh::apply_refresh;

impl<G> SinkRender<G> for MaterializedViewSinkConnection<CollectionMetadata>
where
    G: Scope<Timestamp = Timestamp>,
{
    fn render_sink(
        &self,
        compute_state: &mut ComputeState,
        sink: &ComputeSinkDesc<CollectionMetadata>,
        sink_id: GlobalId,
        as_of: Antichain<Timestamp>,
        start_signal: StartSignal,
        mut ok_collection: Collection<G, Row, Diff>,
        mut err_collection: Collection<G, DataflowError, Diff>,
        _ct_times: Option<Collection<G, (), Diff>>,
    ) -> Option<Rc<dyn Any>> {
        // Attach a probe reporting the compute frontier.
        // The `apply_refresh` operator can round up frontiers, making it impossible to accurately
        // track the progress of the computation, so we need to attach the probe before it.
        let probe = probe::Handle::default();
        ok_collection = ok_collection.probe_with(&probe);
        let collection_state = compute_state.expect_collection_mut(sink_id);
        collection_state.compute_probe = Some(probe);

        // If a `RefreshSchedule` was specified, round up timestamps.
        if let Some(refresh_schedule) = &sink.refresh_schedule {
            ok_collection = apply_refresh(ok_collection, refresh_schedule.clone());
            err_collection = apply_refresh(err_collection, refresh_schedule.clone());
        }

        if sink.up_to != Antichain::default() {
            unimplemented!(
                "UP TO is not supported for persist sinks yet, and shouldn't have been accepted during parsing/planning"
            )
        }

        let token = materialized_view_v2::persist_sink(
            sink_id,
            &self.storage_metadata,
            ok_collection,
            err_collection,
            as_of,
            compute_state,
            start_signal,
        );
        Some(token)
    }
}

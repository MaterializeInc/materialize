// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! The batched subscribe sink.

use std::any::Any;
use std::rc::Rc;

use differential_dataflow::VecCollection;
use mz_compute_types::sinks::{BatchedSubscribeSinkConnection, ComputeSinkDesc};
use mz_repr::{Diff, GlobalId, Row, Timestamp};
use mz_storage_types::controller::CollectionMetadata;
use mz_timely_util::probe::Handle;
use timely::progress::Antichain;

use crate::render::StartSignal;
use crate::render::errors::DataflowErrorSer;
use crate::render::sinks::SinkRender;

impl<'scope> SinkRender<'scope> for BatchedSubscribeSinkConnection {
    /// Renders nothing and returns no token, so the sink reports no responses.
    // TODO: Render the sink, which reports through
    // `ComputeState::batched_subscribe_response_buffer`.
    fn render_sink(
        &self,
        _compute_state: &mut crate::compute_state::ComputeState,
        _sink: &ComputeSinkDesc<CollectionMetadata>,
        _sink_id: GlobalId,
        _as_of: Antichain<Timestamp>,
        _start_signal: StartSignal,
        _sinked_collection: VecCollection<'scope, Timestamp, Row, Diff>,
        _err_collection: VecCollection<'scope, Timestamp, DataflowErrorSer, Diff>,
        _output_probe: &Handle<Timestamp>,
    ) -> Option<Rc<dyn Any>> {
        None
    }
}

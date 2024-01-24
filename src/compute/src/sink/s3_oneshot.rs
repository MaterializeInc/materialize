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
use mz_compute_types::sinks::{ComputeSinkDesc, S3OneshotSinkConnection};
use mz_repr::{Diff, GlobalId, Row, Timestamp};
use mz_storage_types::controller::CollectionMetadata;
use mz_storage_types::errors::DataflowError;
use timely::dataflow::Scope;
use timely::progress::Antichain;
use tracing::info;

use crate::render::sinks::SinkRender;

impl<G> SinkRender<G> for S3OneshotSinkConnection
where
    G: Scope<Timestamp = Timestamp>,
{
    fn render_continuous_sink(
        &self,
        _compute_state: &mut crate::compute_state::ComputeState,
        _sink: &ComputeSinkDesc<CollectionMetadata>,
        _sink_id: GlobalId,
        _as_of: Antichain<Timestamp>,
        _sinked_collection: Collection<G, Row, Diff>,
        _err_collection: Collection<G, DataflowError, Diff>,
    ) -> Option<Rc<dyn Any>>
    where
        G: Scope<Timestamp = Timestamp>,
    {
        info!("Starting s3 sink dataflow to write data to {}", self.prefix);
        // TODO(mouli): implement this!
        None
    }
}

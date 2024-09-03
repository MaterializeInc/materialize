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
use mz_compute_types::sinks::{ComputeSinkDesc, ContinualTaskConnection};
use mz_repr::{Diff, GlobalId, Row, Timestamp};
use mz_storage_types::controller::CollectionMetadata;
use mz_storage_types::errors::DataflowError;
use timely::dataflow::Scope;
use timely::progress::Antichain;

use crate::compute_state::ComputeState;
use crate::render::sinks::SinkRender;
use crate::render::StartSignal;

impl<G> SinkRender<G> for ContinualTaskConnection<CollectionMetadata>
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
        oks: Collection<G, Row, Diff>,
        errs: Collection<G, DataflowError, Diff>,
    ) -> Option<Rc<dyn Any>> {
        todo!(
            "WIP {:?}",
            (
                std::any::type_name_of_val(&compute_state),
                sink,
                sink_id,
                as_of,
                std::any::type_name_of_val(&start_signal),
                std::any::type_name_of_val(&oks),
                std::any::type_name_of_val(&errs),
            )
        );
    }
}

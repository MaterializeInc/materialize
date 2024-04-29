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
use mz_repr::{Diff, GlobalId, Row, Timestamp};
use mz_storage_types::controller::CollectionMetadata;
use mz_storage_types::errors::DataflowError;
use timely::dataflow::Scope;
use timely::progress::Antichain;

use crate::compute_state::ComputeState;
use crate::render::StartSignal;

/// Renders an MV sink writing the given desired collection into the `target` persist collection.
pub(super) fn persist_sink<S>(
    sink_id: GlobalId,
    target: &CollectionMetadata,
    ok_collection: Collection<S, Row, Diff>,
    err_collection: Collection<S, DataflowError, Diff>,
    as_of: Antichain<Timestamp>,
    compute_state: &mut ComputeState,
    start_signal: StartSignal,
) -> Rc<dyn Any>
where
    S: Scope<Timestamp = Timestamp>,
{
    todo!()
}

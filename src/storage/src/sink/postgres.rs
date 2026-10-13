// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Postgres sink.

use differential_dataflow::VecCollection;
use mz_repr::{Diff, GlobalId, Timestamp};
use mz_storage_types::controller::CollectionMetadata;
use mz_storage_types::errors::DataflowError;
use mz_storage_types::sinks::{PostgresSinkConnection, StorageSinkDesc};
use mz_timely_util::builder_async::PressOnDropButton;
use timely::dataflow::StreamVec;

use crate::healthcheck::HealthStatusMessage;
use crate::render::sinks::{SinkBatchStream, SinkRender};
use crate::storage_state::StorageState;

impl<'scope> SinkRender<'scope> for PostgresSinkConnection {
    fn get_key_indices(&self) -> Option<&[usize]> {
        self.key_desc_and_indices
            .as_ref()
            .map(|(_, indices)| indices.as_slice())
    }

    fn get_relation_key_indices(&self) -> Option<&[usize]> {
        self.relation_key_indices.as_deref()
    }

    fn render_sink(
        &self,
        _storage_state: &mut StorageState,
        _sink: &StorageSinkDesc<CollectionMetadata, Timestamp>,
        _sink_id: GlobalId,
        _batches: SinkBatchStream<'scope>,
        _key_is_synthetic: bool,
        _err_collection: VecCollection<'scope, Timestamp, DataflowError, Diff>,
    ) -> (
        StreamVec<'scope, Timestamp, HealthStatusMessage>,
        Vec<PressOnDropButton>,
    ) {
        unimplemented!("Postgres sink dataflow")
    }
}

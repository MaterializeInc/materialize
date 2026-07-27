// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use crate::render::sinks::SinkRender;
use mz_storage_types::sinks::{PostgresSinkConnection, StorageSinkDesc};

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
        storage_state: &mut crate::storage_state::StorageState,
        sink: &StorageSinkDesc<
            mz_storage_types::controller::CollectionMetadata,
            mz_repr::Timestamp,
        >,
        sink_id: mz_repr::GlobalId,
        batches: crate::render::sinks::SinkBatchStream<'scope>,
        key_is_synthetic: bool,
        err_collection: differential_dataflow::VecCollection<
            'scope,
            mz_repr::Timestamp,
            mz_storage_types::errors::DataflowError,
            mz_repr::Diff,
        >,
    ) -> (
        timely::dataflow::StreamVec<
            'scope,
            mz_repr::Timestamp,
            crate::healthcheck::HealthStatusMessage,
        >,
        Vec<mz_timely_util::builder_async::PressOnDropButton>,
    ) {
    }
}

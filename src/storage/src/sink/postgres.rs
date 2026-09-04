// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::{cell::RefCell, rc::Rc};

use differential_dataflow::VecCollection;
use mz_repr::{Diff, GlobalId, Timestamp};
use mz_storage_types::controller::CollectionMetadata;
use mz_storage_types::errors::DataflowError;
use mz_storage_types::sinks::{PostgresSinkConnection, StorageSinkDesc};
use mz_timely_util::builder_async::PressOnDropButton;
use timely::dataflow::StreamVec;
use timely::dataflow::operators::ToStream;
use timely::progress::{Antichain, Timestamp as _};

use crate::healthcheck::{HealthStatusMessage, HealthStatusUpdate, StatusNamespace};
use crate::render::sinks::{SinkBatchStream, SinkRender};
use crate::storage_state::StorageState;

type PostgresRecord = Vec<u8>;

impl<'scope> SinkRender<'scope> for PostgresSinkConnection {
    fn get_key_indices(&self) -> Option<&[usize]> {
        self.key_desc_and_indices
            .as_ref()
            .map(|(_, indices)| indices.as_slice())
    }

    fn get_relation_key_indices(&self) -> Option<&[usize]> {
        self.relation_key_indices.as_deref()
    }

    /// TODO: render the sink. Until then a Postgres sink halts rather than
    /// silently writing nothing, so a sink that somehow gets created reports
    /// why in `mz_internal.mz_sink_statuses` instead of looking healthy.
    fn render_sink(
        &self,
        storage_state: &mut StorageState,
        _sink: &StorageSinkDesc<CollectionMetadata, Timestamp>,
        sink_id: GlobalId,
        batches: SinkBatchStream<'scope>,
        _key_is_synthetic: bool,
        _err_collection: VecCollection<'scope, Timestamp, DataflowError, Diff>,
    ) -> (
        StreamVec<'scope, Timestamp, HealthStatusMessage>,
        Vec<PressOnDropButton>,
    ) {
        let scope = batches.scope();
        let status = std::iter::once(HealthStatusMessage {
            id: None,
            update: HealthStatusUpdate::halting(
                "Postgres sinks are not implemented yet".to_string(),
                None,
            ),
            namespace: StatusNamespace::Postgres,
        })
        .to_stream(scope);

        let write_frontier = Rc::new(RefCell::new(Antichain::from_elem(Timestamp::minimum())));
        storage_state
            .sink_write_frontiers
            .insert(sink_id, Rc::clone(&write_frontier));

        (status, vec![])
    }
}

fn encode_postgres_output<'scope>(
    name: String,
    batches: SinkBatchStream<'scope>,
    connection: &PostgresSinkConnection,
    sink_id: GlobalId,
    from_id: GlobalId,
    key_is_synthetic: bool,
) -> () {
}

fn copy_to_postgres<'scope>(
    name: String,
    records: VecCollection<'scope, Timestamp, PostgresRecord>,
    connection: &PostgresSinkConnection,
    sink_id: GlobalId,
    from_id: GlobalId,
    key_is_synthetic: bool,
) {
}

fn insert_into_target<'scope>(
    name: String,
    records: VecCollection<'scope, Timestamp, PostgresRecord>,
    connection: &PostgresSinkConnection,
    sink_id: GlobalId,
    from_id: GlobalId,
) {
}

fn encode_postgres_record(
    record: &mz_repr::Row,
    connection: &PostgresSinkConnection,
    key_is_synthetic: bool,
) -> PostgresRecord {
    let mut buf = Vec::new();
    if let Some(key_desc_and_indices) = &connection.key_desc_and_indices {
        let (key_desc, key_indices) = key_desc_and_indices;
        let key_row = record.select(key_indices);
        connection
            .format
            .key_format
            .encode(&mut buf, &key_row, key_desc, key_is_synthetic)
            .expect("encoding to buffer cannot fail");
    }
    connection
        .format
        .value_format
        .encode(&mut buf, record, &connection.value_desc, false)
        .expect("encoding to buffer cannot fail");
    buf
}

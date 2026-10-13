// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Writing MV sink batches through the process-global [`SharedBatches`].
//!
//! The workers running in one process coalesce their parts for a given batch interval into a
//! single, larger batch instead of each writing its own small batch. All workers building an
//! interval share `desc.shared_id`, because the `mint` operator broadcasts one description.

use std::sync::Arc;

use mz_persist_client::batch::ProtoBatch;
use mz_persist_client::write::WriteHandle;
use mz_persist_client::{PersistClient, Schemas};
use mz_persist_types::ShardId;
use mz_persist_types::codec_impls::UnitSchema;
use mz_persist_types::part::PartBuilder;
use mz_repr::{RelationDesc, Timestamp};
use mz_storage_operators::persist::{SharedBatchBuilder, SharedBatches};
use mz_storage_types::StorageDiff;
use mz_storage_types::sources::SourceData;

use crate::sink::materialized_view::{BatchDescription, PersistApi};

/// How many updates a worker accumulates before handing a part to the shared batch.
///
/// The shared batch concatenates the parts it receives until they reach persist's blob target
/// size, so this only sets the granularity at which a worker's updates become visible to that
/// coalescing, not the size of the parts persist writes.
const SHARED_PART_LEN: usize = 1024;

/// A sink's handle on the process-global [`SharedBatches`] for its output shard.
pub(super) struct SharedBatchWriter {
    persist_client: PersistClient,
    shard_id: ShardId,
    shard_name: String,
    schemas: Schemas<SourceData, ()>,
    shared_batches: SharedBatches,
}

impl SharedBatchWriter {
    /// Creates a writer for the shard `writer` writes to.
    pub(super) async fn new(
        persist_api: &PersistApi,
        writer: &WriteHandle<SourceData, (), Timestamp, StorageDiff>,
    ) -> Self {
        Self {
            persist_client: persist_api.open_client().await,
            shard_id: persist_api.collection.data_shard,
            shard_name: persist_api.shard_name.clone(),
            // Parts carry the id of the schema they were written under, and a part written under
            // none cannot be decoded through the shard's schema registry. The handle resolves the
            // registered id for this sink's schema, so take it from there rather than assembling
            // an unregistered `Schemas`.
            schemas: Schemas {
                id: writer.schema_id(),
                key: Arc::new(persist_api.collection.relation_desc.clone()),
                val: Arc::new(UnitSchema),
            },
            shared_batches: persist_api.persist_batches.clone(),
        }
    }

    /// Starts this worker's contribution to the shared batch for `desc`.
    pub(super) fn start(&self, desc: &BatchDescription) -> SharedBatchWrite {
        let shared = self.shared_batches.builder(
            desc.shared_id,
            self.persist_client.clone(),
            self.shard_id,
            self.shard_name.clone(),
            self.schemas.clone(),
            desc.lower.clone(),
            desc.upper.clone(),
        );
        SharedBatchWrite {
            builder: PartBuilder::new(&*self.schemas.key, &*self.schemas.val),
            schemas: self.schemas.clone(),
            shared,
        }
    }
}

/// One worker's contribution to the shared batch of one interval.
///
/// The caller must [`finish`](Self::finish) it, also when it pushed nothing: only the last
/// worker to finish receives the batch, and any worker holding a write may be that last one and
/// thus responsible for delivering all workers' data.
pub(super) struct SharedBatchWrite {
    builder: PartBuilder<SourceData, RelationDesc, (), UnitSchema>,
    schemas: Schemas<SourceData, ()>,
    shared: SharedBatchBuilder,
}

impl SharedBatchWrite {
    /// Adds an update, handing a part to the shared batch every [`SHARED_PART_LEN`] updates.
    pub(super) async fn push(&mut self, (k, v): &(SourceData, ()), t: Timestamp, d: StorageDiff) {
        self.builder.push(k, v, t, d);
        if self.builder.len() >= SHARED_PART_LEN {
            let part = self
                .builder
                .finish_and_replace(&*self.schemas.key, &*self.schemas.val);
            self.shared.push(part).await;
        }
    }

    /// Hands over the remaining updates and returns the shared batch if this worker is the last
    /// to finish. Every other worker receives `None`.
    pub(super) async fn finish(self) -> Option<ProtoBatch> {
        self.shared.push(self.builder.finish()).await;
        let batch = self.shared.finish().await;
        batch.map(|batch| batch.into_transmittable_batch())
    }
}

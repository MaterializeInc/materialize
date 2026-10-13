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
//! The workers running in one process coalesce their updates for a given batch interval into a
//! single batch instead of each writing its own small batch. All workers building an interval
//! share `desc.shared_id`, because the `mint` operator broadcasts one description.
//!
//! One task per process encodes and uploads a shared batch, so routing large intervals through it
//! serializes work the workers otherwise do in parallel. Hydrating a 9.5 GB MV on 8 workers against a
//! local blob store took 33 s through the shared batch and 26 s with per-worker batches. Each worker therefore stages
//! its updates and hands them to the shared batch only if they stay below a size threshold. Past
//! the threshold, it writes its own batch, whose parts are large enough on their own.

use std::sync::Arc;

use mz_dyncfg::ConfigValHandle;
use mz_persist_client::batch::{BatchBuilder, ProtoBatch};
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

/// A sink's handle on the process-global [`SharedBatches`] for its output shard.
pub(super) struct SharedBatchWriter {
    persist_client: PersistClient,
    shard_id: ShardId,
    shard_name: String,
    schemas: Schemas<SourceData, ()>,
    shared_batches: SharedBatches,
    enabled: ConfigValHandle<bool>,
    max_bytes: ConfigValHandle<usize>,
}

impl SharedBatchWriter {
    /// Creates a writer for the shard `writer` writes to, which shares batches while `enabled`
    /// and a worker's updates for an interval stay below `max_bytes`.
    pub(super) async fn new(
        persist_api: &PersistApi,
        writer: &WriteHandle<SourceData, (), Timestamp, StorageDiff>,
        enabled: ConfigValHandle<bool>,
        max_bytes: ConfigValHandle<usize>,
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
            enabled,
            max_bytes,
        }
    }

    /// Starts this worker's write for `desc`, or returns `None` when sharing is disabled and the
    /// worker writes its own batch.
    pub(super) fn start(&self, desc: &BatchDescription) -> Option<SharedBatchWrite<'_>> {
        // Read per interval, so a flag flip applies without re-rendering the sink. Workers may
        // disagree on an interval: each either contributes to the shared batch or writes its own,
        // and the append operator appends all of them together.
        if !self.enabled.get() {
            return None;
        }
        let shared = self.shared_batches.builder(
            desc.shared_id,
            self.persist_client.clone(),
            self.shard_id,
            self.shard_name.clone(),
            self.schemas.clone(),
            desc.lower.clone(),
            desc.upper.clone(),
        );
        Some(SharedBatchWrite {
            writer: self,
            desc: desc.clone(),
            max_bytes: self.max_bytes.get(),
            staged: PartBuilder::new(&*self.schemas.key, &*self.schemas.val),
            own: None,
            shared,
        })
    }
}

/// One worker's write for one interval, which contributes to the shared batch until the worker's
/// updates reach the size threshold and writes the worker's own batch after.
///
/// The caller must [`finish`](Self::finish) it, also when it pushed nothing: only the last
/// worker to finish receives the shared batch, and any worker holding a write may be that last
/// one and thus responsible for delivering all workers' data.
pub(super) struct SharedBatchWrite<'a> {
    writer: &'a SharedBatchWriter,
    desc: BatchDescription,
    max_bytes: usize,
    /// Updates not yet handed to either batch. Empty once `own` exists.
    staged: PartBuilder<SourceData, RelationDesc, (), UnitSchema>,
    own: Option<BatchBuilder<SourceData, (), Timestamp, StorageDiff>>,
    shared: SharedBatchBuilder,
}

impl SharedBatchWrite<'_> {
    /// Adds an update.
    pub(super) async fn push(&mut self, (k, v): &(SourceData, ()), t: Timestamp, d: StorageDiff) {
        if let Some(own) = &mut self.own {
            own.add(k, v, &t, &d).await.expect("valid usage");
            return;
        }
        self.staged.push(k, v, t, d);
        if self.staged.goodbytes() >= self.max_bytes {
            let writer = self.writer;
            let mut own = writer.persist_client.batch_builder(
                writer.shard_id,
                &writer.shard_name,
                writer.schemas.clone(),
                self.desc.lower.clone(),
                None,
            );
            let staged = self
                .staged
                .finish_and_replace(&*writer.schemas.key, &*writer.schemas.val);
            own.add_part(staged).await.expect("valid usage");
            self.own = Some(own);
        }
    }

    /// Hands over the staged updates and returns the batches this worker delivers: its own batch
    /// if its updates reached the threshold, and the shared batch if it is the last worker to
    /// finish.
    pub(super) async fn finish(self) -> Vec<ProtoBatch> {
        let own = match self.own {
            Some(own) => Some(
                own.finish(self.desc.upper.clone())
                    .await
                    .expect("valid usage"),
            ),
            None => {
                self.shared.push(self.staged.finish()).await;
                None
            }
        };
        let shared = self.shared.finish().await;
        own.into_iter()
            .chain(shared)
            .map(|batch| batch.into_transmittable_batch())
            .collect()
    }
}

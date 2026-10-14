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
//! serializes work the workers otherwise do in parallel. Each worker therefore stages its updates
//! and hands them to the shared batch only if they stay below a size threshold. Past the
//! threshold, it writes its own batch.

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
            // registered id for this sink's schema.
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

    /// Starts this worker's write for `desc`.
    pub(super) fn start(&self, desc: &BatchDescription) -> SharedBatchWrite<'_> {
        // Read per interval, so a flag flip applies without re-rendering the sink. Workers may
        // disagree on an interval: each either contributes to the shared batch or writes its own,
        // and the append operator appends all of them together. A disabled writer joins no shared
        // batch at all, so turning the flag off also stops the shared builder tasks.
        let shared = self.enabled.get().then(|| {
            self.shared_batches.builder(
                desc.shared_id,
                self.persist_client.clone(),
                self.shard_id,
                self.shard_name.clone(),
                self.schemas.clone(),
                desc.lower.clone(),
                desc.upper.clone(),
            )
        });
        SharedBatchWrite {
            writer: self,
            desc: desc.clone(),
            max_bytes: self.max_bytes.get(),
            staged: PartBuilder::new(&*self.schemas.key, &*self.schemas.val),
            own: None,
            shared,
        }
    }

    fn own_builder(
        &self,
        desc: &BatchDescription,
    ) -> BatchBuilder<SourceData, (), Timestamp, StorageDiff> {
        self.persist_client.batch_builder(
            self.shard_id,
            &self.shard_name,
            self.schemas.clone(),
            desc.lower.clone(),
            None,
        )
    }
}

/// One worker's write for one interval, which contributes to the shared batch until the worker's
/// updates reach the size threshold and writes the worker's own batch after. With sharing disabled,
/// all updates go to the worker's own batch.
///
/// The caller must [`finish`](Self::finish) it, also when it pushed nothing (see
/// [`SharedBatchBuilder::finish`]).
pub(super) struct SharedBatchWrite<'a> {
    writer: &'a SharedBatchWriter,
    desc: BatchDescription,
    max_bytes: usize,
    /// Updates not yet handed to either batch. Empty once `own` exists, and always empty without
    /// `shared`.
    staged: PartBuilder<SourceData, RelationDesc, (), UnitSchema>,
    /// Created on the first update that does not go to the shared batch, so a worker without
    /// updates writes no batch.
    own: Option<BatchBuilder<SourceData, (), Timestamp, StorageDiff>>,
    shared: Option<SharedBatchBuilder>,
}

impl SharedBatchWrite<'_> {
    /// Adds an update.
    pub(super) async fn push(&mut self, (k, v): &(SourceData, ()), t: Timestamp, d: StorageDiff) {
        if self.own.is_none() {
            if self.shared.is_some() {
                self.staged.push(k, v, t, d);
                if self.staged.goodbytes() < self.max_bytes {
                    return;
                }
                let writer = self.writer;
                let staged = self
                    .staged
                    .finish_and_replace(&*writer.schemas.key, &*writer.schemas.val);
                let mut own = writer.own_builder(&self.desc);
                own.add_part(staged).await.expect("valid usage");
                self.own = Some(own);
                return;
            }
            self.own = Some(self.writer.own_builder(&self.desc));
        }
        let own = self.own.as_mut().expect("created above");
        own.add(k, v, &t, &d).await.expect("valid usage");
    }

    /// Hands over the staged updates and returns the batches this worker delivers: its own batch
    /// if it has one, and the shared batch if it is the last worker to finish.
    pub(super) async fn finish(self) -> Vec<ProtoBatch> {
        let own = match self.own {
            Some(own) => Some(
                own.finish(self.desc.upper.clone())
                    .await
                    .expect("valid usage"),
            ),
            None => None,
        };
        let shared = match self.shared {
            Some(shared) => {
                shared.push(self.staged.finish()).await;
                shared.finish().await
            }
            None => None,
        };
        own.into_iter()
            .chain(shared)
            .map(|batch| batch.into_transmittable_batch())
            .collect()
    }
}

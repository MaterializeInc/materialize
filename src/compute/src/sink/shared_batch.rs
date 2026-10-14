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
            // The handle resolves the registered id for this sink's schema.
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
        // and the append operator appends all of them together. A writer that is disabled or has
        // a threshold of zero joins no shared batch at all, so it starts no shared builder task.
        let max_bytes = self.max_bytes.get();
        let shared = (self.enabled.get() && max_bytes > 0).then(|| {
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
            max_bytes,
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

#[cfg(test)]
mod tests {
    use mz_dyncfg::ConfigValHandle;
    use mz_persist_client::Diagnostics;
    use mz_repr::{Datum, Row, SqlScalarType};
    use timely::progress::Antichain;

    use super::*;

    fn desc() -> RelationDesc {
        RelationDesc::builder()
            .with_column("a", SqlScalarType::Int64.nullable(false))
            .finish()
    }

    fn writer(
        client: &PersistClient,
        shared_batches: &SharedBatches,
        shard_id: ShardId,
        enabled: bool,
        max_bytes: usize,
    ) -> SharedBatchWriter {
        SharedBatchWriter {
            persist_client: client.clone(),
            shard_id,
            shard_name: "test".to_string(),
            schemas: Schemas {
                id: None,
                key: Arc::new(desc()),
                val: Arc::new(UnitSchema),
            },
            shared_batches: shared_batches.clone(),
            enabled: ConfigValHandle::disconnected(enabled),
            max_bytes: ConfigValHandle::disconnected(max_bytes),
        }
    }

    fn update(i: i64) -> (SourceData, ()) {
        (SourceData(Ok(Row::pack_slice(&[Datum::Int64(i)]))), ())
    }

    async fn update_counts(
        client: &PersistClient,
        shard_id: ShardId,
        batches: Vec<ProtoBatch>,
    ) -> Vec<usize> {
        let write = client
            .open_writer::<SourceData, (), Timestamp, StorageDiff>(
                shard_id,
                Arc::new(desc()),
                Arc::new(UnitSchema),
                Diagnostics::for_tests(),
            )
            .await
            .expect("valid usage");
        batches
            .into_iter()
            .map(|proto| {
                write
                    .batch_from_transmittable_batch(proto)
                    .into_hollow_batch()
                    .len
            })
            .collect()
    }

    #[mz_ore::test(tokio::test)]
    async fn test_shared_batch_write_crosses_threshold() {
        let client = PersistClient::new_for_tests().await;
        let shared_batches = SharedBatches::new();
        let shard_id = ShardId::new();
        let small = writer(&client, &shared_batches, shard_id, true, usize::MAX);
        let large = writer(&client, &shared_batches, shard_id, true, 1);
        let desc = BatchDescription::new(
            Antichain::from_elem(Timestamp::from(0)),
            Antichain::from_elem(Timestamp::from(1)),
            0,
        );

        let mut small_write = small.start(&desc);
        let mut large_write = large.start(&desc);
        small_write.push(&update(0), Timestamp::from(0), 1).await;
        // The first update moves the staged part to the worker's own batch via `add_part`, and
        // the second goes there via `add`.
        large_write.push(&update(1), Timestamp::from(0), 1).await;
        large_write.push(&update(2), Timestamp::from(0), 1).await;

        let small_batches = small_write.finish().await;
        assert!(
            small_batches.is_empty(),
            "a worker below the threshold that finishes first delivers nothing"
        );
        let large_batches = large_write.finish().await;
        assert_eq!(
            update_counts(&client, shard_id, large_batches).await,
            vec![2, 1],
            "the last worker delivers its own batch and the shared batch"
        );
    }

    #[mz_ore::test(tokio::test)]
    async fn test_shared_batch_write_not_sharing() {
        let client = PersistClient::new_for_tests().await;
        let shared_batches = SharedBatches::new();
        let shard_id = ShardId::new();
        let desc = BatchDescription::new(
            Antichain::from_elem(Timestamp::from(0)),
            Antichain::from_elem(Timestamp::from(1)),
            0,
        );
        for (enabled, max_bytes) in [(false, usize::MAX), (true, 0)] {
            let writer = writer(&client, &shared_batches, shard_id, enabled, max_bytes);
            let mut write = writer.start(&desc);
            assert!(
                write.shared.is_none(),
                "enabled={enabled} max_bytes={max_bytes} joins no shared batch"
            );
            write.push(&update(0), Timestamp::from(0), 1).await;
            assert_eq!(
                update_counts(&client, shard_id, write.finish().await).await,
                vec![1],
                "enabled={enabled} max_bytes={max_bytes} writes one own batch"
            );
        }
    }
}

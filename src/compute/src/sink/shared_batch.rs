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
use mz_persist_types::part::{Part, PartBuilder};
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
    max_bytes: ConfigValHandle<usize>,
}

impl SharedBatchWriter {
    /// Creates a writer for the shard `writer` writes to, which shares a worker's updates for an
    /// interval while they stay below `max_bytes`. Zero disables sharing.
    pub(super) async fn new(
        persist_api: &PersistApi,
        writer: &WriteHandle<SourceData, (), Timestamp, StorageDiff>,
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
            max_bytes,
        }
    }

    /// Starts this worker's write for `desc`.
    pub(super) fn start(&self, desc: &BatchDescription) -> SharedBatchWrite<'_> {
        // Read per interval, so a threshold change applies without re-rendering the sink. Workers may
        // disagree on an interval: each either contributes to the shared batch or writes its own,
        // and the append operator appends all of them together. With a threshold of zero, a
        // writer joins no shared batch at all, so it starts no shared builder task.
        let max_bytes = self.max_bytes.get();
        let shared = (max_bytes > 0).then(|| {
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
            staged_part: None,
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
    /// The part [`push_part`](Self::push_part) holds back for the shared batch. Only one part is
    /// held back, so a worker shares its updates only if they fit a single part.
    staged_part: Option<Part>,
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

    /// Adds a part of updates. Do not mix with [`push`](Self::push) on the same write.
    pub(super) async fn push_part(&mut self, part: Part) {
        if self.own.is_none() {
            let share = self.shared.is_some()
                && self.staged_part.is_none()
                && part.goodbytes() < self.max_bytes;
            if share {
                self.staged_part = Some(part);
                return;
            }
            let mut own = self.writer.own_builder(&self.desc);
            if let Some(staged) = self.staged_part.take() {
                own.add_part(staged).await.expect("valid usage");
            }
            self.own = Some(own);
        }
        let own = self.own.as_mut().expect("created above");
        own.add_part(part).await.expect("valid usage");
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
                if let Some(part) = self.staged_part {
                    shared.push(part).await;
                }
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
            max_bytes: ConfigValHandle::disconnected(max_bytes),
        }
    }

    fn update(i: i64) -> (SourceData, ()) {
        (SourceData(Ok(Row::pack_slice(&[Datum::Int64(i)]))), ())
    }

    fn part(updates: &[i64]) -> Part {
        let mut builder = PartBuilder::new(&desc(), &UnitSchema);
        for i in updates {
            let (k, v) = update(*i);
            builder.push(&k, &v, Timestamp::from(0), 1_i64);
        }
        builder.finish()
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
        let small = writer(&client, &shared_batches, shard_id, usize::MAX);
        let large = writer(&client, &shared_batches, shard_id, 1);
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
    async fn test_shared_batch_write_zero_threshold() {
        let client = PersistClient::new_for_tests().await;
        let shared_batches = SharedBatches::new();
        let shard_id = ShardId::new();
        let desc = BatchDescription::new(
            Antichain::from_elem(Timestamp::from(0)),
            Antichain::from_elem(Timestamp::from(1)),
            0,
        );
        let writer = writer(&client, &shared_batches, shard_id, 0);
        let mut write = writer.start(&desc);
        assert!(
            write.shared.is_none(),
            "a zero threshold joins no shared batch"
        );
        write.push(&update(0), Timestamp::from(0), 1).await;
        assert_eq!(
            update_counts(&client, shard_id, write.finish().await).await,
            vec![1],
            "a zero threshold writes one own batch"
        );
    }

    #[mz_ore::test(tokio::test)]
    async fn test_shared_batch_write_parts() {
        let client = PersistClient::new_for_tests().await;
        let shared_batches = SharedBatches::new();
        let shard_id = ShardId::new();
        let desc = BatchDescription::new(
            Antichain::from_elem(Timestamp::from(0)),
            Antichain::from_elem(Timestamp::from(1)),
            0,
        );
        let small = writer(&client, &shared_batches, shard_id, usize::MAX);
        let large = writer(&client, &shared_batches, shard_id, usize::MAX);

        let mut small_write = small.start(&desc);
        let mut large_write = large.start(&desc);
        small_write.push_part(part(&[0])).await;
        // A second part moves the held-back one to the worker's own batch.
        large_write.push_part(part(&[1])).await;
        large_write.push_part(part(&[2, 3])).await;

        assert!(
            small_write.finish().await.is_empty(),
            "a worker whose updates fit one part shares them"
        );
        assert_eq!(
            update_counts(&client, shard_id, large_write.finish().await).await,
            vec![3, 1],
            "a worker with several parts writes its own batch"
        );
    }
}

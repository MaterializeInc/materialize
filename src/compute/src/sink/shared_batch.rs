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

    /// Starts this worker's write for `desc`, which takes updates one at a time.
    pub(super) fn start(&self, desc: &BatchDescription) -> SharedBatchWrite<'_> {
        SharedBatchWrite {
            staged: PartBuilder::new(&*self.schemas.key, &*self.schemas.val),
            write: self.start_write(desc),
        }
    }

    /// Starts this worker's write for `desc`, which takes updates in parts.
    pub(super) fn start_parts(&self, desc: &BatchDescription) -> SharedPartWrite<'_> {
        SharedPartWrite {
            staged: Vec::new(),
            staged_bytes: 0,
            write: self.start_write(desc),
        }
    }

    fn start_write(&self, desc: &BatchDescription) -> Write<'_> {
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
        Write {
            writer: self,
            desc: desc.clone(),
            max_bytes,
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

/// The state [`SharedBatchWrite`] and [`SharedPartWrite`] share. Each stages its updates for the
/// shared batch in its own form.
struct Write<'a> {
    writer: &'a SharedBatchWriter,
    desc: BatchDescription,
    max_bytes: usize,
    /// Created on the first update that does not go to the shared batch, so a worker without
    /// updates writes no batch.
    own: Option<BatchBuilder<SourceData, (), Timestamp, StorageDiff>>,
    shared: Option<SharedBatchBuilder>,
}

impl Write<'_> {
    /// Creates the worker's own batch, starting with the `staged` parts.
    async fn start_own(
        &mut self,
        staged: impl IntoIterator<Item = Part>,
    ) -> &mut BatchBuilder<SourceData, (), Timestamp, StorageDiff> {
        let mut own = self.writer.own_builder(&self.desc);
        for part in staged {
            own.add_part(part).await.expect("valid usage");
        }
        self.own.insert(own)
    }

    /// Hands the `staged` parts to the shared batch and returns the batches this worker delivers:
    /// its own batch if it has one, and the shared batch if it is the last worker to finish.
    async fn finish(self, staged: impl IntoIterator<Item = Part>) -> Vec<ProtoBatch> {
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
                for part in staged {
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

/// One worker's write for one interval, taking updates one at a time. It contributes to the shared
/// batch until the worker's updates reach the size threshold and writes the worker's own batch
/// after. With sharing disabled, all updates go to the worker's own batch.
///
/// The caller must [`finish`](Self::finish) it, also when it pushed nothing (see
/// [`SharedBatchBuilder::finish`]).
pub(super) struct SharedBatchWrite<'a> {
    /// Updates not yet handed to either batch. Empty once `own` exists, and always empty without
    /// `shared`.
    staged: PartBuilder<SourceData, RelationDesc, (), UnitSchema>,
    write: Write<'a>,
}

impl SharedBatchWrite<'_> {
    /// Adds an update.
    pub(super) async fn push(&mut self, (k, v): &(SourceData, ()), t: Timestamp, d: StorageDiff) {
        let write = &mut self.write;
        let own = match &mut write.own {
            Some(own) => own,
            None if write.shared.is_some() => {
                self.staged.push(k, v, t, d);
                if self.staged.goodbytes() < write.max_bytes {
                    return;
                }
                let schemas = &write.writer.schemas;
                let staged = self.staged.finish_and_replace(&*schemas.key, &*schemas.val);
                write.start_own([staged]).await;
                return;
            }
            None => write.start_own([]).await,
        };
        own.add(k, v, &t, &d).await.expect("valid usage");
    }

    /// Hands over the staged updates and returns the batches this worker delivers: its own batch
    /// if it has one, and the shared batch if it is the last worker to finish.
    pub(super) async fn finish(self) -> Vec<ProtoBatch> {
        self.write.finish([self.staged.finish()]).await
    }
}

/// One worker's write for one interval, taking updates in parts. It holds the parts back for the
/// shared batch while their total size stays below the size threshold. The part that would reach
/// the threshold starts the worker's own batch, which takes the held-back parts and every later
/// one. With sharing disabled, all parts go to the worker's own batch.
///
/// The caller must [`finish`](Self::finish) it, also when it pushed nothing (see
/// [`SharedBatchBuilder::finish`]).
pub(super) struct SharedPartWrite<'a> {
    /// Parts not yet handed to either batch. Empty once `own` exists, and always empty without
    /// `shared`.
    staged: Vec<Part>,
    /// The goodbytes of `staged`.
    staged_bytes: usize,
    write: Write<'a>,
}

impl SharedPartWrite<'_> {
    /// Adds a part of updates.
    pub(super) async fn push_part(&mut self, part: Part) {
        let write = &mut self.write;
        let own = match &mut write.own {
            Some(own) => own,
            None => {
                let staged_bytes = self.staged_bytes.saturating_add(part.goodbytes());
                if write.shared.is_some() && staged_bytes < write.max_bytes {
                    self.staged_bytes = staged_bytes;
                    self.staged.push(part);
                    return;
                }
                self.staged_bytes = 0;
                write.start_own(self.staged.drain(..)).await
            }
        };
        own.add_part(part).await.expect("valid usage");
    }

    /// Hands over the staged parts and returns the batches this worker delivers: its own batch if
    /// it has one, and the shared batch if it is the last worker to finish.
    pub(super) async fn finish(self) -> Vec<ProtoBatch> {
        self.write.finish(self.staged).await
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
            write.write.shared.is_none(),
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
        // Room for two of the parts below, but not three.
        let max_bytes = 3 * part(&[0]).goodbytes() - 1;
        let small = writer(&client, &shared_batches, shard_id, max_bytes);
        let large = writer(&client, &shared_batches, shard_id, max_bytes);

        let mut small_write = small.start_parts(&desc);
        let mut large_write = large.start_parts(&desc);
        small_write.push_part(part(&[0])).await;
        small_write.push_part(part(&[1])).await;
        large_write.push_part(part(&[2])).await;
        large_write.push_part(part(&[3])).await;
        // The third part reaches the threshold and moves the held-back ones to the worker's own
        // batch.
        large_write.push_part(part(&[4])).await;
        assert!(
            large_write.staged.is_empty(),
            "the worker's own batch takes the held-back parts"
        );
        large_write.push_part(part(&[5])).await;

        assert!(
            small_write.finish().await.is_empty(),
            "a worker whose parts stay below the threshold shares them"
        );
        assert_eq!(
            update_counts(&client, shard_id, large_write.finish().await).await,
            vec![4, 2],
            "the last worker delivers its own batch and the shared batch"
        );
    }
}

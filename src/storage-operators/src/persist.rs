// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use mz_ore::task::JoinHandle;
use mz_persist_client::batch::Batch;
use mz_persist_client::{PersistClient, Schemas};
use mz_persist_types::ShardId;
use mz_persist_types::part::Part;
use mz_repr::Timestamp;
use mz_storage_types::StorageDiff;
use mz_storage_types::sources::SourceData;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::fmt::Debug;
use std::sync::{Arc, Weak};
use timely::progress::Antichain;
use uuid::Uuid;

/// A unique identifier for a [SharedBatchBuilder].
#[derive(
    Debug,
    Copy,
    Clone,
    Serialize,
    Deserialize,
    Ord,
    PartialOrd,
    Eq,
    PartialEq,
    Hash
)]
#[serde(transparent)]
pub struct SharedBatchId(Uuid);

impl SharedBatchId {
    pub fn new() -> Self {
        Self(Uuid::new_v4())
    }
}

/// A struct from which to obtain a [SharedBatchBuilder].
#[derive(Debug, Clone)]
pub struct SharedBatches {
    data: Arc<std::sync::Mutex<(usize, BTreeMap<SharedBatchId, Weak<BatchState>>)>>,
}

fn upgrade_or_init<T>(weak: &mut Weak<T>, init: impl FnOnce() -> T) -> Arc<T> {
    if let Some(owned) = weak.upgrade() {
        owned
    } else {
        let owned = Arc::new(init());
        *weak = Arc::downgrade(&owned);
        owned
    }
}

impl SharedBatches {
    pub fn new() -> Self {
        Self {
            data: Arc::default(),
        }
    }

    pub fn builder(
        &self,
        shared_batch_id: SharedBatchId,
        client: PersistClient,
        shard_id: ShardId,
        schemas: Schemas<SourceData, ()>,
        lower: Antichain<Timestamp>,
        upper: Antichain<Timestamp>,
    ) -> SharedBatchBuilder {
        let mut guard = self.data.lock().unwrap();
        let (last_retained_len, state_map) = &mut *guard;
        // We clean out entries from the map where all shared batch handles have been dropped.
        // Amortize by only scanning the map once it's doubled in size.
        if state_map.len() > *last_retained_len * 2 {
            state_map.retain(|_, weak| weak.upgrade().is_some());
            *last_retained_len = state_map.len();
        }
        let weak = state_map.entry(shared_batch_id).or_default();
        let state = upgrade_or_init(weak, || {
            let (tx, mut rx) = tokio::sync::mpsc::channel(4);
            let task_id = format!("shared-batch-{shared_batch_id:?}-{shard_id}",);
            BatchState {
                tx,
                handle: mz_ore::task::spawn(|| task_id, async move {
                    let mut builder = None;
                    while let Some(cmd) = rx.recv().await {
                        match cmd {
                            BatchCommand::Push(part) => {
                                let builder = builder.get_or_insert_with(|| {
                                    client.batch_builder(
                                        shard_id,
                                        schemas.clone(),
                                        lower.clone(),
                                        None,
                                    )
                                });
                                builder.add_part(part).await.expect("valid timestamps");
                            }
                        }
                    }
                    if let Some(builder) = builder {
                        Some(builder.finish(upper).await.expect("valid upper bound"))
                    } else {
                        None
                    }
                }),
            }
        });

        SharedBatchBuilder {
            batch_id: shared_batch_id,
            shard_id,
            state,
        }
    }
}

enum BatchCommand {
    Push(Part),
}

#[derive(Debug)]
struct BatchState {
    tx: tokio::sync::mpsc::Sender<BatchCommand>,
    handle: JoinHandle<Option<Batch<SourceData, (), Timestamp, StorageDiff>>>,
}

/// A handle for a shared batch builder. Everyone with a handle for the same builder can
/// [Self::push] to that builder, but the last handle to call [Self::finish] will receive a batch
/// containing all the data.
///
/// Note that it's quite important to call [Self::finish], even if you haven't pushed anything
/// into the batch.
pub struct SharedBatchBuilder {
    batch_id: SharedBatchId,
    shard_id: ShardId,
    state: Arc<BatchState>,
}

impl Debug for SharedBatchBuilder {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SharedBatchBuilder")
            .field("batch_id", &self.batch_id)
            .field("shard_id", &self.shard_id)
            .finish_non_exhaustive()
    }
}

impl SharedBatchBuilder {
    /// Include the provided part in the batch. If there are a large number of updates that have
    /// not been flushed to S3, this call may wait.
    pub async fn push(&self, part: Part) {
        if part.len() == 0 {
            return;
        }
        self.state
            .tx
            .send(BatchCommand::Push(part))
            .await
            .expect("task failed");
    }

    /// Fetch the results of the batch builder from the shared state, if any.
    ///
    /// Only the last builder to call this method will obtain the resulting batch, and that batch
    /// will include all data written by all workers.
    /// This means that, for any batch interval that we actually want to append, all workers
    /// must call finish even if they did not push any batches themselves.
    pub async fn finish(self) -> Option<Batch<SourceData, (), Timestamp, StorageDiff>> {
        if let Some(state) = Arc::into_inner(self.state) {
            drop(state.tx);
            state.handle.await
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mz_persist_types::codec_impls::UnitSchema;
    use mz_persist_types::part::PartBuilder;
    use mz_repr::{Datum, RelationDesc, Row, SqlScalarType};

    #[mz_ore::test(tokio::test)]
    async fn test_shared_batch() {
        let client = PersistClient::new_for_tests().await;
        let shared = SharedBatches::new();
        let batch_id = SharedBatchId::new();
        let shard_id = ShardId::new();
        let schemas = Schemas {
            id: None,
            key: Arc::new(
                RelationDesc::builder()
                    .with_column("test", SqlScalarType::Bool.nullable(true))
                    .finish(),
            ),
            val: Arc::new(UnitSchema),
        };
        let lower = Antichain::from_elem(0.into());
        let upper = Antichain::from_elem(1.into());
        let first = shared.builder(
            batch_id,
            client.clone(),
            shard_id,
            schemas.clone(),
            lower.clone(),
            upper.clone(),
        );
        let second = shared.builder(
            batch_id,
            client,
            shard_id,
            schemas.clone(),
            lower.clone(),
            upper.clone(),
        );
        let mut builder = PartBuilder::new(&*schemas.key, &*schemas.val);
        builder.push(
            &SourceData(Ok(Row::pack_slice(&[Datum::True]))),
            &(),
            Timestamp::new(0),
            1i64,
        );
        let part = builder.finish();
        first.push(part.clone()).await;
        second.push(part.clone()).await;
        assert!(
            second.finish().await.is_none(),
            "first batch to finish should be empty"
        );
        let batch = first.finish().await.unwrap();
        assert_eq!(
            batch.shard_id(),
            shard_id,
            "batch should be for the expected shard"
        );
        assert_eq!(
            batch.into_hollow_batch().len,
            2,
            "batch should include updates from both pushes"
        )
    }
}

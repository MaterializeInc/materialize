// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! An owning handle for batches stashed in persist.

use mz_ore::task::RuntimeExt;
use mz_persist_client::PersistClient;
use mz_persist_client::batch::{Batch, ProtoBatch};
use mz_persist_types::ShardId;
use mz_repr::Timestamp;
use mz_storage_types::sources::SourceData;
use tokio::runtime::Handle;

/// A batch in the schema every stash writes: `SourceData` keys, no values, `Timestamp` times and
/// `i64` diffs.
pub type StashBatch = Batch<SourceData, (), Timestamp, i64>;

/// Stashed batches that are deleted when the handle is dropped.
///
/// Persist's own `Drop for Batch` only logs, so a batch nobody deletes leaks its blobs. Holding
/// the batches here makes every drop path delete them, except where [`OwnedStash::take`] has
/// moved the responsibility to the caller.
pub struct OwnedStash {
    batches: Vec<StashBatch>,
    runtime: Handle,
}

impl OwnedStash {
    /// Takes ownership of the batches `batches` names in shard `shard_id`.
    ///
    /// Must be called from within a tokio runtime, whose handle performs the deletes on drop.
    ///
    /// # Panics
    ///
    /// Panics if a proto batch is malformed or names a shard other than `shard_id`.
    pub fn from_proto(
        client: &PersistClient,
        shard_id: ShardId,
        batches: Vec<ProtoBatch>,
    ) -> OwnedStash {
        let batches = batches
            .into_iter()
            .map(|batch| client.batch_from_transmittable_batch(&shard_id, batch))
            .collect();
        OwnedStash::from_batches(batches, Handle::current())
    }

    /// Takes ownership of `batches`, which are deleted on `runtime` when the handle is dropped.
    pub fn from_batches(batches: Vec<StashBatch>, runtime: Handle) -> OwnedStash {
        OwnedStash { batches, runtime }
    }

    /// Releases the batches. The caller becomes responsible for deleting them.
    pub fn take(mut self) -> Vec<StashBatch> {
        std::mem::take(&mut self.batches)
    }

    /// The encoded size of all held batches.
    pub fn encoded_size_bytes(&self) -> usize {
        self.batches.iter().map(|b| b.encoded_size_bytes()).sum()
    }
}

impl std::fmt::Debug for OwnedStash {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OwnedStash")
            .field("batches", &self.batches.len())
            .field("encoded_size_bytes", &self.encoded_size_bytes())
            .finish_non_exhaustive()
    }
}

impl Drop for OwnedStash {
    fn drop(&mut self) {
        // Deleting is async and drop must not block, so the deletes run on the runtime captured
        // at construction. Drop itself can run on a thread outside any runtime.
        for batch in self.batches.drain(..) {
            let shard_id = batch.shard_id();
            let _handle = self
                .runtime
                .spawn_named(|| format!("owned_stash::delete({shard_id})"), async move {
                    batch.delete().await
                });
        }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use mz_dyncfg::{ConfigUpdates, ConfigVal};
    use mz_ore::cast::CastLossy;
    use mz_ore::metrics::MetricsRegistry;
    use mz_persist_client::cache::PersistClientCache;
    use mz_persist_client::cfg::PersistConfig;
    use mz_persist_client::rpc::PubSubClientConnection;
    use mz_persist_client::{PersistClient, PersistLocation, Schemas};
    use mz_persist_types::codec_impls::UnitSchema;
    use mz_repr::{Datum, RelationDesc, Row, SqlScalarType, Timestamp};
    use mz_storage_types::sources::SourceData;
    use timely::progress::Antichain;

    use super::*;

    /// How many turns a test gives blob storage to catch up before it declares a deletion lost.
    const BLOB_TURNS: usize = 1_000;

    /// A persist client whose blob traffic is counted through the metrics registry.
    pub(crate) struct CountedClient {
        registry: MetricsRegistry,
        pub(crate) client: PersistClient,
    }

    impl CountedClient {
        pub(crate) async fn new() -> Self {
            let cfg = PersistConfig::new_for_tests();
            let mut updates = ConfigUpdates::default();
            // Without these persist inlines small parts into shard state, where no blob counter
            // sees them and a delete has nothing to remove.
            updates.add_dynamic("persist_blob_target_size", ConfigVal::Usize(0));
            updates.add_dynamic(
                "persist_inline_writes_single_max_bytes",
                ConfigVal::Usize(0),
            );
            // Deletion is a silent no-op when this is off.
            updates.add_dynamic("persist_batch_delete_enabled", ConfigVal::Bool(true));
            updates.apply(&cfg);

            let registry = MetricsRegistry::new();
            let cache =
                PersistClientCache::new(cfg, &registry, |_, _| PubSubClientConnection::noop());
            let client = cache
                .open(PersistLocation::new_in_mem())
                .await
                .expect("in-memory location opens");
            Self { registry, client }
        }

        pub(crate) fn succeeded(&self, op: &str) -> u64 {
            let Some(family) = self
                .registry
                .gather()
                .into_iter()
                .find(|m| m.name() == "mz_persist_external_succeeded_count")
            else {
                return 0;
            };
            family
                .get_metric()
                .iter()
                .filter(|metric| {
                    metric
                        .get_label()
                        .iter()
                        .any(|l| l.name() == "op" && l.value() == op)
                })
                .map(|metric| u64::cast_lossy(metric.get_counter().value()))
                .sum()
        }

        /// Writes one batch holding a few rows and returns its wire form.
        pub(crate) async fn write_batch(&self, shard_id: ShardId) -> ProtoBatch {
            let desc = RelationDesc::builder()
                .with_column("value", SqlScalarType::UInt64.nullable(false))
                .finish();
            let schemas: Schemas<SourceData, ()> = Schemas {
                id: None,
                key: Arc::new(desc),
                val: Arc::new(UnitSchema),
            };
            let mut builder = self.client.batch_builder::<SourceData, (), Timestamp, i64>(
                shard_id,
                "test_stash",
                schemas,
                Antichain::from_elem(Timestamp::MIN),
                None,
            );
            for value in 0..4u64 {
                let row = Row::pack_slice(&[Datum::UInt64(value)]);
                builder
                    .add(&SourceData(Ok(row)), &(), &Timestamp::MIN, &1)
                    .await
                    .expect("valid update");
            }
            builder
                .finish(Antichain::from_elem(Timestamp::MIN.step_forward()))
                .await
                .expect("valid bounds")
                .into_transmittable_batch()
        }

        pub(crate) async fn wait_until_nothing_is_left(&self) {
            for _ in 0..BLOB_TURNS {
                let written = self.succeeded("blob_set");
                if written > 0 && self.succeeded("blob_delete") == written {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
            panic!(
                "{} of {} blob keys left behind",
                self.succeeded("blob_set") - self.succeeded("blob_delete"),
                self.succeeded("blob_set")
            );
        }
    }

    #[mz_ore::test(tokio::test)]
    async fn a_dropped_owned_stash_deletes_its_batches() {
        let persist = CountedClient::new().await;
        let shard_id = ShardId::new();
        let protos = vec![
            persist.write_batch(shard_id).await,
            persist.write_batch(shard_id).await,
        ];
        assert!(persist.succeeded("blob_set") > 0);
        assert_eq!(persist.succeeded("blob_delete"), 0);

        let stash = OwnedStash::from_proto(&persist.client, shard_id, protos);
        assert!(stash.encoded_size_bytes() > 0);
        drop(stash);

        persist.wait_until_nothing_is_left().await;
    }

    #[mz_ore::test(tokio::test)]
    async fn a_taken_owned_stash_deletes_nothing() {
        let persist = CountedClient::new().await;
        let shard_id = ShardId::new();
        let protos = vec![persist.write_batch(shard_id).await];
        let written = persist.succeeded("blob_set");

        let stash = OwnedStash::from_proto(&persist.client, shard_id, protos);
        let batches = stash.take();
        assert_eq!(batches.len(), 1);

        // Give a wrongly spawned delete the same turns a correct one gets.
        for _ in 0..BLOB_TURNS {
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        assert_eq!(persist.succeeded("blob_delete"), 0);

        // The caller owns deletion now.
        for batch in batches {
            batch.delete().await;
        }
        assert_eq!(persist.succeeded("blob_delete"), written);
    }
}

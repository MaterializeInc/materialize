// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Snapshot reads using committed collection metadata, without a controller inventory.

use std::sync::Arc;

use futures::future::BoxFuture;
use futures::stream::BoxStream;
use futures::{FutureExt, StreamExt};
use mz_persist_client::cfg::USE_CRITICAL_SINCE_SNAPSHOT;
use mz_persist_client::read::ReadHandle;
use mz_persist_client::stats::{SnapshotPartsStats, SnapshotStats};
use mz_persist_client::{Diagnostics, PersistClient};
use mz_persist_types::codec_impls::UnitSchema;
use mz_repr::{GlobalId, Row, Timestamp};
use mz_storage_types::StorageDiff;
use mz_storage_types::controller::{CollectionMetadata, StorageError};
use mz_storage_types::sources::SourceData;
use mz_txn_wal::txn_read::{DataSnapshot, TxnsRead};
use timely::progress::Antichain;

pub use crate::storage_collections::SnapshotCursor;

type SnapshotReadHandle = ReadHandle<SourceData, (), Timestamp, StorageDiff>;

/// A cheaply cloneable reader with no collection inventory or read authority.
///
/// Callers must supply committed metadata at this client's Persist location and
/// valid read authority for the requested timestamp. In particular, callers are
/// responsible for rejecting missing IDs and protecting the read from compaction.
#[derive(Clone, Debug)]
pub struct CollectionReader {
    persist: PersistClient,
    txns_read: TxnsRead<Timestamp>,
}

impl CollectionReader {
    pub fn new(persist: PersistClient, txns_read: TxnsRead<Timestamp>) -> Self {
        Self { persist, txns_read }
    }

    async fn read_handle(&self, id: GlobalId, metadata: &CollectionMetadata) -> SnapshotReadHandle {
        self.persist
            .open_leased_reader(
                metadata.data_shard,
                Arc::new(metadata.relation_desc.clone()),
                Arc::new(UnitSchema),
                Diagnostics {
                    shard_name: id.to_string(),
                    handle_purpose: format!("snapshot {id}"),
                },
                USE_CRITICAL_SINCE_SNAPSHOT.get(self.persist.dyncfgs()),
            )
            .await
            .expect("invalid persist usage")
    }

    async fn data_snapshot(
        &self,
        metadata: &CollectionMetadata,
        as_of: Timestamp,
    ) -> Option<DataSnapshot<Timestamp>> {
        let txns_id = metadata.txns_shard.as_ref()?;
        assert_eq!(txns_id, self.txns_read.txns_id());
        // The logical upper can be ahead of the physical data shard upper.
        // Txn-wal identifies intervening writes and intervals empty of writes.
        self.txns_read.update_gt(as_of).await;
        Some(
            self.txns_read
                .data_snapshot(metadata.data_shard, as_of)
                .await,
        )
    }

    /// Returns Persist's aggregate estimates. Lazy-table estimates do not advance
    /// the physical data shard upper. An empty as-of is invalid for lazy tables.
    pub fn snapshot_stats(
        &self,
        id: GlobalId,
        metadata: CollectionMetadata,
        as_of: Antichain<Timestamp>,
    ) -> BoxFuture<'static, Result<SnapshotStats, StorageError>> {
        let reader = self.clone();
        async move {
            let snapshot = if metadata.txns_shard.is_some() {
                let ts = as_of
                    .as_option()
                    .expect("cannot read as_of the empty antichain");
                reader.data_snapshot(&metadata, *ts).await
            } else {
                None
            };
            let handle = reader.read_handle(id, &metadata).await;
            let result = match snapshot {
                Some(snapshot) => snapshot.snapshot_stats_from_leased(&handle).await,
                None => handle.snapshot_stats(Some(as_of)).await,
            };
            handle.expire().await;
            result.map_err(|_| StorageError::ReadBeforeSince(id))
        }
        .boxed()
    }

    /// Returns native per-part statistics, unblocking lazy data shards as needed.
    /// An empty as-of uses Persist directly, including for lazy tables.
    pub fn snapshot_parts_stats(
        &self,
        id: GlobalId,
        metadata: CollectionMetadata,
        as_of: Antichain<Timestamp>,
    ) -> BoxFuture<'static, Result<SnapshotPartsStats, StorageError>> {
        let reader = self.clone();
        async move {
            let handle = reader.read_handle(id, &metadata).await;
            let snapshot = match as_of.as_option() {
                Some(ts) => reader.data_snapshot(&metadata, *ts).await,
                None => None,
            };
            let result = match snapshot {
                Some(snapshot) => snapshot.snapshot_parts_stats(&handle).await,
                None => handle.snapshot_parts_stats(as_of).await,
            };
            handle.expire().await;
            result.map_err(|_| StorageError::ReadBeforeSince(id))
        }
        .boxed()
    }

    /// Fetches consolidated rows and multiplicities, propagating stored data errors.
    pub fn snapshot_and_fetch(
        &self,
        id: GlobalId,
        metadata: CollectionMetadata,
        as_of: Timestamp,
    ) -> BoxFuture<'static, Result<Vec<(Row, StorageDiff)>, StorageError>> {
        let reader = self.clone();
        async move {
            let mut handle = reader.read_handle(id, &metadata).await;
            let result = match reader.data_snapshot(&metadata, as_of).await {
                Some(snapshot) => snapshot.snapshot_and_fetch(&mut handle).await,
                None => handle.snapshot_and_fetch(Antichain::from_elem(as_of)).await,
            };
            handle.expire().await;
            result
                .map_err(|_| StorageError::ReadBeforeSince(id))?
                .into_iter()
                .map(|((data, ()), _, diff)| Ok((data.0?, diff)))
                .collect()
        }
        .boxed()
    }

    /// Returns a cursor retaining its leased read handle and native source data.
    pub fn snapshot_cursor(
        &self,
        id: GlobalId,
        metadata: CollectionMetadata,
        as_of: Timestamp,
    ) -> BoxFuture<'static, Result<SnapshotCursor, StorageError>> {
        let reader = self.clone();
        async move {
            let mut handle = reader.read_handle(id, &metadata).await;
            let result = match reader.data_snapshot(&metadata, as_of).await {
                Some(snapshot) => snapshot.snapshot_cursor(&mut handle, |_| true).await,
                None => {
                    handle
                        .snapshot_cursor(Antichain::from_elem(as_of), |_| true)
                        .await
                }
            };
            match result {
                Ok(cursor) => Ok(SnapshotCursor {
                    _read_handle: handle,
                    cursor,
                }),
                Err(_) => {
                    handle.expire().await;
                    Err(StorageError::ReadBeforeSince(id))
                }
            }
        }
        .boxed()
    }

    /// Streams native source data, timestamps, and differences. Persist's stream
    /// owns the part leases, which remain live after the temporary handle drops.
    pub fn snapshot_and_stream(
        &self,
        id: GlobalId,
        metadata: CollectionMetadata,
        as_of: Timestamp,
    ) -> BoxFuture<
        'static,
        Result<BoxStream<'static, (SourceData, Timestamp, StorageDiff)>, StorageError>,
    > {
        let reader = self.clone();
        async move {
            let mut handle = reader.read_handle(id, &metadata).await;
            let result = match reader.data_snapshot(&metadata, as_of).await {
                Some(snapshot) => snapshot
                    .snapshot_and_stream(&mut handle)
                    .await
                    .map(|s| s.boxed()),
                None => handle
                    .snapshot_and_stream(Antichain::from_elem(as_of))
                    .await
                    .map(|s| s.boxed()),
            };
            match result {
                Ok(stream) => Ok(stream
                    .map(|((data, ()), ts, diff)| (data, ts, diff))
                    .boxed()),
                Err(_) => {
                    handle.expire().await;
                    Err(StorageError::ReadBeforeSince(id))
                }
            }
        }
        .boxed()
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use mz_ore::metrics::MetricsRegistry;
    use mz_persist_client::cache::PersistClientCache;
    use mz_persist_client::critical::Opaque;
    use mz_persist_client::{PersistLocation, ShardId};
    use mz_repr::RelationDesc;
    use mz_storage_types::controller::TxnsCodecRow;
    use mz_txn_wal::metrics::Metrics;
    use mz_txn_wal::txns::TxnsHandle;

    use super::*;

    const ID: GlobalId = GlobalId::User(1);

    fn frontier(ts: u64) -> Antichain<Timestamp> {
        Antichain::from_elem(ts.into())
    }

    // Keep real Persist read authority alive, independently of CollectionReader.
    async fn fixture(lazy: bool) -> (CollectionReader, CollectionMetadata, SnapshotReadHandle) {
        let cache = PersistClientCache::new_no_metrics();
        let mut updates = mz_dyncfg::ConfigUpdates::default();
        updates.add(&USE_CRITICAL_SINCE_SNAPSHOT, false);
        updates.apply(cache.cfg());
        let location = PersistLocation::new_in_mem();
        let client = cache.open(location.clone()).await.unwrap();
        let txns_id = ShardId::new();
        let mut txns: TxnsHandle<SourceData, (), Timestamp, StorageDiff, TxnsCodecRow> =
            TxnsHandle::open(
                0.into(),
                client.clone(),
                mz_dyncfgs::all_dyncfgs(),
                Arc::new(Metrics::new(&MetricsRegistry::new())),
                txns_id,
                Opaque::encode(&0u64),
            )
            .await;
        let metadata = CollectionMetadata {
            persist_location: location,
            data_shard: ShardId::new(),
            relation_desc: RelationDesc::empty(),
            txns_shard: lazy.then_some(txns_id),
        };
        let (mut write, authority) = client
            .open::<SourceData, (), Timestamp, StorageDiff>(
                metadata.data_shard,
                Arc::new(metadata.relation_desc.clone()),
                Arc::new(UnitSchema),
                Diagnostics {
                    shard_name: ID.to_string(),
                    handle_purpose: "reader test".into(),
                },
                false,
            )
            .await
            .unwrap();
        let data = SourceData(Ok(Row::default()));
        if lazy {
            txns.register(1.into(), [write]).await.unwrap();
            let mut txn = txns.begin();
            txn.write(&metadata.data_shard, data, (), 2).await;
            txn.commit_at(&mut txns, 2.into())
                .await
                .unwrap()
                .apply(&mut txns)
                .await;
            // Advance logical time without advancing this table's physical upper.
            txns.begin()
                .commit_at(&mut txns, 10.into())
                .await
                .unwrap()
                .apply(&mut txns)
                .await;
        } else {
            write
                .compare_and_append(
                    [((data, ()), Timestamp::from(2), 2)],
                    frontier(0),
                    frontier(11),
                )
                .await
                .unwrap()
                .unwrap();
            write.expire().await;
        }
        let txns_read = TxnsRead::start::<TxnsCodecRow>(client.clone(), txns_id).await;
        (
            CollectionReader::new(client, txns_read),
            metadata,
            authority,
        )
    }

    async fn snapshot_contract(lazy: bool) {
        let (reader, metadata, authority) = fixture(lazy).await;
        let mut upper_reader =
            mz_persist_client::write::WriteHandle::from_read(&authority, "test upper");
        let upper = upper_reader.fetch_recent_upper().await.clone();
        if lazy {
            assert_eq!(upper, frontier(3));
        }
        let stats = reader
            .snapshot_stats(ID, metadata.clone(), frontier(7))
            .await
            .unwrap();
        assert_eq!(stats.shard_id, metadata.data_shard);
        assert_eq!(stats.num_updates, 1);
        // Estimates must not write to the data shard to unblock the logical as-of.
        assert_eq!(upper_reader.fetch_recent_upper().await, &upper);
        upper_reader.expire().await;

        let parts = reader
            .snapshot_parts_stats(ID, metadata.clone(), frontier(7))
            .await
            .unwrap();
        assert_eq!(parts.shard_id, metadata.data_shard);
        assert!(!parts.parts.is_empty());
        assert!(parts.parts.iter().all(|part| part.encoded_size_bytes > 0));
        assert_eq!(
            reader
                .snapshot_and_fetch(ID, metadata.clone(), 7.into())
                .await
                .unwrap(),
            vec![(Row::default(), 2)],
        );

        // Both incremental interfaces preserve source data, logical timestamps,
        // and multiplicities, and can be consumed after the reader is dropped.
        let mut cursor = reader
            .snapshot_cursor(ID, metadata.clone(), 7.into())
            .await
            .unwrap();
        let stream = reader
            .snapshot_and_stream(ID, metadata, 7.into())
            .await
            .unwrap();
        drop(reader);
        let mut rows = Vec::new();
        while let Some(part) = cursor.next().await {
            rows.extend(part);
        }
        let expected = vec![(SourceData(Ok(Row::default())), Timestamp::from(7), 2)];
        assert_eq!(rows, expected);
        assert_eq!(stream.collect::<Vec<_>>().await, expected);
        drop(cursor);
        authority.expire().await;
    }

    #[mz_ore::test(tokio::test)]
    async fn direct_snapshot_contract() {
        tokio::time::timeout(Duration::from_secs(30), snapshot_contract(false))
            .await
            .unwrap();
    }

    #[mz_ore::test(tokio::test)]
    async fn lazy_snapshot_contract() {
        tokio::time::timeout(Duration::from_secs(30), snapshot_contract(true))
            .await
            .unwrap();
    }

    #[mz_ore::test(tokio::test)]
    async fn stored_data_error() {
        use mz_storage_types::errors::{DataflowError, SourceError, SourceErrorDetails};

        let (reader, metadata, authority) = fixture(false).await;
        let error = DataflowError::from(SourceError {
            error: SourceErrorDetails::Other("snapshot test error".into()),
            hint: None,
        });
        let data = SourceData(Err(error.clone()));
        let mut write =
            mz_persist_client::write::WriteHandle::from_read(&authority, "test data error");
        write
            .compare_and_append(
                [((data.clone(), ()), Timestamp::from(11), 1)],
                frontier(11),
                frontier(12),
            )
            .await
            .unwrap()
            .unwrap();
        write.expire().await;

        assert!(matches!(
            reader.snapshot_and_fetch(ID, metadata.clone(), 11.into()).await,
            Err(StorageError::DataflowError(actual)) if actual == error
        ));
        let mut cursor = reader
            .snapshot_cursor(ID, metadata.clone(), 11.into())
            .await
            .unwrap();
        let mut rows = Vec::new();
        while let Some(part) = cursor.next().await {
            rows.extend(part);
        }
        assert!(rows.contains(&(data.clone(), Timestamp::from(11), 1)));
        let rows = reader
            .snapshot_and_stream(ID, metadata, 11.into())
            .await
            .unwrap()
            .collect::<Vec<_>>()
            .await;
        assert!(rows.contains(&(data, Timestamp::from(11), 1)));
        drop(cursor);
        authority.expire().await;
    }

    #[mz_ore::test(tokio::test)]
    async fn read_before_since() {
        let (reader, metadata, mut authority) = fixture(false).await;
        authority.downgrade_since(&frontier(5)).await;
        // All five interfaces report the requested collection, not a Persist shard ID.
        assert!(matches!(
            reader
                .snapshot_stats(ID, metadata.clone(), frontier(2))
                .await,
            Err(StorageError::ReadBeforeSince(ID))
        ));
        assert!(matches!(
            reader
                .snapshot_parts_stats(ID, metadata.clone(), frontier(2))
                .await,
            Err(StorageError::ReadBeforeSince(ID))
        ));
        assert!(matches!(
            reader
                .snapshot_and_fetch(ID, metadata.clone(), 2.into())
                .await,
            Err(StorageError::ReadBeforeSince(ID))
        ));
        assert!(matches!(
            reader.snapshot_cursor(ID, metadata.clone(), 2.into()).await,
            Err(StorageError::ReadBeforeSince(ID))
        ));
        assert!(matches!(
            reader.snapshot_and_stream(ID, metadata, 2.into()).await,
            Err(StorageError::ReadBeforeSince(ID))
        ));
        authority.expire().await;
    }
}

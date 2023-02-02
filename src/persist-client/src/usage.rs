// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Introspection of storage utilization by persist

use std::collections::BTreeMap;
use std::sync::Arc;
use tracing::info;

use crate::{retry_external, Metrics, ShardId};
use mz_persist::location::{Blob, ExternalError};

use crate::cache::PersistClientCache;
use crate::internal::paths::{BlobKey, BlobKeyPrefix, PartialBlobKey};

/// Provides access to storage usage metrics for a specific Blob
#[derive(Clone, Debug)]
pub struct StorageUsageClient {
    blob: Arc<dyn Blob + Send + Sync>,
    metrics: Arc<Metrics>,
}

impl StorageUsageClient {
    /// Creates a new StorageUsageClient pointed to a specific Blob
    pub async fn open(
        blob_uri: String,
        client_cache: &PersistClientCache,
    ) -> Result<StorageUsageClient, ExternalError> {
        let blob = client_cache.open_blob(blob_uri).await?;
        let metrics = Arc::clone(&client_cache.metrics);
        Ok(StorageUsageClient { blob, metrics })
    }

    /// Returns the size (in bytes) of all blobs owned by a given [ShardId]
    pub async fn shard_size(&self, shard_id: &ShardId) -> u64 {
        retry_external(
            &self.metrics.retries.external.storage_usage_shard_size,
            || self.size(BlobKeyPrefix::Shard(shard_id)),
        )
        .await
    }

    /// Returns a map of [ShardId] to its size (in bytes) stored in persist, as
    /// well as the total bytes stored for unattributable data (the map key is
    /// None) where the shard id is unknown. This latter count _should_ always
    /// be 0, so it being nonzero indicates either persist wrote an invalid blob
    /// key, or another process is storing data under the same path (!)
    pub async fn shard_sizes(&self) -> BTreeMap<Option<ShardId>, u64> {
        retry_external(
            &self.metrics.retries.external.storage_usage_shard_size,
            || async {
                let mut shard_sizes = BTreeMap::new();
                let mut batch_part_bytes = 0;
                let mut batch_part_count = 0;
                let mut rollup_size = 0;
                let mut rollup_count = 0;
                let mut total_size = 0;
                let mut total_count = 0;
                self.blob
                    .list_keys_and_metadata(&BlobKeyPrefix::All.to_string(), &mut |metadata| {
                        match BlobKey::parse_ids(metadata.key) {
                            Ok((shard, partial_blob_key)) => {
                                *shard_sizes.entry(Some(shard)).or_insert(0) +=
                                    metadata.size_in_bytes;

                                match partial_blob_key {
                                    PartialBlobKey::Batch(_, _) => {
                                        batch_part_bytes += metadata.size_in_bytes;
                                        batch_part_count += 1;
                                    }
                                    PartialBlobKey::Rollup(_, _) => {
                                        rollup_size += metadata.size_in_bytes;
                                        rollup_count += 1;
                                    }
                                }
                            }
                            _ => {
                                info!("unknown blob: {}: {}", metadata.key, metadata.size_in_bytes);
                                *shard_sizes.entry(None).or_insert(0) += metadata.size_in_bytes;
                            }
                        }
                        total_size += metadata.size_in_bytes;
                        total_count += 1;
                    })
                    .await?;

                self.metrics
                    .audit
                    .blob_batch_part_bytes
                    .set(batch_part_bytes);
                self.metrics
                    .audit
                    .blob_batch_part_count
                    .set(batch_part_count);
                self.metrics.audit.blob_rollup_bytes.set(rollup_size);
                self.metrics.audit.blob_rollup_count.set(rollup_count);
                self.metrics.audit.blob_bytes.set(total_size);
                self.metrics.audit.blob_count.set(total_count);

                Ok(shard_sizes)
            },
        )
        .await
    }

    /// Returns the size (in bytes) of a subset of blobs specified by
    /// [BlobKeyPrefix]
    ///
    /// Can be safely called within retry_external to ensure it succeeds
    async fn size(&self, prefix: BlobKeyPrefix<'_>) -> Result<u64, ExternalError> {
        let mut total_size = 0;
        self.blob
            .list_keys_and_metadata(&prefix.to_string(), &mut |metadata| {
                total_size += metadata.size_in_bytes;
            })
            .await?;
        Ok(total_size)
    }

    #[cfg(test)]
    fn open_from_blob(blob: Arc<dyn Blob + Send + Sync>) -> StorageUsageClient {
        use mz_build_info::DUMMY_BUILD_INFO;
        use mz_ore::metrics::MetricsRegistry;
        use mz_ore::now::SYSTEM_TIME;

        use crate::PersistConfig;
        let cfg = PersistConfig::new(&DUMMY_BUILD_INFO, SYSTEM_TIME.clone());
        StorageUsageClient {
            blob: Arc::clone(&blob),
            metrics: Arc::new(Metrics::new(&cfg, &MetricsRegistry::new())),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::tests::new_test_client;
    use crate::ShardId;

    use super::*;

    #[tokio::test]
    async fn size() {
        mz_ore::test::init_logging();

        let data = vec![
            (("1".to_owned(), "one".to_owned()), 1, 1),
            (("2".to_owned(), "two".to_owned()), 2, 1),
            (("3".to_owned(), "three".to_owned()), 3, 1),
            (("4".to_owned(), "four".to_owned()), 4, 1),
        ];

        let client = new_test_client().await;
        let shard_id_one = ShardId::new();
        let shard_id_two = ShardId::new();

        // write one row into shard 1
        let (mut write, _) = client
            .expect_open::<String, String, u64, i64>(shard_id_one)
            .await;
        write.expect_append(&data[..1], vec![0], vec![2]).await;

        // write two rows into shard 2 from writer 1
        let (mut write, _) = client
            .expect_open::<String, String, u64, i64>(shard_id_two)
            .await;
        write.expect_append(&data[1..3], vec![0], vec![4]).await;
        let writer_one = write.writer_id.clone();

        // write one row into shard 2 from writer 2
        let (mut write, _) = client
            .expect_open::<String, String, u64, i64>(shard_id_two)
            .await;
        write.expect_append(&data[4..], vec![0], vec![5]).await;
        let writer_two = write.writer_id.clone();

        let usage = StorageUsageClient::open_from_blob(Arc::clone(&write.blob));

        let shard_one_size = usage
            .size(BlobKeyPrefix::Shard(&shard_id_one))
            .await
            .expect("must have shard size");
        let shard_two_size = usage
            .size(BlobKeyPrefix::Shard(&shard_id_two))
            .await
            .expect("must have shard size");
        let writer_one_size = usage
            .size(BlobKeyPrefix::Writer(&shard_id_two, &writer_one))
            .await
            .expect("must have shard size");
        let writer_two_size = usage
            .size(BlobKeyPrefix::Writer(&shard_id_two, &writer_two))
            .await
            .expect("must have shard size");
        let rollups_size = usage
            .size(BlobKeyPrefix::Rollups(&shard_id_two))
            .await
            .expect("must have shard size");
        let all_size = usage
            .size(BlobKeyPrefix::All)
            .await
            .expect("must have shard size");

        assert!(shard_one_size > 0);
        assert!(shard_two_size > 0);
        assert!(shard_one_size < shard_two_size);
        assert_eq!(
            shard_two_size,
            writer_one_size + writer_two_size + rollups_size
        );
        assert_eq!(all_size, shard_one_size + shard_two_size);

        assert_eq!(usage.shard_size(&shard_id_one).await, shard_one_size);
        assert_eq!(usage.shard_size(&shard_id_two).await, shard_two_size);

        let shard_sizes = usage.shard_sizes().await;
        assert_eq!(shard_sizes.len(), 2);
        assert_eq!(shard_sizes.get(&Some(shard_id_one)), Some(&shard_one_size));
        assert_eq!(shard_sizes.get(&Some(shard_id_two)), Some(&shard_two_size));
    }
}

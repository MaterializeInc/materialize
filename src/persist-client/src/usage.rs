// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Introspection of storage utilization by persist

use std::sync::Arc;

use crate::{retry_external, Metrics, ShardId};
use mz_persist::location::{Blob, ExternalError};

use crate::cache::PersistClientCache;
use crate::r#impl::paths::BlobKeyPrefix;

/// Provides access to storage usage metrics for a specific Blob
#[derive(Debug)]
pub struct StorageUsageClient {
    blob: Arc<dyn Blob + Send + Sync>,
    metrics: Arc<Metrics>,
}

impl StorageUsageClient {
    /// Creates a new StorageUsageClient pointed to a specific Blob
    pub async fn open(
        blob_uri: String,
        client_cache: &mut PersistClientCache,
    ) -> Result<StorageUsageClient, ExternalError> {
        let blob = client_cache.open_blob(blob_uri).await?;
        let metrics = Arc::clone(&client_cache.metrics);
        Ok(StorageUsageClient { blob, metrics })
    }

    /// Returns the size (in bytes) of all blobs owned by a given [crate::ShardId]
    pub async fn shard_size(&self, shard_id: &ShardId) -> u64 {
        retry_external(
            &self.metrics.retries.external.storage_usage_shard_size,
            || self.size(BlobKeyPrefix::Shard(shard_id)),
        )
        .await
    }

    /// Returns the size (in bytes) of a subset of blobs specified by [crate::impl::paths::BlobKeyPrefix]
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
        use mz_ore::metrics::MetricsRegistry;
        StorageUsageClient {
            blob: Arc::clone(&blob),
            metrics: Arc::new(Metrics::new(&MetricsRegistry::new())),
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
        let all_size = usage
            .size(BlobKeyPrefix::All)
            .await
            .expect("must have shard size");

        assert!(shard_one_size > 0);
        assert!(shard_two_size > 0);
        assert!(shard_one_size < shard_two_size);
        assert_eq!(shard_two_size, writer_one_size + writer_two_size);
        assert_eq!(all_size, shard_one_size + shard_two_size);

        assert_eq!(usage.shard_size(&shard_id_one).await, shard_one_size);
        assert_eq!(usage.shard_size(&shard_id_two).await, shard_two_size);
    }
}

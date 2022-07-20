// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Introspection of storage utilization by persist

use mz_ore::metrics::MetricsRegistry;
use std::sync::Arc;

use mz_persist::cfg::BlobConfig;
use mz_persist::location::{Blob, ExternalError};

use crate::{retry_external, Metrics, ShardId};

/// Provides access to storage usage metrics for a specific Blob
#[derive(Debug)]
pub struct StorageUsageClient {
    blob: Arc<dyn Blob + Send + Sync>,
}

impl StorageUsageClient {
    /// Creates a new StorageUsageClient pointed to a specific Blob
    pub async fn open(blob_uri: &str) -> Result<StorageUsageClient, ExternalError> {
        let blob = BlobConfig::try_from(blob_uri).await?;

        // TODO: what do we want to do about the metrics here?
        let metrics_registry = MetricsRegistry::new();
        let metrics = Metrics::new(&metrics_registry);
        let blob =
            retry_external(&metrics.retries.external.blob_open, || blob.clone().open()).await;

        Ok(StorageUsageClient { blob })
    }

    /// Returns the size (in bytes) of a shard's data stored within this Blob
    pub async fn shard_size(&self, shard_id: &ShardId) -> Result<u64, ExternalError> {
        let mut total_size = 0;
        self.blob
            .list_keys_and_metadata(Some(&shard_id.to_string()), &mut |metadata| {
                total_size += metadata.size_in_bytes;
            })
            .await?;
        Ok(total_size)
    }

    #[cfg(test)]
    fn open_from_blob(blob: Arc<dyn Blob + Send + Sync>) -> StorageUsageClient {
        StorageUsageClient {
            blob: Arc::clone(&blob),
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::tests::new_test_client;

    use super::*;

    #[tokio::test]
    async fn shard_size() {
        mz_ore::test::init_logging();

        let data = vec![
            (("1".to_owned(), "one".to_owned()), 1, 1),
            (("2".to_owned(), "two".to_owned()), 2, 1),
            (("3".to_owned(), "three".to_owned()), 3, 1),
        ];

        let client = new_test_client().await;
        let shard_id_one = ShardId::new();
        let shard_id_two = ShardId::new();

        // write one row into shard 1
        let (mut write, _) = client
            .expect_open::<String, String, u64, i64>(shard_id_one)
            .await;
        write.expect_append(&data[..1], vec![0], vec![2]).await;

        // write two rows into shard 2
        let (mut write, _) = client
            .expect_open::<String, String, u64, i64>(shard_id_two)
            .await;
        write.expect_append(&data[1..], vec![0], vec![4]).await;

        let usage = StorageUsageClient::open_from_blob(Arc::clone(&write.blob));
        let shard_one_size = usage
            .shard_size(&shard_id_one)
            .await
            .expect("must have shard size");
        let shard_two_size = usage
            .shard_size(&shard_id_two)
            .await
            .expect("must have shard size");

        assert!(shard_one_size > 0);
        assert!(shard_two_size > 0);
        assert!(shard_one_size < shard_two_size);
    }
}

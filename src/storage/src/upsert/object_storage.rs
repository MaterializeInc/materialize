// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! An `UpsertStateBackend` that stores values in object storage.

use std::sync::Arc;

use futures::StreamExt;
use itertools::Itertools;
use mz_repr::GlobalId;
use object_store::ObjectStore;
use object_store::aws::AmazonS3Builder;
use object_store::path::Path;
use serde::{Serialize, de::DeserializeOwned};
use slatedb::object_store::memory::InMemory;

use super::UpsertKey;
use super::types::{
    BincodeOpts, GetStats, MergeStats, MergeValue, PutStats, PutValue, StateValue,
    UpsertStateBackend, UpsertValueAndSize, ValueMetadata, upsert_bincode_opts,
};

/// Configuration for the object storage upsert backend.
#[derive(Clone, Debug)]
pub struct ObjectStorageConfig {
    /// Whether to use in-memory storage (for testing).
    pub in_memory: bool,
    /// S3 bucket name (only used if in_memory is false).
    pub s3_bucket: String,
    /// S3 region (only used if in_memory is false).
    pub s3_region: String,
    /// Custom S3 endpoint (only used if in_memory is false, empty string for AWS default).
    pub s3_endpoint: String,
    /// AWS access key ID (empty string uses environment/default credentials).
    pub s3_access_key_id: String,
    /// AWS secret access key (empty string uses environment/default credentials).
    pub s3_secret_access_key: String,
    /// Unique identifier for this replica instance. Used to differentiate
    /// multiple replicas when using shared object storage.
    pub replica_id: uuid::Uuid,
}

pub struct ObjectStorageUpsertBackend<T, O> {
    store: slatedb::Db,
    bincode_opts: BincodeOpts,
    /// The object store, kept for cleanup on shutdown.
    object_store: Arc<dyn ObjectStore>,
    /// The path prefix used for this backend's data.
    path: String,

    _phantom: std::marker::PhantomData<(T, O)>,
}

impl<T, O> ObjectStorageUpsertBackend<T, O> {
    /// Create a new backend with in-memory storage (for testing).
    pub async fn new_in_memory(
        replica_id: uuid::Uuid,
        source_id: GlobalId,
        worker_id: usize,
    ) -> Self {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
        // Include replica_id in path to ensure uniqueness across replicas
        let path = format!("upsert/{}/{}/{}", replica_id, source_id, worker_id);
        let store = slatedb::Db::open(path.clone(), Arc::clone(&object_store))
            .await
            .expect("db to open");
        Self {
            store,
            bincode_opts: upsert_bincode_opts(),
            object_store,
            path,
            _phantom: std::marker::PhantomData,
        }
    }

    /// Create a new backend with S3 storage.
    pub async fn new_s3(
        bucket: &str,
        region: &str,
        endpoint: &str,
        access_key_id: &str,
        secret_access_key: &str,
        replica_id: uuid::Uuid,
        source_id: GlobalId,
        worker_id: usize,
    ) -> Result<Self, anyhow::Error> {
        let mut builder = AmazonS3Builder::from_env()
            .with_bucket_name(bucket)
            .with_region(region);

        if !endpoint.is_empty() {
            builder = builder.with_endpoint(endpoint).with_allow_http(true);
        }

        // Use explicit credentials if provided, otherwise fall back to env/default
        if !access_key_id.is_empty() && !secret_access_key.is_empty() {
            builder = builder
                .with_access_key_id(access_key_id)
                .with_secret_access_key(secret_access_key);
        }

        let object_store: Arc<dyn ObjectStore> = Arc::new(
            builder
                .build()
                .map_err(|e| anyhow::anyhow!("Failed to create S3 object store: {}", e))?,
        );

        // Include replica_id in path to ensure uniqueness across replicas
        let path = format!("upsert/{}/{}/{}", replica_id, source_id, worker_id);
        let store = slatedb::Db::open(path.clone(), Arc::clone(&object_store))
            .await
            .map_err(|e| anyhow::anyhow!("Failed to open SlateDB: {}", e))?;

        Ok(Self {
            store,
            bincode_opts: upsert_bincode_opts(),
            object_store,
            path,
            _phantom: std::marker::PhantomData,
        })
    }

    /// Create a new backend from configuration.
    pub async fn from_config(
        config: &ObjectStorageConfig,
        source_id: GlobalId,
        worker_id: usize,
    ) -> Result<Self, anyhow::Error> {
        if config.in_memory {
            Ok(Self::new_in_memory(config.replica_id, source_id, worker_id).await)
        } else {
            Self::new_s3(
                &config.s3_bucket,
                &config.s3_region,
                &config.s3_endpoint,
                &config.s3_access_key_id,
                &config.s3_secret_access_key,
                config.replica_id,
                source_id,
                worker_id,
            )
            .await
        }
    }
}

#[async_trait::async_trait(?Send)]
impl<T, O> UpsertStateBackend<T, O> for ObjectStorageUpsertBackend<T, O>
where
    O: Send + Sync + Serialize + DeserializeOwned + 'static,
    T: Send + Sync + Serialize + DeserializeOwned + 'static,
{
    // Slatedb supports merges but I'm lazy and I'm not implementing it yet.
    fn supports_merge(&self) -> bool {
        false
    }

    async fn multi_put<P>(&mut self, puts: P) -> Result<PutStats, anyhow::Error>
    where
        P: IntoIterator<Item = (UpsertKey, PutValue<StateValue<T, O>>)>,
    {
        use bincode::Options;
        use slatedb::WriteBatch;

        let mut stats = PutStats::default();
        let mut batch = WriteBatch::new();

        for (
            key,
            PutValue {
                value,
                previous_value_metadata,
            },
        ) in puts
        {
            let new_size = match &value {
                Some(v) => {
                    let serialized = self
                        .bincode_opts
                        .serialize(v)
                        .map_err(|e| anyhow::anyhow!("Error serializing value: {}", e))?;
                    let size = i64::try_from(serialized.len()).expect("less than i64 size");
                    batch.put(&key, serialized);
                    Some(size)
                }
                None => {
                    batch.delete(&key);
                    None
                }
            };

            stats.adjust(value.as_ref(), new_size, &previous_value_metadata);
            stats.processed_puts += 1;
        }

        self.store
            .write(batch)
            .await
            .map_err(|e| anyhow::anyhow!("Error writing batch to object store: {}", e))?;

        Ok(stats)
    }

    async fn multi_get<'r, G, R>(
        &mut self,
        gets: G,
        results_out: R,
    ) -> Result<GetStats, anyhow::Error>
    where
        G: IntoIterator<Item = UpsertKey>,
        R: IntoIterator<Item = &'r mut UpsertValueAndSize<T, O>>,
    {
        use bincode::Options;

        // Several things to consider here:
        // - slatedb doesn't have a multi-get API yet, so we have to do them one by one.
        // - slatedb's get API returns a Bytes object which is a slice into a full 4KiB block.
        //   We need to copy the data out of it to return owned T and O values.

        let mut stats = GetStats::default();

        for (key, result_out) in gets.into_iter().zip_eq(results_out.into_iter()) {
            stats.processed_gets += 1;

            match self.store.get(&key).await {
                Ok(Some(bytes)) => {
                    let size = bytes.len();
                    stats.processed_gets_size += u64::try_from(size).unwrap_or(u64::MAX);
                    stats.returned_gets += 1;

                    let value: StateValue<T, O> = self
                        .bincode_opts
                        .deserialize(&bytes)
                        .map_err(|e| anyhow::anyhow!("Error deserializing value: {}", e))?;

                    let is_tombstone = value.is_tombstone();
                    *result_out = UpsertValueAndSize {
                        value: Some(value),
                        metadata: Some(ValueMetadata {
                            size: u64::try_from(size).unwrap_or(u64::MAX),
                            is_tombstone,
                        }),
                    };
                }
                Ok(None) => {
                    *result_out = UpsertValueAndSize {
                        value: None,
                        metadata: None,
                    };
                }
                Err(e) => {
                    return Err(anyhow::anyhow!(
                        "Error getting key from object store: {}",
                        e
                    ));
                }
            }
        }

        Ok(stats)
    }

    async fn multi_merge<P>(&mut self, _merges: P) -> Result<MergeStats, anyhow::Error>
    where
        P: IntoIterator<Item = (UpsertKey, MergeValue<StateValue<T, O>>)>,
    {
        unimplemented!("ObjectStorageUpsertBackend::multi_merge is not implemented yet")
    }

    async fn close(self) -> Result<(), anyhow::Error> {
        // First, close the SlateDB database
        self.store
            .close()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to close SlateDB: {}", e))?;

        // Then, delete all objects under our path prefix
        let prefix = Path::from(self.path.as_str());
        let mut list_stream = self.object_store.list(Some(&prefix));

        while let Some(result) = list_stream.next().await {
            match result {
                Ok(meta) => {
                    if let Err(e) = self.object_store.delete(&meta.location).await {
                        tracing::warn!(
                            "Failed to delete object {:?} during cleanup: {}",
                            meta.location,
                            e
                        );
                    }
                }
                Err(e) => {
                    tracing::warn!("Failed to list objects during cleanup: {}", e);
                }
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use mz_repr::Row;

    use super::*;
    use crate::upsert::UpsertValue;

    // Type aliases for tests - using () for timestamp and u64 for "from time" (offset)
    type TestTimestamp = ();
    type TestFromTime = u64;
    type TestStateValue = StateValue<TestTimestamp, TestFromTime>;
    type TestUpsertValueAndSize = UpsertValueAndSize<TestTimestamp, TestFromTime>;

    /// Helper to create an UpsertKey from an i64
    fn make_key(id: i64) -> UpsertKey {
        UpsertKey::from_key(Ok(&Row::pack_slice(&[mz_repr::Datum::Int64(id)])))
    }

    /// Helper to create a Row value
    fn make_row(key: i64, value: i64) -> Row {
        Row::pack_slice(&[mz_repr::Datum::Int64(key), mz_repr::Datum::Int64(value)])
    }

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)]
    async fn test_basic_put_and_get() {
        let replica_id = uuid::Uuid::new_v4();
        let source_id = GlobalId::User(1);
        let worker_id = 0;

        let mut backend =
            ObjectStorageUpsertBackend::new_in_memory(replica_id, source_id, worker_id).await;

        let key1 = make_key(1);
        let key2 = make_key(2);
        let value1: UpsertValue = Ok(make_row(1, 100));
        let value2: UpsertValue = Ok(make_row(2, 200));

        // Put two values
        let puts: Vec<(UpsertKey, PutValue<TestStateValue>)> = vec![
            (
                key1,
                PutValue {
                    value: Some(TestStateValue::finalized_value(value1.clone())),
                    previous_value_metadata: None,
                },
            ),
            (
                key2,
                PutValue {
                    value: Some(TestStateValue::finalized_value(value2.clone())),
                    previous_value_metadata: None,
                },
            ),
        ];

        let put_stats = backend.multi_put(puts).await.unwrap();
        assert_eq!(put_stats.processed_puts, 2);
        assert_eq!(put_stats.values_diff, 2);

        // Get the values back
        let mut results: Vec<TestUpsertValueAndSize> = vec![
            TestUpsertValueAndSize::default(),
            TestUpsertValueAndSize::default(),
        ];
        let get_stats = backend
            .multi_get(vec![key1, key2], results.iter_mut())
            .await
            .unwrap();

        assert_eq!(get_stats.processed_gets, 2);
        assert_eq!(get_stats.returned_gets, 2);

        // Verify the values
        assert!(results[0].value.is_some());
        assert!(results[1].value.is_some());

        let retrieved1 = results[0].value.as_ref().unwrap().clone().into_decoded();
        let retrieved2 = results[1].value.as_ref().unwrap().clone().into_decoded();

        assert_eq!(retrieved1.finalized, Some(value1));
        assert_eq!(retrieved2.finalized, Some(value2));
    }

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)]
    async fn test_get_nonexistent_key() {
        let replica_id = uuid::Uuid::new_v4();
        let source_id = GlobalId::User(2);
        let worker_id = 0;

        let mut backend =
            ObjectStorageUpsertBackend::new_in_memory(replica_id, source_id, worker_id).await;

        let key = make_key(999);

        let mut results: Vec<TestUpsertValueAndSize> = vec![TestUpsertValueAndSize::default()];
        let get_stats = backend
            .multi_get(vec![key], results.iter_mut())
            .await
            .unwrap();

        assert_eq!(get_stats.processed_gets, 1);
        assert_eq!(get_stats.returned_gets, 0);
        assert!(results[0].value.is_none());
        assert!(results[0].metadata.is_none());
    }

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)]
    async fn test_delete() {
        let replica_id = uuid::Uuid::new_v4();
        let source_id = GlobalId::User(3);
        let worker_id = 0;

        let mut backend =
            ObjectStorageUpsertBackend::new_in_memory(replica_id, source_id, worker_id).await;

        let key = make_key(1);
        let value: UpsertValue = Ok(make_row(1, 100));

        // Put a value
        let puts: Vec<(UpsertKey, PutValue<TestStateValue>)> = vec![(
            key,
            PutValue {
                value: Some(TestStateValue::finalized_value(value)),
                previous_value_metadata: None,
            },
        )];
        backend.multi_put(puts).await.unwrap();

        // Verify it exists
        let mut results: Vec<TestUpsertValueAndSize> = vec![TestUpsertValueAndSize::default()];
        backend
            .multi_get(vec![key], results.iter_mut())
            .await
            .unwrap();
        assert!(results[0].value.is_some());

        // Get the metadata for the delete
        let prev_metadata = results[0].metadata.map(|m| ValueMetadata {
            size: m.size.try_into().unwrap(),
            is_tombstone: m.is_tombstone,
        });

        // Delete the value
        let deletes: Vec<(UpsertKey, PutValue<TestStateValue>)> = vec![(
            key,
            PutValue {
                value: None,
                previous_value_metadata: prev_metadata,
            },
        )];
        let delete_stats = backend.multi_put(deletes).await.unwrap();
        assert_eq!(delete_stats.processed_puts, 1);
        assert_eq!(delete_stats.values_diff, -1);

        // Verify it's gone
        let mut results: Vec<TestUpsertValueAndSize> = vec![TestUpsertValueAndSize::default()];
        let get_stats = backend
            .multi_get(vec![key], results.iter_mut())
            .await
            .unwrap();
        assert_eq!(get_stats.returned_gets, 0);
        assert!(results[0].value.is_none());
    }

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)]
    async fn test_update_existing_value() {
        let replica_id = uuid::Uuid::new_v4();
        let source_id = GlobalId::User(4);
        let worker_id = 0;

        let mut backend =
            ObjectStorageUpsertBackend::new_in_memory(replica_id, source_id, worker_id).await;

        let key = make_key(1);
        let value1: UpsertValue = Ok(make_row(1, 100));
        let value2: UpsertValue = Ok(make_row(1, 200));

        // Put initial value
        let puts: Vec<(UpsertKey, PutValue<TestStateValue>)> = vec![(
            key,
            PutValue {
                value: Some(TestStateValue::finalized_value(value1)),
                previous_value_metadata: None,
            },
        )];
        backend.multi_put(puts).await.unwrap();

        // Get metadata for update
        let mut results: Vec<TestUpsertValueAndSize> = vec![TestUpsertValueAndSize::default()];
        backend
            .multi_get(vec![key], results.iter_mut())
            .await
            .unwrap();
        let prev_metadata = results[0].metadata.map(|m| ValueMetadata {
            size: m.size.try_into().unwrap(),
            is_tombstone: m.is_tombstone,
        });

        // Update to new value
        let puts: Vec<(UpsertKey, PutValue<TestStateValue>)> = vec![(
            key,
            PutValue {
                value: Some(TestStateValue::finalized_value(value2.clone())),
                previous_value_metadata: prev_metadata,
            },
        )];
        let update_stats = backend.multi_put(puts).await.unwrap();
        assert_eq!(update_stats.processed_puts, 1);
        // values_diff should be 0 since we're updating, not inserting
        assert_eq!(update_stats.values_diff, 0);

        // Verify the new value
        let mut results: Vec<TestUpsertValueAndSize> = vec![TestUpsertValueAndSize::default()];
        backend
            .multi_get(vec![key], results.iter_mut())
            .await
            .unwrap();

        let retrieved = results[0].value.as_ref().unwrap().clone().into_decoded();
        assert_eq!(retrieved.finalized, Some(value2));
    }

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)]
    async fn test_tombstone() {
        let replica_id = uuid::Uuid::new_v4();
        let source_id = GlobalId::User(5);
        let worker_id = 0;

        let mut backend =
            ObjectStorageUpsertBackend::new_in_memory(replica_id, source_id, worker_id).await;

        let key = make_key(1);

        // Put a tombstone (finalized = None)
        let puts: Vec<(UpsertKey, PutValue<TestStateValue>)> = vec![(
            key,
            PutValue {
                value: Some(TestStateValue::tombstone()),
                previous_value_metadata: None,
            },
        )];
        let put_stats = backend.multi_put(puts).await.unwrap();
        assert_eq!(put_stats.processed_puts, 1);
        // Tombstones don't count as values
        assert_eq!(put_stats.values_diff, 0);
        assert_eq!(put_stats.tombstones_diff, 1);

        // Get the tombstone back
        let mut results: Vec<TestUpsertValueAndSize> = vec![TestUpsertValueAndSize::default()];
        backend
            .multi_get(vec![key], results.iter_mut())
            .await
            .unwrap();

        assert!(results[0].value.is_some());
        assert!(results[0].metadata.as_ref().unwrap().is_tombstone);
        assert!(results[0].value.as_ref().unwrap().is_tombstone());
    }
}

// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Implementation of [Blob] backed by Google Cloud Storage.

use std::fmt::{Debug, Formatter};
use std::future::Future;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, anyhow};
use async_trait::async_trait;
use bytes::{Bytes, BytesMut};
use futures_util::future::{try_join_all, try_join3};
use google_cloud_gax::error::rpc::Code;
use google_cloud_gax::options::RequestOptionsBuilder;
use google_cloud_gax::paginator::ItemPaginator;
use google_cloud_storage::client::{Storage, StorageControl};
use google_cloud_storage::model_ext::ReadRange;
use google_cloud_storage::read_object::ReadObjectResponse;
use uuid::Uuid;

use mz_ore::bytes::SegmentedBytes;
use mz_ore::cast::CastFrom;
use mz_ore::metrics::MetricsRegistry;

use crate::cfg::BlobKnobs;
use crate::location::{Blob, BlobMetadata, Determinate, ExternalError};
use crate::metrics::S3BlobMetrics;

/// The size of each concurrent range read in [GcsBlob::get], matching the
/// part size S3 multipart uploads use.
const GET_CHUNK_SIZE: u64 = 8 * 1024 * 1024;

/// Configuration for opening a [GcsBlob]
#[derive(Clone, Debug)]
pub struct GcsBlobConfig {
    metrics: S3BlobMetrics,
    knobs: Arc<dyn BlobKnobs>,
    storage_client: Storage,
    storage_control_client: StorageControl,
    bucket: String,
    prefix: String,
}

impl GcsBlobConfig {
    const EXTERNAL_TESTS_GCS_BUCKET: &'static str = "MZ_PERSIST_EXTERNAL_STORAGE_TEST_GCS_BUCKET";

    /// Returns a new [GcsBlobConfig] for use in production.
    ///
    /// Stores objects in the given bucket prepended with the (possibly empty)
    /// prefix. GCS credentials must be available in the process or environment.
    pub async fn new(
        bucket: String,
        prefix: String,
        knobs: Box<dyn BlobKnobs>,
        metrics: S3BlobMetrics,
    ) -> Result<Self, ExternalError> {
        let storage_client = Storage::builder()
            .build()
            .await
            .map_err(|e| anyhow!("GCS Storage builder error: {}", e))?;
        let storage_control_client = StorageControl::builder()
            .build()
            .await
            .map_err(|e| anyhow!("GCS StorageControl builder error: {}", e))?;

        Ok(GcsBlobConfig {
            metrics,
            knobs: Arc::from(knobs),
            storage_client,
            storage_control_client,
            bucket,
            prefix,
        })
    }

    /// Returns a new [GcsBlobConfig] for use in tests.
    pub async fn new_for_test() -> Result<Option<Self>, ExternalError> {
        struct TestBlobKnobs;
        impl Debug for TestBlobKnobs {
            fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
                f.debug_struct("TestBlobKnobs").finish_non_exhaustive()
            }
        }
        impl BlobKnobs for TestBlobKnobs {
            fn operation_timeout(&self) -> Duration {
                Duration::from_secs(30)
            }

            fn operation_attempt_timeout(&self) -> Duration {
                Duration::from_secs(10)
            }

            fn connect_timeout(&self) -> Duration {
                Duration::from_secs(5)
            }

            fn read_timeout(&self) -> Duration {
                Duration::from_secs(5)
            }

            fn is_cc_active(&self) -> bool {
                false
            }
        }

        let bucket = match std::env::var(Self::EXTERNAL_TESTS_GCS_BUCKET) {
            Ok(bucket) => bucket,
            Err(_) => {
                if mz_ore::env::is_var_truthy("CI") {
                    panic!("CI is supposed to run this test but something has gone wrong!");
                }
                return Ok(None);
            }
        };
        let prefix = Uuid::new_v4().to_string();
        let metrics = S3BlobMetrics::new(&MetricsRegistry::new());
        Ok(Some(
            GcsBlobConfig::new(bucket, prefix, Box::new(TestBlobKnobs), metrics).await?,
        ))
    }
}

/// Implementation of [Blob] backed by GCS
#[derive(Debug)]
pub struct GcsBlob {
    metrics: S3BlobMetrics,
    knobs: Arc<dyn BlobKnobs>,
    storage_client: Storage,
    storage_control_client: StorageControl,
    bucket: String,
    prefix: String,
}

impl GcsBlob {
    /// Opens the given location for non-exclusive read-write access.
    pub async fn open(config: GcsBlobConfig) -> Result<Self, ExternalError> {
        let ret = GcsBlob {
            metrics: config.metrics,
            knobs: config.knobs,
            storage_client: config.storage_client,
            storage_control_client: config.storage_control_client,
            bucket: config.bucket,
            prefix: config.prefix,
        };
        // Connect before returning success. Listing fails if the bucket is
        // missing or we lack access to it, while reading a missing key would
        // succeed against a missing bucket too.
        ret.with_timeout("open", async {
            ret.metrics.list_objects.inc();
            ret.storage_control_client
                .list_objects()
                .set_parent(ret.formatted_bucket_path())
                .set_prefix(format!("{}/", ret.prefix))
                .set_page_size(1)
                .with_attempt_timeout(ret.knobs.operation_attempt_timeout())
                .send()
                .await
                .map_err(|e| ret.error("ListObjects", e))
        })
        .await?;
        Ok(ret)
    }

    fn get_path(&self, key: &str) -> String {
        format!("{}/{}", self.prefix, key)
    }

    /// Returns the bucket path formatted for gRPC API calls.
    /// The gRPC API requires bucket names in the format `projects/_/buckets/{bucket}`.
    fn formatted_bucket_path(&self) -> String {
        format!("projects/_/buckets/{}", self.bucket)
    }

    /// Fails `op` if it takes longer than the operation timeout, read anew on
    /// each call.
    ///
    /// NOTE: The SDK has no per-attempt timeout for `read_object` and
    /// `write_object` (google-cloud-storage 1.20.0 leaves it as a TODO in
    /// `perform_upload/unbuffered.rs`), nor a way to set connect or read
    /// timeouts, so this is the only bound on a stalled data transfer.
    async fn with_timeout<T>(
        &self,
        op: &str,
        fut: impl Future<Output = Result<T, ExternalError>>,
    ) -> Result<T, ExternalError> {
        let timeout = self.knobs.operation_timeout();
        match tokio::time::timeout(timeout, fut).await {
            Ok(res) => res,
            Err(_) => {
                self.metrics.operation_timeouts.inc();
                Err(ExternalError::from(anyhow!(
                    "GCS {op} timed out after {timeout:?}"
                )))
            }
        }
    }

    /// Converts an SDK error from the call `op` and counts it.
    fn error(&self, op: &str, e: google_cloud_storage::Error) -> ExternalError {
        let code = if let Some(status) = e.status() {
            status.code.name().to_string()
        } else if let Some(code) = e.http_status_code() {
            format!("HTTP{code}")
        } else if e.is_timeout() {
            "Timeout".to_string()
        } else if e.is_connect() {
            "Connect".to_string()
        } else if e.is_authentication() {
            "Authentication".to_string()
        } else if e.is_io() || e.is_transport() {
            "Transport".to_string()
        } else {
            "Unknown".to_string()
        };
        self.metrics
            .error_counts
            .with_label_values(&[op, &code])
            .inc();
        ExternalError::from(anyhow!("GCS {op} error: {e}"))
    }

    async fn get_inner(&self, key: &str) -> Result<Option<SegmentedBytes>, ExternalError> {
        let path = self.get_path(key);
        let bucket = self.formatted_bucket_path();

        // A single stream over one HTTP/1.1 connection is slow for large
        // objects, so read in concurrent chunks. The first read also reports
        // the object's size and generation. Later reads pin that generation,
        // so a concurrent overwrite cannot mix two versions.
        self.metrics.get_part.inc();
        let first = match self
            .storage_client
            .read_object(&bucket, &path)
            .set_read_range(ReadRange::head(GET_CHUNK_SIZE))
            .send()
            .await
        {
            Ok(stream) => stream,
            Err(e) if is_not_found(&e) => return Ok(None),
            // GCS rejects any range of an empty object.
            Err(e) if is_range_not_satisfiable(&e) => return Ok(Some(SegmentedBytes::default())),
            Err(e) => return Err(self.error("ReadObject", e)),
        };
        let size = u64::try_from(first.object().size).context("GCS sent negative size value")?;
        let generation = first.object().generation;
        // The SDK skips checksum validation for range reads, so check the
        // whole object's CRC32C ourselves. GCS sends it only when the range
        // covers the whole object, so larger objects fetch it from the
        // object's metadata, concurrently with the remaining chunks.
        let first_crc32c = first.object().checksums.as_ref().and_then(|c| c.crc32c);
        let expected_crc32c = async {
            let crc32c = match first_crc32c {
                Some(crc32c) => Some(crc32c),
                None => self
                    .storage_control_client
                    .get_object()
                    .set_bucket(&bucket)
                    .set_object(&path)
                    .set_generation(generation)
                    .with_attempt_timeout(self.knobs.operation_attempt_timeout())
                    .send()
                    .await
                    .map_err(|e| self.error("GetObject", e))?
                    .checksums
                    .and_then(|c| c.crc32c),
            };
            crc32c.ok_or_else(|| ExternalError::from(anyhow!("GCS object {path} has no crc32c")))
        };

        let rest = (GET_CHUNK_SIZE..size)
            .step_by(usize::cast_from(GET_CHUNK_SIZE))
            .map(|offset| {
                let count = GET_CHUNK_SIZE.min(size - offset);
                let read = self
                    .storage_client
                    .read_object(&bucket, &path)
                    .set_generation(generation)
                    .set_read_range(ReadRange::segment(offset, count));
                async move {
                    let stream = read.send().await.map_err(|e| self.error("ReadObject", e))?;
                    self.read_body(stream, count).await
                }
            });
        let (first, rest, expected_crc32c) = try_join3(
            self.read_body(first, GET_CHUNK_SIZE.min(size)),
            try_join_all(rest),
            expected_crc32c,
        )
        .await?;

        let mut segments = Vec::with_capacity(rest.len() + 1);
        let mut fetched = 0;
        let mut actual_crc32c = 0;
        for (segment, crc) in std::iter::once(first).chain(rest) {
            fetched += u64::cast_from(segment.len());
            actual_crc32c = crc32c::crc32c_combine(actual_crc32c, crc, segment.len());
            segments.push(segment);
        }
        if fetched != size {
            return Err(anyhow!("GCS ReadObject {path} returned {fetched} of {size} bytes").into());
        }
        if actual_crc32c != expected_crc32c {
            return Err(anyhow!(
                "GCS ReadObject {path} checksum mismatch: expected crc32c {expected_crc32c:#010x}, got {actual_crc32c:#010x}"
            )
            .into());
        }
        Ok(Some(SegmentedBytes::from(segments)))
    }

    /// Collects a read into one allocation, presized to `capacity` bytes, and
    /// returns it with its CRC32C.
    async fn read_body(
        &self,
        mut stream: ReadObjectResponse,
        capacity: u64,
    ) -> Result<(Bytes, u32), ExternalError> {
        let mut buf = BytesMut::with_capacity(usize::cast_from(capacity));
        let mut crc = 0;
        while let Some(data) = stream.next().await {
            let data = data.map_err(|e| self.error("ReadObject", e))?;
            // Hashing as data arrives overlaps it with the transfer.
            crc = crc32c::crc32c_append(crc, &data);
            buf.extend_from_slice(&data);
        }
        Ok((buf.freeze(), crc))
    }

    async fn list_keys_and_metadata_inner(
        &self,
        key_prefix: &str,
        f: &mut (dyn FnMut(BlobMetadata) + Send + Sync),
    ) -> Result<(), ExternalError> {
        let blob_key_prefix = self.get_path(key_prefix);
        let strippable_root_prefix = format!("{}/", self.prefix);

        let mut stream = self
            .storage_control_client
            .list_objects()
            .set_parent(&self.formatted_bucket_path())
            .set_prefix(blob_key_prefix)
            .with_attempt_timeout(self.knobs.operation_attempt_timeout())
            .by_item();
        self.metrics.list_objects.inc();
        while let Some(response) = stream.next().await {
            let obj = response.map_err(|e| self.error("ListObjects", e))?;
            if let Some(key) = obj.name.strip_prefix(&strippable_root_prefix) {
                let size_in_bytes =
                    u64::try_from(obj.size).context("GCS sent negative size value")?;
                f(BlobMetadata { key, size_in_bytes })
            } else {
                return Err(ExternalError::from(anyhow!(
                    "found key with invalid prefix: {}",
                    obj.name
                )));
            }
        }
        Ok(())
    }

    async fn set_inner(&self, key: &str, value: Bytes) -> Result<(), ExternalError> {
        let path = self.get_path(key);
        self.metrics.set_single.inc();
        self.storage_client
            .write_object(&self.formatted_bucket_path(), path, value)
            .send_unbuffered()
            .await
            .map_err(|e| self.error("WriteObject", e))?;
        Ok(())
    }

    async fn delete_inner(&self, key: &str) -> Result<Option<usize>, ExternalError> {
        let path = self.get_path(key);

        // GCS deletes don't report the object size, so fetch it first. Making
        // the delete conditional on the fetched generation keeps the reported
        // size accurate.
        self.metrics.delete_head.inc();
        let obj = match self
            .storage_control_client
            .get_object()
            .set_bucket(&self.formatted_bucket_path())
            .set_object(&path)
            .with_attempt_timeout(self.knobs.operation_attempt_timeout())
            .send()
            .await
        {
            Ok(obj) => obj,
            Err(e) if is_not_found(&e) => return Ok(None),
            Err(e) => return Err(self.error("GetObject", e)),
        };
        let size = usize::try_from(obj.size).context("GCS sent negative size value")?;
        self.metrics.delete_object.inc();
        match self
            .storage_control_client
            .delete_object()
            .set_bucket(&self.formatted_bucket_path())
            .set_object(path)
            // NOTE: `set_generation` would permanently delete that generation,
            // even in a versioned bucket, leaving nothing for `restore`.
            .set_if_generation_match(obj.generation)
            .with_attempt_timeout(self.knobs.operation_attempt_timeout())
            .send()
            .await
        {
            Ok(()) => Ok(Some(size)),
            // Raced with a concurrent delete.
            Err(e) if is_not_found(&e) => Ok(None),
            Err(e) => Err(self.error("DeleteObject", e)),
        }
    }

    async fn restore_inner(&self, key: &str) -> Result<(), ExternalError> {
        let path = self.get_path(key);

        match self
            .storage_control_client
            .get_object()
            .set_bucket(&self.formatted_bucket_path())
            .set_object(&path)
            .with_attempt_timeout(self.knobs.operation_attempt_timeout())
            .send()
            .await
        {
            Ok(_) => return Ok(()),
            Err(e) if is_not_found(&e) => {}
            Err(e) => return Err(self.error("GetObject", e)),
        }

        // Restoring relies on object versioning: a delete leaves the object's
        // data behind as a noncurrent generation, which we copy back to a live
        // object. Soft-deleted objects are not considered.
        let mut stream = self
            .storage_control_client
            .list_objects()
            .set_parent(&self.formatted_bucket_path())
            .set_prefix(&path)
            .set_versions(true)
            .with_attempt_timeout(self.knobs.operation_attempt_timeout())
            .by_item();
        let mut latest_generation = None;
        while let Some(obj) = stream.next().await {
            let obj = obj.map_err(|e| self.error("ListObjects", e))?;
            // The prefix also matches longer keys.
            if obj.name == path && latest_generation < Some(obj.generation) {
                latest_generation = Some(obj.generation);
            }
        }
        let Some(generation) = latest_generation else {
            return Err(Determinate::new(anyhow!(
                "unable to restore {key} in GCS: no valid version exists"
            ))
            .into());
        };

        // A rewrite may take several calls, each resuming from the previous
        // call's token.
        let mut rewrite_token = String::new();
        loop {
            let response = match self
                .storage_control_client
                .rewrite_object()
                .set_source_bucket(self.formatted_bucket_path())
                .set_source_object(&path)
                .set_source_generation(generation)
                .set_destination_bucket(self.formatted_bucket_path())
                .set_destination_name(&path)
                // Only restore if no live object exists.
                .set_if_generation_match(0)
                .set_rewrite_token(rewrite_token)
                .with_attempt_timeout(self.knobs.operation_attempt_timeout())
                .send()
                .await
            {
                Ok(response) => response,
                // A live object appeared concurrently.
                Err(e) if is_failed_precondition(&e) => return Ok(()),
                Err(e) => return Err(self.error("RewriteObject", e)),
            };
            if response.done {
                return Ok(());
            }
            rewrite_token = response.rewrite_token;
        }
    }
}

#[async_trait]
impl Blob for GcsBlob {
    async fn get(&self, key: &str) -> Result<Option<SegmentedBytes>, ExternalError> {
        self.with_timeout("get", self.get_inner(key)).await
    }

    async fn list_keys_and_metadata(
        &self,
        key_prefix: &str,
        f: &mut (dyn FnMut(BlobMetadata) + Send + Sync),
    ) -> Result<(), ExternalError> {
        self.with_timeout("list", self.list_keys_and_metadata_inner(key_prefix, f))
            .await
    }

    async fn set(&self, key: &str, value: Bytes) -> Result<(), ExternalError> {
        self.with_timeout("set", self.set_inner(key, value)).await
    }

    async fn delete(&self, key: &str) -> Result<Option<usize>, ExternalError> {
        self.with_timeout("delete", self.delete_inner(key)).await
    }

    async fn restore(&self, key: &str) -> Result<(), ExternalError> {
        self.with_timeout("restore", self.restore_inner(key)).await
    }
}

fn is_not_found(e: &google_cloud_storage::Error) -> bool {
    e.status().is_some_and(|s| s.code == Code::NotFound) || e.http_status_code() == Some(404)
}

fn is_range_not_satisfiable(e: &google_cloud_storage::Error) -> bool {
    e.http_status_code() == Some(416)
}

fn is_failed_precondition(e: &google_cloud_storage::Error) -> bool {
    e.status()
        .is_some_and(|s| s.code == Code::FailedPrecondition)
        || e.http_status_code() == Some(412)
}

#[cfg(test)]
mod tests {
    use tracing::info;

    use super::*;
    use crate::location::tests::blob_impl_test;

    #[cfg_attr(miri, ignore)] // error: unsupported operation: can't call foreign function `TLS_method` on OS `linux`
    #[mz_ore::test(tokio::test(flavor = "multi_thread"))]
    async fn gcs_blob() -> Result<(), ExternalError> {
        let config = match GcsBlobConfig::new_for_test().await? {
            Some(client) => client,
            None => {
                info!(
                    "{} env not set: skipping test that uses external service",
                    GcsBlobConfig::EXTERNAL_TESTS_GCS_BUCKET
                );
                return Ok(());
            }
        };
        blob_impl_test(move |_path| {
            let config = config.clone();
            async move { GcsBlob::open(config).await }
        })
        .await
    }

    /// `blob_impl_test` accepts a backend that cannot restore, but `GcsBlob`
    /// must restore from a versioned bucket, which the test bucket is.
    #[cfg_attr(miri, ignore)] // error: unsupported operation: can't call foreign function `TLS_method` on OS `linux`
    #[mz_ore::test(tokio::test(flavor = "multi_thread"))]
    async fn gcs_blob_restore() -> Result<(), ExternalError> {
        let Some(config) = GcsBlobConfig::new_for_test().await? else {
            info!(
                "{} env not set: skipping test that uses external service",
                GcsBlobConfig::EXTERNAL_TESTS_GCS_BUCKET
            );
            return Ok(());
        };
        let blob = GcsBlob::open(config).await?;
        blob.set("k", Bytes::from("data")).await?;
        assert_eq!(blob.delete("k").await?, Some("data".len()));
        assert_eq!(blob.get("k").await?, None);
        blob.restore("k").await?;
        assert_eq!(
            blob.get("k").await?.map(|b| b.into_contiguous()),
            Some(b"data".to_vec())
        );
        blob.delete("k").await?;
        Ok(())
    }

    #[cfg_attr(miri, ignore)] // error: unsupported operation: can't call foreign function `TLS_method` on OS `linux`
    #[mz_ore::test(tokio::test(flavor = "multi_thread"))]
    async fn gcs_blob_get_chunk_boundaries() -> Result<(), ExternalError> {
        let Some(config) = GcsBlobConfig::new_for_test().await? else {
            info!(
                "{} env not set: skipping test that uses external service",
                GcsBlobConfig::EXTERNAL_TESTS_GCS_BUCKET
            );
            return Ok(());
        };
        let blob = GcsBlob::open(config).await?;
        let chunk = usize::cast_from(GET_CHUNK_SIZE);
        for len in [0, 1, chunk - 1, chunk, chunk + 1, 3 * chunk - 5] {
            let value: Vec<u8> = (0..len).map(|i| u8::try_from(i % 251).unwrap()).collect();
            let key = format!("k{len}");
            blob.set(&key, Bytes::from(value.clone())).await?;
            let got = blob.get(&key).await?.map(|b| b.into_contiguous());
            assert!(got.as_ref() == Some(&value), "mismatch for len {len}");
            blob.delete(&key).await?;
        }
        Ok(())
    }

    /// Times 128 MiB gets and sets through a single-stream read, [GcsBlob],
    /// and, when `MZ_PERSIST_BENCH_GCS_HMAC_ID` and
    /// `MZ_PERSIST_BENCH_GCS_HMAC_SECRET` are set, [S3Blob] against GCS's S3
    /// API with the HTTP pool. Run in the bucket's region, with
    /// `--nocapture`, for meaningful numbers.
    ///
    /// [S3Blob]: crate::s3::S3Blob
    #[mz_ore::test(tokio::test(flavor = "multi_thread"))]
    #[ignore]
    async fn gcs_blob_throughput() -> Result<(), ExternalError> {
        use std::time::Instant;

        use crate::cfg::BlobKnobs;
        use crate::s3::{BlobHttpPool, S3Blob, S3BlobConfig};

        const LEN: usize = 128 * 1024 * 1024;
        const ITERS: usize = 10;

        #[derive(Debug)]
        struct BenchKnobs;
        impl BlobKnobs for BenchKnobs {
            fn operation_timeout(&self) -> Duration {
                Duration::from_secs(120)
            }
            fn operation_attempt_timeout(&self) -> Duration {
                Duration::from_secs(60)
            }
            fn connect_timeout(&self) -> Duration {
                Duration::from_secs(5)
            }
            fn read_timeout(&self) -> Duration {
                Duration::from_secs(10)
            }
            fn is_cc_active(&self) -> bool {
                false
            }
            fn http_pool(&self) -> BlobHttpPool {
                BlobHttpPool::Gcs
            }
        }

        async fn time<F: Future<Output = Result<(), ExternalError>>>(
            name: &str,
            mut op: impl FnMut() -> F,
        ) -> Result<(), ExternalError> {
            let mut elapsed = Vec::new();
            for _ in 0..ITERS {
                let start = Instant::now();
                op().await?;
                elapsed.push(start.elapsed());
            }
            elapsed.sort();
            let median = elapsed[ITERS / 2];
            let mib_s = 128.0 / median.as_secs_f64();
            println!(
                "{name:<24} median {median:>10.2?} ({mib_s:>5.0} MiB/s), min {:.2?}, max {:.2?}",
                elapsed[0],
                elapsed[ITERS - 1],
            );
            Ok(())
        }

        let config = GcsBlobConfig::new_for_test()
            .await?
            .expect("bucket env var must be set");
        let bucket = config.bucket.clone();
        let prefix = config.prefix.clone();
        let gcs = GcsBlob::open(config).await?;
        let value = Bytes::from(vec![7u8; LEN]);

        time("gcs set", || gcs.set("bench", value.clone())).await?;
        time("gcs get single stream", || async {
            let stream = gcs
                .storage_client
                .read_object(&gcs.formatted_bucket_path(), gcs.get_path("bench"))
                .send()
                .await
                .map_err(|e| gcs.error("ReadObject", e))?;
            let (bytes, _) = gcs.read_body(stream, u64::cast_from(LEN)).await?;
            assert_eq!(bytes.len(), LEN);
            Ok(())
        })
        .await?;
        time("gcs get", || async {
            let bytes = gcs.get("bench").await?.expect("bench blob exists");
            assert_eq!(bytes.len(), LEN);
            Ok(())
        })
        .await?;
        gcs.delete("bench").await?;

        let (Ok(id), Ok(secret)) = (
            std::env::var("MZ_PERSIST_BENCH_GCS_HMAC_ID"),
            std::env::var("MZ_PERSIST_BENCH_GCS_HMAC_SECRET"),
        ) else {
            return Ok(());
        };
        let config = S3BlobConfig::new(
            bucket,
            prefix,
            None,
            Some("https://storage.googleapis.com".into()),
            Some("auto".into()),
            Some((id, secret)),
            Box::new(BenchKnobs),
            S3BlobMetrics::new(&MetricsRegistry::new()),
        )
        .await
        .map_err(|e| anyhow!("{e}"))?;
        let s3 = S3Blob::open(config).await?;
        time("s3 set", || s3.set("bench", value.clone())).await?;
        time("s3 get", || async {
            let bytes = s3.get("bench").await?.expect("bench blob exists");
            assert_eq!(bytes.len(), LEN);
            Ok(())
        })
        .await?;
        s3.delete("bench").await?;
        Ok(())
    }

    #[cfg_attr(miri, ignore)] // error: unsupported operation: can't call foreign function `TLS_method` on OS `linux`
    #[mz_ore::test(tokio::test(flavor = "multi_thread"))]
    async fn gcs_blob_open_missing_bucket() -> Result<(), ExternalError> {
        let Some(mut config) = GcsBlobConfig::new_for_test().await? else {
            info!(
                "{} env not set: skipping test that uses external service",
                GcsBlobConfig::EXTERNAL_TESTS_GCS_BUCKET
            );
            return Ok(());
        };
        config.bucket = format!("mz-missing-{}", Uuid::new_v4());
        let err = GcsBlob::open(config)
            .await
            .expect_err("opening a missing bucket succeeded");
        assert!(err.to_string().contains("ListObjects"), "{err}");
        Ok(())
    }
}

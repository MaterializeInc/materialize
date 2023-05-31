// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! An S3 implementation of [Blob] storage.

use std::cmp;
use std::fmt::{Debug, Formatter};
use std::ops::Range;
use std::sync::atomic::{self, AtomicU64};
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::anyhow;
use async_trait::async_trait;
use aws_config::default_provider::{credentials, region};
use aws_config::meta::region::ProvideRegion;
use aws_config::sts::AssumeRoleProvider;
use aws_config::timeout::TimeoutConfig;
use aws_credential_types::provider::SharedCredentialsProvider;
use aws_credential_types::Credentials;
use aws_sdk_s3::config::{AsyncSleep, Sleep};
use aws_sdk_s3::error::SdkError;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart};
use aws_sdk_s3::Client as S3Client;
use aws_types::region::Region;
use bytes::Bytes;
use futures_util::future::join_all;
use futures_util::{FutureExt, TryFutureExt};
use mz_ore::bytes::SegmentedBytes;
use mz_ore::cast::CastFrom;
use mz_ore::metrics::MetricsRegistry;
use mz_ore::task::RuntimeExt;
use tokio::runtime::Handle as AsyncHandle;
use tracing::{debug, debug_span, trace, trace_span, Instrument};
use uuid::Uuid;

use crate::cfg::BlobKnobs;
use crate::error::Error;
use crate::location::{Atomicity, Blob, BlobMetadata, ExternalError};
use crate::metrics::S3BlobMetrics;

/// Configuration for opening an [S3Blob].
#[derive(Clone, Debug)]
pub struct S3BlobConfig {
    metrics: S3BlobMetrics,
    client: S3Client,
    bucket: String,
    prefix: String,
}

// There is no simple way to hook into the S3 client to capture when its various timeouts
// are hit. Instead, we pass along marker values that inform our [MetricsSleep] impl which
// type of timeout was requested so it can substitute in a dynamic value set by config
// from the caller.
const OPERATION_TIMEOUT_MARKER: Duration = Duration::new(111, 1111);
const OPERATION_ATTEMPT_TIMEOUT_MARKER: Duration = Duration::new(222, 2222);
const CONNECT_TIMEOUT_MARKER: Duration = Duration::new(333, 3333);
const READ_TIMEOUT_MARKER: Duration = Duration::new(444, 4444);

#[derive(Debug)]
struct MetricsSleep {
    knobs: Box<dyn BlobKnobs>,
    metrics: S3BlobMetrics,
}

impl AsyncSleep for MetricsSleep {
    fn sleep(&self, duration: Duration) -> Sleep {
        let (duration, metric) = match duration {
            OPERATION_TIMEOUT_MARKER => (
                self.knobs.operation_timeout(),
                Some(self.metrics.operation_timeouts.clone()),
            ),
            OPERATION_ATTEMPT_TIMEOUT_MARKER => (
                self.knobs.operation_attempt_timeout(),
                Some(self.metrics.operation_attempt_timeouts.clone()),
            ),
            CONNECT_TIMEOUT_MARKER => (
                self.knobs.connect_timeout(),
                Some(self.metrics.connect_timeouts.clone()),
            ),
            READ_TIMEOUT_MARKER => (
                self.knobs.read_timeout(),
                Some(self.metrics.read_timeouts.clone()),
            ),
            duration => (duration, None),
        };

        // the sleep future we return here will only be polled to
        // completion if its corresponding http request to S3 times
        // out, meaning we can chain incrementing the appropriate
        // timeout counter to when it finishes
        Sleep::new(tokio::time::sleep(duration).map(|x| {
            if let Some(counter) = metric {
                counter.inc();
            }
            x
        }))
    }
}

impl S3BlobConfig {
    const EXTERNAL_TESTS_S3_BUCKET: &'static str = "MZ_PERSIST_EXTERNAL_STORAGE_TEST_S3_BUCKET";

    /// Returns a new [S3BlobConfig] for use in production.
    ///
    /// Stores objects in the given bucket prepended with the (possibly empty)
    /// prefix. S3 credentials and region must be available in the process or
    /// environment.
    pub async fn new(
        bucket: String,
        prefix: String,
        role_arn: Option<String>,
        endpoint: Option<String>,
        region: Option<String>,
        credentials: Option<(String, String)>,
        knobs: Box<dyn BlobKnobs>,
        metrics: S3BlobMetrics,
    ) -> Result<Self, Error> {
        let region = match region {
            Some(region_name) => Some(Region::new(region_name)),
            None => region::default_provider().region().await,
        };

        let mut loader = aws_config::from_env().region(region.clone());

        if let Some(role_arn) = role_arn {
            let mut role_provider = AssumeRoleProvider::builder(role_arn).session_name("persist");
            if let Some(region) = region {
                role_provider = role_provider.region(region);
            }
            let default_provider =
                SharedCredentialsProvider::new(credentials::default_provider().await);
            loader = loader.credentials_provider(role_provider.build(default_provider));
        }

        if let Some((access_key_id, secret_access_key)) = credentials {
            loader = loader.credentials_provider(Credentials::from_keys(
                access_key_id,
                secret_access_key,
                None,
            ));
        }

        if let Some(endpoint) = endpoint {
            loader = loader.endpoint_url(endpoint)
        }

        // NB: we must always use the custom sleep impl if we use the timeout marker values
        loader = loader.sleep_impl(MetricsSleep {
            knobs,
            metrics: metrics.clone(),
        });
        loader = loader.timeout_config(
            TimeoutConfig::builder()
                // maximum time allowed for a top-level S3 API call (including internal retries)
                .operation_timeout(OPERATION_TIMEOUT_MARKER)
                // maximum time allowed for a single network call
                .operation_attempt_timeout(OPERATION_ATTEMPT_TIMEOUT_MARKER)
                // maximum time until a connection succeeds
                .connect_timeout(CONNECT_TIMEOUT_MARKER)
                // maximum time to read the first byte of a response
                .read_timeout(READ_TIMEOUT_MARKER)
                .build(),
        );

        let client = mz_aws_s3_util::new_client(&loader.load().await);
        Ok(S3BlobConfig {
            metrics,
            client,
            bucket,
            prefix,
        })
    }

    /// Returns a new [S3BlobConfig] for use in unit tests.
    ///
    /// By default, persist tests that use external storage (like s3) are
    /// no-ops, so that `cargo test` does the right thing without any
    /// configuration. To activate the tests, set the
    /// `MZ_PERSIST_EXTERNAL_STORAGE_TEST_S3_BUCKET` environment variable and
    /// ensure you have valid AWS credentials available in a location where the
    /// AWS Rust SDK can discovery them.
    ///
    /// This intentionally uses the `MZ_PERSIST_EXTERNAL_STORAGE_TEST_S3_BUCKET`
    /// env as the switch for test no-op-ness instead of the presence of a valid
    /// AWS authentication configuration envs because a developers might have
    /// valid credentials present and this isn't an explicit enough signal from
    /// a developer running `cargo test` that it's okay to use these
    /// credentials. It also intentionally does not use the local drop-in s3
    /// replacement to keep persist unit tests light.
    ///
    /// On CI, these tests are enabled by adding the scratch-aws-access plugin
    /// to the `cargo-test` step in `ci/test/pipeline.template.yml` and setting
    /// `MZ_PERSIST_EXTERNAL_STORAGE_TEST_S3_BUCKET` in
    /// `ci/test/cargo-test/mzcompose.py`.
    ///
    /// For a Materialize developer, to opt in to these tests locally for
    /// development, follow the AWS access guide:
    ///
    /// ```text
    /// https://github.com/MaterializeInc/i2/blob/main/doc/aws-access.md
    /// ```
    ///
    /// then running `source src/persist/s3_test_env_mz.sh`. You will also have
    /// to run `aws sso login` if you haven't recently.
    ///
    /// Non-Materialize developers will have to set up their own auto-deleting
    /// bucket and export the same env vars that s3_test_env_mz.sh does.
    ///
    /// Only public for use in src/benches.
    pub async fn new_for_test() -> Result<Option<Self>, Error> {
        let bucket = match std::env::var(Self::EXTERNAL_TESTS_S3_BUCKET) {
            Ok(bucket) => bucket,
            Err(_) => {
                if mz_ore::env::is_var_truthy("CI") {
                    panic!("CI is supposed to run this test but something has gone wrong!");
                }
                return Ok(None);
            }
        };

        struct TestBlobKnobs;
        impl std::fmt::Debug for TestBlobKnobs {
            fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
                f.debug_struct("TestBlobKnobs").finish_non_exhaustive()
            }
        }
        impl BlobKnobs for TestBlobKnobs {
            fn operation_timeout(&self) -> Duration {
                OPERATION_TIMEOUT_MARKER
            }

            fn operation_attempt_timeout(&self) -> Duration {
                OPERATION_ATTEMPT_TIMEOUT_MARKER
            }

            fn connect_timeout(&self) -> Duration {
                CONNECT_TIMEOUT_MARKER
            }

            fn read_timeout(&self) -> Duration {
                READ_TIMEOUT_MARKER
            }
        }

        // Give each test a unique prefix so they don't conflict. We don't have
        // to worry about deleting any data that we create because the bucket is
        // set to auto-delete after 1 day.
        let prefix = Uuid::new_v4().to_string();
        let role_arn = None;
        let metrics = S3BlobMetrics::new(&MetricsRegistry::new());
        let config = S3BlobConfig::new(
            bucket,
            prefix,
            role_arn,
            None,
            None,
            None,
            Box::new(TestBlobKnobs),
            metrics,
        )
        .await?;
        Ok(Some(config))
    }

    /// Returns a clone of Self with a new v4 uuid prefix.
    pub fn clone_with_new_uuid_prefix(&self) -> Self {
        let mut ret = self.clone();
        ret.prefix = Uuid::new_v4().to_string();
        ret
    }
}

/// Implementation of [Blob] backed by S3.
#[derive(Debug)]
pub struct S3Blob {
    metrics: S3BlobMetrics,
    client: S3Client,
    bucket: String,
    prefix: String,
    // Maximum number of keys we get information about per list-objects request.
    //
    // Defaults to 1000 which is the current AWS max.
    max_keys: i32,
    multipart_config: MultipartConfig,
}

impl S3Blob {
    /// Opens the given location for non-exclusive read-write access.
    pub async fn open(config: S3BlobConfig) -> Result<Self, ExternalError> {
        let ret = S3Blob {
            metrics: config.metrics,
            client: config.client,
            bucket: config.bucket,
            prefix: config.prefix,
            max_keys: 1_000,
            multipart_config: MultipartConfig::default(),
        };
        // Connect before returning success. We don't particularly care about
        // what's stored in this blob (nothing writes to it, so presumably it's
        // empty) just that we were able and allowed to fetch it.
        let _ = ret.get("HEALTH_CHECK").await?;
        Ok(ret)
    }

    fn get_path(&self, key: &str) -> String {
        format!("{}/{}", self.prefix, key)
    }
}

#[async_trait]
impl Blob for S3Blob {
    async fn get(&self, key: &str) -> Result<Option<SegmentedBytes>, ExternalError> {
        let start_overall = Instant::now();
        let path = self.get_path(key);

        // S3 advises that it's fastest to download large objects along the part
        // boundaries they were originally uploaded with [1].
        //
        // [1]: https://docs.aws.amazon.com/whitepapers/latest/s3-optimizing-performance-best-practices/use-byte-range-fetches.html
        //
        // One option is to run the same logic as multipart does and do the
        // requests using the resulting byte ranges, but if we ever changed the
        // multipart chunking logic, they wouldn't line up for old blobs written
        // by a previous version.
        //
        // Another option is to store the part boundaries in the metadata we
        // keep about the batch, but this would be large and wasteful.
        //
        // Luckily, s3 exposes a part_number param on GetObject requests that we
        // can use. If an object was created with multipart, it allows
        // requesting each part as they were originally uploaded by the part
        // number index. With this, we can simply send off requests for part
        // number 1..=num_parts and reassemble the results.
        //
        // We could roundtrip the number of parts through persist batch
        // metadata, but with some cleverness, we can avoid even this. Turns
        // out, if multipart upload wasn't used (it was just a normal PutObject
        // request), s3 will still happily return it for a request specifying a
        // part_number of 1. This lets us fire off a first request, which
        // contains the metadata we need to determine how many additional parts
        // we need, if any.
        //
        // So, the following call sends this first request. The SDK even returns
        // the headers before the full data body has completed. This gives us
        // the number of parts. We can then proceed to fetch the body of the
        // first request concurrently with the rest of the parts of the object.

        // For each header and body that we fetch, we track the fastest, and
        // any large deviations from it.
        let min_body_elapsed = Arc::new(MinElapsed::default());
        let min_header_elapsed = Arc::new(MinElapsed::default());
        self.metrics.get_part.inc();

        // Fetch our first header, this tells us how many more are left.
        let header_start = Instant::now();
        let object = self
            .client
            .get_object()
            .bucket(&self.bucket)
            .key(&path)
            .part_number(1)
            .send()
            .await;
        let elapsed = header_start.elapsed();
        min_header_elapsed.observe(elapsed, "s3 download first part header");

        let first_part = match object {
            Ok(object) => object,
            Err(SdkError::ServiceError(err)) if err.err().is_no_such_key() => return Ok(None),
            Err(err) => return Err(ExternalError::from(anyhow!("s3 get meta err: {}", err))),
        };

        // Get the remaining number of parts
        let num_parts = match first_part.parts_count() {
            // For a non-multipart upload, parts_count will be 0. The rest of  the code works
            // perfectly well if we just pretend this was a multipart upload of 1 part.
            0 => 1,
            // For any positive value greater than 0, just return it.
            parts @ 1.. => parts,
            // A negative value is invalid.
            bad => {
                assert!(bad < 0);
                return Err(anyhow!("unexpected number of s3 object parts: {}", bad).into());
            }
        };

        trace!(
            "s3 download first header took {:?} ({num_parts} parts)",
            start_overall.elapsed(),
        );

        // Get handle to our runtime so we can concurrently fetch the remaining parts.
        let async_runtime = AsyncHandle::try_current().map_err(anyhow::Error::msg)?;

        // Fetch the first part's body while we fetch the other parts.
        let min_body_elapsed_ = Arc::clone(&min_body_elapsed);
        let first_body_fut = async move {
            let body_start = Instant::now();
            let result = first_part
                .body
                .collect()
                .map_err(|err| Error::from(format!("s3 get first body err: {}", err)))
                .await;
            let elapsed = body_start.elapsed();
            min_body_elapsed_.observe(elapsed, "s3 download first part body");

            result
        };

        let mut body_handles = Vec::new();
        // TODO: Add the key and part number once this can be annotated with metadata.
        body_handles.push(async_runtime.spawn_named(|| "persist_s3blob_get_body", first_body_fut));

        if num_parts > 1 {
            // Fetch the headers of the rest of the parts. (Starting at part 2 because we already
            // did part 1.)
            for part_num in 2..=num_parts {
                let header_future = self
                    .client
                    .get_object()
                    .bucket(&self.bucket)
                    .key(&path)
                    .part_number(part_num)
                    .send();

                // Clone a handle to our MinElapsed trackers so we can give one to
                // each download task.
                let min_header_elapsed_ = Arc::clone(&min_header_elapsed);
                let min_body_elapsed_ = Arc::clone(&min_body_elapsed);

                let request_future = async move {
                    // Request our headers.
                    let header_start = Instant::now();
                    let object = header_future
                        .await
                        .map_err(|err| ExternalError::from(anyhow!("s3 get meta err: {}", err)))?;
                    let header_elapsed = header_start.elapsed();
                    min_header_elapsed_.observe(header_elapsed, "s3 download part header");

                    // Request the body.
                    let body_start = Instant::now();
                    let body = object
                        .body
                        .collect()
                        .await
                        .map_err(|err| Error::from(format!("s3 get body err: {}", err)))?;
                    let body_elapsed = body_start.elapsed();
                    min_body_elapsed_.observe(body_elapsed, "s3 download part body");

                    Ok(body)
                };

                // TODO: Add the key and part number once this can be annotated
                // with metadata.
                //
                // Spawn all of these futures on the runtime so they run concurrently.
                let request_handle = async_runtime.spawn_named(|| "persist_s3blob_get_header", {
                    self.metrics.get_part.inc();
                    request_future
                });

                body_handles.push(request_handle);
            }
        }

        // Await on all of our parts requests.
        let part_bodies = join_all(body_handles).await;
        let mut segments = vec![];
        for result in part_bodies {
            let part_body = result
                // Tokio failure, we failed to spawn a task.
                .map_err(|err| Error::from(format!("s3 spawn err: {}", err)))?
                // Download failure, we failed to fetch the body from S3.
                .map_err(|err| Error::from(format!("s3 get body err: {}", err)))?;

            // Collect all of our segments.
            segments.extend(part_body.into_segments());
        }

        debug!(
            "s3 GetObject took {:?} ({} parts)",
            start_overall.elapsed(),
            num_parts
        );
        Ok(Some(SegmentedBytes::from(segments)))
    }

    async fn list_keys_and_metadata(
        &self,
        key_prefix: &str,
        f: &mut (dyn FnMut(BlobMetadata) + Send + Sync),
    ) -> Result<(), ExternalError> {
        let mut continuation_token = None;
        // we only want to return keys that match the specified blob key prefix
        let blob_key_prefix = self.get_path(key_prefix);
        // but we want to exclude the shared root prefix from our returned keys,
        // so only the blob key itself is passed in to `f`
        let strippable_root_prefix = format!("{}/", self.prefix);

        loop {
            self.metrics.list_objects.inc();
            let resp = self
                .client
                .list_objects_v2()
                .bucket(&self.bucket)
                .prefix(&blob_key_prefix)
                .max_keys(self.max_keys)
                .set_continuation_token(continuation_token)
                .send()
                .await
                .map_err(|err| Error::from(err.to_string()))?;
            if let Some(contents) = resp.contents {
                for object in contents.iter() {
                    if let Some(key) = object.key.as_ref() {
                        if let Some(key) = key.strip_prefix(&strippable_root_prefix) {
                            f(BlobMetadata {
                                key,
                                size_in_bytes: object
                                    .size
                                    .try_into()
                                    .expect("file in S3 cannot have negative size"),
                            });
                        } else {
                            return Err(ExternalError::from(anyhow!(
                                "found key with invalid prefix: {}",
                                key
                            )));
                        }
                    }
                }
            }

            if resp.next_continuation_token.is_some() {
                continuation_token = resp.next_continuation_token;
            } else {
                break;
            }
        }

        Ok(())
    }

    async fn set(&self, key: &str, value: Bytes, _atomic: Atomicity) -> Result<(), ExternalError> {
        // NB: S3 is always atomic, so we're free to ignore the atomic param.
        let value_len = value.len();
        if self
            .multipart_config
            .should_multipart(value_len)
            .map_err(anyhow::Error::msg)?
        {
            self.set_multi_part(key, value)
                .instrument(debug_span!("s3set_multi", payload_len = value_len))
                .await
        } else {
            self.set_single_part(key, value).await
        }
    }

    async fn delete(&self, key: &str) -> Result<Option<usize>, ExternalError> {
        // There is a race condition here where, if two delete calls for the
        // same key occur simultaneously, both might think they did the actual
        // deletion. This return value is only used for metrics, so it's
        // unfortunate, but fine.
        let path = self.get_path(key);
        self.metrics.delete_head.inc();
        let head_res = self
            .client
            .head_object()
            .bucket(&self.bucket)
            .key(&path)
            .send()
            .await;
        let size_bytes = match head_res {
            Ok(x) => u64::try_from(x.content_length).expect("file in S3 cannot have negative size"),
            Err(SdkError::ServiceError(err)) if err.err().is_not_found() => return Ok(None),
            Err(err) => return Err(ExternalError::from(anyhow!("s3 delete head err: {}", err))),
        };
        self.metrics.delete_object.inc();
        let _ = self
            .client
            .delete_object()
            .bucket(&self.bucket)
            .key(&path)
            .send()
            .await
            .map_err(|err| Error::from(err.to_string()))?;
        Ok(Some(usize::cast_from(size_bytes)))
    }
}

impl S3Blob {
    async fn set_single_part(&self, key: &str, value: Bytes) -> Result<(), ExternalError> {
        let start_overall = Instant::now();
        let path = self.get_path(key);

        let value_len = value.len();
        let part_span = trace_span!("s3set_single", payload_len = value_len);
        self.metrics.set_single.inc();
        self.client
            .put_object()
            .bucket(&self.bucket)
            .key(path)
            .body(ByteStream::from(value))
            .send()
            .instrument(part_span)
            .await
            .map_err(|err| Error::from(err.to_string()))?;
        debug!(
            "s3 PutObject single done {}b / {:?}",
            value_len,
            start_overall.elapsed()
        );
        Ok(())
    }

    // TODO(benesch): remove this once this function no longer makes use of
    // potentially dangerous `as` conversions.
    #[allow(clippy::as_conversions)]
    async fn set_multi_part(&self, key: &str, value: Bytes) -> Result<(), ExternalError> {
        let start_overall = Instant::now();
        let path = self.get_path(key);

        // Start the multi part request and get an upload id.
        trace!("s3 PutObject multi start {}b", value.len());
        self.metrics.set_multi_create.inc();
        let upload_res = self
            .client
            .create_multipart_upload()
            .bucket(&self.bucket)
            .key(&path)
            .send()
            .instrument(debug_span!("s3set_multi_start"))
            .await
            .map_err(|err| Error::from(format!("create_multipart_upload err: {}", err)))?;
        let upload_id = upload_res.upload_id().ok_or_else(|| {
            Error::from("create_multipart_upload response missing upload_id".to_string())
        })?;
        trace!(
            "s3 create_multipart_upload took {:?}",
            start_overall.elapsed()
        );

        let async_runtime = AsyncHandle::try_current().map_err(anyhow::Error::new)?;

        // Fire off all the individual parts.
        //
        // TODO: The aws cli throttles how many of these are outstanding at any
        // given point. We'll likely want to do the same at some point.
        let start_parts = Instant::now();
        let mut part_futs = Vec::new();
        for (part_num, part_range) in self.multipart_config.part_iter(value.len()) {
            // NB: Without this spawn, these will execute serially. This is rust
            // async 101 stuff, but there isn't much async in the persist
            // codebase (yet?) so I thought it worth calling out.
            let part_span = debug_span!("s3set_multi_part", payload_len = part_range.len());
            let part_fut = async_runtime.spawn_named(
                // TODO: Add the key and part number once this can be annotated
                // with metadata.
                || "persist_s3blob_put_part",
                {
                    self.metrics.set_multi_part.inc();
                    self.client
                        .upload_part()
                        .bucket(&self.bucket)
                        .key(&path)
                        .upload_id(upload_id)
                        .part_number(part_num as i32)
                        .body(ByteStream::from(value.slice(part_range)))
                        .send()
                        .instrument(part_span)
                        .map(move |res| (start_parts.elapsed(), res))
                },
            );
            part_futs.push((part_num, part_fut));
        }
        let parts_len = part_futs.len();

        // Wait on all the parts to finish. This is done in part order, no need
        // for joining them in the order they finish.
        //
        // TODO: Consider using something like futures::future::join_all() for
        // this. That would cancel outstanding requests for us if any of them
        // fails. However, it might not play well with using retries for tail
        // latencies. Investigate.
        let min_part_elapsed = MinElapsed::default();
        let mut parts = Vec::with_capacity(parts_len);
        for (part_num, part_fut) in part_futs.into_iter() {
            let (this_part_elapsed, part_res) = part_fut
                .await
                .map_err(|err| Error::from(format!("s3 spawn err: {}", err)))?;
            let part_res =
                part_res.map_err(|err| Error::from(format!("s3 upload_part err: {}", err)))?;
            let part_e_tag = part_res
                .e_tag()
                .ok_or_else(|| Error::from("s3 upload part missing e_tag"))?;
            parts.push(
                CompletedPart::builder()
                    .e_tag(part_e_tag)
                    .part_number(part_num as i32)
                    .build(),
            );
            min_part_elapsed.observe(this_part_elapsed, "s3 upload_part took");
        }
        trace!(
            "s3 upload_parts overall took {:?} ({} parts)",
            start_parts.elapsed(),
            parts_len
        );

        // Complete the upload.
        //
        // Currently, we early return if any of the individual parts fail. This
        // permanently orphans any parts that succeeded. One fix is to call
        // abort_multipart_upload, which deletes them. However, there's also an
        // option for an s3 bucket to auto-delete parts that haven't been
        // completed or aborted after a given amount of time. This latter is
        // simpler and also resilient to ill-timed mz restarts, so we use it for
        // now. We could likely add the accounting necessary to make
        // abort_multipart_upload work, but it would be complex and affect perf.
        // Let's see how far we can get without it.
        let start_complete = Instant::now();
        self.metrics.set_multi_complete.inc();
        self.client
            .complete_multipart_upload()
            .bucket(&self.bucket)
            .key(&path)
            .upload_id(upload_id)
            .multipart_upload(
                CompletedMultipartUpload::builder()
                    .set_parts(Some(parts))
                    .build(),
            )
            .send()
            .instrument(debug_span!("s3set_multi_complete", num_parts = parts_len))
            .await
            .map_err(|err| Error::from(format!("complete_multipart_upload err: {}", err)))?;
        trace!(
            "s3 complete_multipart_upload took {:?}",
            start_complete.elapsed()
        );

        debug!(
            "s3 PutObject multi done {}b / {:?} ({} parts)",
            value.len(),
            start_overall.elapsed(),
            parts_len
        );
        Ok(())
    }
}

#[derive(Clone, Debug)]
struct MultipartConfig {
    multipart_threshold: usize,
    multipart_chunk_size: usize,
}

impl Default for MultipartConfig {
    fn default() -> Self {
        Self {
            multipart_threshold: Self::DEFAULT_MULTIPART_THRESHOLD,
            multipart_chunk_size: Self::DEFAULT_MULTIPART_CHUNK_SIZE,
        }
    }
}

const MB: usize = 1024 * 1024;
const TB: usize = 1024 * 1024 * MB;

impl MultipartConfig {
    /// The minimum object size for which we start using multipart upload.
    ///
    /// From the official `aws cli` tool implementation:
    ///
    /// <https://github.com/aws/aws-cli/blob/2.4.14/awscli/customizations/s3/transferconfig.py#L18-L29>
    const DEFAULT_MULTIPART_THRESHOLD: usize = 8 * MB;
    /// The size of each part (except the last) in a multipart upload.
    ///
    /// From the official `aws cli` tool implementation:
    ///
    /// <https://github.com/aws/aws-cli/blob/2.4.14/awscli/customizations/s3/transferconfig.py#L18-L29>
    const DEFAULT_MULTIPART_CHUNK_SIZE: usize = 8 * MB;

    /// The largest size object creatable in S3.
    ///
    /// From <https://docs.aws.amazon.com/AmazonS3/latest/userguide/qfacts.html>
    const MAX_SINGLE_UPLOAD_SIZE: usize = 5 * TB;
    /// The minimum size of a part in a multipart upload.
    ///
    /// This minimum doesn't apply to the last chunk, which can be any size.
    ///
    /// From <https://docs.aws.amazon.com/AmazonS3/latest/userguide/qfacts.html>
    const MIN_UPLOAD_CHUNK_SIZE: usize = 5 * MB;
    /// The smallest allowable part number (inclusive).
    ///
    /// From <https://docs.aws.amazon.com/AmazonS3/latest/userguide/qfacts.html>
    const MIN_PART_NUM: u32 = 1;
    /// The largest allowable part number (inclusive).
    ///
    /// From <https://docs.aws.amazon.com/AmazonS3/latest/userguide/qfacts.html>
    const MAX_PART_NUM: u32 = 10_000;

    fn should_multipart(&self, blob_len: usize) -> Result<bool, String> {
        if blob_len > Self::MAX_SINGLE_UPLOAD_SIZE {
            return Err(format!(
                "S3 does not support blobs larger than {} bytes got: {}",
                Self::MAX_SINGLE_UPLOAD_SIZE,
                blob_len
            ));
        }
        Ok(blob_len > self.multipart_threshold)
    }

    fn part_iter(&self, blob_len: usize) -> MultipartChunkIter {
        debug_assert!(self.multipart_chunk_size >= MultipartConfig::MIN_UPLOAD_CHUNK_SIZE);
        MultipartChunkIter::new(self.multipart_chunk_size, blob_len)
    }
}

#[derive(Clone, Debug)]
struct MultipartChunkIter {
    total_len: usize,
    part_size: usize,
    part_idx: u32,
}

impl MultipartChunkIter {
    fn new(default_part_size: usize, blob_len: usize) -> Self {
        let max_parts: usize = usize::cast_from(MultipartConfig::MAX_PART_NUM);

        // Compute the minimum part size we can use without going over the max
        // number of parts that S3 allows: `ceil(blob_len / max_parts)`.This
        // will end up getting thrown away by the `cmp::max` for anything
        // smaller than `max_parts * default_part_size = 80GiB`.
        let min_part_size = (blob_len + max_parts - 1) / max_parts;
        let part_size = cmp::max(min_part_size, default_part_size);

        // Part nums are 1-indexed in S3. Convert back to 0-indexed to make the
        // range math easier to follow.
        let part_idx = MultipartConfig::MIN_PART_NUM - 1;
        MultipartChunkIter {
            total_len: blob_len,
            part_size,
            part_idx,
        }
    }
}

impl Iterator for MultipartChunkIter {
    type Item = (u32, Range<usize>);

    fn next(&mut self) -> Option<Self::Item> {
        let part_idx = self.part_idx;
        self.part_idx += 1;

        let start = usize::cast_from(part_idx) * self.part_size;
        if start >= self.total_len {
            return None;
        }
        let end = cmp::min(start + self.part_size, self.total_len);
        let part_num = part_idx + 1;
        Some((part_num, start..end))
    }
}

/// A helper for tracking the minimum of a set of Durations.
#[derive(Debug)]
struct MinElapsed {
    min: AtomicU64,
    alert_factor: u64,
}

impl Default for MinElapsed {
    fn default() -> Self {
        MinElapsed {
            min: AtomicU64::new(u64::MAX),
            alert_factor: 8,
        }
    }
}

impl MinElapsed {
    fn observe(&self, x: Duration, msg: &'static str) {
        let nanos = x.as_nanos();
        let nanos = u64::try_from(nanos).unwrap_or(u64::MAX);

        // Possibly set a new minimum.
        let prev_min = self.min.fetch_min(nanos, atomic::Ordering::SeqCst);

        // Trace if our provided duration was much larger than our minimum.
        let new_min = std::cmp::min(prev_min, nanos);
        if nanos > new_min.saturating_mul(self.alert_factor) {
            let min_duration = Duration::from_nanos(new_min);
            let factor = self.alert_factor;
            debug!("{msg} took {x:?} more than {factor}x the min {min_duration:?}");
        } else {
            trace!("{msg} took {x:?}");
        }
    }
}

// Make sure the "vendored" feature of the openssl_sys crate makes it into the
// transitive dep graph of persist, so that we don't attempt to link against the
// system OpenSSL library. Fake a usage of the crate here so that a good
// samaritan doesn't remove our unused dep.
#[allow(dead_code)]
fn openssl_sys_hack() {
    openssl_sys::init();
}

#[cfg(test)]
mod tests {
    use tracing::info;

    use crate::location::tests::blob_impl_test;

    use super::*;

    #[mz_ore::test(tokio::test(flavor = "multi_thread"))]
    #[cfg_attr(coverage, ignore)] // https://github.com/MaterializeInc/materialize/issues/18898
    #[cfg_attr(miri, ignore)] // unsupported operation: can't call foreign function `epoll_wait` on OS `linux`
    async fn s3_blob() -> Result<(), ExternalError> {
        let config = match S3BlobConfig::new_for_test().await? {
            Some(client) => client,
            None => {
                info!(
                    "{} env not set: skipping test that uses external service",
                    S3BlobConfig::EXTERNAL_TESTS_S3_BUCKET
                );
                return Ok(());
            }
        };
        let config_multipart = config.clone_with_new_uuid_prefix();

        blob_impl_test(move |path| {
            let path = path.to_owned();
            let config = config.clone();
            async move {
                let config = S3BlobConfig {
                    metrics: config.metrics.clone(),
                    client: config.client.clone(),
                    bucket: config.bucket.clone(),
                    prefix: format!("{}/s3_blob_impl_test/{}", config.prefix, path),
                };
                let mut blob = S3Blob::open(config).await?;
                blob.max_keys = 2;
                Ok(blob)
            }
        })
        .await?;

        // Also specifically test multipart. S3 requires all parts but the last
        // to be at least 5MB, which we don't want to do from a test, so this
        // uses the multipart code path but only writes a single part.
        {
            let blob = S3Blob::open(config_multipart).await?;
            blob.set_multi_part("multipart", "foobar".into()).await?;
            assert_eq!(
                blob.get("multipart").await?,
                Some(b"foobar".to_vec().into())
            );
        }

        Ok(())
    }

    #[test]
    fn should_multipart() {
        let config = MultipartConfig::default();
        assert_eq!(config.should_multipart(0), Ok(false));
        assert_eq!(config.should_multipart(1), Ok(false));
        assert_eq!(
            config.should_multipart(MultipartConfig::DEFAULT_MULTIPART_THRESHOLD),
            Ok(false)
        );
        assert_eq!(
            config.should_multipart(MultipartConfig::DEFAULT_MULTIPART_THRESHOLD + 1),
            Ok(true)
        );
        assert_eq!(
            config.should_multipart(MultipartConfig::DEFAULT_MULTIPART_THRESHOLD * 2),
            Ok(true)
        );
        assert_eq!(
            config.should_multipart(MultipartConfig::MAX_SINGLE_UPLOAD_SIZE),
            Ok(true)
        );
        assert_eq!(
            config.should_multipart(MultipartConfig::MAX_SINGLE_UPLOAD_SIZE + 1),
            Err(
                "S3 does not support blobs larger than 5497558138880 bytes got: 5497558138881"
                    .into()
            )
        );
    }

    #[test]
    fn multipart_iter() {
        let iter = MultipartChunkIter::new(10, 0);
        assert_eq!(iter.collect::<Vec<_>>(), vec![]);

        let iter = MultipartChunkIter::new(10, 9);
        assert_eq!(iter.collect::<Vec<_>>(), vec![(1, 0..9)]);

        let iter = MultipartChunkIter::new(10, 10);
        assert_eq!(iter.collect::<Vec<_>>(), vec![(1, 0..10)]);

        let iter = MultipartChunkIter::new(10, 11);
        assert_eq!(iter.collect::<Vec<_>>(), vec![(1, 0..10), (2, 10..11)]);

        let iter = MultipartChunkIter::new(10, 19);
        assert_eq!(iter.collect::<Vec<_>>(), vec![(1, 0..10), (2, 10..19)]);

        let iter = MultipartChunkIter::new(10, 20);
        assert_eq!(iter.collect::<Vec<_>>(), vec![(1, 0..10), (2, 10..20)]);

        let iter = MultipartChunkIter::new(10, 21);
        assert_eq!(
            iter.collect::<Vec<_>>(),
            vec![(1, 0..10), (2, 10..20), (3, 20..21)]
        );
    }
}

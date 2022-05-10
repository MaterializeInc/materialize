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
use std::ops::Range;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use aws_config::default_provider::{credentials, region};
use aws_config::meta::region::ProvideRegion;
use aws_config::sts::AssumeRoleProvider;
use aws_sdk_s3::model::{CompletedMultipartUpload, CompletedPart};
use aws_sdk_s3::types::{ByteStream, SdkError};
use aws_sdk_s3::Client as S3Client;
use aws_types::credentials::SharedCredentialsProvider;
use bytes::{Buf, Bytes};
use futures_executor::block_on;
use futures_util::FutureExt;
use mz_ore::task::RuntimeExt;
use tokio::runtime::Handle as AsyncHandle;
use tracing::{debug, trace};
use uuid::Uuid;

use mz_ore::cast::CastFrom;

use crate::error::Error;
use crate::location::{Atomicity, Blob, BlobMulti, BlobRead, ExternalError, LockInfo};

/// Configuration for opening an [S3Blob] or [S3BlobRead].
#[derive(Clone, Debug)]
pub struct S3BlobConfig {
    client: S3Client,
    bucket: String,
    prefix: String,
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
    ) -> Result<Self, Error> {
        let mut loader = aws_config::from_env();
        if let Some(role_arn) = role_arn {
            let mut role_provider = AssumeRoleProvider::builder(role_arn).session_name("persist");
            if let Some(region) = region::default_provider().region().await {
                role_provider = role_provider.region(region);
            }
            let default_provider =
                SharedCredentialsProvider::new(credentials::default_provider().await);
            loader = loader.credentials_provider(role_provider.build(default_provider));
        }
        let client = mz_aws_util::s3::client(&loader.load().await);
        Ok(S3BlobConfig {
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
    /// `ci/test/cargo-test/mzcompose.yml`.
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

        // Give each test a unique prefix so they don't conflict. We don't have
        // to worry about deleting any data that we create because the bucket is
        // set to auto-delete after 1 day.
        let prefix = Uuid::new_v4().to_string();
        let role_arn = None;
        let config = S3BlobConfig::new(bucket, prefix, role_arn).await?;
        Ok(Some(config))
    }

    /// Returns a clone of Self with a new v4 uuid prefix.
    pub fn clone_with_new_uuid_prefix(&self) -> Self {
        let mut ret = self.clone();
        ret.prefix = Uuid::new_v4().to_string();
        ret
    }
}

#[derive(Debug)]
struct S3BlobCore {
    client: Option<S3Client>,
    bucket: String,
    prefix: String,
    // Maximum number of keys we get information about per list-objects request.
    //
    // Defaults to 1000 which is the current AWS max.
    max_keys: i32,
    multipart_config: MultipartConfig,
}

impl S3BlobCore {
    fn get_path(&self, key: &str) -> String {
        format!("{}/{}", self.prefix, key)
    }

    fn ensure_open(&self) -> Result<&S3Client, Error> {
        self.client
            .as_ref()
            .ok_or_else(|| Error::from("S3Blob unexpectedly closed"))
    }

    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, Error> {
        let start_overall = Instant::now();
        let client = self.ensure_open()?;
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
        let object = client
            .get_object()
            .bucket(&self.bucket)
            .key(&path)
            .part_number(1)
            .send()
            .await;
        let first_part = match object {
            Ok(object) => object,
            Err(SdkError::ServiceError { err, .. }) if err.is_no_such_key() => return Ok(None),
            Err(err) => return Err(Error::from(format!("s3 get meta err: {}", err))),
        };
        let num_parts = if first_part.parts_count() == 0 {
            // For a non-multipart upload, parts_count will be 0. The rest of
            // the code works perfectly well if we just pretend this was a
            // multipart upload of 1 part.
            1
        } else {
            first_part.parts_count()
        };
        trace!(
            "s3 download first header took {:?} ({} parts)",
            start_overall.elapsed(),
            num_parts
        );
        if num_parts < 1 {
            return Err(format!("unexpected number of s3 object parts: {}", num_parts).into());
        }

        // TODO: Plumb an Arc<AsyncRuntime> on S3BlobCore instead. I tried but
        // was getting a "Cannot drop a runtime in a context where blocking is
        // not allowed" error that I didn't want to get distracted with. All the
        // calls into the s3 library require that this context is set anyway, so
        // I suppose this is no worse than where we started.
        let async_runtime = AsyncHandle::try_current().map_err(|err| err.to_string())?;

        // Continue to fetch the first part's body while we fetch the other
        // parts.
        let start_part_body = Instant::now();
        let mut part_bodies = Vec::new();
        let first_body_len = usize::try_from(first_part.content_length)
            .map_err(|err| format!("unexpected s3 content_length: {}", err))?;
        let mut total_body_len = first_body_len;
        part_bodies.push(
            // TODO: Add the key and part number once this can be annotated with
            // metadata.
            async_runtime.spawn_named(
                || "persist_s3blob_get_body",
                first_part
                    .body
                    .collect()
                    .map(move |res| (start_part_body.elapsed(), first_body_len, res)),
            ),
        );

        if num_parts > 1 {
            // Fetch the headers of the rest of the parts. (Starting at part 2
            // because we already did part 1.)
            let start_headers = Instant::now();
            let mut part_futs = Vec::new();
            for part_num in 2..=num_parts {
                // TODO: Add the key and part number once this can be annotated
                // with metadata.
                let part_fut = async_runtime.spawn_named(
                    || "persist_s3blob_get_header",
                    client
                        .get_object()
                        .bucket(&self.bucket)
                        .key(&path)
                        .part_number(part_num)
                        .send()
                        .map(move |res| (start_headers.elapsed(), res)),
                );
                part_futs.push(part_fut);
            }

            // As each part header comes back, first off a task to fetch the
            // body.
            //
            // TODO: Doing this in part order isn't optimal. Perhaps better
            // would be do it as each header finishes. Once we do the below TODO
            // about not copying everything into one Vec, then we won't need to
            // compute the total_body_len and this can just chain the two
            // futures together.
            let mut min_header_elapsed = MinElapsed::default();
            for part_fut in part_futs {
                let (this_header_elapsed, part_res) = part_fut
                    .await
                    .map_err(|err| Error::from(format!("s3 spawn err: {}", err)))?;
                let part_res =
                    part_res.map_err(|err| Error::from(format!("s3 get meta err: {}", err)))?;
                let this_body_len = usize::try_from(part_res.content_length)
                    .map_err(|err| format!("unexpected s3 content_length: {}", err))?;
                total_body_len += this_body_len;

                let min_header_elapsed = min_header_elapsed.observe(this_header_elapsed);
                if this_header_elapsed >= min_header_elapsed * 8 {
                    debug!(
                        "s3 download part header took {:?} more than 8x the min {:?}",
                        this_header_elapsed, min_header_elapsed
                    );
                } else {
                    trace!("s3 download part header took {:?}", this_header_elapsed);
                }

                let start_part_body = Instant::now();
                part_bodies.push(
                    // TODO: Add the key and part number once this can be
                    // annotated with metadata.
                    async_runtime.spawn_named(
                        || "persist_s3blob_get_body",
                        part_res
                            .body
                            .collect()
                            .map(move |res| (start_part_body.elapsed(), this_body_len, res)),
                    ),
                );
            }
            trace!(
                "s3 download part headers took {:?} ({} parts)",
                start_headers.elapsed(),
                num_parts
            );
        }

        let start_bodies = Instant::now();
        let mut min_body_elapsed = MinElapsed::default();
        let mut val = vec![0u8; total_body_len];
        let mut val_offset = 0;
        let mut body_copy_elapsed = Duration::ZERO;
        for part_body in part_bodies {
            let (this_body_elapsed, this_body_len, part_body_res) = part_body
                .await
                .map_err(|err| Error::from(format!("s3 spawn err: {}", err)))?;
            let mut part_body_res =
                part_body_res.map_err(|err| Error::from(format!("s3 get body err: {}", err)))?;

            let min_body_elapsed = min_body_elapsed.observe(this_body_elapsed);
            if this_body_elapsed >= min_body_elapsed * 8 {
                debug!(
                    "s3 download part body took {:?} more than 8x the min {:?}",
                    this_body_elapsed, min_body_elapsed
                );
            } else {
                trace!("s3 download part body took {:?}", this_body_elapsed);
            }

            // TODO: It'd be lovely if we could push these copies into the
            // part_bodies futures so these last copies weren't all done
            // serially. Alternatively, we could change the return type of
            // `Blob::get` to something that can represent a chain of buffers.
            //
            // For example: A 6GiB blob takes ~6s to download and ~3s to collect
            // into this single Vec.
            let start_body_copy = Instant::now();
            if part_body_res.remaining() != this_body_len {
                return Err(format!(
                    "s3 expected body length {} got: {}",
                    this_body_len,
                    part_body_res.remaining()
                )
                .into());
            }
            let part_range = val_offset..val_offset + this_body_len;
            val_offset = part_range.end;
            part_body_res.copy_to_slice(&mut val[part_range]);
            body_copy_elapsed += start_body_copy.elapsed();
        }
        trace!(
            "s3 download part bodies took {:?} ({} parts)",
            start_bodies.elapsed(),
            num_parts
        );
        trace!(
            "s3 copy part bodies took {:?} ({} parts)",
            body_copy_elapsed,
            num_parts
        );

        debug!(
            "s3 GetObject took {:?} ({} parts)",
            start_overall.elapsed(),
            num_parts
        );
        Ok(Some(val))
    }

    async fn list_keys(&self) -> Result<Vec<String>, Error> {
        let mut ret = vec![];
        let client = self.ensure_open()?;
        let mut continuation_token = None;
        let prefix = self.get_path("");

        loop {
            let resp = client
                .list_objects_v2()
                .bucket(&self.bucket)
                .prefix(&self.prefix)
                .max_keys(self.max_keys)
                .set_continuation_token(continuation_token)
                .send()
                .await
                .map_err(|err| Error::from(err.to_string()))?;
            if let Some(contents) = resp.contents {
                for object in contents.iter() {
                    if let Some(key) = object.key.as_ref() {
                        if let Some(key) = key.strip_prefix(&prefix) {
                            ret.push(key.to_string());
                        } else {
                            return Err(Error::from(format!(
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

        Ok(ret)
    }

    fn close(&mut self) -> Option<S3Client> {
        self.client.take()
    }
}

/// Implementation of [BlobRead] backed by S3.
#[derive(Debug)]
pub struct S3BlobRead {
    core: S3BlobCore,
}

#[async_trait]
impl BlobRead for S3BlobRead {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, Error> {
        self.core.get(key).await
    }

    async fn list_keys(&self) -> Result<Vec<String>, Error> {
        self.core.list_keys().await
    }

    async fn close(&mut self) -> Result<bool, Error> {
        Ok(self.core.close().is_some())
    }
}

/// Implementation of [Blob] backed by S3.
//
// TODO: Productionize this:
// - Resolve what to do with LOCK, this impl is race-y.
#[derive(Debug)]
pub struct S3Blob {
    core: S3BlobCore,
}

impl S3Blob {
    const LOCKFILE_KEY: &'static str = "LOCK";

    async fn lock(&mut self, new_lock: LockInfo) -> Result<(), Error> {
        let lockfile_path = self.core.get_path(Self::LOCKFILE_KEY);
        // TODO: This is race-y. See the productionize comment on [S3Blob].
        if let Some(existing) = self.get(Self::LOCKFILE_KEY).await? {
            let _ = new_lock.check_reentrant_for(&lockfile_path, &mut existing.as_slice())?;
        }
        let contents = new_lock.to_string().into_bytes();
        self.set_single_part(Self::LOCKFILE_KEY, contents).await?;
        Ok(())
    }

    async fn set_single_part(&self, key: &str, value: Vec<u8>) -> Result<(), Error> {
        let start_overall = Instant::now();
        let client = self.core.ensure_open()?;
        let path = self.core.get_path(key);

        let value_len = value.len();
        client
            .put_object()
            .bucket(&self.core.bucket)
            .key(path)
            .body(ByteStream::from(value))
            .send()
            .await
            .map_err(|err| Error::from(err.to_string()))?;
        debug!(
            "s3 PutObject single done {}b / {:?}",
            value_len,
            start_overall.elapsed()
        );
        Ok(())
    }

    async fn set_multi_part(&self, key: &str, value: Vec<u8>) -> Result<(), Error> {
        let start_overall = Instant::now();
        let client = self.core.ensure_open()?;
        let path = self.core.get_path(key);

        // Start the multi part request and get an upload id.
        trace!("s3 PutObject multi start {}b", value.len());
        let upload_res = client
            .create_multipart_upload()
            .bucket(&self.core.bucket)
            .key(&path)
            .send()
            .await
            .map_err(|err| Error::from(format!("create_multipart_upload err: {}", err)))?;
        let upload_id = upload_res.upload_id().ok_or_else(|| {
            Error::from(format!(
                "create_multipart_upload response missing upload_id"
            ))
        })?;
        trace!(
            "s3 create_multipart_upload took {:?}",
            start_overall.elapsed()
        );

        // TODO: Plumb an Arc<AsyncRuntime> on S3BlobCore instead. I tried but
        // was getting a "Cannot drop a runtime in a context where blocking is
        // not allowed" error that I didn't want to get distracted with. All the
        // calls into the s3 library require that this context is set anyway, so
        // I suppose this is no worse than where we started.
        let async_runtime = AsyncHandle::try_current().map_err(|err| err.to_string())?;

        // Fire off all the individual parts.
        //
        // TODO: The aws cli throttles how many of these are outstanding at any
        // given point. We'll likely want to do the same at some point.
        let start_parts = Instant::now();
        let value = Bytes::from(value);
        let mut part_futs = Vec::new();
        for (part_num, part_range) in self.core.multipart_config.part_iter(value.len()) {
            // NB: Without this spawn, these will execute serially. This is rust
            // async 101 stuff, but there isn't much async in the persist
            // codebase (yet?) so I thought it worth calling out.
            let part_fut = async_runtime.spawn_named(
                // TODO: Add the key and part number once this can be annotated
                // with metadata.
                || "persist_s3blob_put_part",
                client
                    .upload_part()
                    .bucket(&self.core.bucket)
                    .key(&path)
                    .upload_id(upload_id)
                    .part_number(part_num as i32)
                    .body(ByteStream::from(value.slice(part_range)))
                    .send()
                    .map(move |res| (start_parts.elapsed(), res)),
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
        let mut min_part_elapsed = MinElapsed::default();
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

            let min_part_elapsed = min_part_elapsed.observe(this_part_elapsed);
            if this_part_elapsed >= min_part_elapsed * 8 {
                debug!(
                    "s3 upload_part took {:?} more than 8x the min {:?}",
                    this_part_elapsed, min_part_elapsed
                );
            } else {
                trace!("s3 upload_part took {:?}", this_part_elapsed);
            }
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
        client
            .complete_multipart_upload()
            .bucket(&self.core.bucket)
            .key(&path)
            .upload_id(upload_id)
            .multipart_upload(
                CompletedMultipartUpload::builder()
                    .set_parts(Some(parts))
                    .build(),
            )
            .send()
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

#[async_trait]
impl BlobRead for S3Blob {
    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, Error> {
        self.core.get(key).await
    }

    async fn list_keys(&self) -> Result<Vec<String>, Error> {
        self.core.list_keys().await
    }

    async fn close(&mut self) -> Result<bool, Error> {
        match self.core.close() {
            Some(client) => {
                let lockfile_path = self.core.get_path(Self::LOCKFILE_KEY);
                client
                    .delete_object()
                    .bucket(&self.core.bucket)
                    .key(lockfile_path)
                    .send()
                    .await
                    .map_err(|err| Error::from(err.to_string()))?;
                Ok(true)
            }
            None => Ok(false),
        }
    }
}

#[async_trait]
impl Blob for S3Blob {
    type Config = S3BlobConfig;
    type Read = S3BlobRead;

    /// Returns a new [S3Blob] which stores objects under the given bucket and
    /// prefix.
    ///
    /// All calls to methods on [S3Blob] must be from a thread with a tokio
    /// runtime guard.
    //
    // TODO: Figure out how to make this tokio runtime guard stuff more
    // explicit.
    fn open_exclusive(config: S3BlobConfig, lock_info: LockInfo) -> Result<Self, Error> {
        block_on(async {
            let core = S3BlobCore {
                client: Some(config.client),
                bucket: config.bucket,
                prefix: config.prefix,
                max_keys: 1_000,
                multipart_config: MultipartConfig::default(),
            };
            let mut blob = S3Blob { core };
            let _ = blob.lock(lock_info).await?;
            Ok(blob)
        })
    }

    fn open_read(config: S3BlobConfig) -> Result<S3BlobRead, Error> {
        block_on(async {
            let core = S3BlobCore {
                client: Some(config.client),
                bucket: config.bucket,
                prefix: config.prefix,
                max_keys: 1_000,
                multipart_config: MultipartConfig::default(),
            };
            Ok(S3BlobRead { core })
        })
    }

    async fn set(&mut self, key: &str, value: Vec<u8>, _atomic: Atomicity) -> Result<(), Error> {
        // NB: S3 is always atomic, so we're free to ignore the atomic param.
        if self.core.multipart_config.should_multipart(value.len())? {
            self.set_multi_part(key, value).await
        } else {
            self.set_single_part(key, value).await
        }
    }

    async fn delete(&mut self, key: &str) -> Result<(), Error> {
        let client = self.core.ensure_open()?;
        let path = self.core.get_path(key);
        client
            .delete_object()
            .bucket(&self.core.bucket)
            .key(path)
            .send()
            .await
            .map_err(|err| Error::from(err.to_string()))?;
        Ok(())
    }
}

/// Implementation of [BlobMulti] backed by S3.
#[derive(Debug)]
pub struct S3BlobMulti {
    core: S3BlobCore,
}

impl S3BlobMulti {
    /// Opens the given location for non-exclusive read-write access.
    pub async fn open(config: S3BlobConfig) -> Result<Self, ExternalError> {
        let core = S3BlobCore {
            client: Some(config.client),
            bucket: config.bucket,
            prefix: config.prefix,
            max_keys: 1_000,
            multipart_config: MultipartConfig::default(),
        };
        // Connect before returning success. We don't particularly care about
        // what's stored in this blob (nothing writes to it, so presumably it's
        // empty) just that we were able and allowed to fetch it.
        let _ = core.get("HEALTH_CHECK").await?;
        Ok(S3BlobMulti { core })
    }
}

#[async_trait]
impl BlobMulti for S3BlobMulti {
    async fn get(&self, _deadline: Instant, key: &str) -> Result<Option<Vec<u8>>, ExternalError> {
        let value = self.core.get(key).await?;
        Ok(value)
    }

    async fn list_keys(&self, _deadline: Instant) -> Result<Vec<String>, ExternalError> {
        let keys = self.core.list_keys().await?;
        Ok(keys)
    }

    async fn set(
        &self,
        _deadline: Instant,
        key: &str,
        value: Vec<u8>,
        atomic: Atomicity,
    ) -> Result<(), ExternalError> {
        // TODO: Move this impl here once we delete S3Blob.
        let mut hack = S3Blob {
            core: S3BlobCore {
                client: self.core.client.clone(),
                bucket: self.core.bucket.clone(),
                prefix: self.core.prefix.clone(),
                max_keys: self.core.max_keys,
                multipart_config: self.core.multipart_config.clone(),
            },
        };
        hack.set(key, value, atomic).await?;
        Ok(())
    }

    async fn delete(&self, _deadline: Instant, key: &str) -> Result<(), ExternalError> {
        // TODO: Move this impl here once we delete S3Blob.
        let mut hack = S3Blob {
            core: S3BlobCore {
                client: self.core.client.clone(),
                bucket: self.core.bucket.clone(),
                prefix: self.core.prefix.clone(),
                max_keys: self.core.max_keys,
                multipart_config: self.core.multipart_config.clone(),
            },
        };
        hack.delete(key).await?;
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
        return Ok(blob_len > self.multipart_threshold);
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
#[derive(Debug, Default)]
struct MinElapsed(Option<Duration>);

impl MinElapsed {
    /// Records a new Duration and returns the minimum seen so far.
    fn observe(&mut self, x: Duration) -> Duration {
        let min = self.0.get_or_insert(x);
        if x < *min {
            *min = x;
        }
        *min
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
    use crate::error::Error;
    use crate::location::tests::{blob_impl_test, blob_multi_impl_test};
    use tracing::info;

    use super::*;

    #[tokio::test(flavor = "multi_thread")]
    async fn s3_blob() -> Result<(), Error> {
        mz_ore::test::init_logging();
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
        let config_read = config.clone();
        let config_multipart = config.clone_with_new_uuid_prefix();

        blob_impl_test(
            move |t| {
                let lock_info = (t.reentrance_id, "s3_blob_test").into();
                let config = S3BlobConfig {
                    client: config.client.clone(),
                    bucket: config.bucket.clone(),
                    prefix: format!("{}/s3_blob_impl_test/{}", config.prefix, t.path),
                };
                let mut blob = S3Blob::open_exclusive(config, lock_info)?;
                blob.core.max_keys = 2;
                Ok(blob)
            },
            move |path| {
                let config = S3BlobConfig {
                    client: config_read.client.clone(),
                    bucket: config_read.bucket.clone(),
                    prefix: format!("{}/s3_blob_impl_test/{}", config_read.prefix, path),
                };
                let mut blob = S3Blob::open_read(config)?;
                blob.core.max_keys = 2;
                Ok(blob)
            },
        )
        .await?;

        // Also specifically test multipart. S3 requires all parts but the last
        // to be at least 5MB, which we don't want to do from a test, so this
        // uses the multipart code path but only writes a single part.
        {
            let blob = S3Blob::open_exclusive(
                config_multipart,
                LockInfo::new_no_reentrance("multipart".into()),
            )?;
            blob.set_multi_part("multipart", "foobar".into()).await?;
            assert_eq!(blob.get("multipart").await?, Some("foobar".into()));
        }

        Ok(())
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn s3_blob_multi() -> Result<(), ExternalError> {
        mz_ore::test::init_logging();
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

        blob_multi_impl_test(move |path| {
            let path = path.to_owned();
            let config = config.clone();
            async move {
                let config = S3BlobConfig {
                    client: config.client.clone(),
                    bucket: config.bucket.clone(),
                    prefix: format!("{}/s3_blob_multi_impl_test/{}", config.prefix, path),
                };
                let mut blob = S3BlobMulti::open(config).await?;
                blob.core.max_keys = 2;
                Ok(blob)
            }
        })
        .await?;

        // Also specifically test multipart. S3 requires all parts but the last
        // to be at least 5MB, which we don't want to do from a test, so this
        // uses the multipart code path but only writes a single part.
        {
            let blob = S3Blob::open_exclusive(
                config_multipart,
                LockInfo::new_no_reentrance("multipart".into()),
            )?;
            blob.set_multi_part("multipart", "foobar".into()).await?;
            assert_eq!(blob.get("multipart").await?, Some("foobar".into()));
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

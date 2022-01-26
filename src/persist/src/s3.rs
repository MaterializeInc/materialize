// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! An S3 implementation of [Blob] storage.

use async_trait::async_trait;
use aws_config::default_provider::{credentials, region};
use aws_config::meta::region::ProvideRegion;
use aws_config::sts::AssumeRoleProvider;
use aws_sdk_s3::ByteStream;
use aws_sdk_s3::Client as S3Client;
use aws_sdk_s3::SdkError;
use aws_types::credentials::SharedCredentialsProvider;
use futures_executor::block_on;

use mz_aws_util::config::AwsConfig;
use uuid::Uuid;

use crate::error::Error;
use crate::storage::{Atomicity, Blob, BlobRead, LockInfo};

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
        let config = AwsConfig::from_loader(loader).await;
        let client = mz_aws_util::s3::client(&config);
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
                if ore::env::is_var_truthy("CI") {
                    panic!("CI is supposed to run this test but something has gone wrong!");
                }
                return Ok(None);
            }
        };

        // Give each test a unique prefix so they don't confict. We don't have
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
        let client = self.ensure_open()?;
        let path = self.get_path(key);
        let object = client
            .get_object()
            .bucket(&self.bucket)
            .key(path)
            .send()
            .await;
        let object = match object {
            Ok(object) => object,
            Err(SdkError::ServiceError { err, .. }) if err.is_no_such_key() => return Ok(None),
            Err(err) => return Err(Error::from(err.to_string())),
        };

        let val = object
            .body
            .collect()
            .await
            .map_err(|e| format!("collecting byte buffer: {}", e))?
            // TODO: `into_bytes().to_vec()` results in one extra copy than
            // necessary. Changing `Blob::get` to return `Bytes` directly would
            // be more efficient, or adding a `into_vec()` method upstream.
            .into_bytes()
            .to_vec();
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
            } else {
                return Err(Error::from(format!(
                    "s3 response contents empty: {:?}",
                    resp
                )));
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
        self.set(Self::LOCKFILE_KEY, contents, Atomicity::RequireAtomic)
            .await?;
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
            };
            Ok(S3BlobRead { core })
        })
    }

    async fn set(&mut self, key: &str, value: Vec<u8>, _atomic: Atomicity) -> Result<(), Error> {
        // NB: S3 is always atomic, so we're free to ignore the atomic param.
        let client = self.core.ensure_open()?;
        let path = self.core.get_path(key);

        let body = ByteStream::from(value);
        client
            .put_object()
            .bucket(&self.core.bucket)
            .key(path)
            .body(body)
            .send()
            .await
            .map_err(|err| Error::from(err.to_string()))?;
        Ok(())
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

#[cfg(test)]
mod tests {
    use crate::error::Error;
    use crate::storage::tests::blob_impl_test;

    use super::*;

    #[tokio::test(flavor = "multi_thread")]
    async fn s3_blob() -> Result<(), Error> {
        ore::test::init_logging();
        let config = match S3BlobConfig::new_for_test().await? {
            Some(client) => client,
            None => {
                tracing::info!(
                    "{} env not set: skipping test that uses external service",
                    S3BlobConfig::EXTERNAL_TESTS_S3_BUCKET
                );
                return Ok(());
            }
        };
        let config_read = config.clone();

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
        Ok(())
    }
}

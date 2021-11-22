// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! An S3 implementation of [Blob] storage.

use std::fmt;

use async_trait::async_trait;
use aws_sdk_s3::ByteStream;
use aws_sdk_s3::Client as S3Client;
use aws_sdk_s3::SdkError;
use futures_executor::block_on;

use mz_aws_util::config::AwsConfig;

use crate::error::Error;
use crate::storage::{Atomicity, Blob, LockInfo};

/// Configuration for [S3Blob].
#[derive(Clone)]
pub struct Config {
    client: S3Client,
    bucket: String,
    prefix: String,
}

impl fmt::Debug for Config {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Config")
            .field("client", &"...")
            .field("bucket", &self.bucket)
            .field("prefix", &self.prefix)
            .finish()
    }
}

impl Config {
    #[cfg(test)]
    const EXTERNAL_TESTS_S3_BUCKET: &'static str = "MZ_PERSIST_EXTERNAL_STORAGE_TEST_S3_BUCKET";

    /// Returns a new [Config] for use in production.
    ///
    /// Stores objects in the given bucket prepended with the (possibly empty)
    /// prefix. S3 credentials and region must be available in the process or
    /// environment.
    pub async fn new(bucket: String, prefix: String) -> Result<Self, Error> {
        let config = AwsConfig::load_from_env().await;
        let client = mz_aws_util::s3::client(&config)
            .map_err(|err| format!("connecting client: {}", err))?;
        Ok(Config {
            client,
            bucket,
            prefix,
        })
    }

    /// Returns a new [Config] for use in unit tests.
    ///
    /// By default, persist tests that use external storage (like s3) are
    /// no-ops, so that `cargo test` does the right thing without any
    /// configuration. To activate teh tests, set the
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
    /// development, use the following values (potentially by putting them in a
    /// shell script and sourcing it if you'll do this often):
    ///
    /// ```shell
    ///  export MZ_PERSIST_EXTERNAL_STORAGE_TEST_S3_BUCKET="mtlz-test-persist-1d-lifecycle-delete"
    ///  export AWS_DEFAULT_REGION="us-east-2"
    ///  export AWS_ACCESS_KEY_ID="<scratch key>""
    ///  export AWS_SECRET_ACCESS_KEY="<scratch secret>"
    ///  export AWS_SESSION_TOKEN="<scratch token>"
    /// ```
    ///
    /// You can get these auth envs by going to Materialize's AWS SSO page,
    /// selecting the "Materialize Scratch" account, and then the "Command line
    /// or programmatic access" option. You might have to update these if you
    /// get auth failures.
    ///
    /// Non-Materialize developers will have to set up their own auto-deleting
    /// bucket.
    // TODO(benesch): when the AWS Rust SDK supports reading SSO credentials,
    // we should instruct Materialize developers to set
    // `AWS_PROFILE=mz-scratch-admin` rather than copy/pasting credentials from
    // the web interface.
    #[cfg(test)]
    pub async fn new_for_test() -> Result<Option<Self>, Error> {
        use uuid::Uuid;

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
        let config = Config::new(bucket, prefix).await?;
        Ok(Some(config))
    }
}

/// Implementation of [Blob] backed by S3.
//
// TODO: Productionize this:
// - Resolve what to do with LOCK, this impl is race-y.
pub struct S3Blob {
    client: Option<S3Client>,
    bucket: String,
    prefix: String,
    // Maximum number of keys we get information about per list-objects request.
    //
    // Defaults to 1000 which is the current AWS max.
    max_keys: i32,
}

impl fmt::Debug for S3Blob {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("S3Blob")
            .field("client", &"...")
            .field("bucket", &self.bucket)
            .field("prefix", &self.prefix)
            .finish()
    }
}

impl S3Blob {
    const LOCKFILE_KEY: &'static str = "LOCK";

    /// Returns a new [S3Blob] which stores objects under the given bucket and
    /// prefix.
    ///
    /// All calls to methods on [S3Blob] must be from a thread with a tokio
    /// runtime guard.
    //
    // TODO: Figure out how to make this tokio runtime guard stuff more
    // explicit.
    pub fn new(config: Config, lock_info: LockInfo) -> Result<Self, Error> {
        block_on(async {
            let mut blob = S3Blob {
                client: Some(config.client),
                bucket: config.bucket,
                prefix: config.prefix,
                max_keys: 1_000,
            };
            let _ = blob.lock(lock_info).await?;
            Ok(blob)
        })
    }

    fn get_path(&self, key: &str) -> String {
        format!("{}/{}", self.prefix, key)
    }

    #[cfg(test)]
    fn set_max_keys(&mut self, max_keys: i32) {
        self.max_keys = max_keys;
    }

    async fn lock(&mut self, new_lock: LockInfo) -> Result<(), Error> {
        let lockfile_path = self.get_path(Self::LOCKFILE_KEY);
        // TODO: This is race-y. See the productionize comment on [S3Blob].
        if let Some(existing) = self.get(Self::LOCKFILE_KEY).await? {
            let _ = new_lock.check_reentrant_for(&lockfile_path, &mut existing.as_slice())?;
        }
        let contents = new_lock.to_string().into_bytes();
        self.set(Self::LOCKFILE_KEY, contents, Atomicity::RequireAtomic)
            .await?;
        Ok(())
    }

    fn ensure_open(&self) -> Result<&S3Client, Error> {
        self.client
            .as_ref()
            .ok_or_else(|| Error::from("S3Blob unexpectedly closed"))
    }
}

#[async_trait]
impl Blob for S3Blob {
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

    async fn set(&mut self, key: &str, value: Vec<u8>, _atomic: Atomicity) -> Result<(), Error> {
        // NB: S3 is always atomic, so we're free to ignore the atomic param.
        let client = self.ensure_open()?;
        let path = self.get_path(key);

        let body = ByteStream::from(value);
        client
            .put_object()
            .bucket(&self.bucket)
            .key(path)
            .body(body)
            .send()
            .await
            .map_err(|err| Error::from(err.to_string()))?;
        Ok(())
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

    async fn delete(&mut self, key: &str) -> Result<(), Error> {
        let client = self.ensure_open()?;
        let path = self.get_path(key);
        client
            .delete_object()
            .bucket(&self.bucket)
            .key(path)
            .send()
            .await
            .map_err(|err| Error::from(err.to_string()))?;
        Ok(())
    }

    async fn close(&mut self) -> Result<bool, Error> {
        Ok(self.client.take().is_some())
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
        let config = match Config::new_for_test().await? {
            Some(client) => client,
            None => {
                log::info!(
                    "{} env not set: skipping test that uses external service",
                    Config::EXTERNAL_TESTS_S3_BUCKET
                );
                return Ok(());
            }
        };

        blob_impl_test(move |t| {
            let lock_info = (t.reentrance_id, "s3_blob_test").into();
            let config = Config {
                client: config.client.clone(),
                bucket: config.bucket.clone(),
                prefix: format!("{}/s3_blob_impl_test/{}", config.prefix, t.path),
            };
            let mut blob = S3Blob::new(config, lock_info)?;
            blob.set_max_keys(2);
            Ok(blob)
        })
        .await?;
        Ok(())
    }
}

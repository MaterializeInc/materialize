// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! An S3 implementation of [Blob] storage.

use aws_util::aws::ConnectInfo;
use rusoto_core::{ByteStream, Region, RusotoError};
use rusoto_s3::{
    DeleteObjectRequest, GetObjectError, GetObjectRequest, ListObjectsV2Request, PutObjectRequest,
    S3Client, S3,
};
use tokio::io::AsyncReadExt;

use crate::error::Error;
use crate::storage::{Blob, LockInfo};

/// Configuration for [S3Blob].
#[derive(Clone)]
pub struct Config {
    client: S3Client,
    bucket: String,
    prefix: String,
}

impl Config {
    #[cfg(test)]
    const EXTERNAL_TESTS_S3_BUCKET: &'static str = "MZ_PERSIST_EXTERNAL_STORAGE_TEST_S3_BUCKET";

    /// Returns a new [Config] for use in production.
    ///
    /// Stores objects in the given bucket prepended with the (possibly empty)
    /// prefix. S3 credentials and region must be available in the process or
    /// environment.
    pub fn new(bucket: String, prefix: String) -> Result<Self, Error> {
        let region = Region::default();
        let connect_info = ConnectInfo::new(region, None, None, None)
            .map_err(|err| format!("invalid s3 connection info: {}", err))?;
        let client = aws_util::client::s3(connect_info)
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
    /// configuration. On CI, an environment variable is set which opts in to
    /// the external storage tests (and specifies which bucket to use). If set,
    /// the following authentication environment variables are also requires to
    /// be set and the test will fail if they are not:
    ///
    /// - AWS_DEFAULT_REGION
    /// - AWS_ACCESS_KEY_ID
    /// - AWS_SECRET_ACCESS_KEY
    /// - AWS_SESSION_TOKEN
    ///
    /// This intentionally uses the `MZ_PERSIST_EXTERNAL_STORAGE_TEST_S3_BUCKET`
    /// env as the switch for test no-op-ness instead of the standard aws auth
    /// envs because a developers might have these set and this isn't an
    /// explicit enough signal from a developer running `cargo test` that it's
    /// okay to use these credentials. It also intentionally does not use the
    /// local drop-in s3 replacement to keep persist unit tests light.
    ///
    /// These are set in CI by adding the scratch-aws-access plugin to the
    /// `cargo-test` step in `ci/test/pipeline.template.yml` and setting
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
    #[cfg(test)]
    pub fn new_for_test() -> Result<Option<Self>, Error> {
        use uuid::Uuid;

        let bucket = match std::env::var(Self::EXTERNAL_TESTS_S3_BUCKET) {
            Ok(bucket) => bucket,
            Err(_) => {
                //
                if ore::env::is_var_truthy("BUILDKITE") {
                    panic!("CI is supposed to run this test but something has gone wrong!");
                }
                return Ok(None);
            }
        };
        let default_region = std::env::var("AWS_DEFAULT_REGION")
            .map_err(|err| format!("unavailable AWS_DEFAULT_REGION: {}", err))?;
        let access_key_id = std::env::var("AWS_ACCESS_KEY_ID")
            .map_err(|err| format!("unavailable AWS_ACCESS_KEY_ID: {}", err))?;
        let secret_access_key = std::env::var("AWS_SECRET_ACCESS_KEY")
            .map_err(|err| format!("unavailable AWS_SECRET_ACCESS_KEY: {}", err))?;
        let session_token = std::env::var("AWS_SESSION_TOKEN")
            .map_err(|err| format!("unavailable AWS_SESSION_TOKEN: {}", err))?;
        let region = default_region
            .parse()
            .map_err(|err| format!("invalid AWS_DEFAULT_REGION {}: {}", default_region, err))?;
        let connect_info = ConnectInfo::new(
            region,
            Some(access_key_id),
            Some(secret_access_key),
            Some(session_token),
        )
        .map_err(|err| format!("invalid s3 connection info: {}", err))?;
        let client = aws_util::client::s3(connect_info)
            .map_err(|err| format!("connecting client: {}", err))?;
        // Give each test a unique prefix so they don't confict. We don't have
        // to worry about deleting any data that we create because the bucket is
        // set to auto-delete after 1 day.
        let prefix = Uuid::new_v4().to_string();
        let config = Config {
            client,
            bucket,
            prefix,
        };
        Ok(Some(config))
    }
}

/// Implementation of [Blob] backed by S3.
//
// TODO: Productionize this:
// - Resolve what to do with allow_overwrite, there is no obvious way to support
//   this in s3. (The best I can imagine is the "Legal Hold" feature and
//   enforcing that the bucket has versioning turned off.)
// - Resolve what to do with LOCK, this impl is race-y.
// - Everything on the s3 client is async, but the persist runtime is not. Make
//   the Log and Blob traits async and figure out how to deal with the fallout.
pub struct S3Blob {
    blob_async: S3BlobAsync,
}

impl S3Blob {
    /// Returns a new [S3Blob] which stores objects under the given bucket and
    /// prefix.
    ///
    /// All calls to methods on [S3Blob] must be from a thread with a tokio
    /// runtime guard.
    //
    // TODO: Figure out how to make this tokio runtime guard stuff more
    // explicit.
    pub fn new(config: Config, lock_info: LockInfo) -> Result<Self, Error> {
        let blob_async = futures_executor::block_on(S3BlobAsync::new(config, lock_info))?;
        let blob = S3Blob { blob_async };
        Ok(blob)
    }
}

impl Blob for S3Blob {
    fn get(&self, key: &str) -> Result<Option<Vec<u8>>, Error> {
        // TODO: Make Blob async. See the productionize comment on [S3Blob].
        futures_executor::block_on(self.blob_async.get(key))
    }

    fn set(&mut self, key: &str, value: Vec<u8>, allow_overwrite: bool) -> Result<(), Error> {
        // TODO: Make Blob async. See the productionize comment on [S3Blob].
        futures_executor::block_on(self.blob_async.set(key, value, allow_overwrite))
    }

    fn delete(&mut self, key: &str) -> Result<(), Error> {
        // TODO: Make Blob async. See the productionize comment on [S3Blob].
        futures_executor::block_on(self.blob_async.delete(key))
    }

    fn list_keys(&self) -> Result<Vec<String>, Error> {
        // TODO: Make Blob async. See the productionize comment on [S3Blob].
        futures_executor::block_on(self.blob_async.list_keys())
    }

    fn close(&mut self) -> Result<bool, Error> {
        // TODO: Make Blob async. See the productionize comment on [S3Blob].
        futures_executor::block_on(self.blob_async.close())
    }
}

struct S3BlobAsync {
    client: Option<S3Client>,
    bucket: String,
    prefix: String,
}

impl S3BlobAsync {
    const LOCKFILE_KEY: &'static str = "LOCK";

    async fn new(config: Config, lock_info: LockInfo) -> Result<Self, Error> {
        let blob = S3BlobAsync {
            client: Some(config.client),
            bucket: config.bucket,
            prefix: config.prefix,
        };
        let _ = blob.lock(lock_info).await?;
        Ok(blob)
    }

    fn get_path(&self, key: &str) -> String {
        format!("{}/{}", self.prefix, key)
    }

    async fn lock(&self, new_lock: LockInfo) -> Result<(), Error> {
        let lockfile_path = self.get_path(Self::LOCKFILE_KEY);
        // TODO: This is race-y. See the productionize comment on [S3Blob].
        if let Some(existing) = self.get(Self::LOCKFILE_KEY).await? {
            let _ = new_lock.check_reentrant_for(&lockfile_path, &mut existing.as_slice())?;
        }
        let contents = new_lock.to_string().into_bytes();
        self.set(Self::LOCKFILE_KEY, contents, true).await?;
        Ok(())
    }

    async fn get(&self, key: &str) -> Result<Option<Vec<u8>>, Error> {
        let client = self.ensure_open()?;
        let path = self.get_path(key);
        let object = client
            .get_object(GetObjectRequest {
                bucket: self.bucket.clone(),
                key: path,
                ..Default::default()
            })
            .await;
        let object = match object {
            Ok(object) => object,
            Err(RusotoError::Service(GetObjectError::NoSuchKey(_))) => return Ok(None),
            Err(err) => return Err(Error::from(err.to_string())),
        };

        let mut val = Vec::new();
        object
            .body
            .ok_or_else(|| format!("missing body for key: {}", key))?
            .into_async_read()
            .read_to_end(&mut val)
            .await?;
        Ok(Some(val))
    }

    async fn set(&self, key: &str, value: Vec<u8>, allow_overwrite: bool) -> Result<(), Error> {
        let client = self.ensure_open()?;
        let path = self.get_path(key);

        if !allow_overwrite {
            // TODO: This is inefficient, but it's unclear if there's a good way
            // to implement this with s3. See the productionize comment on
            // [S3Blob].
            //
            // NB: We don't have to worry about races because the locking
            // prevents multiple instantiations of S3Blob pointed at the same
            // place and (for now) usage of the Blob implementers is serialized.
            if let Some(_) = self.get(key).await? {
                return Err(Error::from(format!(
                    "cannot set existing key with allow_overwrite=false: {}",
                    key
                )));
            }
        }

        let body = ByteStream::from(value);
        client
            .put_object(PutObjectRequest {
                bucket: self.bucket.clone(),
                key: path,
                body: Some(body),
                ..Default::default()
            })
            .await
            .map_err(|err| Error::from(err.to_string()))?;
        Ok(())
    }

    async fn list_keys(&self) -> Result<Vec<String>, Error> {
        let mut ret = vec![];
        let client = self.ensure_open()?;
        let mut list_objects_req = ListObjectsV2Request {
            bucket: self.bucket.clone(),
            prefix: Some(self.prefix.clone()),
            max_keys: Some(1_000),
            ..Default::default()
        };
        let prefix = self.get_path("");

        loop {
            let resp = client
                .list_objects_v2(list_objects_req.clone())
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
                list_objects_req.continuation_token = resp.next_continuation_token;
            } else {
                break;
            }
        }

        Ok(ret)
    }

    async fn delete(&self, key: &str) -> Result<(), Error> {
        let client = self.ensure_open()?;
        let path = self.get_path(key);
        client
            .delete_object(DeleteObjectRequest {
                bucket: self.bucket.clone(),
                key: path,
                ..Default::default()
            })
            .await
            .map_err(|err| Error::from(err.to_string()))?;
        Ok(())
    }

    async fn close(&mut self) -> Result<bool, Error> {
        Ok(self.client.take().is_some())
    }

    fn ensure_open(&self) -> Result<&S3Client, Error> {
        self.client
            .as_ref()
            .ok_or_else(|| Error::from("S3Blob unexpectedly closed"))
    }
}

#[cfg(test)]
mod tests {
    use tokio::runtime::Runtime;

    use crate::error::Error;
    use crate::storage::tests::blob_impl_test;

    use super::*;

    #[test]
    fn s3_blob() -> Result<(), Error> {
        ore::test::init_logging();
        let config = match Config::new_for_test()? {
            Some(client) => client,
            None => {
                log::info!(
                    "{} env not set: skipping test that uses external service",
                    Config::EXTERNAL_TESTS_S3_BUCKET
                );
                return Ok(());
            }
        };

        let rt = Runtime::new().unwrap();
        let guard = rt.enter();
        blob_impl_test(move |t| {
            let lock_info = (t.reentrance_id, "s3_blob_test").into();
            let config = Config {
                client: config.client.clone(),
                bucket: config.bucket.clone(),
                prefix: format!("{}/s3_blob_impl_test/{}", config.prefix, t.path),
            };
            S3Blob::new(config, lock_info)
        })?;
        drop(guard);
        Ok(())
    }
}

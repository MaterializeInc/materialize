// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use anyhow::anyhow;
use aws_sdk_s3::Client;
use aws_sdk_s3::error::SdkError;
use aws_sdk_s3::operation::complete_multipart_upload::CompleteMultipartUploadError;
use aws_sdk_s3::operation::create_multipart_upload::CreateMultipartUploadError;
use aws_sdk_s3::operation::upload_part::UploadPartError;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{CompletedMultipartUpload, CompletedPart};
use aws_types::sdk_config::SdkConfig;
use bytes::{Bytes, BytesMut};
use bytesize::ByteSize;
use mz_ore::cast::CastFrom;
use mz_ore::error::ErrorExt;
use mz_ore::task::{JoinHandle, JoinHandleExt, spawn};

/// A multi part uploader which can upload a single object across multiple parts
/// and keeps track of state to eventually finish the upload process.
/// The caller does not need to know the final number of parts beforehand.
///
/// The caller should get an instance by calling `S3MultiPartUploader::try_new` first.
/// Each part can be added by calling `add_chunk`, and can be called one or more times
/// and eventually finish the multi part upload by calling `finish` method.
#[derive(Debug)]
pub struct S3MultiPartUploader {
    client: Client,
    // Config settings for this particular multi part upload.
    config: S3MultiPartUploaderConfig,
    // The s3 bucket.
    bucket: String,
    // The s3 key of the file being uploaded.
    key: String,
    // The upload ID for the ongoing multi part upload.
    upload_id: String,
    // The current part count.
    part_count: i32,
    // Number of bytes uploaded till now.
    total_bytes_uploaded: u64,
    // A buffer to accumulate data till it reaches `part_size_limit` in size, when it
    // will be uploaded as a part for the multi-part upload.
    buffer: BytesMut,
    // The task handles for each part upload.
    upload_handles: Vec<JoinHandle<Result<(Option<String>, i32), S3MultiPartUploadError>>>,
}

/// The largest allowable part number (inclusive).
///
/// From <https://docs.aws.amazon.com/AmazonS3/latest/userguide/qfacts.html>
pub const AWS_S3_MAX_PART_COUNT: i32 = 10_000;
/// The minimum size of a part in a multipart upload.
///
/// This minimum doesn't apply to the last chunk, which can be any size.
///
/// From <https://docs.aws.amazon.com/AmazonS3/latest/userguide/qfacts.html>
const AWS_S3_MIN_PART_SIZE: ByteSize = ByteSize::mib(5);
/// The maximum size of a part in a multipart upload.
///
/// From <https://docs.aws.amazon.com/AmazonS3/latest/userguide/qfacts.html>
const AWS_S3_MAX_PART_SIZE: ByteSize = ByteSize::gib(5);
/// The maximum size of an object in s3.
///
/// From <https://docs.aws.amazon.com/AmazonS3/latest/userguide/qfacts.html>
const AWS_S3_MAX_OBJECT_SIZE: ByteSize = ByteSize::tib(5);

/// Information about a completed multi part upload after `finish` is called.
#[derive(Debug)]
pub struct CompletedUpload {
    /// The total number of parts in the multi part upload.
    pub part_count: u32,
    /// The total number of bytes uploaded in the multi part upload.
    pub total_bytes_uploaded: u64,
    pub bucket: String,
    pub key: String,
}

/// Configuration object to configure the behaviour of the `S3MultiPartUploader`.
#[derive(Debug)]
pub struct S3MultiPartUploaderConfig {
    /// Size of data buffered in memory before being uploaded as a part.
    pub part_size_limit: u64,
    /// The max file size of the file uploaded to s3 by an `S3MultiPartUploader` instance.
    pub file_size_limit: u64,
}

impl S3MultiPartUploaderConfig {
    /// Choosing a reasonable default for the maximum file size which
    /// this uploader can upload. This can be overridden in the
    /// `S3MultiPartUploaderConfig` config.
    const DEFAULT_MAX_FILE_SIZE: ByteSize = ByteSize::gib(5);
    /// Choosing a reasonable default for a part size. This can be overridden in the
    /// `S3MultiPartUploaderConfig` config.
    const DEFAULT_PART_SIZE_LIMIT: ByteSize = ByteSize::mib(10);

    /// As per S3 limits, the part size cannot be less than 5MiB and cannot exceed 5GiB.
    /// As per S3 limits, the object size cannot exceed 5TiB.
    fn validate(&self) -> Result<(), anyhow::Error> {
        let S3MultiPartUploaderConfig {
            part_size_limit,
            file_size_limit,
        } = self;
        if part_size_limit < &AWS_S3_MIN_PART_SIZE.as_u64()
            || part_size_limit > &AWS_S3_MAX_PART_SIZE.as_u64()
        {
            return Err(anyhow!(format!(
                "invalid part size: {}, should be between {} and {} bytes",
                part_size_limit,
                AWS_S3_MIN_PART_SIZE.as_u64(),
                AWS_S3_MAX_PART_SIZE.as_u64()
            )));
        }
        if file_size_limit > &AWS_S3_MAX_OBJECT_SIZE.as_u64() {
            return Err(anyhow!(format!(
                "invalid file size: {}, cannot exceed {} bytes",
                file_size_limit,
                AWS_S3_MAX_OBJECT_SIZE.as_u64()
            )));
        }
        let max_parts_count: u64 = AWS_S3_MAX_PART_COUNT.try_into().expect("i32 to u64");
        // Using `div_ceil` because we want the fraction to be rounded up i.e. 4.5 should be rounded
        // to 5 instead of 4 to accurately get the required number of parts.
        let estimated_parts_count = file_size_limit.div_ceil(*part_size_limit);
        if estimated_parts_count > max_parts_count {
            return Err(anyhow!(format!(
                "total number of possible parts (file_size_limit / part_size_limit): {}, cannot exceed {}",
                estimated_parts_count, AWS_S3_MAX_PART_COUNT
            )));
        }
        Ok(())
    }
}

impl Default for S3MultiPartUploaderConfig {
    fn default() -> Self {
        Self {
            part_size_limit: Self::DEFAULT_PART_SIZE_LIMIT.as_u64(),
            file_size_limit: Self::DEFAULT_MAX_FILE_SIZE.as_u64(),
        }
    }
}

impl S3MultiPartUploader {
    /// Creates a an instance of `S3MultiPartUploader` for the given `bucket` and `path`.
    /// This starts the multi part upload by making a `create_multipart_upload` call, and
    /// initializes all the internal state required to track the ongoing upload.
    pub async fn try_new(
        sdk_config: &SdkConfig,
        bucket: String,
        key: String,
        config: S3MultiPartUploaderConfig,
    ) -> Result<S3MultiPartUploader, S3MultiPartUploadError> {
        // Validate the config
        config.validate()?;

        let client = crate::s3::new_client(sdk_config);
        let res = client
            .create_multipart_upload()
            .bucket(&bucket)
            .key(&key)
            .customize()
            .mutate_request(|req| {
                // For GCS, Content-Length must be 0 when initiating MPU
                // https://cloud.google.com/storage/docs/xml-api/post-object-multipart
                req.headers_mut().insert("Content-Length", "0");
            })
            .send()
            .await?;
        let upload_id = res
            .upload_id()
            .ok_or_else(|| anyhow!("create_multipart_upload response missing upload id"))?
            .to_string();
        Ok(S3MultiPartUploader {
            client,
            bucket,
            key,
            upload_id,
            part_count: 0,
            total_bytes_uploaded: 0,
            buffer: Default::default(),
            config,
            upload_handles: Default::default(),
        })
    }

    /// Adds the `data` to the internal buffer and flushes the buffer if it is more than
    /// the part threshold defined in `S3MultiPartUploaderConfig`.
    /// Returns an `UploadExceedsMaxFileLimit` error if the upload will exceed the configured `file_size_limit`,
    /// unless no data has been added yet. In which case, it will try to do an upload if the data size
    /// is under `part_size_limit` * 10000.
    pub fn buffer_chunk(&mut self, data: &[u8]) -> Result<(), S3MultiPartUploadError> {
        let data_len = u64::cast_from(data.len());

        let aws_max_part_count: u64 = AWS_S3_MAX_PART_COUNT.try_into().expect("i32 to u64");
        let absolute_max_file_limit = std::cmp::min(
            self.config.part_size_limit * aws_max_part_count,
            AWS_S3_MAX_OBJECT_SIZE.as_u64(),
        );

        // If no data has been uploaded yet, we can still do an upload upto `absolute_max_file_limit`.
        let can_force_first_upload = self.added_bytes() == 0 && data_len <= absolute_max_file_limit;

        if data_len <= self.remaining_bytes_limit() || can_force_first_upload {
            self.buffer.extend_from_slice(data);
            self.flush_chunks()?;
            Ok(())
        } else {
            Err(S3MultiPartUploadError::UploadExceedsMaxFileLimit(
                self.config.file_size_limit,
            ))
        }
    }

    /// Finishes the multi part upload.
    ///
    /// Returns the number of parts and number of bytes uploaded.
    pub async fn finish(mut self) -> Result<CompletedUpload, S3MultiPartUploadError> {
        let remaining = self.buffer.split();
        self.upload_part_internal(remaining.freeze())?;

        let mut parts: Vec<CompletedPart> = Vec::with_capacity(self.upload_handles.len());
        for handle in self.upload_handles {
            let (etag, part_num) = handle.wait_and_assert_finished().await?;
            match etag {
                Some(etag) => {
                    parts.push(
                        CompletedPart::builder()
                            .e_tag(etag)
                            .part_number(part_num)
                            .build(),
                    );
                }
                None => Err(anyhow!("etag for part {part_num} is None"))?,
            }
        }

        self.client
            .complete_multipart_upload()
            .bucket(&self.bucket)
            .key(&self.key)
            .upload_id(self.upload_id.clone())
            .multipart_upload(
                CompletedMultipartUpload::builder()
                    .set_parts(Some(parts))
                    .build(),
            )
            .send()
            .await?;
        Ok(CompletedUpload {
            part_count: self.part_count.try_into().expect("i32 to u32"),
            total_bytes_uploaded: self.total_bytes_uploaded,
            bucket: self.bucket,
            key: self.key,
        })
    }

    fn buffer_size(&self) -> u64 {
        u64::cast_from(self.buffer.len())
    }

    /// Internal method, returns the amount of bytes which can still be added to the multi-part upload.
    /// without exceeding `file_size_limit`.
    fn remaining_bytes_limit(&self) -> u64 {
        self.config
            .file_size_limit
            .saturating_sub(self.added_bytes())
    }

    /// Internal method, returns the number of bytes processed till now.
    pub fn added_bytes(&self) -> u64 {
        self.total_bytes_uploaded + self.buffer_size()
    }

    /// Internal method to continuously flush and upload part from the buffer till it is
    /// under the configured `part_size_limit`.
    fn flush_chunks(&mut self) -> Result<(), S3MultiPartUploadError> {
        let part_size_limit = self.config.part_size_limit;
        // TODO (mouli): can probably parallelize the calls here.
        while self.buffer_size() > part_size_limit {
            let data = self.buffer.split_to(usize::cast_from(part_size_limit));
            self.upload_part_internal(data.freeze())?;
        }
        Ok(())
    }

    /// Internal method which actually uploads a single part and updates state.
    fn upload_part_internal(&mut self, data: Bytes) -> Result<(), S3MultiPartUploadError> {
        let num_of_bytes: u64 = u64::cast_from(data.len());

        let next_part_number = self.part_count + 1;
        if next_part_number > AWS_S3_MAX_PART_COUNT {
            return Err(S3MultiPartUploadError::ExceedsMaxPartNumber);
        }
        let client = self.client.clone();
        let bucket = self.bucket.clone();
        let key = self.key.clone();
        let upload_id = self.upload_id.clone();

        let handle = spawn(|| "s3::upload_part", async move {
            let res = client
                .upload_part()
                .bucket(&bucket)
                .key(&key)
                .upload_id(upload_id)
                .part_number(next_part_number)
                .body(ByteStream::from(data))
                .send()
                .await?;
            Ok((res.e_tag, next_part_number))
        });
        self.upload_handles.push(handle);

        self.part_count = next_part_number;
        self.total_bytes_uploaded += num_of_bytes;
        Ok(())
    }
}

#[derive(thiserror::Error, Debug)]
pub enum S3MultiPartUploadError {
    #[error(
        "multi-part upload cannot have more than {} parts",
        AWS_S3_MAX_PART_COUNT
    )]
    ExceedsMaxPartNumber,
    #[error("multi-part upload will exceed configured file_size_limit: {} bytes", .0)]
    UploadExceedsMaxFileLimit(u64),
    #[error("{}", .0.display_with_causes())]
    CreateMultipartUploadError(#[from] SdkError<CreateMultipartUploadError>),
    #[error("{}", .0.display_with_causes())]
    UploadPartError(#[from] SdkError<UploadPartError>),
    #[error("{}", .0.display_with_causes())]
    CompleteMultipartUploadError(#[from] SdkError<CompleteMultipartUploadError>),
    #[error("{}", .0.display_with_causes())]
    Other(#[from] anyhow::Error),
}

/// On CI, these tests are enabled by adding the scratch-aws-access plugin
/// to the `cargo-test` step in `ci/test/pipeline.template.yml` and setting
/// `MZ_S3_UPLOADER_TEST_S3_BUCKET` in
/// `ci/test/cargo-test/mzcompose.py`.
///
/// For a Materialize developer, to opt in to these tests locally for
/// development, follow the AWS access guide:
///
/// ```text
/// https://www.notion.so/materialize/AWS-access-5fbd9513dcdc4e11a7591e8caa5f63fe
/// ```
///
/// then running `source src/aws-util/src/setup_test_env_mz.sh`. You will also have
/// to run `aws sso login` if you haven't recently.
#[cfg(test)]
mod tests {
    use bytes::Bytes;
    use uuid::Uuid;

    use super::*;
    use crate::{defaults, s3};

    fn s3_bucket_key_for_test() -> Option<(String, String)> {
        let bucket = match std::env::var("MZ_S3_UPLOADER_TEST_S3_BUCKET") {
            Ok(bucket) => bucket,
            Err(_) => {
                if mz_ore::env::is_var_truthy("CI") {
                    panic!("CI is supposed to run this test but something has gone wrong!");
                }
                return None;
            }
        };

        let prefix = Uuid::new_v4().to_string();
        let key = format!("cargo_test/{}/file", prefix);
        Some((bucket, key))
    }

    #[mz_ore::test(tokio::test(flavor = "multi_thread"))]
    #[cfg_attr(coverage, ignore)] // https://github.com/MaterializeInc/database-issues/issues/5586
    #[cfg_attr(miri, ignore)] // error: unsupported operation: can't call foreign function `TLS_method` on OS `linux`
    #[ignore] // TODO: Reenable against minio so it can run locally
    async fn multi_part_upload_success() -> Result<(), S3MultiPartUploadError> {
        let sdk_config = defaults().load().await;
        let (bucket, key) = match s3_bucket_key_for_test() {
            Some(tuple) => tuple,
            None => return Ok(()),
        };

        let config = S3MultiPartUploaderConfig::default();
        let mut uploader =
            S3MultiPartUploader::try_new(&sdk_config, bucket.clone(), key.clone(), config).await?;

        let expected_data = "onetwothree";
        uploader.buffer_chunk(b"one")?;
        uploader.buffer_chunk(b"two")?;
        uploader.buffer_chunk(b"three")?;

        // This should trigger one single part upload.
        let CompletedUpload {
            part_count,
            total_bytes_uploaded,
            bucket: _,
            key: _,
        } = uploader.finish().await?;

        // Getting the uploaded object from s3 and validating the contents.
        let s3_client = s3::new_client(&sdk_config);
        let uploaded_object = s3_client
            .get_object()
            .bucket(bucket)
            .key(key)
            .part_number(1) // fetching a particular part, so that the `parts_count` is populated in the result
            .send()
            .await
            .unwrap();

        let uploaded_parts_count: u32 = uploaded_object.parts_count().unwrap().try_into().unwrap();
        assert_eq!(uploaded_parts_count, part_count);
        assert_eq!(part_count, 1);

        let body = uploaded_object.body.collect().await.unwrap().into_bytes();
        assert_eq!(body, expected_data);

        let expected_bytes: u64 = Bytes::from(expected_data).len().try_into().unwrap();
        assert_eq!(total_bytes_uploaded, expected_bytes);

        Ok(())
    }

    #[mz_ore::test(tokio::test(flavor = "multi_thread"))]
    #[cfg_attr(coverage, ignore)] // https://github.com/MaterializeInc/database-issues/issues/5586
    #[cfg_attr(miri, ignore)] // error: unsupported operation: can't call foreign function `TLS_method` on OS `linux`
    #[ignore] // TODO: Reenable against minio so it can run locally
    async fn multi_part_upload_buffer() -> Result<(), S3MultiPartUploadError> {
        let sdk_config = defaults().load().await;
        let (bucket, key) = match s3_bucket_key_for_test() {
            Some(tuple) => tuple,
            None => return Ok(()),
        };

        let config = S3MultiPartUploaderConfig {
            part_size_limit: ByteSize::mib(5).as_u64(),
            file_size_limit: ByteSize::mib(10).as_u64(),
        };
        let mut uploader =
            S3MultiPartUploader::try_new(&sdk_config, bucket.clone(), key.clone(), config).await?;

        // Adding a chunk of 6MiB, should trigger an upload part since part_size_limit is 5MiB
        let expected_data = vec![97; 6291456]; // 6MiB
        let expected_bytes: u64 = u64::cast_from(expected_data.len());
        uploader.buffer_chunk(&expected_data)?;

        assert_eq!(uploader.remaining_bytes_limit(), ByteSize::mib(4).as_u64());

        // Adding another 6MiB should return an error since file_size_limit is 10MiB
        let error = uploader.buffer_chunk(&expected_data).unwrap_err();
        assert!(matches!(
            error,
            S3MultiPartUploadError::UploadExceedsMaxFileLimit(_)
        ));

        let CompletedUpload {
            part_count,
            total_bytes_uploaded,
            bucket: _,
            key: _,
        } = uploader.finish().await?;

        // Getting the uploaded object from s3 and validating the contents.
        let s3_client = s3::new_client(&sdk_config);
        let uploaded_object = s3_client
            .get_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await
            .unwrap();

        assert_eq!(part_count, 2); // 6MiB should be split into two parts, 5MiB and 1MiB

        let body = uploaded_object.body.collect().await.unwrap().into_bytes();
        assert_eq!(body, *expected_data);

        assert_eq!(total_bytes_uploaded, expected_bytes);

        Ok(())
    }

    #[mz_ore::test(tokio::test(flavor = "multi_thread"))]
    #[cfg_attr(coverage, ignore)] // https://github.com/MaterializeInc/database-issues/issues/5586
    #[cfg_attr(miri, ignore)] // error: unsupported operation: can't call foreign function `TLS_method` on OS `linux`
    #[ignore] // TODO: Reenable against minio so it can run locally
    async fn multi_part_upload_no_data() -> Result<(), S3MultiPartUploadError> {
        let sdk_config = defaults().load().await;
        let (bucket, key) = match s3_bucket_key_for_test() {
            Some(tuple) => tuple,
            None => return Ok(()),
        };

        let config = Default::default();
        let uploader =
            S3MultiPartUploader::try_new(&sdk_config, bucket.clone(), key.clone(), config).await?;

        // Calling finish without adding any data should succeed.
        uploader.finish().await.unwrap();

        // The file should exist but have no content.
        let s3_client = s3::new_client(&sdk_config);
        let uploaded_object = s3_client
            .get_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await
            .unwrap();

        assert_eq!(uploaded_object.content_length(), Some(0));

        Ok(())
    }

    #[mz_ore::test]
    fn test_invalid_configs() {
        let config = S3MultiPartUploaderConfig {
            part_size_limit: ByteSize::mib(5).as_u64() - 1,
            file_size_limit: ByteSize::gib(5).as_u64(),
        };
        let error = config.validate().unwrap_err();

        assert_eq!(
            error.to_string(),
            "invalid part size: 5242879, should be between 5242880 and 5368709120 bytes"
        );

        let config = S3MultiPartUploaderConfig {
            part_size_limit: ByteSize::mib(5).as_u64(),
            // Subtracting 1 so that the overall multiplier is a fraction between 10000 and 10001
            // to test rounding.
            file_size_limit: (ByteSize::mib(5).as_u64() * 10001) - 1,
        };
        let error = config.validate().unwrap_err();
        assert_eq!(
            error.to_string(),
            "total number of possible parts (file_size_limit / part_size_limit): 10001, cannot exceed 10000",
        );
    }
}

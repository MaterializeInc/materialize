// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::str::FromStr;

use anyhow::anyhow;
use http::Uri;
use mz_ore::future::InTask;
use mz_repr::{CatalogItemId, GlobalId};
use tracing::{debug, info};

use crate::connections::aws::AwsConnection;
use crate::connections::ConnectionContext;
use crate::sinks::S3UploadInfo;

/// Performs preflight checks for a copy to operation.
///
/// Checks the S3 path for the sink to ensure it's empty (aside from files
/// written by other instances of this sink), validates that we have appropriate
/// permissions, and writes an INCOMPLETE sentinel file to indicate to the user
/// that the upload is in-progress.
///
/// The INCOMPLETE sentinel is used to provide a single atomic operation that a
/// user can wire up a notification on, to know when it is safe to start
/// ingesting the data written by this sink to S3. Since the DeleteObject of the
/// INCOMPLETE sentinel will only trigger one S3 notification, even if it's
/// performed by multiple replicas it simplifies the user ergonomics by only
/// having to listen for a single event (a PutObject sentinel would trigger once
/// for each replica).
pub async fn preflight(
    connection_context: ConnectionContext,
    aws_connection: &AwsConnection,
    connection_details: &S3UploadInfo,
    connection_id: CatalogItemId,
    sink_id: GlobalId,
) -> Result<(), anyhow::Error> {
    info!(%sink_id, "s3 copy to initialization");

    let s3_key_manager = S3KeyManager::new(&sink_id, &connection_details.uri);

    let sdk_config = aws_connection
        .load_sdk_config(&connection_context, connection_id, InTask::Yes)
        .await?;

    let client = mz_aws_util::s3::new_client(&sdk_config);
    let bucket = s3_key_manager.bucket.clone();
    let path_prefix = s3_key_manager.path_prefix().to_string();
    let incomplete_sentinel_key = s3_key_manager.incomplete_sentinel_key();

    // Check that the S3 bucket path is empty before beginning the upload,
    // verify we have DeleteObject permissions,
    // and upload the INCOMPLETE sentinel file to the S3 path.

    if let Some(files) = mz_aws_util::s3::list_bucket_path(&client, &bucket, &path_prefix).await? {
        if !files.is_empty() {
            Err(anyhow::anyhow!(
                "S3 bucket path is not empty, contains {} objects",
                files.len()
            ))?;
        }
    }

    // Confirm we have DeleteObject permissions before proceeding by trying to
    // delete a known non-existent file.
    // S3 will return an AccessDenied error whether or not the object exists,
    // and no error if we have permissions and it doesn't.
    // Other S3-compatible APIs (e.g. GCS) return a 404 error if the object
    // does not exist, so we ignore that error.
    match client
        .delete_object()
        .bucket(&bucket)
        .key(s3_key_manager.data_key(0, 0, "delete_object_test"))
        .send()
        .await
    {
        Err(err) => {
            let err_code = err.raw_response().map(|r| r.status().as_u16());
            if err_code.map_or(false, |r| r == 403) {
                Err(anyhow!("AccessDenied error when using DeleteObject"))?
            } else if err_code.map_or(false, |r| r == 404) {
                // ignore 404s
            } else {
                Err(anyhow!("Error when using DeleteObject: {}", err))?
            }
        }
        Ok(_) => {}
    };

    debug!(%sink_id, "uploading INCOMPLETE sentinel file");
    client
        .put_object()
        .bucket(bucket)
        .key(incomplete_sentinel_key)
        .send()
        .await?;

    Ok::<(), anyhow::Error>(())
}

/// Helper to manage object keys created by this sink based on the S3 URI provided
/// by the user and the GlobalId that identifies this copy-to-s3 sink.
/// Since there may be multiple compute replicas running their own copy of this sink
/// we need to ensure the S3 keys are consistent such that we can detect when objects
/// were created by an instance of this sink or not.
#[derive(Clone)]
pub struct S3KeyManager {
    pub bucket: String,
    pub object_key_prefix: String,
}

impl S3KeyManager {
    pub fn new(sink_id: &GlobalId, s3_uri: &str) -> Self {
        // This url is already validated to be a valid s3 url in sequencer.
        let uri = Uri::from_str(s3_uri).expect("valid s3 url");
        let bucket = uri.host().expect("s3 bucket");
        // TODO: Can an empty path be provided?
        let path = uri.path().trim_start_matches('/').trim_end_matches('/');

        Self {
            bucket: bucket.to_string(),
            object_key_prefix: format!("{}/mz-{}-", path, sink_id),
        }
    }

    /// The S3 key to use for a specific data file, based on the batch
    /// it belongs to and the index within that batch.
    pub fn data_key(&self, batch: u64, file_index: usize, extension: &str) -> String {
        format!(
            "{}batch-{:04}-{:04}.{}",
            self.object_key_prefix, batch, file_index, extension
        )
    }

    /// The S3 key to use for the incomplete sentinel file
    pub fn incomplete_sentinel_key(&self) -> String {
        format!("{}INCOMPLETE", self.object_key_prefix)
    }

    /// The key prefix based on the URI provided by the user. NOTE this doesn't
    /// contain the additional prefix we include on all keys written by the sink
    /// e.g. `mz-{sink_id}-batch-...`
    /// This is useful when listing objects in the bucket with this prefix to
    /// determine if its clear to upload.
    pub fn path_prefix(&self) -> &str {
        self.object_key_prefix.rsplit_once('/').expect("exists").0
    }
}

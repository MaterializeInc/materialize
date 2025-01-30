// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! AWS S3 [`OneshotSource`].

use std::path::Path;
use std::str::FromStr;
use std::sync::Arc;

use derivative::Derivative;
use futures::stream::{BoxStream, TryStreamExt};
use futures::StreamExt;
use mz_ore::future::InTask;
use mz_repr::CatalogItemId;
use mz_storage_types::connections::aws::AwsConnection;
use mz_storage_types::connections::ConnectionContext;
use serde::{Deserialize, Serialize};

use crate::oneshot_source::util::IntoRangeHeaderValue;
use crate::oneshot_source::{
    OneshotObject, OneshotSource, StorageErrorX, StorageErrorXContext, StorageErrorXKind,
};

#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub struct AwsS3Source {
    // Only used for initialization.
    #[derivative(Debug = "ignore")]
    connection: Arc<AwsConnection>,
    connection_id: CatalogItemId,
    #[derivative(Debug = "ignore")]
    context: Arc<ConnectionContext>,

    /// Name of the S3 bucket we'll list from.
    bucket: String,
    /// Optional prefix that can be specified via an S3 URI.
    prefix: Option<String>,
    /// S3 client that is lazily initialized.
    #[derivative(Debug = "ignore")]
    client: std::sync::OnceLock<mz_aws_util::s3::Client>,
}

impl AwsS3Source {
    pub fn new(
        connection: AwsConnection,
        connection_id: CatalogItemId,
        context: ConnectionContext,
        uri: String,
    ) -> Self {
        let uri = http::Uri::from_str(&uri).expect("validated URI in sequencing");

        let bucket = uri
            .host()
            .expect("validated host in sequencing")
            .to_string();
        let prefix = if uri.path().is_empty() || uri.path() == "/" {
            None
        } else {
            // The S3 client expects a trailing `/` but no leading `/`.
            let mut prefix = uri.path().to_string();

            if let Some(suffix) = prefix.strip_prefix('/') {
                prefix = suffix.to_string();
            }
            if !prefix.ends_with('/') {
                prefix = format!("{prefix}/");
            }

            Some(prefix)
        };

        AwsS3Source {
            connection: Arc::new(connection),
            context: Arc::new(context),
            connection_id,
            bucket,
            prefix,
            client: std::sync::OnceLock::new(),
        }
    }

    pub async fn initialize(&self) -> Result<mz_aws_util::s3::Client, anyhow::Error> {
        let sdk_config = self
            .connection
            .load_sdk_config(&self.context, self.connection_id, InTask::Yes)
            .await?;
        let s3_client = mz_aws_util::s3::new_client(&sdk_config);

        Ok(s3_client)
    }

    pub async fn client(&self) -> Result<&mz_aws_util::s3::Client, anyhow::Error> {
        if self.client.get().is_none() {
            let client = self.initialize().await?;
            let _ = self.client.set(client);
        }

        Ok(self.client.get().expect("just initialized"))
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct S3Object {
    /// Key from S3 list operation.
    key: String,
    /// Name of the object, generally the last component of the key.
    name: String,
    /// Size of the object in bytes.
    size: usize,
}

impl OneshotObject for S3Object {
    fn name(&self) -> &str {
        &self.name
    }

    fn size(&self) -> usize {
        self.size
    }

    fn encodings(&self) -> &[super::Encoding] {
        &[]
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct S3Checksum {
    e_tag: Option<String>,
}

impl OneshotSource for AwsS3Source {
    type Object = S3Object;
    type Checksum = S3Checksum;

    async fn list<'a>(
        &'a self,
    ) -> Result<Vec<(Self::Object, Self::Checksum)>, super::StorageErrorX> {
        let client = self.client().await.map_err(StorageErrorXKind::generic)?;
        let mut objects_request = client.list_objects_v2().bucket(&self.bucket);

        // Users can optionally specify a prefix via the S3 uri they originally specify.
        if let Some(prefix) = &self.prefix {
            objects_request = objects_request.prefix(prefix);
        }

        let objects = objects_request
            .send()
            .await
            .map_err(StorageErrorXKind::generic)
            .context("list_objects_v2")?;

        // TODO(cf1): Pagination.

        let objects: Vec<_> = objects
            .contents()
            .iter()
            .map(|o| {
                let key = o
                    .key()
                    .ok_or_else(|| StorageErrorXKind::MissingField("key".into()))?
                    .to_owned();
                let name = Path::new(&key)
                    .file_name()
                    .and_then(|os_name| os_name.to_str())
                    .ok_or_else(|| StorageErrorXKind::Generic(format!("malformed key: {key}")))?
                    .to_string();
                let size = o
                    .size()
                    .ok_or_else(|| StorageErrorXKind::MissingField("size".into()))?;
                let size: usize = size.try_into().map_err(StorageErrorXKind::generic)?;

                let object = S3Object { key, name, size };
                let checksum = S3Checksum {
                    e_tag: o.e_tag().map(|x| x.to_owned()),
                };

                Ok::<_, StorageErrorXKind>((object, checksum))
            })
            .collect::<Result<_, _>>()
            .context("list")?;

        Ok(objects)
    }

    fn get<'s>(
        &'s self,
        object: Self::Object,
        _checksum: Self::Checksum,
        range: Option<std::ops::RangeInclusive<usize>>,
    ) -> BoxStream<'s, Result<bytes::Bytes, StorageErrorX>> {
        let initial_response = async move {
            tracing::info!(name = %object.name(), ?range, "fetching object");

            // TODO(cf1): Validate our checksum.
            let client = self.client().await.map_err(StorageErrorXKind::generic)?;

            let mut request = client.get_object().bucket(&self.bucket).key(&object.name);
            if let Some(range) = range {
                let value = range.into_range_header_value();
                request = request.range(value);
            }

            let object = request
                .send()
                .await
                .map_err(|err| StorageErrorXKind::AwsS3Request(err.to_string()))?;
            // AWS's ByteStream doesn't implement the Stream trait.
            let stream = mz_aws_util::s3::ByteStreamAdapter::new(object.body)
                .err_into()
                .boxed();

            Ok::<_, StorageErrorXKind>(stream)
        };

        futures::stream::once(initial_response)
            .try_flatten()
            .boxed()
    }
}

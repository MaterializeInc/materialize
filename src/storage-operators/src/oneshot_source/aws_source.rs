// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! AWS S3 [`OneshotSource`].

use std::sync::Arc;

use futures::stream::{BoxStream, TryStreamExt};
use futures::StreamExt;
use mz_ore::future::InTask;
use mz_repr::CatalogItemId;
use mz_storage_types::connections::aws::AwsConnection;
use mz_storage_types::connections::ConnectionContext;
use serde::{Deserialize, Serialize};

use crate::oneshot_source::{
    OneshotObject, OneshotSource, StorageErrorX, StorageErrorXContext, StorageErrorXKind,
};

#[derive(Clone)]
pub struct AwsS3Source {
    // Only used for initialization.
    connection: Arc<AwsConnection>,
    context: Arc<ConnectionContext>,
    id: CatalogItemId,

    //
    bucket: String,
    client: std::sync::OnceLock<mz_aws_util::s3::Client>,
}

impl AwsS3Source {
    pub fn new(
        connection: AwsConnection,
        context: ConnectionContext,
        id: CatalogItemId,
        bucket: String,
    ) -> Self {
        AwsS3Source {
            connection: Arc::new(connection),
            context: Arc::new(context),
            id,
            bucket,
            client: std::sync::OnceLock::new(),
        }
    }

    pub async fn init(&self) -> Result<(), anyhow::Error> {
        if self.client.get().is_some() {
            mz_ore::soft_panic_or_log!("AwsS3Client was already initialized!");
            // TODO(parkmycar): Should this return an error?
            return Ok(());
        }

        let sdk_config = self
            .connection
            .load_sdk_config(&self.context, self.id, InTask::Yes)
            .await?;
        let s3_client = mz_aws_util::s3::new_client(&sdk_config);
        self.client
            .set(s3_client)
            .expect("AwsS3Client was initialized in parallel");

        Ok(())
    }

    pub fn client(&self) -> &mz_aws_util::s3::Client {
        self.client
            .get()
            .expect("AwsS3Source used before initializing!")
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct S3Object {
    name: String,
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
        let objects = self
            .client()
            .list_objects_v2()
            .bucket(&self.bucket)
            .send()
            .await
            .expect("OH NO");

        // TODO(parkmycar): Pagination.

        let objects: Vec<_> = objects
            .contents()
            .iter()
            .map(|o| {
                let name = o
                    .key()
                    .ok_or_else(|| StorageErrorXKind::MissingField("key"))?
                    .to_owned();
                let size = o
                    .size()
                    .ok_or_else(|| StorageErrorXKind::MissingField("size"))?;
                let size: usize = size.try_into().map_err(|e| StorageErrorXKind::generic(e))?;

                let object = S3Object { name, size };
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
        checksum: Self::Checksum,
        range: Option<std::ops::RangeInclusive<usize>>,
    ) -> BoxStream<'s, Result<bytes::Bytes, StorageErrorX>> {
        let initial_response = async move {
            // TODO(parkmycar): Support Ranged requests and Checksum checking.

            let object = self
                .client()
                .get_object()
                .bucket(&self.bucket)
                .key(&object.name)
                .send()
                .await
                .map_err(|err| StorageErrorXKind::AwsS3Request(err.to_string()))?;

            let stream = mz_aws_util::s3::ByteStreamAdapter::new(object.body).err_into();
            Ok::<_, StorageErrorXKind>(stream)
        };

        futures::stream::once(initial_response)
            .try_flatten()
            .boxed()
    }
}

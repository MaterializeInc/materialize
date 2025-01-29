// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use aws_sdk_s3::config::Builder;
use aws_types::sdk_config::SdkConfig;
use bytes::Bytes;

pub use aws_sdk_s3::Client;

/// Creates a new client from an [SDK config](aws_types::sdk_config::SdkConfig)
/// with Materialize-specific customizations.
///
/// Specifically, if the SDK config overrides the endpoint URL, the client
/// will be configured to use path-style addressing, as custom AWS endpoints
/// typically do not support virtual host-style addressing.
pub fn new_client(sdk_config: &SdkConfig) -> Client {
    let conf = Builder::from(sdk_config)
        .force_path_style(sdk_config.endpoint_url().is_some())
        .build();
    Client::from_conf(conf)
}

pub async fn list_bucket_path(
    client: &Client,
    bucket: &str,
    prefix: &str,
) -> Result<Option<Vec<String>>, anyhow::Error> {
    let res = client
        .list_objects_v2()
        .bucket(bucket)
        .prefix(prefix)
        .send()
        .await?;
    res.contents
        .map(|objs| {
            objs.into_iter()
                .map(|obj| {
                    obj.key
                        .ok_or(anyhow::anyhow!("key not provided from list_objects_v2"))
                })
                .collect::<Result<Vec<String>, _>>()
        })
        .transpose()
}

/// A wrapper around [`ByteStream`] that implements the [`futures::stream::Stream`] trait.
///
/// [`ByteStream`]: aws_smithy_types::byte_stream::ByteStream
#[pin_project::pin_project]
pub struct ByteStreamAdapter {
    #[pin]
    inner: aws_smithy_types::byte_stream::ByteStream,
}

impl ByteStreamAdapter {
    pub fn new(bytes: aws_smithy_types::byte_stream::ByteStream) -> Self {
        ByteStreamAdapter { inner: bytes }
    }
}

impl futures::stream::Stream for ByteStreamAdapter {
    type Item = Result<Bytes, aws_smithy_types::byte_stream::error::Error>;

    fn poll_next(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        let this = self.project();
        aws_smithy_types::byte_stream::ByteStream::poll_next(this.inner, cx)
    }
}

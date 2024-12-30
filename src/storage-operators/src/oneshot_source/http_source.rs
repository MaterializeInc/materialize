// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Generic HTTP oneshot source that will fetch a file from the public internet.

use std::future::Future;
use std::pin::Pin;

use bytes::Bytes;
use futures::future::FutureExt;
use futures::stream::{BoxStream, StreamExt};
use futures::{Stream, TryStreamExt};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use url::Url;

use crate::oneshot_source::{
    OneshotSource, StorageErrorX, StorageErrorXContext, StorageErrorXKind,
};

/// Generic oneshot source that fetches a file from a URL on the public internet.
#[derive(Clone)]
pub struct HttpOneshotSource {
    client: Client,
    origin: Url,
}

impl HttpOneshotSource {
    pub fn new(client: Client, origin: Url) -> Self {
        HttpOneshotSource { client, origin }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum HttpChecksum {
    /// No checksumming is requested.
    None,
    /// The HTTP [`ETag`](https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/ETag) header.
    ETag(String),
    /// The HTTP [`Last-Modified`](https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Last-Modified) header.
    LastModified(String),
}

impl HttpChecksum {
    fn header_name(&self) -> Option<&reqwest::header::HeaderName> {
        match self {
            HttpChecksum::None => None,
            HttpChecksum::ETag(_) => Some(&reqwest::header::ETAG),
            HttpChecksum::LastModified(_) => Some(&reqwest::header::LAST_MODIFIED),
        }
    }
}

impl OneshotSource for HttpOneshotSource {
    type Object = Url;
    type Checksum = HttpChecksum;

    async fn list<'a>(&'a self) -> Result<Vec<(Self::Object, Self::Checksum)>, StorageErrorX> {
        // TODO(parkmycar): Support listing files from a directory index.

        // Submit a HEAD request so we can discover metadata about the file.
        let response = self
            .client
            .head(self.origin.clone())
            .send()
            .await
            .context("HEAD request")?;

        let get_header = |name: &reqwest::header::HeaderName| {
            let header = response.headers().get(name)?;
            match header.to_str() {
                Err(e) => {
                    tracing::warn!("failed to deserialize header '{name}', err: {e}");
                    None
                }
                Ok(value) => Some(value),
            }
        };

        // Get a checksum from the content.
        let checksum = if let Some(etag) = get_header(&reqwest::header::ETAG) {
            HttpChecksum::ETag(etag.to_string())
        } else if let Some(last_modified) = get_header(&reqwest::header::LAST_MODIFIED) {
            let last_modified = last_modified.to_string();
            HttpChecksum::LastModified(last_modified.to_string())
        } else {
            HttpChecksum::None
        };

        // TODO(parkmycar): We should probably check the content-type as well. At least for advisory purposes.

        Ok(vec![(self.origin.clone(), checksum)])
    }

    fn get<'s>(
        &'s self,
        object: Self::Object,
        _checksum: Self::Checksum,
        _range: Option<std::ops::Range<usize>>,
    ) -> BoxStream<'s, Result<Bytes, StorageErrorX>> {
        // TODO(parkmycar): Support the range param.

        let initial_response = async move {
            let response = self
                .client
                .get(object.to_owned())
                .send()
                .await
                .context("get")?;
            let bytes_stream = response.bytes_stream().err_into();

            Ok::<_, StorageErrorX>(bytes_stream)
        };

        futures::stream::once(initial_response)
            .try_flatten()
            // .flat_map(|result| match result {
            //     Ok(stream) => stream.right_stream(),
            //     Err(err) => futures::stream::once(async move { Err(err) }).left_stream(),
            // })
            .boxed()
    }
}

// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Generic HTTP oneshot source that will fetch a file from the public internet.

use bytes::Bytes;
use futures::stream::{BoxStream, StreamExt};
use futures::TryStreamExt;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use url::Url;

use crate::oneshot_source::{
    Encoding, OneshotObject, OneshotSource, StorageErrorX, StorageErrorXContext,
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

/// Object returned from an [`HttpOneshotSource`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HttpObject {
    /// [`Url`] to access the file.
    url: Url,
    /// Name of the file.
    filename: String,
    /// Any values reporting from the [`Content-Encoding`](https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Content-Encoding) header.
    content_encoding: Vec<Encoding>,
}

impl OneshotObject for HttpObject {
    fn name(&self) -> &str {
        &self.filename
    }

    fn encodings(&self) -> &[Encoding] {
        &self.content_encoding
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

impl OneshotSource for HttpOneshotSource {
    type Object = HttpObject;
    type Checksum = HttpChecksum;

    async fn list<'a>(&'a self) -> Result<Vec<(Self::Object, Self::Checksum)>, StorageErrorX> {
        // TODO(cf3): Support listing files from a directory index.

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

        // TODO(cf1): We should probably check the content-type as well. At least for advisory purposes.

        let filename = self
            .origin
            .path_segments()
            .and_then(|segments| segments.rev().next())
            .map(|s| s.to_string())
            .unwrap_or_default();
        let object = HttpObject {
            url: self.origin.clone(),
            filename,
            content_encoding: vec![],
        };
        tracing::info!(?object, "found objects");

        Ok(vec![(object, checksum)])
    }

    fn get<'s>(
        &'s self,
        object: Self::Object,
        _checksum: Self::Checksum,
        _range: Option<std::ops::Range<usize>>,
    ) -> BoxStream<'s, Result<Bytes, StorageErrorX>> {
        // TODO(cf1): Support the range param.
        // TODO(cf1): Validate our checksum.

        let initial_response = async move {
            let response = self.client.get(object.url).send().await.context("get")?;
            let bytes_stream = response.bytes_stream().err_into();

            Ok::<_, StorageErrorX>(bytes_stream)
        };

        futures::stream::once(initial_response)
            .try_flatten()
            .boxed()
    }
}

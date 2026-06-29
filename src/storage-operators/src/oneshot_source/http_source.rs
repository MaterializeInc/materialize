// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Generic HTTP oneshot source that will fetch a file from the public internet.

use std::net::SocketAddr;
use std::sync::Arc;

use bytes::Bytes;
use derivative::Derivative;
use futures::TryStreamExt;
use futures::stream::{BoxStream, StreamExt};
use mz_ore::url::SensitiveUrl;
use reqwest::Client;
use reqwest::dns::{Addrs, Name, Resolve, Resolving};
use serde::{Deserialize, Serialize};

use crate::oneshot_source::util::IntoRangeHeaderValue;
use crate::oneshot_source::{
    Encoding, OneshotObject, OneshotSource, StorageErrorX, StorageErrorXContext, StorageErrorXKind,
};

/// reqwest DNS resolver that delegates to [`mz_ore::netio::resolve_address`].
///
/// Only the IP resolution step is overridden — reqwest still uses the URL's
/// original hostname for SNI and TLS certificate validation, so HTTPS works
/// normally.
#[derive(Debug)]
struct MzHttpResolver {
    enforce_external_addresses: bool,
}

impl Resolve for MzHttpResolver {
    fn resolve(&self, name: Name) -> Resolving {
        let enforce = self.enforce_external_addresses;
        Box::pin(async move {
            let ips = mz_ore::netio::resolve_address(name.as_str(), enforce).await?;
            // reqwest substitutes the conventional port (80/443) when the
            // SocketAddr's port is 0 and no explicit port was given in the URL.
            let addrs: Addrs = Box::new(
                ips.into_iter()
                    .map(|ip| SocketAddr::new(ip, 0))
                    .collect::<Vec<_>>()
                    .into_iter(),
            );
            Ok(addrs)
        })
    }
}

/// Build a reqwest [`Client`] for fetching `COPY FROM` URLs. This uses
/// [`mz_ore::netio::resolve_address`] for DNS resolution.
///
/// Redirects are disabled: the custom DNS resolver re-validates hostnames on
/// every hop, but reqwest skips DNS for IP-literal targets, so a redirect to
/// `http://127.0.0.1/` would bypass the SSRF check. Refusing to follow
/// redirects closes that hole.
pub fn build_http_client(enforce_external_addresses: bool) -> Result<Client, reqwest::Error> {
    Client::builder()
        .dns_resolver(Arc::new(MzHttpResolver {
            enforce_external_addresses,
        }))
        .redirect(reqwest::redirect::Policy::none())
        .build()
}

/// Returns an error if `response` is a 3xx redirect. Materialize disables
/// redirect following on the HTTP client (see `build_http_client`) to close
/// an SSRF hole, so callers must surface a meaningful error rather than
/// letting the response fall through to header parsing.
fn check_not_redirect(response: &reqwest::Response) -> Result<(), StorageErrorX> {
    if response.status().is_redirection() {
        return Err(StorageErrorXKind::Redirect(response.status().as_u16()).into());
    }
    Ok(())
}

/// Generic oneshot source that fetches a file from a URL on the public internet.
#[derive(Clone, Derivative)]
#[derivative(Debug)]
pub struct HttpOneshotSource {
    #[derivative(Debug = "ignore")]
    client: Client,
    origin: SensitiveUrl,
}

impl HttpOneshotSource {
    pub fn new(client: Client, origin: SensitiveUrl) -> Self {
        HttpOneshotSource { client, origin }
    }
}

/// Object returned from an [`HttpOneshotSource`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HttpObject {
    /// [`SensitiveUrl`] to access the file.
    url: SensitiveUrl,
    /// Name of the file.
    filename: String,
    /// Size of this file reported by the [`Content-Length`](https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Content-Length) header
    size: usize,
    /// Any values reporting from the [`Content-Encoding`](https://developer.mozilla.org/en-US/docs/Web/HTTP/Headers/Content-Encoding) header.
    content_encoding: Vec<Encoding>,
}

impl OneshotObject for HttpObject {
    fn name(&self) -> &str {
        &self.filename
    }

    fn path(&self) -> &str {
        &self.filename
    }

    fn size(&self) -> usize {
        self.size
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

#[cfg(test)]
mod tests {
    use super::*;

    /// `reqwest::dns::Name` has no public constructor, so we exercise
    /// [`MzHttpResolver`] through a fully-built [`Client`]. `localhost`
    /// resolves via /etc/hosts on supported platforms and stays inside the
    /// resolver path (an IP literal would short-circuit DNS entirely).
    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)]
    async fn build_http_client_rejects_localhost_when_enforced() {
        let client = build_http_client(true).expect("build client");
        let err = client
            .get("http://localhost:1/")
            .send()
            .await
            .expect_err("request must fail at DNS resolution");
        // Walk the error chain for the resolver's PrivateAddress message —
        // reqwest wraps it inside its connect error, so a `to_string()` on
        // the top-level error is not enough.
        let mut found = false;
        let mut current: &dyn std::error::Error = &err;
        loop {
            if current.to_string().to_lowercase().contains("private") {
                found = true;
                break;
            }
            match current.source() {
                Some(src) => current = src,
                None => break,
            }
        }
        assert!(found, "expected private-address rejection, got: {err:?}");
    }

    /// With enforcement off the resolver returns the loopback IP, so the
    /// request reaches the connect stage and fails for a different reason
    /// (port 1 is not listening). The point is that DNS does *not* fail.
    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)]
    async fn build_http_client_allows_localhost_when_not_enforced() {
        let client = build_http_client(false).expect("build client");
        let err = client
            .get("http://localhost:1/")
            .send()
            .await
            .expect_err("port 1 should not be listening");
        let mut current: &dyn std::error::Error = &err;
        loop {
            assert!(
                !current.to_string().to_lowercase().contains("private"),
                "expected a connect error, not a DNS rejection: {err:?}"
            );
            match current.source() {
                Some(src) => current = src,
                None => break,
            }
        }
    }
}

impl OneshotSource for HttpOneshotSource {
    type Object = HttpObject;
    type Checksum = HttpChecksum;

    async fn list<'a>(&'a self) -> Result<Vec<(Self::Object, Self::Checksum)>, StorageErrorX> {
        // TODO(cf3): Support listing files from a directory index.

        // To get metadata about a file we'll first try issuing a `HEAD` request, which
        // canonically is the right thing do.
        let response = self
            .client
            .head(self.origin.0.clone())
            .send()
            .await
            .context("HEAD request")?;

        check_not_redirect(&response)?;

        // Not all servers accept `HEAD` requests though, so we'll fallback to a `GET`
        // request and skip fetching the body.
        let headers = match response.error_for_status() {
            Ok(response) => response.headers().clone(),
            Err(err) => {
                tracing::warn!(status = ?err.status(), "HEAD request failed");

                let response = self
                    .client
                    .get(self.origin.0.clone())
                    .send()
                    .await
                    .context("GET request")?;

                check_not_redirect(&response)?;

                let headers = response.headers().clone();

                // Immediately drop the response so we don't attempt to fetch the body.
                drop(response);

                headers
            }
        };

        let get_header = |name: &reqwest::header::HeaderName| {
            let header = headers.get(name)?;
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

        // Get the size of the object from the Conent-Length header.
        let size = get_header(&reqwest::header::CONTENT_LENGTH)
            .ok_or(StorageErrorXKind::MissingSize)
            .and_then(|s| s.parse::<usize>().map_err(StorageErrorXKind::generic))
            .context("content-length header")?;

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
            size,
            content_encoding: vec![],
        };
        tracing::info!(?object, "found objects");

        Ok(vec![(object, checksum)])
    }

    fn get<'s>(
        &'s self,
        object: Self::Object,
        _checksum: Self::Checksum,
        range: Option<std::ops::RangeInclusive<usize>>,
    ) -> BoxStream<'s, Result<Bytes, StorageErrorX>> {
        // TODO(cf1): Validate our checksum.

        let initial_response = async move {
            let mut request = self.client.get(object.url.0);

            if let Some(range) = &range {
                let value = range.into_range_header_value();
                request = request.header(&reqwest::header::RANGE, value);
            }

            // TODO(parkmycar): We should probably assert that the response contains
            // an appropriate Content-Range header in the response, and maybe that we
            // got back an HTTP 206?

            let response = request.send().await.context("get")?;
            check_not_redirect(&response)?;
            let bytes_stream = response.bytes_stream().err_into();

            Ok::<_, StorageErrorX>(bytes_stream)
        };

        futures::stream::once(initial_response)
            .try_flatten()
            .boxed()
    }
}

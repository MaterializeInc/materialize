// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Azurite-only Shared Key request signing.
//!
//! Azurite doesn't accept Entra ID / OAuth tokens, only the Azure Storage
//! "Shared Key" authentication scheme. The new Azure SDK dropped its
//! built-in emulator support, so we install a `Policy` on the test client
//! that signs each outgoing request.
//!
//! Spec: <https://learn.microsoft.com/en-us/rest/api/storageservices/authorize-with-shared-key>
//!
//! This module is `#[cfg(test)]`-gated and never compiled into production
//! builds; the Azurite dev key is a publicly documented constant.

use std::fmt;
use std::sync::Arc;

use async_trait::async_trait;
use azure_core::base64;
use azure_core::http::policies::{Policy, PolicyResult};
use azure_core::http::{Context, Request};
use hmac::{Hmac, Mac};
use sha2::Sha256;
use url::Url;

use crate::error::Error;

use super::{AZURITE_ACCOUNT, BlobContainerClientOptions};

/// Default Azurite account key. Publicly documented, hard-coded.
const AZURITE_KEY: &str =
    "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==";

/// Build a container URL for Azurite from the emulator URL provided by
/// the test harness.
///
/// Azurite expects URLs of the form
/// `http://{host}:{port}/devstoreaccount1/{container}`.
pub fn container_url(url: &Url, container: &str) -> Result<Url, Error> {
    let host = url
        .host_str()
        .ok_or_else(|| Error::from(format!("Azurite URL missing host: {url}")))?;
    let port = url
        .port()
        .ok_or_else(|| Error::from(format!("Azurite URL missing port: {url}")))?;
    Url::parse(&format!(
        "http://{host}:{port}/{AZURITE_ACCOUNT}/{container}"
    ))
    .map_err(|e| Error::from(format!("invalid Azurite URL: {e}")))
}

pub fn is_emulator_url(url: &Url) -> bool {
    url.path()
        .trim_start_matches('/')
        .starts_with(AZURITE_ACCOUNT)
}

pub fn add_shared_key_policy(options: &mut BlobContainerClientOptions) {
    options
        .client_options
        .per_try_policies
        .push(Arc::new(SharedKeyPolicy));
}

pub struct SharedKeyPolicy;

impl fmt::Debug for SharedKeyPolicy {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("SharedKeyPolicy")
    }
}

#[async_trait]
impl Policy for SharedKeyPolicy {
    async fn send(
        &self,
        ctx: &Context,
        request: &mut Request,
        next: &[Arc<dyn Policy>],
    ) -> PolicyResult {
        // `insert_header` stores the value with `'static` bounds, so we
        // have to hand it an owned `String`.
        let date = azure_core::time::to_rfc7231(&azure_core::time::OffsetDateTime::now_utc());
        request.insert_header("x-ms-date", date);
        request.insert_header("x-ms-version", "2024-08-04");

        let string_to_sign = compute_signature(request);
        let key = base64::decode(AZURITE_KEY).expect("valid base64 Azurite key");
        let mut mac = Hmac::<Sha256>::new_from_slice(&key).expect("HMAC accepts any key");
        mac.update(string_to_sign.as_bytes());
        let auth = base64::encode(mac.finalize().into_bytes());
        request.insert_header(
            "authorization",
            format!("SharedKey {AZURITE_ACCOUNT}:{auth}"),
        );

        next[0].send(ctx, request, &next[1..]).await
    }
}

/// Build the canonical string-to-sign per Azure Shared Key spec.
fn compute_signature(request: &Request) -> String {
    let method = match request.method() {
        azure_core::http::Method::Delete => "DELETE",
        azure_core::http::Method::Get => "GET",
        azure_core::http::Method::Head => "HEAD",
        azure_core::http::Method::Patch => "PATCH",
        azure_core::http::Method::Post => "POST",
        azure_core::http::Method::Put => "PUT",
        // `Method` is `#[non_exhaustive]` — unreachable today, but fall
        // back to the method's debug string if the SDK adds a new verb.
        other => return format!("{other:?}"),
    };

    let headers = request.headers();
    let header = |name: &str| {
        headers
            .get_optional_str(&azure_core::http::headers::HeaderName::from(
                name.to_owned(),
            ))
            .unwrap_or("")
    };

    // Content-Length is signed as empty when zero per spec.
    let content_length = {
        let v = header("content-length");
        if v == "0" { "" } else { v }
    };

    // x-ms-* headers, lowercased, sorted, joined as `name:value\n`.
    let mut x_ms_headers: Vec<(String, String)> = headers
        .iter()
        .filter_map(|(name, value)| {
            let n = name.as_str().to_ascii_lowercase();
            if n.starts_with("x-ms-") {
                Some((n, value.as_str().trim().to_owned()))
            } else {
                None
            }
        })
        .collect();
    x_ms_headers.sort_by(|a, b| a.0.cmp(&b.0));
    let canonicalized_headers = x_ms_headers
        .iter()
        .map(|(n, v)| format!("{n}:{v}"))
        .collect::<Vec<_>>()
        .join("\n");

    // Canonicalized resource: `/account/path` + sorted query params
    // joined as `\nname:value`.
    let url = request.url();
    let mut canonicalized_resource = format!("/{AZURITE_ACCOUNT}{}", url.path());
    let mut query_pairs: Vec<(String, String)> = url
        .query_pairs()
        .map(|(k, v)| (k.to_ascii_lowercase(), v.into_owned()))
        .collect();
    query_pairs.sort_by(|a, b| a.0.cmp(&b.0));
    for (k, v) in query_pairs {
        canonicalized_resource.push_str(&format!("\n{k}:{v}"));
    }

    [
        method,
        header("content-encoding"),
        header("content-language"),
        content_length,
        header("content-md5"),
        header("content-type"),
        // `Date` is empty because we always send `x-ms-date` instead.
        "",
        header("if-modified-since"),
        header("if-match"),
        header("if-none-match"),
        header("if-unmodified-since"),
        header("range"),
        &canonicalized_headers,
        &canonicalized_resource,
    ]
    .join("\n")
}

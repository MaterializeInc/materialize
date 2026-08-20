// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Support for Azurite, the Azure Storage emulator that our tests run against.
//!
//! Azurite does not accept Entra ID tokens, only the Azure Storage "Shared Key"
//! scheme. The Azure SDK dropped its built-in emulator support in 1.x, so
//! [SharedKeyPolicy] signs each outgoing request instead.
//!
//! Spec: <https://learn.microsoft.com/en-us/rest/api/storageservices/authorize-with-shared-key>
//!
//! The emulator key below is a publicly documented constant, and this module is
//! only reachable for the equally well-known [ACCOUNT] name, so compiling it
//! into release binaries grants nothing that the account name does not.

use std::fmt::{Debug, Formatter};
use std::sync::Arc;

use async_trait::async_trait;
use azure_core::credentials::Secret;
use azure_core::hmac::hmac_sha256;
use azure_core::http::headers::HeaderName;
use azure_core::http::policies::{Policy, PolicyResult};
use azure_core::http::{Context, Method, Request};
use url::Url;

use crate::error::Error;

/// The well-known account name Azurite serves.
pub const ACCOUNT: &str = "devstoreaccount1";

/// The well-known Azurite account key, published in the emulator's docs.
const KEY: &str =
    "Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==";

/// The storage service API version to ask Azurite for.
///
/// Azurite rejects any `x-ms-version` newer than the one it implements, and the
/// SDK's default is newer than the version our pinned emulator (see
/// `test/azurite/Dockerfile`) knows about. This is the newest version that
/// emulator accepts; raise it when the emulator is upgraded.
pub const API_VERSION: &str = "2025-01-05";

/// Builds the container URL to address Azurite at, given the URL persist was
/// configured with.
///
/// Azurite is run with `--disableProductStyleUrl`, so it reads the account name
/// from the first path segment rather than from the host: the URL has the form
/// `http://{host}:{port}/devstoreaccount1/{container}`. The host may carry the
/// account name as a subdomain as well, which Azurite ignores.
pub fn container_url(url: &Url, container: &str) -> Result<Url, Error> {
    let host = url
        .host_str()
        .ok_or_else(|| Error::from(format!("Azurite URL missing host: {url}")))?;
    let port = url
        .port()
        .ok_or_else(|| Error::from(format!("Azurite URL missing port: {url}")))?;
    Url::parse(&format!("http://{host}:{port}/{ACCOUNT}/{container}"))
        .map_err(|e| Error::from(format!("invalid Azurite URL: {e}")))
}

/// Whether `url` addresses Azurite, i.e. whether it was built by
/// [container_url].
pub fn is_emulator_url(url: &Url) -> bool {
    url.path_segments()
        .is_some_and(|mut segments| segments.next() == Some(ACCOUNT))
}

/// Signs each request with the Azure Storage Shared Key scheme.
///
/// Install as a per-try policy: the signature covers `x-ms-date`, so it has to
/// be recomputed for every attempt rather than once per operation.
pub struct SharedKeyPolicy;

impl Debug for SharedKeyPolicy {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
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
        // `insert_header` stores values with a `'static` bound, so it takes an
        // owned `String`. `x-ms-version` is already set by the generated
        // clients and is signed along with everything else.
        let now = azure_core::time::OffsetDateTime::now_utc();
        request.insert_header("x-ms-date", azure_core::time::to_rfc7231(&now));

        let signature = hmac_sha256(&string_to_sign(request), &Secret::new(KEY))
            .expect("valid base64 Azurite key");
        request.insert_header("authorization", format!("SharedKey {ACCOUNT}:{signature}"));

        next[0].send(ctx, request, &next[1..]).await
    }
}

/// Builds the canonical string to sign for `request`, per the Shared Key spec.
fn string_to_sign(request: &Request) -> String {
    let method = match request.method() {
        Method::Delete => "DELETE",
        Method::Get => "GET",
        Method::Head => "HEAD",
        Method::Patch => "PATCH",
        Method::Post => "POST",
        Method::Put => "PUT",
        // `Method` is `#[non_exhaustive]`. No blob operation we issue uses
        // another verb, so failing to sign here would be a bug we want to see.
        other => panic!("unsigned HTTP method {other:?}"),
    };

    let headers = request.headers();
    let header = |name: &'static str| {
        headers
            .get_optional_str(&HeaderName::from_static(name))
            .unwrap_or("")
    };

    // A zero content length is signed as the empty string. Requests without a
    // body have no `content-length` header at this point: the transport policy,
    // which runs after this one, is what adds `content-length: 0`.
    let content_length = match header("content-length") {
        "0" => "",
        other => other,
    };

    // `x-ms-*` headers, lowercased, sorted by name, one `name:value` per line.
    let mut x_ms_headers: Vec<(String, &str)> = headers
        .iter()
        .filter_map(|(name, value)| {
            let name = name.as_str().to_ascii_lowercase();
            name.starts_with("x-ms-")
                .then(|| (name, value.as_str().trim()))
        })
        .collect();
    x_ms_headers.sort();
    let canonicalized_headers = x_ms_headers
        .iter()
        .map(|(name, value)| format!("{name}:{value}"))
        .collect::<Vec<_>>()
        .join("\n");

    // The canonicalized resource is `/{account}` followed by the resource path,
    // then the query parameters lowercased, sorted by name, one `\nname:value`
    // per parameter.
    //
    // NOTE: the account name appears twice. The emulator addresses the account
    // through the URL path (see [container_url]), so the resource path already
    // begins with it, and Azurite still prepends the account it resolved the
    // request to. Signing this any other way fails with `AuthorizationFailure`.
    let url = request.url();
    let mut canonicalized_resource = format!("/{ACCOUNT}{}", url.path());
    let mut query_pairs: Vec<(String, String)> = url
        .query_pairs()
        .map(|(name, value)| (name.to_ascii_lowercase(), value.into_owned()))
        .collect();
    query_pairs.sort();
    for (name, value) in query_pairs {
        canonicalized_resource.push_str(&format!("\n{name}:{value}"));
    }

    [
        method,
        header("content-encoding"),
        header("content-language"),
        content_length,
        header("content-md5"),
        header("content-type"),
        // `Date` is empty because we sign `x-ms-date` instead.
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

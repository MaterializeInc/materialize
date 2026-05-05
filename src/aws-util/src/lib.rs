// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::future::Future;
use std::net::SocketAddr;
use std::pin::Pin;
use std::task::{Context, Poll};

use aws_config::{BehaviorVersion, ConfigLoader};
use aws_smithy_runtime::client::http::hyper_014::HyperClientBuilder;
use aws_smithy_runtime_api::client::http::{HttpClient, SharedHttpClient};
use hyper_0_14::client::HttpConnector;
use hyper_0_14::client::connect::dns::Name;
use hyper_tls::HttpsConnector;
use tower_service::Service;

#[cfg(feature = "s3")]
pub mod s3;
#[cfg(feature = "s3")]
pub mod s3_uploader;

/// Creates an AWS SDK configuration loader with the defaults for the latest
/// behavior version plus some Materialize-specific overrides.
pub fn defaults() -> ConfigLoader {
    // Use the SDK's latest behavior version. We already pin the crate versions,
    // and CI puts version upgrades through rigorous testing, so we're happy to
    // take the latest behavior version. We can adjust this in the future as
    // necessary, if the AWS SDK ships a new behavior version that causes
    // trouble.
    let behavior_version = BehaviorVersion::latest();

    // This is the only method allowed to call `aws_config::defaults`.
    #[allow(clippy::disallowed_methods)]
    let loader = aws_config::defaults(behavior_version);

    // Install our custom HTTP client.
    let loader = loader.http_client(http_client());

    loader
}

/// Returns an HTTP client for use with the AWS SDK that is appropriately
/// configured for Materialize.
pub fn http_client() -> impl HttpClient {
    // The default AWS HTTP client uses rustls, while our company policy is to
    // use native TLS.
    HyperClientBuilder::new().build(HttpsConnector::new())
}

/// Returns an AWS SDK HTTP client whose DNS resolver delegates to
/// [`mz_ore::netio::resolve_address`].
///
/// Only the IP resolution step is overridden — the SDK still uses the original
/// hostname for SNI and TLS certificate validation, so HTTPS endpoints work
/// unchanged.
pub fn http_client_with_resolver(enforce_external_addresses: bool) -> SharedHttpClient {
    let resolver = MzAwsResolver {
        enforce_external_addresses,
    };
    let mut http = HttpConnector::new_with_resolver(resolver);
    // The SDK speaks HTTPS to the public AWS API; the wrapper we build below
    // handles TLS, but the underlying HTTP connector must allow non-`http://`
    // schemes.
    http.enforce_http(false);
    let https = HttpsConnector::new_with_connector(http);
    HyperClientBuilder::new().build(https)
}

/// A `tower_service::Service<Name>` resolver that delegates to
/// [`mz_ore::netio::resolve_address`], used by [`http_client_with_resolver`].
#[derive(Clone)]
struct MzAwsResolver {
    enforce_external_addresses: bool,
}

impl Service<Name> for MzAwsResolver {
    type Response = std::vec::IntoIter<SocketAddr>;
    type Error = mz_ore::netio::DnsResolutionError;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, name: Name) -> Self::Future {
        let enforce = self.enforce_external_addresses;
        let host = name.as_str().to_string();
        Box::pin(async move {
            let ips = mz_ore::netio::resolve_address(&host, enforce).await?;
            // Hyper substitutes the URL's port (or the default for the scheme)
            // when the SocketAddr's port is 0.
            Ok(ips
                .into_iter()
                .map(|ip| SocketAddr::new(ip, 0))
                .collect::<Vec<_>>()
                .into_iter())
        })
    }
}

#[cfg(test)]
mod tests {
    use std::net::IpAddr;
    use std::str::FromStr;

    use mz_ore::netio::DnsResolutionError;

    use super::*;

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)]
    async fn resolver_rejects_loopback_when_enforced() {
        let mut resolver = MzAwsResolver {
            enforce_external_addresses: true,
        };
        let name = Name::from_str("127.0.0.1").unwrap();
        let err = resolver.call(name).await.expect_err("must reject loopback");
        assert!(
            matches!(err, DnsResolutionError::PrivateAddress),
            "got {err:?}"
        );
    }

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)]
    async fn resolver_allows_loopback_when_not_enforced() {
        let mut resolver = MzAwsResolver {
            enforce_external_addresses: false,
        };
        let name = Name::from_str("127.0.0.1").unwrap();
        let addrs: Vec<SocketAddr> = resolver
            .call(name)
            .await
            .expect("loopback should resolve when enforcement is off")
            .collect();
        assert!(addrs.iter().any(|a| a.ip() == IpAddr::from([127, 0, 0, 1])));
    }

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)]
    async fn resolver_allows_public_when_enforced() {
        let mut resolver = MzAwsResolver {
            enforce_external_addresses: true,
        };
        let name = Name::from_str("8.8.8.8").unwrap();
        let addrs: Vec<SocketAddr> = resolver
            .call(name)
            .await
            .expect("public IP should resolve")
            .collect();
        assert!(addrs.iter().any(|a| a.ip() == IpAddr::from([8, 8, 8, 8])));
    }
}

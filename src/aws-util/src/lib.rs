// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::net::IpAddr;

use aws_config::{BehaviorVersion, ConfigLoader};
use aws_smithy_http_client::tls::{self, rustls_provider::CryptoMode};
use aws_smithy_runtime_api::client::dns::{DnsFuture, ResolveDns, ResolveDnsError};
use aws_smithy_runtime_api::client::http::{HttpClient, SharedHttpClient};

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
    aws_smithy_http_client::Builder::new()
        .tls_provider(tls_provider())
        .build_https()
}

/// Returns an AWS SDK HTTP client whose DNS resolver delegates to
/// [`mz_ore::netio::resolve_address`].
///
/// Only the IP resolution step is overridden. The SDK still uses the original
/// hostname for SNI and TLS certificate validation, so HTTPS endpoints work
/// unchanged.
pub fn http_client_with_resolver(enforce_external_addresses: bool) -> SharedHttpClient {
    aws_smithy_http_client::Builder::new()
        .tls_provider(tls_provider())
        .build_with_resolver(MzAwsResolver {
            enforce_external_addresses,
        })
}

/// rustls on the aws-lc-rs provider, trusting the system's root certificates.
fn tls_provider() -> tls::Provider {
    // TODO(SEC-218): the rest of the workspace is still migrating from OpenSSL
    // to rustls on aws-lc-rs.
    tls::Provider::Rustls(CryptoMode::AwsLc)
}

/// A [`ResolveDns`] implementation that delegates to
/// [`mz_ore::netio::resolve_address`], used by [`http_client_with_resolver`].
#[derive(Clone, Debug)]
struct MzAwsResolver {
    enforce_external_addresses: bool,
}

impl MzAwsResolver {
    async fn resolve(&self, host: &str) -> Result<Vec<IpAddr>, mz_ore::netio::DnsResolutionError> {
        let ips = mz_ore::netio::resolve_address(host, self.enforce_external_addresses).await?;
        Ok(ips.into_iter().collect())
    }
}

impl ResolveDns for MzAwsResolver {
    fn resolve_dns<'a>(&'a self, name: &'a str) -> DnsFuture<'a> {
        DnsFuture::new(async move { self.resolve(name).await.map_err(ResolveDnsError::new) })
    }
}

#[cfg(test)]
mod tests {
    use mz_ore::netio::DnsResolutionError;

    use super::*;

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)]
    async fn resolver_rejects_loopback_when_enforced() {
        let resolver = MzAwsResolver {
            enforce_external_addresses: true,
        };
        let err = resolver
            .resolve("127.0.0.1")
            .await
            .expect_err("must reject loopback");
        assert!(
            matches!(err, DnsResolutionError::PrivateAddress),
            "got {err:?}"
        );
        let err = resolver
            .resolve_dns("127.0.0.1")
            .await
            .expect_err("must reject loopback");
        let source = std::error::Error::source(&err).expect("wraps the resolution error");
        assert!(
            matches!(
                source.downcast_ref::<DnsResolutionError>(),
                Some(DnsResolutionError::PrivateAddress)
            ),
            "got {source:?}"
        );
    }

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)]
    async fn resolver_allows_loopback_when_not_enforced() {
        let resolver = MzAwsResolver {
            enforce_external_addresses: false,
        };
        let addrs = resolver
            .resolve_dns("127.0.0.1")
            .await
            .expect("loopback should resolve when enforcement is off");
        assert!(addrs.contains(&IpAddr::from([127, 0, 0, 1])));
    }

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)]
    async fn resolver_allows_public_when_enforced() {
        let resolver = MzAwsResolver {
            enforce_external_addresses: true,
        };
        let addrs = resolver
            .resolve_dns("8.8.8.8")
            .await
            .expect("public IP should resolve");
        assert!(addrs.contains(&IpAddr::from([8, 8, 8, 8])));
    }
}

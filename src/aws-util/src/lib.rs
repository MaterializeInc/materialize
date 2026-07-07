// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use aws_config::{BehaviorVersion, ConfigLoader};
use aws_smithy_http_client::{Builder, tls};
use aws_smithy_runtime_api::client::dns::{DnsFuture, ResolveDns, ResolveDnsError};
use aws_smithy_runtime_api::client::http::SharedHttpClient;

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
pub fn http_client() -> SharedHttpClient {
    // Company policy is to use rustls with the aws-lc-rs crypto provider so the
    // AWS SDK path fits the FIPS 140-3 crypto story, rather than the SDK's
    // default OS-native TLS stack. We use the non-FIPS `AwsLc` mode to match the
    // rest of the stack. A follow-up flips to the FIPS-validated provider once
    // the NIST certificate lands.
    Builder::new()
        .tls_provider(tls::Provider::Rustls(
            tls::rustls_provider::CryptoMode::AwsLc,
        ))
        .build_https()
}

/// Returns an AWS SDK HTTP client whose DNS resolver delegates to
/// [`mz_ore::netio::resolve_address`].
///
/// Only the IP resolution step is overridden. The SDK still uses the original
/// hostname for SNI and TLS certificate validation, so HTTPS endpoints work
/// unchanged.
pub fn http_client_with_resolver(enforce_external_addresses: bool) -> SharedHttpClient {
    Builder::new()
        .tls_provider(tls::Provider::Rustls(
            tls::rustls_provider::CryptoMode::AwsLc,
        ))
        .build_with_resolver(MzAwsResolver {
            enforce_external_addresses,
        })
}

/// A [`ResolveDns`] resolver that delegates to [`mz_ore::netio::resolve_address`],
/// used by [`http_client_with_resolver`].
///
/// Smithy applies the endpoint's port to the resolved IPs itself, so this only
/// yields bare [`IpAddr`](std::net::IpAddr)s.
#[derive(Debug, Clone)]
struct MzAwsResolver {
    enforce_external_addresses: bool,
}

impl ResolveDns for MzAwsResolver {
    fn resolve_dns<'a>(&'a self, name: &'a str) -> DnsFuture<'a> {
        let enforce = self.enforce_external_addresses;
        let host = name.to_string();
        DnsFuture::new(async move {
            let ips = mz_ore::netio::resolve_address(&host, enforce)
                .await
                .map_err(ResolveDnsError::new)?;
            Ok(ips.into_iter().collect())
        })
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error;
    use std::net::IpAddr;

    use mz_ore::netio::DnsResolutionError;

    use super::*;

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)]
    async fn resolver_rejects_loopback_when_enforced() {
        let resolver = MzAwsResolver {
            enforce_external_addresses: true,
        };
        let err = resolver
            .resolve_dns("127.0.0.1")
            .await
            .expect_err("must reject loopback");
        // Smithy wraps our `DnsResolutionError` in a `ResolveDnsError`, so peel
        // back the source to confirm the rejection reason is the private address.
        let source = err
            .source()
            .and_then(|src| src.downcast_ref::<DnsResolutionError>());
        assert!(
            matches!(source, Some(DnsResolutionError::PrivateAddress)),
            "got {err:?}"
        );
    }

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)]
    async fn resolver_allows_loopback_when_not_enforced() {
        let resolver = MzAwsResolver {
            enforce_external_addresses: false,
        };
        let addrs: Vec<IpAddr> = resolver
            .resolve_dns("127.0.0.1")
            .await
            .expect("loopback should resolve when enforcement is off");
        assert!(addrs.iter().any(|ip| *ip == IpAddr::from([127, 0, 0, 1])));
    }

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)]
    async fn resolver_allows_public_when_enforced() {
        let resolver = MzAwsResolver {
            enforce_external_addresses: true,
        };
        let addrs: Vec<IpAddr> = resolver
            .resolve_dns("8.8.8.8")
            .await
            .expect("public IP should resolve");
        assert!(addrs.iter().any(|ip| *ip == IpAddr::from([8, 8, 8, 8])));
    }
}

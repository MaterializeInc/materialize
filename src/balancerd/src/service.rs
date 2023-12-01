// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

#![allow(missing_docs)]

use std::net::SocketAddr;
use std::path::PathBuf;

use anyhow::Context;
use jsonwebtoken::DecodingKey;
use mz_balancerd::{BalancerConfig, BalancerService, FronteggResolver, Resolver, BUILD_INFO};
use mz_frontegg_auth::{Authentication, AuthenticationConfig};
use mz_ore::metrics::MetricsRegistry;
use mz_server_core::TlsCliArgs;
use tracing::info;

#[derive(Debug, clap::Parser)]
pub struct Args {
    #[clap(long, value_name = "HOST:PORT")]
    pgwire_listen_addr: SocketAddr,
    #[clap(long, value_name = "HOST:PORT")]
    https_listen_addr: SocketAddr,
    #[clap(flatten)]
    tls: TlsCliArgs,
    #[clap(long, value_name = "HOST:PORT")]
    internal_http_listen_addr: SocketAddr,

    /// Static pgwire resolver address to use for local testing.
    #[clap(long, value_name = "HOST:PORT")]
    static_resolver_addr: Option<String>,
    /// Frontegg resolver address template. `{}` is replaced with the user's frontegg tenant id to
    /// get a DNS address. The first IP that address resolves to is the proxy destinaiton.
    #[clap(long,
        value_name = "HOST.{}.NAME:PORT",
        requires_all = &["frontegg-api-token-url", "frontegg-admin-role"],
    )]
    frontegg_resolver_template: Option<String>,
    /// HTTPS resolver address template. `{}` is replaced with the first subdomain of the HTTPS SNI
    /// host address to get a DNS address. The first IP that address resolves to is the proxy
    /// destinaiton.
    #[clap(long, value_name = "HOST.{}.NAME:PORT")]
    https_resolver_template: String,

    /// JWK used to validate JWTs during Frontegg authentication as a PEM public
    /// key. Can optionally be base64 encoded with the URL-safe alphabet.
    #[clap(long, env = "FRONTEGG_JWK", requires = "frontegg-resolver-template")]
    frontegg_jwk: Option<String>,
    /// Path of JWK used to validate JWTs during Frontegg authentication as a PEM public key.
    #[clap(
        long,
        env = "FRONTEGG_JWK_FILE",
        requires = "frontegg-resolver-template"
    )]
    frontegg_jwk_file: Option<PathBuf>,
    /// The full URL (including path) to the Frontegg api-token endpoint.
    #[clap(
        long,
        env = "FRONTEGG_API_TOKEN_URL",
        requires = "frontegg-resolver-template"
    )]
    frontegg_api_token_url: Option<String>,
    /// The name of the admin role in Frontegg.
    #[clap(
        long,
        env = "FRONTEGG_ADMIN_ROLE",
        requires = "frontegg-resolver-template"
    )]
    frontegg_admin_role: Option<String>,
}

pub async fn run(args: Args) -> Result<(), anyhow::Error> {
    let metrics_registry = MetricsRegistry::new();
    let resolver = match (args.static_resolver_addr, args.frontegg_resolver_template) {
        (None, Some(addr_template)) => {
            let auth = Authentication::new(
                AuthenticationConfig {
                    admin_api_token_url: args.frontegg_api_token_url.expect("clap enforced"),
                    decoding_key: match (args.frontegg_jwk, args.frontegg_jwk_file) {
                        (None, Some(path)) => {
                            let jwk = std::fs::read(&path).with_context(|| {
                                format!("read {path:?} for --frontegg-jwk-file")
                            })?;
                            DecodingKey::from_rsa_pem(&jwk)?
                        }
                        (Some(jwk), None) => DecodingKey::from_rsa_pem(jwk.as_bytes())?,
                        _ => anyhow::bail!(
                            "exactly one of --frontegg-jwk or --frontegg-jwk-file must be present"
                        ),
                    },
                    tenant_id: None,
                    now: mz_ore::now::SYSTEM_TIME.clone(),
                    admin_role: args.frontegg_admin_role.expect("clap enforced"),
                },
                mz_frontegg_auth::Client::environmentd_default(),
                &metrics_registry,
            );
            Resolver::Frontegg(FronteggResolver {
                auth,
                addr_template,
            })
        }
        (Some(addr), None) => {
            // As a typo-check, verify that the passed address resolves to at least one IP. This
            // result isn't recorded anywhere: we re-resolve on each request in case DNS changes.
            // Here only to cause startup to crash if mis-typed.
            let mut addrs = tokio::net::lookup_host(&addr)
                .await
                .unwrap_or_else(|_| panic!("could not resolve {addr}"));
            let Some(_resolved) = addrs.next() else {
                panic!("{addr} did not resolve to any addresses");
            };
            drop(addrs);

            Resolver::Static(addr)
        }
        _ => anyhow::bail!(
            "exactly one of --static-resolver-addr or --frontegg-resolver-template must be present"
        ),
    };
    let config = BalancerConfig::new(
        &BUILD_INFO,
        args.internal_http_listen_addr,
        args.pgwire_listen_addr,
        args.https_listen_addr,
        resolver,
        args.https_resolver_template,
        args.tls.into_config()?,
        metrics_registry,
    );
    let service = BalancerService::new(config).await?;
    service.serve().await?;
    info!("balancer service safely exited");
    Ok(())
}

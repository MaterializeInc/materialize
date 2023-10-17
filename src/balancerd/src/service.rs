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

use jsonwebtoken::DecodingKey;
use mz_balancerd::{
    BalancerConfig, BalancerService, FronteggResolver, Metrics, Resolver, BUILD_INFO,
};
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

    /// Static pgwire resolver address to use for local testing.
    #[clap(long, value_name = "HOST:PORT")]
    static_resolver_addr: Option<SocketAddr>,
    /// Frontegg resolver address template. `{}` is replaced with the user's frontegg tenant id to
    /// get a DNS address. The first IP that address resolves to is the proxy destinaiton.
    #[clap(long,
        value_name = "HOST.{}.NAME:PORT",
        requires_all = &["frontegg-jwk", "frontegg-api-token-url", "frontegg-admin-role"],
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
                    decoding_key: DecodingKey::from_rsa_pem(
                        args.frontegg_jwk.expect("clap enforced").as_bytes(),
                    )?,
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
        (Some(addr), None) => Resolver::Static(addr),
        _ => anyhow::bail!(
            "exactly one of --static-resolver-addr or --frontegg-resolver-template must be present"
        ),
    };
    let config = BalancerConfig::new(
        &BUILD_INFO,
        args.pgwire_listen_addr,
        args.https_listen_addr,
        resolver,
        args.https_resolver_template,
        args.tls.into_config()?,
    );
    let metrics = Metrics::new(&config, &metrics_registry);
    let service = BalancerService::new(config, metrics);
    service.serve().await?;
    info!("balancer service safely exited");
    Ok(())
}

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

use mz_balancerd::{BalancerConfig, BalancerService, FronteggResolver, Resolver, BUILD_INFO};
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
}

pub async fn run(args: Args) -> Result<(), anyhow::Error> {
    let metrics_registry = MetricsRegistry::new();
    let resolver = match (args.static_resolver_addr, args.frontegg_resolver_template) {
        (None, Some(addr_template)) => Resolver::Frontegg(FronteggResolver { addr_template }),
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

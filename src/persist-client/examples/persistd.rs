// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::net::SocketAddr;

use mz_build_info::{build_info, BuildInfo};
use mz_ore::metrics::MetricsRegistry;
use mz_ore::now::SYSTEM_TIME;
use mz_persist_client::cache::PersistClientCache;
use mz_persist_client::rpc::Server;
use mz_persist_client::PersistConfig;
use tracing::info;

const BUILD_INFO: BuildInfo = build_info!();

/// An adaptor to Jepsen Maelstrom's txn-list-append workload
///
/// Example usage:
///
///     cargo build --example persistcli && java -jar /path/to/maelstrom.jar test -w txn-list-append --bin ./target/debug/examples/persistcli maelstrom
///
/// The [Maelstrom docs] are a great place to start for an understanding of
/// the specifics of what's going on here.
///
/// [maelstrom docs]: https://github.com/jepsen-io/maelstrom/tree/v0.2.1#documentation
#[derive(Debug, Clone, clap::Parser)]
pub struct Args {
    /// The address on which to listen for trusted RPC connections.
    #[clap(
        long,
        env = "LISTEN_ADDR",
        value_name = "HOST:PORT",
        default_value = "127.0.0.1:2100"
    )]
    listen_addr: SocketAddr,
    /// The address of the internal HTTP server.
    #[clap(
        long,
        env = "INTERNAL_HTTP_LISTEN_ADDR",
        value_name = "HOST:PORT",
        default_value = "127.0.0.1:6879"
    )]
    internal_http_listen_addr: SocketAddr,

    /// Where the persist library should store its blob data.
    #[clap(long, env = "PERSIST_BLOB_URL")]
    persist_blob_url: String,
}

pub async fn run(args: Args) -> Result<(), anyhow::Error> {
    let metrics_registry = MetricsRegistry::new();
    {
        let metrics_registry = metrics_registry.clone();
        info!(
            "serving internal HTTP server on {}",
            args.internal_http_listen_addr
        );
        mz_ore::task::spawn(
            || "http_server",
            axum::Server::bind(&args.internal_http_listen_addr).serve(
                axum::Router::new()
                    .route(
                        "/metrics",
                        axum::routing::get(move || async move {
                            mz_http_util::handle_prometheus(&metrics_registry).await
                        }),
                    )
                    .into_make_service(),
            ),
        );
    }

    let mut persist_clients = PersistClientCache::new(
        PersistConfig::new(&BUILD_INFO, SYSTEM_TIME.clone()),
        &metrics_registry,
    );
    info!("serving RPC traffic on {:?}", args.listen_addr);
    Server::new(&mut persist_clients, args.persist_blob_url)
        .await
        .serve(args.listen_addr)
        .await
}

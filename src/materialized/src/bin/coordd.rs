// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::process;
use std::sync::Arc;
use std::time::Duration;

use log::info;
use structopt::StructOpt;
use tokio::net::TcpListener;
use tracing_subscriber::EnvFilter;

use ore::metrics::MetricsRegistry;
use ore::now::SYSTEM_TIME;

/// Independent coordinator server for Materialize.
#[derive(StructOpt)]
struct Args {
    /// The address on which to listen for SQL connections.
    #[structopt(
        long,
        env = "COORDD_LISTEN_ADDR",
        value_name = "HOST:PORT",
        default_value = "0.0.0.0:6875"
    )]
    listen_addr: SocketAddr,
    /// The address of the dataflowd server to connect to.
    #[structopt(
        short,
        long,
        env = "COORDD_DATAFLOWD_ADDR",
        default_value = "127.0.0.1:6876"
    )]
    dataflowd_addr: SocketAddr,
    /// Number of dataflow worker threads. This must match the number of
    /// workers that the targeted dataflowd was started with.
    #[structopt(
        short,
        long,
        env = "COORDD_DATAFLOWD_WORKERS",
        value_name = "N",
        default_value = "1"
    )]
    workers: usize,
    /// Where to store data.
    #[structopt(
        short = "D",
        long,
        env = "COORDD_DATA_DIRECTORY",
        value_name = "PATH",
        default_value = "mzdata"
    )]
    data_directory: PathBuf,
}

#[tokio::main]
async fn main() {
    if let Err(err) = run(ore::cli::parse_args()).await {
        eprintln!("coordd: {:#}", err);
        process::exit(1);
    }
}

async fn run(args: Args) -> Result<(), anyhow::Error> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_env("COORDD_LOG_FILTER").unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    info!(
        "connecting to dataflowd server at {}...",
        args.dataflowd_addr
    );

    let dataflow_client =
        dataflowd::RemoteClient::connect(args.workers, args.dataflowd_addr).await?;
    let metrics_registry = MetricsRegistry::new();
    let (_coord_handle, coord_client) = coord::serve(coord::Config {
        dataflow_client,
        symbiosis_url: None,
        logging: None,
        data_directory: &args.data_directory,
        timestamp_frequency: Duration::from_secs(1),
        logical_compaction_window: Some(Duration::from_millis(1)),
        experimental_mode: false,
        disable_user_indexes: false,
        safe_mode: false,
        build_info: &materialized::BUILD_INFO,
        metrics_registry: metrics_registry.clone(),
        persist: coord::PersistConfig::disabled(),
        now: SYSTEM_TIME.clone(),
    })
    .await?;

    let listener = TcpListener::bind(&args.listen_addr).await?;
    let pgwire_server = Arc::new(pgwire::Server::new(pgwire::Config {
        tls: None,
        coord_client,
        metrics_registry: &metrics_registry,
    }));

    info!(
        "listening for pgwire connections on {}...",
        listener.local_addr()?
    );

    loop {
        let (conn, _addr) = listener.accept().await?;
        tokio::spawn({
            let pgwire_server = pgwire_server.clone();
            async move { pgwire_server.handle_connection(conn).await }
        });
    }
}

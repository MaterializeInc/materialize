// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use futures::StreamExt;
use std::path::PathBuf;
use std::process;
use std::time::Duration;

use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tracing::info;
use tracing_subscriber::EnvFilter;

use materialized::http;
use materialized::mux::Mux;
use materialized::server_metrics::Metrics;
use ore::metrics::MetricsRegistry;
use ore::now::SYSTEM_TIME;

/// Independent coordinator server for Materialize.
#[derive(clap::Parser)]
struct Args {
    /// The address on which to listen for SQL connections.
    #[clap(
        long,
        env = "COORDD_LISTEN_ADDR",
        value_name = "HOST:PORT",
        default_value = "0.0.0.0:6875"
    )]
    listen_addr: String,
    /// The address of the dataflowd servers to connect to.
    #[clap()]
    dataflowd_addr: Vec<String>,
    /// Number of dataflow worker threads. This must match the number of
    /// workers that the targeted dataflowd was started with.
    #[clap(
        short,
        long,
        env = "COORDD_DATAFLOWD_WORKERS",
        value_name = "N",
        default_value = "1"
    )]
    workers: usize,
    /// Where to store data.
    #[clap(
        short = 'D',
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
        "connecting to dataflowd server at {:?}...",
        args.dataflowd_addr
    );

    let dataflow_client = dataflowd::RemoteClient::connect(&args.dataflowd_addr).await?;

    let mut metrics_registry = MetricsRegistry::new();
    let (coord_handle, coord_client) = coord::serve(coord::Config {
        dataflow_client,
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

    let metrics = Metrics::register_with(
        &mut metrics_registry,
        args.workers,
        coord_handle.start_instant(),
    );

    let listener = TcpListener::bind(&args.listen_addr).await?;

    tokio::spawn({
        let pgwire_server = pgwire::Server::new(pgwire::Config {
            tls: None,
            coord_client: coord_client.clone(),
            metrics_registry: &metrics_registry,
        });
        let http_server = http::Server::new(http::Config {
            tls: None,
            coord_client,
            metrics_registry,
            global_metrics: metrics,
            pgwire_metrics: pgwire_server.metrics(),
        });
        let mut mux = Mux::new();
        mux.add_handler(pgwire_server);
        mux.add_handler(http_server);

        info!(
            "listening for pgwire connections on {}...",
            listener.local_addr()?
        );

        async move {
            let mut incoming = TcpListenerStream::new(listener);
            mux.serve(incoming.by_ref()).await;
        }
    });
    Ok(())
}

// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::path::PathBuf;
use std::process;
use std::time::Duration;

use futures::StreamExt;
use mz_dataflow_types::sources::AwsExternalId;
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tracing::info;
use tracing_subscriber::EnvFilter;
use uuid::Uuid;

use materialized::http;
use materialized::mux::Mux;
use materialized::server_metrics::Metrics;
use mz_coord::LoggingConfig;
use mz_dataflow_types::client::Client;
use mz_dataflowd::SplitClient;
use mz_ore::metrics::MetricsRegistry;
use mz_ore::now::SYSTEM_TIME;
use mz_ore::task;

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
    /// The address of the storage dataflowd servers to connect to.
    #[clap(long)]
    storaged_addr: Vec<String>,
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

    /// An external ID to use for all AWS AssumeRole operations.
    #[clap(long, value_name = "ID")]
    aws_external_id: Option<String>,
}

#[tokio::main]
async fn main() {
    if let Err(err) = run(mz_ore::cli::parse_args()).await {
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

    let dataflow_client: Box<dyn Client + Send + 'static> = if args.storaged_addr.is_empty() {
        info!(
            "connecting to dataflowd server at {:?}...",
            args.dataflowd_addr
        );
        Box::new(mz_dataflowd::RemoteClient::connect(&args.dataflowd_addr).await?)
    } else {
        info!(
            "connecting to dataflowd server at {:?} and storaged server at {:?}...",
            args.dataflowd_addr, args.storaged_addr
        );
        let compute_client = mz_dataflowd::RemoteClient::connect(&args.dataflowd_addr).await?;
        let storage_client = mz_dataflowd::RemoteClient::connect(&args.storaged_addr).await?;
        Box::new(SplitClient::new(storage_client, compute_client))
    };

    let experimental_mode = false;
    let mut metrics_registry = MetricsRegistry::new();
    let coord_storage = mz_coord::catalog::storage::Connection::open(
        &args.data_directory.join("catalog"),
        Some(experimental_mode),
    )?;
    let persister = mz_coord::PersistConfig::disabled()
        .init(
            // TODO(benesch): if we were enabling persistence, we'd want to use
            // a stable reentrance ID here. Using a random UUID essentially
            // amounts to "no reentrance allowed", which provides maximum
            // safety.
            Uuid::new_v4(),
            materialized::BUILD_INFO,
            &metrics_registry,
        )
        .await?;
    // TODO: expose as a parameter
    let granularity = Duration::from_secs(1);
    let (coord_handle, coord_client) = mz_coord::serve(mz_coord::Config {
        dataflow_client,
        logging: Some(LoggingConfig {
            log_logging: false,
            granularity,
            retain_readings_for: granularity,
            metrics_scraping_interval: Some(granularity),
        }),
        storage: coord_storage,
        timestamp_frequency: Duration::from_secs(1),
        logical_compaction_window: Some(Duration::from_millis(1)),
        experimental_mode,
        disable_user_indexes: false,
        safe_mode: false,
        build_info: &materialized::BUILD_INFO,
        aws_external_id: args
            .aws_external_id
            .map(AwsExternalId::ISwearThisCameFromACliArgOrEnvVariable)
            .unwrap_or(AwsExternalId::NotProvided),
        metrics_registry: metrics_registry.clone(),
        persister,
        now: SYSTEM_TIME.clone(),
    })
    .await?;

    let metrics = Metrics::register_with(
        &mut metrics_registry,
        args.workers,
        coord_handle.start_instant(),
    );

    let listener = TcpListener::bind(&args.listen_addr).await?;

    task::spawn(|| "pgwire_server", {
        let pgwire_server = mz_pgwire::Server::new(mz_pgwire::Config {
            tls: None,
            coord_client: coord_client.clone(),
            metrics_registry: &metrics_registry,
            frontegg: None,
        });
        let http_server = http::Server::new(http::Config {
            tls: None,
            frontegg: None,
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

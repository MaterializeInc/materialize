// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::env;
use std::fmt;
use std::path::PathBuf;
use std::process;
use std::sync::{Arc, Mutex};

use anyhow::bail;
use futures::sink::SinkExt;
use futures::stream::TryStreamExt;
use http::HeaderMap;
use serde::de::DeserializeOwned;
use serde::ser::Serialize;
use tokio::net::TcpListener;
use tokio::select;
use tracing::info;
use tracing_subscriber::filter::Targets;

use mz_build_info::{build_info, BuildInfo};
use mz_dataflow_types::client::{GenericClient, StorageClient};
use mz_dataflow_types::ConnectorContext;
use mz_ore::metrics::MetricsRegistry;
use mz_ore::now::SYSTEM_TIME;
use mz_pid_file::PidFile;
use mz_storage::Server;

// Disable jemalloc on macOS, as it is not well supported [0][1][2].
// The issues present as runaway latency on load test workloads that are
// comfortably handled by the macOS system allocator. Consider re-evaluating if
// jemalloc's macOS support improves.
//
// [0]: https://github.com/jemalloc/jemalloc/issues/26
// [1]: https://github.com/jemalloc/jemalloc/issues/843
// [2]: https://github.com/jemalloc/jemalloc/issues/1467
#[cfg(all(not(target_os = "macos"), feature = "jemalloc"))]
#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

const BUILD_INFO: BuildInfo = build_info!();

/// Independent storage server for Materialize.
#[derive(clap::Parser)]
struct Args {
    /// The address on which to listen for a connection from the controller.
    #[clap(
        long,
        env = "STORAGED_LISTEN_ADDR",
        value_name = "HOST:PORT",
        default_value = "127.0.0.1:2100"
    )]
    listen_addr: String,
    /// Number of dataflow worker threads.
    #[clap(
        short,
        long,
        env = "STORAGED_WORKERS",
        value_name = "W",
        default_value = "1"
    )]
    workers: usize,
    /// The hostnames of all storaged processes in the cluster.
    #[clap()]
    hosts: Vec<String>,

    /// An external ID to be supplied to all AWS AssumeRole operations.
    ///
    /// Details: <https://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles_create_for-user_externalid.html>
    #[clap(long, value_name = "ID")]
    aws_external_id: Option<String>,
    /// The address of the storage server to bind or connect to.
    #[clap(
        long,
        env = "STORAGED_STORAGE_ADDR",
        value_name = "HOST:PORT",
        default_value = "127.0.0.1:2101"
    )]
    storage_addr: String,
    /// Whether or not process should die when connection with ADAPTER is lost.
    #[clap(long)]
    linger: bool,
    /// Enable command reconciliation.
    #[clap(long, requires = "linger")]
    reconcile: bool,

    /// The address of the HTTP profiling UI.
    #[clap(long, value_name = "HOST:PORT")]
    http_console_addr: Option<String>,

    /// Where to write a pid lock file. Should only be used for local process orchestrators.
    #[clap(long, value_name = "PATH")]
    pid_file_location: Option<PathBuf>,

    // === Logging options. ===
    /// Which log messages to emit. See `materialized`'s help for more
    /// info.
    ///
    /// The default value for this option is "info".
    #[clap(
        long,
        env = "STORAGED_LOG_FILTER",
        value_name = "FILTER",
        default_value = "info"
    )]
    log_filter: Targets,

    /// Add the process name to the tracing logs
    #[clap(long, hide = true)]
    log_process_name: bool,
}

#[tokio::main]
async fn main() {
    if let Err(err) = run(mz_ore::cli::parse_args()).await {
        eprintln!("storaged: fatal: {:#}", err);
        process::exit(1);
    }
}

fn create_communication_config(args: &Args) -> Result<timely::CommunicationConfig, anyhow::Error> {
    let threads = args.workers;
    if threads > 1 {
        Ok(timely::CommunicationConfig::Process(threads))
    } else {
        Ok(timely::CommunicationConfig::Thread)
    }
}

fn create_timely_config(args: &Args) -> Result<timely::Config, anyhow::Error> {
    Ok(timely::Config {
        worker: timely::WorkerConfig::default(),
        communication: create_communication_config(args)?,
    })
}

async fn run(args: Args) -> Result<(), anyhow::Error> {
    mz_ore::tracing::configure(mz_ore::tracing::TracingConfig {
        log_filter: args.log_filter.clone(),
        opentelemetry_endpoint: None,
        opentelemetry_headers: HeaderMap::new(),
        prefix: args.log_process_name.then(|| "storaged".into()),
        #[cfg(feature = "tokio-console")]
        tokio_console: false,
    })
    .await?;

    if args.workers == 0 {
        bail!("--workers must be greater than 0");
    }
    let timely_config = create_timely_config(&args)?;

    info!("about to bind to {:?}", args.listen_addr);
    let listener = TcpListener::bind(args.listen_addr).await?;

    info!(
        "listening for coordinator connection on {}...",
        listener.local_addr()?
    );

    if let Some(addr) = args.http_console_addr {
        tracing::info!("serving storaged HTTP server on {}", addr);
        mz_ore::task::spawn(
            || "storaged_http_server",
            axum::Server::bind(&addr.parse()?)
                .serve(mz_prof::http::router(&BUILD_INFO).into_make_service()),
        );
    }

    let config = mz_storage::Config {
        workers: args.workers,
        timely_config,
        experimental_mode: false,
        metrics_registry: MetricsRegistry::new(),
        now: SYSTEM_TIME.clone(),
        connector_context: ConnectorContext::from_cli_args(&args.log_filter, args.aws_external_id),
    };

    let serve_config = ServeConfig {
        listener,
        linger: args.linger,
    };

    assert!(
        !args.reconcile,
        "Storage runtime does not support command reconciliation."
    );
    let workers = config.workers;
    let (storage_server, request_rx, _thread) =
        mz_storage::tcp_boundary::server::serve(args.storage_addr, workers).await?;
    let boundary = (0..workers)
        .into_iter()
        .map(|_| Some(storage_server.clone()))
        .collect::<Vec<_>>();
    let boundary = Arc::new(Mutex::new(boundary));
    let (server, client) = mz_storage::serve_boundary_requests(config, request_rx, move |index| {
        boundary.lock().unwrap()[index % workers].take().unwrap()
    })?;
    let client: Box<dyn StorageClient> = Box::new(client);

    let mut _pid_file = None;
    if let Some(pid_file_location) = &args.pid_file_location {
        _pid_file = Some(PidFile::open(&pid_file_location).unwrap());
    }

    serve(serve_config, server, client).await
}

struct ServeConfig {
    listener: TcpListener,
    linger: bool,
}

async fn serve<G, C, R>(
    config: ServeConfig,
    _server: Server,
    mut client: G,
) -> Result<(), anyhow::Error>
where
    G: GenericClient<C, R>,
    C: DeserializeOwned + fmt::Debug + Send + Unpin,
    R: Serialize + fmt::Debug + Send + Unpin,
{
    loop {
        let (conn, _addr) = config.listener.accept().await?;
        info!("coordinator connection accepted");

        let mut conn = mz_dataflow_types::client::tcp::framed_server(conn);
        loop {
            select! {
                cmd = conn.try_next() => match cmd? {
                    None => break,
                    Some(cmd) => { client.send(cmd).await.unwrap(); },
                },
                res = client.recv() => {
                    match res.unwrap() {
                        None => break,
                        Some(response) => { conn.send(response).await?; }
                    }
                }
            }
        }
        if !config.linger {
            break;
        } else {
            info!("coordinator connection gone; lingering");
        }
    }

    info!("coordinator connection gone; terminating");
    Ok(())
}

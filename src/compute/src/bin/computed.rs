// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fmt;
use std::process;
use std::sync::{Arc, Mutex};

use anyhow::bail;
use futures::sink::SinkExt;
use futures::stream::TryStreamExt;
use mz_dataflow_types::sources::AwsExternalId;
use serde::de::DeserializeOwned;
use serde::ser::Serialize;
use tokio::net::TcpListener;
use tokio::select;
use tracing::info;
use tracing_subscriber::EnvFilter;

use mz_dataflow_types::client::{ComputeClient, GenericClient};
use mz_dataflow_types::reconciliation::command::ComputeCommandReconcile;
use mz_ore::metrics::MetricsRegistry;
use mz_ore::now::SYSTEM_TIME;

use mz_compute::server::Server;

// Disable jemalloc on macOS, as it is not well supported [0][1][2].
// The issues present as runaway latency on load test workloads that are
// comfortably handled by the macOS system allocator. Consider re-evaluating if
// jemalloc's macOS support improves.
//
// [0]: https://github.com/jemalloc/jemalloc/issues/26
// [1]: https://github.com/jemalloc/jemalloc/issues/843
// [2]: https://github.com/jemalloc/jemalloc/issues/1467
#[cfg(not(target_os = "macos"))]
#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[derive(clap::ArgEnum, Debug, Copy, Clone, Ord, PartialOrd, Eq, PartialEq)]
enum RuntimeType {
    /// Host only the compute portion of the dataflow.
    Compute,
    /// Host only the storage portion of the dataflow.
    Storage,
}

/// Independent dataflow server for Materialize.
#[derive(clap::Parser)]
struct Args {
    /// The address on which to listen for a connection from the controller.
    #[clap(
        long,
        env = "DATAFLOWD_LISTEN_ADDR",
        value_name = "HOST:PORT",
        default_value = "127.0.0.1:2100"
    )]
    listen_addr: String,
    /// Number of dataflow worker threads.
    #[clap(
        short,
        long,
        env = "DATAFLOWD_WORKERS",
        value_name = "W",
        default_value = "1"
    )]
    workers: usize,
    /// Number of this dataflowd process.
    #[clap(
        short = 'p',
        long,
        env = "DATAFLOWD_PROCESS",
        value_name = "P",
        default_value = "0"
    )]
    process: usize,
    /// Total number of dataflowd processes.
    #[clap(
        short = 'n',
        long,
        env = "DATAFLOWD_PROCESSES",
        value_name = "N",
        default_value = "1"
    )]
    processes: usize,
    /// The hostnames of all dataflowd processes in the cluster.
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
        env = "DATAFLOWD_STORAGE_ADDR",
        value_name = "HOST:PORT",
        default_value = "127.0.0.1:2101"
    )]
    storage_addr: String,
    #[clap(long)]
    linger: bool,
    /// Enable command reconciliation.
    #[clap(long, requires = "linger")]
    reconcile: bool,
}

#[tokio::main]
async fn main() {
    if let Err(err) = run(mz_ore::cli::parse_args()).await {
        eprintln!("dataflowd: {:#}", err);
        process::exit(1);
    }
}

fn create_communication_config(args: &Args) -> Result<timely::CommunicationConfig, anyhow::Error> {
    let threads = args.workers;
    let process = args.process;
    let processes = args.processes;
    let report = true;

    if processes > 1 {
        let mut addresses = Vec::new();
        if args.hosts.is_empty() {
            for index in 0..processes {
                addresses.push(format!("localhost:{}", 2102 + index));
            }
        } else {
            if let Ok(file) = ::std::fs::File::open(args.hosts[0].clone()) {
                let reader = ::std::io::BufReader::new(file);
                use ::std::io::BufRead;
                for line in reader.lines().take(processes) {
                    addresses.push(line?);
                }
            } else {
                addresses.extend(args.hosts.iter().cloned());
            }
            if addresses.len() < processes {
                bail!(
                    "could only read {} addresses from {:?}, but -n: {}",
                    addresses.len(),
                    args.hosts,
                    processes
                );
            }
        }

        assert_eq!(processes, addresses.len());
        Ok(timely::CommunicationConfig::Cluster {
            threads,
            process,
            addresses,
            report,
            log_fn: Box::new(|_| None),
        })
    } else if threads > 1 {
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
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_env("DATAFLOWD_LOG_FILTER")
                .unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

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

    let config = mz_compute::server::Config {
        workers: args.workers,
        timely_config,
        experimental_mode: false,
        metrics_registry: MetricsRegistry::new(),
        now: SYSTEM_TIME.clone(),
        persister: None,
        aws_external_id: args
            .aws_external_id
            .map(AwsExternalId::ISwearThisCameFromACliArgOrEnvVariable)
            .unwrap_or(AwsExternalId::NotProvided),
    };

    let serve_config = ServeConfig {
        listener,
        linger: args.linger,
    };

    let (storage_client, _thread) =
        mz_storage::tcp_boundary::client::connect(args.storage_addr, config.workers).await?;
    let boundary = (0..config.workers)
        .into_iter()
        .map(|_| Some(storage_client.clone()))
        .collect::<Vec<_>>();
    let boundary = Arc::new(Mutex::new(boundary));
    let workers = config.workers;
    let (server, client) = mz_compute::server::serve_boundary(config, move |index| {
        boundary.lock().unwrap()[index % workers].take().unwrap()
    })?;
    let mut client: Box<dyn ComputeClient> = Box::new(client);
    if args.reconcile {
        client = Box::new(ComputeCommandReconcile::new(client))
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

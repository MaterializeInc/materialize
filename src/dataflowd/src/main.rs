// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::net::SocketAddr;
use std::process;

use anyhow::bail;
use futures::sink::SinkExt;
use futures::stream::TryStreamExt;
use log::info;
use structopt::StructOpt;
use tokio::net::TcpListener;
use tokio::select;
use tracing_subscriber::EnvFilter;

use dataflow::Client;
use ore::metrics::MetricsRegistry;
use ore::now::SYSTEM_TIME;

/// Independent dataflow server for Materialize.
#[derive(StructOpt)]
struct Args {
    /// The address on which to listen for a connection from the coordinator.
    #[structopt(
        long,
        env = "DATAFLOWD_LISTEN_ADDR",
        value_name = "HOST:PORT",
        default_value = "0.0.0.0:6876"
    )]
    listen_addr: SocketAddr,
    /// Number of dataflow worker threads.
    #[structopt(
        short,
        long,
        env = "DATAFLOWD_WORKERS",
        value_name = "W",
        default_value = "1"
    )]
    workers: usize,
    /// Number of this dataflow process
    #[structopt(
        short = "p",
        long,
        env = "DATAFLOWD_PROCESS",
        value_name = "P",
        default_value = "0"
    )]
    process: usize,
    /// Number of dataflow processes.
    #[structopt(
        short = "n",
        long,
        env = "DATAFLOWD_PROCESSES",
        value_name = "N",
        default_value = "1"
    )]
    processes: usize,
    /// Dataflowd hosts
    #[structopt(short, long, env = "DATAFLOWD_HOSTS", value_name = "H")]
    hosts: Option<String>,
}

#[tokio::main]
async fn main() {
    if let Err(err) = run(ore::cli::parse_args()).await {
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
        if let Some(hosts) = &args.hosts {
            let file = ::std::fs::File::open(hosts.clone())?;
            let reader = ::std::io::BufReader::new(file);
            use ::std::io::BufRead;
            for line in reader.lines().take(processes) {
                addresses.push(line?);
            }
            if addresses.len() < processes {
                bail!(
                    "could only read {} addresses from {}, but -n: {}",
                    addresses.len(),
                    hosts,
                    processes
                );
            }
        } else {
            for index in 0..processes {
                addresses.push(format!("localhost:{}", 2101 + index));
            }
        }

        assert!(processes == addresses.len());
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

    let (_server, mut client) = dataflow::serve(dataflow::Config {
        workers: args.workers,
        timely_config,
        experimental_mode: false,
        metrics_registry: MetricsRegistry::new(),
        now: SYSTEM_TIME.clone(),
    })?;

    info!("about to bind to args.listen_addr");
    let listener = TcpListener::bind(args.listen_addr).await?;

    info!(
        "listening for coordinator connection on {}...",
        listener.local_addr()?
    );

    let (conn, _addr) = listener.accept().await?;
    info!("coordinator connection accepted");

    let mut conn = dataflowd::framed_server(conn);
    loop {
        select! {
            cmd = conn.try_next() => match cmd? {
                None => break,
                Some(cmd) => client.send(cmd).await,
            },
            Some(response) = client.recv() => conn.send(response).await?,
        }
    }

    info!("coordinator connection gone; terminating");
    Ok(())
}

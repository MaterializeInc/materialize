// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file at the root of this repository.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! `mz-bogo-consensus`: a fast, in-memory persist Consensus backend served
//! over gRPC.
//!
//! Pair with `--persist-consensus-url=bogo://host:port` on environmentd (or
//! testdrive) to bypass CRDB/Postgres entirely during performance testing.

use std::net::SocketAddr;
use std::sync::Arc;

use axum::Router;
use axum::extract::State;
use axum::response::IntoResponse;
use axum::routing;
use mz_bogo_consensus::metrics::BogoMetrics;
use mz_bogo_consensus::proto::bogo_consensus_server::BogoConsensusServer as BogoGrpcServer;
use mz_bogo_consensus::server::BogoConsensusServer;
use mz_ore::cli::{self, CliConfig};
use mz_ore::error::ErrorExt;
use mz_ore::metrics::MetricsRegistry;
use tokio::net::TcpListener;
use tokio::signal;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;
use tracing::info;

/// Run an in-memory persist Consensus backend over gRPC.
#[derive(Debug, clap::Parser)]
#[clap(name = "mz-bogo-consensus")]
struct Args {
    /// The address on which to listen for gRPC requests.
    #[clap(
        long,
        env = "BOGO_CONSENSUS_LISTEN_ADDR",
        value_name = "HOST:PORT",
        default_value = "127.0.0.1:6882"
    )]
    listen_addr: SocketAddr,

    /// The address on which to listen for Prometheus scrapes at `/metrics`.
    /// If unset, no metrics endpoint is started.
    #[clap(
        long,
        env = "BOGO_CONSENSUS_METRICS_LISTEN_ADDR",
        value_name = "HOST:PORT"
    )]
    metrics_listen_addr: Option<SocketAddr>,

    /// Tracing filter directive (passed to `EnvFilter`).
    #[clap(long, env = "BOGO_CONSENSUS_LOG_FILTER", default_value = "info")]
    log_filter: String,
}

#[tokio::main]
async fn main() {
    let args: Args = cli::parse_args(CliConfig {
        env_prefix: Some("MZ_BOGO_CONSENSUS_"),
        enable_version_flag: false,
    });

    let env_filter = tracing_subscriber::EnvFilter::try_new(&args.log_filter)
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info"));
    tracing_subscriber::fmt()
        .with_ansi(false)
        .with_env_filter(env_filter)
        .init();

    if let Err(err) = run(args).await {
        eprintln!("mz-bogo-consensus: fatal: {}", err.display_with_causes());
        std::process::exit(1);
    }
}

async fn run(args: Args) -> Result<(), anyhow::Error> {
    let registry = MetricsRegistry::new();
    let metrics = Arc::new(BogoMetrics::new(&registry));

    if let Some(metrics_addr) = args.metrics_listen_addr {
        spawn_metrics_server(metrics_addr, registry.clone()).await?;
    }

    let server = BogoConsensusServer::new(Arc::clone(&metrics));
    let grpc = BogoGrpcServer::new(server);

    let listener = TcpListener::bind(args.listen_addr).await?;
    let local_addr = listener.local_addr()?;
    info!(addr = %local_addr, "bogo-consensus gRPC server listening");
    let incoming = TcpListenerStream::new(listener);

    Server::builder()
        .add_service(grpc)
        .serve_with_incoming_shutdown(incoming, shutdown_signal())
        .await?;

    info!("bogo-consensus gRPC server shut down");
    Ok(())
}

async fn spawn_metrics_server(
    addr: SocketAddr,
    registry: MetricsRegistry,
) -> Result<(), anyhow::Error> {
    let listener = TcpListener::bind(addr).await?;
    let local_addr = listener.local_addr()?;
    info!(addr = %local_addr, "bogo-consensus metrics server listening");

    let app = Router::new()
        .route("/metrics", routing::get(prometheus_handler))
        .route("/api/livez", routing::get(|| async { "ok" }))
        .with_state(registry);

    mz_ore::task::spawn(|| "bogo_consensus::metrics_http_server", async move {
        if let Err(e) = axum::serve(listener, app.into_make_service()).await {
            tracing::error!(error = %e, "metrics server exited");
        }
    });
    Ok(())
}

async fn prometheus_handler(State(registry): State<MetricsRegistry>) -> impl IntoResponse {
    mz_http_util::handle_prometheus(&registry).await
}

async fn shutdown_signal() {
    let ctrl_c = async {
        let _ = signal::ctrl_c().await;
    };
    #[cfg(unix)]
    let terminate = async {
        match signal::unix::signal(signal::unix::SignalKind::terminate()) {
            Ok(mut s) => {
                s.recv().await;
            }
            Err(e) => {
                tracing::warn!(error = %e, "failed to install SIGTERM handler");
                std::future::pending::<()>().await;
            }
        }
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => info!("received Ctrl-C, shutting down"),
        _ = terminate => info!("received SIGTERM, shutting down"),
    }
}

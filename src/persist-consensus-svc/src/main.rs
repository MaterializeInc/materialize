// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Binary entry point for the group commit consensus service.

use std::net::SocketAddr;

use clap::Parser;
use tonic::transport::Server;
use tracing::info;

use mz_persist::generated::consensus_service::consensus_service_server::ConsensusServiceServer;
use mz_persist_consensus_svc::actor::{Actor, ActorCommand, ActorHandle};
use mz_persist_consensus_svc::metrics::ConsensusMetrics;
use mz_persist_consensus_svc::service::ConsensusGrpcService;
use mz_persist_consensus_svc::wal::s3::S3WalWriter;

/// CLI arguments for the consensus service.
#[derive(Parser, Debug)]
#[command(name = "mz-persist-consensus-svc")]
struct Args {
    /// Address to listen on for gRPC connections.
    #[arg(long, default_value = "0.0.0.0:6890")]
    listen_addr: SocketAddr,

    /// Address to listen on for the HTTP metrics endpoint (/metrics).
    #[arg(long, default_value = "0.0.0.0:6891")]
    metrics_listen_addr: SocketAddr,

    /// S3 bucket for WAL and snapshot storage.
    #[arg(long, env = "CONSENSUS_S3_BUCKET")]
    s3_bucket: String,

    /// S3 key prefix for WAL and snapshot objects.
    #[arg(long, env = "CONSENSUS_S3_PREFIX", default_value = "consensus/")]
    s3_prefix: String,

    /// S3 endpoint override (for LocalStack/MinIO).
    #[arg(long, env = "CONSENSUS_S3_ENDPOINT")]
    s3_endpoint: Option<String>,

    /// S3 region.
    #[arg(long, env = "CONSENSUS_S3_REGION", default_value = "us-east-1")]
    s3_region: String,

    /// Flush collection window in milliseconds. This is the time spent
    /// accumulating CAS ops between S3 writes, not the total period.
    #[arg(long, default_value = "5")]
    flush_interval_ms: u64,

    /// Write a snapshot every this many WAL batches.
    #[arg(long, default_value = "100")]
    snapshot_interval: u64,
}

fn main() {
    let args = Args::parse();

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    // Single-threaded runtime: simplest possible concurrency model.
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("failed to build tokio runtime");

    let local = tokio::task::LocalSet::new();
    local.block_on(&rt, run(args));
}

async fn run(args: Args) {
    let metrics_registry = mz_ore::metrics::MetricsRegistry::new();
    let metrics = ConsensusMetrics::register(&metrics_registry);

    let wal_writer = S3WalWriter::new(
        &args.s3_bucket,
        &args.s3_prefix,
        args.s3_endpoint.as_deref(),
        &args.s3_region,
    )
    .await;

    let (tx, rx) = tokio::sync::mpsc::channel::<ActorCommand>(4096);
    let handle = ActorHandle::new(tx);

    let actor = Actor::new(
        rx,
        wal_writer,
        args.flush_interval_ms,
        args.snapshot_interval,
        metrics,
    );
    tokio::task::spawn_local(actor.run());

    // Recover state from S3 via the actor's command channel.
    info!("recovering state from S3...");
    handle.recover().await.expect("recovery failed");

    // Spawn HTTP metrics server (Send-compatible, uses regular tokio::spawn).
    let metrics_addr = args.metrics_listen_addr;
    tokio::spawn(async move {
        let app = axum::Router::new().route(
            "/metrics",
            axum::routing::get(move || {
                let reg = metrics_registry.clone();
                async move { mz_http_util::handle_prometheus(&reg).await }
            }),
        );
        let listener = tokio::net::TcpListener::bind(metrics_addr)
            .await
            .expect("failed to bind metrics listener");
        info!(addr = %metrics_addr, "starting metrics HTTP server");
        axum::serve(listener, app)
            .await
            .expect("metrics server failed");
    });

    let grpc_service = ConsensusGrpcService { handle };
    info!(addr = %args.listen_addr, "starting gRPC server");

    Server::builder()
        .add_service(ConsensusServiceServer::new(grpc_service))
        .serve(args.listen_addr)
        .await
        .expect("gRPC server failed");
}

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
use std::sync::Arc;

use clap::Parser;
use tonic::transport::Server;
use tracing::info;

use mz_persist::generated::consensus_service::consensus_acceptor_server::ConsensusAcceptorServer;
use mz_persist::generated::consensus_service::consensus_learner_server::ConsensusLearnerServer;
use mz_persist_consensus_svc::acceptor::{Acceptor, AcceptorConfig};
use mz_persist_consensus_svc::learner::{Learner, LearnerConfig};
use mz_persist_consensus_svc::metrics::{AcceptorMetrics, LearnerMetrics};
use mz_persist_consensus_svc::service::{AcceptorGrpcService, LearnerGrpcService};
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

    /// Flush collection window in milliseconds.
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

    // Multi-threaded runtime for gRPC + metrics. The acceptor gets its own
    // dedicated single-threaded runtime on a separate OS thread.
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("failed to build tokio runtime");

    rt.block_on(run(args));
}

async fn run(args: Args) {
    let metrics_registry = mz_ore::metrics::MetricsRegistry::new();
    let acceptor_metrics = AcceptorMetrics::register(&metrics_registry);
    let learner_metrics = LearnerMetrics::register(&metrics_registry);

    let wal_writer = Arc::new(
        S3WalWriter::new(
            &args.s3_bucket,
            &args.s3_prefix,
            args.s3_endpoint.as_deref(),
            &args.s3_region,
        )
        .await,
    );

    // Batch push channel: acceptor → learner (performance optimization).
    let (batch_tx, batch_rx) = tokio::sync::mpsc::channel(256);

    // Spawn acceptor on a dedicated OS thread with its own single-threaded
    // tokio runtime. Isolates flush timer precision from learner CPU work.
    let acceptor_config = AcceptorConfig {
        flush_interval_ms: args.flush_interval_ms,
        ..Default::default()
    };
    let acceptor_wal = Arc::clone(&wal_writer);
    let acceptor_handle = {
        let (tx, rx) = tokio::sync::oneshot::channel();
        let _thread = std::thread::Builder::new()
            .name("acceptor".to_string())
            .spawn(move || {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .expect("failed to build acceptor runtime");
                rt.block_on(async {
                    let (handle, _task) = Acceptor::spawn(
                        acceptor_config,
                        acceptor_wal,
                        Some(batch_tx),
                        acceptor_metrics,
                    );
                    let _ = tx.send(handle.clone());
                    _task.await;
                });
            })
            .expect("failed to spawn acceptor thread");
        rx.await.expect("acceptor thread failed to send handle")
    };

    // Spawn learner on the current runtime.
    let learner_config = LearnerConfig {
        snapshot_interval: args.snapshot_interval,
        ..Default::default()
    };
    let (learner_handle, _learner_task) = Learner::spawn(
        learner_config,
        Arc::clone(&wal_writer),
        batch_rx,
        learner_metrics,
    );

    // Recover state from object storage via the learner.
    info!("recovering state from object storage...");
    let next_batch = learner_handle.recover().await.expect("recovery failed");

    // Tell the acceptor where to start writing.
    acceptor_handle
        .set_batch_number(next_batch)
        .await
        .expect("failed to set acceptor batch number");

    // Spawn HTTP metrics server.
    let metrics_addr = args.metrics_listen_addr;
    mz_ore::task::spawn(|| "metrics-server", async move {
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

    // Build gRPC services.
    let acceptor_service = AcceptorGrpcService {
        handle: acceptor_handle.clone(),
    };
    let learner_service = LearnerGrpcService {
        acceptor_handle,
        learner_handle,
    };

    info!(addr = %args.listen_addr, "starting gRPC server");

    Server::builder()
        .add_service(ConsensusAcceptorServer::new(acceptor_service))
        .add_service(ConsensusLearnerServer::new(learner_service))
        .serve(args.listen_addr)
        .await
        .expect("gRPC server failed");
}

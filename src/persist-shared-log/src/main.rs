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
use mz_persist::generated::consensus_service::persist_shared_log_server::PersistSharedLogServer;
use mz_persist_shared_log::actor::acceptor::ActorAcceptor;
use mz_persist_shared_log::actor::learner::{ActorLearner, LearnerConfig};
use mz_persist_shared_log::actor::metrics::{AcceptorMetrics, LearnerMetrics};
use mz_persist_shared_log::actor::storage::s3::S3Storage;
use mz_persist_shared_log::persist_log::acceptor::PersistAcceptor;
use mz_persist_shared_log::persist_log::learner::{PersistLearner, PersistLearnerConfig};
use mz_persist_shared_log::service::{
    AcceptorGrpcService, LearnerGrpcService, PersistSharedLogGrpcService,
};
use mz_persist_shared_log::traits;
use mz_persist_shared_log::traits::AcceptorConfig;

/// Backend storage mode.
#[derive(Debug, Clone, Copy, clap::ValueEnum)]
enum Backend {
    /// Log + S3 object storage (production).
    Log,
    /// Persist shard (in-memory for now — for development/testing).
    Persist,
}

/// CLI arguments for the consensus service.
#[derive(Parser, Debug)]
#[command(name = "mz-persist-shared-log")]
struct Args {
    /// Address to listen on for gRPC connections.
    #[arg(long, default_value = "0.0.0.0:6890")]
    listen_addr: SocketAddr,

    /// Address to listen on for the HTTP metrics endpoint (/metrics).
    #[arg(long, default_value = "0.0.0.0:6891")]
    metrics_listen_addr: SocketAddr,

    /// Backend storage mode.
    #[arg(long, value_enum, default_value = "log")]
    backend: Backend,

    /// S3 bucket for log and snapshot storage (required for log backend).
    #[arg(long, env = "CONSENSUS_S3_BUCKET")]
    s3_bucket: Option<String>,

    /// S3 key prefix for log and snapshot objects.
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

    /// Write a snapshot every this many log batches.
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

/// Start the gRPC server with the given acceptor and learner handles.
///
/// Registers all three gRPC services (Acceptor, Learner, PersistSharedLog)
/// on a single tonic Server and serves until shutdown.
async fn serve_grpc<A: traits::Acceptor, L: traits::Learner>(
    acceptor: A,
    learner: L,
    listen_addr: SocketAddr,
) {
    let acceptor_service = AcceptorGrpcService {
        handle: acceptor.clone(),
    };
    let learner_service = LearnerGrpcService {
        learner_handle: learner.clone(),
    };
    let persist_shared_log_service = PersistSharedLogGrpcService {
        acceptor: acceptor.clone(),
        learner: learner.clone(),
    };

    info!(addr = %listen_addr, "starting gRPC server");

    Server::builder()
        .add_service(ConsensusAcceptorServer::new(acceptor_service))
        .add_service(ConsensusLearnerServer::new(learner_service))
        .add_service(PersistSharedLogServer::new(persist_shared_log_service))
        .serve(listen_addr)
        .await
        .expect("gRPC server failed");
}

/// Spawn the HTTP metrics server on a background task.
fn spawn_metrics_server(
    metrics_addr: SocketAddr,
    metrics_registry: mz_ore::metrics::MetricsRegistry,
) {
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
}

async fn run(args: Args) {
    let metrics_registry = mz_ore::metrics::MetricsRegistry::new();

    spawn_metrics_server(args.metrics_listen_addr, metrics_registry.clone());

    match args.backend {
        Backend::Log => run_log(args, metrics_registry).await,
        Backend::Persist => run_persist(args, metrics_registry).await,
    }
}

async fn run_log(args: Args, metrics_registry: mz_ore::metrics::MetricsRegistry) {
    let s3_bucket = args
        .s3_bucket
        .expect("--s3-bucket is required for log backend");

    let acceptor_metrics = AcceptorMetrics::register(&metrics_registry);
    let learner_metrics = LearnerMetrics::register(&metrics_registry);

    let store = Arc::new(
        S3Storage::new(
            &s3_bucket,
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
    let acceptor_store = Arc::clone(&store);
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
                    let (handle, _task) = ActorAcceptor::spawn(
                        acceptor_config,
                        acceptor_store,
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

    // Spawn learner on a dedicated OS thread with its own single-threaded
    // tokio runtime, matching the acceptor's isolation pattern. Prevents
    // gRPC handler tasks from competing with learner materialization.
    let learner_config = LearnerConfig {
        snapshot_interval: args.snapshot_interval,
        ..Default::default()
    };
    let (learner_handle, _learner_thread) = ActorLearner::spawn_threaded(
        learner_config,
        Arc::clone(&store),
        batch_rx,
        acceptor_handle.clone(),
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

    serve_grpc(acceptor_handle, learner_handle, args.listen_addr).await;
}

async fn run_persist(args: Args, metrics_registry: mz_ore::metrics::MetricsRegistry) {
    // Register metrics (unused by persist backend today, but keeps the
    // HTTP /metrics endpoint populated with the same gauge names).
    let _acceptor_metrics = AcceptorMetrics::register(&metrics_registry);
    let _learner_metrics = LearnerMetrics::register(&metrics_registry);

    info!("creating in-memory persist client...");
    let persist_client = mz_persist_client::PersistClient::new_for_tests().await;
    let shard_id = mz_persist_client::ShardId::new();

    let acceptor_config = AcceptorConfig {
        flush_interval_ms: args.flush_interval_ms,
        ..Default::default()
    };
    let (acceptor_handle, _acceptor_task) =
        PersistAcceptor::spawn(acceptor_config, &persist_client, shard_id).await;

    let learner_config = PersistLearnerConfig::default();
    let (learner_handle, _learner_task) =
        PersistLearner::spawn(learner_config, &persist_client, shard_id).await;

    info!(
        %shard_id,
        "persist backend ready (in-memory, non-durable)"
    );

    serve_grpc(acceptor_handle, learner_handle, args.listen_addr).await;
}

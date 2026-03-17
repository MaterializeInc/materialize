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

use mz_persist::generated::consensus_service::consensus_acceptor_server::ConsensusAcceptorServer;
use mz_persist::generated::consensus_service::consensus_learner_server::ConsensusLearnerServer;
use mz_persist::generated::consensus_service::persist_shared_log_server::PersistSharedLogServer;
use mz_persist_shared_log::metrics::{AcceptorMetrics, LearnerMetrics};
use mz_persist_shared_log::persist_log::acceptor::PersistAcceptor;
use mz_persist_shared_log::persist_log::learner::{PersistLearner, PersistLearnerConfig};
use mz_persist_shared_log::service::{
    AcceptorGrpcService, LearnerGrpcService, PersistSharedLogGrpcService,
};
use mz_persist_shared_log::traits;
use mz_persist_shared_log::traits::AcceptorConfig;

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

    /// Blob storage URL for persist backend (e.g. file:///tmp/persist/blob).
    /// If omitted, uses in-memory storage.
    #[arg(long, env = "PERSIST_BLOB_URL")]
    blob_url: Option<String>,

    /// Consensus storage URL for persist backend (e.g. postgres://root@localhost:26257/consensus).
    /// If omitted, uses in-memory storage.
    #[arg(long, env = "PERSIST_CONSENSUS_URL")]
    consensus_url: Option<String>,

    /// Shard ID for the persist backend. If omitted, a new shard is created.
    #[arg(long, env = "PERSIST_SHARD_ID")]
    shard_id: Option<String>,
}

fn main() {
    let args = Args::parse();

    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("failed to build tokio runtime");

    rt.block_on(run(args));
}

/// Start the gRPC server with the given acceptor and learner handles.
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

    let acceptor_metrics = AcceptorMetrics::register(&metrics_registry);
    let learner_metrics = LearnerMetrics::register(&metrics_registry);

    let persist_client = match (&args.blob_url, &args.consensus_url) {
        (Some(blob_url), Some(consensus_url)) => {
            info!(%blob_url, %consensus_url, "creating persist client with external storage");
            let persist_config = mz_persist_client::cfg::PersistConfig::new_default_configs(
                &mz_build_info::DUMMY_BUILD_INFO,
                mz_ore::now::SYSTEM_TIME.clone(),
            );
            let cache = mz_persist_client::cache::PersistClientCache::new(
                persist_config,
                &metrics_registry,
                |_, _| mz_persist_client::rpc::PubSubClientConnection::noop(),
            );
            let location = mz_persist_types::PersistLocation {
                blob_uri: blob_url.parse().expect("invalid --blob-url"),
                consensus_uri: consensus_url.parse().expect("invalid --consensus-url"),
            };
            cache
                .open(location)
                .await
                .expect("failed to open persist client")
        }
        (None, None) => {
            info!("creating in-memory persist client (non-durable)");
            mz_persist_client::PersistClient::new_for_tests().await
        }
        _ => {
            panic!("--blob-url and --consensus-url must both be provided, or both omitted");
        }
    };

    let shard_id = match &args.shard_id {
        Some(id) => id.parse().expect("invalid --shard-id"),
        None => {
            let id = mz_persist_client::ShardId::new();
            info!(%id, "generated new shard ID (pass --shard-id to reuse across restarts)");
            id
        }
    };

    let acceptor_config = AcceptorConfig::default();
    let (acceptor_handle, _acceptor_task) =
        PersistAcceptor::spawn(acceptor_config, &persist_client, shard_id, acceptor_metrics).await;

    let learner_config = PersistLearnerConfig::default();
    let (learner_handle, _learner_task) =
        PersistLearner::spawn(learner_config, &persist_client, shard_id, learner_metrics).await;

    info!(%shard_id, "persist backend ready");

    serve_grpc(acceptor_handle, learner_handle, args.listen_addr).await;
}

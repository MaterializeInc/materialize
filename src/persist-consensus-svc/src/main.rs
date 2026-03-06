// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A group commit consensus service for Materialize persist.
//!
//! Batches independent cross-shard CAS writes into a single durable S3 Express
//! One Zone PUT per flush interval, making cost O(1/batch_window) instead of
//! O(shards).

mod actor;
mod crypto;
mod metrics;
mod recovery;
mod s3_wal;

use std::net::SocketAddr;
use std::sync::Arc;

use clap::Parser;
use tonic::transport::Server;
use tracing::{debug, info};

use crate::actor::{Actor, ActorCommand};
use crate::crypto::EnvelopeEncryption;
use crate::metrics::ConsensusMetrics;
use crate::recovery::recover;
use crate::s3_wal::S3WalWriter;

use mz_persist::generated::consensus_service::consensus_service_server::ConsensusServiceServer;

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

    /// Flush interval in milliseconds.
    #[arg(long, default_value = "20")]
    flush_interval_ms: u64,

    /// Write a snapshot every this many WAL batches.
    #[arg(long, default_value = "100")]
    snapshot_interval: u64,

    /// AWS KMS key ARN for envelope encryption of S3 data at rest.
    /// When unset, data is written as plaintext (no encryption).
    #[arg(long, env = "CONSENSUS_KMS_KEY_ID")]
    kms_key_id: Option<String>,

    /// AWS region for KMS. Defaults to the S3 region if unset.
    #[arg(long, env = "CONSENSUS_KMS_REGION")]
    kms_region: Option<String>,

    /// DEK rotation interval in seconds.
    #[arg(long, default_value = "300")]
    dek_rotation_interval_secs: u64,
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

    let encryption = if let Some(ref kms_key_id) = args.kms_key_id {
        let kms_region = args
            .kms_region
            .as_deref()
            .unwrap_or(&args.s3_region);
        let mut kms_config_loader = mz_aws_util::defaults()
            .region(aws_sdk_s3::config::Region::new(kms_region.to_owned()));
        if let Some(ref endpoint) = args.s3_endpoint {
            kms_config_loader = kms_config_loader.endpoint_url(endpoint);
        }
        let kms_config = kms_config_loader.load().await;
        let kms_client = aws_sdk_kms::Client::new(&kms_config);
        let enc = Arc::new(
            EnvelopeEncryption::new(kms_client, kms_key_id.clone())
                .await
                .expect("failed to initialize KMS envelope encryption"),
        );
        enc.start_rotation(std::time::Duration::from_secs(
            args.dek_rotation_interval_secs,
        ));
        info!(kms_key_id = %kms_key_id, "S3 data-at-rest encryption enabled");
        Some(enc)
    } else {
        info!("S3 data-at-rest encryption disabled (no --kms-key-id)");
        None
    };

    let wal_writer = S3WalWriter::new(
        &args.s3_bucket,
        &args.s3_prefix,
        args.s3_endpoint.as_deref(),
        &args.s3_region,
        encryption,
    )
    .await;

    info!("recovering state from S3...");
    let (shards, next_batch) = recover(&wal_writer).await;
    info!(shards = shards.len(), next_batch, "recovery complete");

    let flush_interval =
        tokio::time::interval(std::time::Duration::from_millis(args.flush_interval_ms));

    let (tx, rx) = tokio::sync::mpsc::channel::<ActorCommand>(4096);

    let actor = Actor::new(
        shards,
        rx,
        wal_writer,
        next_batch,
        flush_interval,
        args.snapshot_interval,
        metrics,
    );
    tokio::task::spawn_local(actor.run());

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

    let grpc_service = ConsensusGrpcService { tx };
    info!(addr = %args.listen_addr, "starting gRPC server");

    Server::builder()
        .add_service(ConsensusServiceServer::new(grpc_service))
        .serve(args.listen_addr)
        .await
        .expect("gRPC server failed");
}

// --- gRPC server glue ---

use tokio::sync::oneshot;

use mz_persist::generated::consensus_service::consensus_service_server::ConsensusService;
use mz_persist::generated::consensus_service::{
    ProtoCompareAndSetRequest, ProtoCompareAndSetResponse, ProtoHeadRequest, ProtoHeadResponse,
    ProtoListKeysRequest, ProtoListKeysResponse, ProtoScanRequest, ProtoScanResponse,
    ProtoTruncateRequest, ProtoTruncateResponse,
};

/// The gRPC service implementation that dispatches to the actor.
#[derive(Debug)]
struct ConsensusGrpcService {
    tx: tokio::sync::mpsc::Sender<ActorCommand>,
}

#[tonic::async_trait]
impl ConsensusService for ConsensusGrpcService {
    async fn head(
        &self,
        request: tonic::Request<ProtoHeadRequest>,
    ) -> Result<tonic::Response<ProtoHeadResponse>, tonic::Status> {
        let req = request.into_inner();
        debug!(key = %req.key, "head");
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(ActorCommand::Head {
                key: req.key,
                reply: reply_tx,
            })
            .await
            .map_err(|_| tonic::Status::unavailable("actor shut down"))?;
        let result = reply_rx
            .await
            .map_err(|_| tonic::Status::internal("actor dropped reply"))?;
        match result {
            Ok(resp) => Ok(tonic::Response::new(resp)),
            Err(e) => Err(tonic::Status::internal(format!("{}", e))),
        }
    }

    async fn compare_and_set(
        &self,
        request: tonic::Request<ProtoCompareAndSetRequest>,
    ) -> Result<tonic::Response<ProtoCompareAndSetResponse>, tonic::Status> {
        let req = request.into_inner();
        debug!(key = %req.key, expected = req.expected, new_seqno = req.new.as_ref().map(|v| v.seqno), "cas");
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(ActorCommand::CompareAndSet {
                key: req.key,
                expected: req.expected,
                new: req
                    .new
                    .ok_or_else(|| tonic::Status::invalid_argument("missing `new` field"))?,
                reply: reply_tx,
            })
            .await
            .map_err(|_| tonic::Status::unavailable("actor shut down"))?;
        let result = reply_rx
            .await
            .map_err(|_| tonic::Status::internal("actor dropped reply"))?;
        match result {
            Ok(resp) => Ok(tonic::Response::new(resp)),
            Err(e) => Err(tonic::Status::internal(format!("{}", e))),
        }
    }

    async fn scan(
        &self,
        request: tonic::Request<ProtoScanRequest>,
    ) -> Result<tonic::Response<ProtoScanResponse>, tonic::Status> {
        let req = request.into_inner();
        debug!(key = %req.key, from = req.from, limit = req.limit, "scan");
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(ActorCommand::Scan {
                key: req.key,
                from: req.from,
                limit: req.limit,
                reply: reply_tx,
            })
            .await
            .map_err(|_| tonic::Status::unavailable("actor shut down"))?;
        let result = reply_rx
            .await
            .map_err(|_| tonic::Status::internal("actor dropped reply"))?;
        match result {
            Ok(resp) => Ok(tonic::Response::new(resp)),
            Err(e) => Err(tonic::Status::internal(format!("{}", e))),
        }
    }

    async fn truncate(
        &self,
        request: tonic::Request<ProtoTruncateRequest>,
    ) -> Result<tonic::Response<ProtoTruncateResponse>, tonic::Status> {
        let req = request.into_inner();
        debug!(key = %req.key, seqno = req.seqno, "truncate");
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(ActorCommand::Truncate {
                key: req.key,
                seqno: req.seqno,
                reply: reply_tx,
            })
            .await
            .map_err(|_| tonic::Status::unavailable("actor shut down"))?;
        let result = reply_rx
            .await
            .map_err(|_| tonic::Status::internal("actor dropped reply"))?;
        match result {
            Ok(resp) => Ok(tonic::Response::new(resp)),
            Err(e) => Err(tonic::Status::internal(format!("{}", e))),
        }
    }

    type ListKeysStream =
        tokio_stream::wrappers::ReceiverStream<Result<ProtoListKeysResponse, tonic::Status>>;

    async fn list_keys(
        &self,
        _request: tonic::Request<ProtoListKeysRequest>,
    ) -> Result<tonic::Response<Self::ListKeysStream>, tonic::Status> {
        debug!("list_keys");
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send(ActorCommand::ListKeys { reply: reply_tx })
            .await
            .map_err(|_| tonic::Status::unavailable("actor shut down"))?;
        let result = reply_rx
            .await
            .map_err(|_| tonic::Status::internal("actor dropped reply"))?;
        match result {
            Ok(keys) => {
                let (stream_tx, stream_rx) = tokio::sync::mpsc::channel(64);
                tokio::spawn(async move {
                    for key in keys {
                        if stream_tx
                            .send(Ok(ProtoListKeysResponse { key }))
                            .await
                            .is_err()
                        {
                            break;
                        }
                    }
                });
                Ok(tonic::Response::new(
                    tokio_stream::wrappers::ReceiverStream::new(stream_rx),
                ))
            }
            Err(e) => Err(tonic::Status::internal(format!("{}", e))),
        }
    }
}

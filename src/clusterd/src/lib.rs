// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::LazyLock;

use anyhow::Context;
use axum::http::StatusCode;
use axum::routing;
use fail::FailScenario;
use futures::future;
use hyper_util::rt::TokioIo;
use mz_build_info::{build_info, BuildInfo};
use mz_cloud_resources::AwsExternalIdPrefix;
use mz_compute::server::ComputeInstanceContext;
use mz_compute_client::service::proto_compute_server::ProtoComputeServer;
use mz_http_util::DynamicFilterTarget;
use mz_orchestrator_tracing::{StaticTracingConfig, TracingCliArgs};
use mz_ore::cli::{self, CliConfig};
use mz_ore::error::ErrorExt;
use mz_ore::metrics::MetricsRegistry;
use mz_ore::netio::{Listener, SocketAddr};
use mz_ore::now::SYSTEM_TIME;
use mz_persist_client::cache::PersistClientCache;
use mz_persist_client::cfg::PersistConfig;
use mz_persist_client::rpc::{GrpcPubSubClient, PersistPubSubClient, PersistPubSubClientConfig};
use mz_service::emit_boot_diagnostics;
use mz_service::grpc::{GrpcServer, GrpcServerMetrics, MAX_GRPC_MESSAGE_SIZE};
use mz_service::secrets::SecretsReaderCliArgs;
use mz_storage::storage_state::StorageInstanceContext;
use mz_storage_client::client::proto_storage_server::ProtoStorageServer;
use mz_storage_types::connections::ConnectionContext;
use mz_txn_wal::operator::TxnsContext;
use tower::Service;
use tracing::{error, info};

const BUILD_INFO: BuildInfo = build_info!();

pub static VERSION: LazyLock<String> = LazyLock::new(|| BUILD_INFO.human_version(None));

/// Independent cluster server for Materialize.
#[derive(clap::Parser)]
#[clap(name = "clusterd", version = VERSION.as_str())]
struct Args {
    // === Connection options. ===
    /// The address on which to listen for a connection from the storage
    /// controller.
    #[clap(
        long,
        env = "STORAGE_CONTROLLER_LISTEN_ADDR",
        value_name = "HOST:PORT",
        default_value = "127.0.0.1:2100"
    )]
    storage_controller_listen_addr: SocketAddr,
    /// The address on which to listen for a connection from the compute
    /// controller.
    #[clap(
        long,
        env = "COMPUTE_CONTROLLER_LISTEN_ADDR",
        value_name = "HOST:PORT",
        default_value = "127.0.0.1:2101"
    )]
    compute_controller_listen_addr: SocketAddr,
    /// The address of the internal HTTP server.
    #[clap(
        long,
        env = "INTERNAL_HTTP_LISTEN_ADDR",
        value_name = "HOST:PORT",
        default_value = "127.0.0.1:6878"
    )]
    internal_http_listen_addr: SocketAddr,
    /// The FQDN of this process, for GRPC request validation.
    ///
    /// Not providing this value or setting it to the empty string disables host validation for
    /// GRPC requests.
    #[clap(long, env = "GRPC_HOST", value_name = "NAME")]
    grpc_host: Option<String>,

    // === Storage options. ===
    /// The URL for the Persist PubSub service.
    #[clap(
        long,
        env = "PERSIST_PUBSUB_URL",
        value_name = "http://HOST:PORT",
        default_value = "http://localhost:6879"
    )]
    persist_pubsub_url: String,

    // === Cloud options. ===
    /// An external ID to be supplied to all AWS AssumeRole operations.
    ///
    /// Details: <https://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles_create_for-user_externalid.html>
    #[clap(long, env = "AWS_EXTERNAL_ID", value_name = "ID", parse(from_str = AwsExternalIdPrefix::new_from_cli_argument_or_environment_variable))]
    aws_external_id_prefix: Option<AwsExternalIdPrefix>,

    /// The ARN for a Materialize-controlled role to assume before assuming
    /// a customer's requested role for an AWS connection.
    #[clap(long, env = "AWS_CONNECTION_ROLE_ARN")]
    aws_connection_role_arn: Option<String>,

    // === Secrets reader options. ===
    #[clap(flatten)]
    secrets: SecretsReaderCliArgs,

    // === Tracing options. ===
    #[clap(flatten)]
    tracing: TracingCliArgs,

    // === Other options. ===
    /// An opaque identifier for the environment in which this process is
    /// running.
    #[clap(long, env = "ENVIRONMENT_ID")]
    environment_id: String,

    /// A scratch directory that can be used for ephemeral storage.
    #[clap(long, env = "SCRATCH_DIRECTORY", value_name = "PATH")]
    scratch_directory: Option<PathBuf>,

    /// Optional memory limit (bytes) of the cluster replica
    #[clap(long)]
    announce_memory_limit: Option<usize>,

    /// Whether this size represents a modern "cc" size rather than a legacy
    /// T-shirt size.
    #[clap(long)]
    is_cc: bool,

    /// Set core affinity for Timely workers.
    ///
    /// This flag should only be set if the process is provided with exclusive access to its
    /// supplied CPU cores. If other processes are competing over the same cores, setting core
    /// affinity might degrade dataflow performance rather than improving it.
    #[clap(long)]
    worker_core_affinity: bool,
}

#[tokio::main]
pub async fn main() {
    mz_ore::panic::install_enhanced_handler();

    let args = cli::parse_args(CliConfig {
        env_prefix: Some("CLUSTERD_"),
        enable_version_flag: true,
    });
    if let Err(err) = run(args).await {
        panic!("clusterd: fatal: {}", err.display_with_causes());
    }
}

async fn run(args: Args) -> Result<(), anyhow::Error> {
    let metrics_registry = MetricsRegistry::new();
    let (tracing_handle, _tracing_guard) = args
        .tracing
        .configure_tracing(
            StaticTracingConfig {
                service_name: "clusterd",
                build_info: BUILD_INFO,
            },
            metrics_registry.clone(),
        )
        .await?;

    let tracing_handle = Arc::new(tracing_handle);

    // Keep this _after_ the mz_ore::tracing::configure call so that its panic
    // hook runs _before_ the one that sends things to sentry.
    mz_timely_util::panic::halt_on_timely_communication_panic();

    let _failpoint_scenario = FailScenario::setup();

    emit_boot_diagnostics!(&BUILD_INFO);

    mz_alloc::register_metrics_into(&metrics_registry).await;
    mz_metrics::register_metrics_into(&metrics_registry).await;

    let secrets_reader = args
        .secrets
        .load()
        .await
        .context("loading secrets reader")?;

    mz_ore::task::spawn(|| "clusterd_internal_http_server", {
        let metrics_registry = metrics_registry.clone();
        tracing::info!(
            "serving internal HTTP server on {}",
            args.internal_http_listen_addr
        );
        let listener = Listener::bind(args.internal_http_listen_addr).await?;
        let mut make_service = mz_prof_http::router(&BUILD_INFO)
            .route(
                "/api/livez",
                routing::get(mz_http_util::handle_liveness_check),
            )
            .route(
                "/metrics",
                routing::get(move || async move {
                    mz_http_util::handle_prometheus(&metrics_registry).await
                }),
            )
            .route("/api/tracing", routing::get(mz_http_util::handle_tracing))
            .route(
                "/api/opentelemetry/config",
                routing::put({
                    move |_: axum::Json<DynamicFilterTarget>| async {
                        (
                            StatusCode::BAD_REQUEST,
                            "This endpoint has been replaced. \
                                Use the `opentelemetry_filter` system variable."
                                .to_string(),
                        )
                    }
                }),
            )
            .route(
                "/api/stderr/config",
                routing::put({
                    move |_: axum::Json<DynamicFilterTarget>| async {
                        (
                            StatusCode::BAD_REQUEST,
                            "This endpoint has been replaced. \
                                Use the `log_filter` system variable."
                                .to_string(),
                        )
                    }
                }),
            )
            .into_make_service();

        // Once https://github.com/tokio-rs/axum/pull/2479 lands, this can become just a call to
        // `axum::serve`.
        async move {
            loop {
                let (conn, remote_addr) = match listener.accept().await {
                    Ok(peer) => peer,
                    Err(error) => {
                        error!("internal_http connection failed: {error:#}");
                        break;
                    }
                };

                let tower_service = make_service.call(&conn).await.expect("infallible");
                let hyper_service =
                    hyper::service::service_fn(move |req| tower_service.clone().call(req));

                mz_ore::task::spawn(
                    || format!("clusterd_internal_http_server:{remote_addr}"),
                    async move {
                        if let Err(error) = hyper::server::conn::http1::Builder::new()
                            .serve_connection(TokioIo::new(conn), hyper_service)
                            .await
                        {
                            error!("failed to serve internal_http connection: {error:#}");
                        }
                    },
                );
            }
        }
    });

    let pubsub_caller_id = std::env::var("HOSTNAME")
        .ok()
        .or_else(|| args.tracing.log_prefix.clone())
        .unwrap_or_default();
    let mut persist_cfg =
        PersistConfig::new(&BUILD_INFO, SYSTEM_TIME.clone(), mz_dyncfgs::all_dyncfgs());
    persist_cfg.is_cc_active = args.is_cc;
    persist_cfg.announce_memory_limit = args.announce_memory_limit;
    let persist_clients = Arc::new(PersistClientCache::new(
        persist_cfg,
        &metrics_registry,
        |persist_cfg, metrics| {
            let cfg = PersistPubSubClientConfig {
                url: args.persist_pubsub_url,
                caller_id: pubsub_caller_id,
                persist_cfg: persist_cfg.clone(),
            };
            GrpcPubSubClient::connect(cfg, metrics)
        },
    ));
    let txns_ctx = TxnsContext::default();

    let connection_context = ConnectionContext::from_cli_args(
        args.environment_id,
        &args.tracing.startup_log_filter,
        args.aws_external_id_prefix,
        args.aws_connection_role_arn,
        secrets_reader,
        None,
    );

    let grpc_host = args.grpc_host.and_then(|h| (!h.is_empty()).then_some(h));
    let grpc_server_metrics = GrpcServerMetrics::register_with(&metrics_registry);

    // Start storage server.
    let (_storage_server, storage_client) = mz_storage::serve(
        mz_cluster::server::ClusterConfig {
            metrics_registry: metrics_registry.clone(),
            persist_clients: Arc::clone(&persist_clients),
            txns_ctx: txns_ctx.clone(),
            tracing_handle: Arc::clone(&tracing_handle),
        },
        SYSTEM_TIME.clone(),
        connection_context.clone(),
        StorageInstanceContext::new(args.scratch_directory.clone(), args.announce_memory_limit)?,
    )?;
    info!(
        "listening for storage controller connections on {}",
        args.storage_controller_listen_addr
    );
    mz_ore::task::spawn(
        || "storage_server",
        GrpcServer::serve(
            &grpc_server_metrics,
            args.storage_controller_listen_addr,
            BUILD_INFO.semver_version(),
            grpc_host.clone(),
            storage_client,
            |svc| ProtoStorageServer::new(svc).max_decoding_message_size(MAX_GRPC_MESSAGE_SIZE),
        ),
    );

    // Start compute server.
    let (_compute_server, compute_client) = mz_compute::server::serve(
        mz_cluster::server::ClusterConfig {
            metrics_registry,
            persist_clients,
            txns_ctx,
            tracing_handle,
        },
        ComputeInstanceContext {
            scratch_directory: args.scratch_directory,
            worker_core_affinity: args.worker_core_affinity,
            connection_context,
        },
    )?;
    info!(
        "listening for compute controller connections on {}",
        args.compute_controller_listen_addr
    );
    mz_ore::task::spawn(
        || "compute_server",
        GrpcServer::serve(
            &grpc_server_metrics,
            args.compute_controller_listen_addr,
            BUILD_INFO.semver_version(),
            grpc_host,
            compute_client,
            |svc| ProtoComputeServer::new(svc).max_decoding_message_size(MAX_GRPC_MESSAGE_SIZE),
        ),
    );

    // TODO: unify storage and compute servers to use one timely cluster.

    // Block forever.
    future::pending().await
}

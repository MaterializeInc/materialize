// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::path::PathBuf;
use std::process;
use std::sync::Arc;

use anyhow::{bail, Context};
use axum::routing;
use fail::FailScenario;
use futures::future;
use once_cell::sync::Lazy;
use tokio::sync::Mutex;
use tracing::info;

use mz_build_info::{build_info, BuildInfo};
use mz_cloud_resources::AwsExternalIdPrefix;
use mz_compute_client::service::proto_compute_server::ProtoComputeServer;
use mz_orchestrator_tracing::{StaticTracingConfig, TracingCliArgs};
use mz_ore::cli::{self, CliConfig};
use mz_ore::metrics::MetricsRegistry;
use mz_ore::netio::{Listener, SocketAddr};
use mz_ore::now::SYSTEM_TIME;
use mz_ore::tracing::TracingHandle;
use mz_persist_client::cache::PersistClientCache;
use mz_persist_client::PersistConfig;
use mz_pid_file::PidFile;
use mz_service::grpc::GrpcServer;
use mz_service::secrets::SecretsReaderCliArgs;
use mz_storage_client::client::proto_storage_server::ProtoStorageServer;
use mz_storage_client::types::connections::ConnectionContext;

// Disable jemalloc on macOS, as it is not well supported [0][1][2].
// The issues present as runaway latency on load test workloads that are
// comfortably handled by the macOS system allocator. Consider re-evaluating if
// jemalloc's macOS support improves.
//
// [0]: https://github.com/jemalloc/jemalloc/issues/26
// [1]: https://github.com/jemalloc/jemalloc/issues/843
// [2]: https://github.com/jemalloc/jemalloc/issues/1467
//
// Furthermore, as of Aug. 2022, some engineers are using profiling
// tools, e.g. `heaptrack`, that only work with the system allocator.
#[cfg(all(not(target_os = "macos"), feature = "jemalloc"))]
#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

const BUILD_INFO: BuildInfo = build_info!();

pub static VERSION: Lazy<String> = Lazy::new(|| BUILD_INFO.human_version());

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

    // === Storage options. ===
    /// Number of dataflow worker threads.
    #[clap(long, env = "STORAGE_WORKERS", value_name = "N", default_value = "1")]
    storage_workers: usize,

    // === Cloud options. ===
    /// An external ID to be supplied to all AWS AssumeRole operations.
    ///
    /// Details: <https://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles_create_for-user_externalid.html>
    #[clap(long, env = "AWS_EXTERNAL_ID", value_name = "ID", parse(from_str = AwsExternalIdPrefix::new_from_cli_argument_or_environment_variable))]
    aws_external_id: Option<AwsExternalIdPrefix>,

    // === Process orchestrator options. ===
    /// Where to write a PID lock file.
    ///
    /// Should only be set by the local process orchestrator.
    #[clap(long, env = "PID_FILE_LOCATION", value_name = "PATH")]
    pid_file_location: Option<PathBuf>,

    // === Secrets reader options. ===
    #[clap(flatten)]
    secrets: SecretsReaderCliArgs,

    // === Tracing options. ===
    #[clap(flatten)]
    tracing: TracingCliArgs,
}

#[tokio::main]
async fn main() {
    let args = cli::parse_args(CliConfig {
        env_prefix: Some("CLUSTERD_"),
        enable_version_flag: true,
    });
    if let Err(err) = run(args).await {
        eprintln!("clusterd: fatal: {:#}", err);
        process::exit(1);
    }
}

async fn run(args: Args) -> Result<(), anyhow::Error> {
    mz_ore::panic::set_abort_on_panic();
    let (tracing_handle, _tracing_guard) = args
        .tracing
        .configure_tracing(StaticTracingConfig {
            service_name: "clusterd",
            build_info: BUILD_INFO,
        })
        .await?;

    // Keep this _after_ the mz_ore::tracing::configure call so that its panic
    // hook runs _before_ the one that sends things to sentry.
    mz_timely_util::panic::halt_on_timely_communication_panic();

    let _failpoint_scenario = FailScenario::setup();

    let mut _pid_file = None;
    if let Some(pid_file_location) = &args.pid_file_location {
        _pid_file = Some(PidFile::open(pid_file_location).unwrap());
    }

    let secrets_reader = args
        .secrets
        .load()
        .await
        .context("loading secrets reader")?;

    let metrics_registry = MetricsRegistry::new();

    mz_ore::task::spawn(|| "clusterd_internal_http_server", {
        let metrics_registry = metrics_registry.clone();
        tracing::info!(
            "serving internal HTTP server on {}",
            args.internal_http_listen_addr
        );
        let listener = Listener::bind(args.internal_http_listen_addr).await?;
        axum::Server::builder(listener).serve(
            mz_prof::http::router(&BUILD_INFO)
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
                .route(
                    "/api/opentelemetry/config",
                    routing::put({
                        let tracing_handle = tracing_handle.clone();
                        move |payload| async move {
                            mz_http_util::handle_reload_tracing_filter(
                                &tracing_handle,
                                TracingHandle::reload_opentelemetry_filter,
                                payload,
                            )
                            .await
                        }
                    }),
                )
                .route(
                    "/api/stderr/config",
                    routing::put({
                        let tracing_handle = tracing_handle.clone();
                        move |payload| async move {
                            mz_http_util::handle_reload_tracing_filter(
                                &tracing_handle,
                                TracingHandle::reload_stderr_log_filter,
                                payload,
                            )
                            .await
                        }
                    }),
                )
                .into_make_service(),
        )
    });

    let persist_clients = Arc::new(Mutex::new(PersistClientCache::new(
        PersistConfig::new(&BUILD_INFO, SYSTEM_TIME.clone()),
        &metrics_registry,
    )));

    // Start storage server.
    let (_storage_server, storage_client) = mz_storage::serve(mz_storage::Config {
        persist_clients: Arc::clone(&persist_clients),
        workers: args.storage_workers,
        timely_config: timely::Config {
            worker: timely::WorkerConfig::default(),
            communication: match args.storage_workers {
                0 => bail!("--storage-workers must be greater than 0"),
                1 => timely::CommunicationConfig::Thread,
                _ => timely::CommunicationConfig::Process(args.storage_workers),
            },
        },
        metrics_registry: metrics_registry.clone(),
        now: SYSTEM_TIME.clone(),
        connection_context: ConnectionContext::from_cli_args(
            &args.tracing.log_filter.inner,
            args.aws_external_id,
            secrets_reader,
        ),
    })?;
    info!(
        "listening for storage controller connections on {}",
        args.storage_controller_listen_addr
    );
    mz_ore::task::spawn(
        || "storage_server",
        GrpcServer::serve(
            args.storage_controller_listen_addr,
            BUILD_INFO.semver_version(),
            storage_client,
            ProtoStorageServer::new,
        ),
    );

    // Start compute server.
    let (_compute_server, compute_client) =
        mz_compute::server::serve(mz_compute::server::Config {
            metrics_registry,
            persist_clients,
        })?;
    info!(
        "listening for compute controller connections on {}",
        args.compute_controller_listen_addr
    );
    mz_ore::task::spawn(
        || "compute_server",
        GrpcServer::serve(
            args.compute_controller_listen_addr,
            BUILD_INFO.semver_version(),
            compute_client,
            ProtoComputeServer::new,
        ),
    );

    // TODO: unify storage and compute servers to use one timely cluster.

    // Block forever.
    future::pending().await
}

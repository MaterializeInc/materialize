// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::env;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::process;

use anyhow::{bail, Context};
use axum::routing;
use mz_service::secrets::SecretsReaderCliArgs;
use once_cell::sync::Lazy;
use tracing::info;

use mz_build_info::{build_info, BuildInfo};
use mz_orchestrator_tracing::TracingCliArgs;
use mz_ore::cli::{self, CliConfig};
use mz_ore::metrics::MetricsRegistry;
use mz_ore::now::SYSTEM_TIME;
use mz_pid_file::PidFile;
use mz_service::grpc::GrpcServer;
use mz_storage::protocol::client::proto_storage_server::ProtoStorageServer;
use mz_storage::types::connections::ConnectionContext;

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

/// Independent storage server for Materialize.
#[derive(clap::Parser)]
#[clap(name = "storaged", version = VERSION.as_str())]
struct Args {
    // === Connection options. ===
    /// The address on which to listen for a connection from the controller.
    #[clap(
        long,
        env = "LISTEN_ADDR",
        value_name = "HOST:PORT",
        default_value = "127.0.0.1:2100"
    )]
    controller_listen_addr: SocketAddr,
    /// The address of the internal HTTP server.
    #[clap(
        long,
        env = "INTERNAL_HTTP_LISTEN_ADDR",
        value_name = "HOST:PORT",
        default_value = "127.0.0.1:6878"
    )]
    internal_http_listen_addr: SocketAddr,

    // === Dataflow options. ===
    /// Number of dataflow worker threads.
    #[clap(long, env = "WORKERS", value_name = "N", default_value = "1")]
    workers: usize,

    // === Cloud options. ===
    /// An external ID to be supplied to all AWS AssumeRole operations.
    ///
    /// Details: <https://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles_create_for-user_externalid.html>
    #[clap(long, env = "AWS_EXTERNAL_ID", value_name = "ID")]
    aws_external_id: Option<String>,

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
        env_prefix: Some("STORAGED_"),
        enable_version_flag: true,
    });
    if let Err(err) = run(args).await {
        eprintln!("storaged: fatal: {:#}", err);
        process::exit(1);
    }
}

fn create_communication_config(args: &Args) -> Result<timely::CommunicationConfig, anyhow::Error> {
    let threads = args.workers;
    if threads > 1 {
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
    mz_ore::panic::set_abort_on_panic();
    let otel_enable_callback = mz_ore::tracing::configure("storaged", &args.tracing).await?;

    let mut _pid_file = None;
    if let Some(pid_file_location) = &args.pid_file_location {
        _pid_file = Some(PidFile::open(&pid_file_location).unwrap());
    }

    if args.workers == 0 {
        bail!("--workers must be greater than 0");
    }
    let timely_config = create_timely_config(&args)?;

    info!("about to bind to {:?}", args.controller_listen_addr);

    let metrics_registry = MetricsRegistry::new();
    {
        let metrics_registry = metrics_registry.clone();
        tracing::info!(
            "serving internal HTTP server on {}",
            args.internal_http_listen_addr
        );
        mz_ore::task::spawn(
            || "storaged_internal_http_server",
            axum::Server::bind(&args.internal_http_listen_addr).serve(
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
                        routing::put(move |payload| async move {
                            mz_http_util::handle_enable_otel(otel_enable_callback, payload).await
                        }),
                    )
                    .into_make_service(),
            ),
        );
    }

    let secrets_reader = args
        .secrets
        .load()
        .await
        .context("loading secrets reader")?;
    let config = mz_storage::Config {
        build_info: &BUILD_INFO,
        workers: args.workers,
        timely_config,
        metrics_registry,
        now: SYSTEM_TIME.clone(),
        connection_context: ConnectionContext::from_cli_args(
            &args.tracing.log_filter.inner,
            args.aws_external_id,
            secrets_reader,
        ),
    };

    let (_server, client) = mz_storage::serve(config)?;
    GrpcServer::serve(
        args.controller_listen_addr,
        BUILD_INFO.semver_version(),
        client,
        ProtoStorageServer::new,
    )
    .await
}

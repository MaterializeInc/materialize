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

use anyhow::{bail, Context};
use axum::routing;
use mz_compute::server::CommunicationConfig;
use mz_service::secrets::SecretsReaderCliArgs;
use once_cell::sync::Lazy;
use tracing::info;

use mz_build_info::{build_info, BuildInfo};
use mz_compute_client::service::proto_compute_server::ProtoComputeServer;
use mz_orchestrator_tracing::TracingCliArgs;
use mz_ore::cli::{self, CliConfig};
use mz_ore::metrics::MetricsRegistry;
use mz_ore::now::SYSTEM_TIME;
use mz_pid_file::PidFile;
use mz_service::grpc::GrpcServer;
use mz_storage::client::connections::ConnectionContext;

// Disable jemalloc on macOS, as it is not well supported [0][1][2].
// The issues present as runaway latency on load test workloads that are
// comfortably handled by the macOS system allocator. Consider re-evaluating if
// jemalloc's macOS support improves.
//
// [0]: https://github.com/jemalloc/jemalloc/issues/26
// [1]: https://github.com/jemalloc/jemalloc/issues/843
// [2]: https://github.com/jemalloc/jemalloc/issues/1467
#[cfg(all(not(target_os = "macos"), feature = "jemalloc"))]
#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

const BUILD_INFO: BuildInfo = build_info!();

pub static VERSION: Lazy<String> = Lazy::new(|| BUILD_INFO.human_version());

/// Independent dataflow server for Materialize.
#[derive(clap::Parser)]
#[clap(name = "computed", version = VERSION.as_str())]
struct Args {
    /// The address on which to listen for a connection from the controller.
    #[clap(
        long,
        env = "LISTEN_ADDR",
        value_name = "HOST:PORT",
        default_value = "127.0.0.1:2100"
    )]
    listen_addr: String,
    /// Number of dataflow worker threads.
    #[clap(short, long, env = "WORKERS", value_name = "W", default_value = "1")]
    workers: usize,
    /// Number of this computed process.
    #[clap(short = 'p', long, env = "PROCESS", value_name = "P")]
    process: Option<usize>,
    /// The addresses of all computed processes in the cluster.
    #[clap()]
    addresses: Vec<String>,

    /// An external ID to be supplied to all AWS AssumeRole operations.
    ///
    /// Details: <https://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles_create_for-user_externalid.html>
    #[clap(long, value_name = "ID")]
    aws_external_id: Option<String>,
    /// The address of the internal HTTP server.
    #[clap(long, value_name = "HOST:PORT", default_value = "127.0.0.1:6877")]
    internal_http_listen_addr: String,

    /// Where to write a pid lock file. Should only be used for local process orchestrators.
    #[clap(long, value_name = "PATH")]
    pid_file_location: Option<PathBuf>,

    #[clap(flatten)]
    tracing: TracingCliArgs,

    #[clap(flatten)]
    secrets: SecretsReaderCliArgs,
}

#[tokio::main]
async fn main() {
    let args = cli::parse_args(CliConfig {
        env_prefix: Some("COMPUTED_"),
        enable_version_flag: true,
    });
    if let Err(err) = run(args).await {
        eprintln!("computed: fatal: {:#}", err);
        process::exit(1);
    }
}

fn create_communication_config(args: &Args) -> Result<CommunicationConfig, anyhow::Error> {
    let process = match args.process {
        None => 0,
        Some(process) if process >= args.addresses.len() => {
            bail!(
                "process index {process} out of range [0, {})",
                args.addresses.len()
            );
        }
        Some(process) => process,
    };
    Ok(CommunicationConfig {
        threads: args.workers,
        process,
        addresses: args.addresses.clone(),
    })
}

async fn run(args: Args) -> Result<(), anyhow::Error> {
    mz_ore::panic::set_abort_on_panic();
    let otel_enable_callback = mz_ore::tracing::configure("computed", &args.tracing).await?;

    let mut _pid_file = None;
    if let Some(pid_file_location) = &args.pid_file_location {
        _pid_file = Some(PidFile::open(&pid_file_location).unwrap());
    }

    if args.workers == 0 {
        bail!("--workers must be greater than 0");
    }
    let comm_config = create_communication_config(&args)?;

    info!("about to bind to {:?}", args.listen_addr);

    let metrics_registry = MetricsRegistry::new();
    {
        let metrics_registry = metrics_registry.clone();
        tracing::info!(
            "serving internal HTTP server on {}",
            args.internal_http_listen_addr
        );
        mz_ore::task::spawn(
            || "computed_internal_http_server",
            axum::Server::bind(&args.internal_http_listen_addr.parse()?).serve(
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
    let config = mz_compute::server::Config {
        workers: args.workers,
        comm_config,
        metrics_registry,
        now: SYSTEM_TIME.clone(),
        connection_context: ConnectionContext::from_cli_args(
            &args.tracing.log_filter.inner,
            args.aws_external_id,
            secrets_reader,
        ),
    };

    let (_server, client) = mz_compute::server::serve(config)?;
    GrpcServer::serve(
        args.listen_addr,
        BUILD_INFO.semver_version(),
        client,
        ProtoComputeServer::new,
    )
    .await
}

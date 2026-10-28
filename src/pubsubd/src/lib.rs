// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! An environment-scoped, best-effort relay for committed Persist diffs.
//!
//! The relay has no catalog membership or durable storage credentials. Its lifetime
//! is independent of deployment controllers and SQL-serving processes. Clients
//! recover missed notifications through Persist's durable fallback.

use std::net::SocketAddr;
use std::sync::LazyLock;

use anyhow::Context;
use axum::Router;
use axum::extract::State;
use axum::http::HeaderMap;
use axum::response::IntoResponse;
use axum::routing::get;
use mz_build_info::{BuildInfo, build_info};
use mz_orchestrator_tracing::{StaticTracingConfig, TracingCliArgs};
use mz_ore::cli::{self, CliConfig};
use mz_ore::error::ErrorExt;
use mz_ore::metrics::{MetricsRegistry, register_runtime_metrics};
use mz_ore::now::SYSTEM_TIME;
use mz_persist_client::cfg::PersistConfig;
use mz_persist_client::rpc::PersistGrpcPubSubServer;

const BUILD_INFO: BuildInfo = build_info!();
static VERSION: LazyLock<String> = LazyLock::new(|| BUILD_INFO.human_version(None));

#[derive(Debug, clap::Parser)]
#[clap(about = "Materialize Persist pubsub relay", version = VERSION.as_str())]
struct Args {
    // Backend launch arguments are shared with distributed services. The relay
    // has one process and does not access secrets.
    #[clap(long = "process", hide = true, default_value = "0", value_parser = clap::value_parser!(u16).range(0..=0))]
    _process: u16,
    #[clap(long = "secrets-reader-name-prefix", hide = true)]
    _secrets_reader_name_prefix: Option<String>,
    /// Address for the internal Persist pubsub gRPC service.
    #[clap(long, default_value = "127.0.0.1:6879")]
    listen_addr: SocketAddr,
    /// Address for internal health and metrics requests.
    #[clap(long, default_value = "127.0.0.1:6880")]
    internal_http_listen_addr: SocketAddr,
    #[clap(flatten)]
    tracing: TracingCliArgs,
}

/// Run the standalone relay with command-line configuration.
pub fn main() {
    mz_ore::panic::install_enhanced_handler();
    // Workspace dependencies enable multiple providers. Choose one before tracing
    // exporters or other TLS clients resolve the process default.
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
    let args = cli::parse_args(CliConfig {
        env_prefix: Some("PUBSUBD_"),
        enable_version_flag: true,
    });
    let threads = num_cpus::get().min(num_cpus::get_physical()).max(1);
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(threads)
        .enable_all()
        .build()
        .expect("failed to build pubsub runtime");
    if let Err(error) = runtime.block_on(run(args)) {
        panic!("pubsubd: fatal: {}", error.display_with_causes());
    }
}

async fn run(args: Args) -> Result<(), anyhow::Error> {
    let registry = MetricsRegistry::new();
    let _tracing = args
        .tracing
        .configure_tracing(
            StaticTracingConfig {
                service_name: "pubsubd",
                build_info: BUILD_INFO,
            },
            registry.clone(),
        )
        .await?;
    register_runtime_metrics(
        "main",
        tokio::runtime::Handle::current().metrics(),
        &registry,
    );
    mz_alloc::register_metrics_into(&registry).await;

    let config = PersistConfig::new_default_configs(&BUILD_INFO, SYSTEM_TIME.clone());
    let relay = PersistGrpcPubSubServer::new(&config, &registry);
    let router = Router::new()
        .route("/metrics", get(metrics))
        .route("/api/livez", get(mz_http_util::handle_liveness_check))
        .with_state(registry);
    let listener = tokio::net::TcpListener::bind(args.internal_http_listen_addr)
        .await
        .context("binding pubsub internal HTTP listener")?;

    tokio::select! {
        result = relay.serve(args.listen_addr) => result.context("serving Persist pubsub"),
        result = async { axum::serve(listener, router).await } => result.context("serving pubsub internal HTTP"),
    }
}

async fn metrics(State(registry): State<MetricsRegistry>, headers: HeaderMap) -> impl IntoResponse {
    mz_http_util::handle_prometheus(&registry, headers).await
}

#[cfg(test)]
mod tests {
    use clap::Parser;

    use super::Args;

    #[mz_ore::test]
    fn backend_launch_arguments() {
        let args = Args::try_parse_from([
            "pubsubd",
            "--process=0",
            "--secrets-reader-name-prefix=environment",
            "--listen-addr=127.0.0.1:17000",
            "--internal-http-listen-addr=127.0.0.1:17001",
        ])
        .expect("single-process backend launch");
        assert_eq!(args.listen_addr.port(), 17000);
        assert_eq!(args.internal_http_listen_addr.port(), 17001);
        assert!(Args::try_parse_from(["pubsubd", "--process=1"]).is_err());
    }
}

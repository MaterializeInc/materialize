// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::{
    future,
    net::SocketAddr,
    sync::{Arc, LazyLock},
};

use k8s_openapi::apiextensions_apiserver::pkg::apis::apiextensions::v1::CustomResourceColumnDefinition;
use kube::runtime::watcher;
use tracing::info;

use mz_build_info::{BuildInfo, build_info};
use mz_orchestrator_kubernetes::util::create_client;
use mz_orchestrator_tracing::{StaticTracingConfig, TracingCliArgs};
use mz_orchestratord::{
    controller,
    k8s::register_crds,
    metrics::{self, Metrics},
};
use mz_ore::{
    cli::{self, CliConfig},
    error::ErrorExt,
    metrics::MetricsRegistry,
};

const BUILD_INFO: BuildInfo = build_info!();
static VERSION: LazyLock<String> = LazyLock::new(|| BUILD_INFO.human_version(None));

#[derive(clap::Parser)]
#[clap(name = "orchestratord", version = VERSION.as_str())]
pub struct Args {
    #[structopt(long, env = "KUBERNETES_CONTEXT", default_value = "minikube")]
    kubernetes_context: String,

    #[clap(long, default_value = "[::]:8004")]
    profiling_listen_address: SocketAddr,
    #[clap(long, default_value = "[::]:3100")]
    metrics_listen_address: SocketAddr,

    #[clap(flatten)]
    materialize_controller_args: controller::materialize::MaterializeControllerArgs,

    #[clap(flatten)]
    tracing: TracingCliArgs,

    #[clap(long, hide = true, value_parser(parse_crd_columns))]
    additional_crd_columns: Option<std::vec::Vec<CustomResourceColumnDefinition>>,
}

fn parse_crd_columns(val: &str) -> Result<Vec<CustomResourceColumnDefinition>, serde_json::Error> {
    serde_json::from_str(val)
}

#[tokio::main]
async fn main() {
    mz_ore::panic::install_enhanced_handler();

    let args = cli::parse_args(CliConfig {
        env_prefix: Some("ORCHESTRATORD_"),
        enable_version_flag: true,
    });
    if let Err(err) = run(args).await {
        panic!("orchestratord: fatal: {}", err.display_with_causes());
    }
}

async fn run(args: Args) -> Result<(), anyhow::Error> {
    let metrics_registry = MetricsRegistry::new();
    let (_, _tracing_guard) = args
        .tracing
        .configure_tracing(
            StaticTracingConfig {
                service_name: "orchestratord",
                build_info: BUILD_INFO,
            },
            metrics_registry.clone(),
        )
        .await?;

    let metrics = Arc::new(Metrics::register_into(&metrics_registry));

    let (client, namespace) = create_client(args.kubernetes_context.clone()).await?;
    register_crds(
        client.clone(),
        args.additional_crd_columns.unwrap_or_default(),
    )
    .await?;

    {
        let router = mz_prof_http::router(&BUILD_INFO);
        let address = args.profiling_listen_address.clone();
        mz_ore::task::spawn(|| "profiling API internal web server", async move {
            if let Err(e) = axum::serve(
                tokio::net::TcpListener::bind(&address).await.unwrap(),
                router.into_make_service(),
            )
            .await
            {
                panic!(
                    "profiling API internal web server failed: {}",
                    e.display_with_causes()
                );
            }
        });
    }

    {
        let router = metrics::router(metrics_registry.clone());
        let address = args.metrics_listen_address.clone();
        mz_ore::task::spawn(|| "metrics server", async move {
            if let Err(e) = axum::serve(
                tokio::net::TcpListener::bind(&address).await.unwrap(),
                router.into_make_service(),
            )
            .await
            {
                panic!("metrics server failed: {}", e.display_with_causes());
            }
        });
    }

    mz_ore::task::spawn(
        || "materialize controller",
        k8s_controller::Controller::namespaced_all(
            client.clone(),
            mz_orchestratord::controller::materialize::Context::new(
                args.materialize_controller_args,
                args.tracing,
                namespace,
                Arc::clone(&metrics),
                client.clone(),
            )
            .await,
            watcher::Config::default().timeout(29),
        )
        .run(),
    );

    info!("All tasks started successfully.");

    future::pending().await
}

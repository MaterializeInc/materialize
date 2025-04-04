// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Debug tool for self managed environments.
use std::path::PathBuf;
use std::process;
use std::sync::LazyLock;

use chrono::{DateTime, Utc};
use clap::Parser;
use kube::config::KubeConfigOptions;
use kube::{Client, Config};
use mz_build_info::{build_info, BuildInfo};
use mz_ore::cli::{self, CliConfig};
use mz_ore::error::ErrorExt;
use mz_ore::task;
use tracing::{error, info};
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::EnvFilter;

use crate::docker_dumper::DockerDumper;
use crate::k8s_dumper::K8sDumper;
use crate::kubectl_port_forwarder::create_kubectl_port_forwarder;
use crate::utils::{create_tracing_log_file, validate_pg_connection_string, zip_debug_folder};

mod docker_dumper;
mod k8s_dumper;
mod kubectl_port_forwarder;
mod system_catalog_dumper;
mod utils;

const BUILD_INFO: BuildInfo = build_info!();
static VERSION: LazyLock<String> = LazyLock::new(|| BUILD_INFO.human_version(None));
static ENV_FILTER: &str = "mz_debug=info";

#[derive(Parser, Debug, Clone)]
pub struct SelfManagedDebugMode {
    // === Kubernetes options. ===
    /// If true, the tool will not dump the Kubernetes cluster and will not port-forward the external SQL port.
    #[clap(long, default_value = "false")]
    skip_k8s_dump: bool,
    /// A list of namespaces to dump.
    #[clap(long= "k8s-namespace", required_unless_present = "skip_k8s_dump", action = clap::ArgAction::Append)]
    k8s_namespaces: Vec<String>,
    /// The kubernetes context to use.
    #[clap(long, env = "KUBERNETES_CONTEXT")]
    k8s_context: Option<String>,
    /// If true, the tool will dump the values of secrets in the Kubernetes cluster.
    #[clap(long, default_value = "false")]
    k8s_dump_secret_values: bool,
    /// If true, the tool will automatically port-forward the external SQL port in the Kubernetes cluster.
    #[clap(long, default_value = "false")]
    skip_auto_port_forward: bool,
    /// The address to listen on for the port-forward.
    #[clap(long, default_value = "localhost")]
    auto_port_forward_address: String,
    /// The port to listen on for the port-forward.
    #[clap(long, default_value = "6875")]
    auto_port_forward_port: i32,
}

#[derive(Parser, Debug, Clone)]
pub struct EmulatorDebugMode {
    /// If true, the tool will not dump the docker container.
    #[clap(long, default_value = "false")]
    skip_docker_dump: bool,
    /// The ID of the docker container to dump.
    #[clap(long, required_unless_present = "skip_docker_dump")]
    docker_container_id: Option<String>,
}

#[derive(Parser, Debug, Clone)]
pub enum DebugMode {
    /// Debug self-managed environments
    SelfManaged(SelfManagedDebugMode),
    /// Debug emulator environments
    Emulator(EmulatorDebugMode),
}

#[derive(Parser, Debug, Clone)]
#[clap(name = "mz-debug", next_line_help = true, version = VERSION.as_str())]
pub struct Args {
    #[clap(subcommand)]
    debug_mode: DebugMode,
    /// The URL of the SQL connection used to dump the system catalog.
    /// An example URL is `postgres://root@localhost:6875/materialize?sslmode=disable`.
    // TODO(debug_tool3): Allow users to specify the pgconfig via separate variables
    #[clap(long, required = true, value_parser = validate_pg_connection_string)]
    url: String,
    /// The name of the zip file to create. Should end with `.zip`.
    #[clap(long, default_value = "mz-debug.zip")]
    zip_file_name: String,
    /// If true, the tool will not dump the system catalog.
    #[clap(long, default_value = "false")]
    skip_system_catalog_dump: bool,
}

pub trait ContainerDumper {
    fn dump_container_resources(&self) -> impl std::future::Future<Output = ()>;
}
pub enum ContainerServiceDumper<'n> {
    K8s(K8sDumper<'n>),
    Docker(DockerDumper),
}

impl<'n> ContainerServiceDumper<'n> {
    fn new_k8s_dumper(
        context: &'n Context,
        client: Client,
        k8s_namespaces: Vec<String>,
        k8s_context: Option<String>,
        k8s_dump_secret_values: bool,
    ) -> Self {
        Self::K8s(K8sDumper::new(
            context,
            client,
            k8s_namespaces,
            k8s_context,
            k8s_dump_secret_values,
        ))
    }

    fn new_docker_dumper(context: &'n Context, docker_container_id: String) -> Self {
        Self::Docker(DockerDumper::new(context, docker_container_id))
    }
}

impl<'n> ContainerDumper for ContainerServiceDumper<'n> {
    async fn dump_container_resources(&self) {
        match self {
            ContainerServiceDumper::K8s(dumper) => dumper.dump_container_resources().await,
            ContainerServiceDumper::Docker(dumper) => dumper.dump_container_resources().await,
        }
    }
}

#[derive(Clone)]
pub struct Context {
    start_time: DateTime<Utc>,
}

#[tokio::main]
async fn main() {
    let args: Args = cli::parse_args(CliConfig {
        env_prefix: Some("MZ_DEBUG_"),
        enable_version_flag: true,
    });

    let start_time = Utc::now();

    // We use tracing_subscriber to display the output of tracing to stdout
    // and log to a file included in the debug zip.
    let stdout_layer = tracing_subscriber::fmt::layer()
        .with_target(false)
        .without_time();

    if let Ok(file) = create_tracing_log_file(start_time) {
        let file_layer = tracing_subscriber::fmt::layer()
            .with_writer(file)
            .with_ansi(false);

        let _ = tracing_subscriber::registry()
            .with(EnvFilter::new(ENV_FILTER))
            .with(stdout_layer)
            .with(file_layer)
            .try_init();
    } else {
        let _ = tracing_subscriber::registry()
            .with(EnvFilter::new(ENV_FILTER))
            .with(stdout_layer)
            .try_init();
    }

    let context = Context { start_time };

    if let Err(err) = run(context, args).await {
        error!(
            "mz-debug: fatal: {}\nbacktrace: {}",
            err.display_with_causes(),
            err.backtrace()
        );
        process::exit(1);
    }
}

async fn run(context: Context, args: Args) -> Result<(), anyhow::Error> {
    // Depending on if the user is debugging either a k8s environments or docker environment,
    // dump the respective system's resources.
    let container_system_dumper = match args.debug_mode {
        DebugMode::SelfManaged(args) => {
            if args.skip_k8s_dump {
                None
            } else {
                let client = match create_k8s_client(args.k8s_context.clone()).await {
                    Ok(client) => client,
                    Err(e) => {
                        error!("Failed to create k8s client: {}", e);
                        return Err(e);
                    }
                };

                if !args.skip_auto_port_forward {
                    let port_forwarder = create_kubectl_port_forwarder(&client, &args).await?;
                    task::spawn(|| "port-forwarding", async move {
                        port_forwarder.port_forward().await;
                    });
                    // There may be a delay between when the port forwarding process starts and when it's ready
                    // to use. We wait a few seconds to ensure that port forwarding is ready.
                    tokio::time::sleep(std::time::Duration::from_secs(2)).await;
                }

                let dumper = ContainerServiceDumper::new_k8s_dumper(
                    &context,
                    client,
                    args.k8s_namespaces,
                    args.k8s_context,
                    args.k8s_dump_secret_values,
                );
                Some(dumper)
            }
        }
        DebugMode::Emulator(args) => {
            if args.skip_docker_dump {
                None
            } else {
                let docker_container_id = args
                    .docker_container_id
                    .expect("docker_container_id is required");
                let dumper =
                    ContainerServiceDumper::new_docker_dumper(&context, docker_container_id);
                Some(dumper)
            }
        }
    };

    if let Some(dumper) = container_system_dumper {
        dumper.dump_container_resources().await;
    }

    if !args.skip_system_catalog_dump {
        // Dump the system catalog.
        let catalog_dumper =
            match system_catalog_dumper::SystemCatalogDumper::new(&context, &args.url).await {
                Ok(dumper) => Some(dumper),
                Err(e) => {
                    error!("Failed to dump system catalog: {}", e);
                    None
                }
            };

        if let Some(dumper) = catalog_dumper {
            dumper.dump_all_relations().await;
        }
    }

    info!("Zipping debug directory");

    if let Err(e) = zip_debug_folder(PathBuf::from(&args.zip_file_name)) {
        error!("Failed to zip debug directory: {}", e);
    } else {
        info!("Created zip debug at {}", &args.zip_file_name);
    }

    Ok(())
}

/// Creates a k8s client given a context. If no context is provided, the default context is used.
async fn create_k8s_client(k8s_context: Option<String>) -> Result<Client, anyhow::Error> {
    let kubeconfig_options = KubeConfigOptions {
        context: k8s_context,
        ..Default::default()
    };

    let kubeconfig = Config::from_kubeconfig(&kubeconfig_options).await?;

    let client = Client::try_from(kubeconfig)?;

    Ok(client)
}

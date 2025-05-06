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
use mz_build_info::{BuildInfo, build_info};
use mz_ore::cli::{self, CliConfig};
use mz_ore::error::ErrorExt;
use tracing::{error, info, warn};
use tracing_subscriber::EnvFilter;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

use crate::docker_dumper::DockerDumper;
use crate::k8s_dumper::K8sDumper;
use crate::kubectl_port_forwarder::create_pg_wire_port_forwarder;
use crate::utils::{
    create_tracing_log_file, format_base_path, validate_pg_connection_string, zip_debug_folder,
};

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
    /// If true, the tool will dump debug information in Kubernetes cluster such as logs, pod describes, etc.
    #[clap(long, default_value = "true", action = clap::ArgAction::Set)]
    dump_k8s: bool,
    /// A list of namespaces to dump.
    #[clap(
        long = "k8s-namespace",
        // We require both `require`s because `required_if_eq` doesn't work for default values.
        required_unless_present = "dump_k8s",
        required_if_eq("dump_k8s", "true"),
        action = clap::ArgAction::Append
    )]
    k8s_namespaces: Vec<String>,
    /// The kubernetes context to use.
    #[clap(long, env = "KUBERNETES_CONTEXT")]
    k8s_context: Option<String>,
    /// If true, the tool will dump the values of secrets in the Kubernetes cluster.
    #[clap(long, default_value = "false", action = clap::ArgAction::Set)]
    k8s_dump_secret_values: bool,
    // TODO (debug_tool1): Convert port forwarding variables into a map since we'll be
    // portforwarding multiple times
    /// If true, the tool will automatically port-forward the external SQL port in the Kubernetes cluster.
    /// If dump_k8s is false however, we will not automatically port-forward.
    #[clap(long, default_value = "true", action = clap::ArgAction::Set)]
    auto_port_forward: bool,
    /// The address to listen on for the port-forward.
    #[clap(long, default_value = "127.0.0.1")]
    port_forward_local_address: String,
    /// The port to listen on for the port-forward.
    #[clap(long, default_value = "6875")]
    port_forward_local_port: i32,
    /// The URL of the Materialize SQL connection used to dump the system catalog.
    /// An example URL is `postgres://root@127.0.0.1:6875/materialize?sslmode=disable`.
    /// By default, we will create a connection URL based on `port_forward_local_address` and `port_forward_local_port`.
    // TODO(debug_tool3): Allow users to specify the pgconfig via separate variables
    #[clap(
        long,
        env = "MZ_CONNECTION_URL",
        value_parser = validate_pg_connection_string,
    )]
    mz_connection_url: Option<String>,
}

#[derive(Parser, Debug, Clone)]
pub struct EmulatorDebugMode {
    /// If true, the tool will dump debug information of the docker container.
    #[clap(long, default_value = "true", action = clap::ArgAction::Set)]
    dump_docker: bool,
    /// The ID of the docker container to dump.
    #[clap(
        long,
        // We require both `require`s because `required_if_eq` doesn't work for default values.
        required_unless_present = "dump_docker",
        required_if_eq("dump_docker", "true")
    )]
    docker_container_id: Option<String>,
    /// The URL of the Materialize SQL connection used to dump the system catalog.
    /// An example URL is `postgres://root@127.0.0.1:6875/materialize?sslmode=disable`.
    // TODO(debug_tool3): Allow users to specify the pgconfig via separate variables
    #[clap(
        long,
        env = "MZ_CONNECTION_URL",
        // We assume that the emulator is running on the default port.
        default_value = "postgres://127.0.0.1:6875/materialize?sslmode=prefer",
        value_parser = validate_pg_connection_string,
    )]
    mz_connection_url: String,
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
    /// If true, the tool will dump the system catalog in Materialize.
    #[clap(long, default_value = "true", action = clap::ArgAction::Set, global = true)]
    dump_system_catalog: bool,
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
        // mz_ore::cli::parse_args' env_prefix doesn't apply for subcommand flags. Thus
        // we manually set each env_prefix to MZ_ for each flag.
        env_prefix: None,
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
    // dump the respective system's resources and spin up a port forwarder if auto port forwarding.
    // We'd like to keep the port forwarder alive until the end of the program.
    let (container_system_dumper, _pg_wire_port_forward_connection) = match &args.debug_mode {
        DebugMode::SelfManaged(args) => {
            if args.dump_k8s {
                let client = match create_k8s_client(args.k8s_context.clone()).await {
                    Ok(client) => client,
                    Err(e) => {
                        error!("Failed to create k8s client: {}", e);
                        return Err(e);
                    }
                };
                let port_forward_connection = if args.auto_port_forward {
                        let port_forwarder = create_pg_wire_port_forwarder(&client, args).await?;

                    match port_forwarder.spawn_port_forward().await {
                        Ok(connection) => Some(connection),
                        Err(err) => {
                            warn!("{}", err);
                            None
                        }
                    }
                } else {
                    None
                };
                // Parker: Return it as a tuple (dumper, port_forwarder)
                let dumper = ContainerServiceDumper::new_k8s_dumper(
                    &context,
                    client,
                    args.k8s_namespaces.clone(),
                    args.k8s_context.clone(),
                    args.k8s_dump_secret_values,
                );
                (Some(dumper), port_forward_connection)
            } else {
                (None, None)
            }
        }
        DebugMode::Emulator(args) => {
            if args.dump_docker {
                let docker_container_id = args
                    .docker_container_id
                    .clone()
                    .expect("docker_container_id is required");
                let dumper =
                    ContainerServiceDumper::new_docker_dumper(&context, docker_container_id);
                (Some(dumper), None)
            } else {
                (None, None)
            }
        }
    };

    if let Some(dumper) = container_system_dumper {
        dumper.dump_container_resources().await;
    }

    let connection_url = match &args.debug_mode {
        DebugMode::SelfManaged(args) => kubectl_port_forwarder::create_mz_connection_url(
            args.port_forward_local_address.clone(),
            args.port_forward_local_port,
            args.mz_connection_url.clone(),
        ),
        DebugMode::Emulator(args) => args.mz_connection_url.clone(),
    };
    if args.dump_system_catalog {
        // Dump the system catalog.
        let catalog_dumper = match system_catalog_dumper::SystemCatalogDumper::new(
            &context,
            &connection_url,
        )
        .await
        {
            Ok(dumper) => Some(dumper),
            Err(e) => {
                warn!("Failed to dump system catalog: {}", e);
                None
            }
        };

        if let Some(dumper) = catalog_dumper {
            dumper.dump_all_relations().await;
        }
    }

    info!("Zipping debug directory");

    let base_path = format_base_path(context.start_time);

    let zip_file_name = format!("{}.zip", &base_path.display());

    if let Err(e) = zip_debug_folder(PathBuf::from(&zip_file_name), &base_path) {
        warn!("Failed to zip debug directory: {}", e);
    } else {
        info!("Created zip debug at {}", &zip_file_name);
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

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

use anyhow::Context as AnyhowContext;
use chrono::Utc;
use clap::Parser;
use kube::config::KubeConfigOptions;
use kube::{Client as KubernetesClient, Config};
use mz_build_info::{BuildInfo, build_info};
use mz_ore::cli::{self, CliConfig};
use mz_ore::error::ErrorExt;
use tracing::{error, info, warn};
use tracing_subscriber::EnvFilter;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

use crate::docker_dumper::DockerDumper;
use crate::internal_http_dumper::{dump_emulator_http_resources, dump_self_managed_http_resources};
use crate::k8s_dumper::K8sDumper;
use crate::kubectl_port_forwarder::{PortForwardConnection, create_pg_wire_port_forwarder};
use crate::utils::{
    create_tracing_log_file, format_base_path, validate_pg_connection_string, zip_debug_folder,
};

mod docker_dumper;
mod internal_http_dumper;
mod k8s_dumper;
mod kubectl_port_forwarder;
mod system_catalog_dumper;
mod utils;

const BUILD_INFO: BuildInfo = build_info!();
static VERSION: LazyLock<String> = LazyLock::new(|| BUILD_INFO.human_version(None));
static ENV_FILTER: &str = "mz_debug=info";

#[derive(Parser, Debug, Clone)]
pub struct SelfManagedDebugModeArgs {
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
}

#[derive(Parser, Debug, Clone)]
pub struct EmulatorDebugModeArgs {
    /// If true, the tool will dump debug information of the docker container.
    #[clap(long, default_value = "true", action = clap::ArgAction::Set)]
    dump_docker: bool,
    /// The ID of the docker container to dump.
    #[clap(long)]
    docker_container_id: String,
}

#[derive(Parser, Debug, Clone)]
pub enum DebugModeArgs {
    /// Debug self-managed environments
    SelfManaged(SelfManagedDebugModeArgs),
    /// Debug emulator environments
    Emulator(EmulatorDebugModeArgs),
}

#[derive(Parser, Debug, Clone)]
#[clap(name = "mz-debug", next_line_help = true, version = VERSION.as_str())]
pub struct Args {
    #[clap(subcommand)]
    debug_mode_args: DebugModeArgs,
    /// If true, the tool will dump the system catalog in Materialize.
    #[clap(long, default_value = "true", action = clap::ArgAction::Set, global = true)]
    dump_system_catalog: bool,
    /// If true, the tool will dump the heap profiles in Materialize.
    #[clap(long, default_value = "true", action = clap::ArgAction::Set, global = true)]
    dump_heap_profiles: bool,
    /// If true, the tool will dump the prometheus metrics in Materialize.
    #[clap(long, default_value = "true", action = clap::ArgAction::Set, global = true)]
    dump_prometheus_metrics: bool,
    /// The URL of the Materialize SQL connection used to dump the system catalog.
    /// An example URL is `postgres://root@127.0.0.1:6875/materialize?sslmode=disable`.
    /// By default, we will create a connection URL based on `port_forward_local_address:port_forward_local_port` for self-managed
    /// or `docker_container_ip:6875` for the emulator.
    // TODO(debug_tool3): Allow users to specify the pgconfig via separate variables
    #[clap(
        long,
        env = "MZ_CONNECTION_URL",
        value_parser = validate_pg_connection_string,
        global = true
    )]
    mz_connection_url: Option<String>,
}

pub trait ContainerDumper {
    fn dump_container_resources(&self) -> impl std::future::Future<Output = ()>;
}

struct SelfManagedContext {
    dump_k8s: bool,
    k8s_client: KubernetesClient,
    k8s_context: Option<String>,
    k8s_namespaces: Vec<String>,
    k8s_dump_secret_values: bool,

    _mz_port_forward_process: Option<PortForwardConnection>,
}
#[derive(Clone)]
struct EmulatorContext {
    dump_docker: bool,
    docker_container_id: String,
    container_ip: String,
}

enum DebugModeContext {
    SelfManaged(SelfManagedContext),
    Emulator(EmulatorContext),
}

pub struct Context {
    base_path: PathBuf,
    debug_mode_context: DebugModeContext,

    mz_connection_url: String,
    dump_system_catalog: bool,
    dump_heap_profiles: bool,
    dump_prometheus_metrics: bool,
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
    let base_path = format_base_path(start_time);

    // We use tracing_subscriber to display the output of tracing to stdout
    // and log to a file included in the debug zip.
    let stdout_layer = tracing_subscriber::fmt::layer()
        .with_target(false)
        .without_time();

    if let Ok(file) = create_tracing_log_file(base_path.clone()) {
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

    let initialize_then_run = async move {
        // Preprocess args into contexts
        let context = initialize_context(args, base_path).await?;
        run(context).await
    };

    if let Err(err) = initialize_then_run.await {
        error!(
            "mz-debug: fatal: {}\nbacktrace: {}",
            err.display_with_causes(),
            err.backtrace()
        );
        process::exit(1);
    }
}

fn create_mz_connection_url(
    local_address: String,
    local_port: i32,
    connection_url_override: Option<String>,
) -> String {
    if let Some(connection_url_override) = connection_url_override {
        return connection_url_override;
    }
    format!("postgres://{}:{}?sslmode=prefer", local_address, local_port)
}

async fn initialize_context(
    global_args: Args,
    base_path: PathBuf,
) -> Result<Context, anyhow::Error> {
    let (debug_mode_context, mz_connection_url) = match &global_args.debug_mode_args {
        DebugModeArgs::SelfManaged(args) => {
            let k8s_client = match create_k8s_client(args.k8s_context.clone()).await {
                Ok(k8s_client) => k8s_client,
                Err(e) => {
                    error!("Failed to create k8s client: {}", e);
                    return Err(e);
                }
            };

            let mz_port_forward_process = if args.auto_port_forward {
                let port_forwarder = create_pg_wire_port_forwarder(
                    &k8s_client,
                    &args.k8s_context,
                    &args.k8s_namespaces,
                    &args.port_forward_local_address,
                    args.port_forward_local_port,
                )
                .await?;

                match port_forwarder.spawn_port_forward().await {
                    Ok(process) => Some(process),
                    Err(err) => {
                        warn!("{:#}", err);
                        None
                    }
                }
            } else {
                None
            };
            let mz_connection_url = create_mz_connection_url(
                args.port_forward_local_address.clone(),
                args.port_forward_local_port,
                global_args.mz_connection_url.clone(),
            );
            (
                DebugModeContext::SelfManaged(SelfManagedContext {
                    dump_k8s: args.dump_k8s,
                    k8s_client,
                    k8s_context: args.k8s_context.clone(),
                    k8s_namespaces: args.k8s_namespaces.clone(),
                    k8s_dump_secret_values: args.k8s_dump_secret_values,
                    _mz_port_forward_process: mz_port_forward_process,
                }),
                mz_connection_url,
            )
        }
        DebugModeArgs::Emulator(args) => {
            let container_ip = docker_dumper::get_container_ip(&args.docker_container_id)
                .await
                .with_context(|| {
                    format!(
                        "Failed to get IP for container {}",
                        args.docker_container_id
                    )
                })?;

            let mz_connection_url = create_mz_connection_url(
                container_ip.clone(),
                6875,
                global_args.mz_connection_url.clone(),
            );

            (
                DebugModeContext::Emulator(EmulatorContext {
                    dump_docker: args.dump_docker,
                    docker_container_id: args.docker_container_id.clone(),
                    container_ip,
                }),
                mz_connection_url,
            )
        }
    };

    Ok(Context {
        base_path,
        debug_mode_context,
        mz_connection_url,
        dump_system_catalog: global_args.dump_system_catalog,
        dump_heap_profiles: global_args.dump_heap_profiles,
        dump_prometheus_metrics: global_args.dump_prometheus_metrics,
    })
}

async fn run(context: Context) -> Result<(), anyhow::Error> {
    // Depending on if the user is debugging either a k8s environments or docker environment,
    // dump the respective system's resources
    match &context.debug_mode_context {
        DebugModeContext::SelfManaged(SelfManagedContext {
            k8s_client,
            dump_k8s,
            k8s_context,
            k8s_namespaces,
            k8s_dump_secret_values,
            ..
        }) => {
            if *dump_k8s {
                let dumper = K8sDumper::new(
                    &context,
                    k8s_client.clone(),
                    k8s_namespaces.clone(),
                    k8s_context.clone(),
                    *k8s_dump_secret_values,
                );
                dumper.dump_container_resources().await;
            }
        }
        DebugModeContext::Emulator(EmulatorContext {
            dump_docker,
            docker_container_id,
            ..
        }) => {
            if *dump_docker {
                let dumper = DockerDumper::new(&context, docker_container_id.clone());
                dumper.dump_container_resources().await;
            }
        }
    };

    match &context.debug_mode_context {
        DebugModeContext::SelfManaged(self_managed_context) => {
            if let Err(e) = dump_self_managed_http_resources(&context, self_managed_context).await {
                warn!("Failed to dump self-managed http resources: {:#}", e);
            }
        }
        DebugModeContext::Emulator(emulator_context) => {
            if let Err(e) = dump_emulator_http_resources(&context, emulator_context).await {
                warn!("Failed to dump emulator http resources: {:#}", e);
            }
        }
    };

    if context.dump_system_catalog {
        // Dump the system catalog.
        let catalog_dumper = match system_catalog_dumper::SystemCatalogDumper::new(
            &context.mz_connection_url,
            context.base_path.clone(),
        )
        .await
        {
            Ok(dumper) => Some(dumper),
            Err(e) => {
                warn!("Failed to dump system catalog: {:#}", e);
                None
            }
        };

        if let Some(dumper) = catalog_dumper {
            dumper.dump_all_relations().await;
        }
    }

    info!("Zipping debug directory");

    let zip_file_name = format!("{}.zip", &context.base_path.display());

    if let Err(e) = zip_debug_folder(PathBuf::from(&zip_file_name), &context.base_path) {
        warn!("Failed to zip debug directory: {:#}", e);
    } else {
        info!("Created zip debug at {:#}", &zip_file_name);
    }

    Ok(())
}

/// Creates a k8s client given a context. If no context is provided, the default context is used.
async fn create_k8s_client(k8s_context: Option<String>) -> Result<KubernetesClient, anyhow::Error> {
    let kubeconfig_options = KubeConfigOptions {
        context: k8s_context,
        ..Default::default()
    };

    let kubeconfig = Config::from_kubeconfig(&kubeconfig_options).await?;

    let client = KubernetesClient::try_from(kubeconfig)?;

    Ok(client)
}

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
    create_tracing_log_file, format_base_path, get_k8s_auth_mode, validate_pg_connection_string,
    zip_debug_folder,
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
pub static DEFAULT_MZ_ENVIRONMENTD_PORT: i32 = 6875;

#[derive(Parser, Debug, Clone)]
pub struct SelfManagedDebugModeArgs {
    // === Kubernetes options. ===
    /// If true, the tool will dump debug information in Kubernetes cluster such as logs, pod describes, etc.
    #[clap(long, default_value = "true", action = clap::ArgAction::Set)]
    dump_k8s: bool,

    /// The k8s namespace that the Materialize instance is running in. This is necessary to interact
    /// with the k8s API for gathering logs, port forwarding information, and information about the user's
    /// environment.
    #[clap(long)]
    k8s_namespace: String,
    /// The name of the Materialize instance to target.
    #[clap(long)]
    mz_instance_name: String,
    /// A list of namespaces to dump.
    #[clap(
        long = "additional-k8s-namespace",
        action = clap::ArgAction::Append
    )]
    additional_k8s_namespaces: Option<Vec<String>>,
    /// The kubernetes context to use.
    #[clap(long, env = "KUBERNETES_CONTEXT")]
    k8s_context: Option<String>,
    /// If true, the tool will dump the values of secrets in the Kubernetes cluster.
    #[clap(long, default_value = "false", action = clap::ArgAction::Set)]
    k8s_dump_secret_values: bool,
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
    /// The username to use to connect to Materialize,
    #[clap(long, env = "MZ_USERNAME", global = true)]
    mz_username: Option<String>,
    /// The password to use to connect to Materialize if the authenticator kind is Password.
    #[clap(long, env = "MZ_PASSWORD", global = true)]
    mz_password: Option<String>,
    /// The URL of the Materialize SQL connection used to dump the system catalog.
    /// An example URL is `postgres://root@127.0.0.1:6875/materialize?sslmode=disable`.
    /// This acts as an override. By default, we will connect to the auto-port-forwarded connection for self-managed
    /// or `<docker_container_ip>:6875` for the emulator.
    /// If defined, `mz_username` and `mz_password` flags are ignored.
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

#[derive(Debug, Clone)]
pub struct PasswordAuthCredentials {
    pub username: String,
    pub password: String,
}

#[derive(Debug, Clone)]
pub enum AuthMode {
    None,
    Password(PasswordAuthCredentials),
}

struct PortForwardConnectionInfo {
    connection: PortForwardConnection,
    auth_mode: AuthMode,
}

enum SelfManagedMzConnectionInfo {
    PortForward(PortForwardConnectionInfo),
    ConnectionUrlOverride(String),
}

struct SelfManagedContext {
    dump_k8s: bool,
    k8s_client: KubernetesClient,
    k8s_context: Option<String>,
    k8s_namespace: String,
    mz_instance_name: String,
    k8s_additional_namespaces: Option<Vec<String>>,
    k8s_dump_secret_values: bool,
    mz_connection_info: SelfManagedMzConnectionInfo,
    http_connection_auth_mode: AuthMode,
}

#[derive(Debug, Clone)]
struct ContainerIpInfo {
    local_address: String,
    local_port: i32,
    auth_mode: AuthMode,
}

#[derive(Debug, Clone)]
enum EmulatorMzConnectionInfo {
    ContainerIp(ContainerIpInfo),
    ConnectionUrlOverride(String),
}

#[derive(Debug, Clone)]
struct EmulatorContext {
    dump_docker: bool,
    docker_container_id: String,
    container_ip: String,
    mz_connection_info: EmulatorMzConnectionInfo,
    http_connection_auth_mode: AuthMode,
}

enum DebugModeContext {
    SelfManaged(SelfManagedContext),
    Emulator(EmulatorContext),
}

pub struct Context {
    base_path: PathBuf,
    debug_mode_context: DebugModeContext,
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
    credentials: Option<PasswordAuthCredentials>,
) -> String {
    let password_auth_segment = if let Some(credentials) = credentials {
        format!("{}:{}@", credentials.username, credentials.password)
    } else {
        "".to_string()
    };
    format!(
        "postgres://{}{}:{}?sslmode=prefer",
        password_auth_segment, local_address, local_port
    )
}

async fn initialize_context(
    global_args: Args,
    base_path: PathBuf,
) -> Result<Context, anyhow::Error> {
    let debug_mode_context = match &global_args.debug_mode_args {
        DebugModeArgs::SelfManaged(args) => {
            let k8s_client = match create_k8s_client(args.k8s_context.clone()).await {
                Ok(k8s_client) => k8s_client,
                Err(e) => {
                    error!("Failed to create k8s client: {}", e);
                    return Err(e);
                }
            };

            let auth_mode = match get_k8s_auth_mode(
                global_args.mz_username,
                global_args.mz_password,
                &k8s_client,
                &args.k8s_namespace,
                &args.mz_instance_name,
            )
            .await
            {
                Ok(auth_mode) => auth_mode,
                Err(e) => {
                    warn!("Failed to get auth mode from k8s: {:#}", e);
                    // By default, set auth mode to None.
                    AuthMode::None
                }
            };

            let mz_connection_info = if let Some(mz_connection_url) = global_args.mz_connection_url
            {
                // If the user provides a connection URL, don't bother port forwarding.
                SelfManagedMzConnectionInfo::ConnectionUrlOverride(mz_connection_url)
            } else {
                let create_port_forward_connection = async || {
                    let port_forwarder = create_pg_wire_port_forwarder(
                        &k8s_client,
                        &args.k8s_context,
                        &args.k8s_namespace,
                        &args.mz_instance_name,
                    )
                    .await?;
                    port_forwarder.spawn_port_forward().await
                };

                let port_forward_connection = match create_port_forward_connection().await {
                    Ok(port_forward_connection) => port_forward_connection,
                    Err(e) => {
                        warn!(
                            "Failed to create port forward connection. Set --mz-connection-url to to a Materialize instance",
                        );
                        return Err(e);
                    }
                };

                SelfManagedMzConnectionInfo::PortForward(PortForwardConnectionInfo {
                    connection: port_forward_connection,
                    auth_mode: auth_mode.clone(),
                })
            };

            DebugModeContext::SelfManaged(SelfManagedContext {
                dump_k8s: args.dump_k8s,
                k8s_client,
                k8s_context: args.k8s_context.clone(),
                k8s_namespace: args.k8s_namespace.clone(),
                mz_instance_name: args.mz_instance_name.clone(),
                k8s_additional_namespaces: args.additional_k8s_namespaces.clone(),
                k8s_dump_secret_values: args.k8s_dump_secret_values,
                mz_connection_info,
                http_connection_auth_mode: auth_mode,
            })
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

            // For the emulator, we assume if a user provides a username and password, they
            // want to use password authentication.
            // TODO (debug_tool3): Figure out the auth mode from arguments using docker inspect.
            let auth_mode = if let (Some(mz_username), Some(mz_password)) =
                (&global_args.mz_username, &global_args.mz_password)
            {
                AuthMode::Password(PasswordAuthCredentials {
                    username: mz_username.clone(),
                    password: mz_password.clone(),
                })
            } else {
                AuthMode::None
            };

            let mz_connection_info = if let Some(mz_connection_url) = global_args.mz_connection_url
            {
                EmulatorMzConnectionInfo::ConnectionUrlOverride(mz_connection_url)
            } else {
                EmulatorMzConnectionInfo::ContainerIp(ContainerIpInfo {
                    local_address: container_ip.clone(),
                    local_port: DEFAULT_MZ_ENVIRONMENTD_PORT,
                    auth_mode: auth_mode.clone(),
                })
            };

            DebugModeContext::Emulator(EmulatorContext {
                dump_docker: args.dump_docker,
                docker_container_id: args.docker_container_id.clone(),
                container_ip,
                mz_connection_info,
                http_connection_auth_mode: auth_mode,
            })
        }
    };

    Ok(Context {
        base_path,
        debug_mode_context,
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
            k8s_namespace,
            k8s_additional_namespaces,
            k8s_dump_secret_values,
            ..
        }) => {
            if *dump_k8s {
                let dumper = K8sDumper::new(
                    &context,
                    k8s_client.clone(),
                    k8s_namespace.clone(),
                    k8s_additional_namespaces.clone(),
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
        let connection_url = match &context.debug_mode_context {
            DebugModeContext::SelfManaged(self_managed_context) => {
                match &self_managed_context.mz_connection_info {
                    SelfManagedMzConnectionInfo::PortForward(port_forward) => {
                        let credentials = match &port_forward.auth_mode {
                            AuthMode::Password(credentials) => Some(credentials.clone()),
                            AuthMode::None => None,
                        };
                        create_mz_connection_url(
                            port_forward.connection.local_address.clone(),
                            port_forward.connection.local_port,
                            credentials,
                        )
                    }
                    SelfManagedMzConnectionInfo::ConnectionUrlOverride(connection_url) => {
                        connection_url.clone()
                    }
                }
            }
            DebugModeContext::Emulator(emulator_context) => {
                match &emulator_context.mz_connection_info {
                    EmulatorMzConnectionInfo::ContainerIp(container_ip) => {
                        let credentials = match &container_ip.auth_mode {
                            AuthMode::Password(credentials) => Some(credentials.clone()),
                            AuthMode::None => None,
                        };
                        create_mz_connection_url(
                            container_ip.local_address.clone(),
                            container_ip.local_port,
                            credentials,
                        )
                    }
                    EmulatorMzConnectionInfo::ConnectionUrlOverride(connection_url) => {
                        connection_url.clone()
                    }
                }
            }
        };
        let catalog_dumper = match system_catalog_dumper::SystemCatalogDumper::new(
            &connection_url,
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

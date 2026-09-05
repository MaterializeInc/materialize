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
use mz_build_info::{BuildInfo, build_info};
use mz_ore::cli::{self, CliConfig};
use mz_ore::error::ErrorExt;
use tracing::{error, info, warn};
use tracing_subscriber::EnvFilter;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

use crate::collector::CollectorArgs;
use crate::docker_dumper::DockerDumper;
use crate::internal_http_dumper::dump_emulator_http_resources;
use crate::self_managed::SelfManagedDebugModeArgs;
use crate::utils::{
    create_tracing_log_file, format_base_path, validate_pg_connection_string, zip_debug_folder,
};

mod collector;
mod describe;
mod docker_dumper;
mod internal_http_dumper;
mod k8s_dumper;
mod kubectl_port_forwarder;
mod self_managed;
mod system_catalog_dumper;
mod utils;

const BUILD_INFO: BuildInfo = build_info!();
static VERSION: LazyLock<String> = LazyLock::new(|| BUILD_INFO.human_version(None));
static ENV_FILTER: &str = "mz_debug=info";
pub static DEFAULT_MZ_ENVIRONMENTD_PORT: i32 = 6875;

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
    /// Debug self-managed environments by downloading snapshots from the
    /// in-cluster debug collector the Materialize operator runs.
    SelfManaged(SelfManagedDebugModeArgs),
    /// Debug emulator environments
    Emulator(EmulatorDebugModeArgs),
    /// Run as the in-cluster collector. Deployed by the Materialize operator,
    /// not meant to be invoked by hand.
    #[clap(hide = true)]
    Collector(CollectorArgs),
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
    /// If true, the tool will collect CPU profiles from Materialize. While a CPU
    /// profile is captured, memory profiling is temporarily disabled on that
    /// service and restored afterwards. Defaults to true, except for the
    /// collector's periodic snapshots, where it defaults to false.
    #[clap(long, action = clap::ArgAction::Set, global = true)]
    dump_cpu_profiles: Option<bool>,
    /// How long, in seconds, to sample each CPU profile.
    #[clap(
        long,
        default_value = "10",
        value_parser = clap::value_parser!(u64).range(1..=3600),
        global = true
    )]
    cpu_profile_duration_seconds: u64,
    /// The username to use to connect to Materialize. Emulator only: the
    /// in-cluster collector connects to a self-managed instance itself.
    #[clap(long, env = "MZ_USERNAME", global = true)]
    mz_username: Option<String>,
    /// The password to use to connect to Materialize if the authenticator kind
    /// is Password, Sasl, or Oidc. Emulator only.
    #[clap(long, env = "MZ_PASSWORD", global = true)]
    mz_password: Option<String>,
    /// The URL of the Materialize SQL connection used to dump the system catalog.
    /// An example URL is `postgres://root@127.0.0.1:6875/materialize?sslmode=disable`.
    /// This acts as an override. By default, the emulator is reached at
    /// `<docker_container_ip>:6875`. Emulator only.
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

/// What a dump collects and where it writes it.
#[derive(Debug, Clone)]
pub struct DumpConfig {
    pub base_path: PathBuf,
    pub dump_system_catalog: bool,
    pub dump_heap_profiles: bool,
    pub dump_prometheus_metrics: bool,
    pub dump_cpu_profiles: bool,
    pub cpu_profile_duration_secs: u64,
}

/// Everything an emulator run needs.
pub struct Context {
    dump: DumpConfig,
    emulator: EmulatorContext,
}

#[tokio::main]
async fn main() {
    let args: Args = cli::parse_args(CliConfig {
        // mz_ore::cli::parse_args' env_prefix doesn't apply for subcommand flags. Thus
        // we manually set each env_prefix to MZ_ for each flag.
        env_prefix: None,
        enable_version_flag: true,
    });

    let stdout_layer = tracing_subscriber::fmt::layer()
        .with_target(false)
        .without_time();

    let result = match &args.debug_mode_args {
        DebugModeArgs::Collector(collector_args) => {
            // The collector is a long-running server whose output is its pod's
            // log, so it logs with timestamps and writes no log file of its own.
            let _ = tracing_subscriber::registry()
                .with(EnvFilter::new(ENV_FILTER))
                .with(tracing_subscriber::fmt::layer().with_target(false))
                .try_init();
            collector::run(&args, collector_args).await
        }
        DebugModeArgs::SelfManaged(self_managed_args) => {
            // Nothing is collected locally, so there is no output directory
            // to write a log file into.
            let _ = tracing_subscriber::registry()
                .with(EnvFilter::new(ENV_FILTER))
                .with(stdout_layer)
                .try_init();
            self_managed::run(&args, self_managed_args).await
        }
        DebugModeArgs::Emulator(emulator_args) => {
            let start_time = Utc::now();
            let base_path = format_base_path(start_time);

            // We use tracing_subscriber to display the output of tracing to stdout
            // and log to a file included in the debug zip.
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

            let emulator_args = emulator_args.clone();
            async move {
                let context = initialize_emulator_context(&args, emulator_args, base_path).await?;
                run_emulator(context).await
            }
            .await
        }
    };

    if let Err(err) = result {
        error!(
            "mz-debug: fatal: {}\nbacktrace: {}",
            err.display_with_causes(),
            err.backtrace()
        );
        process::exit(1);
    }
}

/// Builds the connection URL for the system catalog dump.
///
/// With `credentials` the URL carries a username and password; without them
/// it carries none, which connects as the client's operating system user.
pub fn create_mz_connection_url(
    local_address: String,
    local_port: i32,
    credentials: Option<PasswordAuthCredentials>,
) -> String {
    let mut url = url::Url::parse(&format!(
        "postgres://{}:{}?sslmode=prefer",
        local_address, local_port
    ))
    .expect("static prefix is a valid URL");
    if let Some(creds) = credentials {
        url.set_username(&creds.username)
            .expect("postgres scheme allows userinfo");
        url.set_password(Some(&creds.password))
            .expect("postgres scheme allows userinfo");
    }
    url.into()
}

async fn initialize_emulator_context(
    global_args: &Args,
    args: EmulatorDebugModeArgs,
    base_path: PathBuf,
) -> Result<Context, anyhow::Error> {
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

    let mz_connection_info = if let Some(mz_connection_url) = &global_args.mz_connection_url {
        EmulatorMzConnectionInfo::ConnectionUrlOverride(mz_connection_url.clone())
    } else {
        EmulatorMzConnectionInfo::ContainerIp(ContainerIpInfo {
            local_address: container_ip.clone(),
            local_port: DEFAULT_MZ_ENVIRONMENTD_PORT,
            auth_mode: auth_mode.clone(),
        })
    };

    Ok(Context {
        dump: DumpConfig {
            base_path,
            dump_system_catalog: global_args.dump_system_catalog,
            dump_heap_profiles: global_args.dump_heap_profiles,
            dump_prometheus_metrics: global_args.dump_prometheus_metrics,
            dump_cpu_profiles: global_args.dump_cpu_profiles.unwrap_or(true),
            cpu_profile_duration_secs: global_args.cpu_profile_duration_seconds,
        },
        emulator: EmulatorContext {
            dump_docker: args.dump_docker,
            docker_container_id: args.docker_container_id.clone(),
            container_ip,
            mz_connection_info,
            http_connection_auth_mode: auth_mode,
        },
    })
}

async fn run_emulator(context: Context) -> Result<(), anyhow::Error> {
    if context.emulator.dump_docker {
        let dumper = DockerDumper::new(&context.dump, context.emulator.docker_container_id.clone());
        dumper.dump_container_resources().await;
    }

    if let Err(e) = dump_emulator_http_resources(&context.dump, &context.emulator).await {
        warn!("Failed to dump emulator http resources: {:#}", e);
    }

    if context.dump.dump_system_catalog {
        let connection_url = match &context.emulator.mz_connection_info {
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
        };
        let catalog_dumper = match system_catalog_dumper::SystemCatalogDumper::new(
            &connection_url,
            context.dump.base_path.clone(),
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

    let zip_file_name = format!("{}.zip", context.dump.base_path.display());

    if let Err(e) = zip_debug_folder(PathBuf::from(&zip_file_name), &context.dump.base_path) {
        warn!("Failed to zip debug directory: {:#}", e);
    } else {
        info!("Created zip debug at {:#}", &zip_file_name);
    }

    Ok(())
}

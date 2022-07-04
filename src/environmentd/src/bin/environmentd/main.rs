// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Manages a single Materialize environment.
//!
//! It listens for SQL connections on port 6875 (MTRL) and for HTTP connections
//! on port 6876.

use std::cmp;
use std::env;
use std::ffi::CStr;
use std::fs;
use std::iter;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::process;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use anyhow::{bail, Context};
use clap::{ArgEnum, Parser};
use fail::FailScenario;
use http::header::HeaderValue;
use itertools::Itertools;
use jsonwebtoken::DecodingKey;
use mz_persist_client::cache::PersistClientCache;
use once_cell::sync::Lazy;
use sysinfo::{CpuExt, SystemExt};
use tokio::sync::Mutex;
use tower_http::cors::{self, AllowOrigin};
use url::Url;
use uuid::Uuid;

use mz_controller::ControllerConfig;
use mz_environmentd::{SecretsControllerConfig, TlsConfig, TlsMode};
use mz_frontegg_auth::{FronteggAuthentication, FronteggConfig};
use mz_orchestrator::Orchestrator;
use mz_orchestrator_kubernetes::{
    KubernetesImagePullPolicy, KubernetesOrchestrator, KubernetesOrchestratorConfig,
};
use mz_orchestrator_process::{ProcessOrchestrator, ProcessOrchestratorConfig};
use mz_orchestrator_tracing::{TracingCliArgs, TracingOrchestrator};
use mz_ore::cgroup::{detect_memory_limit, MemoryLimit};
use mz_ore::cli::{self, CliConfig, KeyValueArg};
use mz_ore::id_gen::PortAllocator;
use mz_ore::metrics::MetricsRegistry;
use mz_ore::now::SYSTEM_TIME;
use mz_persist_client::PersistLocation;
use mz_storage::client::connections::ConnectionContext;

mod sys;

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

pub static VERSION: Lazy<String> = Lazy::new(|| mz_environmentd::BUILD_INFO.human_version());
pub static LONG_VERSION: Lazy<String> = Lazy::new(|| {
    iter::once(mz_environmentd::BUILD_INFO.human_version())
        .chain(build_info())
        .join("\n")
});

type OptionalDuration = Option<Duration>;

fn parse_optional_duration(s: &str) -> Result<OptionalDuration, anyhow::Error> {
    match s {
        "off" => Ok(None),
        _ => Ok(Some(mz_repr::util::parse_duration(s)?)),
    }
}

/// Manages a single Materialize environment.
#[derive(Parser, Debug)]
#[clap(
    name = "environmentd",
    next_line_help = true,
    version = VERSION.as_str(),
    long_version = LONG_VERSION.as_str(),
)]
pub struct Args {
    /// \[DANGEROUS\] Enable unsafe features.
    ///
    /// Unsafe features fall into two categories:
    ///
    ///   * In development features that are not yet ready for production use.
    ///   * Features useful for development and testing that would pose a
    ///     legitimate security risk if used in Materialize Cloud.
    #[clap(long, env = "UNSAFE_MODE")]
    unsafe_mode: bool,

    /// The address on which to listen for trusted SQL connections.
    ///
    /// Connections to this address are not subject to encryption, authentication,
    /// or access control. Care should be taken to not expose this address to the
    /// public internet
    /// or other unauthorized parties.
    #[clap(
        long,
        hide = true,
        value_name = "HOST:PORT",
        env = "MZ_INTERNAL_SQL_LISTEN_ADDR",
        default_value = "127.0.0.1:6877"
    )]
    internal_sql_listen_addr: SocketAddr,

    /// The address on which to listen for trusted HTTP connections.
    ///
    /// Connections to this address are not subject to encryption, authentication,
    /// or access control. Care should be taken to not expose the listen address
    /// to the public internet or other unauthorized parties.
    #[clap(
        long,
        hide = true,
        value_name = "HOST:PORT",
        env = "MZ_INTERNAL_HTTP_LISTEN_ADDR",
        default_value = "127.0.0.1:6878"
    )]
    internal_http_listen_addr: SocketAddr,

    // === Platform options. ===
    /// The service orchestrator implementation to use.
    #[structopt(long, default_value = "process", arg_enum)]
    orchestrator: OrchestratorKind,
    /// Labels to apply to all services created by the orchestrator in the form
    /// `KEY=VALUE`.
    #[structopt(long, hide = true)]
    orchestrator_service_label: Vec<KeyValueArg<String, String>>,
    /// Node selector to apply to all services created by the orchestrator in
    /// the form `KEY=VALUE`.
    #[structopt(long, hide = true)]
    orchestrator_service_node_selector: Vec<KeyValueArg<String, String>>,
    #[structopt(long)]
    kubernetes_service_account: Option<String>,
    /// The Kubernetes context to use with the Kubernetes orchestrator.
    ///
    /// This defaults to `minikube` to prevent disaster (e.g., connecting to a
    /// production cluster that happens to be the active Kubernetes context.)
    #[structopt(long, hide = true, default_value = "minikube")]
    kubernetes_context: String,
    /// The name of this pod
    #[clap(
        long,
        hide = true,
        env = "POD_NAME",
        required_if_eq("orchestrator", "kubernetes")
    )]
    pod_name: Option<String>,
    /// The name of the Kubernetes secret object to use for storing user secrets
    #[structopt(long, hide = true, required_if_eq("orchestrator", "kubernetes"))]
    user_defined_secret: Option<String>,
    /// The mount location of the Kubernetes secret object to use for storing user secrets
    #[structopt(long, hide = true, required_if_eq("orchestrator", "kubernetes"))]
    user_defined_secret_mount_path: Option<String>,
    /// The storaged image reference to use.
    #[structopt(
        long,
        hide = true,
        required_if_eq("orchestrator", "kubernetes"),
        default_value_if("orchestrator", Some("process"), Some("storaged"))
    )]
    storaged_image: Option<String>,
    /// The computed image reference to use.
    #[structopt(
        long,
        hide = true,
        required_if_eq("orchestrator", "kubernetes"),
        default_value_if("orchestrator", Some("process"), Some("computed"))
    )]
    computed_image: Option<String>,
    /// The image pull policy to use for services created by the Kubernetes
    /// orchestrator.
    #[structopt(long, default_value = "always", arg_enum)]
    kubernetes_image_pull_policy: KubernetesImagePullPolicy,
    /// Whether or not COMPUTE and STORAGE processes should die when their connection with the
    /// ADAPTER is lost.
    #[clap(long, possible_values = &["true", "false"])]
    orchestrator_linger: Option<bool>,
    /// Base port for spawning various services
    #[structopt(long, default_value = "2100")]
    base_service_port: u16,

    // === Performance tuning parameters. ===
    /// How much historical detail to maintain in arrangements.
    ///
    /// Set to "off" to disable logical compaction.
    #[clap(long, env = "LOGICAL_COMPACTION_WINDOW", parse(try_from_str = parse_optional_duration), value_name = "DURATION", default_value = "1ms")]
    logical_compaction_window: OptionalDuration,
    /// Default frequency with which to advance timestamps
    #[clap(long, env = "TIMESTAMP_FREQUENCY", hide = true, parse(try_from_str = mz_repr::util::parse_duration), value_name = "DURATION", default_value = "1s")]
    timestamp_frequency: Duration,

    // === Tracing options. ===
    #[clap(flatten)]
    tracing: TracingCliArgs,

    // === Connection options. ===
    /// The address on which to listen for untrusted SQL connections.
    ///
    /// Connections on this address are subject to encryption, authentication,
    /// and authorization as specified by the `--tls-mode` and `--frontegg-auth`
    /// options.
    #[clap(
        long,
        env = "MZ_SQL_LISTEN_ADDR",
        value_name = "HOST:PORT",
        default_value = "127.0.0.1:6875"
    )]
    sql_listen_addr: SocketAddr,
    /// The address on which to listen for untrusted HTTP connections.
    ///
    /// Connections on this address are subject to encryption, authentication,
    /// and authorization as specified by the `--tls-mode` and `--frontegg-auth`
    /// options.
    #[clap(
        long,
        env = "MZ_HTTP_LISTEN_ADDR",
        value_name = "HOST:PORT",
        default_value = "127.0.0.1:6876"
    )]
    http_listen_addr: SocketAddr,
    /// How stringently to demand TLS authentication and encryption.
    ///
    /// If set to "disable", then environmentd rejects HTTP and PostgreSQL
    /// connections that negotiate TLS.
    ///
    /// If set to "require", then environmentd requires that all HTTP and
    /// PostgreSQL connections negotiate TLS. Unencrypted connections will be
    /// rejected.
    ///
    /// If set to "verify-ca", then environmentd requires that all HTTP and
    /// PostgreSQL connections negotiate TLS and supply a certificate signed by
    /// a trusted certificate authority (CA). HTTP connections will operate as
    /// the system user in this mode, while PostgreSQL connections will assume
    /// the name of whatever user is specified in the handshake.
    ///
    /// The "verify-full" mode is like "verify-ca", except that the Common Name
    /// (CN) field of the certificate must match the name of a valid user. HTTP
    /// and PostgreSQL connections will operate as this user. PostgreSQL
    /// connections must additionally specify the same username in the
    /// connection parameters.
    ///
    /// The most secure mode is "verify-full". This is the default mode when
    /// the --tls-cert option is specified. Otherwise the default is "disable".
    #[clap(
        long, env = "TLS_MODE",
        possible_values = &["disable", "require", "verify-ca", "verify-full"],
        default_value = "disable",
        default_value_ifs = &[
            ("frontegg-tenant", None, Some("require")),
            ("tls-cert", None, Some("verify-full")),
        ],
        value_name = "MODE",
    )]
    tls_mode: String,
    #[clap(
        long,
        env = "TLS_CA",
        required_if_eq("tls-mode", "verify-ca"),
        required_if_eq("tls-mode", "verify-full"),
        value_name = "PATH"
    )]
    tls_ca: Option<PathBuf>,
    /// Certificate file for TLS connections.
    #[clap(
        long,
        env = "TLS_CERT",
        requires = "tls-key",
        required_if_eq_any(&[("tls-mode", "allow"), ("tls-mode", "require"), ("tls-mode", "verify-ca"), ("tls-mode", "verify-full")]),
        value_name = "PATH"
    )]
    tls_cert: Option<PathBuf>,
    /// Private key file for TLS connections.
    #[clap(
        long,
        env = "TLS_KEY",
        requires = "tls-cert",
        required_if_eq_any(&[("tls-mode", "allow"), ("tls-mode", "require"), ("tls-mode", "verify-ca"), ("tls-mode", "verify-full")]),
        value_name = "PATH"
    )]
    tls_key: Option<PathBuf>,
    /// Specifies the tenant id when authenticating users. Must be a valid UUID.
    #[clap(
        long,
        env = "FRONTEGG_TENANT",
        requires_all = &["frontegg-jwk", "frontegg-api-token-url"],
        hide = true
    )]
    frontegg_tenant: Option<Uuid>,
    /// JWK used to validate JWTs during user authentication as a PEM public
    /// key. Can optionally be base64 encoded with the URL-safe alphabet.
    #[clap(long, env = "FRONTEGG_JWK", requires = "frontegg-tenant", hide = true)]
    frontegg_jwk: Option<String>,
    /// The full URL (including path) to the api-token endpoint.
    #[clap(
        long,
        env = "FRONTEGG_API_TOKEN_URL",
        requires = "frontegg-tenant",
        hide = true
    )]
    frontegg_api_token_url: Option<String>,
    /// A common string prefix that is expected to be present at the beginning of passwords.
    #[clap(
        long,
        env = "FRONTEGG_PASSWORD_PREFIX",
        requires = "frontegg-tenant",
        hide = true
    )]
    frontegg_password_prefix: Option<String>,
    /// Enable cross-origin resource sharing (CORS) for HTTP requests from the
    /// specified origin.
    #[structopt(long, env = "CORS_ALLOWED_ORIGIN", hide = true)]
    cors_allowed_origin: Vec<HeaderValue>,

    // === Storage options. ===
    /// Where to store data.
    #[clap(
        short = 'D',
        long,
        env = "DATA_DIRECTORY",
        value_name = "PATH",
        default_value = "mzdata"
    )]
    data_directory: PathBuf,
    /// Where the persist library should store its blob data.
    ///
    /// Defaults to the `persist/blob` in the data directory.
    #[clap(long, env = "PERSIST_BLOB_URL")]
    persist_blob_url: Option<Url>,
    /// Where the persist library should perform consensus.
    #[clap(long, env = "PERSIST_CONSENSUS_URL")]
    persist_consensus_url: Url,
    /// Postgres catalog stash connection string.
    #[clap(long, env = "CATALOG_POSTGRES_STASH", value_name = "POSTGRES_URL")]
    catalog_postgres_stash: String,
    /// Postgres storage stash connection string.
    #[clap(long, env = "STORAGE_POSTGRES_STASH", value_name = "POSTGRES_URL")]
    storage_postgres_stash: String,

    // === AWS options. ===
    /// Prefix for an external ID to be supplied to all AWS AssumeRole operations.
    ///
    /// Details: <https://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles_create_for-user_externalid.html>
    #[clap(long, value_name = "ID")]
    aws_external_id_prefix: Option<String>,

    #[clap(long, env = "CLUSTER_REPLICA_SIZES")]
    cluster_replica_sizes: Option<String>,

    /// Availability zones compute resources may be deployed in.
    #[clap(long, env = "AVAILABILITY_ZONE", use_value_delimiter = true)]
    availability_zone: Vec<String>,

    /// Prefix commands issued by the process orchestrator with the supplied value.
    #[clap(long, env = "PROCESS_ORCHESTRATOR_WRAPPER")]
    process_orchestrator_wrapper: Option<String>,
}

#[derive(ArgEnum, Debug, Clone)]
enum OrchestratorKind {
    Kubernetes,
    Process,
}

impl OrchestratorKind {
    /// Default linger value for orchestrator type.
    ///
    /// Locally it is convenient for all the processes to be cleaned up when environmentd dies
    /// which is why `Process` defaults to false.
    ///
    /// In production we want COMPUTE and STORAGE nodes to be resilient to ADAPTER failures which
    /// is why `Kubernetes` defaults to true.
    pub fn default_linger_value(&self) -> bool {
        match self {
            Self::Kubernetes => true,
            Self::Process => false,
        }
    }
}

fn main() {
    let args = cli::parse_args(CliConfig {
        env_prefix: Some("MZ_"),
        enable_version_flag: true,
    });
    if let Err(err) = run(args) {
        eprintln!("environmentd: {:#}", err);
        process::exit(1);
    }
}

fn run(mut args: Args) -> Result<(), anyhow::Error> {
    mz_ore::panic::set_abort_on_panic();

    // Configure signal handling as soon as possible. We want signals to be
    // handled to our liking ASAP.
    sys::enable_sigusr2_coverage_dump()?;
    sys::enable_termination_signal_cleanup()?;

    // Start Tokio runtime.

    let ncpus_useful = usize::max(1, cmp::min(num_cpus::get(), num_cpus::get_physical()));
    let runtime = Arc::new(
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(ncpus_useful)
            // The default thread name exceeds the Linux limit on thread name
            // length, so pick something shorter.
            .thread_name_fn(|| {
                static ATOMIC_ID: AtomicUsize = AtomicUsize::new(0);
                let id = ATOMIC_ID.fetch_add(1, Ordering::SeqCst);
                format!("tokio:work-{}", id)
            })
            .enable_all()
            .build()?,
    );

    let metrics_registry = MetricsRegistry::new();

    // Configure tracing to log the service name when using the process
    // orchestrator, which intermingles log output from multiple services. Other
    // orchestrators separate log output from different services.
    args.tracing.log_prefix = if matches!(args.orchestrator, OrchestratorKind::Process) {
        Some("environmentd".to_string())
    } else {
        None
    };
    runtime.block_on(mz_ore::tracing::configure("environmentd", &args.tracing))?;

    // Initialize fail crate for failpoint support
    let _failpoint_scenario = FailScenario::setup();

    // Configure connections.
    let tls = if args.tls_mode == "disable" {
        if args.tls_ca.is_some() {
            bail!("cannot specify --tls-mode=disable and --tls-ca simultaneously");
        }
        if args.tls_cert.is_some() {
            bail!("cannot specify --tls-mode=disable and --tls-cert simultaneously");
        }
        if args.tls_key.is_some() {
            bail!("cannot specify --tls-mode=disable and --tls-key simultaneously");
        }
        None
    } else {
        let mode = match args.tls_mode.as_str() {
            "require" => {
                if args.tls_ca.is_some() {
                    bail!("cannot specify --tls-mode=require and --tls-ca simultaneously");
                }
                TlsMode::Require
            }
            "verify-ca" => TlsMode::VerifyCa {
                ca: args.tls_ca.unwrap(),
            },
            "verify-full" => TlsMode::VerifyFull {
                ca: args.tls_ca.unwrap(),
            },
            _ => unreachable!(),
        };
        let cert = args.tls_cert.unwrap();
        let key = args.tls_key.unwrap();
        Some(TlsConfig { mode, cert, key })
    };
    let frontegg = match (
        args.frontegg_tenant,
        args.frontegg_api_token_url,
        args.frontegg_jwk,
    ) {
        (None, None, None) => None,
        (Some(tenant_id), Some(admin_api_token_url), Some(jwk)) => {
            Some(FronteggAuthentication::new(FronteggConfig {
                admin_api_token_url,
                decoding_key: DecodingKey::from_rsa_pem(jwk.as_bytes())?,
                tenant_id,
                now: mz_ore::now::SYSTEM_TIME.clone(),
                refresh_before_secs: 60,
                password_prefix: args.frontegg_password_prefix.unwrap_or_default(),
            }))
        }
        _ => unreachable!("clap enforced"),
    };

    // Configure CORS.
    let cors_allowed_origin = if args
        .cors_allowed_origin
        .iter()
        .any(|val| val.as_bytes() == b"*")
    {
        cors::Any.into()
    } else if !args.cors_allowed_origin.is_empty() {
        AllowOrigin::list(args.cors_allowed_origin)
    } else {
        let port = args.http_listen_addr.port();
        AllowOrigin::list([
            HeaderValue::from_str(&format!("http://localhost:{}", port)).unwrap(),
            HeaderValue::from_str(&format!("http://127.0.0.1:{}", port)).unwrap(),
            HeaderValue::from_str(&format!("http://[::1]:{}", port)).unwrap(),
            HeaderValue::from_str(&format!("https://localhost:{}", port)).unwrap(),
            HeaderValue::from_str(&format!("https://127.0.0.1:{}", port)).unwrap(),
            HeaderValue::from_str(&format!("https://[::1]:{}", port)).unwrap(),
        ])
    };

    // Configure storage.
    fs::create_dir_all(&args.data_directory)
        .with_context(|| format!("creating data directory: {}", args.data_directory.display()))?;
    let catalog_postgres_stash = args.catalog_postgres_stash;

    // Configure controller.
    let cwd = env::current_dir().context("retrieving current working directory")?;
    let orchestrator: Box<dyn Orchestrator> = match args.orchestrator {
        OrchestratorKind::Kubernetes => Box::new(
            runtime
                .block_on(KubernetesOrchestrator::new(KubernetesOrchestratorConfig {
                    context: args.kubernetes_context.clone(),
                    service_labels: args
                        .orchestrator_service_label
                        .into_iter()
                        .map(|l| (l.key, l.value))
                        .collect(),
                    service_node_selector: args
                        .orchestrator_service_node_selector
                        .into_iter()
                        .map(|l| (l.key, l.value))
                        .collect(),
                    service_account: args.kubernetes_service_account,
                    image_pull_policy: args.kubernetes_image_pull_policy,
                    user_defined_secret: args.user_defined_secret.clone().unwrap_or_default(),
                }))
                .context("creating kubernetes orchestrator")?,
        ),
        OrchestratorKind::Process => {
            Box::new(
                runtime
                    .block_on(ProcessOrchestrator::new(ProcessOrchestratorConfig {
                        // Look for binaries in the same directory as the
                        // running binary. When running via `cargo run`, this
                        // means that debug binaries look for other debug
                        // binaries and release binaries look for other release
                        // binaries.
                        image_dir: env::current_exe()?.parent().unwrap().to_path_buf(),
                        port_allocator: Arc::new(PortAllocator::new(
                            args.base_service_port,
                            args.base_service_port
                                .checked_add(1000)
                                .expect("Port number overflow, base-service-port too large."),
                        )),
                        suppress_output: false,
                        data_dir: args.data_directory.clone(),
                        command_wrapper: args
                            .process_orchestrator_wrapper
                            .map_or(Ok(vec![]), |s| shell_words::split(&s))?,
                    }))
                    .context("creating process orchestrator")?,
            )
        }
    };
    let persist_clients = PersistClientCache::new(&metrics_registry);
    let persist_clients = Arc::new(Mutex::new(persist_clients));
    let orchestrator = Arc::new(TracingOrchestrator::new(orchestrator, args.tracing.clone()));
    let controller = ControllerConfig {
        orchestrator,
        linger: args
            .orchestrator_linger
            .unwrap_or_else(|| args.orchestrator.default_linger_value()),
        persist_location: PersistLocation {
            blob_uri: match args.persist_blob_url {
                // TODO: need to handle non-UTF-8 paths here.
                None => format!(
                    "file://{}/{}/persist/blob",
                    cwd.display(),
                    args.data_directory.display()
                ),
                Some(blob_url) => blob_url.to_string(),
            },
            consensus_uri: args.persist_consensus_url.to_string(),
        },
        persist_clients,
        storage_stash_url: args.storage_postgres_stash,
        storaged_image: args.storaged_image.expect("clap enforced"),
        computed_image: args.computed_image.expect("clap enforced"),
    };

    // When inside a cgroup with a cpu limit,
    // the logical cpus can be lower than the physical cpus.
    let memory_limit = detect_memory_limit().unwrap_or(MemoryLimit {
        max: None,
        swap_max: None,
    });
    let memory_max_str = match memory_limit.max {
        Some(max) => format!(", {}KiB limit", max / 1024),
        None => "".to_owned(),
    };
    let swap_max_str = match memory_limit.swap_max {
        Some(max) => format!(", {}KiB limit", max / 1024),
        None => "".to_owned(),
    };

    // Print system information as the very first thing in the logs. The goal is
    // to increase the probability that we can reproduce a reported bug if all
    // we get is the log file.
    let mut system = sysinfo::System::new();
    system.refresh_system();

    eprintln!(
        "booting server
environmentd {mz_version}
{dep_versions}
invoked as: {invocation}
os: {os}
cpus: {ncpus_logical} logical, {ncpus_physical} physical, {ncpus_useful} useful
cpu0: {cpu0}
memory: {memory_total}KB total, {memory_used}KB used{memory_limit}
swap: {swap_total}KB total, {swap_used}KB used{swap_limit}
max log level: {max_log_level}",
        mz_version = mz_environmentd::BUILD_INFO.human_version(),
        dep_versions = build_info().join("\n"),
        invocation = {
            use shell_words::quote as escape;
            env::vars_os()
                .map(|(name, value)| {
                    (
                        name.to_string_lossy().into_owned(),
                        value.to_string_lossy().into_owned(),
                    )
                })
                .filter(|(name, _value)| name.starts_with("MZ_"))
                .map(|(name, value)| format!("{}={}", escape(&name), escape(&value)))
                .chain(env::args().into_iter().map(|arg| escape(&arg).into_owned()))
                .join(" ")
        },
        os = os_info::get(),
        ncpus_logical = num_cpus::get(),
        ncpus_physical = num_cpus::get_physical(),
        ncpus_useful = ncpus_useful,
        cpu0 = {
            match &system.cpus().get(0) {
                None => "<unknown>".to_string(),
                Some(cpu0) => format!("{} {}MHz", cpu0.brand(), cpu0.frequency()),
            }
        },
        memory_total = system.total_memory(),
        memory_used = system.used_memory(),
        memory_limit = memory_max_str,
        swap_total = system.total_swap(),
        swap_used = system.used_swap(),
        swap_limit = swap_max_str,
        max_log_level = ::tracing::level_filters::LevelFilter::current(),
    );

    sys::adjust_rlimits();

    let replica_sizes = match args.cluster_replica_sizes {
        None => Default::default(),
        Some(json) => serde_json::from_str(&json).context("parsing replica size map")?,
    };

    if !args.availability_zone.iter().all_unique() {
        bail!("--availability-zone values must be unique");
    }

    let secrets_path = match args.orchestrator {
        OrchestratorKind::Kubernetes => {
            PathBuf::from(args.user_defined_secret_mount_path.unwrap_or_default())
        }
        OrchestratorKind::Process => args.data_directory.join("secrets"),
    };
    let secrets_controller = match args.orchestrator {
        OrchestratorKind::Kubernetes => SecretsControllerConfig::Kubernetes {
            context: args.kubernetes_context,
            user_defined_secret: args.user_defined_secret.unwrap_or_default(),
            user_defined_secret_mount_path: secrets_path.clone(),
            refresh_pod_name: args.pod_name.unwrap_or_default(),
        },
        OrchestratorKind::Process => SecretsControllerConfig::LocalFileSystem(secrets_path.clone()),
    };

    let server = runtime.block_on(mz_environmentd::serve(mz_environmentd::Config {
        logical_compaction_window: args.logical_compaction_window,
        timestamp_frequency: args.timestamp_frequency,
        sql_listen_addr: args.sql_listen_addr,
        http_listen_addr: args.http_listen_addr,
        internal_sql_listen_addr: args.internal_sql_listen_addr,
        internal_http_listen_addr: args.internal_http_listen_addr,
        tls,
        frontegg,
        cors_allowed_origin,
        catalog_postgres_stash,
        controller,
        secrets_controller,
        unsafe_mode: args.unsafe_mode,
        metrics_registry,
        now: SYSTEM_TIME.clone(),
        replica_sizes,
        availability_zones: args.availability_zone,
        connection_context: ConnectionContext::from_cli_args(
            &args.tracing.log_filter.inner,
            args.aws_external_id_prefix,
            secrets_path,
        ),
    }))?;

    println!(
        "environmentd {} listening...",
        mz_environmentd::BUILD_INFO.human_version()
    );
    println!(" SQL address: {}", server.sql_local_addr());
    println!(" HTTP address: {}", server.http_local_addr());
    println!(
        " Internal SQL address: {}",
        server.internal_sql_local_addr()
    );
    println!(
        " Internal HTTP address: {}",
        server.internal_http_local_addr()
    );

    // Block forever.
    loop {
        thread::park();
    }
}

fn build_info() -> Vec<String> {
    let openssl_version =
        unsafe { CStr::from_ptr(openssl_sys::OpenSSL_version(openssl_sys::OPENSSL_VERSION)) };
    let rdkafka_version = unsafe { CStr::from_ptr(rdkafka_sys::bindings::rd_kafka_version_str()) };
    vec![
        openssl_version.to_string_lossy().into_owned(),
        format!("librdkafka v{}", rdkafka_version.to_string_lossy()),
    ]
}

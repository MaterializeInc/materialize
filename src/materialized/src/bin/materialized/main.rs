// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! The main Materialize server.
//!
//! The name is pronounced "materialize-dee." It listens on port 6875 (MTRL).
//!
//! The design and implementation of materialized is very much in flux. See the
//! draft architecture doc for the most up-to-date plan [0]. Access is limited
//! to those with access to the Material Dropbox Paper folder.
//!
//! [0]: https://paper.dropbox.com/doc/Materialize-architecture-plans--AYSu6vvUu7ZDoOEZl7DNi8UQAg-sZj5rhJmISdZSfK0WBxAl

use std::cmp;
use std::env;
use std::ffi::CStr;
use std::fs;
use std::iter;
use std::net::SocketAddr;
use std::panic;
use std::panic::PanicInfo;
use std::path::PathBuf;
use std::process;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use anyhow::{bail, Context};
use backtrace::Backtrace;
use clap::{ArgEnum, Parser};
use fail::FailScenario;
use http::header::{HeaderName, HeaderValue};
use itertools::Itertools;
use jsonwebtoken::DecodingKey;
use lazy_static::lazy_static;
use mz_persist_client::PersistLocation;
use sysinfo::{ProcessorExt, SystemExt};
use tower_http::cors::{self, AllowOrigin};
use tracing_subscriber::filter::Targets;
use url::Url;
use uuid::Uuid;

use materialized::{
    OrchestratorBackend, OrchestratorConfig, SecretsControllerConfig, TlsConfig, TlsMode,
};
use mz_dataflow_types::ConnectorContext;
use mz_frontegg_auth::{FronteggAuthentication, FronteggConfig};
use mz_orchestrator_kubernetes::{KubernetesImagePullPolicy, KubernetesOrchestratorConfig};
use mz_orchestrator_process::ProcessOrchestratorConfig;
use mz_ore::cgroup::{detect_memory_limit, MemoryLimit};
use mz_ore::cli::KeyValueArg;
use mz_ore::id_gen::PortAllocator;
use mz_ore::metrics::MetricsRegistry;
use mz_ore::now::SYSTEM_TIME;

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

lazy_static! {
    pub static ref VERSION: String = materialized::BUILD_INFO.human_version();
    pub static ref LONG_VERSION: String = {
        iter::once(materialized::BUILD_INFO.human_version())
            .chain(build_info())
            .join("\n")
    };
}

type OptionalDuration = Option<Duration>;

fn parse_optional_duration(s: &str) -> Result<OptionalDuration, anyhow::Error> {
    match s {
        "off" => Ok(None),
        _ => Ok(Some(mz_repr::util::parse_duration(s)?)),
    }
}

/// The streaming SQL materialized view engine.
#[derive(Parser, Debug)]
#[clap(
    next_line_help = true,
    args_override_self = true,
    version = VERSION.as_str(),
    long_version = LONG_VERSION.as_str(),
)]
pub struct Args {
    /// [DANGEROUS] Enable experimental features.
    #[clap(long, env = "MZ_EXPERIMENTAL")]
    experimental: bool,

    /// The address on which Prometheus metrics get exposed.
    ///
    /// This address is never served TLS-encrypted or authenticated so care
    /// should be taken to not expose the listen address to the public internet
    /// or other unauthorized parties.
    #[clap(
        long,
        hide = true,
        value_name = "HOST:PORT",
        env = "MZ_THIRD_PARTY_METRICS_ADDR"
    )]
    metrics_listen_addr: Option<SocketAddr>,

    // === Platform options. ===
    /// The service orchestrator implementation to use.
    #[structopt(long, default_value = "process", arg_enum)]
    orchestrator: Orchestrator,
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
        env = "MZ_POD_NAME",
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
    /// The host on which processes spawned by the process orchestrator listen
    /// for connections.
    #[clap(long, hide = true, env = "MZ_PROCESS_LISTEN_HOST")]
    process_listen_host: Option<String>,
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

    // === Timely worker configuration. ===
    /// Address of a storage process that the controller should connect to.
    #[clap(
        long,
        env = "MZ_STORAGE_CONTROLLER_ADDR",
        value_name = "HOST:ADDR",
        conflicts_with = "orchestrator"
    )]
    storage_controller_addr: Option<String>,

    // === Performance tuning parameters. ===
    /// How much historical detail to maintain in arrangements.
    ///
    /// Set to "off" to disable logical compaction.
    #[clap(long, env = "MZ_LOGICAL_COMPACTION_WINDOW", parse(try_from_str = parse_optional_duration), value_name = "DURATION", default_value = "1ms")]
    logical_compaction_window: OptionalDuration,
    /// Default frequency with which to advance timestamps
    #[clap(long, env = "MZ_TIMESTAMP_FREQUENCY", hide = true, parse(try_from_str = mz_repr::util::parse_duration), value_name = "DURATION", default_value = "1s")]
    timestamp_frequency: Duration,

    // === Logging options. ===
    /// Which log messages to emit.
    ///
    /// This value is a comma-separated list of filter directives. Each filter
    /// directive has the following format:
    ///
    ///     [module::path=]level
    ///
    /// A directive indicates that log messages from the specified module that
    /// are at least as severe as the specified level should be emitted. If a
    /// directive omits the module, then it implicitly applies to all modules.
    /// When directives conflict, the last directive wins. If a log message does
    /// not match any directive, it is not emitted.
    ///
    /// The module path of a log message reflects its location in Materialize's
    /// source code. Choosing module paths for filter directives requires
    /// familiarity with Materialize's codebase and is intended for advanced
    /// users. Note that module paths change frequency from release to release.
    ///
    /// The valid levels for a log message are, in increasing order of severity:
    /// trace, debug, info, warn, and error. The special level "off" may be used
    /// in a directive to suppress all log messages, even errors.
    ///
    /// The default value for this option is "info".
    #[clap(
        long,
        env = "MZ_LOG_FILTER",
        value_name = "FILTER",
        default_value = "info"
    )]
    log_filter: Targets,

    /// Prevent dumping of backtraces on SIGSEGV/SIGBUS
    ///
    /// In the case of OOMs and memory corruptions, it may be advantageous to NOT dump backtraces,
    /// as the attempt to dump the backtraces will segfault on its own, corrupting the core file
    /// further and obfuscating the original bug.
    #[clap(long, hide = true, env = "MZ_NO_SIGBUS_SIGSEGV_BACKTRACES")]
    no_sigbus_sigsegv_backtraces: bool,

    // == Connection options.
    /// The address on which to listen for connections.
    #[clap(
        long,
        env = "MZ_LISTEN_ADDR",
        value_name = "HOST:PORT",
        default_value = "127.0.0.1:6875"
    )]
    listen_addr: SocketAddr,
    /// How stringently to demand TLS authentication and encryption.
    ///
    /// If set to "disable", then materialized rejects HTTP and PostgreSQL
    /// connections that negotiate TLS.
    ///
    /// If set to "require", then materialized requires that all HTTP and
    /// PostgreSQL connections negotiate TLS. Unencrypted connections will be
    /// rejected.
    ///
    /// If set to "verify-ca", then materialized requires that all HTTP and
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
        long, env = "MZ_TLS_MODE",
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
        env = "MZ_TLS_CA",
        required_if_eq("tls-mode", "verify-ca"),
        required_if_eq("tls-mode", "verify-full"),
        value_name = "PATH"
    )]
    tls_ca: Option<PathBuf>,
    /// Certificate file for TLS connections.
    #[clap(
        long,
        env = "MZ_TLS_CERT",
        requires = "tls-key",
        required_if_eq_any(&[("tls-mode", "allow"), ("tls-mode", "require"), ("tls-mode", "verify-ca"), ("tls-mode", "verify-full")]),
        value_name = "PATH"
    )]
    tls_cert: Option<PathBuf>,
    /// Private key file for TLS connections.
    #[clap(
        long,
        env = "MZ_TLS_KEY",
        requires = "tls-cert",
        required_if_eq_any(&[("tls-mode", "allow"), ("tls-mode", "require"), ("tls-mode", "verify-ca"), ("tls-mode", "verify-full")]),
        value_name = "PATH"
    )]
    tls_key: Option<PathBuf>,
    /// Specifies the tenant id when authenticating users. Must be a valid UUID.
    #[clap(
        long,
        env = "MZ_FRONTEGG_TENANT",
        requires_all = &["frontegg-jwk", "frontegg-api-token-url"],
        hide = true
    )]
    frontegg_tenant: Option<Uuid>,
    /// JWK used to validate JWTs during user authentication as a PEM public
    /// key. Can optionally be base64 encoded with the URL-safe alphabet.
    #[clap(
        long,
        env = "MZ_FRONTEGG_JWK",
        requires = "frontegg-tenant",
        hide = true
    )]
    frontegg_jwk: Option<String>,
    /// The full URL (including path) to the api-token endpoint.
    #[clap(
        long,
        env = "MZ_FRONTEGG_API_TOKEN_URL",
        requires = "frontegg-tenant",
        hide = true
    )]
    frontegg_api_token_url: Option<String>,
    /// A common string prefix that is expected to be present at the beginning of passwords.
    #[clap(
        long,
        env = "MZ_FRONTEGG_PASSWORD_PREFIX",
        requires = "frontegg-tenant",
        hide = true
    )]
    frontegg_password_prefix: Option<String>,
    /// Enable cross-origin resource sharing (CORS) for HTTP requests from the
    /// specified origin.
    #[structopt(long, env = "MZ_CORS_ALLOWED_ORIGIN", hide = true)]
    cors_allowed_origin: Vec<HeaderValue>,

    // === Storage options. ===
    /// Where to store data.
    #[clap(
        short = 'D',
        long,
        env = "MZ_DATA_DIRECTORY",
        value_name = "PATH",
        default_value = "mzdata"
    )]
    data_directory: PathBuf,
    /// Where the persist library should store its blob data.
    ///
    /// Defaults to the `persist/blob` in the data directory.
    #[clap(long, env = "MZ_PERSIST_BLOB_URL")]
    persist_blob_url: Option<Url>,
    /// Where the persist library should perform consensus.
    #[clap(long, env = "MZ_PERSIST_CONSENSUS_URL")]
    persist_consensus_url: Url,
    /// Postgres catalog stash connection string.
    #[clap(long, env = "MZ_CATALOG_POSTGRES_STASH", value_name = "POSTGRES_URL")]
    catalog_postgres_stash: String,

    // === AWS options. ===
    /// Prefix for an external ID to be supplied to all AWS AssumeRole operations.
    ///
    /// Details: <https://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles_create_for-user_externalid.html>
    #[clap(long, value_name = "ID")]
    aws_external_id_prefix: Option<String>,

    /// The endpoint to send opentelemetry traces to.
    /// If not provided, tracing is not sent.
    ///
    /// You most likely also need to provide
    /// `--opentelemetry-header`/`MZ_OPENTELEMETRY_HEADER`
    /// depending on the collector you are talking to.
    #[clap(long, env = "MZ_OPENTELEMETRY_ENDPOINT", hide = true)]
    opentelemetry_endpoint: Option<String>,

    /// Headers to pass to the OpenTelemetry collector.
    ///
    /// May be specified multiple times.
    #[clap(
        long,
        value_name = "HEADER",
        env = "MZ_OPENTELEMETRY_HEADER",
        requires = "opentelemetry-endpoint",
        hide = true
    )]
    opentelemetry_header: Vec<KeyValueArg<HeaderName, HeaderValue>>,

    #[clap(long, env = "MZ_CLUSTER_REPLICA_SIZES")]
    cluster_replica_sizes: Option<String>,

    /// Availability zones compute resources may be deployed in.
    #[clap(long, env = "MZ_AVAILABILITY_ZONE", use_value_delimiter = true)]
    availability_zone: Vec<String>,

    #[cfg(feature = "tokio-console")]
    /// Turn on the console-subscriber to use materialize with `tokio-console`
    #[clap(long, hide = true)]
    tokio_console: bool,
}

#[derive(ArgEnum, Debug, Clone)]
enum Orchestrator {
    Kubernetes,
    Process,
}

impl Orchestrator {
    /// Default linger value for orchestrator type.
    ///
    /// Locally it is convenient for all the processes to be cleaned up when materialized dies
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
    if let Err(err) = run(Args::parse()) {
        eprintln!("materialized: {:#}", err);
        process::exit(1);
    }
}

fn run(args: Args) -> Result<(), anyhow::Error> {
    // Configure signal handling as soon as possible. We want signals to be
    // handled to our liking ASAP.
    if !args.no_sigbus_sigsegv_backtraces {
        sys::enable_sigbus_sigsegv_backtraces()?;
    }
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

    // Install a custom panic handler that instructs users to file a bug report.
    // This requires that we configure tracing, so that the panic can be
    // reported as a trace event.
    //
    // Avoid adding code above this point, because panics in that code won't get
    // handled by the custom panic handler.
    let metrics_registry = MetricsRegistry::new();
    runtime.block_on(mz_ore::tracing::configure(mz_ore::tracing::TracingConfig {
        log_filter: args.log_filter.clone(),
        opentelemetry_endpoint: args.opentelemetry_endpoint,
        opentelemetry_headers: args
            .opentelemetry_header
            .into_iter()
            .map(|header| (header.key, header.value))
            .collect(),
        prefix: None,
        #[cfg(feature = "tokio-console")]
        tokio_console: args.tokio_console,
    }))?;
    panic::set_hook(Box::new(handle_panic));

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
        let port = args.listen_addr.port();
        AllowOrigin::list([
            HeaderValue::from_str(&format!("http://localhost:{}", port)).unwrap(),
            HeaderValue::from_str(&format!("http://127.0.0.1:{}", port)).unwrap(),
            HeaderValue::from_str(&format!("http://[::1]:{}", port)).unwrap(),
            HeaderValue::from_str(&format!("https://localhost:{}", port)).unwrap(),
            HeaderValue::from_str(&format!("https://127.0.0.1:{}", port)).unwrap(),
            HeaderValue::from_str(&format!("https://[::1]:{}", port)).unwrap(),
        ])
    };

    // Configure orchestrator.
    let orchestrator = OrchestratorConfig {
        backend: match args.orchestrator {
            Orchestrator::Kubernetes => {
                OrchestratorBackend::Kubernetes(KubernetesOrchestratorConfig {
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
                })
            }
            Orchestrator::Process => {
                OrchestratorBackend::Process(ProcessOrchestratorConfig {
                    // Look for binaries in the same directory as the
                    // running binary. When running via `cargo run`, this
                    // means that debug binaries look for other debug
                    // binaries and release binaries look for other release
                    // binaries.
                    image_dir: env::current_exe()?.parent().unwrap().to_path_buf(),
                    port_allocator: Arc::new(PortAllocator::new(
                        args.base_service_port,
                        args.base_service_port
                            .checked_add(100)
                            .expect("Port number overflow, base-service-port too large."),
                    )),
                    suppress_output: false,
                    process_listen_host: args.process_listen_host,
                    data_dir: args.data_directory.clone(),
                })
            }
        },
        storaged_image: args.storaged_image.expect("clap enforced"),
        computed_image: args.computed_image.expect("clap enforced"),
        linger: args
            .orchestrator_linger
            .unwrap_or_else(|| args.orchestrator.default_linger_value()),
    };

    // Configure storage.
    let data_directory = args.data_directory;
    fs::create_dir_all(&data_directory)
        .with_context(|| format!("creating data directory: {}", data_directory.display()))?;
    let cwd = env::current_dir().context("retrieving current working directory")?;
    let persist_location = PersistLocation {
        blob_uri: match args.persist_blob_url {
            // TODO: need to handle non-UTF-8 paths here.
            None => format!(
                "file://{}/{}/persist/blob",
                cwd.display(),
                data_directory.display()
            ),
            Some(blob_url) => blob_url.to_string(),
        },
        consensus_uri: args.persist_consensus_url.to_string(),
    };
    let catalog_postgres_stash = Some(args.catalog_postgres_stash);

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
materialized {mz_version}
{dep_versions}
invoked as: {invocation}
os: {os}
cpus: {ncpus_logical} logical, {ncpus_physical} physical, {ncpus_useful} useful
cpu0: {cpu0}
memory: {memory_total}KB total, {memory_used}KB used{memory_limit}
swap: {swap_total}KB total, {swap_used}KB used{swap_limit}
max log level: {max_log_level}",
        mz_version = materialized::BUILD_INFO.human_version(),
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
            match &system.processors().get(0) {
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

    let secrets_controller = match args.orchestrator {
        Orchestrator::Kubernetes => SecretsControllerConfig::Kubernetes {
            context: args.kubernetes_context,
            user_defined_secret: args.user_defined_secret.unwrap_or_default(),
            user_defined_secret_mount_path: args.user_defined_secret_mount_path.unwrap_or_default(),
            refresh_pod_name: args.pod_name.unwrap_or_default(),
        },
        Orchestrator::Process => SecretsControllerConfig::LocalFileSystem,
    };

    let server = runtime.block_on(materialized::serve(materialized::Config {
        logical_compaction_window: args.logical_compaction_window,
        timestamp_frequency: args.timestamp_frequency,
        listen_addr: args.listen_addr,
        metrics_listen_addr: args.metrics_listen_addr,
        tls,
        frontegg,
        cors_allowed_origin,
        data_directory,
        persist_location,
        catalog_postgres_stash,
        orchestrator,
        secrets_controller: Some(secrets_controller),
        experimental_mode: args.experimental,
        metrics_registry,
        now: SYSTEM_TIME.clone(),
        replica_sizes,
        availability_zones: args.availability_zone,
        connector_context: ConnectorContext::from_cli_args(
            &args.log_filter,
            args.aws_external_id_prefix,
        ),
    }))?;

    eprintln!(
        "=======================================================================
Thank you for trying Materialize!

We are interested in any and all feedback you have, which may be able
to improve both our software and your queries! Please reach out at:

    Web: https://materialize.com
    GitHub issues: https://github.com/MaterializeInc/materialize/issues
    Email: support@materialize.com
    Twitter: @MaterializeInc
=======================================================================
"
    );

    if args.experimental {
        eprintln!(
            "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
                                WARNING!
!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
Starting Materialize in experimental mode means:

- This node's catalog of views and sources are unstable.

If you use any version of Materialize besides this one, you might
not be able to start the Materialize node. To fix this, you'll have
to remove all of Materialize's data (e.g. rm -rf mzdata) and start
the node anew.

- You must always start this node in experimental mode; it can no
longer be started in non-experimental/regular mode.

For more details, see https://materialize.com/docs/cli#experimental-mode
!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
"
        );
    }

    println!(
        "materialized {} listening on {}...",
        materialized::BUILD_INFO.human_version(),
        server.local_addr(),
    );

    // Block forever.
    loop {
        thread::park();
    }
}

lazy_static! {
    static ref PANIC_MUTEX: Mutex<()> = Mutex::new(());
}

fn handle_panic(panic_info: &PanicInfo) {
    let _guard = PANIC_MUTEX.lock();

    let thr = thread::current();
    let thr_name = thr.name().unwrap_or("<unnamed>");

    let msg = match panic_info.payload().downcast_ref::<&'static str>() {
        Some(s) => *s,
        None => match panic_info.payload().downcast_ref::<String>() {
            Some(s) => &s[..],
            None => "Box<Any>",
        },
    };

    let location = if let Some(loc) = panic_info.location() {
        loc.to_string()
    } else {
        "<unknown>".to_string()
    };

    ::tracing::error!(
        target: "panic",
        "{msg}
thread: {thr_name}
location: {location}
version: {version} ({sha})
backtrace:
{backtrace:?}",
        msg = msg,
        thr_name = thr_name,
        location = location,
        version = materialized::BUILD_INFO.version,
        sha = materialized::BUILD_INFO.sha,
        backtrace = Backtrace::new(),
    );
    eprintln!(
        r#"materialized encountered an internal error and crashed.

We rely on bug reports to diagnose and fix these errors. Please
copy and paste the above details and file a report at:

    https://materialize.com/s/bug
"#,
    );
    process::exit(1);
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

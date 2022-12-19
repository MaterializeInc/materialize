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
use std::iter;
use std::net::{Ipv4Addr, SocketAddr};
use std::path::PathBuf;
use std::process;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;
use std::time::Instant;

use anyhow::{bail, Context};
use clap::{ArgEnum, Parser};
use fail::FailScenario;
use http::header::HeaderValue;
use itertools::Itertools;
use jsonwebtoken::DecodingKey;
use once_cell::sync::Lazy;
use prometheus::IntGauge;
use sysinfo::{CpuExt, SystemExt};
use tokio::sync::Mutex;
use tower_http::cors::{self, AllowOrigin};

use url::Url;
use uuid::Uuid;

use mz_adapter::catalog::{ClusterReplicaSizeMap, StorageHostSizeMap};
use mz_cloud_resources::{AwsExternalIdPrefix, CloudResourceController};
use mz_controller::ControllerConfig;
use mz_environmentd::{TlsConfig, TlsMode, BUILD_INFO};
use mz_frontegg_auth::{FronteggAuthentication, FronteggConfig};
use mz_orchestrator::Orchestrator;
use mz_orchestrator_kubernetes::{
    KubernetesImagePullPolicy, KubernetesOrchestrator, KubernetesOrchestratorConfig,
};
use mz_orchestrator_process::{ProcessOrchestrator, ProcessOrchestratorConfig};
use mz_orchestrator_tracing::{StaticTracingConfig, TracingCliArgs, TracingOrchestrator};
use mz_ore::cgroup::{detect_memory_limit, MemoryLimit};
use mz_ore::cli::{self, CliConfig, KeyValueArg};
use mz_ore::metric;
use mz_ore::metrics::MetricsRegistry;
use mz_ore::now::SYSTEM_TIME;
use mz_persist_client::cache::PersistClientCache;
use mz_persist_client::{PersistConfig, PersistLocation};
use mz_secrets::SecretsController;
use mz_sql::catalog::EnvironmentId;
use mz_stash::PostgresFactory;
use mz_storage_client::types::connections::ConnectionContext;

mod sys;

// Disable jemalloc on macOS, as it is not well supported [0][1][2].
// The issues present as runaway latency on load test workloads that are
// comfortably handled by the macOS system allocator. Consider re-evaluating if
// jemalloc's macOS support improves.
//
// [0]: https://github.com/jemalloc/jemalloc/issues/26
// [1]: https://github.com/jemalloc/jemalloc/issues/843
// [2]: https://github.com/jemalloc/jemalloc/issues/1467
//
// Furthermore, as of Aug. 2022, some engineers are using profiling
// tools, e.g. `heaptrack`, that only work with the system allocator.
#[cfg(all(not(target_os = "macos"), feature = "jemalloc"))]
#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

pub static VERSION: Lazy<String> = Lazy::new(|| mz_environmentd::BUILD_INFO.human_version());
pub static LONG_VERSION: Lazy<String> = Lazy::new(|| {
    iter::once(mz_environmentd::BUILD_INFO.human_version())
        .chain(build_info())
        .join("\n")
});

/// Manages a single Materialize environment.
#[derive(Parser, Debug)]
#[clap(
    name = "environmentd",
    next_line_help = true,
    version = VERSION.as_str(),
    long_version = LONG_VERSION.as_str(),
)]
pub struct Args {
    // === Special modes. ===
    /// Enable unsafe features.
    ///
    /// Unsafe features fall into two categories:
    ///
    ///   * In-development features that are not yet ready for production use.
    ///   * Features useful for development and testing that would pose a
    ///     legitimate security risk if used in Materialize Cloud.
    #[clap(long, env = "UNSAFE_MODE")]
    unsafe_mode: bool,
    /// Enable persisted introspection sources.
    ///
    /// These sources are temporarily disabled because they put significant
    /// pressure on persist compaction, which is currently affected by some
    /// known bugs. Once these are resolved, the plan is to enable persisted
    /// introspection sources by default again.
    ///
    /// See <https://github.com/MaterializeInc/materialize/issues/15415>
    /// for context.
    #[clap(long, env = "PERSISTED_INTROSPECTION")]
    persisted_introspection: bool,

    // === Connection options. ===
    /// The address on which to listen for untrusted SQL connections.
    ///
    /// Connections on this address are subject to encryption, authentication,
    /// and authorization as specified by the `--tls-mode` and `--frontegg-auth`
    /// options.
    #[clap(
        long,
        env = "SQL_LISTEN_ADDR",
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
        env = "HTTP_LISTEN_ADDR",
        value_name = "HOST:PORT",
        default_value = "127.0.0.1:6876"
    )]
    http_listen_addr: SocketAddr,
    /// The address on which to listen for trusted SQL connections.
    ///
    /// Connections to this address are not subject to encryption, authentication,
    /// or access control. Care should be taken to not expose this address to the
    /// public internet
    /// or other unauthorized parties.
    #[clap(
        long,
        value_name = "HOST:PORT",
        env = "INTERNAL_SQL_LISTEN_ADDR",
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
        value_name = "HOST:PORT",
        env = "INTERNAL_HTTP_LISTEN_ADDR",
        default_value = "127.0.0.1:6878"
    )]
    internal_http_listen_addr: SocketAddr,
    /// Enable cross-origin resource sharing (CORS) for HTTP requests from the
    /// specified origin.
    #[structopt(long, env = "CORS_ALLOWED_ORIGIN")]
    cors_allowed_origin: Vec<HeaderValue>,
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
    /// The certificate authority for TLS connections.
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
    /// Enables Frontegg authentication for the specified tenant ID.
    #[clap(
        long,
        env = "FRONTEGG_TENANT",
        requires_all = &["frontegg-jwk", "frontegg-api-token-url"],
        value_name = "UUID",
    )]
    frontegg_tenant: Option<Uuid>,
    /// JWK used to validate JWTs during Frontegg authentication as a PEM public
    /// key. Can optionally be base64 encoded with the URL-safe alphabet.
    #[clap(long, env = "FRONTEGG_JWK", requires = "frontegg-tenant")]
    frontegg_jwk: Option<String>,
    /// The full URL (including path) to the Frontegg api-token endpoint.
    #[clap(long, env = "FRONTEGG_API_TOKEN_URL", requires = "frontegg-tenant")]
    frontegg_api_token_url: Option<String>,
    /// A common string prefix that is expected to be present at the beginning
    /// of all Frontegg passwords.
    #[clap(long, env = "FRONTEGG_PASSWORD_PREFIX", requires = "frontegg-tenant")]
    frontegg_password_prefix: Option<String>,

    // === Orchestrator options. ===
    /// The service orchestrator implementation to use.
    #[structopt(long, arg_enum, env = "ORCHESTRATOR")]
    orchestrator: OrchestratorKind,
    /// Labels to apply to all services created by the Kubernetes orchestrator
    /// in the form `KEY=VALUE`.
    #[structopt(long, env = "ORCHESTRATOR_KUBERNETES_SERVICE_LABEL")]
    orchestrator_kubernetes_service_label: Vec<KeyValueArg<String, String>>,
    /// Node selector to apply to all services created by the Kubernetes
    /// orchestrator in the form `KEY=VALUE`.
    #[structopt(long, env = "ORCHESTRATOR_KUBERNETES_SERVICE_NODE_SELECTOR")]
    orchestrator_kubernetes_service_node_selector: Vec<KeyValueArg<String, String>>,
    /// The name of a service account to apply to all services created by the
    /// Kubernetes orchestrator.
    #[structopt(long, env = "ORCHESTRATOR_KUBERNETES_SERVICE_ACCOUNT")]
    orchestrator_kubernetes_service_account: Option<String>,
    /// The Kubernetes context to use with the Kubernetes orchestrator.
    ///
    /// This defaults to `minikube` to prevent disaster (e.g., connecting to a
    /// production cluster that happens to be the active Kubernetes context.)
    #[structopt(
        long,
        env = "ORCHESTRATOR_KUBERNETES_CONTEXT",
        default_value = "minikube"
    )]
    orchestrator_kubernetes_context: String,
    /// The image pull policy to use for services created by the Kubernetes
    /// orchestrator.
    #[structopt(
        long,
        env = "ORCHESTRATOR_KUBERNETES_IMAGE_PULL_POLICY",
        default_value = "always",
        arg_enum
    )]
    orchestrator_kubernetes_image_pull_policy: KubernetesImagePullPolicy,
    /// Prefix commands issued by the process orchestrator with the supplied
    /// value.
    #[clap(long, env = "ORCHESTRATOR_PROCESS_WRAPPER")]
    orchestrator_process_wrapper: Option<String>,
    /// Where the process orchestrator should store secrets.
    #[clap(
        long,
        env = "ORCHESTRATOR_PROCESS_SECRETS_DIRECTORY",
        value_name = "PATH",
        required_if_eq("orchestrator", "process")
    )]
    orchestrator_process_secrets_directory: Option<PathBuf>,
    /// Whether the process orchestrator should handle crashes in child
    /// processes by crashing the parent process.
    #[clap(long, env = "ORCHESTRATOR_PROCESS_PROPAGATE_CRASHES")]
    orchestrator_process_propagate_crashes: bool,

    /// The init container to use for computed and storaged when using the
    /// kubernetes orchestrator.
    #[clap(long)]
    k8s_init_container_image: Option<String>,

    // === Storage options. ===
    /// Where the persist library should store its blob data.
    #[clap(long, env = "PERSIST_BLOB_URL")]
    persist_blob_url: Url,
    /// Where the persist library should perform consensus.
    #[clap(long, env = "PERSIST_CONSENSUS_URL")]
    persist_consensus_url: Url,
    /// The PostgreSQL URL for the storage stash.
    #[clap(long, env = "STORAGE_STASH_URL", value_name = "POSTGRES_URL")]
    storage_stash_url: String,
    /// The storaged image reference to use.
    #[structopt(
        long,
        required_if_eq("orchestrator", "kubernetes"),
        default_value_if("orchestrator", Some("process"), Some("storaged"))
    )]
    storaged_image: Option<String>,

    // === Compute options. ===
    /// The computed image reference to use.
    #[structopt(
        long,
        required_if_eq("orchestrator", "kubernetes"),
        default_value_if("orchestrator", Some("process"), Some("computed"))
    )]
    computed_image: Option<String>,

    // === Adapter options. ===
    /// The PostgreSQL URL for the adapter stash.
    #[clap(long, env = "ADAPTER_STASH_URL", value_name = "POSTGRES_URL")]
    adapter_stash_url: String,

    // === Cloud options. ===
    #[clap(
        long,
        env = "ENVIRONMENT_ID",
        value_name = "<CLOUD>-<REGION>-<ORG-ID>-<ORDINAL>"
    )]
    environment_id: EnvironmentId,
    /// Prefix for an external ID to be supplied to all AWS AssumeRole operations.
    ///
    /// Details: <https://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles_create_for-user_externalid.html>
    #[clap(long, env = "AWS_EXTERNAL_ID_PREFIX", value_name = "ID", parse(from_str = AwsExternalIdPrefix::new_from_cli_argument_or_environment_variable))]
    aws_external_id_prefix: Option<AwsExternalIdPrefix>,
    /// Availability zones in which storage and compute resources may be
    /// deployed.
    #[clap(long, env = "AVAILABILITY_ZONE", use_value_delimiter = true)]
    availability_zone: Vec<String>,
    /// A map from size name to resource allocations for cluster replicas.
    #[clap(
        long,
        env = "CLUSTER_REPLICA_SIZES",
        requires = "bootstrap-default-cluster-replica-size"
    )]
    cluster_replica_sizes: Option<String>,
    /// The size of the default cluster replica if bootstrapping.
    #[clap(
        long,
        env = "BOOTSTRAP_DEFAULT_CLUSTER_REPLICA_SIZE",
        default_value = "1"
    )]
    bootstrap_default_cluster_replica_size: String,
    /// The size of the builtin cluster replicas if bootstrapping.
    #[clap(
        long,
        env = "BOOTSTRAP_BUILTIN_CLUSTER_REPLICA_SIZE",
        default_value = "1"
    )]
    bootstrap_builtin_cluster_replica_size: String,
    /// An list of NAME=VALUE pairs for bootstrapping system parameters that are
    /// not already modified.
    #[clap(
        long,
        env = "BOOTSTRAP_SYSTEM_PARAMETER",
        multiple = true,
        value_delimiter = ';'
    )]
    bootstrap_system_parameter: Vec<KeyValueArg<String, String>>,
    /// A map from size name to resource allocations for storage hosts.
    #[clap(long, env = "STORAGE_HOST_SIZES")]
    storage_host_sizes: Option<String>,
    /// Default storage host size
    #[clap(long, env = "DEFAULT_STORAGE_HOST_SIZE")]
    default_storage_host_size: Option<String>,
    /// The interval in seconds at which to collect storage usage information.
    #[clap(
        long,
        env = "STORAGE_USAGE_COLLECTION_INTERVAL",
        parse(try_from_str = humantime::parse_duration),
        default_value = "3600s"
    )]
    storage_usage_collection_interval_sec: Duration,
    /// An API key for Segment. Enables export of audit events to Segment.
    #[clap(long, env = "SEGMENT_API_KEY")]
    segment_api_key: Option<String>,
    /// Public IP addresses which the cloud environment has configured for
    /// egress
    #[clap(
        long,
        env = "ANNOUNCE_EGRESS_IP",
        multiple = true,
        use_delimiter = true
    )]
    announce_egress_ip: Vec<Ipv4Addr>,
    /// An SDK key for LaunchDarkly.
    ///
    /// Setting this in combination with [`Self::config_sync_loop_interval`]
    /// will enable synchronization of LaunchDarkly features with system
    /// configuration parameters.
    #[clap(long, env = "LAUNCHDARKLY_SDK_KEY")]
    launchdarkly_sdk_key: Option<String>,
    /// A list of PARAM_NAME=KEY_NAME pairs from system parameter names to
    /// LaunchDarkly feature keys.
    ///
    /// This is used (so far only for testing purposes) when propagating values
    /// from the latter to the former. The identity map is assumed for absent
    /// parameter names.
    #[clap(
        long,
        env = "LAUNCHDARKLY_KEY_MAP",
        multiple = true,
        value_delimiter = ';'
    )]
    launchdarkly_key_map: Vec<KeyValueArg<String, String>>,
    /// The interval in seconds at which to synchronize system parameter values.
    ///
    /// If this is not explicitly set, the loop that synchronizes LaunchDarkly
    /// features with system configuration parameters will not run _even if
    /// [`Self::launchdarkly_sdk_key`] is present_.
    #[clap(
        long,
        env = "CONFIG_SYNC_LOOP_INTERVAL",
        parse(try_from_str = humantime::parse_duration),
    )]
    config_sync_loop_interval: Option<Duration>,

    /// The 12-digit AWS account id, which is used to generate an AWS Principal.
    #[clap(long, env = "AWS_ACCOUNT_ID")]
    aws_account_id: Option<String>,

    // === Tracing options. ===
    #[clap(flatten)]
    tracing: TracingCliArgs,
}

#[derive(ArgEnum, Debug, Clone)]
enum OrchestratorKind {
    Kubernetes,
    Process,
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
    let envd_start = Instant::now();

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
    let metrics = Metrics::register_into(&metrics_registry);

    // Configure tracing to log the service name when using the process
    // orchestrator, which intermingles log output from multiple services. Other
    // orchestrators separate log output from different services.
    args.tracing.log_prefix = if matches!(args.orchestrator, OrchestratorKind::Process) {
        Some("environmentd".to_string())
    } else {
        None
    };
    let (tracing_handle, _tracing_guard) =
        runtime.block_on(args.tracing.configure_tracing(StaticTracingConfig {
            service_name: "environmentd",
            build_info: BUILD_INFO,
        }))?;

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

    // Configure controller.
    let (orchestrator, secrets_controller, cloud_resource_controller): (
        Arc<dyn Orchestrator>,
        Arc<dyn SecretsController>,
        Option<Arc<dyn CloudResourceController>>,
    ) = match args.orchestrator {
        OrchestratorKind::Kubernetes => {
            let orchestrator = Arc::new(
                runtime
                    .block_on(KubernetesOrchestrator::new(KubernetesOrchestratorConfig {
                        context: args.orchestrator_kubernetes_context.clone(),
                        service_labels: args
                            .orchestrator_kubernetes_service_label
                            .into_iter()
                            .map(|l| (l.key, l.value))
                            .collect(),
                        service_node_selector: args
                            .orchestrator_kubernetes_service_node_selector
                            .into_iter()
                            .map(|l| (l.key, l.value))
                            .collect(),
                        service_account: args.orchestrator_kubernetes_service_account,
                        image_pull_policy: args.orchestrator_kubernetes_image_pull_policy,
                        aws_external_id_prefix: args.aws_external_id_prefix.clone(),
                    }))
                    .context("creating kubernetes orchestrator")?,
            );
            let secrets_controller = Arc::clone(&orchestrator);
            let cloud_resource_controller = Arc::clone(&orchestrator);
            (
                orchestrator,
                secrets_controller,
                Some(cloud_resource_controller),
            )
        }
        OrchestratorKind::Process => {
            let orchestrator = Arc::new(
                runtime
                    .block_on(ProcessOrchestrator::new(ProcessOrchestratorConfig {
                        // Look for binaries in the same directory as the
                        // running binary. When running via `cargo run`, this
                        // means that debug binaries look for other debug
                        // binaries and release binaries look for other release
                        // binaries.
                        image_dir: env::current_exe()?.parent().unwrap().to_path_buf(),
                        suppress_output: false,
                        environment_id: args.environment_id.to_string(),
                        secrets_dir: args
                            .orchestrator_process_secrets_directory
                            .expect("clap enforced"),
                        command_wrapper: args
                            .orchestrator_process_wrapper
                            .map_or(Ok(vec![]), |s| shell_words::split(&s))?,
                        propagate_crashes: args.orchestrator_process_propagate_crashes,
                    }))
                    .context("creating process orchestrator")?,
            );
            let secrets_controller = Arc::clone(&orchestrator);
            (orchestrator, secrets_controller, None)
        }
    };
    let secrets_reader = secrets_controller.reader();
    let now = SYSTEM_TIME.clone();
    let persist_clients = PersistClientCache::new(
        PersistConfig::new(&mz_environmentd::BUILD_INFO, now.clone()),
        &metrics_registry,
    );
    let persist_clients = Arc::new(Mutex::new(persist_clients));
    let orchestrator = Arc::new(TracingOrchestrator::new(orchestrator, args.tracing.clone()));
    let controller = ControllerConfig {
        build_info: &mz_environmentd::BUILD_INFO,
        orchestrator,
        persist_location: PersistLocation {
            blob_uri: args.persist_blob_url.to_string(),
            consensus_uri: args.persist_consensus_url.to_string(),
        },
        persist_clients,
        storage_stash_url: args.storage_stash_url,
        storaged_image: args.storaged_image.expect("clap enforced"),
        computed_image: args.computed_image.expect("clap enforced"),
        init_container_image: args.k8s_init_container_image,
        now: SYSTEM_TIME.clone(),
        postgres_factory: PostgresFactory::new(&metrics_registry),
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
            env::args()
                .into_iter()
                .map(|arg| escape(&arg).into_owned())
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

    let cluster_replica_sizes: ClusterReplicaSizeMap = match args.cluster_replica_sizes {
        None => Default::default(),
        Some(json) => serde_json::from_str(&json).context("parsing replica size map")?,
    };

    let storage_host_sizes: StorageHostSizeMap = match args.storage_host_sizes {
        None => Default::default(),
        Some(json) => serde_json::from_str(&json).context("parsing storage host map")?,
    };

    // Ensure default storage host size actually exists in the passed map
    if let Some(default_storage_host_size) = &args.default_storage_host_size {
        if !storage_host_sizes.0.contains_key(default_storage_host_size) {
            bail!("default storage host size is unknown");
        }
    }

    let server = runtime.block_on(mz_environmentd::serve(mz_environmentd::Config {
        sql_listen_addr: args.sql_listen_addr,
        http_listen_addr: args.http_listen_addr,
        internal_sql_listen_addr: args.internal_sql_listen_addr,
        internal_http_listen_addr: args.internal_http_listen_addr,
        tls,
        frontegg,
        cors_allowed_origin,
        adapter_stash_url: args.adapter_stash_url,
        controller,
        secrets_controller,
        cloud_resource_controller,
        unsafe_mode: args.unsafe_mode,
        persisted_introspection: args.persisted_introspection,
        metrics_registry,
        now,
        environment_id: args.environment_id,
        cluster_replica_sizes,
        bootstrap_default_cluster_replica_size: args.bootstrap_default_cluster_replica_size,
        bootstrap_builtin_cluster_replica_size: args.bootstrap_builtin_cluster_replica_size,
        bootstrap_system_parameters: args
            .bootstrap_system_parameter
            .into_iter()
            .map(|kv| (kv.key, kv.value))
            .collect(),
        storage_host_sizes,
        default_storage_host_size: args.default_storage_host_size,
        availability_zones: args.availability_zone,
        connection_context: ConnectionContext::from_cli_args(
            &args.tracing.log_filter.inner,
            args.aws_external_id_prefix,
            secrets_reader,
        ),
        tracing_handle,
        storage_usage_collection_interval: args.storage_usage_collection_interval_sec,
        segment_api_key: args.segment_api_key,
        egress_ips: args.announce_egress_ip,
        aws_account_id: args.aws_account_id,
        launchdarkly_sdk_key: args.launchdarkly_sdk_key,
        launchdarkly_key_map: args
            .launchdarkly_key_map
            .into_iter()
            .map(|kv| (kv.key, kv.value))
            .collect(),
        config_sync_loop_interval: args.config_sync_loop_interval,
    }))?;

    metrics.start_time_environmentd.set(
        envd_start
            .elapsed()
            .as_millis()
            .try_into()
            .expect("must fit"),
    );

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

#[derive(Debug, Clone)]
struct Metrics {
    pub start_time_environmentd: IntGauge,
}

impl Metrics {
    pub fn register_into(registry: &MetricsRegistry) -> Metrics {
        Metrics {
            start_time_environmentd: registry.register(metric!(
                name: "mz_start_time_environmentd",
                help: "Time in milliseconds from environmentd start until the adapter is ready.",
            )),
        }
    }
}

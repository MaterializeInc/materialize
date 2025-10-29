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

use std::ffi::CStr;
use std::fs::File;
use std::net::{IpAddr, SocketAddr};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::LazyLock;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};
use std::{cmp, env, iter, thread};

use anyhow::{Context, bail};
use clap::{ArgAction, Parser, ValueEnum};
use fail::FailScenario;
use http::header::HeaderValue;
use ipnet::IpNet;
use itertools::Itertools;
use mz_adapter::ResultExt;
use mz_adapter_types::bootstrap_builtin_cluster_config::{
    ANALYTICS_CLUSTER_DEFAULT_REPLICATION_FACTOR, BootstrapBuiltinClusterConfig,
    CATALOG_SERVER_CLUSTER_DEFAULT_REPLICATION_FACTOR, DEFAULT_REPLICATION_FACTOR,
    PROBE_CLUSTER_DEFAULT_REPLICATION_FACTOR, SUPPORT_CLUSTER_DEFAULT_REPLICATION_FACTOR,
    SYSTEM_CLUSTER_DEFAULT_REPLICATION_FACTOR,
};
use mz_auth::password::Password;
use mz_aws_secrets_controller::AwsSecretsController;
use mz_build_info::BuildInfo;
use mz_catalog::builtin::{
    UNSAFE_DO_NOT_CALL_THIS_IN_PRODUCTION_BUILTIN_TABLE_FINGERPRINT_WHITESPACE,
    UnsafeBuiltinTableFingerprintWhitespace,
};
use mz_catalog::config::ClusterReplicaSizeMap;
use mz_cloud_resources::{AwsExternalIdPrefix, CloudResourceController};
use mz_controller::ControllerConfig;
use mz_frontegg_auth::{Authenticator as FronteggAuthenticator, FronteggCliArgs};
use mz_license_keys::{ExpirationBehavior, ValidatedLicenseKey};
use mz_orchestrator::Orchestrator;
use mz_orchestrator_kubernetes::{
    KubernetesImagePullPolicy, KubernetesOrchestrator, KubernetesOrchestratorConfig,
};
use mz_orchestrator_process::{
    ProcessOrchestrator, ProcessOrchestratorConfig, ProcessOrchestratorTcpProxyConfig,
};
use mz_orchestrator_tracing::{StaticTracingConfig, TracingCliArgs, TracingOrchestrator};
use mz_ore::cli::{self, CliConfig, KeyValueArg};
use mz_ore::error::ErrorExt;
use mz_ore::metric;
use mz_ore::metrics::{MetricsRegistry, register_runtime_metrics};
use mz_ore::now::SYSTEM_TIME;
use mz_ore::task::RuntimeExt;
use mz_ore::url::SensitiveUrl;
use mz_persist_client::PersistLocation;
use mz_persist_client::cache::PersistClientCache;
use mz_persist_client::cfg::PersistConfig;
use mz_persist_client::rpc::{
    MetricsSameProcessPubSubSender, PersistGrpcPubSubServer, PubSubClientConnection, PubSubSender,
};
use mz_secrets::SecretsController;
use mz_server_core::TlsCliArgs;
use mz_service::emit_boot_diagnostics;
use mz_service::secrets::{SecretsControllerKind, SecretsReaderCliArgs};
use mz_sql::catalog::EnvironmentId;
use mz_storage_types::connections::ConnectionContext;
use opentelemetry::trace::TraceContextExt;
use prometheus::IntGauge;
use tracing::{Instrument, error, info, info_span, warn};
use tracing_opentelemetry::OpenTelemetrySpanExt;
use url::Url;

use crate::environmentd::sys;
use crate::{BUILD_INFO, CatalogConfig, ListenerConfig, Listeners, ListenersConfig};

static VERSION: LazyLock<String> = LazyLock::new(|| BUILD_INFO.human_version(None));
static LONG_VERSION: LazyLock<String> = LazyLock::new(|| {
    iter::once(BUILD_INFO.human_version(None))
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
    /// Enable unsafe features. Unsafe features are those that should never run
    /// in production but are appropriate for testing/local development.
    #[clap(long, env = "UNSAFE_MODE")]
    unsafe_mode: bool,
    /// Enables all feature flags, meant only as a tool for local development;
    /// this should never be enabled in CI.
    #[clap(long, env = "ALL_FEATURES")]
    all_features: bool,

    // === Connection options. ===
    /// Path to a file containing the json-formatted configuration of our
    /// metrics, HTTP, and sql listeners.
    #[clap(
        long,
        env = "LISTENERS_CONFIG_PATH",
        value_name = "PATH",
        action = ArgAction::Set,
    )]
    listeners_config_path: PathBuf,
    /// Password for the mz_system user.
    #[clap(
        long,
        env = "EXTERNAL_LOGIN_PASSWORD_MZ_SYSTEM",
        action = ArgAction::Set,
    )]
    external_login_password_mz_system: Option<Password>,
    /// The address on which to listen for Persist PubSub connections.
    ///
    /// Connections to this address are not subject to encryption, authentication,
    /// or access control. Care should be taken to not expose the listen address
    /// to the public internet or other unauthorized parties.
    #[clap(
        long,
        value_name = "HOST:PORT",
        env = "INTERNAL_PERSIST_PUBSUB_LISTEN_ADDR",
        default_value = "127.0.0.1:6879",
        action = ArgAction::Set,
    )]
    internal_persist_pubsub_listen_addr: SocketAddr,
    /// Enable cross-origin resource sharing (CORS) for HTTP requests from the
    /// specified origin.
    ///
    /// The default allows all local connections.
    /// "*" allows all.
    /// "*.domain.com" allows connections from any matching subdomain.
    ///
    /// Wildcards in other positions (e.g., "https://*.foo.com" or "https://foo.*.com") have no effect.
    #[structopt(long, env = "CORS_ALLOWED_ORIGIN")]
    cors_allowed_origin: Vec<HeaderValue>,
    /// Public CIDR which the cloud environment has configured for
    /// egress.
    #[clap(
        long,
        env = "ANNOUNCE_EGRESS_ADDRESS",
        action = ArgAction::Append,
        use_value_delimiter = true
    )]
    announce_egress_address: Vec<IpNet>,
    /// The external host name to connect to the HTTP server of this
    /// environment.
    ///
    /// Presently used to render webhook URLs for end users in notices and the
    /// system catalog. Not used to establish connections directly.
    #[clap(long, env = "HTTP_HOST_NAME")]
    http_host_name: Option<String>,
    /// The URL of the Materialize console to proxy from the /internal-console
    /// endpoint on the internal HTTP server.
    #[clap(long, env = "INTERNAL_CONSOLE_REDIRECT_URL")]
    internal_console_redirect_url: Option<String>,
    /// TLS arguments.
    #[clap(flatten)]
    tls: TlsCliArgs,
    /// Frontegg arguments.
    #[clap(flatten)]
    frontegg: FronteggCliArgs,
    // === Orchestrator options. ===
    /// The service orchestrator implementation to use.
    #[structopt(long, value_enum, env = "ORCHESTRATOR")]
    orchestrator: OrchestratorKind,
    /// Name of a non-default Kubernetes scheduler, if any.
    #[structopt(long, env = "ORCHESTRATOR_KUBERNETES_SCHEDULER_NAME")]
    orchestrator_kubernetes_scheduler_name: Option<String>,
    /// Annotations to apply to all services created by the Kubernetes orchestrator
    /// in the form `KEY=VALUE`.
    #[structopt(long, env = "ORCHESTRATOR_KUBERNETES_SERVICE_ANNOTATION")]
    orchestrator_kubernetes_service_annotation: Vec<KeyValueArg<String, String>>,
    /// Labels to apply to all services created by the Kubernetes orchestrator
    /// in the form `KEY=VALUE`.
    #[structopt(long, env = "ORCHESTRATOR_KUBERNETES_SERVICE_LABEL")]
    orchestrator_kubernetes_service_label: Vec<KeyValueArg<String, String>>,
    /// Node selector to apply to all services created by the Kubernetes
    /// orchestrator in the form `KEY=VALUE`.
    #[structopt(long, env = "ORCHESTRATOR_KUBERNETES_SERVICE_NODE_SELECTOR")]
    orchestrator_kubernetes_service_node_selector: Vec<KeyValueArg<String, String>>,
    /// Affinity to apply to all services created by the Kubernetes
    /// orchestrator as a JSON string.
    #[structopt(long, env = "ORCHESTRATOR_KUBERNETES_SERVICE_AFFINITY")]
    orchestrator_kubernetes_service_affinity: Option<String>,
    /// Tolerations to apply to all services created by the Kubernetes
    /// orchestrator as a JSON string.
    #[structopt(long, env = "ORCHESTRATOR_KUBERNETES_SERVICE_TOLERATIONS")]
    orchestrator_kubernetes_service_tolerations: Option<String>,
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
        value_enum
    )]
    orchestrator_kubernetes_image_pull_policy: KubernetesImagePullPolicy,
    /// The init container for services created by the Kubernetes orchestrator.
    #[clap(long, env = "ORCHESTRATOR_KUBERNETES_INIT_CONTAINER_IMAGE")]
    orchestrator_kubernetes_init_container_image: Option<String>,
    /// The Kubernetes StorageClass to use for the ephemeral volume attached to
    /// services that request disk.
    ///
    /// If unspecified, the Kubernetes orchestrator will refuse to create
    /// services that request disk.
    #[clap(long, env = "ORCHESTRATOR_KUBERNETES_EPHEMERAL_VOLUME_CLASS")]
    orchestrator_kubernetes_ephemeral_volume_class: Option<String>,
    /// The optional fs group for service's pods' `securityContext`.
    #[clap(long, env = "ORCHESTRATOR_KUBERNETES_SERVICE_FS_GROUP")]
    orchestrator_kubernetes_service_fs_group: Option<i64>,
    /// The prefix to prepend to all kubernetes object names.
    #[clap(long, env = "ORCHESTRATOR_KUBERNETES_NAME_PREFIX")]
    orchestrator_kubernetes_name_prefix: Option<String>,
    /// Whether to enable pod metrics collection.
    #[clap(long, env = "ORCHESTRATOR_KUBERNETES_DISABLE_POD_METRICS_COLLECTION")]
    orchestrator_kubernetes_disable_pod_metrics_collection: bool,
    /// Whether to annotate pods for prometheus service discovery.
    #[clap(
        long,
        env = "ORCHESTRATOR_KUBERNETES_ENABLE_PROMETHEUS_SCRAPE_ANNOTATIONS"
    )]
    orchestrator_kubernetes_enable_prometheus_scrape_annotations: bool,
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
    /// An IP address on which the process orchestrator should bind TCP proxies
    /// for Unix domain sockets.
    ///
    /// When specified, for each named port of each created service, the process
    /// orchestrator will bind a TCP listener to the specified address that
    /// proxies incoming connections to the underlying Unix domain socket. The
    /// allocated TCP port will be emitted as a tracing event.
    ///
    /// The primary use is live debugging the running child services via tools
    /// that do not support Unix domain sockets (e.g., Prometheus, web
    /// browsers).
    #[clap(long, env = "ORCHESTRATOR_PROCESS_TCP_PROXY_LISTEN_ADDR")]
    orchestrator_process_tcp_proxy_listen_addr: Option<IpAddr>,
    /// A directory in which the process orchestrator should write Prometheus
    /// scrape targets, for use with Prometheus's file-based service discovery.
    ///
    /// Each namespaced orchestrator will maintain a single JSON file into the
    /// directory named `NAMESPACE.json` containing the scrape targets for all
    /// extant services. The scrape targets will use the TCP proxy address, as
    /// Prometheus does not support scraping over Unix domain sockets.
    ///
    /// This option is ignored unless
    /// `--orchestrator-process-tcp-proxy-listen-addr` is set.
    ///
    /// See also: <https://prometheus.io/docs/guides/file-sd/>
    #[clap(
        long,
        env = "ORCHESTRATOR_PROCESS_PROMETHEUS_SERVICE_DISCOVERY_DIRECTORY"
    )]
    orchestrator_process_prometheus_service_discovery_directory: Option<PathBuf>,
    /// A scratch directory that orchestrated processes can use for ephemeral storage.
    #[clap(
        long,
        env = "ORCHESTRATOR_PROCESS_SCRATCH_DIRECTORY",
        value_name = "PATH"
    )]
    orchestrator_process_scratch_directory: Option<PathBuf>,
    /// Whether to use coverage build and collect coverage information. Not to be used for
    /// production, only testing.
    #[structopt(long, env = "ORCHESTRATOR_KUBERNETES_COVERAGE")]
    orchestrator_kubernetes_coverage: bool,
    /// The secrets controller implementation to use.
    #[structopt(
        long,
        value_enum,
        env = "SECRETS_CONTROLLER",
        default_value_ifs([
            ("orchestrator", "kubernetes", Some("kubernetes")),
            ("orchestrator", "process", Some("local-file"))
        ]),
        default_value("kubernetes"), // This shouldn't be possible, but it makes clap happy.
    )]
    secrets_controller: SecretsControllerKind,
    /// The list of tags to be set on AWS Secrets Manager secrets created by the
    /// AWS secrets controller.
    #[clap(
        long,
        env = "AWS_SECRETS_CONTROLLER_TAGS",
        action = ArgAction::Append,
        value_delimiter = ';',
        required_if_eq("secrets_controller", "aws-secrets-manager")
    )]
    aws_secrets_controller_tags: Vec<KeyValueArg<String, String>>,
    /// The clusterd image reference to use.
    #[structopt(
        long,
        env = "CLUSTERD_IMAGE",
        required_if_eq("orchestrator", "kubernetes"),
        default_value_if("orchestrator", "process", Some("clusterd"))
    )]
    clusterd_image: Option<String>,
    /// A number representing the environment's generation.
    ///
    /// This is incremented to request that the new process perform a graceful
    /// transition of power from the prior generation.
    #[clap(long, env = "DEPLOY_GENERATION", default_value = "0")]
    deploy_generation: u64,

    /// Can be provided in place of both persist_consensus_url and
    /// timestamp_oracle_url in order to point both at the same backend
    #[clap(
        long,
        env = "METADATA_BACKEND_URL",
        conflicts_with_all = &[
            "persist_consensus_url",
            "timestamp_oracle_url",
        ],
    )]
    metadata_backend_url: Option<SensitiveUrl>,

    /// Helm chart version for self-hosted Materialize. This version does not correspond to the
    /// Materialize (core) version (v0.125.0), but is time-based for our twice-a-year helm chart
    /// releases: v25.1.Z, v25.2.Z in 2025, then v26.1.Z, v26.2.Z in 2026, and so on. This version
    /// is displayed in addition in `SELECT mz_version()` if set.
    #[clap(long, env = "HELM_CHART_VERSION")]
    helm_chart_version: Option<String>,

    // === Storage options. ===
    /// Where the persist library should store its blob data.
    #[clap(long, env = "PERSIST_BLOB_URL")]
    persist_blob_url: SensitiveUrl,
    /// Where the persist library should perform consensus.
    #[clap(long, env = "PERSIST_CONSENSUS_URL")]
    persist_consensus_url: Option<SensitiveUrl>,
    /// The Persist PubSub URL.
    ///
    /// This URL is passed to `clusterd` for discovery of the Persist PubSub service.
    #[clap(
        long,
        env = "PERSIST_PUBSUB_URL",
        default_value = "http://localhost:6879"
    )]
    persist_pubsub_url: String,
    /// The number of worker threads created for the IsolatedRuntime used for
    /// storage related tasks. A negative value will subtract from the number
    /// of threads returned by [`num_cpus::get`].
    #[clap(long, env = "PERSIST_ISOLATED_RUNTIME_THREADS")]
    persist_isolated_runtime_threads: Option<isize>,
    /// The interval in seconds at which to collect storage usage information.
    #[clap(
        long,
        env = "STORAGE_USAGE_COLLECTION_INTERVAL",
        value_parser = humantime::parse_duration,
        default_value = "3600s"
    )]
    storage_usage_collection_interval_sec: Duration,
    /// The period for which to retain usage records. Note that the retention
    /// period is only evaluated at server start time, so rebooting the server
    /// is required to discard old records.
    #[clap(long, env = "STORAGE_USAGE_RETENTION_PERIOD", value_parser = humantime::parse_duration)]
    storage_usage_retention_period: Option<Duration>,

    // === Adapter options. ===
    /// The PostgreSQL URL for the Postgres-backed timestamp oracle.
    #[clap(long, env = "TIMESTAMP_ORACLE_URL", value_name = "POSTGRES_URL")]
    timestamp_oracle_url: Option<SensitiveUrl>,
    /// Availability zones in which storage and compute resources may be
    /// deployed.
    #[clap(long, env = "AVAILABILITY_ZONE", use_value_delimiter = true)]
    availability_zone: Vec<String>,
    /// A map from size name to resource allocations for cluster replicas.
    #[clap(
        long,
        env = "CLUSTER_REPLICA_SIZES",
        requires = "bootstrap_default_cluster_replica_size"
    )]
    cluster_replica_sizes: String,
    /// An API key for Segment. Enables export of audit events to Segment.
    #[clap(long, env = "SEGMENT_API_KEY")]
    segment_api_key: Option<String>,
    /// Whether the Segment client is being used on the client side
    /// (rather than the server side).
    ///
    /// Enabling this causes the Segment server to record the IP address from
    /// which the event was sent.
    #[clap(long, env = "SEGMENT_CLIENT_SIDE")]
    segment_client_side: bool,
    /// Only create a dummy segment client when no segment api key is provided, only to get more
    /// testing coverage.
    #[clap(long, env = "TEST_ONLY_DUMMY_SEGMENT_CLIENT")]
    test_only_dummy_segment_client: bool,
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
        action = ArgAction::Append,
        value_delimiter = ';'
    )]
    launchdarkly_key_map: Vec<KeyValueArg<String, String>>,
    /// The duration at which the system parameter synchronization times out during startup.
    #[clap(
        long,
        env = "CONFIG_SYNC_TIMEOUT",
        value_parser = humantime::parse_duration,
        default_value = "30s"
    )]
    config_sync_timeout: Duration,
    /// The interval in seconds at which to synchronize system parameter values.
    ///
    /// If this is not explicitly set, the loop that synchronizes LaunchDarkly
    /// features with system configuration parameters will not run _even if
    /// [`Self::launchdarkly_sdk_key`] is present_.
    #[clap(
        long,
        env = "CONFIG_SYNC_LOOP_INTERVAL",
        value_parser = humantime::parse_duration,
    )]
    config_sync_loop_interval: Option<Duration>,
    /// Path to a JSON file containing system parameter values.
    /// If specified, this file will be used instead of LaunchDarkly for configuration.
    #[clap(long, env = "CONFIG_SYNC_FILE_PATH", value_name = "PATH")]
    config_sync_file_path: Option<PathBuf>,

    // === Bootstrap options. ===
    #[clap(
        long,
        env = "ENVIRONMENT_ID",
        value_name = "<CLOUD>-<REGION>-<ORG-ID>-<ORDINAL>"
    )]
    environment_id: EnvironmentId,
    /// If set, a role with the provided name will be created with `CREATEDB`
    /// and `CREATECLUSTER` attributes. It will also have `CREATE` privileges on
    /// the `materialize` database, `materialize.public` schema, and
    /// `quickstart` cluster.
    ///
    /// This option is meant for local development and testing to simplify the
    /// initial process of granting attributes and privileges to some default
    /// role.
    #[clap(long, env = "BOOTSTRAP_ROLE")]
    bootstrap_role: Option<String>,
    /// The size of the default cluster replica if bootstrapping.
    #[clap(
        long,
        env = "BOOTSTRAP_DEFAULT_CLUSTER_REPLICA_SIZE",
        default_value = "scale=1,workers=1"
    )]
    bootstrap_default_cluster_replica_size: String,
    /// The size of the builtin system cluster replicas if bootstrapping.
    #[clap(
        long,
        env = "BOOTSTRAP_BUILTIN_SYSTEM_CLUSTER_REPLICA_SIZE",
        default_value = "scale=1,workers=1"
    )]
    bootstrap_builtin_system_cluster_replica_size: String,
    /// The size of the builtin catalog server cluster replicas if bootstrapping.
    #[clap(
        long,
        env = "BOOTSTRAP_BUILTIN_CATALOG_SERVER_CLUSTER_REPLICA_SIZE",
        default_value = "scale=1,workers=1"
    )]
    bootstrap_builtin_catalog_server_cluster_replica_size: String,
    /// The size of the builtin probe cluster replicas if bootstrapping.
    #[clap(
        long,
        env = "BOOTSTRAP_BUILTIN_PROBE_CLUSTER_REPLICA_SIZE",
        default_value = "scale=1,workers=1"
    )]
    bootstrap_builtin_probe_cluster_replica_size: String,
    /// The size of the builtin support cluster replicas if bootstrapping.
    #[clap(
        long,
        env = "BOOTSTRAP_BUILTIN_SUPPORT_CLUSTER_REPLICA_SIZE",
        default_value = "scale=1,workers=1"
    )]
    bootstrap_builtin_support_cluster_replica_size: String,
    /// The size of the builtin analytics cluster replicas if bootstrapping.
    #[clap(
        long,
        env = "BOOTSTRAP_BUILTIN_ANALYTICS_CLUSTER_REPLICA_SIZE",
        default_value = "scale=1,workers=1"
    )]
    bootstrap_builtin_analytics_cluster_replica_size: String,
    #[clap(
        long,
        env = "BOOTSTRAP_DEFAULT_CLUSTER_REPLICATION_FACTOR",
        default_value = DEFAULT_REPLICATION_FACTOR.to_string(),
        value_parser = clap::value_parser!(u32).range(0..=2)
    )]
    bootstrap_default_cluster_replication_factor: u32,
    /// The replication factor of the builtin system cluster replicas if bootstrapping.
    #[clap(
        long,
        env = "BOOTSTRAP_BUILTIN_SYSTEM_CLUSTER_REPLICATION_FACTOR",
        default_value = SYSTEM_CLUSTER_DEFAULT_REPLICATION_FACTOR.to_string(),
        value_parser = clap::value_parser!(u32).range(0..=2)
    )]
    bootstrap_builtin_system_cluster_replication_factor: u32,
    /// The replication factor of the builtin catalog server cluster replicas if bootstrapping.
    #[clap(
        long,
        env = "BOOTSTRAP_BUILTIN_CATALOG_SERVER_CLUSTER_REPLICATION_FACTOR",
        default_value = CATALOG_SERVER_CLUSTER_DEFAULT_REPLICATION_FACTOR.to_string(),
        value_parser = clap::value_parser!(u32).range(0..=2)
    )]
    bootstrap_builtin_catalog_server_cluster_replication_factor: u32,
    /// The replication factor of the builtin probe cluster replicas if bootstrapping.
    #[clap(
        long,
        env = "BOOTSTRAP_BUILTIN_PROBE_CLUSTER_REPLICATION_FACTOR",
        default_value = PROBE_CLUSTER_DEFAULT_REPLICATION_FACTOR.to_string(),
        value_parser = clap::value_parser!(u32).range(0..=2)
    )]
    bootstrap_builtin_probe_cluster_replication_factor: u32,
    /// The replication factor of the builtin support cluster replicas if bootstrapping.
    #[clap(
        long,
        env = "BOOTSTRAP_BUILTIN_SUPPORT_CLUSTER_REPLICATION_FACTOR",
        default_value = SUPPORT_CLUSTER_DEFAULT_REPLICATION_FACTOR.to_string(),
        value_parser = clap::value_parser!(u32).range(0..=2)
    )]
    bootstrap_builtin_support_cluster_replication_factor: u32,
    /// The replication factor of the builtin analytics cluster replicas if bootstrapping.
    #[clap(
        long,
        env = "BOOTSTRAP_BUILTIN_ANALYTICS_CLUSTER_REPLICATION_FACTOR",
        default_value = ANALYTICS_CLUSTER_DEFAULT_REPLICATION_FACTOR.to_string(),
        value_parser = clap::value_parser!(u32).range(0..=2)
    )]
    bootstrap_builtin_analytics_cluster_replication_factor: u32,
    /// An list of NAME=VALUE pairs used to override static defaults
    /// for system parameters.
    #[clap(
        long,
        env = "SYSTEM_PARAMETER_DEFAULT",
        action = ArgAction::Append,
        value_delimiter = ';'
    )]
    system_parameter_default: Vec<KeyValueArg<String, String>>,
    /// File containing a valid Materialize license key.
    #[clap(long, env = "LICENSE_KEY")]
    license_key: Option<String>,

    // === AWS options. ===
    /// The AWS account ID, which will be used to generate ARNs for
    /// Materialize-controlled AWS resources.
    #[clap(long, env = "AWS_ACCOUNT_ID")]
    aws_account_id: Option<String>,
    /// The ARN for a Materialize-controlled role to assume before assuming
    /// a customer's requested role for an AWS connection.
    #[clap(long, env = "AWS_CONNECTION_ROLE_ARN")]
    aws_connection_role_arn: Option<String>,
    /// Prefix for an external ID to be supplied to all AWS AssumeRole operations.
    ///
    /// Details: <https://docs.aws.amazon.com/IAM/latest/UserGuide/id_roles_create_for-user_externalid.html>
    #[clap(long, env = "AWS_EXTERNAL_ID_PREFIX", value_name = "ID", value_parser = AwsExternalIdPrefix::new_from_cli_argument_or_environment_variable)]
    aws_external_id_prefix: Option<AwsExternalIdPrefix>,
    /// The list of supported AWS PrivateLink availability zone ids.
    /// Must be zone IDs, of format e.g. "use-az1".
    #[clap(
        long,
        env = "AWS_PRIVATELINK_AVAILABILITY_ZONES",
        action = ArgAction::Append,
        use_value_delimiter = true
    )]
    aws_privatelink_availability_zones: Option<Vec<String>>,

    // === Tracing options. ===
    #[clap(flatten)]
    tracing: TracingCliArgs,

    // === Testing options. ===
    /// Injects arbitrary whitespace into builtin table fingerprints, which can
    /// trigger builtin item migrations. The amount of whitespace is determined
    /// by
    /// `unsafe_builtin_table_fingerprint_whitespace_version`.
    /// This argument is meant for testing only and as the names suggests
    /// should not be set in production.
    #[clap(long, value_enum, requires = "unsafe_mode")]
    unsafe_builtin_table_fingerprint_whitespace: Option<UnsafeBuiltinTableFingerprintWhitespace>,
    /// Controls the amount of whitespace injected by
    /// `unsafe_builtin_table_fingerprint_whitespace`.
    /// Incrementing this value can allow triggering multiple builtin
    /// migrations from a single test. This argument is meant for testing only
    /// and as the names suggests should not be set in production.
    #[clap(long, requires = "unsafe_mode", default_value = "1")]
    unsafe_builtin_table_fingerprint_whitespace_version: usize,
}

#[derive(ValueEnum, Debug, Clone)]
enum OrchestratorKind {
    Kubernetes,
    Process,
}

// TODO [Alex Hunt] move this to a shared function that can be imported by the
// region-controller.
fn aws_secrets_controller_prefix(env_id: &EnvironmentId) -> String {
    format!("/user-managed/{}/", env_id)
}
fn aws_secrets_controller_key_alias(env_id: &EnvironmentId) -> String {
    // TODO [Alex Hunt] move this to a shared function that can be imported by the
    // region-controller.
    format!("alias/customer_key_{}", env_id)
}

pub fn main() {
    let args = cli::parse_args(CliConfig {
        env_prefix: Some("MZ_"),
        enable_version_flag: true,
    });
    if let Err(err) = run(args) {
        panic!("environmentd: fatal: {}", err.display_with_causes());
    }
}

fn run(mut args: Args) -> Result<(), anyhow::Error> {
    mz_ore::panic::install_enhanced_handler();
    let envd_start = Instant::now();

    // Configure signal handling as soon as possible. We want signals to be
    // handled to our liking ASAP.
    sys::enable_sigusr2_coverage_dump()?;
    sys::enable_termination_signal_cleanup()?;

    let license_key = if let Some(license_key_file) = args.license_key {
        let license_key_text = std::fs::read_to_string(&license_key_file)
            .context("failed to open license key file")?;
        let license_key = mz_license_keys::validate(license_key_text.trim())
            .context("failed to validate license key file")?;
        if license_key.expired {
            let message = format!(
                "The license key provided at {license_key_file} is expired! Please contact Materialize for assistance."
            );
            match license_key.expiration_behavior {
                ExpirationBehavior::Warn | ExpirationBehavior::DisableClusterCreation => {
                    warn!("{message}");
                }
                ExpirationBehavior::Disable => bail!("{message}"),
            }
        }
        license_key
    } else if matches!(args.orchestrator, OrchestratorKind::Kubernetes) {
        bail!("--license-key is required when running in Kubernetes");
    } else {
        // license key checks are optional for the emulator
        ValidatedLicenseKey::disabled()
    };

    // Configure testing options.
    if let Some(fingerprint_whitespace) = args.unsafe_builtin_table_fingerprint_whitespace {
        assert!(args.unsafe_mode);
        let whitespace = "\n".repeat(args.unsafe_builtin_table_fingerprint_whitespace_version);
        *UNSAFE_DO_NOT_CALL_THIS_IN_PRODUCTION_BUILTIN_TABLE_FINGERPRINT_WHITESPACE
            .lock()
            .expect("lock poisoned") = Some((fingerprint_whitespace, whitespace));
    }

    // Start Tokio runtime.

    let ncpus_useful = usize::max(1, cmp::min(num_cpus::get(), num_cpus::get_physical()));
    let runtime = Arc::new(
        tokio::runtime::Builder::new_multi_thread()
            .worker_threads(ncpus_useful)
            // The default thread name exceeds the Linux limit on thread name
            // length, so pick something shorter.
            .thread_name_fn(|| {
                static ATOMIC_ID: AtomicUsize = AtomicUsize::new(0);
                let id = ATOMIC_ID.fetch_add(1, Ordering::Relaxed);
                format!("tokio:work-{}", id)
            })
            .enable_all()
            .build()?,
    );

    // Configure tracing to log the service name when using the process
    // orchestrator, which intermingles log output from multiple services. Other
    // orchestrators separate log output from different services.
    args.tracing.log_prefix = if matches!(args.orchestrator, OrchestratorKind::Process) {
        Some("environmentd".to_string())
    } else {
        None
    };

    let metrics_registry = MetricsRegistry::new();
    let (tracing_handle, _tracing_guard) = runtime.block_on(args.tracing.configure_tracing(
        StaticTracingConfig {
            service_name: "environmentd",
            build_info: BUILD_INFO,
        },
        metrics_registry.clone(),
    ))?;
    register_runtime_metrics("main", runtime.metrics(), &metrics_registry);

    let span = tracing::info_span!("environmentd::run").entered();

    info!("startup: envd init: beginning");
    info!("startup: envd init: preamble beginning");

    let metrics = Metrics::register_into(&metrics_registry, BUILD_INFO);

    runtime.block_on(mz_alloc::register_metrics_into(&metrics_registry));
    runtime.block_on(mz_metrics::register_metrics_into(
        &metrics_registry,
        mz_dyncfgs::all_dyncfgs(),
    ));

    // Initialize fail crate for failpoint support
    let _failpoint_scenario = FailScenario::setup();

    // Configure connections.
    let tls = args.tls.into_config()?;
    let frontegg = FronteggAuthenticator::from_args(args.frontegg, &metrics_registry)?;
    let listeners_config: ListenersConfig = {
        let f = File::open(args.listeners_config_path)?;
        serde_json::from_reader(f)?
    };

    for (_, listener) in &listeners_config.sql {
        listener
            .validate()
            .map_err(|e| anyhow::anyhow!("invalid SQL listener: {}", e))?;
    }

    for (_, listener) in &listeners_config.http {
        listener
            .validate()
            .map_err(|e| anyhow::anyhow!("invalid HTTP listener: {}", e))?;
    }

    // Configure CORS.
    let allowed_origins = if !args.cors_allowed_origin.is_empty() {
        args.cors_allowed_origin
    } else {
        let mut allowed_origins = Vec::with_capacity(listeners_config.http.len() * 6);
        for (_, listener) in &listeners_config.http {
            let port = listener.addr().port();
            allowed_origins.extend([
                HeaderValue::from_str(&format!("http://localhost:{}", port)).unwrap(),
                HeaderValue::from_str(&format!("http://127.0.0.1:{}", port)).unwrap(),
                HeaderValue::from_str(&format!("http://[::1]:{}", port)).unwrap(),
                HeaderValue::from_str(&format!("https://localhost:{}", port)).unwrap(),
                HeaderValue::from_str(&format!("https://127.0.0.1:{}", port)).unwrap(),
                HeaderValue::from_str(&format!("https://[::1]:{}", port)).unwrap(),
            ])
        }
        allowed_origins
    };
    let cors_allowed_origin = mz_http_util::build_cors_allowed_origin(&allowed_origins);

    // Configure controller.
    let entered = info_span!("environmentd::configure_controller").entered();
    let (orchestrator, secrets_controller, cloud_resource_controller): (
        Arc<dyn Orchestrator>,
        Arc<dyn SecretsController>,
        Option<Arc<dyn CloudResourceController>>,
    ) = match args.orchestrator {
        OrchestratorKind::Kubernetes => {
            if args.orchestrator_process_scratch_directory.is_some() {
                bail!(
                    "--orchestrator-process-scratch-directory is \
                      not currently usable with the kubernetes orchestrator"
                );
            }

            let orchestrator = Arc::new(
                runtime
                    .block_on(KubernetesOrchestrator::new(KubernetesOrchestratorConfig {
                        context: args.orchestrator_kubernetes_context.clone(),
                        scheduler_name: args.orchestrator_kubernetes_scheduler_name,
                        service_annotations: args
                            .orchestrator_kubernetes_service_annotation
                            .into_iter()
                            .map(|l| (l.key, l.value))
                            .collect(),
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
                        service_affinity: args.orchestrator_kubernetes_service_affinity,
                        service_tolerations: args.orchestrator_kubernetes_service_tolerations,
                        service_account: args.orchestrator_kubernetes_service_account,
                        image_pull_policy: args.orchestrator_kubernetes_image_pull_policy,
                        aws_external_id_prefix: args.aws_external_id_prefix.clone(),
                        coverage: args.orchestrator_kubernetes_coverage,
                        ephemeral_volume_storage_class: args
                            .orchestrator_kubernetes_ephemeral_volume_class
                            .clone(),
                        service_fs_group: args.orchestrator_kubernetes_service_fs_group.clone(),
                        name_prefix: args.orchestrator_kubernetes_name_prefix.clone(),
                        collect_pod_metrics: !args
                            .orchestrator_kubernetes_disable_pod_metrics_collection,
                        enable_prometheus_scrape_annotations: args
                            .orchestrator_kubernetes_enable_prometheus_scrape_annotations,
                    }))
                    .context("creating kubernetes orchestrator")?,
            );
            let secrets_controller: Arc<dyn SecretsController> = match args.secrets_controller {
                SecretsControllerKind::Kubernetes => {
                    let sc = Arc::clone(&orchestrator);
                    let sc: Arc<dyn SecretsController> = sc;
                    sc
                }
                SecretsControllerKind::AwsSecretsManager => {
                    Arc::new(
                        runtime.block_on(AwsSecretsController::new(
                            // TODO [Alex Hunt] move this to a shared function that can be imported by the
                            // region-controller.
                            &aws_secrets_controller_prefix(&args.environment_id),
                            &aws_secrets_controller_key_alias(&args.environment_id),
                            args.aws_secrets_controller_tags
                                .into_iter()
                                .map(|tag| (tag.key, tag.value))
                                .collect(),
                        )),
                    )
                }
                SecretsControllerKind::LocalFile => bail!(
                    "SecretsControllerKind::LocalFile is not compatible with Orchestrator::Kubernetes."
                ),
            };
            let cloud_resource_controller = Arc::clone(&orchestrator);
            (
                orchestrator,
                secrets_controller,
                Some(cloud_resource_controller),
            )
        }
        OrchestratorKind::Process => {
            if args
                .orchestrator_kubernetes_ephemeral_volume_class
                .is_some()
            {
                bail!(
                    "--orchestrator-kubernetes-ephemeral-volume-class is \
                      not usable with the process orchestrator"
                );
            }
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
                            .clone()
                            .expect("clap enforced"),
                        command_wrapper: args
                            .orchestrator_process_wrapper
                            .map_or(Ok(vec![]), |s| shell_words::split(&s))?,
                        propagate_crashes: args.orchestrator_process_propagate_crashes,
                        tcp_proxy: args.orchestrator_process_tcp_proxy_listen_addr.map(
                            |listen_addr| ProcessOrchestratorTcpProxyConfig {
                                listen_addr,
                                prometheus_service_discovery_dir: args
                                    .orchestrator_process_prometheus_service_discovery_directory,
                            },
                        ),
                        scratch_directory: args
                            .orchestrator_process_scratch_directory
                            .expect("process orchestrator requires scratch directory"),
                    }))
                    .context("creating process orchestrator")?,
            );
            let secrets_controller: Arc<dyn SecretsController> = match args.secrets_controller {
                SecretsControllerKind::Kubernetes => bail!(
                    "SecretsControllerKind::Kubernetes is not compatible with Orchestrator::Process."
                ),
                SecretsControllerKind::AwsSecretsManager => Arc::new(
                    runtime.block_on(AwsSecretsController::new(
                        &aws_secrets_controller_prefix(&args.environment_id),
                        &aws_secrets_controller_key_alias(&args.environment_id),
                        args.aws_secrets_controller_tags
                            .into_iter()
                            .map(|tag| (tag.key, tag.value))
                            .collect(),
                    )),
                ),
                SecretsControllerKind::LocalFile => {
                    let sc = Arc::clone(&orchestrator);
                    let sc: Arc<dyn SecretsController> = sc;
                    sc
                }
            };
            (orchestrator, secrets_controller, None)
        }
    };
    drop(entered);
    let cloud_resource_reader = cloud_resource_controller.as_ref().map(|c| c.reader());
    let secrets_reader = secrets_controller.reader();
    let now = SYSTEM_TIME.clone();

    let mut persist_config =
        PersistConfig::new(&BUILD_INFO, now.clone(), mz_dyncfgs::all_dyncfgs());
    // Start with compaction disabled, later enable it if we're not in read-only mode.
    persist_config.disable_compaction();

    let persist_pubsub_server = PersistGrpcPubSubServer::new(&persist_config, &metrics_registry);
    let persist_pubsub_client = persist_pubsub_server.new_same_process_connection();

    match args.persist_isolated_runtime_threads {
        // Use the default.
        None | Some(0) => (),
        Some(x @ ..=-1) => {
            let threads = num_cpus::get().saturating_add_signed(x).max(1);
            persist_config.isolated_runtime_worker_threads = threads;
        }
        Some(x @ 1..) => {
            let threads = usize::try_from(x).expect("pattern matched a positive value");
            persist_config.isolated_runtime_worker_threads = threads;
        }
    };

    let _server = runtime.spawn_named(
        || "persist::rpc::server",
        async move {
            info!(
                "listening for Persist PubSub connections on {}",
                args.internal_persist_pubsub_listen_addr
            );
            // Intentionally do not bubble up errors here, we don't want to take
            // down environmentd if there are any issues with the pubsub server.
            let res = persist_pubsub_server
                .serve(args.internal_persist_pubsub_listen_addr)
                .await;
            error!("Persist Pubsub server exited {:?}", res);
        }
        .instrument(tracing::info_span!("persist::rpc::server")),
    );

    let persist_clients = {
        // PersistClientCache may spawn tasks, so run within a tokio runtime context
        let _tokio_guard = runtime.enter();
        PersistClientCache::new(persist_config, &metrics_registry, |cfg, metrics| {
            let sender: Arc<dyn PubSubSender> = Arc::new(MetricsSameProcessPubSubSender::new(
                cfg,
                persist_pubsub_client.sender,
                metrics,
            ));
            PubSubClientConnection::new(sender, persist_pubsub_client.receiver)
        })
    };

    let consensus_uri = args.persist_consensus_url.unwrap_or_else(|| {
        args.metadata_backend_url
            .as_ref()
            .map(|metadata_backend_url| {
                SensitiveUrl(
                    Url::parse_with_params(
                        metadata_backend_url.0.as_ref(),
                        &[("options", "--search_path=consensus")],
                    )
                    .unwrap(),
                )
            })
            .expect("either --persist-consensus-url or --metadata-backend-url must be provided")
    });
    let timestamp_oracle_url = args.timestamp_oracle_url.or_else(|| {
        args.metadata_backend_url
            .as_ref()
            .map(|metadata_backend_url| {
                SensitiveUrl(
                    Url::parse_with_params(
                        metadata_backend_url.0.as_ref(),
                        &[("options", "--search_path=tsoracle")],
                    )
                    .unwrap(),
                )
            })
    });

    let persist_clients = Arc::new(persist_clients);
    let connection_context = ConnectionContext::from_cli_args(
        args.environment_id.to_string(),
        &args.tracing.startup_log_filter,
        args.aws_external_id_prefix,
        args.aws_connection_role_arn,
        secrets_reader,
        cloud_resource_reader,
    );
    let orchestrator = Arc::new(TracingOrchestrator::new(orchestrator, args.tracing.clone()));
    let controller = ControllerConfig {
        build_info: &BUILD_INFO,
        orchestrator,
        persist_location: PersistLocation {
            blob_uri: args.persist_blob_url,
            consensus_uri,
        },
        persist_clients: Arc::clone(&persist_clients),
        clusterd_image: args.clusterd_image.expect("clap enforced"),
        init_container_image: args.orchestrator_kubernetes_init_container_image,
        deploy_generation: args.deploy_generation,
        now: SYSTEM_TIME.clone(),
        metrics_registry: metrics_registry.clone(),
        persist_pubsub_url: args.persist_pubsub_url,
        connection_context,
        // When serialized to args in the controller, only the relevant flags will be passed
        // through, so we just set all of them
        secrets_args: SecretsReaderCliArgs {
            secrets_reader: args.secrets_controller,
            secrets_reader_local_file_dir: args.orchestrator_process_secrets_directory,
            secrets_reader_kubernetes_context: Some(args.orchestrator_kubernetes_context),
            secrets_reader_aws_prefix: Some(aws_secrets_controller_prefix(&args.environment_id)),
            secrets_reader_name_prefix: args.orchestrator_kubernetes_name_prefix.clone(),
        },
    };

    let cluster_replica_sizes = ClusterReplicaSizeMap::parse_from_str(
        &args.cluster_replica_sizes,
        !license_key.allow_credit_consumption_override,
    )
    .context("parsing replica size map")?;

    emit_boot_diagnostics!(&BUILD_INFO);
    sys::adjust_rlimits();

    info!(
        "startup: envd init: preamble complete in {:?}",
        envd_start.elapsed()
    );

    let serve_start = Instant::now();
    info!("startup: envd init: serving beginning");
    let server = runtime.block_on(async {
        let listeners = Listeners::bind(listeners_config).await?;
        let catalog_config = CatalogConfig {
            persist_clients,
            metrics: Arc::new(mz_catalog::durable::Metrics::new(&metrics_registry)),
        };
        let server = listeners
            .serve(crate::Config {
                // Special modes.
                unsafe_mode: args.unsafe_mode,
                all_features: args.all_features,
                // Connection options.
                tls,
                tls_reload_certs: mz_server_core::default_cert_reload_ticker(),
                external_login_password_mz_system: args.external_login_password_mz_system,
                frontegg,
                cors_allowed_origin,
                egress_addresses: args.announce_egress_address,
                http_host_name: args.http_host_name,
                internal_console_redirect_url: args.internal_console_redirect_url,
                // Controller options.
                controller,
                secrets_controller,
                cloud_resource_controller,
                // Storage options.
                storage_usage_collection_interval: args.storage_usage_collection_interval_sec,
                storage_usage_retention_period: args.storage_usage_retention_period,
                // Adapter options.
                catalog_config,
                availability_zones: args.availability_zone,
                cluster_replica_sizes,
                timestamp_oracle_url,
                segment_api_key: args.segment_api_key,
                segment_client_side: args.segment_client_side,
                test_only_dummy_segment_client: args.test_only_dummy_segment_client,
                launchdarkly_sdk_key: args.launchdarkly_sdk_key,
                launchdarkly_key_map: args
                    .launchdarkly_key_map
                    .into_iter()
                    .map(|kv| (kv.key, kv.value))
                    .collect(),
                config_sync_timeout: args.config_sync_timeout,
                config_sync_loop_interval: args.config_sync_loop_interval,
                config_sync_file_path: args.config_sync_file_path,

                // Bootstrap options.
                environment_id: args.environment_id,
                bootstrap_role: args.bootstrap_role,
                bootstrap_default_cluster_replica_size: args.bootstrap_default_cluster_replica_size,
                bootstrap_default_cluster_replication_factor: args
                    .bootstrap_default_cluster_replication_factor,
                bootstrap_builtin_system_cluster_config: BootstrapBuiltinClusterConfig {
                    size: args.bootstrap_builtin_system_cluster_replica_size,
                    replication_factor: args.bootstrap_builtin_system_cluster_replication_factor,
                },
                bootstrap_builtin_catalog_server_cluster_config: BootstrapBuiltinClusterConfig {
                    size: args.bootstrap_builtin_catalog_server_cluster_replica_size,
                    replication_factor: args
                        .bootstrap_builtin_catalog_server_cluster_replication_factor,
                },
                bootstrap_builtin_probe_cluster_config: BootstrapBuiltinClusterConfig {
                    size: args.bootstrap_builtin_probe_cluster_replica_size,
                    replication_factor: args.bootstrap_builtin_probe_cluster_replication_factor,
                },
                bootstrap_builtin_support_cluster_config: BootstrapBuiltinClusterConfig {
                    size: args.bootstrap_builtin_support_cluster_replica_size,
                    replication_factor: args.bootstrap_builtin_support_cluster_replication_factor,
                },
                bootstrap_builtin_analytics_cluster_config: BootstrapBuiltinClusterConfig {
                    size: args.bootstrap_builtin_analytics_cluster_replica_size,
                    replication_factor: args.bootstrap_builtin_analytics_cluster_replication_factor,
                },
                system_parameter_defaults: args
                    .system_parameter_default
                    .into_iter()
                    .map(|kv| (kv.key, kv.value))
                    .collect(),
                helm_chart_version: args.helm_chart_version.clone(),
                license_key,
                // AWS options.
                aws_account_id: args.aws_account_id,
                aws_privatelink_availability_zones: args.aws_privatelink_availability_zones,
                // Observability options.
                metrics_registry,
                tracing_handle,
                // Testing options.
                now,
            })
            .await
            .maybe_terminate("booting server")?;
        Ok::<_, anyhow::Error>(server)
    })?;
    info!(
        "startup: envd init: serving complete in {:?}",
        serve_start.elapsed()
    );

    let start_duration = envd_start.elapsed();
    metrics
        .start_time_environmentd
        .set(start_duration.as_millis().try_into().expect("must fit"));
    let span = span.exit();
    let id = span.context().span().span_context().trace_id();
    drop(span);

    info!("startup: envd init: complete in {start_duration:?}");

    println!(
        "environmentd {} listening...",
        BUILD_INFO.human_version(args.helm_chart_version)
    );
    for (name, handle) in &server.sql_listener_handles {
        println!("{} SQL address: {}", name, handle.local_addr);
    }
    for (name, handle) in &server.http_listener_handles {
        println!("{} HTTP address: {}", name, handle.local_addr);
    }
    // TODO move persist pubsub address like metrics address?
    println!(
        " Internal Persist PubSub address: {}",
        args.internal_persist_pubsub_listen_addr
    );

    println!(" Root trace ID: {id}");

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
    pub fn register_into(registry: &MetricsRegistry, build_info: BuildInfo) -> Metrics {
        Metrics {
            start_time_environmentd: registry.register(metric!(
                name: "mz_start_time_environmentd",
                help: "Time in milliseconds from environmentd start until the adapter is ready.",
                const_labels: {
                    "version" => build_info.version,
                    "build_type" => if cfg!(release) { "release" } else { "debug" }
                },
            )),
        }
    }
}

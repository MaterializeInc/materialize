// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A SQL stream processor built on top of [timely dataflow] and
//! [differential dataflow].
//!
//! [differential dataflow]: ../differential_dataflow/index.html
//! [timely dataflow]: ../timely/index.html

use std::collections::BTreeMap;
use std::panic::AssertUnwindSafe;
use std::path::PathBuf;
use std::pin::Pin;
use std::sync::Arc;
use std::time::{Duration, Instant};
use std::{env, io};

use anyhow::{Context, anyhow};
use derivative::Derivative;
use futures::FutureExt;
use ipnet::IpNet;
use mz_adapter::config::{
    SystemParameterSyncClientConfig, SystemParameterSyncConfig, system_parameter_sync,
};
use mz_adapter::webhook::WebhookConcurrencyLimiter;
use mz_adapter::{AdapterError, Client as AdapterClient, load_remote_system_parameters};
use mz_adapter_types::bootstrap_builtin_cluster_config::BootstrapBuiltinClusterConfig;
use mz_adapter_types::dyncfgs::{
    ENABLE_0DT_DEPLOYMENT_PANIC_AFTER_TIMEOUT, WITH_0DT_DEPLOYMENT_DDL_CHECK_INTERVAL,
    WITH_0DT_DEPLOYMENT_MAX_WAIT,
};
use mz_auth::password::Password;
use mz_authenticator::Authenticator;
use mz_build_info::{BuildInfo, build_info};
use mz_catalog::config::ClusterReplicaSizeMap;
use mz_catalog::durable::BootstrapArgs;
use mz_cloud_resources::CloudResourceController;
use mz_controller::ControllerConfig;
use mz_frontegg_auth::Authenticator as FronteggAuthenticator;
use mz_license_keys::ValidatedLicenseKey;
use mz_ore::future::OreFutureExt;
use mz_ore::metrics::MetricsRegistry;
use mz_ore::now::NowFn;
use mz_ore::tracing::TracingHandle;
use mz_ore::url::SensitiveUrl;
use mz_ore::{instrument, task};
use mz_persist_client::cache::PersistClientCache;
use mz_persist_client::usage::StorageUsageClient;
use mz_pgwire::MetricsConfig;
use mz_pgwire_common::ConnectionCounter;
use mz_repr::strconv;
use mz_secrets::SecretsController;
use mz_server_core::listeners::{
    AuthenticatorKind, HttpListenerConfig, ListenerConfig, ListenersConfig, SqlListenerConfig,
};
use mz_server_core::{
    ConnectionStream, ListenerHandle, ReloadTrigger, ReloadingSslContext, ServeConfig,
    TlsCertConfig, TlsMode,
};
use mz_sql::catalog::EnvironmentId;
use mz_sql::session::vars::{Value, VarInput};
use tokio::sync::oneshot;
use tower_http::cors::AllowOrigin;
use tracing::{Instrument, info, info_span};

use crate::deployment::preflight::{PreflightInput, PreflightOutput};
use crate::deployment::state::DeploymentState;
use crate::http::{HttpConfig, HttpServer, InternalRouteConfig};

pub use crate::http::{SqlResponse, WebSocketAuth, WebSocketResponse};

mod deployment;
pub mod environmentd;
pub mod http;
mod telemetry;
#[cfg(feature = "test")]
pub mod test_util;

pub const BUILD_INFO: BuildInfo = build_info!();

/// Configuration for an `environmentd` server.
#[derive(Derivative)]
#[derivative(Debug)]
pub struct Config {
    // === Special modes. ===
    /// Whether to permit usage of unsafe features. This is never meant to run
    /// in production.
    pub unsafe_mode: bool,
    /// Whether the environmentd is running on a local dev machine. This is
    /// never meant to run in production or CI.
    pub all_features: bool,

    // === Connection options. ===
    /// TLS encryption and authentication configuration.
    pub tls: Option<TlsCertConfig>,
    /// Trigger to attempt to reload TLS certififcates.
    #[derivative(Debug = "ignore")]
    pub tls_reload_certs: ReloadTrigger,
    /// Password of the mz_system user.
    pub external_login_password_mz_system: Option<Password>,
    /// Frontegg JWT authentication configuration.
    pub frontegg: Option<FronteggAuthenticator>,
    /// Origins for which cross-origin resource sharing (CORS) for HTTP requests
    /// is permitted.
    pub cors_allowed_origin: AllowOrigin,
    /// Public IP addresses which the cloud environment has configured for
    /// egress.
    pub egress_addresses: Vec<IpNet>,
    /// The external host name to connect to the HTTP server of this
    /// environment.
    ///
    /// Presently used to render webhook URLs for end users in notices and the
    /// system catalog. Not used to establish connections directly.
    pub http_host_name: Option<String>,
    /// The URL of the Materialize console to proxy from the /internal-console
    /// endpoint on the internal HTTP server.
    pub internal_console_redirect_url: Option<String>,

    // === Controller options. ===
    /// Storage and compute controller configuration.
    pub controller: ControllerConfig,
    /// Secrets controller configuration.
    pub secrets_controller: Arc<dyn SecretsController>,
    /// VpcEndpoint controller configuration.
    pub cloud_resource_controller: Option<Arc<dyn CloudResourceController>>,

    // === Storage options. ===
    /// The interval at which to collect storage usage information.
    pub storage_usage_collection_interval: Duration,
    /// How long to retain storage usage records for.
    pub storage_usage_retention_period: Option<Duration>,

    // === Adapter options. ===
    /// Catalog configuration.
    pub catalog_config: CatalogConfig,
    /// Availability zones in which storage and compute resources may be
    /// deployed.
    pub availability_zones: Vec<String>,
    /// A map from size name to resource allocations for cluster replicas.
    pub cluster_replica_sizes: ClusterReplicaSizeMap,
    /// The PostgreSQL URL for the Postgres-backed timestamp oracle.
    pub timestamp_oracle_url: Option<SensitiveUrl>,
    /// An API key for Segment. Enables export of audit events to Segment.
    pub segment_api_key: Option<String>,
    /// Whether the Segment client is being used on the client side
    /// (rather than the server side).
    pub segment_client_side: bool,
    /// Only create a dummy segment client, only to get more testing coverage.
    pub test_only_dummy_segment_client: bool,
    /// An SDK key for LaunchDarkly. Enables system parameter synchronization
    /// with LaunchDarkly.
    pub launchdarkly_sdk_key: Option<String>,
    /// An invertible map from system parameter names to LaunchDarkly feature
    /// keys to use when propagating values from the latter to the former.
    pub launchdarkly_key_map: BTreeMap<String, String>,
    /// The duration at which the system parameter synchronization times out during startup.
    pub config_sync_timeout: Duration,
    /// The interval in seconds at which to synchronize system parameter values.
    pub config_sync_loop_interval: Option<Duration>,
    /// The path for file based config sync
    pub config_sync_file_path: Option<PathBuf>,

    // === Bootstrap options. ===
    /// The cloud ID of this environment.
    pub environment_id: EnvironmentId,
    /// What role, if any, should be initially created with elevated privileges.
    pub bootstrap_role: Option<String>,
    /// The size of the default cluster replica if bootstrapping.
    pub bootstrap_default_cluster_replica_size: String,
    /// The default number of replicas if bootstrapping.
    pub bootstrap_default_cluster_replication_factor: u32,
    /// The config of the builtin system cluster replicas if bootstrapping.
    pub bootstrap_builtin_system_cluster_config: BootstrapBuiltinClusterConfig,
    /// The config of the builtin catalog server cluster replicas if bootstrapping.
    pub bootstrap_builtin_catalog_server_cluster_config: BootstrapBuiltinClusterConfig,
    /// The config of the builtin probe cluster replicas if bootstrapping.
    pub bootstrap_builtin_probe_cluster_config: BootstrapBuiltinClusterConfig,
    /// The config of the builtin support cluster replicas if bootstrapping.
    pub bootstrap_builtin_support_cluster_config: BootstrapBuiltinClusterConfig,
    /// The config of the builtin analytics cluster replicas if bootstrapping.
    pub bootstrap_builtin_analytics_cluster_config: BootstrapBuiltinClusterConfig,
    /// Values to set for system parameters, if those system parameters have not
    /// already been set by the system user.
    pub system_parameter_defaults: BTreeMap<String, String>,
    /// Helm chart version
    pub helm_chart_version: Option<String>,
    /// Configuration managed by license keys
    pub license_key: ValidatedLicenseKey,

    // === AWS options. ===
    /// The AWS account ID, which will be used to generate ARNs for
    /// Materialize-controlled AWS resources.
    pub aws_account_id: Option<String>,
    /// Supported AWS PrivateLink availability zone ids.
    pub aws_privatelink_availability_zones: Option<Vec<String>>,

    // === Observability options. ===
    /// The metrics registry to use.
    pub metrics_registry: MetricsRegistry,
    /// Handle to tracing.
    pub tracing_handle: TracingHandle,

    // === Testing options. ===
    /// A now generation function for mocking time.
    pub now: NowFn,
    /// If `Some`, force running builtin schema migration using the specified
    /// migration mechanism ("evolution" or "replacement").
    pub force_builtin_schema_migration: Option<String>,
}

/// Configuration for the Catalog.
#[derive(Debug, Clone)]
pub struct CatalogConfig {
    /// A process-global cache of (blob_uri, consensus_uri) -> PersistClient.
    pub persist_clients: Arc<PersistClientCache>,
    /// Persist catalog metrics.
    pub metrics: Arc<mz_catalog::durable::Metrics>,
}

pub struct Listener<C> {
    pub handle: ListenerHandle,
    connection_stream: Pin<Box<dyn ConnectionStream>>,
    config: C,
}
impl<C> Listener<C>
where
    C: ListenerConfig,
{
    /// Initializes network listeners for a later call to `serve` at the
    /// specified addresses.
    ///
    /// Splitting this function out from `serve` has two benefits:
    ///
    ///   * It initializes network listeners as early as possible, so that the OS
    ///     will queue incoming connections while the server is booting.
    ///
    ///   * It allows the caller to communicate with the server via the internal
    ///     HTTP port while it is booting.
    ///
    async fn bind(config: C) -> Result<Self, io::Error> {
        let (handle, connection_stream) = mz_server_core::listen(&config.addr()).await?;
        Ok(Self {
            handle,
            connection_stream,
            config,
        })
    }
}

impl Listener<SqlListenerConfig> {
    #[instrument(name = "environmentd::serve_sql")]
    pub async fn serve_sql(
        self,
        name: String,
        active_connection_counter: ConnectionCounter,
        tls_reloading_context: Option<ReloadingSslContext>,
        frontegg: Option<FronteggAuthenticator>,
        adapter_client: AdapterClient,
        metrics: MetricsConfig,
        helm_chart_version: Option<String>,
    ) -> ListenerHandle {
        let label: &'static str = Box::leak(name.into_boxed_str());
        let tls = tls_reloading_context.map(|context| mz_server_core::ReloadingTlsConfig {
            context,
            mode: if self.config.enable_tls {
                TlsMode::Require
            } else {
                TlsMode::Allow
            },
        });
        let authenticator = match self.config.authenticator_kind {
            AuthenticatorKind::Frontegg => Authenticator::Frontegg(
                frontegg.expect("Frontegg args are required with AuthenticatorKind::Frontegg"),
            ),
            AuthenticatorKind::Password => Authenticator::Password(adapter_client.clone()),
            AuthenticatorKind::Sasl => Authenticator::Sasl(adapter_client.clone()),
            AuthenticatorKind::None => Authenticator::None,
        };

        task::spawn(|| format!("{}_sql_server", label), {
            let sql_server = mz_pgwire::Server::new(mz_pgwire::Config {
                label,
                tls,
                adapter_client,
                authenticator,
                metrics,
                active_connection_counter,
                helm_chart_version,
                allowed_roles: self.config.allowed_roles,
            });
            mz_server_core::serve(ServeConfig {
                conns: self.connection_stream,
                server: sql_server,
                // `environmentd` does not currently need to dynamically
                // configure graceful termination behavior.
                dyncfg: None,
            })
        });
        self.handle
    }
}

impl Listener<HttpListenerConfig> {
    #[instrument(name = "environmentd::serve_http")]
    pub async fn serve_http(self, config: HttpConfig) -> ListenerHandle {
        let task_name = format!("{}_http_server", &config.source);
        task::spawn(|| task_name, {
            let http_server = HttpServer::new(config);
            mz_server_core::serve(ServeConfig {
                conns: self.connection_stream,
                server: http_server,
                // `environmentd` does not currently need to dynamically
                // configure graceful termination behavior.
                dyncfg: None,
            })
        });
        self.handle
    }
}

pub struct Listeners {
    pub http: BTreeMap<String, Listener<HttpListenerConfig>>,
    pub sql: BTreeMap<String, Listener<SqlListenerConfig>>,
}

impl Listeners {
    pub async fn bind(config: ListenersConfig) -> Result<Self, io::Error> {
        let mut sql = BTreeMap::new();
        for (name, config) in config.sql {
            sql.insert(name, Listener::bind(config).await?);
        }

        let mut http = BTreeMap::new();
        for (name, config) in config.http {
            http.insert(name, Listener::bind(config).await?);
        }

        Ok(Listeners { http, sql })
    }

    /// Starts an `environmentd` server.
    ///
    /// Returns a handle to the server once it is fully booted.
    #[instrument(name = "environmentd::serve")]
    pub async fn serve(self, config: Config) -> Result<Server, AdapterError> {
        let serve_start = Instant::now();
        info!("startup: envd serve: beginning");
        info!("startup: envd serve: preamble beginning");

        // Validate TLS configuration, if present.
        let tls_reloading_context = match config.tls {
            Some(tls_config) => Some(tls_config.reloading_context(config.tls_reload_certs)?),
            None => None,
        };

        let active_connection_counter = ConnectionCounter::default();
        let (deployment_state, deployment_state_handle) = DeploymentState::new();

        // Launch HTTP servers.
        //
        // We start these servers before we've completed initialization so that
        // metrics are accessible during initialization. Some HTTP
        // endpoints require the adapter to be initialized; requests to those
        // endpoints block until the adapter client is installed.
        // One of these endpoints is /api/readyz,
        // which assumes we're ready when the adapter client exists.
        let webhook_concurrency_limit = WebhookConcurrencyLimiter::default();
        let internal_route_config = Arc::new(InternalRouteConfig {
            deployment_state_handle,
            internal_console_redirect_url: config.internal_console_redirect_url,
        });

        let (authenticator_frontegg_tx, authenticator_frontegg_rx) = oneshot::channel();
        let authenticator_frontegg_rx = authenticator_frontegg_rx.shared();
        let (authenticator_password_tx, authenticator_password_rx) = oneshot::channel();
        let authenticator_password_rx = authenticator_password_rx.shared();
        let (authenticator_none_tx, authenticator_none_rx) = oneshot::channel();
        let authenticator_none_rx = authenticator_none_rx.shared();

        // We can only send the Frontegg and None variants immediately.
        // The Password variant requires an adapter client.
        if let Some(frontegg) = &config.frontegg {
            authenticator_frontegg_tx
                .send(Arc::new(Authenticator::Frontegg(frontegg.clone())))
                .expect("rx known to be live");
        }
        authenticator_none_tx
            .send(Arc::new(Authenticator::None))
            .expect("rx known to be live");

        let (adapter_client_tx, adapter_client_rx) = oneshot::channel();
        let adapter_client_rx = adapter_client_rx.shared();

        let metrics_registry = config.metrics_registry.clone();
        let metrics = http::Metrics::register_into(&metrics_registry, "mz_http");
        let mut http_listener_handles = BTreeMap::new();
        for (name, listener) in self.http {
            let authenticator_kind = listener.config.authenticator_kind();
            let authenticator_rx = match authenticator_kind {
                AuthenticatorKind::Frontegg => authenticator_frontegg_rx.clone(),
                AuthenticatorKind::Password => authenticator_password_rx.clone(),
                AuthenticatorKind::Sasl => authenticator_password_rx.clone(),
                AuthenticatorKind::None => authenticator_none_rx.clone(),
            };
            let source: &'static str = Box::leak(name.clone().into_boxed_str());
            let tls = if listener.config.enable_tls() {
                tls_reloading_context.clone()
            } else {
                None
            };
            let http_config = HttpConfig {
                adapter_client_rx: adapter_client_rx.clone(),
                active_connection_counter: active_connection_counter.clone(),
                helm_chart_version: config.helm_chart_version.clone(),
                source,
                tls,
                authenticator_kind,
                authenticator_rx,
                allowed_origin: config.cors_allowed_origin.clone(),
                concurrent_webhook_req: webhook_concurrency_limit.semaphore(),
                metrics: metrics.clone(),
                metrics_registry: metrics_registry.clone(),
                allowed_roles: listener.config.allowed_roles(),
                internal_route_config: Arc::clone(&internal_route_config),
                routes_enabled: listener.config.routes.clone(),
            };
            http_listener_handles.insert(name.clone(), listener.serve_http(http_config).await);
        }

        info!(
            "startup: envd serve: preamble complete in {:?}",
            serve_start.elapsed()
        );

        let catalog_init_start = Instant::now();
        info!("startup: envd serve: catalog init beginning");

        // Get the current timestamp so we can record when we booted.
        let boot_ts = (config.now)().into();

        let persist_client = config
            .catalog_config
            .persist_clients
            .open(config.controller.persist_location.clone())
            .await
            .context("opening persist client")?;
        let mut openable_adapter_storage = mz_catalog::durable::persist_backed_catalog_state(
            persist_client.clone(),
            config.environment_id.organization_id(),
            BUILD_INFO.semver_version(),
            Some(config.controller.deploy_generation),
            Arc::clone(&config.catalog_config.metrics),
        )
        .await?;

        info!(
            "startup: envd serve: catalog init complete in {:?}",
            catalog_init_start.elapsed()
        );

        let system_param_sync_start = Instant::now();
        info!("startup: envd serve: system parameter sync beginning");
        // Initialize the system parameter frontend
        let system_parameter_sync_config =
            match (config.launchdarkly_sdk_key, config.config_sync_file_path) {
                (None, None) => None,
                (None, Some(f)) => Some(SystemParameterSyncConfig::new(
                    config.environment_id.clone(),
                    &BUILD_INFO,
                    &config.metrics_registry,
                    config.launchdarkly_key_map,
                    SystemParameterSyncClientConfig::File { path: f },
                )),
                (Some(key), None) => Some(SystemParameterSyncConfig::new(
                    config.environment_id.clone(),
                    &BUILD_INFO,
                    &config.metrics_registry,
                    config.launchdarkly_key_map,
                    SystemParameterSyncClientConfig::LaunchDarkly {
                        sdk_key: key,
                        now_fn: config.now.clone(),
                    },
                )),

                (Some(_), Some(_)) => {
                    panic!("Cannot configure both file and Launchdarkly based config syncing")
                }
            };

        let remote_system_parameters = load_remote_system_parameters(
            &mut openable_adapter_storage,
            system_parameter_sync_config.clone(),
            config.config_sync_timeout,
        )
        .await?;
        info!(
            "startup: envd serve: system parameter sync complete in {:?}",
            system_param_sync_start.elapsed()
        );

        let preflight_checks_start = Instant::now();
        info!("startup: envd serve: preflight checks beginning");

        // Determine the maximum wait time when doing a 0dt deployment.
        let with_0dt_deployment_max_wait = {
            let cli_default = config
                .system_parameter_defaults
                .get(WITH_0DT_DEPLOYMENT_MAX_WAIT.name())
                .map(|x| {
                    Duration::parse(VarInput::Flat(x)).map_err(|err| {
                        anyhow!(
                            "failed to parse default for {}: {:?}",
                            WITH_0DT_DEPLOYMENT_MAX_WAIT.name(),
                            err
                        )
                    })
                })
                .transpose()?;
            let compiled_default = WITH_0DT_DEPLOYMENT_MAX_WAIT.default().clone();
            let ld = get_ld_value(
                WITH_0DT_DEPLOYMENT_MAX_WAIT.name(),
                &remote_system_parameters,
                |x| {
                    Duration::parse(VarInput::Flat(x)).map_err(|err| {
                        format!(
                            "failed to parse LD value {} for {}: {:?}",
                            x,
                            WITH_0DT_DEPLOYMENT_MAX_WAIT.name(),
                            err
                        )
                    })
                },
            )?;
            let catalog = openable_adapter_storage
                .get_0dt_deployment_max_wait()
                .await?;
            let computed = ld.or(catalog).or(cli_default).unwrap_or(compiled_default);
            info!(
                ?computed,
                ?ld,
                ?catalog,
                ?cli_default,
                ?compiled_default,
                "determined value for {} system parameter",
                WITH_0DT_DEPLOYMENT_MAX_WAIT.name()
            );
            computed
        };
        // Determine the DDL check interval when doing a 0dt deployment.
        let with_0dt_deployment_ddl_check_interval = {
            let cli_default = config
                .system_parameter_defaults
                .get(WITH_0DT_DEPLOYMENT_DDL_CHECK_INTERVAL.name())
                .map(|x| {
                    Duration::parse(VarInput::Flat(x)).map_err(|err| {
                        anyhow!(
                            "failed to parse default for {}: {:?}",
                            WITH_0DT_DEPLOYMENT_DDL_CHECK_INTERVAL.name(),
                            err
                        )
                    })
                })
                .transpose()?;
            let compiled_default = WITH_0DT_DEPLOYMENT_DDL_CHECK_INTERVAL.default().clone();
            let ld = get_ld_value(
                WITH_0DT_DEPLOYMENT_DDL_CHECK_INTERVAL.name(),
                &remote_system_parameters,
                |x| {
                    Duration::parse(VarInput::Flat(x)).map_err(|err| {
                        format!(
                            "failed to parse LD value {} for {}: {:?}",
                            x,
                            WITH_0DT_DEPLOYMENT_DDL_CHECK_INTERVAL.name(),
                            err
                        )
                    })
                },
            )?;
            let catalog = openable_adapter_storage
                .get_0dt_deployment_ddl_check_interval()
                .await?;
            let computed = ld.or(catalog).or(cli_default).unwrap_or(compiled_default);
            info!(
                ?computed,
                ?ld,
                ?catalog,
                ?cli_default,
                ?compiled_default,
                "determined value for {} system parameter",
                WITH_0DT_DEPLOYMENT_DDL_CHECK_INTERVAL.name()
            );
            computed
        };

        // Determine whether we should panic if we reach the maximum wait time
        // without the preflight checks succeeding.
        let enable_0dt_deployment_panic_after_timeout = {
            let cli_default = config
                .system_parameter_defaults
                .get(ENABLE_0DT_DEPLOYMENT_PANIC_AFTER_TIMEOUT.name())
                .map(|x| {
                    strconv::parse_bool(x).map_err(|err| {
                        anyhow!(
                            "failed to parse default for {}: {}",
                            ENABLE_0DT_DEPLOYMENT_PANIC_AFTER_TIMEOUT.name(),
                            err
                        )
                    })
                })
                .transpose()?;
            let compiled_default = ENABLE_0DT_DEPLOYMENT_PANIC_AFTER_TIMEOUT.default().clone();
            let ld = get_ld_value(
                "enable_0dt_deployment_panic_after_timeout",
                &remote_system_parameters,
                |x| strconv::parse_bool(x).map_err(|x| x.to_string()),
            )?;
            let catalog = openable_adapter_storage
                .get_enable_0dt_deployment_panic_after_timeout()
                .await?;
            let computed = ld.or(catalog).or(cli_default).unwrap_or(compiled_default);
            info!(
                %computed,
                ?ld,
                ?catalog,
                ?cli_default,
                ?compiled_default,
                "determined value for enable_0dt_deployment_panic_after_timeout system parameter",
            );
            computed
        };

        // Perform preflight checks.
        //
        // Preflight checks determine whether to boot in read-only mode or not.
        let bootstrap_args = BootstrapArgs {
            default_cluster_replica_size: config.bootstrap_default_cluster_replica_size.clone(),
            default_cluster_replication_factor: config.bootstrap_default_cluster_replication_factor,
            bootstrap_role: config.bootstrap_role.clone(),
            cluster_replica_size_map: config.cluster_replica_sizes.clone(),
        };
        let preflight_config = PreflightInput {
            boot_ts,
            environment_id: config.environment_id.clone(),
            persist_client,
            deploy_generation: config.controller.deploy_generation,
            deployment_state: deployment_state.clone(),
            openable_adapter_storage,
            catalog_metrics: Arc::clone(&config.catalog_config.metrics),
            caught_up_max_wait: with_0dt_deployment_max_wait,
            panic_after_timeout: enable_0dt_deployment_panic_after_timeout,
            bootstrap_args,
            ddl_check_interval: with_0dt_deployment_ddl_check_interval,
        };
        let PreflightOutput {
            openable_adapter_storage,
            read_only,
            caught_up_trigger,
        } = deployment::preflight::preflight_0dt(preflight_config).await?;

        info!(
            "startup: envd serve: preflight checks complete in {:?}",
            preflight_checks_start.elapsed()
        );

        let catalog_open_start = Instant::now();
        info!("startup: envd serve: durable catalog open beginning");

        let bootstrap_args = BootstrapArgs {
            default_cluster_replica_size: config.bootstrap_default_cluster_replica_size.clone(),
            default_cluster_replication_factor: config.bootstrap_default_cluster_replication_factor,
            bootstrap_role: config.bootstrap_role,
            cluster_replica_size_map: config.cluster_replica_sizes.clone(),
        };

        // Load the adapter durable storage.
        let (adapter_storage, audit_logs_iterator) = if read_only {
            // TODO: behavior of migrations when booting in savepoint mode is
            // not well defined.
            let (adapter_storage, audit_logs_iterator) = openable_adapter_storage
                .open_savepoint(boot_ts, &bootstrap_args)
                .await?;
            // In read-only mode, we intentionally do not call `set_is_leader`,
            // because we are by definition not the leader if we are in
            // read-only mode.

            (adapter_storage, audit_logs_iterator)
        } else {
            let (adapter_storage, audit_logs_iterator) = openable_adapter_storage
                .open(boot_ts, &bootstrap_args)
                .await?;

            // Once we have successfully opened the adapter storage in
            // read/write mode, we can announce we are the leader, as we've
            // fenced out all other environments using the adapter storage.
            deployment_state.set_is_leader();

            (adapter_storage, audit_logs_iterator)
        };

        // Enable Persist compaction if we're not in read only.
        if !read_only {
            config.controller.persist_clients.cfg().enable_compaction();
        }

        info!(
            "startup: envd serve: durable catalog open complete in {:?}",
            catalog_open_start.elapsed()
        );

        let coord_init_start = Instant::now();
        info!("startup: envd serve: coordinator init beginning");

        if !config
            .cluster_replica_sizes
            .0
            .contains_key(&config.bootstrap_default_cluster_replica_size)
        {
            return Err(anyhow!("bootstrap default cluster replica size is unknown").into());
        }
        let envd_epoch = adapter_storage.epoch();

        // Initialize storage usage client.
        let storage_usage_client = StorageUsageClient::open(
            config
                .controller
                .persist_clients
                .open(config.controller.persist_location.clone())
                .await
                .context("opening storage usage client")?,
        );

        // Initialize adapter.
        let segment_client = config.segment_api_key.map(|api_key| {
            mz_segment::Client::new(mz_segment::Config {
                api_key,
                client_side: config.segment_client_side,
            })
        });
        let connection_limiter = active_connection_counter.clone();
        let connection_limit_callback = Box::new(move |limit, superuser_reserved| {
            connection_limiter.update_limit(limit);
            connection_limiter.update_superuser_reserved(superuser_reserved);
        });

        let (adapter_handle, adapter_client) = mz_adapter::serve(mz_adapter::Config {
            connection_context: config.controller.connection_context.clone(),
            connection_limit_callback,
            controller_config: config.controller,
            controller_envd_epoch: envd_epoch,
            storage: adapter_storage,
            audit_logs_iterator,
            timestamp_oracle_url: config.timestamp_oracle_url,
            unsafe_mode: config.unsafe_mode,
            all_features: config.all_features,
            build_info: &BUILD_INFO,
            environment_id: config.environment_id.clone(),
            metrics_registry: config.metrics_registry.clone(),
            now: config.now,
            secrets_controller: config.secrets_controller,
            cloud_resource_controller: config.cloud_resource_controller,
            cluster_replica_sizes: config.cluster_replica_sizes,
            builtin_system_cluster_config: config.bootstrap_builtin_system_cluster_config,
            builtin_catalog_server_cluster_config: config
                .bootstrap_builtin_catalog_server_cluster_config,
            builtin_probe_cluster_config: config.bootstrap_builtin_probe_cluster_config,
            builtin_support_cluster_config: config.bootstrap_builtin_support_cluster_config,
            builtin_analytics_cluster_config: config.bootstrap_builtin_analytics_cluster_config,
            availability_zones: config.availability_zones,
            system_parameter_defaults: config.system_parameter_defaults,
            storage_usage_client,
            storage_usage_collection_interval: config.storage_usage_collection_interval,
            storage_usage_retention_period: config.storage_usage_retention_period,
            segment_client: segment_client.clone(),
            egress_addresses: config.egress_addresses,
            remote_system_parameters,
            aws_account_id: config.aws_account_id,
            aws_privatelink_availability_zones: config.aws_privatelink_availability_zones,
            webhook_concurrency_limit: webhook_concurrency_limit.clone(),
            http_host_name: config.http_host_name,
            tracing_handle: config.tracing_handle,
            read_only_controllers: read_only,
            caught_up_trigger,
            helm_chart_version: config.helm_chart_version.clone(),
            license_key: config.license_key,
            external_login_password_mz_system: config.external_login_password_mz_system,
            force_builtin_schema_migration: config.force_builtin_schema_migration,
        })
        .instrument(info_span!("adapter::serve"))
        .await?;

        info!(
            "startup: envd serve: coordinator init complete in {:?}",
            coord_init_start.elapsed()
        );

        let serve_postamble_start = Instant::now();
        info!("startup: envd serve: postamble beginning");

        // Send adapter client to the HTTP servers.
        authenticator_password_tx
            .send(Arc::new(Authenticator::Password(adapter_client.clone())))
            .expect("rx known to be live");
        adapter_client_tx
            .send(adapter_client.clone())
            .expect("internal HTTP server should not drop first");

        let metrics = mz_pgwire::MetricsConfig::register_into(&config.metrics_registry);

        // Launch SQL server.
        let mut sql_listener_handles = BTreeMap::new();
        for (name, listener) in self.sql {
            sql_listener_handles.insert(
                name.clone(),
                listener
                    .serve_sql(
                        name,
                        active_connection_counter.clone(),
                        tls_reloading_context.clone(),
                        config.frontegg.clone(),
                        adapter_client.clone(),
                        metrics.clone(),
                        config.helm_chart_version.clone(),
                    )
                    .await,
            );
        }

        // Start telemetry reporting loop.
        if let Some(segment_client) = segment_client {
            telemetry::start_reporting(telemetry::Config {
                segment_client,
                adapter_client: adapter_client.clone(),
                environment_id: config.environment_id,
                report_interval: Duration::from_secs(3600),
            });
        } else if config.test_only_dummy_segment_client {
            // We only have access to a segment client in production but we
            // still want to exercise the telemetry reporting code to a degree.
            // So we create a dummy client and report telemetry into the void.
            // This way we at least run the telemetry queries the way a
            // production environment would.
            tracing::debug!("starting telemetry reporting with a dummy segment client");
            let segment_client = mz_segment::Client::new_dummy_client();
            telemetry::start_reporting(telemetry::Config {
                segment_client,
                adapter_client: adapter_client.clone(),
                environment_id: config.environment_id,
                report_interval: Duration::from_secs(180),
            });
        }

        // If system_parameter_sync_config and config_sync_loop_interval are present,
        // start the system_parameter_sync loop.
        if let Some(system_parameter_sync_config) = system_parameter_sync_config {
            task::spawn(
                || "system_parameter_sync",
                AssertUnwindSafe(system_parameter_sync(
                    system_parameter_sync_config,
                    adapter_client.clone(),
                    config.config_sync_loop_interval,
                ))
                .ore_catch_unwind(),
            );
        }

        info!(
            "startup: envd serve: postamble complete in {:?}",
            serve_postamble_start.elapsed()
        );
        info!(
            "startup: envd serve: complete in {:?}",
            serve_start.elapsed()
        );

        Ok(Server {
            sql_listener_handles,
            http_listener_handles,
            _adapter_handle: adapter_handle,
        })
    }
}

fn get_ld_value<V>(
    name: &str,
    remote_system_parameters: &Option<BTreeMap<String, String>>,
    parse: impl Fn(&str) -> Result<V, String>,
) -> Result<Option<V>, anyhow::Error> {
    remote_system_parameters
        .as_ref()
        .and_then(|params| params.get(name))
        .map(|x| {
            parse(x).map_err(|err| anyhow!("failed to parse remote value for {}: {}", name, err))
        })
        .transpose()
}

/// A running `environmentd` server.
pub struct Server {
    // Drop order matters for these fields.
    pub sql_listener_handles: BTreeMap<String, ListenerHandle>,
    pub http_listener_handles: BTreeMap<String, ListenerHandle>,
    _adapter_handle: mz_adapter::Handle,
}

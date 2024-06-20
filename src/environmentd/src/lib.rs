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
use std::env;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::panic::AssertUnwindSafe;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{anyhow, bail, Context};
use derivative::Derivative;
use mz_adapter::config::{system_parameter_sync, SystemParameterSyncConfig};
use mz_adapter::load_remote_system_parameters;
use mz_adapter::webhook::WebhookConcurrencyLimiter;
use mz_adapter_types::dyncfgs::ENABLE_0DT_DEPLOYMENT;
use mz_build_info::{build_info, BuildInfo};
use mz_catalog::config::ClusterReplicaSizeMap;
use mz_catalog::durable::BootstrapArgs;
use mz_cloud_resources::CloudResourceController;
use mz_controller::ControllerConfig;
use mz_frontegg_auth::Authenticator as FronteggAuthentication;
use mz_ore::future::OreFutureExt;
use mz_ore::metrics::MetricsRegistry;
use mz_ore::now::NowFn;
use mz_ore::tracing::TracingHandle;
use mz_ore::{instrument, task};
use mz_persist_client::cache::PersistClientCache;
use mz_persist_client::usage::StorageUsageClient;
use mz_repr::strconv;
use mz_secrets::SecretsController;
use mz_server_core::{ConnectionStream, ListenerHandle, ReloadTrigger, TlsCertConfig};
use mz_sql::catalog::EnvironmentId;
use mz_sql::session::vars::ConnectionCounter;
use tokio::sync::oneshot;
use tower_http::cors::AllowOrigin;
use tracing::{info, info_span, Instrument};

use crate::deployment::preflight::PreflightInput;
use crate::deployment::state::DeploymentState;
use crate::http::{HttpConfig, HttpServer, InternalHttpConfig, InternalHttpServer};

pub use crate::http::{SqlResponse, WebSocketAuth, WebSocketResponse};

mod deployment;
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
    /// Frontegg JWT authentication configuration.
    pub frontegg: Option<FronteggAuthentication>,
    /// Origins for which cross-origin resource sharing (CORS) for HTTP requests
    /// is permitted.
    pub cors_allowed_origin: AllowOrigin,
    /// Public IP addresses which the cloud environment has configured for
    /// egress.
    pub egress_ips: Vec<Ipv4Addr>,
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
    pub timestamp_oracle_url: Option<String>,
    /// An API key for Segment. Enables export of audit events to Segment.
    pub segment_api_key: Option<String>,
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

    // === Bootstrap options. ===
    /// The cloud ID of this environment.
    pub environment_id: EnvironmentId,
    /// What role, if any, should be initially created with elevated privileges.
    pub bootstrap_role: Option<String>,
    /// The size of the default cluster replica if bootstrapping.
    pub bootstrap_default_cluster_replica_size: String,
    /// The size of the builtin system cluster replicas if bootstrapping.
    pub bootstrap_builtin_system_cluster_replica_size: String,
    /// The size of the builtin catalog server cluster replicas if bootstrapping.
    pub bootstrap_builtin_catalog_server_cluster_replica_size: String,
    /// The size of the builtin probe cluster replicas if bootstrapping.
    pub bootstrap_builtin_probe_cluster_replica_size: String,
    /// The size of the builtin support cluster replicas if bootstrapping.
    pub bootstrap_builtin_support_cluster_replica_size: String,
    /// Values to set for system parameters, if those system parameters have not
    /// already been set by the system user.
    pub system_parameter_defaults: BTreeMap<String, String>,

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
}

/// Configuration for network listeners.
pub struct ListenersConfig {
    /// The IP address and port to listen for pgwire connections on.
    pub sql_listen_addr: SocketAddr,
    /// The IP address and port to listen for HTTP connections on.
    pub http_listen_addr: SocketAddr,
    /// The IP address and port to listen on for pgwire connections from the cloud
    /// balancer pods.
    pub balancer_sql_listen_addr: SocketAddr,
    /// The IP address and port to listen for HTTP connections from the cloud balancer pods.
    pub balancer_http_listen_addr: SocketAddr,
    /// The IP address and port to listen for pgwire connections from the cloud
    /// system on.
    pub internal_sql_listen_addr: SocketAddr,
    /// The IP address and port to serve the metrics registry from.
    pub internal_http_listen_addr: SocketAddr,
}

/// Configuration for the Catalog.
#[derive(Debug, Clone)]
pub struct CatalogConfig {
    /// A process-global cache of (blob_uri, consensus_uri) -> PersistClient.
    pub persist_clients: Arc<PersistClientCache>,
    /// Persist catalog metrics.
    pub metrics: Arc<mz_catalog::durable::Metrics>,
}

/// Listeners for an `environmentd` server.
pub struct Listeners {
    // Drop order matters for these fields.
    sql: (ListenerHandle, Pin<Box<dyn ConnectionStream>>),
    http: (ListenerHandle, Pin<Box<dyn ConnectionStream>>),
    balancer_sql: (ListenerHandle, Pin<Box<dyn ConnectionStream>>),
    balancer_http: (ListenerHandle, Pin<Box<dyn ConnectionStream>>),
    internal_sql: (ListenerHandle, Pin<Box<dyn ConnectionStream>>),
    internal_http: (ListenerHandle, Pin<Box<dyn ConnectionStream>>),
}

impl Listeners {
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
    pub async fn bind(
        ListenersConfig {
            sql_listen_addr,
            http_listen_addr,
            balancer_sql_listen_addr,
            balancer_http_listen_addr,
            internal_sql_listen_addr,
            internal_http_listen_addr,
        }: ListenersConfig,
    ) -> Result<Listeners, anyhow::Error> {
        let sql = mz_server_core::listen(&sql_listen_addr).await?;
        let http = mz_server_core::listen(&http_listen_addr).await?;
        let balancer_sql = mz_server_core::listen(&balancer_sql_listen_addr).await?;
        let balancer_http = mz_server_core::listen(&balancer_http_listen_addr).await?;
        let internal_sql = mz_server_core::listen(&internal_sql_listen_addr).await?;
        let internal_http = mz_server_core::listen(&internal_http_listen_addr).await?;
        Ok(Listeners {
            sql,
            http,
            balancer_sql,
            balancer_http,
            internal_sql,
            internal_http,
        })
    }

    /// Like [`Listeners::bind`], but binds each ports to an arbitrary free
    /// local address.
    pub async fn bind_any_local() -> Result<Listeners, anyhow::Error> {
        Listeners::bind(ListenersConfig {
            sql_listen_addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0),
            http_listen_addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0),
            balancer_sql_listen_addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0),
            balancer_http_listen_addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0),
            internal_sql_listen_addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0),
            internal_http_listen_addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0),
        })
        .await
    }

    /// Starts an `environmentd` server.
    ///
    /// Returns a handle to the server once it is fully booted.
    #[instrument(name = "environmentd::serve")]
    pub async fn serve(self, config: Config) -> Result<Server, anyhow::Error> {
        let Listeners {
            sql: (sql_listener, sql_conns),
            http: (http_listener, http_conns),
            balancer_sql: (balancer_sql_listener, balancer_sql_conns),
            balancer_http: (balancer_http_listener, balancer_http_conns),
            internal_sql: (internal_sql_listener, internal_sql_conns),
            internal_http: (internal_http_listener, internal_http_conns),
        } = self;

        // Validate TLS configuration, if present.
        let (pgwire_tls, http_tls) = match &config.tls {
            None => (None, None),
            Some(tls_config) => {
                let context = tls_config.reloading_context(config.tls_reload_certs)?;
                let pgwire_tls = mz_server_core::ReloadingTlsConfig {
                    context: context.clone(),
                    mode: mz_server_core::TlsMode::Require,
                };
                let http_tls = http::ReloadingTlsConfig {
                    context,
                    mode: http::TlsMode::Require,
                };
                (Some(pgwire_tls), Some(http_tls))
            }
        };

        let active_connection_count = Arc::new(Mutex::new(ConnectionCounter::new(0, 0)));

        let (deployment_state, deployment_state_handle) = DeploymentState::new();

        // Start the internal HTTP server.
        //
        // We start this server before we've completed initialization so that
        // metrics are accessible during initialization. Some internal HTTP
        // endpoints require the adapter to be initialized; requests to those
        // endpoints block until the adapter client is installed.
        let (internal_http_adapter_client_tx, internal_http_adapter_client_rx) = oneshot::channel();
        task::spawn(|| "internal_http_server", {
            let internal_http_server = InternalHttpServer::new(InternalHttpConfig {
                metrics_registry: config.metrics_registry.clone(),
                adapter_client_rx: internal_http_adapter_client_rx,
                active_connection_count: Arc::clone(&active_connection_count),
                deployment_state_handle,
                internal_console_redirect_url: config.internal_console_redirect_url,
            });
            mz_server_core::serve(internal_http_conns, internal_http_server, None)
        });

        // Get the current timestamp so we can record when we booted.
        let boot_ts = (config.now)();

        let persist_client = config
            .catalog_config
            .persist_clients
            .open(config.controller.persist_location.clone())
            .await?;
        let mut openable_adapter_storage = mz_catalog::durable::persist_backed_catalog_state(
            persist_client.clone(),
            config.environment_id.organization_id(),
            BUILD_INFO.semver_version(),
            Arc::clone(&config.catalog_config.metrics),
        )
        .await?;

        // Initialize the system parameter frontend if `launchdarkly_sdk_key` is set.
        let system_parameter_sync_config = if let Some(ld_sdk_key) = config.launchdarkly_sdk_key {
            Some(SystemParameterSyncConfig::new(
                config.environment_id.clone(),
                &BUILD_INFO,
                &config.metrics_registry,
                config.now.clone(),
                ld_sdk_key,
                config.launchdarkly_key_map,
            ))
        } else {
            None
        };
        let remote_system_parameters = load_remote_system_parameters(
            &mut openable_adapter_storage,
            system_parameter_sync_config.clone(),
            config.config_sync_timeout,
        )
        .await?;

        // Determine whether we should perform a 0dt deployment.
        let enable_0dt_deployment = {
            let default = config
                .system_parameter_defaults
                .get(ENABLE_0DT_DEPLOYMENT.name())
                .map(|x| {
                    strconv::parse_bool(x).map_err(|err| {
                        anyhow!(
                            "failed to parse default for {}: {}",
                            ENABLE_0DT_DEPLOYMENT.name(),
                            err
                        )
                    })
                })
                .transpose()?;
            let ld = get_ld_value("enable_0dt_deployment", &remote_system_parameters, |x| {
                strconv::parse_bool(x).map_err(|x| x.to_string())
            })?;
            let catalog = openable_adapter_storage.get_enable_0dt_deployment().await?;
            let computed = catalog.or(ld).or(default).unwrap_or(false);
            info!(
                %computed,
                ?default,
                ?ld,
                ?catalog,
                "determined value for enable_0dt_deployment system parameter",
            );
            computed
        };

        // Perform preflight checks.
        //
        // Preflight checks determine whether to boot in read-only mode or not.
        let mut read_only = false;
        let preflight_config = PreflightInput {
            boot_ts,
            environment_id: config.environment_id.clone(),
            persist_client,
            bootstrap_default_cluster_replica_size: config
                .bootstrap_default_cluster_replica_size
                .clone(),
            bootstrap_role: config.bootstrap_role.clone(),
            deploy_generation: config.controller.deploy_generation,
            deployment_state,
            openable_adapter_storage,
            catalog_metrics: Arc::clone(&config.catalog_config.metrics),
        };
        if enable_0dt_deployment {
            (openable_adapter_storage, read_only) =
                deployment::preflight::preflight_0dt(preflight_config).await?;
        } else {
            openable_adapter_storage =
                deployment::preflight::preflight_legacy(preflight_config).await?;
        };

        let bootstrap_args = BootstrapArgs {
            default_cluster_replica_size: config.bootstrap_default_cluster_replica_size.clone(),
            bootstrap_role: config.bootstrap_role,
        };

        let adapter_storage = if read_only {
            // TODO: behavior of migrations when booting in savepoint mode is
            // not well defined.
            openable_adapter_storage
                .open_savepoint(
                    boot_ts,
                    &bootstrap_args,
                    config.controller.deploy_generation,
                    None,
                )
                .await?
        } else {
            openable_adapter_storage
                .open(
                    boot_ts,
                    &bootstrap_args,
                    config.controller.deploy_generation,
                    None,
                )
                .await?
        };

        // Load the adapter catalog from disk.
        if !config
            .cluster_replica_sizes
            .0
            .contains_key(&config.bootstrap_default_cluster_replica_size)
        {
            bail!("bootstrap default cluster replica size is unknown");
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
        let segment_client = config.segment_api_key.map(mz_segment::Client::new);
        let webhook_concurrency_limit = WebhookConcurrencyLimiter::default();
        let (adapter_handle, adapter_client) = mz_adapter::serve(mz_adapter::Config {
            connection_context: config.controller.connection_context.clone(),
            controller_config: config.controller,
            controller_envd_epoch: envd_epoch,
            storage: adapter_storage,
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
            builtin_system_cluster_replica_size: config
                .bootstrap_builtin_system_cluster_replica_size,
            builtin_catalog_server_cluster_replica_size: config
                .bootstrap_builtin_catalog_server_cluster_replica_size,
            builtin_probe_cluster_replica_size: config.bootstrap_builtin_probe_cluster_replica_size,
            builtin_support_cluster_replica_size: config
                .bootstrap_builtin_support_cluster_replica_size,
            availability_zones: config.availability_zones,
            system_parameter_defaults: config.system_parameter_defaults,
            storage_usage_client,
            storage_usage_collection_interval: config.storage_usage_collection_interval,
            storage_usage_retention_period: config.storage_usage_retention_period,
            segment_client: segment_client.clone(),
            egress_ips: config.egress_ips,
            remote_system_parameters,
            aws_account_id: config.aws_account_id,
            aws_privatelink_availability_zones: config.aws_privatelink_availability_zones,
            active_connection_count: Arc::clone(&active_connection_count),
            webhook_concurrency_limit: webhook_concurrency_limit.clone(),
            http_host_name: config.http_host_name,
            tracing_handle: config.tracing_handle,
            read_only_controllers: read_only,
        })
        .instrument(info_span!("adapter::serve"))
        .await?;

        // Install an adapter client in the internal HTTP server.
        internal_http_adapter_client_tx
            .send(adapter_client.clone())
            .expect("internal HTTP server should not drop first");

        let metrics = mz_pgwire::MetricsConfig::register_into(&config.metrics_registry);
        // Launch SQL server.
        task::spawn(|| "sql_server", {
            let sql_server = mz_pgwire::Server::new(mz_pgwire::Config {
                label: "external_pgwire",
                tls: pgwire_tls.clone(),
                adapter_client: adapter_client.clone(),
                frontegg: config.frontegg.clone(),
                metrics: metrics.clone(),
                internal: false,
                active_connection_count: Arc::clone(&active_connection_count),
            });
            mz_server_core::serve(sql_conns, sql_server, None)
        });

        // Launch internal SQL server.
        task::spawn(|| "internal_sql_server", {
            let internal_sql_server = mz_pgwire::Server::new(mz_pgwire::Config {
                label: "internal_pgwire",
                tls: pgwire_tls.map(|mut pgwire_tls| {
                    // Allow, but do not require, TLS connections on the internal
                    // port. Some users of the internal SQL server do not support
                    // TLS, while others require it, so we allow both.
                    //
                    // TODO(benesch): migrate all internal applications to TLS and
                    // remove `TlsMode::Allow`.
                    pgwire_tls.mode = mz_server_core::TlsMode::Allow;
                    pgwire_tls
                }),
                adapter_client: adapter_client.clone(),
                frontegg: None,
                metrics: metrics.clone(),
                internal: true,
                active_connection_count: Arc::clone(&active_connection_count),
            });
            mz_server_core::serve(internal_sql_conns, internal_sql_server, None)
        });

        // Launch HTTP server.
        let http_metrics = http::Metrics::register_into(&config.metrics_registry, "mz_http");
        task::spawn(|| "http_server", {
            let http_server = HttpServer::new(HttpConfig {
                source: "external",
                tls: http_tls,
                frontegg: config.frontegg.clone(),
                adapter_client: adapter_client.clone(),
                allowed_origin: config.cors_allowed_origin.clone(),
                active_connection_count: Arc::clone(&active_connection_count),
                concurrent_webhook_req: webhook_concurrency_limit.semaphore(),
                metrics: http_metrics.clone(),
            });
            mz_server_core::serve(http_conns, http_server, None)
        });

        // Launch HTTP server exposed to balancers
        task::spawn(|| "balancer_http_server", {
            let balancer_http_server = HttpServer::new(HttpConfig {
                source: "balancer",
                // TODO(Alex): implement self-signed TLS for all internal connections
                tls: None,
                frontegg: config.frontegg.clone(),
                adapter_client: adapter_client.clone(),
                allowed_origin: config.cors_allowed_origin,
                active_connection_count: Arc::clone(&active_connection_count),
                concurrent_webhook_req: webhook_concurrency_limit.semaphore(),
                metrics: http_metrics,
            });
            mz_server_core::serve(balancer_http_conns, balancer_http_server, None)
        });

        // Launch SQL server exposed to balancers
        task::spawn(|| "balancer_sql_server", {
            let balancer_sql_server = mz_pgwire::Server::new(mz_pgwire::Config {
                label: "balancer_pgwire",
                tls: None,
                adapter_client: adapter_client.clone(),
                frontegg: config.frontegg.clone(),
                metrics,
                internal: false,
                active_connection_count: Arc::clone(&active_connection_count),
            });
            mz_server_core::serve(balancer_sql_conns, balancer_sql_server, None)
        });

        // Start telemetry reporting loop.
        if let Some(segment_client) = segment_client {
            telemetry::start_reporting(telemetry::Config {
                segment_client,
                adapter_client: adapter_client.clone(),
                environment_id: config.environment_id,
            });
        }

        // If system_parameter_sync_config and config_sync_loop_interval are present,
        // start the system_parameter_sync loop.
        if let Some(system_parameter_sync_config) = system_parameter_sync_config {
            task::spawn(
                || "system_parameter_sync",
                AssertUnwindSafe(system_parameter_sync(
                    system_parameter_sync_config,
                    adapter_client,
                    config.config_sync_loop_interval,
                ))
                .ore_catch_unwind(),
            );
        }

        Ok(Server {
            sql_listener,
            http_listener,
            balancer_sql_listener,
            balancer_http_listener,
            internal_sql_listener,
            internal_http_listener,
            _adapter_handle: adapter_handle,
        })
    }

    pub fn sql_local_addr(&self) -> SocketAddr {
        self.sql.0.local_addr()
    }

    pub fn http_local_addr(&self) -> SocketAddr {
        self.http.0.local_addr()
    }

    pub fn internal_sql_local_addr(&self) -> SocketAddr {
        self.internal_sql.0.local_addr()
    }

    pub fn internal_http_local_addr(&self) -> SocketAddr {
        self.internal_http.0.local_addr()
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
    sql_listener: ListenerHandle,
    http_listener: ListenerHandle,
    balancer_sql_listener: ListenerHandle,
    balancer_http_listener: ListenerHandle,
    internal_sql_listener: ListenerHandle,
    internal_http_listener: ListenerHandle,
    _adapter_handle: mz_adapter::Handle,
}

impl Server {
    pub fn sql_local_addr(&self) -> SocketAddr {
        self.sql_listener.local_addr()
    }

    pub fn http_local_addr(&self) -> SocketAddr {
        self.http_listener.local_addr()
    }

    pub fn balancer_sql_local_addr(&self) -> SocketAddr {
        self.balancer_sql_listener.local_addr()
    }

    pub fn balancer_http_local_addr(&self) -> SocketAddr {
        self.balancer_http_listener.local_addr()
    }

    pub fn internal_sql_local_addr(&self) -> SocketAddr {
        self.internal_sql_listener.local_addr()
    }

    pub fn internal_http_local_addr(&self) -> SocketAddr {
        self.internal_http_listener.local_addr()
    }
}

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
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{anyhow, bail, Context};
use mz_adapter::config::{system_parameter_sync, SystemParameterSyncConfig};
use mz_adapter::load_remote_system_parameters;
use mz_adapter::webhook::WebhookConcurrencyLimiter;
use mz_build_info::{build_info, BuildInfo};
use mz_catalog::config::ClusterReplicaSizeMap;
use mz_catalog::durable::{BootstrapArgs, CatalogError, OpenableDurableCatalogState};
use mz_cloud_resources::CloudResourceController;
use mz_controller::ControllerConfig;
use mz_frontegg_auth::Authenticator as FronteggAuthentication;
use mz_ore::future::OreFutureExt;
use mz_ore::metrics::MetricsRegistry;
use mz_ore::now::NowFn;
use mz_ore::task;
use mz_ore::tracing::TracingHandle;
use mz_persist_client::cache::PersistClientCache;
use mz_persist_client::usage::StorageUsageClient;
use mz_secrets::SecretsController;
use mz_server_core::{ConnectionStream, ListenerHandle, TlsCertConfig};
use mz_sql::catalog::EnvironmentId;
use mz_sql::session::vars::{ConnectionCounter, OwnedVarInput, Var, VarInput, PERSIST_TXN_TABLES};
use mz_storage_types::controller::PersistTxnTablesImpl;
use tokio::sync::oneshot;
use tokio::sync::oneshot::error::RecvError;
use tower_http::cors::AllowOrigin;
use tracing::{info, info_span, Instrument};

use crate::http::{HttpConfig, HttpServer, InternalHttpConfig, InternalHttpServer};

pub mod http;
mod telemetry;
#[cfg(feature = "test")]
pub mod test_util;

pub use crate::http::{SqlResponse, WebSocketAuth, WebSocketResponse};

pub const BUILD_INFO: BuildInfo = build_info!();

/// Configuration for an `environmentd` server.
#[derive(Debug, Clone)]
pub struct Config {
    // === Special modes. ===
    /// Whether to permit usage of unsafe features. This is never meant to run
    /// in production.
    pub unsafe_mode: bool,
    /// Whether the environmentd is running on a local dev machine. This is
    /// never meant to run in production or CI.
    pub all_features: bool,

    // === Connection options. ===
    /// Origins for which cross-origin resource sharing (CORS) for HTTP requests
    /// is permitted.
    pub cors_allowed_origin: AllowOrigin,
    /// TLS encryption and authentication configuration.
    pub tls: Option<TlsCertConfig>,
    /// Frontegg JWT authentication configuration.
    pub frontegg: Option<FronteggAuthentication>,

    // === Controller options. ===
    /// Storage and compute controller configuration.
    pub controller: ControllerConfig,
    /// Secrets controller configuration.
    pub secrets_controller: Arc<dyn SecretsController>,
    /// VpcEndpoint controller configuration.
    pub cloud_resource_controller: Option<Arc<dyn CloudResourceController>>,
    /// Whether to use the new persist-txn tables implementation or the legacy
    /// one.
    ///
    /// If specified, this overrides the value stored in Launch Darkly (and
    /// mirrored to the catalog storage's "config" collection).
    pub persist_txn_tables_cli: Option<PersistTxnTablesImpl>,

    // === Adapter options. ===
    /// Catalog configuration.
    pub catalog_config: CatalogConfig,
    /// The PostgreSQL URL for the Postgres-backed timestamp oracle.
    pub timestamp_oracle_url: Option<String>,

    // === Bootstrap options. ===
    /// The cloud ID of this environment.
    pub environment_id: EnvironmentId,
    /// Availability zones in which storage and compute resources may be
    /// deployed.
    pub availability_zones: Vec<String>,
    /// A map from size name to resource allocations for cluster replicas.
    pub cluster_replica_sizes: ClusterReplicaSizeMap,
    /// The size of the default cluster replica if bootstrapping.
    pub bootstrap_default_cluster_replica_size: String,
    /// The size of the builtin cluster replicas if bootstrapping.
    pub bootstrap_builtin_cluster_replica_size: String,
    /// Values to set for system parameters, if those system parameters have not
    /// already been set by the system user.
    pub system_parameter_defaults: BTreeMap<String, String>,
    /// The interval at which to collect storage usage information.
    pub storage_usage_collection_interval: Duration,
    /// How long to retain storage usage records for.
    pub storage_usage_retention_period: Option<Duration>,
    /// An API key for Segment. Enables export of audit events to Segment.
    pub segment_api_key: Option<String>,
    /// IP Addresses which will be used for egress.
    pub egress_ips: Vec<Ipv4Addr>,
    /// The AWS account ID, which will be used to generate ARNs for
    /// Materialize-controlled AWS resources.
    pub aws_account_id: Option<String>,
    /// Supported AWS PrivateLink availability zone ids.
    pub aws_privatelink_availability_zones: Option<Vec<String>>,
    /// An SDK key for LaunchDarkly. Enables system parameter synchronization
    /// with LaunchDarkly.
    pub launchdarkly_sdk_key: Option<String>,
    /// The duration at which the system parameter synchronization times out during startup.
    pub config_sync_timeout: Duration,
    /// The interval in seconds at which to synchronize system parameter values.
    pub config_sync_loop_interval: Option<Duration>,
    /// An invertible map from system parameter names to LaunchDarkly feature
    /// keys to use when propagating values from the latter to the former.
    pub launchdarkly_key_map: BTreeMap<String, String>,
    /// What role, if any, should be initially created with elevated privileges.
    pub bootstrap_role: Option<String>,
    /// Generation we want deployed. Generally only present when doing a production deploy.
    pub deploy_generation: Option<u64>,
    /// Host name or URL for connecting to the HTTP server of this instance.
    pub http_host_name: Option<String>,
    /// URL of the Web Console to proxy from the /internal-console endpoint on the InternalHTTPServer
    pub internal_console_redirect_url: Option<String>,

    // === Tracing options. ===
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
    #[mz_ore::instrument(name = "environmentd::serve", level = "info")]
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
                let context = tls_config.load_context()?;
                let pgwire_tls = mz_server_core::TlsConfig {
                    context: context.clone(),
                    mode: mz_server_core::TlsMode::Require,
                };
                let http_tls = http::TlsConfig {
                    context,
                    mode: http::TlsMode::Require,
                };
                (Some(pgwire_tls), Some(http_tls))
            }
        };

        let active_connection_count = Arc::new(Mutex::new(ConnectionCounter::new(0, 0)));

        let (ready_to_promote_tx, ready_to_promote_rx) = oneshot::channel();
        let (promote_leader_tx, promote_leader_rx) = oneshot::channel();

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
                promote_leader: promote_leader_tx,
                ready_to_promote: ready_to_promote_rx,
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
        let mut openable_adapter_storage: Box<dyn OpenableDurableCatalogState> = Box::new(
            mz_catalog::durable::persist_backed_catalog_state(
                persist_client.clone(),
                config.environment_id.organization_id(),
                BUILD_INFO.semver_version(),
                Arc::clone(&config.catalog_config.metrics),
            )
            .await?,
        );

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

        'leader_promotion: {
            let Some(deploy_generation) = config.deploy_generation else {
                break 'leader_promotion;
            };
            tracing::info!("Requested deploy generation {deploy_generation}");

            if !openable_adapter_storage.is_initialized().await? {
                tracing::info!("Catalog storage doesn't exist so there's no current deploy generation. We won't wait to be leader");
                break 'leader_promotion;
            }
            // TODO: once all catalogs have a deploy_generation, don't need to handle the Option
            let catalog_generation = openable_adapter_storage.get_deployment_generation().await?;
            tracing::info!("Found catalog generation {catalog_generation:?}");
            if catalog_generation < Some(deploy_generation) {
                tracing::info!("Catalog generation {catalog_generation:?} is less than deploy generation {deploy_generation}. Performing pre-flight checks");
                match openable_adapter_storage
                    .open_savepoint(
                        boot_ts.clone(),
                        &BootstrapArgs {
                            default_cluster_replica_size: config
                                .bootstrap_default_cluster_replica_size
                                .clone(),
                            bootstrap_role: config.bootstrap_role.clone(),
                        },
                        None,
                        None,
                    )
                    .await
                {
                    Ok(adapter_storage) => Box::new(adapter_storage).expire().await,
                    Err(CatalogError::Durable(e)) if e.can_recover_with_write_mode() => {
                        // This is theoretically possible if catalog implementation A is
                        // initialized, implementation B is uninitialized, and we are going to
                        // migrate from A to B. The current code avoids this by always
                        // initializing all implementations, regardless of the target
                        // implementation. Still it's easy to protect against this and worth it in
                        // case things change in the future.
                        tracing::warn!("Unable to perform upgrade test because the target implementation is uninitialized");
                        openable_adapter_storage = Box::new(
                            mz_catalog::durable::persist_backed_catalog_state(
                                persist_client,
                                config.environment_id.organization_id(),
                                BUILD_INFO.semver_version(),
                                Arc::clone(&config.catalog_config.metrics),
                            )
                            .await?,
                        );
                        break 'leader_promotion;
                    }
                    Err(e) => {
                        return Err(
                            anyhow!(e).context("Catalog upgrade would have failed with this error")
                        )
                    }
                }

                if let Err(()) = ready_to_promote_tx.send(()) {
                    return Err(anyhow!(
                        "internal http server closed its end of ready_to_promote"
                    ));
                }

                tracing::info!("Waiting for user to promote this envd to leader");
                if let Err(RecvError { .. }) = promote_leader_rx.await {
                    return Err(anyhow!(
                        "internal http server closed its end of promote_leader"
                    ));
                }

                openable_adapter_storage = Box::new(
                    mz_catalog::durable::persist_backed_catalog_state(
                        persist_client,
                        config.environment_id.organization_id(),
                        BUILD_INFO.semver_version(),
                        Arc::clone(&config.catalog_config.metrics),
                    )
                    .await?,
                );
            } else if catalog_generation == Some(deploy_generation) {
                tracing::info!("Server requested generation {deploy_generation} which is equal to catalog's generation");
            } else {
                mz_ore::halt!("Server started with requested generation {deploy_generation} but catalog was already at {catalog_generation:?}. Deploy generations must increase monotonically");
            }
        }

        let bootstrap_args = BootstrapArgs {
            default_cluster_replica_size: config.bootstrap_default_cluster_replica_size.clone(),
            bootstrap_role: config.bootstrap_role,
        };

        let mut adapter_storage = openable_adapter_storage
            .open(boot_ts, &bootstrap_args, config.deploy_generation, None)
            .await?;

        // Migrate the storage metadata to the persist-backed catalog. This can
        // be deleted in the version following this merge.
        {
            let stash_factory =
                mz_stash::StashFactory::from_metrics(Arc::clone(&config.controller.stash_metrics));
            let tls = mz_tls_util::make_tls(&tokio_postgres::config::Config::from_str(
                &config.controller.storage_stash_url,
            )?)?;

            let mut storage_stash = stash_factory
                .open(config.controller.storage_stash_url.clone(), None, tls, None)
                .await?;

            adapter_storage
                .migrate_storage_state_from(&mut storage_stash)
                .await?;
        }

        let persist_txn_tables_current_ld =
            get_ld_value(PERSIST_TXN_TABLES.name(), &remote_system_parameters, |x| {
                PersistTxnTablesImpl::from_str(x).map_err(|x| x.to_string())
            })?;
        // Get the value from Launch Darkly as of the last time this environment
        // was running. (Ideally it would be the above current value, but that's
        // not guaranteed: we don't want to block startup on it if LD is down.)
        let persist_txn_tables_stash_ld = adapter_storage.get_persist_txn_tables().await?;

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

        // Initialize controller.
        let persist_txn_tables_default = config
            .system_parameter_defaults
            .get(PERSIST_TXN_TABLES.name())
            .map(|x| {
                PersistTxnTablesImpl::from_str(x).map_err(|err| {
                    anyhow!(
                        "failed to parse default for {}: {}",
                        PERSIST_TXN_TABLES.name(),
                        err
                    )
                })
            })
            .transpose()?;
        let mut persist_txn_tables =
            persist_txn_tables_default.unwrap_or(PersistTxnTablesImpl::Eager);
        if let Some(value) = persist_txn_tables_stash_ld {
            persist_txn_tables = value;
        }
        if let Some(value) = persist_txn_tables_current_ld {
            persist_txn_tables = value;
        }
        if let Some(value) = config.persist_txn_tables_cli {
            persist_txn_tables = value;
        }
        info!(
            "persist_txn_tables value of {} computed from default {:?} catalog {:?} LD {:?} and flag {:?}",
            persist_txn_tables,
            persist_txn_tables_default,
            persist_txn_tables_stash_ld,
            persist_txn_tables_current_ld,
            config.persist_txn_tables_cli,
        );

        // Initialize adapter.
        let segment_client = config.segment_api_key.map(mz_segment::Client::new);
        let webhook_concurrency_limit = WebhookConcurrencyLimiter::default();
        let (adapter_handle, adapter_client) = mz_adapter::serve(mz_adapter::Config {
            connection_context: config.controller.connection_context.clone(),
            controller_config: config.controller,
            controller_envd_epoch: envd_epoch,
            controller_persist_txn_tables: persist_txn_tables,
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
            builtin_cluster_replica_size: config.bootstrap_builtin_cluster_replica_size,
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
        })
        .instrument(info_span!(parent: None, "adapter::serve"))
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
    remote_system_parameters: &Option<BTreeMap<String, OwnedVarInput>>,
    parse: impl Fn(&str) -> Result<V, String>,
) -> Result<Option<V>, anyhow::Error> {
    remote_system_parameters
        .as_ref()
        .and_then(|params| params.get(name))
        .map(|value| match value.borrow() {
            VarInput::Flat(s) => Ok(s),
            VarInput::SqlSet([s]) => Ok(s.as_str()),
            VarInput::SqlSet(v) => Err(anyhow!("Invalid remote value for {}: {:?}", name, v,)),
        })
        .transpose()?
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

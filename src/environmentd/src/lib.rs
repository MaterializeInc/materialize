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
use std::net::{Ipv4Addr, SocketAddr};
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{bail, Context};
use mz_stash::Stash;
use openssl::ssl::{SslAcceptor, SslFiletype, SslMethod, SslVerifyMode};
use tokio::sync::{mpsc, oneshot};
use tower_http::cors::AllowOrigin;

use mz_adapter::catalog::storage::BootstrapArgs;
use mz_adapter::catalog::{ClusterReplicaSizeMap, StorageHostSizeMap};
use mz_build_info::{build_info, BuildInfo};
use mz_cloud_resources::CloudResourceController;
use mz_controller::ControllerConfig;
use mz_frontegg_auth::FronteggAuthentication;
use mz_ore::metrics::MetricsRegistry;
use mz_ore::now::NowFn;
use mz_ore::task;
use mz_ore::tracing::TracingTargetCallbacks;
use mz_persist_client::usage::StorageUsageClient;
use mz_secrets::SecretsController;
use mz_storage_client::types::connections::ConnectionContext;

use crate::http::{HttpConfig, HttpServer, InternalHttpConfig, InternalHttpServer};
use crate::server::ListenerHandle;

mod http;
mod server;
mod telemetry;

pub const BUILD_INFO: BuildInfo = build_info!();

/// Configuration for an `environmentd` server.
#[derive(Debug, Clone)]
pub struct Config {
    // === Special modes. ===
    /// Whether to permit usage of unsafe features.
    pub unsafe_mode: bool,
    /// Whether to enable persisted introspection sources.
    pub persisted_introspection: bool,

    // === Connection options. ===
    /// The IP address and port to listen for pgwire connections on.
    pub sql_listen_addr: SocketAddr,
    /// The IP address and port to listen for HTTP connections on.
    pub http_listen_addr: SocketAddr,
    /// The IP address and port to listen for pgwire connections from the cloud
    /// system on.
    pub internal_sql_listen_addr: SocketAddr,
    /// The IP address and port to serve the metrics registry from.
    pub internal_http_listen_addr: SocketAddr,
    /// Origins for which cross-origin resource sharing (CORS) for HTTP requests
    /// is permitted.
    pub cors_allowed_origin: AllowOrigin,
    /// TLS encryption and authentication configuration.
    pub tls: Option<TlsConfig>,
    /// Frontegg JWT authentication configuration.
    pub frontegg: Option<FronteggAuthentication>,

    // === Connection options. ===
    /// Configuration for source and sink connections created by the storage
    /// layer. This can include configuration for external
    /// sources.
    pub connection_context: ConnectionContext,

    // === Controller options. ===
    /// Storage and compute controller configuration.
    pub controller: ControllerConfig,
    /// Secrets controller configuration.
    pub secrets_controller: Arc<dyn SecretsController>,
    /// VpcEndpoint controller configuration.
    pub cloud_resource_controller: Option<Arc<dyn CloudResourceController>>,

    // === Adapter options. ===
    /// The PostgreSQL URL for the adapter stash.
    pub adapter_stash_url: String,

    // === Cloud options. ===
    /// The cloud ID of this environment.
    pub environment_id: String,
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
    pub bootstrap_system_parameters: BTreeMap<String, String>,
    /// A map from size name to resource allocations for storage hosts.
    pub storage_host_sizes: StorageHostSizeMap,
    /// Default storage host size, should be a key from storage_host_sizes.
    pub default_storage_host_size: Option<String>,
    /// The interval at which to collect storage usage information.
    pub storage_usage_collection_interval: Duration,
    /// An API key for Segment. Enables export of audit events to Segment.
    pub segment_api_key: Option<String>,
    /// IP Addresses which will be used for egress.
    pub egress_ips: Vec<Ipv4Addr>,

    // === Tracing options. ===
    /// The metrics registry to use.
    pub metrics_registry: MetricsRegistry,
    /// Callbacks used to modify tracing/logging filters
    pub tracing_target_callbacks: TracingTargetCallbacks,

    // === Testing options. ===
    /// A now generation function for mocking time.
    pub now: NowFn,
}

/// Configures TLS encryption for connections.
#[derive(Debug, Clone)]
pub struct TlsConfig {
    /// The TLS mode to use.
    pub mode: TlsMode,
    /// The path to the TLS certificate.
    pub cert: PathBuf,
    /// The path to the TLS key.
    pub key: PathBuf,
}

/// Configures how strictly to enforce TLS encryption and authentication.
#[derive(Debug, Clone)]
pub enum TlsMode {
    /// Require that all clients connect with TLS, but do not require that they
    /// present a client certificate.
    Require,
    /// Require that clients connect with TLS and present a certificate that
    /// is signed by the specified CA.
    VerifyCa {
        /// The path to a TLS certificate authority.
        ca: PathBuf,
    },
    /// Like [`TlsMode::VerifyCa`], but the `cn` (Common Name) field of the
    /// certificate must additionally match the user named in the connection
    /// request.
    VerifyFull {
        /// The path to a TLS certificate authority.
        ca: PathBuf,
    },
}

/// Start an `environmentd` server.
pub async fn serve(config: Config) -> Result<Server, anyhow::Error> {
    let tls = mz_postgres_util::make_tls(&tokio_postgres::config::Config::from_str(
        &config.adapter_stash_url,
    )?)?;
    let stash = config
        .controller
        .postgres_factory
        .open(config.adapter_stash_url.clone(), None, tls)
        .await?;
    let stash = mz_stash::Cache::new(stash);

    // Validate TLS configuration, if present.
    let (pgwire_tls, http_tls) = match &config.tls {
        None => (None, None),
        Some(tls_config) => {
            let context = {
                // Mozilla publishes three presets: old, intermediate, and modern. They
                // recommend the intermediate preset for general purpose servers, which
                // is what we use, as it is compatible with nearly every client released
                // in the last five years but does not include any known-problematic
                // ciphers. We once tried to use the modern preset, but it was
                // incompatible with Fivetran, and presumably other JDBC-based tools.
                let mut builder = SslAcceptor::mozilla_intermediate_v5(SslMethod::tls())?;
                if let TlsMode::VerifyCa { ca } | TlsMode::VerifyFull { ca } = &tls_config.mode {
                    builder.set_ca_file(ca)?;
                    builder.set_verify(SslVerifyMode::PEER | SslVerifyMode::FAIL_IF_NO_PEER_CERT);
                }
                builder.set_certificate_chain_file(&tls_config.cert)?;
                builder.set_private_key_file(&tls_config.key, SslFiletype::PEM)?;
                builder.build().into_context()
            };
            let pgwire_tls = mz_pgwire::TlsConfig {
                context: context.clone(),
                mode: match tls_config.mode {
                    TlsMode::Require | TlsMode::VerifyCa { .. } => mz_pgwire::TlsMode::Require,
                    TlsMode::VerifyFull { .. } => mz_pgwire::TlsMode::VerifyUser,
                },
            };
            let http_tls = http::TlsConfig {
                context,
                mode: match tls_config.mode {
                    TlsMode::Require | TlsMode::VerifyCa { .. } => http::TlsMode::Require,
                    TlsMode::VerifyFull { .. } => http::TlsMode::AssumeUser,
                },
            };
            (Some(pgwire_tls), Some(http_tls))
        }
    };

    // Initialize network listeners.
    //
    // We do this as early as possible during initialization so that the OS will
    // start queueing incoming connections for when we're ready.
    let (sql_listener, sql_conns) = server::listen(config.sql_listen_addr).await?;
    let (http_listener, http_conns) = server::listen(config.http_listen_addr).await?;
    let (internal_sql_listener, internal_sql_conns) =
        server::listen(config.internal_sql_listen_addr).await?;
    let (internal_http_listener, internal_http_conns) =
        server::listen(config.internal_http_listen_addr).await?;

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
            tracing_target_callbacks: config.tracing_target_callbacks,
            adapter_client_rx: internal_http_adapter_client_rx,
        });
        server::serve(internal_http_conns, internal_http_server)
    });

    let (consolidations_tx, consolidations_rx) = mpsc::unbounded_channel();

    // Load the adapter catalog from disk.
    if !config
        .cluster_replica_sizes
        .0
        .contains_key(&config.bootstrap_default_cluster_replica_size)
    {
        bail!("bootstrap default cluster replica size is unknown");
    }
    let envd_epoch = stash
        .epoch()
        .expect("a real environmentd should always have an epoch number");
    let adapter_storage = mz_adapter::catalog::storage::Connection::open(
        stash,
        &BootstrapArgs {
            now: (config.now)(),
            default_cluster_replica_size: config.bootstrap_default_cluster_replica_size,
            builtin_cluster_replica_size: config.bootstrap_builtin_cluster_replica_size,
            // TODO(benesch, brennan): remove this after v0.27.0-alpha.4 has
            // shipped to cloud since all clusters will have had a default
            // availability zone installed.
            default_availability_zone: config
                .availability_zones
                .first()
                .cloned()
                .unwrap_or_else(|| mz_adapter::DUMMY_AVAILABILITY_ZONE.into()),
        },
        consolidations_tx.clone(),
    )
    .await?;

    // Initialize storage usage client.
    let storage_usage_client = StorageUsageClient::open(
        config.controller.persist_location.blob_uri.clone(),
        &mut *config.controller.persist_clients.lock().await,
    )
    .await
    .context("opening storage usage client")?;

    // Initialize controller.
    let controller = mz_controller::Controller::new(config.controller, envd_epoch).await;

    // Initialize adapter.
    let segment_client = config.segment_api_key.map(mz_segment::Client::new);
    let (adapter_handle, adapter_client) = mz_adapter::serve(mz_adapter::Config {
        dataflow_client: controller,
        storage: adapter_storage,
        unsafe_mode: config.unsafe_mode,
        persisted_introspection: config.persisted_introspection,
        build_info: &BUILD_INFO,
        environment_id: config.environment_id,
        metrics_registry: config.metrics_registry.clone(),
        now: config.now,
        secrets_controller: config.secrets_controller,
        cloud_resource_controller: config.cloud_resource_controller,
        cluster_replica_sizes: config.cluster_replica_sizes,
        storage_host_sizes: config.storage_host_sizes,
        default_storage_host_size: config.default_storage_host_size,
        availability_zones: config.availability_zones,
        bootstrap_system_parameters: config.bootstrap_system_parameters,
        connection_context: config.connection_context,
        storage_usage_client,
        storage_usage_collection_interval: config.storage_usage_collection_interval,
        segment_client: segment_client.clone(),
        egress_ips: config.egress_ips,
        consolidations_tx,
        consolidations_rx,
    })
    .await?;

    // Install an adapter client in the internal HTTP server.
    internal_http_adapter_client_tx
        .send(adapter_client.clone())
        .expect("internal HTTP server should not drop first");

    let metrics = mz_pgwire::MetricsConfig::register_into(&config.metrics_registry);
    // Launch SQL server.
    task::spawn(|| "sql_server", {
        let sql_server = mz_pgwire::Server::new(mz_pgwire::Config {
            tls: pgwire_tls,
            adapter_client: adapter_client.clone(),
            frontegg: config.frontegg.clone(),
            metrics: metrics.clone(),
            internal: false,
        });
        server::serve(sql_conns, sql_server)
    });

    // Launch internal SQL server.
    task::spawn(|| "internal_sql_server", {
        let internal_sql_server = mz_pgwire::Server::new(mz_pgwire::Config {
            tls: None,
            adapter_client: adapter_client.clone(),
            frontegg: None,
            metrics,
            internal: true,
        });
        server::serve(internal_sql_conns, internal_sql_server)
    });

    // Launch HTTP server.
    task::spawn(|| "http_server", {
        let http_server = HttpServer::new(HttpConfig {
            tls: http_tls,
            frontegg: config.frontegg.clone(),
            adapter_client: adapter_client.clone(),
            allowed_origin: config.cors_allowed_origin,
        });
        server::serve(http_conns, http_server)
    });

    // Start telemetry reporting loop.
    if let (Some(segment_client), Some(frontegg)) = (segment_client, config.frontegg) {
        telemetry::start_reporting(telemetry::Config {
            segment_client,
            adapter_client,
            organization_id: frontegg.tenant_id(),
        });
    }

    Ok(Server {
        sql_listener,
        http_listener,
        internal_sql_listener,
        internal_http_listener,
        _adapter_handle: adapter_handle,
    })
}

/// A running `environmentd` server.
pub struct Server {
    // Drop order matters for these fields.
    sql_listener: ListenerHandle,
    http_listener: ListenerHandle,
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

    pub fn internal_sql_local_addr(&self) -> SocketAddr {
        self.internal_sql_listener.local_addr()
    }

    pub fn internal_http_local_addr(&self) -> SocketAddr {
        self.internal_http_listener.local_addr()
    }
}

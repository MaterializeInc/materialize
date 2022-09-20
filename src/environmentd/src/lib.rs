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

use std::env;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{bail, Context};
use futures::StreamExt;
use openssl::ssl::{SslAcceptor, SslFiletype, SslMethod, SslVerifyMode};
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio_stream::wrappers::TcpListenerStream;
use tower_http::cors::AllowOrigin;
use tracing::error;

use mz_adapter::catalog::storage::BootstrapArgs;
use mz_adapter::catalog::{ClusterReplicaSizeMap, StorageHostSizeMap};
use mz_build_info::{build_info, BuildInfo};
use mz_controller::ControllerConfig;
use mz_frontegg_auth::FronteggAuthentication;
use mz_ore::metrics::MetricsRegistry;
use mz_ore::now::NowFn;
use mz_ore::task;
use mz_ore::tracing::OpenTelemetryEnableCallback;
use mz_persist_client::usage::StorageUsageClient;
use mz_secrets::SecretsController;
use mz_storage::types::connections::ConnectionContext;

use crate::tcp_connection::ConnectionHandler;

pub mod http;
pub mod tcp_connection;

pub const BUILD_INFO: BuildInfo = build_info!();
// TODO: should storage usage default go here?

/// Configuration for an `environmentd` server.
#[derive(Debug, Clone)]
pub struct Config {
    // === Special modes. ===
    /// Whether to permit usage of unsafe features.
    pub unsafe_mode: bool,

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
    /// A map from size name to resource allocations for storage hosts.
    pub storage_host_sizes: StorageHostSizeMap,
    /// Default storage host size, should be a key from storage_host_sizes.
    pub default_storage_host_size: Option<String>,
    /// The interval at which to collect storage usage information.
    pub storage_usage_collection_interval: Duration,

    // === Tracing options. ===
    /// The metrics registry to use.
    pub metrics_registry: MetricsRegistry,
    /// A callback to enable or disable the OpenTelemetry tracing collector.
    pub otel_enable_callback: OpenTelemetryEnableCallback,

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
    let stash = mz_stash::Postgres::new(config.adapter_stash_url.clone(), None, tls).await?;
    let stash = mz_stash::Memory::new(stash);

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
    let sql_listener = TcpListener::bind(&config.sql_listen_addr).await?;
    let http_listener = TcpListener::bind(&config.http_listen_addr).await?;
    let internal_sql_listener = TcpListener::bind(&config.internal_sql_listen_addr).await?;
    let sql_local_addr = sql_listener.local_addr()?;
    let http_local_addr = http_listener.local_addr()?;
    let internal_sql_local_addr = internal_sql_listener.local_addr()?;

    // Listen on the internal HTTP API port.
    let internal_http_local_addr = {
        let metrics_registry = config.metrics_registry.clone();
        let server = http::InternalServer::new(metrics_registry, config.otel_enable_callback);
        let bound_server = server.bind(config.internal_http_listen_addr);
        let internal_http_local_addr = bound_server.local_addr();
        task::spawn(|| "internal_http_server", {
            async move {
                if let Err(err) = bound_server.await {
                    error!("error serving metrics endpoint: {}", err);
                }
            }
        });
        internal_http_local_addr
    };

    // Load the adapter catalog from disk.
    if !config
        .cluster_replica_sizes
        .0
        .contains_key(&config.bootstrap_default_cluster_replica_size)
    {
        bail!("bootstrap default cluster replica size is unknown");
    }
    let adapter_storage = mz_adapter::catalog::storage::Connection::open(
        stash,
        &BootstrapArgs {
            default_cluster_replica_size: config.bootstrap_default_cluster_replica_size,
            // TODO(benesch, brennan): remove this after v0.27.0-alpha.4 has
            // shipped to cloud since all clusters will have had a default
            // availability zone installed.
            default_availability_zone: config
                .availability_zones
                .first()
                .cloned()
                .unwrap_or_else(|| mz_adapter::DUMMY_AVAILABILITY_ZONE.into()),
        },
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
    let controller = mz_controller::Controller::new(config.controller).await;

    // Initialize adapter.
    let (adapter_handle, adapter_client) = mz_adapter::serve(mz_adapter::Config {
        dataflow_client: controller,
        storage: adapter_storage,
        unsafe_mode: config.unsafe_mode,
        build_info: &BUILD_INFO,
        environment_id: config.environment_id,
        metrics_registry: config.metrics_registry.clone(),
        now: config.now,
        secrets_controller: config.secrets_controller,
        cluster_replica_sizes: config.cluster_replica_sizes,
        storage_host_sizes: config.storage_host_sizes,
        default_storage_host_size: config.default_storage_host_size,
        availability_zones: config.availability_zones,
        connection_context: config.connection_context,
        storage_usage_client,
        storage_usage_collection_interval: config.storage_usage_collection_interval,
    })
    .await?;

    // TODO(benesch): replace both `TCPListenerStream`s below with
    // `<type>_listener.incoming()` if that is
    // restored when the `Stream` trait stabilizes.

    // Launch task to serve connections.
    //
    // The lifetime of this task is controlled by a trigger that activates on
    // drop. Draining marks the beginning of the server shutdown process and
    // indicates that new user connections (i.e., pgwire and HTTP connections)
    // should be rejected. Once all existing user connections have gracefully
    // terminated, this task exits.
    let (sql_drain_trigger, sql_drain_tripwire) = oneshot::channel();
    task::spawn(|| "pgwire_server", {
        let pgwire_server = mz_pgwire::Server::new(mz_pgwire::Config {
            tls: pgwire_tls,
            adapter_client: adapter_client.clone(),
            frontegg: config.frontegg.clone(),
            internal: false,
        });

        async move {
            let mut incoming = TcpListenerStream::new(sql_listener);
            pgwire_server
                .serve(incoming.by_ref().take_until(sql_drain_tripwire))
                .await;
        }
    });

    // Listen on the internal SQL port.
    let (internal_sql_drain_trigger, internal_sql_local_addr) = {
        let (internal_sql_drain_trigger, internal_sql_drain_tripwire) = oneshot::channel();
        task::spawn(|| "internal_pgwire_server", {
            let internal_pgwire_server = mz_pgwire::Server::new(mz_pgwire::Config {
                tls: None,
                adapter_client: adapter_client.clone(),
                frontegg: None,
                internal: true,
            });
            let mut incoming = TcpListenerStream::new(internal_sql_listener);
            async move {
                internal_pgwire_server
                    .serve(incoming.by_ref().take_until(internal_sql_drain_tripwire))
                    .await
            }
        });
        (internal_sql_drain_trigger, internal_sql_local_addr)
    };

    let (http_drain_trigger, http_drain_tripwire) = oneshot::channel();
    task::spawn(|| "http_server", {
        async move {
            let http_server = http::Server::new(http::Config {
                tls: http_tls,
                frontegg: config.frontegg,
                adapter_client,
                allowed_origin: config.cors_allowed_origin,
            });
            let mut incoming = TcpListenerStream::new(http_listener);
            http_server
                .serve(incoming.by_ref().take_until(http_drain_tripwire))
                .await;
        }
    });

    Ok(Server {
        sql_local_addr,
        http_local_addr,
        internal_sql_local_addr,
        internal_http_local_addr,
        _internal_sql_drain_trigger: internal_sql_drain_trigger,
        _http_drain_trigger: http_drain_trigger,
        _sql_drain_trigger: sql_drain_trigger,
        _adapter_handle: adapter_handle,
    })
}

/// A running `environmentd` server.
pub struct Server {
    sql_local_addr: SocketAddr,
    http_local_addr: SocketAddr,
    internal_sql_local_addr: SocketAddr,
    internal_http_local_addr: SocketAddr,
    // Drop order matters for these fields.
    _internal_sql_drain_trigger: oneshot::Sender<()>,
    _http_drain_trigger: oneshot::Sender<()>,
    _sql_drain_trigger: oneshot::Sender<()>,
    _adapter_handle: mz_adapter::Handle,
}

impl Server {
    pub fn sql_local_addr(&self) -> SocketAddr {
        self.sql_local_addr
    }

    pub fn http_local_addr(&self) -> SocketAddr {
        self.http_local_addr
    }

    pub fn internal_sql_local_addr(&self) -> SocketAddr {
        self.internal_sql_local_addr
    }

    pub fn internal_http_local_addr(&self) -> SocketAddr {
        self.internal_http_local_addr
    }
}

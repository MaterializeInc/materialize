// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

// BEGIN LINT CONFIG
// DO NOT EDIT. Automatically generated by bin/gen-lints.
// Have complaints about the noise? See the note in misc/python/materialize/cli/gen-lints.py first.
#![allow(clippy::style)]
#![allow(clippy::complexity)]
#![allow(clippy::large_enum_variant)]
#![allow(clippy::mutable_key_type)]
#![allow(clippy::stable_sort_primitive)]
#![allow(clippy::map_entry)]
#![allow(clippy::box_default)]
#![warn(clippy::bool_comparison)]
#![warn(clippy::clone_on_ref_ptr)]
#![warn(clippy::no_effect)]
#![warn(clippy::unnecessary_unwrap)]
#![warn(clippy::dbg_macro)]
#![warn(clippy::todo)]
#![warn(clippy::wildcard_dependencies)]
#![warn(clippy::zero_prefixed_literal)]
#![warn(clippy::borrowed_box)]
#![warn(clippy::deref_addrof)]
#![warn(clippy::double_must_use)]
#![warn(clippy::double_parens)]
#![warn(clippy::extra_unused_lifetimes)]
#![warn(clippy::needless_borrow)]
#![warn(clippy::needless_question_mark)]
#![warn(clippy::needless_return)]
#![warn(clippy::redundant_pattern)]
#![warn(clippy::redundant_slicing)]
#![warn(clippy::redundant_static_lifetimes)]
#![warn(clippy::single_component_path_imports)]
#![warn(clippy::unnecessary_cast)]
#![warn(clippy::useless_asref)]
#![warn(clippy::useless_conversion)]
#![warn(clippy::builtin_type_shadow)]
#![warn(clippy::duplicate_underscore_argument)]
#![warn(clippy::double_neg)]
#![warn(clippy::unnecessary_mut_passed)]
#![warn(clippy::wildcard_in_or_patterns)]
#![warn(clippy::crosspointer_transmute)]
#![warn(clippy::excessive_precision)]
#![warn(clippy::overflow_check_conditional)]
#![warn(clippy::as_conversions)]
#![warn(clippy::match_overlapping_arm)]
#![warn(clippy::zero_divided_by_zero)]
#![warn(clippy::must_use_unit)]
#![warn(clippy::suspicious_assignment_formatting)]
#![warn(clippy::suspicious_else_formatting)]
#![warn(clippy::suspicious_unary_op_formatting)]
#![warn(clippy::mut_mutex_lock)]
#![warn(clippy::print_literal)]
#![warn(clippy::same_item_push)]
#![warn(clippy::useless_format)]
#![warn(clippy::write_literal)]
#![warn(clippy::redundant_closure)]
#![warn(clippy::redundant_closure_call)]
#![warn(clippy::unnecessary_lazy_evaluations)]
#![warn(clippy::partialeq_ne_impl)]
#![warn(clippy::redundant_field_names)]
#![warn(clippy::transmutes_expressible_as_ptr_casts)]
#![warn(clippy::unused_async)]
#![warn(clippy::disallowed_methods)]
#![warn(clippy::disallowed_macros)]
#![warn(clippy::disallowed_types)]
#![warn(clippy::from_over_into)]
// END LINT CONFIG

//! A SQL stream processor built on top of [timely dataflow] and
//! [differential dataflow].
//!
//! [differential dataflow]: ../differential_dataflow/index.html
//! [timely dataflow]: ../timely/index.html

use std::collections::BTreeMap;
use std::env;
use std::net::{Ipv4Addr, SocketAddr};
use std::panic::AssertUnwindSafe;
use std::path::PathBuf;
use std::str::FromStr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use anyhow::{bail, Context};
use mz_sql::session::vars::ConnectionCounter;
use openssl::ssl::{SslAcceptor, SslFiletype, SslMethod};
use rand::seq::SliceRandom;
use tokio::sync::oneshot;
use tower_http::cors::AllowOrigin;

use mz_adapter::catalog::storage::BootstrapArgs;
use mz_adapter::catalog::ClusterReplicaSizeMap;
use mz_adapter::config::{system_parameter_sync, SystemParameterBackend, SystemParameterFrontend};
use mz_build_info::{build_info, BuildInfo};
use mz_cloud_resources::CloudResourceController;
use mz_controller::ControllerConfig;
use mz_frontegg_auth::Authentication as FronteggAuthentication;
use mz_ore::future::OreFutureExt;
use mz_ore::metrics::MetricsRegistry;
use mz_ore::now::NowFn;
use mz_ore::task;
use mz_ore::tracing::TracingHandle;
use mz_persist_client::usage::StorageUsageClient;
use mz_secrets::SecretsController;
use mz_sql::catalog::EnvironmentId;
use mz_storage_client::types::connections::ConnectionContext;

use crate::http::{HttpConfig, HttpServer, InternalHttpConfig, InternalHttpServer};
use crate::server::ListenerHandle;

pub mod http;
mod server;
mod telemetry;

pub use crate::http::{SqlResponse, WebSocketAuth, WebSocketResponse};

pub const BUILD_INFO: BuildInfo = build_info!();

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
    /// VpcEndpoint controller configuration.
    pub cloud_resource_controller: Option<Arc<dyn CloudResourceController>>,

    // === Adapter options. ===
    /// The PostgreSQL URL for the adapter stash.
    pub adapter_stash_url: String,

    // === Bootstrap options. ===
    /// The cloud ID of this environment.
    pub environment_id: EnvironmentId,
    /// Availability zones in which storage and compute resources may be
    /// deployed.
    pub availability_zones: Vec<String>,
    /// A map from size name to resource allocations for cluster replicas.
    pub cluster_replica_sizes: ClusterReplicaSizeMap,
    /// The size of the cluster to create for a source or sink if no size is
    /// given.
    pub default_storage_cluster_size: Option<String>,
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
    /// 12-digit AWS account id, which will be used to generate an AWS Principal.
    pub aws_account_id: Option<String>,
    /// Supported AWS PrivateLink availability zone ids.
    pub aws_privatelink_availability_zones: Option<Vec<String>>,
    /// An SDK key for LaunchDarkly. Enables system parameter synchronization
    /// with LaunchDarkly.
    pub launchdarkly_sdk_key: Option<String>,
    /// The interval in seconds at which to synchronize system parameter values.
    pub config_sync_loop_interval: Option<Duration>,
    /// An invertible map from system parameter names to LaunchDarkly feature
    /// keys to use when propagating values from the latter to the former.
    pub launchdarkly_key_map: BTreeMap<String, String>,
    /// What role, if any, should be initially created with elevated privileges.
    pub bootstrap_role: Option<String>,

    // === Tracing options. ===
    /// The metrics registry to use.
    pub metrics_registry: MetricsRegistry,
    /// Handle to tracing.
    pub tracing_handle: TracingHandle,

    // === Testing options. ===
    /// A now generation function for mocking time.
    pub now: NowFn,
}

/// Configures TLS encryption for connections.
#[derive(Debug, Clone)]
pub struct TlsConfig {
    /// The path to the TLS certificate.
    pub cert: PathBuf,
    /// The path to the TLS key.
    pub key: PathBuf,
}

/// Start an `environmentd` server.
#[tracing::instrument(name = "environmentd::serve", level = "info", skip_all)]
pub async fn serve(config: Config) -> Result<Server, anyhow::Error> {
    let tls = mz_postgres_util::make_tls(&tokio_postgres::config::Config::from_str(
        &config.adapter_stash_url,
    )?)?;
    let stash = config
        .controller
        .postgres_factory
        .open(config.adapter_stash_url.clone(), None, tls)
        .await?;

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
                builder.set_certificate_chain_file(&tls_config.cert)?;
                builder.set_private_key_file(&tls_config.key, SslFiletype::PEM)?;
                builder.build().into_context()
            };
            let pgwire_tls = mz_pgwire::TlsConfig {
                context: context.clone(),
                mode: mz_pgwire::TlsMode::Require,
            };
            let http_tls = http::TlsConfig {
                context,
                mode: http::TlsMode::Require,
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
            tracing_handle: config.tracing_handle,
            adapter_client_rx: internal_http_adapter_client_rx,
        });
        server::serve(internal_http_conns, internal_http_server)
    });

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
        config.now.clone(),
        &BootstrapArgs {
            default_cluster_replica_size: config.bootstrap_default_cluster_replica_size,
            builtin_cluster_replica_size: config.bootstrap_builtin_cluster_replica_size,
            // TODO(benesch, brennan): remove this after v0.27.0-alpha.4 has
            // shipped to cloud since all clusters will have had a default
            // availability zone installed.
            default_availability_zone: config
                .availability_zones
                .choose(&mut rand::thread_rng())
                .cloned()
                .unwrap_or_else(|| mz_adapter::DUMMY_AVAILABILITY_ZONE.into()),
            bootstrap_role: config.bootstrap_role,
        },
    )
    .await?;

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
    let controller = mz_controller::Controller::new(config.controller, envd_epoch).await;

    // Initialize the system parameter frontend if `launchdarkly_sdk_key` is set.
    let system_parameter_frontend = if let Some(ld_sdk_key) = config.launchdarkly_sdk_key {
        let ld_key_map = config.launchdarkly_key_map;
        let env_id = config.environment_id.clone();
        let metrics_registry = config.metrics_registry.clone();
        let now = config.now.clone();
        // The `SystemParameterFrontend::new` call needs to be wrapped in a
        // spawn_blocking call because the LaunchDarkly SDK initialization uses
        // `reqwest::blocking::client`. This should be revisited after the SDK
        // is updated to 1.0.0.
        let system_parameter_frontend = task::spawn_blocking(
            || "SystemParameterFrontend::new",
            move || {
                SystemParameterFrontend::new(
                    env_id,
                    &metrics_registry,
                    ld_sdk_key.as_str(),
                    ld_key_map,
                    now,
                )
            },
        )
        .await??;
        Some(Arc::new(system_parameter_frontend))
    } else {
        None
    };

    let active_connection_count = Arc::new(Mutex::new(ConnectionCounter::new(0)));

    // Initialize adapter.
    let segment_client = config.segment_api_key.map(mz_segment::Client::new);
    let (adapter_handle, adapter_client) = mz_adapter::serve(mz_adapter::Config {
        dataflow_client: controller,
        storage: adapter_storage,
        unsafe_mode: config.unsafe_mode,
        build_info: &BUILD_INFO,
        environment_id: config.environment_id.clone(),
        metrics_registry: config.metrics_registry.clone(),
        now: config.now,
        secrets_controller: config.secrets_controller,
        cloud_resource_controller: config.cloud_resource_controller,
        cluster_replica_sizes: config.cluster_replica_sizes,
        default_storage_cluster_size: config.default_storage_cluster_size,
        availability_zones: config.availability_zones,
        system_parameter_defaults: config.system_parameter_defaults,
        connection_context: config.connection_context,
        storage_usage_client,
        storage_usage_collection_interval: config.storage_usage_collection_interval,
        storage_usage_retention_period: config.storage_usage_retention_period,
        segment_client: segment_client.clone(),
        egress_ips: config.egress_ips,
        system_parameter_frontend: system_parameter_frontend.clone(),
        aws_account_id: config.aws_account_id,
        aws_privatelink_availability_zones: config.aws_privatelink_availability_zones,
        active_connection_count: Arc::clone(&active_connection_count),
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
            tls: pgwire_tls.clone(),
            adapter_client: adapter_client.clone(),
            frontegg: config.frontegg.clone(),
            metrics: metrics.clone(),
            internal: false,
            active_connection_count: Arc::clone(&active_connection_count),
        });
        server::serve(sql_conns, sql_server)
    });

    // Launch internal SQL server.
    task::spawn(|| "internal_sql_server", {
        let internal_sql_server = mz_pgwire::Server::new(mz_pgwire::Config {
            tls: pgwire_tls.map(|mut pgwire_tls| {
                // Allow, but do not require, TLS connections on the internal
                // port. Some users of the internal SQL server do not support
                // TLS, while others require it, so we allow both.
                //
                // TODO(benesch): migrate all internal applications to TLS and
                // remove `TlsMode::Allow`.
                pgwire_tls.mode = mz_pgwire::TlsMode::Allow;
                pgwire_tls
            }),
            adapter_client: adapter_client.clone(),
            frontegg: None,
            metrics,
            internal: true,
            active_connection_count: Arc::clone(&active_connection_count),
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
    if let Some(segment_client) = segment_client {
        telemetry::start_reporting(telemetry::Config {
            segment_client,
            adapter_client: adapter_client.clone(),
            environment_id: config.environment_id,
        });
    }

    // If system_parameter_frontend and config_sync_loop_interval are present,
    // start the system_parameter_sync loop.
    if let Some(system_parameter_frontend) = system_parameter_frontend {
        let system_parameter_backend = SystemParameterBackend::new(adapter_client).await?;
        task::spawn(
            || "system_parameter_sync",
            AssertUnwindSafe(system_parameter_sync(
                system_parameter_frontend,
                system_parameter_backend,
                config.config_sync_loop_interval,
            ))
            .ore_catch_unwind(),
        );
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

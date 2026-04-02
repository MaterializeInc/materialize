// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;
use std::error::Error;
use std::future::IntoFuture;
use std::net::{IpAddr, Ipv4Addr, SocketAddr, TcpStream};
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::str::FromStr;
use std::sync::Arc;
use std::sync::LazyLock;
use std::time::Duration;
use std::{env, fs, iter};

use anyhow::anyhow;
use futures::Future;
use futures::future::{BoxFuture, LocalBoxFuture};
use headers::{Header, HeaderMapExt};
use http::Uri;
use hyper::http::header::HeaderMap;
use maplit::btreemap;
use mz_adapter::TimestampExplanation;
use mz_adapter_types::bootstrap_builtin_cluster_config::{
    ANALYTICS_CLUSTER_DEFAULT_REPLICATION_FACTOR, BootstrapBuiltinClusterConfig,
    CATALOG_SERVER_CLUSTER_DEFAULT_REPLICATION_FACTOR, PROBE_CLUSTER_DEFAULT_REPLICATION_FACTOR,
    SUPPORT_CLUSTER_DEFAULT_REPLICATION_FACTOR, SYSTEM_CLUSTER_DEFAULT_REPLICATION_FACTOR,
};

use mz_auth::password::Password;
use mz_catalog::config::ClusterReplicaSizeMap;
use mz_controller::ControllerConfig;
use mz_dyncfg::ConfigUpdates;
use mz_license_keys::ValidatedLicenseKey;
use mz_orchestrator_process::{ProcessOrchestrator, ProcessOrchestratorConfig};
use mz_orchestrator_tracing::{TracingCliArgs, TracingOrchestrator};
use mz_ore::metrics::MetricsRegistry;
use mz_ore::now::{EpochMillis, NowFn, SYSTEM_TIME};
use mz_ore::retry::Retry;
use mz_ore::task;
use mz_ore::tracing::{
    OpenTelemetryConfig, StderrLogConfig, StderrLogFormat, TracingConfig, TracingHandle,
};
use mz_persist_client::PersistLocation;
use mz_persist_client::cache::PersistClientCache;
use mz_persist_client::cfg::{CONSENSUS_CONNECTION_POOL_MAX_SIZE, PersistConfig};
use mz_persist_client::rpc::PersistGrpcPubSubServer;
use mz_secrets::SecretsController;
use mz_server_core::listeners::{
    AllowedRoles, AuthenticatorKind, BaseListenerConfig, HttpRoutesEnabled,
};
use mz_server_core::{ReloadTrigger, TlsCertConfig};
use mz_sql::catalog::EnvironmentId;
use mz_storage_types::connections::ConnectionContext;
use mz_tracing::CloneableEnvFilter;
use postgres::error::DbError;
use postgres::tls::{MakeTlsConnect, TlsConnect};
use postgres::types::{FromSql, Type};
use postgres::{NoTls, Socket};
use rcgen::{BasicConstraints, CertificateParams, DnType, IsCa, Issuer, KeyPair};
use rustls::pki_types::CertificateDer;
use tempfile::TempDir;
use tokio::net::TcpListener;
use tokio::runtime::Runtime;
use tokio_postgres::config::{Host, SslMode};
use tokio_postgres::{AsyncMessage, Client};
use tokio_postgres_rustls::MakeRustlsConnect;
use tokio_stream::wrappers::TcpListenerStream;
use tower_http::cors::AllowOrigin;
use tracing::Level;
use tracing_capture::SharedStorage;
use tracing_subscriber::EnvFilter;
use tungstenite::stream::MaybeTlsStream;
use tungstenite::{Message, WebSocket};

use crate::{
    CatalogConfig, FronteggAuthenticator, HttpListenerConfig, ListenersConfig, SqlListenerConfig,
    WebSocketAuth, WebSocketResponse,
};

pub static KAFKA_ADDRS: LazyLock<String> =
    LazyLock::new(|| env::var("KAFKA_ADDRS").unwrap_or_else(|_| "localhost:9092".into()));

/// Entry point for creating and configuring an `environmentd` test harness.
#[derive(Clone)]
pub struct TestHarness {
    data_directory: Option<PathBuf>,
    tls: Option<TlsCertConfig>,
    frontegg: Option<FronteggAuthenticator>,
    external_login_password_mz_system: Option<Password>,
    listeners_config: ListenersConfig,
    unsafe_mode: bool,
    workers: usize,
    now: NowFn,
    seed: u32,
    storage_usage_collection_interval: Duration,
    storage_usage_retention_period: Option<Duration>,
    default_cluster_replica_size: String,
    default_cluster_replication_factor: u32,
    builtin_system_cluster_config: BootstrapBuiltinClusterConfig,
    builtin_catalog_server_cluster_config: BootstrapBuiltinClusterConfig,
    builtin_probe_cluster_config: BootstrapBuiltinClusterConfig,
    builtin_support_cluster_config: BootstrapBuiltinClusterConfig,
    builtin_analytics_cluster_config: BootstrapBuiltinClusterConfig,

    propagate_crashes: bool,
    enable_tracing: bool,
    // This is currently unrelated to enable_tracing, and is used only to disable orchestrator
    // tracing.
    orchestrator_tracing_cli_args: TracingCliArgs,
    bootstrap_role: Option<String>,
    deploy_generation: u64,
    system_parameter_defaults: BTreeMap<String, String>,
    internal_console_redirect_url: Option<String>,
    metrics_registry: Option<MetricsRegistry>,
    code_version: semver::Version,
    capture: Option<SharedStorage>,
    pub environment_id: EnvironmentId,
}

impl Default for TestHarness {
    fn default() -> TestHarness {
        TestHarness {
            data_directory: None,
            tls: None,
            frontegg: None,
            external_login_password_mz_system: None,
            listeners_config: ListenersConfig {
                sql: btreemap![
                    "external".to_owned() => SqlListenerConfig {
                        addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0),
                        authenticator_kind: AuthenticatorKind::None,
                        allowed_roles: AllowedRoles::Normal,
                        enable_tls: false,
                    },
                    "internal".to_owned() => SqlListenerConfig {
                        addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0),
                        authenticator_kind: AuthenticatorKind::None,
                        allowed_roles: AllowedRoles::NormalAndInternal,
                        enable_tls: false,
                    },
                ],
                http: btreemap![
                    "external".to_owned() => HttpListenerConfig {
                        base: BaseListenerConfig {
                            addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0),
                            authenticator_kind: AuthenticatorKind::None,
                            allowed_roles: AllowedRoles::Normal,
                            enable_tls: false,
                        },
                        routes: HttpRoutesEnabled{
                            base: true,
                            webhook: true,
                            internal: false,
                            metrics: false,
                            profiling: false,
                            mcp_agents: false,
                            mcp_observatory: false,
                            console_config: true,
                        },
                    },
                    "internal".to_owned() => HttpListenerConfig {
                        base: BaseListenerConfig {
                            addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0),
                            authenticator_kind: AuthenticatorKind::None,
                            allowed_roles: AllowedRoles::NormalAndInternal,
                            enable_tls: false,
                        },
                        routes: HttpRoutesEnabled{
                            base: true,
                            webhook: true,
                            internal: true,
                            metrics: true,
                            profiling: true,
                            mcp_agents: false,
                            mcp_observatory: false,
                            console_config: true,
                        },
                    },
                ],
            },
            unsafe_mode: false,
            workers: 1,
            now: SYSTEM_TIME.clone(),
            seed: rand::random(),
            storage_usage_collection_interval: Duration::from_secs(3600),
            storage_usage_retention_period: None,
            default_cluster_replica_size: "scale=1,workers=1".to_string(),
            default_cluster_replication_factor: 1,
            builtin_system_cluster_config: BootstrapBuiltinClusterConfig {
                size: "scale=1,workers=1".to_string(),
                replication_factor: SYSTEM_CLUSTER_DEFAULT_REPLICATION_FACTOR,
            },
            builtin_catalog_server_cluster_config: BootstrapBuiltinClusterConfig {
                size: "scale=1,workers=1".to_string(),
                replication_factor: CATALOG_SERVER_CLUSTER_DEFAULT_REPLICATION_FACTOR,
            },
            builtin_probe_cluster_config: BootstrapBuiltinClusterConfig {
                size: "scale=1,workers=1".to_string(),
                replication_factor: PROBE_CLUSTER_DEFAULT_REPLICATION_FACTOR,
            },
            builtin_support_cluster_config: BootstrapBuiltinClusterConfig {
                size: "scale=1,workers=1".to_string(),
                replication_factor: SUPPORT_CLUSTER_DEFAULT_REPLICATION_FACTOR,
            },
            builtin_analytics_cluster_config: BootstrapBuiltinClusterConfig {
                size: "scale=1,workers=1".to_string(),
                replication_factor: ANALYTICS_CLUSTER_DEFAULT_REPLICATION_FACTOR,
            },
            propagate_crashes: false,
            enable_tracing: false,
            bootstrap_role: Some("materialize".into()),
            deploy_generation: 0,
            // This and startup_log_filter below are both (?) needed to suppress clusterd messages.
            // If we need those in the future, we might need to change both.
            system_parameter_defaults: BTreeMap::from([(
                "log_filter".to_string(),
                "error".to_string(),
            )]),
            internal_console_redirect_url: None,
            metrics_registry: None,
            orchestrator_tracing_cli_args: TracingCliArgs {
                startup_log_filter: CloneableEnvFilter::from_str("error").expect("must parse"),
                ..Default::default()
            },
            code_version: crate::BUILD_INFO.semver_version(),
            environment_id: EnvironmentId::for_tests(),
            capture: None,
        }
    }
}

impl TestHarness {
    /// Starts a test [`TestServer`], panicking if the server could not be started.
    ///
    /// For cases when startup might fail, see [`TestHarness::try_start`].
    pub async fn start(self) -> TestServer {
        self.try_start().await.expect("Failed to start test Server")
    }

    /// Like [`TestHarness::start`] but can specify a cert reload trigger.
    pub async fn start_with_trigger(self, tls_reload_certs: ReloadTrigger) -> TestServer {
        self.try_start_with_trigger(tls_reload_certs)
            .await
            .expect("Failed to start test Server")
    }

    /// Starts a test [`TestServer`], returning an error if the server could not be started.
    pub async fn try_start(self) -> Result<TestServer, anyhow::Error> {
        self.try_start_with_trigger(mz_server_core::cert_reload_never_reload())
            .await
    }

    /// Like [`TestHarness::try_start`] but can specify a cert reload trigger.
    pub async fn try_start_with_trigger(
        self,
        tls_reload_certs: ReloadTrigger,
    ) -> Result<TestServer, anyhow::Error> {
        let listeners = Listeners::new(&self).await?;
        listeners.serve_with_trigger(self, tls_reload_certs).await
    }

    /// Starts a runtime and returns a [`TestServerWithRuntime`].
    pub fn start_blocking(self) -> TestServerWithRuntime {
        let runtime = tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .thread_stack_size(mz_ore::stack::STACK_SIZE)
            .build()
            .expect("failed to spawn runtime for test");
        let runtime = Arc::new(runtime);
        let server = runtime.block_on(self.start());
        TestServerWithRuntime { runtime, server }
    }

    pub fn data_directory(mut self, data_directory: impl Into<PathBuf>) -> Self {
        self.data_directory = Some(data_directory.into());
        self
    }

    pub fn with_tls(mut self, cert_path: impl Into<PathBuf>, key_path: impl Into<PathBuf>) -> Self {
        self.tls = Some(TlsCertConfig {
            cert: cert_path.into(),
            key: key_path.into(),
        });
        for (_, listener) in &mut self.listeners_config.sql {
            listener.enable_tls = true;
        }
        for (_, listener) in &mut self.listeners_config.http {
            listener.base.enable_tls = true;
        }
        self
    }

    pub fn unsafe_mode(mut self) -> Self {
        self.unsafe_mode = true;
        self
    }

    pub fn workers(mut self, workers: usize) -> Self {
        self.workers = workers;
        self
    }

    pub fn with_frontegg_auth(mut self, frontegg: &FronteggAuthenticator) -> Self {
        self.frontegg = Some(frontegg.clone());
        let enable_tls = self.tls.is_some();
        self.listeners_config = ListenersConfig {
            sql: btreemap! {
                "external".to_owned() => SqlListenerConfig {
                    addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0),
                    authenticator_kind: AuthenticatorKind::Frontegg,
                    allowed_roles: AllowedRoles::Normal,
                    enable_tls,
                },
                "internal".to_owned() => SqlListenerConfig {
                    addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0),
                    authenticator_kind: AuthenticatorKind::None,
                    allowed_roles: AllowedRoles::NormalAndInternal,
                    enable_tls: false,
                },
            },
            http: btreemap! {
                "external".to_owned() => HttpListenerConfig {
                    base: BaseListenerConfig {
                        addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0),
                        authenticator_kind: AuthenticatorKind::Frontegg,
                        allowed_roles: AllowedRoles::Normal,
                        enable_tls,
                    },
                    routes: HttpRoutesEnabled{
                        base: true,
                        webhook: true,
                        internal: false,
                        metrics: false,
                        profiling: false,
                        mcp_agents: false,
                        mcp_observatory: false,
                        console_config: true,
                    },
                },
                "internal".to_owned() => HttpListenerConfig {
                    base: BaseListenerConfig {
                        addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0),
                        authenticator_kind: AuthenticatorKind::None,
                        allowed_roles: AllowedRoles::NormalAndInternal,
                        enable_tls: false,
                    },
                    routes: HttpRoutesEnabled{
                        base: true,
                        webhook: true,
                        internal: true,
                        metrics: true,
                        profiling: true,
                        mcp_agents: false,
                        mcp_observatory: false,
                        console_config: true,
                    },
                },
            },
        };
        self
    }

    pub fn with_oidc_auth(
        mut self,
        issuer: Option<String>,
        authentication_claim: Option<String>,
        expected_audiences: Option<Vec<String>>,
    ) -> Self {
        let enable_tls = self.tls.is_some();
        self.listeners_config = ListenersConfig {
            sql: btreemap! {
                "external".to_owned() => SqlListenerConfig {
                    addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0),
                    authenticator_kind: AuthenticatorKind::Oidc,
                    allowed_roles: AllowedRoles::Normal,
                    enable_tls,
                },
                "internal".to_owned() => SqlListenerConfig {
                    addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0),
                    authenticator_kind: AuthenticatorKind::None,
                    allowed_roles: AllowedRoles::NormalAndInternal,
                    enable_tls: false,
                },
            },
            http: btreemap! {
                "external".to_owned() => HttpListenerConfig {
                    base: BaseListenerConfig {
                        addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0),
                        authenticator_kind: AuthenticatorKind::Oidc,
                        allowed_roles: AllowedRoles::Normal,
                        enable_tls,
                    },
                    routes: HttpRoutesEnabled{
                        base: true,
                        webhook: true,
                        internal: false,
                        metrics: false,
                        profiling: false,
                        mcp_agents: false,
                        mcp_observatory: false,
                        console_config: true,
                    },
                },
                "internal".to_owned() => HttpListenerConfig {
                    base: BaseListenerConfig {
                        addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0),
                        authenticator_kind: AuthenticatorKind::None,
                        allowed_roles: AllowedRoles::NormalAndInternal,
                        enable_tls: false,
                    },
                    routes: HttpRoutesEnabled{
                        base: true,
                        webhook: true,
                        internal: true,
                        metrics: true,
                        profiling: true,
                        mcp_agents: false,
                        mcp_observatory: false,
                        console_config: true,
                    },
                },
            },
        };

        if let Some(issuer) = issuer {
            self.system_parameter_defaults
                .insert("oidc_issuer".to_string(), issuer);
        }

        if let Some(authentication_claim) = authentication_claim {
            self.system_parameter_defaults.insert(
                "oidc_authentication_claim".to_string(),
                authentication_claim,
            );
        }

        if let Some(expected_audiences) = expected_audiences {
            self.system_parameter_defaults.insert(
                "oidc_audience".to_string(),
                serde_json::to_string(&expected_audiences).unwrap(),
            );
        }

        self
    }

    pub fn with_password_auth(mut self, mz_system_password: Password) -> Self {
        self.external_login_password_mz_system = Some(mz_system_password);
        let enable_tls = self.tls.is_some();
        self.listeners_config = ListenersConfig {
            sql: btreemap! {
                "external".to_owned() => SqlListenerConfig {
                    addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0),
                    authenticator_kind: AuthenticatorKind::Password,
                    allowed_roles: AllowedRoles::NormalAndInternal,
                    enable_tls,
                },
            },
            http: btreemap! {
                "external".to_owned() => HttpListenerConfig {
                    base: BaseListenerConfig {
                        addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0),
                        authenticator_kind: AuthenticatorKind::Password,
                        allowed_roles: AllowedRoles::NormalAndInternal,
                        enable_tls,
                    },
                    routes: HttpRoutesEnabled{
                        base: true,
                        webhook: true,
                        internal: true,
                        metrics: false,
                        profiling: true,
                        mcp_agents: false,
                        mcp_observatory: false,
                        console_config: true,
                    },
                },
                "metrics".to_owned() => HttpListenerConfig {
                    base: BaseListenerConfig {
                        addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0),
                        authenticator_kind: AuthenticatorKind::None,
                        allowed_roles: AllowedRoles::NormalAndInternal,
                        enable_tls: false,
                    },
                    routes: HttpRoutesEnabled{
                        base: false,
                        webhook: false,
                        internal: false,
                        metrics: true,
                        profiling: false,
                        mcp_agents: false,
                        mcp_observatory: false,
                        console_config: true,
                    },
                },
            },
        };
        self
    }

    pub fn with_sasl_scram_auth(mut self, mz_system_password: Password) -> Self {
        self.external_login_password_mz_system = Some(mz_system_password);
        let enable_tls = self.tls.is_some();
        self.listeners_config = ListenersConfig {
            sql: btreemap! {
                "external".to_owned() => SqlListenerConfig {
                    addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0),
                    authenticator_kind: AuthenticatorKind::Sasl,
                    allowed_roles: AllowedRoles::NormalAndInternal,
                    enable_tls,
                },
            },
            http: btreemap! {
                "external".to_owned() => HttpListenerConfig {
                    base: BaseListenerConfig {
                        addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0),
                        authenticator_kind: AuthenticatorKind::Password,
                        allowed_roles: AllowedRoles::NormalAndInternal,
                        enable_tls,
                    },
                    routes: HttpRoutesEnabled{
                        base: true,
                        webhook: true,
                        internal: true,
                        metrics: false,
                        profiling: true,
                        mcp_agents: false,
                        mcp_observatory: false,
                        console_config: true,
                    },
                },
                "metrics".to_owned() => HttpListenerConfig {
                    base: BaseListenerConfig {
                        addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0),
                        authenticator_kind: AuthenticatorKind::None,
                        allowed_roles: AllowedRoles::NormalAndInternal,
                        enable_tls: false,
                    },
                    routes: HttpRoutesEnabled{
                        base: false,
                        webhook: false,
                        internal: false,
                        metrics: true,
                        profiling: false,
                        mcp_agents: false,
                        mcp_observatory: false,
                        console_config: true,
                    },
                },
            },
        };
        self
    }

    pub fn with_now(mut self, now: NowFn) -> Self {
        self.now = now;
        self
    }

    pub fn with_storage_usage_collection_interval(
        mut self,
        storage_usage_collection_interval: Duration,
    ) -> Self {
        self.storage_usage_collection_interval = storage_usage_collection_interval;
        self
    }

    pub fn with_storage_usage_retention_period(
        mut self,
        storage_usage_retention_period: Duration,
    ) -> Self {
        self.storage_usage_retention_period = Some(storage_usage_retention_period);
        self
    }

    pub fn with_default_cluster_replica_size(
        mut self,
        default_cluster_replica_size: String,
    ) -> Self {
        self.default_cluster_replica_size = default_cluster_replica_size;
        self
    }

    pub fn with_builtin_system_cluster_replica_size(
        mut self,
        builtin_system_cluster_replica_size: String,
    ) -> Self {
        self.builtin_system_cluster_config.size = builtin_system_cluster_replica_size;
        self
    }

    pub fn with_builtin_system_cluster_replication_factor(
        mut self,
        builtin_system_cluster_replication_factor: u32,
    ) -> Self {
        self.builtin_system_cluster_config.replication_factor =
            builtin_system_cluster_replication_factor;
        self
    }

    pub fn with_builtin_catalog_server_cluster_replica_size(
        mut self,
        builtin_catalog_server_cluster_replica_size: String,
    ) -> Self {
        self.builtin_catalog_server_cluster_config.size =
            builtin_catalog_server_cluster_replica_size;
        self
    }

    pub fn with_propagate_crashes(mut self, propagate_crashes: bool) -> Self {
        self.propagate_crashes = propagate_crashes;
        self
    }

    pub fn with_enable_tracing(mut self, enable_tracing: bool) -> Self {
        self.enable_tracing = enable_tracing;
        self
    }

    pub fn with_bootstrap_role(mut self, bootstrap_role: Option<String>) -> Self {
        self.bootstrap_role = bootstrap_role;
        self
    }

    pub fn with_deploy_generation(mut self, deploy_generation: u64) -> Self {
        self.deploy_generation = deploy_generation;
        self
    }

    pub fn with_system_parameter_default(mut self, param: String, value: String) -> Self {
        self.system_parameter_defaults.insert(param, value);
        self
    }

    pub fn with_mcp_routes(mut self, agents: bool, observatory: bool) -> Self {
        for config in self.listeners_config.http.values_mut() {
            config.routes.mcp_agents = agents;
            config.routes.mcp_observatory = observatory;
        }
        self
    }

    pub fn with_internal_console_redirect_url(
        mut self,
        internal_console_redirect_url: Option<String>,
    ) -> Self {
        self.internal_console_redirect_url = internal_console_redirect_url;
        self
    }

    pub fn with_metrics_registry(mut self, registry: MetricsRegistry) -> Self {
        self.metrics_registry = Some(registry);
        self
    }

    pub fn with_code_version(mut self, version: semver::Version) -> Self {
        self.code_version = version;
        self
    }

    pub fn with_capture(mut self, storage: SharedStorage) -> Self {
        self.capture = Some(storage);
        self
    }
}

pub struct Listeners {
    pub inner: crate::Listeners,
}

impl Listeners {
    pub async fn new(config: &TestHarness) -> Result<Listeners, anyhow::Error> {
        let inner = crate::Listeners::bind(config.listeners_config.clone()).await?;
        Ok(Listeners { inner })
    }

    pub async fn serve(self, config: TestHarness) -> Result<TestServer, anyhow::Error> {
        self.serve_with_trigger(config, mz_server_core::cert_reload_never_reload())
            .await
    }

    pub async fn serve_with_trigger(
        self,
        config: TestHarness,
        tls_reload_certs: ReloadTrigger,
    ) -> Result<TestServer, anyhow::Error> {
        let (data_directory, temp_dir) = match config.data_directory {
            None => {
                // If no data directory is provided, we create a temporary
                // directory. The temporary directory is cleaned up when the
                // `TempDir` is dropped, so we keep it alive until the `Server` is
                // dropped.
                let temp_dir = tempfile::tempdir()?;
                (temp_dir.path().to_path_buf(), Some(temp_dir))
            }
            Some(data_directory) => (data_directory, None),
        };
        let scratch_dir = tempfile::tempdir()?;
        let (consensus_uri, timestamp_oracle_url) = {
            let seed = config.seed;
            let cockroach_url = env::var("METADATA_BACKEND_URL")
                .map_err(|_| anyhow!("METADATA_BACKEND_URL environment variable is not set"))?;
            let (client, conn) = tokio_postgres::connect(&cockroach_url, NoTls).await?;
            mz_ore::task::spawn(|| "startup-postgres-conn", async move {
                if let Err(err) = conn.await {
                    panic!("connection error: {}", err);
                };
            });
            client
                .batch_execute(&format!(
                    "CREATE SCHEMA IF NOT EXISTS consensus_{seed};
                    CREATE SCHEMA IF NOT EXISTS tsoracle_{seed};"
                ))
                .await?;
            (
                format!("{cockroach_url}?options=--search_path=consensus_{seed}")
                    .parse()
                    .expect("invalid consensus URI"),
                format!("{cockroach_url}?options=--search_path=tsoracle_{seed}")
                    .parse()
                    .expect("invalid timestamp oracle URI"),
            )
        };
        let metrics_registry = config.metrics_registry.unwrap_or_else(MetricsRegistry::new);
        let orchestrator = ProcessOrchestrator::new(ProcessOrchestratorConfig {
            image_dir: env::current_exe()?
                .parent()
                .unwrap()
                .parent()
                .unwrap()
                .to_path_buf(),
            suppress_output: false,
            environment_id: config.environment_id.to_string(),
            secrets_dir: data_directory.join("secrets"),
            command_wrapper: vec![],
            propagate_crashes: config.propagate_crashes,
            tcp_proxy: None,
            scratch_directory: scratch_dir.path().to_path_buf(),
        })
        .await?;
        let orchestrator = Arc::new(orchestrator);
        // Messing with the clock causes persist to expire leases, causing hangs and
        // panics. Is it possible/desirable to put this back somehow?
        let persist_now = SYSTEM_TIME.clone();
        let dyncfgs = mz_dyncfgs::all_dyncfgs();

        let mut updates = ConfigUpdates::default();
        // Tune down the number of connections to make this all work a little easier
        // with local postgres.
        updates.add(&CONSENSUS_CONNECTION_POOL_MAX_SIZE, 1);
        updates.apply(&dyncfgs);

        let mut persist_cfg = PersistConfig::new(&crate::BUILD_INFO, persist_now.clone(), dyncfgs);
        persist_cfg.build_version = config.code_version;
        // Stress persist more by writing rollups frequently
        persist_cfg.set_rollup_threshold(5);

        let persist_pubsub_server = PersistGrpcPubSubServer::new(&persist_cfg, &metrics_registry);
        let persist_pubsub_client = persist_pubsub_server.new_same_process_connection();
        let persist_pubsub_tcp_listener =
            TcpListener::bind(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0))
                .await
                .expect("pubsub addr binding");
        let persist_pubsub_server_port = persist_pubsub_tcp_listener
            .local_addr()
            .expect("pubsub addr has local addr")
            .port();

        // Spawn the persist pub-sub server.
        mz_ore::task::spawn(|| "persist_pubsub_server", async move {
            persist_pubsub_server
                .serve_with_stream(TcpListenerStream::new(persist_pubsub_tcp_listener))
                .await
                .expect("success")
        });
        let persist_clients =
            PersistClientCache::new(persist_cfg, &metrics_registry, |_, _| persist_pubsub_client);
        let persist_clients = Arc::new(persist_clients);

        let secrets_controller = Arc::clone(&orchestrator);
        let connection_context = ConnectionContext::for_tests(orchestrator.reader());
        let orchestrator = Arc::new(TracingOrchestrator::new(
            orchestrator,
            config.orchestrator_tracing_cli_args,
        ));
        let tracing_handle = if config.enable_tracing {
            let config = TracingConfig::<fn(&tracing::Metadata) -> sentry_tracing::EventFilter> {
                service_name: "environmentd",
                stderr_log: StderrLogConfig {
                    format: StderrLogFormat::Json,
                    filter: EnvFilter::default(),
                },
                opentelemetry: Some(OpenTelemetryConfig {
                    endpoint: "http://fake_address_for_testing:8080".to_string(),
                    headers: http::HeaderMap::new(),
                    filter: EnvFilter::default().add_directive(Level::DEBUG.into()),
                    resource: opentelemetry_sdk::resource::Resource::builder().build(),
                    max_batch_queue_size: 2048,
                    max_export_batch_size: 512,
                    max_concurrent_exports: 1,
                    batch_scheduled_delay: Duration::from_millis(5000),
                    max_export_timeout: Duration::from_secs(30),
                }),
                tokio_console: None,
                sentry: None,
                build_version: crate::BUILD_INFO.version,
                build_sha: crate::BUILD_INFO.sha,
                registry: metrics_registry.clone(),
                capture: config.capture,
            };
            mz_ore::tracing::configure(config).await?
        } else {
            TracingHandle::disabled()
        };
        let host_name = format!(
            "localhost:{}",
            self.inner.http["external"].handle.local_addr.port()
        );
        let catalog_config = CatalogConfig {
            persist_clients: Arc::clone(&persist_clients),
            metrics: Arc::new(mz_catalog::durable::Metrics::new(&MetricsRegistry::new())),
        };

        let inner = self
            .inner
            .serve(crate::Config {
                catalog_config,
                timestamp_oracle_url: Some(timestamp_oracle_url),
                controller: ControllerConfig {
                    build_info: &crate::BUILD_INFO,
                    orchestrator,
                    clusterd_image: "clusterd".into(),
                    init_container_image: None,
                    deploy_generation: config.deploy_generation,
                    persist_location: PersistLocation {
                        blob_uri: format!("file://{}/persist/blob", data_directory.display())
                            .parse()
                            .expect("invalid blob URI"),
                        consensus_uri,
                    },
                    persist_clients,
                    now: config.now.clone(),
                    metrics_registry: metrics_registry.clone(),
                    persist_pubsub_url: format!("http://localhost:{}", persist_pubsub_server_port),
                    secrets_args: mz_service::secrets::SecretsReaderCliArgs {
                        secrets_reader: mz_service::secrets::SecretsControllerKind::LocalFile,
                        secrets_reader_local_file_dir: Some(data_directory.join("secrets")),
                        secrets_reader_kubernetes_context: None,
                        secrets_reader_aws_prefix: None,
                        secrets_reader_name_prefix: None,
                    },
                    connection_context,
                    replica_http_locator: Default::default(),
                },
                secrets_controller,
                cloud_resource_controller: None,
                tls: config.tls,
                frontegg: config.frontegg,
                unsafe_mode: config.unsafe_mode,
                all_features: false,
                metrics_registry: metrics_registry.clone(),
                now: config.now,
                environment_id: config.environment_id,
                cors_allowed_origin: AllowOrigin::list([]),
                cluster_replica_sizes: ClusterReplicaSizeMap::for_tests(),
                bootstrap_default_cluster_replica_size: config.default_cluster_replica_size,
                bootstrap_default_cluster_replication_factor: config
                    .default_cluster_replication_factor,
                bootstrap_builtin_system_cluster_config: config.builtin_system_cluster_config,
                bootstrap_builtin_catalog_server_cluster_config: config
                    .builtin_catalog_server_cluster_config,
                bootstrap_builtin_probe_cluster_config: config.builtin_probe_cluster_config,
                bootstrap_builtin_support_cluster_config: config.builtin_support_cluster_config,
                bootstrap_builtin_analytics_cluster_config: config.builtin_analytics_cluster_config,
                system_parameter_defaults: config.system_parameter_defaults,
                availability_zones: Default::default(),
                tracing_handle,
                storage_usage_collection_interval: config.storage_usage_collection_interval,
                storage_usage_retention_period: config.storage_usage_retention_period,
                segment_api_key: None,
                segment_client_side: false,
                test_only_dummy_segment_client: false,
                egress_addresses: vec![],
                aws_account_id: None,
                aws_privatelink_availability_zones: None,
                launchdarkly_sdk_key: None,
                launchdarkly_key_map: Default::default(),
                config_sync_file_path: None,
                config_sync_timeout: Duration::from_secs(30),
                config_sync_loop_interval: None,
                bootstrap_role: config.bootstrap_role,
                http_host_name: Some(host_name),
                internal_console_redirect_url: config.internal_console_redirect_url,
                tls_reload_certs,
                helm_chart_version: None,
                license_key: ValidatedLicenseKey::for_tests(),
                external_login_password_mz_system: config.external_login_password_mz_system,
                force_builtin_schema_migration: None,
            })
            .await?;

        Ok(TestServer {
            inner,
            metrics_registry,
            _temp_dir: temp_dir,
            _scratch_dir: scratch_dir,
        })
    }
}

/// A running instance of `environmentd`.
pub struct TestServer {
    pub inner: crate::Server,
    pub metrics_registry: MetricsRegistry,
    /// The `TempDir`s are saved to prevent them from being dropped, and thus cleaned up too early.
    _temp_dir: Option<TempDir>,
    _scratch_dir: TempDir,
}

impl TestServer {
    pub fn connect(&self) -> ConnectBuilder<'_, postgres::NoTls, NoHandle> {
        ConnectBuilder::new(self).no_tls()
    }

    pub async fn enable_feature_flags(&self, flags: &[&'static str]) {
        let internal_client = self.connect().internal().await.unwrap();

        for flag in flags {
            internal_client
                .batch_execute(&format!("ALTER SYSTEM SET {} = true;", flag))
                .await
                .unwrap();
        }
    }

    pub async fn disable_feature_flags(&self, flags: &[&'static str]) {
        let internal_client = self.connect().internal().await.unwrap();

        for flag in flags {
            internal_client
                .batch_execute(&format!("ALTER SYSTEM SET {} = false;", flag))
                .await
                .unwrap();
        }
    }

    pub fn ws_addr(&self) -> Uri {
        format!(
            "ws://{}/api/experimental/sql",
            self.inner.http_listener_handles["external"].local_addr
        )
        .parse()
        .unwrap()
    }

    pub fn internal_ws_addr(&self) -> Uri {
        format!(
            "ws://{}/api/experimental/sql",
            self.inner.http_listener_handles["internal"].local_addr
        )
        .parse()
        .unwrap()
    }

    pub fn http_local_addr(&self) -> SocketAddr {
        self.inner.http_listener_handles["external"].local_addr
    }

    pub fn internal_http_local_addr(&self) -> SocketAddr {
        self.inner.http_listener_handles["internal"].local_addr
    }

    pub fn sql_local_addr(&self) -> SocketAddr {
        self.inner.sql_listener_handles["external"].local_addr
    }

    pub fn internal_sql_local_addr(&self) -> SocketAddr {
        self.inner.sql_listener_handles["internal"].local_addr
    }
}

/// A builder struct to configure a pgwire connection to a running [`TestServer`].
///
/// You can create this struct, and thus open a pgwire connection, using [`TestServer::connect`].
pub struct ConnectBuilder<'s, T, H> {
    /// A running `environmentd` test server.
    server: &'s TestServer,

    /// Postgres configuration for connecting to the test server.
    pg_config: tokio_postgres::Config,
    /// Port to use when connecting to the test server.
    port: u16,
    /// Tls settings to use.
    tls: T,

    /// Callback that gets invoked for every notice we receive.
    notice_callback: Option<Box<dyn FnMut(tokio_postgres::error::DbError) + Send + 'static>>,

    /// Type variable for whether or not we include the handle for the spawned [`tokio::task`].
    _with_handle: H,
}

impl<'s> ConnectBuilder<'s, (), NoHandle> {
    fn new(server: &'s TestServer) -> Self {
        let mut pg_config = tokio_postgres::Config::new();
        pg_config
            .host(&Ipv4Addr::LOCALHOST.to_string())
            .user("materialize")
            .options("--welcome_message=off")
            .application_name("environmentd_test_framework");

        ConnectBuilder {
            server,
            pg_config,
            port: server.sql_local_addr().port(),
            tls: (),
            notice_callback: None,
            _with_handle: NoHandle,
        }
    }
}

impl<'s, T, H> ConnectBuilder<'s, T, H> {
    /// Create a pgwire connection without using TLS.
    ///
    /// Note: this is the default for all connections.
    pub fn no_tls(self) -> ConnectBuilder<'s, postgres::NoTls, H> {
        ConnectBuilder {
            server: self.server,
            pg_config: self.pg_config,
            port: self.port,
            tls: postgres::NoTls,
            notice_callback: self.notice_callback,
            _with_handle: self._with_handle,
        }
    }

    /// Create a pgwire connection with TLS.
    pub fn with_tls<Tls>(self, tls: Tls) -> ConnectBuilder<'s, Tls, H>
    where
        Tls: MakeTlsConnect<Socket> + Send + 'static,
        Tls::TlsConnect: Send,
        Tls::Stream: Send,
        <Tls::TlsConnect as TlsConnect<Socket>>::Future: Send,
    {
        ConnectBuilder {
            server: self.server,
            pg_config: self.pg_config,
            port: self.port,
            tls,
            notice_callback: self.notice_callback,
            _with_handle: self._with_handle,
        }
    }

    /// Create a [`ConnectBuilder`] using the provided [`tokio_postgres::Config`].
    pub fn with_config(mut self, pg_config: tokio_postgres::Config) -> Self {
        self.pg_config = pg_config;
        self
    }

    /// Set the [`SslMode`] to be used with the resulting connection.
    pub fn ssl_mode(mut self, mode: SslMode) -> Self {
        self.pg_config.ssl_mode(mode);
        self
    }

    /// Set the user for the pgwire connection.
    pub fn user(mut self, user: &str) -> Self {
        self.pg_config.user(user);
        self
    }

    /// Set the password for the pgwire connection.
    pub fn password(mut self, password: &str) -> Self {
        self.pg_config.password(password);
        self
    }

    /// Set the application name for the pgwire connection.
    pub fn application_name(mut self, application_name: &str) -> Self {
        self.pg_config.application_name(application_name);
        self
    }

    /// Set the database name for the pgwire connection.
    pub fn dbname(mut self, dbname: &str) -> Self {
        self.pg_config.dbname(dbname);
        self
    }

    /// Set the options for the pgwire connection.
    pub fn options(mut self, options: &str) -> Self {
        self.pg_config.options(options);
        self
    }

    /// Configures this [`ConnectBuilder`] to connect to the __internal__ SQL port of the running
    /// [`TestServer`].
    ///
    /// For example, this will change the port we connect to, and the user we connect as.
    pub fn internal(mut self) -> Self {
        self.port = self.server.internal_sql_local_addr().port();
        self.pg_config.user(mz_sql::session::user::SYSTEM_USER_NAME);
        self
    }

    /// Sets a callback for any database notices that are received from the [`TestServer`].
    pub fn notice_callback(self, callback: impl FnMut(DbError) + Send + 'static) -> Self {
        ConnectBuilder {
            notice_callback: Some(Box::new(callback)),
            ..self
        }
    }

    /// Configures this [`ConnectBuilder`] to return the [`mz_ore::task::JoinHandle`] that is
    /// polling the underlying postgres connection, associated with the returned client.
    pub fn with_handle(self) -> ConnectBuilder<'s, T, WithHandle> {
        ConnectBuilder {
            server: self.server,
            pg_config: self.pg_config,
            port: self.port,
            tls: self.tls,
            notice_callback: self.notice_callback,
            _with_handle: WithHandle,
        }
    }

    /// Returns the [`tokio_postgres::Config`] that will be used to connect.
    pub fn as_pg_config(&self) -> &tokio_postgres::Config {
        &self.pg_config
    }
}

/// This trait enables us to either include or omit the [`mz_ore::task::JoinHandle`] in the result
/// of a client connection.
pub trait IncludeHandle: Send {
    type Output;
    fn transform_result(
        client: tokio_postgres::Client,
        handle: mz_ore::task::JoinHandle<()>,
    ) -> Self::Output;
}

/// Type parameter that denotes we __will not__ return the [`mz_ore::task::JoinHandle`] in the
/// result of a [`ConnectBuilder`].
pub struct NoHandle;
impl IncludeHandle for NoHandle {
    type Output = tokio_postgres::Client;
    fn transform_result(
        client: tokio_postgres::Client,
        _handle: mz_ore::task::JoinHandle<()>,
    ) -> Self::Output {
        client
    }
}

/// Type parameter that denotes we __will__ return the [`mz_ore::task::JoinHandle`] in the result of
/// a [`ConnectBuilder`].
pub struct WithHandle;
impl IncludeHandle for WithHandle {
    type Output = (tokio_postgres::Client, mz_ore::task::JoinHandle<()>);
    fn transform_result(
        client: tokio_postgres::Client,
        handle: mz_ore::task::JoinHandle<()>,
    ) -> Self::Output {
        (client, handle)
    }
}

impl<'s, T, H> IntoFuture for ConnectBuilder<'s, T, H>
where
    T: MakeTlsConnect<Socket> + Send + 'static,
    T::TlsConnect: Send,
    T::Stream: Send,
    <T::TlsConnect as TlsConnect<Socket>>::Future: Send,
    H: IncludeHandle,
{
    type Output = Result<H::Output, postgres::Error>;
    type IntoFuture = BoxFuture<'static, Self::Output>;

    fn into_future(mut self) -> Self::IntoFuture {
        Box::pin(async move {
            assert!(
                self.pg_config.get_ports().is_empty(),
                "specifying multiple ports is not supported"
            );
            self.pg_config.port(self.port);

            let (client, mut conn) = self.pg_config.connect(self.tls).await?;
            let mut notice_callback = self.notice_callback.take();

            let handle = task::spawn(|| "connect", async move {
                while let Some(msg) = std::future::poll_fn(|cx| conn.poll_message(cx)).await {
                    match msg {
                        Ok(AsyncMessage::Notice(notice)) => {
                            if let Some(callback) = notice_callback.as_mut() {
                                callback(notice);
                            }
                        }
                        Ok(msg) => {
                            tracing::debug!(?msg, "Dropping message from database");
                        }
                        Err(e) => {
                            // tokio_postgres::Connection docs say:
                            // > Return values of None or Some(Err(_)) are “terminal”; callers
                            // > should not invoke this method again after receiving one of those
                            // > values.
                            tracing::info!("connection error: {e}");
                            break;
                        }
                    }
                }
                tracing::info!("connection closed");
            });

            let output = H::transform_result(client, handle);
            Ok(output)
        })
    }
}

/// A running instance of `environmentd`, that exposes blocking/synchronous test helpers.
///
/// Note: Ideally you should use a [`TestServer`] which relies on an external runtime, e.g. the
/// [`tokio::test`] macro. This struct exists so we can incrementally migrate our existing tests.
pub struct TestServerWithRuntime {
    server: TestServer,
    runtime: Arc<Runtime>,
}

impl TestServerWithRuntime {
    /// Returns the [`Runtime`] owned by this [`TestServerWithRuntime`].
    ///
    /// Can be used to spawn async tasks.
    pub fn runtime(&self) -> &Arc<Runtime> {
        &self.runtime
    }

    /// Returns a referece to the inner running `environmentd` [`crate::Server`]`.
    pub fn inner(&self) -> &crate::Server {
        &self.server.inner
    }

    /// Connect to the __public__ SQL port of the running `environmentd` server.
    pub fn connect<T>(&self, tls: T) -> Result<postgres::Client, postgres::Error>
    where
        T: MakeTlsConnect<Socket> + Send + 'static,
        T::TlsConnect: Send,
        T::Stream: Send,
        <T::TlsConnect as TlsConnect<Socket>>::Future: Send,
    {
        self.pg_config().connect(tls)
    }

    /// Connect to the __internal__ SQL port of the running `environmentd` server.
    pub fn connect_internal<T>(&self, tls: T) -> Result<postgres::Client, anyhow::Error>
    where
        T: MakeTlsConnect<Socket> + Send + 'static,
        T::TlsConnect: Send,
        T::Stream: Send,
        <T::TlsConnect as TlsConnect<Socket>>::Future: Send,
    {
        Ok(self.pg_config_internal().connect(tls)?)
    }

    /// Enable LaunchDarkly feature flags.
    pub fn enable_feature_flags(&self, flags: &[&'static str]) {
        let mut internal_client = self.connect_internal(postgres::NoTls).unwrap();

        for flag in flags {
            internal_client
                .batch_execute(&format!("ALTER SYSTEM SET {} = true;", flag))
                .unwrap();
        }
    }

    /// Disable LaunchDarkly feature flags.
    pub fn disable_feature_flags(&self, flags: &[&'static str]) {
        let mut internal_client = self.connect_internal(postgres::NoTls).unwrap();

        for flag in flags {
            internal_client
                .batch_execute(&format!("ALTER SYSTEM SET {} = false;", flag))
                .unwrap();
        }
    }

    /// Return a [`postgres::Config`] for connecting to the __public__ SQL port of the running
    /// `environmentd` server.
    pub fn pg_config(&self) -> postgres::Config {
        let local_addr = self.server.sql_local_addr();
        let mut config = postgres::Config::new();
        config
            .host(&Ipv4Addr::LOCALHOST.to_string())
            .port(local_addr.port())
            .user("materialize")
            .options("--welcome_message=off");
        config
    }

    /// Return a [`postgres::Config`] for connecting to the __internal__ SQL port of the running
    /// `environmentd` server.
    pub fn pg_config_internal(&self) -> postgres::Config {
        let local_addr = self.server.internal_sql_local_addr();
        let mut config = postgres::Config::new();
        config
            .host(&Ipv4Addr::LOCALHOST.to_string())
            .port(local_addr.port())
            .user("mz_system")
            .options("--welcome_message=off");
        config
    }

    pub fn ws_addr(&self) -> Uri {
        self.server.ws_addr()
    }

    pub fn internal_ws_addr(&self) -> Uri {
        self.server.internal_ws_addr()
    }

    pub fn http_local_addr(&self) -> SocketAddr {
        self.server.http_local_addr()
    }

    pub fn internal_http_local_addr(&self) -> SocketAddr {
        self.server.internal_http_local_addr()
    }

    pub fn sql_local_addr(&self) -> SocketAddr {
        self.server.sql_local_addr()
    }

    pub fn internal_sql_local_addr(&self) -> SocketAddr {
        self.server.internal_sql_local_addr()
    }

    /// Returns the metrics registry for the test server.
    pub fn metrics_registry(&self) -> &MetricsRegistry {
        &self.server.metrics_registry
    }
}

#[derive(Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd)]
pub struct MzTimestamp(pub u64);

impl<'a> FromSql<'a> for MzTimestamp {
    fn from_sql(ty: &Type, raw: &'a [u8]) -> Result<MzTimestamp, Box<dyn Error + Sync + Send>> {
        let n = mz_pgrepr::Numeric::from_sql(ty, raw)?;
        Ok(MzTimestamp(u64::try_from(n.0.0)?))
    }

    fn accepts(ty: &Type) -> bool {
        mz_pgrepr::Numeric::accepts(ty)
    }
}

pub trait PostgresErrorExt {
    fn unwrap_db_error(self) -> DbError;
}

impl PostgresErrorExt for postgres::Error {
    fn unwrap_db_error(self) -> DbError {
        match self.source().and_then(|e| e.downcast_ref::<DbError>()) {
            Some(e) => e.clone(),
            None => panic!("expected DbError, but got: {:?}", self),
        }
    }
}

impl<T, E> PostgresErrorExt for Result<T, E>
where
    E: PostgresErrorExt,
{
    fn unwrap_db_error(self) -> DbError {
        match self {
            Ok(_) => panic!("expected Err(DbError), but got Ok(_)"),
            Err(e) => e.unwrap_db_error(),
        }
    }
}

/// Group commit will block writes until the current time has advanced. This can make
/// performing inserts while using deterministic time difficult. This is a helper
/// method to perform writes and advance the current time.
pub async fn insert_with_deterministic_timestamps(
    table: &'static str,
    values: &'static str,
    server: &TestServer,
    now: Arc<std::sync::Mutex<EpochMillis>>,
) -> Result<(), Box<dyn Error>> {
    let client_write = server.connect().await?;
    let client_read = server.connect().await?;

    let mut current_ts = get_explain_timestamp(table, &client_read).await;

    let insert_query = format!("INSERT INTO {table} VALUES {values}");

    let write_future = client_write.execute(&insert_query, &[]);
    let timestamp_interval = tokio::time::interval(Duration::from_millis(1));

    let mut write_future = std::pin::pin!(write_future);
    let mut timestamp_interval = std::pin::pin!(timestamp_interval);

    // Keep increasing `now` until the write has executed succeed. Table advancements may
    // have increased the global timestamp by an unknown amount.
    loop {
        tokio::select! {
            _ = (&mut write_future) => return Ok(()),
            _ = timestamp_interval.tick() => {
                current_ts += 1;
                *now.lock().expect("lock poisoned") = current_ts;
            }
        };
    }
}

pub async fn get_explain_timestamp(from_suffix: &str, client: &Client) -> EpochMillis {
    try_get_explain_timestamp(from_suffix, client)
        .await
        .unwrap()
}

pub async fn try_get_explain_timestamp(
    from_suffix: &str,
    client: &Client,
) -> Result<EpochMillis, anyhow::Error> {
    let det = get_explain_timestamp_determination(from_suffix, client).await?;
    let ts = det.determination.timestamp_context.timestamp_or_default();
    Ok(ts.into())
}

pub async fn get_explain_timestamp_determination(
    from_suffix: &str,
    client: &Client,
) -> Result<TimestampExplanation<mz_repr::Timestamp>, anyhow::Error> {
    let row = client
        .query_one(
            &format!("EXPLAIN TIMESTAMP AS JSON FOR SELECT * FROM {from_suffix}"),
            &[],
        )
        .await?;
    let explain: String = row.get(0);
    Ok(serde_json::from_str(&explain).unwrap())
}

/// Helper function to create a Postgres source.
///
/// IMPORTANT: Make sure to call closure that is returned at the end of the test to clean up
/// Postgres state.
///
/// WARNING: If multiple tests use this, and the tests are run in parallel, then make sure the test
/// use different postgres tables.
pub async fn create_postgres_source_with_table<'a>(
    server: &TestServer,
    mz_client: &Client,
    table_name: &str,
    table_schema: &str,
    source_name: &str,
) -> (
    Client,
    impl FnOnce(&'a Client, &'a Client) -> LocalBoxFuture<'a, ()>,
) {
    server
        .enable_feature_flags(&["enable_create_table_from_source"])
        .await;

    let postgres_url = env::var("POSTGRES_URL")
        .map_err(|_| anyhow!("POSTGRES_URL environment variable is not set"))
        .unwrap();

    let (pg_client, connection) = tokio_postgres::connect(&postgres_url, postgres::NoTls)
        .await
        .unwrap();

    let pg_config: tokio_postgres::Config = postgres_url.parse().unwrap();
    let user = pg_config.get_user().unwrap_or("postgres");
    let db_name = pg_config.get_dbname().unwrap_or(user);
    let ports = pg_config.get_ports();
    let port = if ports.is_empty() { 5432 } else { ports[0] };
    let hosts = pg_config.get_hosts();
    let host = if hosts.is_empty() {
        "localhost".to_string()
    } else {
        match &hosts[0] {
            Host::Tcp(host) => host.to_string(),
            Host::Unix(host) => host.to_str().unwrap().to_string(),
        }
    };
    let password = pg_config.get_password();

    mz_ore::task::spawn(|| "postgres-source-connection", async move {
        if let Err(e) = connection.await {
            panic!("connection error: {}", e);
        }
    });

    // Create table in Postgres with publication.
    let _ = pg_client
        .execute(&format!("DROP TABLE IF EXISTS {table_name};"), &[])
        .await
        .unwrap();
    let _ = pg_client
        .execute(&format!("DROP PUBLICATION IF EXISTS {source_name};"), &[])
        .await
        .unwrap();
    let _ = pg_client
        .execute(&format!("CREATE TABLE {table_name} {table_schema};"), &[])
        .await
        .unwrap();
    let _ = pg_client
        .execute(
            &format!("ALTER TABLE {table_name} REPLICA IDENTITY FULL;"),
            &[],
        )
        .await
        .unwrap();
    let _ = pg_client
        .execute(
            &format!("CREATE PUBLICATION {source_name} FOR TABLE {table_name};"),
            &[],
        )
        .await
        .unwrap();

    // Create postgres source in Materialize.
    let mut connection_str = format!("HOST '{host}', PORT {port}, USER {user}, DATABASE {db_name}");
    if let Some(password) = password {
        let password = std::str::from_utf8(password).unwrap();
        mz_client
            .batch_execute(&format!("CREATE SECRET s AS '{password}'"))
            .await
            .unwrap();
        connection_str = format!("{connection_str}, PASSWORD SECRET s");
    }
    mz_client
        .batch_execute(&format!(
            "CREATE CONNECTION pgconn TO POSTGRES ({connection_str})"
        ))
        .await
        .unwrap();
    mz_client
        .batch_execute(&format!(
            "CREATE SOURCE {source_name}
            FROM POSTGRES
            CONNECTION pgconn
            (PUBLICATION '{source_name}')"
        ))
        .await
        .unwrap();
    mz_client
        .batch_execute(&format!(
            "CREATE TABLE {table_name}
            FROM SOURCE {source_name}
            (REFERENCE {table_name});"
        ))
        .await
        .unwrap();

    let table_name = table_name.to_string();
    let source_name = source_name.to_string();
    (
        pg_client,
        move |mz_client: &'a Client, pg_client: &'a Client| {
            let f: Pin<Box<dyn Future<Output = ()> + 'a>> = Box::pin(async move {
                mz_client
                    .batch_execute(&format!("DROP SOURCE {source_name} CASCADE;"))
                    .await
                    .unwrap();
                mz_client
                    .batch_execute("DROP CONNECTION pgconn;")
                    .await
                    .unwrap();

                let _ = pg_client
                    .execute(&format!("DROP PUBLICATION {source_name};"), &[])
                    .await
                    .unwrap();
                let _ = pg_client
                    .execute(&format!("DROP TABLE {table_name};"), &[])
                    .await
                    .unwrap();
            });
            f
        },
    )
}

pub async fn wait_for_pg_table_population(mz_client: &Client, view_name: &str, source_rows: i64) {
    let current_isolation = mz_client
        .query_one("SHOW transaction_isolation", &[])
        .await
        .unwrap()
        .get::<_, String>(0);
    mz_client
        .batch_execute("SET transaction_isolation = SERIALIZABLE")
        .await
        .unwrap();
    Retry::default()
        .retry_async(|_| async move {
            let rows = mz_client
                .query_one(&format!("SELECT COUNT(*) FROM {view_name};"), &[])
                .await
                .unwrap()
                .get::<_, i64>(0);
            if rows == source_rows {
                Ok(())
            } else {
                Err(format!(
                    "Waiting for {source_rows} row to be ingested. Currently at {rows}."
                ))
            }
        })
        .await
        .unwrap();
    mz_client
        .batch_execute(&format!(
            "SET transaction_isolation = '{current_isolation}'"
        ))
        .await
        .unwrap();
}

// Initializes a websocket connection. Returns the init messages before the initial ReadyForQuery.
pub fn auth_with_ws(
    ws: &mut WebSocket<MaybeTlsStream<TcpStream>>,
    mut options: BTreeMap<String, String>,
) -> Result<Vec<WebSocketResponse>, anyhow::Error> {
    if !options.contains_key("welcome_message") {
        options.insert("welcome_message".into(), "off".into());
    }
    auth_with_ws_impl(
        ws,
        Message::Text(
            serde_json::to_string(&WebSocketAuth::Basic {
                user: "materialize".into(),
                password: "".into(),
                options,
            })
            .unwrap()
            .into(),
        ),
    )
}

pub fn auth_with_ws_impl(
    ws: &mut WebSocket<MaybeTlsStream<TcpStream>>,
    auth_message: Message,
) -> Result<Vec<WebSocketResponse>, anyhow::Error> {
    ws.send(auth_message)?;

    // Wait for initial ready response.
    let mut msgs = Vec::new();
    loop {
        let resp = ws.read()?;
        match resp {
            Message::Text(msg) => {
                let msg: WebSocketResponse = serde_json::from_str(&msg).unwrap();
                match msg {
                    WebSocketResponse::ReadyForQuery(_) => break,
                    msg => {
                        msgs.push(msg);
                    }
                }
            }
            Message::Ping(_) => continue,
            Message::Close(None) => return Err(anyhow!("ws closed after auth")),
            Message::Close(Some(close_frame)) => {
                return Err(anyhow!("ws closed after auth").context(close_frame));
            }
            _ => panic!("unexpected response: {:?}", resp),
        }
    }
    Ok(msgs)
}

pub fn make_header<H: Header>(h: H) -> HeaderMap {
    let mut map = HeaderMap::new();
    map.typed_insert(h);
    map
}

/// Builds a rustls [`MakeRustlsConnect`] from a [`TestTlsConfig`].
pub fn make_pg_tls(config: TestTlsConfig) -> MakeRustlsConnect {
    MakeRustlsConnect::new((*config.build_rustls_client_config()).clone())
}

/// Performs a TLS handshake to `addr` and returns the peer's leaf certificate
/// in DER encoding.
///
/// We use a raw `tokio_rustls` connection instead of reqwest because
/// `reqwest::tls::TlsInfo::peer_certificate()` only returns the peer cert
/// when reqwest is built with the `native-tls` backend. With the `rustls-tls`
/// backend it always returns `None` — a known reqwest limitation. By dropping
/// down to `tokio_rustls` directly we can call
/// `ServerConnection::peer_certificates()` which always works.
pub async fn peer_certificate_der(
    addr: std::net::SocketAddr,
    tls_config: &TestTlsConfig,
) -> Vec<u8> {
    use tokio_rustls::TlsConnector;

    let connector = TlsConnector::from(tls_config.build_rustls_client_config());
    let server_name = rustls::pki_types::ServerName::IpAddress(addr.ip().into());
    let tcp = tokio::net::TcpStream::connect(addr).await.unwrap();
    let tls = connector.connect(server_name, tcp).await.unwrap();
    let (_, session) = tls.get_ref();
    let certs = session.peer_certificates().expect("peer certificates");
    certs[0].as_ref().to_vec()
}

/// Reads a PEM certificate file and returns the first certificate as DER bytes.
pub fn cert_file_to_der(path: &Path) -> Vec<u8> {
    let pem = fs::read(path).unwrap();
    let certs: Vec<CertificateDer> = rustls_pemfile::certs(&mut &*pem)
        .collect::<Result<_, _>>()
        .unwrap();
    certs[0].as_ref().to_vec()
}

/// Configuration for test TLS connections, replacing the old
/// `FnOnce(&mut SslConnectorBuilder)` closure pattern.
#[derive(Clone, Debug)]
pub struct TestTlsConfig {
    /// CA certificate files to trust. Empty = no custom roots.
    pub ca_certs: Vec<PathBuf>,
    /// Client certificate and key for mTLS.
    pub client_cert: Option<(PathBuf, PathBuf)>,
    /// Whether to verify the server certificate.
    pub verify: bool,
}

impl TestTlsConfig {
    /// No verification, no client cert.
    pub fn no_verify() -> Self {
        TestTlsConfig {
            ca_certs: Vec::new(),
            client_cert: None,
            verify: false,
        }
    }

    /// Verify with the given CA cert.
    pub fn with_ca(ca_cert: &Path) -> Self {
        TestTlsConfig {
            ca_certs: vec![ca_cert.to_path_buf()],
            client_cert: None,
            verify: true,
        }
    }

    /// Add a client certificate (for mTLS).
    pub fn with_client_cert(mut self, cert: &Path, key: &Path) -> Self {
        self.client_cert = Some((cert.to_path_buf(), key.to_path_buf()));
        self
    }

    /// Build the rustls [`ClientConfig`](rustls::ClientConfig) from this configuration.
    pub fn build_rustls_client_config(&self) -> Arc<rustls::ClientConfig> {
        use rustls::client::danger::{
            HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier,
        };
        use rustls::pki_types::{ServerName, UnixTime};
        use rustls::{DigitallySignedStruct, SignatureScheme};

        let provider = mz_ore::crypto::fips_crypto_provider();

        let builder = if self.verify && !self.ca_certs.is_empty() {
            let mut root_store = rustls::RootCertStore::empty();
            for ca_path in &self.ca_certs {
                let pem = fs::read(ca_path).unwrap();
                let certs: Vec<CertificateDer> = rustls_pemfile::certs(&mut &*pem)
                    .collect::<Result<_, _>>()
                    .unwrap();
                for cert in certs {
                    root_store.add(cert).unwrap();
                }
            }
            rustls::ClientConfig::builder_with_provider(provider)
                .with_protocol_versions(&[&rustls::version::TLS12])
                .unwrap()
                .with_root_certificates(root_store)
        } else {
            // No verification.
            #[derive(Debug)]
            struct NoVerifier(Arc<rustls::crypto::CryptoProvider>);
            impl ServerCertVerifier for NoVerifier {
                fn verify_server_cert(
                    &self,
                    _: &CertificateDer<'_>,
                    _: &[CertificateDer<'_>],
                    _: &ServerName<'_>,
                    _: &[u8],
                    _: UnixTime,
                ) -> Result<ServerCertVerified, rustls::Error> {
                    Ok(ServerCertVerified::assertion())
                }
                fn verify_tls12_signature(
                    &self,
                    _: &[u8],
                    _: &CertificateDer<'_>,
                    _: &DigitallySignedStruct,
                ) -> Result<HandshakeSignatureValid, rustls::Error> {
                    Ok(HandshakeSignatureValid::assertion())
                }
                fn verify_tls13_signature(
                    &self,
                    _: &[u8],
                    _: &CertificateDer<'_>,
                    _: &DigitallySignedStruct,
                ) -> Result<HandshakeSignatureValid, rustls::Error> {
                    Ok(HandshakeSignatureValid::assertion())
                }
                fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
                    self.0.signature_verification_algorithms.supported_schemes()
                }
            }
            rustls::ClientConfig::builder_with_provider(Arc::clone(&provider))
                .with_protocol_versions(&[&rustls::version::TLS12])
                .unwrap()
                .dangerous()
                .with_custom_certificate_verifier(Arc::new(NoVerifier(provider)))
        };

        let config = match &self.client_cert {
            Some((cert_path, key_path)) => {
                let cert_pem = fs::read(cert_path).unwrap();
                let key_pem = fs::read(key_path).unwrap();
                let certs: Vec<CertificateDer> = rustls_pemfile::certs(&mut &*cert_pem)
                    .collect::<Result<_, _>>()
                    .unwrap();
                let key = rustls_pemfile::private_key(&mut &*key_pem)
                    .unwrap()
                    .unwrap();
                builder.with_client_auth_cert(certs, key).unwrap()
            }
            None => builder.with_no_client_auth(),
        };

        Arc::new(config)
    }
}

/// A certificate authority for use in tests.
pub struct Ca {
    pub dir: TempDir,
    /// The CA's certificate parameters, used to build an Issuer for signing.
    ca_params: CertificateParams,
    /// The CA's key pair.
    ca_key: KeyPair,
    /// PEM-encoded CA certificate bytes.
    pub cert_pem: Vec<u8>,
    /// PEM-encoded private key bytes.
    pub key_pem: Vec<u8>,
}

impl Ca {
    fn make_ca(name: &str, parent: Option<&Ca>) -> Result<Ca, Box<dyn Error>> {
        let dir = tempfile::tempdir()?;

        let mut params = CertificateParams::new(Vec::<String>::new())?;
        params
            .distinguished_name
            .push(DnType::CommonName, name.to_string());
        params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);

        let key_pair = KeyPair::generate()?;

        let cert = if let Some(parent) = parent {
            let issuer = Issuer::new(parent.ca_params.clone(), &parent.ca_key);
            params.signed_by(&key_pair, &issuer)?
        } else {
            params.self_signed(&key_pair)?
        };

        let cert_pem = cert.pem().into_bytes();
        let key_pem = key_pair.serialize_pem().into_bytes();

        fs::write(dir.path().join("ca.crt"), &cert_pem)?;

        Ok(Ca {
            dir,
            ca_params: params,
            ca_key: key_pair,
            cert_pem,
            key_pem,
        })
    }

    /// Creates a new root certificate authority.
    pub fn new_root(name: &str) -> Result<Ca, Box<dyn Error>> {
        Ca::make_ca(name, None)
    }

    /// Returns the path to the CA's certificate.
    pub fn ca_cert_path(&self) -> PathBuf {
        self.dir.path().join("ca.crt")
    }

    /// Requests a new intermediate certificate authority.
    pub fn request_ca(&self, name: &str) -> Result<Ca, Box<dyn Error>> {
        Ca::make_ca(name, Some(self))
    }

    /// Generates a certificate with the specified Common Name (CN) that is
    /// signed by the CA.
    ///
    /// Returns the paths to the certificate and key.
    pub fn request_client_cert(&self, name: &str) -> Result<(PathBuf, PathBuf), Box<dyn Error>> {
        self.request_cert(name, iter::empty())
    }

    /// Like `request_client_cert`, but permits specifying additional IP
    /// addresses to attach as Subject Alternate Names.
    pub fn request_cert<I>(&self, name: &str, ips: I) -> Result<(PathBuf, PathBuf), Box<dyn Error>>
    where
        I: IntoIterator<Item = IpAddr>,
    {
        let mut params = CertificateParams::new(Vec::<String>::new())?;
        params
            .distinguished_name
            .push(DnType::CommonName, name.to_string());

        let ip_addrs: Vec<IpAddr> = ips.into_iter().collect();
        for ip in &ip_addrs {
            params
                .subject_alt_names
                .push(rcgen::SanType::IpAddress(*ip));
        }

        let key_pair = KeyPair::generate()?;
        let issuer = Issuer::from_params(&self.ca_params, &self.ca_key);
        let cert = params.signed_by(&key_pair, &issuer)?;

        let cert_path = self.dir.path().join(Path::new(name).with_extension("crt"));
        let key_path = self.dir.path().join(Path::new(name).with_extension("key"));
        fs::write(&cert_path, cert.pem())?;
        fs::write(&key_path, key_pair.serialize_pem())?;
        Ok((cert_path, key_path))
    }

    /// Generates an RSA keypair suitable for JWT signing (RS256).
    ///
    /// This is separate from the CA's ECDSA key used for TLS.
    /// Returns a [`JwtRsaKeyPair`] with PEM-encoded private and public keys.
    pub fn generate_jwt_rsa_keypair() -> JwtRsaKeyPair {
        let key_pair = KeyPair::generate_for(&rcgen::PKCS_RSA_SHA256)
            .expect("RSA key generation requires aws_lc_rs feature");
        let private_pem = key_pair.serialize_pem().into_bytes();
        let public_pem = key_pair.public_key_pem().into_bytes();
        JwtRsaKeyPair {
            private_pem,
            public_pem,
        }
    }
}

/// RSA keypair for JWT signing in tests.
///
/// The mock OIDC and Frontegg servers require RSA keys (RS256),
/// which are separate from the ECDSA keys used for TLS certificates.
pub struct JwtRsaKeyPair {
    /// PEM-encoded RSA private key (PKCS8 format).
    pub private_pem: Vec<u8>,
    /// PEM-encoded RSA public key (SPKI format).
    pub public_pem: Vec<u8>,
}

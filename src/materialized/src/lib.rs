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

use std::collections::HashMap;
use std::fs::Permissions;
use std::net::SocketAddr;
use std::num::NonZeroUsize;
use std::os::unix::fs::PermissionsExt;
use std::path::PathBuf;
use std::str::FromStr;
use std::time::Duration;
use std::{env, fs};

use anyhow::{anyhow, Context};
use futures::StreamExt;
use openssl::ssl::{SslAcceptor, SslFiletype, SslMethod, SslVerifyMode};
use tokio::net::TcpListener;
use tokio::sync::oneshot;
use tokio_stream::wrappers::TcpListenerStream;
use tower_http::cors::AllowOrigin;

use mz_build_info::{build_info, BuildInfo};
use mz_dataflow_types::client::controller::ClusterReplicaSizeMap;
use mz_dataflow_types::client::RemoteClient;
use mz_dataflow_types::ConnectorContext;
use mz_frontegg_auth::FronteggAuthentication;
use mz_orchestrator::{Orchestrator, ServiceConfig, ServicePort};
use mz_orchestrator_kubernetes::{KubernetesOrchestrator, KubernetesOrchestratorConfig};
use mz_orchestrator_process::{ProcessOrchestrator, ProcessOrchestratorConfig};
use mz_ore::collections::CollectionExt;
use mz_ore::metrics::MetricsRegistry;
use mz_ore::now::NowFn;
use mz_ore::option::OptionExt;
use mz_ore::task;
use mz_persist_client::PersistLocation;
use mz_pid_file::PidFile;
use mz_secrets::{SecretsController, SecretsReader, SecretsReaderConfig};
use mz_secrets_filesystem::FilesystemSecretsController;
use mz_secrets_kubernetes::{KubernetesSecretsController, KubernetesSecretsControllerConfig};

use crate::mux::Mux;

pub mod http;
pub mod mux;

pub const BUILD_INFO: BuildInfo = build_info!();

/// Configuration for a `materialized` server.
#[derive(Debug, Clone)]
pub struct Config {
    // === Performance tuning options. ===
    /// The historical window in which distinctions are maintained for
    /// arrangements.
    ///
    /// As arrangements accept new timestamps they may optionally collapse prior
    /// timestamps to the same value, retaining their effect but removing their
    /// distinction. A large value or `None` results in a large amount of
    /// historical detail for arrangements; this increases the logical times at
    /// which they can be accurately queried, but consumes more memory. A low
    /// value reduces the amount of memory required but also risks not being
    /// able to use the arrangement in a query that has other constraints on the
    /// timestamps used (e.g. when joined with other arrangements).
    pub logical_compaction_window: Option<Duration>,
    /// The interval at which sources should be timestamped.
    pub timestamp_frequency: Duration,

    // === Connection options. ===
    /// The IP address and port to listen on.
    pub listen_addr: SocketAddr,
    /// The IP address and port to serve the metrics registry from.
    pub metrics_listen_addr: Option<SocketAddr>,
    /// TLS encryption configuration.
    pub tls: Option<TlsConfig>,
    /// Materialize Cloud configuration to enable Frontegg JWT user authentication.
    pub frontegg: Option<FronteggAuthentication>,
    /// Origins for which cross-origin resource sharing (CORS) for HTTP requests
    /// is permitted.
    pub cors_allowed_origin: AllowOrigin,

    // === Storage options. ===
    /// The directory in which `materialized` should store its own metadata.
    pub data_directory: PathBuf,
    /// Where the persist library should store its data.
    pub persist_location: PersistLocation,
    /// Optional Postgres connection string which will use Postgres as the metadata
    /// stash instead of sqlite from the `data_directory`.
    pub catalog_postgres_stash: Option<String>,

    // === Connector options. ===
    /// Configuration for source and sink connectors created by the storage
    /// layer. This can include configuration for external
    /// sources.
    pub connector_context: ConnectorContext,

    // === Platform options. ===
    /// Configuration of service orchestration.
    pub orchestrator: OrchestratorConfig,

    // === Secrets Storage options. ===
    /// Optional configuration for a secrets controller.
    pub secrets_controller: Option<SecretsControllerConfig>,

    // === Mode switches. ===
    /// Whether to permit usage of experimental features.
    pub experimental_mode: bool,
    /// The place where the server's metrics will be reported from.
    pub metrics_registry: MetricsRegistry,
    /// Now generation function.
    pub now: NowFn,
    /// Map of strings to corresponding compute replica sizes.
    pub replica_sizes: ClusterReplicaSizeMap,
    /// Availability zones compute resources may be deployed in.
    pub availability_zones: Vec<String>,
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

/// Configuration for the service orchestrator.
#[derive(Debug, Clone)]
pub struct OrchestratorConfig {
    /// Which orchestrator backend to use.
    pub backend: OrchestratorBackend,
    /// The storaged image reference to use.
    pub storaged_image: String,
    /// The computed image reference to use.
    pub computed_image: String,
    /// Whether or not COMPUTE and STORAGE processes should die when their connection with the
    /// ADAPTER is lost.
    pub linger: bool,
}

/// The orchestrator itself.
#[derive(Debug, Clone)]
pub enum OrchestratorBackend {
    /// A Kubernetes orchestrator.
    Kubernetes(KubernetesOrchestratorConfig),
    /// A local process orchestrator.
    Process(ProcessOrchestratorConfig),
}

/// Configuration for the service orchestrator.
#[derive(Debug, Clone)]
pub enum SecretsControllerConfig {
    LocalFileSystem,
    // Create a Kubernetes Controller.
    Kubernetes {
        /// The name of a Kubernetes context to use, if the Kubernetes configuration
        /// is loaded from the local kubeconfig.
        context: String,
        user_defined_secret: String,
        user_defined_secret_mount_path: String,
        refresh_pod_name: String,
    },
}

/// Start a `materialized` server.
pub async fn serve(config: Config) -> Result<Server, anyhow::Error> {
    match &config.catalog_postgres_stash {
        Some(s) => {
            let tls = mz_postgres_util::make_tls(&tokio_postgres::config::Config::from_str(s)?)?;
            let stash = mz_stash::Postgres::new(s.to_string(), None, tls).await?;
            let stash = mz_stash::Memory::new(stash);
            serve_stash(config, stash).await
        }
        None => {
            let stash = mz_stash::Sqlite::open(&config.data_directory.join("stash"))?;
            serve_stash(config, stash).await
        }
    }
}

async fn serve_stash<S: mz_stash::Append + 'static>(
    config: Config,
    stash: S,
) -> Result<Server, anyhow::Error> {
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

    // Attempt to acquire PID file lock.
    let pid_file =
        PidFile::open(config.data_directory.join("materialized.pid")).map_err(|e| match e {
            // Enhance error with some materialized-specific details.
            mz_pid_file::Error::AlreadyRunning { pid } => anyhow!(
                "another materialized process (PID {}) is running with the same data directory\n\
                data directory: {}\n",
                pid.display_or("<unknown>"),
                fs::canonicalize(&config.data_directory)
                    .unwrap_or_else(|_| config.data_directory.clone())
                    .display(),
            ),
            e => e.into(),
        })?;

    // Initialize network listener.
    let listener = TcpListener::bind(&config.listen_addr).await?;
    let local_addr = listener.local_addr()?;

    // Load the coordinator catalog from disk.
    let coord_storage =
        mz_coord::catalog::storage::Connection::open(stash, Some(config.experimental_mode)).await?;

    // Initialize orchestrator.
    let orchestrator: Box<dyn Orchestrator> = match config.orchestrator.backend {
        OrchestratorBackend::Kubernetes(config) => Box::new(
            KubernetesOrchestrator::new(config)
                .await
                .context("connecting to kubernetes")?,
        ),
        OrchestratorBackend::Process(config) => Box::new(ProcessOrchestrator::new(config).await?),
    };
    let default_listen_host = orchestrator.listen_host();
    let storage_service = orchestrator
        .namespace("storage")
        .ensure_service(
            "runtime",
            ServiceConfig {
                image: config.orchestrator.storaged_image.clone(),
                args: &|_hosts_ports, my_ports, _my_index| {
                    let mut storage_opts = vec![
                        format!("--workers=1"),
                        format!(
                            "--storage-addr={}:{}",
                            default_listen_host, my_ports["compute"]
                        ),
                        format!(
                            "--listen-addr={}:{}",
                            default_listen_host, my_ports["controller"]
                        ),
                        format!(
                            "--http-console-addr={}:{}",
                            default_listen_host, my_ports["http"]
                        ),
                        "--log-process-name".to_string(),
                    ];
                    if config.orchestrator.linger {
                        storage_opts.push(format!("--linger"))
                    }
                    storage_opts
                },
                ports: vec![
                    ServicePort {
                        name: "controller".into(),
                        port_hint: 2100,
                    },
                    ServicePort {
                        name: "compute".into(),
                        port_hint: 2101,
                    },
                    ServicePort {
                        name: "http".into(),
                        port_hint: 6875,
                    },
                ],
                // TODO: limits?
                cpu_limit: None,
                memory_limit: None,
                scale: NonZeroUsize::new(1).unwrap(),
                labels: HashMap::new(),
                availability_zone: None,
            },
        )
        .await?;
    let orchestrator = mz_dataflow_types::client::controller::OrchestratorConfig {
        orchestrator,
        computed_image: config.orchestrator.computed_image,
        storage_addr: storage_service.addresses("compute").into_element(),
        linger: config.orchestrator.linger,
    };

    // Initialize secrets controller.
    let (secrets_controller, secrets_reader) = match config.secrets_controller {
        None | Some(SecretsControllerConfig::LocalFileSystem) => {
            let secrets_storage = config.data_directory.join("secrets");
            fs::create_dir_all(&secrets_storage).with_context(|| {
                format!("creating secrets directory: {}", secrets_storage.display())
            })?;
            let permissions = Permissions::from_mode(0o700);
            fs::set_permissions(secrets_storage.clone(), permissions)?;
            let secrets_controller =
                Box::new(FilesystemSecretsController::new(secrets_storage.clone()));
            let secrets_reader = SecretsReader::new(SecretsReaderConfig {
                mount_path: secrets_storage,
            });
            (
                secrets_controller as Box<dyn SecretsController>,
                secrets_reader,
            )
        }
        Some(SecretsControllerConfig::Kubernetes {
            context,
            user_defined_secret,
            user_defined_secret_mount_path,
            refresh_pod_name,
        }) => {
            let secrets_controller = Box::new(
                KubernetesSecretsController::new(
                    context.to_owned(),
                    KubernetesSecretsControllerConfig {
                        user_defined_secret,
                        user_defined_secret_mount_path: user_defined_secret_mount_path.clone(),
                        refresh_pod_name,
                    },
                )
                .await
                .context("connecting to kubernetes")?,
            );
            let secrets_reader = SecretsReader::new(SecretsReaderConfig {
                mount_path: PathBuf::from(user_defined_secret_mount_path),
            });
            (
                secrets_controller as Box<dyn SecretsController>,
                secrets_reader,
            )
        }
    };

    // Initialize dataflow controller.
    let storage_client = Box::new({
        let mut client =
            RemoteClient::new(&[storage_service.addresses("controller").into_element()]);
        client.connect().await;
        client
    });

    let storage_controller = mz_dataflow_types::client::controller::storage::Controller::new(
        storage_client,
        config.data_directory,
        config.persist_location,
    )
    .await;
    let dataflow_controller =
        mz_dataflow_types::client::Controller::new(orchestrator, storage_controller);

    // Initialize coordinator.
    let (coord_handle, coord_client) = mz_coord::serve(mz_coord::Config {
        dataflow_client: dataflow_controller,
        storage: coord_storage,
        timestamp_frequency: config.timestamp_frequency,
        logical_compaction_window: config.logical_compaction_window,
        experimental_mode: config.experimental_mode,
        build_info: &BUILD_INFO,
        metrics_registry: config.metrics_registry.clone(),
        now: config.now,
        secrets_controller,
        secrets_reader,
        replica_sizes: config.replica_sizes.clone(),
        availability_zones: config.availability_zones.clone(),
        connector_context: config.connector_context,
    })
    .await?;

    // Listen on the third-party metrics port if we are configured for it.
    if let Some(addr) = config.metrics_listen_addr {
        let metrics_registry = config.metrics_registry.clone();
        task::spawn(|| "metrics_server", {
            let server = http::MetricsServer::new(metrics_registry);
            async move {
                server.serve(addr).await;
            }
        });
    }

    // Launch task to serve connections.
    //
    // The lifetime of this task is controlled by a trigger that activates on
    // drop. Draining marks the beginning of the server shutdown process and
    // indicates that new user connections (i.e., pgwire and HTTP connections)
    // should be rejected. Once all existing user connections have gracefully
    // terminated, this task exits.
    let (drain_trigger, drain_tripwire) = oneshot::channel();
    task::spawn(|| "pgwire_server", {
        let pgwire_server = mz_pgwire::Server::new(mz_pgwire::Config {
            tls: pgwire_tls,
            coord_client: coord_client.clone(),
            metrics_registry: &config.metrics_registry,
            frontegg: config.frontegg.clone(),
        });
        let http_server = http::Server::new(http::Config {
            tls: http_tls,
            frontegg: config.frontegg,
            coord_client,
            allowed_origin: config.cors_allowed_origin,
        });
        let mut mux = Mux::new();
        mux.add_handler(pgwire_server);
        mux.add_handler(http_server);
        async move {
            // TODO(benesch): replace with `listener.incoming()` if that is
            // restored when the `Stream` trait stabilizes.
            let mut incoming = TcpListenerStream::new(listener);
            mux.serve(incoming.by_ref().take_until(drain_tripwire))
                .await;
        }
    });

    Ok(Server {
        local_addr,
        _pid_file: pid_file,
        _drain_trigger: drain_trigger,
        _coord_handle: coord_handle,
    })
}

/// A running `materialized` server.
pub struct Server {
    local_addr: SocketAddr,
    _pid_file: PidFile,
    // Drop order matters for these fields.
    _drain_trigger: oneshot::Sender<()>,
    _coord_handle: mz_coord::Handle,
}

impl Server {
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }
}

mod orchestrator;
mod secrets;

use std::collections::BTreeMap;
use std::sync::{Arc, mpsc};
use std::time::Duration;

use mz_adapter_types::bootstrap_builtin_cluster_config::BootstrapBuiltinClusterConfig;
use mz_build_info::{BuildInfo, build_info};
use mz_catalog::config::ClusterReplicaSizeMap;
use mz_compute::server::ComputeInstanceContext;
use mz_controller::ControllerConfig;
use mz_environmentd::{CatalogConfig, Listeners};
use mz_license_keys::ValidatedLicenseKey;
use mz_ore::metrics::MetricsRegistry;
use mz_ore::now::SYSTEM_TIME;
use mz_ore::retry::Retry;
use mz_ore::tracing::TracingHandle;
use mz_persist_client::cache::PersistClientCache;
use mz_persist_types::PersistLocation;
use mz_secrets::{SecretsController, SecretsReader};
use mz_server_core::listeners::{
    AllowedRoles, AuthenticatorKind, BaseListenerConfig, ListenersConfig,
};
use mz_service::secrets::{SecretsControllerKind, SecretsReaderCliArgs};
use mz_sql::catalog::EnvironmentId;
use mz_storage::storage_state::StorageInstanceContext;
use mz_storage_types::connections::ConnectionContext;
use mz_txn_wal::operator::TxnsContext;
use snowglobe::Sim;
use tower_http::cors::AllowOrigin;
use turmoil::net::TcpStream;

use crate::orchestrator::SimulationOrchestrator;
use crate::secrets::FileSecretsController;

const BUILD_INFO: BuildInfo = build_info!();

fn main() -> snowglobe::Result {
    snowglobe::main()
}

#[snowglobe::scene(simulation_duration = "60s", tcp_capacity = 256)]
fn hello_world(mut sim: Sim) {
    let persist_location = init_persist(&mut sim);
    let (orchestrator_tx, orchestrator_rx) = mpsc::channel();
    let secrets_controller = Arc::new(FileSecretsController::new());
    let secrets_reader = secrets_controller.reader();

    sim.host("envd", move || {
        let persist_location = persist_location.clone();
        let orchestrator_tx = orchestrator_tx.clone();
        let secrets_controller = Arc::clone(&secrets_controller);

        async {
            run_environmentd(persist_location, orchestrator_tx, secrets_controller).await?;
            Ok(())
        }
    });

    sim.client("client", async {
        use tokio_postgres::NoTls;
        use tokio_postgres::config::Config;

        let stream = Retry::default()
            .clamp_backoff(Duration::from_secs(1))
            .retry_async(|_| TcpStream::connect("envd:6877"))
            .await
            .unwrap();
        let (client, connection) = Config::new()
            .user("mz_system")
            .connect_raw(stream, NoTls)
            .await?;

        mz_ore::task::spawn(|| "sql connection", async move {
            if let Err(e) = connection.await {
                eprintln!("connection error: {}", e);
            }
        });

        for stmt in [
            "CREATE CLUSTER test SIZE 'scale=1,workers=1'",
            "SET cluster = test",
            "CREATE TABLE t (x text)",
            "INSERT INTO t VALUES ('hello')",
            "CREATE DEFAULT INDEX ON t",
        ] {
            client.query(stmt, &[]).await?;
        }

        let rows = client.query("SELECT * FROM t", &[]).await?;
        let value: &str = rows[0].get(0);
        assert_eq!(value, "hello");

        Ok(())
    });

    while !sim.step().unwrap() {
        for cmd in orchestrator_rx.try_iter() {
            use orchestrator::Command::*;
            match cmd {
                CreateProcess { name, args } => {
                    let secrets_reader = Arc::clone(&secrets_reader);

                    sim.host(name, move || {
                        let secrets_reader = Arc::clone(&secrets_reader);
                        let args = args.clone();
                        async {
                            run_clusterd(secrets_reader, args).await?;
                            Ok(())
                        }
                    });
                }
            }
        }
    }
}

/// Start simulated persist consensus and blob processes.
fn init_persist(sim: &mut Sim) -> PersistLocation {
    use mz_persist::turmoil::*;

    sim.host("consensus", {
        let state = ConsensusState::new();
        move || serve_consensus(7000, state.clone())
    });
    sim.host("blob", {
        let state = BlobState::new();
        move || serve_blob(7000, state.clone())
    });

    PersistLocation {
        blob_uri: "turmoil://blob:7000".parse().unwrap(),
        consensus_uri: "turmoil://consensus:7000".parse().unwrap(),
    }
}

async fn run_environmentd(
    persist_location: PersistLocation,
    orchestrator_tx: mpsc::Sender<orchestrator::Command>,
    secrets_controller: Arc<FileSecretsController>,
) -> anyhow::Result<()> {
    let environment_id = EnvironmentId::for_tests();
    let now = SYSTEM_TIME.clone();

    let orchestrator = SimulationOrchestrator::new(orchestrator_tx);

    let mut persist_clients = PersistClientCache::new_for_turmoil();
    persist_clients.cfg.configs = Arc::new(mz_dyncfgs::all_dyncfgs());
    let persist_clients = Arc::new(persist_clients);

    let secrets_reader = secrets_controller.reader();
    let secrets_dir = secrets_controller.path();

    let connection_context = ConnectionContext::for_tests(secrets_reader);
    let catalog_metrics = mz_catalog::durable::Metrics::new(&MetricsRegistry::new());

    let builtin_cluster_config = BootstrapBuiltinClusterConfig {
        size: "scale=1,workers=1".into(),
        replication_factor: 1,
    };

    let listeners = Listeners::bind(ListenersConfig {
        sql: BTreeMap::from([(
            "sql".into(),
            BaseListenerConfig {
                addr: "0.0.0.0:6877".parse().unwrap(),
                authenticator_kind: AuthenticatorKind::None,
                allowed_roles: AllowedRoles::Internal,
                enable_tls: false,
            },
        )]),
        http: BTreeMap::new(),
    })
    .await?;

    let server = listeners
        .serve(mz_environmentd::Config {
            unsafe_mode: false,
            all_features: false,
            tls: None,
            tls_reload_certs: mz_server_core::cert_reload_never_reload(),
            external_login_password_mz_system: None,
            frontegg: None,
            cors_allowed_origin: AllowOrigin::list([]),
            egress_addresses: Vec::new(),
            http_host_name: None,
            internal_console_redirect_url: None,
            controller: ControllerConfig {
                build_info: &BUILD_INFO,
                orchestrator: Arc::new(orchestrator),
                persist_location,
                persist_clients: Arc::clone(&persist_clients),
                clusterd_image: "???".into(),
                init_container_image: None,
                deploy_generation: 0,
                now,
                metrics_registry: MetricsRegistry::new(),
                persist_pubsub_url: "???".into(),
                secrets_args: SecretsReaderCliArgs {
                    secrets_reader: SecretsControllerKind::LocalFile,
                    secrets_reader_local_file_dir: Some(secrets_dir),
                    secrets_reader_kubernetes_context: None,
                    secrets_reader_aws_prefix: None,
                    secrets_reader_name_prefix: None,
                },
                connection_context,
            },
            secrets_controller,
            cloud_resource_controller: None,
            storage_usage_collection_interval: Duration::from_secs(3600),
            storage_usage_retention_period: None,
            catalog_config: CatalogConfig {
                persist_clients,
                metrics: Arc::new(catalog_metrics),
            },
            availability_zones: Vec::new(),
            cluster_replica_sizes: ClusterReplicaSizeMap::for_tests(),
            timestamp_oracle_url: None,
            segment_api_key: None,
            segment_client_side: false,
            test_only_dummy_segment_client: true,
            launchdarkly_sdk_key: None,
            launchdarkly_key_map: BTreeMap::new(),
            config_sync_timeout: Duration::MAX,
            config_sync_loop_interval: None,
            config_sync_file_path: None,
            environment_id,
            bootstrap_role: None,
            bootstrap_default_cluster_replica_size: "scale=1,workers=1".into(),
            bootstrap_default_cluster_replication_factor: 1,
            bootstrap_builtin_system_cluster_config: builtin_cluster_config.clone(),
            bootstrap_builtin_catalog_server_cluster_config: builtin_cluster_config.clone(),
            bootstrap_builtin_probe_cluster_config: builtin_cluster_config.clone(),
            bootstrap_builtin_support_cluster_config: builtin_cluster_config.clone(),
            bootstrap_builtin_analytics_cluster_config: builtin_cluster_config.clone(),
            system_parameter_defaults: BTreeMap::new(),
            helm_chart_version: None,
            license_key: ValidatedLicenseKey::for_tests(),
            aws_account_id: None,
            aws_privatelink_availability_zones: None,
            metrics_registry: MetricsRegistry::new(),
            tracing_handle: TracingHandle::disabled(),
            now: SYSTEM_TIME.clone(),
            force_builtin_schema_migration: None,
        })
        .await?;
    server.run().await;

    Ok(())
}

async fn run_clusterd(
    secrets_reader: Arc<dyn SecretsReader>,
    args: orchestrator::ClusterdArgs,
) -> anyhow::Result<()> {
    let metrics_registry = MetricsRegistry::new();
    let txns_ctx = TxnsContext::default();
    let tracing_handle = Arc::new(TracingHandle::disabled());
    let connection_context = ConnectionContext::for_tests(secrets_reader);

    let mut persist_clients = PersistClientCache::new_for_turmoil();
    persist_clients.cfg.configs = Arc::new(mz_dyncfgs::all_dyncfgs());
    let persist_clients = Arc::new(persist_clients);

    let mut storage_timely_config = args.storage_timely_config;
    storage_timely_config.process = args.process;
    let mut compute_timely_config = args.compute_timely_config;
    compute_timely_config.process = args.process;

    let storage_client_builder = mz_storage::serve(
        storage_timely_config,
        &metrics_registry,
        Arc::clone(&persist_clients),
        txns_ctx.clone(),
        Arc::clone(&tracing_handle),
        SYSTEM_TIME.clone(),
        connection_context.clone(),
        StorageInstanceContext::new(None, None)?,
    )
    .await?;
    mz_ore::task::spawn(
        || "storage_server",
        mz_service::transport::serve(
            args.storage_controller_listen_addr,
            BUILD_INFO.semver_version(),
            None,
            Duration::MAX,
            storage_client_builder,
            mz_service::transport::NoopMetrics,
        ),
    );

    let compute_client_builder = mz_compute::server::serve(
        compute_timely_config,
        &metrics_registry,
        persist_clients,
        txns_ctx,
        tracing_handle,
        ComputeInstanceContext {
            scratch_directory: None,
            worker_core_affinity: false,
            connection_context,
        },
    )
    .await?;
    mz_ore::task::spawn(
        || "compute_server",
        mz_service::transport::serve(
            args.compute_controller_listen_addr,
            BUILD_INFO.semver_version(),
            None,
            Duration::MAX,
            compute_client_builder,
            mz_service::transport::NoopMetrics,
        ),
    );

    std::future::pending().await
}

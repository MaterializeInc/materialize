mod orchestrator;
mod secrets;

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use mz_adapter_types::bootstrap_builtin_cluster_config::BootstrapBuiltinClusterConfig;
use mz_build_info::build_info;
use mz_catalog::config::ClusterReplicaSizeMap;
use mz_controller::ControllerConfig;
use mz_environmentd::{CatalogConfig, Listeners};
use mz_license_keys::ValidatedLicenseKey;
use mz_ore::metrics::MetricsRegistry;
use mz_ore::now::SYSTEM_TIME;
use mz_ore::retry::Retry;
use mz_ore::tracing::TracingHandle;
use mz_persist_client::cache::PersistClientCache;
use mz_persist_types::PersistLocation;
use mz_secrets::SecretsController;
use mz_server_core::listeners::{
    AllowedRoles, AuthenticatorKind, BaseListenerConfig, ListenersConfig,
};
use mz_service::secrets::{SecretsControllerKind, SecretsReaderCliArgs};
use mz_sql::catalog::EnvironmentId;
use mz_storage_types::connections::ConnectionContext;
use snowglobe::Sim;
use tower_http::cors::AllowOrigin;
use turmoil::net::TcpStream;

use crate::orchestrator::SimulationOrchestrator;
use crate::secrets::FileSecretsController;

fn main() -> snowglobe::Result {
    snowglobe::main()
}

#[snowglobe::scene(simulation_duration = "30s")]
fn envd_startup(mut sim: Sim) {
    let persist_location = init_persist(&mut sim);

    sim.host("envd", move || {
        let persist_location = persist_location.clone();

        async {
            let environment_id = EnvironmentId::for_tests();
            let now = SYSTEM_TIME.clone();

            let orchestrator = SimulationOrchestrator::new();

            let mut persist_clients = PersistClientCache::new_for_turmoil();
            persist_clients.cfg.configs = Arc::new(mz_dyncfgs::all_dyncfgs());
            let persist_clients = Arc::new(persist_clients);

            let secrets_controller = FileSecretsController::new();
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
                        addr: "0.0.0.0:6875".parse().unwrap(),
                        authenticator_kind: AuthenticatorKind::None,
                        allowed_roles: AllowedRoles::Normal,
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
                        build_info: &build_info!(),
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
                    secrets_controller: Arc::new(secrets_controller),
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
    });

    sim.host("clusterd", || futures::future::pending());

    sim.client("client", async {
        use tokio_postgres::NoTls;
        use tokio_postgres::config::Config;

        let stream = Retry::default()
            .clamp_backoff(Duration::from_secs(1))
            .retry_async(|_| TcpStream::connect("envd:6875"))
            .await
            .unwrap();
        let (client, connection) = Config::new()
            .user("materialize")
            .connect_raw(stream, NoTls)
            .await?;

        mz_ore::task::spawn(|| "sql connection", async move {
            if let Err(e) = connection.await {
                eprintln!("connection error: {}", e);
            }
        });

        let rows = client.query("SELECT 'hello world'", &[]).await?;
        let value: &str = rows[0].get(0);
        assert_eq!(value, "hello world");

        Ok(())
    });

    sim.run().unwrap();
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

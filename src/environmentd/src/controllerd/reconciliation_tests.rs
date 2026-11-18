// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZero;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use futures::stream::BoxStream;
use mz_catalog::catalog::{Catalog, Op, ReplicaCreateDropReason};
use mz_catalog::durable::{
    BootstrapArgs, CatalogTimestampOracle, TestCatalogStateBuilder, test_bootstrap_args,
};
use mz_controller::clusters::ReplicaServiceName;
use mz_controller::clusters::provisioning::ReplicaProvisioningConfig;
use mz_controller_types::ReplicaId;
use mz_orchestrator::{
    NamespacedOrchestrator, Service, ServiceConfig, ServiceEvent, ServicePort,
    ServiceProcessMetrics,
};
use mz_ore::metrics::MetricsRegistry;
use mz_ore::now::SYSTEM_TIME;
use mz_persist_client::PersistLocation;
use mz_persist_client::cache::PersistClientCache;
use mz_persist_client::cfg::PersistConfig;
use mz_persist_client::rpc::PubSubClientConnection;
use mz_repr::{AdapterId, Timestamp};
use mz_secrets_cli::{SecretsControllerKind, SecretsReaderCliArgs};
use mz_server_core::listeners::v26_32_0::ListenersConfig;
use mz_timestamp_oracle::{TimestampOracle, WriteTimestamp};
use uuid::Uuid;

use super::provisioning::{AdapterProvisioningConfig, service_name};
use super::{reconcile_adapters, reconcile_replicas};

/// Both catalogs share durable storage, but no process-local progress or PubSub.
async fn disconnected_catalogs() -> (Catalog, Catalog, MetricsRegistry) {
    let registry = MetricsRegistry::new();
    let mut cache = PersistClientCache::new(PersistConfig::new_for_tests(), &registry, |_, _| {
        PubSubClientConnection::noop()
    });
    let location = PersistLocation::new_in_mem();
    let producer = cache.open(location.clone()).await.expect("writer Persist");
    let organization = Uuid::new_v4();
    let bootstrap = test_bootstrap_args();
    let storage = TestCatalogStateBuilder::new(producer.clone())
        .with_organization_id(organization)
        .with_deploy_generation(2)
        .unwrap_build()
        .await
        .open(SYSTEM_TIME().into(), &bootstrap)
        .await
        .expect("initialize durable catalog");
    let writer = Box::pin(Catalog::open_debug_catalog_inner(
        producer,
        storage,
        SYSTEM_TIME.clone(),
        Some(format!("local-az1-{organization}-0").parse().unwrap()),
        &mz_build_info::DUMMY_BUILD_INFO,
        // Native admission must be enabled at birth for the controller to join.
        BTreeMap::from([("enable_catalog_read_protection".into(), "true".into())]),
        &bootstrap,
        Some(false),
        None,
    ))
    .await
    .expect("open writer catalog");

    // Retain the in-memory Blob/Consensus, but give the controller its own StateCache.
    cache.clear_state_cache();
    let observer = cache.open(location).await.expect("controller Persist");
    let storage = TestCatalogStateBuilder::new(observer.clone())
        .with_organization_id(organization)
        .with_deploy_generation(2)
        .unwrap_build()
        .await
        .join()
        .await
        .expect("join controller catalog");
    let config = writer.config();
    let controller = Box::pin(Catalog::open_committed(
        writer.replica_config().into_state(
            config.build_info,
            config.environment_id.clone(),
            config.connection_context.clone(),
            observer,
        ),
        storage,
        None,
    ))
    .await
    .expect("reconstruct controller catalog")
    .catalog;
    (writer, controller, registry)
}

// Admission, catalog commits, and table WAL initialization share one clock.
#[derive(Debug)]
struct InitializationOracle(Mutex<(Timestamp, Timestamp)>);

#[async_trait::async_trait]
impl TimestampOracle<Timestamp> for InitializationOracle {
    async fn write_ts(&self) -> WriteTimestamp {
        let mut state = self.0.lock().unwrap();
        state.1 = state.0.max(state.1).step_forward();
        WriteTimestamp {
            timestamp: state.1,
            advance_to: state.1.step_forward(),
        }
    }

    async fn peek_write_ts(&self) -> Timestamp {
        self.0.lock().unwrap().1
    }

    async fn read_ts(&self) -> Timestamp {
        self.0.lock().unwrap().0
    }

    async fn apply_write(&self, timestamp: Timestamp) {
        let mut state = self.0.lock().unwrap();
        state.0 = state.0.max(timestamp);
        state.1 = state.1.max(timestamp);
    }
}

#[mz_ore::test(tokio::test)]
async fn controller_initialization_publishes_expression_cache() {
    let (template, observer, _) = disconnected_catalogs().await;
    let replica_config = template.replica_config();
    let build_info = template.config().build_info;
    let connection_context = template.config().connection_context.clone();
    observer.expire().await;
    template.expire().await;

    // Exercise configured defaults and overrides that disagree with those defaults.
    for (default, cache_override, enabled) in [
        (true, None, true),
        (false, None, false),
        (false, Some(true), true),
        (true, Some(false), false),
    ] {
        let registry = MetricsRegistry::new();
        let mut persist_config = PersistConfig::new_for_tests();
        persist_config.configs =
            Arc::new(mz_txn_wal::all_dyncfgs((*persist_config.configs).clone()));
        // The debug template cannot warm this environment's expression shard.
        let persist = PersistClientCache::new(persist_config, &registry, |_, _| {
            PubSubClientConnection::noop()
        })
        .open(PersistLocation::new_in_mem())
        .await
        .expect("isolated controller Persist");
        let organization = Uuid::new_v4();
        let now = SYSTEM_TIME().into();
        let oracle = Arc::new(InitializationOracle(Mutex::new((now, now))));
        let builder = || {
            TestCatalogStateBuilder::new(persist.clone())
                .with_organization_id(organization)
                .with_default_deploy_generation()
                .with_timestamp_oracle(CatalogTimestampOracle::new(
                    Arc::<InitializationOracle>::clone(&oracle),
                    SYSTEM_TIME.clone(),
                ))
        };
        let state = |boot_ts, cache_override| {
            let mut state = replica_config.clone().into_state(
                build_info,
                format!("local-az1-{organization}-0").parse().unwrap(),
                connection_context.clone(),
                persist.clone(),
            );
            state.read_only = false;
            state.skip_migrations = false;
            state.builtin_item_migration_config.read_only = false;
            state.boot_ts = boot_ts;
            state.enable_expression_cache_override = cache_override;
            state
                .system_parameter_defaults
                .insert("enable_expression_cache".into(), default.to_string());
            state
        };
        let mut storage = builder()
            .unwrap_build()
            .await
            .open(
                oracle.write_ts().await.timestamp,
                &BootstrapArgs {
                    controller_initialization: true,
                    ..test_bootstrap_args()
                },
            )
            .await
            .expect("fresh controller admission");
        oracle.apply_write(storage.current_upper().await).await;
        let catalog = super::initialize_fresh(
            mz_catalog::config::Config {
                storage,
                metrics_registry: &registry,
                state: state(oracle.write_ts().await.timestamp, cache_override),
            },
            Arc::new(mz_txn_wal::metrics::Metrics::new(&registry)),
            oracle.as_ref(),
        )
        .await
        .expect("controller-only initialization");
        // FIFO barrier only: all expressions must come from initialize_fresh.
        catalog
            .update_expression_cache(Vec::new(), Vec::new(), BTreeSet::new())
            .await;
        catalog.expire().await;

        let mut storage = builder()
            .unwrap_build()
            .await
            .open(oracle.write_ts().await.timestamp, &test_bootstrap_args())
            .await
            .expect("reopen completed deployment");
        oracle.apply_write(storage.current_upper().await).await;
        // Always enable reads, including for the disabled-publication controls.
        // Catalog::open uses this build and the catalog's current item versions.
        let reopened = Catalog::open(mz_catalog::config::Config {
            storage,
            metrics_registry: &MetricsRegistry::new(),
            state: state(oracle.write_ts().await.timestamp, Some(true)),
        })
        .await
        .expect("read controller expression cache");
        assert_eq!(
            reopened.uncached_local_exprs.is_empty(),
            enabled,
            "default={default}, override={cache_override:?}, misses={}",
            reopened.uncached_local_exprs.len(),
        );
        reopened.catalog.expire().await;
    }
}

fn upper_reads(registry: &MetricsRegistry) -> f64 {
    registry
        .gather()
        .into_iter()
        .find(|family| family.name() == "mz_persist_cmd_fetch_upper_count")
        .expect("upper read counter")
        .get_metric()[0]
        .get_counter()
        .value()
}

async fn commit(catalog: &mut Catalog, ops: Vec<Op>) {
    let ts = catalog.current_upper().await;
    catalog
        .transact(None, None, ts, None, ops)
        .await
        .expect("commit peer catalog changes");
}

fn adapter_id(catalog: &Catalog, name: &str) -> Option<AdapterId> {
    catalog
        .state()
        .adapter_declarations()
        .values()
        .find(|declaration| declaration.name == name)
        .map(|declaration| declaration.id)
}

#[derive(Debug, Default)]
struct InventoryOrchestrator {
    observed: Vec<String>,
    // A later list can see services absent from the cleanup's original snapshot.
    late_services: Vec<String>,
    lists: AtomicUsize,
    fail_drop: bool,
    dropped: Mutex<Vec<String>>,
    ensured: Mutex<Vec<String>>,
}

#[derive(Debug)]
struct InventoryService(BTreeMap<String, Vec<String>>);

impl Service for InventoryService {
    fn addresses(&self, port: &str) -> Vec<String> {
        self.0[port].clone()
    }
}

#[async_trait::async_trait]
impl NamespacedOrchestrator for InventoryOrchestrator {
    fn service_addresses(
        &self,
        _: &str,
        _: NonZero<u16>,
        _: &ServicePort,
    ) -> anyhow::Result<Vec<String>> {
        unreachable!("inventory reconciliation uses ensured service addresses")
    }

    fn ensure_service(&self, id: &str, config: ServiceConfig) -> anyhow::Result<Box<dyn Service>> {
        self.ensured.lock().unwrap().push(id.to_owned());
        Ok(Box::new(InventoryService(
            config
                .ports
                .iter()
                .map(|port| {
                    let addresses = (0..config.scale.get())
                        .map(|process| format!("{id}-{process}:{}", port.port_hint))
                        .collect();
                    (port.name.clone(), addresses)
                })
                .collect(),
        )))
    }

    fn drop_service(&self, id: &str) -> anyhow::Result<()> {
        anyhow::ensure!(!self.fail_drop, "drop failed");
        self.dropped.lock().unwrap().push(id.to_owned());
        Ok(())
    }

    async fn list_services(&self) -> anyhow::Result<Vec<String>> {
        let mut observed = self.observed.clone();
        if self.lists.fetch_add(1, Ordering::SeqCst) > 0 {
            observed.extend(self.late_services.clone());
        }
        Ok(observed)
    }

    async fn flush(&self) -> anyhow::Result<()> {
        Ok(())
    }

    fn watch_services(&self) -> BoxStream<'static, anyhow::Result<ServiceEvent>> {
        unreachable!("inventory reconciliation does not watch services")
    }

    async fn fetch_service_metrics(&self, _: &str) -> anyhow::Result<Vec<ServiceProcessMetrics>> {
        unreachable!("inventory reconciliation does not fetch metrics")
    }

    fn update_scheduling_config(
        &self,
        _: mz_orchestrator::scheduling_config::ServiceSchedulingConfig,
    ) {
        unreachable!("inventory reconciliation does not update scheduling")
    }
}

#[mz_ore::test(tokio::test)]
async fn adapter_cleanup_preserves_unobserved_peer_create() {
    let (mut writer, mut controller, registry) = disconnected_catalogs().await;
    let generation = writer.state().deployment_generation();
    commit(
        &mut writer,
        vec![Op::CreateAdapter {
            name: "peer_retired".into(),
            size: "1".into(),
        }],
    )
    .await;
    let retired = adapter_id(&writer, "peer_retired").unwrap();
    controller.sync_to_current_updates().await.unwrap();
    assert_eq!(adapter_id(&controller, "peer_retired"), Some(retired));
    commit(
        &mut writer,
        vec![
            Op::CreateAdapter {
                name: "peer_live".into(),
                size: "1".into(),
            },
            Op::DropAdapter {
                id: retired,
                name: "peer_retired".into(),
            },
        ],
    )
    .await;
    let live = adapter_id(&writer, "peer_live").unwrap();
    assert_eq!(adapter_id(&writer, "peer_retired"), None);

    controller.sync_to_observed_updates().await.unwrap();
    assert_eq!(adapter_id(&controller, "peer_live"), None);
    assert_eq!(adapter_id(&controller, "peer_retired"), Some(retired));
    let live_service = service_name(live, generation);
    let retired_service = service_name(retired, generation);
    // All peer transactions have committed before the backend listing.
    let orchestrator = InventoryOrchestrator {
        observed: vec![
            live_service.clone(),
            retired_service.clone(),
            service_name(retired, generation - 1),
            service_name(retired, generation + 1),
        ],
        ..Default::default()
    };
    let launch = AdapterProvisioningConfig {
        image: "unused-adapterd".into(),
        init_container_image: None,
        args: Vec::new(),
        listeners: ListenersConfig {
            sql: BTreeMap::new(),
            http: BTreeMap::new(),
        },
        generation,
    };
    // Inventory membership, including deletion safety, is independent of size availability.
    let inventory = reconcile_adapters(&mut controller, &orchestrator, &launch, &BTreeMap::new())
        .await
        .expect("reconcile stale adapter inventory");
    assert_eq!(*orchestrator.dropped.lock().unwrap(), vec![retired_service]);
    assert!(orchestrator.ensured.lock().unwrap().is_empty());
    let entry = inventory.iter().find(|entry| entry.id == live).unwrap();
    assert_eq!(entry.service, live_service);
    assert_eq!(entry.name, "peer_live");
    assert!(inventory.iter().all(|entry| entry.id != retired));
    assert_eq!(adapter_id(&controller, "peer_live"), Some(live));

    let settled = InventoryOrchestrator {
        observed: vec![live_service],
        ..Default::default()
    };
    let reads = upper_reads(&registry);
    reconcile_adapters(&mut controller, &settled, &launch, &BTreeMap::new())
        .await
        .unwrap();
    assert_eq!(
        upper_reads(&registry),
        reads,
        "known inventory needs no fresh read"
    );
    assert!(settled.dropped.lock().unwrap().is_empty());

    controller.expire().await;
    writer.expire().await;
}

#[mz_ore::test(tokio::test)]
async fn replica_cleanup_preserves_unobserved_peer_create() {
    let (mut writer, mut controller, registry) = disconnected_catalogs().await;
    let generation = writer.state().deployment_generation();
    let template = writer
        .user_cluster_replicas()
        .next()
        .expect("bootstrap managed replica")
        .clone();
    let ts = writer.current_upper().await;
    let ids = writer.allocate_user_replica_ids(2, ts).await.unwrap();
    let live = ids[0];
    let retired = ids[1];
    commit(
        &mut writer,
        vec![Op::CreateClusterReplica {
            cluster_id: template.cluster_id,
            replica_id: retired,
            name: "peer_retired".into(),
            config: template.config.clone(),
            owner_id: template.owner_id,
            reason: ReplicaCreateDropReason::Manual,
        }],
    )
    .await;
    controller.sync_to_current_updates().await.unwrap();
    assert!(
        controller
            .try_get_cluster_replica(template.cluster_id, retired)
            .is_some()
    );
    commit(
        &mut writer,
        vec![
            Op::CreateClusterReplica {
                cluster_id: template.cluster_id,
                replica_id: live,
                name: "peer_live".into(),
                config: template.config,
                owner_id: template.owner_id,
                reason: ReplicaCreateDropReason::Manual,
            },
            Op::DropClusterReplicaRealization {
                cluster_id: template.cluster_id,
                replica_id: retired,
            },
        ],
    )
    .await;
    assert!(
        writer
            .try_get_cluster_replica(template.cluster_id, retired)
            .is_none()
    );
    controller.sync_to_observed_updates().await.unwrap();
    assert!(
        controller
            .try_get_cluster_replica(template.cluster_id, live)
            .is_none()
    );
    assert!(
        controller
            .try_get_cluster_replica(template.cluster_id, retired)
            .is_some()
    );
    let service = |replica_id, generation| {
        ReplicaServiceName {
            cluster_id: template.cluster_id,
            replica_id,
            generation,
        }
        .to_string()
    };
    let live_service = service(live, generation);
    let retired_service = service(retired, generation);
    let orchestrator = InventoryOrchestrator {
        observed: vec![
            live_service.clone(),
            retired_service.clone(),
            service(retired, generation - 1),
            service(retired, generation + 1),
        ],
        late_services: vec![service(ReplicaId::User(9999), generation)],
        ..Default::default()
    };
    let dyncfg = mz_controller_types::dyncfgs::all_dyncfgs(mz_dyncfg::ConfigSet::default());
    let secrets = SecretsReaderCliArgs {
        secrets_reader: SecretsControllerKind::LocalFile,
        secrets_reader_local_file_dir: Some("unused-secrets".into()),
        secrets_reader_kubernetes_context: None,
        secrets_reader_aws_prefix: None,
        secrets_reader_name_prefix: None,
    };
    let catalog_location = PersistLocation::new_in_mem();
    let catalog_oracle_url = "postgres://unused:26257".parse().unwrap();
    let catalog_config = serde_json::to_string(&writer.replica_config()).unwrap();
    let launch = ReplicaProvisioningConfig {
        deploy_generation: generation,
        connection_context: &writer.config().connection_context,
        persist_pubsub_url: "http://unused:6879",
        catalog_persist_location: &catalog_location,
        catalog_timestamp_oracle_url: &catalog_oracle_url,
        timestamp_oracle_clock_file: None,
        catalog_follower_config: &catalog_config,
        secrets_args: &secrets,
        dyncfg: &dyncfg,
        replica_dyncfg_overrides: None,
        clusterd_image: "unused-clusterd",
        init_container_image: None,
    };
    let inventory = reconcile_replicas(&mut controller, &orchestrator, &launch)
        .await
        .expect("reconcile stale replica inventory");
    assert_eq!(
        *orchestrator.dropped.lock().unwrap(),
        vec![retired_service.clone()]
    );
    let entry = inventory
        .iter()
        .find(|entry| entry.replica_id == live)
        .unwrap();
    assert_eq!(entry.service, live_service);

    // Every process, including endpoint-free nonleaders, needs native context.
    let mut resources = entry.resources.clone();
    resources.allocation.scale = NonZero::new(2).unwrap();
    let spec = launch
        .service_config(
            entry.cluster_id,
            entry.replica_id,
            entry.cluster_name.clone(),
            entry.replica_name.clone(),
            entry.role.clone(),
            resources,
            false,
        )
        .unwrap();
    let peers: Vec<BTreeMap<_, _>> = (0..spec.scale.get())
        .map(|process| {
            spec.ports
                .iter()
                .map(|port| {
                    (
                        port.name.clone(),
                        format!("node-{process}:{}", port.port_hint),
                    )
                })
                .collect()
        })
        .collect();
    for listen_addrs in &peers {
        let args = (spec.args)(mz_orchestrator::ServiceAssignments {
            listen_addrs,
            peer_addrs: &peers,
        });
        for required in [
            format!("--catalog-cluster-id={}", entry.cluster_id),
            format!("--catalog-replica-id={}", entry.replica_id),
            format!("--catalog-deploy-generation={generation}"),
            format!("--catalog-config={catalog_config}"),
            format!(
                "--catalog-persist-blob-url={}",
                catalog_location.blob_uri.to_string_unredacted()
            ),
            format!(
                "--catalog-persist-consensus-url={}",
                catalog_location.consensus_uri.to_string_unredacted()
            ),
            format!(
                "--catalog-timestamp-oracle-url={}",
                catalog_oracle_url.to_string_unredacted()
            ),
        ] {
            assert!(
                args.contains(&required),
                "missing native launch argument {required}"
            );
        }
    }

    assert_eq!(entry.replica_name, "peer_live");
    assert!(inventory.iter().all(|entry| entry.replica_id != retired));
    assert!(
        inventory
            .iter()
            .all(|entry| entry.provisioning_error.is_none())
    );
    assert_eq!(
        orchestrator
            .ensured
            .lock()
            .unwrap()
            .iter()
            .cloned()
            .collect::<BTreeSet<_>>(),
        inventory
            .iter()
            .map(|entry| entry.service.clone())
            .collect::<BTreeSet<_>>()
    );

    let settled = InventoryOrchestrator {
        observed: vec![live_service],
        ..Default::default()
    };
    let reads = upper_reads(&registry);
    reconcile_replicas(&mut controller, &settled, &launch)
        .await
        .unwrap();
    assert_eq!(
        upper_reads(&registry),
        reads,
        "known inventory needs no fresh read"
    );
    assert!(settled.dropped.lock().unwrap().is_empty());

    let failing = InventoryOrchestrator {
        observed: vec![retired_service],
        fail_drop: true,
        ..Default::default()
    };
    let error = reconcile_replicas(&mut controller, &failing, &launch)
        .await
        .expect_err("cleanup must report a rejected process drop");
    assert_eq!(error.to_string(), "drop failed");

    controller.expire().await;
    writer.expire().await;
}

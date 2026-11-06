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
use std::sync::Mutex;

use futures::stream::BoxStream;
use mz_catalog::catalog::{Catalog, Op, ReplicaCreateDropReason};
use mz_catalog::durable::{TestCatalogStateBuilder, test_bootstrap_args};
use mz_controller::clusters::ReplicaServiceName;
use mz_controller::clusters::provisioning::ReplicaProvisioningConfig;
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
use mz_repr::AdapterId;
use mz_secrets_cli::{SecretsControllerKind, SecretsReaderCliArgs};
use mz_server_core::listeners::v26_32_0::ListenersConfig;
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
        .with_default_deploy_generation()
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
        .with_default_deploy_generation()
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
    ))
    .await
    .expect("reconstruct controller catalog")
    .catalog;
    (writer, controller, registry)
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
        self.dropped.lock().unwrap().push(id.to_owned());
        Ok(())
    }

    async fn list_services(&self) -> anyhow::Result<Vec<String>> {
        Ok(self.observed.clone())
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
            service(retired, generation + 1),
        ],
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
    let launch = ReplicaProvisioningConfig {
        deploy_generation: generation,
        connection_context: &writer.config().connection_context,
        persist_pubsub_url: "http://unused:6879",
        catalog_persist_location: None,
        catalog_timestamp_oracle_url: None,
        timestamp_oracle_clock_file: None,
        catalog_follower_config: None,
        secrets_args: &secrets,
        dyncfg: &dyncfg,
        replica_dyncfg_overrides: None,
        clusterd_image: "unused-clusterd",
        init_container_image: None,
    };
    let inventory = reconcile_replicas(&mut controller, &orchestrator, &launch)
        .await
        .expect("reconcile stale replica inventory");
    assert_eq!(*orchestrator.dropped.lock().unwrap(), vec![retired_service]);
    let entry = inventory
        .iter()
        .find(|entry| entry.replica_id == live)
        .unwrap();
    assert_eq!(entry.service, live_service);
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

    controller.expire().await;
    writer.expire().await;
}

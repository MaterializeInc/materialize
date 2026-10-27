// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

use mz_catalog::catalog::{Op, test_support};
use mz_catalog::durable::ClientRealization;
use mz_catalog::memory::objects::{ClusterConfig, ClusterVariant};
use mz_cluster_client::client::TimelyConfig;
use mz_compute::server::{ComputeInstanceContext, ComputeRuntimeRole};
use mz_ore::metrics::MetricsRegistry;
use mz_ore::now::SYSTEM_TIME;
use mz_ore::tracing::TracingHandle;
use mz_persist_client::cache::PersistClientCache;
use mz_persist_client::read::ListenEvent;
use mz_persist_client::{Diagnostics, PersistLocation, ShardId};
use mz_persist_types::codec_impls::UnitSchema;
use mz_repr::{Datum, Row};
use mz_sql::session::user::MZ_SYSTEM_ROLE_ID;
use mz_storage::server::StatusEvent;
use mz_storage::storage_state::StorageInstanceContext;
use mz_storage_client::client::{Status, StatusUpdate};
use mz_storage_client::controller::IntrospectionType;
use mz_storage_types::sources::{SourceData, kafka::KAFKA_PROGRESS_DESC};
use mz_txn_wal::operator::TxnsContext;

use super::*;
use crate::catalog_follower::execution::tests::in_child;
use crate::catalog_follower::tests::{BUILD, debug_catalog, name, store, transact};

#[mz_ore::test(tokio::test)]
async fn completed_sink_health_requires_closed_output_and_current_placement() {
    const TEST: &str = "catalog_follower::execution::storage::sinks::tests::completed_sink_health_requires_closed_output_and_current_placement";
    if !in_child(
        "MZ_CLUSTERD_COMPLETED_SINK_TEST_CHILD",
        TEST,
        Duration::from_secs(120),
    )
    .await
    {
        return;
    }

    let registry = MetricsRegistry::new();
    let mut config = mz_persist_client::cfg::PersistConfig::new_for_tests();
    config.configs = Arc::new(mz_dyncfgs::all_dyncfgs());
    let clients = Arc::new(PersistClientCache::new(config, &registry, |_, _| {
        mz_persist_client::rpc::PubSubClientConnection::noop()
    }));
    let location = PersistLocation::new_in_mem();
    let persist = clients.open(location.clone()).await.unwrap();
    let wal = ShardId::new();
    let mut catalog = debug_catalog(&persist, Some(wal)).await;
    catalog.initialize_storage_metadata().await.unwrap();
    let cluster = catalog.user_clusters().next().unwrap();
    let replica = cluster.replicas().next().unwrap().replica_id;
    let cluster = cluster.id;
    let deployment = catalog.state().deployment_generation();
    assert_eq!(
        catalog.state().active_deployment_generation(),
        Some(deployment)
    );
    let ts = catalog.current_upper().await;
    let incarnation = catalog
        .transact(
            None,
            None,
            ts,
            None,
            vec![Op::CreateClientIncarnation {
                realization: ClientRealization::Replica(replica),
                expected: None,
            }],
        )
        .await
        .unwrap()
        .created_client_incarnations[0];

    let mut ids = Vec::new();
    for (item_name, sql) in [
        ("input", "CREATE TABLE materialize.public.input (a bigint)"),
        (
            "connection",
            "CREATE CONNECTION materialize.public.connection TO KAFKA (BROKER 'localhost:9092', SECURITY PROTOCOL PLAINTEXT)",
        ),
        (
            "sink",
            "CREATE SINK materialize.public.sink IN CLUSTER quickstart FROM materialize.public.input INTO KAFKA CONNECTION materialize.public.connection (TOPIC 'completed-health') FORMAT JSON ENVELOPE DEBEZIUM",
        ),
    ] {
        let (id, global_id) = catalog.allocate_user_id_for_test().await.unwrap();
        let item = test_support::parse_item(
            &mut catalog.state().clone(),
            global_id,
            sql,
            &BTreeMap::new(),
        )
        .unwrap();
        let item_name = name(&catalog, item_name);
        transact(
            &mut catalog,
            vec![Op::CreateItem {
                id,
                name: item_name,
                item,
                owner_id: MZ_SYSTEM_ROLE_ID,
            }],
        )
        .await;
        ids.push((id, global_id));
    }
    let input = ids[0].1;
    let (sink_item, sink) = ids[2];
    let mut requirement = catalog.state().maintained_read_requirements()[&sink].clone();
    requirement.frontier = None;
    transact(
        &mut catalog,
        vec![Op::SetReadProtection {
            bounds: vec![],
            requirements: vec![requirement],
        }],
    )
    .await;

    let input_desc = catalog
        .state()
        .try_get_desc_by_global_id(&input)
        .unwrap()
        .into_owned();
    persist
        .register_schema::<SourceData, (), Timestamp, i64>(
            catalog.state().storage_metadata().collection_metadata[&input],
            &input_desc,
            &UnitSchema,
            Diagnostics::for_tests(),
        )
        .await
        .unwrap();
    let mut output = persist
        .open_writer::<SourceData, (), Timestamp, i64>(
            catalog.state().storage_metadata().collection_metadata[&sink],
            Arc::new(KAFKA_PROGRESS_DESC.clone()),
            Arc::new(UnitSchema),
            Diagnostics::for_tests(),
        )
        .await
        .unwrap();
    let store = store(&persist).await;

    let timely = TimelyConfig {
        workers: 1,
        addresses: vec!["127.0.0.1:0".into()],
        ..Default::default()
    };
    let connection_context = catalog.config().connection_context.clone();
    let mut compute = mz_compute::server::serve(
        timely.clone(),
        ComputeRuntimeRole::Solo,
        true,
        &registry,
        Arc::clone(&clients),
        TxnsContext::default(),
        Arc::new(TracingHandle::disabled()),
        ComputeInstanceContext {
            scratch_directory: None,
            worker_core_affinity: false,
            connection_context: connection_context.clone(),
        },
    )
    .await
    .unwrap();
    let mut storage = mz_storage::server::serve_with_replica(
        timely,
        true,
        &registry,
        Arc::clone(&clients),
        TxnsContext::default(),
        Arc::new(TracingHandle::disabled()),
        SYSTEM_TIME.clone(),
        connection_context,
        StorageInstanceContext::new(None, None),
    )
    .await
    .unwrap();
    let instance = mz_catalog::compute_config::replica_instance_config(
        &catalog,
        cluster,
        replica,
        location.clone(),
        clients.cfg().state_version_target(),
    );
    let mut driver = ReplicaEnactment::new(
        compute.take_replica().unwrap(),
        instance,
        incarnation,
        BTreeMap::new(),
        cluster,
        replica,
        &registry,
        Some(storage.take_replica().unwrap()),
    );
    driver
        .configure_status_history(&catalog, persist.clone(), deployment)
        .unwrap();

    let builtin = &*mz_catalog::builtin::MZ_STORAGE_STATUSES_RAW;
    let health_id = catalog
        .get_entry(&catalog.resolve_builtin_storage_collection(builtin))
        .latest_global_id();
    let reader = persist
        .open_leased_reader::<SourceData, (), Timestamp, i64>(
            catalog.state().storage_metadata().collection_metadata[&health_id],
            Arc::new(builtin.desc.clone()),
            Arc::new(UnitSchema),
            Diagnostics::for_tests(),
            false,
        )
        .await
        .unwrap();
    let mut reader = Some(reader);
    let mut current = None;
    let mut rows = BTreeMap::<Row, i64>::new();
    let mut effects = ReplicaEffects::default();

    for phase in 0..3 {
        if phase == 1 {
            // Completion must come from Persist, not the NULL recovery requirement.
            output
                .compare_and_append(
                    Vec::<((SourceData, ()), Timestamp, i64)>::new(),
                    Antichain::from_elem(Timestamp::MIN),
                    Antichain::new(),
                )
                .await
                .unwrap()
                .unwrap();
        } else if phase == 2 {
            let ts = catalog.current_upper().await;
            let elsewhere = catalog.allocate_user_cluster_id(ts).await.unwrap();
            transact(
                &mut catalog,
                vec![Op::CreateCluster {
                    id: elsewhere,
                    name: "elsewhere".into(),
                    introspection_sources: vec![],
                    owner_id: MZ_SYSTEM_ROLE_ID,
                    config: ClusterConfig {
                        variant: ClusterVariant::Unmanaged,
                        workload_class: None,
                    },
                }],
            )
            .await;
            let mut item = catalog.get_entry(&sink_item).item().clone();
            let CatalogItem::Sink(definition) = &mut item else {
                panic!("expected sink");
            };
            definition.cluster_id = elsewhere;
            definition.create_sql = "CREATE SINK materialize.public.sink IN CLUSTER elsewhere FROM materialize.public.input INTO KAFKA CONNECTION materialize.public.connection (TOPIC 'completed-health') FORMAT JSON ENVELOPE DEBEZIUM".into();
            let sink_name = name(&catalog, "sink");
            transact(
                &mut catalog,
                vec![Op::UpdateItem {
                    id: sink_item,
                    name: sink_name,
                    to_item: item,
                }],
            )
            .await;
        }
        let metadata = storage_metadata::resolve(
            &catalog,
            &BTreeSet::from([input, sink]),
            &store,
            BUILD,
            &persist,
            &location,
            Some(wal),
        )
        .await
        .unwrap();
        assert!(metadata.pending.is_empty(), "{:?}", metadata.pending);
        assert_eq!(metadata.uppers[&sink].is_empty(), phase != 0);
        assert_eq!(
            catalog.state().maintained_read_requirements()[&sink].frontier,
            None
        );
        assert!(
            !driver
                .install_sinks(&mut catalog, &mut effects, cluster, BUILD, &metadata)
                .await
                .unwrap()
        );

        // A distinct witness on the same publication queue makes both absence
        // checks meaningful even if the publisher has not run yet.
        let witness = input;
        let at = mz_ore::now::to_datetime(phase + 1);
        driver
            .io
            .storage
            .as_ref()
            .unwrap()
            .history
            .as_ref()
            .unwrap()
            .record(
                StatusEvent {
                    owner: witness,
                    execution: Some(1),
                    history: IntrospectionType::SourceStatusHistory,
                    update: StatusUpdate::new(witness, at, Status::Running),
                },
                true,
            );
        // Subscribing at MIN waits for the first publication to advance upper.
        if let Some(reader) = reader.take() {
            current = Some(
                reader
                    .subscribe(Antichain::from_elem(Timestamp::MIN))
                    .await
                    .unwrap(),
            );
        }
        let current = current.as_mut().unwrap();
        loop {
            for event in current.fetch_next().await {
                if let ListenEvent::Updates(updates) = event {
                    for ((data, ()), _, diff) in updates {
                        *rows.entry(data.0.unwrap()).or_default() += diff;
                    }
                }
            }
            rows.retain(|_, diff| *diff != 0);
            if rows.keys().any(|row| {
                let values = row.unpack();
                values[1].unwrap_str() == witness.to_string()
                    && values[0] == Datum::TimestampTz(at.try_into().unwrap())
            }) {
                break;
            }
        }
        assert!(rows.values().all(|diff| *diff == 1));
        let sink_rows: Vec<_> = rows
            .keys()
            .filter(|row| row.unpack()[1].unwrap_str() == sink.to_string())
            .collect();
        if phase == 1 {
            assert_eq!(sink_rows.len(), 1);
            let values = sink_rows[0].unpack();
            assert_eq!(values[0], Datum::Null);
            assert_eq!(values[2].unwrap_str(), "running");
            assert_eq!(values[5].unwrap_str(), replica.to_string());
            assert_eq!(values[6], Datum::UInt64(deployment));
            assert_eq!(values[7], Datum::UInt64(incarnation));
        } else {
            assert!(sink_rows.is_empty(), "phase {phase}: {sink_rows:?}");
        }
    }
    std::process::exit(0);
}

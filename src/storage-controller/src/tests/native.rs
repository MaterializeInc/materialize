// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use mz_storage_client::statistics::SinkStatisticsUpdate;
use mz_storage_types::connections::string_or_secret::StringOrSecret;
use mz_storage_types::connections::{MySqlConnection, MySqlSslMode};
use mz_storage_types::sources::load_generator::{
    LoadGenerator, LoadGeneratorOutput, LoadGeneratorSourceExportDetails,
};
use mz_storage_types::sources::mysql::{MySqlSourceConnection, MySqlSourceDetails};
use mz_storage_types::sources::{
    LoadGeneratorSourceConnection, SourceEnvelope, SourceExportDetails,
};

use super::*;

const SOURCE: GlobalId = GlobalId::User(10);
const TABLE: GlobalId = GlobalId::User(11);
const SIBLING: GlobalId = GlobalId::User(12);
const SINK: GlobalId = GlobalId::User(20);

async fn native_controller(single_replica: bool) -> (Controller, StorageMetadata) {
    let (mut controller, _, _) = test_controller(true, false).await;
    let cluster = export_description(TABLE).instance_id;
    controller.create_instance(cluster, None);
    let connection = if single_replica {
        GenericSourceConnection::MySql(MySqlSourceConnection {
            connection_id: mz_repr::CatalogItemId::User(1),
            connection: MySqlConnection {
                host: "localhost".into(),
                port: 3306,
                user: StringOrSecret::String("test".into()),
                password: None,
                tunnel: Tunnel::Direct,
                tls_mode: MySqlSslMode::Disabled,
                tls_root_cert: None,
                tls_identity: None,
                aws_connection: None,
            },
            details: MySqlSourceDetails {},
        })
    } else {
        GenericSourceConnection::LoadGenerator(LoadGeneratorSourceConnection {
            load_generator: LoadGenerator::Counter {
                max_cardinality: None,
            },
            tick_micros: None,
            as_of: 0,
            up_to: 0,
        })
    };
    let ingestion = IngestionDescription {
        desc: SourceDesc {
            connection,
            timestamp_interval: Duration::from_secs(1),
        },
        remap_metadata: (),
        source_exports: BTreeMap::new(),
        instance_id: cluster,
        remap_collection_id: SOURCE,
    };
    let mut source = CollectionDescription::for_other(RelationDesc::empty(), None);
    source.data_source = DataSource::Ingestion(ingestion);
    let mut descriptions = vec![(SOURCE, source)];
    if !single_replica {
        for id in [TABLE, SIBLING] {
            let mut table = CollectionDescription::for_other(RelationDesc::empty(), None);
            table.data_source = DataSource::IngestionExport {
                ingestion_id: SOURCE,
                details: SourceExportDetails::LoadGenerator(LoadGeneratorSourceExportDetails {
                    output: LoadGeneratorOutput::Default,
                }),
                data_config: SourceExportDataConfig {
                    encoding: None,
                    envelope: SourceEnvelope::CdcV2,
                },
            };
            descriptions.push((id, table));
        }
        let mut sink = CollectionDescription::for_other(RelationDesc::empty(), None);
        sink.data_source = DataSource::Sink {
            desc: export_description(TABLE),
        };
        descriptions.push((SINK, sink));
    }
    for (id, typ, desc) in [
        (
            GlobalId::System(1),
            IntrospectionType::ShardMapping,
            RelationDesc::builder()
                .with_column("object_id", mz_repr::SqlScalarType::String.nullable(false))
                .with_column("shard_id", mz_repr::SqlScalarType::String.nullable(false))
                .finish(),
        ),
        (
            GlobalId::System(2),
            IntrospectionType::SourceStatusHistory,
            MZ_SOURCE_STATUS_HISTORY_DESC.clone(),
        ),
        (
            GlobalId::System(3),
            IntrospectionType::SinkStatusHistory,
            MZ_SINK_STATUS_HISTORY_DESC.clone(),
        ),
    ] {
        let mut description = CollectionDescription::for_other(desc, None);
        description.data_source = DataSource::Introspection(typ);
        descriptions.push((id, description));
    }
    let metadata = StorageMetadata {
        collection_metadata: descriptions
            .iter()
            .map(|(id, _)| (*id, ShardId::new()))
            .collect(),
        compaction_bounds: descriptions
            .iter()
            .map(|(id, _)| (*id, frontier(0)))
            .collect(),
        ..Default::default()
    };
    controller
        .create_collections_for_bootstrap(&metadata, None, descriptions, &BTreeSet::new())
        .await
        .unwrap();
    (controller, metadata)
}

#[mz_ore::test(tokio::test)]
async fn native_lifecycle_is_inventory_only() {
    let (mut controller, mut metadata) = native_controller(false).await;
    let cluster = export_description(TABLE).instance_id;
    let replica = ReplicaId::User(1);
    controller.register_replica(cluster, replica);
    controller.initialization_complete();
    controller.update_parameters(StorageParameters::default());
    assert!(controller.instances[&cluster].legacy.is_none());
    for id in [SOURCE, TABLE, SIBLING, SINK] {
        assert!(matches!(
            controller.collections[&id].extra_state,
            CollectionStateExtra::Native { .. }
        ));
        assert!(controller.collection_metadata(id).is_ok());
    }
    assert_eq!(
        controller
            .active_ingestion_exports(cluster)
            .copied()
            .collect::<BTreeSet<_>>(),
        BTreeSet::from([SOURCE, TABLE, SIBLING])
    );
    assert!(controller.export(SINK).is_err());
    assert!(controller.is_sink(SINK));

    let DataSource::Ingestion(desc) = &controller.collections[&SOURCE].data_source else {
        panic!("source");
    };
    let mut desc = desc.desc.clone();
    desc.timestamp_interval = Duration::from_secs(2);
    controller
        .alter_ingestion_source_desc(BTreeMap::from([(SOURCE, desc.clone())]))
        .await
        .unwrap();
    let DataSource::Ingestion(current) = &controller.collections[&SOURCE].data_source else {
        panic!("source");
    };
    assert_eq!(current.desc, desc);

    // Native alteration applies the committed definition even before the sink
    // reports progress. Recovery validation and input holds belong to replicas.
    let second_cluster = StorageInstanceId::system(2).unwrap();
    controller.create_instance(second_cluster, None);
    let mut replacement = export_description(SIBLING);
    replacement.instance_id = second_cluster;
    controller
        .alter_export(SINK, replacement.clone())
        .await
        .unwrap();
    assert_eq!(
        controller.collections[&SINK].data_source,
        DataSource::Sink { desc: replacement }
    );
    assert_eq!(
        controller.collections[&SINK].extra_state.instance_id(),
        Some(second_cluster)
    );

    for id in [TABLE, SINK] {
        metadata.collection_metadata.remove(&id);
        metadata.compaction_bounds.remove(&id);
    }
    controller.drop_tables(&metadata, vec![TABLE]).unwrap();
    let DataSource::Ingestion(current) = &controller.collections[&SOURCE].data_source else {
        panic!("source");
    };
    assert_eq!(
        current.source_exports.keys().copied().collect::<Vec<_>>(),
        [SIBLING]
    );
    controller.drop_sinks(&metadata, vec![SINK]).unwrap();
    assert!(controller.dropped_objects.is_empty());
    assert!(controller.check_exists(TABLE).is_err());
    assert!(controller.check_exists(SINK).is_err());
    controller.process().unwrap();
    assert!(controller.instances.values().all(|i| i.legacy.is_none()));
}

fn process_observations(controller: &mut Controller) {
    while let Ok(response) = controller.instance_response_rx.try_recv() {
        controller.stashed_responses.push(response);
    }
    controller.process().unwrap();
}

fn status(controller: &mut Controller, id: GlobalId, replica: ReplicaId, status: Status) {
    controller
        .replica_observations()
        .send(
            replica,
            StorageResponse::StatusUpdate(StatusUpdate::new(
                id,
                mz_ore::now::to_datetime((controller.now)()),
                status,
            )),
        )
        .unwrap();
    process_observations(controller);
}

#[mz_ore::test(tokio::test)]
async fn native_observations_preserve_sibling_hydration() {
    let (mut controller, _) = native_controller(false).await;
    let cluster = export_description(TABLE).instance_id;
    assert!(controller.collection_hydrated(TABLE).unwrap());
    // Consume the zero-replica statuses before introducing replicas.
    controller.process().unwrap();
    let r1 = ReplicaId::User(1);
    let r2 = ReplicaId::User(2);
    controller.register_replica(cluster, r1);
    controller.register_replica(cluster, r2);
    assert!(!controller.collection_hydrated(TABLE).unwrap());
    for id in [SOURCE, TABLE, SIBLING] {
        status(&mut controller, id, r1, Status::Running);
        status(&mut controller, id, r2, Status::Running);
    }
    controller
        .replica_observations()
        .send(r1, StorageResponse::QueryReady)
        .unwrap();
    process_observations(&mut controller);
    assert!(controller.collection_hydrated(TABLE).unwrap());
    assert!(
        !controller
            .collections_hydrated_on_replicas(Some(vec![r1]), &cluster, &BTreeSet::new())
            .unwrap()
    );
    for id in [SOURCE, TABLE, SIBLING] {
        status(&mut controller, id, r1, Status::Running);
    }
    status(&mut controller, TABLE, r1, Status::Stalled);
    let excluded = BTreeSet::from([SOURCE, SIBLING]);
    assert!(
        controller
            .collections_hydrated_on_replicas(Some(vec![r1]), &cluster, &excluded)
            .unwrap()
    );
    status(&mut controller, TABLE, r1, Status::Starting);
    assert!(controller.collection_hydrated(TABLE).unwrap());
    assert!(
        !controller
            .collections_hydrated_on_replicas(Some(vec![r1]), &cluster, &excluded)
            .unwrap()
    );
    assert!(
        controller
            .collections_hydrated_on_replicas(Some(vec![r2]), &cluster, &excluded)
            .unwrap()
    );
    controller.drop_replica(cluster, r2);
    assert!(!controller.collection_hydrated(TABLE).unwrap());
    assert!(controller.collection_hydrated(SIBLING).unwrap());
    status(&mut controller, TABLE, r2, Status::Running);
    assert!(!controller.collection_hydrated(TABLE).unwrap());

    controller
        .replica_observations()
        .send(
            r1,
            StorageResponse::StatisticsUpdates(vec![], vec![SinkStatisticsUpdate::new(SINK)]),
        )
        .unwrap();
    process_observations(&mut controller);
    controller.initialization_complete();
    assert!(
        controller
            .sink_statistics
            .lock()
            .unwrap()
            .contains_key(&(SINK, Some(r1)))
    );
    controller.drop_replica(cluster, r1);
    controller.process().unwrap();
    assert!(controller.collection_hydrated(TABLE).unwrap());
    controller.register_replica(cluster, r1);
    assert!(!controller.collection_hydrated(SIBLING).unwrap());
}

#[mz_ore::test(tokio::test)]
async fn single_replica_source_hydration_follows_lowest_member() {
    let (mut controller, _) = native_controller(true).await;
    let cluster = export_description(SOURCE).instance_id;
    let low = ReplicaId::User(1);
    let high = ReplicaId::User(2);
    controller.register_replica(cluster, high);
    status(&mut controller, SOURCE, high, Status::Running);
    assert!(controller.collection_hydrated(SOURCE).unwrap());
    controller.register_replica(cluster, low);
    assert!(!controller.collection_hydrated(SOURCE).unwrap());
    let excluded = BTreeSet::new();
    assert!(
        controller
            .collections_hydrated_on_replicas(Some(vec![high]), &cluster, &excluded)
            .unwrap()
    );
    assert!(
        !controller
            .collections_hydrated_on_replicas(Some(vec![low]), &cluster, &excluded)
            .unwrap()
    );
    status(&mut controller, SOURCE, low, Status::Running);
    controller.drop_replica(cluster, high);
    assert!(controller.collection_hydrated(SOURCE).unwrap());
    controller.drop_replica(cluster, low);
    assert!(
        controller
            .active_ingestion_exports(cluster)
            .next()
            .is_none()
    );
}

#[mz_ore::test(tokio::test)]
async fn read_only_prewarming_retains_legacy_execution() {
    let (mut controller, _, _) = test_controller(false, true).await;
    let cluster = StorageInstanceId::system(1).unwrap();
    controller.create_instance(cluster, None);
    assert!(controller.instances[&cluster].legacy.is_some());
}

#[mz_ore::test(tokio::test)]
#[should_panic(expected = "native replicas must be registered without transport")]
async fn native_mode_rejects_legacy_transport() {
    let (mut controller, _, _) = test_controller(true, false).await;
    let cluster = StorageInstanceId::system(1).unwrap();
    controller.create_instance(cluster, None);
    controller.connect_replica(
        cluster,
        ReplicaId::User(1),
        ClusterReplicaLocation { ctl_addrs: vec![] },
    );
}

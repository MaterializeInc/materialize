// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::*;
use mz_catalog::durable::objects::serialization::proto::TxnWalShardValue;
use mz_catalog::expr_cache::GlobalExpressions;
use mz_compute_types::dataflows::DataflowDescription;
use mz_compute_types::sinks::{ComputeSinkDesc, MaterializedViewSinkConnection};
use mz_persist_types::codec_impls::UnitSchema;
use mz_repr::role_id::RoleId;
use mz_repr::{CatalogItemId, SqlScalarType};
use mz_sql::names::SchemaId as CatalogSchemaId;

const BUILD: &str = "1.0.0";

fn item(snapshot: &mut Snapshot, id: u64, sql: &str, aliases: &[u64]) {
    let mut version = RelationVersion::root();
    let extra_versions = aliases
        .iter()
        .map(|id| {
            version = version.bump();
            (version, GlobalId::User(*id))
        })
        .collect();
    let (key, value) = objects::Item {
        id: CatalogItemId::User(id),
        oid: 1,
        global_id: GlobalId::User(id),
        schema_id: CatalogSchemaId::User(1),
        name: format!("item{id}"),
        create_sql: sql.into(),
        owner_id: RoleId::User(1),
        privileges: Vec::new(),
        extra_versions,
        ephemeral_owner_session: None,
    }
    .into_key_value();
    snapshot.items.insert(key.into_proto(), value.into_proto());
}

fn mapping(snapshot: &mut Snapshot, id: u64, shard: ShardId) {
    let (key, value) = objects::StorageCollectionMetadata {
        id: GlobalId::User(id),
        shard,
    }
    .into_key_value();
    snapshot
        .storage_collection_metadata
        .insert(key.into_proto(), value.into_proto());
}

fn select(snapshot: &mut Snapshot, id: u64, revision: Uuid) {
    let (key, value) = objects::WrittenPlan {
        id: GlobalId::User(id),
        build_version: BUILD.into(),
        revision,
    }
    .into_key_value();
    snapshot
        .written_plans
        .insert(key.into_proto(), value.into_proto());
}

async fn store(persist: &PersistClient) -> ExpressionCacheHandle {
    ExpressionCacheHandle::open_plan_store(
        BUILD.parse().expect("valid metadata fixture"),
        persist,
        ShardId::new(),
    )
    .await
}

async fn register(persist: &PersistClient, shard: ShardId, desc: &RelationDesc) {
    assert_eq!(
        persist
            .register_schema::<SourceData, (), Timestamp, StorageDiff>(
                shard,
                desc,
                &UnitSchema,
                diagnostics(GlobalId::User(1)),
            )
            .await
            .expect("valid metadata fixture"),
        Some(RelationVersion::root().into())
    );
}

#[mz_ore::test(tokio::test)]
async fn table_aliases_use_exact_schema_versions() {
    let persist = PersistClient::new_for_tests().await;
    let store = store(&persist).await;
    let shard = ShardId::new();
    let wal = ShardId::new();
    let old = RelationDesc::empty();
    let new = RelationDesc::builder()
        .with_column("a", SqlScalarType::Int64.nullable(true))
        .finish();
    register(&persist, shard, &old).await;
    let evolved = persist
        .compare_and_evolve_schema::<SourceData, (), Timestamp, StorageDiff>(
            shard,
            RelationVersion::root().into(),
            &new,
            &UnitSchema,
            diagnostics(GlobalId::User(1)),
        )
        .await
        .expect("valid metadata fixture");
    assert!(matches!(
        evolved,
        mz_persist_client::schema::CaESchema::Ok(_)
    ));
    // The WAL is ahead of the data shard. Both table aliases must report WAL progress.
    let mut writer = persist
        .open_writer::<SourceData, (), Timestamp, StorageDiff>(
            wal,
            std::sync::Arc::new(old.clone()),
            std::sync::Arc::new(UnitSchema),
            diagnostics(GlobalId::User(1)),
        )
        .await
        .expect("valid metadata fixture");
    let updates: Vec<((SourceData, ()), Timestamp, StorageDiff)> = Vec::new();
    writer
        .compare_and_append(
            updates,
            Antichain::from_elem(Timestamp::from(0)),
            Antichain::from_elem(Timestamp::from(9)),
        )
        .await
        .expect("valid metadata fixture")
        .expect("valid metadata fixture");
    writer.expire().await;
    let mut snapshot = Snapshot::empty();
    item(&mut snapshot, 1, "CREATE TABLE t (a bigint)", &[2, 3]);
    for id in [1, 2, 3] {
        mapping(&mut snapshot, id, shard);
    }
    snapshot.txn_wal_shard.insert(
        (),
        TxnWalShardValue {
            shard: wal.to_string(),
        },
    );
    let result = resolve(
        &snapshot,
        &BTreeSet::from([GlobalId::User(1), GlobalId::User(2), GlobalId::User(3)]),
        &store,
        BUILD,
        &persist,
        &PersistLocation::new_in_mem(),
    )
    .await
    .expect("valid metadata fixture");
    assert_eq!(result.metadata[&GlobalId::User(1)].relation_desc, old);
    assert_eq!(result.metadata[&GlobalId::User(2)].relation_desc, new);
    assert_eq!(
        result.pending,
        BTreeMap::from([(
            GlobalId::User(3),
            Pending::Schema(Some(RelationVersion::root().bump().bump().into()))
        )])
    );
    assert_eq!(
        result.uppers,
        BTreeMap::from([
            (GlobalId::User(1), Antichain::from_elem(Timestamp::from(9))),
            (GlobalId::User(2), Antichain::from_elem(Timestamp::from(9))),
        ])
    );
}

#[mz_ore::test(tokio::test)]
async fn mv_alias_uses_selected_cross_cluster_writer_not_persist_schema() {
    let persist = PersistClient::new_for_tests().await;
    let store = store(&persist).await;
    let shard = ShardId::new();
    register(&persist, shard, &RelationDesc::empty()).await;
    let desc = RelationDesc::builder()
        .with_column("a", SqlScalarType::String.nullable(false))
        .finish();
    let writer = GlobalId::User(3);
    let version = RelationVersion::root().bump().bump();
    let revision = Uuid::new_v4();
    let mut snapshot = Snapshot::empty();
    // A consumer only asks for the retired alias. The producer's placement and
    // latest output must not constrain lookup to the consumer's local members.
    item(
        &mut snapshot,
        1,
        "CREATE MATERIALIZED VIEW mv IN CLUSTER [u99] AS SELECT 1",
        &[2, 3],
    );
    mapping(&mut snapshot, 1, shard);
    let wanted = BTreeSet::from([GlobalId::User(1)]);
    let location = PersistLocation::new_in_mem();
    let result = resolve(&snapshot, &wanted, &store, BUILD, &persist, &location)
        .await
        .expect("valid metadata fixture");
    assert_eq!(
        result.pending[&GlobalId::User(1)],
        Pending::ProducerSelection(writer)
    );
    select(&mut snapshot, 3, revision);
    let result = resolve(&snapshot, &wanted, &store, BUILD, &persist, &location)
        .await
        .expect("valid metadata fixture");
    assert_eq!(
        result.pending[&GlobalId::User(1)],
        Pending::ProducerBytes(writer, revision)
    );
    let mut plan = GlobalExpressions {
        global_mir: DataflowDescription::new("mv".into()),
        physical_plan: DataflowDescription::new("mv".into()),
        dataflow_metainfos: Default::default(),
        optimizer_features: Default::default(),
        item_version: version,
    };
    plan.physical_plan.sink_exports.insert(
        writer,
        ComputeSinkDesc {
            from: GlobalId::Transient(1),
            from_desc: desc.clone(),
            connection: ComputeSinkConnection::MaterializedView(MaterializedViewSinkConnection {
                value_desc: desc.clone(),
                storage_metadata: (),
            }),
            with_snapshot: true,
            up_to: Antichain::new(),
            non_null_assertions: Vec::new(),
            refresh_schedule: None,
        },
    );
    store
        .write_plans(vec![(writer, revision, plan)])
        .await
        .expect("valid metadata fixture");
    let result = resolve(&snapshot, &wanted, &store, BUILD, &persist, &location)
        .await
        .expect("valid metadata fixture");
    assert!(result.pending.is_empty());
    assert_eq!(
        result.metadata[&GlobalId::User(1)],
        CollectionMetadata {
            persist_location: location,
            data_shard: shard,
            relation_desc: desc,
            txns_shard: None,
        }
    );
    assert_eq!(
        result.uppers[&GlobalId::User(1)],
        Antichain::from_elem(Timestamp::from(0))
    );
}

#[mz_ore::test(tokio::test)]
async fn missing_metadata_and_orphan_mappings_are_pending() {
    let persist = PersistClient::new_for_tests().await;
    let store = store(&persist).await;
    let mut snapshot = Snapshot::empty();
    item(&mut snapshot, 1, "CREATE TABLE t (a bigint)", &[]);
    item(&mut snapshot, 2, "CREATE TABLE t2 (a bigint)", &[]);
    mapping(&mut snapshot, 2, ShardId::new());
    item(
        &mut snapshot,
        3,
        "CREATE SUBSOURCE progress (a bigint) WITH (PROGRESS)",
        &[],
    );
    mapping(&mut snapshot, 3, ShardId::new());
    mapping(&mut snapshot, 4, ShardId::new());
    let result = resolve(
        &snapshot,
        &(1..=4).map(GlobalId::User).collect(),
        &store,
        BUILD,
        &persist,
        &PersistLocation::new_in_mem(),
    )
    .await
    .expect("valid metadata fixture");
    assert!(result.metadata.is_empty());
    assert!(result.uppers.is_empty());
    assert_eq!(
        result.pending,
        BTreeMap::from([
            (GlobalId::User(1), Pending::ShardMapping),
            (GlobalId::User(2), Pending::TxnWalShard),
            (GlobalId::User(3), Pending::Schema(None)),
            (GlobalId::User(4), Pending::Definition),
        ])
    );
}

#[mz_ore::test(tokio::test)]
async fn builtin_descriptors_and_wal_participation_come_from_build_definitions() {
    let persist = PersistClient::new_for_tests().await;
    let store = store(&persist).await;
    let mut snapshot = Snapshot::empty();
    let wal = ShardId::new();
    snapshot.txn_wal_shard.insert(
        (),
        TxnWalShardValue {
            shard: wal.to_string(),
        },
    );
    let table = BUILTINS::iter()
        .find(|builtin| matches!(builtin, Builtin::Table(_)))
        .expect("valid metadata fixture");
    let source = BUILTINS::iter()
        .find(|builtin| matches!(builtin, Builtin::Source(_)))
        .expect("valid metadata fixture");
    let location = PersistLocation::new_in_mem();
    let mut expected = BTreeMap::new();
    for (number, builtin) in [(1, table), (2, source)] {
        let id = GlobalId::System(number);
        let (desc, transactional) = match builtin {
            Builtin::Table(table) => (table.desc.clone(), true),
            Builtin::Source(source) => (source.desc.clone(), false),
            _ => unreachable!("selected table and source"),
        };
        let (key, value) = objects::SystemObjectMapping {
            description: objects::SystemObjectDescription {
                schema_name: builtin.schema().into(),
                object_name: builtin.name().into(),
                object_type: builtin.catalog_item_type(),
            },
            unique_identifier: objects::SystemObjectUniqueIdentifier {
                catalog_id: CatalogItemId::System(number),
                global_id: id,
                fingerprint: String::new(),
            },
        }
        .into_key_value();
        snapshot
            .system_object_mappings
            .insert(key.into_proto(), value.into_proto());
        let shard = ShardId::new();
        let (key, value) = objects::StorageCollectionMetadata { id, shard }.into_key_value();
        snapshot
            .storage_collection_metadata
            .insert(key.into_proto(), value.into_proto());
        expected.insert(
            id,
            CollectionMetadata {
                persist_location: location.clone(),
                data_shard: shard,
                relation_desc: desc,
                txns_shard: transactional.then_some(wal),
            },
        );
    }
    // Neither shard has a Persist schema. Builtin descriptors do not require one.
    let wanted = expected.keys().copied().collect();
    let result = resolve(&snapshot, &wanted, &store, BUILD, &persist, &location)
        .await
        .expect("valid metadata fixture");
    assert!(result.pending.is_empty());
    assert_eq!(result.metadata, expected);
}

// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::super::tests::{BUILD, committed, create_table, debug_catalog, name, store, transact};
use super::*;
use mz_catalog::builtin::{BUILTINS, Builtin};
use mz_catalog::catalog::Op;
use mz_catalog::durable::Transaction;
use mz_catalog::expr_cache::GlobalExpressions;
use mz_compute_types::dataflows::DataflowDescription;
use mz_compute_types::sinks::{ComputeSinkDesc, MaterializedViewSinkConnection};
use mz_persist_types::codec_impls::UnitSchema;
use mz_repr::SqlScalarType;
use mz_repr::role_id::RoleId;
use mz_storage_client::controller::StorageTxn;

// Read the fixture's bootstrap identity from durable storage. Production obtains
// the same immutable identity from OpenCommittedCatalog.
async fn resolve(
    catalog: &Catalog,
    wanted: &BTreeSet<GlobalId>,
    store: &ExpressionCacheHandle,
    build: &str,
    persist: &PersistClient,
    location: &PersistLocation,
) -> anyhow::Result<Resolution> {
    let wal = catalog
        .storage()
        .await
        .transaction()
        .await?
        .get_txn_wal_shard();
    super::resolve(catalog, wanted, store, build, persist, location, wal).await
}

async fn durable(catalog: &Catalog, change: impl FnOnce(&mut Transaction<'_>)) {
    let mut storage = catalog.storage().await;
    let mut tx = storage.transaction().await.expect("durable transaction");
    change(&mut tx);
    let _ = tx.get_and_commit_op_updates();
    let ts = tx.upper();
    tx.commit(ts).await.expect("commit fixture");
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
async fn cold_source_metadata_does_not_require_adapter_registration() {
    let persist = PersistClient::new_for_tests().await;
    let store = store(&persist).await;
    let mut writer = debug_catalog(&persist, Some(ShardId::new())).await;
    let (item_id, global_id) = writer
        .allocate_user_id_for_test()
        .await
        .expect("source IDs");
    let mut state = writer.state().clone();
    let cluster = writer
        .user_clusters()
        .next()
        .expect("bootstrap cluster")
        .name
        .clone();
    let item = mz_catalog::catalog::test_support::parse_item(
        &mut state,
        global_id,
        &format!("CREATE SOURCE materialize.public.cold_source IN CLUSTER {cluster} FROM LOAD GENERATOR COUNTER"),
        &BTreeMap::new(),
    ).expect("source definition");
    let CatalogItem::Source(source) = &item else {
        panic!("source plan");
    };
    let expected_desc = source.desc.clone();
    let source_name = name(&writer, "cold_source");
    transact(
        &mut writer,
        vec![Op::CreateItem {
            id: item_id,
            name: source_name,
            item,
            owner_id: RoleId::System(1),
        }],
    )
    .await;
    let (catalog, _) = committed(&writer, &persist).await;
    drop(writer);
    let result = resolve(
        &catalog,
        &BTreeSet::from([global_id]),
        &store,
        BUILD,
        &persist,
        &PersistLocation::new_in_mem(),
    )
    .await
    .expect("cold source metadata");
    assert!(result.pending.is_empty(), "{:?}", result.pending);
    let metadata = &result.metadata[&global_id];
    assert_eq!(metadata.relation_desc, expected_desc);
    assert_eq!(metadata.txns_shard, None);
    assert!(
        persist
            .latest_schema::<SourceData, (), Timestamp, StorageDiff>(
                metadata.data_shard,
                diagnostics(global_id)
            )
            .await
            .expect("schema observation")
            .is_none()
    );
}

#[mz_ore::test(tokio::test)]
async fn table_aliases_use_exact_schema_versions_and_wal_upper() {
    let persist = PersistClient::new_for_tests().await;
    let store = store(&persist).await;
    let wal = ShardId::new();
    let mut catalog = debug_catalog(&persist, Some(wal)).await;
    let (item, root) = create_table(&mut catalog, "versioned_table").await;
    let mut aliases = vec![root];
    for column in ["a", "b"] {
        let (_, new_global_id) = catalog.allocate_user_id_for_test().await.expect("alias ID");
        transact(
            &mut catalog,
            vec![Op::AlterAddColumn {
                id: item,
                new_global_id,
                name: column.into(),
                typ: SqlScalarType::Int64.nullable(true),
                sql: mz_sql_parser::parser::parse_data_type("bigint").expect("type"),
            }],
        )
        .await;
        aliases.push(new_global_id);
    }
    let shard = catalog.state().storage_metadata().collection_metadata[&root];
    for alias in &aliases {
        assert_eq!(
            catalog.state().storage_metadata().collection_metadata[alias],
            shard
        );
    }
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
            diagnostics(root),
        )
        .await
        .expect("evolve schema");
    assert!(matches!(
        evolved,
        mz_persist_client::schema::CaESchema::Ok(_)
    ));
    // Only the WAL advances. Observing the data shard instead would return zero.
    let mut writer = persist
        .open_writer::<SourceData, (), Timestamp, StorageDiff>(
            wal,
            std::sync::Arc::new(old.clone()),
            std::sync::Arc::new(UnitSchema),
            diagnostics(root),
        )
        .await
        .expect("WAL writer");
    writer
        .compare_and_append(
            Vec::<((SourceData, ()), Timestamp, StorageDiff)>::new(),
            Antichain::from_elem(Timestamp::from(0)),
            Antichain::from_elem(Timestamp::from(9)),
        )
        .await
        .expect("append")
        .expect("upper");
    writer.expire().await;

    let result = resolve(
        &catalog,
        &aliases.iter().copied().collect(),
        &store,
        BUILD,
        &persist,
        &PersistLocation::new_in_mem(),
    )
    .await
    .expect("resolve tables");
    assert_eq!(result.metadata[&root].relation_desc, old);
    assert_eq!(result.metadata[&aliases[1]].relation_desc, new);
    assert_eq!(
        result.pending,
        BTreeMap::from([(
            aliases[2],
            Pending::Schema(Some(RelationVersion::root().bump().bump().into())),
        )])
    );
    assert_eq!(
        result.uppers,
        BTreeMap::from([
            (root, Antichain::from_elem(Timestamp::from(9))),
            (aliases[1], Antichain::from_elem(Timestamp::from(9))),
        ])
    );
    for metadata in result.metadata.values() {
        assert_eq!(metadata.data_shard, shard);
        assert_eq!(metadata.txns_shard, Some(wal));
    }
    catalog.expire().await;
}

#[mz_ore::test(tokio::test)]
async fn mv_alias_uses_selected_cross_cluster_writer_not_persist_schema() {
    let persist = PersistClient::new_for_tests().await;
    let store = store(&persist).await;
    let writer_catalog = debug_catalog(&persist, None).await;
    let (item, alias) = writer_catalog
        .allocate_user_id_for_test()
        .await
        .expect("MV IDs");
    let (_, writer) = writer_catalog
        .allocate_user_id_for_test()
        .await
        .expect("writer ID");
    let producer_cluster = writer_catalog
        .user_clusters()
        .next()
        .expect("producer cluster")
        .id;
    let consumer_cluster = writer_catalog
        .clusters()
        .find(|c| c.id != producer_cluster)
        .expect("other cluster")
        .id;
    let schema = name(&writer_catalog, "producer")
        .qualifiers
        .schema_spec
        .into();
    let version = RelationVersion::root().bump();
    let shard = ShardId::new();
    let writer_shard = ShardId::new();
    durable(&writer_catalog, |tx| {
        tx.insert_user_item(
            item,
            alias,
            schema,
            "producer",
            format!("CREATE MATERIALIZED VIEW materialize.public.producer IN CLUSTER [{producer_cluster}] AS SELECT 1 AS a"),
            RoleId::System(1),
            vec![],
            &Default::default(),
            BTreeMap::from([(version, writer)]),
            None,
        ).expect("durable MV");
        tx.insert_collection_metadata(BTreeMap::from([(alias, shard), (writer, writer_shard)])).expect("MV shards");
    }).await;
    let (mut catalog, initial) = committed(&writer_catalog, &persist).await;
    let CatalogItem::MaterializedView(mv) = catalog.get_entry(&item).item() else {
        panic!("native MV");
    };
    assert_eq!(mv.cluster_id, producer_cluster);
    assert_eq!(mv.global_id_writes(), writer);
    let mut producer = super::super::ReplicaEffects::default();
    super::super::absorb_updates(
        &mut producer,
        &catalog,
        producer_cluster,
        BUILD,
        initial.clone(),
    );
    producer
        .observe_plans(
            &catalog,
            producer_cluster,
            mz_controller_types::ReplicaId::User(1),
            &store,
            BUILD,
        )
        .await
        .expect("producer inventory");
    assert!(producer.pending.contains(&item));
    let mut consumer = super::super::ReplicaEffects::default();
    super::super::absorb_updates(&mut consumer, &catalog, consumer_cluster, BUILD, initial);
    consumer
        .observe_plans(
            &catalog,
            consumer_cluster,
            mz_controller_types::ReplicaId::User(1),
            &store,
            BUILD,
        )
        .await
        .expect("consumer inventory");
    assert!(!consumer.pending.contains(&item));
    assert!(!consumer.selected.contains_key(&item));
    // The alias is readable even though its producer is not a local compute member.
    register(&persist, shard, &RelationDesc::empty()).await;
    let wanted = BTreeSet::from([alias]);
    let location = PersistLocation::new_in_mem();
    let result = resolve(&catalog, &wanted, &store, BUILD, &persist, &location)
        .await
        .expect("selection pending");
    assert_eq!(
        result.pending,
        BTreeMap::from([(alias, Pending::ProducerSelection(writer))])
    );

    let revision = Uuid::new_v4();
    durable(&writer_catalog, |tx| {
        tx.set_written_plan(writer, BUILD, Some(revision))
            .expect("select writer")
    })
    .await;
    catalog
        .sync_to_current_updates()
        .await
        .expect("native selection sync");
    let result = resolve(&catalog, &wanted, &store, BUILD, &persist, &location)
        .await
        .expect("bytes pending");
    assert_eq!(
        result.pending,
        BTreeMap::from([(alias, Pending::ProducerBytes(writer, revision))])
    );
    let desc = RelationDesc::builder()
        .with_column("a", SqlScalarType::String.nullable(false))
        .finish();
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
            non_null_assertions: vec![],
            refresh_schedule: None,
        },
    );
    store
        .write_plans(vec![(writer, revision, plan)])
        .await
        .expect("writer bytes");
    producer
        .observe_plans(
            &catalog,
            producer_cluster,
            mz_controller_types::ReplicaId::User(1),
            &store,
            BUILD,
        )
        .await
        .expect("observe selected producer");
    assert!(!producer.pending.contains(&item));
    let (selected_id, selected_revision, selected_plan) = &producer.selected[&item];
    assert_eq!(*selected_id, writer);
    assert_eq!(*selected_revision, revision);
    assert_eq!(selected_plan.item_version, version);
    let result = resolve(&catalog, &wanted, &store, BUILD, &persist, &location)
        .await
        .expect("resolve alias");
    assert!(result.pending.is_empty());
    assert_eq!(
        result.metadata,
        BTreeMap::from([(
            alias,
            CollectionMetadata {
                persist_location: location,
                data_shard: shard,
                relation_desc: desc,
                txns_shard: None,
            }
        )])
    );
    assert_eq!(
        result.uppers,
        BTreeMap::from([(alias, Antichain::from_elem(Timestamp::from(0)))])
    );
    catalog.expire().await;
    writer_catalog.expire().await;
}

#[mz_ore::test(tokio::test)]
async fn missing_wal_identity_remains_pending() {
    let persist = PersistClient::new_for_tests().await;
    let store = store(&persist).await;
    let mut writer = debug_catalog(&persist, None).await;
    let (_, table) = create_table(&mut writer, "no_wal").await;
    let shard = writer.state().storage_metadata().collection_metadata[&table];
    register(&persist, shard, &RelationDesc::empty()).await;
    let result = resolve(
        &writer,
        &BTreeSet::from([table]),
        &store,
        BUILD,
        &persist,
        &PersistLocation::new_in_mem(),
    )
    .await
    .expect("WAL pending");
    assert_eq!(
        result.pending,
        BTreeMap::from([(table, Pending::TxnWalShard)])
    );
    assert!(result.metadata.is_empty());
    assert!(result.uppers.is_empty());
    writer.expire().await;
}

#[mz_ore::test(tokio::test)]
async fn missing_definitions_mappings_and_schemas_remain_pending() {
    let persist = PersistClient::new_for_tests().await;
    let store = store(&persist).await;
    let mut writer = debug_catalog(&persist, Some(ShardId::new())).await;
    let (_, unmapped) = create_table(&mut writer, "unmapped").await;
    let (_, no_schema) = create_table(&mut writer, "no_schema").await;
    let (_, orphan) = writer.allocate_user_id_for_test().await.expect("orphan ID");
    durable(&writer, |tx| {
        tx.delete_collection_metadata(BTreeSet::from([unmapped]));
        tx.insert_collection_metadata(BTreeMap::from([(orphan, ShardId::new())]))
            .expect("orphan mapping");
    })
    .await;
    let (catalog, _) = committed(&writer, &persist).await;
    let result = resolve(
        &catalog,
        &BTreeSet::from([unmapped, no_schema, orphan]),
        &store,
        BUILD,
        &persist,
        &PersistLocation::new_in_mem(),
    )
    .await
    .expect("pending metadata");
    assert!(result.metadata.is_empty());
    assert!(result.uppers.is_empty());
    assert_eq!(
        result.pending,
        BTreeMap::from([
            (unmapped, Pending::ShardMapping),
            (
                no_schema,
                Pending::Schema(Some(RelationVersion::root().into()))
            ),
            (orphan, Pending::Definition),
        ])
    );
    catalog.expire().await;
    writer.expire().await;
}

#[mz_ore::test(tokio::test)]
async fn builtin_descriptors_and_wal_ownership_come_from_native_catalog() {
    let persist = PersistClient::new_for_tests().await;
    let store = store(&persist).await;
    let wal = ShardId::new();
    let writer = debug_catalog(&persist, Some(wal)).await;
    let table = BUILTINS::iter()
        .find(|b| matches!(b, Builtin::Table(_)))
        .expect("builtin table");
    let source = BUILTINS::iter()
        .find(|b| matches!(b, Builtin::Source(_)))
        .expect("builtin source");
    let location = PersistLocation::new_in_mem();
    let mut expected = BTreeMap::new();
    let mut mappings = BTreeMap::new();
    for builtin in [table, source] {
        let item = writer.state().resolve_builtin_object(builtin);
        let id = writer.get_entry(&item).latest_global_id();
        let (desc, transactional) = match builtin {
            Builtin::Table(table) => (table.desc.clone(), true),
            Builtin::Source(source) => (source.desc.clone(), false),
            _ => unreachable!("selected table and source"),
        };
        let shard = match writer
            .state()
            .storage_metadata()
            .collection_metadata
            .get(&id)
        {
            Some(shard) => *shard,
            None => {
                let shard = ShardId::new();
                mappings.insert(id, shard);
                shard
            }
        };
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
    durable(&writer, |tx| {
        tx.insert_collection_metadata(mappings)
            .expect("builtin mappings");
    })
    .await;
    let (catalog, _) = committed(&writer, &persist).await;
    // Neither shard has a Persist schema. Builtin descriptors must suffice.
    let result = resolve(
        &catalog,
        &expected.keys().copied().collect(),
        &store,
        BUILD,
        &persist,
        &location,
    )
    .await
    .expect("builtin metadata");
    assert!(result.pending.is_empty());
    assert_eq!(result.metadata, expected);
    assert_eq!(
        result.uppers.keys().collect::<Vec<_>>(),
        result.metadata.keys().collect::<Vec<_>>()
    );
    catalog.expire().await;
    writer.expire().await;
}

// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::*;
use mz_catalog::SYSTEM_CONN_ID;
use mz_catalog::builtin::{Builtin, MZ_TABLES_IND};
use mz_catalog::catalog::{DropObjectInfo, Op};
use mz_catalog::durable::{TestCatalogStateBuilder, test_bootstrap_args};
use mz_catalog::memory::objects::{Table, TableDataSource};
use mz_compute_types::dataflows::{DataflowDescription, IndexDesc};
use mz_persist_client::{PersistClient, ShardId};
use mz_repr::role_id::RoleId;
use mz_repr::{RelationDesc, ReprRelationType, VersionedRelationDesc};
use mz_sql::names::{ItemQualifiers, QualifiedItemName, ResolvedDatabaseSpecifier, ResolvedIds};

pub(super) const BUILD: &str = "1.0.0";

pub(super) async fn debug_catalog(persist: &PersistClient) -> Catalog {
    let organization = Uuid::new_v4();
    let bootstrap = test_bootstrap_args();
    let storage = TestCatalogStateBuilder::new(persist.clone())
        .with_organization_id(organization)
        .with_default_deploy_generation()
        .unwrap_build()
        .await
        .open(mz_ore::now::SYSTEM_TIME().into(), &bootstrap)
        .await
        .expect("initialize durable catalog");
    Box::pin(Catalog::open_debug_catalog_inner(
        persist.clone(),
        storage,
        mz_ore::now::SYSTEM_TIME.clone(),
        Some(
            format!("local-az1-{organization}-0")
                .parse()
                .expect("environment"),
        ),
        &mz_build_info::DUMMY_BUILD_INFO,
        BTreeMap::new(),
        &bootstrap,
        None,
        None,
    ))
    .await
    .expect("open native catalog")
}

pub(super) async fn committed(
    writer: &Catalog,
    persist: &PersistClient,
) -> (Catalog, Vec<ParsedStateUpdate>) {
    let config = writer.config();
    let storage = TestCatalogStateBuilder::new(persist.clone())
        .with_organization_id(config.environment_id.organization_id())
        .with_default_deploy_generation()
        .unwrap_build()
        .await
        .join()
        .await
        .expect("join native catalog");
    let opened = Box::pin(Catalog::open_committed(
        writer.replica_config().into_state(
            config.build_info,
            config.environment_id.clone(),
            config.connection_context.clone(),
            persist.clone(),
        ),
        storage,
    ))
    .await
    .expect("reconstruct committed catalog");
    (opened.catalog, opened.initial_updates)
}

pub(super) fn name(catalog: &Catalog, item: &str) -> QualifiedItemName {
    let database_spec = ResolvedDatabaseSpecifier::Id(
        catalog
            .resolve_database("materialize")
            .expect("database")
            .id,
    );
    let schema = catalog
        .resolve_schema_in_database(&database_spec, "public", &SYSTEM_CONN_ID)
        .expect("schema");
    QualifiedItemName {
        qualifiers: ItemQualifiers {
            database_spec,
            schema_spec: schema.id.clone(),
        },
        item: item.into(),
    }
}

pub(super) async fn transact(catalog: &mut Catalog, ops: Vec<Op>) -> Vec<ParsedStateUpdate> {
    let ts = catalog.current_upper().await;
    catalog
        .transact(None, ts, None, ops)
        .await
        .expect("native catalog transaction")
        .catalog_updates
}

pub(super) async fn create_table(
    catalog: &mut Catalog,
    table_name: &str,
) -> (CatalogItemId, GlobalId) {
    let (id, global_id) = catalog.allocate_user_id_for_test().await.expect("IDs");
    let op = Op::CreateItem {
        id,
        name: name(catalog, table_name),
        item: CatalogItem::Table(Table {
            create_sql: Some(format!("CREATE TABLE materialize.public.{table_name} ()")),
            desc: VersionedRelationDesc::new(RelationDesc::empty()),
            collections: BTreeMap::from([(RelationVersion::root(), global_id)]),
            conn_id: None,
            resolved_ids: ResolvedIds::empty(),
            custom_logical_compaction_window: None,
            is_retained_metrics_object: false,
            data_source: TableDataSource::TableWrites { defaults: vec![] },
        }),
        owner_id: RoleId::System(1),
    };
    transact(catalog, vec![op]).await;
    (id, global_id)
}

pub(super) async fn store(persist: &PersistClient) -> ExpressionCacheHandle {
    ExpressionCacheHandle::open_plan_store(BUILD.parse().expect("build"), persist, ShardId::new())
        .await
}

fn index_plan(id: GlobalId, on: GlobalId) -> GlobalExpressions {
    let mut plan = GlobalExpressions {
        global_mir: DataflowDescription::new("index".into()),
        physical_plan: DataflowDescription::new("index".into()),
        dataflow_metainfos: Default::default(),
        optimizer_features: Default::default(),
        item_version: RelationVersion::root(),
    };
    plan.physical_plan.index_exports.insert(
        id,
        (
            IndexDesc {
                on_id: on,
                key: vec![],
            },
            ReprRelationType::empty(),
        ),
    );
    plan
}

#[mz_ore::test(tokio::test)]
async fn native_bootstrap_updates_and_selection_retry() {
    let persist = PersistClient::new_for_tests().await;
    let store = store(&persist).await;
    let mut writer = debug_catalog(&persist).await;
    let cluster = writer.user_clusters().next().expect("user cluster").id;
    let replica = ReplicaId::User(1);
    let builtin =
        writer
            .state()
            .resolve_builtin_object(&Builtin::<mz_sql::catalog::IdReference>::Index(
                &MZ_TABLES_IND,
            ));
    let CatalogItem::Index(mut index) = writer.get_entry(&builtin).item().clone() else {
        panic!("builtin index");
    };
    let builtin_cluster = index.cluster_id;
    let on = index.on;
    let (id, global_id) = writer.allocate_user_id_for_test().await.expect("IDs");
    index.global_id = global_id;
    index.cluster_id = cluster;
    index.create_sql = format!(
        "CREATE INDEX materialize.public.follower_index IN CLUSTER [{cluster}] ON mz_catalog.mz_tables (schema_id)"
    );
    let op = Op::CreateItem {
        id,
        name: name(&writer, "follower_index"),
        item: CatalogItem::Index(index),
        owner_id: RoleId::System(1),
    };
    transact(&mut writer, vec![op]).await;
    let (mut follower, initial) = committed(&writer, &persist).await;
    let mut effects = ReplicaEffects::default();
    absorb_updates(&mut effects, &follower, cluster, BUILD, initial.clone());
    effects
        .observe_plans(&follower, cluster, replica, &store, BUILD)
        .await
        .expect("observe");
    assert_eq!(effects.pending, BTreeSet::from([id]));
    assert!(effects.selected.is_empty());

    // Bootstrap includes build-defined cluster membership, not just durable user items.
    let mut builtins = ReplicaEffects::default();
    absorb_updates(&mut builtins, &follower, builtin_cluster, BUILD, initial);
    builtins
        .observe_plans(&follower, builtin_cluster, replica, &store, BUILD)
        .await
        .expect("observe builtins");
    assert!(builtins.pending.contains(&builtin));
    assert!(!builtins.pending.contains(&id));

    let revision = Uuid::new_v4();
    transact(
        &mut writer,
        vec![Op::SetWrittenPlan {
            id: global_id,
            build_version: BUILD.into(),
            expected_revision: None,
            revision: Some(revision),
            imports: BTreeSet::new(),
        }],
    )
    .await;
    let (_, updates) = follower
        .sync_to_current_updates()
        .await
        .expect("native sync");
    assert!(!updates.is_empty());
    absorb_updates(&mut effects, &follower, cluster, BUILD, updates);
    effects
        .observe_plans(&follower, cluster, replica, &store, BUILD)
        .await
        .expect("wait for bytes");
    assert_eq!(effects.pending, BTreeSet::from([id]));
    assert!(effects.selected.is_empty());

    let plan = index_plan(global_id, on);
    store
        .write_plans(vec![(global_id, revision, plan.clone())])
        .await
        .expect("publish bytes");
    // Retry does not require another catalog update.
    effects
        .observe_plans(&follower, cluster, replica, &store, BUILD)
        .await
        .expect("retry");
    assert!(effects.pending.is_empty());
    assert_eq!(effects.selected[&id], (global_id, revision, plan));

    let replacement = Uuid::new_v4();
    transact(
        &mut writer,
        vec![Op::SetWrittenPlan {
            id: global_id,
            build_version: BUILD.into(),
            expected_revision: Some(revision),
            revision: Some(replacement),
            imports: BTreeSet::new(),
        }],
    )
    .await;
    let (_, updates) = follower
        .sync_to_current_updates()
        .await
        .expect("replacement sync");
    absorb_updates(&mut effects, &follower, cluster, BUILD, updates);
    effects
        .observe_plans(&follower, cluster, replica, &store, BUILD)
        .await
        .expect("pending replacement");
    assert!(
        effects.selected.is_empty(),
        "superseded bytes must not remain selected"
    );
    assert_eq!(effects.pending, BTreeSet::from([id]));
    store
        .write_plans(vec![(global_id, replacement, index_plan(global_id, on))])
        .await
        .expect("replacement bytes");
    effects
        .observe_plans(&follower, cluster, replica, &store, BUILD)
        .await
        .expect("load replacement");
    assert_eq!(effects.selected[&id].1, replacement);

    transact(
        &mut writer,
        vec![Op::SetWrittenPlan {
            id: global_id,
            build_version: BUILD.into(),
            expected_revision: Some(replacement),
            revision: None,
            imports: BTreeSet::new(),
        }],
    )
    .await;
    let (_, updates) = follower
        .sync_to_current_updates()
        .await
        .expect("unselection sync");
    absorb_updates(&mut effects, &follower, cluster, BUILD, updates);
    effects
        .observe_plans(&follower, cluster, replica, &store, BUILD)
        .await
        .expect("observe unselection");
    assert!(
        effects.selected.is_empty(),
        "unselected bytes must not remain selected"
    );

    // One stream read can deliver several timestamps changing the same item.
    for to_name in ["renamed_once", "renamed_twice"] {
        let current_full_name = writer.resolve_full_name(writer.get_entry(&id).name(), None);
        transact(
            &mut writer,
            vec![Op::RenameItem {
                id,
                current_full_name,
                to_name: to_name.into(),
            }],
        )
        .await;
    }
    let (_, updates) = follower
        .sync_to_current_updates()
        .await
        .expect("multi-timestamp sync");
    assert!(updates.iter().map(|u| u.ts).collect::<BTreeSet<_>>().len() >= 2);
    absorb_updates(&mut effects, &follower, cluster, BUILD, updates);
    effects
        .observe_plans(&follower, cluster, replica, &store, BUILD)
        .await
        .expect("observe renamed item");
    assert_eq!(effects.pending, BTreeSet::from([id]));

    transact(
        &mut writer,
        vec![Op::DropObjects(vec![DropObjectInfo::Item(id)])],
    )
    .await;
    let (_, updates) = follower
        .sync_to_current_updates()
        .await
        .expect("native drop sync");
    absorb_updates(&mut effects, &follower, cluster, BUILD, updates);
    effects
        .observe_plans(&follower, cluster, replica, &store, BUILD)
        .await
        .expect("observe drop");
    assert!(effects.pending.is_empty());
    assert!(effects.selected.is_empty());
    follower.expire().await;
    writer.expire().await;
}

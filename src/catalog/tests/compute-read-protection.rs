// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

#![recursion_limit = "256"]

use std::collections::{BTreeMap, BTreeSet};

use mz_catalog::builtin::BUILTINS;
use mz_catalog::durable::objects::{
    ClusterConfig, ClusterVariant, CollectionCompactionBound, DurableType, SystemObjectDescription,
    SystemObjectMapping, SystemObjectUniqueIdentifier,
};
use mz_catalog::durable::{
    CatalogError, DurableCatalogError, Snapshot, TestCatalogStateBuilder, Transaction,
    test_bootstrap_args,
};
use mz_controller_types::ClusterId;
use mz_ore::now::SYSTEM_TIME;
use mz_persist_client::{PersistClient, ShardId};
use mz_proto::RustType;
use mz_repr::role_id::RoleId;
use mz_repr::{CatalogItemId, GlobalId, Timestamp};
use mz_sql::catalog::CatalogItemType;
use mz_sql::names::SchemaId;
use mz_storage_client::controller::StorageTxn;

#[derive(Clone, Copy, Debug)]
enum Owner {
    Ordinary,
    Builtin,
    Logging,
}

const OWNERS: [Owner; 3] = [Owner::Ordinary, Owner::Builtin, Owner::Logging];

impl Owner {
    fn id(self, n: u64) -> GlobalId {
        match self {
            Self::Ordinary => GlobalId::User(n),
            Self::Builtin => GlobalId::System(n),
            Self::Logging => GlobalId::IntrospectionSourceIndex(n),
        }
    }

    fn item_id(self, n: u64) -> CatalogItemId {
        match self {
            Self::Ordinary => CatalogItemId::User(n),
            Self::Builtin => CatalogItemId::System(n),
            Self::Logging => CatalogItemId::IntrospectionSourceIndex(n),
        }
    }

    fn mapping(self, n: u64) -> SystemObjectMapping {
        SystemObjectMapping {
            description: SystemObjectDescription {
                schema_name: "mz_catalog".to_string(),
                object_type: CatalogItemType::Index,
                object_name: format!("governed_idx_{n}"),
            },
            unique_identifier: SystemObjectUniqueIdentifier {
                catalog_id: self.item_id(n),
                global_id: self.id(n),
                fingerprint: "index definition".to_string(),
            },
        }
    }

    fn insert(self, txn: &mut Transaction<'_>, n: u64) {
        match self {
            Self::Ordinary => txn.insert_item(
                self.item_id(n),
                20000 + u32::try_from(n).unwrap(),
                self.id(n),
                SchemaId::User(1),
                &format!("governed_idx_{n}"),
                format!("CREATE INDEX governed_idx_{n} ON t (a)"),
                RoleId::User(1),
                Vec::new(),
                BTreeMap::new(),
                None,
            ),
            Self::Builtin => txn.set_system_object_mappings(vec![self.mapping(n)]),
            Self::Logging => txn.insert_introspection_source_indexes(
                vec![(
                    cluster_id(),
                    format!("log_{n}"),
                    self.item_id(n),
                    self.id(n),
                )],
                &Default::default(),
            ),
        }
        .unwrap();
    }

    fn remove(self, txn: &mut Transaction<'_>, n: u64) {
        match self {
            Self::Ordinary => txn.remove_item(self.item_id(n)),
            Self::Builtin => {
                txn.remove_system_object_mappings(BTreeSet::from([self.mapping(n).description]))
            }
            Self::Logging => txn.remove_introspection_source_indexes(BTreeSet::from([(
                cluster_id(),
                format!("log_{n}"),
            )])),
        }
        .unwrap();
    }
}

fn cluster_id() -> ClusterId {
    ClusterId::user(1000).unwrap()
}

fn bounds(snapshot: Snapshot) -> BTreeMap<GlobalId, Option<Timestamp>> {
    snapshot
        .collection_compaction_bounds
        .into_iter()
        .map(|entry| {
            let (key, value) = RustType::from_proto(entry).unwrap();
            let bound = CollectionCompactionBound::from_key_value(key, value);
            (bound.id, bound.frontier)
        })
        .collect()
}

async fn commit(mut txn: Transaction<'_>) {
    txn.finalize_index_compaction_bounds();
    let _ = txn.get_and_commit_op_updates();
    let ts = txn.upper();
    txn.commit(ts).await.unwrap();
}

async fn reject(mut txn: Transaction<'_>, message: &str) {
    txn.finalize_index_compaction_bounds();
    match txn.validate_read_protection().unwrap_err() {
        CatalogError::Durable(DurableCatalogError::InvalidReadProtection(detail)) => {
            assert!(detail.contains(message), "{detail}");
        }
        error => panic!("{error}"),
    }
    let _ = txn.get_and_commit_op_updates();
    let ts = txn.upper();
    assert!(matches!(
        txn.commit(ts).await,
        Err(CatalogError::Durable(
            DurableCatalogError::InvalidReadProtection(_)
        ))
    ));
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)]
async fn index_birth_bootstrap_and_drop() {
    let builder = TestCatalogStateBuilder::new(PersistClient::new_for_tests().await)
        .with_default_deploy_generation();
    let mut state = builder
        .clone()
        .unwrap_build()
        .await
        .open(SYSTEM_TIME().into(), &test_bootstrap_args())
        .await
        .unwrap();
    let _ = state.sync_to_current_updates().await.unwrap();

    let mut txn = state.transaction().await.unwrap();
    for owner in OWNERS {
        owner.insert(&mut txn, 1000);
    }
    assert!(bounds(txn.current_snapshot()).is_empty());
    txn.set_config("catalog_read_protection_enabled".to_string(), Some(1))
        .unwrap();
    txn.finalize_index_compaction_bounds();
    for owner in OWNERS {
        owner.insert(&mut txn, 1001);
    }
    assert!(bounds(txn.current_snapshot()).is_empty());
    commit(txn).await;
    Box::new(state).expire().await;

    let mut state = builder
        .unwrap_build()
        .await
        .open(SYSTEM_TIME().into(), &test_bootstrap_args())
        .await
        .unwrap();
    let _ = state.sync_to_current_updates().await.unwrap();
    assert!(bounds(state.snapshot().await.unwrap()).is_empty());
    let expected: BTreeMap<_, _> = OWNERS
        .into_iter()
        .flat_map(|owner| [1000, 1001].map(|n| (owner.id(n), Some(10.into()))))
        .collect();
    let mut txn = state.transaction().await.unwrap();
    for (id, frontier) in &expected {
        txn.set_collection_compaction_bound(*id, *frontier).unwrap();
    }
    commit(txn).await;
    assert_eq!(bounds(state.snapshot().await.unwrap()), expected);
    let mut txn = state.transaction().await.unwrap();
    let mut item = txn.get_item(&Owner::Ordinary.item_id(1001)).unwrap();
    item.global_id = Owner::Ordinary.id(1002);
    txn.update_items(BTreeMap::from([(item.id, item)])).unwrap();
    let mut mapping = Owner::Builtin.mapping(1001);
    mapping.unique_identifier.global_id = Owner::Builtin.id(1002);
    txn.update_system_object_mappings(BTreeMap::from([(Owner::Builtin.item_id(1001), mapping)]))
        .unwrap();
    txn.update_introspection_source_index_gids(std::iter::once((
        cluster_id(),
        std::iter::once((
            "log_1001".to_string(),
            Owner::Logging.item_id(1001),
            Owner::Logging.id(1002),
            22000,
        )),
    )))
    .unwrap();
    txn.finalize_index_compaction_bounds();
    for owner in OWNERS {
        let current = bounds(txn.current_snapshot());
        assert!(!current.contains_key(&owner.id(1001)));
        assert!(!current.contains_key(&owner.id(1002)));
    }
    commit(txn).await;
    let mut txn = state.transaction().await.unwrap();
    for owner in OWNERS {
        owner.remove(&mut txn, 1000);
    }
    txn.remove_items(&BTreeSet::from([Owner::Ordinary.item_id(1001)]))
        .unwrap();
    Owner::Builtin.remove(&mut txn, 1001);
    Owner::Logging.remove(&mut txn, 1001);
    commit(txn).await;
    assert!(bounds(state.snapshot().await.unwrap()).is_empty());
    Box::new(state).expire().await;
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)]
async fn surviving_index_permissions_are_monotonic() {
    let mut state = TestCatalogStateBuilder::new(PersistClient::new_for_tests().await)
        .with_default_deploy_generation()
        .unwrap_build()
        .await
        .open(SYSTEM_TIME().into(), &test_bootstrap_args())
        .await
        .unwrap();
    let _ = state.sync_to_current_updates().await.unwrap();
    let mut txn = state.transaction().await.unwrap();
    txn.set_config("catalog_read_protection_enabled".to_string(), Some(1))
        .unwrap();
    for owner in OWNERS {
        owner.insert(&mut txn, 1000);
        txn.set_collection_compaction_bound(owner.id(1000), Some(10.into()))
            .unwrap();
    }
    commit(txn).await;
    let expected = bounds(state.snapshot().await.unwrap());

    for owner in OWNERS {
        let mut txn = state.transaction().await.unwrap();
        txn.set_collection_compaction_bound(owner.id(1000), Some(9.into()))
            .unwrap();
        reject(txn, "regressed").await;

        let mut txn = state.transaction().await.unwrap();
        owner.remove(&mut txn, 1000);
        owner.insert(&mut txn, 1000);
        txn.set_collection_compaction_bound(owner.id(1000), Some(9.into()))
            .unwrap();
        reject(txn, "regressed").await;

        let mut txn = state.transaction().await.unwrap();
        owner.remove(&mut txn, 1000);
        txn.set_config("catalog_read_protection_enabled".to_string(), Some(0))
            .unwrap();
        owner.insert(&mut txn, 1000);
        txn.finalize_index_compaction_bounds();
        txn.validate_read_protection().unwrap();
        assert_eq!(bounds(txn.current_snapshot()), expected);
        drop(txn);
        assert_eq!(bounds(state.snapshot().await.unwrap()), expected);
    }

    let mut txn = state.transaction().await.unwrap();
    let mut mapping = Owner::Builtin.mapping(1000);
    mapping.unique_identifier.fingerprint = "changed definition".to_string();
    txn.set_system_object_mappings(vec![mapping.clone()])
        .unwrap();
    txn.update_system_object_mappings(BTreeMap::from([(Owner::Builtin.item_id(1000), mapping)]))
        .unwrap();
    txn.finalize_index_compaction_bounds();
    assert_eq!(bounds(txn.current_snapshot()), expected);
    for owner in OWNERS {
        owner.remove(&mut txn, 1000);
        owner.insert(&mut txn, 1000);
        txn.set_collection_compaction_bound(owner.id(1000), Some(20.into()))
            .unwrap();
    }
    commit(txn).await;
    let mut txn = state.transaction().await.unwrap();
    for owner in OWNERS {
        txn.set_collection_compaction_bound(owner.id(1000), None)
            .unwrap();
    }
    commit(txn).await;
    for owner in OWNERS {
        let mut txn = state.transaction().await.unwrap();
        txn.set_collection_compaction_bound(owner.id(1000), Some(20.into()))
            .unwrap();
        reject(txn, "regressed").await;
    }
    Box::new(state).expire().await;
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)]
async fn index_id_swaps_preserve_bounds() {
    let mut state = TestCatalogStateBuilder::new(PersistClient::new_for_tests().await)
        .with_default_deploy_generation()
        .unwrap_build()
        .await
        .open(SYSTEM_TIME().into(), &test_bootstrap_args())
        .await
        .unwrap();
    let _ = state.sync_to_current_updates().await.unwrap();
    let mut txn = state.transaction().await.unwrap();
    txn.set_config("catalog_read_protection_enabled".to_string(), Some(1))
        .unwrap();
    for owner in OWNERS {
        for (n, frontier) in [(1000, Some(10.into())), (1001, None)] {
            owner.insert(&mut txn, n);
            txn.set_collection_compaction_bound(owner.id(n), frontier)
                .unwrap();
        }
    }
    commit(txn).await;
    let expected = bounds(state.snapshot().await.unwrap());

    for enabled in [false, true] {
        let mut txn = state.transaction().await.unwrap();
        txn.set_config(
            "catalog_read_protection_enabled".to_string(),
            Some(u64::from(enabled)),
        )
        .unwrap();
        let items = [1000, 1001].map(|n| {
            let mut item = txn.get_item(&Owner::Ordinary.item_id(n)).unwrap();
            item.global_id = Owner::Ordinary.id(2001 - n);
            (item.id, item)
        });
        txn.update_items(BTreeMap::from(items)).unwrap();

        let mappings = [1000, 1001].map(|n| {
            let mut mapping = Owner::Builtin.mapping(n);
            mapping.unique_identifier.global_id = Owner::Builtin.id(2001 - n);
            (Owner::Builtin.item_id(n), mapping)
        });
        txn.update_system_object_mappings(BTreeMap::from(mappings))
            .unwrap();
        assert_eq!(bounds(txn.current_snapshot()), expected);
        let mut intermediate = Owner::Builtin.mapping(1000);
        intermediate.unique_identifier.global_id = Owner::Builtin.id(1002);
        txn.set_system_object_mappings(vec![
            intermediate,
            Owner::Builtin.mapping(1000),
            Owner::Builtin.mapping(1001),
        ])
        .unwrap();

        let mut intermediate = Owner::Builtin.mapping(1000);
        intermediate.unique_identifier.global_id = Owner::Builtin.id(1002);
        txn.set_system_object_mappings(vec![intermediate]).unwrap();
        txn.set_collection_compaction_bound(Owner::Builtin.id(1002), None)
            .unwrap();
        txn.set_system_object_mappings(vec![Owner::Builtin.mapping(1000)])
            .unwrap();

        // Intermediate identities must not mask a surviving ID or retain bounds.
        txn.update_introspection_source_index_gids(std::iter::once((
            cluster_id(),
            [(1000, 1002), (1001, 1000), (1000, 1001)]
                .into_iter()
                .map(|(n, gid)| {
                    (
                        format!("log_{n}"),
                        Owner::Logging.item_id(n),
                        Owner::Logging.id(gid),
                        22000 + u32::try_from(n).unwrap(),
                    )
                }),
        )))
        .unwrap();
        txn.finalize_index_compaction_bounds();
        assert_eq!(bounds(txn.current_snapshot()), expected);
        txn.validate_read_protection().unwrap();
    }
    let mut txn = state.transaction().await.unwrap();
    let mut item = txn.get_item(&Owner::Ordinary.item_id(1001)).unwrap();
    item.global_id = Owner::Ordinary.id(1000);
    txn.update_item(item.id, item).unwrap();
    commit(txn).await;
    let mut txn = state.transaction().await.unwrap();
    Owner::Ordinary.remove(&mut txn, 1000);
    txn.finalize_index_compaction_bounds();
    assert_eq!(
        bounds(txn.current_snapshot())[&Owner::Ordinary.id(1000)],
        Some(10.into())
    );
    commit(txn).await;
    Box::new(state).expire().await;
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)]
async fn cannot_adopt_live_ungoverned_indices() {
    let mut state = TestCatalogStateBuilder::new(PersistClient::new_for_tests().await)
        .with_default_deploy_generation()
        .unwrap_build()
        .await
        .open(SYSTEM_TIME().into(), &test_bootstrap_args())
        .await
        .unwrap();
    let _ = state.sync_to_current_updates().await.unwrap();
    let mut txn = state.transaction().await.unwrap();
    for owner in OWNERS {
        owner.insert(&mut txn, 1000);
    }
    commit(txn).await;

    for owner in OWNERS {
        for frontier in [Some(Timestamp::MIN), None] {
            let mut txn = state.transaction().await.unwrap();
            txn.set_config("catalog_read_protection_enabled".to_string(), Some(1))
                .unwrap();
            txn.set_collection_compaction_bound(owner.id(1000), frontier)
                .unwrap();
            reject(txn, "cannot introduce").await;
        }
        let mut txn = state.transaction().await.unwrap();
        txn.set_config("catalog_read_protection_enabled".to_string(), Some(1))
            .unwrap();
        owner.remove(&mut txn, 1000);
        owner.insert(&mut txn, 1000);
        txn.set_collection_compaction_bound(owner.id(1000), Some(10.into()))
            .unwrap();
        reject(txn, "cannot introduce").await;
    }
    Box::new(state).expire().await;
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)]
async fn index_permission_is_not_a_recovery_boundary() {
    let mut state = TestCatalogStateBuilder::new(PersistClient::new_for_tests().await)
        .with_default_deploy_generation()
        .unwrap_build()
        .await
        .open(SYSTEM_TIME().into(), &test_bootstrap_args())
        .await
        .unwrap();
    let _ = state.sync_to_current_updates().await.unwrap();
    let mut txn = state.transaction().await.unwrap();
    txn.set_config("catalog_read_protection_enabled".to_string(), Some(1))
        .unwrap();
    for owner in OWNERS {
        owner.insert(&mut txn, 1000);
        txn.set_collection_compaction_bound(owner.id(1000), Some(10.into()))
            .unwrap();
    }
    let output = GlobalId::User(2000);
    txn.insert_collection_metadata(BTreeMap::from([(output, ShardId::new())]))
        .unwrap();
    commit(txn).await;
    for owner in OWNERS {
        let mut txn = state.transaction().await.unwrap();
        txn.set_maintained_read_requirement(
            output,
            BTreeSet::from([owner.id(1000)]),
            Some(10.into()),
        )
        .unwrap();
        reject(txn, "needs input").await;
    }

    for (sql, object_type) in [
        (
            "CREATE MATERIALIZED VIEW v AS SELECT 1",
            CatalogItemType::MaterializedView,
        ),
        (
            "CREATE METRIC SINK s FROM v INTO CONNECTION c",
            CatalogItemType::MetricSink,
        ),
        ("CREATE VIEW v AS SELECT 1", CatalogItemType::View),
    ] {
        let mut txn = state.transaction().await.unwrap();
        txn.insert_item(
            CatalogItemId::User(3000),
            23000,
            GlobalId::User(3000),
            SchemaId::User(1),
            "not_an_index",
            sql.to_string(),
            RoleId::User(1),
            Vec::new(),
            BTreeMap::new(),
            None,
        )
        .unwrap();
        txn.set_collection_compaction_bound(GlobalId::User(3000), None)
            .unwrap();
        txn.delete_collection_metadata(BTreeSet::from([GlobalId::User(3000)]));
        reject(txn, "neither storage metadata nor an index identity").await;

        let mut txn = state.transaction().await.unwrap();
        let mut mapping = Owner::Builtin.mapping(2000);
        mapping.description.object_type = object_type;
        txn.set_system_object_mappings(vec![mapping]).unwrap();
        assert!(!bounds(txn.current_snapshot()).contains_key(&Owner::Builtin.id(2000)));
        txn.set_collection_compaction_bound(Owner::Builtin.id(2000), None)
            .unwrap();
        reject(txn, "neither storage metadata nor an index identity").await;
    }
    Box::new(state).expire().await;
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)]
async fn cluster_and_ephemeral_cleanup() {
    let mut state = TestCatalogStateBuilder::new(PersistClient::new_for_tests().await)
        .with_default_deploy_generation()
        .unwrap_build()
        .await
        .open(SYSTEM_TIME().into(), &test_bootstrap_args())
        .await
        .unwrap();
    let _ = state.sync_to_current_updates().await.unwrap();
    let mut txn = state.transaction().await.unwrap();
    txn.set_config("catalog_read_protection_enabled".to_string(), Some(1))
        .unwrap();
    txn.insert_user_cluster(
        cluster_id(),
        "governed_cluster",
        vec![(
            BUILTINS::logs().next().unwrap(),
            Owner::Logging.item_id(1000),
            Owner::Logging.id(1000),
        )],
        RoleId::User(1),
        Vec::new(),
        ClusterConfig {
            variant: ClusterVariant::Unmanaged,
            workload_class: None,
        },
        &Default::default(),
    )
    .unwrap();
    Owner::Ordinary.insert(&mut txn, 1000);
    let mut item = txn.get_item(&Owner::Ordinary.item_id(1000)).unwrap();
    item.ephemeral_owner_session = Some(uuid::Uuid::new_v4());
    txn.update_item(item.id, item).unwrap();
    for owner in [Owner::Ordinary, Owner::Logging] {
        txn.set_collection_compaction_bound(owner.id(1000), Some(10.into()))
            .unwrap();
    }
    assert_eq!(bounds(txn.current_snapshot()).len(), 2);
    Owner::Ordinary.insert(&mut txn, 1001);
    let mut table = txn.get_item(&Owner::Ordinary.item_id(1001)).unwrap();
    table.create_sql = "CREATE TABLE t (a INT)".to_string();
    txn.update_item(table.id, table).unwrap();
    let storage = Owner::Ordinary.id(1001);
    txn.insert_collection_metadata(BTreeMap::from([(storage, ShardId::new())]))
        .unwrap();
    txn.set_collection_compaction_bound(storage, Some(10.into()))
        .unwrap();
    commit(txn).await;
    let mut txn = state.transaction().await.unwrap();
    txn.remove_clusters(&BTreeSet::from([cluster_id()]))
        .unwrap();
    txn.remove_ephemeral_items();
    txn.remove_items(&BTreeSet::from([Owner::Ordinary.item_id(1001)]))
        .unwrap();
    txn.finalize_index_compaction_bounds();
    assert_eq!(
        bounds(txn.current_snapshot()),
        BTreeMap::from([(storage, Some(10.into()))])
    );
    txn.delete_collection_metadata(BTreeSet::from([storage]));
    commit(txn).await;
    assert!(bounds(state.snapshot().await.unwrap()).is_empty());
    Box::new(state).expire().await;
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)]
async fn unchanged_consumers_follow_bound_and_lifetime_changes() {
    let builder = TestCatalogStateBuilder::new(PersistClient::new_for_tests().await)
        .with_default_deploy_generation();
    let mut state = builder
        .clone()
        .unwrap_build()
        .await
        .open(SYSTEM_TIME().into(), &test_bootstrap_args())
        .await
        .unwrap();
    let _ = state.sync_to_current_updates().await.unwrap();
    let input = GlobalId::User(2000);
    let output = GlobalId::User(2001);
    let mut txn = state.transaction().await.unwrap();
    txn.insert_collection_metadata(BTreeMap::from([
        (input, ShardId::new()),
        (output, ShardId::new()),
    ]))
    .unwrap();
    txn.set_collection_compaction_bound(input, Some(10.into()))
        .unwrap();
    txn.set_maintained_read_requirement(output, BTreeSet::from([input]), Some(20.into()))
        .unwrap();
    commit(txn).await;
    Box::new(state).expire().await;

    let mut state = builder
        .unwrap_build()
        .await
        .open(SYSTEM_TIME().into(), &test_bootstrap_args())
        .await
        .unwrap();
    let _ = state.sync_to_current_updates().await.unwrap();
    for bound in [Some(21.into()), None] {
        let mut txn = state.transaction().await.unwrap();
        txn.set_collection_compaction_bound(input, bound).unwrap();
        reject(txn, "needs input").await;
    }
    let mut txn = state.transaction().await.unwrap();
    txn.delete_collection_metadata(BTreeSet::from([input]));
    reject(txn, "needs input").await;

    let mut txn = state.transaction().await.unwrap();
    txn.set_maintained_read_requirement(output, BTreeSet::from([input]), None)
        .unwrap();
    txn.delete_collection_metadata(BTreeSet::from([input]));
    commit(txn).await;
    let mut txn = state.transaction().await.unwrap();
    let unknown = GlobalId::User(3000);
    txn.set_maintained_read_requirement(unknown, BTreeSet::new(), None)
        .unwrap();
    txn.delete_collection_metadata(BTreeSet::from([unknown]));
    reject(txn, "has no storage metadata").await;
    Box::new(state).expire().await;
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)]
async fn incremental_dry_run_projects_final_identities_and_edges() {
    let mut state = TestCatalogStateBuilder::new(PersistClient::new_for_tests().await)
        .with_default_deploy_generation()
        .unwrap_build()
        .await
        .open(SYSTEM_TIME().into(), &test_bootstrap_args())
        .await
        .unwrap();
    let _ = state.sync_to_current_updates().await.unwrap();
    let initial = state.snapshot().await.unwrap();
    let input = GlobalId::User(2000);
    let other_input = GlobalId::User(2001);
    let output = GlobalId::User(2002);
    let mut dry_run = state.transaction_from_snapshot(initial.clone()).unwrap();
    let txn = dry_run.transaction_mut();
    txn.set_config("catalog_read_protection_enabled".to_string(), Some(1))
        .unwrap();
    for owner in OWNERS {
        owner.insert(txn, 1000);
    }
    txn.insert_collection_metadata(BTreeMap::from([
        (input, ShardId::new()),
        (other_input, ShardId::new()),
        (output, ShardId::new()),
    ]))
    .unwrap();
    for id in [input, other_input] {
        txn.set_collection_compaction_bound(id, Some(10.into()))
            .unwrap();
    }
    txn.set_maintained_read_requirement(output, BTreeSet::from([input]), Some(20.into()))
        .unwrap();
    txn.finalize_index_compaction_bounds();
    txn.validate_read_protection().unwrap();
    let snapshot = dry_run.current_snapshot();
    drop(dry_run);

    let mut dry_run = state.transaction_from_snapshot(snapshot.clone()).unwrap();
    let txn = dry_run.transaction_mut();
    txn.set_collection_compaction_bound(input, Some(21.into()))
        .unwrap();
    assert!(txn.validate_read_protection().is_err());
    drop(dry_run);

    let mut dry_run = state.transaction_from_snapshot(snapshot).unwrap();
    let txn = dry_run.transaction_mut();
    for owner in OWNERS {
        txn.set_collection_compaction_bound(owner.id(1000), Some(10.into()))
            .unwrap();
        owner.remove(txn, 1000);
        owner.insert(txn, 1000);
    }
    txn.set_maintained_read_requirement(output, BTreeSet::from([other_input]), Some(20.into()))
        .unwrap();
    txn.finalize_index_compaction_bounds();
    txn.validate_read_protection().unwrap();
    let snapshot = dry_run.current_snapshot();
    drop(dry_run);

    let mut dry_run = state.transaction_from_snapshot(snapshot).unwrap();
    let txn = dry_run.transaction_mut();
    txn.delete_collection_metadata(BTreeSet::from([input]));
    txn.validate_read_protection().unwrap();
    txn.set_collection_compaction_bound(other_input, Some(21.into()))
        .unwrap();
    assert!(txn.validate_read_protection().is_err());
    txn.set_maintained_read_requirement(output, BTreeSet::from([other_input]), None)
        .unwrap();
    for owner in OWNERS {
        owner.remove(txn, 1000);
    }
    txn.finalize_index_compaction_bounds();
    txn.validate_read_protection().unwrap();
    let snapshot = dry_run.current_snapshot();
    for owner in OWNERS {
        assert!(!bounds(snapshot.clone()).contains_key(&owner.id(1000)));
    }
    drop(dry_run);
    for owner in OWNERS {
        let mut dry_run = state.transaction_from_snapshot(snapshot.clone()).unwrap();
        let txn = dry_run.transaction_mut();
        txn.set_collection_compaction_bound(other_input, Some(22.into()))
            .unwrap();
        txn.validate_read_protection().unwrap();
        txn.set_collection_compaction_bound(owner.id(1000), Some(10.into()))
            .unwrap();
        assert!(txn.validate_read_protection().is_err());
    }
    assert_eq!(state.snapshot().await.unwrap(), initial);
    Box::new(state).expire().await;
}

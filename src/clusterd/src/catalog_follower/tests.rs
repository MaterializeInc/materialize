// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::*;
use mz_compute_types::dataflows::{DataflowDescription, IndexDesc, IndexImport};
use mz_repr::role_id::RoleId;
use mz_repr::{CatalogItemId, ReprRelationType};
use mz_sql::names::SchemaId;

const BUILD: &str = "1.0.0";

fn item(snapshot: &mut Snapshot, id: u64, sql: &str) -> objects::ItemKey {
    let (key, value) = objects::Item {
        id: CatalogItemId::User(id),
        oid: 1,
        global_id: GlobalId::User(id),
        schema_id: SchemaId::User(1),
        name: format!("item{id}"),
        create_sql: sql.into(),
        owner_id: RoleId::User(1),
        privileges: Vec::new(),
        extra_versions: BTreeMap::new(),
        ephemeral_owner_session: None,
    }
    .into_key_value();
    snapshot.items.insert(key.into_proto(), value.into_proto());
    key
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

fn plan(import: Option<GlobalId>) -> GlobalExpressions {
    let mut plan = GlobalExpressions {
        global_mir: DataflowDescription::new("mir".into()),
        physical_plan: DataflowDescription::new("lir".into()),
        dataflow_metainfos: Default::default(),
        optimizer_features: Default::default(),
        item_version: RelationVersion::root(),
    };
    if let Some(id) = import {
        plan.physical_plan.index_imports.insert(
            id,
            IndexImport {
                desc: IndexDesc {
                    on_id: GlobalId::User(99),
                    key: Vec::new(),
                },
                typ: ReprRelationType::empty(),
                monotonic: false,
                with_snapshot: true,
            },
        );
    }
    plan
}

#[mz_ore::test]
fn complete_prefix_dependencies_and_rewrite() {
    let cluster = ClusterId::User(1);
    let upstream = GlobalId::User(2);
    let downstream = GlobalId::User(1);
    let revision = Uuid::from_u128(1);
    let mut snapshot = Snapshot::empty();
    item(&mut snapshot, 1, "CREATE INDEX i IN CLUSTER [u1] ON t (a)");
    let upstream_key = item(&mut snapshot, 2, "CREATE INDEX j IN CLUSTER [u1] ON t (a)");
    select(&mut snapshot, 1, revision);
    let mut state = derive(snapshot.clone(), cluster, BUILD).expect("valid catalog fixture");
    state
        .plans
        .insert((downstream, revision), plan(Some(upstream)));
    state.check_plans().expect("valid plan fixture");
    assert_eq!(state.pending.get(&upstream), Some(&Pending::Selection));
    assert_eq!(
        state.pending.get(&downstream),
        Some(&Pending::Dependencies(BTreeSet::from([upstream])))
    );

    // Both creations and selections are visible together, regardless of ID order.
    select(&mut snapshot, 2, revision);
    let mut state = derive(snapshot.clone(), cluster, BUILD).expect("valid catalog fixture");
    state
        .plans
        .insert((downstream, revision), plan(Some(upstream)));
    state.check_plans().expect("valid plan fixture");
    assert_eq!(
        state.pending.get(&upstream),
        Some(&Pending::Bytes(revision))
    );
    state.plans.insert((upstream, revision), plan(None));
    state.check_plans().expect("valid plan fixture");
    assert!(state.pending.is_empty());
    assert_eq!(state.plans[&(downstream, revision)], plan(Some(upstream)));

    // A dropped import must block the old selected plan, not disappear from it.
    snapshot.items.remove(&upstream_key.into_proto());
    let mut dropped = derive(snapshot.clone(), cluster, BUILD).expect("valid catalog fixture");
    dropped
        .plans
        .insert((downstream, revision), plan(Some(upstream)));
    dropped.check_plans().expect("valid plan fixture");
    assert!(!dropped.members.contains_key(&upstream));
    assert!(!dropped.selections.contains_key(&upstream));
    assert_eq!(
        dropped.pending.get(&downstream),
        Some(&Pending::Imports(BTreeSet::from([upstream])))
    );

    let rewrite = Uuid::from_u128(2);
    select(&mut snapshot, 1, rewrite);
    let mut rewritten = derive(snapshot, cluster, BUILD).expect("valid catalog fixture");
    rewritten.plans.insert((downstream, rewrite), plan(None));
    rewritten.check_plans().expect("valid plan fixture");
    assert!(rewritten.pending.is_empty());
    assert_eq!(rewritten.selections[&downstream], rewrite);
}

#[mz_ore::test]
fn inventory_is_not_selected_plans() {
    let mut snapshot = Snapshot::empty();
    item(
        &mut snapshot,
        1,
        "CREATE MATERIALIZED VIEW mv IN CLUSTER [u1] AS SELECT 1",
    );
    item(&mut snapshot, 2, "CREATE INDEX i IN CLUSTER [u2] ON t (a)");
    select(&mut snapshot, 2, Uuid::from_u128(1));
    select(&mut snapshot, 3, Uuid::from_u128(1));
    let mut state = derive(snapshot, ClusterId::User(1), BUILD).expect("valid catalog fixture");
    state.check_plans().expect("valid plan fixture");
    assert_eq!(
        state.members.keys().copied().collect::<Vec<_>>(),
        vec![GlobalId::User(1)]
    );
    assert!(state.selections.is_empty());
    assert_eq!(
        state.pending,
        BTreeMap::from([(GlobalId::User(1), Pending::Selection)])
    );
}

#[mz_ore::test]
fn replacement_aliases_remain_readable_without_writer_selections() {
    let mut snapshot = Snapshot::empty();
    let key = item(
        &mut snapshot,
        1,
        "CREATE MATERIALIZED VIEW mv IN CLUSTER [u1] AS SELECT 1",
    );
    let value = snapshot.items[&key.clone().into_proto()].clone();
    let mut mv = objects::Item::from_key_value(
        key,
        RustType::from_proto(value).expect("valid item fixture"),
    );
    let version = RelationVersion::from_raw(1);
    mv.extra_versions.insert(version, GlobalId::User(2));
    let (key, value) = mv.into_key_value();
    snapshot.items.insert(key.into_proto(), value.into_proto());
    item(&mut snapshot, 3, "CREATE INDEX i IN CLUSTER [u1] ON mv (a)");
    let revision = Uuid::from_u128(1);
    select(&mut snapshot, 2, revision);
    select(&mut snapshot, 3, revision);

    let mut state = derive(snapshot, ClusterId::User(1), BUILD).expect("valid snapshot");
    let mut writer = plan(None);
    writer.item_version = version;
    state.plans.insert((GlobalId::User(2), revision), writer);
    let mut reader = plan(None);
    reader
        .global_mir
        .import_source(GlobalId::User(1), mz_repr::SqlRelationType::empty(), false);
    reader.physical_plan.source_imports = reader.global_mir.source_imports.clone();
    state.plans.insert((GlobalId::User(3), revision), reader);
    state.check_plans().expect("valid plans");
    assert_eq!(state.members[&GlobalId::User(1)], Member::Storage);
    assert!(state.pending.is_empty(), "{:?}", state.pending);
}

#[mz_ore::test]
fn webhook_placement_distinguishes_cluster_sources_from_tables() {
    let mut snapshot = Snapshot::empty();
    item(
        &mut snapshot,
        1,
        "CREATE SOURCE hook IN CLUSTER [u1] FROM WEBHOOK BODY FORMAT TEXT",
    );
    item(
        &mut snapshot,
        2,
        "CREATE TABLE hook_table FROM WEBHOOK BODY FORMAT TEXT",
    );
    let state = derive(snapshot, ClusterId::User(1), BUILD).expect("valid webhooks");
    assert_eq!(
        state.members,
        BTreeMap::from([(GlobalId::User(1), Member::Storage)])
    );
}

#[mz_ore::test]
fn builtin_placement_requires_durable_resolution() {
    let clusters = BTreeMap::from([("c".into(), ClusterId::System(1))]);
    let sql = "CREATE INDEX i IN CLUSTER c ON t (a)";
    assert!(placement(sql, None).is_err());
    assert_eq!(
        placement(sql, Some(&clusters)).expect("resolved builtin placement"),
        Some((ClusterId::System(1), true))
    );
    assert!(placement(sql, Some(&BTreeMap::new())).is_err());
}

#[mz_ore::test]
fn builtin_inventory_covers_build_definitions() {
    let mut snapshot = Snapshot::empty();
    let mut cluster_ids = Vec::new();
    for (i, builtin) in mz_catalog::builtin::BUILTIN_CLUSTERS.iter().enumerate() {
        let id = ClusterId::System(u64::try_from(i).expect("builtin count fits u64") + 1);
        cluster_ids.push(id);
        let (key, value) = objects::Cluster {
            id,
            name: builtin.name.into(),
            owner_id: RoleId::System(1),
            privileges: Vec::new(),
            config: objects::ClusterConfig {
                variant: objects::ClusterVariant::Unmanaged,
                workload_class: None,
            },
        }
        .into_key_value();
        snapshot
            .clusters
            .insert(key.into_proto(), value.into_proto());
    }
    let mut expected = BTreeSet::new();
    for (i, builtin) in BUILTINS::iter().enumerate() {
        let id = u64::try_from(i).expect("builtin count fits u64") + 1;
        let (key, value) = objects::SystemObjectMapping {
            description: objects::SystemObjectDescription {
                schema_name: builtin.schema().into(),
                object_name: builtin.name().into(),
                object_type: builtin.catalog_item_type(),
            },
            unique_identifier: objects::SystemObjectUniqueIdentifier {
                catalog_id: CatalogItemId::System(id),
                global_id: GlobalId::System(id),
                fingerprint: String::new(),
            },
        }
        .into_key_value();
        snapshot
            .system_object_mappings
            .insert(key.into_proto(), value.into_proto());
        if matches!(builtin, Builtin::Index(_) | Builtin::MaterializedView(_)) {
            expected.insert(GlobalId::System(id));
        }
    }
    let mut observed = BTreeSet::new();
    for cluster_id in cluster_ids {
        let mut state = derive(snapshot.clone(), cluster_id, BUILD).expect("valid catalog fixture");
        state.check_plans().expect("valid plan fixture");
        assert_eq!(state.pending.len(), state.members.len());
        for id in state.members.keys() {
            assert!(
                observed.insert(*id),
                "builtin must belong to exactly one cluster"
            );
        }
    }
    assert_eq!(observed, expected);
}

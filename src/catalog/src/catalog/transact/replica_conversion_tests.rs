// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;

use mz_persist_client::PersistClient;
use mz_sql::DEFAULT_SCHEMA;
use mz_sql::catalog::{CatalogDatabase, ReplicaTarget};
use mz_sql::names::{ItemQualifiers, QualifiedItemName, ResolvedDatabaseSpecifier};
use mz_sql::session::user::MZ_SYSTEM_ROLE_ID;
use mz_sql::session::vars::DEFAULT_DATABASE_NAME;
use uuid::Uuid;

use super::ReplicaCreateDropReason;
use crate::SYSTEM_CONN_ID;
use crate::catalog::state::LocalExpressionCache;
use crate::catalog::{Catalog, Op};
use crate::memory::objects::{CatalogItem, ClusterConfig, ClusterVariant};

async fn commit(catalog: &mut Catalog, ops: Vec<Op>) {
    catalog
        .transact(None, catalog.current_upper().await, None, ops)
        .await
        .expect("commit conversion fixture operation");
    catalog
        .state()
        .check_consistency()
        .expect("consistent catalog");
}

async fn open_follower(active: &Catalog, persist: PersistClient, organization: Uuid) -> Catalog {
    let bootstrap = crate::catalog::test_bootstrap_args();
    let storage = crate::durable::TestCatalogStateBuilder::new(persist)
        .with_organization_id(organization)
        .with_deploy_generation(2)
        .unwrap_build()
        .await
        .open_read_only(&bootstrap)
        .await
        .expect("open follower storage");
    Catalog::open_committed(
        Catalog::diagnostic_state_config(&active.diagnostic_config),
        storage,
    )
    .await
    .expect("reconstruct follower")
    .catalog
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)]
async fn unrealized_mv_pin_survives_replica_conversion_round_trip() {
    let persist = PersistClient::new_for_tests().await;
    let organization = Uuid::new_v4();
    let bootstrap = crate::catalog::test_bootstrap_args();
    let mut original = Catalog::open_debug_catalog(persist.clone(), organization, &bootstrap)
        .await
        .expect("open original deployment");
    let template = original
        .user_cluster_replicas()
        .next()
        .expect("bootstrap replica");
    let replica_config = template.config.clone();
    let managed = original.get_cluster(template.cluster_id).config.clone();
    assert!(matches!(managed.variant, ClusterVariant::Managed(_)));
    let unmanaged = ClusterConfig {
        variant: ClusterVariant::Unmanaged,
        workload_class: None,
    };
    let cluster_id = original
        .allocate_user_cluster_id(original.current_upper().await)
        .await
        .expect("allocate cluster");
    let ids = original
        .allocate_user_replica_ids(2, original.current_upper().await)
        .await
        .expect("allocate declaration and promoted realization");
    let declaration_id = ids[0];
    let promoted_id = ids[1];
    assert_ne!(declaration_id, promoted_id);
    commit(
        &mut original,
        vec![
            Op::CreateCluster {
                id: cluster_id,
                name: "conversion_pin".into(),
                introspection_sources: crate::builtin::BUILTINS::logs().collect(),
                owner_id: MZ_SYSTEM_ROLE_ID,
                config: unmanaged.clone(),
            },
            Op::CreateClusterReplica {
                cluster_id,
                replica_id: declaration_id,
                name: "r1".into(),
                config: replica_config.clone(),
                owner_id: MZ_SYSTEM_ROLE_ID,
                reason: ReplicaCreateDropReason::Manual,
            },
        ],
    )
    .await;

    let database = original
        .resolve_database(DEFAULT_DATABASE_NAME)
        .expect("default database");
    let database_spec = ResolvedDatabaseSpecifier::Id(database.id());
    let schema = original
        .resolve_schema_in_database(&database_spec, DEFAULT_SCHEMA, &SYSTEM_CONN_ID)
        .expect("default schema");
    let name = QualifiedItemName {
        qualifiers: ItemQualifiers {
            database_spec,
            schema_spec: schema.id.clone(),
        },
        item: "pinned_mv".into(),
    };
    let sql = format!(
        "CREATE MATERIALIZED VIEW {}.{}.pinned_mv IN CLUSTER conversion_pin REPLICA r1 AS SELECT 1 AS a AS OF 0",
        database.name, schema.name.schema,
    );
    let (mv_id, mv_global_id) = original
        .allocate_user_id_for_test()
        .await
        .expect("allocate MV identity");
    let item = original
        .state()
        .clone()
        .with_enable_for_item_parsing(|state| {
            state.parse_item(
                mv_global_id,
                &sql,
                &BTreeMap::new(),
                None,
                false,
                None,
                &mut LocalExpressionCache::Closed,
                None,
            )
        })
        .expect("plan pinned MV");
    commit(
        &mut original,
        vec![Op::CreateItem {
            id: mv_id,
            name,
            item,
            owner_id: MZ_SYSTEM_ROLE_ID,
        }],
    )
    .await;
    let mv_shard = original.state().storage_metadata().collection_metadata[&mv_global_id];

    // Promotion occurs while the pin is declaration-based. The serving physical
    // identity differs from the declaration that the follower will first bind.
    let storage = crate::durable::TestCatalogStateBuilder::new(persist.clone())
        .with_organization_id(organization)
        .with_deploy_generation(1)
        .unwrap_build()
        .await
        .open(mz_ore::now::SYSTEM_TIME().into(), &bootstrap)
        .await
        .expect("promote unmanaged deployment");
    let mut active = Catalog::open_debug_catalog_inner(
        persist.clone(),
        storage,
        mz_ore::now::SYSTEM_TIME.clone(),
        Some(
            format!("local-az1-{organization}-0")
                .parse()
                .expect("environment ID"),
        ),
        &mz_build_info::DUMMY_BUILD_INFO,
        BTreeMap::new(),
        &bootstrap,
        None,
        None,
    )
    .await
    .expect("open promoted catalog");
    commit(
        &mut active,
        vec![Op::CreateClusterReplicaRealization {
            cluster_id,
            replica_id: promoted_id,
            name: "r1".into(),
            config: replica_config,
            owner_id: MZ_SYSTEM_ROLE_ID,
            declaration_id: Some(declaration_id),
        }],
    )
    .await;
    let mut follower = open_follower(&active, persist.clone(), organization).await;
    assert_eq!(follower.state().deployment_generation(), 2);
    assert_eq!(follower.get_cluster(cluster_id).replica_id("r1"), None);
    let target = |catalog: &Catalog| {
        let CatalogItem::MaterializedView(mv) = catalog.get_entry(&mv_id).item() else {
            panic!("pinned MV must survive conversion");
        };
        mv.target_replica
    };
    assert_eq!(
        target(&follower),
        Some(ReplicaTarget::Declaration(declaration_id))
    );
    assert_eq!(
        active
            .state()
            .physical_replica_for_target(cluster_id, ReplicaTarget::Declaration(declaration_id),),
        Some(promoted_id)
    );
    let configure = |config| Op::UpdateClusterConfig {
        id: cluster_id,
        name: "conversion_pin".into(),
        config,
        reconfiguration_audit: None,
        burst_audit: None,
    };
    commit(&mut active, vec![configure(managed)]).await;
    follower
        .sync_to_current_updates()
        .await
        .expect("replay managed conversion without a local realization");
    assert!(follower.get_cluster(cluster_id).is_managed());
    assert_eq!(follower.get_cluster(cluster_id).replica_id("r1"), None);
    assert_eq!(target(&active), Some(ReplicaTarget::Physical(promoted_id)));
    assert_eq!(
        target(&follower),
        Some(ReplicaTarget::Declaration(declaration_id))
    );

    // No managed promotion or local realization intervenes. Incremental replay
    // must repair the unresolved pin when shared intent returns under a new ID.
    commit(&mut active, vec![configure(unmanaged)]).await;
    follower
        .sync_to_current_updates()
        .await
        .expect("replay unmanaged conversion");
    let reconstructed = open_follower(&active, persist, organization).await;
    assert_eq!(
        follower.observed_position(),
        reconstructed.observed_position()
    );
    assert_eq!(
        target(&reconstructed),
        Some(ReplicaTarget::Declaration(promoted_id))
    );
    assert_eq!(target(&follower), target(&reconstructed));
    for catalog in [&follower, &reconstructed] {
        let state = catalog.state();
        assert!(!catalog.get_cluster(cluster_id).is_managed());
        assert_eq!(catalog.get_cluster(cluster_id).replica_id("r1"), None);
        assert!(
            !state.replica_target_exists(cluster_id, ReplicaTarget::Declaration(declaration_id))
        );
        assert!(state.replica_target_exists(cluster_id, ReplicaTarget::Declaration(promoted_id)));
        assert_eq!(
            state.storage_metadata().collection_metadata[&mv_global_id],
            mv_shard
        );
        let CatalogItem::MaterializedView(mv) = state.get_entry(&mv_id).item() else {
            panic!("pinned MV must survive conversion");
        };
        let CatalogItem::MaterializedView(original_mv) = original.get_entry(&mv_id).item() else {
            panic!("original pinned MV");
        };
        assert_eq!(mv.create_sql, original_mv.create_sql);
        state.check_consistency().expect("consistent follower");
    }
    reconstructed.expire().await;
    follower.expire().await;
    active.expire().await;
    original.expire().await;
}

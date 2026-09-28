// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeMap;

use mz_controller_types::{ClusterId, ReplicaId};
use mz_persist_client::PersistClient;
use mz_sql::names::CommentObjectId;
use mz_sql::session::user::MZ_SYSTEM_ROLE_ID;
use uuid::Uuid;

use super::{DropObjectInfo, ReplicaCreateDropReason};
use crate::catalog::{Catalog, Op};
use crate::memory::objects::{ClusterConfig, ClusterVariant};

async fn commit(catalog: &mut Catalog, ops: Vec<Op>) {
    catalog
        .transact(None, catalog.current_upper().await, None, ops)
        .await
        .expect("commit replica comment operation");
    catalog
        .state()
        .check_consistency()
        .expect("consistent catalog");
}

fn assert_comment(catalog: &Catalog, cluster_id: ClusterId, expected: Option<(ReplicaId, &str)>) {
    let comments: Vec<_> = catalog
        .state()
        .comments
        .iter()
        .filter_map(|(id, sub, text)| match id {
            CommentObjectId::ClusterReplica((cluster, replica)) if cluster == cluster_id => {
                assert_eq!(sub, None);
                Some((replica, text))
            }
            _ => None,
        })
        .collect();
    assert_eq!(comments, expected.into_iter().collect::<Vec<_>>());
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)]
async fn replica_comments_survive_promotion_retirement_and_conversion() {
    let persist = PersistClient::new_for_tests().await;
    let organization = Uuid::new_v4();
    let bootstrap = crate::catalog::test_bootstrap_args();
    let mut catalog = Catalog::open_debug_catalog(persist.clone(), organization, &bootstrap)
        .await
        .expect("open original deployment");
    let template = catalog
        .user_cluster_replicas()
        .next()
        .expect("bootstrap replica");
    let replica_config = template.config.clone();
    let managed = catalog.get_cluster(template.cluster_id).config.clone();
    assert!(matches!(managed.variant, ClusterVariant::Managed(_)));
    let unmanaged = ClusterConfig {
        variant: ClusterVariant::Unmanaged,
        workload_class: None,
    };
    let cluster_id = catalog
        .allocate_user_cluster_id(catalog.current_upper().await)
        .await
        .expect("allocate cluster");
    let ids = catalog
        .allocate_user_replica_ids(2, catalog.current_upper().await)
        .await
        .expect("allocate replicas");
    let replica_id = ids[0];
    let comment = |replica_id, text: Option<&str>| Op::Comment {
        object_id: CommentObjectId::ClusterReplica((cluster_id, replica_id)),
        sub_component: None,
        comment: text.map(str::to_owned),
    };
    let realize = || Op::CreateClusterReplicaRealization {
        cluster_id,
        replica_id,
        name: "r1".into(),
        config: replica_config.clone(),
        owner_id: MZ_SYSTEM_ROLE_ID,
        carryover_from: None,
    };
    let configure = |config| Op::UpdateClusterConfig {
        id: cluster_id,
        name: "commented".into(),
        config,
        reconfiguration_audit: None,
        burst_audit: None,
    };
    commit(
        &mut catalog,
        vec![
            Op::CreateCluster {
                id: cluster_id,
                name: "commented".into(),
                introspection_sources: crate::builtin::BUILTINS::logs().collect(),
                owner_id: MZ_SYSTEM_ROLE_ID,
                config: unmanaged.clone(),
            },
            Op::CreateClusterReplica {
                cluster_id,
                replica_id,
                name: "r1".into(),
                config: replica_config.clone(),
                owner_id: MZ_SYSTEM_ROLE_ID,
                reason: ReplicaCreateDropReason::Manual,
            },
            comment(replica_id, Some("acknowledged")),
        ],
    )
    .await;

    let storage = crate::durable::TestCatalogStateBuilder::new(persist.clone())
        .with_organization_id(organization)
        .with_deploy_generation(1)
        .unwrap_build()
        .await
        .open(mz_ore::now::SYSTEM_TIME().into(), &bootstrap)
        .await
        .expect("promote deployment");
    let mut promoted = Catalog::open_debug_catalog_inner(
        persist,
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
    assert_comment(&promoted, cluster_id, Some((replica_id, "acknowledged")));
    commit(&mut promoted, vec![realize()]).await;
    assert_eq!(
        promoted.get_cluster(cluster_id).replica_id("r1"),
        Some(replica_id)
    );
    assert_eq!(
        promoted.state().comment_id_to_audit_log_name(
            CommentObjectId::ClusterReplica((cluster_id, replica_id)),
            &crate::SYSTEM_CONN_ID,
        ),
        "commented.r1"
    );
    commit(&mut promoted, vec![comment(replica_id, None)]).await;
    assert_comment(&promoted, cluster_id, None);
    commit(&mut promoted, vec![comment(replica_id, Some("shared"))]).await;
    assert_comment(&promoted, cluster_id, Some((replica_id, "shared")));

    // A declaration comment remains valid with no local realization.
    commit(
        &mut promoted,
        vec![Op::DropClusterReplicaRealization {
            cluster_id,
            replica_id,
        }],
    )
    .await;
    assert_comment(&promoted, cluster_id, Some((replica_id, "shared")));
    commit(&mut promoted, vec![realize()]).await;

    // Conversion must see a comment written earlier in the same transaction.
    commit(
        &mut promoted,
        vec![
            comment(replica_id, Some("managed")),
            configure(managed.clone()),
        ],
    )
    .await;
    assert_comment(&promoted, cluster_id, Some((replica_id, "managed")));
    assert!(
        !promoted
            .state()
            .replica_declarations()
            .any(|declaration| declaration.replica_id == replica_id)
    );
    assert_eq!(
        promoted.get_cluster(cluster_id).replica_id("r1"),
        Some(replica_id)
    );
    // Controller retirement ends only its deployment's membership, even when
    // it is the active deployment. The peer still owns this logical identity.
    commit(
        &mut promoted,
        vec![Op::DropObjects(vec![DropObjectInfo::ClusterReplica((
            cluster_id,
            replica_id,
            ReplicaCreateDropReason::Retired,
        ))])],
    )
    .await;
    assert_eq!(promoted.get_cluster(cluster_id).replica_id("r1"), None);
    assert_comment(&promoted, cluster_id, Some((replica_id, "managed")));
    assert!(
        promoted
            .state()
            .replica_target_exists(cluster_id, replica_id)
    );
    commit(
        &mut promoted,
        vec![Op::CreateClusterReplicaRealization {
            cluster_id,
            replica_id,
            name: "r1".into(),
            config: replica_config.clone(),
            owner_id: MZ_SYSTEM_ROLE_ID,
            carryover_from: Some(0),
        }],
    )
    .await;
    commit(&mut promoted, vec![configure(unmanaged)]).await;
    assert_comment(&promoted, cluster_id, Some((replica_id, "managed")));
    assert!(
        promoted
            .state()
            .replica_declarations()
            .any(|declaration| declaration.replica_id == replica_id)
    );
    assert_eq!(
        promoted.get_cluster(cluster_id).replica_id("r1"),
        Some(replica_id)
    );

    // Shared DROP removes the logical identity and its comment.
    commit(
        &mut promoted,
        vec![Op::DropClusterReplicaRealization {
            cluster_id,
            replica_id,
        }],
    )
    .await;
    commit(&mut promoted, vec![realize()]).await;
    commit(
        &mut promoted,
        vec![Op::DropObjects(vec![DropObjectInfo::ClusterReplica((
            cluster_id,
            replica_id,
            ReplicaCreateDropReason::Manual,
        ))])],
    )
    .await;
    assert_comment(&promoted, cluster_id, None);

    assert!(
        !promoted
            .state()
            .replica_target_exists(cluster_id, replica_id)
    );

    // A private replica comment has no lifetime beyond its last realization.
    let managed_id = ids[1];
    assert_ne!(managed_id, replica_id);
    commit(
        &mut promoted,
        vec![
            configure(managed),
            Op::CreateClusterReplicaRealization {
                cluster_id,
                replica_id: managed_id,
                name: "r1".into(),
                config: replica_config,
                owner_id: MZ_SYSTEM_ROLE_ID,
                carryover_from: None,
            },
            comment(managed_id, Some("physical")),
        ],
    )
    .await;
    commit(
        &mut promoted,
        vec![Op::DropClusterReplicaRealization {
            cluster_id,
            replica_id: managed_id,
        }],
    )
    .await;
    assert_comment(&promoted, cluster_id, None);
}

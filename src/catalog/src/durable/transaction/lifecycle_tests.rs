// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

#![allow(clippy::unwrap_used)]

use super::*;

use mz_controller_types::clusters::ReplicaLogging;
use mz_persist_client::PersistClient;

use crate::durable::objects::ClusterVariant;
use crate::durable::{ReplicaLocation, TestCatalogStateBuilder, test_bootstrap_args};

const COLLECTION: GlobalId = GlobalId::User(1000);
const CLUSTER: ClusterId = ClusterId::User(1000);
const OTHER_CLUSTER: ClusterId = ClusterId::User(1001);
const REPLICA: ReplicaId = ReplicaId::User(1000);
const OTHER_REPLICA: ReplicaId = ReplicaId::User(1001);

fn insert_replica(tx: &mut Transaction<'_>, cluster: ClusterId, replica: ReplicaId) {
    tx.insert_cluster_replica_with_id(
        cluster,
        replica,
        "r1",
        ReplicaConfig {
            location: ReplicaLocation::Unmanaged {
                storagectl_addrs: Vec::new(),
                computectl_addrs: Vec::new(),
            },
            logging: ReplicaLogging {
                log_logging: false,
                interval: None,
            },
            arrangement_compression: false,
        },
        RoleId::User(1),
    )
    .unwrap();
}

fn protect(tx: &mut Transaction<'_>, replica: Option<ReplicaId>) -> u64 {
    let client = tx.replace_client_incarnation(replica, None).unwrap();
    tx.publish_client_read_requirements(client, BTreeMap::from([(COLLECTION, 20.into())]))
        .unwrap();
    client
}

async fn commit(mut tx: Transaction<'_>) {
    tx.finalize_index_compaction_bounds();
    let _ = tx.get_and_commit_op_updates();
    let ts = tx.upper();
    tx.commit(ts).await.unwrap();
}

#[derive(Clone, Copy, Debug)]
enum Removal {
    Scoped,
    Qualified,
    Cluster,
}

impl Removal {
    fn apply(self, tx: &mut Transaction<'_>) {
        match self {
            Self::Scoped => tx.remove_cluster_replica(REPLICA),
            Self::Qualified => tx.remove_cluster_replicas(&BTreeSet::from([
                ClusterReplicaKey {
                    id: REPLICA,
                    deployment_generation: 7,
                },
                ClusterReplicaKey {
                    id: REPLICA,
                    deployment_generation: 8,
                },
            ])),
            Self::Cluster => tx.remove_clusters(&BTreeSet::from([CLUSTER])),
        }
        .unwrap();
    }
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)]
async fn realization_replacement_and_removal_preserve_other_owners() {
    for removal in [Removal::Scoped, Removal::Qualified, Removal::Cluster] {
        let version = semver::Version::new(0, 0, 0);
        let builder = TestCatalogStateBuilder::new(PersistClient::new_for_tests().await)
            .with_version(version.clone())
            .with_deploy_generation(7);
        let mut state = builder
            .clone()
            .unwrap_build()
            .await
            .open(SYSTEM_TIME().into(), &test_bootstrap_args())
            .await
            .unwrap();
        state.sync_to_current_updates().await.unwrap();
        let mut tx = state.transaction().await.unwrap();
        tx.set_config("catalog_read_protection_enabled".into(), Some(1))
            .unwrap();
        for (cluster, name) in [(CLUSTER, "retired"), (OTHER_CLUSTER, "unrelated")] {
            tx.insert_user_cluster(
                cluster,
                name,
                Vec::new(),
                RoleId::User(1),
                Vec::new(),
                ClusterConfig {
                    variant: ClusterVariant::Unmanaged,
                    workload_class: None,
                },
                &Default::default(),
            )
            .unwrap();
        }
        // A fresh storage collection is readable from the minimum timestamp.
        // Its birth bound needs neither index inputs nor an existing read grant.
        tx.insert_collection_metadata(BTreeMap::from([(COLLECTION, ShardId::new())]))
            .unwrap();
        tx.set_collection_compaction_bound(COLLECTION, Some(0.into()))
            .unwrap();
        insert_replica(&mut tx, CLUSTER, REPLICA);
        insert_replica(&mut tx, OTHER_CLUSTER, OTHER_REPLICA);
        let predecessor = protect(&mut tx, Some(REPLICA));
        let unrelated = protect(&mut tx, Some(OTHER_REPLICA));
        let adapter = protect(&mut tx, None);
        commit(tx).await;

        let mut tx = state.transaction().await.unwrap();
        let affected = tx
            .replace_client_incarnation(Some(REPLICA), Some(predecessor))
            .unwrap();
        assert_ne!(affected, predecessor);
        commit(tx).await;
        let transferred = state.snapshot().await.unwrap();
        assert!(
            !transferred
                .client_incarnations
                .contains_key(&ClientIncarnationKey { id: predecessor })
        );
        assert!(
            !transferred
                .client_read_requirements
                .contains_key(&ClientReadRequirementKey {
                    incarnation: predecessor,
                    id: COLLECTION,
                })
        );
        // A losing startup attempt must retain its predecessor expectation, not
        // take authority from the execution that won while it was retrying.
        let mut tx = state.transaction().await.unwrap();
        assert!(
            tx.replace_client_incarnation(Some(REPLICA), Some(predecessor))
                .is_err()
        );
        assert!(
            tx.publish_client_read_requirements(predecessor, BTreeMap::new())
                .is_err()
        );
        drop(tx);
        assert_eq!(state.snapshot().await.unwrap(), transferred);

        // A prewarming peer retains independent protection under the same public
        // replica ID. Neither joining nor retiring our realization promotes it.
        let mut prewarmer = builder
            .with_deploy_generation(8)
            .unwrap_build()
            .await
            .join_prewarming(&version.to_string())
            .await
            .unwrap();
        prewarmer.sync_to_current_updates().await.unwrap();
        let mut tx = prewarmer.transaction().await.unwrap();
        insert_replica(&mut tx, CLUSTER, REPLICA);
        let peer = protect(&mut tx, Some(REPLICA));
        commit(tx).await;
        prewarmer.expire().await;
        state.sync_to_current_updates().await.unwrap();

        let before = state.snapshot().await.unwrap();
        for client in [peer, affected, unrelated, adapter] {
            assert!(
                before
                    .client_incarnations
                    .contains_key(&ClientIncarnationKey { id: client })
            );
            assert_eq!(
                before
                    .client_read_requirements
                    .get(&ClientReadRequirementKey {
                        incarnation: client,
                        id: COLLECTION,
                    }),
                Some(&ClientReadRequirementValue {
                    frontier: 20.into()
                }),
            );
        }

        // Abandoning the removal must preserve both membership and protection.
        let mut tx = state.transaction().await.unwrap();
        removal.apply(&mut tx);
        drop(tx);
        assert_eq!(state.snapshot().await.unwrap(), before, "{removal:?}");

        let mut tx = state.transaction().await.unwrap();
        removal.apply(&mut tx);
        commit(tx).await;
        let after = state.snapshot().await.unwrap();
        let mut retired = BTreeSet::from([affected]);
        if !matches!(removal, Removal::Scoped) {
            retired.insert(peer);
        }
        let mut expected = before;
        for client in &retired {
            expected
                .client_incarnations
                .remove(&ClientIncarnationKey { id: *client });
            expected
                .client_read_requirements
                .remove(&ClientReadRequirementKey {
                    incarnation: *client,
                    id: COLLECTION,
                });
        }
        expected.cluster_replicas.remove(&ClusterReplicaKey {
            id: REPLICA,
            deployment_generation: 7,
        });
        if !matches!(removal, Removal::Scoped) {
            expected.cluster_replicas.remove(&ClusterReplicaKey {
                id: REPLICA,
                deployment_generation: 8,
            });
        }
        if matches!(removal, Removal::Cluster) {
            expected.clusters.remove(&ClusterKey { id: CLUSTER });
        }
        assert_eq!(
            after.cluster_replicas, expected.cluster_replicas,
            "{removal:?}"
        );
        assert_eq!(after.clusters, expected.clusters, "{removal:?}");
        assert_eq!(
            after.client_incarnations, expected.client_incarnations,
            "{removal:?}"
        );
        assert_eq!(
            after.client_read_requirements, expected.client_read_requirements,
            "{removal:?}"
        );

        let mut tx = state.transaction().await.unwrap();
        for client in retired {
            for requirements in [BTreeMap::new(), BTreeMap::from([(COLLECTION, 20.into())])] {
                assert!(
                    matches!(
                        tx.publish_client_read_requirements(client, requirements),
                        Err(CatalogError::Durable(
                            DurableCatalogError::InvalidReadProtection(_)
                        ))
                    ),
                    "{removal:?}: retired client {client} published"
                );
            }
        }
        drop(tx);
        assert_eq!(state.snapshot().await.unwrap(), after);
        state.expire().await;
    }
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)]
async fn replacement_retains_index_history_without_granting_earlier_reads() {
    let other_index = GlobalId::User(1001);
    let mut state = TestCatalogStateBuilder::new(PersistClient::new_for_tests().await)
        .with_default_deploy_generation()
        .expect_build("build index retention catalog")
        .await
        .open(SYSTEM_TIME().into(), &test_bootstrap_args())
        .await
        .expect("open index retention catalog");
    state
        .sync_to_current_updates()
        .await
        .expect("sync bootstrapped catalog");
    let mut tx = state.transaction().await.expect("begin index creation");
    tx.set_config("catalog_read_protection_enabled".into(), Some(1))
        .expect("enable read protection");
    for (n, id) in [(1000, COLLECTION), (1001, other_index)] {
        tx.insert_item(
            CatalogItemId::User(n),
            20000 + u32::try_from(n).expect("fixture ID fits an OID"),
            id,
            SchemaId::User(1),
            &format!("retained_idx_{n}"),
            format!("CREATE INDEX retained_idx_{n} ON t (a)"),
            RoleId::User(1),
            Vec::new(),
            BTreeMap::new(),
            None,
        )
        .expect("insert durable index definition");
    }
    let predecessor = tx
        .replace_client_incarnation(None, None)
        .expect("create initial execution");
    tx.publish_client_read_requirements(predecessor, BTreeMap::from([(COLLECTION, 20.into())]))
        .expect("publish initial index grant");
    commit(tx).await;
    let mut tx = state.transaction().await.expect("begin permission advance");
    for id in [COLLECTION, other_index] {
        tx.set_collection_compaction_bound(id, Some(15.into()))
            .expect("advance index permission");
    }
    commit(tx).await;
    let before = state
        .snapshot()
        .await
        .expect("snapshot committed permission");

    // Rekeying cannot admit either an earlier existing grant or a new target
    // below its committed permission, even through an intermediate execution.
    for target in [COLLECTION, other_index] {
        let mut tx = state
            .transaction()
            .await
            .expect("begin invalid acquisition");
        let intermediate = tx
            .replace_client_incarnation(None, Some(predecessor))
            .expect("replace initial execution");
        let successor = tx
            .replace_client_incarnation(None, Some(intermediate))
            .expect("replace intermediate execution");
        let mut requirements = BTreeMap::from([(COLLECTION, 20.into())]);
        requirements.insert(target, 14.into());
        tx.publish_client_read_requirements(successor, requirements)
            .expect("stage invalid acquisition for commit validation");
        tx.finalize_index_compaction_bounds();
        let _ = tx.get_and_commit_op_updates();
        let ts = tx.upper();
        let error = tx
            .commit(ts)
            .await
            .expect_err("reject read below permission");
        assert!(
            matches!(
                error,
                CatalogError::Durable(DurableCatalogError::InvalidReadProtection(ref detail))
                    if detail.contains("readable at 14")
            ),
            "{error}"
        );
        assert_eq!(
            state
                .snapshot()
                .await
                .expect("snapshot rejected replacement"),
            before,
        );
    }

    let mut tx = state.transaction().await.expect("begin index retirement");
    tx.remove_item(CatalogItemId::User(1000))
        .expect("drop protected index");
    commit(tx).await;
    let retired = state.snapshot().await.expect("snapshot retired index");
    assert!(
        !retired
            .collection_compaction_bounds
            .contains_key(&CollectionCompactionBoundKey { id: COLLECTION })
    );

    // Predecessor history is not permission to acquire an earlier grant after
    // retirement, even though the index's compaction bound is now absent.
    let mut tx = state
        .transaction()
        .await
        .expect("begin retired acquisition");
    let successor = tx
        .replace_client_incarnation(None, Some(predecessor))
        .expect("replace retired index reader");
    tx.publish_client_read_requirements(successor, BTreeMap::from([(COLLECTION, 19.into())]))
        .expect("stage earlier retired index grant");
    tx.finalize_index_compaction_bounds();
    let _ = tx.get_and_commit_op_updates();
    let ts = tx.upper();
    let error = tx
        .commit(ts)
        .await
        .expect_err("reject fresh retired index grant");
    assert!(
        matches!(
            error,
            CatalogError::Durable(DurableCatalogError::InvalidReadProtection(ref detail))
                if detail.contains("no live storage or index identity")
        ),
        "{error}"
    );
    assert_eq!(
        state
            .snapshot()
            .await
            .expect("snapshot rejected retired acquisition"),
        retired,
    );

    // A dropped index has no permission for fresh grants. Its retained history
    // must nevertheless survive both a single replacement and a chain.
    let mut current = predecessor;
    for replacements in [1, 2] {
        let before = state.snapshot().await.expect("snapshot retained history");
        let mut tx = state.transaction().await.expect("begin history transfer");
        for _ in 0..replacements {
            current = tx
                .replace_client_incarnation(None, Some(current))
                .expect("rekey retained index history");
        }
        commit(tx).await;
        let after = state
            .snapshot()
            .await
            .expect("snapshot transferred history");
        assert_eq!(
            after.client_read_requirements,
            imbl::OrdMap::from_iter([(
                ClientReadRequirementKey {
                    incarnation: current,
                    id: COLLECTION,
                },
                ClientReadRequirementValue {
                    frontier: 20.into()
                },
            )]),
        );
        assert_eq!(after.items, before.items);
        assert_eq!(
            after.collection_compaction_bounds,
            before.collection_compaction_bounds
        );
    }
    state.expire().await;
}

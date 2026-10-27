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
use crate::durable::{
    DEFAULT_ADAPTER_ID, ReplicaLocation, TestCatalogStateBuilder, test_bootstrap_args,
};

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

fn protect(tx: &mut Transaction<'_>, realization: ClientRealization) -> u64 {
    let client = tx.replace_client_incarnation(realization, None).unwrap();
    tx.grant_client_read_requirements(client, BTreeMap::from([(COLLECTION, 20.into())]))
        .unwrap();
    client
}

async fn commit(mut tx: Transaction<'_>) {
    tx.finalize_index_compaction_bounds();
    let _ = tx.get_and_commit_op_updates();
    let ts = tx.upper();
    tx.commit(ts).await.unwrap();
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)]
async fn adapter_replacement_and_drop_preserve_other_adapter() {
    let mut state = TestCatalogStateBuilder::new(PersistClient::new_for_tests().await)
        .with_default_deploy_generation()
        .unwrap_build()
        .await
        .open(SYSTEM_TIME().into(), &test_bootstrap_args())
        .await
        .unwrap();
    state.sync_to_current_updates().await.unwrap();
    let empty_retirement = state.client_incarnation_retirement().unwrap();
    assert!(empty_retirement.next_id.is_none());
    let mut tx = state.transaction().await.unwrap();
    tx.set_config("catalog_read_protection_enabled".into(), Some(1))
        .unwrap();
    let a = DEFAULT_ADAPTER_ID;
    let b = tx.allocate_adapter_id().unwrap();
    assert_ne!(a, b);
    tx.set_adapter_declaration(AdapterDeclaration {
        id: b,
        name: "other_adapter".into(),
        size: tx.get_adapter_declaration(a).unwrap().size,
    })
    .unwrap();
    tx.insert_collection_metadata(BTreeMap::from([(COLLECTION, ShardId::new())]))
        .unwrap();
    tx.set_collection_compaction_bound(COLLECTION, Some(0.into()))
        .unwrap();
    let first = protect(&mut tx, ClientRealization::Adapter(a));
    let other = protect(&mut tx, ClientRealization::Adapter(b));
    commit(tx).await;
    let before = state.snapshot().await.unwrap();
    let before_retirement = state.client_incarnation_retirement().unwrap();
    assert!(!empty_retirement.is_retired(first));
    assert!(!empty_retirement.is_retired(other));

    let mut tx = state.transaction().await.unwrap();
    let successor = tx
        .replace_client_incarnation(ClientRealization::Adapter(a), Some(first))
        .unwrap();
    assert_ne!(successor, first);
    commit(tx).await;
    let replaced = state.snapshot().await.unwrap();
    let replaced_retirement = state.client_incarnation_retirement().unwrap();
    // A newly admitted identity missing from an older prefix is unknown, not retired.
    assert!(!before_retirement.is_retired(successor));
    assert!(!before_retirement.is_retired(first));
    assert!(replaced_retirement.is_retired(first));
    assert!(!replaced_retirement.is_retired(successor));
    assert!(!replaced_retirement.is_retired(other));
    let mut expected_clients = before.client_incarnations;
    let execution = expected_clients
        .remove(&ClientIncarnationKey { id: first })
        .unwrap();
    expected_clients.insert(ClientIncarnationKey { id: successor }, execution);
    let mut expected_grants = before.client_read_requirements;
    let grant = expected_grants
        .remove(&ClientReadRequirementKey {
            incarnation: first,
            id: COLLECTION,
        })
        .unwrap();
    expected_grants.insert(
        ClientReadRequirementKey {
            incarnation: successor,
            id: COLLECTION,
        },
        grant,
    );
    assert_eq!(replaced.client_incarnations, expected_clients);
    assert_eq!(replaced.client_read_requirements, expected_grants);
    assert_eq!(replaced.adapter_declarations, before.adapter_declarations);

    let mut tx = state.transaction().await.unwrap();
    assert_eq!(tx.remove_adapter_declaration(a).unwrap().id, a);
    commit(tx).await;
    let dropped = state.snapshot().await.unwrap();
    let dropped_retirement = state.client_incarnation_retirement().unwrap();
    assert!(dropped_retirement.is_retired(first));
    assert!(dropped_retirement.is_retired(successor));
    assert!(!dropped_retirement.is_retired(other));
    expected_clients.remove(&ClientIncarnationKey { id: successor });
    expected_grants.remove(&ClientReadRequirementKey {
        incarnation: successor,
        id: COLLECTION,
    });
    assert_eq!(dropped.client_incarnations, expected_clients);
    assert_eq!(dropped.client_read_requirements, expected_grants);
    let mut expected_declarations = replaced.adapter_declarations;
    expected_declarations.remove(&AdapterDeclarationKey { id: a });
    assert_eq!(dropped.adapter_declarations, expected_declarations);

    let mut tx = state.transaction().await.unwrap();
    for expected in [None, Some(first), Some(successor)] {
        assert!(matches!(
            tx.replace_client_incarnation(ClientRealization::Adapter(a), expected),
            Err(CatalogError::Durable(
                DurableCatalogError::InvalidReadProtection(_)
            ))
        ));
    }
    for retired in [first, successor] {
        assert!(
            tx.grant_client_read_requirements(retired, BTreeMap::from([(COLLECTION, 20.into())]))
                .is_err()
        );
        assert!(
            tx.update_client_read_requirements(retired, BTreeMap::from([(COLLECTION, None)]))
                .is_err()
        );
    }
    // B can still publish using its original execution after A is removed.
    assert_eq!(
        tx.grant_client_read_requirements(other, BTreeMap::from([(COLLECTION, 20.into())]))
            .unwrap(),
        BTreeMap::from([(COLLECTION, 20.into())]),
    );
    commit(tx).await;
    assert_eq!(state.snapshot().await.unwrap(), dropped);
    state.expire().await;
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)]
async fn reopening_empty_adapter_inventory_does_not_initialize_again() {
    let builder = TestCatalogStateBuilder::new(PersistClient::new_for_tests().await)
        .with_default_deploy_generation();
    let mut options = test_bootstrap_args();
    options.default_adapter_size = "test-size".into();
    let mut state = builder
        .clone()
        .unwrap_build()
        .await
        .open(SYSTEM_TIME().into(), &options)
        .await
        .unwrap();
    state.sync_to_current_updates().await.unwrap();
    let mut tx = state.transaction().await.unwrap();
    let default = tx.remove_adapter_declaration(DEFAULT_ADAPTER_ID).unwrap();
    assert_eq!(default.name, "default");
    assert_eq!(default.size, "test-size");
    assert_eq!(tx.get_adapter_declarations().count(), 0);
    commit(tx).await;
    state.expire().await;

    let mut reopened = builder
        .unwrap_build()
        .await
        .open(SYSTEM_TIME().into(), &options)
        .await
        .unwrap();
    reopened.sync_to_current_updates().await.unwrap();
    let mut tx = reopened.transaction().await.unwrap();
    assert_eq!(tx.get_adapter_declarations().count(), 0);
    assert!(tx.allocate_adapter_id().unwrap() > DEFAULT_ADAPTER_ID);
    drop(tx);
    reopened.expire().await;
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
        let predecessor = protect(&mut tx, ClientRealization::Replica(REPLICA));
        let unrelated = protect(&mut tx, ClientRealization::Replica(OTHER_REPLICA));
        let adapter = protect(&mut tx, ClientRealization::Adapter(DEFAULT_ADAPTER_ID));
        commit(tx).await;

        let mut tx = state.transaction().await.unwrap();
        let affected = tx
            .replace_client_incarnation(ClientRealization::Replica(REPLICA), Some(predecessor))
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
            tx.replace_client_incarnation(ClientRealization::Replica(REPLICA), Some(predecessor))
                .is_err()
        );
        assert!(
            tx.update_client_read_requirements(predecessor, BTreeMap::from([(COLLECTION, None)]))
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
        let peer = protect(&mut tx, ClientRealization::Replica(REPLICA));
        commit(tx).await;
        let warm_retirement = prewarmer.client_incarnation_retirement().unwrap();
        for client in [peer, affected, unrelated, adapter] {
            assert!(!warm_retirement.is_retired(client));
        }
        prewarmer.expire().await;
        state.sync_to_current_updates().await.unwrap();
        let retirement = state.client_incarnation_retirement().unwrap();
        assert!(retirement.is_retired(predecessor));
        for client in [peer, affected, unrelated, adapter] {
            assert!(!retirement.is_retired(client));
        }

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
        let retirement = state.client_incarnation_retirement().unwrap();
        for client in [peer, affected, unrelated, adapter] {
            assert_eq!(retirement.is_retired(client), retired.contains(&client));
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
            for result in [
                tx.grant_client_read_requirements(
                    client,
                    BTreeMap::from([(COLLECTION, 20.into())]),
                )
                .map(|_| ()),
                tx.update_client_read_requirements(client, BTreeMap::from([(COLLECTION, None)])),
            ] {
                assert!(
                    matches!(
                        result,
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
        .replace_client_incarnation(ClientRealization::Adapter(DEFAULT_ADAPTER_ID), None)
        .expect("create initial execution");
    tx.grant_client_read_requirements(predecessor, BTreeMap::from([(COLLECTION, 20.into())]))
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
            .replace_client_incarnation(
                ClientRealization::Adapter(DEFAULT_ADAPTER_ID),
                Some(predecessor),
            )
            .expect("replace initial execution");
        let successor = tx
            .replace_client_incarnation(
                ClientRealization::Adapter(DEFAULT_ADAPTER_ID),
                Some(intermediate),
            )
            .expect("replace intermediate execution");
        let mut requirements = BTreeMap::from([(COLLECTION, 20.into())]);
        requirements.insert(target, 14.into());
        tx.grant_client_read_requirements(successor, requirements)
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
        .replace_client_incarnation(
            ClientRealization::Adapter(DEFAULT_ADAPTER_ID),
            Some(predecessor),
        )
        .expect("replace retired index reader");
    tx.grant_client_read_requirements(successor, BTreeMap::from([(COLLECTION, 19.into())]))
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
                .replace_client_incarnation(
                    ClientRealization::Adapter(DEFAULT_ADAPTER_ID),
                    Some(current),
                )
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

// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Native catalog metadata contracts. These tests do not exercise expression
//! bytes or controller writer ordering.

use std::collections::{BTreeMap, BTreeSet};

use mz_controller_types::ReplicaId;
use mz_persist_client::PersistClient;
use mz_repr::GlobalId;
use uuid::Uuid;

use super::{DropObjectInfo, ReplicaCreateDropReason};
use crate::catalog::{Catalog, CatalogError, Op};
use crate::durable::objects::ReplicaPlanOwner;
use crate::memory::error::{Error, ErrorKind};

async fn protected_catalog() -> (Catalog, PersistClient, Uuid) {
    let persist = PersistClient::new_for_tests().await;
    let organization = Uuid::new_v4();
    let catalog = open_protected_catalog(persist.clone(), organization).await;
    (catalog, persist, organization)
}

async fn open_protected_catalog(persist: PersistClient, organization: Uuid) -> Catalog {
    let bootstrap = crate::catalog::test_bootstrap_args();
    let storage = crate::durable::TestCatalogStateBuilder::new(persist.clone())
        .with_organization_id(organization)
        .with_default_deploy_generation()
        .unwrap_build()
        .await
        .open(mz_ore::now::SYSTEM_TIME().into(), &bootstrap)
        .await
        .expect("open durable catalog");
    Catalog::open_debug_catalog_inner(
        persist.clone(),
        storage,
        mz_ore::now::SYSTEM_TIME.clone(),
        Some(
            format!("local-az1-{organization}-0")
                .parse()
                .expect("environment ID"),
        ),
        &mz_build_info::DUMMY_BUILD_INFO,
        BTreeMap::from([("enable_catalog_read_protection".into(), "true".into())]),
        &bootstrap,
        None,
        None,
    )
    .await
    .expect("open protected catalog")
}

#[mz_ore::test(tokio::test(flavor = "multi_thread", worker_threads = 2))]
#[cfg_attr(miri, ignore)]
async fn protected_bootstrap_absorbs_publication_without_replay_cas() {
    use std::sync::{Arc, Mutex};

    for phase in ["before_transaction", "before_commit", "before_replay"] {
        let (catalog, persist, organization) = protected_catalog().await;
        catalog.expire().await;
        let mut peer = crate::durable::TestCatalogStateBuilder::new(persist.clone())
            .with_organization_id(organization)
            .with_default_deploy_generation()
            .unwrap_build()
            .await
            .join()
            .await
            .expect("join bootstrap peer");
        peer.sync_to_current_updates()
            .await
            .expect("consume peer snapshot");
        let retired_index = if phase == "before_commit" {
            use crate::durable::objects::{
                SystemObjectDescription, SystemObjectMapping, SystemObjectUniqueIdentifier,
            };
            let mut tx = peer.transaction().await.expect("prepare retired builtin");
            let (catalog_id, global_id) = tx
                .allocate_system_item_ids(1)
                .expect("allocate builtin identity")[0];
            tx.set_system_object_mappings(vec![SystemObjectMapping {
                description: SystemObjectDescription {
                    schema_name: "mz_internal".into(),
                    object_type: mz_sql::catalog::CatalogItemType::Index,
                    object_name: "mz_bootstrap_retired_test_index".into(),
                },
                unique_identifier: SystemObjectUniqueIdentifier {
                    catalog_id,
                    global_id,
                    fingerprint: "retired test index".into(),
                },
            }])
            .expect("persist retired builtin mapping");
            tx.set_collection_compaction_bound(global_id, Some(mz_repr::Timestamp::MIN))
                .expect("publish builtin bound");
            let _ = tx.get_and_commit_op_updates();
            let ts = tx.upper();
            tx.commit(ts).await.expect("commit retired builtin fixture");
            Some(global_id)
        } else {
            None
        };
        let peer = Arc::new(tokio::sync::Mutex::new(peer));
        let publication = Arc::new(Mutex::new(None));
        let point = format!("catalog_initialize_{phase}_{organization}");
        let handle = tokio::runtime::Handle::current();
        fail::cfg_callback(&point, {
            let peer = Arc::clone(&peer);
            let publication = Arc::clone(&publication);
            move || {
                let mut publication = publication.lock().expect("publication mutex");
                if publication.is_some() {
                    return;
                }
                *publication = Some(tokio::task::block_in_place(|| {
                    handle.block_on(async {
                        let mut peer = peer.lock().await;
                        peer.sync_to_current_updates().await.expect("refresh peer");
                        let mut tx = peer.transaction().await.expect("start peer publication");
                        let incarnation =
                            tx.create_client_incarnation(None).expect("register peer");
                        let _ = tx.get_and_commit_op_updates();
                        let ts = tx.upper();
                        tx.commit(ts).await.expect("publish during bootstrap");
                        (incarnation, peer.current_upper().await)
                    })
                }));
            }
        })
        .expect("install bootstrap rendezvous");
        let mut catalog = open_protected_catalog(persist, organization).await;
        fail::remove(&point);
        let (incarnation, upper) = publication
            .lock()
            .expect("publication mutex")
            .expect("bootstrap crossed rendezvous");
        if phase == "before_replay" {
            assert_eq!(
                catalog.current_upper().await,
                upper,
                "heavy read-only reconstruction must not commit after publication"
            );
            assert!(
                !catalog
                    .state()
                    .client_incarnations()
                    .contains_key(&incarnation)
            );
        } else {
            assert!(
                catalog
                    .state()
                    .client_incarnations()
                    .contains_key(&incarnation)
            );
        }
        catalog
            .sync_to_current_updates()
            .await
            .expect("apply queued publication");
        assert!(
            catalog
                .state()
                .client_incarnations()
                .contains_key(&incarnation)
        );
        catalog
            .check_consistency()
            .expect("consistent reconstructed catalog");
        if let Some(id) = retired_index {
            assert!(
                !catalog
                    .state()
                    .collection_compaction_bounds()
                    .contains_key(&id),
                "retired builtin bound must be removed with its identity"
            );
        }
        catalog.expire().await;
        Arc::try_unwrap(peer)
            .expect("rendezvous released peer")
            .into_inner()
            .expire()
            .await;
    }
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)]
async fn wal_identity_read_preserves_pending_replica_updates() {
    let (mut catalog, persist, organization) = protected_catalog().await;
    let shard = catalog.txn_wal_shard().await.expect("initialized WAL");

    let mut peer = crate::durable::TestCatalogStateBuilder::new(persist)
        .with_organization_id(organization)
        .with_default_deploy_generation()
        .unwrap_build()
        .await
        .join()
        .await
        .expect("join peer writer");
    peer.sync_to_current_updates()
        .await
        .expect("consume peer snapshot");
    let incarnation = {
        let mut tx = peer.transaction().await.expect("start peer publication");
        let incarnation = tx.create_client_incarnation(None).expect("register peer");
        let _ = tx.get_and_commit_op_updates();
        let ts = tx.upper();
        tx.commit(ts).await.expect("commit peer publication");
        incarnation
    };
    assert_eq!(
        catalog
            .txn_wal_shard()
            .await
            .expect("read WAL despite pending metadata"),
        shard
    );
    assert!(
        !catalog
            .state()
            .client_incarnations()
            .contains_key(&incarnation)
    );
    catalog
        .sync_to_current_updates()
        .await
        .expect("apply pending replica metadata");
    assert!(
        catalog
            .state()
            .client_incarnations()
            .contains_key(&incarnation)
    );
    catalog.expire().await;
    peer.expire().await;
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)]
async fn replica_plan_dry_run_returns_contention_for_refresh() {
    const CHILD: &str = "MZ_CATALOG_DRY_RUN_CONTENTION_CHILD";
    const DONE: &str = "dry-run contention refreshed successfully";
    if std::env::var_os(CHILD).is_none() {
        // A fatal catalog error can exit with status zero. Require completion
        // evidence from the child, not just a successful process exit.
        let output = tokio::time::timeout(
            std::time::Duration::from_secs(120),
            tokio::process::Command::new(std::env::current_exe().expect("test binary"))
                .args([
                    "--exact",
                    "catalog::transact::replica_plan_tests::replica_plan_dry_run_returns_contention_for_refresh",
                    "--nocapture",
                ])
                .env(CHILD, "1")
                .kill_on_drop(true)
                .output(),
        )
        .await
        .expect("bounded dry-run test")
        .expect("run child");
        assert!(
            output.status.success() && String::from_utf8_lossy(&output.stdout).contains(DONE),
            "child did not complete: {}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr),
        );
        return;
    }
    Box::pin(exercise_dry_run_contention()).await;
    println!("{DONE}");
}

async fn exercise_dry_run_contention() {
    let (mut catalog, persist, organization) = Box::pin(protected_catalog()).await;
    let cluster = catalog.user_clusters().next().expect("bootstrap cluster");
    let replica = cluster
        .replicas()
        .next()
        .expect("bootstrap replica")
        .replica_id;
    let id = GlobalId::Transient(1_000_010);
    let op = select(
        id,
        &Catalog::expression_build_version(catalog.config().build_info).to_string(),
        None,
        Uuid::new_v4(),
        Some(ReplicaPlanOwner {
            replica_id: replica,
            name: "contention_metric".into(),
        }),
        cluster.log_indexes.values().copied().collect(),
    );
    let storage = crate::durable::TestCatalogStateBuilder::new(persist.clone())
        .with_organization_id(organization)
        .with_default_deploy_generation()
        .unwrap_build()
        .await
        .join()
        .await
        .expect("join peer");
    let state = catalog.replica_config().into_state(
        catalog.config().build_info,
        catalog.config().environment_id.clone(),
        catalog.config().connection_context.clone(),
        persist,
    );
    let mut peer = Box::pin(Catalog::open_committed(state, storage))
        .await
        .expect("open peer")
        .catalog;
    let revision = catalog.transient_revision();
    peer.transact(
        None,
        peer.current_upper().await,
        None,
        vec![Op::CreateClientIncarnation {
            replica_id: Some(replica),
        }],
    )
    .await
    .expect("peer metadata publication");
    let error = catalog
        .transact_incremental_dry_run(
            catalog.state(),
            vec![op.clone()],
            None,
            None,
            catalog.current_upper().await,
        )
        .await
        .expect_err("stale dry run must ask the caller to refresh");
    assert!(matches!(error, CatalogError::Catalog(error) if matches!(
        error.kind, ErrorKind::Durable(crate::durable::DurableCatalogError::CatalogOutOfSync { .. })
    )));
    catalog
        .sync_to_current_updates()
        .await
        .expect("refresh peer metadata");
    assert_eq!(
        catalog.transient_revision(),
        revision,
        "metadata does not invalidate planning"
    );
    let (candidate, _) = catalog
        .transact_incremental_dry_run(
            catalog.state(),
            vec![op],
            None,
            None,
            catalog.current_upper().await,
        )
        .await
        .expect("revalidated dry run");
    assert!(
        candidate
            .written_plans()
            .keys()
            .any(|(selected, _)| *selected == id)
    );
}

fn select(
    id: GlobalId,
    build: &str,
    expected_revision: Option<Uuid>,
    revision: Uuid,
    replica_owner: Option<ReplicaPlanOwner>,
    imports: BTreeSet<GlobalId>,
) -> Op {
    Op::SetWrittenPlan {
        id,
        build_version: build.into(),
        expected_revision,
        revision: Some(revision),
        imports,
        replica_owner,
    }
}

#[mz_ore::test(tokio::test)]
async fn replica_plan_scope_survives_reopen_and_drop_is_build_local() {
    for drop_cluster in [false, true] {
        let (mut catalog, persist, organization) = protected_catalog().await;
        let cluster = catalog
            .user_clusters()
            .next()
            .expect("bootstrap user cluster");
        let cluster_id = cluster.id;
        assert!(
            cluster.bound_objects.is_empty(),
            "drop fixture has no SQL dependents"
        );
        let replica_id = cluster
            .replicas()
            .next()
            .expect("bootstrap replica")
            .replica_id;
        let imports: BTreeSet<_> = cluster.log_indexes.values().copied().collect();
        assert!(!imports.is_empty(), "exercise real cluster log imports");
        let owner = ReplicaPlanOwner {
            replica_id,
            name: "observer".into(),
        };
        let other_cluster = catalog
            .clusters()
            .find(|c| c.id != cluster_id)
            .expect("another cluster");
        let other_owner = ReplicaPlanOwner {
            replica_id: other_cluster
                .replicas()
                .next()
                .expect("another replica")
                .replica_id,
            name: owner.name.clone(),
        };
        let other_imports = other_cluster.log_indexes.values().copied().collect();
        let build =
            Catalog::expression_build_version(catalog.state().config().build_info).to_string();
        let foreign_build = format!("{build}-other");
        let id = GlobalId::Transient(1_000_000);
        let other_id = GlobalId::Transient(1_000_001);
        let revision = Uuid::new_v4();
        let foreign_revision = Uuid::new_v4();
        let other_revision = Uuid::new_v4();
        let ts = catalog.current_upper().await;
        catalog
            .transact(
                None,
                ts,
                None,
                vec![
                    select(
                        id,
                        &build,
                        None,
                        revision,
                        Some(owner.clone()),
                        imports.clone(),
                    ),
                    select(
                        id,
                        &foreign_build,
                        None,
                        foreign_revision,
                        Some(owner.clone()),
                        imports,
                    ),
                    select(
                        other_id,
                        &build,
                        None,
                        other_revision,
                        Some(other_owner.clone()),
                        other_imports,
                    ),
                ],
            )
            .await
            .expect("log-only observers may share names across replicas and builds");

        let bootstrap = crate::catalog::test_bootstrap_args();
        let follower =
            Catalog::open_debug_read_only_catalog(persist.clone(), organization, &bootstrap)
                .await
                .expect("reopen selections");
        for state in [catalog.state(), follower.state()] {
            assert!(state.try_get_entry_by_global_id(&id).is_none());
            assert_eq!(state.written_plan(id, &build), Some(revision));
            assert_eq!(
                state.written_plan(id, &foreign_build),
                Some(foreign_revision)
            );
            assert_eq!(state.written_plan_replica_owner(id, &build), Some(&owner));
            assert_eq!(
                state.written_plan_replica_owner(id, &foreign_build),
                Some(&owner)
            );
        }
        drop(follower);

        // SQL planning expands a cluster drop to its replicas. Keep that contract
        // here rather than constructing a cluster-only drop with dangling replicas.
        let mut objects = vec![DropObjectInfo::ClusterReplica((
            cluster_id,
            replica_id,
            ReplicaCreateDropReason::Manual,
        ))];
        if drop_cluster {
            objects.push(DropObjectInfo::Cluster(cluster_id));
        }
        let ts = catalog.current_upper().await;
        catalog
            .transact(None, ts, None, vec![Op::DropObjects(objects)])
            .await
            .expect("drop observer owner");
        let follower = Catalog::open_debug_read_only_catalog(persist, organization, &bootstrap)
            .await
            .expect("reopen after drop");
        for state in [catalog.state(), follower.state()] {
            assert_eq!(state.written_plan(id, &build), None);
            assert_eq!(state.written_plan_replica_owner(id, &build), None);
            assert_eq!(
                state.written_plan(id, &foreign_build),
                Some(foreign_revision)
            );
            assert_eq!(
                state.written_plan_replica_owner(id, &foreign_build),
                Some(&owner)
            );
            assert_eq!(state.written_plan(other_id, &build), Some(other_revision));
            assert_eq!(
                state.written_plan_replica_owner(other_id, &build),
                Some(&other_owner)
            );
        }
    }
}

#[mz_ore::test(tokio::test)]
async fn replica_plan_selection_rejects_invalid_scope() {
    let (catalog, _, _) = protected_catalog().await;
    let base = catalog.state().clone();
    let cluster = catalog
        .user_clusters()
        .next()
        .expect("bootstrap user cluster");
    let owner = ReplicaPlanOwner {
        replica_id: cluster
            .replicas()
            .next()
            .expect("bootstrap replica")
            .replica_id,
        name: "observer".into(),
    };
    let imports: BTreeSet<_> = cluster.log_indexes.values().copied().collect();
    assert!(!imports.is_empty());
    let foreign_log = catalog
        .clusters()
        .filter(|c| c.id != cluster.id)
        .flat_map(|c| c.log_indexes.values())
        .next()
        .copied()
        .expect("foreign cluster log");
    let storage_id = catalog
        .entries()
        .find(|entry| matches!(entry.item(), crate::memory::objects::CatalogItem::Table(_)))
        .expect("catalog table definition")
        .latest_global_id();
    let id = GlobalId::Transient(1_000_000);
    let mut foreign_imports = imports.clone();
    foreign_imports.insert(foreign_log);
    let mut storage_imports = imports.clone();
    storage_imports.insert(storage_id);
    let missing_owner = ReplicaPlanOwner {
        replica_id: ReplicaId::User(u64::MAX),
        ..owner.clone()
    };
    let empty_name = ReplicaPlanOwner {
        name: String::new(),
        ..owner.clone()
    };
    for (label, id, owner, inputs) in [
        (
            "foreign cluster log",
            id,
            Some(owner.clone()),
            foreign_imports,
        ),
        ("storage import", id, Some(owner.clone()), storage_imports),
        ("missing replica", id, Some(missing_owner), imports.clone()),
        ("empty name", id, Some(empty_name), imports.clone()),
        (
            "nontransient ID",
            GlobalId::User(1_000_000),
            Some(owner.clone()),
            imports.clone(),
        ),
        ("SQL-owned ID", storage_id, Some(owner), imports.clone()),
        ("ordinary selection needs a SQL entry", id, None, imports),
    ] {
        let result = catalog
            .transact_incremental_dry_run(
                &base,
                vec![select(
                    id,
                    "test-build",
                    None,
                    Uuid::new_v4(),
                    owner,
                    inputs,
                )],
                None,
                None,
                1.into(),
            )
            .await;
        assert!(
            matches!(result, Err(CatalogError::DDLTransactionRace)),
            "{label}: {result:?}"
        );
    }
}

#[mz_ore::test(tokio::test)]
async fn replica_plan_selection_cas_uniqueness_and_immutable_owner() {
    let (catalog, _, _) = protected_catalog().await;
    let base = catalog.state().clone();
    let cluster = catalog
        .user_clusters()
        .next()
        .expect("bootstrap user cluster");
    let owner = ReplicaPlanOwner {
        replica_id: cluster
            .replicas()
            .next()
            .expect("bootstrap replica")
            .replica_id,
        name: "observer".into(),
    };
    let imports: BTreeSet<_> = cluster.log_indexes.values().copied().collect();
    let id = GlobalId::Transient(1_000_000);
    let revision = Uuid::new_v4();
    let (selected, snapshot) = catalog
        .transact_incremental_dry_run(
            &base,
            vec![select(
                id,
                "test-build",
                None,
                revision,
                Some(owner.clone()),
                imports.clone(),
            )],
            None,
            None,
            1.into(),
        )
        .await
        .expect("initial observer selection");

    let stale = catalog
        .transact_incremental_dry_run(
            &selected,
            vec![select(
                id,
                "test-build",
                None,
                Uuid::new_v4(),
                Some(owner.clone()),
                imports.clone(),
            )],
            None,
            Some(snapshot.clone()),
            1.into(),
        )
        .await;
    assert!(
        matches!(stale, Err(CatalogError::DDLTransactionRace)),
        "{stale:?}"
    );

    let renamed = ReplicaPlanOwner {
        name: "renamed".into(),
        ..owner.clone()
    };
    let other_replica = catalog
        .clusters()
        .flat_map(|c| c.replicas())
        .find(|r| r.replica_id != owner.replica_id)
        .expect("another replica")
        .replica_id;
    let reassigned = ReplicaPlanOwner {
        replica_id: other_replica,
        ..owner.clone()
    };
    for (label, next_id, expected, next_owner) in [
        (
            "competing export for same owner",
            GlobalId::Transient(1_000_001),
            None,
            Some(owner.clone()),
        ),
        ("rename selected owner", id, Some(revision), Some(renamed)),
        (
            "reassign selected owner",
            id,
            Some(revision),
            Some(reassigned),
        ),
        ("erase selected owner", id, Some(revision), None),
    ] {
        let result = catalog
            .transact_incremental_dry_run(
                &selected,
                vec![select(
                    next_id,
                    "test-build",
                    expected,
                    Uuid::new_v4(),
                    next_owner,
                    imports.clone(),
                )],
                None,
                Some(snapshot.clone()),
                1.into(),
            )
            .await;
        assert!(
            matches!(
                result,
                Err(CatalogError::Catalog(Error {
                    kind: ErrorKind::Durable(
                        crate::durable::DurableCatalogError::UniquenessViolation
                    ),
                }))
            ),
            "{label}: {result:?}"
        );
    }
    let replacement = Uuid::new_v4();
    let (replaced, _) = catalog
        .transact_incremental_dry_run(
            &selected,
            vec![select(
                id,
                "test-build",
                Some(revision),
                replacement,
                Some(owner.clone()),
                imports,
            )],
            None,
            Some(snapshot),
            1.into(),
        )
        .await
        .expect("matching CAS may replace a revision without changing ownership");
    assert_eq!(replaced.written_plan(id, "test-build"), Some(replacement));
    assert_eq!(
        replaced.written_plan_replica_owner(id, "test-build"),
        Some(&owner)
    );
    assert_eq!(catalog.state().written_plan(id, "test-build"), None);
}

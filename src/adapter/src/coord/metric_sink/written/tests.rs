// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use super::*;
use mz_catalog::builtin::BUILTINS;
use mz_catalog::catalog::ReplicaCreateDropReason;
use mz_catalog::durable::{TestCatalogStateBuilder, test_bootstrap_args};
use mz_catalog::memory::objects::{ClusterConfig, ClusterVariant};
use mz_compute_types::sinks::ComputeSinkConnection;
use mz_ore::metrics::MetricsRegistry;
use mz_persist_client::PersistClient;
use mz_sql::session::user::MZ_SYSTEM_ROLE_ID;
use mz_sql::session::vars::OwnedVarInput;
use uuid::Uuid;

async fn catalog() -> Catalog {
    let persist = PersistClient::new_for_tests().await;
    let organization = Uuid::new_v4();
    let bootstrap = test_bootstrap_args();
    let storage = TestCatalogStateBuilder::new(persist.clone())
        .with_organization_id(organization)
        .with_default_deploy_generation()
        .unwrap_build()
        .await
        .open(mz_ore::now::SYSTEM_TIME().into(), &bootstrap)
        .await
        .expect("open durable catalog");
    Catalog::open_debug_catalog_inner(
        persist,
        storage,
        mz_ore::now::SYSTEM_TIME.clone(),
        Some(
            format!("local-az1-{organization}-0")
                .parse()
                .expect("valid environment ID"),
        ),
        &mz_build_info::DUMMY_BUILD_INFO,
        BTreeMap::from([
            ("enable_catalog_read_protection".into(), "true".into()),
            ("enable_metric_sink".into(), "true".into()),
        ]),
        &bootstrap,
        Some(true),
        None,
    )
    .await
    .expect("open protected catalog")
}

async fn prepare(
    catalog: &Catalog,
    candidate: CatalogState,
    replicas: Option<&BTreeSet<ReplicaId>>,
) -> Vec<Op> {
    prepare_selections(
        catalog,
        Arc::new(candidate),
        replicas,
        catalog.current_upper().await,
        OptimizerMetrics::register_into(
            &MetricsRegistry::new(),
            std::time::Duration::from_secs(60),
        ),
    )
    .await
    .expect("prepare curated selections")
}

async fn commit(catalog: &mut Catalog, ops: Vec<Op>) {
    let ts = catalog.current_upper().await;
    catalog
        .transact(None, ts, None, ops)
        .await
        .expect("commit catalog operations");
}

async fn new_replica(catalog: &Catalog) -> (ReplicaId, Vec<Op>, CatalogState) {
    let cluster = catalog
        .user_clusters()
        .next()
        .expect("bootstrap user cluster");
    let config = cluster
        .replicas()
        .next()
        .expect("bootstrap replica")
        .config
        .clone();
    let replica_id = catalog
        .allocate_user_replica_ids(1, catalog.current_upper().await)
        .await
        .expect("allocate replica ID")[0];
    let ops = vec![Op::CreateClusterReplica {
        cluster_id: cluster.id,
        replica_id,
        name: format!("curated_{replica_id}"),
        config,
        owner_id: MZ_SYSTEM_ROLE_ID,
        reason: ReplicaCreateDropReason::Manual,
    }];
    let (candidate, _) = catalog
        .transact_incremental_dry_run(
            catalog.state(),
            ops.clone(),
            None,
            None,
            catalog.current_upper().await,
        )
        .await
        .expect("validate replica candidate");
    (replica_id, ops, candidate)
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)]
async fn curated_written_admission_and_bootstrap_flag_semantics() {
    let mut catalog = catalog().await;
    let ops = prepare(&catalog, catalog.state().clone(), None).await;
    assert!(!ops.is_empty());
    // Read real immutable bytes before committing any selection.
    let revisions = ops
        .iter()
        .map(|op| match op {
            Op::SetWrittenPlan {
                id,
                revision: Some(revision),
                replica_owner: Some(_),
                ..
            } => (*id, *revision),
            _ => panic!("expected owned selection"),
        })
        .collect();
    let plans = catalog
        .read_written_plans(revisions)
        .await
        .expect("read selected immutable plans");
    assert_eq!(plans.len(), ops.len());
    for op in &ops {
        let Op::SetWrittenPlan {
            id,
            replica_owner: Some(owner),
            ..
        } = op
        else {
            unreachable!()
        };
        assert!(matches!(id, GlobalId::Transient(_)));
        assert!(catalog.try_get_entry_by_global_id(id).is_none());
        let plan = &plans[id];
        assert!(plan.physical_plan.source_imports.is_empty());
        assert!(plan.physical_plan.index_exports.is_empty());
        assert!(!plan.physical_plan.index_imports.is_empty());
        assert_eq!(plan.physical_plan.sink_exports.len(), 1);
        let sink = &plan.physical_plan.sink_exports[id];
        assert_ne!(sink.from, *id);
        let ComputeSinkConnection::MetricSink(connection) = &sink.connection else {
            panic!("metric sink")
        };
        assert_eq!(connection.label, owner.name);
    }
    commit(&mut catalog, ops).await;
    let selected = catalog.state().written_plans().clone();
    assert!(
        prepare(&catalog, catalog.state().clone(), None)
            .await
            .is_empty()
    );
    assert_eq!(catalog.state().written_plans(), &selected);

    // Denylist edits and resets select their changes in the config transaction,
    // without rebuilding unaffected immutable plans.
    for config_op in [
        Op::UpdateSystemConfiguration {
            name: DISABLED_METRIC_SINKS.name().into(),
            value: OwnedVarInput::Flat(CURATED[0].name.into()),
        },
        Op::ResetSystemConfiguration {
            name: DISABLED_METRIC_SINKS.name().into(),
        },
    ] {
        let mut ops = vec![config_op];
        let (candidate, _) = catalog
            .transact_incremental_dry_run(
                catalog.state(),
                ops.clone(),
                None,
                None,
                catalog.current_upper().await,
            )
            .await
            .expect("validate denylist candidate");
        let denied = metric_sink_denied(candidate.system_config(), CURATED[0].name);
        let selections = prepare(&catalog, candidate, Some(&BTreeSet::new())).await;
        assert!(!selections.is_empty());
        assert!(selections.iter().all(|op| matches!(op,
            Op::SetWrittenPlan { revision, replica_owner: Some(owner), .. }
                if owner.name == CURATED[0].name && revision.is_none() == denied
        )));
        ops.extend(selections);
        commit(&mut catalog, ops).await;
        for (key, selection) in &selected {
            if selection
                .replica_owner
                .as_ref()
                .expect("curated selection has a replica owner")
                .name
                != CURATED[0].name
            {
                assert_eq!(catalog.state().written_plans().get(key), Some(selection));
            }
        }
        assert_eq!(
            catalog.state().written_plans().values().any(|selection| {
                selection
                    .replica_owner
                    .as_ref()
                    .expect("curated selection has a replica owner")
                    .name
                    == CURATED[0].name
            }),
            !denied
        );
    }
    let selected = catalog.state().written_plans().clone();

    commit(
        &mut catalog,
        vec![Op::UpdateSystemConfiguration {
            name: "enable_metric_sink".into(),
            value: OwnedVarInput::Flat("false".into()),
        }],
    )
    .await;
    // A live flag change and live CREATE admission do not withdraw old work.
    let (disabled_replica, disabled_ops, candidate) = new_replica(&catalog).await;
    let selections = prepare(
        &catalog,
        candidate,
        Some(&BTreeSet::from([disabled_replica])),
    )
    .await;
    assert!(selections.is_empty());
    commit(&mut catalog, disabled_ops).await;
    assert_eq!(catalog.state().written_plans(), &selected);
    let retract = prepare(&catalog, catalog.state().clone(), None).await;
    assert_eq!(retract.len(), selected.len());
    commit(&mut catalog, retract).await;
    assert!(catalog.state().written_plans().is_empty());
    commit(
        &mut catalog,
        vec![Op::UpdateSystemConfiguration {
            name: "enable_metric_sink".into(),
            value: OwnedVarInput::Flat("true".into()),
        }],
    )
    .await;
    let (enabled_replica, mut enabled_ops, candidate) = new_replica(&catalog).await;
    let selections = prepare(
        &catalog,
        candidate,
        Some(&BTreeSet::from([enabled_replica])),
    )
    .await;
    assert_eq!(selections.len(), CURATED.len());
    enabled_ops.extend(selections);
    commit(&mut catalog, enabled_ops).await;
    assert_eq!(catalog.state().written_plans().len(), CURATED.len());
    assert!(catalog.state().written_plans().values().all(|selection| {
        selection
            .replica_owner
            .as_ref()
            .expect("replica-owned selection")
            .replica_id
            == enabled_replica
    }));
    let fresh = prepare(&catalog, catalog.state().clone(), None).await;
    assert!(fresh.iter().any(|op| matches!(op,
        Op::SetWrittenPlan { replica_owner: Some(owner), .. }
            if owner.replica_id == disabled_replica
    )));
    for op in fresh {
        let Op::SetWrittenPlan { id, .. } = op else {
            unreachable!()
        };
        assert!(selected.keys().all(|(previous, _)| *previous != id));
    }
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)]
async fn curated_written_same_batch_cluster_and_replica() {
    let mut catalog = catalog().await;
    let replica_config = catalog
        .user_clusters()
        .next()
        .expect("bootstrap user cluster")
        .replicas()
        .next()
        .expect("bootstrap replica")
        .config
        .clone();
    let cluster = catalog
        .allocate_user_cluster_id(catalog.current_upper().await)
        .await
        .expect("allocate cluster ID");
    let replica = catalog
        .allocate_user_replica_ids(1, catalog.current_upper().await)
        .await
        .expect("allocate replica ID")[0];
    let mut ops = vec![
        Op::CreateCluster {
            id: cluster,
            name: "curated_test".into(),
            introspection_sources: BUILTINS::logs().collect(),
            owner_id: MZ_SYSTEM_ROLE_ID,
            config: ClusterConfig {
                variant: ClusterVariant::Unmanaged,
                workload_class: None,
            },
        },
        Op::CreateClusterReplica {
            cluster_id: cluster,
            replica_id: replica,
            name: "r".into(),
            config: replica_config,
            owner_id: MZ_SYSTEM_ROLE_ID,
            reason: ReplicaCreateDropReason::Manual,
        },
    ];
    let (candidate, _) = catalog
        .transact_incremental_dry_run(
            catalog.state(),
            ops.clone(),
            None,
            None,
            catalog.current_upper().await,
        )
        .await
        .expect("validate cluster and replica candidate");
    let logs: BTreeSet<_> = candidate
        .get_cluster(cluster)
        .log_indexes
        .values()
        .copied()
        .collect();
    let selections = prepare(&catalog, candidate, Some(&BTreeSet::from([replica]))).await;
    assert_eq!(selections.len(), CURATED.len());
    for selection in &selections {
        let Op::SetWrittenPlan {
            imports,
            replica_owner: Some(owner),
            ..
        } = selection
        else {
            unreachable!()
        };
        assert_eq!(owner.replica_id, replica);
        assert!(!imports.is_empty());
        assert!(imports.is_subset(&logs));
    }
    ops.extend(selections);
    commit(&mut catalog, ops).await;
    assert_eq!(catalog.state().written_plans().len(), CURATED.len());
    assert!(
        prepare(
            &catalog,
            catalog.state().clone(),
            Some(&BTreeSet::from([replica]))
        )
        .await
        .is_empty()
    );
}

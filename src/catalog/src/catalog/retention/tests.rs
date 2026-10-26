// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

use mz_expr::CollectionPlan;
use mz_persist_client::critical::Opaque;
use mz_persist_client::{Diagnostics, PersistClient};
use mz_persist_types::codec_impls::UnitSchema;
use mz_proto::RustType;
use mz_repr::{CatalogItemId, Datum, GlobalId, Row, Timestamp};
use mz_sql::DEFAULT_SCHEMA;
use mz_sql::catalog::CatalogDatabase;
use mz_sql::names::{ItemQualifiers, QualifiedItemName, ResolvedDatabaseSpecifier};
use mz_sql::session::user::MZ_SYSTEM_ROLE_ID;
use mz_sql::session::vars::DEFAULT_DATABASE_NAME;
use mz_storage_client::storage_collections::CollectionFrontiers;
use mz_storage_types::StorageDiff;
use mz_storage_types::sources::load_generator::LoadGeneratorOutput;
use mz_storage_types::sources::{SourceData, SourceExportStatementDetails};
use prost::Message;
use timely::progress::Antichain;
use uuid::Uuid;

use crate::SYSTEM_CONN_ID;
use crate::catalog::{Catalog, DropObjectInfo, Op, TransactionCatchUp, test_support};
use crate::durable::objects::{CollectionCompactionBound, MaintainedReadRequirement};
use crate::memory::implications::ParsedStateUpdateKind;
use crate::memory::objects::{CatalogItem, StateDiff};

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)]
async fn transaction_open_applies_only_compatible_peer_grants() {
    let mut f = Fixture::new().await;
    let id = f.source("peer_grant_input").await;
    let frontier = *f.catalog.state().collection_compaction_bounds()[&id]
        .as_option()
        .expect("readable source");
    let replica_id = f
        .catalog
        .clusters()
        .flat_map(|cluster| cluster.replicas())
        .next()
        .expect("builtin replica")
        .replica_id;
    let ts = f.catalog.current_upper().await;
    let incarnations = f
        .catalog
        .transact(
            None,
            None,
            ts,
            None,
            vec![
                Op::CreateClientIncarnation {
                    realization: crate::durable::ClientRealization::Adapter(
                        crate::durable::DEFAULT_ADAPTER_ID,
                    ),
                    expected: None,
                },
                Op::CreateClientIncarnation {
                    realization: crate::durable::ClientRealization::Replica(replica_id),
                    expected: None,
                },
            ],
        )
        .await
        .expect("register independent executions")
        .created_client_incarnations;
    let [writer, peer_incarnation]: [u64; 2] = incarnations.try_into().expect("two executions");
    let mut catch_up = TransactionCatchUp::new(writer);
    let mut peer = f.peer().await;
    let revision = f.catalog.transient_revision();
    let _ = f.catalog.take_read_protection_changes();

    for reject_candidate in [true, false] {
        peer.sync_to_current_updates().await.expect("refresh peer");
        let mut tx = peer.transaction().await.expect("open peer publication");
        if reject_candidate {
            tx.grant_client_read_requirements(peer_incarnation, BTreeMap::from([(id, frontier)]))
                .expect("publish peer grant");
        } else {
            tx.update_client_read_requirements(peer_incarnation, BTreeMap::from([(id, None)]))
                .expect("release peer grant");
        }
        let _ = tx.get_and_commit_op_updates();
        let ts = tx.upper();
        tx.commit(ts).await.expect("commit peer publication");

        if reject_candidate {
            // Another handle user can synchronize without draining the prefix.
            // Transaction open must include these already-buffered rows too.
            let mut storage = f.catalog.storage().await;
            assert!(matches!(
                storage.ensure_not_out_of_sync(peer.synced_upper()).await,
                Err(crate::durable::CatalogError::Durable(
                    crate::durable::DurableCatalogError::CatalogOutOfSync { .. }
                ))
            ));
        }

        let op = if reject_candidate {
            // The adapter execution already exists. Failure must not undo the
            // committed peer prefix or its incremental compaction bookkeeping.
            Op::CreateClientIncarnation {
                realization: crate::durable::ClientRealization::Adapter(
                    crate::durable::DEFAULT_ADAPTER_ID,
                ),
                expected: None,
            }
        } else {
            Op::GrantClientReadRequirements {
                incarnation: writer,
                requirements: BTreeMap::from([(id, frontier)]),
            }
        };
        let ts = f.catalog.current_upper().await;
        let result = f
            .catalog
            .transact(Some(&mut catch_up), None, ts, None, vec![op])
            .await;
        assert_eq!(
            result.is_err(),
            reject_candidate,
            "{:?}",
            result.as_ref().err()
        );
        assert_eq!(
            f.catalog
                .state()
                .client_read_requirements()
                .get(&(peer_incarnation, id)),
            reject_candidate.then_some(&frontier),
        );
        assert!(f.catalog.take_read_protection_changes().contains(&id));
        assert_eq!(f.catalog.transient_revision(), revision);
    }
    assert_eq!(
        f.catalog.state().client_read_requirements()[&(writer, id)],
        frontier
    );

    // Own-map changes, sampled proposals and mixed prefixes must return intact
    // to the existing refresh owner instead of being absorbed into this attempt.
    let mut grant = frontier;
    for (publisher, sampled, structural) in [
        (writer, None, false),
        (peer_incarnation, Some("bounds"), false),
        (peer_incarnation, Some("requirements"), false),
        (peer_incarnation, None, true),
    ] {
        peer.sync_to_current_updates().await.expect("refresh peer");
        let mut tx = peer
            .transaction()
            .await
            .expect("open conflicting publication");
        grant = grant.step_forward();
        if structural {
            tx.update_client_read_requirements(publisher, BTreeMap::from([(id, None)]))
                .expect("release conflicting grant");
        } else {
            // A publisher can introduce its first grant or advance an existing one.
            tx.grant_client_read_requirements(publisher, BTreeMap::from([(id, grant)]))
                .expect("establish conflicting grant");
            tx.update_client_read_requirements(publisher, BTreeMap::from([(id, Some(grant))]))
                .expect("advance conflicting grant");
        }
        if structural {
            tx.upsert_system_config("max_tables", "777".into())
                .expect("stage structural change");
        }
        let _ = tx.get_and_commit_op_updates();
        let ts = tx.upper();
        tx.commit(ts).await.expect("commit conflicting prefix");

        let before = f.catalog.state().client_read_requirements().clone();
        let ops = if sampled == Some("bounds") {
            vec![Op::SetReadProtection {
                requirements: vec![],
                bounds: vec![CollectionCompactionBound {
                    id,
                    frontier: Some(frontier),
                }],
            }]
        } else if sampled == Some("requirements") {
            vec![Op::SetReadProtection {
                requirements: vec![f.catalog.state().maintained_read_requirements()[&id].clone()],
                bounds: vec![],
            }]
        } else {
            vec![]
        };
        let ts = f.catalog.current_upper().await;
        let error = match f
            .catalog
            .transact(Some(&mut catch_up), None, ts, None, ops)
            .await
        {
            Ok(_) => panic!("ineligible catch-up must reject the attempt"),
            Err(error) => error,
        };
        assert!(matches!(
            error,
            crate::catalog::CatalogError::Catalog(error) if matches!(
                error.kind,
                crate::memory::error::ErrorKind::Durable(
                    crate::durable::DurableCatalogError::CatalogOutOfSync { .. }
                )
            )
        ));
        assert_eq!(f.catalog.state().client_read_requirements(), &before);
        f.catalog
            .sync_to_current_updates()
            .await
            .expect("refresh rejected prefix");
        assert_eq!(
            f.catalog
                .state()
                .client_read_requirements()
                .get(&(publisher, id)),
            (!structural).then_some(&grant),
        );
    }
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)]
async fn combined_publication_commits_projected_clients_recovery_and_bounds() {
    let mut f = Fixture::new().await;
    let input = f.source("combined_input").await;
    let retained = f.source("unmentioned_input").await;
    let incarnation = f.catch_up().await.incarnation;
    let initial = [input, retained]
        .into_iter()
        .map(|id| {
            *f.catalog.state().collection_compaction_bounds()[&id]
                .as_option()
                .expect("readable source")
        })
        .max()
        .expect("two inputs");
    f.transact(vec![Op::GrantClientReadRequirements {
        incarnation,
        requirements: BTreeMap::from([(input, initial), (retained, initial)]),
    }])
    .await;

    let advanced = initial.step_forward();
    for id in [input, retained] {
        f.append(
            id,
            advanced.step_forward().step_forward().into(),
            Vec::new(),
        )
        .await;
    }
    for (client_frontier, recovery_frontier) in
        [(Some(advanced), advanced), (None, advanced.step_forward())]
    {
        let updates = BTreeMap::from([(input, client_frontier)]);
        let requirements: Vec<_> = [input, retained]
            .into_iter()
            .map(|id| MaintainedReadRequirement {
                frontier: Some(recovery_frontier),
                ..f.catalog.state().maintained_read_requirements()[&id].clone()
            })
            .collect();
        let bounds = [input, retained]
            .into_iter()
            .map(|id| {
                let frontier = f
                    .catalog
                    .state()
                    .client_read_frontier_after_update(id, Some((incarnation, &updates)))
                    .map_or(recovery_frontier, |held| recovery_frontier.min(held));
                CollectionCompactionBound {
                    id,
                    frontier: Some(frontier),
                }
            })
            .collect();

        // Neither the committed client grant nor the recovery requirement permits
        // this input bound alone. The unmentioned grant must still limit its input.
        assert!(f.catalog.state().client_read_frontier(input) < Some(recovery_frontier));
        f.transact(vec![
            Op::UpdateClientReadRequirements {
                incarnation,
                requirements: updates,
            },
            Op::SetReadProtection {
                requirements: requirements.clone(),
                bounds,
            },
        ])
        .await;

        // Reconstruct from durable storage rather than only checking candidate state.
        f = f.reopen().await;
        let state = f.catalog.state();
        assert_eq!(
            state.client_read_requirements().get(&(incarnation, input)),
            client_frontier.as_ref(),
        );
        assert_eq!(
            state.client_read_requirements()[&(incarnation, retained)],
            initial,
        );
        for requirement in requirements {
            assert_eq!(
                state.maintained_read_requirements()[&requirement.id],
                requirement,
            );
        }
        assert_eq!(
            state.collection_compaction_bounds()[&input],
            Antichain::from_elem(recovery_frontier),
        );
        assert_eq!(
            state.collection_compaction_bounds()[&retained],
            Antichain::from_elem(initial),
        );
    }
    f.catalog.expire().await;
}

async fn publish_frontier(
    peer: &mut dyn crate::durable::DurableCatalogState,
    id: GlobalId,
    mut requirement: MaintainedReadRequirement,
    frontier: Timestamp,
) {
    peer.sync_to_current_updates()
        .await
        .expect("refresh frontier writer");
    let mut tx = peer.transaction().await.expect("open frontier publication");
    requirement.frontier = Some(frontier);
    tx.set_maintained_read_requirement(requirement.id, requirement.inputs, requirement.frontier)
        .expect("advance recovery requirement");
    tx.set_collection_compaction_bound(id, Some(frontier))
        .expect("advance permission");
    let _ = tx.get_and_commit_op_updates();
    let ts = tx.upper();
    tx.commit(ts).await.expect("commit frontier publication");
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)]
async fn automatic_mv_birth_recomputed_across_frontier_catch_up() {
    let mut f = Fixture::new().await;
    let input = f.source("automatic_input").await;
    let (_, index) = f.create_with_plan(
        "automatic_index",
        format!("CREATE INDEX automatic_index IN CLUSTER quickstart ON {}.automatic_input (counter)", f.prefix),
        Some(BTreeSet::from([input])),
    ).await;
    let mut catch_up = f.catch_up().await;
    let mut peer = f.peer().await;
    tokio::time::pause();

    // The logical input retains earlier permission than the selected index.
    // Only reconsidering actual imports can discover the new birth floor.
    for after_candidate in [false, true] {
        let name = if after_candidate {
            "mv_after_candidate"
        } else {
            "mv_at_open"
        };
        let (item_id, mv_id, mut ops) = f.create_ops(
            name,
            format!("CREATE MATERIALIZED VIEW {}.{name} IN CLUSTER quickstart AS SELECT * FROM {}.automatic_input", f.prefix, f.prefix),
            Some(BTreeSet::from([index])),
        ).await;
        let selected_revision = match &ops[1] {
            Op::SetWrittenPlan { revision, .. } => *revision,
            _ => unreachable!(),
        };
        ops.push(Op::PublishCreatorReadRequirements {
            incarnation: catch_up.incarnation,
            requirements: BTreeMap::new(),
            minimums: BTreeMap::from([(mv_id, Timestamp::MIN)]),
        });
        let floor = f.catalog.state().collection_compaction_bounds()[&index]
            .as_option()
            .expect("readable index")
            .step_forward();
        let requirement = f.catalog.state().maintained_read_requirements()[&input].clone();
        let planning_revision = f.catalog.transient_revision();
        if !after_candidate {
            publish_frontier(&mut *peer, index, requirement.clone(), floor).await;
        }
        let (hook, mut candidates) = tokio::sync::mpsc::unbounded_channel();
        f.catalog.before_commit = Some(hook);
        let ts = f.catalog.current_upper().await;
        let transaction = async {
            let result = f
                .catalog
                .transact(Some(&mut catch_up), None, ts, None, ops)
                .await;
            f.catalog.before_commit = None;
            result
        };
        let interference = async {
            let mut attempts = 0;
            while let Some(resume) = candidates.recv().await {
                attempts += 1;
                assert!(attempts <= 2, "one conflict requires at most one retry");
                if after_candidate && attempts == 1 {
                    publish_frontier(&mut *peer, index, requirement.clone(), floor).await;
                }
                resume.send(()).expect("resume candidate");
            }
            attempts
        };
        let (result, attempts) = tokio::join!(transaction, interference);
        let result = result.expect("admit automatic birth at current permission");
        assert_eq!(attempts, if after_candidate { 2 } else { 1 });
        assert!(
            catch_up.updates.is_empty(),
            "success transfers caught-up effects"
        );
        assert_eq!(f.catalog.transient_revision(), planning_revision + 1);
        let state = f.catalog.state();
        assert_eq!(
            state.maintained_read_requirements()[&input],
            MaintainedReadRequirement {
                frontier: Some(floor),
                ..requirement
            }
        );
        assert_eq!(state.written_plan(mv_id, "test-build"), selected_revision);
        assert_eq!(
            state.written_plans()[&(mv_id, "test-build".into())].imports,
            BTreeSet::from([index])
        );
        assert_eq!(
            state
                .get_entry(&item_id)
                .materialized_view()
                .expect("admitted materialized view")
                .initial_as_of,
            Some(Antichain::from_elem(floor))
        );
        assert_eq!(
            state.collection_compaction_bounds()[&mv_id],
            Antichain::from_elem(floor)
        );
        assert_eq!(
            state.maintained_read_requirements()[&mv_id],
            MaintainedReadRequirement {
                id: mv_id,
                inputs: BTreeSet::from([input]),
                frontier: Some(floor)
            }
        );
        assert_eq!(
            result.granted_client_read_requirements,
            BTreeMap::from([(catch_up.incarnation, BTreeMap::from([(mv_id, floor)]))])
        );
        assert_eq!(
            state.client_read_requirements()[&(catch_up.incarnation, mv_id)],
            floor
        );
        assert!(
            result
                .catalog_updates
                .iter()
                .any(|update| update.diff == StateDiff::Addition
                    && matches!(&update.kind,
                ParsedStateUpdateKind::CollectionCompactionBound(bound)
                    if bound.id == index && bound.frontier == Some(floor)))
        );

        // Retire the first MV and its grant so they cannot pin the next import.
        f.transact(vec![
            Op::UpdateClientReadRequirements {
                incarnation: catch_up.incarnation,
                requirements: BTreeMap::from([(mv_id, None)]),
            },
            Op::DropObjects(vec![DropObjectInfo::Item(item_id)]),
        ])
        .await;
    }
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)]
async fn caught_up_bound_survives_failure_and_is_superseded_by_drop() {
    let mut f = Fixture::new().await;
    let id = f.source("caught_up_input").await;
    let item_id = f.catalog.state().get_entry_by_global_id(&id).id();
    let mut catch_up = f.catch_up().await;
    let mut peer = f.peer().await;
    let frontier = f.catalog.state().collection_compaction_bounds()[&id]
        .as_option()
        .expect("readable source")
        .step_forward();
    let requirement = f.catalog.state().maintained_read_requirements()[&id].clone();
    let revision = f.catalog.transient_revision();
    let _ = f.catalog.take_read_protection_changes();
    publish_frontier(&mut *peer, id, requirement, frontier).await;
    let ts = f.catalog.current_upper().await;
    let result = f
        .catalog
        .transact(
            Some(&mut catch_up),
            None,
            ts,
            None,
            vec![Op::CreateClientIncarnation {
                realization: crate::durable::ClientRealization::Adapter(
                    crate::durable::DEFAULT_ADAPTER_ID,
                ),
                expected: None,
            }],
        )
        .await;
    assert!(
        matches!(result, Err(crate::catalog::CatalogError::Catalog(error))
            if matches!(error.kind, crate::memory::error::ErrorKind::Durable(
                crate::durable::DurableCatalogError::InvalidReadProtection(_)))),
        "duplicate adapter execution must fail after catch-up"
    );
    assert_eq!(f.catalog.transient_revision(), revision);
    assert_eq!(
        f.catalog.state().collection_compaction_bounds()[&id],
        Antichain::from_elem(frontier)
    );
    assert!(f.catalog.take_read_protection_changes().contains(&id));
    assert_eq!(catch_up.updates.len(), 1);
    let update = &catch_up.updates[&id];
    assert_eq!(update.diff, StateDiff::Addition);
    assert!(
        matches!(&update.kind, ParsedStateUpdateKind::CollectionCompactionBound(bound)
        if bound.id == id && bound.frontier == Some(frontier))
    );

    let mut catch_up = TransactionCatchUp::new(catch_up.incarnation);
    let requirement = f.catalog.state().maintained_read_requirements()[&id].clone();
    publish_frontier(&mut *peer, id, requirement, frontier.step_forward()).await;
    let ts = f.catalog.current_upper().await;
    let result = f
        .catalog
        .transact(
            Some(&mut catch_up),
            None,
            ts,
            None,
            vec![Op::DropObjects(vec![DropObjectInfo::Item(item_id)])],
        )
        .await
        .expect("drop supersedes undelivered bound");
    assert!(catch_up.updates.is_empty());
    assert!(f.catalog.state().try_get_entry(&item_id).is_none());
    assert!(
        !result
            .catalog_updates
            .iter()
            .any(|update| update.diff == StateDiff::Addition
                && matches!(&update.kind,
            ParsedStateUpdateKind::CollectionCompactionBound(bound) if bound.id == id))
    );
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)]
async fn frontier_retry_exhaustion_returns_committed_effects() {
    let mut f = Fixture::new().await;
    let id = f.source("contended_input").await;
    let mut catch_up = f.catch_up().await;
    let (view_id, _, ops) = f
        .create_ops(
            "contended_view",
            format!(
                "CREATE VIEW {}.contended_view AS SELECT * FROM {}.contended_input",
                f.prefix, f.prefix
            ),
            None,
        )
        .await;
    let mut peer = f.peer().await;
    let requirement = f.catalog.state().maintained_read_requirements()[&id].clone();
    let initial = *f.catalog.state().collection_compaction_bounds()[&id]
        .as_option()
        .expect("readable source");
    let revision = f.catalog.transient_revision();
    tokio::time::pause();
    let (hook, mut candidates) = tokio::sync::mpsc::unbounded_channel();
    f.catalog.before_commit = Some(hook);
    let ts = f.catalog.current_upper().await;
    let transaction = async {
        let result = f
            .catalog
            .transact(Some(&mut catch_up), None, ts, None, ops)
            .await;
        f.catalog.before_commit = None;
        result
    };
    let interference = async {
        let mut attempts = 0;
        let mut frontier = initial;
        while let Some(resume) = candidates.recv().await {
            attempts += 1;
            assert!(attempts <= 3, "retry work must be bounded");
            frontier = frontier.step_forward();
            publish_frontier(&mut *peer, id, requirement.clone(), frontier).await;
            resume.send(()).expect("resume losing candidate");
        }
        attempts
    };
    let (result, attempts) = tokio::join!(transaction, interference);
    assert_eq!(attempts, 3);
    assert!(
        matches!(result, Err(crate::catalog::CatalogError::Catalog(error))
        if matches!(error.kind, crate::memory::error::ErrorKind::Durable(
            crate::durable::DurableCatalogError::CatalogOutOfSync { .. })))
    );
    assert_eq!(f.catalog.transient_revision(), revision);
    assert!(f.catalog.state().try_get_entry(&view_id).is_none());
    let caught_up = initial.step_forward().step_forward();
    assert_eq!(
        f.catalog.state().collection_compaction_bounds()[&id],
        Antichain::from_elem(caught_up)
    );
    assert_eq!(
        catch_up.updates.len(),
        1,
        "coalesce repeated bound advancements"
    );
    assert!(matches!(&catch_up.updates[&id].kind,
        ParsedStateUpdateKind::CollectionCompactionBound(bound)
            if bound.frontier == Some(caught_up)));
    // The final losing prefix remains with the refresh owner.
    f.catalog
        .sync_to_current_updates()
        .await
        .expect("owner refreshes after exhaustion");
    assert_eq!(
        f.catalog.state().collection_compaction_bounds()[&id],
        Antichain::from_elem(caught_up.step_forward())
    );
}

/// A native durable catalog, with no controller or query client. Source data
/// advances only through explicit Persist writes, never a running generator.
struct Fixture {
    catalog: Catalog,
    persist: PersistClient,
    qualifiers: ItemQualifiers,
    prefix: String,
    observed_uppers: BTreeMap<GlobalId, Antichain<Timestamp>>,
}

impl Fixture {
    async fn new() -> Self {
        let persist = PersistClient::new_for_tests().await;
        let organization = Uuid::new_v4();
        let mut bootstrap = crate::durable::test_bootstrap_args();
        bootstrap.default_cluster_replication_factor = 0;
        let storage = crate::durable::TestCatalogStateBuilder::new(persist.clone())
            .with_organization_id(organization)
            .with_default_deploy_generation()
            .unwrap_build()
            .await
            .open(Timestamp::MIN, &bootstrap)
            .await
            .expect("open durable catalog");
        let catalog = Catalog::open_debug_catalog_inner(
            persist.clone(),
            storage,
            (|| 0u64).into(),
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
        .expect("open protected catalog");
        let database = catalog
            .resolve_database(DEFAULT_DATABASE_NAME)
            .expect("default database");
        let database_spec = ResolvedDatabaseSpecifier::Id(database.id());
        let schema = catalog
            .resolve_schema_in_database(&database_spec, DEFAULT_SCHEMA, &SYSTEM_CONN_ID)
            .expect("default schema");
        let qualifiers = ItemQualifiers {
            database_spec,
            schema_spec: schema.id.clone(),
        };
        let prefix = format!("{}.{}", database.name, schema.name.schema);
        assert_eq!(
            catalog
                .state()
                .resolve_cluster("quickstart")
                .expect("default cluster")
                .replicas()
                .count(),
            0
        );
        assert!(catalog.state().client_incarnations().is_empty());
        Self {
            catalog,
            persist,
            qualifiers,
            prefix,
            observed_uppers: BTreeMap::new(),
        }
    }

    async fn transact(&mut self, ops: Vec<Op>) {
        let ts = self.catalog.current_upper().await;
        self.catalog
            .transact(None, None, ts, None, ops)
            .await
            .expect("catalog transaction");
    }

    async fn reopen(self) -> Self {
        let config = self.catalog.config();
        let organization = config.environment_id.organization_id();
        let state_config = self.catalog.replica_config().into_state(
            config.build_info,
            config.environment_id.clone(),
            config.connection_context.clone(),
            self.persist.clone(),
        );
        self.catalog.expire().await;
        let storage = crate::durable::TestCatalogStateBuilder::new(self.persist.clone())
            .with_organization_id(organization)
            .with_default_deploy_generation()
            .unwrap_build()
            .await
            .join()
            .await
            .expect("join after shutdown");
        let opened = Box::pin(Catalog::open_committed(state_config, storage))
            .await
            .expect("reconstruct native catalog");
        Self {
            catalog: opened.catalog,
            ..self
        }
    }

    async fn create(&mut self, name: &str, sql: String) -> (CatalogItemId, GlobalId) {
        self.create_with_plan(name, sql, None).await
    }

    async fn create_with_plan(
        &mut self,
        name: &str,
        sql: String,
        imports: Option<BTreeSet<GlobalId>>,
    ) -> (CatalogItemId, GlobalId) {
        let (id, global_id, ops) = self.create_ops(name, sql, imports).await;
        self.transact(ops).await;
        (id, global_id)
    }

    async fn create_ops(
        &mut self,
        name: &str,
        sql: String,
        imports: Option<BTreeSet<GlobalId>>,
    ) -> (CatalogItemId, GlobalId, Vec<Op>) {
        let (id, global_id) = self
            .catalog
            .allocate_user_id_for_test()
            .await
            .expect("allocate fixture IDs");
        let item =
            test_support::parse_item(&mut self.catalog.state, global_id, &sql, &BTreeMap::new())
                .unwrap_or_else(|err| panic!("parse {sql}: {err}"));
        let mut ops = vec![Op::CreateItem {
            id,
            name: QualifiedItemName {
                qualifiers: self.qualifiers.clone(),
                item: name.into(),
            },
            item,
            owner_id: MZ_SYSTEM_ROLE_ID,
        }];
        if let Some(imports) = imports {
            ops.push(self.select_plan(global_id, "test-build", Some(imports)));
        }
        (id, global_id, ops)
    }

    async fn catch_up(&mut self) -> TransactionCatchUp {
        let ts = self.catalog.current_upper().await;
        let result = self
            .catalog
            .transact(
                None,
                None,
                ts,
                None,
                vec![Op::CreateClientIncarnation {
                    realization: crate::durable::ClientRealization::Adapter(
                        crate::durable::DEFAULT_ADAPTER_ID,
                    ),
                    expected: None,
                }],
            )
            .await
            .expect("register adapter execution");
        TransactionCatchUp::new(result.created_client_incarnations[0])
    }

    async fn peer(&self) -> Box<dyn crate::durable::DurableCatalogState> {
        crate::durable::TestCatalogStateBuilder::new(self.persist.clone())
            .with_organization_id(self.catalog.config().environment_id.organization_id())
            .with_default_deploy_generation()
            .unwrap_build()
            .await
            .join()
            .await
            .expect("join frontier writer")
    }

    fn select_plan(&self, id: GlobalId, build: &str, imports: Option<BTreeSet<GlobalId>>) -> Op {
        // Catalog admission checks the selection metadata, not immutable bytes.
        Op::SetWrittenPlan {
            id,
            build_version: build.into(),
            expected_revision: self.catalog.state().written_plan(id, build),
            revision: imports.as_ref().map(|_| Uuid::new_v4()),
            imports: imports.unwrap_or_default(),
            replica_owner: None,
        }
    }

    async fn source(&mut self, name: &str) -> GlobalId {
        self.create(
            &format!("{name}_ingestion"),
            format!(
                "CREATE SOURCE {}.{name}_ingestion IN CLUSTER quickstart \
                 FROM LOAD GENERATOR COUNTER",
                self.prefix,
            ),
        )
        .await;
        let details = hex::encode(
            SourceExportStatementDetails::LoadGenerator {
                output: LoadGeneratorOutput::Default,
            }
            .into_proto()
            .encode_to_vec(),
        );
        // This is an ingestion export, not TableWrites. Its durable upper is
        // the data shard's upper and does not require a transaction WAL.
        self.create(
            name,
            format!(
                "CREATE TABLE {}.{name} FROM SOURCE {}.{name}_ingestion \
                 (REFERENCE counter) WITH (DETAILS '{details}')",
                self.prefix, self.prefix,
            ),
        )
        .await
        .1
    }

    async fn index(&mut self, name: &str, on: &str, imports: &[GlobalId]) -> GlobalId {
        let (_, index) = self
            .create_with_plan(
                name,
                format!(
                    "CREATE INDEX {name} IN CLUSTER quickstart \
                     ON {}.{on} (counter) WITH (RETAIN HISTORY FOR '10 seconds')",
                    self.prefix,
                ),
                Some(imports.iter().copied().collect()),
            )
            .await;
        assert!(
            !self
                .catalog
                .state()
                .maintained_read_requirements()
                .contains_key(&index),
            "index object retention is derived, not a maintained-read record"
        );
        index
    }

    async fn advance_upper(&mut self, id: GlobalId, upper: u64) {
        self.append(id, upper, Vec::new()).await;
    }

    async fn append(
        &mut self,
        id: GlobalId,
        upper: u64,
        updates: Vec<((SourceData, ()), Timestamp, StorageDiff)>,
    ) {
        let shard = self.catalog.state().storage_metadata().collection_metadata[&id];
        let desc = self
            .catalog
            .state()
            .try_get_desc_by_global_id(&id)
            .expect("persisted relation")
            .into_owned();
        let mut writer = self
            .persist
            .open_writer::<SourceData, (), Timestamp, StorageDiff>(
                shard,
                Arc::new(desc),
                Arc::new(UnitSchema),
                Diagnostics::for_tests(),
            )
            .await
            .expect("open source writer");
        let expected = writer.upper().clone();
        writer
            .compare_and_append(
                updates,
                expected,
                Antichain::from_elem(Timestamp::new(upper)),
            )
            .await
            .expect("valid upper advance")
            .expect("uncontended upper advance");
        writer.expire().await;
        self.observed_uppers
            .insert(id, Antichain::from_elem(Timestamp::new(upper)));
        // Advance the source's independent recovery requirement from its durable
        // output, so its birth protection cannot mask missing index retention.
        if let Some(mut requirement) = self
            .catalog
            .state()
            .maintained_read_requirements()
            .get(&id)
            .cloned()
        {
            requirement.frontier = Some(Timestamp::new(upper).saturating_sub(1));
            self.transact(vec![Op::SetReadProtection {
                requirements: vec![requirement],
                bounds: vec![],
            }])
            .await;
        }
    }

    async fn propose_bounds(&mut self, ids: &[GlobalId], proposed: u64, expected: u64) {
        let proposed = Timestamp::new(proposed);
        let frontiers: Vec<_> = self
            .observed_uppers
            .iter()
            .map(|(id, upper)| CollectionFrontiers {
                id: *id,
                write_frontier: upper.clone(),
                implied_capability: Antichain::from_elem(Timestamp::MIN),
                read_capabilities: Antichain::from_elem(Timestamp::MIN),
            })
            .collect();
        let mut bounds: BTreeMap<_, _> = ids
            .iter()
            .map(|id| {
                let frontier = self
                    .catalog
                    .state()
                    .maintained_read_frontier(*id, &BTreeSet::new())
                    .map_or(proposed, |required| proposed.min(required));
                (*id, Antichain::from_elem(frontier))
            })
            .collect();
        bounds.extend(self.catalog.state().index_retention_proposals(&frontiers));
        self.transact(vec![Op::SetReadProtection {
            requirements: vec![],
            bounds: bounds
                .into_iter()
                .map(|(id, frontier)| CollectionCompactionBound {
                    id,
                    frontier: frontier.as_option().copied(),
                })
                .collect(),
        }])
        .await;
        for id in ids {
            self.assert_bound(*id, expected);
        }
    }

    async fn set_bounds(&mut self, bounds: &[(GlobalId, u64)]) {
        self.transact(vec![Op::SetReadProtection {
            requirements: vec![],
            bounds: bounds
                .iter()
                .map(|(id, frontier)| CollectionCompactionBound {
                    id: *id,
                    frontier: Some(Timestamp::new(*frontier)),
                })
                .collect(),
        }])
        .await;
    }

    fn assert_bound(&self, id: GlobalId, expected: u64) {
        assert_eq!(
            self.catalog.state().collection_compaction_bounds()[&id],
            Antichain::from_elem(Timestamp::new(expected)),
            "committed compaction bound for {id}",
        );
    }
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)]
async fn index_retention_tracks_durable_input_upper_without_replicas() {
    let mut f = Fixture::new().await;
    let input = f.source("input").await;
    f.advance_upper(input, 100_000).await;
    let index = f.index("retaining_index", "input", &[input]).await;

    f.propose_bounds(&[input, index], 100_000, 90_000).await;
    // No catalog object or client grant changes. A creation-time pin cannot
    // satisfy this second proposal, only the new durable input upper can.
    f.advance_upper(input, 120_000).await;
    f.propose_bounds(&[input, index], 120_000, 110_000).await;
    assert!(f.catalog.state().client_incarnations().is_empty());
    assert!(f.catalog.state().client_read_requirements().is_empty());
    f.catalog.expire().await;
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)]
async fn index_plan_selection_atomically_protects_actual_imports() {
    let mut f = Fixture::new().await;
    let input = f.source("input").await;
    f.advance_upper(input, 100_000).await;
    f.set_bounds(&[(input, 20_000)]).await;
    let old = f.index("old_import", "input", &[input]).await;
    let replacement = f.index("replacement_import", "input", &[input]).await;
    let past = f.index("past_import", "input", &[input]).await;
    // All progress proposals are below the observed upper's retention frontier.
    // Leave the logical input behind the actual import to distinguish admission.
    f.set_bounds(&[(old, 40_000), (past, 80_000)]).await;
    f.assert_bound(input, 20_000);
    f.assert_bound(old, 40_000);
    f.assert_bound(past, 80_000);

    let index = f.index("retaining_index", "input", &[old]).await;
    f.assert_bound(index, 40_000);
    let initial_revision = f.catalog.state().written_plan(index, "test-build");
    assert!(initial_revision.is_some());
    f.set_bounds(&[(old, 80_000)]).await;
    f.assert_bound(old, 40_000);

    let invalid = f.select_plan(index, "test-build", Some(BTreeSet::from([past])));
    let ts = f.catalog.current_upper().await;
    assert!(
        f.catalog
            .transact(None, None, ts, None, vec![invalid])
            .await
            .is_err(),
        "a selected import must still be readable at the owner's bound",
    );
    assert_eq!(
        f.catalog.state().written_plan(index, "test-build"),
        initial_revision,
    );
    f.assert_bound(index, 40_000);

    f.transact(vec![f.select_plan(
        index,
        "other-build",
        Some(BTreeSet::from([old])),
    )])
    .await;
    // Only selection metadata changes. The SQL definition remains unchanged.
    f.transact(vec![f.select_plan(
        index,
        "test-build",
        Some(BTreeSet::from([replacement])),
    )])
    .await;
    assert_ne!(
        f.catalog.state().written_plan(index, "test-build"),
        initial_revision,
    );
    f.set_bounds(&[(old, 80_000), (replacement, 80_000)]).await;
    f.assert_bound(old, 40_000);
    f.assert_bound(replacement, 40_000);

    // Retiring the other build releases only its import. The replacement is
    // protected by the surviving selection without any replica or client hold.
    f.transact(vec![f.select_plan(index, "other-build", None)])
        .await;
    f.set_bounds(&[(old, 80_000), (replacement, 80_000)]).await;
    f.assert_bound(old, 80_000);
    f.assert_bound(replacement, 40_000);
    f.assert_bound(index, 40_000);
    assert!(f.catalog.state().client_incarnations().is_empty());
    f.catalog.expire().await;
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)]
async fn replacing_client_preserves_index_object_retention() {
    let mut f = Fixture::new().await;
    let input = f.source("input").await;
    let row = SourceData(Ok(Row::pack_slice(&[Datum::Int64(42)])));
    f.append(
        input,
        100_000,
        vec![((row.clone(), ()), Timestamp::new(50_000), 1)],
    )
    .await;
    f.index("retaining_index", "input", &[input]).await;

    let ts = f.catalog.current_upper().await;
    let incarnation = f
        .catalog
        .transact(
            None,
            None,
            ts,
            None,
            vec![Op::CreateClientIncarnation {
                realization: crate::durable::ClientRealization::Adapter(
                    crate::durable::DEFAULT_ADAPTER_ID,
                ),
                expected: None,
            }],
        )
        .await
        .expect("create client")
        .created_client_incarnations[0];
    f.transact(vec![Op::GrantClientReadRequirements {
        incarnation,
        requirements: BTreeMap::from([(input, Timestamp::new(80_000))]),
    }])
    .await;
    f.propose_bounds(&[input], 80_000, 80_000).await;

    let ts = f.catalog.current_upper().await;
    let successor = f
        .catalog
        .transact(
            None,
            None,
            ts,
            None,
            vec![Op::CreateClientIncarnation {
                realization: crate::durable::ClientRealization::Adapter(
                    crate::durable::DEFAULT_ADAPTER_ID,
                ),
                expected: Some(incarnation),
            }],
        )
        .await
        .expect("replace client")
        .created_client_incarnations[0];
    assert!(
        !f.catalog
            .state()
            .client_incarnations()
            .contains_key(&incarnation)
    );
    f.transact(vec![Op::UpdateClientReadRequirements {
        incarnation: successor,
        requirements: BTreeMap::from([(input, None)]),
    }])
    .await;
    assert!(f.catalog.state().client_read_requirements().is_empty());
    f.propose_bounds(&[input], 100_000, 90_000).await;

    // No leased reader or query token exists while committed permission is
    // applied to Persist. The critical handle is the compaction enforcer.
    let shard = f.catalog.state().storage_metadata().collection_metadata[&input];
    let mut since = f
        .persist
        .open_critical_since::<SourceData, (), Timestamp, StorageDiff>(
            shard,
            PersistClient::CONTROLLER_CRITICAL_SINCE,
            Opaque::encode(&0u64),
            Diagnostics::for_tests(),
        )
        .await
        .expect("open compaction enforcer");
    let opaque = since.opaque().clone();
    since
        .compare_and_downgrade_since(
            &opaque,
            (
                &opaque,
                &f.catalog.state().collection_compaction_bounds()[&input],
            ),
        )
        .await
        .expect("apply committed permission");
    drop(since);
    let mut f = f.reopen().await;
    assert!(f.catalog.state().client_read_requirements().is_empty());
    assert_eq!(
        f.persist.recent_since::<SourceData, (), Timestamp, StorageDiff>(
            shard, Diagnostics::for_tests(),
        ).await.expect("observe applied since after restart"),
        Antichain::from_elem(Timestamp::new(90_000)),
    );
    let mut reader = f
        .persist
        .open_leased_reader::<SourceData, (), Timestamp, StorageDiff>(
            shard,
            Arc::new(
                f.catalog
                    .state()
                    .try_get_desc_by_global_id(&input)
                    .expect("reconstructed source descriptor")
                    .into_owned(),
            ),
            Arc::new(UnitSchema),
            Diagnostics::for_tests(),
            false,
        )
        .await
        .expect("open historical reader after compaction");
    assert!(
        reader
            .snapshot_and_fetch(Antichain::from_elem(Timestamp::new(85_000)))
            .await
            .is_err()
    );
    let rows = reader
        .snapshot_and_fetch(Antichain::from_elem(Timestamp::new(90_000)))
        .await
        .expect("policy-retained snapshot remains readable");
    assert_eq!(
        rows.into_iter()
            .map(|((row, ()), _, diff)| (row, diff))
            .collect::<Vec<_>>(),
        vec![(row, 1)]
    );
    reader.expire().await;
    f.advance_upper(input, 120_000).await;
    f.propose_bounds(&[input], 120_000, 110_000).await;
    f.catalog.expire().await;
}

#[mz_ore::test(tokio::test)]
#[cfg_attr(miri, ignore)]
async fn index_retention_protects_logical_input_eliminated_by_optimizer() {
    let mut f = Fixture::new().await;
    let live = f.source("live_input").await;
    let eliminated = f.source("eliminated_input").await;
    f.advance_upper(live, 100_000).await;
    f.advance_upper(eliminated, 100_000).await;
    let (view_id, _) = f
        .create(
            "logical_view",
            format!(
                "CREATE VIEW {}.logical_view AS \
                 SELECT counter FROM {}.live_input UNION ALL \
                 SELECT counter FROM {}.eliminated_input WHERE false",
                f.prefix, f.prefix, f.prefix,
            ),
        )
        .await;
    let CatalogItem::View(view) = f.catalog.state().get_entry(&view_id).item() else {
        panic!("expected view");
    };
    assert_eq!(
        view.raw_expr.depends_on(),
        BTreeSet::from([live, eliminated])
    );
    assert_eq!(
        view.locally_optimized_expr.depends_on(),
        BTreeSet::from([live]),
        "fixture must actually eliminate the second input"
    );
    f.index("retaining_index", "logical_view", &[live]).await;
    f.propose_bounds(&[live, eliminated], 100_000, 90_000).await;
    f.catalog.expire().await;
}

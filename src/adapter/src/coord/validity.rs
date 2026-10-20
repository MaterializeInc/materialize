// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{BTreeMap, BTreeSet};

use mz_cluster_client::ReplicaId;
use mz_compute_types::ComputeInstanceId;
use mz_repr::CatalogItemId;
use mz_sql::catalog::CatalogItem;
use mz_sql::rbac::UnauthorizedError;
use mz_sql::session::user::RoleMetadata;

use crate::AdapterError;
use crate::catalog::{Catalog, CatalogReadRecorder, CatalogReadSet};

// The inner fields of PlanValidity are not pub to prevent callers from using them in SQL logic.
// Callers are responsible for tracking their own needed IDs explicitly and not using
// PlanValidity as a logic sidecar.

/// A struct to hold information about the validity of plans and if they should be abandoned after
/// doing work off of the Coordinator thread.
///
/// Concurrent DDLs (those returning `false` from `must_serialize_ddl()`) can mutate the catalog
/// while a staged statement is being optimized off-thread. `check` is called at every off-thread
/// → on-thread hop in `sequence_staged` so that dropped dependencies / clusters / replicas /
/// roles surface as a user-facing error instead of panicking later when the persisted SQL is
/// re-parsed during catalog application.
///
/// Read-modify-write plans can opt into definition observations with
/// [`Self::with_dependency_definition_check`]. Read-only plans retain identity
/// checks because renames do not invalidate their stable-ID references.
#[derive(Debug, Clone)]
pub struct PlanValidity {
    /// The most recent revision at which this plan was verified as valid.
    transient_revision: u64,
    /// Objects on which the plan depends.
    dependency_ids: BTreeSet<CatalogItemId>,
    dependency_reads: Option<BTreeMap<CatalogItemId, CatalogReadSet>>,
    catalog_reads: Option<CatalogReadRecorder>,
    cluster_id: Option<ComputeInstanceId>,
    replica_id: Option<ReplicaId>,
    role_metadata: RoleMetadata,
}

impl PlanValidity {
    pub fn new(
        catalog: &Catalog,
        dependency_ids: BTreeSet<CatalogItemId>,
        cluster_id: Option<ComputeInstanceId>,
        replica_id: Option<ReplicaId>,
        role_metadata: RoleMetadata,
    ) -> Self {
        PlanValidity {
            transient_revision: catalog.transient_revision(),
            dependency_ids,
            dependency_reads: None,
            catalog_reads: None,
            cluster_id,
            replica_id,
            role_metadata,
        }
    }

    /// Observe dependency definitions for plans that read-modify-write stored SQL.
    pub fn with_dependency_definition_check(mut self, catalog: &Catalog) -> Self {
        self.dependency_reads = Some(
            self.dependency_ids
                .iter()
                .map(|id| (*id, observe_definition(catalog, *id)))
                .collect(),
        );
        self
    }

    /// Checks the recorder's current observations, including reads added after construction.
    pub fn with_catalog_reads(mut self, reads: Option<CatalogReadRecorder>) -> Self {
        self.catalog_reads = reads;
        self
    }

    /// Captures catalog predicates to validate before the prepared writes.
    pub fn catalog_preconditions(&self) -> Vec<crate::catalog::Op> {
        let mut ops: Vec<_> = self
            .dependency_reads
            .iter()
            .flat_map(|reads| reads.values())
            .cloned()
            .map(crate::catalog::Op::ValidateReads)
            .collect();
        if let Some(reads) = &self.catalog_reads {
            ops.push(crate::catalog::Op::ValidateReads(reads.snapshot()));
        }
        ops
    }

    pub fn extend_dependencies(
        &mut self,
        catalog: &Catalog,
        ids: impl IntoIterator<Item = CatalogItemId>,
    ) {
        for id in ids {
            if self.dependency_ids.insert(id) {
                if let Some(reads) = &mut self.dependency_reads {
                    reads.insert(id, observe_definition(catalog, id));
                }
            }
        }
    }

    /// Reject missing dependencies or changes to explicitly observed definitions.
    pub fn check(&mut self, catalog: &Catalog) -> Result<(), AdapterError> {
        if self.transient_revision == catalog.transient_revision() {
            return self.check_catalog_reads(catalog);
        }
        // If the transient revision changed, we have to recheck. If successful, bump the revision
        // so next check uses the above fast path.
        if let Some(cluster_id) = self.cluster_id {
            let Some(cluster) = catalog.try_get_cluster(cluster_id) else {
                return Err(AdapterError::ConcurrentDependencyDrop {
                    dependency_kind: "cluster",
                    dependency_id: cluster_id.to_string(),
                });
            };

            if let Some(replica_id) = self.replica_id {
                if cluster.replica(replica_id).is_none() {
                    return Err(AdapterError::ConcurrentDependencyDrop {
                        dependency_kind: "cluster replica",
                        dependency_id: format!("{replica_id} of cluster {cluster_id}"),
                    });
                }
            }
        }
        for id in self.dependency_ids.iter() {
            if catalog.try_get_entry(id).is_none() {
                return Err(AdapterError::ConcurrentDependencyDrop {
                    dependency_kind: "catalog item",
                    dependency_id: id.to_string(),
                });
            }
            if let Some(reads) = &self.dependency_reads {
                if let Some(reads) = reads.get(id) {
                    if reads.check(catalog.state()).is_err() {
                        return Err(AdapterError::ConcurrentDependencyMutation {
                            dependency_id: id.to_string(),
                        });
                    }
                }
            }
        }
        if catalog
            .try_get_role(&self.role_metadata.current_role)
            .is_none()
        {
            return Err(AdapterError::Unauthorized(
                UnauthorizedError::ConcurrentRoleDrop(self.role_metadata.current_role.clone()),
            ));
        }
        if catalog
            .try_get_role(&self.role_metadata.session_role)
            .is_none()
        {
            return Err(AdapterError::Unauthorized(
                UnauthorizedError::ConcurrentRoleDrop(self.role_metadata.session_role.clone()),
            ));
        }

        if catalog
            .try_get_role(&self.role_metadata.authenticated_role)
            .is_none()
        {
            return Err(AdapterError::Unauthorized(
                UnauthorizedError::ConcurrentRoleDrop(
                    self.role_metadata.authenticated_role.clone(),
                ),
            ));
        }
        self.check_catalog_reads(catalog)?;
        self.transient_revision = catalog.transient_revision();
        Ok(())
    }

    fn check_catalog_reads(&self, catalog: &Catalog) -> Result<(), AdapterError> {
        // Preparation can add observations without changing the catalog revision.
        if let Some(reads) = &self.catalog_reads {
            reads.snapshot().check(catalog.state())?;
        }
        Ok(())
    }
}

fn observe_definition(catalog: &Catalog, id: CatalogItemId) -> CatalogReadSet {
    let mut reads = CatalogReadSet::default();
    let _ = reads.observe(catalog.state(), "dependency definition", move |state| {
        state
            .try_get_entry(&id)
            .map(|entry| entry.create_sql().to_owned())
    });
    reads
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use mz_adapter_types::connection::ConnectionId;
    use mz_auth::AuthenticatorKind;
    use mz_cluster_client::ReplicaId;
    use mz_controller_types::ClusterId;
    use mz_ore::metrics::MetricsRegistry;
    use mz_ore::{assert_contains, assert_ok};
    use mz_repr::CatalogItemId;
    use mz_repr::role_id::RoleId;
    use mz_sql::catalog::{CatalogDatabase, CatalogSchema, RoleAttributesRaw, SessionCatalog};
    use mz_sql::session::metadata::SessionMetadata;
    use uuid::Uuid;

    use crate::AdapterError;
    use crate::catalog::{Catalog, Op};
    use crate::coord::validity::PlanValidity;
    use crate::metrics::Metrics;
    use crate::session::{Session, SessionConfig};

    #[mz_ore::test(tokio::test)]
    #[cfg_attr(miri, ignore)] // unsupported operation: returning ready events from epoll_wait is not yet implemented
    async fn test_plan_validity() {
        Catalog::with_debug(|mut catalog| async move {
            let conn_id = ConnectionId::Static(1);
            let user = String::from("validity_user");
            let role = "validity_role";
            let metrics_registry = MetricsRegistry::new();
            let metrics = Metrics::register_into(&metrics_registry);

            let commit_ts = catalog.current_upper().await;
            catalog
                .transact(
                    None,
                    commit_ts,
                    None,
                    vec![Op::CreateRole {
                        name: role.into(),
                        attributes: RoleAttributesRaw::new(),
                    }],
                )
                .await
                .expect("is ok");
            let role = catalog.try_get_role_by_name(role).expect("must exist");
            // Can't use a dummy session because we need a valid role for the validity check.
            let mut session = Session::new(
                &mz_build_info::DUMMY_BUILD_INFO,
                SessionConfig {
                    conn_id,
                    uuid: Uuid::new_v4(),
                    user,
                    client_ip: None,
                    external_metadata_rx: None,
                    helm_chart_version: None,
                    authenticator_kind: AuthenticatorKind::None,
                    groups: None,
                },
                metrics.session_metrics(),
            );
            session.initialize_role_metadata(role.id);
            let mut empty = PlanValidity::new(
                &catalog,
                BTreeSet::new(),
                None,
                None,
                session.role_metadata().clone(),
            );
            // Push the revision back by 1 so check() takes the slow path instead of returning
            // early on revision equality.
            empty.transient_revision = empty
                .transient_revision
                .checked_sub(1)
                .expect("must subtract");
            let some_system_cluster = catalog
                .clusters()
                .find(|c| matches!(c.id, ClusterId::System(_)))
                .expect("must exist")
                .id;

            // Plan generation and result assertion closures.
            let tests: &[(
                Box<dyn Fn(&mut PlanValidity, &Catalog)>,
                Box<dyn Fn(Result<(), AdapterError>)>,
            )] = &[
                (
                    Box::new(|_validity, _catalog| {}),
                    Box::new(|res| assert_ok!(res)),
                ),
                (
                    Box::new(|validity, _catalog| {
                        validity.cluster_id = Some(ClusterId::user(3).expect("3 is a valid ID"));
                    }),
                    Box::new(|res| {
                        assert_contains!(
                            res.expect_err("must err").to_string(),
                            "cluster 'u3' was dropped"
                        )
                    }),
                ),
                (
                    Box::new(|validity, _catalog| {
                        validity.cluster_id = Some(some_system_cluster);
                        validity.replica_id = Some(ReplicaId::User(4));
                    }),
                    Box::new(|res| {
                        assert_contains!(
                            res.expect_err("must err").to_string(),
                            format!(
                                "cluster replica 'u4 of cluster {}' was dropped",
                                some_system_cluster
                            ),
                        )
                    }),
                ),
                (
                    Box::new(|validity, catalog| {
                        validity.extend_dependencies(catalog, vec![CatalogItemId::User(6)]);
                    }),
                    Box::new(|res| {
                        assert_contains!(
                            res.expect_err("must err").to_string(),
                            "catalog item 'u6' was dropped"
                        )
                    }),
                ),
                (
                    Box::new(|validity, _catalog| {
                        validity.role_metadata.current_role = RoleId::User(5);
                    }),
                    Box::new(|res| {
                        assert_contains!(
                            res.expect_err("must err").to_string(),
                            "role u5 was concurrently dropped"
                        )
                    }),
                ),
                (
                    Box::new(|validity, _catalog| {
                        validity.role_metadata.session_role = RoleId::User(5);
                    }),
                    Box::new(|res| {
                        assert_contains!(
                            res.expect_err("must err").to_string(),
                            "role u5 was concurrently dropped"
                        )
                    }),
                ),
                (
                    Box::new(|validity, _catalog| {
                        validity.role_metadata.authenticated_role = RoleId::User(5);
                    }),
                    Box::new(|res| {
                        assert_contains!(
                            res.expect_err("must err").to_string(),
                            "role u5 was concurrently dropped"
                        )
                    }),
                ),
            ];
            for (get_validity, check_res) in tests {
                let mut validity = empty.clone();
                get_validity(&mut validity, &catalog);
                let res = validity.check(&catalog);
                check_res(res);
            }

            let database_spec = mz_sql::names::ResolvedDatabaseSpecifier::Id(
                catalog
                    .resolve_database(crate::session::DEFAULT_DATABASE_NAME)
                    .expect("default database")
                    .id(),
            );
            let schema_spec = *catalog
                .resolve_schema_in_database(
                    &database_spec,
                    mz_sql::DEFAULT_SCHEMA,
                    &mz_catalog::SYSTEM_CONN_ID,
                )
                .expect("default schema")
                .id();
            let name = mz_sql::names::QualifiedItemName {
                qualifiers: mz_sql::names::ItemQualifiers {
                    database_spec,
                    schema_spec,
                },
                item: "validity_secret".to_owned(),
            };
            let full_name = catalog.resolve_full_name(&name, None);
            let (live_id, global_id) = catalog
                .allocate_user_id_for_test()
                .await
                .expect("allocate secret identity");
            let timestamp = catalog.current_upper().await;
            catalog
                .transact(
                    None,
                    timestamp,
                    None,
                    vec![Op::CreateItem {
                        id: live_id,
                        name,
                        item: mz_catalog::memory::objects::CatalogItem::Secret(
                            mz_catalog::memory::objects::Secret {
                                create_sql: format!("CREATE SECRET {full_name} AS 'fixture'"),
                                global_id,
                            },
                        ),
                        owner_id: *session.current_role_id(),
                    }],
                )
                .await
                .expect("create dependency");
            let mut unarmed = PlanValidity::new(
                &catalog,
                BTreeSet::from([live_id]),
                None,
                None,
                session.role_metadata().clone(),
            );
            let mut armed = unarmed.clone().with_dependency_definition_check(&catalog);
            let reads = crate::catalog::CatalogReadRecorder::default();
            let snapshot = catalog.state().clone();
            let prepared_catalog = snapshot
                .for_system_session()
                .with_read_recorder(reads.clone());
            let mut observed = unarmed.clone().with_catalog_reads(Some(reads));
            let mut extended = PlanValidity::new(
                &catalog,
                BTreeSet::new(),
                None,
                None,
                session.role_metadata().clone(),
            )
            .with_dependency_definition_check(&catalog);
            extended.extend_dependencies(&catalog, [live_id]);
            let timestamp = catalog.current_upper().await;
            catalog
                .transact(
                    None,
                    timestamp,
                    None,
                    vec![Op::RenameItem {
                        id: live_id,
                        current_full_name: full_name,
                        to_name: "renamed_validity_secret".to_owned(),
                    }],
                )
                .await
                .expect("rename dependency");
            for validity in [&mut armed, &mut extended] {
                assert!(matches!(
                    validity.check(&catalog),
                    Err(AdapterError::ConcurrentDependencyMutation { .. })
                ));
            }
            assert_ok!(unarmed.check(&catalog));
            assert_ok!(observed.check(&catalog));
            let _ = prepared_catalog.get_item(&live_id).create_sql();
            assert!(matches!(
                observed.check(&catalog),
                Err(AdapterError::DDLTransactionRace)
            ));
            catalog.expire().await;
        })
        .await
    }
}

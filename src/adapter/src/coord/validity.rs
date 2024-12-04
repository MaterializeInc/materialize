// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::BTreeSet;

use mz_cluster_client::ReplicaId;
use mz_compute_types::ComputeInstanceId;
use mz_repr::CatalogItemId;
use mz_sql::rbac::UnauthorizedError;
use mz_sql::session::user::RoleMetadata;

use crate::catalog::Catalog;
use crate::AdapterError;

// The inner fields of PlanValidity are not pub to prevent callers from using them in SQL logic.
// Callers are responsible for tracking their own needed IDs explicitly and not using
// PlanValidity as a logic sidecar.

/// A struct to hold information about the validity of plans and if they should be abandoned after
/// doing work off of the Coordinator thread.
#[derive(Debug, Clone)]
pub enum PlanValidity {
    /// Requires a specific transient revision.
    RequireRevision { required_revision: u64 },
    /// Checks various catalog IDs. Uses the transient revision only as a cache marker.
    Checks {
        /// The most recent revision at which this plan was verified as valid.
        transient_revision: u64,
        /// Objects on which the plan depends.
        dependency_ids: BTreeSet<CatalogItemId>,
        cluster_id: Option<ComputeInstanceId>,
        replica_id: Option<ReplicaId>,
        role_metadata: RoleMetadata,
    },
}

impl PlanValidity {
    pub fn new(
        transient_revision: u64,
        dependency_ids: BTreeSet<CatalogItemId>,
        cluster_id: Option<ComputeInstanceId>,
        replica_id: Option<ReplicaId>,
        role_metadata: RoleMetadata,
    ) -> Self {
        PlanValidity::Checks {
            transient_revision,
            dependency_ids,
            cluster_id,
            replica_id,
            role_metadata,
        }
    }

    /// WARNING: This is currently a no-op and `check` will always succeed.
    ///
    /// Sets the required `transient_revision` of the catalog. Should only be used by serialized
    /// statements (and thus should never fail for users), but here as an internal failsafe against
    /// programming errors.
    pub fn require_transient_revision(required_revision: u64) -> Self {
        PlanValidity::RequireRevision { required_revision }
    }

    /// Panics if not called on a Checks variant.
    pub fn extend_dependencies(&mut self, ids: impl Iterator<Item = CatalogItemId>) {
        let Self::Checks { dependency_ids, .. } = self else {
            unreachable!();
        };
        dependency_ids.extend(ids);
    }

    /// Returns an error if the current catalog no longer has all dependencies.
    pub fn check(&mut self, catalog: &Catalog) -> Result<(), AdapterError> {
        match self {
            PlanValidity::RequireRevision { required_revision } => {
                if catalog.transient_revision() != *required_revision {
                    // TODO: We would like to use this as a programming check that no catalog
                    // revisions were made as a double-check that all DDLs are serialized. However,
                    // since only *most* DDLs are serialized (see `must_serialize_ddl()` for those
                    // that aren't), it is possible for two DDLs to run concurrently and the catalog
                    // revision to increment during the off-thread work from this DDL. For example,
                    // a CREATE VIEW could be off-thread optimizing while an ALTER SECRET runs and
                    // increments the revision. For now we assume this check is not strictly needed
                    // because we have thought medium hard about the DDLs that do not require
                    // serialization, so they do not pose a correctness problem when executing
                    // concurrently with any other DDL.
                    //
                    // If we want to remove the need to even think at all about DDL ordering
                    // correctness we would need to refactor all calls to catalog_transact to
                    // require passing the serialized DDL lock. Statements would be responsible for
                    // getting the lock at the latest possible correct time. ALTER SECRET for
                    // example could acquire the lock after interacting with k8s, but most other
                    // DDLs would get the lock for their entire sequencing duration.

                    //soft_panic_or_log!("another DDL executed while this assumed it was serial");
                }
                Ok(())
            }
            PlanValidity::Checks {
                transient_revision,
                dependency_ids,
                cluster_id,
                replica_id,
                role_metadata,
            } => {
                if *transient_revision == catalog.transient_revision() {
                    return Ok(());
                }
                // If the transient revision changed, we have to recheck. If successful, bump the revision
                // so next check uses the above fast path.
                if let Some(cluster_id) = cluster_id {
                    let Some(cluster) = catalog.try_get_cluster(*cluster_id) else {
                        return Err(AdapterError::ChangedPlan(format!(
                            "cluster {} was removed",
                            cluster_id
                        )));
                    };

                    if let Some(replica_id) = replica_id {
                        if cluster.replica(*replica_id).is_none() {
                            return Err(AdapterError::ChangedPlan(format!(
                                "replica {} of cluster {} was removed",
                                replica_id, cluster_id
                            )));
                        }
                    }
                }
                // It is sufficient to check that all the dependency_ids still exist because we assume:
                // - Ids do not mutate.
                // - Ids are not reused.
                // - If an id was dropped, this will detect it and error.
                for id in dependency_ids.iter() {
                    if catalog.try_get_entry(id).is_none() {
                        return Err(AdapterError::ChangedPlan(format!(
                            "dependency was removed: {id}",
                        )));
                    }
                }
                if catalog.try_get_role(&role_metadata.current_role).is_none() {
                    return Err(AdapterError::Unauthorized(
                        UnauthorizedError::ConcurrentRoleDrop(role_metadata.current_role.clone()),
                    ));
                }
                if catalog.try_get_role(&role_metadata.session_role).is_none() {
                    return Err(AdapterError::Unauthorized(
                        UnauthorizedError::ConcurrentRoleDrop(role_metadata.session_role.clone()),
                    ));
                }

                if catalog
                    .try_get_role(&role_metadata.authenticated_role)
                    .is_none()
                {
                    return Err(AdapterError::Unauthorized(
                        UnauthorizedError::ConcurrentRoleDrop(
                            role_metadata.authenticated_role.clone(),
                        ),
                    ));
                }
                *transient_revision = catalog.transient_revision();
                Ok(())
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use mz_adapter_types::connection::ConnectionId;
    use mz_cluster_client::ReplicaId;
    use mz_controller_types::ClusterId;
    use mz_ore::metrics::MetricsRegistry;
    use mz_ore::{assert_contains, assert_ok};
    use mz_repr::role_id::RoleId;
    use mz_repr::{CatalogItemId, Timestamp};
    use mz_sql::catalog::RoleAttributes;
    use mz_sql::session::metadata::SessionMetadata;
    use uuid::Uuid;

    use crate::catalog::{Catalog, Op};
    use crate::coord::validity::PlanValidity;
    use crate::metrics::Metrics;
    use crate::session::{Session, SessionConfig};
    use crate::AdapterError;

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
                        attributes: RoleAttributes::new(),
                    }],
                )
                .await
                .expect("is ok");
            let role = catalog.try_get_role_by_name(role).expect("must exist");
            // Can't use a dummy session because we need a valid role for the validity check.
            let mut session = Session::<Timestamp>::new(
                &mz_build_info::DUMMY_BUILD_INFO,
                SessionConfig {
                    conn_id,
                    uuid: Uuid::new_v4(),
                    user,
                    client_ip: None,
                    external_metadata_rx: None,
                    helm_chart_version: None,
                },
                metrics.session_metrics(),
            );
            session.initialize_role_metadata(role.id);
            let empty = PlanValidity::new(
                // Set the transient rev 1 down so the check logic runs.
                catalog
                    .transient_revision()
                    .checked_sub(1)
                    .expect("must subtract"),
                BTreeSet::new(),
                None,
                None,
                session.role_metadata().clone(),
            );
            let some_system_cluster = catalog
                .clusters()
                .find(|c| matches!(c.id, ClusterId::System(_)))
                .expect("must exist");

            // Plan generation and result assertion closures.
            let tests: &[(
                Box<dyn Fn(&mut PlanValidity)>,
                Box<dyn Fn(Result<(), AdapterError>)>,
            )] = &[
                (Box::new(|_validity| {}), Box::new(|res| assert_ok!(res))),
                (
                    Box::new(|validity| {
                        let PlanValidity::Checks { cluster_id, .. } = validity else {
                            panic!();
                        };
                        *cluster_id = Some(ClusterId::user(3).expect("3 is a valid ID"));
                    }),
                    Box::new(|res| {
                        assert_contains!(
                            res.expect_err("must err").to_string(),
                            "cluster u3 was removed"
                        )
                    }),
                ),
                (
                    Box::new(|validity| {
                        let PlanValidity::Checks {
                            cluster_id,
                            replica_id,
                            ..
                        } = validity
                        else {
                            panic!();
                        };
                        *cluster_id = Some(some_system_cluster.id);
                        *replica_id = Some(ReplicaId::User(4));
                    }),
                    Box::new(|res| {
                        assert_contains!(
                            res.expect_err("must err").to_string(),
                            format!(
                                "replica u4 of cluster {} was removed",
                                some_system_cluster.id
                            ),
                        )
                    }),
                ),
                (
                    Box::new(|validity| {
                        validity.extend_dependencies(vec![CatalogItemId::User(6)].into_iter());
                    }),
                    Box::new(|res| {
                        assert_contains!(
                            res.expect_err("must err").to_string(),
                            "dependency was removed: u6"
                        )
                    }),
                ),
                (
                    Box::new(|validity| {
                        let PlanValidity::Checks { role_metadata, .. } = validity else {
                            panic!();
                        };
                        role_metadata.current_role = RoleId::User(5);
                    }),
                    Box::new(|res| {
                        assert_contains!(
                            res.expect_err("must err").to_string(),
                            "role u5 was concurrently dropped"
                        )
                    }),
                ),
                (
                    Box::new(|validity| {
                        let PlanValidity::Checks { role_metadata, .. } = validity else {
                            panic!();
                        };
                        role_metadata.session_role = RoleId::User(5);
                    }),
                    Box::new(|res| {
                        assert_contains!(
                            res.expect_err("must err").to_string(),
                            "role u5 was concurrently dropped"
                        )
                    }),
                ),
                (
                    Box::new(|validity| {
                        let PlanValidity::Checks { role_metadata, .. } = validity else {
                            panic!();
                        };
                        role_metadata.authenticated_role = RoleId::User(5);
                    }),
                    Box::new(|res| {
                        assert_contains!(
                            res.expect_err("must err").to_string(),
                            "role u5 was concurrently dropped"
                        )
                    }),
                ),
                (
                    Box::new(|validity| {
                        *validity = PlanValidity::require_transient_revision(100);
                    }),
                    Box::new(|res| {
                        // This check is a no-op.
                        assert!(res.is_ok());
                    }),
                ),
                (
                    Box::new(|validity| {
                        *validity =
                            PlanValidity::require_transient_revision(catalog.transient_revision());
                    }),
                    Box::new(|res| {
                        assert!(res.is_ok());
                    }),
                ),
            ];
            for (get_validity, check_res) in tests {
                let mut validity = empty.clone();
                get_validity(&mut validity);
                let res = validity.check(&catalog);
                check_res(res);
            }
        })
        .await
    }
}

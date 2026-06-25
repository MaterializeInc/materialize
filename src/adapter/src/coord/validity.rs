// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::collections::{BTreeMap, BTreeSet};
use std::hash::{Hash, Hasher};

use mz_cluster_client::ReplicaId;
use mz_compute_types::ComputeInstanceId;
use mz_repr::CatalogItemId;
use mz_sql::catalog::CatalogItem;
use mz_sql::rbac::UnauthorizedError;
use mz_sql::session::user::RoleMetadata;

use crate::AdapterError;
use crate::catalog::Catalog;

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
/// Opt-in: callers that perform a read-modify-write of a dependency's `create_sql` (e.g.
/// `ALTER CONNECTION ... ROTATE KEYS`, see SQL-272) can additionally arm a `create_sql`-hash
/// check via [`Self::with_dependency_hash_check`]. When armed, `check` re-hashes each
/// dependency's `create_sql` and fails with [`AdapterError::ConcurrentDependencyMutation`] on a
/// mismatch. The check must stay opt-in: a benign concurrent `RENAME` rewrites the dependency's
/// `create_sql` and would otherwise spuriously fail read-only plans like peeks and SUBSCRIBE,
/// whose optimized dataflows are unaffected by a rename (dependencies are referenced by stable
/// id).
#[derive(Debug, Clone)]
pub struct PlanValidity {
    /// The most recent revision at which this plan was verified as valid.
    transient_revision: u64,
    /// Objects on which the plan depends.
    dependency_ids: BTreeSet<CatalogItemId>,
    /// Hash of each dependency's `create_sql` at the time this plan was built. Empty unless
    /// `check_dependency_hashes` is set. When set, `check` compares the live hash against the
    /// captured one; a mismatch means the dependency was concurrently modified.
    dependency_hashes: BTreeMap<CatalogItemId, u64>,
    /// Whether `check` should compare `dependency_hashes` against the catalog. Off by default;
    /// armed by [`Self::with_dependency_hash_check`] for the narrow set of plans that
    /// read-modify-write a dependency's `create_sql`.
    check_dependency_hashes: bool,
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
            dependency_hashes: BTreeMap::new(),
            check_dependency_hashes: false,
            cluster_id,
            replica_id,
            role_metadata,
        }
    }

    /// Arm the `create_sql`-hash check on this validity. Use only for plans that
    /// read-modify-write a dependency's `create_sql` (e.g. `ALTER CONNECTION ... ROTATE KEYS`).
    /// Snapshots the current `create_sql` of every id already in `dependency_ids`; subsequent
    /// calls to [`Self::extend_dependencies`] will also capture hashes.
    pub fn with_dependency_hash_check(mut self, catalog: &Catalog) -> Self {
        self.check_dependency_hashes = true;
        self.dependency_hashes = self
            .dependency_ids
            .iter()
            .filter_map(|id| hash_item_create_sql(catalog, *id).map(|h| (*id, h)))
            .collect();
        self
    }

    pub fn extend_dependencies(
        &mut self,
        catalog: &Catalog,
        ids: impl IntoIterator<Item = CatalogItemId>,
    ) {
        for id in ids {
            if self.dependency_ids.insert(id) && self.check_dependency_hashes {
                if let Some(hash) = hash_item_create_sql(catalog, id) {
                    self.dependency_hashes.insert(id, hash);
                }
            }
        }
    }

    /// Returns an error if the current catalog no longer has all dependencies, or — when the
    /// hash check is armed via [`Self::with_dependency_hash_check`] — if any dependency's
    /// `create_sql` has changed since this `PlanValidity` was built.
    pub fn check(&mut self, catalog: &Catalog) -> Result<(), AdapterError> {
        if self.transient_revision == catalog.transient_revision() {
            return Ok(());
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
        // Ids don't mutate and aren't reused, so a missing entry means the dependency was
        // dropped. When the hash check is armed, a `create_sql` mismatch additionally means the
        // dependency was concurrently modified between plan time and now.
        for id in self.dependency_ids.iter() {
            let Some(entry) = catalog.try_get_entry(id) else {
                return Err(AdapterError::ConcurrentDependencyDrop {
                    dependency_kind: "catalog item",
                    dependency_id: id.to_string(),
                });
            };
            if self.check_dependency_hashes {
                if let Some(expected) = self.dependency_hashes.get(id) {
                    let current = hash_create_sql(entry.create_sql());
                    if current != *expected {
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
        self.transient_revision = catalog.transient_revision();
        Ok(())
    }
}

/// Returns the hash of `id`'s `create_sql`, or `None` if the catalog has no such entry.
fn hash_item_create_sql(catalog: &Catalog, id: CatalogItemId) -> Option<u64> {
    catalog
        .try_get_entry(&id)
        .map(|entry| hash_create_sql(entry.create_sql()))
}

fn hash_create_sql(sql: &str) -> u64 {
    let mut h = std::collections::hash_map::DefaultHasher::new();
    sql.hash(&mut h);
    h.finish()
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
    use mz_sql::catalog::RoleAttributesRaw;
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
                .expect("must exist");

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
                        validity.cluster_id = Some(some_system_cluster.id);
                        validity.replica_id = Some(ReplicaId::User(4));
                    }),
                    Box::new(|res| {
                        assert_contains!(
                            res.expect_err("must err").to_string(),
                            format!(
                                "cluster replica 'u4 of cluster {}' was dropped",
                                some_system_cluster.id
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

            // Pick any existing catalog entry to stand in as a "live" dependency. A real id is
            // required so `check`'s drop probe (`try_get_entry`) succeeds and we exercise the
            // hash branch.
            let live_id = catalog
                .entries()
                .next()
                .expect("debug catalog must have at least one entry")
                .id();

            // Armed + hash mismatch → ConcurrentDependencyMutation.
            let mut armed = PlanValidity::new(
                &catalog,
                BTreeSet::from_iter(std::iter::once(live_id)),
                None,
                None,
                session.role_metadata().clone(),
            )
            .with_dependency_hash_check(&catalog);
            armed.transient_revision = armed
                .transient_revision
                .checked_sub(1)
                .expect("must subtract");
            // Corrupt the captured hash to simulate a concurrent mutation of the dependency's
            // `create_sql`.
            armed.dependency_hashes.insert(live_id, u64::MAX);
            assert_contains!(
                armed.check(&catalog).expect_err("must err").to_string(),
                "was concurrently modified"
            );

            // Unarmed (default) + same hash mismatch → ignored, returns Ok. Proves the opt-in
            // gate keeps the check off the shared read-only paths.
            let mut unarmed = PlanValidity::new(
                &catalog,
                BTreeSet::from_iter(std::iter::once(live_id)),
                None,
                None,
                session.role_metadata().clone(),
            );
            unarmed.transient_revision = unarmed
                .transient_revision
                .checked_sub(1)
                .expect("must subtract");
            // Stuff a bogus hash in. The flag is off, so `check` must not look at it.
            unarmed.dependency_hashes.insert(live_id, u64::MAX);
            assert_ok!(unarmed.check(&catalog));
        })
        .await
    }
}

// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Logic related to applying updates from a [`mz_catalog::durable::DurableCatalogState`] to a
//! [`CatalogState`].

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fmt::Debug;
use std::str::FromStr;

use mz_catalog::builtin::BUILTIN_LOG_LOOKUP;
use mz_catalog::memory::error::{Error, ErrorKind};
use mz_catalog::memory::objects::{
    Cluster, ClusterReplica, ClusterReplicaProcessStatus, Database, Role, Schema, StateUpdate,
    StateUpdateKind,
};
use mz_compute_client::controller::ComputeReplicaConfig;
use mz_controller::clusters::{ClusterStatus, ReplicaConfig, ReplicaLogging};
use mz_ore::cast::CastFrom;
use mz_ore::instrument;
use mz_ore::now::to_datetime;
use mz_repr::adt::mz_acl_item::{MzAclItem, PrivilegeMap};
use mz_repr::{Diff, GlobalId};
use mz_sql::catalog::{CatalogError as SqlCatalogError, CatalogSchema};
use mz_sql::names::{
    ItemQualifiers, QualifiedItemName, QualifiedSchemaName, ResolvedDatabaseSpecifier,
    SchemaSpecifier,
};
use mz_sql::plan;
use mz_sql::session::vars::{VarError, VarInput};
use once_cell::sync::Lazy;
use regex::Regex;
use tracing::warn;

use crate::catalog::CatalogState;
use crate::AdapterError;

enum ApplyUpdateError {
    Error(Error),
    AwaitingIdDependency((GlobalId, mz_catalog::durable::Item, Diff)),
    AwaitingNameDependency((String, mz_catalog::durable::Item, Diff)),
}

impl CatalogState {
    /// Update in-memory catalog state from a list of updates made to the durable catalog state.
    ///
    /// This is meant specifically for bootstrapping because it does not produce builtin table
    /// updates. The builtin tables need to be loaded before we can produce builtin table updates
    /// which creates a bootstrapping problem.
    // TODO(jkosh44) It is very IMPORTANT that per timestamp, the updates are sorted retractions
    // then additions. Within the retractions the objects should be sorted in reverse dependency
    // order (objects->schema->database, replica->cluster, etc.). Within the additions the objects
    // should be sorted in dependency order (database->schema->objects, cluster->replica, etc.).
    // Objects themselves also need to be sorted by dependency order, this will be tricky but we can
    // look at the existing bootstrap code for ways of doing this. For now we rely on the caller
    // providing objects in dependency order.
    #[instrument]
    pub(crate) fn apply_updates_for_bootstrap(
        &mut self,
        updates: Vec<StateUpdate>,
    ) -> Result<(), Error> {
        let mut awaiting_id_dependencies: BTreeMap<GlobalId, Vec<_>> = BTreeMap::new();
        let mut awaiting_name_dependencies: BTreeMap<String, Vec<_>> = BTreeMap::new();
        let mut updates: VecDeque<_> = updates.into_iter().collect();
        while let Some(StateUpdate { kind, diff }) = updates.pop_front() {
            assert_eq!(
                diff, 1,
                "initial catalog updates should be consolidated: ({kind:?}, {diff:?})"
            );
            match self.apply_update(kind, diff) {
                Ok(None) => {}
                Ok(Some(id)) => {
                    // Enqueue any items waiting on this dependency.
                    let mut resolved_dependent_items = Vec::new();
                    if let Some(dependent_items) = awaiting_id_dependencies.remove(&id) {
                        resolved_dependent_items.extend(dependent_items);
                    }
                    let entry = self.get_entry(&id);
                    let full_name = self.resolve_full_name(entry.name(), None);
                    if let Some(dependent_items) =
                        awaiting_name_dependencies.remove(&full_name.to_string())
                    {
                        resolved_dependent_items.extend(dependent_items);
                    }
                    let resolved_dependent_items =
                        resolved_dependent_items
                            .into_iter()
                            .map(|(item, diff)| StateUpdate {
                                kind: StateUpdateKind::Item(item),
                                diff,
                            });
                    updates.extend(resolved_dependent_items);
                }
                Err(ApplyUpdateError::Error(err)) => return Err(err),
                Err(ApplyUpdateError::AwaitingIdDependency((id, item, diff))) => {
                    awaiting_id_dependencies
                        .entry(id)
                        .or_default()
                        .push((item, diff));
                }
                Err(ApplyUpdateError::AwaitingNameDependency((name, item, diff))) => {
                    awaiting_name_dependencies
                        .entry(name)
                        .or_default()
                        .push((item, diff));
                }
            }
        }

        // Error on any unsatisfied dependencies.
        if let Some((missing_dep, mut dependents)) = awaiting_id_dependencies.into_iter().next() {
            let (
                mz_catalog::durable::Item {
                    id,
                    oid: _,
                    schema_id,
                    name,
                    create_sql: _,
                    owner_id: _,
                    privileges: _,
                },
                diff,
            ) = dependents.remove(0);
            let schema = self.find_non_temp_schema(&schema_id);
            let name = QualifiedItemName {
                qualifiers: ItemQualifiers {
                    database_spec: schema.database().clone(),
                    schema_spec: schema.id().clone(),
                },
                item: name,
            };
            let name = self.resolve_full_name(&name, None);
            let action = if diff == 1 { "deserialize" } else { "remove" };

            return Err(Error::new(ErrorKind::Corruption {
                detail: format!(
                    "failed to {} item {} ({}): {}",
                    action,
                    id,
                    name,
                    plan::PlanError::InvalidId(missing_dep)
                ),
            }));
        }

        if let Some((missing_dep, mut dependents)) = awaiting_name_dependencies.into_iter().next() {
            let (
                mz_catalog::durable::Item {
                    id,
                    oid: _,
                    schema_id,
                    name,
                    create_sql: _,
                    owner_id: _,
                    privileges: _,
                },
                diff,
            ) = dependents.remove(0);
            let schema = self.find_non_temp_schema(&schema_id);
            let name = QualifiedItemName {
                qualifiers: ItemQualifiers {
                    database_spec: schema.database().clone(),
                    schema_spec: schema.id().clone(),
                },
                item: name,
            };
            let name = self.resolve_full_name(&name, None);
            let action = if diff == 1 { "deserialize" } else { "remove" };
            return Err(Error::new(ErrorKind::Corruption {
                detail: format!(
                    "failed to {} item {} ({}): {}",
                    action,
                    id,
                    name,
                    Error {
                        kind: ErrorKind::Sql(SqlCatalogError::UnknownItem(missing_dep))
                    }
                ),
            }));
        }

        Ok(())
    }

    /// Applies an update to `self`.
    ///
    /// Returns a `GlobalId` on success, if the applied update added a new `GlobalID` to `self`.
    /// Returns a dependency on failure, if the update could not be applied due to a missing
    /// dependency.
    #[instrument(level = "debug")]
    fn apply_update(
        &mut self,
        kind: StateUpdateKind,
        diff: Diff,
    ) -> Result<Option<GlobalId>, ApplyUpdateError> {
        assert!(
            diff == 1 || diff == -1,
            "invalid update in catalog updates: ({kind:?}, {diff:?})"
        );
        match kind {
            StateUpdateKind::Role(role) => {
                self.apply_role_update(role, diff);
                Ok(None)
            }
            StateUpdateKind::Database(database) => {
                self.apply_database_update(database, diff);
                Ok(None)
            }
            StateUpdateKind::Schema(schema) => {
                self.apply_schema_update(schema, diff);
                Ok(None)
            }
            StateUpdateKind::DefaultPrivilege(default_privilege) => {
                self.apply_default_privilege_update(default_privilege, diff);
                Ok(None)
            }
            StateUpdateKind::SystemPrivilege(system_privilege) => {
                self.apply_system_privilege_update(system_privilege, diff);
                Ok(None)
            }
            StateUpdateKind::SystemConfiguration(system_configuration) => {
                self.apply_system_configuration_update(system_configuration, diff);
                Ok(None)
            }
            StateUpdateKind::Cluster(cluster) => {
                self.apply_cluster_update(cluster, diff);
                Ok(None)
            }
            StateUpdateKind::IntrospectionSourceIndex(introspection_source_index) => {
                self.apply_introspection_source_index_update(introspection_source_index, diff);
                Ok(None)
            }
            StateUpdateKind::ClusterReplica(cluster_replica) => {
                self.apply_cluster_replica_update(cluster_replica, diff);
                Ok(None)
            }
            StateUpdateKind::Item(item) => self.apply_item_update(item, diff),
            StateUpdateKind::Comment(comment) => {
                self.apply_comment_update(comment, diff);
                Ok(None)
            }
        }
    }

    #[instrument(level = "debug")]
    fn apply_role_update(&mut self, role: mz_catalog::durable::Role, diff: Diff) {
        apply(
            &mut self.roles_by_id,
            role.id,
            || Role {
                name: role.name.clone(),
                id: role.id,
                oid: role.oid,
                attributes: role.attributes,
                membership: role.membership,
                vars: role.vars,
            },
            diff,
        );
        apply(&mut self.roles_by_name, role.name, || role.id, diff);
    }

    #[instrument(level = "debug")]
    fn apply_database_update(&mut self, database: mz_catalog::durable::Database, diff: Diff) {
        apply(
            &mut self.database_by_id,
            database.id.clone(),
            || Database {
                name: database.name.clone(),
                id: database.id.clone(),
                oid: database.oid,
                schemas_by_id: BTreeMap::new(),
                schemas_by_name: BTreeMap::new(),
                owner_id: database.owner_id,
                privileges: PrivilegeMap::from_mz_acl_items(database.privileges),
            },
            diff,
        );
        apply(
            &mut self.database_by_name,
            database.name,
            || database.id.clone(),
            diff,
        );
    }

    #[instrument(level = "debug")]
    fn apply_schema_update(&mut self, schema: mz_catalog::durable::Schema, diff: Diff) {
        let (schemas_by_id, schemas_by_name, database_spec) = match &schema.database_id {
            Some(database_id) => {
                let db = self
                    .database_by_id
                    .get_mut(database_id)
                    .expect("catalog out of sync");
                (
                    &mut db.schemas_by_id,
                    &mut db.schemas_by_name,
                    ResolvedDatabaseSpecifier::Id(*database_id),
                )
            }
            None => (
                &mut self.ambient_schemas_by_id,
                &mut self.ambient_schemas_by_name,
                ResolvedDatabaseSpecifier::Ambient,
            ),
        };
        apply(
            schemas_by_id,
            schema.id.clone(),
            || Schema {
                name: QualifiedSchemaName {
                    database: database_spec,
                    schema: schema.name.clone(),
                },
                id: SchemaSpecifier::Id(schema.id.clone()),
                oid: schema.oid,
                items: BTreeMap::new(),
                functions: BTreeMap::new(),
                types: BTreeMap::new(),
                owner_id: schema.owner_id,
                privileges: PrivilegeMap::from_mz_acl_items(schema.privileges),
            },
            diff,
        );
        apply(schemas_by_name, schema.name.clone(), || schema.id, diff);
    }

    #[instrument(level = "debug")]
    fn apply_default_privilege_update(
        &mut self,
        default_privilege: mz_catalog::durable::DefaultPrivilege,
        diff: Diff,
    ) {
        match diff {
            1 => self
                .default_privileges
                .grant(default_privilege.object, default_privilege.acl_item),
            -1 => self
                .default_privileges
                .revoke(&default_privilege.object, &default_privilege.acl_item),
            _ => unreachable!("invalid diff: {diff}"),
        }
    }

    #[instrument(level = "debug")]
    fn apply_system_privilege_update(&mut self, system_privilege: MzAclItem, diff: Diff) {
        match diff {
            1 => self.system_privileges.grant(system_privilege),
            -1 => self.system_privileges.revoke(&system_privilege),
            _ => unreachable!("invalid diff: {diff}"),
        }
    }

    #[instrument(level = "debug")]
    fn apply_system_configuration_update(
        &mut self,
        system_configuration: mz_catalog::durable::SystemConfiguration,
        diff: Diff,
    ) {
        let res = match diff {
            1 => self.insert_system_configuration(
                &system_configuration.name,
                VarInput::Flat(&system_configuration.value),
            ),
            -1 => self.remove_system_configuration(&system_configuration.name),
            _ => unreachable!("invalid diff: {diff}"),
        };
        match res {
            Ok(_) => (),
            // When system variables are deleted, nothing deletes them from the underlying
            // durable catalog, which isn't great. Still, we need to be able to ignore
            // unknown variables.
            Err(Error {
                kind: ErrorKind::VarError(VarError::UnknownParameter(name)),
            }) => {
                warn!(%name, "unknown system parameter from catalog storage");
            }
            Err(e) => panic!("unable to update system variable: {e:?}"),
        }
    }

    #[instrument(level = "debug")]
    fn apply_cluster_update(&mut self, cluster: mz_catalog::durable::Cluster, diff: Diff) {
        apply(
            &mut self.clusters_by_id,
            cluster.id,
            || Cluster {
                name: cluster.name.clone(),
                id: cluster.id,
                bound_objects: BTreeSet::new(),
                log_indexes: BTreeMap::new(),
                replica_id_by_name_: BTreeMap::new(),
                replicas_by_id_: BTreeMap::new(),
                owner_id: cluster.owner_id,
                privileges: PrivilegeMap::from_mz_acl_items(cluster.privileges),
                config: cluster.config.into(),
            },
            diff,
        );
        apply(
            &mut self.clusters_by_name,
            cluster.name,
            || cluster.id,
            diff,
        );
    }

    #[instrument(level = "debug")]
    fn apply_introspection_source_index_update(
        &mut self,
        introspection_source_index: mz_catalog::durable::IntrospectionSourceIndex,
        diff: Diff,
    ) {
        // TODO(jkosh44) There may be some old deleted logs stored durably that no longer
        // exists. For now we ignore them, but we should clean them up.
        if let Some(log) = BUILTIN_LOG_LOOKUP.get(introspection_source_index.name.as_str()) {
            match diff {
                1 => {
                    self.insert_introspection_source_index(
                        introspection_source_index.cluster_id,
                        log,
                        introspection_source_index.index_id,
                        introspection_source_index.oid,
                    );
                }
                -1 => {
                    self.drop_item(introspection_source_index.index_id);
                }
                _ => unreachable!("invalid diff: {diff}"),
            }
            let cluster = self
                .clusters_by_id
                .get_mut(&introspection_source_index.cluster_id)
                .expect("catalog out of sync");
            apply(
                &mut cluster.log_indexes,
                log.variant,
                || introspection_source_index.index_id,
                diff,
            );
        }
    }

    #[instrument(level = "debug")]
    fn apply_cluster_replica_update(
        &mut self,
        cluster_replica: mz_catalog::durable::ClusterReplica,
        diff: Diff,
    ) {
        let cluster = self
            .clusters_by_id
            .get(&cluster_replica.cluster_id)
            .expect("catalog out of sync");
        let azs = cluster.availability_zones();
        let location = self
            .concretize_replica_location(cluster_replica.config.location, &vec![], azs)
            .expect("catalog in unexpected state");
        let cluster = self
            .clusters_by_id
            .get_mut(&cluster_replica.cluster_id)
            .expect("catalog out of sync");
        apply(
            &mut cluster.replicas_by_id_,
            cluster_replica.replica_id,
            || {
                let logging = ReplicaLogging {
                    log_logging: cluster_replica.config.logging.log_logging,
                    interval: cluster_replica.config.logging.interval,
                };
                let config = ReplicaConfig {
                    location,
                    compute: ComputeReplicaConfig { logging },
                };
                ClusterReplica {
                    name: cluster_replica.name.clone(),
                    cluster_id: cluster_replica.cluster_id,
                    replica_id: cluster_replica.replica_id,
                    process_status: (0..config.location.num_processes())
                        .map(|process_id| {
                            let status = ClusterReplicaProcessStatus {
                                status: ClusterStatus::NotReady(None),
                                time: to_datetime((self.config.now)()),
                            };
                            (u64::cast_from(process_id), status)
                        })
                        .collect(),
                    config,
                    owner_id: cluster_replica.owner_id,
                }
            },
            diff,
        );
        apply(
            &mut cluster.replica_id_by_name_,
            cluster_replica.name,
            || cluster_replica.replica_id,
            diff,
        );
    }

    /// Applies an item update to `self`.
    ///
    /// Returns a `GlobalId` on success, if the applied update added a new `GlobalID` to `self`.
    /// Returns a dependency on failure, if the update could not be applied due to a missing
    /// dependency.
    #[instrument(level = "debug")]
    fn apply_item_update(
        &mut self,
        item: mz_catalog::durable::Item,
        diff: Diff,
    ) -> Result<Option<GlobalId>, ApplyUpdateError> {
        // If we knew beforehand that the items were being applied in dependency
        // order, then we could fully delegate to `self.insert_item(...)` and`self.drop_item(...)`.
        // However, we don't know that items are applied in dependency order, so we must handle the
        // case that the item is valid, but we haven't applied all of its dependencies yet.
        match diff {
            1 => {
                // TODO(benesch): a better way of detecting when a view has depended
                // upon a non-existent logging view. This is fine for now because
                // the only goal is to produce a nicer error message; we'll bail out
                // safely even if the error message we're sniffing out changes.
                static LOGGING_ERROR: Lazy<Regex> =
                    Lazy::new(|| Regex::new("mz_catalog.[^']*").expect("valid regex"));

                let catalog_item = match self.deserialize_item(item.id, &item.create_sql) {
                    Ok(item) => item,
                    Err(AdapterError::Catalog(Error {
                        kind: ErrorKind::Sql(SqlCatalogError::UnknownItem(name)),
                    })) if LOGGING_ERROR.is_match(&name.to_string()) => {
                        return Err(ApplyUpdateError::Error(Error::new(
                            ErrorKind::UnsatisfiableLoggingDependency {
                                depender_name: name,
                            },
                        )));
                    }
                    // If we were missing a dependency, wait for it to be added.
                    Err(AdapterError::PlanError(plan::PlanError::InvalidId(missing_dep))) => {
                        return Err(ApplyUpdateError::AwaitingIdDependency((
                            missing_dep,
                            item,
                            diff,
                        )));
                    }
                    // If we were missing a dependency, wait for it to be added.
                    Err(AdapterError::PlanError(plan::PlanError::Catalog(
                        SqlCatalogError::UnknownItem(missing_dep),
                    ))) => {
                        return match GlobalId::from_str(&missing_dep) {
                            Ok(id) => Err(ApplyUpdateError::AwaitingIdDependency((id, item, diff))),
                            Err(_) => Err(ApplyUpdateError::AwaitingNameDependency((
                                missing_dep,
                                item,
                                diff,
                            ))),
                        }
                    }
                    Err(e) => {
                        let schema = self.find_non_temp_schema(&item.schema_id);
                        let name = QualifiedItemName {
                            qualifiers: ItemQualifiers {
                                database_spec: schema.database().clone(),
                                schema_spec: schema.id().clone(),
                            },
                            item: item.name,
                        };
                        let name = self.resolve_full_name(&name, None);
                        return Err(ApplyUpdateError::Error(Error::new(ErrorKind::Corruption {
                            detail: format!(
                                "failed to deserialize item {} ({}): {}\n\n{}",
                                item.id, name, e, item.create_sql
                            ),
                        })));
                    }
                };
                let schema = self.find_non_temp_schema(&item.schema_id);
                let name = QualifiedItemName {
                    qualifiers: ItemQualifiers {
                        database_spec: schema.database().clone(),
                        schema_spec: schema.id().clone(),
                    },
                    item: item.name,
                };
                self.insert_item(
                    item.id,
                    item.oid,
                    name,
                    catalog_item,
                    item.owner_id,
                    PrivilegeMap::from_mz_acl_items(item.privileges),
                );
                Ok(Some(item.id))
            }
            -1 => {
                let entry = self.get_entry(&item.id);
                if let Some(id) = entry.referenced_by().first() {
                    return Err(ApplyUpdateError::AwaitingIdDependency((*id, item, diff)));
                }
                if let Some(id) = entry.used_by().first() {
                    return Err(ApplyUpdateError::AwaitingIdDependency((*id, item, diff)));
                }
                self.drop_item(item.id);
                Ok(None)
            }
            _ => unreachable!("invalid diff: {diff}"),
        }
    }

    #[instrument(level = "debug")]
    fn apply_comment_update(&mut self, comment: mz_catalog::durable::Comment, diff: Diff) {
        match diff {
            1 => {
                let prev = self.comments.update_comment(
                    comment.object_id,
                    comment.sub_component,
                    Some(comment.comment),
                );
                assert_eq!(
                    prev, None,
                    "values must be explicitly retracted before inserting a new value"
                );
            }
            -1 => {
                let prev =
                    self.comments
                        .update_comment(comment.object_id, comment.sub_component, None);
                assert_eq!(
                    prev,
                    Some(comment.comment),
                    "retraction does not match existing value: ({:?}, {:?})",
                    comment.object_id,
                    comment.sub_component,
                );
            }
            _ => unreachable!("invalid diff: {diff}"),
        }
    }
}

fn apply<K, V>(map: &mut BTreeMap<K, V>, key: K, value: impl FnOnce() -> V, diff: Diff)
where
    K: Ord + Debug,
    V: PartialEq + Debug,
{
    if diff == 1 {
        let prev = map.insert(key, value());
        assert_eq!(
            prev, None,
            "values must be explicitly retracted before inserting a new value"
        );
    } else if diff == -1 {
        let prev = map.remove(&key);
        // We can't assert the exact contents of the previous value, since we don't know
        // what it should look like.
        assert!(
            prev.is_some(),
            "retraction does not match existing value: {key:?}"
        );
    }
}

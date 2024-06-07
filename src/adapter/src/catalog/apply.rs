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

use itertools::Itertools;
use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Debug;
use std::iter;

use mz_catalog::builtin::{Builtin, BuiltinTable, BUILTIN_LOG_LOOKUP, BUILTIN_LOOKUP};
use mz_catalog::durable::objects::SystemObjectDescription;
use mz_catalog::memory::error::{Error, ErrorKind};
use mz_catalog::memory::objects::{
    CatalogItem, ClusterReplica, DataSourceDesc, Func, Log, Source, StateDiff, StateDiffType,
    StateUpdate, StateUpdateKind, Table, TodoTraitName, Type, YetAnotherStateUpdateKind,
};
use mz_catalog::SYSTEM_CONN_ID;
use mz_compute_client::controller::ComputeReplicaConfig;
use mz_controller::clusters::{ReplicaConfig, ReplicaLogging};
use mz_ore::instrument;
use mz_pgrepr::oid::INVALID_OID;
use mz_repr::adt::mz_acl_item::{MzAclItem, PrivilegeMap};
use mz_repr::role_id::RoleId;
use mz_repr::{Diff, GlobalId};
use mz_sql::catalog::{CatalogItemType, CatalogSchema, CatalogType, NameReference};
use mz_sql::names::{
    ItemQualifiers, QualifiedItemName, ResolvedDatabaseSpecifier, ResolvedIds, SchemaSpecifier,
};
use mz_sql::rbac;
use mz_sql::session::user::MZ_SYSTEM_ROLE_ID;
use mz_sql::session::vars::{VarError, VarInput};
use mz_sql_parser::ast::Expr;
use mz_storage_types::sources::Timeline;
use tracing::warn;

use crate::catalog::{BuiltinTableUpdate, Catalog, CatalogState};

/// Sort [`StateUpdate`]s in dependency order.
///
/// Panics if `updates` is not consolidated.
fn sort_updates(updates: Vec<StateUpdate>) -> Vec<StateUpdate> {
    fn push_update(
        update: StateUpdate,
        retractions: &mut Vec<StateUpdate>,
        additions: &mut Vec<StateUpdate>,
    ) {
        match update.kind.diff_type() {
            StateDiffType::Delete => retractions.push(update),
            StateDiffType::Insert | StateDiffType::Update => additions.push(update),
        }
    }

    // Partition updates by type so that we can weave different update types into the right spots.
    let mut pre_cluster_retractions = Vec::new();
    let mut pre_cluster_additions = Vec::new();
    let mut cluster_retractions = Vec::new();
    let mut cluster_additions = Vec::new();
    let mut builtin_item_updates = Vec::new();
    let mut item_retractions = Vec::new();
    let mut item_additions = Vec::new();
    let mut post_item_retractions = Vec::new();
    let mut post_item_additions = Vec::new();
    for update in updates {
        match update.kind {
            StateUpdateKind::Role(_)
            | StateUpdateKind::Database(_)
            | StateUpdateKind::Schema(_)
            | StateUpdateKind::DefaultPrivilege(_)
            | StateUpdateKind::SystemPrivilege(_)
            | StateUpdateKind::SystemConfiguration(_) => push_update(
                update,
                &mut pre_cluster_retractions,
                &mut pre_cluster_additions,
            ),
            StateUpdateKind::Cluster(_)
            | StateUpdateKind::IntrospectionSourceIndex(_)
            | StateUpdateKind::ClusterReplica(_) => {
                push_update(update, &mut cluster_retractions, &mut cluster_additions)
            }
            StateUpdateKind::SystemObjectMapping(system_object_mapping) => {
                builtin_item_updates.push(system_object_mapping)
            }
            StateUpdateKind::Item(item) => match item.diff_type() {
                StateDiffType::Delete => item_retractions.push(item),
                StateDiffType::Insert | StateDiffType::Update => item_additions.push(item),
            },
            StateUpdateKind::Comment(_)
            | StateUpdateKind::AuditLog(_)
            | StateUpdateKind::StorageUsage(_)
            | StateUpdateKind::StorageCollectionMetadata(_)
            | StateUpdateKind::UnfinalizedShard(_) => {
                push_update(update, &mut post_item_retractions, &mut post_item_additions)
            }
        }
    }

    // Sort builtin item updates by dependency.
    let builtin_item_updates = builtin_item_updates
        .into_iter()
        .map(|system_object_mapping| {
            let key = system_object_mapping.key_ref();
            let description = SystemObjectDescription {
                schema_name: key.schema_name,
                object_type: key.object_type,
                object_name: key.object_name,
            };
            let idx = BUILTIN_LOOKUP.get(&description).expect("missing builtin").0;
            (idx, system_object_mapping)
        })
        .sorted_by_key(|(idx, _)| *idx)
        .map(|(_, system_object_mapping)| system_object_mapping);

    // Further partition builtin item updates.
    let mut other_builtin_retractions = Vec::new();
    let mut other_builtin_additions = Vec::new();
    let mut builtin_index_retractions = Vec::new();
    let mut builtin_index_additions = Vec::new();
    for builtin_item_update in builtin_item_updates {
        match &builtin_item_update.key_ref().object_type {
            CatalogItemType::Index => push_update(
                StateUpdate {
                    kind: StateUpdateKind::SystemObjectMapping(builtin_item_update),
                },
                &mut builtin_index_retractions,
                &mut builtin_index_additions,
            ),
            CatalogItemType::Table
            | CatalogItemType::Source
            | CatalogItemType::Sink
            | CatalogItemType::View
            | CatalogItemType::MaterializedView
            | CatalogItemType::Type
            | CatalogItemType::Func
            | CatalogItemType::Secret
            | CatalogItemType::Connection => push_update(
                StateUpdate {
                    kind: StateUpdateKind::SystemObjectMapping(builtin_item_update),
                },
                &mut other_builtin_retractions,
                &mut other_builtin_additions,
            ),
        }
    }

    // Sort item updates by GlobalId.
    fn sort_item_updates(
        item_updates: Vec<StateDiff<mz_catalog::durable::Item>>,
    ) -> Vec<StateUpdate> {
        item_updates
            .into_iter()
            .sorted_by_key(|item| item.key_ref().gid)
            .map(|item| StateUpdate {
                kind: StateUpdateKind::Item(item),
            })
            .collect()
    }
    let item_retractions = sort_item_updates(item_retractions);
    let item_additions = sort_item_updates(item_additions);

    // Put everything back together.
    iter::empty()
        // All retractions must be reversed.
        .chain(post_item_retractions.into_iter().rev())
        .chain(item_retractions.into_iter().rev())
        .chain(builtin_index_retractions.into_iter().rev())
        .chain(cluster_retractions.into_iter().rev())
        .chain(other_builtin_retractions.into_iter().rev())
        .chain(pre_cluster_retractions.into_iter().rev())
        .chain(pre_cluster_additions.into_iter())
        .chain(other_builtin_additions.into_iter())
        .chain(cluster_additions.into_iter())
        .chain(builtin_index_additions.into_iter())
        .chain(item_additions.into_iter())
        .chain(post_item_additions.into_iter())
        .collect()
}

impl CatalogState {
    /// Update in-memory catalog state from a list of updates made to the durable catalog state.
    ///
    /// Returns builtin table updates corresponding to the changes to catalog state.
    ///
    /// This is meant specifically for bootstrapping because it batches and applies builtin view
    /// additions separately from other update types.
    #[must_use]
    #[instrument]
    pub(crate) async fn apply_updates_for_bootstrap(
        &mut self,
        updates: Vec<StateUpdate>,
    ) -> Vec<BuiltinTableUpdate<&'static BuiltinTable>> {
        // Most updates are applied one at a time, but builtin view additions are applied separately
        // in a batch for performance reasons. A constraint is that updates must be applied in
        // order. This method is modeled as a simple state machine that batches then applies groups
        // of builtin view additions and all other updates.

        enum ApplyState {
            Updates(Vec<StateUpdate>),
            BuiltinViewAdditions(Vec<(&'static Builtin<NameReference>, GlobalId)>),
        }

        let mut state = ApplyState::Updates(Vec::new());
        let updates = sort_updates(updates);
        let mut builtin_table_updates = Vec::with_capacity(updates.len());

        for update in updates {
            match (&mut state, update) {
                (
                    ApplyState::Updates(updates),
                    StateUpdate {
                        kind:
                            StateUpdateKind::SystemObjectMapping(StateDiff::Insert(
                                system_object_mapping,
                            )),
                    },
                ) if matches!(
                    system_object_mapping.description.object_type,
                    CatalogItemType::View
                ) =>
                {
                    // Apply updates and start batching builtin view additions.
                    let builtin_table_update = self.apply_updates(std::mem::take(updates));
                    builtin_table_updates.extend(builtin_table_update);
                    let view_addition = lookup_builtin_view_addition(system_object_mapping);
                    state = ApplyState::BuiltinViewAdditions(vec![view_addition]);
                }
                (ApplyState::Updates(updates), update) => {
                    // Continue batching updates.
                    updates.push(update);
                }
                (
                    ApplyState::BuiltinViewAdditions(builtin_view_additions),
                    StateUpdate {
                        kind:
                            StateUpdateKind::SystemObjectMapping(StateDiff::Insert(
                                system_object_mapping,
                            )),
                    },
                ) if matches!(
                    system_object_mapping.description.object_type,
                    CatalogItemType::View
                ) =>
                {
                    // Continue batching builtin view additions.
                    let view_addition = lookup_builtin_view_addition(system_object_mapping);
                    builtin_view_additions.push(view_addition);
                }
                (ApplyState::BuiltinViewAdditions(builtin_view_additions), update) => {
                    // Apply all builtin view additions in a batch and start batching updates.
                    let builtin_table_update =
                        Catalog::parse_views(self, std::mem::take(builtin_view_additions)).await;
                    builtin_table_updates.extend(builtin_table_update);
                    state = ApplyState::Updates(vec![update]);
                }
            }
        }

        // Apply remaining state.
        match state {
            ApplyState::Updates(updates) => {
                let builtin_table_update = self.apply_updates(updates);
                builtin_table_updates.extend(builtin_table_update);
            }
            ApplyState::BuiltinViewAdditions(builtin_view_additions) => {
                let builtin_table_update = Catalog::parse_views(self, builtin_view_additions).await;
                builtin_table_updates.extend(builtin_table_update);
            }
        }

        builtin_table_updates
    }

    /// Update in-memory catalog state from a list of updates made to the durable catalog state.
    ///
    /// Returns builtin table updates corresponding to the changes to catalog state.
    #[must_use]
    #[instrument]
    pub(crate) fn apply_updates(
        &mut self,
        updates: Vec<StateUpdate>,
    ) -> Vec<BuiltinTableUpdate<&'static BuiltinTable>> {
        let mut builtin_table_updates = Vec::with_capacity(updates.len());

        for update in &updates {
            if let Some(retraction) = update.kind.retraction() {
                builtin_table_updates.extend(self.generate_builtin_table_update(retraction, -1));
            }
        }

        let additions: Vec<_> = updates
            .iter()
            .filter_map(|update| update.kind.addition())
            .collect();

        for update in updates {
            self.apply_update(update);
        }

        for addition in additions {
            builtin_table_updates.extend(self.generate_builtin_table_update(addition, 1));
        }

        builtin_table_updates
    }

    #[instrument(level = "debug")]
    fn apply_update(&mut self, update: StateUpdate) {
        match update.kind {
            StateUpdateKind::Role(role) => {
                self.apply_role_update(role);
            }
            StateUpdateKind::Database(database) => {
                self.apply_database_update(database);
            }
            StateUpdateKind::Schema(schema) => {
                self.apply_schema_update(schema);
            }
            StateUpdateKind::DefaultPrivilege(default_privilege) => {
                self.apply_default_privilege_update(default_privilege);
            }
            StateUpdateKind::SystemPrivilege(system_privilege) => {
                self.apply_system_privilege_update(system_privilege);
            }
            StateUpdateKind::SystemConfiguration(system_configuration) => {
                self.apply_system_configuration_update(system_configuration);
            }
            StateUpdateKind::Cluster(cluster) => {
                self.apply_cluster_update(cluster);
            }
            StateUpdateKind::IntrospectionSourceIndex(introspection_source_index) => {
                self.apply_introspection_source_index_update(introspection_source_index);
            }
            StateUpdateKind::ClusterReplica(cluster_replica) => {
                self.apply_cluster_replica_update(cluster_replica);
            }
            StateUpdateKind::SystemObjectMapping(system_object_mapping) => {
                self.apply_system_object_mapping_update(system_object_mapping);
            }
            StateUpdateKind::Item(item) => {
                self.apply_item_update(item);
            }
            StateUpdateKind::Comment(comment) => {
                self.apply_comment_update(comment);
            }
            StateUpdateKind::AuditLog(_audit_log) => {
                // Audit logs are not stored in-memory.
            }
            StateUpdateKind::StorageUsage(_storage_usage) => {
                // Storage usage events are not stored in-memory.
            }
            StateUpdateKind::StorageCollectionMetadata(storage_collection_metadata) => {
                self.apply_storage_collection_metadata_update(storage_collection_metadata);
            }
            StateUpdateKind::UnfinalizedShard(unfinalized_shard) => {
                self.apply_unfinalized_shard_update(unfinalized_shard);
            }
        }
    }

    #[instrument(level = "debug")]
    fn apply_role_update(&mut self, role: StateDiff<mz_catalog::durable::Role>) {
        apply_inverted(
            &mut self.roles_by_name,
            |role| role.name.clone(),
            |role| role.id,
            &role,
        );
        apply_catalog_type(&mut self.roles_by_id, |role| role.id, role);
    }

    #[instrument(level = "debug")]
    fn apply_database_update(&mut self, database: StateDiff<mz_catalog::durable::Database>) {
        apply_inverted(
            &mut self.database_by_name,
            |database| database.name.clone(),
            |database| database.id.clone(),
            &database,
        );
        apply_catalog_type(&mut self.database_by_id, |database| database.id, database);
    }

    #[instrument(level = "debug")]
    fn apply_schema_update(&mut self, schema: StateDiff<mz_catalog::durable::Schema>) {
        // TODO(jkosh44) The complexity of finding the map is a good hint that database_id should
        // go in the key.
        match schema {
            StateDiff::Delete(retraction) => {
                let (schemas_by_id, schemas_by_name) = match &retraction.database_id {
                    Some(database_id) => {
                        let db = self
                            .database_by_id
                            .get_mut(database_id)
                            .expect("catalog out of sync");
                        (&mut db.schemas_by_id, &mut db.schemas_by_name)
                    }
                    None => (
                        &mut self.ambient_schemas_by_id,
                        &mut self.ambient_schemas_by_name,
                    ),
                };
                let prev = schemas_by_name.remove(&retraction.name);
                assert_eq!(
                    prev,
                    Some(retraction.id),
                    "retraction does not match existing value: {:?}",
                    retraction.name
                );
                let prev = schemas_by_id.remove(&retraction.id);
                assert!(
                    prev.is_some(),
                    "retraction does not match existing value: {:?}",
                    retraction.id
                );
            }
            StateDiff::Insert(addition) => {
                let (schemas_by_id, schemas_by_name) = match &addition.database_id {
                    Some(database_id) => {
                        let db = self
                            .database_by_id
                            .get_mut(database_id)
                            .expect("catalog out of sync");
                        (&mut db.schemas_by_id, &mut db.schemas_by_name)
                    }
                    None => (
                        &mut self.ambient_schemas_by_id,
                        &mut self.ambient_schemas_by_name,
                    ),
                };
                let prev = schemas_by_name.insert(addition.name.clone(), addition.id);
                assert_eq!(
                    prev, None,
                    "values must be explicitly retracted before inserting a new value: {:?}",
                    addition.name
                );
                let prev = schemas_by_id.insert(addition.id, addition.into());
                assert_eq!(
                    prev, None,
                    "values must be explicitly retracted before inserting a new value"
                );
            }
            StateDiff::Update {
                retraction,
                addition,
            } => {
                assert_eq!(retraction.database_id, addition.database_id);
                let (schemas_by_id, schemas_by_name) = match &retraction.database_id {
                    Some(database_id) => {
                        let db = self
                            .database_by_id
                            .get_mut(database_id)
                            .expect("catalog out of sync");
                        (&mut db.schemas_by_id, &mut db.schemas_by_name)
                    }
                    None => (
                        &mut self.ambient_schemas_by_id,
                        &mut self.ambient_schemas_by_name,
                    ),
                };
                let prev = schemas_by_name.remove(&retraction.name);
                assert_eq!(
                    prev,
                    Some(retraction.id),
                    "retraction does not match existing value: {:?}",
                    retraction.name
                );
                let prev = schemas_by_name.insert(addition.name.clone(), addition.id);
                assert_eq!(
                    prev, None,
                    "values must be explicitly retracted before inserting a new value"
                );
                assert_eq!(retraction.id, addition.id);
                let value = schemas_by_id
                    .get_mut(&retraction.id)
                    .unwrap_or_else(|| panic!("missing value for {:?}", retraction.id));
                value.update_from(addition);
            }
        }
    }

    #[instrument(level = "debug")]
    fn apply_default_privilege_update(
        &mut self,
        default_privilege: StateDiff<mz_catalog::durable::DefaultPrivilege>,
    ) {
        match default_privilege {
            StateDiff::Delete(retraction) => self
                .default_privileges
                .revoke(&retraction.object, &retraction.acl_item),
            StateDiff::Insert(addition) => self
                .default_privileges
                .grant(addition.object, addition.acl_item),
            StateDiff::Update {
                retraction,
                addition,
            } => {
                self.default_privileges
                    .revoke(&retraction.object, &retraction.acl_item);
                self.default_privileges
                    .grant(addition.object, addition.acl_item);
            }
        }
    }

    #[instrument(level = "debug")]
    fn apply_system_privilege_update(&mut self, system_privilege: StateDiff<MzAclItem>) {
        match system_privilege {
            StateDiff::Delete(retraction) => self.system_privileges.revoke(&retraction),
            StateDiff::Insert(addition) => self.system_privileges.grant(addition),
            StateDiff::Update {
                retraction,
                addition,
            } => {
                self.system_privileges.revoke(&retraction);
                self.system_privileges.grant(addition);
            }
        }
    }

    #[instrument(level = "debug")]
    fn apply_system_configuration_update(
        &mut self,
        system_configuration: StateDiff<mz_catalog::durable::SystemConfiguration>,
    ) {
        let res = match system_configuration {
            StateDiff::Delete(retraction) => self.remove_system_configuration(&retraction.name),
            StateDiff::Insert(addition) | StateDiff::Update { addition, .. } => {
                self.insert_system_configuration(&addition.name, VarInput::Flat(&addition.value))
            }
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
    fn apply_cluster_update(&mut self, cluster: StateDiff<mz_catalog::durable::Cluster>) {
        apply_inverted(
            &mut self.clusters_by_name,
            |cluster| cluster.name.clone(),
            |cluster| cluster.id,
            &cluster,
        );
        apply_catalog_type(&mut self.clusters_by_id, |cluster| cluster.id, cluster);
    }

    #[instrument(level = "debug")]
    fn apply_introspection_source_index_update(
        &mut self,
        introspection_source_index: StateDiff<mz_catalog::durable::IntrospectionSourceIndex>,
    ) {
        let key = introspection_source_index.key_ref();
        let log = BUILTIN_LOG_LOOKUP
            .get(key.name.as_str())
            .expect("missing log");
        let cluster = self
            .clusters_by_id
            .get_mut(&key.cluster_id)
            .expect("catalog out of sync");
        apply_inverted(
            &mut cluster.log_indexes,
            |_| log.variant,
            |introspection_source_index| introspection_source_index.index_id,
            &introspection_source_index,
        );
        match introspection_source_index {
            StateDiff::Delete(retraction) => {
                self.drop_item(retraction.index_id);
            }
            StateDiff::Insert(addition) => {
                self.insert_introspection_source_index(
                    addition.cluster_id,
                    log,
                    addition.index_id,
                    addition.oid,
                );
            }
            StateDiff::Update {
                retraction,
                addition,
            } => {
                // TODO(jkosh44) Is this right? Do we need to do some in place update stuff?
                self.drop_item(retraction.index_id);
                self.insert_introspection_source_index(
                    addition.cluster_id,
                    log,
                    addition.index_id,
                    addition.oid,
                );
            }
        }
    }

    #[instrument(level = "debug")]
    fn apply_cluster_replica_update(
        &mut self,
        cluster_replica: StateDiff<mz_catalog::durable::ClusterReplica>,
    ) {
        // TODO(jkosh44) The complexity of finding the map is a good hint that cluster_id should go
        // in the key.
        match cluster_replica {
            StateDiff::Delete(retraction) => {
                let cluster = self
                    .clusters_by_id
                    .get_mut(&retraction.cluster_id)
                    .expect("catalog out of sync");
                let replicas_by_id = &mut cluster.replicas_by_id_;
                let replicas_by_name = &mut cluster.replica_id_by_name_;
                let prev = replicas_by_name.remove(&retraction.name);
                assert_eq!(
                    prev,
                    Some(retraction.replica_id),
                    "retraction does not match existing value: {:?}",
                    retraction.name
                );
                let prev = replicas_by_id.remove(&retraction.replica_id);
                assert!(
                    prev.is_some(),
                    "retraction does not match existing value: {:?}",
                    retraction.replica_id
                );
            }
            StateDiff::Insert(addition) => {
                let cluster = self
                    .clusters_by_id
                    .get(&addition.cluster_id)
                    .expect("catalog out of sync");
                let azs = cluster.availability_zones().clone();
                let location = self
                    .concretize_replica_location(addition.config.location, &vec![], azs)
                    .expect("catalog in unexpected state");
                let logging = ReplicaLogging {
                    log_logging: addition.config.logging.log_logging,
                    interval: addition.config.logging.interval,
                };
                let config = ReplicaConfig {
                    location,
                    compute: ComputeReplicaConfig { logging },
                };
                let replica = ClusterReplica {
                    name: addition.name.clone(),
                    cluster_id: addition.cluster_id,
                    replica_id: addition.replica_id,
                    config,
                    owner_id: addition.owner_id,
                };
                let cluster = self
                    .clusters_by_id
                    .get_mut(&addition.cluster_id)
                    .expect("catalog out of sync");
                let replicas_by_id = &mut cluster.replicas_by_id_;
                let replicas_by_name = &mut cluster.replica_id_by_name_;
                let prev = replicas_by_name.insert(addition.name.clone(), addition.replica_id);
                assert_eq!(
                    prev, None,
                    "values must be explicitly retracted before inserting a new value: {:?}",
                    addition.name
                );
                let prev = replicas_by_id.insert(addition.replica_id, replica);
                assert_eq!(
                    prev, None,
                    "values must be explicitly retracted before inserting a new value"
                );
            }
            // TODO(jkosh44) Double check that there's no update in place stuff to do.
            StateDiff::Update {
                addition,
                retraction,
            } => {
                assert_eq!(retraction.cluster_id, addition.cluster_id);
                let cluster = self
                    .clusters_by_id
                    .get(&addition.cluster_id)
                    .expect("catalog out of sync");
                let azs = cluster.availability_zones().clone();
                let location = self
                    .concretize_replica_location(addition.config.location, &vec![], azs)
                    .expect("catalog in unexpected state");
                let logging = ReplicaLogging {
                    log_logging: addition.config.logging.log_logging,
                    interval: addition.config.logging.interval,
                };
                let config = ReplicaConfig {
                    location,
                    compute: ComputeReplicaConfig { logging },
                };
                let replica = ClusterReplica {
                    name: addition.name.clone(),
                    cluster_id: addition.cluster_id,
                    replica_id: addition.replica_id,
                    config,
                    owner_id: addition.owner_id,
                };
                let cluster = self
                    .clusters_by_id
                    .get_mut(&addition.cluster_id)
                    .expect("catalog out of sync");
                let replicas_by_id = &mut cluster.replicas_by_id_;
                let replicas_by_name = &mut cluster.replica_id_by_name_;
                let prev = replicas_by_name.remove(&retraction.name);
                assert_eq!(
                    prev,
                    Some(retraction.replica_id),
                    "retraction does not match existing value: {:?}",
                    retraction.name
                );
                let prev = replicas_by_name.insert(addition.name.clone(), addition.replica_id);
                assert_eq!(
                    prev, None,
                    "values must be explicitly retracted before inserting a new value"
                );
                assert_eq!(retraction.replica_id, addition.replica_id);
                let prev = replicas_by_id.insert(addition.replica_id, replica);
                assert!(
                    prev.is_some(),
                    "cluster replica update does not match existing value: {:?}",
                    addition.replica_id
                );
            }
        }
    }

    #[instrument(level = "debug")]
    fn apply_system_object_mapping_update(
        &mut self,
        system_object_mapping: StateDiff<mz_catalog::durable::SystemObjectMapping>,
    ) {
        match system_object_mapping {
            StateDiff::Delete(retraction) => {
                self.drop_item(retraction.unique_identifier.id);
            }
            StateDiff::Insert(addition) => {
                self.apply_system_object_mapping_insert(addition);
            }
            StateDiff::Update {
                retraction,
                addition,
            } => {
                // TODO(jkosh44) Is there in place updates we need to do?
                self.drop_item(retraction.unique_identifier.id);
                if addition.description.object_type == CatalogItemType::View {
                    // TODO(jkosh44) Gross, all copied from parse_views.
                    let (builtin, id) = lookup_builtin_view_addition(addition);
                    let Builtin::View(view) = builtin else {
                        unreachable!("known to be view");
                    };
                    let create_sql = view.create_sql();
                    let item = self
                        .parse_item(&create_sql, None, false, None)
                        .expect("invalid persisted SQL");
                    let schema_id = self.ambient_schemas_by_name[view.schema];
                    let name = QualifiedItemName {
                        qualifiers: ItemQualifiers {
                            database_spec: ResolvedDatabaseSpecifier::Ambient,
                            schema_spec: SchemaSpecifier::Id(schema_id),
                        },
                        item: view.name.into(),
                    };
                    let mut acl_items = vec![rbac::owner_privilege(
                        mz_sql::catalog::ObjectType::View,
                        MZ_SYSTEM_ROLE_ID,
                    )];
                    acl_items.extend_from_slice(&view.access);
                    self.insert_item(
                        id,
                        view.oid,
                        name,
                        item,
                        MZ_SYSTEM_ROLE_ID,
                        PrivilegeMap::from_mz_acl_items(acl_items),
                    );
                } else {
                    self.apply_system_object_mapping_insert(addition);
                }
            }
        }
    }

    #[instrument(level = "debug")]
    fn apply_system_object_mapping_insert(
        &mut self,
        system_object_mapping: mz_catalog::durable::SystemObjectMapping,
    ) {
        let id = system_object_mapping.unique_identifier.id;
        let builtin = BUILTIN_LOOKUP
            .get(&system_object_mapping.description)
            .expect("missing builtin")
            .1;
        let schema_id = self.ambient_schemas_by_name[builtin.schema()];
        let name = QualifiedItemName {
            qualifiers: ItemQualifiers {
                database_spec: ResolvedDatabaseSpecifier::Ambient,
                schema_spec: SchemaSpecifier::Id(schema_id),
            },
            item: builtin.name().into(),
        };
        match builtin {
            Builtin::Log(log) => {
                let mut acl_items = vec![rbac::owner_privilege(
                    mz_sql::catalog::ObjectType::Source,
                    MZ_SYSTEM_ROLE_ID,
                )];
                acl_items.extend_from_slice(&log.access);
                self.insert_item(
                    id,
                    log.oid,
                    name.clone(),
                    CatalogItem::Log(Log {
                        variant: log.variant,
                    }),
                    MZ_SYSTEM_ROLE_ID,
                    PrivilegeMap::from_mz_acl_items(acl_items),
                );
            }

            Builtin::Table(table) => {
                let mut acl_items = vec![rbac::owner_privilege(
                    mz_sql::catalog::ObjectType::Table,
                    MZ_SYSTEM_ROLE_ID,
                )];
                acl_items.extend_from_slice(&table.access);

                self.insert_item(
                    id,
                    table.oid,
                    name.clone(),
                    CatalogItem::Table(Table {
                        create_sql: None,
                        desc: table.desc.clone(),
                        defaults: vec![Expr::null(); table.desc.arity()],
                        conn_id: None,
                        resolved_ids: ResolvedIds(BTreeSet::new()),
                        custom_logical_compaction_window: table.is_retained_metrics_object.then(
                            || {
                                self.system_config()
                                    .metrics_retention()
                                    .try_into()
                                    .expect("invalid metrics retention")
                            },
                        ),
                        is_retained_metrics_object: table.is_retained_metrics_object,
                    }),
                    MZ_SYSTEM_ROLE_ID,
                    PrivilegeMap::from_mz_acl_items(acl_items),
                );
            }
            Builtin::Index(index) => {
                let mut item = self
                    .parse_item(
                        &index.create_sql(),
                        None,
                        index.is_retained_metrics_object,
                        if index.is_retained_metrics_object { Some(self.system_config().metrics_retention().try_into().expect("invalid metrics retention")) } else { None },
                    )
                    .unwrap_or_else(|e| {
                        panic!(
                            "internal error: failed to load bootstrap index:\n\
                                    {}\n\
                                    error:\n\
                                    {:?}\n\n\
                                    make sure that the schema name is specified in the builtin index's create sql statement.",
                            index.name, e
                        )
                    });
                let CatalogItem::Index(_) = &mut item else {
                    panic!("internal error: builtin index {}'s SQL does not begin with \"CREATE INDEX\".", index.name);
                };

                self.insert_item(
                    id,
                    index.oid,
                    name,
                    item,
                    MZ_SYSTEM_ROLE_ID,
                    PrivilegeMap::default(),
                );
            }
            Builtin::View(_) => {
                // parse_views is responsible for inserting all builtin views.
                unreachable!("views added elsewhere");
            }

            // Note: Element types must be loaded before array types.
            Builtin::Type(typ) => {
                let typ = self.resolve_builtin_type_references(typ);
                if let CatalogType::Array { element_reference } = typ.details.typ {
                    let entry = self.get_entry_mut(&element_reference);
                    let item_type = match &mut entry.item {
                        CatalogItem::Type(item_type) => item_type,
                        _ => unreachable!("types can only reference other types"),
                    };
                    item_type.details.array_id = Some(id);
                }

                // Assert that no built-in types are record types so that we don't
                // need to bother to build a description. Only record types need
                // descriptions.
                let desc = None;
                assert!(!matches!(typ.details.typ, CatalogType::Record { .. }));
                let schema_id = self.resolve_system_schema(typ.schema);

                self.insert_item(
                    id,
                    typ.oid,
                    QualifiedItemName {
                        qualifiers: ItemQualifiers {
                            database_spec: ResolvedDatabaseSpecifier::Ambient,
                            schema_spec: SchemaSpecifier::Id(schema_id),
                        },
                        item: typ.name.to_owned(),
                    },
                    CatalogItem::Type(Type {
                        create_sql: None,
                        details: typ.details.clone(),
                        desc,
                        resolved_ids: ResolvedIds(BTreeSet::new()),
                    }),
                    MZ_SYSTEM_ROLE_ID,
                    PrivilegeMap::from_mz_acl_items(vec![
                        rbac::default_builtin_object_privilege(mz_sql::catalog::ObjectType::Type),
                        rbac::owner_privilege(mz_sql::catalog::ObjectType::Type, MZ_SYSTEM_ROLE_ID),
                    ]),
                );
            }

            Builtin::Func(func) => {
                // This OID is never used. `func` has a `Vec` of implementations and
                // each implementation has its own OID. Those are the OIDs that are
                // actually used by the system.
                let oid = INVALID_OID;
                self.insert_item(
                    id,
                    oid,
                    name.clone(),
                    CatalogItem::Func(Func { inner: func.inner }),
                    MZ_SYSTEM_ROLE_ID,
                    PrivilegeMap::default(),
                );
            }

            Builtin::Source(coll) => {
                let mut acl_items = vec![rbac::owner_privilege(
                    mz_sql::catalog::ObjectType::Source,
                    MZ_SYSTEM_ROLE_ID,
                )];
                acl_items.extend_from_slice(&coll.access);

                self.insert_item(
                    id,
                    coll.oid,
                    name.clone(),
                    CatalogItem::Source(Source {
                        create_sql: None,
                        data_source: DataSourceDesc::Introspection(coll.data_source),
                        desc: coll.desc.clone(),
                        timeline: Timeline::EpochMilliseconds,
                        resolved_ids: ResolvedIds(BTreeSet::new()),
                        custom_logical_compaction_window: coll.is_retained_metrics_object.then(
                            || {
                                self.system_config()
                                    .metrics_retention()
                                    .try_into()
                                    .expect("invalid metrics retention")
                            },
                        ),
                        is_retained_metrics_object: coll.is_retained_metrics_object,
                    }),
                    MZ_SYSTEM_ROLE_ID,
                    PrivilegeMap::from_mz_acl_items(acl_items),
                );
            }
        }
    }

    #[instrument(level = "debug")]
    fn apply_item_update(&mut self, item: StateDiff<mz_catalog::durable::Item>) {
        match item {
            StateDiff::Delete(retraction) => {
                self.drop_item(retraction.id);
            }
            StateDiff::Insert(addition) => {
                let catalog_item = self
                    .deserialize_item(&addition.create_sql)
                    .expect("invalid persisted SQL");
                let schema = self.find_non_temp_schema(&addition.schema_id);
                let name = QualifiedItemName {
                    qualifiers: ItemQualifiers {
                        database_spec: schema.database().clone(),
                        schema_spec: schema.id().clone(),
                    },
                    item: addition.name,
                };
                self.insert_item(
                    addition.id,
                    addition.oid,
                    name,
                    catalog_item,
                    addition.owner_id,
                    PrivilegeMap::from_mz_acl_items(addition.privileges),
                );
            }
            StateDiff::Update {
                retraction: _,
                addition,
            } => {
                let catalog_item = self
                    .deserialize_item(&addition.create_sql)
                    .expect("invalid persisted SQL");
                let schema = self.find_non_temp_schema(&addition.schema_id);
                let name = QualifiedItemName {
                    qualifiers: ItemQualifiers {
                        database_spec: schema.database().clone(),
                        schema_spec: schema.id().clone(),
                    },
                    item: addition.name,
                };
                // TODO(jkosh44) assertions for update?
                self.update_entry(
                    addition.id,
                    addition.oid,
                    name,
                    catalog_item,
                    addition.owner_id,
                    PrivilegeMap::from_mz_acl_items(addition.privileges),
                );
            }
        }
    }

    pub(crate) fn update_entry(
        &mut self,
        id: GlobalId,
        oid: u32,
        name: QualifiedItemName,
        item: CatalogItem,
        owner_id: RoleId,
        privileges: PrivilegeMap,
    ) {
        let old_entry = self.entry_by_id.remove(&id).expect("catalog out of sync");
        let conn_id = old_entry.item().conn_id().unwrap_or(&SYSTEM_CONN_ID);
        let schema = self.get_schema_mut(
            &old_entry.name().qualifiers.database_spec,
            &old_entry.name().qualifiers.schema_spec,
            conn_id,
        );
        schema.items.remove(&old_entry.name().item);

        // Dropped deps
        let dropped_references: Vec<_> = old_entry
            .references()
            .0
            .difference(&item.references().0)
            .cloned()
            .collect();
        let dropped_uses: Vec<_> = old_entry.uses().difference(&item.uses()).cloned().collect();

        // We only need to install this item on items in the `referenced_by` of new
        // dependencies.
        let new_references: Vec<_> = item
            .references()
            .0
            .difference(&old_entry.references().0)
            .cloned()
            .collect();
        // We only need to install this item on items in the `used_by` of new
        // dependencies.
        let new_uses: Vec<_> = item.uses().difference(&old_entry.uses()).cloned().collect();

        let mut new_entry = old_entry;
        new_entry.item = item;
        new_entry.id = id;
        new_entry.oid = oid;
        new_entry.name = name;
        new_entry.owner_id = owner_id;
        new_entry.privileges = privileges;

        schema.items.insert(new_entry.name().item.clone(), id);

        for u in dropped_references {
            // OK if we no longer have this entry because we are dropping our
            // dependency on it.
            if let Some(metadata) = self.entry_by_id.get_mut(&u) {
                metadata.referenced_by.retain(|dep_id| *dep_id != id)
            }
        }

        for u in dropped_uses {
            // OK if we no longer have this entry because we are dropping our
            // dependency on it.
            if let Some(metadata) = self.entry_by_id.get_mut(&u) {
                metadata.used_by.retain(|dep_id| *dep_id != id)
            }
        }

        for u in new_references {
            match self.entry_by_id.get_mut(&u) {
                Some(metadata) => metadata.referenced_by.push(new_entry.id()),
                None => panic!(
                    "Catalog: missing dependent catalog item {} while updating {}",
                    &u,
                    self.resolve_full_name(new_entry.name(), new_entry.conn_id())
                ),
            }
        }
        for u in new_uses {
            match self.entry_by_id.get_mut(&u) {
                Some(metadata) => metadata.used_by.push(new_entry.id()),
                None => panic!(
                    "Catalog: missing dependent catalog item {} while updating {}",
                    &u,
                    self.resolve_full_name(new_entry.name(), new_entry.conn_id())
                ),
            }
        }

        self.entry_by_id.insert(id, new_entry);
    }

    #[instrument(level = "debug")]
    fn apply_comment_update(&mut self, comment: StateDiff<mz_catalog::durable::Comment>) {
        match comment {
            StateDiff::Delete(retraction) => {
                let prev = self.comments.update_comment(
                    retraction.object_id,
                    retraction.sub_component,
                    None,
                );
                assert_eq!(
                    prev,
                    Some(retraction.comment),
                    "retraction does not match existing value: ({:?}, {:?})",
                    retraction.object_id,
                    retraction.sub_component,
                );
            }
            StateDiff::Insert(addition) => {
                let prev = self.comments.update_comment(
                    addition.object_id,
                    addition.sub_component,
                    Some(addition.comment),
                );
                assert_eq!(
                    prev, None,
                    "values must be explicitly retracted before inserting a new value: ({:?}, {:?})",
                    addition.object_id,
                    addition.sub_component,
                );
            }
            StateDiff::Update {
                retraction,
                addition,
            } => {
                let prev = self.comments.update_comment(
                    addition.object_id,
                    addition.sub_component,
                    Some(addition.comment),
                );
                assert_eq!(
                    prev,
                    Some(retraction.comment),
                    "retraction does not match existing value: ({:?}, {:?})",
                    retraction.object_id,
                    retraction.sub_component,
                );
            }
        }
    }

    #[instrument(level = "debug")]
    fn apply_storage_collection_metadata_update(
        &mut self,
        storage_collection_metadata: StateDiff<mz_catalog::durable::StorageCollectionMetadata>,
    ) {
        match storage_collection_metadata {
            StateDiff::Delete(retraction) => {
                let prev = self
                    .storage_metadata
                    .collection_metadata
                    .remove(&retraction.id);
                assert_eq!(
                    prev,
                    Some(retraction.shard),
                    "retraction does not match existing value: {:?}",
                    retraction.id
                );
            }
            StateDiff::Insert(addition) => {
                let prev = self
                    .storage_metadata
                    .collection_metadata
                    .insert(addition.id, addition.shard);
                assert_eq!(
                    prev, None,
                    "values must be explicitly retracted before inserting a new value: {:?}",
                    addition.id,
                );
            }
            StateDiff::Update {
                retraction,
                addition,
            } => {
                let prev = self
                    .storage_metadata
                    .collection_metadata
                    .insert(addition.id, addition.shard);
                assert_eq!(
                    prev,
                    Some(retraction.shard),
                    "retraction does not match existing value: {:?}",
                    retraction.id
                );
            }
        }
    }

    #[instrument(level = "debug")]
    fn apply_unfinalized_shard_update(
        &mut self,
        unfinalized_shard: StateDiff<mz_catalog::durable::UnfinalizedShard>,
    ) {
        match unfinalized_shard {
            StateDiff::Delete(retraction) => {
                let removed = self
                    .storage_metadata
                    .unfinalized_shards
                    .remove(&retraction.shard);
                assert!(
                    removed,
                    "retraction does not match existing value: {retraction:?}"
                );
            }
            StateDiff::Insert(addition) => {
                let newly_inserted = self
                    .storage_metadata
                    .unfinalized_shards
                    .insert(addition.shard);
                assert!(
                    newly_inserted,
                    "values must be explicitly retracted before inserting a new value",
                );
            }
            StateDiff::Update {
                retraction,
                addition,
            } => {
                let removed = self
                    .storage_metadata
                    .unfinalized_shards
                    .remove(&retraction.shard);
                assert!(
                    removed,
                    "retraction does not match existing value: {retraction:?}"
                );
                let newly_inserted = self
                    .storage_metadata
                    .unfinalized_shards
                    .insert(addition.shard);
                assert!(
                    newly_inserted,
                    "values must be explicitly retracted before inserting a new value",
                );
            }
        }
    }

    /// Generate a list of `BuiltinTableUpdate`s that correspond to a single update made to the
    /// durable catalog.
    #[instrument(level = "debug")]
    fn generate_builtin_table_update(
        &self,
        kind: YetAnotherStateUpdateKind,
        diff: Diff,
    ) -> Vec<BuiltinTableUpdate<&'static BuiltinTable>> {
        assert!(
            diff == 1 || diff == -1,
            "invalid update in catalog updates: ({kind:?}, {diff:?})"
        );
        match kind {
            YetAnotherStateUpdateKind::Role(role) => {
                let mut builtin_table_updates = self.pack_role_update(role.id, diff);
                for group_id in role.membership.map.keys() {
                    builtin_table_updates
                        .push(self.pack_role_members_update(*group_id, role.id, diff))
                }
                builtin_table_updates
            }
            YetAnotherStateUpdateKind::Database(database) => {
                vec![self.pack_database_update(&database.id, diff)]
            }
            YetAnotherStateUpdateKind::Schema(schema) => {
                let db_spec = schema.database_id.into();
                vec![self.pack_schema_update(&db_spec, &schema.id, diff)]
            }
            YetAnotherStateUpdateKind::DefaultPrivilege(default_privilege) => {
                vec![self.pack_default_privileges_update(
                    &default_privilege.object,
                    &default_privilege.acl_item.grantee,
                    &default_privilege.acl_item.acl_mode,
                    diff,
                )]
            }
            YetAnotherStateUpdateKind::SystemPrivilege(system_privilege) => {
                vec![self.pack_system_privileges_update(system_privilege, diff)]
            }
            YetAnotherStateUpdateKind::SystemConfiguration(_) => Vec::new(),
            YetAnotherStateUpdateKind::Cluster(cluster) => {
                self.pack_cluster_update(&cluster.name, diff)
            }
            YetAnotherStateUpdateKind::IntrospectionSourceIndex(introspection_source_index) => {
                self.pack_item_update(introspection_source_index.index_id, diff)
            }
            YetAnotherStateUpdateKind::ClusterReplica(cluster_replica) => self
                .pack_cluster_replica_update(
                    cluster_replica.cluster_id,
                    &cluster_replica.name,
                    diff,
                ),
            YetAnotherStateUpdateKind::SystemObjectMapping(system_object_mapping) => {
                self.pack_item_update(system_object_mapping.unique_identifier.id, diff)
            }
            YetAnotherStateUpdateKind::Item(item) => self.pack_item_update(item.id, diff),
            YetAnotherStateUpdateKind::Comment(comment) => vec![self.pack_comment_update(
                comment.object_id,
                comment.sub_component,
                &comment.comment,
                diff,
            )],
            YetAnotherStateUpdateKind::AuditLog(audit_log) => {
                vec![self
                    .pack_audit_log_update(&audit_log.event, diff)
                    .expect("could not pack audit log update")]
            }
            YetAnotherStateUpdateKind::StorageUsage(storage_usage) => {
                vec![self.pack_storage_usage_update(&storage_usage.metric, diff)]
            }
            YetAnotherStateUpdateKind::StorageCollectionMetadata(_)
            | YetAnotherStateUpdateKind::UnfinalizedShard(_) => Vec::new(),
        }
    }
}

/// Inserts `key` and `value` into `map` if `diff` is 1, otherwise remove them from `map` if `diff`
/// is -1.
/// TODO(jkosh44)
fn apply_inverted<K, V, D>(
    map: &mut BTreeMap<K, V>,
    key_fn: impl Fn(&D) -> K,
    value_fn: impl Fn(&D) -> V,
    state_diff: &StateDiff<D>,
) where
    K: Ord + Debug + Clone,
    V: PartialEq + Debug,
{
    match state_diff {
        StateDiff::Delete(retraction) => {
            let key = key_fn(retraction);
            let value = value_fn(retraction);
            let prev = map.remove(&key);
            assert_eq!(
                prev,
                Some(value),
                "retraction does not match existing value: {key:?}"
            );
        }
        StateDiff::Insert(addition) => {
            let key = key_fn(addition);
            let value = value_fn(addition);
            let prev = map.insert(key.clone(), value);
            assert_eq!(
                prev, None,
                "values must be explicitly retracted before inserting a new value: {key:?}"
            );
        }
        StateDiff::Update {
            retraction,
            addition,
        } => {
            let retraction_key = key_fn(retraction);
            let retraction_value = value_fn(retraction);
            let prev = map.remove(&retraction_key);
            assert_eq!(
                prev,
                Some(retraction_value),
                "retraction does not match existing value: {retraction_key:?}"
            );
            let addition_key = key_fn(addition);
            let addition_value = value_fn(addition);
            let prev = map.insert(addition_key, addition_value);
            assert_eq!(
                prev, None,
                "values must be explicitly retracted before inserting a new value"
            );
        }
    }
}

/// Inserts `key` and `value` into `map` if `diff` is 1, otherwise remove them from `map` if `diff`
/// is -1.
/// TODO(jkosh44)
fn apply_catalog_type<K, V, D>(
    map: &mut BTreeMap<K, V>,
    key_fn: impl Fn(&D) -> K,
    state_diff: StateDiff<D>,
) where
    K: Ord + Debug,
    V: TodoTraitName<D> + PartialEq + Debug,
{
    match state_diff {
        StateDiff::Delete(retraction) => {
            let key = key_fn(&retraction);
            let prev = map.remove(&key);
            // We can't assert the exact contents of the previous value, since we don't know
            // what it should look like.
            assert!(
                prev.is_some(),
                "retraction does not match existing value: {key:?}"
            );
        }
        StateDiff::Insert(addition) => {
            let key = key_fn(&addition);
            let value = addition.into();
            let prev = map.insert(key, value);
            assert_eq!(
                prev, None,
                "values must be explicitly retracted before inserting a new value"
            );
        }
        StateDiff::Update {
            retraction,
            addition,
        } => {
            let key = key_fn(&retraction);
            assert_eq!(key, key_fn(&addition));
            let value = map
                .get_mut(&key)
                .unwrap_or_else(|| panic!("missing value for {key:?}"));
            // TODO(jkosh44) Some assertions for retraction?
            value.update_from(addition);
        }
    }
}

fn lookup_builtin_view_addition(
    system_object_mapping: mz_catalog::durable::SystemObjectMapping,
) -> (&'static Builtin<NameReference>, GlobalId) {
    let (_, builtin) = BUILTIN_LOOKUP
        .get(&system_object_mapping.description)
        .expect("missing builtin view");
    (*builtin, system_object_mapping.unique_identifier.id)
}

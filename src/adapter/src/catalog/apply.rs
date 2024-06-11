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
use mz_catalog::durable::Item;
use mz_catalog::memory::error::{Error, ErrorKind};
use mz_catalog::memory::objects::{
    CatalogItem, Cluster, ClusterReplica, DataSourceDesc, Database, Func, Log, Role, Schema,
    Source, StateDiff, StateUpdate, StateUpdateKind, Table, Type,
};
use mz_compute_client::controller::ComputeReplicaConfig;
use mz_controller::clusters::{ReplicaConfig, ReplicaLogging};
use mz_ore::instrument;
use mz_pgrepr::oid::INVALID_OID;
use mz_repr::adt::mz_acl_item::{MzAclItem, PrivilegeMap};
use mz_repr::GlobalId;
use mz_sql::catalog::{CatalogItemType, CatalogSchema, CatalogType, NameReference};
use mz_sql::names::{
    ItemQualifiers, QualifiedItemName, QualifiedSchemaName, ResolvedDatabaseSpecifier, ResolvedIds,
    SchemaSpecifier,
};
use mz_sql::rbac;
use mz_sql::session::user::MZ_SYSTEM_ROLE_ID;
use mz_sql::session::vars::{VarError, VarInput};
use mz_sql_parser::ast::Expr;
use mz_storage_types::sources::Timeline;
use tracing::warn;

use crate::catalog::{BuiltinTableUpdate, Catalog, CatalogState};

/// Sort [`StateUpdate`]s in dependency order.
fn sort_updates(updates: Vec<StateUpdate>) -> Vec<StateUpdate> {
    fn push_update<T>(
        update: T,
        diff: StateDiff,
        retractions: &mut Vec<T>,
        additions: &mut Vec<T>,
    ) {
        match diff {
            StateDiff::Retraction => retractions.push(update),
            StateDiff::Addition => additions.push(update),
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
        let diff = update.diff.clone();
        match update.kind {
            StateUpdateKind::Role(_)
            | StateUpdateKind::Database(_)
            | StateUpdateKind::Schema(_)
            | StateUpdateKind::DefaultPrivilege(_)
            | StateUpdateKind::SystemPrivilege(_)
            | StateUpdateKind::SystemConfiguration(_) => push_update(
                update,
                diff,
                &mut pre_cluster_retractions,
                &mut pre_cluster_additions,
            ),
            StateUpdateKind::Cluster(_)
            | StateUpdateKind::IntrospectionSourceIndex(_)
            | StateUpdateKind::ClusterReplica(_) => push_update(
                update,
                diff,
                &mut cluster_retractions,
                &mut cluster_additions,
            ),
            StateUpdateKind::SystemObjectMapping(system_object_mapping) => {
                builtin_item_updates.push((system_object_mapping, update.diff))
            }
            StateUpdateKind::Item(item) => push_update(
                (item, update.diff),
                diff,
                &mut item_retractions,
                &mut item_additions,
            ),
            StateUpdateKind::Comment(_)
            | StateUpdateKind::AuditLog(_)
            | StateUpdateKind::StorageUsage(_)
            | StateUpdateKind::StorageCollectionMetadata(_)
            | StateUpdateKind::UnfinalizedShard(_) => push_update(
                update,
                diff,
                &mut post_item_retractions,
                &mut post_item_additions,
            ),
        }
    }

    // Sort builtin item updates by dependency.
    let builtin_item_updates = builtin_item_updates
        .into_iter()
        .map(|(system_object_mapping, diff)| {
            let idx = BUILTIN_LOOKUP
                .get(&system_object_mapping.description)
                .expect("missing builtin")
                .0;
            (idx, system_object_mapping, diff)
        })
        .sorted_by_key(|(idx, _, _)| *idx)
        .map(|(_, system_object_mapping, diff)| (system_object_mapping, diff));

    // Further partition builtin item updates.
    let mut other_builtin_retractions = Vec::new();
    let mut other_builtin_additions = Vec::new();
    let mut builtin_index_retractions = Vec::new();
    let mut builtin_index_additions = Vec::new();
    for (builtin_item_update, diff) in builtin_item_updates {
        match &builtin_item_update.description.object_type {
            CatalogItemType::Index => push_update(
                StateUpdate {
                    kind: StateUpdateKind::SystemObjectMapping(builtin_item_update),
                    diff,
                },
                diff,
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
                    diff,
                },
                diff,
                &mut other_builtin_retractions,
                &mut other_builtin_additions,
            ),
        }
    }

    // Sort item updates by GlobalId.
    fn sort_item_updates(item_updates: Vec<(Item, StateDiff)>) -> Vec<StateUpdate> {
        item_updates
            .into_iter()
            .sorted_by_key(|(item, _diff)| item.id)
            .map(|(item, diff)| StateUpdate {
                kind: StateUpdateKind::Item(item),
                diff,
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

        fn lookup_builtin_view_addition(
            system_object_mapping: mz_catalog::durable::SystemObjectMapping,
        ) -> (&'static Builtin<NameReference>, GlobalId) {
            let (_, builtin) = BUILTIN_LOOKUP
                .get(&system_object_mapping.description)
                .expect("missing builtin view");
            (*builtin, system_object_mapping.unique_identifier.id)
        }

        let mut state = ApplyState::Updates(Vec::new());
        let updates = sort_updates(updates);
        let mut builtin_table_updates = Vec::with_capacity(updates.len());

        for update in updates {
            match (&mut state, update) {
                (
                    ApplyState::Updates(updates),
                    StateUpdate {
                        kind: StateUpdateKind::SystemObjectMapping(system_object_mapping),
                        diff: StateDiff::Addition,
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
                        kind: StateUpdateKind::SystemObjectMapping(system_object_mapping),
                        diff: StateDiff::Addition,
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
        for StateUpdate { kind, diff } in updates {
            match diff {
                StateDiff::Retraction => {
                    // We want the builtin table retraction to match the state of the catalog
                    // before applying the update.
                    builtin_table_updates
                        .extend(self.generate_builtin_table_update(kind.clone(), diff));
                    self.apply_update(kind, diff);
                }
                StateDiff::Addition => {
                    self.apply_update(kind.clone(), diff);
                    // We want the builtin table addition to match the state of the catalog
                    // after applying the update.
                    builtin_table_updates
                        .extend(self.generate_builtin_table_update(kind.clone(), diff));
                }
            }
        }
        builtin_table_updates
    }

    #[instrument(level = "debug")]
    fn apply_update(&mut self, kind: StateUpdateKind, diff: StateDiff) {
        match kind {
            StateUpdateKind::Role(role) => {
                self.apply_role_update(role, diff);
            }
            StateUpdateKind::Database(database) => {
                self.apply_database_update(database, diff);
            }
            StateUpdateKind::Schema(schema) => {
                self.apply_schema_update(schema, diff);
            }
            StateUpdateKind::DefaultPrivilege(default_privilege) => {
                self.apply_default_privilege_update(default_privilege, diff);
            }
            StateUpdateKind::SystemPrivilege(system_privilege) => {
                self.apply_system_privilege_update(system_privilege, diff);
            }
            StateUpdateKind::SystemConfiguration(system_configuration) => {
                self.apply_system_configuration_update(system_configuration, diff);
            }
            StateUpdateKind::Cluster(cluster) => {
                self.apply_cluster_update(cluster, diff);
            }
            StateUpdateKind::IntrospectionSourceIndex(introspection_source_index) => {
                self.apply_introspection_source_index_update(introspection_source_index, diff);
            }
            StateUpdateKind::ClusterReplica(cluster_replica) => {
                self.apply_cluster_replica_update(cluster_replica, diff);
            }
            StateUpdateKind::SystemObjectMapping(system_object_mapping) => {
                self.apply_system_object_mapping_update(system_object_mapping, diff);
            }
            StateUpdateKind::Item(item) => {
                self.apply_item_update(item, diff);
            }
            StateUpdateKind::Comment(comment) => {
                self.apply_comment_update(comment, diff);
            }
            StateUpdateKind::AuditLog(_audit_log) => {
                // Audit logs are not stored in-memory.
            }
            StateUpdateKind::StorageUsage(_storage_usage) => {
                // Storage usage events are not stored in-memory.
            }
            StateUpdateKind::StorageCollectionMetadata(storage_collection_metadata) => {
                self.apply_storage_collection_metadata_update(storage_collection_metadata, diff);
            }
            StateUpdateKind::UnfinalizedShard(unfinalized_shard) => {
                self.apply_unfinalized_shard_update(unfinalized_shard, diff);
            }
        }
    }

    #[instrument(level = "debug")]
    fn apply_role_update(&mut self, role: mz_catalog::durable::Role, diff: StateDiff) {
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
    fn apply_database_update(&mut self, database: mz_catalog::durable::Database, diff: StateDiff) {
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
    fn apply_schema_update(&mut self, schema: mz_catalog::durable::Schema, diff: StateDiff) {
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
        diff: StateDiff,
    ) {
        match diff {
            StateDiff::Addition => self
                .default_privileges
                .grant(default_privilege.object, default_privilege.acl_item),
            StateDiff::Retraction => self
                .default_privileges
                .revoke(&default_privilege.object, &default_privilege.acl_item),
        }
    }

    #[instrument(level = "debug")]
    fn apply_system_privilege_update(&mut self, system_privilege: MzAclItem, diff: StateDiff) {
        match diff {
            StateDiff::Addition => self.system_privileges.grant(system_privilege),
            StateDiff::Retraction => self.system_privileges.revoke(&system_privilege),
        }
    }

    #[instrument(level = "debug")]
    fn apply_system_configuration_update(
        &mut self,
        system_configuration: mz_catalog::durable::SystemConfiguration,
        diff: StateDiff,
    ) {
        let res = match diff {
            StateDiff::Addition => self.insert_system_configuration(
                &system_configuration.name,
                VarInput::Flat(&system_configuration.value),
            ),
            StateDiff::Retraction => self.remove_system_configuration(&system_configuration.name),
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
    fn apply_cluster_update(&mut self, cluster: mz_catalog::durable::Cluster, diff: StateDiff) {
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
        diff: StateDiff,
    ) {
        let log = BUILTIN_LOG_LOOKUP
            .get(introspection_source_index.name.as_str())
            .expect("missing log");
        match diff {
            StateDiff::Addition => {
                self.insert_introspection_source_index(
                    introspection_source_index.cluster_id,
                    log,
                    introspection_source_index.index_id,
                    introspection_source_index.oid,
                );
            }
            StateDiff::Retraction => {
                self.drop_item(introspection_source_index.index_id);
            }
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

    #[instrument(level = "debug")]
    fn apply_cluster_replica_update(
        &mut self,
        cluster_replica: mz_catalog::durable::ClusterReplica,
        diff: StateDiff,
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

    #[instrument(level = "debug")]
    fn apply_system_object_mapping_update(
        &mut self,
        system_object_mapping: mz_catalog::durable::SystemObjectMapping,
        diff: StateDiff,
    ) {
        let id = system_object_mapping.unique_identifier.id;

        if let StateDiff::Retraction = diff {
            self.drop_item(id);
            return;
        }

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
    fn apply_item_update(&mut self, item: mz_catalog::durable::Item, diff: StateDiff) {
        match diff {
            StateDiff::Addition => {
                let catalog_item = self
                    .deserialize_item(&item.create_sql)
                    .expect("invalid persisted SQL");
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
            }
            StateDiff::Retraction => {
                self.drop_item(item.id);
            }
        }
    }

    #[instrument(level = "debug")]
    fn apply_comment_update(&mut self, comment: mz_catalog::durable::Comment, diff: StateDiff) {
        match diff {
            StateDiff::Addition => {
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
            StateDiff::Retraction => {
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
        }
    }

    #[instrument(level = "debug")]
    fn apply_storage_collection_metadata_update(
        &mut self,
        storage_collection_metadata: mz_catalog::durable::StorageCollectionMetadata,
        diff: StateDiff,
    ) {
        apply(
            &mut self.storage_metadata.collection_metadata,
            storage_collection_metadata.id,
            || storage_collection_metadata.shard,
            diff,
        );
    }

    #[instrument(level = "debug")]
    fn apply_unfinalized_shard_update(
        &mut self,
        unfinalized_shard: mz_catalog::durable::UnfinalizedShard,
        diff: StateDiff,
    ) {
        match diff {
            StateDiff::Addition => {
                let newly_inserted = self
                    .storage_metadata
                    .unfinalized_shards
                    .insert(unfinalized_shard.shard.clone());
                assert!(
                    newly_inserted,
                    "values must be explicitly retracted before inserting a new value: {unfinalized_shard:?}",
                );
            }
            StateDiff::Retraction => {
                let removed = self
                    .storage_metadata
                    .unfinalized_shards
                    .remove(&unfinalized_shard.shard);
                assert!(
                    removed,
                    "retraction does not match existing value: {unfinalized_shard:?}"
                );
            }
        }
    }

    /// Generate a list of `BuiltinTableUpdate`s that correspond to a list of updates made to the
    /// durable catalog.
    #[instrument]
    pub(crate) fn generate_builtin_table_updates(
        &self,
        updates: Vec<StateUpdate>,
    ) -> Vec<BuiltinTableUpdate> {
        let mut builtin_table_updates = Vec::new();
        for StateUpdate { kind, diff } in updates {
            let builtin_table_update = self.generate_builtin_table_update(kind, diff);
            let builtin_table_update = self.resolve_builtin_table_updates(builtin_table_update);
            builtin_table_updates.extend(builtin_table_update);
        }
        builtin_table_updates
    }

    /// Generate a list of `BuiltinTableUpdate`s that correspond to a single update made to the
    /// durable catalog.
    #[instrument(level = "debug")]
    fn generate_builtin_table_update(
        &self,
        kind: StateUpdateKind,
        diff: StateDiff,
    ) -> Vec<BuiltinTableUpdate<&'static BuiltinTable>> {
        let diff = diff.into();
        match kind {
            StateUpdateKind::Role(role) => {
                let mut builtin_table_updates = self.pack_role_update(role.id, diff);
                for group_id in role.membership.map.keys() {
                    builtin_table_updates
                        .push(self.pack_role_members_update(*group_id, role.id, diff))
                }
                builtin_table_updates
            }
            StateUpdateKind::Database(database) => {
                vec![self.pack_database_update(&database.id, diff)]
            }
            StateUpdateKind::Schema(schema) => {
                let db_spec = schema.database_id.into();
                vec![self.pack_schema_update(&db_spec, &schema.id, diff)]
            }
            StateUpdateKind::DefaultPrivilege(default_privilege) => {
                vec![self.pack_default_privileges_update(
                    &default_privilege.object,
                    &default_privilege.acl_item.grantee,
                    &default_privilege.acl_item.acl_mode,
                    diff,
                )]
            }
            StateUpdateKind::SystemPrivilege(system_privilege) => {
                vec![self.pack_system_privileges_update(system_privilege, diff)]
            }
            StateUpdateKind::SystemConfiguration(_) => Vec::new(),
            StateUpdateKind::Cluster(cluster) => self.pack_cluster_update(&cluster.name, diff),
            StateUpdateKind::IntrospectionSourceIndex(introspection_source_index) => {
                self.pack_item_update(introspection_source_index.index_id, diff)
            }
            StateUpdateKind::ClusterReplica(cluster_replica) => self.pack_cluster_replica_update(
                cluster_replica.cluster_id,
                &cluster_replica.name,
                diff,
            ),
            StateUpdateKind::SystemObjectMapping(system_object_mapping) => {
                self.pack_item_update(system_object_mapping.unique_identifier.id, diff)
            }
            StateUpdateKind::Item(item) => self.pack_item_update(item.id, diff),
            StateUpdateKind::Comment(comment) => vec![self.pack_comment_update(
                comment.object_id,
                comment.sub_component,
                &comment.comment,
                diff,
            )],
            StateUpdateKind::AuditLog(audit_log) => {
                vec![self
                    .pack_audit_log_update(&audit_log.event, diff)
                    .expect("could not pack audit log update")]
            }
            StateUpdateKind::StorageUsage(storage_usage) => {
                vec![self.pack_storage_usage_update(&storage_usage.metric, diff)]
            }
            StateUpdateKind::StorageCollectionMetadata(_)
            | StateUpdateKind::UnfinalizedShard(_) => Vec::new(),
        }
    }
}

/// Inserts `key` and `value` into `map` if `diff` is an addition, otherwise remove them from `map`
/// if `diff` is a retraction.
fn apply<K, V>(map: &mut BTreeMap<K, V>, key: K, value: impl FnOnce() -> V, diff: StateDiff)
where
    K: Ord + Debug,
    V: PartialEq + Debug,
{
    match diff {
        StateDiff::Retraction => {
            let prev = map.remove(&key);
            // We can't assert the exact contents of the previous value, since we don't know
            // what it should look like.
            assert!(
                prev.is_some(),
                "retraction does not match existing value: {key:?}"
            );
        }
        StateDiff::Addition => {
            let prev = map.insert(key, value());
            assert_eq!(
                prev, None,
                "values must be explicitly retracted before inserting a new value"
            );
        }
    }
}

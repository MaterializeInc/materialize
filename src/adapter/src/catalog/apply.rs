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

use core::panic;
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fmt::Debug;
use std::iter;
use std::str::FromStr;
use std::sync::Arc;

use futures::future;
use itertools::{Either, Itertools};
use mz_adapter_types::connection::ConnectionId;
use mz_catalog::SYSTEM_CONN_ID;
use mz_catalog::builtin::{
    BUILTIN_LOG_LOOKUP, BUILTIN_LOOKUP, Builtin, BuiltinLog, BuiltinTable, BuiltinView,
};
use mz_catalog::durable::objects::{
    ClusterKey, DatabaseKey, DurableType, ItemKey, NetworkPolicyKey, RoleAuthKey, RoleKey,
    SchemaKey,
};
use mz_catalog::durable::{CatalogError, SystemObjectMapping};
use mz_catalog::memory::error::{Error, ErrorKind};
use mz_catalog::memory::objects::{
    CatalogEntry, CatalogItem, Cluster, ClusterReplica, DataSourceDesc, Database, Func, Index, Log,
    NetworkPolicy, Role, RoleAuth, Schema, Source, StateDiff, StateUpdate, StateUpdateKind, Table,
    TableDataSource, TemporaryItem, Type, UpdateFrom,
};
use mz_compute_types::config::ComputeReplicaConfig;
use mz_controller::clusters::{ReplicaConfig, ReplicaLogging};
use mz_controller_types::ClusterId;
use mz_expr::MirScalarExpr;
use mz_ore::collections::CollectionExt;
use mz_ore::tracing::OpenTelemetryContext;
use mz_ore::{assert_none, instrument, soft_assert_no_log};
use mz_pgrepr::oid::INVALID_OID;
use mz_repr::adt::mz_acl_item::{MzAclItem, PrivilegeMap};
use mz_repr::role_id::RoleId;
use mz_repr::{CatalogItemId, Diff, GlobalId, RelationVersion, Timestamp, VersionedRelationDesc};
use mz_sql::catalog::CatalogError as SqlCatalogError;
use mz_sql::catalog::{CatalogItem as SqlCatalogItem, CatalogItemType, CatalogSchema, CatalogType};
use mz_sql::names::{
    FullItemName, ItemQualifiers, QualifiedItemName, RawDatabaseSpecifier,
    ResolvedDatabaseSpecifier, ResolvedIds, SchemaSpecifier,
};
use mz_sql::session::user::MZ_SYSTEM_ROLE_ID;
use mz_sql::session::vars::{VarError, VarInput};
use mz_sql::{plan, rbac};
use mz_sql_parser::ast::Expr;
use mz_storage_types::sources::Timeline;
use tracing::{Instrument, info_span, warn};

use crate::AdapterError;
use crate::catalog::state::LocalExpressionCache;
use crate::catalog::{BuiltinTableUpdate, CatalogState};
use crate::util::index_sql;

/// Maintains the state of retractions while applying catalog state updates for a single timestamp.
/// [`CatalogState`] maintains denormalized state for certain catalog objects. Updating an object
/// results in applying a retraction for that object followed by applying an addition for that
/// object. When applying those additions it can be extremely expensive to re-build that
/// denormalized state from scratch. To avoid that issue we stash the denormalized state from
/// retractions, so it can be used during additions.
///
/// Not all objects maintain denormalized state, so we only stash the retractions for the subset of
/// objects that maintain denormalized state.
// TODO(jkosh44) It might be simpler or more future proof to include all object types here, even if
// the update step is a no-op for certain types.
#[derive(Debug, Clone, Default)]
struct InProgressRetractions {
    roles: BTreeMap<RoleKey, Role>,
    role_auths: BTreeMap<RoleAuthKey, RoleAuth>,
    databases: BTreeMap<DatabaseKey, Database>,
    schemas: BTreeMap<SchemaKey, Schema>,
    clusters: BTreeMap<ClusterKey, Cluster>,
    network_policies: BTreeMap<NetworkPolicyKey, NetworkPolicy>,
    items: BTreeMap<ItemKey, CatalogEntry>,
    temp_items: BTreeMap<CatalogItemId, CatalogEntry>,
    introspection_source_indexes: BTreeMap<CatalogItemId, CatalogEntry>,
    system_object_mappings: BTreeMap<CatalogItemId, CatalogEntry>,
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
        local_expression_cache: &mut LocalExpressionCache,
    ) -> Vec<BuiltinTableUpdate<&'static BuiltinTable>> {
        let mut builtin_table_updates = Vec::with_capacity(updates.len());
        let updates = sort_updates(updates);

        let mut groups: Vec<Vec<_>> = Vec::new();
        for (_, updates) in &updates.into_iter().chunk_by(|update| update.ts) {
            groups.push(updates.collect());
        }
        for updates in groups {
            let mut apply_state = BootstrapApplyState::Updates(Vec::new());
            let mut retractions = InProgressRetractions::default();

            for update in updates {
                let next_apply_state = BootstrapApplyState::new(update);
                let (next_apply_state, builtin_table_update) = apply_state
                    .step(
                        next_apply_state,
                        self,
                        &mut retractions,
                        local_expression_cache,
                    )
                    .await;
                apply_state = next_apply_state;
                builtin_table_updates.extend(builtin_table_update);
            }

            // Apply remaining state.
            let builtin_table_update = apply_state
                .apply(self, &mut retractions, local_expression_cache)
                .await;
            builtin_table_updates.extend(builtin_table_update);
        }
        builtin_table_updates
    }

    /// Update in-memory catalog state from a list of updates made to the durable catalog state.
    ///
    /// Returns builtin table updates corresponding to the changes to catalog state.
    #[instrument]
    pub(crate) fn apply_updates(
        &mut self,
        updates: Vec<StateUpdate>,
    ) -> Result<Vec<BuiltinTableUpdate<&'static BuiltinTable>>, CatalogError> {
        let mut builtin_table_updates = Vec::with_capacity(updates.len());
        let updates = sort_updates(updates);

        for (_, updates) in &updates.into_iter().chunk_by(|update| update.ts) {
            let mut retractions = InProgressRetractions::default();
            let builtin_table_update = self.apply_updates_inner(
                updates.collect(),
                &mut retractions,
                &mut LocalExpressionCache::Closed,
            )?;
            builtin_table_updates.extend(builtin_table_update);
        }

        Ok(builtin_table_updates)
    }

    #[instrument(level = "debug")]
    fn apply_updates_inner(
        &mut self,
        updates: Vec<StateUpdate>,
        retractions: &mut InProgressRetractions,
        local_expression_cache: &mut LocalExpressionCache,
    ) -> Result<Vec<BuiltinTableUpdate<&'static BuiltinTable>>, CatalogError> {
        soft_assert_no_log!(
            updates.iter().map(|update| update.ts).all_equal(),
            "all timestamps should be equal: {updates:?}"
        );

        let mut update_system_config = false;

        let mut builtin_table_updates = Vec::with_capacity(updates.len());
        for StateUpdate { kind, ts: _, diff } in updates {
            if matches!(kind, StateUpdateKind::SystemConfiguration(_)) {
                update_system_config = true;
            }

            match diff {
                StateDiff::Retraction => {
                    // We want the builtin table retraction to match the state of the catalog
                    // before applying the update.
                    builtin_table_updates
                        .extend(self.generate_builtin_table_update(kind.clone(), diff));
                    self.apply_update(kind, diff, retractions, local_expression_cache)?;
                }
                StateDiff::Addition => {
                    self.apply_update(kind.clone(), diff, retractions, local_expression_cache)?;
                    // We want the builtin table addition to match the state of the catalog
                    // after applying the update.
                    builtin_table_updates
                        .extend(self.generate_builtin_table_update(kind.clone(), diff));
                }
            }
        }

        if update_system_config {
            self.system_configuration.dyncfg_updates();
        }

        Ok(builtin_table_updates)
    }

    #[instrument(level = "debug")]
    fn apply_update(
        &mut self,
        kind: StateUpdateKind,
        diff: StateDiff,
        retractions: &mut InProgressRetractions,
        local_expression_cache: &mut LocalExpressionCache,
    ) -> Result<(), CatalogError> {
        match kind {
            StateUpdateKind::Role(role) => {
                self.apply_role_update(role, diff, retractions);
            }
            StateUpdateKind::RoleAuth(role_auth) => {
                self.apply_role_auth_update(role_auth, diff, retractions);
            }
            StateUpdateKind::Database(database) => {
                self.apply_database_update(database, diff, retractions);
            }
            StateUpdateKind::Schema(schema) => {
                self.apply_schema_update(schema, diff, retractions);
            }
            StateUpdateKind::DefaultPrivilege(default_privilege) => {
                self.apply_default_privilege_update(default_privilege, diff, retractions);
            }
            StateUpdateKind::SystemPrivilege(system_privilege) => {
                self.apply_system_privilege_update(system_privilege, diff, retractions);
            }
            StateUpdateKind::SystemConfiguration(system_configuration) => {
                self.apply_system_configuration_update(system_configuration, diff, retractions);
            }
            StateUpdateKind::Cluster(cluster) => {
                self.apply_cluster_update(cluster, diff, retractions);
            }
            StateUpdateKind::NetworkPolicy(network_policy) => {
                self.apply_network_policy_update(network_policy, diff, retractions);
            }
            StateUpdateKind::IntrospectionSourceIndex(introspection_source_index) => {
                self.apply_introspection_source_index_update(
                    introspection_source_index,
                    diff,
                    retractions,
                );
            }
            StateUpdateKind::ClusterReplica(cluster_replica) => {
                self.apply_cluster_replica_update(cluster_replica, diff, retractions);
            }
            StateUpdateKind::SystemObjectMapping(system_object_mapping) => {
                self.apply_system_object_mapping_update(
                    system_object_mapping,
                    diff,
                    retractions,
                    local_expression_cache,
                );
            }
            StateUpdateKind::TemporaryItem(item) => {
                self.apply_temporary_item_update(item, diff, retractions);
            }
            StateUpdateKind::Item(item) => {
                self.apply_item_update(item, diff, retractions, local_expression_cache)?;
            }
            StateUpdateKind::Comment(comment) => {
                self.apply_comment_update(comment, diff, retractions);
            }
            StateUpdateKind::SourceReferences(source_reference) => {
                self.apply_source_references_update(source_reference, diff, retractions);
            }
            StateUpdateKind::AuditLog(_audit_log) => {
                // Audit logs are not stored in-memory.
            }
            StateUpdateKind::StorageCollectionMetadata(storage_collection_metadata) => {
                self.apply_storage_collection_metadata_update(
                    storage_collection_metadata,
                    diff,
                    retractions,
                );
            }
            StateUpdateKind::UnfinalizedShard(unfinalized_shard) => {
                self.apply_unfinalized_shard_update(unfinalized_shard, diff, retractions);
            }
        }

        Ok(())
    }

    #[instrument(level = "debug")]
    fn apply_role_auth_update(
        &mut self,
        role_auth: mz_catalog::durable::RoleAuth,
        diff: StateDiff,
        retractions: &mut InProgressRetractions,
    ) {
        apply_with_update(
            &mut self.role_auth_by_id,
            role_auth,
            |role_auth| role_auth.role_id,
            diff,
            &mut retractions.role_auths,
        );
    }

    #[instrument(level = "debug")]
    fn apply_role_update(
        &mut self,
        role: mz_catalog::durable::Role,
        diff: StateDiff,
        retractions: &mut InProgressRetractions,
    ) {
        apply_inverted_lookup(&mut self.roles_by_name, &role.name, role.id, diff);
        apply_with_update(
            &mut self.roles_by_id,
            role,
            |role| role.id,
            diff,
            &mut retractions.roles,
        );
    }

    #[instrument(level = "debug")]
    fn apply_database_update(
        &mut self,
        database: mz_catalog::durable::Database,
        diff: StateDiff,
        retractions: &mut InProgressRetractions,
    ) {
        apply_inverted_lookup(
            &mut self.database_by_name,
            &database.name,
            database.id,
            diff,
        );
        apply_with_update(
            &mut self.database_by_id,
            database,
            |database| database.id,
            diff,
            &mut retractions.databases,
        );
    }

    #[instrument(level = "debug")]
    fn apply_schema_update(
        &mut self,
        schema: mz_catalog::durable::Schema,
        diff: StateDiff,
        retractions: &mut InProgressRetractions,
    ) {
        let (schemas_by_id, schemas_by_name) = match &schema.database_id {
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
        apply_inverted_lookup(schemas_by_name, &schema.name, schema.id, diff);
        apply_with_update(
            schemas_by_id,
            schema,
            |schema| schema.id,
            diff,
            &mut retractions.schemas,
        );
    }

    #[instrument(level = "debug")]
    fn apply_default_privilege_update(
        &mut self,
        default_privilege: mz_catalog::durable::DefaultPrivilege,
        diff: StateDiff,
        _retractions: &mut InProgressRetractions,
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
    fn apply_system_privilege_update(
        &mut self,
        system_privilege: MzAclItem,
        diff: StateDiff,
        _retractions: &mut InProgressRetractions,
    ) {
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
        _retractions: &mut InProgressRetractions,
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
    fn apply_cluster_update(
        &mut self,
        cluster: mz_catalog::durable::Cluster,
        diff: StateDiff,
        retractions: &mut InProgressRetractions,
    ) {
        apply_inverted_lookup(&mut self.clusters_by_name, &cluster.name, cluster.id, diff);
        apply_with_update(
            &mut self.clusters_by_id,
            cluster,
            |cluster| cluster.id,
            diff,
            &mut retractions.clusters,
        );
    }

    #[instrument(level = "debug")]
    fn apply_network_policy_update(
        &mut self,
        policy: mz_catalog::durable::NetworkPolicy,
        diff: StateDiff,
        retractions: &mut InProgressRetractions,
    ) {
        apply_inverted_lookup(
            &mut self.network_policies_by_name,
            &policy.name,
            policy.id,
            diff,
        );
        apply_with_update(
            &mut self.network_policies_by_id,
            policy,
            |policy| policy.id,
            diff,
            &mut retractions.network_policies,
        );
    }

    #[instrument(level = "debug")]
    fn apply_introspection_source_index_update(
        &mut self,
        introspection_source_index: mz_catalog::durable::IntrospectionSourceIndex,
        diff: StateDiff,
        retractions: &mut InProgressRetractions,
    ) {
        let cluster = self
            .clusters_by_id
            .get_mut(&introspection_source_index.cluster_id)
            .expect("catalog out of sync");
        let log = BUILTIN_LOG_LOOKUP
            .get(introspection_source_index.name.as_str())
            .expect("missing log");
        apply_inverted_lookup(
            &mut cluster.log_indexes,
            &log.variant,
            introspection_source_index.index_id,
            diff,
        );

        match diff {
            StateDiff::Addition => {
                if let Some(mut entry) = retractions
                    .introspection_source_indexes
                    .remove(&introspection_source_index.item_id)
                {
                    // This should only happen during startup as a result of builtin migrations. We
                    // create a new index item and replace the old one with it.
                    let (index_name, index) = self.create_introspection_source_index(
                        introspection_source_index.cluster_id,
                        log,
                        introspection_source_index.index_id,
                    );
                    assert_eq!(entry.id, introspection_source_index.item_id);
                    assert_eq!(entry.oid, introspection_source_index.oid);
                    assert_eq!(entry.name, index_name);
                    entry.item = index;
                    self.insert_entry(entry);
                } else {
                    self.insert_introspection_source_index(
                        introspection_source_index.cluster_id,
                        log,
                        introspection_source_index.item_id,
                        introspection_source_index.index_id,
                        introspection_source_index.oid,
                    );
                }
            }
            StateDiff::Retraction => {
                let entry = self.drop_item(introspection_source_index.item_id);
                retractions
                    .introspection_source_indexes
                    .insert(entry.id, entry);
            }
        }
    }

    #[instrument(level = "debug")]
    fn apply_cluster_replica_update(
        &mut self,
        cluster_replica: mz_catalog::durable::ClusterReplica,
        diff: StateDiff,
        _retractions: &mut InProgressRetractions,
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
        apply_inverted_lookup(
            &mut cluster.replica_id_by_name_,
            &cluster_replica.name,
            cluster_replica.replica_id,
            diff,
        );
        match diff {
            StateDiff::Retraction => {
                let prev = cluster.replicas_by_id_.remove(&cluster_replica.replica_id);
                assert!(
                    prev.is_some(),
                    "retraction does not match existing value: {:?}",
                    cluster_replica.replica_id
                );
            }
            StateDiff::Addition => {
                let logging = ReplicaLogging {
                    log_logging: cluster_replica.config.logging.log_logging,
                    interval: cluster_replica.config.logging.interval,
                };
                let config = ReplicaConfig {
                    location,
                    compute: ComputeReplicaConfig { logging },
                };
                let mem_cluster_replica = ClusterReplica {
                    name: cluster_replica.name.clone(),
                    cluster_id: cluster_replica.cluster_id,
                    replica_id: cluster_replica.replica_id,
                    config,
                    owner_id: cluster_replica.owner_id,
                };
                let prev = cluster
                    .replicas_by_id_
                    .insert(cluster_replica.replica_id, mem_cluster_replica);
                assert_eq!(
                    prev, None,
                    "values must be explicitly retracted before inserting a new value: {:?}",
                    cluster_replica.replica_id
                );
            }
        }
    }

    #[instrument(level = "debug")]
    fn apply_system_object_mapping_update(
        &mut self,
        system_object_mapping: mz_catalog::durable::SystemObjectMapping,
        diff: StateDiff,
        retractions: &mut InProgressRetractions,
        local_expression_cache: &mut LocalExpressionCache,
    ) {
        let item_id = system_object_mapping.unique_identifier.catalog_id;
        let global_id = system_object_mapping.unique_identifier.global_id;

        if system_object_mapping.unique_identifier.runtime_alterable() {
            // Runtime-alterable system objects have real entries in the items
            // collection and so get handled through the normal `insert_item`
            // and `drop_item` code paths.
            return;
        }

        if let StateDiff::Retraction = diff {
            let entry = self.drop_item(item_id);
            retractions.system_object_mappings.insert(item_id, entry);
            return;
        }

        if let Some(entry) = retractions.system_object_mappings.remove(&item_id) {
            // This implies that we updated the fingerprint for some builtin item. The retraction
            // was parsed, planned, and optimized using the compiled in definition, not the
            // definition from a previous version. So we can just stick the old entry back into the
            // catalog.
            self.insert_entry(entry);
            return;
        }

        let builtin = BUILTIN_LOOKUP
            .get(&system_object_mapping.description)
            .expect("missing builtin")
            .1;
        let schema_name = builtin.schema();
        let schema_id = self
            .ambient_schemas_by_name
            .get(schema_name)
            .unwrap_or_else(|| panic!("unknown ambient schema: {schema_name}"));
        let name = QualifiedItemName {
            qualifiers: ItemQualifiers {
                database_spec: ResolvedDatabaseSpecifier::Ambient,
                schema_spec: SchemaSpecifier::Id(*schema_id),
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
                    item_id,
                    log.oid,
                    name.clone(),
                    CatalogItem::Log(Log {
                        variant: log.variant,
                        global_id,
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
                    item_id,
                    table.oid,
                    name.clone(),
                    CatalogItem::Table(Table {
                        create_sql: None,
                        desc: VersionedRelationDesc::new(table.desc.clone()),
                        collections: [(RelationVersion::root(), global_id)].into_iter().collect(),
                        conn_id: None,
                        resolved_ids: ResolvedIds::empty(),
                        custom_logical_compaction_window: table.is_retained_metrics_object.then(
                            || {
                                self.system_config()
                                    .metrics_retention()
                                    .try_into()
                                    .expect("invalid metrics retention")
                            },
                        ),
                        is_retained_metrics_object: table.is_retained_metrics_object,
                        data_source: TableDataSource::TableWrites {
                            defaults: vec![Expr::null(); table.desc.arity()],
                        },
                    }),
                    MZ_SYSTEM_ROLE_ID,
                    PrivilegeMap::from_mz_acl_items(acl_items),
                );
            }
            Builtin::Index(index) => {
                let custom_logical_compaction_window =
                    index.is_retained_metrics_object.then(|| {
                        self.system_config()
                            .metrics_retention()
                            .try_into()
                            .expect("invalid metrics retention")
                    });
                // Indexes can't be versioned.
                let versions = BTreeMap::new();

                let item = self
                    .parse_item(
                        global_id,
                        &index.create_sql(),
                        &versions,
                        None,
                        index.is_retained_metrics_object,
                        custom_logical_compaction_window,
                        local_expression_cache,
                        None,
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
                let CatalogItem::Index(_) = item else {
                    panic!(
                        "internal error: builtin index {}'s SQL does not begin with \"CREATE INDEX\".",
                        index.name
                    );
                };

                self.insert_item(
                    item_id,
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
                    item_type.details.array_id = Some(item_id);
                }

                // Assert that no built-in types are record types so that we don't
                // need to bother to build a description. Only record types need
                // descriptions.
                let desc = None;
                assert!(!matches!(typ.details.typ, CatalogType::Record { .. }));
                let schema_id = self.resolve_system_schema(typ.schema);

                self.insert_item(
                    item_id,
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
                        global_id,
                        details: typ.details.clone(),
                        desc,
                        resolved_ids: ResolvedIds::empty(),
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
                    item_id,
                    oid,
                    name.clone(),
                    CatalogItem::Func(Func {
                        inner: func.inner,
                        global_id,
                    }),
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
                    item_id,
                    coll.oid,
                    name.clone(),
                    CatalogItem::Source(Source {
                        create_sql: None,
                        data_source: DataSourceDesc::Introspection(coll.data_source),
                        desc: coll.desc.clone(),
                        global_id,
                        timeline: Timeline::EpochMilliseconds,
                        resolved_ids: ResolvedIds::empty(),
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
            Builtin::ContinualTask(ct) => {
                let mut acl_items = vec![rbac::owner_privilege(
                    mz_sql::catalog::ObjectType::Source,
                    MZ_SYSTEM_ROLE_ID,
                )];
                acl_items.extend_from_slice(&ct.access);
                // Continual Tasks can't be versioned.
                let versions = BTreeMap::new();

                let item = self
                    .parse_item(
                        global_id,
                        &ct.create_sql(),
                        &versions,
                        None,
                        false,
                        None,
                        local_expression_cache,
                        None,
                    )
                    .unwrap_or_else(|e| {
                        panic!(
                            "internal error: failed to load bootstrap continual task:\n\
                                    {}\n\
                                    error:\n\
                                    {:?}\n\n\
                                    make sure that the schema name is specified in the builtin continual task's create sql statement.",
                            ct.name, e
                        )
                    });
                let CatalogItem::ContinualTask(_) = &item else {
                    panic!(
                        "internal error: builtin continual task {}'s SQL does not begin with \"CREATE CONTINUAL TASK\".",
                        ct.name
                    );
                };

                self.insert_item(
                    item_id,
                    ct.oid,
                    name,
                    item,
                    MZ_SYSTEM_ROLE_ID,
                    PrivilegeMap::from_mz_acl_items(acl_items),
                );
            }
            Builtin::Connection(connection) => {
                // Connections can't be versioned.
                let versions = BTreeMap::new();
                let mut item = self
                    .parse_item(
                        global_id,
                        connection.sql,
                        &versions,
                        None,
                        false,
                        None,
                        local_expression_cache,
                        None,
                    )
                    .unwrap_or_else(|e| {
                        panic!(
                            "internal error: failed to load bootstrap connection:\n\
                                    {}\n\
                                    error:\n\
                                    {:?}\n\n\
                                    make sure that the schema name is specified in the builtin connection's create sql statement.",
                            connection.name, e
                        )
                    });
                let CatalogItem::Connection(_) = &mut item else {
                    panic!(
                        "internal error: builtin connection {}'s SQL does not begin with \"CREATE CONNECTION\".",
                        connection.name
                    );
                };

                let mut acl_items = vec![rbac::owner_privilege(
                    mz_sql::catalog::ObjectType::Connection,
                    connection.owner_id.clone(),
                )];
                acl_items.extend_from_slice(connection.access);

                self.insert_item(
                    item_id,
                    connection.oid,
                    name.clone(),
                    item,
                    connection.owner_id.clone(),
                    PrivilegeMap::from_mz_acl_items(acl_items),
                );
            }
        }
    }

    #[instrument(level = "debug")]
    fn apply_temporary_item_update(
        &mut self,
        TemporaryItem {
            id,
            oid,
            name,
            item,
            owner_id,
            privileges,
        }: TemporaryItem,
        diff: StateDiff,
        retractions: &mut InProgressRetractions,
    ) {
        match diff {
            StateDiff::Addition => {
                let entry = match retractions.temp_items.remove(&id) {
                    Some(mut retraction) => {
                        assert_eq!(retraction.id, id);
                        retraction.item = item;
                        retraction.id = id;
                        retraction.oid = oid;
                        retraction.name = name;
                        retraction.owner_id = owner_id;
                        retraction.privileges = privileges;
                        retraction
                    }
                    None => CatalogEntry {
                        item,
                        referenced_by: Vec::new(),
                        used_by: Vec::new(),
                        id,
                        oid,
                        name,
                        owner_id,
                        privileges,
                    },
                };
                self.insert_entry(entry);
            }
            StateDiff::Retraction => {
                let entry = self.drop_item(id);
                retractions.temp_items.insert(id, entry);
            }
        }
    }

    #[instrument(level = "debug")]
    fn apply_item_update(
        &mut self,
        item: mz_catalog::durable::Item,
        diff: StateDiff,
        retractions: &mut InProgressRetractions,
        local_expression_cache: &mut LocalExpressionCache,
    ) -> Result<(), CatalogError> {
        match diff {
            StateDiff::Addition => {
                let key = item.key();
                let mz_catalog::durable::Item {
                    id,
                    oid,
                    global_id,
                    schema_id,
                    name,
                    create_sql,
                    owner_id,
                    privileges,
                    extra_versions,
                } = item;
                let schema = self.find_non_temp_schema(&schema_id);
                let name = QualifiedItemName {
                    qualifiers: ItemQualifiers {
                        database_spec: schema.database().clone(),
                        schema_spec: schema.id().clone(),
                    },
                    item: name.clone(),
                };
                let entry = match retractions.items.remove(&key) {
                    Some(mut retraction) => {
                        assert_eq!(retraction.id, item.id);
                        // We only reparse the SQL if it's changed. Otherwise, we use the existing
                        // item. This is a performance optimization and not needed for correctness.
                        // This makes it difficult to use the `UpdateFrom` trait, but the structure
                        // is still the same as the trait.
                        if retraction.create_sql() != create_sql {
                            let item = self
                                .deserialize_item(
                                    global_id,
                                    &create_sql,
                                    &extra_versions,
                                    local_expression_cache,
                                    Some(retraction.item),
                                )
                                .unwrap_or_else(|e| {
                                    panic!("{e:?}: invalid persisted SQL: {create_sql}")
                                });
                            retraction.item = item;
                        }
                        retraction.id = id;
                        retraction.oid = oid;
                        retraction.name = name;
                        retraction.owner_id = owner_id;
                        retraction.privileges = PrivilegeMap::from_mz_acl_items(privileges);

                        retraction
                    }
                    None => {
                        let catalog_item = self
                            .deserialize_item(
                                global_id,
                                &create_sql,
                                &extra_versions,
                                local_expression_cache,
                                None,
                            )
                            .unwrap_or_else(|e| {
                                panic!("{e:?}: invalid persisted SQL: {create_sql}")
                            });
                        CatalogEntry {
                            item: catalog_item,
                            referenced_by: Vec::new(),
                            used_by: Vec::new(),
                            id,
                            oid,
                            name,
                            owner_id,
                            privileges: PrivilegeMap::from_mz_acl_items(privileges),
                        }
                    }
                };

                self.insert_entry(entry);
            }
            StateDiff::Retraction => {
                let entry = self.drop_item(item.id);
                let key = item.into_key_value().0;
                retractions.items.insert(key, entry);
            }
        }
        Ok(())
    }

    #[instrument(level = "debug")]
    fn apply_comment_update(
        &mut self,
        comment: mz_catalog::durable::Comment,
        diff: StateDiff,
        _retractions: &mut InProgressRetractions,
    ) {
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
    fn apply_source_references_update(
        &mut self,
        source_references: mz_catalog::durable::SourceReferences,
        diff: StateDiff,
        _retractions: &mut InProgressRetractions,
    ) {
        match diff {
            StateDiff::Addition => {
                let prev = self
                    .source_references
                    .insert(source_references.source_id, source_references.into());
                assert!(
                    prev.is_none(),
                    "values must be explicitly retracted before inserting a new value: {prev:?}"
                );
            }
            StateDiff::Retraction => {
                let prev = self.source_references.remove(&source_references.source_id);
                assert!(
                    prev.is_some(),
                    "retraction for a non-existent existing value: {source_references:?}"
                );
            }
        }
    }

    #[instrument(level = "debug")]
    fn apply_storage_collection_metadata_update(
        &mut self,
        storage_collection_metadata: mz_catalog::durable::StorageCollectionMetadata,
        diff: StateDiff,
        _retractions: &mut InProgressRetractions,
    ) {
        apply_inverted_lookup(
            &mut self.storage_metadata.collection_metadata,
            &storage_collection_metadata.id,
            storage_collection_metadata.shard,
            diff,
        );
    }

    #[instrument(level = "debug")]
    fn apply_unfinalized_shard_update(
        &mut self,
        unfinalized_shard: mz_catalog::durable::UnfinalizedShard,
        diff: StateDiff,
        _retractions: &mut InProgressRetractions,
    ) {
        match diff {
            StateDiff::Addition => {
                let newly_inserted = self
                    .storage_metadata
                    .unfinalized_shards
                    .insert(unfinalized_shard.shard);
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
        for StateUpdate { kind, ts: _, diff } in updates {
            let builtin_table_update = self.generate_builtin_table_update(kind, diff);
            let builtin_table_update = self.resolve_builtin_table_updates(builtin_table_update);
            builtin_table_updates.extend(builtin_table_update);
        }
        builtin_table_updates
    }

    /// Generate a list of `BuiltinTableUpdate`s that correspond to a single update made to the
    /// durable catalog.
    #[instrument(level = "debug")]
    pub(crate) fn generate_builtin_table_update(
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
                self.pack_item_update(introspection_source_index.item_id, diff)
            }
            StateUpdateKind::ClusterReplica(cluster_replica) => self.pack_cluster_replica_update(
                cluster_replica.cluster_id,
                &cluster_replica.name,
                diff,
            ),
            StateUpdateKind::SystemObjectMapping(system_object_mapping) => {
                // Runtime-alterable system objects have real entries in the
                // items collection and so get handled through the normal
                // `StateUpdateKind::Item`.`
                if !system_object_mapping.unique_identifier.runtime_alterable() {
                    self.pack_item_update(system_object_mapping.unique_identifier.catalog_id, diff)
                } else {
                    vec![]
                }
            }
            StateUpdateKind::TemporaryItem(item) => self.pack_item_update(item.id, diff),
            StateUpdateKind::Item(item) => self.pack_item_update(item.id, diff),
            StateUpdateKind::Comment(comment) => vec![self.pack_comment_update(
                comment.object_id,
                comment.sub_component,
                &comment.comment,
                diff,
            )],
            StateUpdateKind::SourceReferences(source_references) => {
                self.pack_source_references_update(&source_references, diff)
            }
            StateUpdateKind::AuditLog(audit_log) => {
                vec![
                    self.pack_audit_log_update(&audit_log.event, diff)
                        .expect("could not pack audit log update"),
                ]
            }
            StateUpdateKind::NetworkPolicy(policy) => self
                .pack_network_policy_update(&policy.id, diff)
                .expect("could not pack audit log update"),
            StateUpdateKind::StorageCollectionMetadata(_)
            | StateUpdateKind::UnfinalizedShard(_)
            | StateUpdateKind::RoleAuth(_) => Vec::new(),
        }
    }

    fn get_entry_mut(&mut self, id: &CatalogItemId) -> &mut CatalogEntry {
        self.entry_by_id
            .get_mut(id)
            .unwrap_or_else(|| panic!("catalog out of sync, missing id {id}"))
    }

    fn get_schema_mut(
        &mut self,
        database_spec: &ResolvedDatabaseSpecifier,
        schema_spec: &SchemaSpecifier,
        conn_id: &ConnectionId,
    ) -> &mut Schema {
        // Keep in sync with `get_schemas`
        match (database_spec, schema_spec) {
            (ResolvedDatabaseSpecifier::Ambient, SchemaSpecifier::Temporary) => self
                .temporary_schemas
                .get_mut(conn_id)
                .expect("catalog out of sync"),
            (ResolvedDatabaseSpecifier::Ambient, SchemaSpecifier::Id(id)) => self
                .ambient_schemas_by_id
                .get_mut(id)
                .expect("catalog out of sync"),
            (ResolvedDatabaseSpecifier::Id(database_id), SchemaSpecifier::Id(schema_id)) => self
                .database_by_id
                .get_mut(database_id)
                .expect("catalog out of sync")
                .schemas_by_id
                .get_mut(schema_id)
                .expect("catalog out of sync"),
            (ResolvedDatabaseSpecifier::Id(_), SchemaSpecifier::Temporary) => {
                unreachable!("temporary schemas are in the ambient database")
            }
        }
    }

    /// Install builtin views to the catalog. This is its own function so that views can be
    /// optimized in parallel.
    ///
    /// The implementation is similar to `apply_updates_for_bootstrap` and determines dependency
    /// problems by sniffing out specific errors and then retrying once those dependencies are
    /// complete. This doesn't work for everything (casts, function implementations) so we also need
    /// to have a bucket for everything at the end. Additionally, because this executes in parellel,
    /// we must maintain a completed set otherwise races could result in orphaned views languishing
    /// in awaiting with nothing retriggering the attempt.
    #[instrument(name = "catalog::parse_views")]
    async fn parse_builtin_views(
        state: &mut CatalogState,
        builtin_views: Vec<(&'static BuiltinView, CatalogItemId, GlobalId)>,
        retractions: &mut InProgressRetractions,
        local_expression_cache: &mut LocalExpressionCache,
    ) -> Vec<BuiltinTableUpdate<&'static BuiltinTable>> {
        let mut builtin_table_updates = Vec::with_capacity(builtin_views.len());
        let (updates, additions): (Vec<_>, Vec<_>) =
            builtin_views
                .into_iter()
                .partition_map(|(view, item_id, gid)| {
                    match retractions.system_object_mappings.remove(&item_id) {
                        Some(entry) => Either::Left(entry),
                        None => Either::Right((view, item_id, gid)),
                    }
                });

        for entry in updates {
            // This implies that we updated the fingerprint for some builtin view. The retraction
            // was parsed, planned, and optimized using the compiled in definition, not the
            // definition from a previous version. So we can just stick the old entry back into the
            // catalog.
            let item_id = entry.id();
            state.insert_entry(entry);
            builtin_table_updates.extend(state.pack_item_update(item_id, Diff::ONE));
        }

        let mut handles = Vec::new();
        let mut awaiting_id_dependencies: BTreeMap<CatalogItemId, Vec<CatalogItemId>> =
            BTreeMap::new();
        let mut awaiting_name_dependencies: BTreeMap<String, Vec<CatalogItemId>> = BTreeMap::new();
        // Some errors are due to the implementation of casts or SQL functions that depend on some
        // view. Instead of figuring out the exact view dependency, delay these until the end.
        let mut awaiting_all = Vec::new();
        // Completed views, needed to avoid race conditions.
        let mut completed_ids: BTreeSet<CatalogItemId> = BTreeSet::new();
        let mut completed_names: BTreeSet<String> = BTreeSet::new();

        // Avoid some reference lifetime issues by not passing `builtin` into the spawned task.
        let mut views: BTreeMap<CatalogItemId, (&BuiltinView, GlobalId)> = additions
            .into_iter()
            .map(|(view, item_id, gid)| (item_id, (view, gid)))
            .collect();
        let item_ids: Vec<_> = views.keys().copied().collect();

        let mut ready: VecDeque<CatalogItemId> = views.keys().cloned().collect();
        while !handles.is_empty() || !ready.is_empty() || !awaiting_all.is_empty() {
            if handles.is_empty() && ready.is_empty() {
                // Enqueue the views that were waiting for all the others.
                ready.extend(awaiting_all.drain(..));
            }

            // Spawn tasks for all ready views.
            if !ready.is_empty() {
                let spawn_state = Arc::new(state.clone());
                while let Some(id) = ready.pop_front() {
                    let (view, global_id) = views.get(&id).expect("must exist");
                    let global_id = *global_id;
                    let create_sql = view.create_sql();
                    // Views can't be versioned.
                    let versions = BTreeMap::new();

                    let span = info_span!(parent: None, "parse builtin view", name = view.name);
                    OpenTelemetryContext::obtain().attach_as_parent_to(&span);
                    let task_state = Arc::clone(&spawn_state);
                    let cached_expr = local_expression_cache.remove_cached_expression(&global_id);
                    let handle = mz_ore::task::spawn(
                        || "parse view",
                        async move {
                            let res = task_state.parse_item_inner(
                                global_id,
                                &create_sql,
                                &versions,
                                None,
                                false,
                                None,
                                cached_expr,
                                None,
                            );
                            (id, global_id, res)
                        }
                        .instrument(span),
                    );
                    handles.push(handle);
                }
            }

            // Wait for a view to be ready.
            let (handle, _idx, remaining) = future::select_all(handles).await;
            handles = remaining;
            let (id, global_id, res) = handle.expect("must join");
            let mut insert_cached_expr = |cached_expr| {
                if let Some(cached_expr) = cached_expr {
                    local_expression_cache.insert_cached_expression(global_id, cached_expr);
                }
            };
            match res {
                Ok((item, uncached_expr)) => {
                    if let Some((uncached_expr, optimizer_features)) = uncached_expr {
                        local_expression_cache.insert_uncached_expression(
                            global_id,
                            uncached_expr,
                            optimizer_features,
                        );
                    }
                    // Add item to catalog.
                    let (view, _gid) = views.remove(&id).expect("must exist");
                    let schema_id = state
                        .ambient_schemas_by_name
                        .get(view.schema)
                        .unwrap_or_else(|| panic!("unknown ambient schema: {}", view.schema));
                    let qname = QualifiedItemName {
                        qualifiers: ItemQualifiers {
                            database_spec: ResolvedDatabaseSpecifier::Ambient,
                            schema_spec: SchemaSpecifier::Id(*schema_id),
                        },
                        item: view.name.into(),
                    };
                    let mut acl_items = vec![rbac::owner_privilege(
                        mz_sql::catalog::ObjectType::View,
                        MZ_SYSTEM_ROLE_ID,
                    )];
                    acl_items.extend_from_slice(&view.access);

                    state.insert_item(
                        id,
                        view.oid,
                        qname,
                        item,
                        MZ_SYSTEM_ROLE_ID,
                        PrivilegeMap::from_mz_acl_items(acl_items),
                    );

                    // Enqueue any items waiting on this dependency.
                    let mut resolved_dependent_items = Vec::new();
                    if let Some(dependent_items) = awaiting_id_dependencies.remove(&id) {
                        resolved_dependent_items.extend(dependent_items);
                    }
                    let entry = state.get_entry(&id);
                    let full_name = state.resolve_full_name(entry.name(), None).to_string();
                    if let Some(dependent_items) = awaiting_name_dependencies.remove(&full_name) {
                        resolved_dependent_items.extend(dependent_items);
                    }
                    ready.extend(resolved_dependent_items);

                    completed_ids.insert(id);
                    completed_names.insert(full_name);
                }
                // If we were missing a dependency, wait for it to be added.
                Err((
                    AdapterError::PlanError(plan::PlanError::InvalidId(missing_dep)),
                    cached_expr,
                )) => {
                    insert_cached_expr(cached_expr);
                    if completed_ids.contains(&missing_dep) {
                        ready.push_back(id);
                    } else {
                        awaiting_id_dependencies
                            .entry(missing_dep)
                            .or_default()
                            .push(id);
                    }
                }
                // If we were missing a dependency, wait for it to be added.
                Err((
                    AdapterError::PlanError(plan::PlanError::Catalog(
                        SqlCatalogError::UnknownItem(missing_dep),
                    )),
                    cached_expr,
                )) => {
                    insert_cached_expr(cached_expr);
                    match CatalogItemId::from_str(&missing_dep) {
                        Ok(missing_dep) => {
                            if completed_ids.contains(&missing_dep) {
                                ready.push_back(id);
                            } else {
                                awaiting_id_dependencies
                                    .entry(missing_dep)
                                    .or_default()
                                    .push(id);
                            }
                        }
                        Err(_) => {
                            if completed_names.contains(&missing_dep) {
                                ready.push_back(id);
                            } else {
                                awaiting_name_dependencies
                                    .entry(missing_dep)
                                    .or_default()
                                    .push(id);
                            }
                        }
                    }
                }
                Err((
                    AdapterError::PlanError(plan::PlanError::InvalidCast { .. }),
                    cached_expr,
                )) => {
                    insert_cached_expr(cached_expr);
                    awaiting_all.push(id);
                }
                Err((e, _)) => {
                    let (bad_view, _gid) = views.get(&id).expect("must exist");
                    panic!(
                        "internal error: failed to load bootstrap view:\n\
                            {name}\n\
                            error:\n\
                            {e:?}\n\n\
                            Make sure that the schema name is specified in the builtin view's create sql statement.
                            ",
                        name = bad_view.name,
                    )
                }
            }
        }

        assert!(awaiting_id_dependencies.is_empty());
        assert!(
            awaiting_name_dependencies.is_empty(),
            "awaiting_name_dependencies: {awaiting_name_dependencies:?}"
        );
        assert!(awaiting_all.is_empty());
        assert!(views.is_empty());

        // Generate a builtin table update for all the new views.
        builtin_table_updates.extend(
            item_ids
                .into_iter()
                .flat_map(|id| state.pack_item_update(id, Diff::ONE)),
        );

        builtin_table_updates
    }

    /// Associates a name, `CatalogItemId`, and entry.
    fn insert_entry(&mut self, entry: CatalogEntry) {
        if !entry.id.is_system() {
            if let Some(cluster_id) = entry.item.cluster_id() {
                self.clusters_by_id
                    .get_mut(&cluster_id)
                    .expect("catalog out of sync")
                    .bound_objects
                    .insert(entry.id);
            };
        }

        for u in entry.references().items() {
            match self.entry_by_id.get_mut(u) {
                Some(metadata) => metadata.referenced_by.push(entry.id()),
                None => panic!(
                    "Catalog: missing dependent catalog item {} while installing {}",
                    &u,
                    self.resolve_full_name(entry.name(), entry.conn_id())
                ),
            }
        }
        for u in entry.uses() {
            // Ignore self for self-referential tasks (e.g. Continual Tasks), if
            // present.
            if u == entry.id() {
                continue;
            }
            match self.entry_by_id.get_mut(&u) {
                Some(metadata) => metadata.used_by.push(entry.id()),
                None => panic!(
                    "Catalog: missing dependent catalog item {} while installing {}",
                    &u,
                    self.resolve_full_name(entry.name(), entry.conn_id())
                ),
            }
        }
        for gid in entry.item.global_ids() {
            self.entry_by_global_id.insert(gid, entry.id());
        }
        let conn_id = entry.item().conn_id().unwrap_or(&SYSTEM_CONN_ID);
        let schema = self.get_schema_mut(
            &entry.name().qualifiers.database_spec,
            &entry.name().qualifiers.schema_spec,
            conn_id,
        );

        let prev_id = match entry.item() {
            CatalogItem::Func(_) => schema
                .functions
                .insert(entry.name().item.clone(), entry.id()),
            CatalogItem::Type(_) => schema.types.insert(entry.name().item.clone(), entry.id()),
            _ => schema.items.insert(entry.name().item.clone(), entry.id()),
        };

        assert!(
            prev_id.is_none(),
            "builtin name collision on {:?}",
            entry.name().item.clone()
        );

        self.entry_by_id.insert(entry.id(), entry.clone());
    }

    /// Associates a name, [`CatalogItemId`], and entry.
    fn insert_item(
        &mut self,
        id: CatalogItemId,
        oid: u32,
        name: QualifiedItemName,
        item: CatalogItem,
        owner_id: RoleId,
        privileges: PrivilegeMap,
    ) {
        let entry = CatalogEntry {
            item,
            name,
            id,
            oid,
            used_by: Vec::new(),
            referenced_by: Vec::new(),
            owner_id,
            privileges,
        };

        self.insert_entry(entry);
    }

    #[mz_ore::instrument(level = "trace")]
    fn drop_item(&mut self, id: CatalogItemId) -> CatalogEntry {
        let metadata = self.entry_by_id.remove(&id).expect("catalog out of sync");
        for u in metadata.references().items() {
            if let Some(dep_metadata) = self.entry_by_id.get_mut(u) {
                dep_metadata.referenced_by.retain(|u| *u != metadata.id())
            }
        }
        for u in metadata.uses() {
            if let Some(dep_metadata) = self.entry_by_id.get_mut(&u) {
                dep_metadata.used_by.retain(|u| *u != metadata.id())
            }
        }
        for gid in metadata.global_ids() {
            self.entry_by_global_id.remove(&gid);
        }

        let conn_id = metadata.item().conn_id().unwrap_or(&SYSTEM_CONN_ID);
        let schema = self.get_schema_mut(
            &metadata.name().qualifiers.database_spec,
            &metadata.name().qualifiers.schema_spec,
            conn_id,
        );
        if metadata.item_type() == CatalogItemType::Type {
            schema
                .types
                .remove(&metadata.name().item)
                .expect("catalog out of sync");
        } else {
            // Functions would need special handling, but we don't yet support
            // dropping functions.
            assert_ne!(metadata.item_type(), CatalogItemType::Func);

            schema
                .items
                .remove(&metadata.name().item)
                .expect("catalog out of sync");
        };

        if !id.is_system() {
            if let Some(cluster_id) = metadata.item().cluster_id() {
                assert!(
                    self.clusters_by_id
                        .get_mut(&cluster_id)
                        .expect("catalog out of sync")
                        .bound_objects
                        .remove(&id),
                    "catalog out of sync"
                );
            }
        }

        metadata
    }

    fn insert_introspection_source_index(
        &mut self,
        cluster_id: ClusterId,
        log: &'static BuiltinLog,
        item_id: CatalogItemId,
        global_id: GlobalId,
        oid: u32,
    ) {
        let (index_name, index) =
            self.create_introspection_source_index(cluster_id, log, global_id);
        self.insert_item(
            item_id,
            oid,
            index_name,
            index,
            MZ_SYSTEM_ROLE_ID,
            PrivilegeMap::default(),
        );
    }

    fn create_introspection_source_index(
        &self,
        cluster_id: ClusterId,
        log: &'static BuiltinLog,
        global_id: GlobalId,
    ) -> (QualifiedItemName, CatalogItem) {
        let source_name = FullItemName {
            database: RawDatabaseSpecifier::Ambient,
            schema: log.schema.into(),
            item: log.name.into(),
        };
        let index_name = format!("{}_{}_primary_idx", log.name, cluster_id);
        let mut index_name = QualifiedItemName {
            qualifiers: ItemQualifiers {
                database_spec: ResolvedDatabaseSpecifier::Ambient,
                schema_spec: SchemaSpecifier::Id(self.get_mz_introspection_schema_id()),
            },
            item: index_name.clone(),
        };
        index_name = self.find_available_name(index_name, &SYSTEM_CONN_ID);
        let index_item_name = index_name.item.clone();
        let (log_item_id, log_global_id) = self.resolve_builtin_log(log);
        let index = CatalogItem::Index(Index {
            global_id,
            on: log_global_id,
            keys: log
                .variant
                .index_by()
                .into_iter()
                .map(MirScalarExpr::column)
                .collect(),
            create_sql: index_sql(
                index_item_name,
                cluster_id,
                source_name,
                &log.variant.desc(),
                &log.variant.index_by(),
            ),
            conn_id: None,
            resolved_ids: [(log_item_id, log_global_id)].into_iter().collect(),
            cluster_id,
            is_retained_metrics_object: false,
            custom_logical_compaction_window: None,
        });
        (index_name, index)
    }

    /// Insert system configuration `name` with `value`.
    ///
    /// Return a `bool` value indicating whether the configuration was modified
    /// by the call.
    fn insert_system_configuration(&mut self, name: &str, value: VarInput) -> Result<bool, Error> {
        Ok(self.system_configuration.set(name, value)?)
    }

    /// Reset system configuration `name`.
    ///
    /// Return a `bool` value indicating whether the configuration was modified
    /// by the call.
    fn remove_system_configuration(&mut self, name: &str) -> Result<bool, Error> {
        Ok(self.system_configuration.reset(name)?)
    }
}

/// Sort [`StateUpdate`]s in timestamp then dependency order
fn sort_updates(mut updates: Vec<StateUpdate>) -> Vec<StateUpdate> {
    let mut sorted_updates = Vec::with_capacity(updates.len());

    updates.sort_by_key(|update| update.ts);
    for (_, updates) in &updates.into_iter().chunk_by(|update| update.ts) {
        let sorted_ts_updates = sort_updates_inner(updates.collect());
        sorted_updates.extend(sorted_ts_updates);
    }

    sorted_updates
}

/// Sort [`StateUpdate`]s in dependency order for a single timestamp.
fn sort_updates_inner(updates: Vec<StateUpdate>) -> Vec<StateUpdate> {
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

    soft_assert_no_log!(
        updates.iter().map(|update| update.ts).all_equal(),
        "all timestamps should be equal: {updates:?}"
    );

    // Partition updates by type so that we can weave different update types into the right spots.
    let mut pre_cluster_retractions = Vec::new();
    let mut pre_cluster_additions = Vec::new();
    let mut cluster_retractions = Vec::new();
    let mut cluster_additions = Vec::new();
    let mut builtin_item_updates = Vec::new();
    let mut item_retractions = Vec::new();
    let mut item_additions = Vec::new();
    let mut temp_item_retractions = Vec::new();
    let mut temp_item_additions = Vec::new();
    let mut post_item_retractions = Vec::new();
    let mut post_item_additions = Vec::new();
    for update in updates {
        let diff = update.diff.clone();
        match update.kind {
            StateUpdateKind::Role(_)
            | StateUpdateKind::RoleAuth(_)
            | StateUpdateKind::Database(_)
            | StateUpdateKind::Schema(_)
            | StateUpdateKind::DefaultPrivilege(_)
            | StateUpdateKind::SystemPrivilege(_)
            | StateUpdateKind::SystemConfiguration(_)
            | StateUpdateKind::NetworkPolicy(_) => push_update(
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
                builtin_item_updates.push((system_object_mapping, update.ts, update.diff))
            }
            StateUpdateKind::TemporaryItem(item) => push_update(
                (item, update.ts, update.diff),
                diff,
                &mut temp_item_retractions,
                &mut temp_item_additions,
            ),
            StateUpdateKind::Item(item) => push_update(
                (item, update.ts, update.diff),
                diff,
                &mut item_retractions,
                &mut item_additions,
            ),
            StateUpdateKind::Comment(_)
            | StateUpdateKind::SourceReferences(_)
            | StateUpdateKind::AuditLog(_)
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
        .map(|(system_object_mapping, ts, diff)| {
            let idx = BUILTIN_LOOKUP
                .get(&system_object_mapping.description)
                .expect("missing builtin")
                .0;
            (idx, system_object_mapping, ts, diff)
        })
        .sorted_by_key(|(idx, _, _, _)| *idx)
        .map(|(_, system_object_mapping, ts, diff)| (system_object_mapping, ts, diff));

    // Further partition builtin item updates.
    let mut other_builtin_retractions = Vec::new();
    let mut other_builtin_additions = Vec::new();
    let mut builtin_index_retractions = Vec::new();
    let mut builtin_index_additions = Vec::new();
    for (builtin_item_update, ts, diff) in builtin_item_updates {
        match &builtin_item_update.description.object_type {
            CatalogItemType::Index | CatalogItemType::ContinualTask => push_update(
                StateUpdate {
                    kind: StateUpdateKind::SystemObjectMapping(builtin_item_update),
                    ts,
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
                    ts,
                    diff,
                },
                diff,
                &mut other_builtin_retractions,
                &mut other_builtin_additions,
            ),
        }
    }

    type ConnectionKey = (mz_catalog::durable::Item, Timestamp, StateDiff);

    /// Topologically sort `CONNECTION` items by dependencies.
    ///
    /// `CONNECTION`s can depend on one another, e.g. a `KAFKA CONNECTION` contains
    /// a list of `BROKERS` and these broker definitions can themselves reference other
    /// connections. This `BROKERS` list can also be `ALTER`ed and thus it's possible
    /// for a connection to depend on another with an ID greater than its own.
    fn sort_connections(connections: &mut Vec<ConnectionKey>) {
        let mut topo: BTreeMap<CatalogItemId, BTreeSet<CatalogItemId>> = BTreeMap::default();
        let mut connection_in_degree: BTreeMap<CatalogItemId, (ConnectionKey, usize)> = connections
            .iter()
            .map(|conn| (conn.0.id, (conn.clone(), 0)))
            .collect();
        let existing_connections: BTreeSet<_> = connections.iter().map(|item| item.0.id).collect();

        // Initialize our set of topological sort.
        tracing::info!(?connections, "sorting connections");
        for (connection, _, _) in connections.drain(..) {
            let statement = mz_sql::parse::parse(&connection.create_sql)
                .expect("valid CONNECTION create_sql")
                .into_element()
                .ast;
            let mut dependencies = mz_sql::names::dependencies(&statement)
                .expect("failed to find dependencies of CONNECTION");
            // Be defensive and remove any possible self references.
            dependencies.remove(&connection.id);
            // It's possible we're applying updates to a connection where the
            // dependency already exists and thus it's not in `connections`.
            dependencies.retain(|dep| existing_connections.contains(dep));

            for dep in dependencies.iter() {
                connection_in_degree
                    .entry(*dep)
                    .and_modify(|(_, in_degree)| *in_degree += 1);
            }

            // Be defensive and ensure we're not clobbering any items.
            assert_none!(topo.insert(connection.id, dependencies));
        }

        let mut to_sort: VecDeque<ConnectionKey> = VecDeque::new();
        for (conn, in_degree) in connection_in_degree.values().cloned() {
            if in_degree == 0 {
                to_sort.push_back(conn);
            }
        }

        // Do a topological sort, pushing back into the provided Vec.
        while !to_sort.is_empty() {
            let Some(current) = to_sort.pop_front() else {
                panic!(
                    "Programming error, impossible situation nothing in queue after checking it is not empty."
                )
            };
            let Some(outgoing_conns) = topo.get(&current.0.id) else {
                panic!("Programming error, could not find dependencies for connection.")
            };

            for outgoing_conn in outgoing_conns.iter() {
                connection_in_degree
                    .entry(*outgoing_conn)
                    .and_modify(|(_, in_degree)| *in_degree -= 1);
            }

            connections.push(current);
        }
    }

    /// Sort item updates by dependency.
    ///
    /// First we group items into groups that are totally ordered by dependency. For example, when
    /// sorting all items by dependency we know that all tables can come after all sources, because
    /// a source can never depend on a table. Within these groups, the ID order matches the
    /// dependency order.
    ///
    /// It used to be the case that the ID order of ALL items matched the dependency order. However,
    /// certain migrations shuffled item IDs around s.t. this was no longer true. A much better
    /// approach would be to investigate each item, discover their exact dependencies, and then
    /// perform a topological sort. This is non-trivial because we only have the CREATE SQL of each
    /// item here. Within the SQL the dependent items are sometimes referred to by ID and sometimes
    /// referred to by name.
    ///
    /// The logic of this function should match [`sort_temp_item_updates`].
    fn sort_item_updates(
        item_updates: Vec<(mz_catalog::durable::Item, Timestamp, StateDiff)>,
    ) -> VecDeque<(mz_catalog::durable::Item, Timestamp, StateDiff)> {
        // Partition items into groups s.t. each item in one group has a predefined order with all
        // items in other groups. For example, all sinks are ordered greater than all tables.
        let mut types = Vec::new();
        // N.B. Functions can depend on system tables, but not user tables.
        // TODO(udf): This will change when UDFs are supported.
        let mut funcs = Vec::new();
        let mut secrets = Vec::new();
        let mut connections = Vec::new();
        let mut sources = Vec::new();
        let mut tables = Vec::new();
        let mut derived_items = Vec::new();
        let mut sinks = Vec::new();
        let mut continual_tasks = Vec::new();

        for update in item_updates {
            match update.0.item_type() {
                CatalogItemType::Type => types.push(update),
                CatalogItemType::Func => funcs.push(update),
                CatalogItemType::Secret => secrets.push(update),
                CatalogItemType::Connection => connections.push(update),
                CatalogItemType::Source => sources.push(update),
                CatalogItemType::Table => tables.push(update),
                CatalogItemType::View
                | CatalogItemType::MaterializedView
                | CatalogItemType::Index => derived_items.push(update),
                CatalogItemType::Sink => sinks.push(update),
                CatalogItemType::ContinualTask => continual_tasks.push(update),
            }
        }

        // Within each group, sort by ID.
        for group in [
            &mut types,
            &mut funcs,
            &mut secrets,
            &mut sources,
            &mut tables,
            &mut derived_items,
            &mut sinks,
            &mut continual_tasks,
        ] {
            group.sort_by_key(|(item, _, _)| item.id);
        }

        // HACK(parkmycar): Connections are special and can depend on one another. Additionally
        // connections can be `ALTER`ed and thus a `CONNECTION` can depend on another whose ID
        // is greater than its own.
        sort_connections(&mut connections);

        iter::empty()
            .chain(types)
            .chain(funcs)
            .chain(secrets)
            .chain(connections)
            .chain(sources)
            .chain(tables)
            .chain(derived_items)
            .chain(sinks)
            .chain(continual_tasks)
            .collect()
    }

    let item_retractions = sort_item_updates(item_retractions);
    let item_additions = sort_item_updates(item_additions);

    /// Sort temporary item updates by dependency.
    ///
    /// The logic of this function should match [`sort_item_updates`].
    fn sort_temp_item_updates(
        temp_item_updates: Vec<(TemporaryItem, Timestamp, StateDiff)>,
    ) -> VecDeque<(TemporaryItem, Timestamp, StateDiff)> {
        // Partition items into groups s.t. each item in one group has a predefined order with all
        // items in other groups. For example, all sinks are ordered greater than all tables.
        let mut types = Vec::new();
        // N.B. Functions can depend on system tables, but not user tables.
        let mut funcs = Vec::new();
        let mut secrets = Vec::new();
        let mut connections = Vec::new();
        let mut sources = Vec::new();
        let mut tables = Vec::new();
        let mut derived_items = Vec::new();
        let mut sinks = Vec::new();
        let mut continual_tasks = Vec::new();

        for update in temp_item_updates {
            match update.0.item.typ() {
                CatalogItemType::Type => types.push(update),
                CatalogItemType::Func => funcs.push(update),
                CatalogItemType::Secret => secrets.push(update),
                CatalogItemType::Connection => connections.push(update),
                CatalogItemType::Source => sources.push(update),
                CatalogItemType::Table => tables.push(update),
                CatalogItemType::View
                | CatalogItemType::MaterializedView
                | CatalogItemType::Index => derived_items.push(update),
                CatalogItemType::Sink => sinks.push(update),
                CatalogItemType::ContinualTask => continual_tasks.push(update),
            }
        }

        // Within each group, sort by ID.
        for group in [
            &mut types,
            &mut funcs,
            &mut secrets,
            &mut connections,
            &mut sources,
            &mut tables,
            &mut derived_items,
            &mut sinks,
            &mut continual_tasks,
        ] {
            group.sort_by_key(|(item, _, _)| item.id);
        }

        iter::empty()
            .chain(types)
            .chain(funcs)
            .chain(secrets)
            .chain(connections)
            .chain(sources)
            .chain(tables)
            .chain(derived_items)
            .chain(sinks)
            .chain(continual_tasks)
            .collect()
    }
    let temp_item_retractions = sort_temp_item_updates(temp_item_retractions);
    let temp_item_additions = sort_temp_item_updates(temp_item_additions);

    /// Merge sorted temporary and non-temp items.
    fn merge_item_updates(
        mut item_updates: VecDeque<(mz_catalog::durable::Item, Timestamp, StateDiff)>,
        mut temp_item_updates: VecDeque<(TemporaryItem, Timestamp, StateDiff)>,
    ) -> Vec<StateUpdate> {
        let mut state_updates = Vec::with_capacity(item_updates.len() + temp_item_updates.len());

        while let (Some((item, _, _)), Some((temp_item, _, _))) =
            (item_updates.front(), temp_item_updates.front())
        {
            if item.id < temp_item.id {
                let (item, ts, diff) = item_updates.pop_front().expect("non-empty");
                state_updates.push(StateUpdate {
                    kind: StateUpdateKind::Item(item),
                    ts,
                    diff,
                });
            } else if item.id > temp_item.id {
                let (temp_item, ts, diff) = temp_item_updates.pop_front().expect("non-empty");
                state_updates.push(StateUpdate {
                    kind: StateUpdateKind::TemporaryItem(temp_item),
                    ts,
                    diff,
                });
            } else {
                unreachable!(
                    "two items cannot have the same ID: item={item:?}, temp_item={temp_item:?}"
                );
            }
        }

        while let Some((item, ts, diff)) = item_updates.pop_front() {
            state_updates.push(StateUpdate {
                kind: StateUpdateKind::Item(item),
                ts,
                diff,
            });
        }

        while let Some((temp_item, ts, diff)) = temp_item_updates.pop_front() {
            state_updates.push(StateUpdate {
                kind: StateUpdateKind::TemporaryItem(temp_item),
                ts,
                diff,
            });
        }

        state_updates
    }
    let item_retractions = merge_item_updates(item_retractions, temp_item_retractions);
    let item_additions = merge_item_updates(item_additions, temp_item_additions);

    // Put everything back together.
    iter::empty()
        // All retractions must be reversed.
        .chain(post_item_retractions.into_iter().rev())
        .chain(item_retractions.into_iter().rev())
        .chain(builtin_index_retractions.into_iter().rev())
        .chain(cluster_retractions.into_iter().rev())
        .chain(other_builtin_retractions.into_iter().rev())
        .chain(pre_cluster_retractions.into_iter().rev())
        .chain(pre_cluster_additions)
        .chain(other_builtin_additions)
        .chain(cluster_additions)
        .chain(builtin_index_additions)
        .chain(item_additions)
        .chain(post_item_additions)
        .collect()
}

/// Most updates are applied one at a time, but during bootstrap, certain types are applied
/// separately in a batch for performance reasons. A constraint is that updates must be applied in
/// order. This process is modeled as a state machine that batches then applies groups of updates.
enum BootstrapApplyState {
    /// Additions of builtin views.
    BuiltinViewAdditions(Vec<(&'static BuiltinView, CatalogItemId, GlobalId)>),
    /// Item updates that aren't builtin view additions.
    Items(Vec<StateUpdate>),
    /// All other updates.
    Updates(Vec<StateUpdate>),
}

impl BootstrapApplyState {
    fn new(update: StateUpdate) -> BootstrapApplyState {
        match update {
            StateUpdate {
                kind: StateUpdateKind::SystemObjectMapping(system_object_mapping),
                diff: StateDiff::Addition,
                ..
            } if matches!(
                system_object_mapping.description.object_type,
                CatalogItemType::View
            ) =>
            {
                let view_addition = lookup_builtin_view_addition(system_object_mapping);
                BootstrapApplyState::BuiltinViewAdditions(vec![view_addition])
            }
            StateUpdate {
                kind: StateUpdateKind::IntrospectionSourceIndex(_),
                ..
            }
            | StateUpdate {
                kind: StateUpdateKind::SystemObjectMapping(_),
                ..
            }
            | StateUpdate {
                kind: StateUpdateKind::Item(_),
                ..
            } => BootstrapApplyState::Items(vec![update]),
            update => BootstrapApplyState::Updates(vec![update]),
        }
    }

    /// Apply all updates that have been batched in `self`.
    ///
    /// We make sure to enable all "enable_for_item_parsing" feature flags when applying item
    /// updates during bootstrap. See [`CatalogState::with_enable_for_item_parsing`] for more
    /// details.
    async fn apply(
        self,
        state: &mut CatalogState,
        retractions: &mut InProgressRetractions,
        local_expression_cache: &mut LocalExpressionCache,
    ) -> Vec<BuiltinTableUpdate<&'static BuiltinTable>> {
        match self {
            BootstrapApplyState::BuiltinViewAdditions(builtin_view_additions) => {
                let restore = state.system_configuration.clone();
                state.system_configuration.enable_for_item_parsing();
                let builtin_table_updates = CatalogState::parse_builtin_views(
                    state,
                    builtin_view_additions,
                    retractions,
                    local_expression_cache,
                )
                .await;
                state.system_configuration = restore;
                builtin_table_updates
            }
            BootstrapApplyState::Items(updates) => state.with_enable_for_item_parsing(|state| {
                state
                    .apply_updates_inner(updates, retractions, local_expression_cache)
                    .expect("corrupt catalog")
            }),
            BootstrapApplyState::Updates(updates) => state
                .apply_updates_inner(updates, retractions, local_expression_cache)
                .expect("corrupt catalog"),
        }
    }

    async fn step(
        self,
        next: BootstrapApplyState,
        state: &mut CatalogState,
        retractions: &mut InProgressRetractions,
        local_expression_cache: &mut LocalExpressionCache,
    ) -> (
        BootstrapApplyState,
        Vec<BuiltinTableUpdate<&'static BuiltinTable>>,
    ) {
        match (self, next) {
            (
                BootstrapApplyState::BuiltinViewAdditions(mut builtin_view_additions),
                BootstrapApplyState::BuiltinViewAdditions(next_builtin_view_additions),
            ) => {
                // Continue batching builtin view additions.
                builtin_view_additions.extend(next_builtin_view_additions);
                (
                    BootstrapApplyState::BuiltinViewAdditions(builtin_view_additions),
                    Vec::new(),
                )
            }
            (BootstrapApplyState::Items(mut updates), BootstrapApplyState::Items(next_updates)) => {
                // Continue batching item updates.
                updates.extend(next_updates);
                (BootstrapApplyState::Items(updates), Vec::new())
            }
            (
                BootstrapApplyState::Updates(mut updates),
                BootstrapApplyState::Updates(next_updates),
            ) => {
                // Continue batching updates.
                updates.extend(next_updates);
                (BootstrapApplyState::Updates(updates), Vec::new())
            }
            (apply_state, next_apply_state) => {
                // Apply the current batch and start batching new apply state.
                let builtin_table_update = apply_state
                    .apply(state, retractions, local_expression_cache)
                    .await;
                (next_apply_state, builtin_table_update)
            }
        }
    }
}

/// Helper method to updated inverted lookup maps. The keys are generally names and the values are
/// generally IDs.
///
/// Importantly, when retracting it's expected that the existing value will match `value` exactly.
fn apply_inverted_lookup<K, V>(map: &mut BTreeMap<K, V>, key: &K, value: V, diff: StateDiff)
where
    K: Ord + Clone + Debug,
    V: PartialEq + Debug,
{
    match diff {
        StateDiff::Retraction => {
            let prev = map.remove(key);
            assert_eq!(
                prev,
                Some(value),
                "retraction does not match existing value: {key:?}"
            );
        }
        StateDiff::Addition => {
            let prev = map.insert(key.clone(), value);
            assert_eq!(
                prev, None,
                "values must be explicitly retracted before inserting a new value: {key:?}"
            );
        }
    }
}

/// Helper method to update catalog state, that may need to be updated from a previously retracted
/// object.
fn apply_with_update<K, V, D>(
    map: &mut BTreeMap<K, V>,
    durable: D,
    key_fn: impl FnOnce(&D) -> K,
    diff: StateDiff,
    retractions: &mut BTreeMap<D::Key, V>,
) where
    K: Ord,
    V: UpdateFrom<D> + PartialEq + Debug,
    D: DurableType,
    D::Key: Ord,
{
    match diff {
        StateDiff::Retraction => {
            let mem_key = key_fn(&durable);
            let value = map
                .remove(&mem_key)
                .expect("retraction does not match existing value: {key:?}");
            let durable_key = durable.into_key_value().0;
            retractions.insert(durable_key, value);
        }
        StateDiff::Addition => {
            let mem_key = key_fn(&durable);
            let durable_key = durable.key();
            let value = match retractions.remove(&durable_key) {
                Some(mut retraction) => {
                    retraction.update_from(durable);
                    retraction
                }
                None => durable.into(),
            };
            let prev = map.insert(mem_key, value);
            assert_eq!(
                prev, None,
                "values must be explicitly retracted before inserting a new value"
            );
        }
    }
}

/// Looks up a [`BuiltinView`] from a [`SystemObjectMapping`].
fn lookup_builtin_view_addition(
    mapping: SystemObjectMapping,
) -> (&'static BuiltinView, CatalogItemId, GlobalId) {
    let (_, builtin) = BUILTIN_LOOKUP
        .get(&mapping.description)
        .expect("missing builtin view");
    let Builtin::View(view) = builtin else {
        unreachable!("programming error, expected BuiltinView found {builtin:?}");
    };

    (
        view,
        mapping.unique_identifier.catalog_id,
        mapping.unique_identifier.global_id,
    )
}

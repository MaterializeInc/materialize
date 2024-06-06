// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Logic related to executing catalog transactions.

use mz_adapter_types::compaction::CompactionWindow;
use mz_adapter_types::connection::ConnectionId;
use mz_audit_log::{
    CreateOrDropClusterReplicaReasonV1, EventDetails, EventType, IdFullNameV1, ObjectType,
    SchedulingDecisionsWithReasonsV1, VersionedEvent,
};
use mz_catalog::builtin::BuiltinLog;
use mz_catalog::durable::Transaction;
use mz_catalog::memory::error::{AmbiguousRename, Error, ErrorKind};
use mz_catalog::memory::objects::{
    CatalogItem, ClusterConfig, ClusterReplicaProcessStatus, Database, Role, Schema,
};
use mz_catalog::SYSTEM_CONN_ID;
use mz_controller::clusters::{
    ClusterEvent, ManagedReplicaLocation, ReplicaConfig, ReplicaLocation,
};
use mz_controller_types::{ClusterId, ReplicaId};
use mz_ore::cast::CastFrom;
use mz_ore::instrument;
use mz_ore::now::EpochMillis;
use mz_repr::adt::mz_acl_item::{merge_mz_acl_items, AclMode, MzAclItem, PrivilegeMap};
use mz_repr::role_id::RoleId;
use mz_repr::GlobalId;
use mz_sql::catalog::{
    CatalogCluster, CatalogDatabase, CatalogError as SqlCatalogError,
    CatalogItem as SqlCatalogItem, CatalogRole, CatalogSchema, DefaultPrivilegeAclItem,
    DefaultPrivilegeObject, RoleAttributes, RoleMembership, RoleVars,
};
use mz_sql::names::{
    CommentObjectId, DatabaseId, FullItemName, ObjectId, QualifiedItemName, QualifiedSchemaName,
    ResolvedDatabaseSpecifier, SchemaId, SchemaSpecifier, SystemObjectId,
};
use mz_sql::session::user::{MZ_SUPPORT_ROLE_ID, MZ_SYSTEM_ROLE_ID};
use mz_sql::session::vars::{OwnedVarInput, Var, VarInput, TXN_WAL_TABLES};
use mz_sql::{rbac, DEFAULT_SCHEMA};
use mz_sql_parser::ast::{QualifiedReplica, Value};
use mz_storage_client::controller::StorageController;
use std::collections::{BTreeMap, BTreeSet};
use tracing::{info, trace};

use crate::catalog::{
    catalog_type_to_audit_object_type, is_reserved_name, is_reserved_role_name,
    object_type_to_audit_object_type, system_object_type_to_audit_object_type, BuiltinTableUpdate,
    Catalog, CatalogState, UpdatePrivilegeVariant,
};
use crate::coord::cluster_scheduling::SchedulingDecision;
use crate::coord::ConnMeta;
use crate::util::ResultExt;
use crate::AdapterError;

#[derive(Debug, Clone)]
pub enum Op {
    AlterRetainHistory {
        id: GlobalId,
        value: Option<Value>,
        window: CompactionWindow,
    },
    AlterRole {
        id: RoleId,
        name: String,
        attributes: RoleAttributes,
        vars: RoleVars,
    },
    CreateDatabase {
        name: String,
        owner_id: RoleId,
    },
    CreateSchema {
        database_id: ResolvedDatabaseSpecifier,
        schema_name: String,
        owner_id: RoleId,
    },
    CreateRole {
        name: String,
        attributes: RoleAttributes,
    },
    CreateCluster {
        id: ClusterId,
        name: String,
        introspection_sources: Vec<(&'static BuiltinLog, GlobalId)>,
        owner_id: RoleId,
        config: ClusterConfig,
    },
    CreateClusterReplica {
        cluster_id: ClusterId,
        id: ReplicaId,
        name: String,
        config: ReplicaConfig,
        owner_id: RoleId,
        reason: ReplicaCreateDropReason,
    },
    CreateItem {
        id: GlobalId,
        name: QualifiedItemName,
        item: CatalogItem,
        owner_id: RoleId,
    },
    Comment {
        object_id: CommentObjectId,
        sub_component: Option<usize>,
        comment: Option<String>,
    },
    DropObjects(Vec<DropObjectInfo>),
    GrantRole {
        role_id: RoleId,
        member_id: RoleId,
        grantor_id: RoleId,
    },
    RenameCluster {
        id: ClusterId,
        name: String,
        to_name: String,
        check_reserved_names: bool,
    },
    RenameClusterReplica {
        cluster_id: ClusterId,
        replica_id: ReplicaId,
        name: QualifiedReplica,
        to_name: String,
    },
    RenameItem {
        id: GlobalId,
        current_full_name: FullItemName,
        to_name: String,
    },
    RenameSchema {
        database_spec: ResolvedDatabaseSpecifier,
        schema_spec: SchemaSpecifier,
        new_name: String,
        check_reserved_names: bool,
    },
    UpdateOwner {
        id: ObjectId,
        new_owner: RoleId,
    },
    UpdatePrivilege {
        target_id: SystemObjectId,
        privilege: MzAclItem,
        variant: UpdatePrivilegeVariant,
    },
    UpdateDefaultPrivilege {
        privilege_object: DefaultPrivilegeObject,
        privilege_acl_item: DefaultPrivilegeAclItem,
        variant: UpdatePrivilegeVariant,
    },
    RevokeRole {
        role_id: RoleId,
        member_id: RoleId,
        grantor_id: RoleId,
    },
    UpdateClusterConfig {
        id: ClusterId,
        name: String,
        config: ClusterConfig,
    },
    UpdateClusterReplicaStatus {
        event: ClusterEvent,
    },
    UpdateItem {
        id: GlobalId,
        name: QualifiedItemName,
        to_item: CatalogItem,
    },
    UpdateStorageUsage {
        shard_id: Option<String>,
        size_bytes: u64,
        collection_timestamp: EpochMillis,
    },
    UpdateSystemConfiguration {
        name: String,
        value: OwnedVarInput,
    },
    ResetSystemConfiguration {
        name: String,
    },
    ResetAllSystemConfiguration,
    /// Performs updates to builtin table, `mz_ssh_tunnel_connections`. The
    /// `mz_ssh_tunnel_connections` table is weird. Its contents are not fully derived from catalog
    /// state, they're derived from the secrets controller. However, it still must be kept in-sync
    /// with the catalog state. Storing the contents of the table in the durable catalog would
    /// simplify the situation, but then we'd have to somehow encode public keys the CREATE SQL
    /// of connections, which is annoying. In order to successfully balance all of these
    /// constraints, we handle all updates to the table separately.
    ///
    /// TODO(jkosh44) In a multi-writer or high availability catalog world, this will not work. If
    /// a process crashes after updating the durable catalog but before updating the builtin table,
    /// then another listening catalog will never know to update the builtin table.
    SshTunnelConnectionsUpdates {
        builtin_table_update: BuiltinTableUpdate,
    },
    /// Performs a dry run of the commit, but errors with
    /// [`AdapterError::TransactionDryRun`].
    ///
    /// When using this value, it should be included only as the last element of
    /// the transaction and should not be the only value in the transaction.
    TransactionDryRun,
}

/// Almost the same as `ObjectId`, but the `ClusterReplica` case has an extra
/// `ReplicaCreateDropReason` field. This is forwarded to `mz_audit_events.details` when applying
/// the `Op::DropObjects`.
#[derive(Debug, Clone)]
pub enum DropObjectInfo {
    Cluster(ClusterId),
    ClusterReplica((ClusterId, ReplicaId, ReplicaCreateDropReason)),
    Database(DatabaseId),
    Schema((ResolvedDatabaseSpecifier, SchemaSpecifier)),
    Role(RoleId),
    Item(GlobalId),
}

impl DropObjectInfo {
    /// Creates a `DropObjectInfo` from an `ObjectId`.
    /// If it is a `ClusterReplica`, the reason will be set to `ReplicaCreateDropReason::Manual`.
    pub(crate) fn manual_drop_from_object_id(id: ObjectId) -> Self {
        match id {
            ObjectId::Cluster(cluster_id) => DropObjectInfo::Cluster(cluster_id),
            ObjectId::ClusterReplica((cluster_id, replica_id)) => DropObjectInfo::ClusterReplica((
                cluster_id,
                replica_id,
                ReplicaCreateDropReason::Manual,
            )),
            ObjectId::Database(database_id) => DropObjectInfo::Database(database_id),
            ObjectId::Schema(schema) => DropObjectInfo::Schema(schema),
            ObjectId::Role(role_id) => DropObjectInfo::Role(role_id),
            ObjectId::Item(global_id) => DropObjectInfo::Item(global_id),
        }
    }

    /// Creates an `ObjectId` from a `DropObjectInfo`.
    /// Loses the `ReplicaCreateDropReason` if there is one!
    fn to_object_id(&self) -> ObjectId {
        match &self {
            DropObjectInfo::Cluster(cluster_id) => ObjectId::Cluster(cluster_id.clone()),
            DropObjectInfo::ClusterReplica((cluster_id, replica_id, _reason)) => {
                ObjectId::ClusterReplica((cluster_id.clone(), replica_id.clone()))
            }
            DropObjectInfo::Database(database_id) => ObjectId::Database(database_id.clone()),
            DropObjectInfo::Schema(schema) => ObjectId::Schema(schema.clone()),
            DropObjectInfo::Role(role_id) => ObjectId::Role(role_id.clone()),
            DropObjectInfo::Item(global_id) => ObjectId::Item(global_id.clone()),
        }
    }
}

/// The reason for creating or dropping a replica.
#[derive(Debug, Clone)]
pub enum ReplicaCreateDropReason {
    /// The user initiated the replica create or drop, e.g., by
    /// - creating/dropping a cluster,
    /// - ALTERing various options on a managed cluster,
    /// - CREATE/DROP CLUSTER REPLICA on an unmanaged cluster.
    Manual,
    /// The automated cluster scheduling initiated the replica create or drop, e.g., a
    /// materialized view is needing a refresh on a SCHEDULE ON REFRESH cluster.
    ClusterScheduling(Vec<SchedulingDecision>),
}

impl ReplicaCreateDropReason {
    pub fn into_audit_log(
        self,
    ) -> (
        CreateOrDropClusterReplicaReasonV1,
        Option<SchedulingDecisionsWithReasonsV1>,
    ) {
        let (reason, scheduling_policies) = match self {
            ReplicaCreateDropReason::Manual => (CreateOrDropClusterReplicaReasonV1::Manual, None),
            ReplicaCreateDropReason::ClusterScheduling(scheduling_decisions) => (
                CreateOrDropClusterReplicaReasonV1::Schedule,
                Some(scheduling_decisions),
            ),
        };
        (
            reason,
            scheduling_policies
                .as_ref()
                .map(SchedulingDecision::reasons_to_audit_log_reasons),
        )
    }
}

pub struct TransactionResult {
    pub builtin_table_updates: Vec<BuiltinTableUpdate>,
    pub audit_events: Vec<VersionedEvent>,
}

impl Catalog {
    fn should_audit_log_item(item: &CatalogItem) -> bool {
        !item.is_temporary()
    }

    /// Gets GlobalIds of temporary items to be created, checks for name collisions
    /// within a connection id.
    fn temporary_ids(
        &self,
        ops: &[Op],
        temporary_drops: BTreeSet<(&ConnectionId, String)>,
    ) -> Result<Vec<GlobalId>, Error> {
        let mut creating = BTreeSet::new();
        let mut temporary_ids = Vec::with_capacity(ops.len());
        for op in ops.iter() {
            if let Op::CreateItem {
                id,
                name,
                item,
                owner_id: _,
            } = op
            {
                if let Some(conn_id) = item.conn_id() {
                    if self.item_exists_in_temp_schemas(conn_id, &name.item)
                        && !temporary_drops.contains(&(conn_id, name.item.clone()))
                        || creating.contains(&(conn_id, &name.item))
                    {
                        return Err(
                            SqlCatalogError::ItemAlreadyExists(*id, name.item.clone()).into()
                        );
                    } else {
                        creating.insert((conn_id, &name.item));
                        temporary_ids.push(id.clone());
                    }
                }
            }
        }
        Ok(temporary_ids)
    }

    #[instrument(name = "catalog::transact")]
    pub async fn transact(
        &mut self,
        // n.b. this is an option to prevent us from needing to build out a
        // dummy impl of `StorageController` for tests.
        storage_controller: Option<&mut dyn StorageController<Timestamp = mz_repr::Timestamp>>,
        oracle_write_ts: mz_repr::Timestamp,
        session: Option<&ConnMeta>,
        ops: Vec<Op>,
    ) -> Result<TransactionResult, AdapterError> {
        trace!("transact: {:?}", ops);
        fail::fail_point!("catalog_transact", |arg| {
            Err(AdapterError::Unstructured(anyhow::anyhow!(
                "failpoint: {arg:?}"
            )))
        });

        let drop_ids: BTreeSet<GlobalId> = ops
            .iter()
            .filter_map(|op| match op {
                Op::DropObjects(drop_object_infos) => {
                    let ids = drop_object_infos.iter().map(|info| info.to_object_id());
                    let item_ids = ids.filter_map(|id| match id {
                        ObjectId::Item(id) => Some(id),
                        _ => None,
                    });
                    Some(item_ids)
                }
                _ => None,
            })
            .flatten()
            .collect();
        let temporary_drops = drop_ids
            .iter()
            .filter_map(|id| {
                let entry = self.get_entry(id);
                match entry.item().conn_id() {
                    Some(conn_id) => Some((conn_id, entry.name().item.clone())),
                    None => None,
                }
            })
            .collect();
        let temporary_ids = self.temporary_ids(&ops, temporary_drops)?;
        let mut builtin_table_updates = vec![];
        let mut audit_events = vec![];
        let mut storage = self.storage().await;
        let mut tx = storage
            .transaction()
            .await
            .unwrap_or_terminate("starting catalog transaction");
        // Prepare a candidate catalog state.
        let mut state = self.state.clone();

        Self::transact_inner(
            storage_controller,
            oracle_write_ts,
            session,
            ops,
            temporary_ids,
            &mut builtin_table_updates,
            &mut audit_events,
            &mut tx,
            &mut state,
        )
        .await?;

        // The user closure was successful, apply the updates. Terminate the
        // process if this fails, because we have to restart envd due to
        // indeterminate catalog state, which we only reconcile during catalog
        // init.
        tx.commit()
            .await
            .unwrap_or_terminate("catalog storage transaction commit must succeed");

        // Dropping here keeps the mutable borrow on self, preventing us accidentally
        // mutating anything until after f is executed.
        drop(storage);
        self.state = state;
        self.transient_revision += 1;

        // Drop in-memory planning metadata.
        let dropped_notices = self.drop_plans_and_metainfos(&drop_ids);
        if self.state.system_config().enable_mz_notices() {
            // Generate retractions for the Builtin tables.
            self.state().pack_optimizer_notices(
                &mut builtin_table_updates,
                dropped_notices.iter(),
                -1,
            );
        }

        Ok(TransactionResult {
            builtin_table_updates,
            audit_events,
        })
    }

    /// Performs the transaction described by `ops`.
    ///
    /// # Panics
    /// - If `ops` contains [`Op::TransactionDryRun`] and the value is not the
    ///   final element.
    /// - If the only element of `ops` is [`Op::TransactionDryRun`].
    #[instrument(name = "catalog::transact_inner")]
    async fn transact_inner(
        storage_controller: Option<&mut dyn StorageController<Timestamp = mz_repr::Timestamp>>,
        oracle_write_ts: mz_repr::Timestamp,
        session: Option<&ConnMeta>,
        mut ops: Vec<Op>,
        temporary_ids: Vec<GlobalId>,
        builtin_table_updates: &mut Vec<BuiltinTableUpdate>,
        audit_events: &mut Vec<VersionedEvent>,
        tx: &mut Transaction<'_>,
        state: &mut CatalogState,
    ) -> Result<(), AdapterError> {
        let dry_run_ops = match ops.last() {
            Some(Op::TransactionDryRun) => {
                // Remove dry run marker.
                ops.pop();
                assert!(!ops.is_empty(), "TransactionDryRun must not be the only op");
                ops.clone()
            }
            Some(_) => vec![],
            None => return Ok(()),
        };

        let mut storage_collections_to_create = BTreeSet::new();
        let mut storage_collections_to_drop = BTreeSet::new();

        for op in ops {
            Self::transact_op(
                oracle_write_ts,
                session,
                op,
                &temporary_ids,
                builtin_table_updates,
                audit_events,
                tx,
                state,
                &mut storage_collections_to_create,
                &mut storage_collections_to_drop,
            )
            .await?;
            tx.commit_op();
        }

        if dry_run_ops.is_empty() {
            if let Some(c) = storage_controller {
                c.prepare_state(
                    tx,
                    storage_collections_to_create,
                    storage_collections_to_drop,
                )
                .await?;
            }

            state.update_storage_metadata(tx);

            Ok(())
        } else {
            Err(AdapterError::TransactionDryRun {
                new_ops: dry_run_ops,
                new_state: state.clone(),
            })
        }
    }

    /// Performs the transaction operation described by `op`.
    #[instrument]
    async fn transact_op(
        oracle_write_ts: mz_repr::Timestamp,
        session: Option<&ConnMeta>,
        op: Op,
        temporary_ids: &Vec<GlobalId>,
        builtin_table_updates: &mut Vec<BuiltinTableUpdate>,
        audit_events: &mut Vec<VersionedEvent>,
        tx: &mut Transaction<'_>,
        state: &mut CatalogState,
        storage_collections_to_create: &mut BTreeSet<GlobalId>,
        storage_collections_to_drop: &mut BTreeSet<GlobalId>,
    ) -> Result<(), AdapterError> {
        match op {
            Op::TransactionDryRun => {
                unreachable!("TransactionDryRun can only be used a final element of ops")
            }
            Op::AlterRetainHistory { id, value, window } => {
                let entry = state.get_entry(&id);
                if id.is_system() {
                    let name = entry.name();
                    let full_name =
                        state.resolve_full_name(name, session.map(|session| session.conn_id()));
                    return Err(AdapterError::Catalog(Error::new(ErrorKind::ReadOnlyItem(
                        full_name.to_string(),
                    ))));
                }

                let mut new_entry = entry.clone();
                let previous = new_entry
                    .item
                    .update_retain_history(value.clone(), window)
                    .map_err(|_| {
                        AdapterError::Catalog(Error::new(ErrorKind::Internal(
                            "planner should have rejected invalid alter retain history item type"
                                .to_string(),
                        )))
                    })?;

                builtin_table_updates.extend(state.pack_item_update(id, -1));

                if Self::should_audit_log_item(new_entry.item()) {
                    let details =
                        EventDetails::AlterRetainHistoryV1(mz_audit_log::AlterRetainHistoryV1 {
                            id: id.to_string(),
                            old_history: previous.map(|previous| previous.to_string()),
                            new_history: value.map(|v| v.to_string()),
                        });
                    state.add_to_audit_log(
                        oracle_write_ts,
                        session,
                        tx,
                        builtin_table_updates,
                        audit_events,
                        EventType::Alter,
                        catalog_type_to_audit_object_type(new_entry.item().typ()),
                        details,
                    )?;
                }

                Self::update_item(
                    state,
                    builtin_table_updates,
                    id,
                    new_entry.name.clone(),
                    new_entry.item().clone(),
                )?;
                tx.update_item(id, new_entry.into())?;
            }
            Op::AlterRole {
                id,
                name,
                attributes,
                vars,
            } => {
                state.ensure_not_reserved_role(&id)?;
                builtin_table_updates.extend(state.pack_role_update(id, -1));

                let existing_role = state.get_role_mut(&id);
                existing_role.attributes = attributes;
                existing_role.vars = vars;
                tx.update_role(id, existing_role.clone().into())?;
                builtin_table_updates.extend(state.pack_role_update(id, 1));

                state.add_to_audit_log(
                    oracle_write_ts,
                    session,
                    tx,
                    builtin_table_updates,
                    audit_events,
                    EventType::Alter,
                    ObjectType::Role,
                    EventDetails::IdNameV1(mz_audit_log::IdNameV1 {
                        id: id.to_string(),
                        name: name.clone(),
                    }),
                )?;

                info!("update role {name} ({id})");
            }
            Op::CreateDatabase { name, owner_id } => {
                let database_owner_privileges = vec![rbac::owner_privilege(
                    mz_sql::catalog::ObjectType::Database,
                    owner_id,
                )];
                let database_default_privileges = state
                    .default_privileges
                    .get_applicable_privileges(
                        owner_id,
                        None,
                        None,
                        mz_sql::catalog::ObjectType::Database,
                    )
                    .map(|item| item.mz_acl_item(owner_id));
                let database_privileges: Vec<_> = merge_mz_acl_items(
                    database_owner_privileges
                        .into_iter()
                        .chain(database_default_privileges),
                )
                .collect();

                let schema_owner_privileges = vec![rbac::owner_privilege(
                    mz_sql::catalog::ObjectType::Schema,
                    owner_id,
                )];
                let schema_default_privileges = state
                    .default_privileges
                    .get_applicable_privileges(
                        owner_id,
                        None,
                        None,
                        mz_sql::catalog::ObjectType::Schema,
                    )
                    .map(|item| item.mz_acl_item(owner_id))
                    // Special default privilege on public schemas.
                    .chain(std::iter::once(MzAclItem {
                        grantee: RoleId::Public,
                        grantor: owner_id,
                        acl_mode: AclMode::USAGE,
                    }));
                let schema_privileges: Vec<_> = merge_mz_acl_items(
                    schema_owner_privileges
                        .into_iter()
                        .chain(schema_default_privileges),
                )
                .collect();

                let (database_id, database_oid) =
                    tx.insert_user_database(&name, owner_id, database_privileges.clone())?;
                let (schema_id, schema_oid) = tx.insert_user_schema(
                    database_id,
                    DEFAULT_SCHEMA,
                    owner_id,
                    schema_privileges.clone(),
                )?;
                state.add_to_audit_log(
                    oracle_write_ts,
                    session,
                    tx,
                    builtin_table_updates,
                    audit_events,
                    EventType::Create,
                    ObjectType::Database,
                    EventDetails::IdNameV1(mz_audit_log::IdNameV1 {
                        id: database_id.to_string(),
                        name: name.clone(),
                    }),
                )?;
                info!("create database {}", name);
                state.database_by_id.insert(
                    database_id.clone(),
                    Database {
                        name: name.clone(),
                        id: database_id.clone(),
                        oid: database_oid,
                        schemas_by_id: BTreeMap::new(),
                        schemas_by_name: BTreeMap::new(),
                        owner_id,
                        privileges: PrivilegeMap::from_mz_acl_items(database_privileges),
                    },
                );
                state
                    .database_by_name
                    .insert(name.clone(), database_id.clone());
                builtin_table_updates.push(state.pack_database_update(&database_id, 1));

                state.add_to_audit_log(
                    oracle_write_ts,
                    session,
                    tx,
                    builtin_table_updates,
                    audit_events,
                    EventType::Create,
                    ObjectType::Schema,
                    EventDetails::SchemaV2(mz_audit_log::SchemaV2 {
                        id: schema_id.to_string(),
                        name: DEFAULT_SCHEMA.to_string(),
                        database_name: Some(name),
                    }),
                )?;
                Self::create_schema(
                    state,
                    builtin_table_updates,
                    schema_id,
                    schema_oid,
                    database_id,
                    DEFAULT_SCHEMA.to_string(),
                    owner_id,
                    PrivilegeMap::from_mz_acl_items(schema_privileges),
                )?;
            }
            Op::CreateSchema {
                database_id,
                schema_name,
                owner_id,
            } => {
                if is_reserved_name(&schema_name) {
                    return Err(AdapterError::Catalog(Error::new(
                        ErrorKind::ReservedSchemaName(schema_name),
                    )));
                }
                let database_id = match database_id {
                    ResolvedDatabaseSpecifier::Id(id) => id,
                    ResolvedDatabaseSpecifier::Ambient => {
                        return Err(AdapterError::Catalog(Error::new(
                            ErrorKind::ReadOnlySystemSchema(schema_name),
                        )));
                    }
                };
                let owner_privileges = vec![rbac::owner_privilege(
                    mz_sql::catalog::ObjectType::Schema,
                    owner_id,
                )];
                let default_privileges = state
                    .default_privileges
                    .get_applicable_privileges(
                        owner_id,
                        Some(database_id),
                        None,
                        mz_sql::catalog::ObjectType::Schema,
                    )
                    .map(|item| item.mz_acl_item(owner_id));
                let privileges: Vec<_> =
                    merge_mz_acl_items(owner_privileges.into_iter().chain(default_privileges))
                        .collect();
                let (schema_id, schema_oid) =
                    tx.insert_user_schema(database_id, &schema_name, owner_id, privileges.clone())?;
                state.add_to_audit_log(
                    oracle_write_ts,
                    session,
                    tx,
                    builtin_table_updates,
                    audit_events,
                    EventType::Create,
                    ObjectType::Schema,
                    EventDetails::SchemaV2(mz_audit_log::SchemaV2 {
                        id: schema_id.to_string(),
                        name: schema_name.clone(),
                        database_name: Some(state.database_by_id[&database_id].name.clone()),
                    }),
                )?;
                Self::create_schema(
                    state,
                    builtin_table_updates,
                    schema_id,
                    schema_oid,
                    database_id,
                    schema_name,
                    owner_id,
                    PrivilegeMap::from_mz_acl_items(privileges),
                )?;
            }
            Op::CreateRole { name, attributes } => {
                if is_reserved_role_name(&name) {
                    return Err(AdapterError::Catalog(Error::new(
                        ErrorKind::ReservedRoleName(name),
                    )));
                }
                let membership = RoleMembership::new();
                let vars = RoleVars::default();
                let (id, oid) = tx.insert_user_role(
                    name.clone(),
                    attributes.clone(),
                    membership.clone(),
                    vars.clone(),
                )?;
                state.add_to_audit_log(
                    oracle_write_ts,
                    session,
                    tx,
                    builtin_table_updates,
                    audit_events,
                    EventType::Create,
                    ObjectType::Role,
                    EventDetails::IdNameV1(mz_audit_log::IdNameV1 {
                        id: id.to_string(),
                        name: name.clone(),
                    }),
                )?;
                info!("create role {}", name);
                state.roles_by_name.insert(name.clone(), id);
                state.roles_by_id.insert(
                    id,
                    Role {
                        name,
                        id,
                        oid,
                        attributes,
                        membership,
                        vars,
                    },
                );
                builtin_table_updates.extend(state.pack_role_update(id, 1));
            }
            Op::CreateCluster {
                id,
                name,
                introspection_sources,
                owner_id,
                config,
            } => {
                if is_reserved_name(&name) {
                    return Err(AdapterError::Catalog(Error::new(
                        ErrorKind::ReservedClusterName(name),
                    )));
                }
                let owner_privileges = vec![rbac::owner_privilege(
                    mz_sql::catalog::ObjectType::Cluster,
                    owner_id,
                )];
                let default_privileges = state
                    .default_privileges
                    .get_applicable_privileges(
                        owner_id,
                        None,
                        None,
                        mz_sql::catalog::ObjectType::Cluster,
                    )
                    .map(|item| item.mz_acl_item(owner_id));
                let privileges: Vec<_> =
                    merge_mz_acl_items(owner_privileges.into_iter().chain(default_privileges))
                        .collect();

                let introspection_sources = tx.insert_user_cluster(
                    id,
                    &name,
                    introspection_sources,
                    owner_id,
                    privileges.clone(),
                    config.clone().into(),
                )?;
                state.add_to_audit_log(
                    oracle_write_ts,
                    session,
                    tx,
                    builtin_table_updates,
                    audit_events,
                    EventType::Create,
                    ObjectType::Cluster,
                    EventDetails::IdNameV1(mz_audit_log::IdNameV1 {
                        id: id.to_string(),
                        name: name.clone(),
                    }),
                )?;
                info!("create cluster {}", name);
                let introspection_source_ids: Vec<GlobalId> =
                    introspection_sources.iter().map(|(_, id, _)| *id).collect();
                state.insert_cluster(
                    id,
                    name.clone(),
                    introspection_sources,
                    owner_id,
                    PrivilegeMap::from_mz_acl_items(privileges),
                    config,
                );
                builtin_table_updates.extend(state.pack_cluster_update(&name, 1));
                for id in introspection_source_ids {
                    builtin_table_updates.extend(state.pack_item_update(id, 1));
                }
            }
            Op::CreateClusterReplica {
                cluster_id,
                id,
                name,
                config,
                owner_id,
                reason,
            } => {
                if is_reserved_name(&name) {
                    return Err(AdapterError::Catalog(Error::new(
                        ErrorKind::ReservedReplicaName(name),
                    )));
                }
                let cluster = state.get_cluster(cluster_id);
                tx.insert_cluster_replica(cluster_id, id, &name, config.clone().into(), owner_id)?;
                if let ReplicaLocation::Managed(ManagedReplicaLocation {
                    size,
                    disk,
                    billed_as,
                    internal,
                    ..
                }) = &config.location
                {
                    let (reason, scheduling_policies) = reason.into_audit_log();
                    let details = EventDetails::CreateClusterReplicaV2(
                        mz_audit_log::CreateClusterReplicaV2 {
                            cluster_id: cluster_id.to_string(),
                            cluster_name: cluster.name.clone(),
                            replica_id: Some(id.to_string()),
                            replica_name: name.clone(),
                            logical_size: size.clone(),
                            disk: *disk,
                            billed_as: billed_as.clone(),
                            internal: *internal,
                            reason,
                            scheduling_policies,
                        },
                    );
                    state.add_to_audit_log(
                        oracle_write_ts,
                        session,
                        tx,
                        builtin_table_updates,
                        audit_events,
                        EventType::Create,
                        ObjectType::ClusterReplica,
                        details,
                    )?;
                }
                let num_processes = config.location.num_processes();
                state.insert_cluster_replica(cluster_id, name.clone(), id, config, owner_id);
                builtin_table_updates
                    .extend(state.pack_cluster_replica_update(cluster_id, &name, 1));
                for process_id in 0..num_processes {
                    let update = state.pack_cluster_replica_status_update(
                        cluster_id,
                        id,
                        u64::cast_from(process_id),
                        1,
                    );
                    builtin_table_updates.push(update);
                }
            }
            Op::CreateItem {
                id,
                name,
                item,
                owner_id,
            } => {
                state.check_unstable_dependencies(&item)?;

                if item.is_storage_collection() {
                    storage_collections_to_create.insert(id);
                }

                let system_user = session.map_or(false, |s| s.user().is_system_user());
                if !system_user {
                    if let Some(id @ ClusterId::System(_)) = item.cluster_id() {
                        let cluster_name = state.clusters_by_id[&id].name.clone();
                        return Err(AdapterError::Catalog(Error::new(
                            ErrorKind::ReadOnlyCluster(cluster_name),
                        )));
                    }
                }

                let owner_privileges = vec![rbac::owner_privilege(item.typ().into(), owner_id)];
                let default_privileges = state
                    .default_privileges
                    .get_applicable_privileges(
                        owner_id,
                        name.qualifiers.database_spec.id(),
                        Some(name.qualifiers.schema_spec.into()),
                        item.typ().into(),
                    )
                    .map(|item| item.mz_acl_item(owner_id));
                // mz_support can read all progress sources.
                let progress_source_privilege = if item.is_progress_source() {
                    Some(MzAclItem {
                        grantee: MZ_SUPPORT_ROLE_ID,
                        grantor: owner_id,
                        acl_mode: AclMode::SELECT,
                    })
                } else {
                    None
                };
                let privileges: Vec<_> = merge_mz_acl_items(
                    owner_privileges
                        .into_iter()
                        .chain(default_privileges)
                        .chain(progress_source_privilege),
                )
                .collect();

                let oid;

                if item.is_temporary() {
                    if name.qualifiers.database_spec != ResolvedDatabaseSpecifier::Ambient
                        || name.qualifiers.schema_spec != SchemaSpecifier::Temporary
                    {
                        return Err(AdapterError::Catalog(Error::new(
                            ErrorKind::InvalidTemporarySchema,
                        )));
                    }
                    oid = tx.allocate_oid()?;
                } else {
                    if let Some(temp_id) =
                        item.uses().iter().find(|id| match state.try_get_entry(id) {
                            Some(entry) => entry.item().is_temporary(),
                            None => temporary_ids.contains(id),
                        })
                    {
                        let temp_item = state.get_entry(temp_id);
                        return Err(AdapterError::Catalog(Error::new(
                            ErrorKind::InvalidTemporaryDependency(temp_item.name().item.clone()),
                        )));
                    }
                    if name.qualifiers.database_spec == ResolvedDatabaseSpecifier::Ambient
                        && !system_user
                    {
                        let schema_name = state
                            .resolve_full_name(&name, session.map(|session| session.conn_id()))
                            .schema;
                        return Err(AdapterError::Catalog(Error::new(
                            ErrorKind::ReadOnlySystemSchema(schema_name),
                        )));
                    }
                    let schema_id = name.qualifiers.schema_spec.clone().into();
                    let serialized_item = item.to_serialized();
                    oid = tx.insert_user_item(
                        id,
                        schema_id,
                        &name.item,
                        serialized_item,
                        owner_id,
                        privileges.clone(),
                    )?;
                }

                if Self::should_audit_log_item(&item) {
                    let name = Self::full_name_detail(
                        &state.resolve_full_name(&name, session.map(|session| session.conn_id())),
                    );
                    let details = match &item {
                        CatalogItem::Source(s) => {
                            EventDetails::CreateSourceSinkV3(mz_audit_log::CreateSourceSinkV3 {
                                id: id.to_string(),
                                name,
                                external_type: s.source_type().to_string(),
                            })
                        }
                        CatalogItem::Sink(s) => {
                            EventDetails::CreateSourceSinkV3(mz_audit_log::CreateSourceSinkV3 {
                                id: id.to_string(),
                                name,
                                external_type: s.sink_type().to_string(),
                            })
                        }
                        _ => EventDetails::IdFullNameV1(IdFullNameV1 {
                            id: id.to_string(),
                            name,
                        }),
                    };
                    state.add_to_audit_log(
                        oracle_write_ts,
                        session,
                        tx,
                        builtin_table_updates,
                        audit_events,
                        EventType::Create,
                        catalog_type_to_audit_object_type(item.typ()),
                        details,
                    )?;
                }
                state.insert_item(
                    id,
                    oid,
                    name,
                    item,
                    owner_id,
                    PrivilegeMap::from_mz_acl_items(privileges),
                );
                builtin_table_updates.extend(state.pack_item_update(id, 1));
            }
            Op::Comment {
                object_id,
                sub_component,
                comment,
            } => {
                tx.update_comment(object_id, sub_component, comment.clone())?;
                let prev_comment =
                    state
                        .comments
                        .update_comment(object_id, sub_component, comment.clone());

                // If we're replacing or deleting a comment, we need to issue a retraction for
                // the previous value.
                if let Some(prev) = prev_comment {
                    builtin_table_updates.push(state.pack_comment_update(
                        object_id,
                        sub_component,
                        &prev,
                        -1,
                    ));
                }

                if let Some(new) = comment {
                    builtin_table_updates.push(state.pack_comment_update(
                        object_id,
                        sub_component,
                        &new,
                        1,
                    ));
                }
            }
            Op::DropObjects(drop_object_infos) => {
                // Generate all of the objects that need to get dropped.
                let delta = ObjectsToDrop::generate(drop_object_infos, state, session)?;

                // Drop any associated comments.
                let deleted = tx.drop_comments(&delta.comments)?;
                let dropped = state.comments.drop_comments(&delta.comments);
                mz_ore::soft_assert_eq_or_log!(
                    deleted,
                    dropped,
                    "transaction and state out of sync"
                );

                let updates = dropped.into_iter().map(|(id, col_pos, comment)| {
                    state.pack_comment_update(id, col_pos, &comment, -1)
                });
                builtin_table_updates.extend(updates);

                // Drop any items.
                let items_to_drop = delta
                    .items
                    .iter()
                    .filter(|id| !state.get_entry(id).item().is_temporary())
                    .copied()
                    .collect();
                tx.remove_items(&items_to_drop)?;

                for item_id in delta.items {
                    let entry = state.get_entry(&item_id);

                    if entry.item().is_storage_collection() {
                        storage_collections_to_drop.insert(item_id);
                    }

                    builtin_table_updates.extend(state.pack_item_update(item_id, -1));
                    if Self::should_audit_log_item(entry.item()) {
                        state.add_to_audit_log(
                            oracle_write_ts,
                            session,
                            tx,
                            builtin_table_updates,
                            audit_events,
                            EventType::Drop,
                            catalog_type_to_audit_object_type(entry.item().typ()),
                            EventDetails::IdFullNameV1(IdFullNameV1 {
                                id: item_id.to_string(),
                                name: Self::full_name_detail(&state.resolve_full_name(
                                    entry.name(),
                                    session.map(|session| session.conn_id()),
                                )),
                            }),
                        )?;
                    }
                    state.drop_item(item_id);
                }

                // Drop any schemas.
                let schemas = delta
                    .schemas
                    .iter()
                    .map(|(schema_spec, database_spec)| {
                        (SchemaId::from(schema_spec), *database_spec)
                    })
                    .collect();
                tx.remove_schemas(&schemas)?;

                for (schema_spec, database_spec) in delta.schemas {
                    let schema = state.get_schema(
                        &database_spec,
                        &schema_spec,
                        session
                            .map(|session| session.conn_id())
                            .unwrap_or(&SYSTEM_CONN_ID),
                    );

                    let schema_id = SchemaId::from(schema_spec);
                    let database_id = match database_spec {
                        ResolvedDatabaseSpecifier::Ambient => None,
                        ResolvedDatabaseSpecifier::Id(database_id) => Some(database_id),
                    };

                    builtin_table_updates.push(state.pack_schema_update(
                        &database_spec,
                        &schema_id,
                        -1,
                    ));
                    state.add_to_audit_log(
                        oracle_write_ts,
                        session,
                        tx,
                        builtin_table_updates,
                        audit_events,
                        EventType::Drop,
                        ObjectType::Schema,
                        EventDetails::SchemaV2(mz_audit_log::SchemaV2 {
                            id: schema_id.to_string(),
                            name: schema.name.schema.to_string(),
                            database_name: database_id
                                .map(|database_id| state.database_by_id[&database_id].name.clone()),
                        }),
                    )?;

                    if let ResolvedDatabaseSpecifier::Id(database_id) = database_spec {
                        let db = state
                            .database_by_id
                            .get_mut(&database_id)
                            .expect("catalog out of sync");
                        let schema = &db.schemas_by_id[&schema_id];
                        db.schemas_by_name.remove(&schema.name.schema);
                        db.schemas_by_id.remove(&schema_id);
                    }
                }

                // Drop any databases.
                tx.remove_databases(&delta.databases)?;

                for database_id in delta.databases {
                    let database = state.get_database(&database_id).clone();

                    builtin_table_updates.push(state.pack_database_update(&database_id, -1));
                    state.add_to_audit_log(
                        oracle_write_ts,
                        session,
                        tx,
                        builtin_table_updates,
                        audit_events,
                        EventType::Drop,
                        ObjectType::Database,
                        EventDetails::IdNameV1(mz_audit_log::IdNameV1 {
                            id: database_id.to_string(),
                            name: database.name.clone(),
                        }),
                    )?;

                    state.database_by_name.remove(database.name());
                    state.database_by_id.remove(&database_id);
                }

                // Drop any roles.
                tx.remove_roles(&delta.roles)?;

                for role_id in delta.roles {
                    builtin_table_updates.extend(state.pack_role_update(role_id, -1));

                    let role = state
                        .roles_by_id
                        .remove(&role_id)
                        .expect("catalog out of sync");
                    state.roles_by_name.remove(role.name());

                    state.add_to_audit_log(
                        oracle_write_ts,
                        session,
                        tx,
                        builtin_table_updates,
                        audit_events,
                        EventType::Drop,
                        ObjectType::Role,
                        EventDetails::IdNameV1(mz_audit_log::IdNameV1 {
                            id: role.id.to_string(),
                            name: role.name.clone(),
                        }),
                    )?;
                    info!("drop role {}", role.name());
                }

                // Drop any replicas.
                let replicas = delta.replicas.keys().copied().collect();
                tx.remove_cluster_replicas(&replicas)?;

                for (replica_id, (cluster_id, reason)) in delta.replicas {
                    let cluster = state.get_cluster(cluster_id);
                    let replica = cluster.replica(replica_id).expect("Must exist");

                    for process_id in replica.process_status.keys() {
                        let update = state.pack_cluster_replica_status_update(
                            cluster_id,
                            replica_id,
                            *process_id,
                            -1,
                        );
                        builtin_table_updates.push(update);
                    }

                    builtin_table_updates.extend(state.pack_cluster_replica_update(
                        cluster_id,
                        &replica.name,
                        -1,
                    ));

                    let (reason, scheduling_policies) = reason.into_audit_log();
                    let details =
                        EventDetails::DropClusterReplicaV2(mz_audit_log::DropClusterReplicaV2 {
                            cluster_id: cluster_id.to_string(),
                            cluster_name: cluster.name.clone(),
                            replica_id: Some(replica_id.to_string()),
                            replica_name: replica.name.clone(),
                            reason,
                            scheduling_policies,
                        });
                    state.add_to_audit_log(
                        oracle_write_ts,
                        session,
                        tx,
                        builtin_table_updates,
                        audit_events,
                        EventType::Drop,
                        ObjectType::ClusterReplica,
                        details,
                    )?;

                    let cluster = state
                        .clusters_by_id
                        .get_mut(&cluster_id)
                        .expect("can only drop replicas from known instances");
                    cluster.remove_replica(replica_id);
                }

                // Drop any clusters.
                tx.remove_clusters(&delta.clusters)?;

                for cluster_id in delta.clusters {
                    let cluster = state.get_cluster(cluster_id);

                    builtin_table_updates.extend(state.pack_cluster_update(&cluster.name, -1));
                    for id in cluster.log_indexes.values() {
                        builtin_table_updates.extend(state.pack_item_update(*id, -1));
                    }

                    state.add_to_audit_log(
                        oracle_write_ts,
                        session,
                        tx,
                        builtin_table_updates,
                        audit_events,
                        EventType::Drop,
                        ObjectType::Cluster,
                        EventDetails::IdNameV1(mz_audit_log::IdNameV1 {
                            id: cluster.id.to_string(),
                            name: cluster.name.clone(),
                        }),
                    )?;
                    let cluster = state
                        .clusters_by_id
                        .remove(&cluster_id)
                        .expect("can only drop known clusters");
                    state.clusters_by_name.remove(&cluster.name);

                    for id in cluster.log_indexes.values() {
                        state.drop_item(*id);
                    }

                    assert!(
                        cluster.bound_objects.is_empty() && cluster.replicas().next().is_none(),
                        "not all items dropped before cluster"
                    );
                }
            }
            Op::GrantRole {
                role_id,
                member_id,
                grantor_id,
            } => {
                state.ensure_not_reserved_role(&member_id)?;
                state.ensure_grantable_role(&role_id)?;
                if state.collect_role_membership(&role_id).contains(&member_id) {
                    let group_role = state.get_role(&role_id);
                    let member_role = state.get_role(&member_id);
                    return Err(AdapterError::Catalog(Error::new(
                        ErrorKind::CircularRoleMembership {
                            role_name: group_role.name().to_string(),
                            member_name: member_role.name().to_string(),
                        },
                    )));
                }
                let member_role = state.get_role_mut(&member_id);
                member_role.membership.map.insert(role_id, grantor_id);
                tx.update_role(member_id, member_role.clone().into())?;
                builtin_table_updates.push(state.pack_role_members_update(role_id, member_id, 1));

                state.add_to_audit_log(
                    oracle_write_ts,
                    session,
                    tx,
                    builtin_table_updates,
                    audit_events,
                    EventType::Grant,
                    ObjectType::Role,
                    EventDetails::GrantRoleV2(mz_audit_log::GrantRoleV2 {
                        role_id: role_id.to_string(),
                        member_id: member_id.to_string(),
                        grantor_id: grantor_id.to_string(),
                        executed_by: session
                            .map(|session| session.authenticated_role_id())
                            .unwrap_or(&MZ_SYSTEM_ROLE_ID)
                            .to_string(),
                    }),
                )?;
            }
            Op::RevokeRole {
                role_id,
                member_id,
                grantor_id,
            } => {
                state.ensure_not_reserved_role(&member_id)?;
                state.ensure_grantable_role(&role_id)?;
                builtin_table_updates.push(state.pack_role_members_update(role_id, member_id, -1));
                let member_role = state.get_role_mut(&member_id);
                member_role.membership.map.remove(&role_id);
                tx.update_role(member_id, member_role.clone().into())?;

                state.add_to_audit_log(
                    oracle_write_ts,
                    session,
                    tx,
                    builtin_table_updates,
                    audit_events,
                    EventType::Revoke,
                    ObjectType::Role,
                    EventDetails::RevokeRoleV2(mz_audit_log::RevokeRoleV2 {
                        role_id: role_id.to_string(),
                        member_id: member_id.to_string(),
                        grantor_id: grantor_id.to_string(),
                        executed_by: session
                            .map(|session| session.authenticated_role_id())
                            .unwrap_or(&MZ_SYSTEM_ROLE_ID)
                            .to_string(),
                    }),
                )?;
            }
            Op::UpdatePrivilege {
                target_id,
                privilege,
                variant,
            } => {
                let update_privilege_fn = |privileges: &mut PrivilegeMap| match variant {
                    UpdatePrivilegeVariant::Grant => {
                        privileges.grant(privilege);
                    }
                    UpdatePrivilegeVariant::Revoke => {
                        privileges.revoke(&privilege);
                    }
                };
                match &target_id {
                    SystemObjectId::Object(object_id) => match object_id {
                        ObjectId::Cluster(id) => {
                            let cluster_name = state.get_cluster(*id).name().to_string();
                            builtin_table_updates
                                .extend(state.pack_cluster_update(&cluster_name, -1));
                            let cluster = state.get_cluster_mut(*id);
                            update_privilege_fn(&mut cluster.privileges);
                            tx.update_cluster(*id, cluster.clone().into())?;
                            builtin_table_updates
                                .extend(state.pack_cluster_update(&cluster_name, 1));
                        }
                        ObjectId::Database(id) => {
                            builtin_table_updates.push(state.pack_database_update(id, -1));
                            let database = state.get_database_mut(id);
                            update_privilege_fn(&mut database.privileges);
                            let database = state.get_database(id);
                            tx.update_database(*id, database.clone().into())?;
                            builtin_table_updates.push(state.pack_database_update(id, 1));
                        }
                        ObjectId::Schema((database_spec, schema_spec)) => {
                            let schema_id = schema_spec.clone().into();
                            builtin_table_updates.push(state.pack_schema_update(
                                database_spec,
                                &schema_id,
                                -1,
                            ));
                            let schema = state.get_schema_mut(
                                database_spec,
                                schema_spec,
                                session
                                    .map(|session| session.conn_id())
                                    .unwrap_or(&SYSTEM_CONN_ID),
                            );
                            update_privilege_fn(&mut schema.privileges);
                            let database_id = match &database_spec {
                                ResolvedDatabaseSpecifier::Ambient => None,
                                ResolvedDatabaseSpecifier::Id(id) => Some(*id),
                            };
                            tx.update_schema(
                                schema_id,
                                schema.clone().into_durable_schema(database_id),
                            )?;
                            builtin_table_updates.push(state.pack_schema_update(
                                database_spec,
                                &schema_id,
                                1,
                            ));
                        }
                        ObjectId::Item(id) => {
                            builtin_table_updates.extend(state.pack_item_update(*id, -1));
                            let entry = state.get_entry_mut(id);
                            update_privilege_fn(&mut entry.privileges);
                            if !entry.item().is_temporary() {
                                tx.update_item(*id, entry.clone().into())?;
                            }
                            builtin_table_updates.extend(state.pack_item_update(*id, 1));
                        }
                        ObjectId::Role(_) | ObjectId::ClusterReplica(_) => {}
                    },
                    SystemObjectId::System => {
                        if let Some(existing_privilege) = state
                            .system_privileges
                            .get_acl_item(&privilege.grantee, &privilege.grantor)
                        {
                            builtin_table_updates.push(
                                state.pack_system_privileges_update(existing_privilege.clone(), -1),
                            );
                        }
                        update_privilege_fn(&mut state.system_privileges);
                        let new_privilege = state
                            .system_privileges
                            .get_acl_item(&privilege.grantee, &privilege.grantor);
                        tx.set_system_privilege(
                            privilege.grantee,
                            privilege.grantor,
                            new_privilege.map(|new_privilege| new_privilege.acl_mode),
                        )?;
                        if let Some(new_privilege) = new_privilege {
                            builtin_table_updates.push(
                                state.pack_system_privileges_update(new_privilege.clone(), 1),
                            );
                        }
                    }
                }
                let object_type = state.get_system_object_type(&target_id);
                let object_id_str = match &target_id {
                    SystemObjectId::System => "SYSTEM".to_string(),
                    SystemObjectId::Object(id) => id.to_string(),
                };
                state.add_to_audit_log(
                    oracle_write_ts,
                    session,
                    tx,
                    builtin_table_updates,
                    audit_events,
                    variant.into(),
                    system_object_type_to_audit_object_type(&object_type),
                    EventDetails::UpdatePrivilegeV1(mz_audit_log::UpdatePrivilegeV1 {
                        object_id: object_id_str,
                        grantee_id: privilege.grantee.to_string(),
                        grantor_id: privilege.grantor.to_string(),
                        privileges: privilege.acl_mode.to_string(),
                    }),
                )?;
            }
            Op::UpdateDefaultPrivilege {
                privilege_object,
                privilege_acl_item,
                variant,
            } => {
                if let Some(acl_mode) = state
                    .default_privileges
                    .get_privileges_for_grantee(&privilege_object, &privilege_acl_item.grantee)
                {
                    builtin_table_updates.push(state.pack_default_privileges_update(
                        &privilege_object,
                        &privilege_acl_item.grantee,
                        acl_mode,
                        -1,
                    ));
                }
                match variant {
                    UpdatePrivilegeVariant::Grant => state
                        .default_privileges
                        .grant(privilege_object.clone(), privilege_acl_item.clone()),
                    UpdatePrivilegeVariant::Revoke => state
                        .default_privileges
                        .revoke(&privilege_object, &privilege_acl_item),
                }
                let new_acl_mode = state
                    .default_privileges
                    .get_privileges_for_grantee(&privilege_object, &privilege_acl_item.grantee);
                tx.set_default_privilege(
                    privilege_object.role_id,
                    privilege_object.database_id,
                    privilege_object.schema_id,
                    privilege_object.object_type,
                    privilege_acl_item.grantee,
                    new_acl_mode.cloned(),
                )?;
                if let Some(new_acl_mode) = new_acl_mode {
                    builtin_table_updates.push(state.pack_default_privileges_update(
                        &privilege_object,
                        &privilege_acl_item.grantee,
                        new_acl_mode,
                        1,
                    ));
                }
                state.add_to_audit_log(
                    oracle_write_ts,
                    session,
                    tx,
                    builtin_table_updates,
                    audit_events,
                    variant.into(),
                    object_type_to_audit_object_type(privilege_object.object_type),
                    EventDetails::AlterDefaultPrivilegeV1(mz_audit_log::AlterDefaultPrivilegeV1 {
                        role_id: privilege_object.role_id.to_string(),
                        database_id: privilege_object.database_id.map(|id| id.to_string()),
                        schema_id: privilege_object.schema_id.map(|id| id.to_string()),
                        grantee_id: privilege_acl_item.grantee.to_string(),
                        privileges: privilege_acl_item.acl_mode.to_string(),
                    }),
                )?;
            }
            Op::RenameCluster {
                id,
                name,
                to_name,
                check_reserved_names,
            } => {
                if id.is_system() {
                    return Err(AdapterError::Catalog(Error::new(
                        ErrorKind::ReadOnlyCluster(name.clone()),
                    )));
                }
                if check_reserved_names && is_reserved_name(&to_name) {
                    return Err(AdapterError::Catalog(Error::new(
                        ErrorKind::ReservedClusterName(to_name),
                    )));
                }
                tx.rename_cluster(id, &name, &to_name)?;
                builtin_table_updates.extend(state.pack_cluster_update(&name, -1));
                state.rename_cluster(id, to_name.clone());
                builtin_table_updates.extend(state.pack_cluster_update(&to_name, 1));
                state.add_to_audit_log(
                    oracle_write_ts,
                    session,
                    tx,
                    builtin_table_updates,
                    audit_events,
                    EventType::Alter,
                    ObjectType::Cluster,
                    EventDetails::RenameClusterV1(mz_audit_log::RenameClusterV1 {
                        id: id.to_string(),
                        old_name: name.clone(),
                        new_name: to_name.clone(),
                    }),
                )?;
                info!("rename cluster {name} to {to_name}");
            }
            Op::RenameClusterReplica {
                cluster_id,
                replica_id,
                name,
                to_name,
            } => {
                if is_reserved_name(&to_name) {
                    return Err(AdapterError::Catalog(Error::new(
                        ErrorKind::ReservedReplicaName(to_name),
                    )));
                }
                tx.rename_cluster_replica(replica_id, &name, &to_name)?;
                builtin_table_updates.extend(state.pack_cluster_replica_update(
                    cluster_id,
                    name.replica.as_str(),
                    -1,
                ));
                state.rename_cluster_replica(cluster_id, replica_id, to_name.clone());
                builtin_table_updates
                    .extend(state.pack_cluster_replica_update(cluster_id, &to_name, 1));
                state.add_to_audit_log(
                    oracle_write_ts,
                    session,
                    tx,
                    builtin_table_updates,
                    audit_events,
                    EventType::Alter,
                    ObjectType::ClusterReplica,
                    EventDetails::RenameClusterReplicaV1(mz_audit_log::RenameClusterReplicaV1 {
                        cluster_id: cluster_id.to_string(),
                        replica_id: replica_id.to_string(),
                        old_name: name.replica.as_str().to_string(),
                        new_name: to_name.clone(),
                    }),
                )?;
                info!("rename cluster replica {name} to {to_name}");
            }
            Op::RenameItem {
                id,
                to_name,
                current_full_name,
            } => {
                let mut updates = Vec::new();

                let entry = state.get_entry(&id);
                if let CatalogItem::Type(_) = entry.item() {
                    return Err(AdapterError::Catalog(Error::new(ErrorKind::TypeRename(
                        current_full_name.to_string(),
                    ))));
                }

                if entry.id().is_system() {
                    let name = state
                        .resolve_full_name(entry.name(), session.map(|session| session.conn_id()));
                    return Err(AdapterError::Catalog(Error::new(ErrorKind::ReadOnlyItem(
                        name.to_string(),
                    ))));
                }

                let mut to_full_name = current_full_name.clone();
                to_full_name.item.clone_from(&to_name);

                let mut to_qualified_name = entry.name().clone();
                to_qualified_name.item.clone_from(&to_name);

                let details = EventDetails::RenameItemV1(mz_audit_log::RenameItemV1 {
                    id: id.to_string(),
                    old_name: Self::full_name_detail(&current_full_name),
                    new_name: Self::full_name_detail(&to_full_name),
                });
                if Self::should_audit_log_item(entry.item()) {
                    state.add_to_audit_log(
                        oracle_write_ts,
                        session,
                        tx,
                        builtin_table_updates,
                        audit_events,
                        EventType::Alter,
                        catalog_type_to_audit_object_type(entry.item().typ()),
                        details,
                    )?;
                }

                // Rename item itself.
                let mut new_entry = entry.clone();
                new_entry.name.item.clone_from(&to_name);
                new_entry.item = entry
                    .item()
                    .rename_item_refs(current_full_name.clone(), to_full_name.item.clone(), true)
                    .map_err(|e| {
                        Error::new(ErrorKind::from(AmbiguousRename {
                            depender: state
                                .resolve_full_name(entry.name(), entry.conn_id())
                                .to_string(),
                            dependee: state
                                .resolve_full_name(entry.name(), entry.conn_id())
                                .to_string(),
                            message: e,
                        }))
                    })?;

                for id in entry.referenced_by() {
                    let dependent_item = state.get_entry(id);
                    let mut to_entry = dependent_item.clone();
                    to_entry.item = dependent_item
                        .item()
                        .rename_item_refs(
                            current_full_name.clone(),
                            to_full_name.item.clone(),
                            false,
                        )
                        .map_err(|e| {
                            Error::new(ErrorKind::from(AmbiguousRename {
                                depender: state
                                    .resolve_full_name(
                                        dependent_item.name(),
                                        dependent_item.conn_id(),
                                    )
                                    .to_string(),
                                dependee: state
                                    .resolve_full_name(entry.name(), entry.conn_id())
                                    .to_string(),
                                message: e,
                            }))
                        })?;

                    if !new_entry.item().is_temporary() {
                        tx.update_item(*id, to_entry.clone().into())?;
                    }
                    builtin_table_updates.extend(state.pack_item_update(*id, -1));

                    updates.push((id.clone(), dependent_item.name().clone(), to_entry.item));
                }
                if !new_entry.item().is_temporary() {
                    tx.update_item(id, new_entry.clone().into())?;
                }
                builtin_table_updates.extend(state.pack_item_update(id, -1));
                updates.push((id, to_qualified_name, new_entry.item));
                for (id, to_name, to_item) in updates {
                    Self::update_item(state, builtin_table_updates, id, to_name, to_item)?;
                }
            }
            Op::RenameSchema {
                database_spec,
                schema_spec,
                new_name,
                check_reserved_names,
            } => {
                if check_reserved_names && is_reserved_name(&new_name) {
                    return Err(AdapterError::Catalog(Error::new(
                        ErrorKind::ReservedSchemaName(new_name),
                    )));
                }

                let conn_id = session
                    .map(|session| session.conn_id())
                    .unwrap_or(&SYSTEM_CONN_ID);

                let schema = state.get_schema(&database_spec, &schema_spec, conn_id);
                let cur_name = schema.name().schema.clone();

                let ResolvedDatabaseSpecifier::Id(database_id) = database_spec else {
                    return Err(AdapterError::Catalog(Error::new(
                        ErrorKind::AmbientSchemaRename(cur_name),
                    )));
                };
                let database = state.get_database(&database_id);
                let database_name = &database.name;

                let mut updates = Vec::new();
                let mut items_to_update = BTreeMap::new();

                let mut update_item = |id| {
                    if items_to_update.contains_key(id) {
                        return Ok(());
                    }

                    let entry = state.get_entry(id);

                    // Update our item.
                    let mut new_entry = entry.clone();
                    new_entry.item = entry
                        .item
                        .rename_schema_refs(database_name, &cur_name, &new_name)
                        .map_err(|(s, _i)| {
                            Error::new(ErrorKind::from(AmbiguousRename {
                                depender: state
                                    .resolve_full_name(entry.name(), entry.conn_id())
                                    .to_string(),
                                dependee: format!("{database_name}.{cur_name}"),
                                message: format!("ambiguous reference to schema named {s}"),
                            }))
                        })?;

                    // Queue updates for Catalog storage and Builtin Tables.
                    if !new_entry.item().is_temporary() {
                        items_to_update.insert(*id, new_entry.clone().into());
                    }
                    builtin_table_updates.extend(state.pack_item_update(*id, -1));
                    updates.push((id.clone(), entry.name().clone(), new_entry.item));

                    Ok::<_, AdapterError>(())
                };

                // Update all of the items in the schema.
                for (_name, item_id) in &schema.items {
                    // Update the item itself.
                    update_item(item_id)?;

                    // Update everything that depends on this item.
                    for id in state.get_entry(item_id).referenced_by() {
                        update_item(id)?;
                    }
                }
                // Note: When updating the transaction it's very important that we update the
                // items as a whole group, otherwise we exhibit quadratic behavior.
                tx.update_items(items_to_update)?;

                // Renaming temporary schemas is not supported.
                let SchemaSpecifier::Id(schema_id) = *schema.id() else {
                    let schema_name = schema.name().schema.clone();
                    return Err(AdapterError::Catalog(crate::catalog::Error::new(
                        crate::catalog::ErrorKind::ReadOnlySystemSchema(schema_name),
                    )));
                };
                // Delete the old schema from the builtin table.
                builtin_table_updates.push(state.pack_schema_update(
                    &database_spec,
                    &schema_id,
                    -1,
                ));

                // Add an entry to the audit log.
                let database_name = database_spec
                    .id()
                    .map(|id| state.get_database(&id).name.clone());
                let details = EventDetails::RenameSchemaV1(mz_audit_log::RenameSchemaV1 {
                    id: schema_id.to_string(),
                    old_name: schema.name().schema.clone(),
                    new_name: new_name.clone(),
                    database_name,
                });
                state.add_to_audit_log(
                    oracle_write_ts,
                    session,
                    tx,
                    builtin_table_updates,
                    audit_events,
                    EventType::Alter,
                    mz_audit_log::ObjectType::Schema,
                    details,
                )?;

                // Update the schema itself.
                let schema = state.get_schema_mut(&database_spec, &schema_spec, conn_id);
                let old_name = schema.name().schema.clone();
                schema.name.schema.clone_from(&new_name);
                let new_schema = schema.clone().into_durable_schema(database_spec.id());
                tx.update_schema(schema_id, new_schema)?;

                // Update the references to this schema.
                match (&database_spec, &schema_spec) {
                    (ResolvedDatabaseSpecifier::Id(db_id), SchemaSpecifier::Id(_)) => {
                        let database = state.get_database_mut(db_id);
                        let Some(prev_id) = database.schemas_by_name.remove(&old_name) else {
                            panic!("Catalog state inconsistency! Expected to find schema with name {old_name}");
                        };
                        database.schemas_by_name.insert(new_name.clone(), prev_id);
                    }
                    (ResolvedDatabaseSpecifier::Ambient, SchemaSpecifier::Id(_)) => {
                        let Some(prev_id) = state.ambient_schemas_by_name.remove(&old_name) else {
                            panic!("Catalog state inconsistency! Expected to find schema with name {old_name}");
                        };
                        state
                            .ambient_schemas_by_name
                            .insert(new_name.clone(), prev_id);
                    }
                    (ResolvedDatabaseSpecifier::Ambient, SchemaSpecifier::Temporary) => {
                        // No external references to rename.
                    }
                    (ResolvedDatabaseSpecifier::Id(_), SchemaSpecifier::Temporary) => {
                        unreachable!("temporary schemas are in the ambient database")
                    }
                }

                // Update the new schema in the builtin table.
                builtin_table_updates.push(state.pack_schema_update(&database_spec, &schema_id, 1));

                for (id, new_name, new_item) in updates {
                    Self::update_item(state, builtin_table_updates, id, new_name, new_item)?;
                }
            }
            Op::UpdateOwner { id, new_owner } => {
                let conn_id = session
                    .map(|session| session.conn_id())
                    .unwrap_or(&SYSTEM_CONN_ID);
                let old_owner = state
                    .get_owner_id(&id, conn_id)
                    .expect("cannot update the owner of an object without an owner");
                match &id {
                    ObjectId::Cluster(id) => {
                        let cluster_name = state.get_cluster(*id).name().to_string();
                        if id.is_system() {
                            return Err(AdapterError::Catalog(Error::new(
                                ErrorKind::ReadOnlyCluster(cluster_name),
                            )));
                        }
                        builtin_table_updates.extend(state.pack_cluster_update(&cluster_name, -1));
                        let cluster = state.get_cluster_mut(*id);
                        Self::update_privilege_owners(
                            &mut cluster.privileges,
                            cluster.owner_id,
                            new_owner,
                        );
                        cluster.owner_id = new_owner;
                        tx.update_cluster(*id, cluster.clone().into())?;
                        builtin_table_updates.extend(state.pack_cluster_update(&cluster_name, 1));
                    }
                    ObjectId::ClusterReplica((cluster_id, replica_id)) => {
                        let cluster = state.get_cluster(*cluster_id);
                        let replica_name = cluster
                            .replica(*replica_id)
                            .expect("catalog out of sync")
                            .name
                            .clone();
                        if replica_id.is_system() {
                            return Err(AdapterError::Catalog(Error::new(
                                ErrorKind::ReadOnlyClusterReplica(replica_name),
                            )));
                        }
                        builtin_table_updates.extend(state.pack_cluster_replica_update(
                            *cluster_id,
                            &replica_name,
                            -1,
                        ));
                        let cluster = state.get_cluster_mut(*cluster_id);
                        let replica = cluster
                            .replica_mut(*replica_id)
                            .expect("catalog out of sync");
                        replica.owner_id = new_owner;
                        tx.update_cluster_replica(*replica_id, replica.clone().into())?;
                        builtin_table_updates.extend(state.pack_cluster_replica_update(
                            *cluster_id,
                            &replica_name,
                            1,
                        ));
                    }
                    ObjectId::Database(id) => {
                        let database = state.get_database(id);
                        if id.is_system() {
                            return Err(AdapterError::Catalog(Error::new(
                                ErrorKind::ReadOnlyDatabase(database.name().to_string()),
                            )));
                        }
                        builtin_table_updates.push(state.pack_database_update(id, -1));
                        let database = state.get_database_mut(id);
                        Self::update_privilege_owners(
                            &mut database.privileges,
                            database.owner_id,
                            new_owner,
                        );
                        database.owner_id = new_owner;
                        let database = state.get_database(id);
                        tx.update_database(*id, database.clone().into())?;
                        builtin_table_updates.push(state.pack_database_update(id, 1));
                    }
                    ObjectId::Schema((database_spec, schema_spec)) => {
                        let schema_id: SchemaId = schema_spec.clone().into();
                        if schema_id.is_system() {
                            let schema = state.get_schema(database_spec, schema_spec, conn_id);
                            let name = schema.name();
                            let full_name = state.resolve_full_schema_name(name);
                            return Err(AdapterError::Catalog(Error::new(
                                ErrorKind::ReadOnlySystemSchema(full_name.to_string()),
                            )));
                        }
                        builtin_table_updates.push(state.pack_schema_update(
                            database_spec,
                            &schema_id,
                            -1,
                        ));
                        let schema = state.get_schema_mut(database_spec, schema_spec, conn_id);
                        Self::update_privilege_owners(
                            &mut schema.privileges,
                            schema.owner_id,
                            new_owner,
                        );
                        schema.owner_id = new_owner;
                        let database_id = match database_spec {
                            ResolvedDatabaseSpecifier::Ambient => None,
                            ResolvedDatabaseSpecifier::Id(id) => Some(id),
                        };
                        tx.update_schema(
                            schema_id,
                            schema.clone().into_durable_schema(database_id.copied()),
                        )?;
                        builtin_table_updates.push(state.pack_schema_update(
                            database_spec,
                            &schema_id,
                            1,
                        ));
                    }
                    ObjectId::Item(id) => {
                        if id.is_system() {
                            let entry = state.get_entry(id);
                            let full_name = state.resolve_full_name(
                                entry.name(),
                                session.map(|session| session.conn_id()),
                            );
                            return Err(AdapterError::Catalog(Error::new(
                                ErrorKind::ReadOnlyItem(full_name.to_string()),
                            )));
                        }
                        builtin_table_updates.extend(state.pack_item_update(*id, -1));
                        let entry = state.get_entry_mut(id);
                        Self::update_privilege_owners(
                            &mut entry.privileges,
                            entry.owner_id,
                            new_owner,
                        );
                        entry.owner_id = new_owner;
                        if !entry.item().is_temporary() {
                            tx.update_item(*id, entry.clone().into())?;
                        }
                        builtin_table_updates.extend(state.pack_item_update(*id, 1));
                    }
                    ObjectId::Role(_) => unreachable!("roles have no owner"),
                }
                let object_type = state.get_object_type(&id);
                state.add_to_audit_log(
                    oracle_write_ts,
                    session,
                    tx,
                    builtin_table_updates,
                    audit_events,
                    EventType::Alter,
                    object_type_to_audit_object_type(object_type),
                    EventDetails::UpdateOwnerV1(mz_audit_log::UpdateOwnerV1 {
                        object_id: id.to_string(),
                        old_owner_id: old_owner.to_string(),
                        new_owner_id: new_owner.to_string(),
                    }),
                )?;
            }
            Op::UpdateClusterConfig { id, name, config } => {
                builtin_table_updates.extend(state.pack_cluster_update(&name, -1));
                let cluster = state.get_cluster_mut(id);
                cluster.config = config;
                tx.update_cluster(id, cluster.clone().into())?;
                builtin_table_updates.extend(state.pack_cluster_update(&name, 1));
                info!("update cluster {}", name);
            }
            Op::UpdateClusterReplicaStatus { event } => {
                builtin_table_updates.push(state.pack_cluster_replica_status_update(
                    event.cluster_id,
                    event.replica_id,
                    event.process_id,
                    -1,
                ));
                state.ensure_cluster_status(
                    event.cluster_id,
                    event.replica_id,
                    event.process_id,
                    ClusterReplicaProcessStatus {
                        status: event.status,
                        time: event.time,
                    },
                );
                builtin_table_updates.push(state.pack_cluster_replica_status_update(
                    event.cluster_id,
                    event.replica_id,
                    event.process_id,
                    1,
                ));
            }
            Op::UpdateItem { id, name, to_item } => {
                builtin_table_updates.extend(state.pack_item_update(id, -1));
                Self::update_item(
                    state,
                    builtin_table_updates,
                    id,
                    name.clone(),
                    to_item.clone(),
                )?;
                let entry = state.get_entry(&id);
                tx.update_item(id, entry.clone().into())?;

                if Self::should_audit_log_item(&to_item) {
                    let name = Self::full_name_detail(
                        &state.resolve_full_name(&name, session.map(|session| session.conn_id())),
                    );

                    state.add_to_audit_log(
                        oracle_write_ts,
                        session,
                        tx,
                        builtin_table_updates,
                        audit_events,
                        EventType::Alter,
                        catalog_type_to_audit_object_type(to_item.typ()),
                        EventDetails::UpdateItemV1(mz_audit_log::UpdateItemV1 {
                            id: id.to_string(),
                            name,
                        }),
                    )?;
                }
            }
            Op::UpdateStorageUsage {
                shard_id,
                size_bytes,
                collection_timestamp,
            } => {
                state.add_to_storage_usage(
                    tx,
                    builtin_table_updates,
                    shard_id,
                    size_bytes,
                    collection_timestamp,
                )?;
            }
            Op::UpdateSystemConfiguration { name, value } => {
                Self::update_system_configuration(state, tx, &name, value.borrow())?;
            }
            Op::ResetSystemConfiguration { name } => {
                state.remove_system_configuration(&name)?;
                tx.remove_system_config(&name);
                // This mirrors the `txn_wal_tables` "system var" into the catalog
                // storage "config" collection so that we can toggle the flag with
                // Launch Darkly, but use it in boot before Launch Darkly is available.
                if name == TXN_WAL_TABLES.name() {
                    tx.set_txn_wal_tables(state.system_configuration.txn_wal_tables())?;
                }
            }
            Op::ResetAllSystemConfiguration => {
                state.clear_system_configuration();
                tx.clear_system_configs();
                tx.set_txn_wal_tables(state.system_configuration.txn_wal_tables())?;
            }
            Op::SshTunnelConnectionsUpdates {
                builtin_table_update,
            } => {
                builtin_table_updates.push(builtin_table_update);
            }
        };
        Ok(())
    }

    pub(crate) fn update_item(
        state: &mut CatalogState,
        builtin_table_updates: &mut Vec<BuiltinTableUpdate>,
        id: GlobalId,
        to_name: QualifiedItemName,
        to_item: CatalogItem,
    ) -> Result<(), AdapterError> {
        let old_entry = state.entry_by_id.remove(&id).expect("catalog out of sync");
        info!(
            "update {} {} ({})",
            old_entry.item_type(),
            state.resolve_full_name(old_entry.name(), old_entry.conn_id()),
            id
        );

        let conn_id = old_entry.item().conn_id().unwrap_or(&SYSTEM_CONN_ID);
        let schema = state.get_schema_mut(
            &old_entry.name().qualifiers.database_spec,
            &old_entry.name().qualifiers.schema_spec,
            conn_id,
        );
        schema.items.remove(&old_entry.name().item);

        // Dropped deps
        let dropped_references: Vec<_> = old_entry
            .references()
            .0
            .difference(&to_item.references().0)
            .cloned()
            .collect();
        let dropped_uses: Vec<_> = old_entry
            .uses()
            .difference(&to_item.uses())
            .cloned()
            .collect();

        // We only need to install this item on items in the `referenced_by` of new
        // dependencies.
        let new_references: Vec<_> = to_item
            .references()
            .0
            .difference(&old_entry.references().0)
            .cloned()
            .collect();
        // We only need to install this item on items in the `used_by` of new
        // dependencies.
        let new_uses: Vec<_> = to_item
            .uses()
            .difference(&old_entry.uses())
            .cloned()
            .collect();

        let mut new_entry = old_entry.clone();
        new_entry.name = to_name;
        new_entry.item = to_item;

        schema.items.insert(new_entry.name().item.clone(), id);

        for u in dropped_references {
            // OK if we no longer have this entry because we are dropping our
            // dependency on it.
            if let Some(metadata) = state.entry_by_id.get_mut(&u) {
                metadata.referenced_by.retain(|dep_id| *dep_id != id)
            }
        }

        for u in dropped_uses {
            // OK if we no longer have this entry because we are dropping our
            // dependency on it.
            if let Some(metadata) = state.entry_by_id.get_mut(&u) {
                metadata.used_by.retain(|dep_id| *dep_id != id)
            }
        }

        for u in new_references {
            match state.entry_by_id.get_mut(&u) {
                Some(metadata) => metadata.referenced_by.push(new_entry.id()),
                None => panic!(
                    "Catalog: missing dependent catalog item {} while updating {}",
                    &u,
                    state.resolve_full_name(new_entry.name(), new_entry.conn_id())
                ),
            }
        }
        for u in new_uses {
            match state.entry_by_id.get_mut(&u) {
                Some(metadata) => metadata.used_by.push(new_entry.id()),
                None => panic!(
                    "Catalog: missing dependent catalog item {} while updating {}",
                    &u,
                    state.resolve_full_name(new_entry.name(), new_entry.conn_id())
                ),
            }
        }

        state.entry_by_id.insert(id, new_entry);
        builtin_table_updates.extend(state.pack_item_update(id, 1));
        Ok(())
    }

    pub(crate) fn update_system_configuration(
        state: &mut CatalogState,
        tx: &mut Transaction,
        name: &str,
        value: VarInput,
    ) -> Result<(), AdapterError> {
        state.insert_system_configuration(name, value)?;
        let var = state.get_system_configuration(name)?;
        tx.upsert_system_config(name, var.value())?;
        // This mirrors the `txn_wal_tables` "system var" into the catalog
        // storage "config" collection so that we can toggle the flag with
        // Launch Darkly, but use it in boot before Launch Darkly is available.
        if name == TXN_WAL_TABLES.name() {
            tx.set_txn_wal_tables(state.system_configuration.txn_wal_tables())?;
        }
        Ok(())
    }

    fn create_schema(
        state: &mut CatalogState,
        builtin_table_updates: &mut Vec<BuiltinTableUpdate>,
        id: SchemaId,
        oid: u32,
        database_id: DatabaseId,
        schema_name: String,
        owner_id: RoleId,
        privileges: PrivilegeMap,
    ) -> Result<(), AdapterError> {
        info!(
            "create schema {}.{}",
            state.get_database(&database_id).name,
            schema_name
        );
        let db = state
            .database_by_id
            .get_mut(&database_id)
            .expect("catalog out of sync");
        db.schemas_by_id.insert(
            id.clone(),
            Schema {
                name: QualifiedSchemaName {
                    database: ResolvedDatabaseSpecifier::Id(database_id.clone()),
                    schema: schema_name.clone(),
                },
                id: SchemaSpecifier::Id(id.clone()),
                oid,
                items: BTreeMap::new(),
                functions: BTreeMap::new(),
                types: BTreeMap::new(),
                owner_id,
                privileges,
            },
        );
        db.schemas_by_name.insert(schema_name, id.clone());
        builtin_table_updates.push(state.pack_schema_update(
            &ResolvedDatabaseSpecifier::Id(database_id.clone()),
            &id,
            1,
        ));
        Ok(())
    }

    /// Update privileges to reflect the new owner. Based off of PostgreSQL's
    /// implementation:
    /// <https://github.com/postgres/postgres/blob/43a33ef54e503b61f269d088f2623ba3b9484ad7/src/backend/utils/adt/acl.c#L1078-L1177>
    fn update_privilege_owners(
        privileges: &mut PrivilegeMap,
        old_owner: RoleId,
        new_owner: RoleId,
    ) {
        // TODO(jkosh44) Would be nice not to clone every privilege.
        let mut flat_privileges: Vec<_> = privileges.all_values_owned().collect();

        let mut new_present = false;
        for privilege in flat_privileges.iter_mut() {
            // Old owner's granted privilege are updated to be granted by the new
            // owner.
            if privilege.grantor == old_owner {
                privilege.grantor = new_owner;
            } else if privilege.grantor == new_owner {
                new_present = true;
            }
            // Old owner's privileges is given to the new owner.
            if privilege.grantee == old_owner {
                privilege.grantee = new_owner;
            } else if privilege.grantee == new_owner {
                new_present = true;
            }
        }

        // If the old privilege list contained references to the new owner, we may
        // have created duplicate entries. Here we try and consolidate them. This
        // is inspired by PostgreSQL's algorithm but not identical.
        if new_present {
            // Group privileges by (grantee, grantor).
            let privilege_map: BTreeMap<_, Vec<_>> =
                flat_privileges
                    .into_iter()
                    .fold(BTreeMap::new(), |mut accum, privilege| {
                        accum
                            .entry((privilege.grantee, privilege.grantor))
                            .or_default()
                            .push(privilege);
                        accum
                    });

            // Consolidate and update all privileges.
            flat_privileges = privilege_map
                .into_iter()
                .map(|((grantee, grantor), values)|
                    // Combine the acl_mode of all mz_aclitems with the same grantee and grantor.
                    values.into_iter().fold(
                        MzAclItem::empty(grantee, grantor),
                        |mut accum, mz_aclitem| {
                            accum.acl_mode =
                                accum.acl_mode.union(mz_aclitem.acl_mode);
                            accum
                        },
                    ))
                .collect();
        }

        *privileges = PrivilegeMap::from_mz_acl_items(flat_privileges);
    }
}

/// All of the objects that need to be removed in response to an [`Op::DropObjects`].
///
/// Note: Previously we used to omit a single `Op::DropObject` for every object
/// we needed to drop. But removing a batch of objects from a durable Catalog
/// Transaction is O(n) where `n` is the number of objects that exist in the
/// Catalog. This resulted in an unacceptable `O(m * n)` performance for a
/// `DROP ... CASCADE` statement.
#[derive(Debug, Default)]
pub(crate) struct ObjectsToDrop {
    pub comments: BTreeSet<CommentObjectId>,
    pub databases: BTreeSet<DatabaseId>,
    pub schemas: BTreeMap<SchemaSpecifier, ResolvedDatabaseSpecifier>,
    pub clusters: BTreeSet<ClusterId>,
    pub replicas: BTreeMap<ReplicaId, (ClusterId, ReplicaCreateDropReason)>,
    pub roles: BTreeSet<RoleId>,
    pub items: Vec<GlobalId>,
}

impl ObjectsToDrop {
    pub fn generate(
        drop_object_infos: impl IntoIterator<Item = DropObjectInfo>,
        state: &CatalogState,
        session: Option<&ConnMeta>,
    ) -> Result<Self, AdapterError> {
        let mut delta = ObjectsToDrop::default();

        for drop_object_info in drop_object_infos {
            delta.add_item(drop_object_info, state, session)?;
        }

        Ok(delta)
    }

    fn add_item(
        &mut self,
        drop_object_info: DropObjectInfo,
        state: &CatalogState,
        session: Option<&ConnMeta>,
    ) -> Result<(), AdapterError> {
        self.comments
            .insert(state.get_comment_id(drop_object_info.to_object_id()));

        match drop_object_info {
            DropObjectInfo::Database(database_id) => {
                let database = &state.database_by_id[&database_id];
                if database_id.is_system() {
                    return Err(AdapterError::Catalog(Error::new(
                        ErrorKind::ReadOnlyDatabase(database.name().to_string()),
                    )));
                }

                self.databases.insert(database_id);
            }
            DropObjectInfo::Schema((database_spec, schema_spec)) => {
                let schema = state.get_schema(
                    &database_spec,
                    &schema_spec,
                    session
                        .map(|session| session.conn_id())
                        .unwrap_or(&SYSTEM_CONN_ID),
                );
                let schema_id: SchemaId = schema_spec.into();
                if schema_id.is_system() {
                    let name = schema.name();
                    let full_name = state.resolve_full_schema_name(name);
                    return Err(AdapterError::Catalog(Error::new(
                        ErrorKind::ReadOnlySystemSchema(full_name.to_string()),
                    )));
                }

                self.schemas.insert(schema_spec, database_spec);
            }
            DropObjectInfo::Role(role_id) => {
                let name = state.get_role(&role_id).name().to_string();
                if role_id.is_system() || role_id.is_predefined() {
                    return Err(AdapterError::Catalog(Error::new(
                        ErrorKind::ReservedRoleName(name.clone()),
                    )));
                }
                state.ensure_not_reserved_role(&role_id)?;

                self.roles.insert(role_id);
            }
            DropObjectInfo::Cluster(cluster_id) => {
                let cluster = state.get_cluster(cluster_id);
                let name = &cluster.name;
                if cluster_id.is_system() {
                    return Err(AdapterError::Catalog(Error::new(
                        ErrorKind::ReadOnlyCluster(name.clone()),
                    )));
                }

                self.clusters.insert(cluster_id);
            }
            DropObjectInfo::ClusterReplica((cluster_id, replica_id, reason)) => {
                let cluster = state.get_cluster(cluster_id);
                let replica = cluster.replica(replica_id).expect("Must exist");

                self.replicas
                    .insert(replica.replica_id, (cluster.id, reason));
            }
            DropObjectInfo::Item(item_id) => {
                let entry = state.get_entry(&item_id);
                if item_id.is_system() {
                    let name = entry.name();
                    let full_name =
                        state.resolve_full_name(name, session.map(|session| session.conn_id()));
                    return Err(AdapterError::Catalog(Error::new(ErrorKind::ReadOnlyItem(
                        full_name.to_string(),
                    ))));
                }

                self.items.push(item_id);
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use mz_repr::adt::mz_acl_item::{AclMode, MzAclItem, PrivilegeMap};
    use mz_repr::role_id::RoleId;

    use crate::catalog::Catalog;

    #[mz_ore::test]
    fn test_update_privilege_owners() {
        let old_owner = RoleId::User(1);
        let new_owner = RoleId::User(2);
        let other_role = RoleId::User(3);

        // older owner exists as grantor.
        let mut privileges = PrivilegeMap::from_mz_acl_items(vec![
            MzAclItem {
                grantee: other_role,
                grantor: old_owner,
                acl_mode: AclMode::UPDATE,
            },
            MzAclItem {
                grantee: other_role,
                grantor: new_owner,
                acl_mode: AclMode::SELECT,
            },
        ]);
        Catalog::update_privilege_owners(&mut privileges, old_owner, new_owner);
        assert_eq!(1, privileges.all_values().count());
        assert_eq!(
            vec![MzAclItem {
                grantee: other_role,
                grantor: new_owner,
                acl_mode: AclMode::SELECT.union(AclMode::UPDATE)
            }],
            privileges.all_values_owned().collect::<Vec<_>>()
        );

        // older owner exists as grantee.
        let mut privileges = PrivilegeMap::from_mz_acl_items(vec![
            MzAclItem {
                grantee: old_owner,
                grantor: other_role,
                acl_mode: AclMode::UPDATE,
            },
            MzAclItem {
                grantee: new_owner,
                grantor: other_role,
                acl_mode: AclMode::SELECT,
            },
        ]);
        Catalog::update_privilege_owners(&mut privileges, old_owner, new_owner);
        assert_eq!(1, privileges.all_values().count());
        assert_eq!(
            vec![MzAclItem {
                grantee: new_owner,
                grantor: other_role,
                acl_mode: AclMode::SELECT.union(AclMode::UPDATE)
            }],
            privileges.all_values_owned().collect::<Vec<_>>()
        );

        // older owner exists as grantee and grantor.
        let mut privileges = PrivilegeMap::from_mz_acl_items(vec![
            MzAclItem {
                grantee: old_owner,
                grantor: old_owner,
                acl_mode: AclMode::UPDATE,
            },
            MzAclItem {
                grantee: new_owner,
                grantor: new_owner,
                acl_mode: AclMode::SELECT,
            },
        ]);
        Catalog::update_privilege_owners(&mut privileges, old_owner, new_owner);
        assert_eq!(1, privileges.all_values().count());
        assert_eq!(
            vec![MzAclItem {
                grantee: new_owner,
                grantor: new_owner,
                acl_mode: AclMode::SELECT.union(AclMode::UPDATE)
            }],
            privileges.all_values_owned().collect::<Vec<_>>()
        );
    }
}

// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Logic related to executing catalog transactions.

use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

use itertools::Itertools;
use mz_adapter_types::compaction::CompactionWindow;
use mz_adapter_types::connection::ConnectionId;
use mz_adapter_types::dyncfgs::{
    ENABLE_0DT_DEPLOYMENT_PANIC_AFTER_TIMEOUT, WITH_0DT_DEPLOYMENT_DDL_CHECK_INTERVAL,
    WITH_0DT_DEPLOYMENT_MAX_WAIT,
};
use mz_audit_log::{
    CreateOrDropClusterReplicaReasonV1, EventDetails, EventType, IdFullNameV1, IdNameV1,
    ObjectType, SchedulingDecisionsWithReasonsV2, VersionedEvent, VersionedStorageUsage,
};
use mz_catalog::SYSTEM_CONN_ID;
use mz_catalog::builtin::BuiltinLog;
use mz_catalog::durable::{NetworkPolicy, Transaction};
use mz_catalog::memory::error::{AmbiguousRename, Error, ErrorKind};
use mz_catalog::memory::objects::{
    CatalogItem, ClusterConfig, DataSourceDesc, SourceReferences, StateDiff, StateUpdate,
    StateUpdateKind, TemporaryItem,
};
use mz_controller::clusters::{ManagedReplicaLocation, ReplicaConfig, ReplicaLocation};
use mz_controller_types::{ClusterId, ReplicaId};
use mz_ore::collections::HashSet;
use mz_ore::instrument;
use mz_ore::now::EpochMillis;
use mz_persist_types::ShardId;
use mz_repr::adt::mz_acl_item::{AclMode, MzAclItem, PrivilegeMap, merge_mz_acl_items};
use mz_repr::network_policy_id::NetworkPolicyId;
use mz_repr::role_id::RoleId;
use mz_repr::{CatalogItemId, ColumnName, ColumnType, Diff, GlobalId, strconv};
use mz_sql::ast::RawDataType;
use mz_sql::catalog::{
    CatalogDatabase, CatalogError as SqlCatalogError, CatalogItem as SqlCatalogItem, CatalogRole,
    CatalogSchema, DefaultPrivilegeAclItem, DefaultPrivilegeObject, PasswordAction,
    RoleAttributesRaw, RoleMembership, RoleVars,
};
use mz_sql::names::{
    CommentObjectId, DatabaseId, FullItemName, ObjectId, QualifiedItemName,
    ResolvedDatabaseSpecifier, SchemaId, SchemaSpecifier, SystemObjectId,
};
use mz_sql::plan::{NetworkPolicyRule, PlanError};
use mz_sql::session::user::{MZ_SUPPORT_ROLE_ID, MZ_SYSTEM_ROLE_ID};
use mz_sql::session::vars::OwnedVarInput;
use mz_sql::session::vars::{Value as VarValue, VarInput};
use mz_sql::{DEFAULT_SCHEMA, rbac};
use mz_sql_parser::ast::{QualifiedReplica, Value};
use mz_storage_client::storage_collections::StorageCollections;
use tracing::{info, trace};

use crate::AdapterError;
use crate::catalog::{
    BuiltinTableUpdate, Catalog, CatalogState, UpdatePrivilegeVariant,
    catalog_type_to_audit_object_type, comment_id_to_audit_object_type, is_reserved_name,
    is_reserved_role_name, object_type_to_audit_object_type,
    system_object_type_to_audit_object_type,
};
use crate::coord::ConnMeta;
use crate::coord::cluster_scheduling::SchedulingDecision;
use crate::util::ResultExt;

#[derive(Debug, Clone)]
pub enum Op {
    AlterRetainHistory {
        id: CatalogItemId,
        value: Option<Value>,
        window: CompactionWindow,
    },
    AlterRole {
        id: RoleId,
        name: String,
        attributes: RoleAttributesRaw,
        nopassword: bool,
        vars: RoleVars,
    },
    AlterNetworkPolicy {
        id: NetworkPolicyId,
        rules: Vec<NetworkPolicyRule>,
        name: String,
        owner_id: RoleId,
    },
    AlterAddColumn {
        id: CatalogItemId,
        new_global_id: GlobalId,
        name: ColumnName,
        typ: ColumnType,
        sql: RawDataType,
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
        attributes: RoleAttributesRaw,
    },
    CreateCluster {
        id: ClusterId,
        name: String,
        introspection_sources: Vec<&'static BuiltinLog>,
        owner_id: RoleId,
        config: ClusterConfig,
    },
    CreateClusterReplica {
        cluster_id: ClusterId,
        name: String,
        config: ReplicaConfig,
        owner_id: RoleId,
        reason: ReplicaCreateDropReason,
    },
    CreateItem {
        id: CatalogItemId,
        name: QualifiedItemName,
        item: CatalogItem,
        owner_id: RoleId,
    },
    CreateNetworkPolicy {
        rules: Vec<NetworkPolicyRule>,
        name: String,
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
        id: CatalogItemId,
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
    UpdateClusterReplicaConfig {
        cluster_id: ClusterId,
        replica_id: ReplicaId,
        config: ReplicaConfig,
    },
    UpdateItem {
        id: CatalogItemId,
        name: QualifiedItemName,
        to_item: CatalogItem,
    },
    UpdateSourceReferences {
        source_id: CatalogItemId,
        references: SourceReferences,
    },
    UpdateSystemConfiguration {
        name: String,
        value: OwnedVarInput,
    },
    ResetSystemConfiguration {
        name: String,
    },
    ResetAllSystemConfiguration,
    /// Performs updates to the storage usage table, which probably should be a builtin source.
    ///
    /// TODO(jkosh44) In a multi-writer or high availability catalog world, this
    /// might not work. If a process crashes after collecting storage usage events
    /// but before updating the builtin table, then another listening catalog
    /// will never know to update the builtin table.
    WeirdStorageUsageUpdates {
        object_id: Option<String>,
        size_bytes: u64,
        collection_timestamp: EpochMillis,
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
    Item(CatalogItemId),
    NetworkPolicy(NetworkPolicyId),
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
            ObjectId::Item(item_id) => DropObjectInfo::Item(item_id),
            ObjectId::NetworkPolicy(policy_id) => DropObjectInfo::NetworkPolicy(policy_id),
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
            DropObjectInfo::Item(item_id) => ObjectId::Item(item_id.clone()),
            DropObjectInfo::NetworkPolicy(network_policy_id) => {
                ObjectId::NetworkPolicy(network_policy_id.clone())
            }
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
        Option<SchedulingDecisionsWithReasonsV2>,
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

    /// Gets [`CatalogItemId`]s of temporary items to be created, checks for name collisions
    /// within a connection id.
    fn temporary_ids(
        &self,
        ops: &[Op],
        temporary_drops: BTreeSet<(&ConnectionId, String)>,
    ) -> Result<BTreeSet<CatalogItemId>, Error> {
        let mut creating = BTreeSet::new();
        let mut temporary_ids = BTreeSet::new();
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
                        temporary_ids.insert(id.clone());
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
        storage_collections: Option<
            &mut Arc<dyn StorageCollections<Timestamp = mz_repr::Timestamp> + Send + Sync>,
        >,
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

        let drop_ids: BTreeSet<CatalogItemId> = ops
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
        let dropped_global_ids = drop_ids
            .iter()
            .flat_map(|item_id| self.get_global_ids(item_id))
            .collect();

        let temporary_ids = self.temporary_ids(&ops, temporary_drops)?;
        let mut builtin_table_updates = vec![];
        let mut audit_events = vec![];
        let mut storage = self.storage().await;
        let mut tx = storage
            .transaction()
            .await
            .unwrap_or_terminate("starting catalog transaction");

        let new_state = Self::transact_inner(
            storage_collections,
            oracle_write_ts,
            session,
            ops,
            temporary_ids,
            &mut builtin_table_updates,
            &mut audit_events,
            &mut tx,
            &self.state,
        )
        .await?;

        // The user closure was successful, apply the updates. Terminate the
        // process if this fails, because we have to restart envd due to
        // indeterminate catalog state, which we only reconcile during catalog
        // init.
        tx.commit(oracle_write_ts)
            .await
            .unwrap_or_terminate("catalog storage transaction commit must succeed");

        // Dropping here keeps the mutable borrow on self, preventing us accidentally
        // mutating anything until after f is executed.
        drop(storage);
        if let Some(new_state) = new_state {
            self.transient_revision += 1;
            self.state = new_state;
        }

        // Drop in-memory planning metadata.
        let dropped_notices = self.drop_plans_and_metainfos(&dropped_global_ids);
        if self.state.system_config().enable_mz_notices() {
            // Generate retractions for the Builtin tables.
            self.state().pack_optimizer_notices(
                &mut builtin_table_updates,
                dropped_notices.iter(),
                Diff::MINUS_ONE,
            );
        }

        Ok(TransactionResult {
            builtin_table_updates,
            audit_events,
        })
    }

    /// Performs the transaction described by `ops` and returns the new state of the catalog, if
    /// it has changed. If `ops` don't result in a change in the state this method returns `None`.
    ///
    /// # Panics
    /// - If `ops` contains [`Op::TransactionDryRun`] and the value is not the
    ///   final element.
    /// - If the only element of `ops` is [`Op::TransactionDryRun`].
    #[instrument(name = "catalog::transact_inner")]
    async fn transact_inner(
        storage_collections: Option<
            &mut Arc<dyn StorageCollections<Timestamp = mz_repr::Timestamp> + Send + Sync>,
        >,
        oracle_write_ts: mz_repr::Timestamp,
        session: Option<&ConnMeta>,
        mut ops: Vec<Op>,
        temporary_ids: BTreeSet<CatalogItemId>,
        builtin_table_updates: &mut Vec<BuiltinTableUpdate>,
        audit_events: &mut Vec<VersionedEvent>,
        tx: &mut Transaction<'_>,
        state: &CatalogState,
    ) -> Result<Option<CatalogState>, AdapterError> {
        let mut state = Cow::Borrowed(state);

        let dry_run_ops = match ops.last() {
            Some(Op::TransactionDryRun) => {
                // Remove dry run marker.
                ops.pop();
                assert!(!ops.is_empty(), "TransactionDryRun must not be the only op");
                ops.clone()
            }
            Some(_) => vec![],
            None => return Ok(None),
        };

        let mut storage_collections_to_create = BTreeSet::new();
        let mut storage_collections_to_drop = BTreeSet::new();
        let mut storage_collections_to_register = BTreeMap::new();

        for op in ops {
            let (weird_builtin_table_update, temporary_item_updates) = Self::transact_op(
                oracle_write_ts,
                session,
                op,
                &temporary_ids,
                audit_events,
                tx,
                &*state,
                &mut storage_collections_to_create,
                &mut storage_collections_to_drop,
                &mut storage_collections_to_register,
            )
            .await?;

            // Certain builtin tables are not derived from the durable catalog state, so they need
            // to be updated ad-hoc based on the current transaction. This is weird and will not
            // work if we ever want multi-writer catalogs or high availability (HA) catalogs. If
            // this instance crashes after committing the durable catalog but before applying the
            // weird builtin table updates, then they'll be lost forever in a multi-writer or HA
            // world. Currently, this works fine and there are no correctness issues because
            // whenever a Coordinator crashes, a new Coordinator starts up, and it will set the
            // state of all builtin tables to the correct values.
            if let Some(builtin_table_update) = weird_builtin_table_update {
                builtin_table_updates.push(builtin_table_update);
            }

            // Temporary items are not stored in the durable catalog, so they need to be handled
            // separately for updating state and builtin tables.
            // TODO(jkosh44) Some more thought needs to be given as to how temporary tables work
            // in a multi-subscriber catalog world.
            let op_id = tx.op_id().into();
            let temporary_item_updates =
                temporary_item_updates
                    .into_iter()
                    .map(|(item, diff)| StateUpdate {
                        kind: StateUpdateKind::TemporaryItem(item),
                        ts: op_id,
                        diff,
                    });

            let mut updates: Vec<_> = tx.get_and_commit_op_updates();
            updates.extend(temporary_item_updates);
            if !updates.is_empty() {
                let op_builtin_table_updates = state.to_mut().apply_updates(updates)?;
                let op_builtin_table_updates = state
                    .to_mut()
                    .resolve_builtin_table_updates(op_builtin_table_updates);
                builtin_table_updates.extend(op_builtin_table_updates);
            }
        }

        if dry_run_ops.is_empty() {
            // `storage_collections` should only be `None` for tests.
            if let Some(c) = storage_collections {
                c.prepare_state(
                    tx,
                    storage_collections_to_create,
                    storage_collections_to_drop,
                    storage_collections_to_register,
                )
                .await?;
            }

            let updates = tx.get_and_commit_op_updates();
            if !updates.is_empty() {
                let op_builtin_table_updates = state.to_mut().apply_updates(updates)?;
                let op_builtin_table_updates = state
                    .to_mut()
                    .resolve_builtin_table_updates(op_builtin_table_updates);
                builtin_table_updates.extend(op_builtin_table_updates);
            }

            match state {
                Cow::Owned(state) => Ok(Some(state)),
                Cow::Borrowed(_) => Ok(None),
            }
        } else {
            Err(AdapterError::TransactionDryRun {
                new_ops: dry_run_ops,
                new_state: state.into_owned(),
            })
        }
    }

    /// Performs the transaction operation described by `op`. This function prepares the changes in
    /// `tx`, but does not update `state`. `state` will be updated when applying the durable
    /// changes.
    ///
    /// Optionally returns a builtin table update for any builtin table updates than cannot be
    /// derived from the durable catalog state, and temporary item diffs. These are all very weird
    /// scenarios and ideally in the future don't exist.
    #[instrument]
    async fn transact_op(
        oracle_write_ts: mz_repr::Timestamp,
        session: Option<&ConnMeta>,
        op: Op,
        temporary_ids: &BTreeSet<CatalogItemId>,
        audit_events: &mut Vec<VersionedEvent>,
        tx: &mut Transaction<'_>,
        state: &CatalogState,
        storage_collections_to_create: &mut BTreeSet<GlobalId>,
        storage_collections_to_drop: &mut BTreeSet<GlobalId>,
        storage_collections_to_register: &mut BTreeMap<GlobalId, ShardId>,
    ) -> Result<(Option<BuiltinTableUpdate>, Vec<(TemporaryItem, StateDiff)>), AdapterError> {
        let mut weird_builtin_table_update = None;
        let mut temporary_item_updates = Vec::new();

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

                if Self::should_audit_log_item(new_entry.item()) {
                    let details =
                        EventDetails::AlterRetainHistoryV1(mz_audit_log::AlterRetainHistoryV1 {
                            id: id.to_string(),
                            old_history: previous.map(|previous| previous.to_string()),
                            new_history: value.map(|v| v.to_string()),
                        });
                    CatalogState::add_to_audit_log(
                        &state.system_configuration,
                        oracle_write_ts,
                        session,
                        tx,
                        audit_events,
                        EventType::Alter,
                        catalog_type_to_audit_object_type(new_entry.item().typ()),
                        details,
                    )?;
                }

                tx.update_item(id, new_entry.into())?;

                Self::log_update(state, &id);
            }
            Op::AlterRole {
                id,
                name,
                attributes,
                nopassword,
                vars,
            } => {
                state.ensure_not_reserved_role(&id)?;

                let mut existing_role = state.get_role(&id).clone();
                let password = attributes.password.clone();
                existing_role.attributes = attributes.into();
                existing_role.vars = vars;
                let password_action = if nopassword {
                    PasswordAction::Clear
                } else if let Some(password) = password {
                    PasswordAction::Set(password)
                } else {
                    PasswordAction::NoChange
                };
                tx.update_role(id, existing_role.into(), password_action)?;

                CatalogState::add_to_audit_log(
                    &state.system_configuration,
                    oracle_write_ts,
                    session,
                    tx,
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
            Op::AlterNetworkPolicy {
                id,
                rules,
                name,
                owner_id: _owner_id,
            } => {
                let existing_policy = state.get_network_policy(&id).clone();
                let mut policy: NetworkPolicy = existing_policy.into();
                policy.rules = rules;
                if is_reserved_name(&name) {
                    return Err(AdapterError::Catalog(Error::new(
                        ErrorKind::ReservedNetworkPolicyName(name),
                    )));
                }
                tx.update_network_policy(id, policy.clone())?;

                CatalogState::add_to_audit_log(
                    &state.system_configuration,
                    oracle_write_ts,
                    session,
                    tx,
                    audit_events,
                    EventType::Alter,
                    ObjectType::NetworkPolicy,
                    EventDetails::IdNameV1(mz_audit_log::IdNameV1 {
                        id: id.to_string(),
                        name: name.clone(),
                    }),
                )?;

                info!("update network policy {name} ({id})");
            }
            Op::AlterAddColumn {
                id,
                new_global_id,
                name,
                typ,
                sql,
            } => {
                let mut new_entry = state.get_entry(&id).clone();
                let version = new_entry.item.add_column(name, typ, sql)?;
                // All versions of a table share the same shard, so it shouldn't matter what
                // GlobalId we use here.
                let shard_id = state
                    .storage_metadata()
                    .get_collection_shard(new_entry.latest_global_id())?;

                // TODO(alter_table): Support adding columns to sources.
                let CatalogItem::Table(table) = &mut new_entry.item else {
                    return Err(AdapterError::Unsupported("adding columns to non-Table"));
                };
                table.collections.insert(version, new_global_id);

                tx.update_item(id, new_entry.into())?;
                storage_collections_to_register.insert(new_global_id, shard_id);
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

                let temporary_oids: HashSet<_> = state.get_temporary_oids().collect();
                let (database_id, _) = tx.insert_user_database(
                    &name,
                    owner_id,
                    database_privileges.clone(),
                    &temporary_oids,
                )?;
                let (schema_id, _) = tx.insert_user_schema(
                    database_id,
                    DEFAULT_SCHEMA,
                    owner_id,
                    schema_privileges.clone(),
                    &temporary_oids,
                )?;
                CatalogState::add_to_audit_log(
                    &state.system_configuration,
                    oracle_write_ts,
                    session,
                    tx,
                    audit_events,
                    EventType::Create,
                    ObjectType::Database,
                    EventDetails::IdNameV1(mz_audit_log::IdNameV1 {
                        id: database_id.to_string(),
                        name: name.clone(),
                    }),
                )?;
                info!("create database {}", name);

                CatalogState::add_to_audit_log(
                    &state.system_configuration,
                    oracle_write_ts,
                    session,
                    tx,
                    audit_events,
                    EventType::Create,
                    ObjectType::Schema,
                    EventDetails::SchemaV2(mz_audit_log::SchemaV2 {
                        id: schema_id.to_string(),
                        name: DEFAULT_SCHEMA.to_string(),
                        database_name: Some(name),
                    }),
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
                let (schema_id, _) = tx.insert_user_schema(
                    database_id,
                    &schema_name,
                    owner_id,
                    privileges.clone(),
                    &state.get_temporary_oids().collect(),
                )?;
                CatalogState::add_to_audit_log(
                    &state.system_configuration,
                    oracle_write_ts,
                    session,
                    tx,
                    audit_events,
                    EventType::Create,
                    ObjectType::Schema,
                    EventDetails::SchemaV2(mz_audit_log::SchemaV2 {
                        id: schema_id.to_string(),
                        name: schema_name.clone(),
                        database_name: Some(state.database_by_id[&database_id].name.clone()),
                    }),
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
                let (id, _) = tx.insert_user_role(
                    name.clone(),
                    attributes.clone(),
                    membership.clone(),
                    vars.clone(),
                    &state.get_temporary_oids().collect(),
                )?;
                CatalogState::add_to_audit_log(
                    &state.system_configuration,
                    oracle_write_ts,
                    session,
                    tx,
                    audit_events,
                    EventType::Create,
                    ObjectType::Role,
                    EventDetails::IdNameV1(mz_audit_log::IdNameV1 {
                        id: id.to_string(),
                        name: name.clone(),
                    }),
                )?;
                info!("create role {}", name);
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
                let introspection_source_ids: Vec<_> = introspection_sources
                    .iter()
                    .map(|introspection_source| {
                        Transaction::allocate_introspection_source_index_id(
                            &id,
                            introspection_source.variant,
                        )
                    })
                    .collect();

                let introspection_sources = introspection_sources
                    .into_iter()
                    .zip_eq(introspection_source_ids)
                    .map(|(log, (item_id, gid))| (log, item_id, gid))
                    .collect();

                tx.insert_user_cluster(
                    id,
                    &name,
                    introspection_sources,
                    owner_id,
                    privileges.clone(),
                    config.clone().into(),
                    &state.get_temporary_oids().collect(),
                )?;
                CatalogState::add_to_audit_log(
                    &state.system_configuration,
                    oracle_write_ts,
                    session,
                    tx,
                    audit_events,
                    EventType::Create,
                    ObjectType::Cluster,
                    EventDetails::IdNameV1(mz_audit_log::IdNameV1 {
                        id: id.to_string(),
                        name: name.clone(),
                    }),
                )?;
                info!("create cluster {}", name);
            }
            Op::CreateClusterReplica {
                cluster_id,
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
                let id =
                    tx.insert_cluster_replica(cluster_id, &name, config.clone().into(), owner_id)?;
                if let ReplicaLocation::Managed(ManagedReplicaLocation {
                    size,
                    billed_as,
                    internal,
                    ..
                }) = &config.location
                {
                    let (reason, scheduling_policies) = reason.into_audit_log();
                    let details = EventDetails::CreateClusterReplicaV4(
                        mz_audit_log::CreateClusterReplicaV4 {
                            cluster_id: cluster_id.to_string(),
                            cluster_name: cluster.name.clone(),
                            replica_id: Some(id.to_string()),
                            replica_name: name.clone(),
                            logical_size: size.clone(),
                            billed_as: billed_as.clone(),
                            internal: *internal,
                            reason,
                            scheduling_policies,
                        },
                    );
                    CatalogState::add_to_audit_log(
                        &state.system_configuration,
                        oracle_write_ts,
                        session,
                        tx,
                        audit_events,
                        EventType::Create,
                        ObjectType::ClusterReplica,
                        details,
                    )?;
                }
            }
            Op::CreateItem {
                id,
                name,
                item,
                owner_id,
            } => {
                state.check_unstable_dependencies(&item)?;

                match &item {
                    CatalogItem::Table(table) => {
                        let gids: Vec<_> = table.global_ids().collect();
                        assert_eq!(gids.len(), 1);
                        storage_collections_to_create.extend(gids);
                    }
                    CatalogItem::Source(source) => {
                        storage_collections_to_create.insert(source.global_id());
                    }
                    CatalogItem::MaterializedView(mv) => {
                        storage_collections_to_create.insert(mv.global_id());
                    }
                    CatalogItem::ContinualTask(ct) => {
                        storage_collections_to_create.insert(ct.global_id());
                    }
                    CatalogItem::Sink(sink) => {
                        storage_collections_to_create.insert(sink.global_id());
                    }
                    CatalogItem::Log(_)
                    | CatalogItem::View(_)
                    | CatalogItem::Index(_)
                    | CatalogItem::Type(_)
                    | CatalogItem::Func(_)
                    | CatalogItem::Secret(_)
                    | CatalogItem::Connection(_) => (),
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

                let temporary_oids = state.get_temporary_oids().collect();

                if item.is_temporary() {
                    if name.qualifiers.database_spec != ResolvedDatabaseSpecifier::Ambient
                        || name.qualifiers.schema_spec != SchemaSpecifier::Temporary
                    {
                        return Err(AdapterError::Catalog(Error::new(
                            ErrorKind::InvalidTemporarySchema,
                        )));
                    }
                    let oid = tx.allocate_oid(&temporary_oids)?;
                    let item = TemporaryItem {
                        id,
                        oid,
                        name: name.clone(),
                        item: item.clone(),
                        owner_id,
                        privileges: PrivilegeMap::from_mz_acl_items(privileges),
                    };
                    temporary_item_updates.push((item, StateDiff::Addition));
                } else {
                    if let Some(temp_id) =
                        item.uses()
                            .iter()
                            .find(|id| match state.try_get_entry(*id) {
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
                    let item_type = item.typ();
                    let (create_sql, global_id, versions) = item.to_serialized();
                    tx.insert_user_item(
                        id,
                        global_id,
                        schema_id,
                        &name.item,
                        create_sql,
                        owner_id,
                        privileges.clone(),
                        &temporary_oids,
                        versions,
                    )?;
                    info!(
                        "create {} {} ({})",
                        item_type,
                        state.resolve_full_name(&name, None),
                        id
                    );
                }

                if Self::should_audit_log_item(&item) {
                    let name = Self::full_name_detail(
                        &state.resolve_full_name(&name, session.map(|session| session.conn_id())),
                    );
                    let details = match &item {
                        CatalogItem::Source(s) => {
                            let cluster_id = match s.data_source {
                                // Ingestion exports don't have their own cluster, but
                                // run on their ingestion's cluster.
                                DataSourceDesc::IngestionExport { ingestion_id, .. } => {
                                    match state.get_entry(&ingestion_id).cluster_id() {
                                        Some(cluster_id) => Some(cluster_id.to_string()),
                                        None => None,
                                    }
                                }
                                _ => match item.cluster_id() {
                                    Some(cluster_id) => Some(cluster_id.to_string()),
                                    None => None,
                                },
                            };

                            EventDetails::CreateSourceSinkV4(mz_audit_log::CreateSourceSinkV4 {
                                id: id.to_string(),
                                cluster_id,
                                name,
                                external_type: s.source_type().to_string(),
                            })
                        }
                        CatalogItem::Sink(s) => {
                            EventDetails::CreateSourceSinkV4(mz_audit_log::CreateSourceSinkV4 {
                                id: id.to_string(),
                                cluster_id: Some(s.cluster_id.to_string()),
                                name,
                                external_type: s.sink_type().to_string(),
                            })
                        }
                        CatalogItem::Index(i) => {
                            EventDetails::CreateIndexV1(mz_audit_log::CreateIndexV1 {
                                id: id.to_string(),
                                name,
                                cluster_id: i.cluster_id.to_string(),
                            })
                        }
                        CatalogItem::MaterializedView(mv) => {
                            EventDetails::CreateMaterializedViewV1(
                                mz_audit_log::CreateMaterializedViewV1 {
                                    id: id.to_string(),
                                    name,
                                    cluster_id: mv.cluster_id.to_string(),
                                },
                            )
                        }
                        _ => EventDetails::IdFullNameV1(IdFullNameV1 {
                            id: id.to_string(),
                            name,
                        }),
                    };
                    CatalogState::add_to_audit_log(
                        &state.system_configuration,
                        oracle_write_ts,
                        session,
                        tx,
                        audit_events,
                        EventType::Create,
                        catalog_type_to_audit_object_type(item.typ()),
                        details,
                    )?;
                }
            }
            Op::CreateNetworkPolicy {
                rules,
                name,
                owner_id,
            } => {
                if state.network_policies_by_name.contains_key(&name) {
                    return Err(AdapterError::PlanError(PlanError::Catalog(
                        SqlCatalogError::NetworkPolicyAlreadyExists(name),
                    )));
                }
                if is_reserved_name(&name) {
                    return Err(AdapterError::Catalog(Error::new(
                        ErrorKind::ReservedNetworkPolicyName(name),
                    )));
                }

                let owner_privileges = vec![rbac::owner_privilege(
                    mz_sql::catalog::ObjectType::NetworkPolicy,
                    owner_id,
                )];
                let default_privileges = state
                    .default_privileges
                    .get_applicable_privileges(
                        owner_id,
                        None,
                        None,
                        mz_sql::catalog::ObjectType::NetworkPolicy,
                    )
                    .map(|item| item.mz_acl_item(owner_id));
                let privileges: Vec<_> =
                    merge_mz_acl_items(owner_privileges.into_iter().chain(default_privileges))
                        .collect();

                let temporary_oids: HashSet<_> = state.get_temporary_oids().collect();
                let id = tx.insert_user_network_policy(
                    name.clone(),
                    rules,
                    privileges,
                    owner_id,
                    &temporary_oids,
                )?;

                CatalogState::add_to_audit_log(
                    &state.system_configuration,
                    oracle_write_ts,
                    session,
                    tx,
                    audit_events,
                    EventType::Create,
                    ObjectType::NetworkPolicy,
                    EventDetails::IdNameV1(mz_audit_log::IdNameV1 {
                        id: id.to_string(),
                        name: name.clone(),
                    }),
                )?;

                info!("created network policy {name} ({id})");
            }
            Op::Comment {
                object_id,
                sub_component,
                comment,
            } => {
                tx.update_comment(object_id, sub_component, comment)?;
                let entry = state.get_comment_id_entry(&object_id);
                let should_log = entry
                    .map(|entry| Self::should_audit_log_item(entry.item()))
                    // Things that aren't catalog entries can't be temp, so should be logged.
                    .unwrap_or(true);
                // TODO: We need a conn_id to resolve schema names. This means that system-initiated
                // comments won't be logged for now.
                if let (Some(conn_id), true) =
                    (session.map(|session| session.conn_id()), should_log)
                {
                    CatalogState::add_to_audit_log(
                        &state.system_configuration,
                        oracle_write_ts,
                        session,
                        tx,
                        audit_events,
                        EventType::Comment,
                        comment_id_to_audit_object_type(object_id),
                        EventDetails::IdNameV1(IdNameV1 {
                            // CommentObjectIds don't have a great string representation, but debug will do for now.
                            id: format!("{object_id:?}"),
                            name: state.comment_id_to_audit_log_name(object_id, conn_id),
                        }),
                    )?;
                }
            }
            Op::UpdateSourceReferences {
                source_id,
                references,
            } => {
                tx.update_source_references(
                    source_id,
                    references
                        .references
                        .into_iter()
                        .map(|reference| reference.into())
                        .collect(),
                    references.updated_at,
                )?;
            }
            Op::DropObjects(drop_object_infos) => {
                // Generate all of the objects that need to get dropped.
                let delta = ObjectsToDrop::generate(drop_object_infos, state, session)?;

                // Drop any associated comments.
                tx.drop_comments(&delta.comments)?;

                // Drop any items.
                let (durable_items_to_drop, temporary_items_to_drop): (BTreeSet<_>, BTreeSet<_>) =
                    delta
                        .items
                        .iter()
                        .map(|id| id)
                        .partition(|id| !state.get_entry(*id).item().is_temporary());
                tx.remove_items(&durable_items_to_drop)?;
                temporary_item_updates.extend(temporary_items_to_drop.into_iter().map(|id| {
                    let entry = state.get_entry(&id);
                    (entry.clone().into(), StateDiff::Retraction)
                }));

                for item_id in delta.items {
                    let entry = state.get_entry(&item_id);

                    if entry.item().is_storage_collection() {
                        storage_collections_to_drop.extend(entry.global_ids());
                    }

                    if state.source_references.contains_key(&item_id) {
                        tx.remove_source_references(item_id)?;
                    }

                    if Self::should_audit_log_item(entry.item()) {
                        CatalogState::add_to_audit_log(
                            &state.system_configuration,
                            oracle_write_ts,
                            session,
                            tx,
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
                    info!(
                        "drop {} {} ({})",
                        entry.item_type(),
                        state.resolve_full_name(entry.name(), entry.conn_id()),
                        item_id
                    );
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

                    CatalogState::add_to_audit_log(
                        &state.system_configuration,
                        oracle_write_ts,
                        session,
                        tx,
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
                }

                // Drop any databases.
                tx.remove_databases(&delta.databases)?;

                for database_id in delta.databases {
                    let database = state.get_database(&database_id).clone();

                    CatalogState::add_to_audit_log(
                        &state.system_configuration,
                        oracle_write_ts,
                        session,
                        tx,
                        audit_events,
                        EventType::Drop,
                        ObjectType::Database,
                        EventDetails::IdNameV1(mz_audit_log::IdNameV1 {
                            id: database_id.to_string(),
                            name: database.name.clone(),
                        }),
                    )?;
                }

                // Drop any roles.
                tx.remove_user_roles(&delta.roles)?;

                for role_id in delta.roles {
                    let role = state
                        .roles_by_id
                        .get(&role_id)
                        .expect("catalog out of sync");

                    CatalogState::add_to_audit_log(
                        &state.system_configuration,
                        oracle_write_ts,
                        session,
                        tx,
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

                // Drop any network policies.
                tx.remove_network_policies(&delta.network_policies)?;

                for network_policy_id in delta.network_policies {
                    let policy = state
                        .network_policies_by_id
                        .get(&network_policy_id)
                        .expect("catalog out of sync");

                    CatalogState::add_to_audit_log(
                        &state.system_configuration,
                        oracle_write_ts,
                        session,
                        tx,
                        audit_events,
                        EventType::Drop,
                        ObjectType::NetworkPolicy,
                        EventDetails::IdNameV1(mz_audit_log::IdNameV1 {
                            id: policy.id.to_string(),
                            name: policy.name.clone(),
                        }),
                    )?;
                    info!("drop network policy {}", policy.name.clone());
                }

                // Drop any replicas.
                let replicas = delta.replicas.keys().copied().collect();
                tx.remove_cluster_replicas(&replicas)?;

                for (replica_id, (cluster_id, reason)) in delta.replicas {
                    let cluster = state.get_cluster(cluster_id);
                    let replica = cluster.replica(replica_id).expect("Must exist");

                    let (reason, scheduling_policies) = reason.into_audit_log();
                    let details =
                        EventDetails::DropClusterReplicaV3(mz_audit_log::DropClusterReplicaV3 {
                            cluster_id: cluster_id.to_string(),
                            cluster_name: cluster.name.clone(),
                            replica_id: Some(replica_id.to_string()),
                            replica_name: replica.name.clone(),
                            reason,
                            scheduling_policies,
                        });
                    CatalogState::add_to_audit_log(
                        &state.system_configuration,
                        oracle_write_ts,
                        session,
                        tx,
                        audit_events,
                        EventType::Drop,
                        ObjectType::ClusterReplica,
                        details,
                    )?;
                }

                // Drop any clusters.
                tx.remove_clusters(&delta.clusters)?;

                for cluster_id in delta.clusters {
                    let cluster = state.get_cluster(cluster_id);

                    CatalogState::add_to_audit_log(
                        &state.system_configuration,
                        oracle_write_ts,
                        session,
                        tx,
                        audit_events,
                        EventType::Drop,
                        ObjectType::Cluster,
                        EventDetails::IdNameV1(mz_audit_log::IdNameV1 {
                            id: cluster.id.to_string(),
                            name: cluster.name.clone(),
                        }),
                    )?;
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
                let mut member_role = state.get_role(&member_id).clone();
                member_role.membership.map.insert(role_id, grantor_id);
                tx.update_role(member_id, member_role.into(), PasswordAction::NoChange)?;

                CatalogState::add_to_audit_log(
                    &state.system_configuration,
                    oracle_write_ts,
                    session,
                    tx,
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
                let mut member_role = state.get_role(&member_id).clone();
                member_role.membership.map.remove(&role_id);
                tx.update_role(member_id, member_role.into(), PasswordAction::NoChange)?;

                CatalogState::add_to_audit_log(
                    &state.system_configuration,
                    oracle_write_ts,
                    session,
                    tx,
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
                            let mut cluster = state.get_cluster(*id).clone();
                            update_privilege_fn(&mut cluster.privileges);
                            tx.update_cluster(*id, cluster.into())?;
                        }
                        ObjectId::Database(id) => {
                            let mut database = state.get_database(id).clone();
                            update_privilege_fn(&mut database.privileges);
                            tx.update_database(*id, database.into())?;
                        }
                        ObjectId::NetworkPolicy(id) => {
                            let mut policy = state.get_network_policy(id).clone();
                            update_privilege_fn(&mut policy.privileges);
                            tx.update_network_policy(*id, policy.into())?;
                        }
                        ObjectId::Schema((database_spec, schema_spec)) => {
                            let schema_id = schema_spec.clone().into();
                            let mut schema = state
                                .get_schema(
                                    database_spec,
                                    schema_spec,
                                    session
                                        .map(|session| session.conn_id())
                                        .unwrap_or(&SYSTEM_CONN_ID),
                                )
                                .clone();
                            update_privilege_fn(&mut schema.privileges);
                            tx.update_schema(schema_id, schema.into())?;
                        }
                        ObjectId::Item(id) => {
                            let entry = state.get_entry(id);
                            let mut new_entry = entry.clone();
                            update_privilege_fn(&mut new_entry.privileges);
                            if !new_entry.item().is_temporary() {
                                tx.update_item(*id, new_entry.into())?;
                            } else {
                                temporary_item_updates
                                    .push((entry.clone().into(), StateDiff::Retraction));
                                temporary_item_updates
                                    .push((new_entry.into(), StateDiff::Addition));
                            }
                        }
                        ObjectId::Role(_) | ObjectId::ClusterReplica(_) => {}
                    },
                    SystemObjectId::System => {
                        let mut system_privileges = state.system_privileges.clone();
                        update_privilege_fn(&mut system_privileges);
                        let new_privilege =
                            system_privileges.get_acl_item(&privilege.grantee, &privilege.grantor);
                        tx.set_system_privilege(
                            privilege.grantee,
                            privilege.grantor,
                            new_privilege.map(|new_privilege| new_privilege.acl_mode),
                        )?;
                    }
                }
                let object_type = state.get_system_object_type(&target_id);
                let object_id_str = match &target_id {
                    SystemObjectId::System => "SYSTEM".to_string(),
                    SystemObjectId::Object(id) => id.to_string(),
                };
                CatalogState::add_to_audit_log(
                    &state.system_configuration,
                    oracle_write_ts,
                    session,
                    tx,
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
                let mut default_privileges = state.default_privileges.clone();
                match variant {
                    UpdatePrivilegeVariant::Grant => default_privileges
                        .grant(privilege_object.clone(), privilege_acl_item.clone()),
                    UpdatePrivilegeVariant::Revoke => {
                        default_privileges.revoke(&privilege_object, &privilege_acl_item)
                    }
                }
                let new_acl_mode = default_privileges
                    .get_privileges_for_grantee(&privilege_object, &privilege_acl_item.grantee);
                tx.set_default_privilege(
                    privilege_object.role_id,
                    privilege_object.database_id,
                    privilege_object.schema_id,
                    privilege_object.object_type,
                    privilege_acl_item.grantee,
                    new_acl_mode.cloned(),
                )?;
                CatalogState::add_to_audit_log(
                    &state.system_configuration,
                    oracle_write_ts,
                    session,
                    tx,
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
                CatalogState::add_to_audit_log(
                    &state.system_configuration,
                    oracle_write_ts,
                    session,
                    tx,
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
                CatalogState::add_to_audit_log(
                    &state.system_configuration,
                    oracle_write_ts,
                    session,
                    tx,
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
                    CatalogState::add_to_audit_log(
                        &state.system_configuration,
                        oracle_write_ts,
                        session,
                        tx,
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

                    if !to_entry.item().is_temporary() {
                        tx.update_item(*id, to_entry.into())?;
                    } else {
                        temporary_item_updates
                            .push((dependent_item.clone().into(), StateDiff::Retraction));
                        temporary_item_updates.push((to_entry.into(), StateDiff::Addition));
                    }
                    updates.push(*id);
                }
                if !new_entry.item().is_temporary() {
                    tx.update_item(id, new_entry.into())?;
                } else {
                    temporary_item_updates.push((entry.clone().into(), StateDiff::Retraction));
                    temporary_item_updates.push((new_entry.into(), StateDiff::Addition));
                }

                updates.push(id);
                for id in updates {
                    Self::log_update(state, &id);
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
                        items_to_update.insert(*id, new_entry.into());
                    } else {
                        temporary_item_updates.push((entry.clone().into(), StateDiff::Retraction));
                        temporary_item_updates.push((new_entry.into(), StateDiff::Addition));
                    }
                    updates.push(id);

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
                CatalogState::add_to_audit_log(
                    &state.system_configuration,
                    oracle_write_ts,
                    session,
                    tx,
                    audit_events,
                    EventType::Alter,
                    mz_audit_log::ObjectType::Schema,
                    details,
                )?;

                // Update the schema itself.
                let mut new_schema = schema.clone();
                new_schema.name.schema.clone_from(&new_name);
                tx.update_schema(schema_id, new_schema.into())?;

                for id in updates {
                    Self::log_update(state, id);
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
                        let mut cluster = state.get_cluster(*id).clone();
                        if id.is_system() {
                            return Err(AdapterError::Catalog(Error::new(
                                ErrorKind::ReadOnlyCluster(cluster.name),
                            )));
                        }
                        Self::update_privilege_owners(
                            &mut cluster.privileges,
                            cluster.owner_id,
                            new_owner,
                        );
                        cluster.owner_id = new_owner;
                        tx.update_cluster(*id, cluster.into())?;
                    }
                    ObjectId::ClusterReplica((cluster_id, replica_id)) => {
                        let cluster = state.get_cluster(*cluster_id);
                        let mut replica = cluster
                            .replica(*replica_id)
                            .expect("catalog out of sync")
                            .clone();
                        if replica_id.is_system() {
                            return Err(AdapterError::Catalog(Error::new(
                                ErrorKind::ReadOnlyClusterReplica(replica.name),
                            )));
                        }
                        replica.owner_id = new_owner;
                        tx.update_cluster_replica(*replica_id, replica.into())?;
                    }
                    ObjectId::Database(id) => {
                        let mut database = state.get_database(id).clone();
                        if id.is_system() {
                            return Err(AdapterError::Catalog(Error::new(
                                ErrorKind::ReadOnlyDatabase(database.name),
                            )));
                        }
                        Self::update_privilege_owners(
                            &mut database.privileges,
                            database.owner_id,
                            new_owner,
                        );
                        database.owner_id = new_owner;
                        tx.update_database(*id, database.clone().into())?;
                    }
                    ObjectId::Schema((database_spec, schema_spec)) => {
                        let schema_id: SchemaId = schema_spec.clone().into();
                        let mut schema = state
                            .get_schema(database_spec, schema_spec, conn_id)
                            .clone();
                        if schema_id.is_system() {
                            let name = schema.name();
                            let full_name = state.resolve_full_schema_name(name);
                            return Err(AdapterError::Catalog(Error::new(
                                ErrorKind::ReadOnlySystemSchema(full_name.to_string()),
                            )));
                        }
                        Self::update_privilege_owners(
                            &mut schema.privileges,
                            schema.owner_id,
                            new_owner,
                        );
                        schema.owner_id = new_owner;
                        tx.update_schema(schema_id, schema.into())?;
                    }
                    ObjectId::Item(id) => {
                        let entry = state.get_entry(id);
                        let mut new_entry = entry.clone();
                        if id.is_system() {
                            let full_name = state.resolve_full_name(
                                new_entry.name(),
                                session.map(|session| session.conn_id()),
                            );
                            return Err(AdapterError::Catalog(Error::new(
                                ErrorKind::ReadOnlyItem(full_name.to_string()),
                            )));
                        }
                        Self::update_privilege_owners(
                            &mut new_entry.privileges,
                            new_entry.owner_id,
                            new_owner,
                        );
                        new_entry.owner_id = new_owner;
                        if !new_entry.item().is_temporary() {
                            tx.update_item(*id, new_entry.into())?;
                        } else {
                            temporary_item_updates
                                .push((entry.clone().into(), StateDiff::Retraction));
                            temporary_item_updates.push((new_entry.into(), StateDiff::Addition));
                        }
                    }
                    ObjectId::NetworkPolicy(id) => {
                        let mut policy = state.get_network_policy(id).clone();
                        if id.is_system() {
                            return Err(AdapterError::Catalog(Error::new(
                                ErrorKind::ReadOnlyNetworkPolicy(policy.name),
                            )));
                        }
                        Self::update_privilege_owners(
                            &mut policy.privileges,
                            policy.owner_id,
                            new_owner,
                        );
                        policy.owner_id = new_owner;
                        tx.update_network_policy(*id, policy.into())?;
                    }
                    ObjectId::Role(_) => unreachable!("roles have no owner"),
                }
                let object_type = state.get_object_type(&id);
                CatalogState::add_to_audit_log(
                    &state.system_configuration,
                    oracle_write_ts,
                    session,
                    tx,
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
                let mut cluster = state.get_cluster(id).clone();
                cluster.config = config;
                tx.update_cluster(id, cluster.into())?;
                info!("update cluster {}", name);

                CatalogState::add_to_audit_log(
                    &state.system_configuration,
                    oracle_write_ts,
                    session,
                    tx,
                    audit_events,
                    EventType::Alter,
                    ObjectType::Cluster,
                    EventDetails::IdNameV1(mz_audit_log::IdNameV1 {
                        id: id.to_string(),
                        name,
                    }),
                )?;
            }
            Op::UpdateClusterReplicaConfig {
                replica_id,
                cluster_id,
                config,
            } => {
                let replica = state.get_cluster_replica(cluster_id, replica_id).to_owned();
                info!("update replica {}", replica.name);
                tx.update_cluster_replica(
                    replica_id,
                    mz_catalog::durable::ClusterReplica {
                        cluster_id,
                        replica_id,
                        name: replica.name.clone(),
                        config: config.clone().into(),
                        owner_id: replica.owner_id,
                    },
                )?;
            }
            Op::UpdateItem { id, name, to_item } => {
                let mut entry = state.get_entry(&id).clone();
                entry.name = name.clone();
                entry.item = to_item.clone();
                tx.update_item(id, entry.into())?;

                if Self::should_audit_log_item(&to_item) {
                    let mut full_name = Self::full_name_detail(
                        &state.resolve_full_name(&name, session.map(|session| session.conn_id())),
                    );
                    full_name.item = name.item;

                    CatalogState::add_to_audit_log(
                        &state.system_configuration,
                        oracle_write_ts,
                        session,
                        tx,
                        audit_events,
                        EventType::Alter,
                        catalog_type_to_audit_object_type(to_item.typ()),
                        EventDetails::UpdateItemV1(mz_audit_log::UpdateItemV1 {
                            id: id.to_string(),
                            name: full_name,
                        }),
                    )?;
                }

                Self::log_update(state, &id);
            }
            Op::UpdateSystemConfiguration { name, value } => {
                let parsed_value = state.parse_system_configuration(&name, value.borrow())?;
                tx.upsert_system_config(&name, parsed_value.clone())?;
                // This mirrors some "system vars" into the catalog storage
                // "config" collection so that we can toggle the flag with
                // Launch Darkly, but use it in boot before Launch Darkly is
                // available.
                if name == WITH_0DT_DEPLOYMENT_MAX_WAIT.name() {
                    let with_0dt_deployment_max_wait =
                        Duration::parse(VarInput::Flat(&parsed_value))
                            .expect("parsing succeeded above");
                    tx.set_0dt_deployment_max_wait(with_0dt_deployment_max_wait)?;
                } else if name == WITH_0DT_DEPLOYMENT_DDL_CHECK_INTERVAL.name() {
                    let with_0dt_deployment_ddl_check_interval =
                        Duration::parse(VarInput::Flat(&parsed_value))
                            .expect("parsing succeeded above");
                    tx.set_0dt_deployment_ddl_check_interval(
                        with_0dt_deployment_ddl_check_interval,
                    )?;
                } else if name == ENABLE_0DT_DEPLOYMENT_PANIC_AFTER_TIMEOUT.name() {
                    let panic_after_timeout =
                        strconv::parse_bool(&parsed_value).expect("parsing succeeded above");
                    tx.set_enable_0dt_deployment_panic_after_timeout(panic_after_timeout)?;
                }

                CatalogState::add_to_audit_log(
                    &state.system_configuration,
                    oracle_write_ts,
                    session,
                    tx,
                    audit_events,
                    EventType::Alter,
                    ObjectType::System,
                    EventDetails::SetV1(mz_audit_log::SetV1 {
                        name,
                        value: Some(value.borrow().to_vec().join(", ")),
                    }),
                )?;
            }
            Op::ResetSystemConfiguration { name } => {
                tx.remove_system_config(&name);
                // This mirrors some "system vars" into the catalog storage
                // "config" collection so that we can toggle the flag with
                // Launch Darkly, but use it in boot before Launch Darkly is
                // available.
                if name == WITH_0DT_DEPLOYMENT_MAX_WAIT.name() {
                    tx.reset_0dt_deployment_max_wait()?;
                } else if name == WITH_0DT_DEPLOYMENT_DDL_CHECK_INTERVAL.name() {
                    tx.reset_0dt_deployment_ddl_check_interval()?;
                }

                CatalogState::add_to_audit_log(
                    &state.system_configuration,
                    oracle_write_ts,
                    session,
                    tx,
                    audit_events,
                    EventType::Alter,
                    ObjectType::System,
                    EventDetails::SetV1(mz_audit_log::SetV1 { name, value: None }),
                )?;
            }
            Op::ResetAllSystemConfiguration => {
                tx.clear_system_configs();
                tx.reset_0dt_deployment_max_wait()?;
                tx.reset_0dt_deployment_ddl_check_interval()?;

                CatalogState::add_to_audit_log(
                    &state.system_configuration,
                    oracle_write_ts,
                    session,
                    tx,
                    audit_events,
                    EventType::Alter,
                    ObjectType::System,
                    EventDetails::ResetAllV1,
                )?;
            }
            Op::WeirdStorageUsageUpdates {
                object_id,
                size_bytes,
                collection_timestamp,
            } => {
                let id = tx.allocate_storage_usage_ids()?;
                let metric =
                    VersionedStorageUsage::new(id, object_id, size_bytes, collection_timestamp);
                let builtin_table_update = state.pack_storage_usage_update(metric, Diff::ONE);
                let builtin_table_update = state.resolve_builtin_table_update(builtin_table_update);
                weird_builtin_table_update = Some(builtin_table_update);
            }
        };
        Ok((weird_builtin_table_update, temporary_item_updates))
    }

    fn log_update(state: &CatalogState, id: &CatalogItemId) {
        let entry = state.get_entry(id);
        info!(
            "update {} {} ({})",
            entry.item_type(),
            state.resolve_full_name(entry.name(), entry.conn_id()),
            id
        );
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
    pub items: Vec<CatalogItemId>,
    pub network_policies: BTreeSet<NetworkPolicyId>,
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
            DropObjectInfo::NetworkPolicy(network_policy_id) => {
                let policy = state.get_network_policy(&network_policy_id);
                let name = &policy.name;
                if network_policy_id.is_system() {
                    return Err(AdapterError::Catalog(Error::new(
                        ErrorKind::ReadOnlyNetworkPolicy(name.clone()),
                    )));
                }

                self.network_policies.insert(network_policy_id);
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

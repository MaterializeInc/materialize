// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! In-memory metadata storage for the coordinator.

use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fmt::Debug;
use std::net::Ipv4Addr;
use std::sync::Arc;
use std::time::Instant;

use anyhow::bail;
use itertools::Itertools;
use mz_adapter_types::compaction::CompactionWindow;
use mz_adapter_types::connection::ConnectionId;
use mz_catalog::durable::Transaction;
use mz_sql::session::metadata::SessionMetadata;
use once_cell::sync::Lazy;
use serde::Serialize;
use timely::progress::Antichain;
use tokio::sync::mpsc;
use tracing::{info, warn};

use mz_audit_log::{EventDetails, EventType, ObjectType, VersionedEvent, VersionedStorageUsage};
use mz_build_info::DUMMY_BUILD_INFO;
use mz_catalog::builtin::{
    Builtin, BuiltinCluster, BuiltinLog, BuiltinSource, BuiltinTable, BuiltinType, BUILTINS,
};
use mz_catalog::config::{AwsPrincipalContext, ClusterReplicaSizeMap};
use mz_catalog::memory::error::{Error, ErrorKind};
use mz_catalog::memory::objects::{
    CatalogEntry, CatalogItem, Cluster, ClusterConfig, ClusterReplica, ClusterReplicaProcessStatus,
    CommentsMap, Connection, DataSourceDesc, Database, DefaultPrivileges, Index, MaterializedView,
    Role, Schema, Secret, Sink, Source, StateUpdate, StateUpdateKind, Table, Type, View,
};
use mz_catalog::SYSTEM_CONN_ID;
use mz_controller::clusters::{
    ClusterStatus, ManagedReplicaAvailabilityZones, ManagedReplicaLocation, ProcessId,
    ReplicaAllocation, ReplicaConfig, ReplicaLocation, UnmanagedReplicaLocation,
};
use mz_controller_types::{ClusterId, ReplicaId};
use mz_expr::MirScalarExpr;
use mz_ore::cast::CastFrom;
use mz_ore::collections::CollectionExt;
use mz_ore::now::{to_datetime, EpochMillis, NOW_ZERO};
use mz_ore::str::StrExt;
use mz_ore::{instrument, soft_assert_no_log};
use mz_pgrepr::oid::INVALID_OID;
use mz_repr::adt::mz_acl_item::PrivilegeMap;
use mz_repr::namespaces::{
    INFORMATION_SCHEMA, MZ_CATALOG_SCHEMA, MZ_INTERNAL_SCHEMA, MZ_TEMP_SCHEMA, MZ_UNSAFE_SCHEMA,
    PG_CATALOG_SCHEMA,
};
use mz_repr::role_id::RoleId;
use mz_repr::{Diff, GlobalId, RelationDesc};
use mz_secrets::InMemorySecretsController;
use mz_sql::catalog::{
    CatalogCluster, CatalogClusterReplica, CatalogConfig, CatalogDatabase,
    CatalogError as SqlCatalogError, CatalogItem as SqlCatalogItem, CatalogItemType, CatalogRole,
    CatalogSchema, CatalogTypeDetails, EnvironmentId, IdReference, NameReference, SessionCatalog,
    SystemObjectType, TypeReference,
};
use mz_sql::names::{
    CommentObjectId, DatabaseId, FullItemName, FullSchemaName, ItemQualifiers, ObjectId,
    PartialItemName, QualifiedItemName, QualifiedSchemaName, RawDatabaseSpecifier,
    ResolvedDatabaseSpecifier, ResolvedIds, SchemaId, SchemaSpecifier, SystemObjectId,
};
use mz_sql::plan::{
    CreateConnectionPlan, CreateIndexPlan, CreateMaterializedViewPlan, CreateSecretPlan,
    CreateSinkPlan, CreateSourcePlan, CreateTablePlan, CreateTypePlan, CreateViewPlan, Params,
    Plan, PlanContext,
};
use mz_sql::rbac;
use mz_sql::session::user::MZ_SYSTEM_ROLE_ID;
use mz_sql::session::vars::{SystemVars, Var, VarError, VarInput, DEFAULT_DATABASE_NAME};
use mz_sql_parser::ast::QualifiedReplica;
use mz_storage_client::controller::StorageMetadata;
use mz_storage_types::connections::inline::{
    ConnectionResolver, InlinedConnection, IntoInlineConnection,
};
use mz_storage_types::connections::ConnectionContext;

// DO NOT add any more imports from `crate` outside of `crate::catalog`.
use crate::catalog::{BuiltinTableUpdate, ConnCatalog};
use crate::coord::ConnMeta;
use crate::optimize::{self, Optimize};
use crate::session::Session;
use crate::util::index_sql;
use crate::AdapterError;

/// The in-memory representation of the Catalog. This struct is not directly used to persist
/// metadata to persistent storage. For persistent metadata see
/// [`mz_catalog::durable::DurableCatalogState`].
///
/// [`Serialize`] is implemented to create human readable dumps of the in-memory state, not for
/// storing the contents of this struct on disk.
#[derive(Debug, Clone, Serialize)]
pub struct CatalogState {
    pub(super) database_by_name: BTreeMap<String, DatabaseId>,
    #[serde(serialize_with = "mz_ore::serde::map_key_to_string")]
    pub(super) database_by_id: BTreeMap<DatabaseId, Database>,
    #[serde(serialize_with = "skip_temp_items")]
    pub(super) entry_by_id: BTreeMap<GlobalId, CatalogEntry>,
    pub(super) ambient_schemas_by_name: BTreeMap<String, SchemaId>,
    #[serde(serialize_with = "mz_ore::serde::map_key_to_string")]
    pub(super) ambient_schemas_by_id: BTreeMap<SchemaId, Schema>,
    #[serde(skip)]
    pub(super) temporary_schemas: BTreeMap<ConnectionId, Schema>,
    #[serde(serialize_with = "mz_ore::serde::map_key_to_string")]
    pub(super) clusters_by_id: BTreeMap<ClusterId, Cluster>,
    pub(super) clusters_by_name: BTreeMap<String, ClusterId>,
    pub(super) roles_by_name: BTreeMap<String, RoleId>,
    #[serde(serialize_with = "mz_ore::serde::map_key_to_string")]
    pub(super) roles_by_id: BTreeMap<RoleId, Role>,
    #[serde(skip)]
    pub(super) config: mz_sql::catalog::CatalogConfig,
    pub(super) cluster_replica_sizes: ClusterReplicaSizeMap,
    #[serde(skip)]
    pub(crate) availability_zones: Vec<String>,
    #[serde(skip)]
    pub(super) system_configuration: SystemVars,
    pub(super) egress_ips: Vec<Ipv4Addr>,
    pub(super) aws_principal_context: Option<AwsPrincipalContext>,
    pub(super) aws_privatelink_availability_zones: Option<BTreeSet<String>>,
    pub(super) http_host_name: Option<String>,
    pub(super) default_privileges: DefaultPrivileges,
    pub(super) system_privileges: PrivilegeMap,
    pub(super) comments: CommentsMap,
    pub(super) storage_metadata: StorageMetadata,
}

fn skip_temp_items<S>(
    entries: &BTreeMap<GlobalId, CatalogEntry>,
    serializer: S,
) -> Result<S::Ok, S::Error>
where
    S: serde::Serializer,
{
    mz_ore::serde::map_key_to_string(
        entries.iter().filter(|(_k, v)| v.conn_id().is_none()),
        serializer,
    )
}

impl CatalogState {
    pub fn empty() -> Self {
        CatalogState {
            database_by_name: Default::default(),
            database_by_id: Default::default(),
            entry_by_id: Default::default(),
            ambient_schemas_by_name: Default::default(),
            ambient_schemas_by_id: Default::default(),
            temporary_schemas: Default::default(),
            clusters_by_id: Default::default(),
            clusters_by_name: Default::default(),
            roles_by_name: Default::default(),
            roles_by_id: Default::default(),
            config: CatalogConfig {
                start_time: Default::default(),
                start_instant: Instant::now(),
                nonce: Default::default(),
                environment_id: EnvironmentId::for_tests(),
                session_id: Default::default(),
                build_info: &DUMMY_BUILD_INFO,
                timestamp_interval: Default::default(),
                now: NOW_ZERO.clone(),
                connection_context: ConnectionContext::for_tests(Arc::new(
                    InMemorySecretsController::new(),
                )),
            },
            cluster_replica_sizes: Default::default(),
            availability_zones: Default::default(),
            system_configuration: Default::default(),
            egress_ips: Default::default(),
            aws_principal_context: Default::default(),
            aws_privatelink_availability_zones: Default::default(),
            http_host_name: Default::default(),
            default_privileges: Default::default(),
            system_privileges: Default::default(),
            comments: Default::default(),
            storage_metadata: Default::default(),
        }
    }

    pub fn for_session<'a>(&'a self, session: &'a Session) -> ConnCatalog<'a> {
        let search_path = self.resolve_search_path(session);
        let database = self
            .database_by_name
            .get(session.vars().database())
            .map(|id| id.clone());
        let state = match session.transaction().catalog_state() {
            Some(txn_catalog_state) => Cow::Borrowed(txn_catalog_state),
            None => Cow::Borrowed(self),
        };
        ConnCatalog {
            state,
            unresolvable_ids: BTreeSet::new(),
            conn_id: session.conn_id().clone(),
            cluster: session.vars().cluster().into(),
            database,
            search_path,
            role_id: session.current_role_id().clone(),
            prepared_statements: Some(session.prepared_statements()),
            notices_tx: session.retain_notice_transmitter(),
        }
    }

    pub fn for_sessionless_user(&self, role_id: RoleId) -> ConnCatalog {
        let (notices_tx, _notices_rx) = mpsc::unbounded_channel();
        let cluster = self.system_configuration.default_cluster();

        ConnCatalog {
            state: Cow::Borrowed(self),
            unresolvable_ids: BTreeSet::new(),
            conn_id: SYSTEM_CONN_ID.clone(),
            cluster,
            database: self
                .resolve_database(DEFAULT_DATABASE_NAME)
                .ok()
                .map(|db| db.id()),
            // Leaving the system's search path empty allows us to catch issues
            // where catalog object names have not been normalized correctly.
            search_path: Vec::new(),
            role_id,
            prepared_statements: None,
            notices_tx,
        }
    }

    pub fn for_system_session(&self) -> ConnCatalog {
        self.for_sessionless_user(MZ_SYSTEM_ROLE_ID)
    }

    /// Computes the IDs of any indexes that transitively depend on this catalog
    /// entry.
    pub fn dependent_indexes(&self, id: GlobalId) -> Vec<GlobalId> {
        let mut out = Vec::new();
        self.dependent_indexes_inner(id, &mut out);
        out
    }

    fn dependent_indexes_inner(&self, id: GlobalId, out: &mut Vec<GlobalId>) {
        let entry = self.get_entry(&id);
        match entry.item() {
            CatalogItem::Index(_) => out.push(id),
            _ => {
                for id in entry.used_by() {
                    self.dependent_indexes_inner(*id, out)
                }
            }
        }
    }

    /// Returns an iterator over the deduplicated identifiers of all
    /// objects this catalog entry transitively depends on (where
    /// "depends on" is meant in the sense of [`CatalogItem::uses`], rather than
    /// [`CatalogItem::references`]).
    pub fn transitive_uses(&self, id: GlobalId) -> impl Iterator<Item = GlobalId> + '_ {
        struct I<'a> {
            queue: VecDeque<GlobalId>,
            seen: BTreeSet<GlobalId>,
            this: &'a CatalogState,
        }
        impl<'a> Iterator for I<'a> {
            type Item = GlobalId;
            fn next(&mut self) -> Option<Self::Item> {
                if let Some(next) = self.queue.pop_front() {
                    for child in self.this.get_entry(&next).item().uses() {
                        if !self.seen.contains(&child) {
                            self.queue.push_back(child);
                            self.seen.insert(child);
                        }
                    }
                    Some(next)
                } else {
                    None
                }
            }
        }

        I {
            queue: [id].into_iter().collect(),
            seen: [id].into_iter().collect(),
            this: self,
        }
    }

    /// Computes the IDs of any log sources this catalog entry transitively
    /// depends on.
    pub fn introspection_dependencies(&self, id: GlobalId) -> Vec<GlobalId> {
        let mut out = Vec::new();
        self.introspection_dependencies_inner(id, &mut out);
        out
    }

    fn introspection_dependencies_inner(&self, id: GlobalId, out: &mut Vec<GlobalId>) {
        match self.get_entry(&id).item() {
            CatalogItem::Log(_) => out.push(id),
            item @ (CatalogItem::View(_)
            | CatalogItem::MaterializedView(_)
            | CatalogItem::Connection(_)) => {
                // TODO(jkosh44) Unclear if this table wants to include all uses or only references.
                for id in &item.references().0 {
                    self.introspection_dependencies_inner(*id, out);
                }
            }
            CatalogItem::Sink(sink) => self.introspection_dependencies_inner(sink.from, out),
            CatalogItem::Index(idx) => self.introspection_dependencies_inner(idx.on, out),
            CatalogItem::Table(_)
            | CatalogItem::Source(_)
            | CatalogItem::Type(_)
            | CatalogItem::Func(_)
            | CatalogItem::Secret(_) => (),
        }
    }

    /// Returns all the IDs of all objects that depend on `ids`, including `ids` themselves.
    ///
    /// The order is guaranteed to be in reverse dependency order, i.e. the leafs will appear
    /// earlier in the list than the roots. This is particularly userful for the order to drop
    /// objects.
    pub(super) fn object_dependents(
        &self,
        object_ids: &Vec<ObjectId>,
        conn_id: &ConnectionId,
        seen: &mut BTreeSet<ObjectId>,
    ) -> Vec<ObjectId> {
        let mut dependents = Vec::new();
        for object_id in object_ids {
            match object_id {
                ObjectId::Cluster(id) => {
                    dependents.extend_from_slice(&self.cluster_dependents(*id, seen));
                }
                ObjectId::ClusterReplica((cluster_id, replica_id)) => dependents.extend_from_slice(
                    &self.cluster_replica_dependents(*cluster_id, *replica_id, seen),
                ),
                ObjectId::Database(id) => {
                    dependents.extend_from_slice(&self.database_dependents(*id, conn_id, seen))
                }
                ObjectId::Schema((database_spec, schema_spec)) => {
                    dependents.extend_from_slice(&self.schema_dependents(
                        database_spec.clone(),
                        schema_spec.clone(),
                        conn_id,
                        seen,
                    ));
                }
                id @ ObjectId::Role(_) => {
                    let unseen = seen.insert(id.clone());
                    if unseen {
                        dependents.push(id.clone());
                    }
                }
                ObjectId::Item(id) => {
                    dependents.extend_from_slice(&self.item_dependents(*id, seen))
                }
            }
        }
        dependents
    }

    /// Returns all the IDs of all objects that depend on `cluster_id`, including `cluster_id`
    /// itself.
    ///
    /// The order is guaranteed to be in reverse dependency order, i.e. the leafs will appear
    /// earlier in the list than the roots. This is particularly userful for the order to drop
    /// objects.
    fn cluster_dependents(
        &self,
        cluster_id: ClusterId,
        seen: &mut BTreeSet<ObjectId>,
    ) -> Vec<ObjectId> {
        let mut dependents = Vec::new();
        let object_id = ObjectId::Cluster(cluster_id);
        if !seen.contains(&object_id) {
            seen.insert(object_id.clone());
            let cluster = self.get_cluster(cluster_id);
            for item_id in cluster.bound_objects() {
                dependents.extend_from_slice(&self.item_dependents(*item_id, seen));
            }
            for replica_id in cluster.replica_ids().values() {
                dependents.extend_from_slice(&self.cluster_replica_dependents(
                    cluster_id,
                    *replica_id,
                    seen,
                ));
            }
            dependents.push(object_id);
        }
        dependents
    }

    /// Returns all the IDs of all objects that depend on `replica_id`, including `replica_id`
    /// itself.
    ///
    /// The order is guaranteed to be in reverse dependency order, i.e. the leafs will appear
    /// earlier in the list than the roots. This is particularly userful for the order to drop
    /// objects.
    pub(super) fn cluster_replica_dependents(
        &self,
        cluster_id: ClusterId,
        replica_id: ReplicaId,
        seen: &mut BTreeSet<ObjectId>,
    ) -> Vec<ObjectId> {
        let mut dependents = Vec::new();
        let object_id = ObjectId::ClusterReplica((cluster_id, replica_id));
        if !seen.contains(&object_id) {
            seen.insert(object_id.clone());
            dependents.push(object_id);
        }
        dependents
    }

    /// Returns all the IDs of all objects that depend on `database_id`, including `database_id`
    /// itself.
    ///
    /// The order is guaranteed to be in reverse dependency order, i.e. the leafs will appear
    /// earlier in the list than the roots. This is particularly userful for the order to drop
    /// objects.
    fn database_dependents(
        &self,
        database_id: DatabaseId,
        conn_id: &ConnectionId,
        seen: &mut BTreeSet<ObjectId>,
    ) -> Vec<ObjectId> {
        let mut dependents = Vec::new();
        let object_id = ObjectId::Database(database_id);
        if !seen.contains(&object_id) {
            seen.insert(object_id.clone());
            let database = self.get_database(&database_id);
            for schema_id in database.schema_ids().values() {
                dependents.extend_from_slice(&self.schema_dependents(
                    ResolvedDatabaseSpecifier::Id(database_id),
                    SchemaSpecifier::Id(*schema_id),
                    conn_id,
                    seen,
                ));
            }
            dependents.push(object_id);
        }
        dependents
    }

    /// Returns all the IDs of all objects that depend on `schema_id`, including `schema_id`
    /// itself.
    ///
    /// The order is guaranteed to be in reverse dependency order, i.e. the leafs will appear
    /// earlier in the list than the roots. This is particularly userful for the order to drop
    /// objects.
    fn schema_dependents(
        &self,
        database_spec: ResolvedDatabaseSpecifier,
        schema_spec: SchemaSpecifier,
        conn_id: &ConnectionId,
        seen: &mut BTreeSet<ObjectId>,
    ) -> Vec<ObjectId> {
        let mut dependents = Vec::new();
        let object_id = ObjectId::Schema((database_spec, schema_spec.clone()));
        if !seen.contains(&object_id) {
            seen.insert(object_id.clone());
            let schema = self.get_schema(&database_spec, &schema_spec, conn_id);
            for item_id in schema.item_ids() {
                dependents.extend_from_slice(&self.item_dependents(item_id, seen));
            }
            dependents.push(object_id)
        }
        dependents
    }

    /// Returns all the IDs of all objects that depend on `item_id`, including `item_id`
    /// itself.
    ///
    /// The order is guaranteed to be in reverse dependency order, i.e. the leafs will appear
    /// earlier in the list than the roots. This is particularly userful for the order to drop
    /// objects.
    pub(super) fn item_dependents(
        &self,
        item_id: GlobalId,
        seen: &mut BTreeSet<ObjectId>,
    ) -> Vec<ObjectId> {
        let mut dependents = Vec::new();
        let object_id = ObjectId::Item(item_id);
        if !seen.contains(&object_id) {
            seen.insert(object_id.clone());
            for dependent_id in self.get_entry(&item_id).used_by() {
                dependents.extend_from_slice(&self.item_dependents(*dependent_id, seen));
            }
            for subsource_id in self.get_entry(&item_id).subsources() {
                dependents.extend_from_slice(&self.item_dependents(subsource_id, seen));
            }
            dependents.push(object_id);
        }
        dependents
    }

    /// Indicates whether the indicated item is considered stable or not.
    ///
    /// Only stable items can be used as dependencies of other catalog items.
    fn is_stable(&self, id: GlobalId) -> bool {
        let mz_internal_id = self.ambient_schemas_by_name[MZ_INTERNAL_SCHEMA];
        match &self.get_entry(&id).name().qualifiers.schema_spec {
            SchemaSpecifier::Temporary => true,
            SchemaSpecifier::Id(id) => *id != mz_internal_id,
        }
    }

    pub(super) fn check_unstable_dependencies(&self, item: &CatalogItem) -> Result<(), Error> {
        if self.system_config().enable_unstable_dependencies() {
            return Ok(());
        }

        let unstable_dependencies: Vec<_> = item
            .references()
            .0
            .iter()
            .filter(|id| !self.is_stable(**id))
            .map(|id| self.get_entry(id).name().item.clone())
            .collect();

        // It's okay to create a temporary object with unstable
        // dependencies, since we will never need to reboot a catalog
        // that contains it.
        if unstable_dependencies.is_empty() || item.is_temporary() {
            Ok(())
        } else {
            let object_type = item.typ().to_string();
            Err(Error {
                kind: ErrorKind::UnstableDependency {
                    object_type,
                    unstable_dependencies,
                },
            })
        }
    }

    pub fn resolve_full_name(
        &self,
        name: &QualifiedItemName,
        conn_id: Option<&ConnectionId>,
    ) -> FullItemName {
        let conn_id = conn_id.unwrap_or(&SYSTEM_CONN_ID);

        let database = match &name.qualifiers.database_spec {
            ResolvedDatabaseSpecifier::Ambient => RawDatabaseSpecifier::Ambient,
            ResolvedDatabaseSpecifier::Id(id) => {
                RawDatabaseSpecifier::Name(self.get_database(id).name().to_string())
            }
        };
        let schema = self
            .get_schema(
                &name.qualifiers.database_spec,
                &name.qualifiers.schema_spec,
                conn_id,
            )
            .name()
            .schema
            .clone();
        FullItemName {
            database,
            schema,
            item: name.item.clone(),
        }
    }

    pub(super) fn resolve_full_schema_name(&self, name: &QualifiedSchemaName) -> FullSchemaName {
        let database = match &name.database {
            ResolvedDatabaseSpecifier::Ambient => RawDatabaseSpecifier::Ambient,
            ResolvedDatabaseSpecifier::Id(id) => {
                RawDatabaseSpecifier::Name(self.get_database(id).name().to_string())
            }
        };
        FullSchemaName {
            database,
            schema: name.schema.clone(),
        }
    }

    pub fn get_entry(&self, id: &GlobalId) -> &CatalogEntry {
        self.entry_by_id
            .get(id)
            .unwrap_or_else(|| panic!("catalog out of sync, missing id {id}"))
    }

    pub fn get_entry_mut(&mut self, id: &GlobalId) -> &mut CatalogEntry {
        self.entry_by_id
            .get_mut(id)
            .unwrap_or_else(|| panic!("catalog out of sync, missing id {id}"))
    }

    /// Gets a type named `name` from exactly one of the system schemas.
    ///
    /// # Panics
    /// - If `name` is not an entry in any system schema
    /// - If more than one system schema has an entry named `name`.
    pub(super) fn get_system_type(&self, name: &str) -> &CatalogEntry {
        let mut res = None;
        for system_schema in &[
            PG_CATALOG_SCHEMA,
            INFORMATION_SCHEMA,
            MZ_CATALOG_SCHEMA,
            MZ_INTERNAL_SCHEMA,
            MZ_UNSAFE_SCHEMA,
        ] {
            let schema_id = &self.ambient_schemas_by_name[*system_schema];
            let schema = &self.ambient_schemas_by_id[schema_id];
            if let Some(global_id) = schema.types.get(name) {
                match res {
                    None => res = Some(self.get_entry(global_id)),
                    Some(_) => panic!("only call get_system_type on objects uniquely identifiable in one system schema"),
                }
            }
        }

        res.unwrap_or_else(|| panic!("cannot find type {} in system schema", name))
    }

    pub fn get_item_by_name(
        &self,
        name: &QualifiedItemName,
        conn_id: &ConnectionId,
    ) -> Option<&CatalogEntry> {
        self.get_schema(
            &name.qualifiers.database_spec,
            &name.qualifiers.schema_spec,
            conn_id,
        )
        .items
        .get(&name.item)
        .and_then(|id| self.try_get_entry(id))
    }

    pub fn get_type_by_name(
        &self,
        name: &QualifiedItemName,
        conn_id: &ConnectionId,
    ) -> Option<&CatalogEntry> {
        self.get_schema(
            &name.qualifiers.database_spec,
            &name.qualifiers.schema_spec,
            conn_id,
        )
        .types
        .get(&name.item)
        .and_then(|id| self.try_get_entry(id))
    }

    pub(super) fn find_available_name(
        &self,
        mut name: QualifiedItemName,
        conn_id: &ConnectionId,
    ) -> QualifiedItemName {
        let mut i = 0;
        let orig_item_name = name.item.clone();
        while self.get_item_by_name(&name, conn_id).is_some() {
            i += 1;
            name.item = format!("{}{}", orig_item_name, i);
        }
        name
    }

    pub fn try_get_entry(&self, id: &GlobalId) -> Option<&CatalogEntry> {
        self.entry_by_id.get(id)
    }

    pub(crate) fn get_cluster(&self, cluster_id: ClusterId) -> &Cluster {
        self.try_get_cluster(cluster_id)
            .unwrap_or_else(|| panic!("unknown cluster {cluster_id}"))
    }

    pub(super) fn get_cluster_mut(&mut self, cluster_id: ClusterId) -> &mut Cluster {
        self.try_get_cluster_mut(cluster_id)
            .unwrap_or_else(|| panic!("unknown cluster {cluster_id}"))
    }

    pub(super) fn try_get_cluster(&self, cluster_id: ClusterId) -> Option<&Cluster> {
        self.clusters_by_id.get(&cluster_id)
    }

    pub(super) fn try_get_cluster_mut(&mut self, cluster_id: ClusterId) -> Option<&mut Cluster> {
        self.clusters_by_id.get_mut(&cluster_id)
    }

    pub(super) fn try_get_role(&self, id: &RoleId) -> Option<&Role> {
        self.roles_by_id.get(id)
    }

    pub fn get_role(&self, id: &RoleId) -> &Role {
        self.roles_by_id.get(id).expect("catalog out of sync")
    }

    pub fn get_roles(&self) -> impl Iterator<Item = &RoleId> {
        self.roles_by_id.keys()
    }

    pub(super) fn try_get_role_by_name(&self, role_name: &str) -> Option<&Role> {
        self.roles_by_name
            .get(role_name)
            .map(|id| &self.roles_by_id[id])
    }

    pub(super) fn get_role_mut(&mut self, id: &RoleId) -> &mut Role {
        self.roles_by_id.get_mut(id).expect("catalog out of sync")
    }

    pub(crate) fn collect_role_membership(&self, id: &RoleId) -> BTreeSet<RoleId> {
        let mut membership = BTreeSet::new();
        let mut queue = VecDeque::from(vec![id]);
        while let Some(cur_id) = queue.pop_front() {
            if !membership.contains(cur_id) {
                membership.insert(cur_id.clone());
                let role = self.get_role(cur_id);
                soft_assert_no_log!(
                    !role.membership().keys().contains(id),
                    "circular membership exists in the catalog"
                );
                queue.extend(role.membership().keys());
            }
        }
        membership.insert(RoleId::Public);
        membership
    }

    /// Returns the URL for POST-ing data to a webhook source, if `id` corresponds to a webhook
    /// source.
    ///
    /// Note: Identifiers for the source, e.g. item name, are URL encoded.
    pub fn try_get_webhook_url(&self, id: &GlobalId) -> Option<url::Url> {
        let entry = self.try_get_entry(id)?;
        // Note: Webhook sources can never be created in the temporary schema, hence passing None.
        let name = self.resolve_full_name(entry.name(), None);
        let host_name = self
            .http_host_name
            .as_ref()
            .map(|x| x.as_str())
            .unwrap_or_else(|| "HOST");

        let RawDatabaseSpecifier::Name(database) = name.database else {
            return None;
        };

        let mut url = url::Url::parse(&format!("https://{host_name}/api/webhook")).ok()?;
        url.path_segments_mut()
            .ok()?
            .push(&database)
            .push(&name.schema)
            .push(&name.item);

        Some(url)
    }

    /// Parse a SQL string into a catalog view item with only a limited
    /// context.
    #[mz_ore::instrument]
    pub fn parse_view_item(&self, create_sql: String) -> Result<CatalogItem, anyhow::Error> {
        let mut session_catalog = self.for_system_session();

        // Enable catalog features that might be required during planning in
        // [Catalog::open]. Existing catalog items might have been created while
        // a specific feature flag was turned on, so we need to ensure that this
        // is also the case during catalog rehydration in order to avoid panics.
        //
        // WARNING / CONTRACT:
        // 1. Features used in this method that related to parsing / planning
        //    should be `enable_for_item_parsing` set to `true`.
        // 2. After this step, feature flag configuration must not be
        //    overridden.
        session_catalog.system_vars_mut().enable_for_item_parsing();

        let stmt = mz_sql::parse::parse(&create_sql)?.into_element().ast;
        let (stmt, resolved_ids) = mz_sql::names::resolve(&session_catalog, stmt)?;
        let plan = mz_sql::plan::plan(
            None,
            &session_catalog,
            stmt,
            &Params::empty(),
            &resolved_ids,
        )?;
        Ok(match plan {
            Plan::CreateView(CreateViewPlan { view, .. }) => {
                // Collect optimizer parameters.
                let optimizer_config =
                    optimize::OptimizerConfig::from(session_catalog.system_vars());

                // Build an optimizer for this VIEW.
                let mut optimizer = optimize::view::Optimizer::new(optimizer_config, None);

                // HIR ⇒ MIR lowering and MIR ⇒ MIR optimization (local)
                let raw_expr = view.expr;
                let optimized_expr = optimizer.optimize(raw_expr.clone())?;

                CatalogItem::View(View {
                    create_sql: view.create_sql,
                    raw_expr,
                    desc: RelationDesc::new(optimized_expr.typ(), view.column_names),
                    optimized_expr,
                    conn_id: None,
                    resolved_ids,
                })
            }
            _ => bail!("Expected valid CREATE VIEW statement"),
        })
    }

    pub(crate) fn deserialize_plan(
        &self,
        id: GlobalId,
        create_sql: &str,
        force_if_exists_skip: bool,
    ) -> Result<(Plan, ResolvedIds), AdapterError> {
        // TODO - The `None` needs to be changed if we ever allow custom
        // logical compaction windows in user-defined objects.
        let pcx = PlanContext::zero()
            .with_planning_id(id)
            .with_ignore_if_exists_errors(force_if_exists_skip);
        let mut session_catalog = self.for_system_session();
        self.parse_plan(create_sql, Some(&pcx), &mut session_catalog)
    }

    /// Parses the given SQL string into a pair of [`Plan`] and a [`ResolvedIds)`.
    #[mz_ore::instrument]
    pub(crate) fn parse_plan(
        &self,
        create_sql: &str,
        pcx: Option<&PlanContext>,
        catalog: &mut ConnCatalog,
    ) -> Result<(Plan, ResolvedIds), AdapterError> {
        // Enable catalog features that might be required during planning in
        // [Catalog::open]. Existing catalog items might have been created while
        // a specific feature flag was turned on, so we need to ensure that this
        // is also the case during catalog rehydration in order to avoid panics.
        //
        // WARNING / CONTRACT:
        // 1. Features used in this method that related to parsing / planning
        //    should be `enable_for_item_parsing` set to `true`.
        // 2. After this step, feature flag configuration must not be
        //    overridden.
        catalog.system_vars_mut().enable_for_item_parsing();

        let stmt = mz_sql::parse::parse(create_sql)?.into_element().ast;
        let (stmt, resolved_ids) = mz_sql::names::resolve(catalog, stmt)?;
        let plan = mz_sql::plan::plan(pcx, catalog, stmt, &Params::empty(), &resolved_ids)?;

        return Ok((plan, resolved_ids));
    }

    pub(crate) fn deserialize_item(
        &self,
        id: GlobalId,
        create_sql: &str,
    ) -> Result<CatalogItem, AdapterError> {
        // TODO - The `None` needs to be changed if we ever allow custom
        // logical compaction windows in user-defined objects.
        let pcx = PlanContext::zero().with_planning_id(id);
        self.parse_item(id, create_sql, Some(&pcx), false, None)
    }

    /// Parses the given SQL string into a `CatalogItem`.
    #[mz_ore::instrument]
    pub(crate) fn parse_item(
        &self,
        id: GlobalId,
        create_sql: &str,
        pcx: Option<&PlanContext>,
        is_retained_metrics_object: bool,
        custom_logical_compaction_window: Option<CompactionWindow>,
    ) -> Result<CatalogItem, AdapterError> {
        let mut session_catalog = self.for_system_session();

        let (plan, resolved_ids) = self.parse_plan(create_sql, pcx, &mut session_catalog)?;

        Ok(match plan {
            Plan::CreateTable(CreateTablePlan { table, .. }) => CatalogItem::Table(Table {
                create_sql: Some(table.create_sql),
                desc: table.desc,
                defaults: table.defaults,
                conn_id: None,
                resolved_ids,
                custom_logical_compaction_window: custom_logical_compaction_window
                    .or(table.compaction_window),
                is_retained_metrics_object,
            }),
            Plan::CreateSource(CreateSourcePlan {
                source,
                timeline,
                in_cluster,
                ..
            }) => CatalogItem::Source(Source {
                create_sql: Some(source.create_sql),
                data_source: match source.data_source {
                    mz_sql::plan::DataSourceDesc::Ingestion(ingestion) => {
                        DataSourceDesc::ingestion(
                            id,
                            ingestion,
                            match in_cluster {
                                Some(id) => id,
                                None => {
                                    return Err(AdapterError::Unstructured(anyhow::anyhow!(
                                        "ingestion-based sources must have cluster specified"
                                    )))
                                }
                            },
                        )
                    }
                    mz_sql::plan::DataSourceDesc::SourceExport {
                        ingestion_id,
                        output_index,
                    } => DataSourceDesc::SourceExport {
                        id: ingestion_id,
                        output_index,
                    },
                    mz_sql::plan::DataSourceDesc::Progress => DataSourceDesc::Progress,
                    mz_sql::plan::DataSourceDesc::Source => DataSourceDesc::Source,
                    mz_sql::plan::DataSourceDesc::Webhook {
                        validate_using,
                        body_format,
                        headers,
                    } => DataSourceDesc::Webhook {
                        validate_using,
                        body_format,
                        headers,
                        cluster_id: in_cluster
                            .expect("webhook sources must use an existing cluster"),
                    },
                },
                desc: source.desc,
                timeline,
                resolved_ids,
                custom_logical_compaction_window: source
                    .compaction_window
                    .or(custom_logical_compaction_window),
                is_retained_metrics_object,
            }),
            Plan::CreateView(CreateViewPlan { view, .. }) => {
                // Collect optimizer parameters.
                let optimizer_config =
                    optimize::OptimizerConfig::from(session_catalog.system_vars());

                // Build an optimizer for this VIEW.
                let mut optimizer = optimize::view::Optimizer::new(optimizer_config, None);

                // HIR ⇒ MIR lowering and MIR ⇒ MIR optimization (local)
                let raw_expr = view.expr;
                let optimized_expr = optimizer.optimize(raw_expr.clone())?;

                CatalogItem::View(View {
                    create_sql: view.create_sql,
                    raw_expr,
                    desc: RelationDesc::new(optimized_expr.typ(), view.column_names),
                    optimized_expr,
                    conn_id: None,
                    resolved_ids,
                })
            }
            Plan::CreateMaterializedView(CreateMaterializedViewPlan {
                materialized_view, ..
            }) => {
                // Collect optimizer parameters.
                let optimizer_config =
                    optimize::OptimizerConfig::from(session_catalog.system_vars());
                // Build an optimizer for this VIEW.
                // TODO(aalexandrov): ideally this should be a materialized_view::Optimizer.
                let mut optimizer = optimize::view::Optimizer::new(optimizer_config, None);

                let raw_expr = materialized_view.expr;
                let optimized_expr = optimizer.optimize(raw_expr.clone())?;
                let mut typ = optimized_expr.typ();
                for &i in &materialized_view.non_null_assertions {
                    typ.column_types[i].nullable = false;
                }
                let desc = RelationDesc::new(typ, materialized_view.column_names);

                let initial_as_of = materialized_view
                    .as_of
                    .map(|time| Antichain::from_elem(time.into()));

                CatalogItem::MaterializedView(MaterializedView {
                    create_sql: materialized_view.create_sql,
                    raw_expr,
                    optimized_expr,
                    desc,
                    resolved_ids,
                    cluster_id: materialized_view.cluster_id,
                    non_null_assertions: materialized_view.non_null_assertions,
                    custom_logical_compaction_window: materialized_view.compaction_window,
                    refresh_schedule: materialized_view.refresh_schedule,
                    initial_as_of,
                })
            }
            Plan::CreateIndex(CreateIndexPlan { index, .. }) => CatalogItem::Index(Index {
                create_sql: index.create_sql,
                on: index.on,
                keys: index.keys,
                conn_id: None,
                resolved_ids,
                cluster_id: index.cluster_id,
                custom_logical_compaction_window: custom_logical_compaction_window
                    .or(index.compaction_window),
                is_retained_metrics_object,
            }),
            Plan::CreateSink(CreateSinkPlan {
                sink,
                with_snapshot,
                in_cluster,
                ..
            }) => CatalogItem::Sink(Sink {
                create_sql: sink.create_sql,
                from: sink.from,
                connection: sink.connection,
                envelope: sink.envelope,
                with_snapshot,
                resolved_ids,
                cluster_id: in_cluster,
            }),
            Plan::CreateType(CreateTypePlan { typ, .. }) => CatalogItem::Type(Type {
                create_sql: Some(typ.create_sql),
                desc: typ.inner.desc(&session_catalog)?,
                details: CatalogTypeDetails {
                    array_id: None,
                    typ: typ.inner,
                    pg_metadata: None,
                },
                resolved_ids,
            }),
            Plan::CreateSecret(CreateSecretPlan { secret, .. }) => CatalogItem::Secret(Secret {
                create_sql: secret.create_sql,
            }),
            Plan::CreateConnection(CreateConnectionPlan {
                connection:
                    mz_sql::plan::Connection {
                        create_sql,
                        connection,
                    },
                ..
            }) => CatalogItem::Connection(Connection {
                create_sql,
                connection,
                resolved_ids,
            }),
            _ => {
                return Err(Error::new(ErrorKind::Corruption {
                    detail: "catalog entry generated inappropriate plan".to_string(),
                })
                .into())
            }
        })
    }

    /// Returns all indexes on the given object and cluster known in the
    /// catalog.
    pub fn get_indexes_on(
        &self,
        id: GlobalId,
        cluster: ClusterId,
    ) -> impl Iterator<Item = (GlobalId, &Index)> {
        let index_matches = move |idx: &Index| idx.on == id && idx.cluster_id == cluster;

        self.try_get_entry(&id)
            .into_iter()
            .map(move |e| {
                e.used_by()
                    .iter()
                    .filter_map(move |uses_id| match self.get_entry(uses_id).item() {
                        CatalogItem::Index(index) if index_matches(index) => {
                            Some((*uses_id, index))
                        }
                        _ => None,
                    })
            })
            .flatten()
    }

    /// Associates a name, `GlobalId`, and entry.
    pub(super) fn insert_item(
        &mut self,
        id: GlobalId,
        oid: u32,
        name: QualifiedItemName,
        item: CatalogItem,
        owner_id: RoleId,
        privileges: PrivilegeMap,
    ) {
        if !id.is_system() {
            info!(
                "create {} {} ({})",
                item.typ(),
                self.resolve_full_name(&name, None),
                id
            );
        }

        if !id.is_system() {
            if let Some(cluster_id) = item.cluster_id() {
                self.clusters_by_id
                    .get_mut(&cluster_id)
                    .expect("catalog out of sync")
                    .bound_objects
                    .insert(id);
            };
        }

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
        for u in &entry.references().0 {
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
            match self.entry_by_id.get_mut(&u) {
                Some(metadata) => metadata.used_by.push(entry.id()),
                None => panic!(
                    "Catalog: missing dependent catalog item {} while installing {}",
                    &u,
                    self.resolve_full_name(entry.name(), entry.conn_id())
                ),
            }
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

    #[mz_ore::instrument(level = "trace")]
    pub(super) fn drop_item(&mut self, id: GlobalId) {
        let metadata = self.entry_by_id.remove(&id).expect("catalog out of sync");
        info!(
            "drop {} {} ({})",
            metadata.item_type(),
            self.resolve_full_name(metadata.name(), metadata.conn_id()),
            id
        );
        for u in &metadata.references().0 {
            if let Some(dep_metadata) = self.entry_by_id.get_mut(u) {
                dep_metadata.referenced_by.retain(|u| *u != metadata.id())
            }
        }
        for u in metadata.uses() {
            if let Some(dep_metadata) = self.entry_by_id.get_mut(&u) {
                dep_metadata.used_by.retain(|u| *u != metadata.id())
            }
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
    }

    pub(super) fn get_database(&self, database_id: &DatabaseId) -> &Database {
        &self.database_by_id[database_id]
    }

    pub(super) fn get_database_mut(&mut self, database_id: &DatabaseId) -> &mut Database {
        self.database_by_id
            .get_mut(database_id)
            .expect("catalog out of sync")
    }

    pub(super) fn insert_cluster(
        &mut self,
        id: ClusterId,
        name: String,
        introspection_source_indexes: Vec<(&'static BuiltinLog, GlobalId, u32)>,
        owner_id: RoleId,
        privileges: PrivilegeMap,
        config: ClusterConfig,
    ) {
        let mut log_indexes = BTreeMap::new();
        for (log, index_id, oid) in introspection_source_indexes {
            let source_name = FullItemName {
                database: RawDatabaseSpecifier::Ambient,
                schema: log.schema.into(),
                item: log.name.into(),
            };
            let index_name = format!("{}_{}_primary_idx", log.name, id);
            let mut index_name = QualifiedItemName {
                qualifiers: ItemQualifiers {
                    database_spec: ResolvedDatabaseSpecifier::Ambient,
                    schema_spec: SchemaSpecifier::Id(self.get_mz_internal_schema_id().clone()),
                },
                item: index_name.clone(),
            };
            index_name = self.find_available_name(index_name, &SYSTEM_CONN_ID);
            let index_item_name = index_name.item.clone();
            let log_id = self.resolve_builtin_log(log);
            self.insert_item(
                index_id,
                oid,
                index_name,
                CatalogItem::Index(Index {
                    on: log_id,
                    keys: log
                        .variant
                        .index_by()
                        .into_iter()
                        .map(MirScalarExpr::Column)
                        .collect(),
                    create_sql: index_sql(
                        index_item_name,
                        id,
                        source_name,
                        &log.variant.desc(),
                        &log.variant.index_by(),
                    ),
                    conn_id: None,
                    resolved_ids: ResolvedIds(BTreeSet::from_iter([log_id])),
                    cluster_id: id,
                    is_retained_metrics_object: false,
                    custom_logical_compaction_window: None,
                }),
                MZ_SYSTEM_ROLE_ID,
                PrivilegeMap::default(),
            );
            log_indexes.insert(log.variant.clone(), index_id);
        }

        self.clusters_by_id.insert(
            id,
            Cluster {
                name: name.clone(),
                id,
                bound_objects: BTreeSet::new(),
                log_indexes,
                replica_id_by_name_: BTreeMap::new(),
                replicas_by_id_: BTreeMap::new(),
                owner_id,
                privileges,
                config,
            },
        );
        assert!(self.clusters_by_name.insert(name, id).is_none());
    }

    pub(super) fn rename_cluster(&mut self, id: ClusterId, to_name: String) {
        let cluster = self.get_cluster_mut(id);
        let old_name = std::mem::take(&mut cluster.name);
        cluster.name = to_name.clone();

        assert!(self.clusters_by_name.remove(&old_name).is_some());
        assert!(self.clusters_by_name.insert(to_name, id).is_none());
    }

    pub(super) fn insert_cluster_replica(
        &mut self,
        cluster_id: ClusterId,
        replica_name: String,
        replica_id: ReplicaId,
        config: ReplicaConfig,
        owner_id: RoleId,
    ) {
        let replica = ClusterReplica {
            name: replica_name.clone(),
            cluster_id,
            replica_id,
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
            owner_id,
        };
        let cluster = self
            .clusters_by_id
            .get_mut(&cluster_id)
            .expect("catalog out of sync");
        cluster.insert_replica(replica);
    }

    /// Renames a cluster replica.
    ///
    /// Panics if the cluster or cluster replica does not exist.
    pub(super) fn rename_cluster_replica(
        &mut self,
        cluster_id: ClusterId,
        replica_id: ReplicaId,
        to_name: String,
    ) {
        let cluster = self.get_cluster_mut(cluster_id);
        cluster.rename_replica(replica_id, to_name);
    }

    /// Inserts or updates the status of the specified cluster replica process.
    ///
    /// Panics if the cluster or replica does not exist.
    pub(super) fn ensure_cluster_status(
        &mut self,
        cluster_id: ClusterId,
        replica_id: ReplicaId,
        process_id: ProcessId,
        status: ClusterReplicaProcessStatus,
    ) {
        let replica = self.get_cluster_replica_mut(cluster_id, replica_id);
        replica.process_status.insert(process_id, status);
    }

    /// Gets a reference to the specified replica of the specified cluster.
    ///
    /// Returns `None` if either the cluster or the replica does not
    /// exist.
    fn try_get_cluster_replica(
        &self,
        id: ClusterId,
        replica_id: ReplicaId,
    ) -> Option<&ClusterReplica> {
        self.try_get_cluster(id)
            .and_then(|cluster| cluster.replica(replica_id))
    }

    /// Gets a reference to the specified replica of the specified cluster.
    ///
    /// Panics if either the cluster or the replica does not exist.
    pub(super) fn get_cluster_replica(
        &self,
        cluster_id: ClusterId,
        replica_id: ReplicaId,
    ) -> &ClusterReplica {
        self.try_get_cluster_replica(cluster_id, replica_id)
            .unwrap_or_else(|| panic!("unknown cluster replica: {cluster_id}.{replica_id}"))
    }

    /// Gets a mutable reference to the specified replica of the specified
    /// cluster.
    ///
    /// Returns `None` if either the clustere or the replica does not
    /// exist.
    fn try_get_cluster_replica_mut(
        &mut self,
        id: ClusterId,
        replica_id: ReplicaId,
    ) -> Option<&mut ClusterReplica> {
        self.try_get_cluster_mut(id)
            .and_then(|cluster| cluster.replica_mut(replica_id))
    }

    /// Gets a mutable reference to the specified replica of the specified
    /// cluster.
    ///
    /// Panics if either the cluster or the replica does not exist.
    fn get_cluster_replica_mut(
        &mut self,
        cluster_id: ClusterId,
        replica_id: ReplicaId,
    ) -> &mut ClusterReplica {
        self.try_get_cluster_replica_mut(cluster_id, replica_id)
            .unwrap_or_else(|| panic!("unknown cluster replica: {cluster_id}.{replica_id}"))
    }

    /// Gets the status of the given cluster replica process.
    ///
    /// Panics if the cluster or replica does not exist
    pub(super) fn get_cluster_status(
        &self,
        cluster_id: ClusterId,
        replica_id: ReplicaId,
        process_id: ProcessId,
    ) -> &ClusterReplicaProcessStatus {
        &self
            .get_cluster_replica(cluster_id, replica_id)
            .process_status[&process_id]
    }

    /// Get system configuration `name`.
    pub fn get_system_configuration(&self, name: &str) -> Result<&dyn Var, Error> {
        Ok(self.system_configuration.get(name)?)
    }

    /// Set the default value for `name`, which is the value it will be reset to.
    pub(super) fn set_system_configuration_default(
        &mut self,
        name: &str,
        value: VarInput,
    ) -> Result<(), Error> {
        Ok(self.system_configuration.set_default(name, value)?)
    }

    /// Insert system configuration `name` with `value`.
    ///
    /// Return a `bool` value indicating whether the configuration was modified
    /// by the call.
    pub(super) fn insert_system_configuration(
        &mut self,
        name: &str,
        value: VarInput,
    ) -> Result<bool, Error> {
        Ok(self.system_configuration.set(name, value)?)
    }

    /// Reset system configuration `name`.
    ///
    /// Return a `bool` value indicating whether the configuration was modified
    /// by the call.
    pub(super) fn remove_system_configuration(&mut self, name: &str) -> Result<bool, Error> {
        Ok(self.system_configuration.reset(name)?)
    }

    /// Remove all system configurations.
    pub(super) fn clear_system_configuration(&mut self) {
        self.system_configuration.reset_all();
    }

    /// Gets the schema map for the database matching `database_spec`.
    pub(super) fn resolve_schema_in_database(
        &self,
        database_spec: &ResolvedDatabaseSpecifier,
        schema_name: &str,
        conn_id: &ConnectionId,
    ) -> Result<&Schema, SqlCatalogError> {
        let schema = match database_spec {
            ResolvedDatabaseSpecifier::Ambient if schema_name == MZ_TEMP_SCHEMA => {
                self.temporary_schemas.get(conn_id)
            }
            ResolvedDatabaseSpecifier::Ambient => self
                .ambient_schemas_by_name
                .get(schema_name)
                .and_then(|id| self.ambient_schemas_by_id.get(id)),
            ResolvedDatabaseSpecifier::Id(id) => self.database_by_id.get(id).and_then(|db| {
                db.schemas_by_name
                    .get(schema_name)
                    .and_then(|id| db.schemas_by_id.get(id))
            }),
        };
        schema.ok_or_else(|| SqlCatalogError::UnknownSchema(schema_name.into()))
    }

    pub fn get_schema(
        &self,
        database_spec: &ResolvedDatabaseSpecifier,
        schema_spec: &SchemaSpecifier,
        conn_id: &ConnectionId,
    ) -> &Schema {
        // Keep in sync with `get_schemas_mut`
        match (database_spec, schema_spec) {
            (ResolvedDatabaseSpecifier::Ambient, SchemaSpecifier::Temporary) => {
                &self.temporary_schemas[conn_id]
            }
            (ResolvedDatabaseSpecifier::Ambient, SchemaSpecifier::Id(id)) => {
                &self.ambient_schemas_by_id[id]
            }

            (ResolvedDatabaseSpecifier::Id(database_id), SchemaSpecifier::Id(schema_id)) => {
                &self.database_by_id[database_id].schemas_by_id[schema_id]
            }
            (ResolvedDatabaseSpecifier::Id(_), SchemaSpecifier::Temporary) => {
                unreachable!("temporary schemas are in the ambient database")
            }
        }
    }

    pub(super) fn get_schema_mut(
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

    pub(super) fn find_non_temp_schema(&self, schema_id: &SchemaId) -> &Schema {
        self.database_by_id
            .values()
            .filter_map(|database| database.schemas_by_id.get(schema_id))
            .chain(self.ambient_schemas_by_id.values())
            .filter(|schema| schema.id() == &SchemaSpecifier::from(*schema_id))
            .into_first()
    }

    pub fn get_mz_catalog_schema_id(&self) -> &SchemaId {
        &self.ambient_schemas_by_name[MZ_CATALOG_SCHEMA]
    }

    pub fn get_pg_catalog_schema_id(&self) -> &SchemaId {
        &self.ambient_schemas_by_name[PG_CATALOG_SCHEMA]
    }

    pub fn get_information_schema_id(&self) -> &SchemaId {
        &self.ambient_schemas_by_name[INFORMATION_SCHEMA]
    }

    pub fn get_mz_internal_schema_id(&self) -> &SchemaId {
        &self.ambient_schemas_by_name[MZ_INTERNAL_SCHEMA]
    }

    pub fn get_mz_unsafe_schema_id(&self) -> &SchemaId {
        &self.ambient_schemas_by_name[MZ_UNSAFE_SCHEMA]
    }

    pub fn is_system_schema(&self, schema: &str) -> bool {
        schema == MZ_CATALOG_SCHEMA
            || schema == PG_CATALOG_SCHEMA
            || schema == INFORMATION_SCHEMA
            || schema == MZ_INTERNAL_SCHEMA
            || schema == MZ_UNSAFE_SCHEMA
    }

    pub fn is_system_schema_id(&self, id: &SchemaId) -> bool {
        id == self.get_mz_catalog_schema_id()
            || id == self.get_pg_catalog_schema_id()
            || id == self.get_information_schema_id()
            || id == self.get_mz_internal_schema_id()
            || id == self.get_mz_unsafe_schema_id()
    }

    pub fn is_system_schema_specifier(&self, spec: &SchemaSpecifier) -> bool {
        match spec {
            SchemaSpecifier::Temporary => false,
            SchemaSpecifier::Id(id) => self.is_system_schema_id(id),
        }
    }

    /// Creates a new schema in the `Catalog` for temporary items
    /// indicated by the TEMPORARY or TEMP keywords.
    pub fn create_temporary_schema(
        &mut self,
        conn_id: &ConnectionId,
        owner_id: RoleId,
    ) -> Result<(), Error> {
        // Temporary schema OIDs are never used, and it's therefore wasteful to go to the durable
        // catalog to allocate a new OID for every temporary schema. Instead, we give them all the
        // same invalid OID. This matches the semantics of temporary schema `GlobalId`s which are
        // all -1.
        let oid = INVALID_OID;
        self.temporary_schemas.insert(
            conn_id.clone(),
            Schema {
                name: QualifiedSchemaName {
                    database: ResolvedDatabaseSpecifier::Ambient,
                    schema: MZ_TEMP_SCHEMA.into(),
                },
                id: SchemaSpecifier::Temporary,
                oid,
                items: BTreeMap::new(),
                functions: BTreeMap::new(),
                types: BTreeMap::new(),
                owner_id,
                privileges: PrivilegeMap::from_mz_acl_items(vec![rbac::owner_privilege(
                    mz_sql::catalog::ObjectType::Schema,
                    owner_id,
                )]),
            },
        );
        Ok(())
    }

    /// Optimized lookup for a builtin table.
    ///
    /// Panics if the builtin table doesn't exist in the catalog.
    pub fn resolve_builtin_table(&self, builtin: &'static BuiltinTable) -> GlobalId {
        self.resolve_builtin_object(&Builtin::<IdReference>::Table(builtin))
    }

    /// Optimized lookup for a builtin log.
    ///
    /// Panics if the builtin log doesn't exist in the catalog.
    pub fn resolve_builtin_log(&self, builtin: &'static BuiltinLog) -> GlobalId {
        self.resolve_builtin_object(&Builtin::<IdReference>::Log(builtin))
    }

    /// Optimized lookup for a builtin storage collection.
    ///
    /// Panics if the builtin storage collection doesn't exist in the catalog.
    pub fn resolve_builtin_source(&self, builtin: &'static BuiltinSource) -> GlobalId {
        self.resolve_builtin_object(&Builtin::<IdReference>::Source(builtin))
    }

    /// Optimized lookup for a builtin object.
    ///
    /// Panics if the builtin object doesn't exist in the catalog.
    pub fn resolve_builtin_object<T: TypeReference>(&self, builtin: &Builtin<T>) -> GlobalId {
        let schema_id = &self.ambient_schemas_by_name[builtin.schema()];
        let schema = &self.ambient_schemas_by_id[schema_id];
        schema.items[builtin.name()].clone()
    }

    pub fn config(&self) -> &mz_sql::catalog::CatalogConfig {
        &self.config
    }

    pub fn resolve_database(&self, database_name: &str) -> Result<&Database, SqlCatalogError> {
        match self.database_by_name.get(database_name) {
            Some(id) => Ok(&self.database_by_id[id]),
            None => Err(SqlCatalogError::UnknownDatabase(database_name.into())),
        }
    }

    pub fn resolve_schema(
        &self,
        current_database: Option<&DatabaseId>,
        database_name: Option<&str>,
        schema_name: &str,
        conn_id: &ConnectionId,
    ) -> Result<&Schema, SqlCatalogError> {
        let database_spec = match database_name {
            // If a database is explicitly specified, validate it. Note that we
            // intentionally do not validate `current_database` to permit
            // querying `mz_catalog` with an invalid session database, e.g., so
            // that you can run `SHOW DATABASES` to *find* a valid database.
            Some(database) => Some(ResolvedDatabaseSpecifier::Id(
                self.resolve_database(database)?.id().clone(),
            )),
            None => current_database.map(|id| ResolvedDatabaseSpecifier::Id(id.clone())),
        };

        // First try to find the schema in the named database.
        if let Some(database_spec) = database_spec {
            if let Ok(schema) =
                self.resolve_schema_in_database(&database_spec, schema_name, conn_id)
            {
                return Ok(schema);
            }
        }

        // Then fall back to the ambient database.
        if let Ok(schema) = self.resolve_schema_in_database(
            &ResolvedDatabaseSpecifier::Ambient,
            schema_name,
            conn_id,
        ) {
            return Ok(schema);
        }

        Err(SqlCatalogError::UnknownSchema(schema_name.into()))
    }

    /// Optimized lookup for a system schema.
    ///
    /// Panics if the system schema doesn't exist in the catalog.
    pub fn resolve_system_schema(&self, name: &str) -> SchemaId {
        self.ambient_schemas_by_name[name]
    }

    pub fn resolve_search_path(
        &self,
        session: &dyn SessionMetadata,
    ) -> Vec<(ResolvedDatabaseSpecifier, SchemaSpecifier)> {
        let database = self
            .database_by_name
            .get(session.database())
            .map(|id| id.clone());

        session
            .search_path()
            .iter()
            .map(|schema| {
                self.resolve_schema(database.as_ref(), None, schema.as_str(), session.conn_id())
            })
            .filter_map(|schema| schema.ok())
            .map(|schema| (schema.name().database.clone(), schema.id().clone()))
            .collect()
    }

    pub fn effective_search_path(
        &self,
        search_path: &[(ResolvedDatabaseSpecifier, SchemaSpecifier)],
        include_temp_schema: bool,
    ) -> Vec<(ResolvedDatabaseSpecifier, SchemaSpecifier)> {
        let mut v = Vec::with_capacity(search_path.len() + 3);
        // Temp schema is only included for relations and data types, not for functions and operators
        let temp_schema = (
            ResolvedDatabaseSpecifier::Ambient,
            SchemaSpecifier::Temporary,
        );
        if include_temp_schema && !search_path.contains(&temp_schema) {
            v.push(temp_schema);
        }
        let default_schemas = [
            (
                ResolvedDatabaseSpecifier::Ambient,
                SchemaSpecifier::Id(self.get_mz_catalog_schema_id().clone()),
            ),
            (
                ResolvedDatabaseSpecifier::Ambient,
                SchemaSpecifier::Id(self.get_pg_catalog_schema_id().clone()),
            ),
        ];
        for schema in default_schemas.into_iter() {
            if !search_path.contains(&schema) {
                v.push(schema);
            }
        }
        v.extend_from_slice(search_path);
        v
    }

    pub fn resolve_cluster(&self, name: &str) -> Result<&Cluster, SqlCatalogError> {
        let id = self
            .clusters_by_name
            .get(name)
            .ok_or_else(|| SqlCatalogError::UnknownCluster(name.to_string()))?;
        Ok(&self.clusters_by_id[id])
    }

    pub fn resolve_builtin_cluster(&self, cluster: &BuiltinCluster) -> &Cluster {
        let id = self
            .clusters_by_name
            .get(cluster.name)
            .expect("failed to lookup BuiltinCluster by name");
        self.clusters_by_id
            .get(id)
            .expect("failed to lookup BuiltinCluster by ID")
    }

    pub fn resolve_cluster_replica(
        &self,
        cluster_replica_name: &QualifiedReplica,
    ) -> Result<&ClusterReplica, SqlCatalogError> {
        let cluster = self.resolve_cluster(cluster_replica_name.cluster.as_str())?;
        let replica_name = cluster_replica_name.replica.as_str();
        let replica_id = cluster
            .replica_id(replica_name)
            .ok_or_else(|| SqlCatalogError::UnknownClusterReplica(replica_name.to_string()))?;
        Ok(cluster.replica(replica_id).expect("Must exist"))
    }

    /// Resolves [`PartialItemName`] into a [`CatalogEntry`].
    ///
    /// If `name` does not specify a database, the `current_database` is used.
    /// If `name` does not specify a schema, then the schemas in `search_path`
    /// are searched in order.
    #[allow(clippy::useless_let_if_seq)]
    pub fn resolve(
        &self,
        get_schema_entries: fn(&Schema) -> &BTreeMap<String, GlobalId>,
        current_database: Option<&DatabaseId>,
        search_path: &Vec<(ResolvedDatabaseSpecifier, SchemaSpecifier)>,
        name: &PartialItemName,
        conn_id: &ConnectionId,
        err_gen: fn(String) -> SqlCatalogError,
    ) -> Result<&CatalogEntry, SqlCatalogError> {
        // If a schema name was specified, just try to find the item in that
        // schema. If no schema was specified, try to find the item in the connection's
        // temporary schema. If the item is not found, try to find the item in every
        // schema in the search path.
        let schemas = match &name.schema {
            Some(schema_name) => {
                match self.resolve_schema(
                    current_database,
                    name.database.as_deref(),
                    schema_name,
                    conn_id,
                ) {
                    Ok(schema) => vec![(schema.name.database.clone(), schema.id.clone())],
                    Err(e) => return Err(e),
                }
            }
            None => match self
                .get_schema(
                    &ResolvedDatabaseSpecifier::Ambient,
                    &SchemaSpecifier::Temporary,
                    conn_id,
                )
                .items
                .get(&name.item)
            {
                Some(id) => return Ok(self.get_entry(id)),
                None => search_path.to_vec(),
            },
        };

        for (database_spec, schema_spec) in schemas {
            let schema = self.get_schema(&database_spec, &schema_spec, conn_id);

            if let Some(id) = get_schema_entries(schema).get(&name.item) {
                return Ok(&self.entry_by_id[id]);
            }
        }
        Err(err_gen(name.to_string()))
    }

    /// Resolves `name` to a non-function [`CatalogEntry`].
    pub fn resolve_entry(
        &self,
        current_database: Option<&DatabaseId>,
        search_path: &Vec<(ResolvedDatabaseSpecifier, SchemaSpecifier)>,
        name: &PartialItemName,
        conn_id: &ConnectionId,
    ) -> Result<&CatalogEntry, SqlCatalogError> {
        self.resolve(
            |schema| &schema.items,
            current_database,
            search_path,
            name,
            conn_id,
            SqlCatalogError::UnknownItem,
        )
    }

    /// Resolves `name` to a function [`CatalogEntry`].
    pub fn resolve_function(
        &self,
        current_database: Option<&DatabaseId>,
        search_path: &Vec<(ResolvedDatabaseSpecifier, SchemaSpecifier)>,
        name: &PartialItemName,
        conn_id: &ConnectionId,
    ) -> Result<&CatalogEntry, SqlCatalogError> {
        self.resolve(
            |schema| &schema.functions,
            current_database,
            search_path,
            name,
            conn_id,
            |name| SqlCatalogError::UnknownFunction {
                name,
                alternative: None,
            },
        )
    }

    /// Resolves `name` to a type [`CatalogEntry`].
    pub fn resolve_type(
        &self,
        current_database: Option<&DatabaseId>,
        search_path: &Vec<(ResolvedDatabaseSpecifier, SchemaSpecifier)>,
        name: &PartialItemName,
        conn_id: &ConnectionId,
    ) -> Result<&CatalogEntry, SqlCatalogError> {
        static NON_PG_CATALOG_TYPES: Lazy<
            BTreeMap<&'static str, &'static BuiltinType<NameReference>>,
        > = Lazy::new(|| {
            BUILTINS::types()
                .filter(|typ| typ.schema != PG_CATALOG_SCHEMA)
                .map(|typ| (typ.name, typ))
                .collect()
        });

        let entry = self.resolve(
            |schema| &schema.types,
            current_database,
            search_path,
            name,
            conn_id,
            |name| SqlCatalogError::UnknownType { name },
        )?;

        if conn_id != &SYSTEM_CONN_ID && name.schema.as_deref() == Some(PG_CATALOG_SCHEMA) {
            if let Some(typ) = NON_PG_CATALOG_TYPES.get(entry.name().item.as_str()) {
                warn!(
                    "user specified an incorrect schema of {} for the type {}, which should be in \
                    the {} schema. This works now due to a bug but will be fixed in a later release.",
                    PG_CATALOG_SCHEMA.quoted(),
                    typ.name.quoted(),
                    typ.schema.quoted(),
                )
            }
        }

        Ok(entry)
    }

    /// For an [`ObjectId`] gets the corresponding [`CommentObjectId`].
    pub(super) fn get_comment_id(&self, object_id: ObjectId) -> CommentObjectId {
        match object_id {
            ObjectId::Item(global_id) => {
                let entry = self.get_entry(&global_id);
                match entry.item_type() {
                    CatalogItemType::Table => CommentObjectId::Table(global_id),
                    CatalogItemType::Source => CommentObjectId::Source(global_id),
                    CatalogItemType::Sink => CommentObjectId::Sink(global_id),
                    CatalogItemType::View => CommentObjectId::View(global_id),
                    CatalogItemType::MaterializedView => {
                        CommentObjectId::MaterializedView(global_id)
                    }
                    CatalogItemType::Index => CommentObjectId::Index(global_id),
                    CatalogItemType::Func => CommentObjectId::Func(global_id),
                    CatalogItemType::Connection => CommentObjectId::Connection(global_id),
                    CatalogItemType::Type => CommentObjectId::Type(global_id),
                    CatalogItemType::Secret => CommentObjectId::Secret(global_id),
                }
            }
            ObjectId::Role(role_id) => CommentObjectId::Role(role_id),
            ObjectId::Database(database_id) => CommentObjectId::Database(database_id),
            ObjectId::Schema((database, schema)) => CommentObjectId::Schema((database, schema)),
            ObjectId::Cluster(cluster_id) => CommentObjectId::Cluster(cluster_id),
            ObjectId::ClusterReplica(cluster_replica_id) => {
                CommentObjectId::ClusterReplica(cluster_replica_id)
            }
        }
    }

    /// Return current system configuration.
    pub fn system_config(&self) -> &SystemVars {
        &self.system_configuration
    }

    /// Serializes the catalog's in-memory state.
    ///
    /// There are no guarantees about the format of the serialized state, except
    /// that the serialized state for two identical catalogs will compare
    /// identically.
    pub fn dump(&self) -> Result<String, Error> {
        // Dump the base catalog.
        let mut dump = serde_json::to_value(&self).map_err(|e| {
            Error::new(ErrorKind::Unstructured(format!(
                // Don't panic here because we don't have compile-time failures for maps with
                // non-string keys.
                "internal error: could not dump catalog: {}",
                e
            )))
        })?;

        // Stitch in system parameter defaults.
        dump.as_object_mut()
            .expect("state must have been dumped")
            .insert(
                "system_parameter_defaults".into(),
                serde_json::json!(self.system_config().defaults()),
            );

        // Emit as pretty-printed JSON.
        Ok(serde_json::to_string_pretty(&dump).expect("cannot fail on serde_json::Value"))
    }

    pub fn availability_zones(&self) -> &[String] {
        &self.availability_zones
    }

    pub fn concretize_replica_location(
        &self,
        location: mz_catalog::durable::ReplicaLocation,
        allowed_sizes: &Vec<String>,
        allowed_availability_zones: Option<&[String]>,
    ) -> Result<ReplicaLocation, Error> {
        let location = match location {
            mz_catalog::durable::ReplicaLocation::Unmanaged {
                storagectl_addrs,
                storage_addrs,
                computectl_addrs,
                compute_addrs,
                workers,
            } => {
                if allowed_availability_zones.is_some() {
                    return Err(Error {
                        kind: ErrorKind::Internal(
                            "tried concretize unmanaged replica with specific availability_zones"
                                .to_string(),
                        ),
                    });
                }
                ReplicaLocation::Unmanaged(UnmanagedReplicaLocation {
                    storagectl_addrs,
                    storage_addrs,
                    computectl_addrs,
                    compute_addrs,
                    workers,
                })
            }
            mz_catalog::durable::ReplicaLocation::Managed {
                size,
                availability_zone,
                disk,
                billed_as,
                internal,
            } => {
                if allowed_availability_zones.is_some() && availability_zone.is_some() {
                    return Err(Error {
                        kind: ErrorKind::Internal(
                            "tried concretize managed replica with specific availability zones and availability zone".to_string(),
                        ),
                    });
                }
                self.ensure_valid_replica_size(allowed_sizes, &size)?;
                let cluster_replica_sizes = &self.cluster_replica_sizes;

                ReplicaLocation::Managed(ManagedReplicaLocation {
                    allocation: cluster_replica_sizes
                        .0
                        .get(&size)
                        .expect("catalog out of sync")
                        .clone(),
                    availability_zones: match (availability_zone, allowed_availability_zones) {
                        (Some(az), _) => ManagedReplicaAvailabilityZones::FromReplica(Some(az)),
                        (None, Some(azs)) if azs.is_empty() => {
                            ManagedReplicaAvailabilityZones::FromCluster(None)
                        }
                        (None, Some(azs)) => {
                            ManagedReplicaAvailabilityZones::FromCluster(Some(azs.to_vec()))
                        }
                        (None, None) => ManagedReplicaAvailabilityZones::FromReplica(None),
                    },
                    size,
                    disk,
                    billed_as,
                    internal,
                })
            }
        };
        Ok(location)
    }

    pub(crate) fn ensure_valid_replica_size(
        &self,
        allowed_sizes: &[String],
        size: &String,
    ) -> Result<(), Error> {
        let cluster_replica_sizes = &self.cluster_replica_sizes;

        if !cluster_replica_sizes.0.contains_key(size)
            || (!allowed_sizes.is_empty() && !allowed_sizes.contains(size))
            || cluster_replica_sizes.0[size].disabled
        {
            let mut entries = cluster_replica_sizes
                .enabled_allocations()
                .collect::<Vec<_>>();

            if !allowed_sizes.is_empty() {
                let allowed_sizes = BTreeSet::<&String>::from_iter(allowed_sizes.iter());
                entries.retain(|(name, _)| allowed_sizes.contains(name));
            }

            entries.sort_by_key(
                |(
                    _name,
                    ReplicaAllocation {
                        scale, cpu_limit, ..
                    },
                )| (scale, cpu_limit),
            );

            Err(Error {
                kind: ErrorKind::InvalidClusterReplicaSize {
                    size: size.to_owned(),
                    expected: entries.into_iter().map(|(name, _)| name.clone()).collect(),
                },
            })
        } else {
            Ok(())
        }
    }

    pub fn ensure_not_reserved_role(&self, role_id: &RoleId) -> Result<(), Error> {
        if role_id.is_builtin() {
            let role = self.get_role(role_id);
            Err(Error::new(ErrorKind::ReservedRoleName(
                role.name().to_string(),
            )))
        } else {
            Ok(())
        }
    }

    pub fn ensure_grantable_role(&self, role_id: &RoleId) -> Result<(), Error> {
        let is_grantable = !role_id.is_public() && !role_id.is_system();
        if is_grantable {
            Ok(())
        } else {
            let role = self.get_role(role_id);
            Err(Error::new(ErrorKind::UngrantableRoleName(
                role.name().to_string(),
            )))
        }
    }

    pub fn ensure_not_system_role(&self, role_id: &RoleId) -> Result<(), Error> {
        if role_id.is_system() {
            let role = self.get_role(role_id);
            Err(Error::new(ErrorKind::ReservedSystemRoleName(
                role.name().to_string(),
            )))
        } else {
            Ok(())
        }
    }

    pub fn ensure_not_predefined_role(&self, role_id: &RoleId) -> Result<(), Error> {
        if role_id.is_predefined() {
            let role = self.get_role(role_id);
            Err(Error::new(ErrorKind::ReservedSystemRoleName(
                role.name().to_string(),
            )))
        } else {
            Ok(())
        }
    }

    // TODO(mjibson): Is there a way to make this a closure to avoid explicitly
    // passing tx, session, and builtin_table_updates?
    pub(crate) fn add_to_audit_log(
        &self,
        oracle_write_ts: mz_repr::Timestamp,
        session: Option<&ConnMeta>,
        tx: &mut mz_catalog::durable::Transaction,
        builtin_table_updates: &mut Vec<BuiltinTableUpdate>,
        audit_events: &mut Vec<VersionedEvent>,
        event_type: EventType,
        object_type: ObjectType,
        details: EventDetails,
    ) -> Result<(), Error> {
        let user = session.map(|session| session.user().name.to_string());

        // unsafe_mock_audit_event_timestamp can only be set to Some when running in unsafe mode.

        let occurred_at = match self
            .system_configuration
            .unsafe_mock_audit_event_timestamp()
        {
            Some(ts) => ts.into(),
            _ => oracle_write_ts.into(),
        };
        let id = tx.allocate_audit_log_id()?;
        let event = VersionedEvent::new(id, event_type, object_type, details, user, occurred_at);
        builtin_table_updates.push(self.pack_audit_log_update(&event)?);
        audit_events.push(event.clone());
        tx.insert_audit_log_event(event);
        Ok(())
    }

    pub(super) fn add_to_storage_usage(
        &self,
        tx: &mut mz_catalog::durable::Transaction,
        builtin_table_updates: &mut Vec<BuiltinTableUpdate>,
        shard_id: Option<String>,
        size_bytes: u64,
        collection_timestamp: EpochMillis,
    ) -> Result<(), Error> {
        let id =
            tx.get_and_increment_id(mz_catalog::durable::STORAGE_USAGE_ID_ALLOC_KEY.to_string())?;

        let details = VersionedStorageUsage::new(id, shard_id, size_bytes, collection_timestamp);
        builtin_table_updates.push(self.pack_storage_usage_update(&details)?);
        tx.insert_storage_usage_event(details);
        Ok(())
    }

    pub(super) fn get_owner_id(&self, id: &ObjectId, conn_id: &ConnectionId) -> Option<RoleId> {
        match id {
            ObjectId::Cluster(id) => Some(self.get_cluster(*id).owner_id()),
            ObjectId::ClusterReplica((cluster_id, replica_id)) => Some(
                self.get_cluster_replica(*cluster_id, *replica_id)
                    .owner_id(),
            ),
            ObjectId::Database(id) => Some(self.get_database(id).owner_id()),
            ObjectId::Schema((database_spec, schema_spec)) => Some(
                self.get_schema(database_spec, schema_spec, conn_id)
                    .owner_id(),
            ),
            ObjectId::Item(id) => Some(*self.get_entry(id).owner_id()),
            ObjectId::Role(_) => None,
        }
    }

    pub(super) fn get_object_type(&self, object_id: &ObjectId) -> mz_sql::catalog::ObjectType {
        match object_id {
            ObjectId::Cluster(_) => mz_sql::catalog::ObjectType::Cluster,
            ObjectId::ClusterReplica(_) => mz_sql::catalog::ObjectType::ClusterReplica,
            ObjectId::Database(_) => mz_sql::catalog::ObjectType::Database,
            ObjectId::Schema(_) => mz_sql::catalog::ObjectType::Schema,
            ObjectId::Role(_) => mz_sql::catalog::ObjectType::Role,
            ObjectId::Item(id) => self.get_entry(id).item_type().into(),
        }
    }

    pub(super) fn get_system_object_type(
        &self,
        id: &SystemObjectId,
    ) -> mz_sql::catalog::SystemObjectType {
        match id {
            SystemObjectId::Object(object_id) => {
                SystemObjectType::Object(self.get_object_type(object_id))
            }
            SystemObjectId::System => SystemObjectType::System,
        }
    }

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
    pub(crate) fn apply_updates_for_bootstrap(&mut self, updates: Vec<StateUpdate>) {
        for StateUpdate { kind, diff } in updates {
            assert_eq!(
                diff, 1,
                "initial catalog updates should be consolidated: ({kind:?}, {diff:?})"
            );
            self.apply_update(kind, diff);
        }
    }

    #[instrument(level = "debug")]
    fn apply_update(&mut self, kind: StateUpdateKind, diff: Diff) {
        fn apply<K, V>(map: &mut BTreeMap<K, V>, key: K, value: impl FnOnce() -> V, diff: Diff)
        where
            K: Ord + Debug,
            V: PartialEq + Eq + Debug,
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

        assert!(
            diff == 1 || diff == -1,
            "invalid update in catalog updates: ({kind:?}, {diff:?})"
        );
        match kind {
            StateUpdateKind::Role(role) => {
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
            StateUpdateKind::Database(database) => {
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
            StateUpdateKind::Schema(schema) => {
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
            StateUpdateKind::DefaultPrivilege(default_privilege) => match diff {
                1 => self
                    .default_privileges
                    .grant(default_privilege.object, default_privilege.acl_item),
                -1 => self
                    .default_privileges
                    .revoke(&default_privilege.object, &default_privilege.acl_item),
                _ => unreachable!("invalid diff: {diff}"),
            },
            StateUpdateKind::SystemPrivilege(system_privilege) => match diff {
                1 => self.system_privileges.grant(system_privilege),
                -1 => self.system_privileges.revoke(&system_privilege),
                _ => unreachable!("invalid diff: {diff}"),
            },
            StateUpdateKind::SystemConfiguration(system_configuration) => {
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
            StateUpdateKind::Comment(comment) => match diff {
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
                    let prev = self.comments.update_comment(
                        comment.object_id,
                        comment.sub_component,
                        None,
                    );
                    assert_eq!(
                        prev,
                        Some(comment.comment),
                        "retraction does not match existing value: ({:?}, {:?})",
                        comment.object_id,
                        comment.sub_component,
                    );
                }
                _ => unreachable!("invalid diff: {diff}"),
            },
        }
    }

    /// Synchronizes the local view of the [`StorageMetadata`] with the
    /// [`Transaction`]'s.
    ///
    /// This must be called after any `Transaction` is given to the storage
    /// controller, otherwise subsequent storage operations will have
    /// inconsistent metadata.
    pub(super) fn update_storage_metadata(&mut self, tx: &Transaction<'_>) {
        use mz_storage_client::controller::StorageTxn;
        self.storage_metadata.collection_metadata = tx.get_collection_metadata();
        // If we will not perform shard finalization in storage, there's no
        // benefit in exerting memory pressure on `envd` to cache all of these
        // unused values.
        if self
            .system_configuration
            .enable_storage_shard_finalization()
        {
            self.storage_metadata.unfinalized_shards = tx.get_unfinalized_shards();
        }
    }

    /// Returns a read-only view of the current [`StorageMetadata`].
    ///
    /// To write to this struct, you must use a [`Transaction`], followed by a
    /// call to `update_storage_metadata`.
    pub fn storage_metadata(&self) -> &StorageMetadata {
        &self.storage_metadata
    }
}

impl ConnectionResolver for CatalogState {
    fn resolve_connection(
        &self,
        id: GlobalId,
    ) -> mz_storage_types::connections::Connection<InlinedConnection> {
        use mz_storage_types::connections::Connection::*;
        match self
            .get_entry(&id)
            .connection()
            .expect("catalog out of sync")
            .connection
            .clone()
        {
            Kafka(conn) => Kafka(conn.into_inline_connection(self)),
            Postgres(conn) => Postgres(conn.into_inline_connection(self)),
            Csr(conn) => Csr(conn.into_inline_connection(self)),
            Ssh(conn) => Ssh(conn),
            Aws(conn) => Aws(conn),
            AwsPrivatelink(conn) => AwsPrivatelink(conn),
            MySql(conn) => MySql(conn.into_inline_connection(self)),
        }
    }
}

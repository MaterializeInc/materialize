// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Persistent metadata storage for the coordinator.

use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::net::Ipv4Addr;
use std::ops::BitOr;
use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::bail;
use chrono::{DateTime, Utc};
use futures::Future;
use itertools::Itertools;
use once_cell::sync::Lazy;
use regex::Regex;
use serde::{Deserialize, Serialize};
use tokio::sync::{Mutex, MutexGuard};
use tracing::{info, trace, warn};
use uuid::Uuid;

use mz_audit_log::{
    EventDetails, EventType, FullNameV1, IdFullNameV1, ObjectType, VersionedEvent,
    VersionedStorageUsage,
};
use mz_build_info::DUMMY_BUILD_INFO;
use mz_compute_client::controller::ComputeReplicaConfig;
use mz_compute_client::logging::LogVariant;
use mz_compute_client::protocol::command::ComputeParameters;
use mz_controller::clusters::ClusterRole;
use mz_controller::clusters::{
    ClusterEvent, ClusterId, ClusterStatus, ManagedReplicaLocation, ProcessId, ReplicaAllocation,
    ReplicaConfig, ReplicaId, ReplicaLocation, ReplicaLogging, UnmanagedReplicaLocation,
};
use mz_expr::{MirScalarExpr, OptimizedMirRelationExpr};
use mz_ore::cast::CastFrom;
use mz_ore::collections::CollectionExt;
use mz_ore::metrics::MetricsRegistry;
use mz_ore::now::{to_datetime, EpochMillis, NowFn};
use mz_ore::soft_assert;
use mz_persist_client::cfg::{PersistParameters, RetryParameters};
use mz_pgrepr::oid::FIRST_USER_OID;
use mz_repr::adt::mz_acl_item::{AclMode, MzAclItem};
use mz_repr::role_id::RoleId;
use mz_repr::{explain::ExprHumanizer, Diff, GlobalId, RelationDesc, ScalarType};
use mz_secrets::InMemorySecretsController;
use mz_sql::ast::display::AstDisplay;
use mz_sql::ast::Expr;
use mz_sql::catalog::{
    CatalogCluster, CatalogClusterReplica, CatalogDatabase, CatalogError as SqlCatalogError,
    CatalogItem as SqlCatalogItem, CatalogItemType as SqlCatalogItemType, CatalogItemType,
    CatalogRole, CatalogSchema, CatalogType, CatalogTypeDetails, EnvironmentId, IdReference,
    NameReference, RoleAttributes, SessionCatalog, TypeReference,
};
use mz_sql::func::OP_IMPLS;
use mz_sql::names::{
    Aug, DatabaseId, FullItemName, FullSchemaName, ItemQualifiers, ObjectId, PartialItemName,
    QualifiedItemName, QualifiedSchemaName, RawDatabaseSpecifier, ResolvedDatabaseSpecifier,
    SchemaId, SchemaSpecifier, PUBLIC_ROLE_NAME,
};
use mz_sql::plan::{
    CreateConnectionPlan, CreateIndexPlan, CreateMaterializedViewPlan, CreateSecretPlan,
    CreateSinkPlan, CreateSourcePlan, CreateTablePlan, CreateTypePlan, CreateViewPlan, Params,
    Plan, PlanContext, SourceSinkClusterConfig as PlanStorageClusterConfig, StatementDesc,
};
use mz_sql::session::user::{INTROSPECTION_USER, SYSTEM_USER};
use mz_sql::session::vars::{
    OwnedVarInput, SystemVars, Var, VarError, VarInput, CONFIG_HAS_SYNCED_ONCE,
};
use mz_sql::{plan, DEFAULT_SCHEMA};
use mz_sql_parser::ast::{CreateSinkOption, CreateSourceOption, Statement, WithOptionValue};
use mz_ssh_util::keys::SshKeyPairSet;
use mz_stash::{Stash, StashFactory};
use mz_storage_client::controller::IntrospectionType;
use mz_storage_client::types::parameters::StorageParameters;
use mz_storage_client::types::sinks::{
    SinkEnvelope, StorageSinkConnection, StorageSinkConnectionBuilder,
};
use mz_storage_client::types::sources::{SourceConnection, SourceDesc, SourceEnvelope, Timeline};
use mz_transform::Optimizer;

use crate::catalog::builtin::{
    Builtin, BuiltinLog, BuiltinRole, BuiltinTable, BuiltinType, Fingerprint, BUILTINS,
    BUILTIN_PREFIXES, INFORMATION_SCHEMA, MZ_CATALOG_SCHEMA, MZ_INTERNAL_SCHEMA, MZ_TEMP_SCHEMA,
    PG_CATALOG_SCHEMA,
};
pub use crate::catalog::builtin_table_updates::BuiltinTableUpdate;
pub use crate::catalog::config::{AwsPrincipalContext, ClusterReplicaSizeMap, Config};
pub use crate::catalog::error::{AmbiguousRename, Error, ErrorKind};
use crate::catalog::storage::{BootstrapArgs, Transaction, MZ_SYSTEM_ROLE_ID};
use crate::client::ConnectionId;
use crate::config::{SynchronizedParameters, SystemParameterFrontend};
use crate::coord::DEFAULT_LOGICAL_COMPACTION_WINDOW;
use crate::session::{PreparedStatement, Session, DEFAULT_DATABASE_NAME};
use crate::util::{index_sql, ResultExt};
use crate::{rbac, AdapterError, DUMMY_AVAILABILITY_ZONE};

use self::builtin::{BuiltinCluster, BuiltinSource};

mod builtin_table_updates;
mod config;
mod error;
mod migrate;

pub mod builtin;
pub mod storage;

pub const SYSTEM_CONN_ID: ConnectionId = 0;

const CREATE_SQL_TODO: &str = "TODO";

pub const DEFAULT_CLUSTER_REPLICA_NAME: &str = "r1";
pub const LINKED_CLUSTER_REPLICA_NAME: &str = "linked";

/// A `Catalog` keeps track of the SQL objects known to the planner.
///
/// For each object, it keeps track of both forward and reverse dependencies:
/// i.e., which objects are depended upon by the object, and which objects
/// depend upon the object. It enforces the SQL rules around dropping: an object
/// cannot be dropped until all of the objects that depend upon it are dropped.
/// It also enforces uniqueness of names.
///
/// SQL mandates a hierarchy of exactly three layers. A catalog contains
/// databases, databases contain schemas, and schemas contain catalog items,
/// like sources, sinks, view, and indexes.
///
/// To the outside world, databases, schemas, and items are all identified by
/// name. Items can be referred to by their [`FullItemName`], which fully and
/// unambiguously specifies the item, or a [`PartialItemName`], which can omit the
/// database name and/or the schema name. Partial names can be converted into
/// full names via a complicated resolution process documented by the
/// [`CatalogState::resolve`] method.
///
/// The catalog also maintains special "ambient schemas": virtual schemas,
/// implicitly present in all databases, that house various system views.
/// The big examples of ambient schemas are `pg_catalog` and `mz_catalog`.
#[derive(Debug)]
pub struct Catalog {
    state: CatalogState,
    storage: Arc<Mutex<storage::Connection>>,
    transient_revision: u64,
}

// Implement our own Clone because derive can't unless S is Clone, which it's
// not (hence the Arc).
impl Clone for Catalog {
    fn clone(&self) -> Self {
        Self {
            state: self.state.clone(),
            storage: Arc::clone(&self.storage),
            transient_revision: self.transient_revision,
        }
    }
}

#[derive(Debug, Clone)]
pub struct CatalogState {
    database_by_name: BTreeMap<String, DatabaseId>,
    database_by_id: BTreeMap<DatabaseId, Database>,
    entry_by_id: BTreeMap<GlobalId, CatalogEntry>,
    ambient_schemas_by_name: BTreeMap<String, SchemaId>,
    ambient_schemas_by_id: BTreeMap<SchemaId, Schema>,
    temporary_schemas: BTreeMap<ConnectionId, Schema>,
    clusters_by_id: BTreeMap<ClusterId, Cluster>,
    clusters_by_name: BTreeMap<String, ClusterId>,
    clusters_by_linked_object_id: BTreeMap<GlobalId, ClusterId>,
    roles_by_name: BTreeMap<String, RoleId>,
    roles_by_id: BTreeMap<RoleId, Role>,
    config: mz_sql::catalog::CatalogConfig,
    oid_counter: u32,
    cluster_replica_sizes: ClusterReplicaSizeMap,
    default_storage_cluster_size: Option<String>,
    availability_zones: Vec<String>,
    system_configuration: SystemVars,
    egress_ips: Vec<Ipv4Addr>,
    aws_principal_context: Option<AwsPrincipalContext>,
    aws_privatelink_availability_zones: Option<BTreeSet<String>>,
}

impl CatalogState {
    pub fn allocate_oid(&mut self) -> Result<u32, Error> {
        let oid = self.oid_counter;
        if oid == u32::max_value() {
            return Err(Error::new(ErrorKind::OidExhaustion));
        }
        self.oid_counter += 1;
        Ok(oid)
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
                for id in item.uses() {
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
    fn object_dependents(
        &self,
        object_ids: Vec<ObjectId>,
        seen: &mut BTreeSet<ObjectId>,
    ) -> Vec<ObjectId> {
        let mut dependents = Vec::new();
        for object_id in object_ids {
            match object_id {
                ObjectId::Cluster(id) => {
                    dependents.extend_from_slice(&self.cluster_dependents(id, seen));
                }
                ObjectId::ClusterReplica((cluster_id, replica_id)) => dependents.extend_from_slice(
                    &self.cluster_replica_dependents(cluster_id, replica_id, seen),
                ),
                ObjectId::Database(id) => {
                    dependents.extend_from_slice(&self.database_dependents(id, seen))
                }
                ObjectId::Schema((database_id, schema_id)) => {
                    dependents.extend_from_slice(&self.schema_dependents(
                        database_id,
                        schema_id,
                        seen,
                    ));
                }
                id @ ObjectId::Role(_) => {
                    seen.insert(id.clone());
                    dependents.push(id);
                }
                ObjectId::Item(id) => dependents.extend_from_slice(&self.item_dependents(id, seen)),
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
            for replica_id in cluster.replicas().values() {
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
    fn cluster_replica_dependents(
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
        seen: &mut BTreeSet<ObjectId>,
    ) -> Vec<ObjectId> {
        let mut dependents = Vec::new();
        let object_id = ObjectId::Database(database_id);
        if !seen.contains(&object_id) {
            seen.insert(object_id.clone());
            let database = self.get_database(&database_id);
            for schema_id in database.schemas().values() {
                dependents.extend_from_slice(&self.schema_dependents(
                    database_id,
                    *schema_id,
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
        database_id: DatabaseId,
        schema_id: SchemaId,
        seen: &mut BTreeSet<ObjectId>,
    ) -> Vec<ObjectId> {
        let mut dependents = Vec::new();
        let object_id = ObjectId::Schema((database_id, schema_id));
        if !seen.contains(&object_id) {
            seen.insert(object_id.clone());
            let schema = self
                .get_database(&database_id)
                .schemas_by_id
                .get(&schema_id)
                .expect("catalog out of sync");
            for item_id in schema.items().values() {
                dependents.extend_from_slice(&self.item_dependents(*item_id, seen));
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
    fn item_dependents(&self, item_id: GlobalId, seen: &mut BTreeSet<ObjectId>) -> Vec<ObjectId> {
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
            if let Some(linked_cluster_id) = self.clusters_by_linked_object_id.get(&item_id) {
                dependents.extend_from_slice(&self.cluster_dependents(*linked_cluster_id, seen));
            }
        }
        dependents
    }

    pub fn uses_tables(&self, id: GlobalId) -> bool {
        match self.get_entry(&id).item() {
            CatalogItem::Table(_) => true,
            item @ (CatalogItem::View(_) | CatalogItem::MaterializedView(_)) => {
                item.uses().iter().any(|id| self.uses_tables(*id))
            }
            CatalogItem::Index(idx) => self.uses_tables(idx.on),
            CatalogItem::Source(_)
            | CatalogItem::Log(_)
            | CatalogItem::Func(_)
            | CatalogItem::Sink(_)
            | CatalogItem::Type(_)
            | CatalogItem::Secret(_)
            | CatalogItem::Connection(_) => false,
        }
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

    fn ensure_no_unstable_uses(&self, item: &CatalogItem) -> Result<(), AdapterError> {
        let unstable_dependencies: Vec<_> = item
            .uses()
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
            Err(AdapterError::UnstableDependency {
                object_type,
                unstable_dependencies,
            })
        }
    }

    pub fn resolve_full_name(
        &self,
        name: &QualifiedItemName,
        conn_id: Option<ConnectionId>,
    ) -> FullItemName {
        let conn_id = conn_id.unwrap_or(SYSTEM_CONN_ID);

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

    pub fn get_entry(&self, id: &GlobalId) -> &CatalogEntry {
        &self.entry_by_id[id]
    }

    pub fn get_entry_mut(&mut self, id: &GlobalId) -> &mut CatalogEntry {
        self.entry_by_id.get_mut(id).expect("catalog out of sync")
    }

    pub fn try_get_entry_in_schema(
        &self,
        name: &QualifiedItemName,
        conn_id: ConnectionId,
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

    /// Gets an entry named `item` from exactly one of system schemas.
    ///
    /// # Panics
    /// - If `item` is not an entry in any system schema
    /// - If more than one system schema has an entry named `item`.
    fn get_entry_in_system_schemas(&self, item: &str) -> &CatalogEntry {
        let mut res = None;
        for system_schema in &[
            PG_CATALOG_SCHEMA,
            INFORMATION_SCHEMA,
            MZ_CATALOG_SCHEMA,
            MZ_INTERNAL_SCHEMA,
        ] {
            let schema_id = &self.ambient_schemas_by_name[*system_schema];
            let schema = &self.ambient_schemas_by_id[schema_id];
            if let Some(global_id) = schema.items.get(item) {
                match res {
                    None => res = Some(self.get_entry(global_id)),
                    Some(_) => panic!("only call get_entry_in_system_schemas on objects uniquely identifiable in one system schema"),
                }
            }
        }

        res.unwrap_or_else(|| panic!("cannot find {} in system schema", item))
    }

    pub fn item_exists(&self, name: &QualifiedItemName, conn_id: ConnectionId) -> bool {
        self.try_get_entry_in_schema(name, conn_id).is_some()
    }

    fn find_available_name(
        &self,
        mut name: QualifiedItemName,
        conn_id: ConnectionId,
    ) -> QualifiedItemName {
        let mut i = 0;
        let orig_item_name = name.item.clone();
        while self.item_exists(&name, conn_id) {
            i += 1;
            name.item = format!("{}{}", orig_item_name, i);
        }
        name
    }

    pub fn try_get_entry(&self, id: &GlobalId) -> Option<&CatalogEntry> {
        self.entry_by_id.get(id)
    }

    fn get_cluster(&self, cluster_id: ClusterId) -> &Cluster {
        self.try_get_cluster(cluster_id)
            .unwrap_or_else(|| panic!("unknown cluster {cluster_id}"))
    }

    fn get_cluster_mut(&mut self, cluster_id: ClusterId) -> &mut Cluster {
        self.try_get_cluster_mut(cluster_id)
            .unwrap_or_else(|| panic!("unknown cluster {cluster_id}"))
    }

    fn try_get_cluster(&self, cluster_id: ClusterId) -> Option<&Cluster> {
        self.clusters_by_id.get(&cluster_id)
    }

    fn try_get_cluster_mut(&mut self, cluster_id: ClusterId) -> Option<&mut Cluster> {
        self.clusters_by_id.get_mut(&cluster_id)
    }

    fn get_linked_cluster(&self, object_id: GlobalId) -> Option<&Cluster> {
        self.clusters_by_linked_object_id
            .get(&object_id)
            .map(|id| &self.clusters_by_id[id])
    }

    fn get_storage_object_size(&self, object_id: GlobalId) -> Option<&str> {
        let cluster = self.get_linked_cluster(object_id)?;
        let replica_id = cluster.replica_id_by_name[LINKED_CLUSTER_REPLICA_NAME];
        let replica = &cluster.replicas_by_id[&replica_id];
        match &replica.config.location {
            ReplicaLocation::Unmanaged(_) => None,
            ReplicaLocation::Managed(ManagedReplicaLocation { size, .. }) => Some(size),
        }
    }

    fn get_role(&self, id: &RoleId) -> &Role {
        self.roles_by_id.get(id).expect("catalog out of sync")
    }

    fn try_get_role_by_name(&self, role_name: &str) -> Option<&Role> {
        self.roles_by_name
            .get(role_name)
            .map(|id| &self.roles_by_id[id])
    }

    fn get_role_mut(&mut self, id: &RoleId) -> &mut Role {
        self.roles_by_id.get_mut(id).expect("catalog out of sync")
    }

    fn collect_role_membership(&self, id: &RoleId) -> BTreeSet<RoleId> {
        let mut membership = BTreeSet::new();
        let mut queue = VecDeque::from(vec![id]);
        while let Some(cur_id) = queue.pop_front() {
            if !membership.contains(cur_id) {
                membership.insert(cur_id.clone());
                let role = self.get_role(cur_id);
                soft_assert!(
                    !role.membership().contains(id),
                    "circular membership exists in the catalog"
                );
                queue.extend(role.membership().into_iter());
            }
        }
        membership
    }

    /// Parse a SQL string into a catalog view item with only a limited
    /// context.
    #[tracing::instrument(level = "info", skip_all)]
    pub fn parse_view_item(&self, create_sql: String) -> Result<CatalogItem, anyhow::Error> {
        let mut session_catalog = ConnCatalog {
            state: Cow::Borrowed(self),
            conn_id: SYSTEM_CONN_ID,
            cluster: "default".into(),
            database: self
                .resolve_database(DEFAULT_DATABASE_NAME)
                .ok()
                .map(|db| db.id()),
            search_path: Vec::new(),
            role_id: MZ_SYSTEM_ROLE_ID,
            prepared_statements: None,
        };
        enable_features_required_for_catalog_open(&mut session_catalog);

        let stmt = mz_sql::parse::parse(&create_sql)?.into_element();
        let (stmt, depends_on) = mz_sql::names::resolve(&session_catalog, stmt)?;
        let depends_on = depends_on.into_iter().collect();
        let plan = mz_sql::plan::plan(None, &session_catalog, stmt, &Params::empty())?;
        Ok(match plan {
            Plan::CreateView(CreateViewPlan { view, .. }) => {
                let optimizer = Optimizer::logical_optimizer();
                let optimized_expr = optimizer.optimize(view.expr)?;
                let desc = RelationDesc::new(optimized_expr.typ(), view.column_names);
                CatalogItem::View(View {
                    create_sql: view.create_sql,
                    optimized_expr,
                    desc,
                    conn_id: None,
                    depends_on,
                })
            }
            _ => bail!("Expected valid CREATE VIEW statement"),
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
    fn insert_item(
        &mut self,
        id: GlobalId,
        oid: u32,
        name: QualifiedItemName,
        item: CatalogItem,
        owner_id: RoleId,
        privileges: Vec<MzAclItem>,
    ) {
        if !id.is_system() && !item.is_placeholder() {
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
            owner_id,
            privileges,
        };
        for u in entry.uses() {
            match self.entry_by_id.get_mut(u) {
                Some(metadata) => metadata.used_by.push(entry.id),
                None => panic!(
                    "Catalog: missing dependent catalog item {} while installing {}",
                    &u,
                    self.resolve_full_name(&entry.name, entry.conn_id())
                ),
            }
        }
        let conn_id = entry.item().conn_id().unwrap_or(SYSTEM_CONN_ID);
        let schema = self.get_schema_mut(
            &entry.name().qualifiers.database_spec,
            &entry.name().qualifiers.schema_spec,
            conn_id,
        );

        let prev_id = if let CatalogItem::Func(_) = entry.item() {
            schema.functions.insert(entry.name.item.clone(), entry.id)
        } else {
            schema.items.insert(entry.name.item.clone(), entry.id)
        };

        assert!(
            prev_id.is_none(),
            "builtin name collision on {:?}",
            entry.name.item.clone()
        );

        self.entry_by_id.insert(entry.id, entry.clone());
    }

    #[tracing::instrument(level = "trace", skip(self))]
    fn drop_item(&mut self, id: GlobalId) {
        let metadata = self.entry_by_id.remove(&id).expect("catalog out of sync");
        if !metadata.item.is_placeholder() {
            info!(
                "drop {} {} ({})",
                metadata.item_type(),
                self.resolve_full_name(&metadata.name, metadata.conn_id()),
                id
            );
        }
        for u in metadata.uses() {
            if let Some(dep_metadata) = self.entry_by_id.get_mut(u) {
                dep_metadata.used_by.retain(|u| *u != metadata.id)
            }
        }

        let conn_id = metadata.item.conn_id().unwrap_or(SYSTEM_CONN_ID);
        let schema = self.get_schema_mut(
            &metadata.name().qualifiers.database_spec,
            &metadata.name().qualifiers.schema_spec,
            conn_id,
        );
        schema
            .items
            .remove(&metadata.name().item)
            .expect("catalog out of sync");

        if !id.is_system() {
            if let Some(cluster_id) = metadata.item.cluster_id() {
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

    fn get_database(&self, database_id: &DatabaseId) -> &Database {
        &self.database_by_id[database_id]
    }

    fn get_database_mut(&mut self, database_id: &DatabaseId) -> &mut Database {
        self.database_by_id
            .get_mut(database_id)
            .expect("catalog out of sync")
    }

    fn insert_cluster(
        &mut self,
        id: ClusterId,
        name: String,
        linked_object_id: Option<GlobalId>,
        introspection_source_indexes: Vec<(&'static BuiltinLog, GlobalId)>,
        owner_id: RoleId,
        privileges: Vec<MzAclItem>,
    ) {
        let mut log_indexes = BTreeMap::new();
        for (log, index_id) in introspection_source_indexes {
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
            index_name = self.find_available_name(index_name, SYSTEM_CONN_ID);
            let index_item_name = index_name.item.clone();
            // TODO(clusters): Avoid panicking here on ID exhaustion
            // before stabilization.
            //
            // The OID counter is an i32, and could plausibly be exhausted.
            // Preallocating OIDs for each logging index is eminently
            // doable, but annoying enough that we don't bother now.
            let oid = self
                .allocate_oid()
                .unwrap_or_terminate("cannot return error here");
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
                    depends_on: vec![log_id],
                    cluster_id: id,
                    is_retained_metrics_object: false,
                    custom_logical_compaction_window: None,
                }),
                MZ_SYSTEM_ROLE_ID,
                Vec::new(),
            );
            log_indexes.insert(log.variant.clone(), index_id);
        }

        self.clusters_by_id.insert(
            id,
            Cluster {
                name: name.clone(),
                id,
                linked_object_id,
                bound_objects: BTreeSet::new(),
                log_indexes,
                replica_id_by_name: BTreeMap::new(),
                replicas_by_id: BTreeMap::new(),
                owner_id,
                privileges,
            },
        );
        assert!(self.clusters_by_name.insert(name, id).is_none());
        if let Some(linked_object_id) = linked_object_id {
            assert!(self
                .clusters_by_linked_object_id
                .insert(linked_object_id, id)
                .is_none());
        }
    }

    fn insert_cluster_replica(
        &mut self,
        cluster_id: ClusterId,
        replica_name: String,
        replica_id: ReplicaId,
        config: ReplicaConfig,
        owner_id: RoleId,
    ) {
        let replica = ClusterReplica {
            name: replica_name.clone(),
            process_status: (0..config.location.num_processes())
                .map(|process_id| {
                    let status = ClusterReplicaProcessStatus {
                        status: ClusterStatus::NotReady,
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
        assert!(cluster
            .replica_id_by_name
            .insert(replica_name, replica_id)
            .is_none());
        assert!(cluster.replicas_by_id.insert(replica_id, replica).is_none());
    }

    /// Inserts or updates the status of the specified cluster replica process.
    ///
    /// Panics if the cluster or replica does not exist.
    fn ensure_cluster_status(
        &mut self,
        cluster_id: ClusterId,
        replica_id: ReplicaId,
        process_id: ProcessId,
        status: ClusterReplicaProcessStatus,
    ) {
        let replica = self
            .try_get_cluster_replica_mut(cluster_id, replica_id)
            .unwrap_or_else(|| panic!("unknown cluster replica: {cluster_id}.{replica_id}"));
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
        self.clusters_by_id
            .get(&id)
            .and_then(|cluster| cluster.replicas_by_id.get(&replica_id))
    }

    /// Gets a reference to the specified replica of the specified cluster.
    ///
    /// Panics if either the cluster or the replica does not exist.
    fn get_cluster_replica(&self, cluster_id: ClusterId, replica_id: ReplicaId) -> &ClusterReplica {
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
        self.clusters_by_id
            .get_mut(&id)
            .and_then(|cluster| cluster.replicas_by_id.get_mut(&replica_id))
    }

    /// Gets the status of the given cluster replica process.
    ///
    /// Panics if the cluster or replica does not exist
    fn get_cluster_status(
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
    pub fn get_system_configuration(&self, name: &str) -> Result<&dyn Var, AdapterError> {
        Ok(self.system_configuration.get(name)?)
    }

    /// Insert system configuration `name` with `value`.
    ///
    /// Return a `bool` value indicating whether the configuration was modified
    /// by the call.
    fn insert_system_configuration(
        &mut self,
        name: &str,
        value: VarInput,
    ) -> Result<bool, AdapterError> {
        Ok(self.system_configuration.set(name, value)?)
    }

    /// Reset system configuration `name`.
    ///
    /// Return a `bool` value indicating whether the configuration was modified
    /// by the call.
    fn remove_system_configuration(&mut self, name: &str) -> Result<bool, AdapterError> {
        Ok(self.system_configuration.reset(name)?)
    }

    /// Remove all system configurations.
    fn clear_system_configuration(&mut self) {
        self.system_configuration = SystemVars::default();
    }

    /// Gets the schema map for the database matching `database_spec`.
    fn resolve_schema_in_database(
        &self,
        database_spec: &ResolvedDatabaseSpecifier,
        schema_name: &str,
        conn_id: ConnectionId,
    ) -> Result<&Schema, SqlCatalogError> {
        let schema = match database_spec {
            ResolvedDatabaseSpecifier::Ambient if schema_name == MZ_TEMP_SCHEMA => {
                self.temporary_schemas.get(&conn_id)
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
        conn_id: ConnectionId,
    ) -> &Schema {
        // Keep in sync with `get_schemas_mut`
        match (database_spec, schema_spec) {
            (ResolvedDatabaseSpecifier::Ambient, SchemaSpecifier::Temporary) => {
                &self.temporary_schemas[&conn_id]
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

    fn get_schema_mut(
        &mut self,
        database_spec: &ResolvedDatabaseSpecifier,
        schema_spec: &SchemaSpecifier,
        conn_id: ConnectionId,
    ) -> &mut Schema {
        // Keep in sync with `get_schemas`
        match (database_spec, schema_spec) {
            (ResolvedDatabaseSpecifier::Ambient, SchemaSpecifier::Temporary) => self
                .temporary_schemas
                .get_mut(&conn_id)
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

    pub fn is_system_schema(&self, schema: &str) -> bool {
        schema == MZ_CATALOG_SCHEMA
            || schema == PG_CATALOG_SCHEMA
            || schema == INFORMATION_SCHEMA
            || schema == MZ_INTERNAL_SCHEMA
    }

    pub fn is_system_schema_id(&self, id: &SchemaId) -> bool {
        id == self.get_mz_catalog_schema_id()
            || id == self.get_pg_catalog_schema_id()
            || id == self.get_information_schema_id()
            || id == self.get_mz_internal_schema_id()
    }

    pub fn is_system_schema_specifier(&self, spec: &SchemaSpecifier) -> bool {
        match spec {
            SchemaSpecifier::Temporary => false,
            SchemaSpecifier::Id(id) => self.is_system_schema_id(id),
        }
    }

    /// Optimized lookup for a builtin table
    ///
    /// Panics if the builtin table doesn't exist in the catalog
    pub fn resolve_builtin_table(&self, builtin: &'static BuiltinTable) -> GlobalId {
        self.resolve_builtin_object(&Builtin::<IdReference>::Table(builtin))
    }

    /// Optimized lookup for a builtin log
    ///
    /// Panics if the builtin log doesn't exist in the catalog
    pub fn resolve_builtin_log(&self, builtin: &'static BuiltinLog) -> GlobalId {
        self.resolve_builtin_object(&Builtin::<IdReference>::Log(builtin))
    }

    /// Optimized lookup for a builtin storage collection
    ///
    /// Panics if the builtin storage collection doesn't exist in the catalog
    pub fn resolve_builtin_source(&self, builtin: &'static BuiltinSource) -> GlobalId {
        self.resolve_builtin_object(&Builtin::<IdReference>::Source(builtin))
    }

    /// Optimized lookup for a builtin object
    ///
    /// Panics if the builtin object doesn't exist in the catalog
    pub fn resolve_builtin_object<T: TypeReference>(&self, builtin: &Builtin<T>) -> GlobalId {
        let schema_id = &self.ambient_schemas_by_name[builtin.schema()];
        let schema = &self.ambient_schemas_by_id[schema_id];
        schema.items[builtin.name()].clone()
    }

    pub fn config(&self) -> &mz_sql::catalog::CatalogConfig {
        &self.config
    }

    pub fn unsafe_mode(&self) -> bool {
        self.config.unsafe_mode
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
        conn_id: ConnectionId,
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
        conn_id: ConnectionId,
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
        Err(SqlCatalogError::UnknownItem(name.to_string()))
    }

    /// Resolves `name` to a non-function [`CatalogEntry`].
    pub fn resolve_entry(
        &self,
        current_database: Option<&DatabaseId>,
        search_path: &Vec<(ResolvedDatabaseSpecifier, SchemaSpecifier)>,
        name: &PartialItemName,
        conn_id: ConnectionId,
    ) -> Result<&CatalogEntry, SqlCatalogError> {
        self.resolve(
            |schema| &schema.items,
            current_database,
            search_path,
            name,
            conn_id,
        )
    }

    /// Resolves `name` to a function [`CatalogEntry`].
    pub fn resolve_function(
        &self,
        current_database: Option<&DatabaseId>,
        search_path: &Vec<(ResolvedDatabaseSpecifier, SchemaSpecifier)>,
        name: &PartialItemName,
        conn_id: ConnectionId,
    ) -> Result<&CatalogEntry, SqlCatalogError> {
        self.resolve(
            |schema| &schema.functions,
            current_database,
            search_path,
            name,
            conn_id,
        )
    }

    /// Return current system configuration.
    pub fn system_config(&self) -> &SystemVars {
        &self.system_configuration
    }

    pub fn require_unsafe_mode(&self, feature_name: &'static str) -> Result<(), AdapterError> {
        if !self.config.unsafe_mode {
            Err(AdapterError::Unsupported(feature_name))
        } else {
            Ok(())
        }
    }

    /// Serializes the catalog's in-memory state.
    ///
    /// There are no guarantees about the format of the serialized state, except
    /// that the serialized state for two identical catalogs will compare
    /// identically.
    pub fn dump(&self) -> String {
        serde_json::to_string(&self.database_by_id).expect("serialization cannot fail")
    }

    pub fn availability_zones(&self) -> &[String] {
        &self.availability_zones
    }

    /// Returns the default storage cluster size .
    ///
    /// If a default size was given as configuration, it is always used,
    /// otherwise the smallest size is used instead.
    pub fn default_linked_cluster_size(&self) -> String {
        match &self.default_storage_cluster_size {
            Some(default_storage_cluster_size) => default_storage_cluster_size.clone(),
            None => {
                let (size, _allocation) = self
                    .cluster_replica_sizes
                    .0
                    .iter()
                    .min_by_key(|(_, a)| (a.scale, a.workers, a.memory_limit))
                    .expect("should have at least one valid cluster replica size");
                size.clone()
            }
        }
    }

    pub fn ensure_not_reserved_role(&self, role_id: &RoleId) -> Result<(), Error> {
        if role_id.is_system() || role_id.is_public() {
            let role = self.get_role(role_id);
            Err(Error::new(ErrorKind::ReservedRoleName(
                role.name().to_string(),
            )))
        } else {
            Ok(())
        }
    }

    // TODO(mjibson): Is there a way to make this a closure to avoid explicitly
    // passing tx, session, and builtin_table_updates?
    fn add_to_audit_log(
        &self,
        oracle_write_ts: mz_repr::Timestamp,
        session: Option<&Session>,
        tx: &mut storage::Transaction,
        builtin_table_updates: &mut Vec<BuiltinTableUpdate>,
        audit_events: &mut Vec<VersionedEvent>,
        event_type: EventType,
        object_type: ObjectType,
        details: EventDetails,
    ) -> Result<(), Error> {
        let user = session.map(|session| session.user().name.to_string());
        let occurred_at = match (
            self.unsafe_mode(),
            self.system_configuration.mock_audit_event_timestamp(),
        ) {
            (true, Some(ts)) => ts.into(),
            _ => oracle_write_ts.into(),
        };
        let id = tx.get_and_increment_id(storage::AUDIT_LOG_ID_ALLOC_KEY.to_string())?;
        let event = VersionedEvent::new(id, event_type, object_type, details, user, occurred_at);
        builtin_table_updates.push(self.pack_audit_log_update(&event)?);
        audit_events.push(event.clone());
        tx.insert_audit_log_event(event);
        Ok(())
    }

    fn add_to_storage_usage(
        &self,
        tx: &mut storage::Transaction,
        builtin_table_updates: &mut Vec<BuiltinTableUpdate>,
        shard_id: Option<String>,
        size_bytes: u64,
        collection_timestamp: EpochMillis,
    ) -> Result<(), Error> {
        let id = tx.get_and_increment_id(storage::STORAGE_USAGE_ID_ALLOC_KEY.to_string())?;

        let details = VersionedStorageUsage::new(id, shard_id, size_bytes, collection_timestamp);
        builtin_table_updates.push(self.pack_storage_usage_update(&details)?);
        tx.insert_storage_usage_event(details);
        Ok(())
    }
}

#[derive(Debug)]
pub struct ConnCatalog<'a> {
    state: Cow<'a, CatalogState>,
    conn_id: ConnectionId,
    cluster: String,
    database: Option<DatabaseId>,
    search_path: Vec<(ResolvedDatabaseSpecifier, SchemaSpecifier)>,
    role_id: RoleId,
    prepared_statements: Option<Cow<'a, BTreeMap<String, PreparedStatement>>>,
}

impl ConnCatalog<'_> {
    pub fn conn_id(&self) -> ConnectionId {
        self.conn_id
    }

    pub fn state(&self) -> &CatalogState {
        &*self.state
    }

    pub fn into_owned(self) -> ConnCatalog<'static> {
        ConnCatalog {
            state: Cow::Owned(self.state.into_owned()),
            conn_id: self.conn_id,
            cluster: self.cluster,
            database: self.database,
            search_path: self.search_path,
            role_id: self.role_id,
            prepared_statements: self.prepared_statements.map(|s| Cow::Owned(s.into_owned())),
        }
    }

    /// Returns the schemas:
    /// - mz_catalog
    /// - pg_catalog
    /// - temp (if requested)
    /// - all schemas from the session's search_path var that exist
    pub fn effective_search_path(
        &self,
        include_temp_schema: bool,
    ) -> Vec<(ResolvedDatabaseSpecifier, SchemaSpecifier)> {
        let mut v = Vec::with_capacity(self.search_path.len() + 3);
        // Temp schema is only included for relations and data types, not for functions and operators
        let temp_schema = (
            ResolvedDatabaseSpecifier::Ambient,
            SchemaSpecifier::Temporary,
        );
        if include_temp_schema && !self.search_path.contains(&temp_schema) {
            v.push(temp_schema);
        }
        let default_schemas = [
            (
                ResolvedDatabaseSpecifier::Ambient,
                SchemaSpecifier::Id(self.state.get_mz_catalog_schema_id().clone()),
            ),
            (
                ResolvedDatabaseSpecifier::Ambient,
                SchemaSpecifier::Id(self.state.get_pg_catalog_schema_id().clone()),
            ),
        ];
        for schema in default_schemas.into_iter() {
            if !self.search_path.contains(&schema) {
                v.push(schema);
            }
        }
        v.extend_from_slice(&self.search_path);
        v
    }
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Database {
    pub name: String,
    pub id: DatabaseId,
    #[serde(skip)]
    pub oid: u32,
    pub schemas_by_id: BTreeMap<SchemaId, Schema>,
    pub schemas_by_name: BTreeMap<String, SchemaId>,
    pub owner_id: RoleId,
    pub privileges: Vec<MzAclItem>,
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Schema {
    pub name: QualifiedSchemaName,
    pub id: SchemaSpecifier,
    #[serde(skip)]
    pub oid: u32,
    pub items: BTreeMap<String, GlobalId>,
    pub functions: BTreeMap<String, GlobalId>,
    pub owner_id: RoleId,
    pub privileges: Vec<MzAclItem>,
}

#[derive(Debug, Serialize, Clone)]
pub struct Role {
    pub name: String,
    pub id: RoleId,
    #[serde(skip)]
    pub oid: u32,
    pub attributes: RoleAttributes,
    pub membership: RoleMembership,
}

impl Role {
    pub fn is_user(&self) -> bool {
        self.id.is_user()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq, Ord, PartialOrd)]
// These attributes are needed because the key of a map must be a string. We also
// get the added benefit of flattening this struct in it's serialized form.
#[serde(into = "BTreeMap<String, RoleId>")]
#[serde(try_from = "BTreeMap<String, RoleId>")]
pub struct RoleMembership {
    /// Key is the role that some role is a member of, value is the grantor role ID.
    pub map: BTreeMap<RoleId, RoleId>,
}

impl RoleMembership {
    fn new() -> RoleMembership {
        RoleMembership {
            map: BTreeMap::new(),
        }
    }
}

impl From<RoleMembership> for BTreeMap<String, RoleId> {
    fn from(value: RoleMembership) -> Self {
        value
            .map
            .into_iter()
            .map(|(k, v)| (k.to_string(), v))
            .collect()
    }
}

impl TryFrom<BTreeMap<String, RoleId>> for RoleMembership {
    type Error = anyhow::Error;

    fn try_from(value: BTreeMap<String, RoleId>) -> Result<Self, Self::Error> {
        Ok(RoleMembership {
            map: value
                .into_iter()
                .map(|(k, v)| Ok((RoleId::from_str(&k)?, v)))
                .collect::<Result<_, anyhow::Error>>()?,
        })
    }
}

#[derive(Debug, Serialize, Clone)]
pub struct Cluster {
    pub name: String,
    pub id: ClusterId,
    pub log_indexes: BTreeMap<LogVariant, GlobalId>,
    pub linked_object_id: Option<GlobalId>,
    /// Objects bound to this cluster. Does not include introspection source
    /// indexes.
    pub bound_objects: BTreeSet<GlobalId>,
    pub replica_id_by_name: BTreeMap<String, ReplicaId>,
    pub replicas_by_id: BTreeMap<ReplicaId, ClusterReplica>,
    pub owner_id: RoleId,
    pub privileges: Vec<MzAclItem>,
}

impl Cluster {
    /// The role of the cluster. Currently used to set alert severity.
    pub fn role(&self) -> ClusterRole {
        // NOTE - These roles power monitoring systems. Do not change
        // them without talking to the cloud or observability groups.
        if self.name == SYSTEM_USER.name {
            ClusterRole::SystemCritical
        } else if self.name == INTROSPECTION_USER.name {
            ClusterRole::System
        } else {
            ClusterRole::User
        }
    }
}

#[derive(Debug, Serialize, Clone)]
pub struct ClusterReplica {
    pub name: String,
    pub config: ReplicaConfig,
    pub process_status: BTreeMap<ProcessId, ClusterReplicaProcessStatus>,
    pub owner_id: RoleId,
}

impl ClusterReplica {
    /// Computes the status of the cluster replica as a whole.
    pub fn status(&self) -> ClusterStatus {
        self.process_status
            .values()
            .fold(ClusterStatus::Ready, |s, p| match (s, p.status) {
                (ClusterStatus::Ready, ClusterStatus::Ready) => ClusterStatus::Ready,
                _ => ClusterStatus::NotReady,
            })
    }
}

#[derive(Debug, Serialize, Clone)]
pub struct ClusterReplicaProcessStatus {
    pub status: ClusterStatus,
    pub time: DateTime<Utc>,
}

#[derive(Clone, Debug)]
pub struct CatalogEntry {
    item: CatalogItem,
    used_by: Vec<GlobalId>,
    id: GlobalId,
    oid: u32,
    name: QualifiedItemName,
    owner_id: RoleId,
    privileges: Vec<MzAclItem>,
}

#[derive(Debug, Clone, Serialize)]
pub enum CatalogItem {
    Table(Table),
    Source(Source),
    Log(Log),
    View(View),
    MaterializedView(MaterializedView),
    Sink(Sink),
    Index(Index),
    Type(Type),
    Func(Func),
    Secret(Secret),
    Connection(Connection),
}

#[derive(Debug, Clone, Serialize)]
pub struct Table {
    pub create_sql: String,
    pub desc: RelationDesc,
    #[serde(skip)]
    pub defaults: Vec<Expr<Aug>>,
    pub conn_id: Option<ConnectionId>,
    pub depends_on: Vec<GlobalId>,
    pub custom_logical_compaction_window: Option<Duration>,
    /// Whether the table's logical compaction window is controlled by
    /// METRICS_RETENTION
    pub is_retained_metrics_object: bool,
}

impl Table {
    // The Coordinator controls insertions for tables (including system tables),
    // so they are realtime.
    pub fn timeline(&self) -> Timeline {
        Timeline::EpochMilliseconds
    }
}

#[derive(Debug, Clone, Serialize)]
pub enum DataSourceDesc {
    /// Receives data from an external system
    Ingestion(Ingestion),
    /// Receives data from some other source
    Source,
    /// Receives introspection data from an internal system
    Introspection(IntrospectionType),
    /// Receives data from the source's reclocking/remapping operations.
    Progress,
}

#[derive(Debug, Clone, Serialize)]
pub struct Source {
    pub create_sql: String,
    pub data_source: DataSourceDesc,
    pub desc: RelationDesc,
    pub timeline: Timeline,
    pub depends_on: Vec<GlobalId>,
    pub custom_logical_compaction_window: Option<Duration>,
    /// Whether the source's logical compaction window is controlled by
    /// METRICS_RETENTION
    pub is_retained_metrics_object: bool,
}

impl Source {
    /// Returns whether this source ingests data from an external source.
    pub fn is_external(&self) -> bool {
        match self.data_source {
            DataSourceDesc::Ingestion(_) => true,
            DataSourceDesc::Introspection(_)
            | DataSourceDesc::Progress
            | DataSourceDesc::Source => false,
        }
    }

    /// Type of the source.
    pub fn source_type(&self) -> &str {
        match &self.data_source {
            DataSourceDesc::Ingestion(ingestion) => ingestion.desc.connection.name(),
            DataSourceDesc::Progress | DataSourceDesc::Source => "subsource",
            DataSourceDesc::Introspection(_) => "source",
        }
    }

    /// Envelope of the source.
    pub fn envelope(&self) -> Option<&str> {
        // Note how "none"/"append-only" is different from `None`. Source
        // sources don't have an envelope (internal logs, for example), while
        // other sources have an envelope that we call the "NONE"-envelope.

        match &self.data_source {
            // NOTE(aljoscha): We could move the block for ingestsions into
            // `SourceEnvelope` itself, but that one feels more like an internal
            // thing and adapter should own how we represent envelopes as a
            // string? It would not be hard to convince me otherwise, though.
            DataSourceDesc::Ingestion(ingestion) => match ingestion.desc.envelope() {
                SourceEnvelope::None(_) => Some("none"),
                SourceEnvelope::Debezium(_) => {
                    // NOTE(aljoscha): This is currently not used in production.
                    // DEBEZIUM sources transparently use `DEBEZIUM UPSERT`.
                    Some("debezium")
                }
                SourceEnvelope::Upsert(upsert_envelope) => match upsert_envelope.style {
                    mz_storage_client::types::sources::UpsertStyle::Default(_) => Some("upsert"),
                    mz_storage_client::types::sources::UpsertStyle::Debezium { .. } => {
                        // NOTE(aljoscha): Should we somehow mark that this is
                        // using upsert internally? See note above about
                        // DEBEZIUM.
                        Some("debezium")
                    }
                },
                SourceEnvelope::CdcV2 => {
                    // TODO(aljoscha): Should we even report this? It's
                    // currently not exposed.
                    Some("materialize")
                }
            },
            DataSourceDesc::Introspection(_)
            | DataSourceDesc::Progress
            | DataSourceDesc::Source => None,
        }
    }

    /// Connection ID of the source, if one exists.
    pub fn connection_id(&self) -> Option<GlobalId> {
        match &self.data_source {
            DataSourceDesc::Ingestion(ingestion) => ingestion.desc.connection.connection_id(),
            DataSourceDesc::Introspection(_)
            | DataSourceDesc::Progress
            | DataSourceDesc::Source => None,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct Log {
    pub variant: LogVariant,
    /// Whether the log is backed by a storage collection.
    pub has_storage_collection: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct Ingestion {
    // TODO(benesch): this field contains connection information that could be
    // derived from the connection ID. Too hard to fix at the moment.
    pub desc: SourceDesc,
    pub source_imports: BTreeSet<GlobalId>,
    /// The *additional* subsource exports of this ingestion. Each collection identified by its
    /// GlobalId will contain the contents of this ingestion's output stream that is identified by
    /// the index.
    ///
    /// This map does *not* include the export of the source associated with the ingestion itself
    pub subsource_exports: BTreeMap<GlobalId, usize>,
    pub cluster_id: ClusterId,
    /// The ID of this collection's remap/progress collection.
    // MIGRATION: v0.44 This can be converted to a `GlobalId` in v0.46
    pub remap_collection_id: Option<GlobalId>,
}

#[derive(Debug, Clone, Serialize)]
pub struct Sink {
    pub create_sql: String,
    pub from: GlobalId,
    // TODO(benesch): this field duplicates information that could be derived
    // from the connection ID. Too hard to fix at the moment.
    pub connection: StorageSinkConnectionState,
    pub envelope: SinkEnvelope,
    pub with_snapshot: bool,
    pub depends_on: Vec<GlobalId>,
    pub cluster_id: ClusterId,
}

impl Sink {
    pub fn sink_type(&self) -> &str {
        match &self.connection {
            StorageSinkConnectionState::Pending(pending) => pending.name(),
            StorageSinkConnectionState::Ready(ready) => ready.name(),
        }
    }

    /// Envelope of the sink.
    pub fn envelope(&self) -> Option<&str> {
        match &self.envelope {
            SinkEnvelope::Debezium => Some("debezium"),
            SinkEnvelope::Upsert => Some("upsert"),
        }
    }

    pub fn connection_id(&self) -> Option<GlobalId> {
        match &self.connection {
            StorageSinkConnectionState::Pending(pending) => pending.connection_id(),
            StorageSinkConnectionState::Ready(ready) => ready.connection_id(),
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub enum StorageSinkConnectionState {
    Pending(StorageSinkConnectionBuilder),
    Ready(StorageSinkConnection),
}

#[derive(Debug, Clone, Serialize)]
pub struct View {
    pub create_sql: String,
    pub optimized_expr: OptimizedMirRelationExpr,
    pub desc: RelationDesc,
    pub conn_id: Option<ConnectionId>,
    pub depends_on: Vec<GlobalId>,
}

#[derive(Debug, Clone, Serialize)]
pub struct MaterializedView {
    pub create_sql: String,
    pub optimized_expr: OptimizedMirRelationExpr,
    pub desc: RelationDesc,
    pub depends_on: Vec<GlobalId>,
    pub cluster_id: ClusterId,
}

#[derive(Debug, Clone, Serialize)]
pub struct Index {
    pub create_sql: String,
    pub on: GlobalId,
    pub keys: Vec<MirScalarExpr>,
    pub conn_id: Option<ConnectionId>,
    pub depends_on: Vec<GlobalId>,
    pub cluster_id: ClusterId,
    pub custom_logical_compaction_window: Option<Duration>,
    pub is_retained_metrics_object: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct Type {
    pub create_sql: String,
    #[serde(skip)]
    pub details: CatalogTypeDetails<IdReference>,
    pub depends_on: Vec<GlobalId>,
}

#[derive(Debug, Clone, Serialize)]
pub struct Func {
    #[serde(skip)]
    pub inner: &'static mz_sql::func::Func,
}

#[derive(Debug, Clone, Serialize)]
pub struct Secret {
    pub create_sql: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct Connection {
    pub create_sql: String,
    pub connection: mz_storage_client::types::connections::Connection,
    pub depends_on: Vec<GlobalId>,
}

pub struct TransactionResult<R> {
    pub builtin_table_updates: Vec<BuiltinTableUpdate>,
    pub audit_events: Vec<VersionedEvent>,
    pub result: R,
}

impl CatalogItem {
    /// Returns a string indicating the type of this catalog entry.
    pub(crate) fn typ(&self) -> mz_sql::catalog::CatalogItemType {
        match self {
            CatalogItem::Table(_) => mz_sql::catalog::CatalogItemType::Table,
            CatalogItem::Source(_) => mz_sql::catalog::CatalogItemType::Source,
            CatalogItem::Log(_) => mz_sql::catalog::CatalogItemType::Source,
            CatalogItem::Sink(_) => mz_sql::catalog::CatalogItemType::Sink,
            CatalogItem::View(_) => mz_sql::catalog::CatalogItemType::View,
            CatalogItem::MaterializedView(_) => mz_sql::catalog::CatalogItemType::MaterializedView,
            CatalogItem::Index(_) => mz_sql::catalog::CatalogItemType::Index,
            CatalogItem::Type(_) => mz_sql::catalog::CatalogItemType::Type,
            CatalogItem::Func(_) => mz_sql::catalog::CatalogItemType::Func,
            CatalogItem::Secret(_) => mz_sql::catalog::CatalogItemType::Secret,
            CatalogItem::Connection(_) => mz_sql::catalog::CatalogItemType::Connection,
        }
    }

    pub fn desc(&self, name: &FullItemName) -> Result<Cow<RelationDesc>, SqlCatalogError> {
        match &self {
            CatalogItem::Source(src) => Ok(Cow::Borrowed(&src.desc)),
            CatalogItem::Log(log) => Ok(Cow::Owned(log.variant.desc())),
            CatalogItem::Table(tbl) => Ok(Cow::Borrowed(&tbl.desc)),
            CatalogItem::View(view) => Ok(Cow::Borrowed(&view.desc)),
            CatalogItem::MaterializedView(mview) => Ok(Cow::Borrowed(&mview.desc)),
            CatalogItem::Func(_)
            | CatalogItem::Index(_)
            | CatalogItem::Sink(_)
            | CatalogItem::Type(_)
            | CatalogItem::Secret(_)
            | CatalogItem::Connection(_) => Err(SqlCatalogError::InvalidDependency {
                name: name.to_string(),
                typ: self.typ(),
            }),
        }
    }

    pub fn func(
        &self,
        entry: &CatalogEntry,
    ) -> Result<&'static mz_sql::func::Func, SqlCatalogError> {
        match &self {
            CatalogItem::Func(func) => Ok(func.inner),
            _ => Err(SqlCatalogError::UnexpectedType {
                name: entry.name().item.to_string(),
                actual_type: entry.item_type(),
                expected_type: CatalogItemType::Func,
            }),
        }
    }

    pub fn source_desc(
        &self,
        entry: &CatalogEntry,
    ) -> Result<Option<&SourceDesc>, SqlCatalogError> {
        match &self {
            CatalogItem::Source(source) => match &source.data_source {
                DataSourceDesc::Ingestion(ingestion) => Ok(Some(&ingestion.desc)),
                DataSourceDesc::Introspection(_)
                | DataSourceDesc::Progress
                | DataSourceDesc::Source => Ok(None),
            },
            _ => Err(SqlCatalogError::UnexpectedType {
                name: entry.name().item.to_string(),
                actual_type: entry.item_type(),
                expected_type: CatalogItemType::Source,
            }),
        }
    }

    /// Collects the identifiers of the dataflows that this item depends
    /// upon.
    pub fn uses(&self) -> &[GlobalId] {
        match self {
            CatalogItem::Func(_) => &[],
            CatalogItem::Index(idx) => &idx.depends_on,
            CatalogItem::Sink(sink) => &sink.depends_on,
            CatalogItem::Source(source) => &source.depends_on,
            CatalogItem::Log(_) => &[],
            CatalogItem::Table(table) => &table.depends_on,
            CatalogItem::Type(typ) => &typ.depends_on,
            CatalogItem::View(view) => &view.depends_on,
            CatalogItem::MaterializedView(mview) => &mview.depends_on,
            CatalogItem::Secret(_) => &[],
            CatalogItem::Connection(connection) => &connection.depends_on,
        }
    }

    /// Indicates whether this item is a placeholder for a future item
    /// or if it's actually a real item.
    pub fn is_placeholder(&self) -> bool {
        match self {
            CatalogItem::Func(_)
            | CatalogItem::Index(_)
            | CatalogItem::Source(_)
            | CatalogItem::Log(_)
            | CatalogItem::Table(_)
            | CatalogItem::Type(_)
            | CatalogItem::View(_)
            | CatalogItem::MaterializedView(_)
            | CatalogItem::Secret(_)
            | CatalogItem::Connection(_) => false,
            CatalogItem::Sink(s) => match s.connection {
                StorageSinkConnectionState::Pending(_) => true,
                StorageSinkConnectionState::Ready(_) => false,
            },
        }
    }

    /// Returns the connection ID that this item belongs to, if this item is
    /// temporary.
    pub fn conn_id(&self) -> Option<ConnectionId> {
        match self {
            CatalogItem::View(view) => view.conn_id,
            CatalogItem::Index(index) => index.conn_id,
            CatalogItem::Table(table) => table.conn_id,
            CatalogItem::Log(_)
            | CatalogItem::Source(_)
            | CatalogItem::Sink(_)
            | CatalogItem::MaterializedView(_)
            | CatalogItem::Secret(_)
            | CatalogItem::Type(_)
            | CatalogItem::Func(_)
            | CatalogItem::Connection(_) => None,
        }
    }

    /// Indicates whether this item is temporary or not.
    pub fn is_temporary(&self) -> bool {
        self.conn_id().is_some()
    }

    /// Returns a clone of `self` with all instances of `from` renamed to `to`
    /// (with the option of including the item's own name) or errors if request
    /// is ambiguous.
    fn rename_item_refs(
        &self,
        from: FullItemName,
        to_item_name: String,
        rename_self: bool,
    ) -> Result<CatalogItem, String> {
        let do_rewrite = |create_sql: String| -> Result<String, String> {
            let mut create_stmt = mz_sql::parse::parse(&create_sql)
                .expect("invalid create sql persisted to catalog")
                .into_element();
            if rename_self {
                mz_sql::ast::transform::create_stmt_rename(&mut create_stmt, to_item_name.clone());
            }
            // Determination of what constitutes an ambiguous request is done here.
            mz_sql::ast::transform::create_stmt_rename_refs(&mut create_stmt, from, to_item_name)?;
            Ok(create_stmt.to_ast_string_stable())
        };

        match self {
            CatalogItem::Table(i) => {
                let mut i = i.clone();
                i.create_sql = do_rewrite(i.create_sql)?;
                Ok(CatalogItem::Table(i))
            }
            CatalogItem::Log(i) => Ok(CatalogItem::Log(i.clone())),
            CatalogItem::Source(i) => {
                let mut i = i.clone();
                i.create_sql = do_rewrite(i.create_sql)?;
                Ok(CatalogItem::Source(i))
            }
            CatalogItem::Sink(i) => {
                let mut i = i.clone();
                i.create_sql = do_rewrite(i.create_sql)?;
                Ok(CatalogItem::Sink(i))
            }
            CatalogItem::View(i) => {
                let mut i = i.clone();
                i.create_sql = do_rewrite(i.create_sql)?;
                Ok(CatalogItem::View(i))
            }
            CatalogItem::MaterializedView(i) => {
                let mut i = i.clone();
                i.create_sql = do_rewrite(i.create_sql)?;
                Ok(CatalogItem::MaterializedView(i))
            }
            CatalogItem::Index(i) => {
                let mut i = i.clone();
                i.create_sql = do_rewrite(i.create_sql)?;
                Ok(CatalogItem::Index(i))
            }
            CatalogItem::Secret(i) => {
                let mut i = i.clone();
                i.create_sql = do_rewrite(i.create_sql)?;
                Ok(CatalogItem::Secret(i))
            }
            CatalogItem::Func(_) | CatalogItem::Type(_) => {
                unreachable!("{}s cannot be renamed", self.typ())
            }
            CatalogItem::Connection(i) => {
                let mut i = i.clone();
                i.create_sql = do_rewrite(i.create_sql)?;
                Ok(CatalogItem::Connection(i))
            }
        }
    }

    /// If the object is considered a "compute object"
    /// (i.e., it is managed by the compute controller),
    /// this function returns its cluster ID. Otherwise, it returns nothing.
    ///
    /// This function differs from `cluster_id` because while all
    /// compute objects run on a cluster, the converse is not true.
    pub fn is_compute_object_on_cluster(&self) -> Option<ClusterId> {
        match self {
            CatalogItem::Index(index) => Some(index.cluster_id),
            CatalogItem::Table(_)
            | CatalogItem::Source(_)
            | CatalogItem::Log(_)
            | CatalogItem::View(_)
            | CatalogItem::MaterializedView(_)
            | CatalogItem::Sink(_)
            | CatalogItem::Type(_)
            | CatalogItem::Func(_)
            | CatalogItem::Secret(_)
            | CatalogItem::Connection(_) => None,
        }
    }

    pub fn cluster_id(&self) -> Option<ClusterId> {
        match self {
            CatalogItem::MaterializedView(mv) => Some(mv.cluster_id),
            CatalogItem::Index(index) => Some(index.cluster_id),
            CatalogItem::Source(source) => match &source.data_source {
                DataSourceDesc::Ingestion(ingestion) => Some(ingestion.cluster_id),
                DataSourceDesc::Introspection(_)
                | DataSourceDesc::Progress
                | DataSourceDesc::Source => None,
            },
            CatalogItem::Sink(sink) => Some(sink.cluster_id),
            CatalogItem::Table(_)
            | CatalogItem::Log(_)
            | CatalogItem::View(_)
            | CatalogItem::Type(_)
            | CatalogItem::Func(_)
            | CatalogItem::Secret(_)
            | CatalogItem::Connection(_) => None,
        }
    }

    /// The custom compaction window, if any has been set.
    // Note[btv]: As of 2023-04-10, this is only set
    // for objects with `is_retained_metrics_object`. That
    // may not always be true in the future, if we enable user-settable
    // compaction windows.
    pub fn custom_logical_compaction_window(&self) -> Option<Duration> {
        match self {
            CatalogItem::Table(table) => table.custom_logical_compaction_window,
            CatalogItem::Source(source) => source.custom_logical_compaction_window,
            CatalogItem::Index(index) => index.custom_logical_compaction_window,
            CatalogItem::MaterializedView(_)
            | CatalogItem::Log(_)
            | CatalogItem::View(_)
            | CatalogItem::Sink(_)
            | CatalogItem::Type(_)
            | CatalogItem::Func(_)
            | CatalogItem::Secret(_)
            | CatalogItem::Connection(_) => None,
        }
    }

    /// The initial compaction window, for objects that have one; that is,
    /// tables, sources, indexes, and MVs.
    ///
    /// If `custom_logical_compaction_window()` returns something, use
    /// that.  Otherwise, use a sensible default (currently 1s).
    ///
    /// For objects that do not have the concept of compaction window,
    /// return nothing.
    pub fn initial_logical_compaction_window(&self) -> Option<Duration> {
        let custom_logical_compaction_window = match self {
            CatalogItem::Table(_)
            | CatalogItem::Source(_)
            | CatalogItem::Index(_)
            | CatalogItem::MaterializedView(_) => self.custom_logical_compaction_window(),
            CatalogItem::Log(_)
            | CatalogItem::View(_)
            | CatalogItem::Sink(_)
            | CatalogItem::Type(_)
            | CatalogItem::Func(_)
            | CatalogItem::Secret(_)
            | CatalogItem::Connection(_) => return None,
        };
        Some(custom_logical_compaction_window.unwrap_or(DEFAULT_LOGICAL_COMPACTION_WINDOW))
    }

    /// Whether the item's logical compaction window
    /// is controlled by the METRICS_RETENTION
    /// system var.
    pub fn is_retained_metrics_object(&self) -> bool {
        match self {
            CatalogItem::Table(table) => table.is_retained_metrics_object,
            CatalogItem::Source(source) => source.is_retained_metrics_object,
            CatalogItem::Index(index) => index.is_retained_metrics_object,
            CatalogItem::Log(_)
            | CatalogItem::View(_)
            | CatalogItem::MaterializedView(_)
            | CatalogItem::Sink(_)
            | CatalogItem::Type(_)
            | CatalogItem::Func(_)
            | CatalogItem::Secret(_)
            | CatalogItem::Connection(_) => false,
        }
    }
}

impl CatalogEntry {
    /// Reports the description of the datums produced by this catalog item.
    pub fn desc(&self, name: &FullItemName) -> Result<Cow<RelationDesc>, SqlCatalogError> {
        self.item.desc(name)
    }

    /// Returns the [`mz_sql::func::Func`] associated with this `CatalogEntry`.
    pub fn func(&self) -> Result<&'static mz_sql::func::Func, SqlCatalogError> {
        self.item.func(self)
    }

    /// Returns the inner [`Index`] if this entry is an index, else `None`.
    pub fn index(&self) -> Option<&Index> {
        match self.item() {
            CatalogItem::Index(idx) => Some(idx),
            _ => None,
        }
    }

    /// Returns the inner [`Source`] if this entry is a source, else `None`.
    pub fn source(&self) -> Option<&Source> {
        match self.item() {
            CatalogItem::Source(src) => Some(src),
            _ => None,
        }
    }

    /// Returns the inner [`Sink`] if this entry is a sink, else `None`.
    pub fn sink(&self) -> Option<&Sink> {
        match self.item() {
            CatalogItem::Sink(sink) => Some(sink),
            _ => None,
        }
    }

    /// Returns the inner [`Secret`] if this entry is a secret, else `None`.
    pub fn secret(&self) -> Option<&Secret> {
        match self.item() {
            CatalogItem::Secret(secret) => Some(secret),
            _ => None,
        }
    }

    pub fn connection(&self) -> Result<&Connection, SqlCatalogError> {
        match self.item() {
            CatalogItem::Connection(connection) => Ok(connection),
            _ => Err(SqlCatalogError::UnknownConnection(self.name().to_string())),
        }
    }

    /// Returns the [`mz_storage_client::types::sources::SourceDesc`] associated with
    /// this `CatalogEntry`, if any.
    pub fn source_desc(&self) -> Result<Option<&SourceDesc>, SqlCatalogError> {
        self.item.source_desc(self)
    }

    /// Reports whether this catalog entry is a connection.
    pub fn is_connection(&self) -> bool {
        matches!(self.item(), CatalogItem::Connection(_))
    }

    /// Reports whether this catalog entry is a table.
    pub fn is_table(&self) -> bool {
        matches!(self.item(), CatalogItem::Table(_))
    }

    /// Reports whether this catalog entry is a source. Note that this includes
    /// subsources.
    pub fn is_source(&self) -> bool {
        matches!(self.item(), CatalogItem::Source(_))
    }

    /// Reports whether this catalog entry is a subsource.
    pub fn is_subsource(&self) -> bool {
        match &self.item() {
            CatalogItem::Source(source) => matches!(
                &source.data_source,
                DataSourceDesc::Progress | DataSourceDesc::Source
            ),
            _ => false,
        }
    }

    /// Returns the `GlobalId` of all of this entry's subsources.
    pub fn subsources(&self) -> Vec<GlobalId> {
        match &self.item() {
            CatalogItem::Source(source) => match &source.data_source {
                DataSourceDesc::Ingestion(ingestion) => ingestion
                    .subsource_exports
                    .keys()
                    .copied()
                    .chain(std::iter::once(
                        ingestion
                            .remap_collection_id
                            .expect("remap collection must named by this point"),
                    ))
                    .collect(),
                DataSourceDesc::Introspection(_)
                | DataSourceDesc::Progress
                | DataSourceDesc::Source => vec![],
            },
            CatalogItem::Table(_)
            | CatalogItem::Log(_)
            | CatalogItem::View(_)
            | CatalogItem::MaterializedView(_)
            | CatalogItem::Sink(_)
            | CatalogItem::Index(_)
            | CatalogItem::Type(_)
            | CatalogItem::Func(_)
            | CatalogItem::Secret(_)
            | CatalogItem::Connection(_) => vec![],
        }
    }

    /// Reports whether this catalog entry is a sink.
    pub fn is_sink(&self) -> bool {
        matches!(self.item(), CatalogItem::Sink(_))
    }

    /// Reports whether this catalog entry is a materialized view.
    pub fn is_materialized_view(&self) -> bool {
        matches!(self.item(), CatalogItem::MaterializedView(_))
    }

    /// Reports whether this catalog entry is a secret.
    pub fn is_secret(&self) -> bool {
        matches!(self.item(), CatalogItem::Secret(_))
    }

    /// Reports whether this catalog entry is a introspection source.
    pub fn is_introspection_source(&self) -> bool {
        matches!(self.item(), CatalogItem::Log(_))
    }

    /// Reports whether this catalog entry can be treated as a relation, it can produce rows.
    pub fn is_relation(&self) -> bool {
        match self.item {
            CatalogItem::Table(_)
            | CatalogItem::Source(_)
            | CatalogItem::Log(_)
            | CatalogItem::View(_)
            | CatalogItem::MaterializedView(_) => true,
            CatalogItem::Sink(_)
            | CatalogItem::Index(_)
            | CatalogItem::Type(_)
            | CatalogItem::Func(_)
            | CatalogItem::Secret(_)
            | CatalogItem::Connection(_) => false,
        }
    }

    /// Collects the identifiers of the dataflows that this dataflow depends
    /// upon.
    pub fn uses(&self) -> &[GlobalId] {
        self.item.uses()
    }

    /// Returns the `CatalogItem` associated with this catalog entry.
    pub fn item(&self) -> &CatalogItem {
        &self.item
    }

    /// Returns the global ID of this catalog entry.
    pub fn id(&self) -> GlobalId {
        self.id
    }

    /// Returns the OID of this catalog entry.
    pub fn oid(&self) -> u32 {
        self.oid
    }

    /// Returns the fully qualified name of this catalog entry.
    pub fn name(&self) -> &QualifiedItemName {
        &self.name
    }

    /// Returns the identifiers of the dataflows that depend upon this dataflow.
    pub fn used_by(&self) -> &[GlobalId] {
        &self.used_by
    }

    /// Returns the connection ID that this item belongs to, if this item is
    /// temporary.
    pub fn conn_id(&self) -> Option<ConnectionId> {
        self.item.conn_id()
    }

    /// Returns the role ID of the entry owner.
    pub fn owner_id(&self) -> &RoleId {
        &self.owner_id
    }
}

struct AllocatedBuiltinSystemIds<T> {
    all_builtins: Vec<(T, GlobalId)>,
    new_builtins: Vec<(T, GlobalId)>,
    migrated_builtins: Vec<GlobalId>,
}

/// Functions can share the same name as any other catalog item type
/// within a given schema.
/// For example, a function can have the same name as a type, e.g.
/// 'date'.
/// As such, system objects are keyed in the catalog storage by the
/// tuple (schema_name, object_type, object_name), which is guaranteed
/// to be unique.
#[derive(Debug)]
pub struct SystemObjectMapping {
    schema_name: String,
    object_type: CatalogItemType,
    object_name: String,
    id: GlobalId,
    fingerprint: String,
}

#[derive(Debug)]
pub enum CatalogItemRebuilder {
    SystemSource(CatalogItem),
    Object {
        id: GlobalId,
        sql: String,
        is_retained_metrics_object: bool,
        custom_logical_compaction_window: Option<Duration>,
    },
}

impl CatalogItemRebuilder {
    fn new(
        entry: &CatalogEntry,
        id: GlobalId,
        ancestor_ids: &BTreeMap<GlobalId, GlobalId>,
    ) -> Self {
        if id.is_system() && (entry.is_table() || entry.is_introspection_source()) {
            Self::SystemSource(entry.item().clone())
        } else {
            let create_sql = entry.create_sql().to_string();
            assert_ne!(create_sql.to_lowercase(), CREATE_SQL_TODO.to_lowercase());
            let mut create_stmt = mz_sql::parse::parse(&create_sql)
                .expect("invalid create sql persisted to catalog")
                .into_element();
            mz_sql::ast::transform::create_stmt_replace_ids(&mut create_stmt, ancestor_ids);
            Self::Object {
                id,
                sql: create_stmt.to_ast_string_stable(),
                is_retained_metrics_object: entry.item().is_retained_metrics_object(),
                custom_logical_compaction_window: entry.item().custom_logical_compaction_window(),
            }
        }
    }

    fn build(self, catalog: &Catalog) -> CatalogItem {
        match self {
            Self::SystemSource(item) => item,
            Self::Object {
                id,
                sql,
                is_retained_metrics_object,
                custom_logical_compaction_window,
            } => catalog
                .parse_item(
                    id,
                    sql.clone(),
                    None,
                    is_retained_metrics_object,
                    custom_logical_compaction_window,
                )
                .unwrap_or_else(|error| panic!("invalid persisted create sql ({error:?}): {sql}")),
        }
    }
}

#[derive(Debug)]
pub struct BuiltinMigrationMetadata {
    // Used to drop objects on STORAGE nodes
    pub previous_sink_ids: Vec<GlobalId>,
    pub previous_materialized_view_ids: Vec<GlobalId>,
    pub previous_source_ids: Vec<GlobalId>,
    // Used to update in memory catalog state
    pub all_drop_ops: Vec<GlobalId>,
    pub all_create_ops: Vec<(
        GlobalId,
        u32,
        QualifiedItemName,
        RoleId,
        Vec<MzAclItem>,
        CatalogItemRebuilder,
    )>,
    pub introspection_source_index_updates:
        BTreeMap<ClusterId, Vec<(LogVariant, String, GlobalId)>>,
    // Used to update persisted on disk catalog state
    pub migrated_system_object_mappings: BTreeMap<GlobalId, SystemObjectMapping>,
    pub user_drop_ops: Vec<GlobalId>,
    pub user_create_ops: Vec<(GlobalId, SchemaId, String)>,
}

impl BuiltinMigrationMetadata {
    fn new() -> BuiltinMigrationMetadata {
        BuiltinMigrationMetadata {
            previous_sink_ids: Vec::new(),
            previous_materialized_view_ids: Vec::new(),
            previous_source_ids: Vec::new(),
            all_drop_ops: Vec::new(),
            all_create_ops: Vec::new(),
            introspection_source_index_updates: BTreeMap::new(),
            migrated_system_object_mappings: BTreeMap::new(),
            user_drop_ops: Vec::new(),
            user_create_ops: Vec::new(),
        }
    }
}

impl Catalog {
    /// Opens or creates a catalog that stores data at `path`.
    ///
    /// Returns the catalog, metadata about builtin objects that have changed
    /// schemas since last restart, a list of updates to builtin tables that
    /// describe the initial state of the catalog, and the version of the
    /// catalog before any migrations were performed.
    #[tracing::instrument(name = "catalog::open", level = "info", skip_all)]
    pub async fn open(
        config: Config<'_>,
    ) -> Result<
        (
            Catalog,
            BuiltinMigrationMetadata,
            Vec<BuiltinTableUpdate>,
            String,
        ),
        AdapterError,
    > {
        let mut catalog = Catalog {
            state: CatalogState {
                database_by_name: BTreeMap::new(),
                database_by_id: BTreeMap::new(),
                entry_by_id: BTreeMap::new(),
                ambient_schemas_by_name: BTreeMap::new(),
                ambient_schemas_by_id: BTreeMap::new(),
                temporary_schemas: BTreeMap::new(),
                clusters_by_id: BTreeMap::new(),
                clusters_by_name: BTreeMap::new(),
                clusters_by_linked_object_id: BTreeMap::new(),
                roles_by_name: BTreeMap::new(),
                roles_by_id: BTreeMap::new(),
                config: mz_sql::catalog::CatalogConfig {
                    start_time: to_datetime((config.now)()),
                    start_instant: Instant::now(),
                    nonce: rand::random(),
                    unsafe_mode: config.unsafe_mode,
                    environment_id: config.environment_id,
                    session_id: Uuid::new_v4(),
                    build_info: config.build_info,
                    timestamp_interval: Duration::from_secs(1),
                    now: config.now.clone(),
                },
                oid_counter: FIRST_USER_OID,
                cluster_replica_sizes: config.cluster_replica_sizes,
                default_storage_cluster_size: config.default_storage_cluster_size,
                availability_zones: config.availability_zones,
                system_configuration: SystemVars::default(),
                egress_ips: config.egress_ips,
                aws_principal_context: config.aws_principal_context,
                aws_privatelink_availability_zones: config.aws_privatelink_availability_zones,
            },
            transient_revision: 0,
            storage: Arc::new(Mutex::new(config.storage)),
        };

        catalog.create_temporary_schema(SYSTEM_CONN_ID, MZ_SYSTEM_ROLE_ID)?;

        let databases = catalog.storage().await.load_databases().await?;
        for storage::Database {
            id,
            name,
            owner_id,
            privileges,
        } in databases
        {
            let oid = catalog.allocate_oid()?;
            catalog.state.database_by_id.insert(
                id.clone(),
                Database {
                    name: name.clone(),
                    id,
                    oid,
                    schemas_by_id: BTreeMap::new(),
                    schemas_by_name: BTreeMap::new(),
                    owner_id,
                    privileges,
                },
            );
            catalog
                .state
                .database_by_name
                .insert(name.clone(), id.clone());
        }

        let schemas = catalog.storage().await.load_schemas().await?;
        for storage::Schema {
            id,
            name,
            database_id,
            owner_id,
            privileges,
        } in schemas
        {
            let oid = catalog.allocate_oid()?;
            let (schemas_by_id, schemas_by_name, database_spec) = match &database_id {
                Some(database_id) => {
                    let db = catalog
                        .state
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
                    &mut catalog.state.ambient_schemas_by_id,
                    &mut catalog.state.ambient_schemas_by_name,
                    ResolvedDatabaseSpecifier::Ambient,
                ),
            };
            schemas_by_id.insert(
                id.clone(),
                Schema {
                    name: QualifiedSchemaName {
                        database: database_spec,
                        schema: name.clone(),
                    },
                    id: SchemaSpecifier::Id(id.clone()),
                    oid,
                    items: BTreeMap::new(),
                    functions: BTreeMap::new(),
                    owner_id,
                    privileges,
                },
            );
            schemas_by_name.insert(name.clone(), id);
        }

        let roles = catalog.storage().await.load_roles().await?;
        for storage::Role {
            id,
            name,
            attributes,
            membership,
        } in roles
        {
            let oid = catalog.allocate_oid()?;
            catalog.state.roles_by_name.insert(name.clone(), id);
            catalog.state.roles_by_id.insert(
                id,
                Role {
                    name,
                    id,
                    oid,
                    attributes,
                    membership,
                },
            );
        }

        catalog
            .load_system_configuration(
                config.bootstrap_system_parameters,
                config.system_parameter_frontend,
            )
            .await?;

        // Now that LD is loaded, set the intended stash timeout.
        // TODO: Move this into the stash constructor.
        catalog
            .storage()
            .await
            .set_connect_timeout(catalog.system_config().crdb_connect_timeout())
            .await;

        catalog.load_builtin_types().await?;

        let persisted_builtin_ids = catalog.storage().await.load_system_gids().await?;
        let AllocatedBuiltinSystemIds {
            all_builtins,
            new_builtins,
            migrated_builtins,
        } = catalog
            .allocate_system_ids(
                BUILTINS::iter()
                    .filter(|builtin| !matches!(builtin, Builtin::Type(_)))
                    .collect(),
                |builtin| {
                    persisted_builtin_ids
                        .get(&(
                            builtin.schema().to_string(),
                            builtin.catalog_item_type(),
                            builtin.name().to_string(),
                        ))
                        .cloned()
                },
            )
            .await?;

        let id_fingerprint_map: BTreeMap<GlobalId, String> = all_builtins
            .iter()
            .map(|(builtin, id)| (*id, builtin.fingerprint()))
            .collect();
        let (builtin_indexes, builtin_non_indexes): (Vec<_>, Vec<_>) = all_builtins
            .into_iter()
            .partition(|(builtin, _)| matches!(builtin, Builtin::Index(_)));

        {
            let span = tracing::span!(tracing::Level::DEBUG, "builtin_non_indexes");
            let _enter = span.enter();
            for (builtin, id) in builtin_non_indexes {
                let schema_id = catalog.state.ambient_schemas_by_name[builtin.schema()];
                let name = QualifiedItemName {
                    qualifiers: ItemQualifiers {
                        database_spec: ResolvedDatabaseSpecifier::Ambient,
                        schema_spec: SchemaSpecifier::Id(schema_id),
                    },
                    item: builtin.name().into(),
                };
                match builtin {
                    Builtin::Log(log) => {
                        let oid = catalog.allocate_oid()?;
                        catalog.state.insert_item(
                            id,
                            oid,
                            name.clone(),
                            CatalogItem::Log(Log {
                                variant: log.variant.clone(),
                                has_storage_collection: false,
                            }),
                            MZ_SYSTEM_ROLE_ID,
                            vec![
                                rbac::default_catalog_privilege(
                                    mz_sql_parser::ast::ObjectType::Source,
                                ),
                                rbac::owner_privilege(
                                    mz_sql_parser::ast::ObjectType::Source,
                                    MZ_SYSTEM_ROLE_ID,
                                ),
                            ],
                        );
                    }

                    Builtin::Table(table) => {
                        let oid = catalog.allocate_oid()?;
                        catalog.state.insert_item(
                            id,
                            oid,
                            name.clone(),
                            CatalogItem::Table(Table {
                                create_sql: CREATE_SQL_TODO.to_string(),
                                desc: table.desc.clone(),
                                defaults: vec![Expr::null(); table.desc.arity()],
                                conn_id: None,
                                depends_on: vec![],
                                custom_logical_compaction_window: table
                                    .is_retained_metrics_object
                                    .then(|| catalog.state.system_config().metrics_retention()),
                                is_retained_metrics_object: table.is_retained_metrics_object,
                            }),
                            MZ_SYSTEM_ROLE_ID,
                            vec![
                                rbac::default_catalog_privilege(
                                    mz_sql_parser::ast::ObjectType::Table,
                                ),
                                rbac::owner_privilege(
                                    mz_sql_parser::ast::ObjectType::Table,
                                    MZ_SYSTEM_ROLE_ID,
                                ),
                            ],
                        );
                    }
                    Builtin::Index(_) => {
                        unreachable!("handled later once clusters have been created")
                    }
                    Builtin::View(view) => {
                        let item = catalog
                        .parse_item(
                            id,
                            view.sql.into(),
                            None,
                            false,
                            None
                        )
                        .unwrap_or_else(|e| {
                            panic!(
                                "internal error: failed to load bootstrap view:\n\
                                    {}\n\
                                    error:\n\
                                    {:?}\n\n\
                                    make sure that the schema name is specified in the builtin view's create sql statement.",
                                view.name, e
                            )
                        });
                        let oid = catalog.allocate_oid()?;
                        catalog.state.insert_item(
                            id,
                            oid,
                            name,
                            item,
                            MZ_SYSTEM_ROLE_ID,
                            vec![
                                rbac::default_catalog_privilege(
                                    mz_sql_parser::ast::ObjectType::View,
                                ),
                                rbac::owner_privilege(
                                    mz_sql_parser::ast::ObjectType::View,
                                    MZ_SYSTEM_ROLE_ID,
                                ),
                            ],
                        );
                    }

                    Builtin::Type(_) => unreachable!("loaded separately"),

                    Builtin::Func(func) => {
                        let oid = catalog.allocate_oid()?;
                        catalog.state.insert_item(
                            id,
                            oid,
                            name.clone(),
                            CatalogItem::Func(Func { inner: func.inner }),
                            MZ_SYSTEM_ROLE_ID,
                            Vec::new(),
                        );
                    }

                    Builtin::Source(coll) => {
                        let introspection_type = match &coll.data_source {
                            Some(i) => i.clone(),
                            None => continue,
                        };

                        let oid = catalog.allocate_oid()?;
                        catalog.state.insert_item(
                            id,
                            oid,
                            name.clone(),
                            CatalogItem::Source(Source {
                                create_sql: CREATE_SQL_TODO.to_string(),
                                data_source: DataSourceDesc::Introspection(introspection_type),
                                desc: coll.desc.clone(),
                                timeline: Timeline::EpochMilliseconds,
                                depends_on: vec![],
                                custom_logical_compaction_window: coll
                                    .is_retained_metrics_object
                                    .then(|| catalog.state.system_config().metrics_retention()),
                                is_retained_metrics_object: coll.is_retained_metrics_object,
                            }),
                            MZ_SYSTEM_ROLE_ID,
                            vec![
                                rbac::default_catalog_privilege(
                                    mz_sql_parser::ast::ObjectType::Source,
                                ),
                                rbac::owner_privilege(
                                    mz_sql_parser::ast::ObjectType::Source,
                                    MZ_SYSTEM_ROLE_ID,
                                ),
                            ],
                        );
                    }
                }
            }
        }

        let clusters = catalog.storage().await.load_clusters().await?;
        for storage::Cluster {
            id,
            name,
            linked_object_id,
            owner_id,
            privileges,
        } in clusters
        {
            let introspection_source_index_gids = catalog
                .storage()
                .await
                .load_introspection_source_index_gids(id)
                .await?;

            let AllocatedBuiltinSystemIds {
                all_builtins: all_indexes,
                new_builtins: new_indexes,
                ..
            } = catalog
                .allocate_system_ids(BUILTINS::logs().collect(), |log| {
                    introspection_source_index_gids
                        .get(log.name)
                        .cloned()
                        // We migrate introspection sources later so we can hardcode the fingerprint as ""
                        .map(|id| (id, "".to_string()))
                })
                .await?;

            catalog
                .storage()
                .await
                .set_introspection_source_index_gids(
                    new_indexes
                        .iter()
                        .map(|(log, index_id)| (id, log.name, *index_id))
                        .collect(),
                )
                .await?;

            catalog.state.insert_cluster(
                id,
                name,
                linked_object_id,
                all_indexes,
                owner_id,
                privileges,
            );
        }

        let replicas = catalog.storage().await.load_cluster_replicas().await?;
        for storage::ClusterReplica {
            cluster_id,
            replica_id,
            name,
            serialized_config,
            owner_id,
        } in replicas
        {
            let logging = ReplicaLogging {
                log_logging: serialized_config.logging.log_logging,
                interval: serialized_config.logging.interval,
            };
            let config = ReplicaConfig {
                location: catalog
                    .concretize_replica_location(serialized_config.location, &vec![])?,
                compute: ComputeReplicaConfig {
                    logging,
                    idle_arrangement_merge_effort: serialized_config.idle_arrangement_merge_effort,
                },
            };

            // And write the allocated sources back to storage
            catalog
                .storage()
                .await
                .set_replica_config(replica_id, cluster_id, name.clone(), &config, owner_id)
                .await?;

            catalog
                .state
                .insert_cluster_replica(cluster_id, name, replica_id, config, owner_id);
        }

        for (builtin, id) in builtin_indexes {
            let schema_id = catalog.state.ambient_schemas_by_name[builtin.schema()];
            let name = QualifiedItemName {
                qualifiers: ItemQualifiers {
                    database_spec: ResolvedDatabaseSpecifier::Ambient,
                    schema_spec: SchemaSpecifier::Id(schema_id),
                },
                item: builtin.name().into(),
            };
            match builtin {
                Builtin::Index(index) => {
                    let mut item = catalog
                        .parse_item(
                            id,
                            index.sql.into(),
                            None,
                            index.is_retained_metrics_object,
                            if index.is_retained_metrics_object { Some(catalog.state.system_config().metrics_retention())} else { None },
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

                    let oid = catalog.allocate_oid()?;
                    catalog
                        .state
                        .insert_item(id, oid, name, item, MZ_SYSTEM_ROLE_ID, Vec::new());
                }
                Builtin::Log(_)
                | Builtin::Table(_)
                | Builtin::View(_)
                | Builtin::Type(_)
                | Builtin::Func(_)
                | Builtin::Source(_) => {
                    unreachable!("handled above")
                }
            }
        }

        let new_system_id_mappings = new_builtins
            .iter()
            .map(|(builtin, id)| SystemObjectMapping {
                schema_name: builtin.schema().to_string(),
                object_type: builtin.catalog_item_type(),
                object_name: builtin.name().to_string(),
                id: *id,
                fingerprint: builtin.fingerprint(),
            })
            .collect();
        catalog
            .storage()
            .await
            .set_system_object_mapping(new_system_id_mappings)
            .await?;

        let last_seen_version = catalog
            .storage()
            .await
            .get_catalog_content_version()
            .await?
            .unwrap_or_else(|| "new".to_string());

        if !config.skip_migrations {
            migrate::migrate(&mut catalog, config.connection_context)
                .await
                .map_err(|e| {
                    Error::new(ErrorKind::FailedMigration {
                        last_seen_version: last_seen_version.clone(),
                        this_version: catalog.config().build_info.version,
                        cause: e.to_string(),
                    })
                })?;
            catalog
                .storage()
                .await
                .set_catalog_content_version(catalog.config().build_info.version)
                .await?;
        }

        let mut catalog = {
            let mut storage = catalog.storage().await;
            let mut tx = storage.transaction().await?;
            let catalog = Self::load_catalog_items(&mut tx, &catalog)?;
            tx.commit().await?;
            catalog
        };

        let mut builtin_migration_metadata = catalog
            .generate_builtin_migration_metadata(migrated_builtins, id_fingerprint_map)
            .await?;
        catalog.apply_in_memory_builtin_migration(&mut builtin_migration_metadata)?;
        catalog
            .apply_persisted_builtin_migration(&mut builtin_migration_metadata)
            .await?;

        // Load public keys for SSH connections from the secrets store to the catalog
        for (id, entry) in catalog.state.entry_by_id.iter_mut() {
            if let CatalogItem::Connection(ref mut connection) = entry.item {
                if let mz_storage_client::types::connections::Connection::Ssh(ref mut ssh) =
                    connection.connection
                {
                    let secret = config.secrets_reader.read(*id).await?;
                    let keyset = SshKeyPairSet::from_bytes(&secret)?;
                    let public_key_pair = keyset.public_keys();
                    ssh.public_keys = Some(public_key_pair);
                }
            }
        }

        let mut builtin_table_updates = vec![];
        for (schema_id, schema) in &catalog.state.ambient_schemas_by_id {
            let db_spec = ResolvedDatabaseSpecifier::Ambient;
            builtin_table_updates.push(catalog.state.pack_schema_update(&db_spec, schema_id, 1));
            for (_item_name, item_id) in &schema.items {
                builtin_table_updates.extend(catalog.state.pack_item_update(*item_id, 1));
            }
            for (_item_name, function_id) in &schema.functions {
                builtin_table_updates.extend(catalog.state.pack_item_update(*function_id, 1));
            }
        }
        for (_id, db) in &catalog.state.database_by_id {
            builtin_table_updates.push(catalog.state.pack_database_update(db, 1));
            let db_spec = ResolvedDatabaseSpecifier::Id(db.id.clone());
            for (schema_id, schema) in &db.schemas_by_id {
                builtin_table_updates
                    .push(catalog.state.pack_schema_update(&db_spec, schema_id, 1));
                for (_item_name, item_id) in &schema.items {
                    builtin_table_updates.extend(catalog.state.pack_item_update(*item_id, 1));
                }
                for (_item_name, function_id) in &schema.functions {
                    builtin_table_updates.extend(catalog.state.pack_item_update(*function_id, 1));
                }
            }
        }
        for (_id, role) in &catalog.state.roles_by_id {
            if let Some(builtin_update) = catalog.state.pack_role_update(role.id, 1) {
                builtin_table_updates.push(builtin_update);
            }
            for group_id in role.membership.map.keys() {
                builtin_table_updates.push(
                    catalog
                        .state
                        .pack_role_members_update(*group_id, role.id, 1),
                )
            }
        }
        for (id, cluster) in &catalog.state.clusters_by_id {
            builtin_table_updates.push(catalog.state.pack_cluster_update(&cluster.name, 1));
            if let Some(linked_object_id) = cluster.linked_object_id {
                builtin_table_updates.push(catalog.state.pack_cluster_link_update(
                    &cluster.name,
                    linked_object_id,
                    1,
                ));
            }
            for (replica_name, replica_id) in &cluster.replica_id_by_name {
                builtin_table_updates.push(catalog.state.pack_cluster_replica_update(
                    *id,
                    replica_name,
                    1,
                ));
                let replica = catalog.state.get_cluster_replica(*id, *replica_id);
                for process_id in 0..replica.config.location.num_processes() {
                    let update = catalog.state.pack_cluster_replica_status_update(
                        *id,
                        *replica_id,
                        u64::cast_from(process_id),
                        1,
                    );
                    builtin_table_updates.push(update);
                }
            }
        }
        // Operators aren't stored in the catalog, but we would like them in
        // introspection views.
        for (op, func) in OP_IMPLS.iter() {
            match func {
                mz_sql::func::Func::Scalar(impls) => {
                    for imp in impls {
                        builtin_table_updates.push(catalog.state.pack_op_update(
                            op,
                            imp.details(),
                            1,
                        ));
                    }
                }
                _ => unreachable!("all operators must be scalar functions"),
            }
        }
        let audit_logs = catalog.storage().await.load_audit_log().await?;
        for event in audit_logs {
            builtin_table_updates.push(catalog.state.pack_audit_log_update(&event)?);
        }

        // To avoid reading over storage_usage events multiple times, do both
        // the table updates and delete calculations in a single read over the
        // data.
        let storage_usage_events = catalog
            .storage()
            .await
            .fetch_and_prune_storage_usage(config.storage_usage_retention_period)
            .await?;
        for event in storage_usage_events {
            builtin_table_updates.push(catalog.state.pack_storage_usage_update(&event)?);
        }

        for ip in &catalog.state.egress_ips {
            builtin_table_updates.push(catalog.state.pack_egress_ip_update(ip)?);
        }

        Ok((
            catalog,
            builtin_migration_metadata,
            builtin_table_updates,
            last_seen_version,
        ))
    }

    /// Loads the system configuration from the various locations in which its
    /// values and value overrides can reside.
    ///
    /// This method should _always_ be called during catalog creation _before_
    /// any other operations that depend on system configuration values.
    ///
    /// Configuration is loaded in the following order:
    ///
    /// 1. Load parameters from the configuration persisted in the catalog
    ///    storage backend.
    /// 2. Overwrite without persisting selected parameter values from the
    ///    configuration passed in the provided `bootstrap_system_parameters`
    ///    map.
    /// 3. Overwrite and persist selected parameter values from the
    ///    configuration that can be pulled from the provided
    ///    `system_parameter_frontend` (if present).
    ///
    /// # Errors
    #[tracing::instrument(level = "info", skip_all)]
    async fn load_system_configuration(
        &mut self,
        bootstrap_system_parameters: BTreeMap<String, String>,
        system_parameter_frontend: Option<Arc<SystemParameterFrontend>>,
    ) -> Result<(), AdapterError> {
        let (system_config, boot_ts) = {
            let mut storage = self.storage().await;
            let system_config = storage.load_system_configuration().await?;
            let boot_ts = storage.boot_ts();
            (system_config, boot_ts)
        };
        for (name, value) in &bootstrap_system_parameters {
            match self
                .state
                .insert_system_configuration(name, VarInput::Flat(value))
            {
                Ok(_) => (),
                Err(AdapterError::VarError(VarError::UnknownParameter(name))) => {
                    warn!(%name, "cannot load unknown system parameter from stash");
                }
                Err(e) => return Err(e),
            };
        }
        for (name, value) in system_config {
            match self
                .state
                .insert_system_configuration(&name, VarInput::Flat(&value))
            {
                Ok(_) => (),
                Err(AdapterError::VarError(VarError::UnknownParameter(name))) => {
                    warn!(%name, "cannot load unknown system parameter from stash");
                }
                Err(e) => return Err(e),
            };
        }
        if let Some(system_parameter_frontend) = system_parameter_frontend {
            if !self.state.system_config().config_has_synced_once() {
                tracing::info!("parameter sync on boot: start sync");

                // We intentionally block initial startup, potentially forever,
                // on initializing LaunchDarkly. This may seem scary, but the
                // alternative is even scarier. Over time, we expect that the
                // compiled-in default values for the system parameters will
                // drift substantially from the defaults configured in
                // LaunchDarkly, to the point that starting an environment
                // without loading the latest values from LaunchDarkly will
                // result in running an untested configuration.
                //
                // Note this only applies during initial startup. Restarting
                // after we've synced once doesn't block on LaunchDarkly, as it
                // seems reasonable to assume that the last-synced configuration
                // was valid enough.
                //
                // This philosophy appears to provide a good balance between not
                // running untested configurations in production while also not
                // making LaunchDarkly a "tier 1" dependency for existing
                // environments.
                //
                // If this proves to be an issue, we could seek to address the
                // configuration drift in a different way--for example, by
                // writing a script that runs in CI nightly and checks for
                // deviation between the compiled Rust code and LaunchDarkly.
                //
                // If it is absolutely necessary to bring up a new environment
                // while LaunchDarkly is down, the following manual mitigation
                // can be performed:
                //
                //    1. Edit the environmentd startup parameters to omit the
                //       LaunchDarkly configuration.
                //    2. Boot environmentd.
                //    3. Run `ALTER SYSTEM config_has_synced_once = true`.
                //    4. Adjust any other parameters as necessary to avoid
                //       running a nonstandard configuration in production.
                //    5. Edit the environmentd startup parameters to restore the
                //       LaunchDarkly configuration, for when LaunchDarkly comes
                //       back online.
                //    6. Reboot environmentd.
                system_parameter_frontend.ensure_initialized().await;

                let mut params = SynchronizedParameters::new(self.state.system_config().clone());
                system_parameter_frontend.pull(&mut params);
                let ops = params
                    .modified()
                    .into_iter()
                    .map(|param| {
                        let name = param.name;
                        let value = param.value;
                        tracing::debug!(name, value, "sync parameter");
                        Op::UpdateSystemConfiguration {
                            name,
                            value: OwnedVarInput::Flat(value),
                        }
                    })
                    .chain(std::iter::once({
                        let name = CONFIG_HAS_SYNCED_ONCE.name().to_string();
                        let value = true.to_string();
                        tracing::debug!(name, value, "sync parameter");
                        Op::UpdateSystemConfiguration {
                            name,
                            value: OwnedVarInput::Flat(value),
                        }
                    }))
                    .collect::<Vec<_>>();
                self.transact(boot_ts, None, ops, |_| Ok(()))
                    .await
                    .unwrap_or_terminate("cannot fail to transact");
                tracing::info!("parameter sync on boot: end sync");
            } else {
                tracing::info!("parameter sync on boot: skipping sync as config has synced once");
            }
        }
        Ok(())
    }

    /// Loads built-in system types into the catalog.
    ///
    /// Built-in types sometimes have references to other built-in types, and sometimes these
    /// references are circular. This makes loading built-in types more complicated than other
    /// built-in objects, and requires us to make multiple passes over the types to correctly
    /// resolve all references.
    #[tracing::instrument(level = "info", skip_all)]
    async fn load_builtin_types(&mut self) -> Result<(), Error> {
        let persisted_builtin_ids = self.storage().await.load_system_gids().await?;

        let AllocatedBuiltinSystemIds {
            all_builtins,
            new_builtins,
            migrated_builtins,
        } = self
            .allocate_system_ids(BUILTINS::types().collect(), |typ| {
                persisted_builtin_ids
                    .get(&(
                        typ.schema.to_string(),
                        CatalogItemType::Type,
                        typ.name.to_string(),
                    ))
                    .cloned()
            })
            .await?;
        assert!(migrated_builtins.is_empty(), "types cannot be migrated");
        let name_to_id_map: BTreeMap<&str, GlobalId> = all_builtins
            .into_iter()
            .map(|(typ, id)| (typ.name, id))
            .collect();

        // Replace named references with id references
        let mut builtin_types: Vec<_> = BUILTINS::types()
            .map(|typ| Self::resolve_builtin_type(typ, &name_to_id_map))
            .collect();

        // Resolve array_id for types
        let mut element_id_to_array_id = BTreeMap::new();
        for typ in &builtin_types {
            match &typ.details.typ {
                CatalogType::Array { element_reference } => {
                    let array_id = name_to_id_map[typ.name];
                    element_id_to_array_id.insert(*element_reference, array_id);
                }
                _ => {}
            }
        }
        let pg_catalog_schema_id = self.state.get_pg_catalog_schema_id().clone();
        for typ in &mut builtin_types {
            let element_id = name_to_id_map[typ.name];
            typ.details.array_id = element_id_to_array_id.get(&element_id).map(|id| id.clone());
        }

        // Insert into catalog
        for typ in builtin_types {
            let element_id = name_to_id_map[typ.name];
            self.state.insert_item(
                element_id,
                typ.oid,
                QualifiedItemName {
                    qualifiers: ItemQualifiers {
                        database_spec: ResolvedDatabaseSpecifier::Ambient,
                        schema_spec: SchemaSpecifier::Id(pg_catalog_schema_id),
                    },
                    item: typ.name.to_owned(),
                },
                CatalogItem::Type(Type {
                    create_sql: format!("CREATE TYPE {}", typ.name),
                    details: typ.details.clone(),
                    depends_on: vec![],
                }),
                MZ_SYSTEM_ROLE_ID,
                vec![
                    rbac::default_catalog_privilege(mz_sql_parser::ast::ObjectType::Type),
                    rbac::owner_privilege(mz_sql_parser::ast::ObjectType::Type, MZ_SYSTEM_ROLE_ID),
                ],
            );
        }

        let new_system_id_mappings = new_builtins
            .iter()
            .map(|(typ, id)| SystemObjectMapping {
                schema_name: typ.schema.to_string(),
                object_type: CatalogItemType::Type,
                object_name: typ.name.to_string(),
                id: *id,
                fingerprint: typ.fingerprint(),
            })
            .collect();
        self.storage()
            .await
            .set_system_object_mapping(new_system_id_mappings)
            .await?;

        Ok(())
    }

    fn resolve_builtin_type(
        builtin: &BuiltinType<NameReference>,
        name_to_id_map: &BTreeMap<&str, GlobalId>,
    ) -> BuiltinType<IdReference> {
        let typ: CatalogType<IdReference> = match &builtin.details.typ {
            CatalogType::Array { element_reference } => CatalogType::Array {
                element_reference: name_to_id_map[element_reference],
            },
            CatalogType::List { element_reference } => CatalogType::List {
                element_reference: name_to_id_map[element_reference],
            },
            CatalogType::Map {
                key_reference,
                value_reference,
            } => CatalogType::Map {
                key_reference: name_to_id_map[key_reference],
                value_reference: name_to_id_map[value_reference],
            },
            CatalogType::Range { element_reference } => CatalogType::Range {
                element_reference: name_to_id_map[element_reference],
            },
            CatalogType::Record { fields } => CatalogType::Record {
                fields: fields
                    .into_iter()
                    .map(|(column_name, reference)| {
                        (column_name.clone(), name_to_id_map[reference])
                    })
                    .collect(),
            },
            CatalogType::Bool => CatalogType::Bool,
            CatalogType::Bytes => CatalogType::Bytes,
            CatalogType::Char => CatalogType::Char,
            CatalogType::Date => CatalogType::Date,
            CatalogType::Float32 => CatalogType::Float32,
            CatalogType::Float64 => CatalogType::Float64,
            CatalogType::Int16 => CatalogType::Int16,
            CatalogType::Int32 => CatalogType::Int32,
            CatalogType::Int64 => CatalogType::Int64,
            CatalogType::UInt16 => CatalogType::UInt16,
            CatalogType::UInt32 => CatalogType::UInt32,
            CatalogType::UInt64 => CatalogType::UInt64,
            CatalogType::MzTimestamp => CatalogType::MzTimestamp,
            CatalogType::Interval => CatalogType::Interval,
            CatalogType::Jsonb => CatalogType::Jsonb,
            CatalogType::Numeric => CatalogType::Numeric,
            CatalogType::Oid => CatalogType::Oid,
            CatalogType::PgLegacyChar => CatalogType::PgLegacyChar,
            CatalogType::Pseudo => CatalogType::Pseudo,
            CatalogType::RegClass => CatalogType::RegClass,
            CatalogType::RegProc => CatalogType::RegProc,
            CatalogType::RegType => CatalogType::RegType,
            CatalogType::String => CatalogType::String,
            CatalogType::Time => CatalogType::Time,
            CatalogType::Timestamp => CatalogType::Timestamp,
            CatalogType::TimestampTz => CatalogType::TimestampTz,
            CatalogType::Uuid => CatalogType::Uuid,
            CatalogType::VarChar => CatalogType::VarChar,
            CatalogType::Int2Vector => CatalogType::Int2Vector,
            CatalogType::MzAclItem => CatalogType::MzAclItem,
        };

        BuiltinType {
            name: builtin.name,
            schema: builtin.schema,
            oid: builtin.oid,
            details: CatalogTypeDetails {
                array_id: builtin.details.array_id,
                typ,
            },
        }
    }

    /// The objects in the catalog form one or more DAGs (directed acyclic graph) via object
    /// dependencies. To migrate a builtin object we must drop that object along with all of its
    /// descendants, and then recreate that object along with all of its descendants using new
    /// GlobalId`s. To achieve this we perform a DFS (depth first search) on the catalog items
    /// starting with the nodes that correspond to builtin objects that have changed schemas.
    ///
    /// Objects need to be dropped starting from the leafs of the DAG going up towards the roots,
    /// and they need to be recreated starting at the roots of the DAG and going towards the leafs.
    pub async fn generate_builtin_migration_metadata(
        &self,
        migrated_ids: Vec<GlobalId>,
        id_fingerprint_map: BTreeMap<GlobalId, String>,
    ) -> Result<BuiltinMigrationMetadata, Error> {
        // First obtain a topological sorting of all migrated objects and their children.
        let mut visited_set = BTreeSet::new();
        let mut topological_sort = Vec::new();
        for id in migrated_ids {
            if !visited_set.contains(&id) {
                let migrated_topological_sort = self.topological_sort(id, &mut visited_set);
                topological_sort.extend(migrated_topological_sort);
            }
        }
        topological_sort.reverse();

        // Then process all objects in sorted order.
        let mut migration_metadata = BuiltinMigrationMetadata::new();
        let mut ancestor_ids = BTreeMap::new();
        let mut migrated_log_ids = BTreeMap::new();
        let log_name_map: BTreeMap<_, _> = BUILTINS::logs()
            .map(|log| (log.variant.clone(), log.name))
            .collect();
        for entry in topological_sort {
            let id = entry.id();

            let new_id = match id {
                GlobalId::System(_) => self
                    .storage()
                    .await
                    .allocate_system_ids(1)
                    .await?
                    .into_element(),
                GlobalId::User(_) => self.storage().await.allocate_user_id().await?,
                _ => unreachable!("can't migrate id: {id}"),
            };

            let name = self.resolve_full_name(entry.name(), None);
            info!("migrating {name} from {id} to {new_id}");

            // Generate value to update fingerprint and global ID persisted mapping for system objects.
            // Not every system object has a fingerprint, like introspection source indexes.
            if let Some(fingerprint) = id_fingerprint_map.get(&id) {
                assert!(
                    id.is_system(),
                    "id_fingerprint_map should only contain builtin objects"
                );
                let schema_name = self
                    .get_schema(
                        &entry.name.qualifiers.database_spec,
                        &entry.name.qualifiers.schema_spec,
                        entry.conn_id().unwrap_or(SYSTEM_CONN_ID),
                    )
                    .name
                    .schema
                    .as_str();
                migration_metadata.migrated_system_object_mappings.insert(
                    id,
                    SystemObjectMapping {
                        schema_name: schema_name.to_string(),
                        object_type: entry.item_type(),
                        object_name: entry.name.item.clone(),
                        id: new_id,
                        fingerprint: fingerprint.clone(),
                    },
                );
            }

            ancestor_ids.insert(id, new_id);

            // Push drop commands.
            match entry.item() {
                CatalogItem::Table(_) | CatalogItem::Source(_) => {
                    migration_metadata.previous_source_ids.push(id)
                }
                CatalogItem::Sink(_) => migration_metadata.previous_sink_ids.push(id),
                CatalogItem::MaterializedView(_) => {
                    migration_metadata.previous_materialized_view_ids.push(id)
                }
                CatalogItem::Log(log) => {
                    migrated_log_ids.insert(id, log.variant.clone());
                }
                CatalogItem::Index(index) => {
                    if id.is_system() {
                        if let Some(variant) = migrated_log_ids.get(&index.on) {
                            migration_metadata
                                .introspection_source_index_updates
                                .entry(index.cluster_id)
                                .or_default()
                                .push((
                                    variant.clone(),
                                    log_name_map
                                        .get(variant)
                                        .expect("all variants have a name")
                                        .to_string(),
                                    new_id,
                                ));
                        }
                    }
                }
                CatalogItem::View(_) => {
                    // Views don't have any external objects to drop.
                }
                CatalogItem::Type(_)
                | CatalogItem::Func(_)
                | CatalogItem::Secret(_)
                | CatalogItem::Connection(_) => unreachable!(
                    "impossible to migrate schema for builtin {}",
                    entry.item().typ()
                ),
            }
            if id.is_user() {
                migration_metadata.user_drop_ops.push(id);
            }
            migration_metadata.all_drop_ops.push(id);

            // Push create commands.
            let name = entry.name.clone();
            if id.is_user() {
                let schema_id = name.qualifiers.schema_spec.clone().into();
                migration_metadata
                    .user_create_ops
                    .push((new_id, schema_id, name.item.clone()));
            }
            let item_rebuilder = CatalogItemRebuilder::new(entry, new_id, &ancestor_ids);
            migration_metadata.all_create_ops.push((
                new_id,
                entry.oid,
                name,
                entry.owner_id,
                entry.privileges.clone(),
                item_rebuilder,
            ));
        }

        // Reverse drop commands.
        migration_metadata.previous_sink_ids.reverse();
        migration_metadata.previous_materialized_view_ids.reverse();
        migration_metadata.previous_source_ids.reverse();
        migration_metadata.all_drop_ops.reverse();
        migration_metadata.user_drop_ops.reverse();

        Ok(migration_metadata)
    }

    fn topological_sort(
        &self,
        id: GlobalId,
        visited_set: &mut BTreeSet<GlobalId>,
    ) -> Vec<&CatalogEntry> {
        let mut topological_sort = Vec::new();
        visited_set.insert(id);
        let entry = self.get_entry(&id);
        for dependant in &entry.used_by {
            if !visited_set.contains(dependant) {
                let child_topological_sort = self.topological_sort(*dependant, visited_set);
                topological_sort.extend(child_topological_sort);
            }
        }
        topological_sort.push(entry);
        topological_sort
    }

    pub fn apply_in_memory_builtin_migration(
        &mut self,
        migration_metadata: &mut BuiltinMigrationMetadata,
    ) -> Result<(), Error> {
        assert_eq!(
            migration_metadata.all_drop_ops.len(),
            migration_metadata.all_create_ops.len(),
            "we should be re-creating every dropped object"
        );
        for id in migration_metadata.all_drop_ops.drain(..) {
            self.state.drop_item(id);
        }
        for (id, oid, name, owner_id, privileges, item_rebuilder) in
            migration_metadata.all_create_ops.drain(..)
        {
            let item = item_rebuilder.build(self);
            self.state
                .insert_item(id, oid, name, item, owner_id, privileges);
        }
        for (cluster_id, updates) in &migration_metadata.introspection_source_index_updates {
            let log_indexes = &mut self
                .state
                .clusters_by_id
                .get_mut(cluster_id)
                .unwrap_or_else(|| panic!("invalid cluster {cluster_id}"))
                .log_indexes;
            for (variant, _name, new_id) in updates {
                log_indexes.remove(variant);
                log_indexes.insert(variant.clone(), new_id.clone());
            }
        }

        Ok(())
    }

    #[tracing::instrument(level = "info", skip_all)]
    pub async fn apply_persisted_builtin_migration(
        &self,
        migration_metadata: &mut BuiltinMigrationMetadata,
    ) -> Result<(), Error> {
        let mut storage = self.storage().await;
        let mut tx = storage.transaction().await?;
        tx.remove_items(migration_metadata.user_drop_ops.drain(..).collect())?;
        for (id, schema_id, name) in migration_metadata.user_create_ops.drain(..) {
            let entry = self.get_entry(&id);
            let item = entry.item();
            let serialized_item = Self::serialize_item(item);
            tx.insert_item(
                id,
                schema_id,
                &name,
                serialized_item,
                entry.owner_id,
                entry.privileges.clone(),
            )?;
        }
        tx.update_system_object_mappings(std::mem::take(
            &mut migration_metadata.migrated_system_object_mappings,
        ))?;
        tx.update_introspection_source_index_gids(
            std::mem::take(&mut migration_metadata.introspection_source_index_updates)
                .into_iter()
                .map(|(cluster_id, updates)| {
                    (
                        cluster_id,
                        updates
                            .into_iter()
                            .map(|(_variant, name, index_id)| (name, index_id)),
                    )
                }),
        )?;

        tx.commit().await?;

        Ok(())
    }

    /// Returns the catalog's transient revision, which starts at 1 and is
    /// incremented on every change. This is not persisted to disk, and will
    /// restart on every load.
    pub fn transient_revision(&self) -> u64 {
        self.transient_revision
    }

    /// Takes a catalog which only has items in its on-disk storage ("unloaded")
    /// and cannot yet resolve names, and returns a catalog loaded with those
    /// items.
    ///
    /// This function requires transactions to support loading a catalog with
    /// the transaction's currently in-flight updates to existing catalog
    /// objects, which is necessary for at least one catalog migration.
    ///
    /// TODO(justin): it might be nice if these were two different types.
    #[tracing::instrument(level = "info", skip_all)]
    pub fn load_catalog_items<'a>(
        tx: &mut storage::Transaction<'a>,
        c: &Catalog,
    ) -> Result<Catalog, Error> {
        let mut c = c.clone();
        let mut awaiting_id_dependencies: BTreeMap<GlobalId, Vec<_>> = BTreeMap::new();
        let mut awaiting_name_dependencies: BTreeMap<String, Vec<_>> = BTreeMap::new();
        let mut items: VecDeque<_> = tx.loaded_items().into_iter().collect();
        while let Some(item) = items.pop_front() {
            let d_c = item.definition.clone();
            // TODO(benesch): a better way of detecting when a view has depended
            // upon a non-existent logging view. This is fine for now because
            // the only goal is to produce a nicer error message; we'll bail out
            // safely even if the error message we're sniffing out changes.
            static LOGGING_ERROR: Lazy<Regex> =
                Lazy::new(|| Regex::new("mz_catalog.[^']*").expect("valid regex"));

            let catalog_item = match c.deserialize_item(item.id, d_c) {
                Ok(item) => item,
                Err(AdapterError::SqlCatalog(SqlCatalogError::UnknownItem(name)))
                    if LOGGING_ERROR.is_match(&name.to_string()) =>
                {
                    return Err(Error::new(ErrorKind::UnsatisfiableLoggingDependency {
                        depender_name: name,
                    }));
                }
                // If we were missing a dependency, wait for it to be added.
                Err(AdapterError::PlanError(plan::PlanError::InvalidId(missing_dep))) => {
                    awaiting_id_dependencies
                        .entry(missing_dep)
                        .or_default()
                        .push(item);
                    continue;
                }
                // If we were missing a dependency, wait for it to be added.
                Err(AdapterError::PlanError(plan::PlanError::Catalog(
                    SqlCatalogError::UnknownItem(missing_dep),
                ))) => {
                    match GlobalId::from_str(&missing_dep) {
                        Ok(id) => {
                            awaiting_id_dependencies.entry(id).or_default().push(item);
                        }
                        Err(_) => {
                            awaiting_name_dependencies
                                .entry(missing_dep)
                                .or_default()
                                .push(item);
                        }
                    }
                    continue;
                }
                Err(e) => {
                    return Err(Error::new(ErrorKind::Corruption {
                        detail: format!(
                            "failed to deserialize item {} ({}): {}",
                            item.id, item.name, e
                        ),
                    }))
                }
            };
            let oid = c.allocate_oid()?;

            // Enqueue any items waiting on this dependency.
            if let Some(dependent_items) = awaiting_id_dependencies.remove(&item.id) {
                items.extend(dependent_items);
            }
            let full_name = c.resolve_full_name(&item.name, None);
            if let Some(dependent_items) = awaiting_name_dependencies.remove(&full_name.to_string())
            {
                items.extend(dependent_items);
            }

            c.state.insert_item(
                item.id,
                oid,
                item.name,
                catalog_item,
                item.owner_id,
                item.privileges,
            );
        }

        // Error on any unsatisfied dependencies.
        if let Some((missing_dep, mut dependents)) = awaiting_id_dependencies.into_iter().next() {
            let storage::Item {
                id,
                name,
                definition: _,
                owner_id: _,
                privileges: _,
            } = dependents.remove(0);
            return Err(Error::new(ErrorKind::Corruption {
                detail: format!(
                    "failed to deserialize item {} ({}): {}",
                    id,
                    name,
                    AdapterError::PlanError(plan::PlanError::InvalidId(missing_dep))
                ),
            }));
        }

        if let Some((missing_dep, mut dependents)) = awaiting_name_dependencies.into_iter().next() {
            let storage::Item {
                id,
                name,
                definition: _,
                owner_id: _,
                privileges: _,
            } = dependents.remove(0);
            return Err(Error::new(ErrorKind::Corruption {
                detail: format!(
                    "failed to deserialize item {} ({}): {}",
                    id,
                    name,
                    AdapterError::SqlCatalog(SqlCatalogError::UnknownItem(missing_dep))
                ),
            }));
        }

        c.transient_revision = 1;
        Ok(c)
    }

    /// Creates a debug catalog from a debug stash based on the current
    /// `COCKROACH_URL` with parameters set appropriately for debug contexts,
    /// like in tests.
    ///
    /// WARNING! This function can arbitrarily fail because it does not make any
    /// effort to adjust the catalog's contents' structure or semantics to the
    /// currently running version, i.e. it does not apply any migrations.
    ///
    /// This function must not be called in production contexts. Use
    /// [`Catalog::open`] with appropriately set configuration parameters
    /// instead.
    pub async fn with_debug<F, Fut, T>(now: NowFn, f: F) -> T
    where
        F: FnOnce(Catalog) -> Fut,
        Fut: Future<Output = T>,
    {
        Stash::with_debug_stash(move |stash| async move {
            let catalog = Self::open_debug_stash(stash, now)
                .await
                .expect("unable to open debug stash");
            f(catalog).await
        })
        .await
        .expect("unable to open debug stash")
    }

    /// Opens a debug postgres catalog at `url`.
    ///
    /// If specified, `schema` will set the connection's `search_path` to `schema`.
    ///
    /// See [`Catalog::with_debug`].
    pub async fn open_debug_postgres(
        url: String,
        schema: Option<String>,
        now: NowFn,
    ) -> Result<Catalog, anyhow::Error> {
        let tls = mz_postgres_util::make_tls(&tokio_postgres::Config::new())
            .expect("unable to create TLS connector");
        let factory = StashFactory::new(&MetricsRegistry::new());
        let stash = factory.open(url, schema, tls).await?;
        Self::open_debug_stash(stash, now).await
    }

    /// Opens a debug catalog from a stash.
    pub async fn open_debug_stash(stash: Stash, now: NowFn) -> Result<Catalog, anyhow::Error> {
        let metrics_registry = &MetricsRegistry::new();
        let storage = storage::Connection::open(
            stash,
            now.clone(),
            &BootstrapArgs {
                default_cluster_replica_size: "1".into(),
                builtin_cluster_replica_size: "1".into(),
                default_availability_zone: DUMMY_AVAILABILITY_ZONE.into(),
            },
        )
        .await?;
        let secrets_reader = Arc::new(InMemorySecretsController::new());
        let (catalog, _, _, _) = Catalog::open(Config {
            storage,
            unsafe_mode: true,
            build_info: &DUMMY_BUILD_INFO,
            environment_id: EnvironmentId::for_tests(),
            now,
            skip_migrations: true,
            metrics_registry,
            cluster_replica_sizes: Default::default(),
            default_storage_cluster_size: None,
            bootstrap_system_parameters: Default::default(),
            availability_zones: vec![],
            secrets_reader,
            egress_ips: vec![],
            aws_principal_context: None,
            aws_privatelink_availability_zones: None,
            system_parameter_frontend: None,
            // when debugging, no reaping
            storage_usage_retention_period: None,
            connection_context: None,
        })
        .await?;
        Ok(catalog)
    }

    pub fn for_session<'a>(&'a self, session: &'a Session) -> ConnCatalog<'a> {
        Self::for_session_state(&self.state, session)
    }

    pub fn for_session_state<'a>(state: &'a CatalogState, session: &'a Session) -> ConnCatalog<'a> {
        let database = state
            .database_by_name
            .get(session.vars().database())
            .map(|id| id.clone());
        let search_path = session
            .vars()
            .search_path()
            .iter()
            .map(|schema| {
                state.resolve_schema(database.as_ref(), None, schema.as_str(), session.conn_id())
            })
            .filter_map(|schema| schema.ok())
            .map(|schema| (schema.name().database.clone(), schema.id().clone()))
            .collect();
        ConnCatalog {
            state: Cow::Borrowed(state),
            conn_id: session.conn_id(),
            cluster: session.vars().cluster().into(),
            database,
            search_path,
            role_id: session.role_id().clone(),
            prepared_statements: Some(Cow::Borrowed(session.prepared_statements())),
        }
    }

    pub fn for_sessionless_user(&self, role_id: RoleId) -> ConnCatalog {
        ConnCatalog {
            state: Cow::Borrowed(&self.state),
            conn_id: SYSTEM_CONN_ID,
            cluster: "default".into(),
            database: self
                .resolve_database(DEFAULT_DATABASE_NAME)
                .ok()
                .map(|db| db.id()),
            search_path: Vec::new(),
            role_id,
            prepared_statements: None,
        }
    }

    // Leaving the system's search path empty allows us to catch issues
    // where catalog object names have not been normalized correctly.
    pub fn for_system_session(&self) -> ConnCatalog {
        self.for_sessionless_user(MZ_SYSTEM_ROLE_ID)
    }

    async fn storage<'a>(&'a self) -> MutexGuard<'a, storage::Connection> {
        self.storage.lock().await
    }

    /// Allocate new system ids for any new builtin objects and looks up existing system ids for
    /// existing builtin objects
    async fn allocate_system_ids<T, F>(
        &self,
        builtins: Vec<T>,
        builtin_lookup: F,
    ) -> Result<AllocatedBuiltinSystemIds<T>, Error>
    where
        T: Copy + Fingerprint,
        F: Fn(&T) -> Option<(GlobalId, String)>,
    {
        let new_builtin_amount = builtins
            .iter()
            .filter(|builtin| builtin_lookup(builtin).is_none())
            .count();

        let mut global_ids = self
            .storage()
            .await
            .allocate_system_ids(
                new_builtin_amount
                    .try_into()
                    .expect("builtins should fit into u64"),
            )
            .await?
            .into_iter();

        let mut all_builtins = Vec::new();
        let mut new_builtins = Vec::new();
        let mut migrated_builtins = Vec::new();
        for builtin in &builtins {
            match builtin_lookup(builtin) {
                Some((id, old_fingerprint)) => {
                    all_builtins.push((*builtin, id));
                    let new_fingerprint = builtin.fingerprint();
                    if old_fingerprint != new_fingerprint {
                        migrated_builtins.push(id);
                    }
                }
                None => {
                    let id = global_ids.next().expect("not enough global IDs");
                    all_builtins.push((*builtin, id));
                    new_builtins.push((*builtin, id));
                }
            }
        }

        Ok(AllocatedBuiltinSystemIds {
            all_builtins,
            new_builtins,
            migrated_builtins,
        })
    }

    pub async fn allocate_user_id(&self) -> Result<GlobalId, Error> {
        self.storage().await.allocate_user_id().await
    }

    #[cfg(test)]
    pub async fn allocate_system_id(&self) -> Result<GlobalId, Error> {
        self.storage()
            .await
            .allocate_system_ids(1)
            .await
            .map(|ids| ids.into_element())
    }

    pub async fn allocate_user_cluster_id(&self) -> Result<ClusterId, Error> {
        self.storage().await.allocate_user_cluster_id().await
    }

    pub async fn allocate_replica_id(&self) -> Result<ReplicaId, Error> {
        self.storage().await.allocate_replica_id().await
    }

    pub fn allocate_oid(&mut self) -> Result<u32, Error> {
        self.state.allocate_oid()
    }

    /// Get all global timestamps that has been persisted to disk.
    pub async fn get_all_persisted_timestamps(
        &self,
    ) -> Result<BTreeMap<Timeline, mz_repr::Timestamp>, Error> {
        self.storage().await.get_all_persisted_timestamps().await
    }

    /// Get the next user ID without allocating it.
    pub async fn get_next_user_global_id(&self) -> Result<GlobalId, Error> {
        self.storage().await.get_next_user_global_id().await
    }

    /// Get the next replica id without allocating it.
    pub async fn get_next_replica_id(&self) -> Result<ReplicaId, Error> {
        self.storage().await.get_next_replica_id().await
    }

    /// Persist new global timestamp for a timeline to disk.
    pub async fn persist_timestamp(
        &self,
        timeline: &Timeline,
        timestamp: mz_repr::Timestamp,
    ) -> Result<(), Error> {
        self.storage()
            .await
            .persist_timestamp(timeline, timestamp)
            .await
    }

    pub fn resolve_database(&self, database_name: &str) -> Result<&Database, SqlCatalogError> {
        self.state.resolve_database(database_name)
    }

    pub fn resolve_schema(
        &self,
        current_database: Option<&DatabaseId>,
        database_name: Option<&str>,
        schema_name: &str,
        conn_id: ConnectionId,
    ) -> Result<&Schema, SqlCatalogError> {
        self.state
            .resolve_schema(current_database, database_name, schema_name, conn_id)
    }

    pub fn resolve_schema_in_database(
        &self,
        database_spec: &ResolvedDatabaseSpecifier,
        schema_name: &str,
        conn_id: ConnectionId,
    ) -> Result<&Schema, SqlCatalogError> {
        self.state
            .resolve_schema_in_database(database_spec, schema_name, conn_id)
    }

    /// Resolves `name` to a non-function [`CatalogEntry`].
    pub fn resolve_entry(
        &self,
        current_database: Option<&DatabaseId>,
        search_path: &Vec<(ResolvedDatabaseSpecifier, SchemaSpecifier)>,
        name: &PartialItemName,
        conn_id: ConnectionId,
    ) -> Result<&CatalogEntry, SqlCatalogError> {
        self.state
            .resolve_entry(current_database, search_path, name, conn_id)
    }

    /// Resolves a `BuiltinTable`.
    pub fn resolve_builtin_table(&self, builtin: &'static BuiltinTable) -> GlobalId {
        self.state.resolve_builtin_table(builtin)
    }

    /// Resolves a `BuiltinLog`.
    pub fn resolve_builtin_log(&self, builtin: &'static BuiltinLog) -> GlobalId {
        self.state.resolve_builtin_log(builtin)
    }

    /// Resolves a `BuiltinSource`.
    pub fn resolve_builtin_storage_collection(&self, builtin: &'static BuiltinSource) -> GlobalId {
        self.state.resolve_builtin_source(builtin)
    }

    /// Resolves `name` to a function [`CatalogEntry`].
    pub fn resolve_function(
        &self,
        current_database: Option<&DatabaseId>,
        search_path: &Vec<(ResolvedDatabaseSpecifier, SchemaSpecifier)>,
        name: &PartialItemName,
        conn_id: ConnectionId,
    ) -> Result<&CatalogEntry, SqlCatalogError> {
        self.state
            .resolve_function(current_database, search_path, name, conn_id)
    }

    pub fn resolve_cluster(&self, name: &str) -> Result<&Cluster, SqlCatalogError> {
        self.state.resolve_cluster(name)
    }

    /// Resolves a [`Cluster`] for a [`BuiltinCluster`]
    ///
    /// # Panics
    /// * If the [`BuiltinCluster`] doesn't exist.
    ///
    pub fn resolve_builtin_cluster(&self, cluster: &BuiltinCluster) -> &Cluster {
        self.state.resolve_builtin_cluster(cluster)
    }

    pub fn active_cluster(&self, session: &Session) -> Result<&Cluster, AdapterError> {
        // TODO(benesch): this check here is not sufficiently protective. It'd
        // be very easy for a code path to accidentally avoid this check by
        // calling `resolve_cluster(session.vars().cluster())`.
        if session.user().name != SYSTEM_USER.name
            && session.user().name != INTROSPECTION_USER.name
            && session.vars().cluster() == SYSTEM_USER.name
        {
            coord_bail!(
                "system cluster '{}' cannot execute user queries",
                SYSTEM_USER.name
            );
        }
        let cluster = self.resolve_cluster(session.vars().cluster())?;
        // Disallow queries on storage clusters. There's no technical reason for
        // this restriction, just a philosophical one: we want all crashes in
        // a storage cluster to be the result of sources and sinks, not user
        // queries.
        if cluster.bound_objects.iter().any(|id| {
            matches!(
                self.get_entry(id).item_type(),
                CatalogItemType::Source | CatalogItemType::Sink
            )
        }) {
            coord_bail!("cannot execute queries on cluster containing sources or sinks");
        }
        Ok(cluster)
    }

    pub fn state(&self) -> &CatalogState {
        &self.state
    }

    pub fn resolve_full_name(
        &self,
        name: &QualifiedItemName,
        conn_id: Option<ConnectionId>,
    ) -> FullItemName {
        self.state.resolve_full_name(name, conn_id)
    }

    /// Returns the named catalog item, if it exists.
    pub fn try_get_entry_in_schema(
        &self,
        name: &QualifiedItemName,
        conn_id: ConnectionId,
    ) -> Option<&CatalogEntry> {
        self.state.try_get_entry_in_schema(name, conn_id)
    }

    pub fn item_exists(&self, name: &QualifiedItemName, conn_id: ConnectionId) -> bool {
        self.state.item_exists(name, conn_id)
    }

    pub fn try_get_entry(&self, id: &GlobalId) -> Option<&CatalogEntry> {
        self.state.try_get_entry(id)
    }

    pub fn get_entry(&self, id: &GlobalId) -> &CatalogEntry {
        self.state.get_entry(id)
    }

    pub fn get_schema(
        &self,
        database_spec: &ResolvedDatabaseSpecifier,
        schema_spec: &SchemaSpecifier,
        conn_id: ConnectionId,
    ) -> &Schema {
        self.state.get_schema(database_spec, schema_spec, conn_id)
    }

    pub fn get_mz_catalog_schema_id(&self) -> &SchemaId {
        self.state.get_mz_catalog_schema_id()
    }

    pub fn get_pg_catalog_schema_id(&self) -> &SchemaId {
        self.state.get_pg_catalog_schema_id()
    }

    pub fn get_information_schema_id(&self) -> &SchemaId {
        self.state.get_information_schema_id()
    }

    pub fn get_mz_internal_schema_id(&self) -> &SchemaId {
        self.state.get_mz_internal_schema_id()
    }

    pub fn get_database(&self, id: &DatabaseId) -> &Database {
        self.state.get_database(id)
    }

    pub fn get_role(&self, id: &RoleId) -> &Role {
        self.state.get_role(id)
    }

    pub fn try_get_role_by_name(&self, role_name: &str) -> Option<&Role> {
        self.state.try_get_role_by_name(role_name)
    }

    /// Creates a new schema in the `Catalog` for temporary items
    /// indicated by the TEMPORARY or TEMP keywords.
    pub fn create_temporary_schema(
        &mut self,
        conn_id: ConnectionId,
        owner_id: RoleId,
    ) -> Result<(), Error> {
        let oid = self.allocate_oid()?;
        self.state.temporary_schemas.insert(
            conn_id,
            Schema {
                name: QualifiedSchemaName {
                    database: ResolvedDatabaseSpecifier::Ambient,
                    schema: MZ_TEMP_SCHEMA.into(),
                },
                id: SchemaSpecifier::Temporary,
                oid,
                items: BTreeMap::new(),
                functions: BTreeMap::new(),
                owner_id,
                privileges: vec![rbac::owner_privilege(
                    mz_sql_parser::ast::ObjectType::Schema,
                    owner_id,
                )],
            },
        );
        Ok(())
    }

    fn item_exists_in_temp_schemas(&self, conn_id: ConnectionId, item_name: &str) -> bool {
        self.state.temporary_schemas[&conn_id]
            .items
            .contains_key(item_name)
    }

    pub fn drop_temp_item_ops(&mut self, conn_id: &ConnectionId) -> Vec<Op> {
        let temp_ids = self.state.temporary_schemas[conn_id]
            .items
            .values()
            .cloned()
            .map(ObjectId::Item)
            .collect();
        self.object_dependents(temp_ids)
            .into_iter()
            .map(Op::DropObject)
            .collect()
    }
    pub fn drop_temporary_schema(&mut self, conn_id: &ConnectionId) -> Result<(), Error> {
        if !self.state.temporary_schemas[conn_id].items.is_empty() {
            return Err(Error::new(ErrorKind::SchemaNotEmpty(MZ_TEMP_SCHEMA.into())));
        }
        self.state.temporary_schemas.remove(conn_id);
        Ok(())
    }

    pub(crate) fn object_dependents(&self, object_ids: Vec<ObjectId>) -> Vec<ObjectId> {
        let mut seen = BTreeSet::new();
        self.state.object_dependents(object_ids, &mut seen)
    }

    pub(crate) fn cluster_replica_dependents(
        &self,
        cluster_id: ClusterId,
        replica_id: ReplicaId,
    ) -> Vec<ObjectId> {
        let mut seen = BTreeSet::new();
        self.state
            .cluster_replica_dependents(cluster_id, replica_id, &mut seen)
    }

    pub(crate) fn item_dependents(&self, item_id: GlobalId) -> Vec<ObjectId> {
        let mut seen = BTreeSet::new();
        self.state.item_dependents(item_id, &mut seen)
    }

    /// Gets GlobalIds of temporary items to be created, checks for name collisions
    /// within a connection id.
    fn temporary_ids(
        &self,
        ops: &[Op],
        temporary_drops: BTreeSet<(ConnectionId, String)>,
    ) -> Result<Vec<GlobalId>, Error> {
        let mut creating = BTreeSet::new();
        let mut temporary_ids = Vec::with_capacity(ops.len());
        for op in ops.iter() {
            if let Op::CreateItem {
                id,
                oid: _,
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
                        return Err(Error::new(ErrorKind::ItemAlreadyExists(
                            *id,
                            name.item.clone(),
                        )));
                    } else {
                        creating.insert((conn_id, &name.item));
                        temporary_ids.push(id.clone());
                    }
                }
            }
        }
        Ok(temporary_ids)
    }

    fn should_audit_log_item(item: &CatalogItem) -> bool {
        !item.is_temporary()
    }

    fn full_name_detail(name: &FullItemName) -> FullNameV1 {
        FullNameV1 {
            database: name.database.to_string(),
            schema: name.schema.clone(),
            item: name.item.clone(),
        }
    }

    pub fn find_available_cluster_name(&self, name: &str) -> String {
        let mut i = 0;
        let mut candidate = name.to_string();
        while self.state.clusters_by_name.contains_key(&candidate) {
            i += 1;
            candidate = format!("{}{}", name, i);
        }
        candidate
    }

    /// Gets the linked cluster associated with the provided object ID, if it
    /// exists.
    pub fn get_linked_cluster(&self, object_id: GlobalId) -> Option<&Cluster> {
        self.state.get_linked_cluster(object_id)
    }

    /// Gets the size associated with the provided source or sink ID, if a
    /// linked cluster exists.
    pub fn get_storage_object_size(&self, object_id: GlobalId) -> Option<&str> {
        self.state.get_storage_object_size(object_id)
    }

    pub fn concretize_replica_location(
        &self,
        location: SerializedReplicaLocation,
        allowed_sizes: &Vec<String>,
    ) -> Result<ReplicaLocation, AdapterError> {
        let location = match location {
            SerializedReplicaLocation::Unmanaged {
                storagectl_addrs,
                storage_addrs,
                computectl_addrs,
                compute_addrs,
                workers,
            } => ReplicaLocation::Unmanaged(UnmanagedReplicaLocation {
                storagectl_addrs,
                storage_addrs,
                computectl_addrs,
                compute_addrs,
                workers,
            }),
            SerializedReplicaLocation::Managed {
                size,
                availability_zone,
                az_user_specified,
            } => {
                let cluster_replica_sizes = &self.state.cluster_replica_sizes;

                if !cluster_replica_sizes.0.contains_key(&size)
                    || (!allowed_sizes.is_empty() && !allowed_sizes.contains(&size))
                {
                    let mut entries = cluster_replica_sizes.0.iter().collect::<Vec<_>>();

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

                    return Err(AdapterError::InvalidClusterReplicaSize {
                        size,
                        expected: entries.into_iter().map(|(name, _)| name.clone()).collect(),
                    });
                }

                ReplicaLocation::Managed(ManagedReplicaLocation {
                    allocation: cluster_replica_sizes
                        .0
                        .get(&size)
                        .expect("catalog out of sync")
                        .clone(),
                    availability_zone,
                    size,
                    az_user_specified,
                })
            }
        };
        Ok(location)
    }

    pub fn cluster_replica_sizes(&self) -> &ClusterReplicaSizeMap {
        &self.state.cluster_replica_sizes
    }

    /// Returns the default size to use for linked clusters.
    pub fn default_linked_cluster_size(&self) -> String {
        self.state.default_linked_cluster_size()
    }

    #[tracing::instrument(name = "catalog::transact", level = "debug", skip_all)]
    pub async fn transact<F, R>(
        &mut self,
        oracle_write_ts: mz_repr::Timestamp,
        session: Option<&Session>,
        ops: Vec<Op>,
        f: F,
    ) -> Result<TransactionResult<R>, AdapterError>
    where
        F: FnOnce(&CatalogState) -> Result<R, AdapterError>,
    {
        trace!("transact: {:?}", ops);
        fail::fail_point!("catalog_transact", |arg| {
            Err(AdapterError::Unstructured(anyhow::anyhow!(
                "failpoint: {arg:?}"
            )))
        });

        let drop_ids: BTreeSet<_> = ops
            .iter()
            .filter_map(|op| match op {
                Op::DropObject(ObjectId::Item(id)) => Some(*id),
                _ => None,
            })
            .collect();
        let temporary_drops = drop_ids
            .iter()
            .filter_map(|id| {
                let entry = self.get_entry(id);
                match entry.item.conn_id() {
                    Some(conn_id) => Some((conn_id, entry.name().item.clone())),
                    None => None,
                }
            })
            .collect();
        let temporary_ids = self.temporary_ids(&ops, temporary_drops)?;
        let mut builtin_table_updates = vec![];
        let mut audit_events = vec![];
        let mut storage = self.storage().await;
        let mut tx = storage.transaction().await?;
        // Prepare a candidate catalog state.
        let mut state = self.state.clone();

        Self::transact_inner(
            oracle_write_ts,
            session,
            ops,
            temporary_ids,
            &mut builtin_table_updates,
            &mut audit_events,
            &mut tx,
            &mut state,
        )?;

        let result = f(&state)?;

        // The user closure was successful, apply the updates. Terminate the
        // process if this fails, because we have to restart envd due to
        // indeterminate stash state, which we only reconcile during catalog
        // init.
        tx.commit()
            .await
            .unwrap_or_terminate("catalog storage transaction commit must succeed");

        // Dropping here keeps the mutable borrow on self, preventing us accidentally
        // mutating anything until after f is executed.
        drop(storage);
        self.state = state;
        self.transient_revision += 1;

        Ok(TransactionResult {
            builtin_table_updates,
            audit_events,
            result,
        })
    }

    #[tracing::instrument(name = "catalog::transact_inner", level = "debug", skip_all)]
    fn transact_inner(
        oracle_write_ts: mz_repr::Timestamp,
        session: Option<&Session>,
        ops: Vec<Op>,
        temporary_ids: Vec<GlobalId>,
        builtin_table_updates: &mut Vec<BuiltinTableUpdate>,
        audit_events: &mut Vec<VersionedEvent>,
        tx: &mut Transaction<'_>,
        state: &mut CatalogState,
    ) -> Result<(), AdapterError> {
        fn sql_type_to_object_type(sql_type: SqlCatalogItemType) -> ObjectType {
            match sql_type {
                SqlCatalogItemType::Connection => ObjectType::Connection,
                SqlCatalogItemType::Func => ObjectType::Func,
                SqlCatalogItemType::Index => ObjectType::Index,
                SqlCatalogItemType::MaterializedView => ObjectType::MaterializedView,
                SqlCatalogItemType::Secret => ObjectType::Secret,
                SqlCatalogItemType::Sink => ObjectType::Sink,
                SqlCatalogItemType::Source => ObjectType::Source,
                SqlCatalogItemType::Table => ObjectType::Table,
                SqlCatalogItemType::Type => ObjectType::Type,
                SqlCatalogItemType::View => ObjectType::View,
            }
        }

        // NOTE(benesch): to support altering legacy sized sources and sinks
        // (those with linked clusters), we need to generate retractions for
        // `mz_sources` and `mz_sinks` in a separate pass over the operations.
        // The reason is that the alteration is split over several operations:
        // dropping the linked cluster, recreating it, and then altering the
        // source or sink. By the time we get to altering the source or sink,
        // we've already recreated the linked cluster at the new size, and can
        // no longer determine the old size of the cluster.
        //
        // This is a bit tangled, and this code is ugly and only works for
        // transactions that don't alter the same source or sink more than once,
        // but it doesn't seem worth refactoring since all this code will be
        // removed once cluster unification is complete.
        let mut old_source_sink_sizes = BTreeMap::new();
        for op in &ops {
            if let Op::AlterSource { id, .. } | Op::AlterSink { id, .. } = op {
                builtin_table_updates.extend(state.pack_item_update(*id, -1));
                let existing = old_source_sink_sizes.insert(
                    *id,
                    state.get_storage_object_size(*id).map(|s| s.to_string()),
                );
                if existing.is_some() {
                    coord_bail!("internal error: attempted to alter same source/sink twice in same transaction (id {id})");
                }
            }
        }

        for op in ops {
            match op {
                Op::AlterRole {
                    id,
                    name,
                    attributes,
                } => {
                    state.ensure_not_reserved_role(&id)?;
                    if let Some(builtin_update) = state.pack_role_update(id, -1) {
                        builtin_table_updates.push(builtin_update);
                    }
                    let existing_role = state.get_role_mut(&id);
                    existing_role.attributes = attributes;
                    tx.update_role(id, existing_role.clone().into())?;
                    if let Some(builtin_update) = state.pack_role_update(id, 1) {
                        builtin_table_updates.push(builtin_update);
                    }

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
                Op::AlterSink { id, cluster_config } => {
                    use mz_sql::ast::Value;
                    use mz_sql_parser::ast::CreateSinkOptionName::*;

                    let entry = state.get_entry(&id);
                    let name = entry.name().clone();

                    if entry.id().is_system() {
                        let schema_name = state
                            .resolve_full_name(&name, session.map(|session| session.conn_id()))
                            .schema;
                        return Err(AdapterError::Catalog(Error::new(
                            ErrorKind::ReadOnlySystemSchema(schema_name),
                        )));
                    }

                    let old_sink = match entry.item() {
                        CatalogItem::Sink(sink) => sink.clone(),
                        other => {
                            coord_bail!("ALTER SINK entry was not a sink: {}", other.typ())
                        }
                    };

                    // Since the catalog serializes the items using only their creation statement
                    // and context, we need to parse and rewrite the with options in that statement.
                    // (And then make any other changes to the source definition to match.)
                    let mut stmt = mz_sql::parse::parse(&old_sink.create_sql)
                        .expect("invalid create sql persisted to catalog")
                        .into_element();

                    let create_stmt = match &mut stmt {
                        Statement::CreateSink(s) => s,
                        _ => coord_bail!("sink {id} was not created with a CREATE SINK statement"),
                    };

                    create_stmt
                        .with_options
                        .retain(|x| ![Size].contains(&x.name));

                    let new_cluster_option = match &cluster_config {
                        PlanStorageClusterConfig::Existing { .. } => {
                            coord_bail!("cannot set cluster of existing source");
                        }
                        PlanStorageClusterConfig::Linked { size } => Some((Size, size.clone())),
                        PlanStorageClusterConfig::Undefined => None,
                    };

                    if let Some((name, value)) = new_cluster_option {
                        create_stmt.with_options.push(CreateSinkOption {
                            name,
                            value: Some(WithOptionValue::Value(Value::String(value))),
                        });
                    }

                    let new_size = match &cluster_config {
                        PlanStorageClusterConfig::Linked { size } => Some(size.clone()),
                        _ => None,
                    };

                    let create_sql = stmt.to_ast_string_stable();
                    let sink = CatalogItem::Sink(Sink {
                        create_sql,
                        ..old_sink
                    });

                    let ser = Self::serialize_item(&sink);
                    tx.update_item(id, &name.item, &ser)?;

                    state.add_to_audit_log(
                        oracle_write_ts,
                        session,
                        tx,
                        builtin_table_updates,
                        audit_events,
                        EventType::Alter,
                        ObjectType::Sink,
                        EventDetails::AlterSourceSinkV1(mz_audit_log::AlterSourceSinkV1 {
                            id: id.to_string(),
                            name: Self::full_name_detail(&state.resolve_full_name(
                                &name,
                                session.map(|session| session.conn_id()),
                            )),
                            old_size: old_source_sink_sizes[&id].clone(),
                            new_size,
                        }),
                    )?;

                    let to_name = entry.name().clone();
                    update_item(state, builtin_table_updates, id, to_name, sink)?;
                }
                Op::AlterSource { id, cluster_config } => {
                    use mz_sql::ast::Value;
                    use mz_sql_parser::ast::CreateSourceOptionName::*;

                    let entry = state.get_entry(&id);
                    let name = entry.name().clone();

                    if entry.id().is_system() {
                        let schema_name = state
                            .resolve_full_name(&name, session.map(|session| session.conn_id()))
                            .schema;
                        return Err(AdapterError::Catalog(Error::new(
                            ErrorKind::ReadOnlySystemSchema(schema_name),
                        )));
                    }

                    let old_source = match entry.item() {
                        CatalogItem::Source(source) => source.clone(),
                        other => {
                            coord_bail!("ALTER SOURCE entry was not a source: {}", other.typ())
                        }
                    };

                    // Since the catalog serializes the items using only their creation statement
                    // and context, we need to parse and rewrite the with options in that statement.
                    // (And then make any other changes to the source definition to match.)
                    let mut stmt = mz_sql::parse::parse(&old_source.create_sql)
                        .expect("invalid create sql persisted to catalog")
                        .into_element();

                    let create_stmt = match &mut stmt {
                        Statement::CreateSource(s) => s,
                        _ => coord_bail!(
                            "source {id} was not created with a CREATE SOURCE statement"
                        ),
                    };

                    create_stmt
                        .with_options
                        .retain(|x| ![Size].contains(&x.name));

                    let new_cluster_option = match &cluster_config {
                        PlanStorageClusterConfig::Existing { .. } => {
                            coord_bail!("cannot set cluster of existing source");
                        }
                        PlanStorageClusterConfig::Linked { size } => Some((Size, size.clone())),
                        PlanStorageClusterConfig::Undefined => None,
                    };

                    if let Some((name, value)) = new_cluster_option {
                        create_stmt.with_options.push(CreateSourceOption {
                            name,
                            value: Some(WithOptionValue::Value(Value::String(value))),
                        });
                    }

                    let new_size = match &cluster_config {
                        PlanStorageClusterConfig::Linked { size } => Some(size.clone()),
                        _ => None,
                    };

                    let create_sql = stmt.to_ast_string_stable();
                    let source = CatalogItem::Source(Source {
                        create_sql,
                        ..old_source
                    });

                    let ser = Self::serialize_item(&source);
                    tx.update_item(id, &name.item, &ser)?;

                    state.add_to_audit_log(
                        oracle_write_ts,
                        session,
                        tx,
                        builtin_table_updates,
                        audit_events,
                        EventType::Alter,
                        ObjectType::Source,
                        EventDetails::AlterSourceSinkV1(mz_audit_log::AlterSourceSinkV1 {
                            id: id.to_string(),
                            name: Self::full_name_detail(&state.resolve_full_name(
                                &name,
                                session.map(|session| session.conn_id()),
                            )),
                            old_size: old_source_sink_sizes[&id].clone(),
                            new_size,
                        }),
                    )?;

                    let to_name = entry.name().clone();
                    update_item(state, builtin_table_updates, id, to_name, source)?;
                }
                Op::CreateDatabase {
                    name,
                    oid,
                    public_schema_oid,
                    owner_id,
                } => {
                    let database_privileges = vec![rbac::owner_privilege(
                        mz_sql_parser::ast::ObjectType::Database,
                        owner_id,
                    )];
                    let default_schema_privileges = vec![
                        rbac::owner_privilege(mz_sql_parser::ast::ObjectType::Schema, owner_id),
                        // Default schemas provide usage and create privileges to PUBLIC by default.
                        MzAclItem {
                            grantee: RoleId::Public,
                            grantor: owner_id,
                            acl_mode: AclMode::USAGE.bitor(AclMode::CREATE),
                        },
                    ];
                    let database_id =
                        tx.insert_database(&name, owner_id, database_privileges.clone())?;
                    let schema_id = tx.insert_schema(
                        database_id,
                        DEFAULT_SCHEMA,
                        owner_id,
                        default_schema_privileges.clone(),
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
                            oid,
                            schemas_by_id: BTreeMap::new(),
                            schemas_by_name: BTreeMap::new(),
                            owner_id,
                            privileges: database_privileges,
                        },
                    );
                    state
                        .database_by_name
                        .insert(name.clone(), database_id.clone());
                    builtin_table_updates
                        .push(state.pack_database_update(&state.database_by_id[&database_id], 1));

                    state.add_to_audit_log(
                        oracle_write_ts,
                        session,
                        tx,
                        builtin_table_updates,
                        audit_events,
                        EventType::Create,
                        ObjectType::Schema,
                        EventDetails::SchemaV1(mz_audit_log::SchemaV1 {
                            id: schema_id.to_string(),
                            name: DEFAULT_SCHEMA.to_string(),
                            database_name: name,
                        }),
                    )?;
                    create_schema(
                        state,
                        builtin_table_updates,
                        schema_id,
                        public_schema_oid,
                        database_id,
                        DEFAULT_SCHEMA.to_string(),
                        owner_id,
                        default_schema_privileges,
                    )?;
                }
                Op::CreateSchema {
                    database_id,
                    schema_name,
                    oid,
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
                    let privileges = vec![rbac::owner_privilege(
                        mz_sql::ast::ObjectType::Schema,
                        owner_id,
                    )];
                    let schema_id =
                        tx.insert_schema(database_id, &schema_name, owner_id, privileges.clone())?;
                    state.add_to_audit_log(
                        oracle_write_ts,
                        session,
                        tx,
                        builtin_table_updates,
                        audit_events,
                        EventType::Create,
                        ObjectType::Schema,
                        EventDetails::SchemaV1(mz_audit_log::SchemaV1 {
                            id: schema_id.to_string(),
                            name: schema_name.clone(),
                            database_name: state.database_by_id[&database_id].name.clone(),
                        }),
                    )?;
                    create_schema(
                        state,
                        builtin_table_updates,
                        schema_id,
                        oid,
                        database_id,
                        schema_name,
                        owner_id,
                        privileges,
                    )?;
                }
                Op::CreateRole {
                    name,
                    oid,
                    attributes,
                } => {
                    if is_reserved_role_name(&name) {
                        return Err(AdapterError::Catalog(Error::new(
                            ErrorKind::ReservedRoleName(name),
                        )));
                    }
                    let membership = RoleMembership::new();
                    let serialized_role = SerializedRole {
                        name: name.clone(),
                        attributes: Some(attributes.clone()),
                        membership: Some(membership.clone()),
                    };
                    let id = tx.insert_user_role(serialized_role)?;
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
                        },
                    );
                    if let Some(builtin_update) = state.pack_role_update(id, 1) {
                        builtin_table_updates.push(builtin_update);
                    }
                }
                Op::CreateCluster {
                    id,
                    name,
                    linked_object_id,
                    introspection_sources,
                    owner_id,
                } => {
                    if is_reserved_name(&name) {
                        return Err(AdapterError::Catalog(Error::new(
                            ErrorKind::ReservedClusterName(name),
                        )));
                    }
                    let privileges = vec![rbac::owner_privilege(
                        mz_sql::ast::ObjectType::Cluster,
                        owner_id,
                    )];
                    tx.insert_user_cluster(
                        id,
                        &name,
                        linked_object_id,
                        &introspection_sources,
                        owner_id,
                        privileges.clone(),
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
                        introspection_sources.iter().map(|(_, id)| *id).collect();
                    state.insert_cluster(
                        id,
                        name.clone(),
                        linked_object_id,
                        introspection_sources,
                        owner_id,
                        privileges,
                    );
                    builtin_table_updates.push(state.pack_cluster_update(&name, 1));
                    if let Some(linked_object_id) = linked_object_id {
                        builtin_table_updates.push(state.pack_cluster_link_update(
                            &name,
                            linked_object_id,
                            1,
                        ));
                    }
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
                } => {
                    if is_reserved_name(&name) {
                        return Err(AdapterError::Catalog(Error::new(
                            ErrorKind::ReservedReplicaName(name),
                        )));
                    }
                    let cluster = state.get_cluster(cluster_id);
                    if cluster_id.is_system()
                        && !session
                            .map(|session| session.user().is_internal())
                            .unwrap_or(false)
                    {
                        return Err(AdapterError::Catalog(Error::new(
                            ErrorKind::ReadOnlyCluster(cluster.name.clone()),
                        )));
                    }
                    tx.insert_cluster_replica(
                        cluster_id,
                        id,
                        &name,
                        &config.clone().into(),
                        owner_id,
                    )?;
                    if let ReplicaLocation::Managed(ManagedReplicaLocation { size, .. }) =
                        &config.location
                    {
                        let details = EventDetails::CreateClusterReplicaV1(
                            mz_audit_log::CreateClusterReplicaV1 {
                                cluster_id: cluster_id.to_string(),
                                cluster_name: cluster.name.clone(),
                                replica_id: Some(id.to_string()),
                                replica_name: name.clone(),
                                logical_size: size.clone(),
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
                        .push(state.pack_cluster_replica_update(cluster_id, &name, 1));
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
                    oid,
                    name,
                    item,
                    owner_id,
                } => {
                    state.ensure_no_unstable_uses(&item)?;

                    if let Some(id @ ClusterId::System(_)) = item.cluster_id() {
                        let cluster_name = state.clusters_by_id[&id].name.clone();
                        return Err(AdapterError::Catalog(Error::new(
                            ErrorKind::ReadOnlyCluster(cluster_name),
                        )));
                    }

                    let mut privileges = vec![rbac::owner_privilege(item.typ().into(), owner_id)];
                    // Everyone has default USAGE privileges on types.
                    if item.typ() == CatalogItemType::Type {
                        privileges.push(MzAclItem {
                            grantee: RoleId::Public,
                            grantor: owner_id,
                            acl_mode: AclMode::USAGE,
                        });
                    }

                    if item.is_temporary() {
                        if name.qualifiers.database_spec != ResolvedDatabaseSpecifier::Ambient
                            || name.qualifiers.schema_spec != SchemaSpecifier::Temporary
                        {
                            return Err(AdapterError::Catalog(Error::new(
                                ErrorKind::InvalidTemporarySchema,
                            )));
                        }
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
                                ErrorKind::InvalidTemporaryDependency(
                                    temp_item.name().item.clone(),
                                ),
                            )));
                        }
                        if let ResolvedDatabaseSpecifier::Ambient = name.qualifiers.database_spec {
                            // We allow users to create indexes on system objects to speed up
                            // debugging related queries.
                            if item.typ() != CatalogItemType::Index {
                                let schema_name = state
                                    .resolve_full_name(
                                        &name,
                                        session.map(|session| session.conn_id()),
                                    )
                                    .schema;
                                return Err(AdapterError::Catalog(Error::new(
                                    ErrorKind::ReadOnlySystemSchema(schema_name),
                                )));
                            }
                        }
                        let schema_id = name.qualifiers.schema_spec.clone().into();
                        let serialized_item = Self::serialize_item(&item);
                        tx.insert_item(
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
                            &state
                                .resolve_full_name(&name, session.map(|session| session.conn_id())),
                        );
                        let size = state.get_storage_object_size(id).map(|s| s.to_string());
                        let details = match &item {
                            CatalogItem::Source(s) => {
                                EventDetails::CreateSourceSinkV2(mz_audit_log::CreateSourceSinkV2 {
                                    id: id.to_string(),
                                    name,
                                    size,
                                    external_type: s.source_type().to_string(),
                                })
                            }
                            CatalogItem::Sink(s) => {
                                EventDetails::CreateSourceSinkV2(mz_audit_log::CreateSourceSinkV2 {
                                    id: id.to_string(),
                                    name,
                                    size,
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
                            sql_type_to_object_type(item.typ()),
                            details,
                        )?;
                    }
                    state.insert_item(id, oid, name, item, owner_id, privileges);
                    builtin_table_updates.extend(state.pack_item_update(id, 1));
                }
                Op::DropObject(id) => match id {
                    ObjectId::Database(id) => {
                        let database = &state.database_by_id[&id];
                        tx.remove_database(&id)?;
                        builtin_table_updates.push(state.pack_database_update(database, -1));
                        state.add_to_audit_log(
                            oracle_write_ts,
                            session,
                            tx,
                            builtin_table_updates,
                            audit_events,
                            EventType::Drop,
                            ObjectType::Database,
                            EventDetails::IdNameV1(mz_audit_log::IdNameV1 {
                                id: id.to_string(),
                                name: database.name.clone(),
                            }),
                        )?;
                        let db = state.database_by_id.get(&id).expect("catalog out of sync");
                        state.database_by_name.remove(db.name());
                        state.database_by_id.remove(&id);
                    }
                    ObjectId::Schema((database_id, schema_id)) => {
                        let schema = &state.database_by_id[&database_id].schemas_by_id[&schema_id];
                        tx.remove_schema(&database_id, &schema_id)?;
                        builtin_table_updates.push(state.pack_schema_update(
                            &ResolvedDatabaseSpecifier::Id(database_id.clone()),
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
                            EventDetails::SchemaV1(mz_audit_log::SchemaV1 {
                                id: schema_id.to_string(),
                                name: schema.name.schema.to_string(),
                                database_name: state.database_by_id[&database_id].name.clone(),
                            }),
                        )?;
                        let db = state
                            .database_by_id
                            .get_mut(&database_id)
                            .expect("catalog out of sync");
                        let schema = db
                            .schemas_by_id
                            .get(&schema_id)
                            .expect("catalog out of sync");
                        db.schemas_by_name.remove(&schema.name.schema);
                        db.schemas_by_id.remove(&schema_id);
                    }
                    ObjectId::Role(id) => {
                        let name = state.get_role(&id).name().to_string();
                        state.ensure_not_reserved_role(&id)?;
                        tx.remove_role(&name)?;
                        if let Some(builtin_update) = state.pack_role_update(id, -1) {
                            builtin_table_updates.push(builtin_update);
                        }
                        let role = state.roles_by_id.remove(&id).expect("catalog out of sync");
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
                                name: name.clone(),
                            }),
                        )?;
                        info!("drop role {}", role.name());
                    }
                    ObjectId::Cluster(id) => {
                        let cluster = state.get_cluster(id);
                        let name = &cluster.name;
                        if is_reserved_name(name) {
                            return Err(AdapterError::Catalog(Error::new(
                                ErrorKind::ReadOnlyCluster(name.clone()),
                            )));
                        }
                        tx.remove_cluster(id)?;
                        builtin_table_updates.push(state.pack_cluster_update(name, -1));
                        if let Some(linked_object_id) = cluster.linked_object_id {
                            builtin_table_updates.push(state.pack_cluster_link_update(
                                name,
                                linked_object_id,
                                -1,
                            ));
                        }
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
                                name: name.clone(),
                            }),
                        )?;
                        let cluster = state
                            .clusters_by_id
                            .remove(&id)
                            .expect("can only drop known clusters");
                        state.clusters_by_name.remove(&cluster.name);

                        if let Some(linked_object_id) = cluster.linked_object_id {
                            state
                                .clusters_by_linked_object_id
                                .remove(&linked_object_id)
                                .expect("can only drop known clusters");
                        }

                        for id in cluster.log_indexes.values() {
                            state.drop_item(*id);
                        }

                        assert!(
                            cluster.bound_objects.is_empty() && cluster.replicas_by_id.is_empty(),
                            "not all items dropped before cluster"
                        );
                    }
                    ObjectId::ClusterReplica((cluster_id, replica_id)) => {
                        let cluster = state.get_cluster(cluster_id);
                        let replica = &cluster.replicas_by_id[&replica_id];
                        if is_reserved_name(&replica.name) {
                            return Err(AdapterError::Catalog(Error::new(
                                ErrorKind::ReservedReplicaName(replica.name.clone()),
                            )));
                        }
                        if cluster_id.is_system()
                            && !session
                                .map(|session| session.user().is_internal())
                                .unwrap_or(false)
                        {
                            return Err(AdapterError::Catalog(Error::new(
                                ErrorKind::ReadOnlyCluster(cluster.name.clone()),
                            )));
                        }
                        tx.remove_cluster_replica(replica_id)?;

                        for process_id in replica.process_status.keys() {
                            let update = state.pack_cluster_replica_status_update(
                                cluster_id,
                                replica_id,
                                *process_id,
                                -1,
                            );
                            builtin_table_updates.push(update);
                        }

                        builtin_table_updates.push(state.pack_cluster_replica_update(
                            cluster_id,
                            &replica.name,
                            -1,
                        ));

                        let details = EventDetails::DropClusterReplicaV1(
                            mz_audit_log::DropClusterReplicaV1 {
                                cluster_id: cluster_id.to_string(),
                                cluster_name: cluster.name.clone(),
                                replica_id: Some(replica_id.to_string()),
                                replica_name: replica.name.clone(),
                            },
                        );
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
                        let replica = cluster
                            .replicas_by_id
                            .remove(&replica_id)
                            .expect("catalog out of sync");
                        cluster
                            .replica_id_by_name
                            .remove(&replica.name)
                            .expect("catalog out of sync");
                        assert_eq!(
                            cluster.replica_id_by_name.len(),
                            cluster.replicas_by_id.len()
                        );
                    }
                    ObjectId::Item(id) => {
                        let entry = state.get_entry(&id);
                        if !entry.item().is_temporary() {
                            tx.remove_item(id)?;
                        }

                        builtin_table_updates.extend(state.pack_item_update(id, -1));
                        if Self::should_audit_log_item(&entry.item) {
                            state.add_to_audit_log(
                                oracle_write_ts,
                                session,
                                tx,
                                builtin_table_updates,
                                audit_events,
                                EventType::Drop,
                                sql_type_to_object_type(entry.item().typ()),
                                EventDetails::IdFullNameV1(IdFullNameV1 {
                                    id: id.to_string(),
                                    name: Self::full_name_detail(&state.resolve_full_name(
                                        &entry.name,
                                        session.map(|session| session.conn_id()),
                                    )),
                                }),
                            )?;
                        }
                        state.drop_item(id);
                    }
                },
                Op::DropTimeline(timeline) => {
                    tx.remove_timestamp(timeline);
                }
                Op::GrantRole {
                    role_id,
                    member_id,
                    grantor_id,
                } => {
                    state.ensure_not_reserved_role(&member_id)?;
                    state.ensure_not_reserved_role(&role_id)?;
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
                    builtin_table_updates
                        .push(state.pack_role_members_update(role_id, member_id, 1));

                    state.add_to_audit_log(
                        oracle_write_ts,
                        session,
                        tx,
                        builtin_table_updates,
                        audit_events,
                        EventType::Grant,
                        ObjectType::Role,
                        EventDetails::GrantRoleV1(mz_audit_log::GrantRoleV1 {
                            role_id: role_id.to_string(),
                            member_id: member_id.to_string(),
                            grantor_id: grantor_id.to_string(),
                        }),
                    )?;
                }
                Op::RevokeRole { role_id, member_id } => {
                    state.ensure_not_reserved_role(&member_id)?;
                    state.ensure_not_reserved_role(&role_id)?;
                    builtin_table_updates
                        .push(state.pack_role_members_update(role_id, member_id, -1));
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
                        EventDetails::RevokeRoleV1(mz_audit_log::RevokeRoleV1 {
                            role_id: role_id.to_string(),
                            member_id: member_id.to_string(),
                        }),
                    )?;
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
                        let schema_name = state
                            .resolve_full_name(
                                entry.name(),
                                session.map(|session| session.conn_id()),
                            )
                            .schema;
                        return Err(AdapterError::Catalog(Error::new(
                            ErrorKind::ReadOnlySystemSchema(schema_name),
                        )));
                    }

                    let mut to_full_name = current_full_name.clone();
                    to_full_name.item = to_name.clone();

                    let mut to_qualified_name = entry.name().clone();
                    to_qualified_name.item = to_name;

                    let details = EventDetails::RenameItemV1(mz_audit_log::RenameItemV1 {
                        id: id.to_string(),
                        old_name: Self::full_name_detail(&current_full_name),
                        new_name: Self::full_name_detail(&to_full_name),
                    });
                    if Self::should_audit_log_item(&entry.item) {
                        state.add_to_audit_log(
                            oracle_write_ts,
                            session,
                            tx,
                            builtin_table_updates,
                            audit_events,
                            EventType::Alter,
                            sql_type_to_object_type(entry.item().typ()),
                            details,
                        )?;
                    }

                    // Rename item itself.
                    let item = entry
                        .item
                        .rename_item_refs(
                            current_full_name.clone(),
                            to_full_name.item.clone(),
                            true,
                        )
                        .map_err(|e| {
                            Error::new(ErrorKind::from(AmbiguousRename {
                                depender: state
                                    .resolve_full_name(&entry.name, entry.conn_id())
                                    .to_string(),
                                dependee: state
                                    .resolve_full_name(&entry.name, entry.conn_id())
                                    .to_string(),
                                message: e,
                            }))
                        })?;
                    let serialized_item = Self::serialize_item(&item);

                    for id in entry.used_by() {
                        let dependent_item = state.get_entry(id);
                        let to_item = dependent_item
                            .item
                            .rename_item_refs(
                                current_full_name.clone(),
                                to_full_name.item.clone(),
                                false,
                            )
                            .map_err(|e| {
                                Error::new(ErrorKind::from(AmbiguousRename {
                                    depender: state
                                        .resolve_full_name(
                                            &dependent_item.name,
                                            dependent_item.conn_id(),
                                        )
                                        .to_string(),
                                    dependee: state
                                        .resolve_full_name(&entry.name, entry.conn_id())
                                        .to_string(),
                                    message: e,
                                }))
                            })?;

                        if !item.is_temporary() {
                            let serialized_item = Self::serialize_item(&to_item);
                            tx.update_item(*id, &dependent_item.name().item, &serialized_item)?;
                        }
                        builtin_table_updates.extend(state.pack_item_update(*id, -1));

                        updates.push((id.clone(), dependent_item.name().clone(), to_item));
                    }
                    if !item.is_temporary() {
                        tx.update_item(id, &to_full_name.item, &serialized_item)?;
                    }
                    builtin_table_updates.extend(state.pack_item_update(id, -1));
                    updates.push((id, to_qualified_name, item));
                    for (id, to_name, to_item) in updates {
                        update_item(state, builtin_table_updates, id, to_name, to_item)?;
                    }
                }
                Op::UpdateOwner { id, new_owner } => match id {
                    ObjectId::Cluster(id) => {
                        let cluster_name = state.get_cluster(id).name().to_string();
                        if id.is_system() {
                            return Err(AdapterError::Catalog(Error::new(
                                ErrorKind::ReadOnlyCluster(cluster_name),
                            )));
                        }
                        builtin_table_updates.push(state.pack_cluster_update(&cluster_name, -1));
                        let cluster = state.get_cluster_mut(id);
                        cluster.owner_id = new_owner;
                        tx.update_cluster(id, cluster)?;
                        builtin_table_updates.push(state.pack_cluster_update(&cluster_name, 1));
                    }
                    ObjectId::ClusterReplica((cluster_id, replica_id)) => {
                        let cluster = state.get_cluster(cluster_id);
                        if cluster_id.is_system() {
                            return Err(AdapterError::Catalog(Error::new(
                                ErrorKind::ReadOnlyCluster(cluster.name().to_string()),
                            )));
                        }
                        let replica_name = cluster
                            .replicas_by_id
                            .get(&replica_id)
                            .expect("catalog out of sync")
                            .name
                            .clone();
                        if is_reserved_name(&replica_name) {
                            return Err(AdapterError::Catalog(Error::new(
                                ErrorKind::ReservedReplicaName(replica_name),
                            )));
                        }
                        builtin_table_updates.push(state.pack_cluster_replica_update(
                            cluster_id,
                            &replica_name,
                            -1,
                        ));
                        let cluster = state.get_cluster_mut(cluster_id);
                        let replica = cluster
                            .replicas_by_id
                            .get_mut(&replica_id)
                            .expect("catalog out of sync");
                        replica.owner_id = new_owner;
                        tx.update_cluster_replica(cluster_id, replica_id, replica)?;
                        builtin_table_updates.push(state.pack_cluster_replica_update(
                            cluster_id,
                            &replica_name,
                            1,
                        ));
                    }
                    ObjectId::Database(id) => {
                        let database = state.get_database(&id);
                        builtin_table_updates.push(state.pack_database_update(database, -1));
                        let database = state.get_database_mut(&id);
                        database.owner_id = new_owner;
                        let database = state.get_database(&id);
                        tx.update_database(id, database)?;
                        builtin_table_updates.push(state.pack_database_update(database, 1));
                    }
                    ObjectId::Schema((database_id, schema_id)) => {
                        builtin_table_updates.push(state.pack_schema_update(
                            &ResolvedDatabaseSpecifier::Id(database_id),
                            &schema_id,
                            -1,
                        ));
                        let database = state.get_database_mut(&database_id);
                        let schema = database
                            .schemas_by_id
                            .get_mut(&schema_id)
                            .expect("catalog out of sync");
                        schema.owner_id = new_owner;
                        tx.update_schema(Some(database_id), schema_id, schema)?;
                        builtin_table_updates.push(state.pack_schema_update(
                            &ResolvedDatabaseSpecifier::Id(database_id),
                            &schema_id,
                            1,
                        ));
                    }
                    ObjectId::Item(id) => {
                        if id.is_system() {
                            let entry = state.get_entry(&id);
                            let full_name = state.resolve_full_name(
                                entry.name(),
                                session.map(|session| session.conn_id()),
                            );
                            return Err(AdapterError::Catalog(Error::new(
                                ErrorKind::ReadOnlyItem(full_name.to_string()),
                            )));
                        }
                        builtin_table_updates.extend(state.pack_item_update(id, -1));
                        let entry = state.get_entry_mut(&id);
                        entry.owner_id = new_owner;
                        tx.update_item(
                            id,
                            &entry.name().item,
                            &Self::serialize_item(entry.item()),
                        )?;
                        builtin_table_updates.extend(state.pack_item_update(id, 1));
                    }
                    ObjectId::Role(_) => unreachable!("roles have no owner"),
                },
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
                    let ser = Self::serialize_item(&to_item);
                    tx.update_item(id, &name.item, &ser)?;
                    builtin_table_updates.extend(state.pack_item_update(id, -1));
                    update_item(state, builtin_table_updates, id, name, to_item)?;
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
                    state.insert_system_configuration(&name, value.borrow())?;
                    let var = state.get_system_configuration(&name)?;
                    if !var.safe() {
                        state.require_unsafe_mode(var.name())?;
                    }
                    tx.upsert_system_config(&name, var.value())?;
                }
                Op::ResetSystemConfiguration { name } => {
                    state.remove_system_configuration(&name)?;
                    let var = state.get_system_configuration(&name)?;
                    if !var.safe() {
                        state.require_unsafe_mode(var.name())?;
                    }
                    tx.remove_system_config(&name);
                }
                Op::ResetAllSystemConfiguration => {
                    state.clear_system_configuration();
                    tx.clear_system_configs();
                }
                Op::UpdateRotatedKeys {
                    id,
                    previous_public_key_pair,
                    new_public_key_pair,
                } => {
                    let entry = state.get_entry(&id);
                    // Retract old keys
                    builtin_table_updates.extend(state.pack_ssh_tunnel_connection_update(
                        id,
                        &previous_public_key_pair,
                        -1,
                    ));
                    // Insert the new rotated keys
                    builtin_table_updates.extend(state.pack_ssh_tunnel_connection_update(
                        id,
                        &new_public_key_pair,
                        1,
                    ));

                    let mut connection = entry.connection()?.clone();
                    if let mz_storage_client::types::connections::Connection::Ssh(ref mut ssh) =
                        connection.connection
                    {
                        ssh.public_keys = Some(new_public_key_pair)
                    }
                    let new_item = CatalogItem::Connection(connection);

                    let old_entry = state.entry_by_id.remove(&id).expect("catalog out of sync");
                    info!(
                        "update {} {} ({})",
                        old_entry.item_type(),
                        state.resolve_full_name(&old_entry.name, old_entry.conn_id()),
                        id
                    );
                    let mut new_entry = old_entry;
                    new_entry.item = new_item;
                    state.entry_by_id.insert(id, new_entry);
                }
            };
        }

        fn update_item(
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
                state.resolve_full_name(&old_entry.name, old_entry.conn_id()),
                id
            );
            assert_eq!(old_entry.uses(), to_item.uses());
            let conn_id = old_entry.item().conn_id().unwrap_or(SYSTEM_CONN_ID);
            let schema = &mut state.get_schema_mut(
                &old_entry.name().qualifiers.database_spec,
                &old_entry.name().qualifiers.schema_spec,
                conn_id,
            );
            schema.items.remove(&old_entry.name().item);
            let mut new_entry = old_entry.clone();
            new_entry.name = to_name;
            new_entry.item = to_item;
            schema.items.insert(new_entry.name().item.clone(), id);
            state.entry_by_id.insert(id, new_entry);
            builtin_table_updates.extend(state.pack_item_update(id, 1));
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
            privileges: Vec<MzAclItem>,
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
        Ok(())
    }

    pub async fn consolidate(&self, collections: &[mz_stash::Id]) -> Result<(), AdapterError> {
        Ok(self.storage().await.consolidate(collections).await?)
    }

    pub async fn confirm_leadership(&self) -> Result<(), AdapterError> {
        Ok(self.storage().await.confirm_leadership().await?)
    }

    fn serialize_item(item: &CatalogItem) -> SerializedCatalogItem {
        match item {
            CatalogItem::Table(table) => SerializedCatalogItem::V1 {
                create_sql: table.create_sql.clone(),
            },
            CatalogItem::Log(_) => unreachable!("builtin logs cannot be serialized"),
            CatalogItem::Source(source) => {
                assert!(
                    match source.data_source {
                        DataSourceDesc::Introspection(_) => false,
                        _ => true,
                    },
                    "cannot serialize introspection/builtin sources",
                );
                SerializedCatalogItem::V1 {
                    create_sql: source.create_sql.clone(),
                }
            }
            CatalogItem::View(view) => SerializedCatalogItem::V1 {
                create_sql: view.create_sql.clone(),
            },
            CatalogItem::MaterializedView(mview) => SerializedCatalogItem::V1 {
                create_sql: mview.create_sql.clone(),
            },
            CatalogItem::Index(index) => SerializedCatalogItem::V1 {
                create_sql: index.create_sql.clone(),
            },
            CatalogItem::Sink(sink) => SerializedCatalogItem::V1 {
                create_sql: sink.create_sql.clone(),
            },
            CatalogItem::Type(typ) => SerializedCatalogItem::V1 {
                create_sql: typ.create_sql.clone(),
            },
            CatalogItem::Secret(secret) => SerializedCatalogItem::V1 {
                create_sql: secret.create_sql.clone(),
            },
            CatalogItem::Connection(connection) => SerializedCatalogItem::V1 {
                create_sql: connection.create_sql.clone(),
            },
            CatalogItem::Func(_) => unreachable!("cannot serialize functions yet"),
        }
    }

    fn deserialize_item(
        &self,
        id: GlobalId,
        SerializedCatalogItem::V1 { create_sql }: SerializedCatalogItem,
    ) -> Result<CatalogItem, AdapterError> {
        // TODO - The `None` needs to be changed if we ever allow custom
        // logical compaction windows in user-defined objects.
        self.parse_item(id, create_sql, Some(&PlanContext::zero()), false, None)
    }

    // Parses the given SQL string into a `CatalogItem`.
    #[tracing::instrument(level = "info", skip(self, pcx))]
    fn parse_item(
        &self,
        id: GlobalId,
        create_sql: String,
        pcx: Option<&PlanContext>,
        is_retained_metrics_object: bool,
        custom_logical_compaction_window: Option<Duration>,
    ) -> Result<CatalogItem, AdapterError> {
        let mut session_catalog = self.for_system_session();
        enable_features_required_for_catalog_open(&mut session_catalog);

        let stmt = mz_sql::parse::parse(&create_sql)?.into_element();
        let (stmt, depends_on) = mz_sql::names::resolve(&session_catalog, stmt)?;
        let depends_on = depends_on.into_iter().collect();
        let plan = mz_sql::plan::plan(pcx, &session_catalog, stmt, &Params::empty())?;
        Ok(match plan {
            Plan::CreateTable(CreateTablePlan { table, .. }) => CatalogItem::Table(Table {
                create_sql: table.create_sql,
                desc: table.desc,
                defaults: table.defaults,
                conn_id: None,
                depends_on,
                custom_logical_compaction_window,
                is_retained_metrics_object,
            }),
            Plan::CreateSource(CreateSourcePlan {
                source,
                timeline,
                cluster_config,
                ..
            }) => CatalogItem::Source(Source {
                create_sql: source.create_sql,
                data_source: match source.data_source {
                    mz_sql::plan::DataSourceDesc::Ingestion(ingestion) => {
                        DataSourceDesc::Ingestion(Ingestion {
                            desc: ingestion.desc,
                            source_imports: ingestion.source_imports,
                            subsource_exports: ingestion.subsource_exports,
                            cluster_id: match cluster_config {
                                plan::SourceSinkClusterConfig::Existing { id } => id,
                                plan::SourceSinkClusterConfig::Linked { .. }
                                | plan::SourceSinkClusterConfig::Undefined => {
                                    self.state.clusters_by_linked_object_id[&id]
                                }
                            },
                            remap_collection_id: ingestion.progress_subsource,
                        })
                    }
                    mz_sql::plan::DataSourceDesc::Progress => DataSourceDesc::Progress,
                    mz_sql::plan::DataSourceDesc::Source => DataSourceDesc::Source,
                },
                desc: source.desc,
                timeline,
                depends_on,
                custom_logical_compaction_window,
                is_retained_metrics_object,
            }),
            Plan::CreateView(CreateViewPlan { view, .. }) => {
                let optimizer = Optimizer::logical_optimizer();
                let optimized_expr = optimizer.optimize(view.expr)?;
                let desc = RelationDesc::new(optimized_expr.typ(), view.column_names);
                CatalogItem::View(View {
                    create_sql: view.create_sql,
                    optimized_expr,
                    desc,
                    conn_id: None,
                    depends_on,
                })
            }
            Plan::CreateMaterializedView(CreateMaterializedViewPlan {
                materialized_view, ..
            }) => {
                let optimizer = Optimizer::logical_optimizer();
                let optimized_expr = optimizer.optimize(materialized_view.expr)?;
                let desc = RelationDesc::new(optimized_expr.typ(), materialized_view.column_names);
                CatalogItem::MaterializedView(MaterializedView {
                    create_sql: materialized_view.create_sql,
                    optimized_expr,
                    desc,
                    depends_on,
                    cluster_id: materialized_view.cluster_id,
                })
            }
            Plan::CreateIndex(CreateIndexPlan { index, .. }) => CatalogItem::Index(Index {
                create_sql: index.create_sql,
                on: index.on,
                keys: index.keys,
                conn_id: None,
                depends_on,
                cluster_id: index.cluster_id,
                custom_logical_compaction_window,
                is_retained_metrics_object,
            }),
            Plan::CreateSink(CreateSinkPlan {
                sink,
                with_snapshot,
                cluster_config,
                ..
            }) => CatalogItem::Sink(Sink {
                create_sql: sink.create_sql,
                from: sink.from,
                connection: StorageSinkConnectionState::Pending(sink.connection_builder),
                envelope: sink.envelope,
                with_snapshot,
                depends_on,
                cluster_id: match cluster_config {
                    plan::SourceSinkClusterConfig::Existing { id } => id,
                    plan::SourceSinkClusterConfig::Linked { .. }
                    | plan::SourceSinkClusterConfig::Undefined => {
                        self.state.clusters_by_linked_object_id[&id]
                    }
                },
            }),
            Plan::CreateType(CreateTypePlan { typ, .. }) => CatalogItem::Type(Type {
                create_sql: typ.create_sql,
                details: CatalogTypeDetails {
                    array_id: None,
                    typ: typ.inner,
                },
                depends_on,
            }),
            Plan::CreateSecret(CreateSecretPlan { secret, .. }) => CatalogItem::Secret(Secret {
                create_sql: secret.create_sql,
            }),
            Plan::CreateConnection(CreateConnectionPlan { connection, .. }) => {
                CatalogItem::Connection(Connection {
                    create_sql: connection.create_sql,
                    connection: connection.connection,
                    depends_on,
                })
            }
            _ => {
                return Err(Error::new(ErrorKind::Corruption {
                    detail: "catalog entry generated inappropriate plan".to_string(),
                })
                .into())
            }
        })
    }

    pub fn uses_tables(&self, id: GlobalId) -> bool {
        self.state.uses_tables(id)
    }

    /// Return the ids of all log sources the given object depends on.
    pub fn introspection_dependencies(&self, id: GlobalId) -> Vec<GlobalId> {
        self.state.introspection_dependencies(id)
    }

    /// Serializes the catalog's in-memory state.
    ///
    /// There are no guarantees about the format of the serialized state, except
    /// that the serialized state for two identical catalogs will compare
    /// identically.
    pub fn dump(&self) -> String {
        self.state.dump()
    }

    pub fn config(&self) -> &mz_sql::catalog::CatalogConfig {
        self.state.config()
    }

    pub fn entries(&self) -> impl Iterator<Item = &CatalogEntry> {
        self.state.entry_by_id.values()
    }

    pub fn user_connections(&self) -> impl Iterator<Item = &CatalogEntry> {
        self.entries()
            .filter(|entry| entry.is_connection() && entry.id.is_user())
    }

    pub fn user_tables(&self) -> impl Iterator<Item = &CatalogEntry> {
        self.entries()
            .filter(|entry| entry.is_table() && entry.id.is_user())
    }

    pub fn user_sources(&self) -> impl Iterator<Item = &CatalogEntry> {
        self.entries()
            .filter(|entry| entry.is_source() && entry.id.is_user())
    }

    pub fn user_sinks(&self) -> impl Iterator<Item = &CatalogEntry> {
        self.entries()
            .filter(|entry| entry.is_sink() && entry.id.is_user())
    }

    pub fn user_materialized_views(&self) -> impl Iterator<Item = &CatalogEntry> {
        self.entries()
            .filter(|entry| entry.is_materialized_view() && entry.id.is_user())
    }

    pub fn user_secrets(&self) -> impl Iterator<Item = &CatalogEntry> {
        self.entries()
            .filter(|entry| entry.is_secret() && entry.id.is_user())
    }

    pub fn clusters(&self) -> impl Iterator<Item = &Cluster> {
        self.state.clusters_by_id.values()
    }

    pub fn get_cluster(&self, cluster_id: ClusterId) -> &Cluster {
        self.state.get_cluster(cluster_id)
    }

    pub fn try_get_cluster(&self, cluster_id: ClusterId) -> Option<&Cluster> {
        self.state.try_get_cluster(cluster_id)
    }

    pub fn user_clusters(&self) -> impl Iterator<Item = &Cluster> {
        self.clusters().filter(|cluster| cluster.id.is_user())
    }

    pub fn databases(&self) -> impl Iterator<Item = &Database> {
        self.state.database_by_id.values()
    }

    pub fn user_roles(&self) -> impl Iterator<Item = &Role> {
        self.state
            .roles_by_id
            .values()
            .filter(|role| role.is_user())
    }

    /// Allocate ids for introspection sources. Called once per cluster creation.
    pub async fn allocate_introspection_sources(&self) -> Vec<(&'static BuiltinLog, GlobalId)> {
        let log_amount = BUILTINS::logs().count();
        let system_ids = self
            .storage()
            .await
            .allocate_system_ids(
                log_amount
                    .try_into()
                    .expect("builtin logs should fit into u64"),
            )
            .await
            .unwrap_or_terminate("cannot fail to allocate system ids");
        BUILTINS::logs().zip(system_ids.into_iter()).collect()
    }

    pub fn pack_item_update(&self, id: GlobalId, diff: Diff) -> Vec<BuiltinTableUpdate> {
        self.state.pack_item_update(id, diff)
    }

    pub fn system_config(&self) -> &SystemVars {
        self.state.system_config()
    }

    pub fn unsafe_mode(&self) -> bool {
        self.config().unsafe_mode
    }

    pub fn require_unsafe_mode(&self, feature_name: &'static str) -> Result<(), AdapterError> {
        self.state.require_unsafe_mode(feature_name)
    }

    /// Return the current compute configuration, derived from the system configuration.
    pub fn compute_config(&self) -> ComputeParameters {
        let config = self.system_config();
        ComputeParameters {
            max_result_size: Some(config.max_result_size()),
            dataflow_max_inflight_bytes: Some(config.dataflow_max_inflight_bytes()),
            persist: self.persist_config(),
        }
    }

    /// Return the current storage configuration, derived from the system configuration.
    pub fn storage_config(&self) -> StorageParameters {
        StorageParameters {
            enable_multi_worker_storage_persist_sink: self
                .system_config()
                .enable_multi_worker_storage_persist_sink(),
            persist: self.persist_config(),
            pg_replication_timeouts: mz_postgres_util::ReplicationTimeouts {
                connect_timeout: Some(self.system_config().pg_replication_connect_timeout()),
                keepalives_retries: Some(self.system_config().pg_replication_keepalives_retries()),
                keepalives_idle: Some(self.system_config().pg_replication_keepalives_idle()),
                keepalives_interval: Some(
                    self.system_config().pg_replication_keepalives_interval(),
                ),
                tcp_user_timeout: Some(self.system_config().pg_replication_tcp_user_timeout()),
            },
        }
    }

    fn persist_config(&self) -> PersistParameters {
        let config = self.system_config();
        PersistParameters {
            blob_target_size: Some(config.persist_blob_target_size()),
            compaction_minimum_timeout: Some(config.persist_compaction_minimum_timeout()),
            consensus_connect_timeout: Some(config.crdb_connect_timeout()),
            sink_minimum_batch_updates: Some(config.persist_sink_minimum_batch_updates()),
            storage_sink_minimum_batch_updates: Some(
                config.storage_persist_sink_minimum_batch_updates(),
            ),
            next_listen_batch_retryer: Some(RetryParameters {
                initial_backoff: config.persist_next_listen_batch_retryer_initial_backoff(),
                multiplier: config.persist_next_listen_batch_retryer_multiplier(),
                clamp: config.persist_next_listen_batch_retryer_clamp(),
            }),
            stats_collection_enabled: Some(config.persist_stats_collection_enabled()),
            stats_filter_enabled: Some(config.persist_stats_filter_enabled()),
        }
    }

    pub fn ensure_not_reserved_role(&self, role_id: &RoleId) -> Result<(), Error> {
        self.state.ensure_not_reserved_role(role_id)
    }
}

pub fn is_reserved_name(name: &str) -> bool {
    BUILTIN_PREFIXES
        .iter()
        .any(|prefix| name.starts_with(prefix))
}

pub fn is_reserved_role_name(name: &str) -> bool {
    is_reserved_name(name) || is_public_role(name)
}

pub fn is_public_role(name: &str) -> bool {
    name == &*PUBLIC_ROLE_NAME
}

/// Enable catalog features that might be required during planning in
/// [Catalog::open]. Existing catalog items might have been created while a
/// specific feature flag turned on, so we need to ensure that this is also the
/// case during catalog rehydration in order to avoid panics.
fn enable_features_required_for_catalog_open(session_catalog: &mut ConnCatalog) {
    if !session_catalog
        .system_vars()
        .enable_with_mutually_recursive()
    {
        session_catalog
            .system_vars_mut()
            .set_enable_with_mutually_recursive(true);
    }
}

#[derive(Debug, Clone)]
pub enum Op {
    AlterSink {
        id: GlobalId,
        cluster_config: plan::SourceSinkClusterConfig,
    },
    AlterSource {
        id: GlobalId,
        cluster_config: plan::SourceSinkClusterConfig,
    },
    AlterRole {
        id: RoleId,
        name: String,
        attributes: RoleAttributes,
    },
    CreateDatabase {
        name: String,
        oid: u32,
        public_schema_oid: u32,
        owner_id: RoleId,
    },
    CreateSchema {
        database_id: ResolvedDatabaseSpecifier,
        schema_name: String,
        oid: u32,
        owner_id: RoleId,
    },
    CreateRole {
        name: String,
        oid: u32,
        attributes: RoleAttributes,
    },
    CreateCluster {
        id: ClusterId,
        name: String,
        linked_object_id: Option<GlobalId>,
        introspection_sources: Vec<(&'static BuiltinLog, GlobalId)>,
        owner_id: RoleId,
    },
    CreateClusterReplica {
        cluster_id: ClusterId,
        id: ReplicaId,
        name: String,
        config: ReplicaConfig,
        owner_id: RoleId,
    },
    CreateItem {
        id: GlobalId,
        oid: u32,
        name: QualifiedItemName,
        item: CatalogItem,
        owner_id: RoleId,
    },
    DropObject(ObjectId),
    DropTimeline(Timeline),
    GrantRole {
        role_id: RoleId,
        member_id: RoleId,
        grantor_id: RoleId,
    },
    RenameItem {
        id: GlobalId,
        current_full_name: FullItemName,
        to_name: String,
    },
    UpdateOwner {
        id: ObjectId,
        new_owner: RoleId,
    },
    RevokeRole {
        role_id: RoleId,
        member_id: RoleId,
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
    UpdateRotatedKeys {
        id: GlobalId,
        previous_public_key_pair: (String, String),
        new_public_key_pair: (String, String),
    },
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub enum SerializedCatalogItem {
    V1 { create_sql: String },
}

/// Serialized (stored alongside the replica) logging configuration of
/// a replica. Serialized variant of `ReplicaLogging`.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct SerializedReplicaLogging {
    log_logging: bool,
    interval: Option<Duration>,
}

impl From<ReplicaLogging> for SerializedReplicaLogging {
    fn from(
        ReplicaLogging {
            log_logging,
            interval,
        }: ReplicaLogging,
    ) -> Self {
        Self {
            log_logging,
            interval,
        }
    }
}

/// A [`ReplicaConfig`] that is serialized as JSON and persisted to the catalog
/// stash. This is a separate type to allow us to evolve the on-disk format
/// independently from the SQL layer.
#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq, Ord, PartialOrd)]
pub struct SerializedReplicaConfig {
    pub location: SerializedReplicaLocation,
    pub logging: SerializedReplicaLogging,
    pub idle_arrangement_merge_effort: Option<u32>,
}

impl From<ReplicaConfig> for SerializedReplicaConfig {
    fn from(config: ReplicaConfig) -> Self {
        SerializedReplicaConfig {
            location: config.location.into(),
            logging: config.compute.logging.into(),
            idle_arrangement_merge_effort: config.compute.idle_arrangement_merge_effort,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub enum SerializedReplicaLocation {
    Unmanaged {
        storagectl_addrs: Vec<String>,
        storage_addrs: Vec<String>,
        computectl_addrs: Vec<String>,
        compute_addrs: Vec<String>,
        workers: usize,
    },
    Managed {
        size: String,
        availability_zone: String,
        /// `true` if the AZ was specified by the user and must be respected;
        /// `false` if it was picked arbitrarily by Materialize.
        az_user_specified: bool,
    },
}

impl From<ReplicaLocation> for SerializedReplicaLocation {
    fn from(loc: ReplicaLocation) -> Self {
        match loc {
            ReplicaLocation::Unmanaged(UnmanagedReplicaLocation {
                storagectl_addrs,
                storage_addrs,
                computectl_addrs,
                compute_addrs,
                workers,
            }) => Self::Unmanaged {
                storagectl_addrs,
                storage_addrs,
                computectl_addrs,
                compute_addrs,
                workers,
            },
            ReplicaLocation::Managed(ManagedReplicaLocation {
                allocation: _,
                size,
                availability_zone,
                az_user_specified,
            }) => SerializedReplicaLocation::Managed {
                size,
                availability_zone,
                az_user_specified,
            },
        }
    }
}

/// A [`Role`] that is serialized as JSON and persisted to the catalog
/// stash. This is a separate type to allow us to evolve the on-disk format
/// independently from the SQL layer.
#[derive(Debug, Clone, Serialize, Deserialize, Eq, PartialEq, Ord, PartialOrd)]
pub struct SerializedRole {
    pub name: String,
    // TODO(jkosh44): Remove Option when stash consolidation bug is fixed
    pub attributes: Option<RoleAttributes>,
    // TODO(jkosh44): Remove Option in v0.49.0
    pub membership: Option<RoleMembership>,
}

impl From<Role> for SerializedRole {
    fn from(role: Role) -> Self {
        SerializedRole {
            name: role.name,
            attributes: Some(role.attributes),
            membership: Some(role.membership),
        }
    }
}

impl From<&BuiltinRole> for SerializedRole {
    fn from(role: &BuiltinRole) -> Self {
        SerializedRole {
            name: role.name.to_string(),
            attributes: Some(role.attributes.clone()),
            membership: Some(RoleMembership::new()),
        }
    }
}

impl ConnCatalog<'_> {
    fn resolve_item_name(
        &self,
        name: &PartialItemName,
    ) -> Result<&QualifiedItemName, SqlCatalogError> {
        self.resolve_item(name).map(|entry| entry.name())
    }

    /// returns a `PartialItemName` with the minimum amount of qualifiers to unambiguously resolve
    /// the object.
    fn minimal_qualification(&self, qualified_name: &QualifiedItemName) -> PartialItemName {
        let database_id = match &qualified_name.qualifiers.database_spec {
            ResolvedDatabaseSpecifier::Ambient => None,
            ResolvedDatabaseSpecifier::Id(id)
                if self.database.is_some() && self.database == Some(*id) =>
            {
                None
            }
            ResolvedDatabaseSpecifier::Id(id) => Some(id.clone()),
        };

        let schema_spec = if database_id.is_none()
            && self.resolve_item_name(&PartialItemName {
                database: None,
                schema: None,
                item: qualified_name.item.clone(),
            }) == Ok(qualified_name)
        {
            None
        } else {
            // If `search_path` does not contain `full_name.schema`, the
            // `PartialName` must contain it.
            Some(qualified_name.qualifiers.schema_spec.clone())
        };

        let res = PartialItemName {
            database: database_id.map(|id| self.get_database(&id).name().to_string()),
            schema: schema_spec.map(|spec| {
                self.get_schema(&qualified_name.qualifiers.database_spec, &spec)
                    .name()
                    .schema
                    .clone()
            }),
            item: qualified_name.item.clone(),
        };
        assert_eq!(self.resolve_item_name(&res), Ok(qualified_name));
        res
    }
}

impl ExprHumanizer for ConnCatalog<'_> {
    fn humanize_id(&self, id: GlobalId) -> Option<String> {
        self.state
            .entry_by_id
            .get(&id)
            .map(|entry| entry.name())
            .map(|name| self.resolve_full_name(name).to_string())
    }

    fn humanize_id_unqualified(&self, id: GlobalId) -> Option<String> {
        self.state
            .entry_by_id
            .get(&id)
            .map(|entry| entry.name())
            .map(|name| name.item.clone())
    }

    fn humanize_scalar_type(&self, typ: &ScalarType) -> String {
        use ScalarType::*;

        match typ {
            Array(t) => format!("{}[]", self.humanize_scalar_type(t)),
            List {
                custom_id: Some(global_id),
                ..
            }
            | Map {
                custom_id: Some(global_id),
                ..
            } => {
                let item = self.get_item(global_id);
                self.minimal_qualification(item.name()).to_string()
            }
            List { element_type, .. } => {
                format!("{} list", self.humanize_scalar_type(element_type))
            }
            Map { value_type, .. } => format!(
                "map[{}=>{}]",
                self.humanize_scalar_type(&ScalarType::String),
                self.humanize_scalar_type(value_type)
            ),
            Record {
                custom_id: Some(id),
                ..
            } => {
                let item = self.get_item(id);
                self.minimal_qualification(item.name()).to_string()
            }
            Record { fields, .. } => format!(
                "record({})",
                fields
                    .iter()
                    .map(|f| format!("{}: {}", f.0, self.humanize_column_type(&f.1)))
                    .join(",")
            ),
            PgLegacyChar => "\"char\"".into(),
            UInt16 => "uint2".into(),
            UInt32 => "uint4".into(),
            UInt64 => "uint8".into(),
            ty => {
                let pgrepr_type = mz_pgrepr::Type::from(ty);
                let pg_catalog_schema =
                    SchemaSpecifier::Id(self.state.get_pg_catalog_schema_id().clone());

                let res = if self
                    .effective_search_path(true)
                    .iter()
                    .any(|(_, schema)| schema == &pg_catalog_schema)
                {
                    pgrepr_type.name().to_string()
                } else {
                    // If PG_CATALOG_SCHEMA is not in search path, you need
                    // qualified object name to refer to type.
                    let name = QualifiedItemName {
                        qualifiers: ItemQualifiers {
                            database_spec: ResolvedDatabaseSpecifier::Ambient,
                            schema_spec: pg_catalog_schema,
                        },
                        item: pgrepr_type.name().to_string(),
                    };
                    self.resolve_full_name(&name).to_string()
                };
                res
            }
        }
    }
}

impl SessionCatalog for ConnCatalog<'_> {
    fn active_role_id(&self) -> &RoleId {
        &self.role_id
    }

    fn get_prepared_statement_desc(&self, name: &str) -> Option<&StatementDesc> {
        self.prepared_statements
            .as_ref()
            .map(|ps| ps.get(name).map(|ps| ps.desc()))
            .flatten()
    }

    fn active_database(&self) -> Option<&DatabaseId> {
        self.database.as_ref()
    }

    fn active_cluster(&self) -> &str {
        &self.cluster
    }

    fn search_path(&self) -> &[(ResolvedDatabaseSpecifier, SchemaSpecifier)] {
        &self.search_path
    }

    fn resolve_database(
        &self,
        database_name: &str,
    ) -> Result<&dyn mz_sql::catalog::CatalogDatabase, SqlCatalogError> {
        Ok(self.state.resolve_database(database_name)?)
    }

    fn get_database(&self, id: &DatabaseId) -> &dyn mz_sql::catalog::CatalogDatabase {
        self.state
            .database_by_id
            .get(id)
            .expect("database doesn't exist")
    }

    fn resolve_schema(
        &self,
        database_name: Option<&str>,
        schema_name: &str,
    ) -> Result<&dyn mz_sql::catalog::CatalogSchema, SqlCatalogError> {
        Ok(self.state.resolve_schema(
            self.database.as_ref(),
            database_name,
            schema_name,
            self.conn_id,
        )?)
    }

    fn resolve_schema_in_database(
        &self,
        database_spec: &ResolvedDatabaseSpecifier,
        schema_name: &str,
    ) -> Result<&dyn mz_sql::catalog::CatalogSchema, SqlCatalogError> {
        Ok(self
            .state
            .resolve_schema_in_database(database_spec, schema_name, self.conn_id)?)
    }

    fn get_schema(
        &self,
        database_spec: &ResolvedDatabaseSpecifier,
        schema_spec: &SchemaSpecifier,
    ) -> &dyn CatalogSchema {
        self.state
            .get_schema(database_spec, schema_spec, self.conn_id)
    }

    fn is_system_schema(&self, schema: &str) -> bool {
        self.state.is_system_schema(schema)
    }

    fn is_system_schema_specifier(&self, schema: &SchemaSpecifier) -> bool {
        match schema {
            SchemaSpecifier::Temporary => false,
            SchemaSpecifier::Id(id) => self.state.is_system_schema_id(id),
        }
    }

    fn resolve_role(
        &self,
        role_name: &str,
    ) -> Result<&dyn mz_sql::catalog::CatalogRole, SqlCatalogError> {
        match self.state.try_get_role_by_name(role_name) {
            Some(role) => Ok(role),
            None => Err(SqlCatalogError::UnknownRole(role_name.into())),
        }
    }

    fn try_get_role(&self, id: &RoleId) -> Option<&dyn CatalogRole> {
        Some(self.state.roles_by_id.get(id)?)
    }

    fn get_role(&self, id: &RoleId) -> &dyn mz_sql::catalog::CatalogRole {
        self.state.get_role(id)
    }

    fn collect_role_membership(&self, id: &RoleId) -> BTreeSet<RoleId> {
        self.state.collect_role_membership(id)
    }

    fn resolve_cluster(
        &self,
        cluster_name: Option<&str>,
    ) -> Result<&dyn mz_sql::catalog::CatalogCluster, SqlCatalogError> {
        Ok(self
            .state
            .resolve_cluster(cluster_name.unwrap_or_else(|| self.active_cluster()))?)
    }

    fn resolve_item(
        &self,
        name: &PartialItemName,
    ) -> Result<&dyn mz_sql::catalog::CatalogItem, SqlCatalogError> {
        Ok(self.state.resolve_entry(
            self.database.as_ref(),
            &self.effective_search_path(true),
            name,
            self.conn_id,
        )?)
    }

    fn resolve_function(
        &self,
        name: &PartialItemName,
    ) -> Result<&dyn mz_sql::catalog::CatalogItem, SqlCatalogError> {
        Ok(self.state.resolve_function(
            self.database.as_ref(),
            &self.effective_search_path(false),
            name,
            self.conn_id,
        )?)
    }

    fn try_get_item(&self, id: &GlobalId) -> Option<&dyn mz_sql::catalog::CatalogItem> {
        Some(self.state.try_get_entry(id)?)
    }

    fn get_item(&self, id: &GlobalId) -> &dyn mz_sql::catalog::CatalogItem {
        self.state.get_entry(id)
    }

    fn item_exists(&self, name: &QualifiedItemName) -> bool {
        self.state.item_exists(name, self.conn_id)
    }

    fn get_cluster(&self, id: ClusterId) -> &dyn mz_sql::catalog::CatalogCluster {
        &self.state.clusters_by_id[&id]
    }

    fn get_cluster_replica(
        &self,
        cluster_id: ClusterId,
        replica_id: ReplicaId,
    ) -> &dyn mz_sql::catalog::CatalogClusterReplica {
        let cluster = self.get_cluster(cluster_id);
        cluster.replica(replica_id)
    }

    fn find_available_name(&self, name: QualifiedItemName) -> QualifiedItemName {
        self.state.find_available_name(name, self.conn_id)
    }

    fn resolve_full_name(&self, name: &QualifiedItemName) -> FullItemName {
        self.state.resolve_full_name(name, Some(self.conn_id))
    }

    fn resolve_full_schema_name(&self, name: &QualifiedSchemaName) -> FullSchemaName {
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

    fn config(&self) -> &mz_sql::catalog::CatalogConfig {
        self.state.config()
    }

    fn now(&self) -> EpochMillis {
        (self.state.config().now)()
    }

    fn aws_privatelink_availability_zones(&self) -> Option<BTreeSet<String>> {
        self.state.aws_privatelink_availability_zones.clone()
    }

    fn system_vars(&self) -> &SystemVars {
        &self.state.system_configuration
    }

    fn system_vars_mut(&mut self) -> &mut SystemVars {
        &mut self.state.to_mut().system_configuration
    }

    fn get_owner_id(&self, id: &ObjectId) -> Option<RoleId> {
        match id {
            ObjectId::Cluster(id) => Some(self.get_cluster(*id).owner_id()),
            ObjectId::ClusterReplica((cluster_id, replica_id)) => Some(
                self.get_cluster_replica(*cluster_id, *replica_id)
                    .owner_id(),
            ),
            ObjectId::Database(id) => Some(self.get_database(id).owner_id()),
            ObjectId::Schema((database_id, schema_id)) => Some(
                self.get_schema(
                    &ResolvedDatabaseSpecifier::Id(*database_id),
                    &SchemaSpecifier::Id(*schema_id),
                )
                .owner_id(),
            ),
            ObjectId::Item(id) => Some(self.get_item(id).owner_id()),
            ObjectId::Role(_) => None,
        }
    }

    fn object_dependents(&self, ids: Vec<ObjectId>) -> Vec<ObjectId> {
        let mut seen = BTreeSet::new();
        self.state.object_dependents(ids, &mut seen)
    }

    fn item_dependents(&self, id: GlobalId) -> Vec<ObjectId> {
        let mut seen = BTreeSet::new();
        self.state.item_dependents(id, &mut seen)
    }
}

impl mz_sql::catalog::CatalogDatabase for Database {
    fn name(&self) -> &str {
        &self.name
    }

    fn id(&self) -> DatabaseId {
        self.id
    }

    fn has_schemas(&self) -> bool {
        !self.schemas_by_name.is_empty()
    }

    fn schemas(&self) -> &BTreeMap<String, SchemaId> {
        &self.schemas_by_name
    }

    fn owner_id(&self) -> RoleId {
        self.owner_id
    }
}

impl mz_sql::catalog::CatalogSchema for Schema {
    fn database(&self) -> &ResolvedDatabaseSpecifier {
        &self.name.database
    }

    fn name(&self) -> &QualifiedSchemaName {
        &self.name
    }

    fn id(&self) -> &SchemaSpecifier {
        &self.id
    }

    fn has_items(&self) -> bool {
        !self.items.is_empty()
    }

    fn items(&self) -> &BTreeMap<String, GlobalId> {
        &self.items
    }

    fn owner_id(&self) -> RoleId {
        self.owner_id
    }
}

impl mz_sql::catalog::CatalogRole for Role {
    fn name(&self) -> &str {
        &self.name
    }

    fn id(&self) -> RoleId {
        self.id
    }

    fn is_inherit(&self) -> bool {
        self.attributes.inherit
    }

    fn create_role(&self) -> bool {
        self.attributes.create_role
    }

    fn create_db(&self) -> bool {
        self.attributes.create_db
    }

    fn create_cluster(&self) -> bool {
        self.attributes.create_cluster
    }

    fn membership(&self) -> BTreeSet<&RoleId> {
        self.membership
            .map
            .iter()
            .map(|(role_id, _grantor_id)| role_id)
            .collect()
    }
}

impl mz_sql::catalog::CatalogCluster<'_> for Cluster {
    fn name(&self) -> &str {
        &self.name
    }

    fn id(&self) -> ClusterId {
        self.id
    }

    fn linked_object_id(&self) -> Option<GlobalId> {
        self.linked_object_id
    }

    fn bound_objects(&self) -> &BTreeSet<GlobalId> {
        &self.bound_objects
    }

    fn replicas(&self) -> &BTreeMap<String, ReplicaId> {
        &self.replica_id_by_name
    }

    fn replica(&self, id: ReplicaId) -> &dyn CatalogClusterReplica {
        self.replicas_by_id.get(&id).expect("catalog out of sync")
    }

    fn owner_id(&self) -> RoleId {
        self.owner_id
    }
}

impl mz_sql::catalog::CatalogClusterReplica<'_> for ClusterReplica {
    fn name(&self) -> &str {
        &self.name
    }

    fn owner_id(&self) -> RoleId {
        self.owner_id
    }
}

impl mz_sql::catalog::CatalogItem for CatalogEntry {
    fn name(&self) -> &QualifiedItemName {
        self.name()
    }

    fn id(&self) -> GlobalId {
        self.id()
    }

    fn oid(&self) -> u32 {
        self.oid()
    }

    fn desc(&self, name: &FullItemName) -> Result<Cow<RelationDesc>, SqlCatalogError> {
        self.desc(name)
    }

    fn func(&self) -> Result<&'static mz_sql::func::Func, SqlCatalogError> {
        self.func()
    }

    fn source_desc(&self) -> Result<Option<&SourceDesc>, SqlCatalogError> {
        self.source_desc()
    }

    fn connection(
        &self,
    ) -> Result<&mz_storage_client::types::connections::Connection, SqlCatalogError> {
        Ok(&self.connection()?.connection)
    }

    fn create_sql(&self) -> &str {
        match self.item() {
            CatalogItem::Table(Table { create_sql, .. }) => create_sql,
            CatalogItem::Source(Source { create_sql, .. }) => create_sql,
            CatalogItem::Sink(Sink { create_sql, .. }) => create_sql,
            CatalogItem::View(View { create_sql, .. }) => create_sql,
            CatalogItem::MaterializedView(MaterializedView { create_sql, .. }) => create_sql,
            CatalogItem::Index(Index { create_sql, .. }) => create_sql,
            CatalogItem::Type(Type { create_sql, .. }) => create_sql,
            CatalogItem::Secret(Secret { create_sql, .. }) => create_sql,
            CatalogItem::Connection(Connection { create_sql, .. }) => create_sql,
            CatalogItem::Func(_) => "<builtin>",
            CatalogItem::Log(_) => "<builtin>",
        }
    }

    fn item_type(&self) -> SqlCatalogItemType {
        self.item().typ()
    }

    fn index_details(&self) -> Option<(&[MirScalarExpr], GlobalId)> {
        if let CatalogItem::Index(Index { keys, on, .. }) = self.item() {
            Some((keys, *on))
        } else {
            None
        }
    }

    fn table_details(&self) -> Option<&[Expr<Aug>]> {
        if let CatalogItem::Table(Table { defaults, .. }) = self.item() {
            Some(defaults)
        } else {
            None
        }
    }

    fn type_details(&self) -> Option<&CatalogTypeDetails<IdReference>> {
        if let CatalogItem::Type(Type { details, .. }) = self.item() {
            Some(details)
        } else {
            None
        }
    }

    fn uses(&self) -> &[GlobalId] {
        self.uses()
    }

    fn used_by(&self) -> &[GlobalId] {
        self.used_by()
    }

    fn subsources(&self) -> Vec<GlobalId> {
        self.subsources()
    }

    fn owner_id(&self) -> RoleId {
        self.owner_id
    }
}

#[cfg(test)]
mod tests {
    use itertools::Itertools;
    use std::collections::{BTreeMap, BTreeSet};
    use std::iter;

    use mz_controller::clusters::ClusterId;
    use mz_expr::{MirRelationExpr, OptimizedMirRelationExpr};
    use mz_ore::collections::CollectionExt;
    use mz_ore::now::{NOW_ZERO, SYSTEM_TIME};
    use mz_repr::{GlobalId, RelationDesc, RelationType, ScalarType};
    use mz_sql::catalog::CatalogDatabase;
    use mz_sql::names;
    use mz_sql::names::{
        DatabaseId, ItemQualifiers, PartialItemName, QualifiedItemName, ResolvedDatabaseSpecifier,
        SchemaId, SchemaSpecifier,
    };
    use mz_sql::plan::StatementContext;
    use mz_sql::session::vars::VarInput;
    use mz_sql::DEFAULT_SCHEMA;
    use mz_sql_parser::ast::Expr;
    use mz_stash::DebugStashFactory;

    use crate::catalog::storage::MZ_SYSTEM_ROLE_ID;
    use crate::catalog::{
        Catalog, CatalogItem, Index, MaterializedView, Op, Table, SYSTEM_CONN_ID,
    };
    use crate::session::{Session, DEFAULT_DATABASE_NAME};

    /// System sessions have an empty `search_path` so it's necessary to
    /// schema-qualify all referenced items.
    ///
    /// Dummy (and ostensibly client) sessions contain system schemas in their
    /// search paths, so do not require schema qualification on system objects such
    /// as types.
    #[tokio::test]
    async fn test_minimal_qualification() {
        Catalog::with_debug(NOW_ZERO.clone(), |catalog| async move {
            struct TestCase {
                input: QualifiedItemName,
                system_output: PartialItemName,
                normal_output: PartialItemName,
            }

            let test_cases = vec![
                TestCase {
                    input: QualifiedItemName {
                        qualifiers: ItemQualifiers {
                            database_spec: ResolvedDatabaseSpecifier::Ambient,
                            schema_spec: SchemaSpecifier::Id(
                                catalog.get_pg_catalog_schema_id().clone(),
                            ),
                        },
                        item: "numeric".to_string(),
                    },
                    system_output: PartialItemName {
                        database: None,
                        schema: None,
                        item: "numeric".to_string(),
                    },
                    normal_output: PartialItemName {
                        database: None,
                        schema: None,
                        item: "numeric".to_string(),
                    },
                },
                TestCase {
                    input: QualifiedItemName {
                        qualifiers: ItemQualifiers {
                            database_spec: ResolvedDatabaseSpecifier::Ambient,
                            schema_spec: SchemaSpecifier::Id(
                                catalog.get_mz_catalog_schema_id().clone(),
                            ),
                        },
                        item: "mz_array_types".to_string(),
                    },
                    system_output: PartialItemName {
                        database: None,
                        schema: None,
                        item: "mz_array_types".to_string(),
                    },
                    normal_output: PartialItemName {
                        database: None,
                        schema: None,
                        item: "mz_array_types".to_string(),
                    },
                },
            ];

            for tc in test_cases {
                assert_eq!(
                    catalog
                        .for_system_session()
                        .minimal_qualification(&tc.input),
                    tc.system_output
                );
                assert_eq!(
                    catalog
                        .for_session(&Session::dummy())
                        .minimal_qualification(&tc.input),
                    tc.normal_output
                );
            }
        })
        .await
    }

    #[tokio::test]
    async fn test_catalog_revision() {
        let debug_stash_factory = DebugStashFactory::new().await;
        {
            let stash = debug_stash_factory.open_debug().await;
            let mut catalog = Catalog::open_debug_stash(stash, NOW_ZERO.clone())
                .await
                .expect("unable to open debug catalog");
            assert_eq!(catalog.transient_revision(), 1);
            catalog
                .transact(
                    mz_repr::Timestamp::MIN,
                    None,
                    vec![Op::CreateDatabase {
                        name: "test".to_string(),
                        oid: 1,
                        public_schema_oid: 2,
                        owner_id: MZ_SYSTEM_ROLE_ID,
                    }],
                    |_catalog| Ok(()),
                )
                .await
                .expect("failed to transact");
            assert_eq!(catalog.transient_revision(), 2);
        }
        {
            let stash = debug_stash_factory.open_debug().await;
            let catalog = Catalog::open_debug_stash(stash, NOW_ZERO.clone())
                .await
                .expect("unable to open debug catalog");
            // Re-opening the same stash resets the transient_revision to 1.
            assert_eq!(catalog.transient_revision(), 1);
        }
    }

    #[tokio::test]
    async fn test_effective_search_path() {
        Catalog::with_debug(NOW_ZERO.clone(), |catalog| async move {
            let mz_catalog_schema = (
                ResolvedDatabaseSpecifier::Ambient,
                SchemaSpecifier::Id(catalog.state().get_mz_catalog_schema_id().clone()),
            );
            let pg_catalog_schema = (
                ResolvedDatabaseSpecifier::Ambient,
                SchemaSpecifier::Id(catalog.state().get_pg_catalog_schema_id().clone()),
            );
            let mz_temp_schema = (
                ResolvedDatabaseSpecifier::Ambient,
                SchemaSpecifier::Temporary,
            );

            // Behavior with the default search_schema (public)
            let session = Session::dummy();
            let conn_catalog = catalog.for_session(&session);
            assert_ne!(
                conn_catalog.effective_search_path(false),
                conn_catalog.search_path
            );
            assert_ne!(
                conn_catalog.effective_search_path(true),
                conn_catalog.search_path
            );
            assert_eq!(
                conn_catalog.effective_search_path(false),
                vec![
                    mz_catalog_schema.clone(),
                    pg_catalog_schema.clone(),
                    conn_catalog.search_path[0].clone()
                ]
            );
            assert_eq!(
                conn_catalog.effective_search_path(true),
                vec![
                    mz_temp_schema.clone(),
                    mz_catalog_schema.clone(),
                    pg_catalog_schema.clone(),
                    conn_catalog.search_path[0].clone()
                ]
            );

            // missing schemas are added when missing
            let mut session = Session::dummy();
            session
                .vars_mut()
                .set("search_path", VarInput::Flat("pg_catalog"), false)
                .expect("failed to set search_path");
            let conn_catalog = catalog.for_session(&session);
            assert_ne!(
                conn_catalog.effective_search_path(false),
                conn_catalog.search_path
            );
            assert_ne!(
                conn_catalog.effective_search_path(true),
                conn_catalog.search_path
            );
            assert_eq!(
                conn_catalog.effective_search_path(false),
                vec![mz_catalog_schema.clone(), pg_catalog_schema.clone()]
            );
            assert_eq!(
                conn_catalog.effective_search_path(true),
                vec![
                    mz_temp_schema.clone(),
                    mz_catalog_schema.clone(),
                    pg_catalog_schema.clone()
                ]
            );

            let mut session = Session::dummy();
            session
                .vars_mut()
                .set("search_path", VarInput::Flat("mz_catalog"), false)
                .expect("failed to set search_path");
            let conn_catalog = catalog.for_session(&session);
            assert_ne!(
                conn_catalog.effective_search_path(false),
                conn_catalog.search_path
            );
            assert_ne!(
                conn_catalog.effective_search_path(true),
                conn_catalog.search_path
            );
            assert_eq!(
                conn_catalog.effective_search_path(false),
                vec![pg_catalog_schema.clone(), mz_catalog_schema.clone()]
            );
            assert_eq!(
                conn_catalog.effective_search_path(true),
                vec![
                    mz_temp_schema.clone(),
                    pg_catalog_schema.clone(),
                    mz_catalog_schema.clone()
                ]
            );

            let mut session = Session::dummy();
            session
                .vars_mut()
                .set("search_path", VarInput::Flat("mz_temp"), false)
                .expect("failed to set search_path");
            let conn_catalog = catalog.for_session(&session);
            assert_ne!(
                conn_catalog.effective_search_path(false),
                conn_catalog.search_path
            );
            assert_ne!(
                conn_catalog.effective_search_path(true),
                conn_catalog.search_path
            );
            assert_eq!(
                conn_catalog.effective_search_path(false),
                vec![
                    mz_catalog_schema.clone(),
                    pg_catalog_schema.clone(),
                    mz_temp_schema.clone()
                ]
            );
            assert_eq!(
                conn_catalog.effective_search_path(true),
                vec![mz_catalog_schema, pg_catalog_schema, mz_temp_schema]
            );
        })
        .await
    }

    #[tokio::test]
    async fn test_builtin_migration() {
        enum ItemNamespace {
            System,
            User,
        }

        enum SimplifiedItem {
            Table,
            MaterializedView { depends_on: Vec<String> },
            Index { on: String },
        }

        struct SimplifiedCatalogEntry {
            name: String,
            namespace: ItemNamespace,
            item: SimplifiedItem,
        }

        impl SimplifiedCatalogEntry {
            // A lot of the fields here aren't actually used in the test so we can fill them in with dummy
            // values.
            fn to_catalog_item(
                self,
                id_mapping: &BTreeMap<String, GlobalId>,
            ) -> (String, ItemNamespace, CatalogItem) {
                let item = match self.item {
                    SimplifiedItem::Table => CatalogItem::Table(Table {
                        create_sql: "TODO".to_string(),
                        desc: RelationDesc::empty()
                            .with_column("a", ScalarType::Int32.nullable(true))
                            .with_key(vec![0]),
                        defaults: vec![Expr::null(); 1],
                        conn_id: None,
                        depends_on: vec![],
                        custom_logical_compaction_window: None,
                        is_retained_metrics_object: false,
                    }),
                    SimplifiedItem::MaterializedView { depends_on } => {
                        let table_list = depends_on.iter().join(",");
                        let depends_on = convert_name_vec_to_id_vec(depends_on, id_mapping);
                        CatalogItem::MaterializedView(MaterializedView {
                            create_sql: format!(
                                "CREATE MATERIALIZED VIEW mv AS SELECT * FROM {table_list}"
                            ),
                            optimized_expr: OptimizedMirRelationExpr(MirRelationExpr::Constant {
                                rows: Ok(Vec::new()),
                                typ: RelationType {
                                    column_types: Vec::new(),
                                    keys: Vec::new(),
                                },
                            }),
                            desc: RelationDesc::empty()
                                .with_column("a", ScalarType::Int32.nullable(true))
                                .with_key(vec![0]),
                            depends_on,
                            cluster_id: ClusterId::User(1),
                        })
                    }
                    SimplifiedItem::Index { on } => {
                        let on_id = id_mapping[&on];
                        CatalogItem::Index(Index {
                            create_sql: format!("CREATE INDEX idx ON {on} (a)"),
                            on: on_id,
                            keys: Vec::new(),
                            conn_id: None,
                            depends_on: vec![on_id],
                            cluster_id: ClusterId::User(1),
                            custom_logical_compaction_window: None,
                            is_retained_metrics_object: false,
                        })
                    }
                };
                (self.name, self.namespace, item)
            }
        }

        struct BuiltinMigrationTestCase {
            test_name: &'static str,
            initial_state: Vec<SimplifiedCatalogEntry>,
            migrated_names: Vec<String>,
            expected_previous_sink_names: Vec<String>,
            expected_previous_materialized_view_names: Vec<String>,
            expected_previous_source_names: Vec<String>,
            expected_all_drop_ops: Vec<String>,
            expected_user_drop_ops: Vec<String>,
            expected_all_create_ops: Vec<String>,
            expected_user_create_ops: Vec<String>,
            expected_migrated_system_object_mappings: Vec<String>,
        }

        async fn add_item(
            catalog: &mut Catalog,
            name: String,
            item: CatalogItem,
            item_namespace: ItemNamespace,
        ) -> GlobalId {
            let id = match item_namespace {
                ItemNamespace::User => catalog
                    .allocate_user_id()
                    .await
                    .expect("cannot fail to allocate user ids"),
                ItemNamespace::System => catalog
                    .allocate_system_id()
                    .await
                    .expect("cannot fail to allocate system ids"),
            };
            let oid = catalog
                .allocate_oid()
                .expect("cannot fail to allocate oids");
            let database_id = catalog
                .resolve_database(DEFAULT_DATABASE_NAME)
                .expect("failed to resolve default database")
                .id();
            let database_spec = ResolvedDatabaseSpecifier::Id(database_id);
            let schema_spec = catalog
                .resolve_schema_in_database(&database_spec, DEFAULT_SCHEMA, SYSTEM_CONN_ID)
                .expect("failed to resolve default schemazs")
                .id
                .clone();
            catalog
                .transact(
                    mz_repr::Timestamp::MIN,
                    None,
                    vec![Op::CreateItem {
                        id,
                        oid,
                        name: QualifiedItemName {
                            qualifiers: ItemQualifiers {
                                database_spec,
                                schema_spec,
                            },
                            item: name,
                        },
                        item,
                        owner_id: MZ_SYSTEM_ROLE_ID,
                    }],
                    |_| Ok(()),
                )
                .await
                .expect("failed to transact");
            id
        }

        fn convert_name_vec_to_id_vec(
            name_vec: Vec<String>,
            id_lookup: &BTreeMap<String, GlobalId>,
        ) -> Vec<GlobalId> {
            name_vec.into_iter().map(|name| id_lookup[&name]).collect()
        }

        fn convert_id_vec_to_name_vec(
            id_vec: Vec<GlobalId>,
            name_lookup: &BTreeMap<GlobalId, String>,
        ) -> Vec<String> {
            id_vec
                .into_iter()
                .map(|id| name_lookup[&id].clone())
                .collect()
        }

        let test_cases = vec![
            BuiltinMigrationTestCase {
                test_name: "no_migrations",
                initial_state: vec![SimplifiedCatalogEntry {
                    name: "s1".to_string(),
                    namespace: ItemNamespace::System,
                    item: SimplifiedItem::Table,
                }],
                migrated_names: vec![],
                expected_previous_sink_names: vec![],
                expected_previous_materialized_view_names: vec![],
                expected_previous_source_names: vec![],
                expected_all_drop_ops: vec![],
                expected_user_drop_ops: vec![],
                expected_all_create_ops: vec![],
                expected_user_create_ops: vec![],
                expected_migrated_system_object_mappings: vec![],
            },
            BuiltinMigrationTestCase {
                test_name: "single_migrations",
                initial_state: vec![SimplifiedCatalogEntry {
                    name: "s1".to_string(),
                    namespace: ItemNamespace::System,
                    item: SimplifiedItem::Table,
                }],
                migrated_names: vec!["s1".to_string()],
                expected_previous_sink_names: vec![],
                expected_previous_materialized_view_names: vec![],
                expected_previous_source_names: vec!["s1".to_string()],
                expected_all_drop_ops: vec!["s1".to_string()],
                expected_user_drop_ops: vec![],
                expected_all_create_ops: vec!["s1".to_string()],
                expected_user_create_ops: vec![],
                expected_migrated_system_object_mappings: vec!["s1".to_string()],
            },
            BuiltinMigrationTestCase {
                test_name: "child_migrations",
                initial_state: vec![
                    SimplifiedCatalogEntry {
                        name: "s1".to_string(),
                        namespace: ItemNamespace::System,
                        item: SimplifiedItem::Table,
                    },
                    SimplifiedCatalogEntry {
                        name: "u1".to_string(),
                        namespace: ItemNamespace::User,
                        item: SimplifiedItem::MaterializedView {
                            depends_on: vec!["s1".to_string()],
                        },
                    },
                ],
                migrated_names: vec!["s1".to_string()],
                expected_previous_sink_names: vec![],
                expected_previous_materialized_view_names: vec!["u1".to_string()],
                expected_previous_source_names: vec!["s1".to_string()],
                expected_all_drop_ops: vec!["u1".to_string(), "s1".to_string()],
                expected_user_drop_ops: vec!["u1".to_string()],
                expected_all_create_ops: vec!["s1".to_string(), "u1".to_string()],
                expected_user_create_ops: vec!["u1".to_string()],
                expected_migrated_system_object_mappings: vec!["s1".to_string()],
            },
            BuiltinMigrationTestCase {
                test_name: "multi_child_migrations",
                initial_state: vec![
                    SimplifiedCatalogEntry {
                        name: "s1".to_string(),
                        namespace: ItemNamespace::System,
                        item: SimplifiedItem::Table,
                    },
                    SimplifiedCatalogEntry {
                        name: "u1".to_string(),
                        namespace: ItemNamespace::User,
                        item: SimplifiedItem::MaterializedView {
                            depends_on: vec!["s1".to_string()],
                        },
                    },
                    SimplifiedCatalogEntry {
                        name: "u2".to_string(),
                        namespace: ItemNamespace::User,
                        item: SimplifiedItem::MaterializedView {
                            depends_on: vec!["s1".to_string()],
                        },
                    },
                ],
                migrated_names: vec!["s1".to_string()],
                expected_previous_sink_names: vec![],
                expected_previous_materialized_view_names: vec!["u1".to_string(), "u2".to_string()],
                expected_previous_source_names: vec!["s1".to_string()],
                expected_all_drop_ops: vec!["u1".to_string(), "u2".to_string(), "s1".to_string()],
                expected_user_drop_ops: vec!["u1".to_string(), "u2".to_string()],
                expected_all_create_ops: vec!["s1".to_string(), "u2".to_string(), "u1".to_string()],
                expected_user_create_ops: vec!["u2".to_string(), "u1".to_string()],
                expected_migrated_system_object_mappings: vec!["s1".to_string()],
            },
            BuiltinMigrationTestCase {
                test_name: "topological_sort",
                initial_state: vec![
                    SimplifiedCatalogEntry {
                        name: "s1".to_string(),
                        namespace: ItemNamespace::System,
                        item: SimplifiedItem::Table,
                    },
                    SimplifiedCatalogEntry {
                        name: "s2".to_string(),
                        namespace: ItemNamespace::System,
                        item: SimplifiedItem::Table,
                    },
                    SimplifiedCatalogEntry {
                        name: "u1".to_string(),
                        namespace: ItemNamespace::User,
                        item: SimplifiedItem::MaterializedView {
                            depends_on: vec!["s2".to_string()],
                        },
                    },
                    SimplifiedCatalogEntry {
                        name: "u2".to_string(),
                        namespace: ItemNamespace::User,
                        item: SimplifiedItem::MaterializedView {
                            depends_on: vec!["s1".to_string(), "u1".to_string()],
                        },
                    },
                ],
                migrated_names: vec!["s1".to_string(), "s2".to_string()],
                expected_previous_sink_names: vec![],
                expected_previous_materialized_view_names: vec!["u2".to_string(), "u1".to_string()],
                expected_previous_source_names: vec!["s1".to_string(), "s2".to_string()],
                expected_all_drop_ops: vec![
                    "u2".to_string(),
                    "s1".to_string(),
                    "u1".to_string(),
                    "s2".to_string(),
                ],
                expected_user_drop_ops: vec!["u2".to_string(), "u1".to_string()],
                expected_all_create_ops: vec![
                    "s2".to_string(),
                    "u1".to_string(),
                    "s1".to_string(),
                    "u2".to_string(),
                ],
                expected_user_create_ops: vec!["u1".to_string(), "u2".to_string()],
                expected_migrated_system_object_mappings: vec!["s1".to_string(), "s2".to_string()],
            },
            BuiltinMigrationTestCase {
                test_name: "topological_sort_complex",
                initial_state: vec![
                    SimplifiedCatalogEntry {
                        name: "s273".to_string(),
                        namespace: ItemNamespace::System,
                        item: SimplifiedItem::Table,
                    },
                    SimplifiedCatalogEntry {
                        name: "s322".to_string(),
                        namespace: ItemNamespace::System,
                        item: SimplifiedItem::Table,
                    },
                    SimplifiedCatalogEntry {
                        name: "s317".to_string(),
                        namespace: ItemNamespace::System,
                        item: SimplifiedItem::Table,
                    },
                    SimplifiedCatalogEntry {
                        name: "s349".to_string(),
                        namespace: ItemNamespace::System,
                        item: SimplifiedItem::MaterializedView {
                            depends_on: vec!["s273".to_string()],
                        },
                    },
                    SimplifiedCatalogEntry {
                        name: "s421".to_string(),
                        namespace: ItemNamespace::System,
                        item: SimplifiedItem::MaterializedView {
                            depends_on: vec!["s273".to_string()],
                        },
                    },
                    SimplifiedCatalogEntry {
                        name: "s295".to_string(),
                        namespace: ItemNamespace::System,
                        item: SimplifiedItem::MaterializedView {
                            depends_on: vec!["s273".to_string()],
                        },
                    },
                    SimplifiedCatalogEntry {
                        name: "s296".to_string(),
                        namespace: ItemNamespace::System,
                        item: SimplifiedItem::MaterializedView {
                            depends_on: vec!["s295".to_string()],
                        },
                    },
                    SimplifiedCatalogEntry {
                        name: "s320".to_string(),
                        namespace: ItemNamespace::System,
                        item: SimplifiedItem::MaterializedView {
                            depends_on: vec!["s295".to_string()],
                        },
                    },
                    SimplifiedCatalogEntry {
                        name: "s340".to_string(),
                        namespace: ItemNamespace::System,
                        item: SimplifiedItem::MaterializedView {
                            depends_on: vec!["s295".to_string()],
                        },
                    },
                    SimplifiedCatalogEntry {
                        name: "s318".to_string(),
                        namespace: ItemNamespace::System,
                        item: SimplifiedItem::MaterializedView {
                            depends_on: vec!["s295".to_string()],
                        },
                    },
                    SimplifiedCatalogEntry {
                        name: "s323".to_string(),
                        namespace: ItemNamespace::System,
                        item: SimplifiedItem::MaterializedView {
                            depends_on: vec!["s295".to_string(), "s322".to_string()],
                        },
                    },
                    SimplifiedCatalogEntry {
                        name: "s330".to_string(),
                        namespace: ItemNamespace::System,
                        item: SimplifiedItem::MaterializedView {
                            depends_on: vec!["s318".to_string(), "s317".to_string()],
                        },
                    },
                    SimplifiedCatalogEntry {
                        name: "s321".to_string(),
                        namespace: ItemNamespace::System,
                        item: SimplifiedItem::MaterializedView {
                            depends_on: vec!["s318".to_string()],
                        },
                    },
                    SimplifiedCatalogEntry {
                        name: "s315".to_string(),
                        namespace: ItemNamespace::System,
                        item: SimplifiedItem::MaterializedView {
                            depends_on: vec!["s296".to_string()],
                        },
                    },
                    SimplifiedCatalogEntry {
                        name: "s354".to_string(),
                        namespace: ItemNamespace::System,
                        item: SimplifiedItem::MaterializedView {
                            depends_on: vec!["s296".to_string()],
                        },
                    },
                    SimplifiedCatalogEntry {
                        name: "s327".to_string(),
                        namespace: ItemNamespace::System,
                        item: SimplifiedItem::MaterializedView {
                            depends_on: vec!["s296".to_string()],
                        },
                    },
                    SimplifiedCatalogEntry {
                        name: "s339".to_string(),
                        namespace: ItemNamespace::System,
                        item: SimplifiedItem::MaterializedView {
                            depends_on: vec!["s296".to_string()],
                        },
                    },
                    SimplifiedCatalogEntry {
                        name: "s355".to_string(),
                        namespace: ItemNamespace::System,
                        item: SimplifiedItem::MaterializedView {
                            depends_on: vec!["s315".to_string()],
                        },
                    },
                ],
                migrated_names: vec![
                    "s273".to_string(),
                    "s317".to_string(),
                    "s318".to_string(),
                    "s320".to_string(),
                    "s321".to_string(),
                    "s322".to_string(),
                    "s323".to_string(),
                    "s330".to_string(),
                    "s339".to_string(),
                    "s340".to_string(),
                ],
                expected_previous_sink_names: vec![],
                expected_previous_materialized_view_names: vec![
                    "s349".to_string(),
                    "s421".to_string(),
                    "s355".to_string(),
                    "s315".to_string(),
                    "s354".to_string(),
                    "s327".to_string(),
                    "s339".to_string(),
                    "s296".to_string(),
                    "s320".to_string(),
                    "s340".to_string(),
                    "s330".to_string(),
                    "s321".to_string(),
                    "s318".to_string(),
                    "s323".to_string(),
                    "s295".to_string(),
                ],
                expected_previous_source_names: vec![
                    "s273".to_string(),
                    "s317".to_string(),
                    "s322".to_string(),
                ],
                expected_all_drop_ops: vec![
                    "s349".to_string(),
                    "s421".to_string(),
                    "s355".to_string(),
                    "s315".to_string(),
                    "s354".to_string(),
                    "s327".to_string(),
                    "s339".to_string(),
                    "s296".to_string(),
                    "s320".to_string(),
                    "s340".to_string(),
                    "s330".to_string(),
                    "s321".to_string(),
                    "s318".to_string(),
                    "s323".to_string(),
                    "s295".to_string(),
                    "s273".to_string(),
                    "s317".to_string(),
                    "s322".to_string(),
                ],
                expected_user_drop_ops: vec![],
                expected_all_create_ops: vec![
                    "s322".to_string(),
                    "s317".to_string(),
                    "s273".to_string(),
                    "s295".to_string(),
                    "s323".to_string(),
                    "s318".to_string(),
                    "s321".to_string(),
                    "s330".to_string(),
                    "s340".to_string(),
                    "s320".to_string(),
                    "s296".to_string(),
                    "s339".to_string(),
                    "s327".to_string(),
                    "s354".to_string(),
                    "s315".to_string(),
                    "s355".to_string(),
                    "s421".to_string(),
                    "s349".to_string(),
                ],
                expected_user_create_ops: vec![],
                expected_migrated_system_object_mappings: vec![
                    "s322".to_string(),
                    "s317".to_string(),
                    "s273".to_string(),
                    "s295".to_string(),
                    "s323".to_string(),
                    "s318".to_string(),
                    "s321".to_string(),
                    "s330".to_string(),
                    "s340".to_string(),
                    "s320".to_string(),
                    "s296".to_string(),
                    "s339".to_string(),
                    "s327".to_string(),
                    "s354".to_string(),
                    "s315".to_string(),
                    "s355".to_string(),
                    "s421".to_string(),
                    "s349".to_string(),
                ],
            },
            BuiltinMigrationTestCase {
                test_name: "system_child_migrations",
                initial_state: vec![
                    SimplifiedCatalogEntry {
                        name: "s1".to_string(),
                        namespace: ItemNamespace::System,
                        item: SimplifiedItem::Table,
                    },
                    SimplifiedCatalogEntry {
                        name: "s2".to_string(),
                        namespace: ItemNamespace::System,
                        item: SimplifiedItem::Index {
                            on: "s1".to_string(),
                        },
                    },
                ],
                migrated_names: vec!["s1".to_string()],
                expected_previous_sink_names: vec![],
                expected_previous_materialized_view_names: vec![],
                expected_previous_source_names: vec!["s1".to_string()],
                expected_all_drop_ops: vec!["s2".to_string(), "s1".to_string()],
                expected_user_drop_ops: vec![],
                expected_all_create_ops: vec!["s1".to_string(), "s2".to_string()],
                expected_user_create_ops: vec![],
                expected_migrated_system_object_mappings: vec!["s1".to_string(), "s2".to_string()],
            },
        ];

        for test_case in test_cases {
            Catalog::with_debug(NOW_ZERO.clone(), |mut catalog| async move {
                let mut id_mapping = BTreeMap::new();
                let mut name_mapping = BTreeMap::new();
                for entry in test_case.initial_state {
                    let (name, namespace, item) = entry.to_catalog_item(&id_mapping);
                    let id = add_item(&mut catalog, name.clone(), item, namespace).await;
                    id_mapping.insert(name.clone(), id);
                    name_mapping.insert(id, name);
                }

                let migrated_ids = test_case
                    .migrated_names
                    .into_iter()
                    .map(|name| id_mapping[&name])
                    .collect();
                let id_fingerprint_map: BTreeMap<GlobalId, String> = id_mapping
                    .iter()
                    .filter(|(_name, id)| id.is_system())
                    // We don't use the new fingerprint in this test, so we can just hard code it
                    .map(|(_name, id)| (*id, "".to_string()))
                    .collect();
                let migration_metadata = catalog
                    .generate_builtin_migration_metadata(migrated_ids, id_fingerprint_map)
                    .await
                    .expect("failed to generate builtin migration metadata");

                assert_eq!(
                    convert_id_vec_to_name_vec(migration_metadata.previous_sink_ids, &name_mapping),
                    test_case.expected_previous_sink_names,
                    "{} test failed with wrong previous sink ids",
                    test_case.test_name
                );
                assert_eq!(
                    convert_id_vec_to_name_vec(
                        migration_metadata.previous_materialized_view_ids,
                        &name_mapping
                    ),
                    test_case.expected_previous_materialized_view_names,
                    "{} test failed with wrong previous materialized view ids",
                    test_case.test_name
                );
                assert_eq!(
                    convert_id_vec_to_name_vec(
                        migration_metadata.previous_source_ids,
                        &name_mapping
                    ),
                    test_case.expected_previous_source_names,
                    "{} test failed with wrong previous source ids",
                    test_case.test_name
                );
                assert_eq!(
                    convert_id_vec_to_name_vec(migration_metadata.all_drop_ops, &name_mapping),
                    test_case.expected_all_drop_ops,
                    "{} test failed with wrong all drop ops",
                    test_case.test_name
                );
                assert_eq!(
                    convert_id_vec_to_name_vec(migration_metadata.user_drop_ops, &name_mapping),
                    test_case.expected_user_drop_ops,
                    "{} test failed with wrong user drop ops",
                    test_case.test_name
                );
                assert_eq!(
                    migration_metadata
                        .all_create_ops
                        .into_iter()
                        .map(|(_, _, name, _, _, _)| name.item)
                        .collect::<Vec<_>>(),
                    test_case.expected_all_create_ops,
                    "{} test failed with wrong all create ops",
                    test_case.test_name
                );
                assert_eq!(
                    migration_metadata
                        .user_create_ops
                        .into_iter()
                        .map(|(_, _, name)| name)
                        .collect::<Vec<_>>(),
                    test_case.expected_user_create_ops,
                    "{} test failed with wrong user create ops",
                    test_case.test_name
                );
                assert_eq!(
                    migration_metadata
                        .migrated_system_object_mappings
                        .values()
                        .map(|mapping| mapping.object_name.clone())
                        .collect::<BTreeSet<_>>(),
                    test_case
                        .expected_migrated_system_object_mappings
                        .into_iter()
                        .collect::<BTreeSet<_>>(),
                    "{} test failed with wrong migrated system object mappings",
                    test_case.test_name
                );
            })
            .await
        }
    }

    #[tokio::test]
    async fn test_normalized_create() {
        Catalog::with_debug(NOW_ZERO.clone(), |catalog| {
            let catalog = catalog.for_system_session();
            let scx = &mut StatementContext::new(None, &catalog);

            let parsed = mz_sql_parser::parser::parse_statements(
                "create view public.foo as select 1 as bar",
            )
            .expect("")
            .into_element();

            let (stmt, _) = names::resolve(scx.catalog, parsed).expect("");

            // Ensure that all identifiers are quoted.
            assert_eq!(
                r#"CREATE VIEW "materialize"."public"."foo" AS SELECT 1 AS "bar""#,
                mz_sql::normalize::create_statement(scx, stmt).expect(""),
            );

            async {}
        })
        .await;
    }

    // Test that if a large catalog item is somehow committed, then we can still load the catalog.
    #[tokio::test]
    #[cfg_attr(miri, ignore)] // slow
    async fn test_large_catalog_item() {
        let view_def = "CREATE VIEW \"materialize\".\"public\".\"v\" AS SELECT 1 FROM (SELECT 1";
        let column = ", 1";
        let view_def_size = view_def.bytes().count();
        let column_size = column.bytes().count();
        let column_count =
            (mz_sql_parser::parser::MAX_STATEMENT_BATCH_SIZE - view_def_size) / column_size + 1;
        let columns = iter::repeat(column).take(column_count).join("");
        let create_sql = format!("{view_def}{columns})");
        let create_sql_check = create_sql.clone();
        assert!(mz_sql_parser::parser::parse_statements(&create_sql).is_ok());
        assert!(mz_sql_parser::parser::parse_statements_with_limit(&create_sql).is_err());

        let debug_stash_factory = DebugStashFactory::new().await;
        let id = GlobalId::User(1);
        {
            let stash = debug_stash_factory.open_debug().await;
            let mut catalog = Catalog::open_debug_stash(stash, SYSTEM_TIME.clone())
                .await
                .expect("unable to open debug catalog");
            let item = catalog
                .state()
                .parse_view_item(create_sql)
                .expect("unable to parse view");
            catalog
                .transact(
                    SYSTEM_TIME().into(),
                    None,
                    vec![Op::CreateItem {
                        item,
                        name: QualifiedItemName {
                            qualifiers: ItemQualifiers {
                                database_spec: ResolvedDatabaseSpecifier::Id(DatabaseId::new(1)),
                                schema_spec: SchemaSpecifier::Id(SchemaId::new(3)),
                            },
                            item: "v".to_string(),
                        },
                        oid: 1,
                        id,
                        owner_id: MZ_SYSTEM_ROLE_ID,
                    }],
                    |_catalog| Ok(()),
                )
                .await
                .expect("failed to transact");
        }
        {
            let stash = debug_stash_factory.open_debug().await;
            let catalog = Catalog::open_debug_stash(stash, SYSTEM_TIME.clone())
                .await
                .expect("unable to open debug catalog");
            let view = catalog.get_entry(&id);
            assert_eq!("v", view.name.item);
            match &view.item {
                CatalogItem::View(view) => assert_eq!(create_sql_check, view.create_sql),
                item => panic!("expected view, got {}", item.typ()),
            }
        }
    }
}

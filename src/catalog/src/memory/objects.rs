// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! The current types used by the in-memory Catalog. Many of the objects in this module are
//! extremely similar to the objects found in [`crate::durable::objects`] but in a format that is
//! easier consumed by higher layers.

use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet};
use std::ops::{Deref, DerefMut};
use std::sync::{Arc, LazyLock};
use std::time::Duration;

use chrono::{DateTime, Utc};
use mz_adapter_types::compaction::CompactionWindow;
use mz_adapter_types::connection::ConnectionId;
use mz_compute_client::logging::LogVariant;
use mz_controller::clusters::{ClusterRole, ClusterStatus, ReplicaConfig, ReplicaLogging};
use mz_controller_types::{ClusterId, ReplicaId};
use mz_expr::{MirScalarExpr, OptimizedMirRelationExpr};
use mz_ore::collections::CollectionExt;
use mz_repr::adt::mz_acl_item::{AclMode, MzAclItem, PrivilegeMap};
use mz_repr::network_policy_id::NetworkPolicyId;
use mz_repr::optimize::OptimizerFeatureOverrides;
use mz_repr::refresh_schedule::RefreshSchedule;
use mz_repr::role_id::RoleId;
use mz_repr::{
    CatalogItemId, ColumnName, Diff, GlobalId, RelationDesc, RelationVersion,
    RelationVersionSelector, SqlColumnType, Timestamp, VersionedRelationDesc,
};
use mz_sql::ast::display::AstDisplay;
use mz_sql::ast::{
    ColumnDef, ColumnOption, ColumnOptionDef, ColumnVersioned, Expr, Raw, RawDataType, Statement,
    UnresolvedItemName, Value, WithOptionValue,
};
use mz_sql::catalog::{
    CatalogClusterReplica, CatalogError as SqlCatalogError, CatalogItem as SqlCatalogItem,
    CatalogItemType as SqlCatalogItemType, CatalogItemType, CatalogSchema, CatalogType,
    CatalogTypeDetails, DefaultPrivilegeAclItem, DefaultPrivilegeObject, IdReference,
    RoleAttributes, RoleMembership, RoleVars, SystemObjectType,
};
use mz_sql::names::{
    Aug, CommentObjectId, DatabaseId, DependencyIds, FullItemName, QualifiedItemName,
    QualifiedSchemaName, ResolvedDatabaseSpecifier, ResolvedIds, SchemaId, SchemaSpecifier,
};
use mz_sql::plan::{
    ClusterSchedule, ComputeReplicaConfig, ComputeReplicaIntrospectionConfig, ConnectionDetails,
    CreateClusterManagedPlan, CreateClusterPlan, CreateClusterVariant, CreateSourcePlan,
    HirRelationExpr, NetworkPolicyRule, PlanError, WebhookBodyFormat, WebhookHeaders,
    WebhookValidation,
};
use mz_sql::rbac;
use mz_sql::session::vars::OwnedVarInput;
use mz_storage_client::controller::IntrospectionType;
use mz_storage_types::connections::inline::ReferencedConnection;
use mz_storage_types::sinks::{SinkEnvelope, StorageSinkConnection};
use mz_storage_types::sources::load_generator::LoadGenerator;
use mz_storage_types::sources::{
    GenericSourceConnection, SourceConnection, SourceDesc, SourceEnvelope, SourceExportDataConfig,
    SourceExportDetails, Timeline,
};
use serde::ser::SerializeSeq;
use serde::{Deserialize, Serialize};
use timely::progress::Antichain;
use tracing::debug;

use crate::builtin::{MZ_CATALOG_SERVER_CLUSTER, MZ_SYSTEM_CLUSTER};
use crate::durable;
use crate::durable::objects::item_type;

/// Used to update `self` from the input value while consuming the input value.
pub trait UpdateFrom<T>: From<T> {
    fn update_from(&mut self, from: T);
}

#[derive(Debug, Serialize, Clone, PartialEq, Eq)]
pub struct Database {
    pub name: String,
    pub id: DatabaseId,
    pub oid: u32,
    #[serde(serialize_with = "mz_ore::serde::map_key_to_string")]
    pub schemas_by_id: BTreeMap<SchemaId, Schema>,
    pub schemas_by_name: BTreeMap<String, SchemaId>,
    pub owner_id: RoleId,
    pub privileges: PrivilegeMap,
}

impl From<Database> for durable::Database {
    fn from(database: Database) -> durable::Database {
        durable::Database {
            id: database.id,
            oid: database.oid,
            name: database.name,
            owner_id: database.owner_id,
            privileges: database.privileges.into_all_values().collect(),
        }
    }
}

impl From<durable::Database> for Database {
    fn from(
        durable::Database {
            id,
            oid,
            name,
            owner_id,
            privileges,
        }: durable::Database,
    ) -> Database {
        Database {
            id,
            oid,
            schemas_by_id: BTreeMap::new(),
            schemas_by_name: BTreeMap::new(),
            name,
            owner_id,
            privileges: PrivilegeMap::from_mz_acl_items(privileges),
        }
    }
}

impl UpdateFrom<durable::Database> for Database {
    fn update_from(
        &mut self,
        durable::Database {
            id,
            oid,
            name,
            owner_id,
            privileges,
        }: durable::Database,
    ) {
        self.id = id;
        self.oid = oid;
        self.name = name;
        self.owner_id = owner_id;
        self.privileges = PrivilegeMap::from_mz_acl_items(privileges);
    }
}

#[derive(Debug, Serialize, Clone, PartialEq, Eq)]
pub struct Schema {
    pub name: QualifiedSchemaName,
    pub id: SchemaSpecifier,
    pub oid: u32,
    pub items: BTreeMap<String, CatalogItemId>,
    pub functions: BTreeMap<String, CatalogItemId>,
    pub types: BTreeMap<String, CatalogItemId>,
    pub owner_id: RoleId,
    pub privileges: PrivilegeMap,
}

impl From<Schema> for durable::Schema {
    fn from(schema: Schema) -> durable::Schema {
        durable::Schema {
            id: schema.id.into(),
            oid: schema.oid,
            name: schema.name.schema,
            database_id: schema.name.database.id(),
            owner_id: schema.owner_id,
            privileges: schema.privileges.into_all_values().collect(),
        }
    }
}

impl From<durable::Schema> for Schema {
    fn from(
        durable::Schema {
            id,
            oid,
            name,
            database_id,
            owner_id,
            privileges,
        }: durable::Schema,
    ) -> Schema {
        Schema {
            name: QualifiedSchemaName {
                database: database_id.into(),
                schema: name,
            },
            id: id.into(),
            oid,
            items: BTreeMap::new(),
            functions: BTreeMap::new(),
            types: BTreeMap::new(),
            owner_id,
            privileges: PrivilegeMap::from_mz_acl_items(privileges),
        }
    }
}

impl UpdateFrom<durable::Schema> for Schema {
    fn update_from(
        &mut self,
        durable::Schema {
            id,
            oid,
            name,
            database_id,
            owner_id,
            privileges,
        }: durable::Schema,
    ) {
        self.name = QualifiedSchemaName {
            database: database_id.into(),
            schema: name,
        };
        self.id = id.into();
        self.oid = oid;
        self.owner_id = owner_id;
        self.privileges = PrivilegeMap::from_mz_acl_items(privileges);
    }
}

#[derive(Debug, Serialize, Clone, PartialEq, Eq)]
pub struct Role {
    pub name: String,
    pub id: RoleId,
    pub oid: u32,
    pub attributes: RoleAttributes,
    pub membership: RoleMembership,
    pub vars: RoleVars,
}

impl Role {
    pub fn is_user(&self) -> bool {
        self.id.is_user()
    }

    pub fn vars<'a>(&'a self) -> impl Iterator<Item = (&'a str, &'a OwnedVarInput)> {
        self.vars.map.iter().map(|(name, val)| (name.as_str(), val))
    }
}

impl From<Role> for durable::Role {
    fn from(role: Role) -> durable::Role {
        durable::Role {
            id: role.id,
            oid: role.oid,
            name: role.name,
            attributes: role.attributes,
            membership: role.membership,
            vars: role.vars,
        }
    }
}

impl From<durable::Role> for Role {
    fn from(
        durable::Role {
            id,
            oid,
            name,
            attributes,
            membership,
            vars,
        }: durable::Role,
    ) -> Self {
        Role {
            name,
            id,
            oid,
            attributes,
            membership,
            vars,
        }
    }
}

impl UpdateFrom<durable::Role> for Role {
    fn update_from(
        &mut self,
        durable::Role {
            id,
            oid,
            name,
            attributes,
            membership,
            vars,
        }: durable::Role,
    ) {
        self.id = id;
        self.oid = oid;
        self.name = name;
        self.attributes = attributes;
        self.membership = membership;
        self.vars = vars;
    }
}

#[derive(Debug, Serialize, Clone, PartialEq, Eq)]
pub struct RoleAuth {
    pub role_id: RoleId,
    pub password_hash: Option<String>,
    pub updated_at: u64,
}

impl From<RoleAuth> for durable::RoleAuth {
    fn from(role_auth: RoleAuth) -> durable::RoleAuth {
        durable::RoleAuth {
            role_id: role_auth.role_id,
            password_hash: role_auth.password_hash,
            updated_at: role_auth.updated_at,
        }
    }
}

impl From<durable::RoleAuth> for RoleAuth {
    fn from(
        durable::RoleAuth {
            role_id,
            password_hash,
            updated_at,
        }: durable::RoleAuth,
    ) -> RoleAuth {
        RoleAuth {
            role_id,
            password_hash,
            updated_at,
        }
    }
}

impl UpdateFrom<durable::RoleAuth> for RoleAuth {
    fn update_from(&mut self, from: durable::RoleAuth) {
        self.role_id = from.role_id;
        self.password_hash = from.password_hash;
    }
}

#[derive(Debug, Serialize, Clone, PartialEq)]
pub struct Cluster {
    pub name: String,
    pub id: ClusterId,
    pub config: ClusterConfig,
    #[serde(skip)]
    pub log_indexes: BTreeMap<LogVariant, GlobalId>,
    /// Objects bound to this cluster. Does not include introspection source
    /// indexes.
    pub bound_objects: BTreeSet<CatalogItemId>,
    pub replica_id_by_name_: BTreeMap<String, ReplicaId>,
    #[serde(serialize_with = "mz_ore::serde::map_key_to_string")]
    pub replicas_by_id_: BTreeMap<ReplicaId, ClusterReplica>,
    pub owner_id: RoleId,
    pub privileges: PrivilegeMap,
}

impl Cluster {
    /// The role of the cluster. Currently used to set alert severity.
    pub fn role(&self) -> ClusterRole {
        // NOTE - These roles power monitoring systems. Do not change
        // them without talking to the cloud or observability groups.
        if self.name == MZ_SYSTEM_CLUSTER.name {
            ClusterRole::SystemCritical
        } else if self.name == MZ_CATALOG_SERVER_CLUSTER.name {
            ClusterRole::System
        } else {
            ClusterRole::User
        }
    }

    /// Returns `true` if the cluster is a managed cluster.
    pub fn is_managed(&self) -> bool {
        matches!(self.config.variant, ClusterVariant::Managed { .. })
    }

    /// Lists the user replicas, which are those that do not have the internal flag set.
    pub fn user_replicas(&self) -> impl Iterator<Item = &ClusterReplica> {
        self.replicas().filter(|r| !r.config.location.internal())
    }

    /// Lists all replicas in the cluster
    pub fn replicas(&self) -> impl Iterator<Item = &ClusterReplica> {
        self.replicas_by_id_.values()
    }

    /// Lookup a replica by ID.
    pub fn replica(&self, replica_id: ReplicaId) -> Option<&ClusterReplica> {
        self.replicas_by_id_.get(&replica_id)
    }

    /// Lookup a replica ID by name.
    pub fn replica_id(&self, name: &str) -> Option<ReplicaId> {
        self.replica_id_by_name_.get(name).copied()
    }

    /// Returns the availability zones of this cluster, if they exist.
    pub fn availability_zones(&self) -> Option<&[String]> {
        match &self.config.variant {
            ClusterVariant::Managed(managed) => Some(&managed.availability_zones),
            ClusterVariant::Unmanaged => None,
        }
    }

    pub fn try_to_plan(&self) -> Result<CreateClusterPlan, PlanError> {
        let name = self.name.clone();
        let variant = match &self.config.variant {
            ClusterVariant::Managed(ClusterVariantManaged {
                size,
                availability_zones,
                logging,
                replication_factor,
                optimizer_feature_overrides,
                schedule,
            }) => {
                let introspection = match logging {
                    ReplicaLogging {
                        log_logging,
                        interval: Some(interval),
                    } => Some(ComputeReplicaIntrospectionConfig {
                        debugging: *log_logging,
                        interval: interval.clone(),
                    }),
                    ReplicaLogging {
                        log_logging: _,
                        interval: None,
                    } => None,
                };
                let compute = ComputeReplicaConfig { introspection };
                CreateClusterVariant::Managed(CreateClusterManagedPlan {
                    replication_factor: replication_factor.clone(),
                    size: size.clone(),
                    availability_zones: availability_zones.clone(),
                    compute,
                    optimizer_feature_overrides: optimizer_feature_overrides.clone(),
                    schedule: schedule.clone(),
                })
            }
            ClusterVariant::Unmanaged => {
                // Unmanaged clusters are deprecated, so hopefully we can remove
                // them before we have to implement this.
                return Err(PlanError::Unsupported {
                    feature: "SHOW CREATE for unmanaged clusters".to_string(),
                    discussion_no: None,
                });
            }
        };
        let workload_class = self.config.workload_class.clone();
        Ok(CreateClusterPlan {
            name,
            variant,
            workload_class,
        })
    }
}

impl From<Cluster> for durable::Cluster {
    fn from(cluster: Cluster) -> durable::Cluster {
        durable::Cluster {
            id: cluster.id,
            name: cluster.name,
            owner_id: cluster.owner_id,
            privileges: cluster.privileges.into_all_values().collect(),
            config: cluster.config.into(),
        }
    }
}

impl From<durable::Cluster> for Cluster {
    fn from(
        durable::Cluster {
            id,
            name,
            owner_id,
            privileges,
            config,
        }: durable::Cluster,
    ) -> Self {
        Cluster {
            name: name.clone(),
            id,
            bound_objects: BTreeSet::new(),
            log_indexes: BTreeMap::new(),
            replica_id_by_name_: BTreeMap::new(),
            replicas_by_id_: BTreeMap::new(),
            owner_id,
            privileges: PrivilegeMap::from_mz_acl_items(privileges),
            config: config.into(),
        }
    }
}

impl UpdateFrom<durable::Cluster> for Cluster {
    fn update_from(
        &mut self,
        durable::Cluster {
            id,
            name,
            owner_id,
            privileges,
            config,
        }: durable::Cluster,
    ) {
        self.id = id;
        self.name = name;
        self.owner_id = owner_id;
        self.privileges = PrivilegeMap::from_mz_acl_items(privileges);
        self.config = config.into();
    }
}

#[derive(Debug, Serialize, Clone, PartialEq)]
pub struct ClusterReplica {
    pub name: String,
    pub cluster_id: ClusterId,
    pub replica_id: ReplicaId,
    pub config: ReplicaConfig,
    pub owner_id: RoleId,
}

impl From<ClusterReplica> for durable::ClusterReplica {
    fn from(replica: ClusterReplica) -> durable::ClusterReplica {
        durable::ClusterReplica {
            cluster_id: replica.cluster_id,
            replica_id: replica.replica_id,
            name: replica.name,
            config: replica.config.into(),
            owner_id: replica.owner_id,
        }
    }
}

#[derive(Debug, Serialize, Clone, PartialEq, Eq)]
pub struct ClusterReplicaProcessStatus {
    pub status: ClusterStatus,
    pub time: DateTime<Utc>,
}

#[derive(Debug, Serialize, Clone, PartialEq)]
pub struct SourceReferences {
    pub updated_at: u64,
    pub references: Vec<SourceReference>,
}

#[derive(Debug, Serialize, Clone, PartialEq)]
pub struct SourceReference {
    pub name: String,
    pub namespace: Option<String>,
    pub columns: Vec<String>,
}

impl From<SourceReference> for durable::SourceReference {
    fn from(source_reference: SourceReference) -> durable::SourceReference {
        durable::SourceReference {
            name: source_reference.name,
            namespace: source_reference.namespace,
            columns: source_reference.columns,
        }
    }
}

impl SourceReferences {
    pub fn to_durable(self, source_id: CatalogItemId) -> durable::SourceReferences {
        durable::SourceReferences {
            source_id,
            updated_at: self.updated_at,
            references: self.references.into_iter().map(Into::into).collect(),
        }
    }
}

impl From<durable::SourceReference> for SourceReference {
    fn from(source_reference: durable::SourceReference) -> SourceReference {
        SourceReference {
            name: source_reference.name,
            namespace: source_reference.namespace,
            columns: source_reference.columns,
        }
    }
}

impl From<durable::SourceReferences> for SourceReferences {
    fn from(source_references: durable::SourceReferences) -> SourceReferences {
        SourceReferences {
            updated_at: source_references.updated_at,
            references: source_references
                .references
                .into_iter()
                .map(|source_reference| source_reference.into())
                .collect(),
        }
    }
}

impl From<mz_sql::plan::SourceReference> for SourceReference {
    fn from(source_reference: mz_sql::plan::SourceReference) -> SourceReference {
        SourceReference {
            name: source_reference.name,
            namespace: source_reference.namespace,
            columns: source_reference.columns,
        }
    }
}

impl From<mz_sql::plan::SourceReferences> for SourceReferences {
    fn from(source_references: mz_sql::plan::SourceReferences) -> SourceReferences {
        SourceReferences {
            updated_at: source_references.updated_at,
            references: source_references
                .references
                .into_iter()
                .map(|source_reference| source_reference.into())
                .collect(),
        }
    }
}

impl From<SourceReferences> for mz_sql::plan::SourceReferences {
    fn from(source_references: SourceReferences) -> mz_sql::plan::SourceReferences {
        mz_sql::plan::SourceReferences {
            updated_at: source_references.updated_at,
            references: source_references
                .references
                .into_iter()
                .map(|source_reference| source_reference.into())
                .collect(),
        }
    }
}

impl From<SourceReference> for mz_sql::plan::SourceReference {
    fn from(source_reference: SourceReference) -> mz_sql::plan::SourceReference {
        mz_sql::plan::SourceReference {
            name: source_reference.name,
            namespace: source_reference.namespace,
            columns: source_reference.columns,
        }
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct CatalogEntry {
    pub item: CatalogItem,
    #[serde(skip)]
    pub referenced_by: Vec<CatalogItemId>,
    // TODO(database-issues#7922)––this should have an invariant tied to it that all
    // dependents (i.e. entries in this field) have IDs greater than this
    // entry's ID.
    #[serde(skip)]
    pub used_by: Vec<CatalogItemId>,
    pub id: CatalogItemId,
    pub oid: u32,
    pub name: QualifiedItemName,
    pub owner_id: RoleId,
    pub privileges: PrivilegeMap,
}

/// A [`CatalogEntry`] that is associated with a specific "collection" of data.
/// A single item in the catalog may be associated with multiple "collections".
///
/// Here "collection" generally means a pTVC, e.g. a Persist Shard, an Index, a
/// currently running dataflow, etc.
///
/// Items in the Catalog have a stable name -> ID mapping, in other words for
/// the entire lifetime of an object its [`CatalogItemId`] will _never_ change.
/// Similarly, we need to maintain a stable mapping from [`GlobalId`] to pTVC.
/// This presents a challenge when `ALTER`-ing an object, e.g. adding columns
/// to a table. We can't just change the schema of the underlying Persist Shard
/// because that would be rebinding the [`GlobalId`] of the pTVC. Instead we
/// allocate a new [`GlobalId`] to refer to the new version of the table, and
/// then the [`CatalogEntry`] tracks the [`GlobalId`] for each version.
///
/// TODO(ct): Add a note here if we end up using this for associating continual
/// tasks with a single catalog item.
#[derive(Clone, Debug)]
pub struct CatalogCollectionEntry {
    pub entry: CatalogEntry,
    pub version: RelationVersionSelector,
}

impl CatalogCollectionEntry {
    pub fn relation_desc(&self) -> Option<Cow<'_, RelationDesc>> {
        self.item().relation_desc(self.version)
    }
}

impl mz_sql::catalog::CatalogCollectionItem for CatalogCollectionEntry {
    fn relation_desc(&self) -> Option<Cow<'_, RelationDesc>> {
        self.item().relation_desc(self.version)
    }

    fn global_id(&self) -> GlobalId {
        self.entry
            .item()
            .global_id_for_version(self.version)
            .expect("catalog corruption, missing version!")
    }
}

impl Deref for CatalogCollectionEntry {
    type Target = CatalogEntry;

    fn deref(&self) -> &CatalogEntry {
        &self.entry
    }
}

impl mz_sql::catalog::CatalogItem for CatalogCollectionEntry {
    fn name(&self) -> &QualifiedItemName {
        self.entry.name()
    }

    fn id(&self) -> CatalogItemId {
        self.entry.id()
    }

    fn global_ids(&self) -> Box<dyn Iterator<Item = GlobalId> + '_> {
        Box::new(self.entry.global_ids())
    }

    fn oid(&self) -> u32 {
        self.entry.oid()
    }

    fn func(&self) -> Result<&'static mz_sql::func::Func, SqlCatalogError> {
        self.entry.func()
    }

    fn source_desc(&self) -> Result<Option<&SourceDesc<ReferencedConnection>>, SqlCatalogError> {
        self.entry.source_desc()
    }

    fn connection(
        &self,
    ) -> Result<mz_storage_types::connections::Connection<ReferencedConnection>, SqlCatalogError>
    {
        mz_sql::catalog::CatalogItem::connection(&self.entry)
    }

    fn create_sql(&self) -> &str {
        self.entry.create_sql()
    }

    fn item_type(&self) -> SqlCatalogItemType {
        self.entry.item_type()
    }

    fn index_details(&self) -> Option<(&[MirScalarExpr], GlobalId)> {
        self.entry.index_details()
    }

    fn writable_table_details(&self) -> Option<&[Expr<Aug>]> {
        self.entry.writable_table_details()
    }

    fn replacement_target(&self) -> Option<CatalogItemId> {
        self.entry.replacement_target()
    }

    fn type_details(&self) -> Option<&CatalogTypeDetails<IdReference>> {
        self.entry.type_details()
    }

    fn references(&self) -> &ResolvedIds {
        self.entry.references()
    }

    fn uses(&self) -> BTreeSet<CatalogItemId> {
        self.entry.uses()
    }

    fn referenced_by(&self) -> &[CatalogItemId] {
        self.entry.referenced_by()
    }

    fn used_by(&self) -> &[CatalogItemId] {
        self.entry.used_by()
    }

    fn subsource_details(
        &self,
    ) -> Option<(CatalogItemId, &UnresolvedItemName, &SourceExportDetails)> {
        self.entry.subsource_details()
    }

    fn source_export_details(
        &self,
    ) -> Option<(
        CatalogItemId,
        &UnresolvedItemName,
        &SourceExportDetails,
        &SourceExportDataConfig<ReferencedConnection>,
    )> {
        self.entry.source_export_details()
    }

    fn is_progress_source(&self) -> bool {
        self.entry.is_progress_source()
    }

    fn progress_id(&self) -> Option<CatalogItemId> {
        self.entry.progress_id()
    }

    fn owner_id(&self) -> RoleId {
        *self.entry.owner_id()
    }

    fn privileges(&self) -> &PrivilegeMap {
        self.entry.privileges()
    }

    fn cluster_id(&self) -> Option<ClusterId> {
        self.entry.item().cluster_id()
    }

    fn at_version(
        &self,
        version: RelationVersionSelector,
    ) -> Box<dyn mz_sql::catalog::CatalogCollectionItem> {
        Box::new(CatalogCollectionEntry {
            entry: self.entry.clone(),
            version,
        })
    }

    fn latest_version(&self) -> Option<RelationVersion> {
        self.entry.latest_version()
    }
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
    ContinualTask(ContinualTask),
}

impl From<CatalogEntry> for durable::Item {
    fn from(entry: CatalogEntry) -> durable::Item {
        let (create_sql, global_id, extra_versions) = entry.item.into_serialized();
        durable::Item {
            id: entry.id,
            oid: entry.oid,
            global_id,
            schema_id: entry.name.qualifiers.schema_spec.into(),
            name: entry.name.item,
            create_sql,
            owner_id: entry.owner_id,
            privileges: entry.privileges.into_all_values().collect(),
            extra_versions,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct Table {
    /// Parse-able SQL that defines this table.
    pub create_sql: Option<String>,
    /// [`VersionedRelationDesc`] of this table, derived from the `create_sql`.
    pub desc: VersionedRelationDesc,
    /// Versions of this table, and the [`GlobalId`]s that refer to them.
    #[serde(serialize_with = "mz_ore::serde::map_key_to_string")]
    pub collections: BTreeMap<RelationVersion, GlobalId>,
    /// If created in the `TEMPORARY` schema, the [`ConnectionId`] for that session.
    #[serde(skip)]
    pub conn_id: Option<ConnectionId>,
    /// Other catalog objects referenced by this table, e.g. custom types.
    pub resolved_ids: ResolvedIds,
    /// Custom compaction window, e.g. set via `ALTER RETAIN HISTORY`.
    pub custom_logical_compaction_window: Option<CompactionWindow>,
    /// Whether the table's logical compaction window is controlled by the ['metrics_retention']
    /// session variable.
    ///
    /// ['metrics_retention']: mz_sql::session::vars::METRICS_RETENTION
    pub is_retained_metrics_object: bool,
    /// Where data for this table comes from, e.g. `INSERT` statements or an upstream source.
    pub data_source: TableDataSource,
}

impl Table {
    pub fn timeline(&self) -> Timeline {
        match &self.data_source {
            // The Coordinator controls insertions for writable tables
            // (including system tables), so they are realtime.
            TableDataSource::TableWrites { .. } => Timeline::EpochMilliseconds,
            TableDataSource::DataSource { timeline, .. } => timeline.clone(),
        }
    }

    /// Returns all of the [`GlobalId`]s that this [`Table`] can be referenced by.
    pub fn global_ids(&self) -> impl Iterator<Item = GlobalId> + '_ {
        self.collections.values().copied()
    }

    /// Returns the latest [`GlobalId`] for this [`Table`] which should be used for writes.
    pub fn global_id_writes(&self) -> GlobalId {
        *self
            .collections
            .last_key_value()
            .expect("at least one version of a table")
            .1
    }

    /// Returns all of the collections and their [`RelationDesc`]s associated with this [`Table`].
    pub fn collection_descs(
        &self,
    ) -> impl Iterator<Item = (GlobalId, RelationVersion, RelationDesc)> + '_ {
        self.collections.iter().map(|(version, gid)| {
            let desc = self
                .desc
                .at_version(RelationVersionSelector::Specific(*version));
            (*gid, *version, desc)
        })
    }

    /// Returns the [`RelationDesc`] for a specific [`GlobalId`].
    pub fn desc_for(&self, id: &GlobalId) -> RelationDesc {
        let (version, _gid) = self
            .collections
            .iter()
            .find(|(_version, gid)| *gid == id)
            .expect("GlobalId to exist");
        self.desc
            .at_version(RelationVersionSelector::Specific(*version))
    }
}

#[derive(Clone, Debug, Serialize)]
pub enum TableDataSource {
    /// The table owns data created via INSERT/UPDATE/DELETE statements.
    TableWrites {
        #[serde(skip)]
        defaults: Vec<Expr<Aug>>,
    },

    /// The table receives its data from the identified `DataSourceDesc`.
    /// This table type does not support INSERT/UPDATE/DELETE statements.
    DataSource {
        desc: DataSourceDesc,
        timeline: Timeline,
    },
}

#[derive(Debug, Clone, Serialize)]
pub enum DataSourceDesc {
    /// Receives data from an external system
    Ingestion {
        desc: SourceDesc<ReferencedConnection>,
        cluster_id: ClusterId,
    },
    /// Receives data from an external system
    OldSyntaxIngestion {
        desc: SourceDesc<ReferencedConnection>,
        cluster_id: ClusterId,
        // If we're dealing with an old syntax ingestion the progress id will be some other collection
        // and the ingestion itself will have the data from an external reference
        progress_subsource: CatalogItemId,
        data_config: SourceExportDataConfig<ReferencedConnection>,
        details: SourceExportDetails,
    },
    /// This source receives its data from the identified ingestion,
    /// specifically the output identified by `external_reference`.
    /// N.B. that `external_reference` should not be used to identify
    /// anything downstream of purification, as the purification process
    /// encodes source-specific identifiers into the `details` struct.
    /// The `external_reference` field is only used here for displaying
    /// human-readable names in system tables.
    IngestionExport {
        ingestion_id: CatalogItemId,
        external_reference: UnresolvedItemName,
        details: SourceExportDetails,
        data_config: SourceExportDataConfig<ReferencedConnection>,
    },
    /// Receives introspection data from an internal system
    Introspection(IntrospectionType),
    /// Receives data from the source's reclocking/remapping operations.
    Progress,
    /// Receives data from HTTP requests.
    Webhook {
        /// Optional components used to validation a webhook request.
        validate_using: Option<WebhookValidation>,
        /// Describes how we deserialize the body of a webhook request.
        body_format: WebhookBodyFormat,
        /// Describes whether or not to include headers and how to map them.
        headers: WebhookHeaders,
        /// The cluster which this source is associated with.
        cluster_id: ClusterId,
    },
}

impl DataSourceDesc {
    /// The key and value formats of the data source.
    pub fn formats(&self) -> (Option<&str>, Option<&str>) {
        match &self {
            DataSourceDesc::Ingestion { .. } => (None, None),
            DataSourceDesc::OldSyntaxIngestion { data_config, .. } => {
                match &data_config.encoding.as_ref() {
                    Some(encoding) => match &encoding.key {
                        Some(key) => (Some(key.type_()), Some(encoding.value.type_())),
                        None => (None, Some(encoding.value.type_())),
                    },
                    None => (None, None),
                }
            }
            DataSourceDesc::IngestionExport { data_config, .. } => match &data_config.encoding {
                Some(encoding) => match &encoding.key {
                    Some(key) => (Some(key.type_()), Some(encoding.value.type_())),
                    None => (None, Some(encoding.value.type_())),
                },
                None => (None, None),
            },
            DataSourceDesc::Introspection(_)
            | DataSourceDesc::Webhook { .. }
            | DataSourceDesc::Progress => (None, None),
        }
    }

    /// Envelope of the data source.
    pub fn envelope(&self) -> Option<&str> {
        // Note how "none"/"append-only" is different from `None`. Source
        // sources don't have an envelope (internal logs, for example), while
        // other sources have an envelope that we call the "NONE"-envelope.

        fn envelope_string(envelope: &SourceEnvelope) -> &str {
            match envelope {
                SourceEnvelope::None(_) => "none",
                SourceEnvelope::Upsert(upsert_envelope) => match upsert_envelope.style {
                    mz_storage_types::sources::envelope::UpsertStyle::Default(_) => "upsert",
                    mz_storage_types::sources::envelope::UpsertStyle::Debezium { .. } => {
                        // NOTE(aljoscha): Should we somehow mark that this is
                        // using upsert internally? See note above about
                        // DEBEZIUM.
                        "debezium"
                    }
                    mz_storage_types::sources::envelope::UpsertStyle::ValueErrInline { .. } => {
                        "upsert-value-err-inline"
                    }
                },
                SourceEnvelope::CdcV2 => {
                    // TODO(aljoscha): Should we even report this? It's
                    // currently not exposed.
                    "materialize"
                }
            }
        }

        match self {
            // NOTE(aljoscha): We could move the block for ingestions into
            // `SourceEnvelope` itself, but that one feels more like an internal
            // thing and adapter should own how we represent envelopes as a
            // string? It would not be hard to convince me otherwise, though.
            DataSourceDesc::Ingestion { .. } => None,
            DataSourceDesc::OldSyntaxIngestion { data_config, .. } => {
                Some(envelope_string(&data_config.envelope))
            }
            DataSourceDesc::IngestionExport { data_config, .. } => {
                Some(envelope_string(&data_config.envelope))
            }
            DataSourceDesc::Introspection(_)
            | DataSourceDesc::Webhook { .. }
            | DataSourceDesc::Progress => None,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct Source {
    /// Parse-able SQL that defines this table.
    pub create_sql: Option<String>,
    /// [`GlobalId`] used to reference this source from outside the catalog.
    pub global_id: GlobalId,
    // TODO: Unskip: currently blocked on some inner BTreeMap<X, _> problems.
    #[serde(skip)]
    pub data_source: DataSourceDesc,
    /// [`RelationDesc`] of this source, derived from the `create_sql`.
    pub desc: RelationDesc,
    /// The timeline this source exists on.
    pub timeline: Timeline,
    /// Other catalog objects referenced by this table, e.g. custom types.
    pub resolved_ids: ResolvedIds,
    /// This value is ignored for subsources, i.e. for
    /// [`DataSourceDesc::IngestionExport`]. Instead, it uses the primary
    /// sources logical compaction window.
    pub custom_logical_compaction_window: Option<CompactionWindow>,
    /// Whether the source's logical compaction window is controlled by
    /// METRICS_RETENTION
    pub is_retained_metrics_object: bool,
}

impl Source {
    /// Creates a new `Source`.
    ///
    /// # Panics
    /// - If an ingestion-based plan is not given a cluster_id.
    /// - If a non-ingestion-based source has a defined cluster config in its plan.
    /// - If a non-ingestion-based source is given a cluster_id.
    pub fn new(
        plan: CreateSourcePlan,
        global_id: GlobalId,
        resolved_ids: ResolvedIds,
        custom_logical_compaction_window: Option<CompactionWindow>,
        is_retained_metrics_object: bool,
    ) -> Source {
        Source {
            create_sql: Some(plan.source.create_sql),
            data_source: match plan.source.data_source {
                mz_sql::plan::DataSourceDesc::Ingestion(desc) => DataSourceDesc::Ingestion {
                    desc,
                    cluster_id: plan
                        .in_cluster
                        .expect("ingestion-based sources must be given a cluster ID"),
                },
                mz_sql::plan::DataSourceDesc::OldSyntaxIngestion {
                    desc,
                    progress_subsource,
                    data_config,
                    details,
                } => DataSourceDesc::OldSyntaxIngestion {
                    desc,
                    cluster_id: plan
                        .in_cluster
                        .expect("ingestion-based sources must be given a cluster ID"),
                    progress_subsource,
                    data_config,
                    details,
                },
                mz_sql::plan::DataSourceDesc::Progress => {
                    assert!(
                        plan.in_cluster.is_none(),
                        "subsources must not have a host config or cluster_id defined"
                    );
                    DataSourceDesc::Progress
                }
                mz_sql::plan::DataSourceDesc::IngestionExport {
                    ingestion_id,
                    external_reference,
                    details,
                    data_config,
                } => {
                    assert!(
                        plan.in_cluster.is_none(),
                        "subsources must not have a host config or cluster_id defined"
                    );
                    DataSourceDesc::IngestionExport {
                        ingestion_id,
                        external_reference,
                        details,
                        data_config,
                    }
                }
                mz_sql::plan::DataSourceDesc::Webhook {
                    validate_using,
                    body_format,
                    headers,
                    cluster_id,
                } => {
                    mz_ore::soft_assert_or_log!(
                        cluster_id.is_none(),
                        "cluster_id set at Source level for Webhooks"
                    );
                    DataSourceDesc::Webhook {
                        validate_using,
                        body_format,
                        headers,
                        cluster_id: plan
                            .in_cluster
                            .expect("webhook sources must be given a cluster ID"),
                    }
                }
            },
            desc: plan.source.desc,
            global_id,
            timeline: plan.timeline,
            resolved_ids,
            custom_logical_compaction_window: plan
                .source
                .compaction_window
                .or(custom_logical_compaction_window),
            is_retained_metrics_object,
        }
    }

    /// Type of the source.
    pub fn source_type(&self) -> &str {
        match &self.data_source {
            DataSourceDesc::Ingestion { desc, .. }
            | DataSourceDesc::OldSyntaxIngestion { desc, .. } => desc.connection.name(),
            DataSourceDesc::Progress => "progress",
            DataSourceDesc::IngestionExport { .. } => "subsource",
            DataSourceDesc::Introspection(_) => "source",
            DataSourceDesc::Webhook { .. } => "webhook",
        }
    }

    /// Connection ID of the source, if one exists.
    pub fn connection_id(&self) -> Option<CatalogItemId> {
        match &self.data_source {
            DataSourceDesc::Ingestion { desc, .. }
            | DataSourceDesc::OldSyntaxIngestion { desc, .. } => desc.connection.connection_id(),
            DataSourceDesc::IngestionExport { .. }
            | DataSourceDesc::Introspection(_)
            | DataSourceDesc::Webhook { .. }
            | DataSourceDesc::Progress => None,
        }
    }

    /// The single [`GlobalId`] that refers to this Source.
    pub fn global_id(&self) -> GlobalId {
        self.global_id
    }

    /// The expensive resource that each source consumes is persist shards. To
    /// prevent abuse, we want to prevent users from creating sources that use an
    /// unbounded number of persist shards. But we also don't want to count
    /// persist shards that are mandated by teh system (e.g., the progress
    /// shard) so that future versions of Materialize can introduce additional
    /// per-source shards (e.g., a per-source status shard) without impacting
    /// the limit calculation.
    pub fn user_controllable_persist_shard_count(&self) -> i64 {
        match &self.data_source {
            DataSourceDesc::Ingestion { .. } => 0,
            DataSourceDesc::OldSyntaxIngestion { desc, .. } => {
                match &desc.connection {
                    // These multi-output sources do not use their primary
                    // source's data shard, so we don't include it in accounting
                    // for users.
                    GenericSourceConnection::Postgres(_)
                    | GenericSourceConnection::MySql(_)
                    | GenericSourceConnection::SqlServer(_) => 0,
                    GenericSourceConnection::LoadGenerator(lg) => match lg.load_generator {
                        // Load generators that output data in their primary shard
                        LoadGenerator::Clock
                        | LoadGenerator::Counter { .. }
                        | LoadGenerator::Datums
                        | LoadGenerator::KeyValue(_) => 1,
                        LoadGenerator::Auction
                        | LoadGenerator::Marketing
                        | LoadGenerator::Tpch { .. } => 0,
                    },
                    GenericSourceConnection::Kafka(_) => 1,
                }
            }
            //  DataSourceDesc::IngestionExport represents a subsource, which
            //  use a data shard.
            DataSourceDesc::IngestionExport { .. } => 1,
            DataSourceDesc::Webhook { .. } => 1,
            // Introspection and progress subsources are not under the user's control, so shouldn't
            // count toward their quota.
            DataSourceDesc::Introspection(_) | DataSourceDesc::Progress => 0,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct Log {
    /// The category of data this log stores.
    pub variant: LogVariant,
    /// [`GlobalId`] used to reference this log from outside the catalog.
    pub global_id: GlobalId,
}

impl Log {
    /// The single [`GlobalId`] that refers to this Log.
    pub fn global_id(&self) -> GlobalId {
        self.global_id
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct Sink {
    /// Parse-able SQL that defines this sink.
    pub create_sql: String,
    /// [`GlobalId`] used to reference this sink from outside the catalog, e.g storage.
    pub global_id: GlobalId,
    /// Collection we read into this sink.
    pub from: GlobalId,
    /// Connection to the external service we're sinking into, e.g. Kafka.
    pub connection: StorageSinkConnection<ReferencedConnection>,
    /// Envelope we use to sink into the external system.
    ///
    /// TODO(guswynn): this probably should just be in the `connection`.
    pub envelope: SinkEnvelope,
    /// Emit an initial snapshot into the sink.
    pub with_snapshot: bool,
    /// Used to fence other writes into this sink as we evolve the upstream materialized view.
    pub version: u64,
    /// Other catalog objects this sink references.
    pub resolved_ids: ResolvedIds,
    /// Cluster this sink runs on.
    pub cluster_id: ClusterId,
    /// Commit interval for the sink.
    pub commit_interval: Option<Duration>,
}

impl Sink {
    pub fn sink_type(&self) -> &str {
        self.connection.name()
    }

    /// Envelope of the sink.
    pub fn envelope(&self) -> Option<&str> {
        match &self.envelope {
            SinkEnvelope::Debezium => Some("debezium"),
            SinkEnvelope::Upsert => Some("upsert"),
        }
    }

    /// Output a combined format string of the sink. For legacy reasons
    /// if the key-format is none or the key & value formats are
    /// both the same (either avro or json), we return the value format name,
    /// otherwise we return a composite name.
    pub fn combined_format(&self) -> Option<Cow<'_, str>> {
        match &self.connection {
            StorageSinkConnection::Kafka(connection) => Some(connection.format.get_format_name()),
            _ => None,
        }
    }

    /// Output distinct key_format and value_format of the sink.
    pub fn formats(&self) -> Option<(Option<&str>, &str)> {
        match &self.connection {
            StorageSinkConnection::Kafka(connection) => {
                let key_format = connection
                    .format
                    .key_format
                    .as_ref()
                    .map(|f| f.get_format_name());
                let value_format = connection.format.value_format.get_format_name();
                Some((key_format, value_format))
            }
            _ => None,
        }
    }

    pub fn connection_id(&self) -> Option<CatalogItemId> {
        self.connection.connection_id()
    }

    /// The single [`GlobalId`] that this Sink can be referenced by.
    pub fn global_id(&self) -> GlobalId {
        self.global_id
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct View {
    /// Parse-able SQL that defines this view.
    pub create_sql: String,
    /// [`GlobalId`] used to reference this view from outside the catalog, e.g. compute.
    pub global_id: GlobalId,
    /// Unoptimized high-level expression from parsing the `create_sql`.
    pub raw_expr: Arc<HirRelationExpr>,
    /// Optimized mid-level expression from (locally) optimizing the `raw_expr`.
    pub optimized_expr: Arc<OptimizedMirRelationExpr>,
    /// Columns of this view.
    pub desc: RelationDesc,
    /// If created in the `TEMPORARY` schema, the [`ConnectionId`] for that session.
    pub conn_id: Option<ConnectionId>,
    /// Other catalog objects that are referenced by this view, determined at name resolution.
    pub resolved_ids: ResolvedIds,
    /// All of the catalog objects that are referenced by this view.
    pub dependencies: DependencyIds,
}

impl View {
    /// The single [`GlobalId`] this [`View`] can be referenced by.
    pub fn global_id(&self) -> GlobalId {
        self.global_id
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct MaterializedView {
    /// Parse-able SQL that defines this materialized view.
    pub create_sql: String,
    /// Versions of this materialized view, and the [`GlobalId`]s that refer to them.
    #[serde(serialize_with = "mz_ore::serde::map_key_to_string")]
    pub collections: BTreeMap<RelationVersion, GlobalId>,
    /// Raw high-level expression from planning, derived from the `create_sql`.
    pub raw_expr: Arc<HirRelationExpr>,
    /// Optimized mid-level expression, derived from the `raw_expr`.
    pub optimized_expr: Arc<OptimizedMirRelationExpr>,
    /// [`VersionedRelationDesc`] of this materialized view, derived from the `create_sql`.
    pub desc: VersionedRelationDesc,
    /// Other catalog items that this materialized view references, determined at name resolution.
    pub resolved_ids: ResolvedIds,
    /// All of the catalog objects that are referenced by this view.
    pub dependencies: DependencyIds,
    /// ID of the materialized view this materialized view is intended to replace.
    pub replacement_target: Option<CatalogItemId>,
    /// Cluster that this materialized view runs on.
    pub cluster_id: ClusterId,
    /// Column indexes that we assert are not `NULL`.
    ///
    /// TODO(parkmycar): Switch this to use the `ColumnIdx` type.
    pub non_null_assertions: Vec<usize>,
    /// Custom compaction window, e.g. set via `ALTER RETAIN HISTORY`.
    pub custom_logical_compaction_window: Option<CompactionWindow>,
    /// Schedule to refresh this materialized view, e.g. set via `REFRESH EVERY` option.
    pub refresh_schedule: Option<RefreshSchedule>,
    /// The initial `as_of` of the storage collection associated with the materialized view.
    ///
    /// Note: This doesn't change upon restarts.
    /// (The dataflow's initial `as_of` can be different.)
    pub initial_as_of: Option<Antichain<mz_repr::Timestamp>>,
}

impl MaterializedView {
    /// Returns all [`GlobalId`]s that this [`MaterializedView`] can be referenced by.
    pub fn global_ids(&self) -> impl Iterator<Item = GlobalId> + '_ {
        self.collections.values().copied()
    }

    /// The latest [`GlobalId`] for this [`MaterializedView`] which represents the writing
    /// version.
    pub fn global_id_writes(&self) -> GlobalId {
        *self
            .collections
            .last_key_value()
            .expect("at least one version of a materialized view")
            .1
    }

    /// Returns all collections and their [`RelationDesc`]s associated with this [`MaterializedView`].
    pub fn collection_descs(
        &self,
    ) -> impl Iterator<Item = (GlobalId, RelationVersion, RelationDesc)> + '_ {
        self.collections.iter().map(|(version, gid)| {
            let desc = self
                .desc
                .at_version(RelationVersionSelector::Specific(*version));
            (*gid, *version, desc)
        })
    }

    /// Returns the [`RelationDesc`] for a specific [`GlobalId`].
    pub fn desc_for(&self, id: &GlobalId) -> RelationDesc {
        let (version, _gid) = self
            .collections
            .iter()
            .find(|(_version, gid)| *gid == id)
            .expect("GlobalId to exist");
        self.desc
            .at_version(RelationVersionSelector::Specific(*version))
    }

    /// Apply the given replacement materialized view to this [`MaterializedView`].
    pub fn apply_replacement(&mut self, replacement: Self) {
        let target_id = replacement
            .replacement_target
            .expect("replacement has target");

        fn parse(create_sql: &str) -> mz_sql::ast::CreateMaterializedViewStatement<Raw> {
            let res = mz_sql::parse::parse(create_sql).unwrap_or_else(|e| {
                panic!("invalid create_sql persisted in catalog: {e}\n{create_sql}");
            });
            if let Statement::CreateMaterializedView(cmvs) = res.into_element().ast {
                cmvs
            } else {
                panic!("invalid MV create_sql persisted in catalog\n{create_sql}");
            }
        }

        let old_stmt = parse(&self.create_sql);
        let rpl_stmt = parse(&replacement.create_sql);
        let new_stmt = mz_sql::ast::CreateMaterializedViewStatement {
            if_exists: old_stmt.if_exists,
            name: old_stmt.name,
            columns: rpl_stmt.columns,
            replacing: None,
            in_cluster: rpl_stmt.in_cluster,
            query: rpl_stmt.query,
            as_of: rpl_stmt.as_of,
            with_options: rpl_stmt.with_options,
        };
        let create_sql = new_stmt.to_ast_string_stable();

        let mut collections = std::mem::take(&mut self.collections);
        // Note: We can't use `self.desc.latest_version` here because a replacement doesn't
        // necessary evolve the relation schema, so that version might be lower than the actual
        // latest version.
        let latest_version = collections.keys().max().expect("at least one version");
        let new_version = latest_version.bump();
        collections.insert(new_version, replacement.global_id_writes());

        let mut resolved_ids = replacement.resolved_ids;
        resolved_ids.remove_item(&target_id);
        let mut dependencies = replacement.dependencies;
        dependencies.0.remove(&target_id);

        *self = Self {
            create_sql,
            collections,
            raw_expr: replacement.raw_expr,
            optimized_expr: replacement.optimized_expr,
            desc: replacement.desc,
            resolved_ids,
            dependencies,
            replacement_target: None,
            cluster_id: replacement.cluster_id,
            non_null_assertions: replacement.non_null_assertions,
            custom_logical_compaction_window: replacement.custom_logical_compaction_window,
            refresh_schedule: replacement.refresh_schedule,
            initial_as_of: replacement.initial_as_of,
        };
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct Index {
    /// Parse-able SQL that defines this table.
    pub create_sql: String,
    /// [`GlobalId`] used to reference this index from outside the catalog, e.g. compute.
    pub global_id: GlobalId,
    /// The [`GlobalId`] this Index is on.
    pub on: GlobalId,
    /// Keys of the index.
    pub keys: Arc<[MirScalarExpr]>,
    /// If created in the `TEMPORARY` schema, the [`ConnectionId`] for that session.
    pub conn_id: Option<ConnectionId>,
    /// Other catalog objects referenced by this index, e.g. the object we're indexing.
    pub resolved_ids: ResolvedIds,
    /// Cluster this index is installed on.
    pub cluster_id: ClusterId,
    /// Custom compaction window, e.g. set via `ALTER RETAIN HISTORY`.
    pub custom_logical_compaction_window: Option<CompactionWindow>,
    /// Whether the table's logical compaction window is controlled by the ['metrics_retention']
    /// session variable.
    ///
    /// ['metrics_retention']: mz_sql::session::vars::METRICS_RETENTION
    pub is_retained_metrics_object: bool,
}

impl Index {
    /// The [`GlobalId`] that refers to this Index.
    pub fn global_id(&self) -> GlobalId {
        self.global_id
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct Type {
    /// Parse-able SQL that defines this type.
    pub create_sql: Option<String>,
    /// [`GlobalId`] used to reference this type from outside the catalog.
    pub global_id: GlobalId,
    #[serde(skip)]
    pub details: CatalogTypeDetails<IdReference>,
    /// Other catalog objects referenced by this type.
    pub resolved_ids: ResolvedIds,
}

#[derive(Debug, Clone, Serialize)]
pub struct Func {
    /// Static definition of the function.
    #[serde(skip)]
    pub inner: &'static mz_sql::func::Func,
    /// [`GlobalId`] used to reference this function from outside the catalog.
    pub global_id: GlobalId,
}

#[derive(Debug, Clone, Serialize)]
pub struct Secret {
    /// Parse-able SQL that defines this secret.
    pub create_sql: String,
    /// [`GlobalId`] used to reference this secret from outside the catalog.
    pub global_id: GlobalId,
}

#[derive(Debug, Clone, Serialize)]
pub struct Connection {
    /// Parse-able SQL that defines this connection.
    pub create_sql: String,
    /// [`GlobalId`] used to reference this connection from the storage layer.
    pub global_id: GlobalId,
    /// The kind of connection.
    pub details: ConnectionDetails,
    /// Other objects this connection depends on.
    pub resolved_ids: ResolvedIds,
}

impl Connection {
    /// The single [`GlobalId`] used to reference this connection.
    pub fn global_id(&self) -> GlobalId {
        self.global_id
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct ContinualTask {
    /// Parse-able SQL that defines this continual task.
    pub create_sql: String,
    /// [`GlobalId`] used to reference this continual task from outside the catalog.
    pub global_id: GlobalId,
    /// [`GlobalId`] of the collection that we read into this continual task.
    pub input_id: GlobalId,
    pub with_snapshot: bool,
    /// ContinualTasks are self-referential. We make this work by using a
    /// placeholder `LocalId` for the CT itself through name resolution and
    /// planning. Then we fill in the real `GlobalId` before constructing this
    /// catalog item.
    pub raw_expr: Arc<HirRelationExpr>,
    /// Columns for this continual task.
    pub desc: RelationDesc,
    /// Other catalog items that this continual task references, determined at name resolution.
    pub resolved_ids: ResolvedIds,
    /// All of the catalog objects that are referenced by this continual task.
    pub dependencies: DependencyIds,
    /// Cluster that this continual task runs on.
    pub cluster_id: ClusterId,
    /// See the comment on [MaterializedView::initial_as_of].
    pub initial_as_of: Option<Antichain<mz_repr::Timestamp>>,
}

impl ContinualTask {
    /// The single [`GlobalId`] used to reference this continual task.
    pub fn global_id(&self) -> GlobalId {
        self.global_id
    }
}

#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct NetworkPolicy {
    pub name: String,
    pub id: NetworkPolicyId,
    pub oid: u32,
    pub rules: Vec<NetworkPolicyRule>,
    pub owner_id: RoleId,
    pub privileges: PrivilegeMap,
}

impl From<NetworkPolicy> for durable::NetworkPolicy {
    fn from(policy: NetworkPolicy) -> durable::NetworkPolicy {
        durable::NetworkPolicy {
            id: policy.id,
            oid: policy.oid,
            name: policy.name,
            rules: policy.rules,
            owner_id: policy.owner_id,
            privileges: policy.privileges.into_all_values().collect(),
        }
    }
}

impl From<durable::NetworkPolicy> for NetworkPolicy {
    fn from(
        durable::NetworkPolicy {
            id,
            oid,
            name,
            rules,
            owner_id,
            privileges,
        }: durable::NetworkPolicy,
    ) -> Self {
        NetworkPolicy {
            id,
            oid,
            name,
            rules,
            owner_id,
            privileges: PrivilegeMap::from_mz_acl_items(privileges),
        }
    }
}

impl UpdateFrom<durable::NetworkPolicy> for NetworkPolicy {
    fn update_from(
        &mut self,
        durable::NetworkPolicy {
            id,
            oid,
            name,
            rules,
            owner_id,
            privileges,
        }: durable::NetworkPolicy,
    ) {
        self.id = id;
        self.oid = oid;
        self.name = name;
        self.rules = rules;
        self.owner_id = owner_id;
        self.privileges = PrivilegeMap::from_mz_acl_items(privileges);
    }
}

impl CatalogItem {
    /// Returns a string indicating the type of this catalog entry.
    pub fn typ(&self) -> mz_sql::catalog::CatalogItemType {
        match self {
            CatalogItem::Table(_) => CatalogItemType::Table,
            CatalogItem::Source(_) => CatalogItemType::Source,
            CatalogItem::Log(_) => CatalogItemType::Source,
            CatalogItem::Sink(_) => CatalogItemType::Sink,
            CatalogItem::View(_) => CatalogItemType::View,
            CatalogItem::MaterializedView(_) => CatalogItemType::MaterializedView,
            CatalogItem::Index(_) => CatalogItemType::Index,
            CatalogItem::Type(_) => CatalogItemType::Type,
            CatalogItem::Func(_) => CatalogItemType::Func,
            CatalogItem::Secret(_) => CatalogItemType::Secret,
            CatalogItem::Connection(_) => CatalogItemType::Connection,
            CatalogItem::ContinualTask(_) => CatalogItemType::ContinualTask,
        }
    }

    /// Returns the [`GlobalId`]s that reference this item, if any.
    pub fn global_ids(&self) -> impl Iterator<Item = GlobalId> + '_ {
        let gid = match self {
            CatalogItem::Source(source) => source.global_id,
            CatalogItem::Log(log) => log.global_id,
            CatalogItem::Sink(sink) => sink.global_id,
            CatalogItem::View(view) => view.global_id,
            CatalogItem::MaterializedView(mv) => {
                return itertools::Either::Left(mv.collections.values().copied());
            }
            CatalogItem::ContinualTask(ct) => ct.global_id,
            CatalogItem::Index(index) => index.global_id,
            CatalogItem::Func(func) => func.global_id,
            CatalogItem::Type(ty) => ty.global_id,
            CatalogItem::Secret(secret) => secret.global_id,
            CatalogItem::Connection(conn) => conn.global_id,
            CatalogItem::Table(table) => {
                return itertools::Either::Left(table.collections.values().copied());
            }
        };
        itertools::Either::Right(std::iter::once(gid))
    }

    /// Returns the most up-to-date [`GlobalId`] for this item.
    ///
    /// Note: The only type of object that can have multiple [`GlobalId`]s are tables.
    pub fn latest_global_id(&self) -> GlobalId {
        match self {
            CatalogItem::Source(source) => source.global_id,
            CatalogItem::Log(log) => log.global_id,
            CatalogItem::Sink(sink) => sink.global_id,
            CatalogItem::View(view) => view.global_id,
            CatalogItem::MaterializedView(mv) => mv.global_id_writes(),
            CatalogItem::ContinualTask(ct) => ct.global_id,
            CatalogItem::Index(index) => index.global_id,
            CatalogItem::Func(func) => func.global_id,
            CatalogItem::Type(ty) => ty.global_id,
            CatalogItem::Secret(secret) => secret.global_id,
            CatalogItem::Connection(conn) => conn.global_id,
            CatalogItem::Table(table) => table.global_id_writes(),
        }
    }

    /// Whether this item represents a storage collection.
    pub fn is_storage_collection(&self) -> bool {
        match self {
            CatalogItem::Table(_)
            | CatalogItem::Source(_)
            | CatalogItem::MaterializedView(_)
            | CatalogItem::Sink(_)
            | CatalogItem::ContinualTask(_) => true,
            CatalogItem::Log(_)
            | CatalogItem::View(_)
            | CatalogItem::Index(_)
            | CatalogItem::Type(_)
            | CatalogItem::Func(_)
            | CatalogItem::Secret(_)
            | CatalogItem::Connection(_) => false,
        }
    }

    /// Returns the [`RelationDesc`] for items that yield rows, at the requested
    /// version.
    ///
    /// Some item types honor `version` so callers can ask for the schema that
    /// matches a specific [`GlobalId`] or historical definition. Other relation
    /// types ignore `version` because they have a single shape. Non-relational
    /// items ( for example functions, indexes, sinks, secrets, and connections)
    /// return `None`.
    pub fn relation_desc(&self, version: RelationVersionSelector) -> Option<Cow<'_, RelationDesc>> {
        match &self {
            CatalogItem::Source(src) => Some(Cow::Borrowed(&src.desc)),
            CatalogItem::Log(log) => Some(Cow::Owned(log.variant.desc())),
            CatalogItem::Table(tbl) => Some(Cow::Owned(tbl.desc.at_version(version))),
            CatalogItem::View(view) => Some(Cow::Borrowed(&view.desc)),
            CatalogItem::MaterializedView(mview) => {
                Some(Cow::Owned(mview.desc.at_version(version)))
            }
            CatalogItem::ContinualTask(ct) => Some(Cow::Borrowed(&ct.desc)),
            CatalogItem::Func(_)
            | CatalogItem::Index(_)
            | CatalogItem::Sink(_)
            | CatalogItem::Secret(_)
            | CatalogItem::Connection(_)
            | CatalogItem::Type(_) => None,
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
    ) -> Result<Option<&SourceDesc<ReferencedConnection>>, SqlCatalogError> {
        match &self {
            CatalogItem::Source(source) => match &source.data_source {
                DataSourceDesc::Ingestion { desc, .. }
                | DataSourceDesc::OldSyntaxIngestion { desc, .. } => Ok(Some(desc)),
                DataSourceDesc::IngestionExport { .. }
                | DataSourceDesc::Introspection(_)
                | DataSourceDesc::Webhook { .. }
                | DataSourceDesc::Progress => Ok(None),
            },
            _ => Err(SqlCatalogError::UnexpectedType {
                name: entry.name().item.to_string(),
                actual_type: entry.item_type(),
                expected_type: CatalogItemType::Source,
            }),
        }
    }

    /// Reports whether this catalog entry is a progress source.
    pub fn is_progress_source(&self) -> bool {
        matches!(
            self,
            CatalogItem::Source(Source {
                data_source: DataSourceDesc::Progress,
                ..
            })
        )
    }

    /// Collects the identifiers of the objects that were encountered when resolving names in the
    /// item's DDL statement.
    pub fn references(&self) -> &ResolvedIds {
        static EMPTY: LazyLock<ResolvedIds> = LazyLock::new(ResolvedIds::empty);
        match self {
            CatalogItem::Func(_) => &*EMPTY,
            CatalogItem::Index(idx) => &idx.resolved_ids,
            CatalogItem::Sink(sink) => &sink.resolved_ids,
            CatalogItem::Source(source) => &source.resolved_ids,
            CatalogItem::Log(_) => &*EMPTY,
            CatalogItem::Table(table) => &table.resolved_ids,
            CatalogItem::Type(typ) => &typ.resolved_ids,
            CatalogItem::View(view) => &view.resolved_ids,
            CatalogItem::MaterializedView(mview) => &mview.resolved_ids,
            CatalogItem::Secret(_) => &*EMPTY,
            CatalogItem::Connection(connection) => &connection.resolved_ids,
            CatalogItem::ContinualTask(ct) => &ct.resolved_ids,
        }
    }

    /// Collects the identifiers of the objects used by this [`CatalogItem`].
    ///
    /// Like [`CatalogItem::references()`] but also includes objects that are not directly
    /// referenced. For example this will include any catalog objects used to implement functions
    /// and casts in the item.
    pub fn uses(&self) -> BTreeSet<CatalogItemId> {
        let mut uses: BTreeSet<_> = self.references().items().copied().collect();
        match self {
            // TODO(jkosh44) This isn't really correct for functions. They may use other objects in
            // their implementation. However, currently there's no way to get that information.
            CatalogItem::Func(_) => {}
            CatalogItem::Index(_) => {}
            CatalogItem::Sink(_) => {}
            CatalogItem::Source(_) => {}
            CatalogItem::Log(_) => {}
            CatalogItem::Table(_) => {}
            CatalogItem::Type(_) => {}
            CatalogItem::View(view) => uses.extend(view.dependencies.0.iter().copied()),
            CatalogItem::MaterializedView(mview) => {
                uses.extend(mview.dependencies.0.iter().copied())
            }
            CatalogItem::ContinualTask(ct) => uses.extend(ct.dependencies.0.iter().copied()),
            CatalogItem::Secret(_) => {}
            CatalogItem::Connection(_) => {}
        }
        uses
    }

    /// Returns the connection ID that this item belongs to, if this item is
    /// temporary.
    pub fn conn_id(&self) -> Option<&ConnectionId> {
        match self {
            CatalogItem::View(view) => view.conn_id.as_ref(),
            CatalogItem::Index(index) => index.conn_id.as_ref(),
            CatalogItem::Table(table) => table.conn_id.as_ref(),
            CatalogItem::Log(_)
            | CatalogItem::Source(_)
            | CatalogItem::Sink(_)
            | CatalogItem::MaterializedView(_)
            | CatalogItem::Secret(_)
            | CatalogItem::Type(_)
            | CatalogItem::Func(_)
            | CatalogItem::Connection(_)
            | CatalogItem::ContinualTask(_) => None,
        }
    }

    /// Sets the connection ID that this item belongs to, which makes it a
    /// temporary item.
    pub fn set_conn_id(&mut self, conn_id: Option<ConnectionId>) {
        match self {
            CatalogItem::View(view) => view.conn_id = conn_id,
            CatalogItem::Index(index) => index.conn_id = conn_id,
            CatalogItem::Table(table) => table.conn_id = conn_id,
            CatalogItem::Log(_)
            | CatalogItem::Source(_)
            | CatalogItem::Sink(_)
            | CatalogItem::MaterializedView(_)
            | CatalogItem::Secret(_)
            | CatalogItem::Type(_)
            | CatalogItem::Func(_)
            | CatalogItem::Connection(_)
            | CatalogItem::ContinualTask(_) => (),
        }
    }

    /// Indicates whether this item is temporary or not.
    pub fn is_temporary(&self) -> bool {
        self.conn_id().is_some()
    }

    pub fn rename_schema_refs(
        &self,
        database_name: &str,
        cur_schema_name: &str,
        new_schema_name: &str,
    ) -> Result<CatalogItem, (String, String)> {
        let do_rewrite = |create_sql: String| -> Result<String, (String, String)> {
            let mut create_stmt = mz_sql::parse::parse(&create_sql)
                .expect("invalid create sql persisted to catalog")
                .into_element()
                .ast;

            // Rename all references to cur_schema_name.
            mz_sql::ast::transform::create_stmt_rename_schema_refs(
                &mut create_stmt,
                database_name,
                cur_schema_name,
                new_schema_name,
            )?;

            Ok(create_stmt.to_ast_string_stable())
        };

        match self {
            CatalogItem::Table(i) => {
                let mut i = i.clone();
                i.create_sql = i.create_sql.map(do_rewrite).transpose()?;
                Ok(CatalogItem::Table(i))
            }
            CatalogItem::Log(i) => Ok(CatalogItem::Log(i.clone())),
            CatalogItem::Source(i) => {
                let mut i = i.clone();
                i.create_sql = i.create_sql.map(do_rewrite).transpose()?;
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
            CatalogItem::Connection(i) => {
                let mut i = i.clone();
                i.create_sql = do_rewrite(i.create_sql)?;
                Ok(CatalogItem::Connection(i))
            }
            CatalogItem::Type(i) => {
                let mut i = i.clone();
                i.create_sql = i.create_sql.map(do_rewrite).transpose()?;
                Ok(CatalogItem::Type(i))
            }
            CatalogItem::Func(i) => Ok(CatalogItem::Func(i.clone())),
            CatalogItem::ContinualTask(i) => {
                let mut i = i.clone();
                i.create_sql = do_rewrite(i.create_sql)?;
                Ok(CatalogItem::ContinualTask(i))
            }
        }
    }

    /// Returns a clone of `self` with all instances of `from` renamed to `to`
    /// (with the option of including the item's own name) or errors if request
    /// is ambiguous.
    pub fn rename_item_refs(
        &self,
        from: FullItemName,
        to_item_name: String,
        rename_self: bool,
    ) -> Result<CatalogItem, String> {
        let do_rewrite = |create_sql: String| -> Result<String, String> {
            let mut create_stmt = mz_sql::parse::parse(&create_sql)
                .expect("invalid create sql persisted to catalog")
                .into_element()
                .ast;
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
                i.create_sql = i.create_sql.map(do_rewrite).transpose()?;
                Ok(CatalogItem::Table(i))
            }
            CatalogItem::Log(i) => Ok(CatalogItem::Log(i.clone())),
            CatalogItem::Source(i) => {
                let mut i = i.clone();
                i.create_sql = i.create_sql.map(do_rewrite).transpose()?;
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
            CatalogItem::ContinualTask(i) => {
                let mut i = i.clone();
                i.create_sql = do_rewrite(i.create_sql)?;
                Ok(CatalogItem::ContinualTask(i))
            }
        }
    }

    /// Updates the retain history for an item. Returns the previous retain history value. Returns
    /// an error if this item does not support retain history.
    pub fn update_retain_history(
        &mut self,
        value: Option<Value>,
        window: CompactionWindow,
    ) -> Result<Option<WithOptionValue<Raw>>, ()> {
        let update = |mut ast: &mut Statement<Raw>| {
            // Each statement type has unique option types. This macro handles them commonly.
            macro_rules! update_retain_history {
                ( $stmt:ident, $opt:ident, $name:ident ) => {{
                    // Replace or add the option.
                    let pos = $stmt
                        .with_options
                        .iter()
                        // In case there are ever multiple, look for the last one.
                        .rposition(|o| o.name == mz_sql_parser::ast::$name::RetainHistory);
                    if let Some(value) = value {
                        let next = mz_sql_parser::ast::$opt {
                            name: mz_sql_parser::ast::$name::RetainHistory,
                            value: Some(WithOptionValue::RetainHistoryFor(value)),
                        };
                        if let Some(idx) = pos {
                            let previous = $stmt.with_options[idx].clone();
                            $stmt.with_options[idx] = next;
                            previous.value
                        } else {
                            $stmt.with_options.push(next);
                            None
                        }
                    } else {
                        if let Some(idx) = pos {
                            $stmt.with_options.swap_remove(idx).value
                        } else {
                            None
                        }
                    }
                }};
            }
            let previous = match &mut ast {
                Statement::CreateTable(stmt) => {
                    update_retain_history!(stmt, TableOption, TableOptionName)
                }
                Statement::CreateIndex(stmt) => {
                    update_retain_history!(stmt, IndexOption, IndexOptionName)
                }
                Statement::CreateSource(stmt) => {
                    update_retain_history!(stmt, CreateSourceOption, CreateSourceOptionName)
                }
                Statement::CreateMaterializedView(stmt) => {
                    update_retain_history!(stmt, MaterializedViewOption, MaterializedViewOptionName)
                }
                _ => {
                    return Err(());
                }
            };
            Ok(previous)
        };

        let res = self.update_sql(update)?;
        let cw = self
            .custom_logical_compaction_window_mut()
            .expect("item must have compaction window");
        *cw = Some(window);
        Ok(res)
    }

    pub fn add_column(
        &mut self,
        name: ColumnName,
        typ: SqlColumnType,
        sql: RawDataType,
    ) -> Result<RelationVersion, PlanError> {
        let CatalogItem::Table(table) = self else {
            return Err(PlanError::Unsupported {
                feature: "adding columns to a non-Table".to_string(),
                discussion_no: None,
            });
        };
        let next_version = table.desc.add_column(name.clone(), typ);

        let update = |mut ast: &mut Statement<Raw>| match &mut ast {
            Statement::CreateTable(stmt) => {
                let version = ColumnOptionDef {
                    name: None,
                    option: ColumnOption::Versioned {
                        action: ColumnVersioned::Added,
                        version: next_version.into(),
                    },
                };
                let column = ColumnDef {
                    name: name.into(),
                    data_type: sql,
                    collation: None,
                    options: vec![version],
                };
                stmt.columns.push(column);
                Ok(())
            }
            _ => Err(()),
        };

        self.update_sql(update)
            .map_err(|()| PlanError::Unstructured("expected CREATE TABLE statement".to_string()))?;
        Ok(next_version)
    }

    /// Updates the create_sql field of this item. Returns an error if this is a builtin item,
    /// otherwise returns f's result.
    pub fn update_sql<F, T>(&mut self, f: F) -> Result<T, ()>
    where
        F: FnOnce(&mut Statement<Raw>) -> Result<T, ()>,
    {
        let create_sql = match self {
            CatalogItem::Table(Table { create_sql, .. })
            | CatalogItem::Type(Type { create_sql, .. })
            | CatalogItem::Source(Source { create_sql, .. }) => create_sql.as_mut(),
            CatalogItem::Sink(Sink { create_sql, .. })
            | CatalogItem::View(View { create_sql, .. })
            | CatalogItem::MaterializedView(MaterializedView { create_sql, .. })
            | CatalogItem::Index(Index { create_sql, .. })
            | CatalogItem::Secret(Secret { create_sql, .. })
            | CatalogItem::Connection(Connection { create_sql, .. })
            | CatalogItem::ContinualTask(ContinualTask { create_sql, .. }) => Some(create_sql),
            CatalogItem::Func(_) | CatalogItem::Log(_) => None,
        };
        let Some(create_sql) = create_sql else {
            return Err(());
        };
        let mut ast = mz_sql_parser::parser::parse_statements(create_sql)
            .expect("non-system items must be parseable")
            .into_element()
            .ast;
        debug!("rewrite: {}", ast.to_ast_string_redacted());
        let t = f(&mut ast)?;
        *create_sql = ast.to_ast_string_stable();
        debug!("rewrote: {}", ast.to_ast_string_redacted());
        Ok(t)
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
            | CatalogItem::Connection(_)
            | CatalogItem::ContinualTask(_) => None,
        }
    }

    pub fn cluster_id(&self) -> Option<ClusterId> {
        match self {
            CatalogItem::MaterializedView(mv) => Some(mv.cluster_id),
            CatalogItem::Index(index) => Some(index.cluster_id),
            CatalogItem::Source(source) => match &source.data_source {
                DataSourceDesc::Ingestion { cluster_id, .. }
                | DataSourceDesc::OldSyntaxIngestion { cluster_id, .. } => Some(*cluster_id),
                // This is somewhat of a lie because the export runs on the same
                // cluster as its ingestion but we don't yet have a way of
                // cross-referencing the items
                DataSourceDesc::IngestionExport { .. } => None,
                DataSourceDesc::Webhook { cluster_id, .. } => Some(*cluster_id),
                DataSourceDesc::Introspection(_) | DataSourceDesc::Progress => None,
            },
            CatalogItem::Sink(sink) => Some(sink.cluster_id),
            CatalogItem::ContinualTask(ct) => Some(ct.cluster_id),
            CatalogItem::Table(_)
            | CatalogItem::Log(_)
            | CatalogItem::View(_)
            | CatalogItem::Type(_)
            | CatalogItem::Func(_)
            | CatalogItem::Secret(_)
            | CatalogItem::Connection(_) => None,
        }
    }

    /// The custom compaction window, if any has been set. This does not reflect any propagated
    /// compaction window (i.e., source -> subsource).
    pub fn custom_logical_compaction_window(&self) -> Option<CompactionWindow> {
        match self {
            CatalogItem::Table(table) => table.custom_logical_compaction_window,
            CatalogItem::Source(source) => source.custom_logical_compaction_window,
            CatalogItem::Index(index) => index.custom_logical_compaction_window,
            CatalogItem::MaterializedView(mview) => mview.custom_logical_compaction_window,
            CatalogItem::Log(_)
            | CatalogItem::View(_)
            | CatalogItem::Sink(_)
            | CatalogItem::Type(_)
            | CatalogItem::Func(_)
            | CatalogItem::Secret(_)
            | CatalogItem::Connection(_)
            | CatalogItem::ContinualTask(_) => None,
        }
    }

    /// Mutable access to the custom compaction window, or None if this type does not support custom
    /// compaction windows. This does not reflect any propagated compaction window (i.e., source ->
    /// subsource).
    pub fn custom_logical_compaction_window_mut(
        &mut self,
    ) -> Option<&mut Option<CompactionWindow>> {
        let cw = match self {
            CatalogItem::Table(table) => &mut table.custom_logical_compaction_window,
            CatalogItem::Source(source) => &mut source.custom_logical_compaction_window,
            CatalogItem::Index(index) => &mut index.custom_logical_compaction_window,
            CatalogItem::MaterializedView(mview) => &mut mview.custom_logical_compaction_window,
            CatalogItem::Log(_)
            | CatalogItem::View(_)
            | CatalogItem::Sink(_)
            | CatalogItem::Type(_)
            | CatalogItem::Func(_)
            | CatalogItem::Secret(_)
            | CatalogItem::Connection(_)
            | CatalogItem::ContinualTask(_) => return None,
        };
        Some(cw)
    }

    /// The initial compaction window, for objects that have one; that is, tables, sources, indexes,
    /// and MVs. This does not reflect any propagated compaction window (i.e., source -> subsource).
    ///
    /// If `custom_logical_compaction_window()` returns something, use that.  Otherwise, use a
    /// sensible default (currently 1s).
    ///
    /// For objects that do not have the concept of compaction window, return None.
    pub fn initial_logical_compaction_window(&self) -> Option<CompactionWindow> {
        let custom_logical_compaction_window = match self {
            CatalogItem::Table(_)
            | CatalogItem::Source(_)
            | CatalogItem::Index(_)
            | CatalogItem::MaterializedView(_)
            | CatalogItem::ContinualTask(_) => self.custom_logical_compaction_window(),
            CatalogItem::Log(_)
            | CatalogItem::View(_)
            | CatalogItem::Sink(_)
            | CatalogItem::Type(_)
            | CatalogItem::Func(_)
            | CatalogItem::Secret(_)
            | CatalogItem::Connection(_) => return None,
        };
        Some(custom_logical_compaction_window.unwrap_or(CompactionWindow::Default))
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
            | CatalogItem::Connection(_)
            | CatalogItem::ContinualTask(_) => false,
        }
    }

    pub fn to_serialized(&self) -> (String, GlobalId, BTreeMap<RelationVersion, GlobalId>) {
        match self {
            CatalogItem::Table(table) => {
                let create_sql = table
                    .create_sql
                    .clone()
                    .expect("builtin tables cannot be serialized");
                let mut collections = table.collections.clone();
                let global_id = collections
                    .remove(&RelationVersion::root())
                    .expect("at least one version");
                (create_sql, global_id, collections)
            }
            CatalogItem::Log(_) => unreachable!("builtin logs cannot be serialized"),
            CatalogItem::Source(source) => {
                assert!(
                    !matches!(source.data_source, DataSourceDesc::Introspection(_)),
                    "cannot serialize introspection/builtin sources",
                );
                let create_sql = source
                    .create_sql
                    .clone()
                    .expect("builtin sources cannot be serialized");
                (create_sql, source.global_id, BTreeMap::new())
            }
            CatalogItem::View(view) => (view.create_sql.clone(), view.global_id, BTreeMap::new()),
            CatalogItem::MaterializedView(mview) => {
                let mut collections = mview.collections.clone();
                let global_id = collections
                    .remove(&RelationVersion::root())
                    .expect("at least one version");
                (mview.create_sql.clone(), global_id, collections)
            }
            CatalogItem::Index(index) => {
                (index.create_sql.clone(), index.global_id, BTreeMap::new())
            }
            CatalogItem::Sink(sink) => (sink.create_sql.clone(), sink.global_id, BTreeMap::new()),
            CatalogItem::Type(typ) => {
                let create_sql = typ
                    .create_sql
                    .clone()
                    .expect("builtin types cannot be serialized");
                (create_sql, typ.global_id, BTreeMap::new())
            }
            CatalogItem::Secret(secret) => {
                (secret.create_sql.clone(), secret.global_id, BTreeMap::new())
            }
            CatalogItem::Connection(connection) => (
                connection.create_sql.clone(),
                connection.global_id,
                BTreeMap::new(),
            ),
            CatalogItem::Func(_) => unreachable!("cannot serialize functions yet"),
            CatalogItem::ContinualTask(ct) => {
                (ct.create_sql.clone(), ct.global_id, BTreeMap::new())
            }
        }
    }

    pub fn into_serialized(self) -> (String, GlobalId, BTreeMap<RelationVersion, GlobalId>) {
        match self {
            CatalogItem::Table(mut table) => {
                let create_sql = table
                    .create_sql
                    .expect("builtin tables cannot be serialized");
                let global_id = table
                    .collections
                    .remove(&RelationVersion::root())
                    .expect("at least one version");
                (create_sql, global_id, table.collections)
            }
            CatalogItem::Log(_) => unreachable!("builtin logs cannot be serialized"),
            CatalogItem::Source(source) => {
                assert!(
                    !matches!(source.data_source, DataSourceDesc::Introspection(_)),
                    "cannot serialize introspection/builtin sources",
                );
                let create_sql = source
                    .create_sql
                    .expect("builtin sources cannot be serialized");
                (create_sql, source.global_id, BTreeMap::new())
            }
            CatalogItem::View(view) => (view.create_sql, view.global_id, BTreeMap::new()),
            CatalogItem::MaterializedView(mut mview) => {
                let global_id = mview
                    .collections
                    .remove(&RelationVersion::root())
                    .expect("at least one version");
                (mview.create_sql, global_id, mview.collections)
            }
            CatalogItem::Index(index) => (index.create_sql, index.global_id, BTreeMap::new()),
            CatalogItem::Sink(sink) => (sink.create_sql, sink.global_id, BTreeMap::new()),
            CatalogItem::Type(typ) => {
                let create_sql = typ.create_sql.expect("builtin types cannot be serialized");
                (create_sql, typ.global_id, BTreeMap::new())
            }
            CatalogItem::Secret(secret) => (secret.create_sql, secret.global_id, BTreeMap::new()),
            CatalogItem::Connection(connection) => {
                (connection.create_sql, connection.global_id, BTreeMap::new())
            }
            CatalogItem::Func(_) => unreachable!("cannot serialize functions yet"),
            CatalogItem::ContinualTask(ct) => (ct.create_sql, ct.global_id, BTreeMap::new()),
        }
    }

    /// Returns a global ID for a specific version selector. Returns `None` if the item does
    /// not have versions or if the version does not exist.
    pub fn global_id_for_version(&self, version: RelationVersionSelector) -> Option<GlobalId> {
        let collections = match self {
            CatalogItem::MaterializedView(mv) => &mv.collections,
            CatalogItem::Table(table) => &table.collections,
            CatalogItem::Source(source) => return Some(source.global_id),
            CatalogItem::Log(log) => return Some(log.global_id),
            CatalogItem::View(view) => return Some(view.global_id),
            CatalogItem::Sink(sink) => return Some(sink.global_id),
            CatalogItem::Index(index) => return Some(index.global_id),
            CatalogItem::Type(ty) => return Some(ty.global_id),
            CatalogItem::Func(func) => return Some(func.global_id),
            CatalogItem::Secret(secret) => return Some(secret.global_id),
            CatalogItem::Connection(conn) => return Some(conn.global_id),
            CatalogItem::ContinualTask(ct) => return Some(ct.global_id),
        };
        match version {
            RelationVersionSelector::Latest => collections.values().last().copied(),
            RelationVersionSelector::Specific(version) => collections.get(&version).copied(),
        }
    }
}

impl CatalogEntry {
    /// Reports the latest [`RelationDesc`] of the rows produced by this [`CatalogEntry`], if it
    /// produces rows.
    pub fn relation_desc_latest(&self) -> Option<Cow<'_, RelationDesc>> {
        self.item.relation_desc(RelationVersionSelector::Latest)
    }

    /// Reports if the item has columns.
    pub fn has_columns(&self) -> bool {
        match self.item() {
            CatalogItem::Type(Type { details, .. }) => {
                matches!(details.typ, CatalogType::Record { .. })
            }
            _ => self.relation_desc_latest().is_some(),
        }
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

    /// Returns the inner [`MaterializedView`] if this entry is a materialized view, else `None`.
    pub fn materialized_view(&self) -> Option<&MaterializedView> {
        match self.item() {
            CatalogItem::MaterializedView(mv) => Some(mv),
            _ => None,
        }
    }

    /// Returns the inner [`Table`] if this entry is a table, else `None`.
    pub fn table(&self) -> Option<&Table> {
        match self.item() {
            CatalogItem::Table(tbl) => Some(tbl),
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
            _ => {
                let db_name = match self.name().qualifiers.database_spec {
                    ResolvedDatabaseSpecifier::Ambient => "".to_string(),
                    ResolvedDatabaseSpecifier::Id(id) => format!("{id}."),
                };
                Err(SqlCatalogError::UnknownConnection(format!(
                    "{}{}.{}",
                    db_name,
                    self.name().qualifiers.schema_spec,
                    self.name().item
                )))
            }
        }
    }

    /// Returns the [`mz_storage_types::sources::SourceDesc`] associated with
    /// this `CatalogEntry`, if any.
    pub fn source_desc(
        &self,
    ) -> Result<Option<&SourceDesc<ReferencedConnection>>, SqlCatalogError> {
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

    /// Reports whether this catalog entry is a subsource and, if it is, the
    /// ingestion it is an export of, as well as the item it exports.
    pub fn subsource_details(
        &self,
    ) -> Option<(CatalogItemId, &UnresolvedItemName, &SourceExportDetails)> {
        match &self.item() {
            CatalogItem::Source(source) => match &source.data_source {
                DataSourceDesc::IngestionExport {
                    ingestion_id,
                    external_reference,
                    details,
                    data_config: _,
                } => Some((*ingestion_id, external_reference, details)),
                _ => None,
            },
            _ => None,
        }
    }

    /// Reports whether this catalog entry is a source export and, if it is, the
    /// ingestion it is an export of, as well as the item it exports.
    pub fn source_export_details(
        &self,
    ) -> Option<(
        CatalogItemId,
        &UnresolvedItemName,
        &SourceExportDetails,
        &SourceExportDataConfig<ReferencedConnection>,
    )> {
        match &self.item() {
            CatalogItem::Source(source) => match &source.data_source {
                DataSourceDesc::IngestionExport {
                    ingestion_id,
                    external_reference,
                    details,
                    data_config,
                } => Some((*ingestion_id, external_reference, details, data_config)),
                _ => None,
            },
            CatalogItem::Table(table) => match &table.data_source {
                TableDataSource::DataSource {
                    desc:
                        DataSourceDesc::IngestionExport {
                            ingestion_id,
                            external_reference,
                            details,
                            data_config,
                        },
                    timeline: _,
                } => Some((*ingestion_id, external_reference, details, data_config)),
                _ => None,
            },
            _ => None,
        }
    }

    /// Reports whether this catalog entry is a progress source.
    pub fn is_progress_source(&self) -> bool {
        self.item().is_progress_source()
    }

    /// Returns the `GlobalId` of all of this entry's progress ID.
    pub fn progress_id(&self) -> Option<CatalogItemId> {
        match &self.item() {
            CatalogItem::Source(source) => match &source.data_source {
                DataSourceDesc::Ingestion { .. } => Some(self.id),
                DataSourceDesc::OldSyntaxIngestion {
                    progress_subsource, ..
                } => Some(*progress_subsource),
                DataSourceDesc::IngestionExport { .. }
                | DataSourceDesc::Introspection(_)
                | DataSourceDesc::Progress
                | DataSourceDesc::Webhook { .. } => None,
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
            | CatalogItem::Connection(_)
            | CatalogItem::ContinualTask(_) => None,
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

    /// Reports whether this catalog entry is a view.
    pub fn is_view(&self) -> bool {
        matches!(self.item(), CatalogItem::View(_))
    }

    /// Reports whether this catalog entry is a secret.
    pub fn is_secret(&self) -> bool {
        matches!(self.item(), CatalogItem::Secret(_))
    }

    /// Reports whether this catalog entry is an introspection source.
    pub fn is_introspection_source(&self) -> bool {
        matches!(self.item(), CatalogItem::Log(_))
    }

    /// Reports whether this catalog entry is an index.
    pub fn is_index(&self) -> bool {
        matches!(self.item(), CatalogItem::Index(_))
    }

    /// Reports whether this catalog entry is a continual task.
    pub fn is_continual_task(&self) -> bool {
        matches!(self.item(), CatalogItem::ContinualTask(_))
    }

    /// Reports whether this catalog entry can be treated as a relation, it can produce rows.
    pub fn is_relation(&self) -> bool {
        mz_sql::catalog::ObjectType::from(self.item_type()).is_relation()
    }

    /// Collects the identifiers of the objects that were encountered when
    /// resolving names in the item's DDL statement.
    pub fn references(&self) -> &ResolvedIds {
        self.item.references()
    }

    /// Collects the identifiers of the objects used by this [`CatalogEntry`].
    ///
    /// Like [`CatalogEntry::references()`] but also includes objects that are not directly
    /// referenced. For example this will include any catalog objects used to implement functions
    /// and casts in the item.
    pub fn uses(&self) -> BTreeSet<CatalogItemId> {
        self.item.uses()
    }

    /// Returns the `CatalogItem` associated with this catalog entry.
    pub fn item(&self) -> &CatalogItem {
        &self.item
    }

    /// Returns the [`CatalogItemId`] of this catalog entry.
    pub fn id(&self) -> CatalogItemId {
        self.id
    }

    /// Returns all of the [`GlobalId`]s associated with this item.
    pub fn global_ids(&self) -> impl Iterator<Item = GlobalId> + '_ {
        self.item().global_ids()
    }

    pub fn latest_global_id(&self) -> GlobalId {
        self.item().latest_global_id()
    }

    /// Returns the OID of this catalog entry.
    pub fn oid(&self) -> u32 {
        self.oid
    }

    /// Returns the fully qualified name of this catalog entry.
    pub fn name(&self) -> &QualifiedItemName {
        &self.name
    }

    /// Returns the identifiers of the dataflows that are directly referenced by this dataflow.
    pub fn referenced_by(&self) -> &[CatalogItemId] {
        &self.referenced_by
    }

    /// Returns the identifiers of the dataflows that depend upon this dataflow.
    pub fn used_by(&self) -> &[CatalogItemId] {
        &self.used_by
    }

    /// Returns the connection ID that this item belongs to, if this item is
    /// temporary.
    pub fn conn_id(&self) -> Option<&ConnectionId> {
        self.item.conn_id()
    }

    /// Returns the role ID of the entry owner.
    pub fn owner_id(&self) -> &RoleId {
        &self.owner_id
    }

    /// Returns the privileges of the entry.
    pub fn privileges(&self) -> &PrivilegeMap {
        &self.privileges
    }
}

#[derive(Debug, Clone, Default)]
pub struct CommentsMap {
    map: BTreeMap<CommentObjectId, BTreeMap<Option<usize>, String>>,
}

impl CommentsMap {
    pub fn update_comment(
        &mut self,
        object_id: CommentObjectId,
        sub_component: Option<usize>,
        comment: Option<String>,
    ) -> Option<String> {
        let object_comments = self.map.entry(object_id).or_default();

        // Either replace the existing comment, or remove it if comment is None/NULL.
        let (empty, prev) = if let Some(comment) = comment {
            let prev = object_comments.insert(sub_component, comment);
            (false, prev)
        } else {
            let prev = object_comments.remove(&sub_component);
            (object_comments.is_empty(), prev)
        };

        // Cleanup entries that are now empty.
        if empty {
            self.map.remove(&object_id);
        }

        // Return the previous comment, if there was one, for easy removal.
        prev
    }

    /// Remove all comments for `object_id` from the map.
    ///
    /// Generally there is one comment for a given [`CommentObjectId`], but in the case of
    /// relations you can also have comments on the individual columns. Dropping the comments for a
    /// relation will also drop all of the comments on any columns.
    pub fn drop_comments(
        &mut self,
        object_ids: &BTreeSet<CommentObjectId>,
    ) -> Vec<(CommentObjectId, Option<usize>, String)> {
        let mut removed_comments = Vec::new();

        for object_id in object_ids {
            if let Some(comments) = self.map.remove(object_id) {
                let removed = comments
                    .into_iter()
                    .map(|(sub_comp, comment)| (object_id.clone(), sub_comp, comment));
                removed_comments.extend(removed);
            }
        }

        removed_comments
    }

    pub fn iter(&self) -> impl Iterator<Item = (CommentObjectId, Option<usize>, &str)> {
        self.map
            .iter()
            .map(|(id, comments)| {
                comments
                    .iter()
                    .map(|(pos, comment)| (*id, *pos, comment.as_str()))
            })
            .flatten()
    }

    pub fn get_object_comments(
        &self,
        object_id: CommentObjectId,
    ) -> Option<&BTreeMap<Option<usize>, String>> {
        self.map.get(&object_id)
    }
}

impl Serialize for CommentsMap {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let comment_count = self
            .map
            .iter()
            .map(|(_object_id, comments)| comments.len())
            .sum();

        let mut seq = serializer.serialize_seq(Some(comment_count))?;
        for (object_id, sub) in &self.map {
            for (sub_component, comment) in sub {
                seq.serialize_element(&(
                    format!("{object_id:?}"),
                    format!("{sub_component:?}"),
                    comment,
                ))?;
            }
        }
        seq.end()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Default)]
pub struct DefaultPrivileges {
    #[serde(serialize_with = "mz_ore::serde::map_key_to_string")]
    privileges: BTreeMap<DefaultPrivilegeObject, RoleDefaultPrivileges>,
}

// Use a new type here because otherwise we have two levels of BTreeMap, both needing
// map_key_to_string.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Default)]
struct RoleDefaultPrivileges(
    /// Denormalized, the key is the grantee Role.
    #[serde(serialize_with = "mz_ore::serde::map_key_to_string")]
    BTreeMap<RoleId, DefaultPrivilegeAclItem>,
);

impl Deref for RoleDefaultPrivileges {
    type Target = BTreeMap<RoleId, DefaultPrivilegeAclItem>;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl DerefMut for RoleDefaultPrivileges {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl DefaultPrivileges {
    /// Add a new default privilege into the set of all default privileges.
    pub fn grant(&mut self, object: DefaultPrivilegeObject, privilege: DefaultPrivilegeAclItem) {
        if privilege.acl_mode.is_empty() {
            return;
        }

        let privileges = self.privileges.entry(object).or_default();
        if let Some(default_privilege) = privileges.get_mut(&privilege.grantee) {
            default_privilege.acl_mode |= privilege.acl_mode;
        } else {
            privileges.insert(privilege.grantee, privilege);
        }
    }

    /// Revoke a default privilege from the set of all default privileges.
    pub fn revoke(&mut self, object: &DefaultPrivilegeObject, privilege: &DefaultPrivilegeAclItem) {
        if let Some(privileges) = self.privileges.get_mut(object) {
            if let Some(default_privilege) = privileges.get_mut(&privilege.grantee) {
                default_privilege.acl_mode =
                    default_privilege.acl_mode.difference(privilege.acl_mode);
                if default_privilege.acl_mode.is_empty() {
                    privileges.remove(&privilege.grantee);
                }
            }
            if privileges.is_empty() {
                self.privileges.remove(object);
            }
        }
    }

    /// Get the privileges that will be granted on all objects matching `object` to `grantee`, if
    /// any exist.
    pub fn get_privileges_for_grantee(
        &self,
        object: &DefaultPrivilegeObject,
        grantee: &RoleId,
    ) -> Option<&AclMode> {
        self.privileges
            .get(object)
            .and_then(|privileges| privileges.get(grantee))
            .map(|privilege| &privilege.acl_mode)
    }

    /// Get all default privileges that apply to the provided object details.
    pub fn get_applicable_privileges(
        &self,
        role_id: RoleId,
        database_id: Option<DatabaseId>,
        schema_id: Option<SchemaId>,
        object_type: mz_sql::catalog::ObjectType,
    ) -> impl Iterator<Item = DefaultPrivilegeAclItem> + '_ {
        // Privileges consider all relations to be of type table due to PostgreSQL compatibility. We
        // don't require the caller to worry about that and we will map their `object_type` to the
        // correct type for privileges.
        let privilege_object_type = if object_type.is_relation() {
            mz_sql::catalog::ObjectType::Table
        } else {
            object_type
        };
        let valid_acl_mode = rbac::all_object_privileges(SystemObjectType::Object(object_type));

        // Collect all entries that apply to the provided object details.
        // If either `database_id` or `schema_id` are `None`, then we might end up with duplicate
        // entries in the vec below. That's OK because we consolidate the results after.
        [
            DefaultPrivilegeObject {
                role_id,
                database_id,
                schema_id,
                object_type: privilege_object_type,
            },
            DefaultPrivilegeObject {
                role_id,
                database_id,
                schema_id: None,
                object_type: privilege_object_type,
            },
            DefaultPrivilegeObject {
                role_id,
                database_id: None,
                schema_id: None,
                object_type: privilege_object_type,
            },
            DefaultPrivilegeObject {
                role_id: RoleId::Public,
                database_id,
                schema_id,
                object_type: privilege_object_type,
            },
            DefaultPrivilegeObject {
                role_id: RoleId::Public,
                database_id,
                schema_id: None,
                object_type: privilege_object_type,
            },
            DefaultPrivilegeObject {
                role_id: RoleId::Public,
                database_id: None,
                schema_id: None,
                object_type: privilege_object_type,
            },
        ]
        .into_iter()
        .filter_map(|object| self.privileges.get(&object))
        .flat_map(|acl_map| acl_map.values())
        // Consolidate privileges with a common grantee.
        .fold(
            BTreeMap::new(),
            |mut accum, DefaultPrivilegeAclItem { grantee, acl_mode }| {
                let accum_acl_mode = accum.entry(grantee).or_insert_with(AclMode::empty);
                *accum_acl_mode |= *acl_mode;
                accum
            },
        )
        .into_iter()
        // Restrict the acl_mode to only privileges valid for the provided object type. If the
        // default privilege has an object type of Table, then it may contain privileges valid for
        // tables but not other relations. If the passed in object type is another relation, then
        // we need to remove any privilege that is not valid for the specified relation.
        .map(move |(grantee, acl_mode)| (grantee, acl_mode & valid_acl_mode))
        // Filter out empty privileges.
        .filter(|(_, acl_mode)| !acl_mode.is_empty())
        .map(|(grantee, acl_mode)| DefaultPrivilegeAclItem {
            grantee: *grantee,
            acl_mode,
        })
    }

    pub fn iter(
        &self,
    ) -> impl Iterator<
        Item = (
            &DefaultPrivilegeObject,
            impl Iterator<Item = &DefaultPrivilegeAclItem>,
        ),
    > {
        self.privileges
            .iter()
            .map(|(object, acl_map)| (object, acl_map.values()))
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialOrd, PartialEq, Eq, Ord)]
pub struct ClusterConfig {
    pub variant: ClusterVariant,
    pub workload_class: Option<String>,
}

impl ClusterConfig {
    pub fn features(&self) -> Option<&OptimizerFeatureOverrides> {
        match &self.variant {
            ClusterVariant::Managed(managed) => Some(&managed.optimizer_feature_overrides),
            ClusterVariant::Unmanaged => None,
        }
    }
}

impl From<ClusterConfig> for durable::ClusterConfig {
    fn from(config: ClusterConfig) -> Self {
        Self {
            variant: config.variant.into(),
            workload_class: config.workload_class,
        }
    }
}

impl From<durable::ClusterConfig> for ClusterConfig {
    fn from(config: durable::ClusterConfig) -> Self {
        Self {
            variant: config.variant.into(),
            workload_class: config.workload_class,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialOrd, PartialEq, Eq, Ord)]
pub struct ClusterVariantManaged {
    pub size: String,
    pub availability_zones: Vec<String>,
    pub logging: ReplicaLogging,
    pub replication_factor: u32,
    pub optimizer_feature_overrides: OptimizerFeatureOverrides,
    pub schedule: ClusterSchedule,
}

impl From<ClusterVariantManaged> for durable::ClusterVariantManaged {
    fn from(managed: ClusterVariantManaged) -> Self {
        Self {
            size: managed.size,
            availability_zones: managed.availability_zones,
            logging: managed.logging,
            replication_factor: managed.replication_factor,
            optimizer_feature_overrides: managed.optimizer_feature_overrides.into(),
            schedule: managed.schedule,
        }
    }
}

impl From<durable::ClusterVariantManaged> for ClusterVariantManaged {
    fn from(managed: durable::ClusterVariantManaged) -> Self {
        Self {
            size: managed.size,
            availability_zones: managed.availability_zones,
            logging: managed.logging,
            replication_factor: managed.replication_factor,
            optimizer_feature_overrides: managed.optimizer_feature_overrides.into(),
            schedule: managed.schedule,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialOrd, PartialEq, Eq, Ord)]
pub enum ClusterVariant {
    Managed(ClusterVariantManaged),
    Unmanaged,
}

impl From<ClusterVariant> for durable::ClusterVariant {
    fn from(variant: ClusterVariant) -> Self {
        match variant {
            ClusterVariant::Managed(managed) => Self::Managed(managed.into()),
            ClusterVariant::Unmanaged => Self::Unmanaged,
        }
    }
}

impl From<durable::ClusterVariant> for ClusterVariant {
    fn from(variant: durable::ClusterVariant) -> Self {
        match variant {
            durable::ClusterVariant::Managed(managed) => Self::Managed(managed.into()),
            durable::ClusterVariant::Unmanaged => Self::Unmanaged,
        }
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

    fn schema_ids(&self) -> &BTreeMap<String, SchemaId> {
        &self.schemas_by_name
    }

    // `as` is ok to use to cast to a trait object.
    #[allow(clippy::as_conversions)]
    fn schemas(&self) -> Vec<&dyn CatalogSchema> {
        self.schemas_by_id
            .values()
            .map(|schema| schema as &dyn CatalogSchema)
            .collect()
    }

    fn owner_id(&self) -> RoleId {
        self.owner_id
    }

    fn privileges(&self) -> &PrivilegeMap {
        &self.privileges
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

    fn item_ids(&self) -> Box<dyn Iterator<Item = CatalogItemId> + '_> {
        Box::new(
            self.items
                .values()
                .chain(self.functions.values())
                .chain(self.types.values())
                .copied(),
        )
    }

    fn owner_id(&self) -> RoleId {
        self.owner_id
    }

    fn privileges(&self) -> &PrivilegeMap {
        &self.privileges
    }
}

impl mz_sql::catalog::CatalogRole for Role {
    fn name(&self) -> &str {
        &self.name
    }

    fn id(&self) -> RoleId {
        self.id
    }

    fn membership(&self) -> &BTreeMap<RoleId, RoleId> {
        &self.membership.map
    }

    fn attributes(&self) -> &RoleAttributes {
        &self.attributes
    }

    fn vars(&self) -> &BTreeMap<String, OwnedVarInput> {
        &self.vars.map
    }
}

impl mz_sql::catalog::CatalogNetworkPolicy for NetworkPolicy {
    fn name(&self) -> &str {
        &self.name
    }

    fn id(&self) -> NetworkPolicyId {
        self.id
    }

    fn owner_id(&self) -> RoleId {
        self.owner_id
    }

    fn privileges(&self) -> &PrivilegeMap {
        &self.privileges
    }
}

impl mz_sql::catalog::CatalogCluster<'_> for Cluster {
    fn name(&self) -> &str {
        &self.name
    }

    fn id(&self) -> ClusterId {
        self.id
    }

    fn bound_objects(&self) -> &BTreeSet<CatalogItemId> {
        &self.bound_objects
    }

    fn replica_ids(&self) -> &BTreeMap<String, ReplicaId> {
        &self.replica_id_by_name_
    }

    // `as` is ok to use to cast to a trait object.
    #[allow(clippy::as_conversions)]
    fn replicas(&self) -> Vec<&dyn CatalogClusterReplica<'_>> {
        self.replicas()
            .map(|replica| replica as &dyn CatalogClusterReplica)
            .collect()
    }

    fn replica(&self, id: ReplicaId) -> &dyn CatalogClusterReplica<'_> {
        self.replica(id).expect("catalog out of sync")
    }

    fn owner_id(&self) -> RoleId {
        self.owner_id
    }

    fn privileges(&self) -> &PrivilegeMap {
        &self.privileges
    }

    fn is_managed(&self) -> bool {
        self.is_managed()
    }

    fn managed_size(&self) -> Option<&str> {
        match &self.config.variant {
            ClusterVariant::Managed(ClusterVariantManaged { size, .. }) => Some(size),
            _ => None,
        }
    }

    fn schedule(&self) -> Option<&ClusterSchedule> {
        match &self.config.variant {
            ClusterVariant::Managed(ClusterVariantManaged { schedule, .. }) => Some(schedule),
            _ => None,
        }
    }

    fn try_to_plan(&self) -> Result<CreateClusterPlan, PlanError> {
        self.try_to_plan()
    }
}

impl mz_sql::catalog::CatalogClusterReplica<'_> for ClusterReplica {
    fn name(&self) -> &str {
        &self.name
    }

    fn cluster_id(&self) -> ClusterId {
        self.cluster_id
    }

    fn replica_id(&self) -> ReplicaId {
        self.replica_id
    }

    fn owner_id(&self) -> RoleId {
        self.owner_id
    }

    fn internal(&self) -> bool {
        self.config.location.internal()
    }
}

impl mz_sql::catalog::CatalogItem for CatalogEntry {
    fn name(&self) -> &QualifiedItemName {
        self.name()
    }

    fn id(&self) -> CatalogItemId {
        self.id()
    }

    fn global_ids(&self) -> Box<dyn Iterator<Item = GlobalId> + '_> {
        Box::new(self.global_ids())
    }

    fn oid(&self) -> u32 {
        self.oid()
    }

    fn func(&self) -> Result<&'static mz_sql::func::Func, SqlCatalogError> {
        self.func()
    }

    fn source_desc(&self) -> Result<Option<&SourceDesc<ReferencedConnection>>, SqlCatalogError> {
        self.source_desc()
    }

    fn connection(
        &self,
    ) -> Result<mz_storage_types::connections::Connection<ReferencedConnection>, SqlCatalogError>
    {
        Ok(self.connection()?.details.to_connection())
    }

    fn create_sql(&self) -> &str {
        match self.item() {
            CatalogItem::Table(Table { create_sql, .. }) => {
                create_sql.as_deref().unwrap_or("<builtin>")
            }
            CatalogItem::Source(Source { create_sql, .. }) => {
                create_sql.as_deref().unwrap_or("<builtin>")
            }
            CatalogItem::Sink(Sink { create_sql, .. }) => create_sql,
            CatalogItem::View(View { create_sql, .. }) => create_sql,
            CatalogItem::MaterializedView(MaterializedView { create_sql, .. }) => create_sql,
            CatalogItem::Index(Index { create_sql, .. }) => create_sql,
            CatalogItem::Type(Type { create_sql, .. }) => {
                create_sql.as_deref().unwrap_or("<builtin>")
            }
            CatalogItem::Secret(Secret { create_sql, .. }) => create_sql,
            CatalogItem::Connection(Connection { create_sql, .. }) => create_sql,
            CatalogItem::Func(_) => "<builtin>",
            CatalogItem::Log(_) => "<builtin>",
            CatalogItem::ContinualTask(ContinualTask { create_sql, .. }) => create_sql,
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

    fn writable_table_details(&self) -> Option<&[Expr<Aug>]> {
        if let CatalogItem::Table(Table {
            data_source: TableDataSource::TableWrites { defaults },
            ..
        }) = self.item()
        {
            Some(defaults.as_slice())
        } else {
            None
        }
    }

    fn replacement_target(&self) -> Option<CatalogItemId> {
        if let CatalogItem::MaterializedView(mv) = self.item() {
            mv.replacement_target
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

    fn references(&self) -> &ResolvedIds {
        self.references()
    }

    fn uses(&self) -> BTreeSet<CatalogItemId> {
        self.uses()
    }

    fn referenced_by(&self) -> &[CatalogItemId] {
        self.referenced_by()
    }

    fn used_by(&self) -> &[CatalogItemId] {
        self.used_by()
    }

    fn subsource_details(
        &self,
    ) -> Option<(CatalogItemId, &UnresolvedItemName, &SourceExportDetails)> {
        self.subsource_details()
    }

    fn source_export_details(
        &self,
    ) -> Option<(
        CatalogItemId,
        &UnresolvedItemName,
        &SourceExportDetails,
        &SourceExportDataConfig<ReferencedConnection>,
    )> {
        self.source_export_details()
    }

    fn is_progress_source(&self) -> bool {
        self.is_progress_source()
    }

    fn progress_id(&self) -> Option<CatalogItemId> {
        self.progress_id()
    }

    fn owner_id(&self) -> RoleId {
        self.owner_id
    }

    fn privileges(&self) -> &PrivilegeMap {
        &self.privileges
    }

    fn cluster_id(&self) -> Option<ClusterId> {
        self.item().cluster_id()
    }

    fn at_version(
        &self,
        version: RelationVersionSelector,
    ) -> Box<dyn mz_sql::catalog::CatalogCollectionItem> {
        Box::new(CatalogCollectionEntry {
            entry: self.clone(),
            version,
        })
    }

    fn latest_version(&self) -> Option<RelationVersion> {
        self.table().map(|t| t.desc.latest_version())
    }
}

/// A single update to the catalog state.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct StateUpdate {
    pub kind: StateUpdateKind,
    pub ts: Timestamp,
    pub diff: StateDiff,
}

/// The contents of a single state update.
///
/// Variants are listed in dependency order.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum StateUpdateKind {
    Role(durable::objects::Role),
    RoleAuth(durable::objects::RoleAuth),
    Database(durable::objects::Database),
    Schema(durable::objects::Schema),
    DefaultPrivilege(durable::objects::DefaultPrivilege),
    SystemPrivilege(MzAclItem),
    SystemConfiguration(durable::objects::SystemConfiguration),
    Cluster(durable::objects::Cluster),
    NetworkPolicy(durable::objects::NetworkPolicy),
    IntrospectionSourceIndex(durable::objects::IntrospectionSourceIndex),
    ClusterReplica(durable::objects::ClusterReplica),
    SourceReferences(durable::objects::SourceReferences),
    SystemObjectMapping(durable::objects::SystemObjectMapping),
    // Temporary items are not actually updated via the durable catalog, but
    // this allows us to model them the same way as all other items in parts of
    // the pipeline.
    TemporaryItem(TemporaryItem),
    Item(durable::objects::Item),
    Comment(durable::objects::Comment),
    AuditLog(durable::objects::AuditLog),
    // Storage updates.
    StorageCollectionMetadata(durable::objects::StorageCollectionMetadata),
    UnfinalizedShard(durable::objects::UnfinalizedShard),
}

/// Valid diffs for catalog state updates.
#[derive(Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd)]
pub enum StateDiff {
    Retraction,
    Addition,
}

impl From<StateDiff> for Diff {
    fn from(diff: StateDiff) -> Self {
        match diff {
            StateDiff::Retraction => Diff::MINUS_ONE,
            StateDiff::Addition => Diff::ONE,
        }
    }
}
impl TryFrom<Diff> for StateDiff {
    type Error = String;

    fn try_from(diff: Diff) -> Result<Self, Self::Error> {
        match diff {
            Diff::MINUS_ONE => Ok(Self::Retraction),
            Diff::ONE => Ok(Self::Addition),
            diff => Err(format!("invalid diff {diff}")),
        }
    }
}

/// Information needed to process an update to a temporary item.
#[derive(Debug, Clone, Ord, PartialOrd, PartialEq, Eq)]
pub struct TemporaryItem {
    pub id: CatalogItemId,
    pub oid: u32,
    pub global_id: GlobalId,
    pub schema_id: SchemaId,
    pub name: String,
    pub conn_id: Option<ConnectionId>,
    pub create_sql: String,
    pub owner_id: RoleId,
    pub privileges: Vec<MzAclItem>,
    pub extra_versions: BTreeMap<RelationVersion, GlobalId>,
}

impl From<CatalogEntry> for TemporaryItem {
    fn from(entry: CatalogEntry) -> Self {
        let conn_id = entry.conn_id().cloned();
        let (create_sql, global_id, extra_versions) = entry.item.to_serialized();

        TemporaryItem {
            id: entry.id,
            oid: entry.oid,
            global_id,
            schema_id: entry.name.qualifiers.schema_spec.into(),
            name: entry.name.item,
            conn_id,
            create_sql,
            owner_id: entry.owner_id,
            privileges: entry.privileges.into_all_values().collect(),
            extra_versions,
        }
    }
}

impl TemporaryItem {
    pub fn item_type(&self) -> CatalogItemType {
        item_type(&self.create_sql)
    }
}

/// The same as [`StateUpdateKind`], but without `TemporaryItem` so we can derive [`Ord`].
#[derive(Debug, Clone, Ord, PartialOrd, Eq, PartialEq)]
pub enum BootstrapStateUpdateKind {
    Role(durable::objects::Role),
    RoleAuth(durable::objects::RoleAuth),
    Database(durable::objects::Database),
    Schema(durable::objects::Schema),
    DefaultPrivilege(durable::objects::DefaultPrivilege),
    SystemPrivilege(MzAclItem),
    SystemConfiguration(durable::objects::SystemConfiguration),
    Cluster(durable::objects::Cluster),
    NetworkPolicy(durable::objects::NetworkPolicy),
    IntrospectionSourceIndex(durable::objects::IntrospectionSourceIndex),
    ClusterReplica(durable::objects::ClusterReplica),
    SourceReferences(durable::objects::SourceReferences),
    SystemObjectMapping(durable::objects::SystemObjectMapping),
    Item(durable::objects::Item),
    Comment(durable::objects::Comment),
    AuditLog(durable::objects::AuditLog),
    // Storage updates.
    StorageCollectionMetadata(durable::objects::StorageCollectionMetadata),
    UnfinalizedShard(durable::objects::UnfinalizedShard),
}

impl From<BootstrapStateUpdateKind> for StateUpdateKind {
    fn from(value: BootstrapStateUpdateKind) -> Self {
        match value {
            BootstrapStateUpdateKind::Role(kind) => StateUpdateKind::Role(kind),
            BootstrapStateUpdateKind::RoleAuth(kind) => StateUpdateKind::RoleAuth(kind),
            BootstrapStateUpdateKind::Database(kind) => StateUpdateKind::Database(kind),
            BootstrapStateUpdateKind::Schema(kind) => StateUpdateKind::Schema(kind),
            BootstrapStateUpdateKind::DefaultPrivilege(kind) => {
                StateUpdateKind::DefaultPrivilege(kind)
            }
            BootstrapStateUpdateKind::SystemPrivilege(kind) => {
                StateUpdateKind::SystemPrivilege(kind)
            }
            BootstrapStateUpdateKind::SystemConfiguration(kind) => {
                StateUpdateKind::SystemConfiguration(kind)
            }
            BootstrapStateUpdateKind::SourceReferences(kind) => {
                StateUpdateKind::SourceReferences(kind)
            }
            BootstrapStateUpdateKind::Cluster(kind) => StateUpdateKind::Cluster(kind),
            BootstrapStateUpdateKind::NetworkPolicy(kind) => StateUpdateKind::NetworkPolicy(kind),
            BootstrapStateUpdateKind::IntrospectionSourceIndex(kind) => {
                StateUpdateKind::IntrospectionSourceIndex(kind)
            }
            BootstrapStateUpdateKind::ClusterReplica(kind) => StateUpdateKind::ClusterReplica(kind),
            BootstrapStateUpdateKind::SystemObjectMapping(kind) => {
                StateUpdateKind::SystemObjectMapping(kind)
            }
            BootstrapStateUpdateKind::Item(kind) => StateUpdateKind::Item(kind),
            BootstrapStateUpdateKind::Comment(kind) => StateUpdateKind::Comment(kind),
            BootstrapStateUpdateKind::AuditLog(kind) => StateUpdateKind::AuditLog(kind),
            BootstrapStateUpdateKind::StorageCollectionMetadata(kind) => {
                StateUpdateKind::StorageCollectionMetadata(kind)
            }
            BootstrapStateUpdateKind::UnfinalizedShard(kind) => {
                StateUpdateKind::UnfinalizedShard(kind)
            }
        }
    }
}

impl TryFrom<StateUpdateKind> for BootstrapStateUpdateKind {
    type Error = TemporaryItem;

    fn try_from(value: StateUpdateKind) -> Result<Self, Self::Error> {
        match value {
            StateUpdateKind::Role(kind) => Ok(BootstrapStateUpdateKind::Role(kind)),
            StateUpdateKind::RoleAuth(kind) => Ok(BootstrapStateUpdateKind::RoleAuth(kind)),
            StateUpdateKind::Database(kind) => Ok(BootstrapStateUpdateKind::Database(kind)),
            StateUpdateKind::Schema(kind) => Ok(BootstrapStateUpdateKind::Schema(kind)),
            StateUpdateKind::DefaultPrivilege(kind) => {
                Ok(BootstrapStateUpdateKind::DefaultPrivilege(kind))
            }
            StateUpdateKind::SystemPrivilege(kind) => {
                Ok(BootstrapStateUpdateKind::SystemPrivilege(kind))
            }
            StateUpdateKind::SystemConfiguration(kind) => {
                Ok(BootstrapStateUpdateKind::SystemConfiguration(kind))
            }
            StateUpdateKind::Cluster(kind) => Ok(BootstrapStateUpdateKind::Cluster(kind)),
            StateUpdateKind::NetworkPolicy(kind) => {
                Ok(BootstrapStateUpdateKind::NetworkPolicy(kind))
            }
            StateUpdateKind::IntrospectionSourceIndex(kind) => {
                Ok(BootstrapStateUpdateKind::IntrospectionSourceIndex(kind))
            }
            StateUpdateKind::ClusterReplica(kind) => {
                Ok(BootstrapStateUpdateKind::ClusterReplica(kind))
            }
            StateUpdateKind::SourceReferences(kind) => {
                Ok(BootstrapStateUpdateKind::SourceReferences(kind))
            }
            StateUpdateKind::SystemObjectMapping(kind) => {
                Ok(BootstrapStateUpdateKind::SystemObjectMapping(kind))
            }
            StateUpdateKind::TemporaryItem(kind) => Err(kind),
            StateUpdateKind::Item(kind) => Ok(BootstrapStateUpdateKind::Item(kind)),
            StateUpdateKind::Comment(kind) => Ok(BootstrapStateUpdateKind::Comment(kind)),
            StateUpdateKind::AuditLog(kind) => Ok(BootstrapStateUpdateKind::AuditLog(kind)),
            StateUpdateKind::StorageCollectionMetadata(kind) => {
                Ok(BootstrapStateUpdateKind::StorageCollectionMetadata(kind))
            }
            StateUpdateKind::UnfinalizedShard(kind) => {
                Ok(BootstrapStateUpdateKind::UnfinalizedShard(kind))
            }
        }
    }
}

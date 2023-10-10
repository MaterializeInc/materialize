// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! The current types used by the [`crate::catalog::Catalog`].

use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet};
use std::ops::{Deref, DerefMut};
use std::time::Duration;

use chrono::{DateTime, Utc};
use once_cell::sync::Lazy;
use serde::ser::SerializeSeq;
use serde::{Deserialize, Serialize};

use mz_catalog::builtin::{MZ_INTROSPECTION_CLUSTER, MZ_SYSTEM_CLUSTER};
use mz_compute_client::logging::LogVariant;
use mz_controller::clusters::{
    ClusterRole, ClusterStatus, ProcessId, ReplicaConfig, ReplicaLogging,
};
use mz_controller_types::{ClusterId, ReplicaId};
use mz_expr::{MirScalarExpr, OptimizedMirRelationExpr};
use mz_ore::collections::CollectionExt;
use mz_repr::adt::mz_acl_item::{AclMode, PrivilegeMap};
use mz_repr::role_id::RoleId;
use mz_repr::{GlobalId, RelationDesc};
use mz_sql::ast::display::AstDisplay;
use mz_sql::ast::Expr;
use mz_sql::catalog::{
    CatalogClusterReplica, CatalogError as SqlCatalogError, CatalogItem as SqlCatalogItem,
    CatalogItemType as SqlCatalogItemType, CatalogItemType, CatalogSchema, CatalogTypeDetails,
    DefaultPrivilegeAclItem, DefaultPrivilegeObject, IdReference, RoleAttributes, RoleMembership,
    RoleVars, SystemObjectType,
};
use mz_sql::names::{
    Aug, CommentObjectId, DatabaseId, FullItemName, QualifiedItemName, QualifiedSchemaName,
    ResolvedDatabaseSpecifier, ResolvedIds, SchemaId, SchemaSpecifier,
};
use mz_sql::plan::{
    CreateSourcePlan, Ingestion as PlanIngestion, WebhookHeaders, WebhookValidation,
};
use mz_sql::rbac;
use mz_sql::session::vars::OwnedVarInput;
use mz_storage_client::controller::IntrospectionType;
use mz_storage_types::connections::inline::ReferencedConnection;
use mz_storage_types::sinks::{SinkEnvelope, StorageSinkConnection, StorageSinkConnectionBuilder};
use mz_storage_types::sources::{
    IngestionDescription, SourceConnection, SourceDesc, SourceEnvelope, SourceExport, Timeline,
};

use crate::client::ConnectionId;
use crate::coord::DEFAULT_LOGICAL_COMPACTION_WINDOW;

#[derive(Debug, Serialize, Clone)]
pub struct Database {
    pub name: String,
    pub id: DatabaseId,
    #[serde(skip)]
    pub oid: u32,
    #[serde(serialize_with = "mz_ore::serde::map_key_to_string")]
    pub schemas_by_id: BTreeMap<SchemaId, Schema>,
    pub schemas_by_name: BTreeMap<String, SchemaId>,
    pub owner_id: RoleId,
    pub privileges: PrivilegeMap,
}

impl From<Database> for mz_catalog::Database {
    fn from(database: Database) -> mz_catalog::Database {
        mz_catalog::Database {
            id: database.id,
            name: database.name,
            owner_id: database.owner_id,
            privileges: database.privileges.into_all_values().collect(),
        }
    }
}

#[derive(Debug, Serialize, Clone)]
pub struct Schema {
    pub name: QualifiedSchemaName,
    pub id: SchemaSpecifier,
    #[serde(skip)]
    pub oid: u32,
    pub items: BTreeMap<String, GlobalId>,
    pub functions: BTreeMap<String, GlobalId>,
    pub owner_id: RoleId,
    pub privileges: PrivilegeMap,
}

impl Schema {
    pub(crate) fn into_durable_schema(self, database_id: Option<DatabaseId>) -> mz_catalog::Schema {
        mz_catalog::Schema {
            id: self.id.into(),
            name: self.name.schema,
            database_id,
            owner_id: self.owner_id,
            privileges: self.privileges.into_all_values().collect(),
        }
    }
}

#[derive(Debug, Serialize, Clone)]
pub struct Role {
    pub name: String,
    pub id: RoleId,
    #[serde(skip)]
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

impl From<Role> for mz_catalog::Role {
    fn from(role: Role) -> mz_catalog::Role {
        mz_catalog::Role {
            id: role.id,
            name: role.name,
            attributes: role.attributes,
            membership: role.membership,
            vars: role.vars,
        }
    }
}

#[derive(Debug, Serialize, Clone)]
pub struct Cluster {
    pub name: String,
    pub id: ClusterId,
    pub config: ClusterConfig,
    #[serde(skip)]
    pub log_indexes: BTreeMap<LogVariant, GlobalId>,
    pub linked_object_id: Option<GlobalId>,
    /// Objects bound to this cluster. Does not include introspection source
    /// indexes.
    pub bound_objects: BTreeSet<GlobalId>,
    pub(super) replica_id_by_name_: BTreeMap<String, ReplicaId>,
    #[serde(serialize_with = "mz_ore::serde::map_key_to_string")]
    pub(super) replicas_by_id_: BTreeMap<ReplicaId, ClusterReplica>,
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
        } else if self.name == MZ_INTROSPECTION_CLUSTER.name {
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

    /// Lookup a replica by ID and return a mutable reference.
    pub(crate) fn replica_mut(&mut self, replica_id: ReplicaId) -> Option<&mut ClusterReplica> {
        self.replicas_by_id_.get_mut(&replica_id)
    }

    /// Lookup a replica ID by name.
    pub fn replica_id(&self, name: &str) -> Option<ReplicaId> {
        self.replica_id_by_name_.get(name).copied()
    }

    /// Insert a new replica into the cluster.
    ///
    /// Panics if the name or ID are reused.
    pub(crate) fn insert_replica(&mut self, replica: ClusterReplica) {
        assert!(self
            .replica_id_by_name_
            .insert(replica.name.clone(), replica.replica_id)
            .is_none());
        assert!(self
            .replicas_by_id_
            .insert(replica.replica_id, replica)
            .is_none());
    }

    /// Remove a replica from this cluster.
    ///
    /// Panics if the replica ID does not exist, or if the internal state is inconsistent.
    pub(crate) fn remove_replica(&mut self, replica_id: ReplicaId) {
        let replica = self
            .replicas_by_id_
            .remove(&replica_id)
            .expect("catalog out of sync");
        self.replica_id_by_name_
            .remove(&replica.name)
            .expect("catalog out of sync");
        assert_eq!(self.replica_id_by_name_.len(), self.replicas_by_id_.len());
    }

    /// Renames a replica to a new name.
    ///
    /// Panics if the replica ID is unknown, or new name is not unique, or the internal state is
    /// inconsistent.
    pub(crate) fn rename_replica(&mut self, replica_id: ReplicaId, to_name: String) {
        let replica = self.replica_mut(replica_id).expect("Must exist");
        let old_name = std::mem::take(&mut replica.name);
        replica.name = to_name.clone();

        assert!(self.replica_id_by_name_.remove(&old_name).is_some());
        assert!(self
            .replica_id_by_name_
            .insert(to_name, replica_id)
            .is_none());
    }
}

impl From<Cluster> for mz_catalog::Cluster {
    fn from(cluster: Cluster) -> mz_catalog::Cluster {
        mz_catalog::Cluster {
            id: cluster.id,
            name: cluster.name,
            linked_object_id: cluster.linked_object_id,
            owner_id: cluster.owner_id,
            privileges: cluster.privileges.into_all_values().collect(),
            config: cluster.config.into(),
        }
    }
}

#[derive(Debug, Serialize, Clone)]
pub struct ClusterReplica {
    pub name: String,
    pub cluster_id: ClusterId,
    pub replica_id: ReplicaId,
    pub config: ReplicaConfig,
    #[serde(skip)]
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
                (x, y) => {
                    let reason_x = match x {
                        ClusterStatus::NotReady(reason) => reason,
                        ClusterStatus::Ready => None,
                    };
                    let reason_y = match y {
                        ClusterStatus::NotReady(reason) => reason,
                        ClusterStatus::Ready => None,
                    };
                    // Arbitrarily pick the first known not-ready reason.
                    ClusterStatus::NotReady(reason_x.or(reason_y))
                }
            })
    }
}

impl From<ClusterReplica> for mz_catalog::ClusterReplica {
    fn from(replica: ClusterReplica) -> mz_catalog::ClusterReplica {
        mz_catalog::ClusterReplica {
            cluster_id: replica.cluster_id,
            replica_id: replica.replica_id,
            name: replica.name,
            config: replica.config.into(),
            owner_id: replica.owner_id,
        }
    }
}

#[derive(Debug, Serialize, Clone)]
pub struct ClusterReplicaProcessStatus {
    pub status: ClusterStatus,
    pub time: DateTime<Utc>,
}

#[derive(Clone, Debug, Serialize)]
pub struct CatalogEntry {
    pub(super) item: CatalogItem,
    #[serde(skip)]
    pub(super) used_by: Vec<GlobalId>,
    pub(super) id: GlobalId,
    #[serde(skip)]
    pub(super) oid: u32,
    pub(super) name: QualifiedItemName,
    pub(super) owner_id: RoleId,
    pub(super) privileges: PrivilegeMap,
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

impl From<CatalogEntry> for mz_catalog::Item {
    fn from(entry: CatalogEntry) -> mz_catalog::Item {
        mz_catalog::Item {
            id: entry.id,
            name: entry.name,
            create_sql: entry.item.into_serialized(),
            owner_id: entry.owner_id,
            privileges: entry.privileges.into_all_values().collect(),
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct Table {
    pub create_sql: String,
    pub desc: RelationDesc,
    #[serde(skip)]
    pub defaults: Vec<Expr<Aug>>,
    #[serde(skip)]
    pub conn_id: Option<ConnectionId>,
    pub resolved_ids: ResolvedIds,
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
    Ingestion(IngestionDescription<(), ReferencedConnection>),
    /// Receives data from some other source
    Source,
    /// Receives introspection data from an internal system
    Introspection(IntrospectionType),
    /// Receives data from the source's reclocking/remapping operations.
    Progress,
    /// Receives data from HTTP requests.
    Webhook {
        /// Optional components used to validation a webhook request.
        validate_using: Option<WebhookValidation>,
        /// Describes whether or not to include headers and how to map them.
        headers: WebhookHeaders,
        /// The cluster which this source is associated with.
        cluster_id: ClusterId,
    },
}

impl DataSourceDesc {
    /// Describes the ingestion to the adapter, which essentially just enriches the a higher-level
    /// [`PlanIngestion`] with a [`ClusterId`].
    pub fn ingestion(
        id: GlobalId,
        ingestion: PlanIngestion,
        instance_id: ClusterId,
    ) -> DataSourceDesc {
        let source_imports = ingestion
            .source_imports
            .iter()
            .map(|id| (*id, ()))
            .collect();

        let source_exports = ingestion
            .subsource_exports
            .iter()
            // By convention the first output corresponds to the main source object
            .chain(std::iter::once((&id, &0)))
            .map(|(id, output_index)| {
                let export = SourceExport {
                    output_index: *output_index,
                    storage_metadata: (),
                };
                (*id, export)
            })
            .collect();

        DataSourceDesc::Ingestion(IngestionDescription {
            desc: ingestion.desc.clone(),
            ingestion_metadata: (),
            source_imports,
            source_exports,
            instance_id,
            remap_collection_id: ingestion.progress_subsource,
        })
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct Source {
    pub create_sql: String,
    // TODO: Unskip: currently blocked on some inner BTreeMap<X, _> problems.
    #[serde(skip)]
    pub data_source: DataSourceDesc,
    pub desc: RelationDesc,
    pub timeline: Timeline,
    pub resolved_ids: ResolvedIds,
    pub custom_logical_compaction_window: Option<Duration>,
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
        id: GlobalId,
        plan: CreateSourcePlan,
        cluster_id: Option<ClusterId>,
        resolved_ids: ResolvedIds,
        custom_logical_compaction_window: Option<Duration>,
        is_retained_metrics_object: bool,
    ) -> Source {
        Source {
            create_sql: plan.source.create_sql,
            data_source: match plan.source.data_source {
                mz_sql::plan::DataSourceDesc::Ingestion(ingestion) => DataSourceDesc::ingestion(
                    id,
                    ingestion,
                    cluster_id.expect("ingestion-based sources must be given a cluster ID"),
                ),
                mz_sql::plan::DataSourceDesc::Progress => {
                    assert!(
                        matches!(
                            plan.cluster_config,
                            mz_sql::plan::SourceSinkClusterConfig::Undefined
                        ) && cluster_id.is_none(),
                        "subsources must not have a host config or cluster_id defined"
                    );
                    DataSourceDesc::Progress
                }
                mz_sql::plan::DataSourceDesc::Source => {
                    assert!(
                        matches!(
                            plan.cluster_config,
                            mz_sql::plan::SourceSinkClusterConfig::Undefined
                        ) && cluster_id.is_none(),
                        "subsources must not have a host config or cluster_id defined"
                    );
                    DataSourceDesc::Source
                }
                mz_sql::plan::DataSourceDesc::Webhook {
                    validate_using,
                    headers,
                } => {
                    assert!(
                        matches!(
                            plan.cluster_config,
                            mz_sql::plan::SourceSinkClusterConfig::Existing { .. }
                        ) && cluster_id.is_some(),
                        "webhook sources must be created on an existing cluster"
                    );
                    DataSourceDesc::Webhook {
                        validate_using,
                        headers,
                        cluster_id: cluster_id.expect("checked above"),
                    }
                }
            },
            desc: plan.source.desc,
            timeline: plan.timeline,
            resolved_ids,
            custom_logical_compaction_window,
            is_retained_metrics_object,
        }
    }

    /// Returns whether this source ingests data from an external source.
    pub fn is_external(&self) -> bool {
        match self.data_source {
            DataSourceDesc::Ingestion(_) | DataSourceDesc::Webhook { .. } => true,
            DataSourceDesc::Introspection(_)
            | DataSourceDesc::Progress
            | DataSourceDesc::Source => false,
        }
    }

    /// Type of the source.
    pub fn source_type(&self) -> &str {
        match &self.data_source {
            DataSourceDesc::Ingestion(ingestion) => ingestion.desc.connection.name(),
            DataSourceDesc::Progress => "progress",
            DataSourceDesc::Source => "subsource",
            DataSourceDesc::Introspection(_) => "source",
            DataSourceDesc::Webhook { .. } => "webhook",
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
                    mz_storage_types::sources::UpsertStyle::Default(_) => Some("upsert"),
                    mz_storage_types::sources::UpsertStyle::Debezium { .. } => {
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
            | DataSourceDesc::Webhook { .. }
            | DataSourceDesc::Progress
            | DataSourceDesc::Source => None,
        }
    }

    /// Connection ID of the source, if one exists.
    pub fn connection_id(&self) -> Option<GlobalId> {
        match &self.data_source {
            DataSourceDesc::Ingestion(ingestion) => ingestion.desc.connection.connection_id(),
            DataSourceDesc::Introspection(_)
            | DataSourceDesc::Webhook { .. }
            | DataSourceDesc::Progress
            | DataSourceDesc::Source => None,
        }
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
            DataSourceDesc::Ingestion(ingestion) => {
                // Ingestions with subsources only use persist shards for their
                // subsources (i.e. not the primary source's persist shard);
                // those without subsources use 1 (their primary source's
                // persist shard).
                std::cmp::max(1, i64::try_from(ingestion.source_exports.len().saturating_sub(1)).expect("fewer than i64::MAX persist shards"))
            }
            DataSourceDesc::Webhook { .. } => 1,
            //  DataSourceDesc::Source represents subsources, which are accounted for in their
            //  primary source's ingestion.
            DataSourceDesc::Source
            // Introspection and progress subsources are not under the user's control, so shouldn't
            // count toward their quota.
            | DataSourceDesc::Introspection(_)
            | DataSourceDesc::Progress => 0,
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
pub struct Sink {
    pub create_sql: String,
    pub from: GlobalId,
    // TODO(benesch): this field duplicates information that could be derived
    // from the connection ID. Too hard to fix at the moment.
    #[serde(skip)]
    pub connection: StorageSinkConnectionState,
    pub envelope: SinkEnvelope,
    pub with_snapshot: bool,
    pub resolved_ids: ResolvedIds,
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
    Pending(StorageSinkConnectionBuilder<ReferencedConnection>),
    Ready(StorageSinkConnection<ReferencedConnection>),
}

#[derive(Debug, Clone, Serialize)]
pub struct View {
    pub create_sql: String,
    pub optimized_expr: OptimizedMirRelationExpr,
    pub desc: RelationDesc,
    pub conn_id: Option<ConnectionId>,
    pub resolved_ids: ResolvedIds,
}

#[derive(Debug, Clone, Serialize)]
pub struct MaterializedView {
    pub create_sql: String,
    pub optimized_expr: OptimizedMirRelationExpr,
    pub desc: RelationDesc,
    pub resolved_ids: ResolvedIds,
    pub cluster_id: ClusterId,
}

#[derive(Debug, Clone, Serialize)]
pub struct Index {
    pub create_sql: String,
    pub on: GlobalId,
    pub keys: Vec<MirScalarExpr>,
    pub conn_id: Option<ConnectionId>,
    pub resolved_ids: ResolvedIds,
    pub cluster_id: ClusterId,
    pub custom_logical_compaction_window: Option<Duration>,
    pub is_retained_metrics_object: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct Type {
    pub create_sql: String,
    #[serde(skip)]
    pub details: CatalogTypeDetails<IdReference>,
    pub resolved_ids: ResolvedIds,
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
    pub connection: mz_storage_types::connections::Connection<ReferencedConnection>,
    pub resolved_ids: ResolvedIds,
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
    ) -> Result<Option<&SourceDesc<ReferencedConnection>>, SqlCatalogError> {
        match &self {
            CatalogItem::Source(source) => match &source.data_source {
                DataSourceDesc::Ingestion(ingestion) => Ok(Some(&ingestion.desc)),
                DataSourceDesc::Introspection(_)
                | DataSourceDesc::Webhook { .. }
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

    /// Collects the identifiers of the objects that were encountered when
    /// resolving names in the item's DDL statement.
    pub fn uses(&self) -> &ResolvedIds {
        static EMPTY: Lazy<ResolvedIds> = Lazy::new(|| ResolvedIds(BTreeSet::new()));
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
    pub(crate) fn rename_item_refs(
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
                DataSourceDesc::Ingestion(ingestion) => Some(ingestion.instance_id),
                DataSourceDesc::Webhook { cluster_id, .. } => Some(*cluster_id),
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

    pub(crate) fn to_serialized(&self) -> String {
        match self {
            CatalogItem::Table(table) => table.create_sql.clone(),
            CatalogItem::Log(_) => unreachable!("builtin logs cannot be serialized"),
            CatalogItem::Source(source) => {
                assert!(
                    !matches!(source.data_source, DataSourceDesc::Introspection(_)),
                    "cannot serialize introspection/builtin sources",
                );
                source.create_sql.clone()
            }
            CatalogItem::View(view) => view.create_sql.clone(),
            CatalogItem::MaterializedView(mview) => mview.create_sql.clone(),
            CatalogItem::Index(index) => index.create_sql.clone(),
            CatalogItem::Sink(sink) => sink.create_sql.clone(),
            CatalogItem::Type(typ) => typ.create_sql.clone(),
            CatalogItem::Secret(secret) => secret.create_sql.clone(),
            CatalogItem::Connection(connection) => connection.create_sql.clone(),
            CatalogItem::Func(_) => unreachable!("cannot serialize functions yet"),
        }
    }

    pub(crate) fn into_serialized(self) -> String {
        match self {
            CatalogItem::Table(table) => table.create_sql,
            CatalogItem::Log(_) => unreachable!("builtin logs cannot be serialized"),
            CatalogItem::Source(source) => {
                assert!(
                    !matches!(source.data_source, DataSourceDesc::Introspection(_)),
                    "cannot serialize introspection/builtin sources",
                );
                source.create_sql
            }
            CatalogItem::View(view) => view.create_sql,
            CatalogItem::MaterializedView(mview) => mview.create_sql,
            CatalogItem::Index(index) => index.create_sql,
            CatalogItem::Sink(sink) => sink.create_sql,
            CatalogItem::Type(typ) => typ.create_sql,
            CatalogItem::Secret(secret) => secret.create_sql,
            CatalogItem::Connection(connection) => connection.create_sql,
            CatalogItem::Func(_) => unreachable!("cannot serialize functions yet"),
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
    pub fn subsources(&self) -> BTreeSet<GlobalId> {
        match &self.item() {
            CatalogItem::Source(source) => match &source.data_source {
                DataSourceDesc::Ingestion(ingestion) => ingestion
                    .source_exports
                    .keys()
                    .filter(|id| id != &&self.id)
                    .copied()
                    .chain(std::iter::once(ingestion.remap_collection_id))
                    .collect(),
                DataSourceDesc::Introspection(_)
                | DataSourceDesc::Webhook { .. }
                | DataSourceDesc::Progress
                | DataSourceDesc::Source => BTreeSet::new(),
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
            | CatalogItem::Connection(_) => BTreeSet::new(),
        }
    }

    /// Returns the `GlobalId` of all of this entry's progress ID.
    pub fn progress_id(&self) -> Option<GlobalId> {
        match &self.item() {
            CatalogItem::Source(source) => match &source.data_source {
                DataSourceDesc::Ingestion(ingestion) => Some(ingestion.remap_collection_id),
                DataSourceDesc::Introspection(_)
                | DataSourceDesc::Progress
                | DataSourceDesc::Webhook { .. }
                | DataSourceDesc::Source => None,
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
            | CatalogItem::Connection(_) => None,
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

    /// Reports whether this catalog entry is an introspection source.
    pub fn is_introspection_source(&self) -> bool {
        matches!(self.item(), CatalogItem::Log(_))
    }

    /// Reports whether this catalog entry is an index.
    pub fn is_index(&self) -> bool {
        matches!(self.item(), CatalogItem::Index(_))
    }

    /// Reports whether this catalog entry can be treated as a relation, it can produce rows.
    pub fn is_relation(&self) -> bool {
        mz_sql::catalog::ObjectType::from(self.item_type()).is_relation()
    }

    /// Collects the identifiers of the objects that were encountered when
    /// resolving names in the item's DDL statement.
    pub fn uses(&self) -> &ResolvedIds {
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
        object_id: CommentObjectId,
    ) -> Vec<(CommentObjectId, Option<usize>, String)> {
        match self.map.remove(&object_id) {
            None => Vec::new(),
            Some(comments) => comments
                .into_iter()
                .map(|(sub_comp, comment)| (object_id, sub_comp, comment))
                .collect(),
        }
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
    pub(crate) fn grant(
        &mut self,
        object: DefaultPrivilegeObject,
        privilege: DefaultPrivilegeAclItem,
    ) {
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
    pub(crate) fn revoke(
        &mut self,
        object: &DefaultPrivilegeObject,
        privilege: &DefaultPrivilegeAclItem,
    ) {
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
    pub(crate) fn get_privileges_for_grantee(
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
    pub(crate) fn get_applicable_privileges(
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

    pub(crate) fn iter(
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
}

impl From<ClusterConfig> for mz_catalog::ClusterConfig {
    fn from(config: ClusterConfig) -> Self {
        Self {
            variant: config.variant.into(),
        }
    }
}

impl From<mz_catalog::ClusterConfig> for ClusterConfig {
    fn from(config: mz_catalog::ClusterConfig) -> Self {
        Self {
            variant: config.variant.into(),
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialOrd, PartialEq, Eq, Ord)]
pub struct ClusterVariantManaged {
    pub size: String,
    pub availability_zones: Vec<String>,
    pub logging: ReplicaLogging,
    pub idle_arrangement_merge_effort: Option<u32>,
    pub replication_factor: u32,
    pub disk: bool,
}

impl From<ClusterVariantManaged> for mz_catalog::ClusterVariantManaged {
    fn from(managed: ClusterVariantManaged) -> Self {
        Self {
            size: managed.size,
            availability_zones: managed.availability_zones,
            logging: managed.logging,
            idle_arrangement_merge_effort: managed.idle_arrangement_merge_effort,
            replication_factor: managed.replication_factor,
            disk: managed.disk,
        }
    }
}

impl From<mz_catalog::ClusterVariantManaged> for ClusterVariantManaged {
    fn from(managed: mz_catalog::ClusterVariantManaged) -> Self {
        Self {
            size: managed.size,
            availability_zones: managed.availability_zones,
            logging: managed.logging,
            idle_arrangement_merge_effort: managed.idle_arrangement_merge_effort,
            replication_factor: managed.replication_factor,
            disk: managed.disk,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialOrd, PartialEq, Eq, Ord)]
pub enum ClusterVariant {
    Managed(ClusterVariantManaged),
    Unmanaged,
}

impl From<ClusterVariant> for mz_catalog::ClusterVariant {
    fn from(variant: ClusterVariant) -> Self {
        match variant {
            ClusterVariant::Managed(managed) => Self::Managed(managed.into()),
            ClusterVariant::Unmanaged => Self::Unmanaged,
        }
    }
}

impl From<mz_catalog::ClusterVariant> for ClusterVariant {
    fn from(variant: mz_catalog::ClusterVariant) -> Self {
        match variant {
            mz_catalog::ClusterVariant::Managed(managed) => Self::Managed(managed.into()),
            mz_catalog::ClusterVariant::Unmanaged => Self::Unmanaged,
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

    fn item_ids(&self) -> &BTreeMap<String, GlobalId> {
        &self.items
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

    fn replica_ids(&self) -> &BTreeMap<String, ReplicaId> {
        &self.replica_id_by_name_
    }

    // `as` is ok to use to cast to a trait object.
    #[allow(clippy::as_conversions)]
    fn replicas(&self) -> Vec<&dyn CatalogClusterReplica> {
        self.replicas()
            .map(|replica| replica as &dyn CatalogClusterReplica)
            .collect()
    }

    fn replica(&self, id: ReplicaId) -> &dyn CatalogClusterReplica {
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

    fn source_desc(&self) -> Result<Option<&SourceDesc<ReferencedConnection>>, SqlCatalogError> {
        self.source_desc()
    }

    fn connection(
        &self,
    ) -> Result<&mz_storage_types::connections::Connection<ReferencedConnection>, SqlCatalogError>
    {
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

    fn uses(&self) -> &ResolvedIds {
        self.uses()
    }

    fn used_by(&self) -> &[GlobalId] {
        self.used_by()
    }

    fn is_subsource(&self) -> bool {
        self.is_subsource()
    }

    fn subsources(&self) -> BTreeSet<GlobalId> {
        self.subsources()
    }

    fn progress_id(&self) -> Option<GlobalId> {
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
}

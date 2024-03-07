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

use chrono::{DateTime, Utc};
use mz_adapter_types::compaction::CompactionWindow;
use mz_adapter_types::connection::ConnectionId;
use mz_compute_client::logging::LogVariant;
use mz_controller::clusters::{
    ClusterRole, ClusterStatus, ProcessId, ReplicaConfig, ReplicaLogging,
};
use mz_controller_types::{ClusterId, ReplicaId};
use mz_expr::refresh_schedule::RefreshSchedule;
use mz_expr::{CollectionPlan, MirScalarExpr, OptimizedMirRelationExpr};
use mz_ore::collections::CollectionExt;
use mz_repr::adt::mz_acl_item::{AclMode, MzAclItem, PrivilegeMap};
use mz_repr::optimize::OptimizerFeatureOverrides;
use mz_repr::role_id::RoleId;
use mz_repr::{Diff, GlobalId, RelationDesc};
use mz_sql::ast::display::AstDisplay;
use mz_sql::ast::{Expr, Raw, Statement, Value, WithOptionValue};
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
    CreateSourcePlan, HirRelationExpr, Ingestion as PlanIngestion, WebhookBodyFormat,
    WebhookHeaders, WebhookValidation,
};
use mz_sql::rbac;
use mz_sql::session::vars::OwnedVarInput;
use mz_sql_parser::ast::ClusterScheduleOptionValue;
use mz_storage_client::controller::IntrospectionType;
use mz_storage_types::connections::inline::ReferencedConnection;
use mz_storage_types::sinks::{KafkaSinkFormat, SinkEnvelope, StorageSinkConnection};
use mz_storage_types::sources::{
    IngestionDescription, SourceConnection, SourceDesc, SourceEnvelope, SourceExport, Timeline,
};
use once_cell::sync::Lazy;
use serde::ser::SerializeSeq;
use serde::{Deserialize, Serialize};
use timely::progress::Antichain;
use tracing::debug;

use crate::builtin::{MZ_INTROSPECTION_CLUSTER, MZ_SYSTEM_CLUSTER};
use crate::durable;

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

#[derive(Debug, Serialize, Clone, PartialEq, Eq)]
pub struct Schema {
    pub name: QualifiedSchemaName,
    pub id: SchemaSpecifier,
    pub oid: u32,
    pub items: BTreeMap<String, GlobalId>,
    pub functions: BTreeMap<String, GlobalId>,
    pub types: BTreeMap<String, GlobalId>,
    pub owner_id: RoleId,
    pub privileges: PrivilegeMap,
}

impl Schema {
    pub fn into_durable_schema(self, database_id: Option<DatabaseId>) -> durable::Schema {
        durable::Schema {
            id: self.id.into(),
            oid: self.oid,
            name: self.name.schema,
            database_id,
            owner_id: self.owner_id,
            privileges: self.privileges.into_all_values().collect(),
        }
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

#[derive(Debug, Serialize, Clone)]
pub struct Cluster {
    pub name: String,
    pub id: ClusterId,
    pub config: ClusterConfig,
    #[serde(skip)]
    pub log_indexes: BTreeMap<LogVariant, GlobalId>,
    /// Objects bound to this cluster. Does not include introspection source
    /// indexes.
    pub bound_objects: BTreeSet<GlobalId>,
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
    pub fn replica_mut(&mut self, replica_id: ReplicaId) -> Option<&mut ClusterReplica> {
        self.replicas_by_id_.get_mut(&replica_id)
    }

    /// Lookup a replica ID by name.
    pub fn replica_id(&self, name: &str) -> Option<ReplicaId> {
        self.replica_id_by_name_.get(name).copied()
    }

    /// Insert a new replica into the cluster.
    ///
    /// Panics if the name or ID are reused.
    pub fn insert_replica(&mut self, replica: ClusterReplica) {
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
    pub fn remove_replica(&mut self, replica_id: ReplicaId) {
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
    pub fn rename_replica(&mut self, replica_id: ReplicaId, to_name: String) {
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

#[derive(Debug, Serialize, Clone)]
pub struct ClusterReplicaProcessStatus {
    pub status: ClusterStatus,
    pub time: DateTime<Utc>,
}

#[derive(Clone, Debug, Serialize)]
pub struct CatalogEntry {
    pub item: CatalogItem,
    #[serde(skip)]
    pub referenced_by: Vec<GlobalId>,
    #[serde(skip)]
    pub used_by: Vec<GlobalId>,
    pub id: GlobalId,
    pub oid: u32,
    pub name: QualifiedItemName,
    pub owner_id: RoleId,
    pub privileges: PrivilegeMap,
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

impl From<CatalogEntry> for durable::Item {
    fn from(entry: CatalogEntry) -> durable::Item {
        durable::Item {
            id: entry.id,
            oid: entry.oid,
            schema_id: entry.name.qualifiers.schema_spec.into(),
            name: entry.name.item,
            create_sql: entry.item.into_serialized(),
            owner_id: entry.owner_id,
            privileges: entry.privileges.into_all_values().collect(),
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct Table {
    pub create_sql: Option<String>,
    pub desc: RelationDesc,
    #[serde(skip)]
    pub defaults: Vec<Expr<Aug>>,
    #[serde(skip)]
    pub conn_id: Option<ConnectionId>,
    pub resolved_ids: ResolvedIds,
    pub custom_logical_compaction_window: Option<CompactionWindow>,
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
    /// Receives data from some other source
    SourceExport { id: GlobalId, output_index: usize },
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
    /// Describes the ingestion to the adapter, which essentially just enriches the a higher-level
    /// [`PlanIngestion`] with a [`ClusterId`].
    pub fn ingestion(
        id: GlobalId,
        ingestion: PlanIngestion,
        instance_id: ClusterId,
    ) -> DataSourceDesc {
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
            source_exports,
            instance_id,
            remap_collection_id: ingestion.progress_subsource,
        })
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct Source {
    pub create_sql: Option<String>,
    // TODO: Unskip: currently blocked on some inner BTreeMap<X, _> problems.
    #[serde(skip)]
    pub data_source: DataSourceDesc,
    pub desc: RelationDesc,
    pub timeline: Timeline,
    pub resolved_ids: ResolvedIds,
    /// This value is ignored for subsources, i.e. for
    /// [`DataSourceDesc::Source`]. Instead, it uses the primary sources logical
    /// compaction window.
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
        id: GlobalId,
        plan: CreateSourcePlan,
        resolved_ids: ResolvedIds,
        custom_logical_compaction_window: Option<CompactionWindow>,
        is_retained_metrics_object: bool,
    ) -> Source {
        Source {
            create_sql: Some(plan.source.create_sql),
            data_source: match plan.source.data_source {
                mz_sql::plan::DataSourceDesc::Ingestion(ingestion) => DataSourceDesc::ingestion(
                    id,
                    ingestion,
                    plan.in_cluster
                        .expect("ingestion-based sources must be given a cluster ID"),
                ),
                mz_sql::plan::DataSourceDesc::Progress => {
                    assert!(
                        plan.in_cluster.is_none(),
                        "subsources must not have a host config or cluster_id defined"
                    );
                    DataSourceDesc::Progress
                }
                mz_sql::plan::DataSourceDesc::SourceExport {
                    ingestion_id,
                    output_index,
                } => {
                    assert!(
                        plan.in_cluster.is_none(),
                        "subsources must not have a host config or cluster_id defined"
                    );
                    DataSourceDesc::SourceExport {
                        id: ingestion_id,
                        output_index,
                    }
                }
                mz_sql::plan::DataSourceDesc::Source => {
                    assert!(
                        plan.in_cluster.is_none(),
                        "subsources must not have a host config or cluster_id defined"
                    );
                    DataSourceDesc::Source
                }
                mz_sql::plan::DataSourceDesc::Webhook {
                    validate_using,
                    body_format,
                    headers,
                } => DataSourceDesc::Webhook {
                    validate_using,
                    body_format,
                    headers,
                    cluster_id: plan
                        .in_cluster
                        .expect("webhook sources must be given a cluster ID"),
                },
            },
            desc: plan.source.desc,
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
            DataSourceDesc::Ingestion(ingestion) => ingestion.desc.connection.name(),
            DataSourceDesc::Progress => "progress",
            DataSourceDesc::SourceExport { .. } => "subsourcev2",
            DataSourceDesc::Source => "subsource",
            DataSourceDesc::Introspection(_) => "source",
            DataSourceDesc::Webhook { .. } => "webhook",
        }
    }

    /// The key and value formats of the source.
    pub fn formats(&self) -> (Option<&str>, Option<&str>) {
        match &self.data_source {
            DataSourceDesc::Ingestion(ingestion) => match &ingestion.desc.encoding {
                Some(encoding) => match &encoding.key {
                    Some(key) => (Some(key.type_()), Some(encoding.value.type_())),
                    None => (None, Some(encoding.value.type_())),
                },
                None => (None, None),
            },
            // This isn't quite right because these values are encoded--just
            // that they're encoded by the source.
            DataSourceDesc::SourceExport { .. } => (None, None),
            DataSourceDesc::Introspection(_)
            | DataSourceDesc::Webhook { .. }
            | DataSourceDesc::Progress
            | DataSourceDesc::Source => (None, None),
        }
    }

    /// Envelope of the source.
    pub fn envelope(&self) -> Option<&str> {
        // Note how "none"/"append-only" is different from `None`. Source
        // sources don't have an envelope (internal logs, for example), while
        // other sources have an envelope that we call the "NONE"-envelope.

        match &self.data_source {
            // NOTE(aljoscha): We could move the block for ingestions into
            // `SourceEnvelope` itself, but that one feels more like an internal
            // thing and adapter should own how we represent envelopes as a
            // string? It would not be hard to convince me otherwise, though.
            DataSourceDesc::Ingestion(ingestion) => match ingestion.desc.envelope() {
                SourceEnvelope::None(_) => Some("none"),
                SourceEnvelope::Upsert(upsert_envelope) => match upsert_envelope.style {
                    mz_storage_types::sources::envelope::UpsertStyle::Default(_) => Some("upsert"),
                    mz_storage_types::sources::envelope::UpsertStyle::Debezium { .. } => {
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
            // This isn't quite right because these sources have an envelope
            // applied--just by their source.
            DataSourceDesc::SourceExport { .. } => None,
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
            DataSourceDesc::SourceExport { .. }
            | DataSourceDesc::Introspection(_)
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
            //  DataSourceDesc::Source represents subsources, which are
            //  accounted for in their primary source's ingestion. However,
            //  maybe we can fix this.
            DataSourceDesc::SourceExport { .. } => 0,
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
    pub connection: StorageSinkConnection<ReferencedConnection>,
    // TODO(guswynn): this probably should just be in the `connection`.
    pub envelope: SinkEnvelope,
    pub with_snapshot: bool,
    pub resolved_ids: ResolvedIds,
    pub cluster_id: ClusterId,
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

    /// Output format of the sink.
    pub fn format(&self) -> &str {
        let StorageSinkConnection::Kafka(connection) = &self.connection;
        match &connection.format {
            KafkaSinkFormat::Avro { .. } => "avro",
            KafkaSinkFormat::Json => "json",
        }
    }

    pub fn connection_id(&self) -> Option<GlobalId> {
        self.connection.connection_id()
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct View {
    pub create_sql: String,
    pub raw_expr: HirRelationExpr,
    pub optimized_expr: OptimizedMirRelationExpr,
    pub desc: RelationDesc,
    pub conn_id: Option<ConnectionId>,
    pub resolved_ids: ResolvedIds,
}

#[derive(Debug, Clone, Serialize)]
pub struct MaterializedView {
    pub create_sql: String,
    pub raw_expr: HirRelationExpr,
    pub optimized_expr: OptimizedMirRelationExpr,
    pub desc: RelationDesc,
    pub resolved_ids: ResolvedIds,
    pub cluster_id: ClusterId,
    pub non_null_assertions: Vec<usize>,
    pub custom_logical_compaction_window: Option<CompactionWindow>,
    pub refresh_schedule: Option<RefreshSchedule>,
    // The initial `as_of` of the storage collection associated with the materialized view.
    // (The dataflow's initial `as_of` can be different.)
    pub initial_as_of: Option<Antichain<mz_repr::Timestamp>>,
}

#[derive(Debug, Clone, Serialize)]
pub struct Index {
    pub create_sql: String,
    pub on: GlobalId,
    pub keys: Vec<MirScalarExpr>,
    pub conn_id: Option<ConnectionId>,
    pub resolved_ids: ResolvedIds,
    pub cluster_id: ClusterId,
    pub custom_logical_compaction_window: Option<CompactionWindow>,
    pub is_retained_metrics_object: bool,
}

#[derive(Debug, Clone, Serialize)]
pub struct Type {
    pub create_sql: Option<String>,
    #[serde(skip)]
    pub details: CatalogTypeDetails<IdReference>,
    pub desc: Option<RelationDesc>,
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
    pub fn typ(&self) -> mz_sql::catalog::CatalogItemType {
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

    /// Whether this item represents a storage collection.
    pub fn is_storage_collection(&self) -> bool {
        match self {
            CatalogItem::Table(_) | CatalogItem::Source(_) | CatalogItem::MaterializedView(_) => {
                true
            }
            CatalogItem::Log(_)
            | CatalogItem::Sink(_)
            | CatalogItem::View(_)
            | CatalogItem::Index(_)
            | CatalogItem::Type(_)
            | CatalogItem::Func(_)
            | CatalogItem::Secret(_)
            | CatalogItem::Connection(_) => false,
        }
    }

    pub fn desc(&self, name: &FullItemName) -> Result<Cow<RelationDesc>, SqlCatalogError> {
        self.desc_opt().ok_or(SqlCatalogError::InvalidDependency {
            name: name.to_string(),
            typ: self.typ(),
        })
    }

    pub fn desc_opt(&self) -> Option<Cow<RelationDesc>> {
        match &self {
            CatalogItem::Source(src) => Some(Cow::Borrowed(&src.desc)),
            CatalogItem::Log(log) => Some(Cow::Owned(log.variant.desc())),
            CatalogItem::Table(tbl) => Some(Cow::Borrowed(&tbl.desc)),
            CatalogItem::View(view) => Some(Cow::Borrowed(&view.desc)),
            CatalogItem::MaterializedView(mview) => Some(Cow::Borrowed(&mview.desc)),
            CatalogItem::Type(typ) => typ.desc.as_ref().map(Cow::Borrowed),
            CatalogItem::Func(_)
            | CatalogItem::Index(_)
            | CatalogItem::Sink(_)
            | CatalogItem::Secret(_)
            | CatalogItem::Connection(_) => None,
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
                DataSourceDesc::SourceExport { .. }
                | DataSourceDesc::Introspection(_)
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

    /// Collects the identifiers of the objects that were encountered when
    /// resolving names in the item's DDL statement.
    pub fn references(&self) -> &ResolvedIds {
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

    /// Collects the identifiers of the objects used by this [`CatalogItem`].
    ///
    /// Like [`CatalogItem::references()`] but also includes objects that are not directly
    /// referenced. For example this will include any catalog objects used to implement functions
    /// and casts in the item.
    pub fn uses(&self) -> BTreeSet<GlobalId> {
        let mut uses = self.references().0.clone();
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
            CatalogItem::View(view) => uses.extend(view.raw_expr.depends_on()),
            CatalogItem::MaterializedView(mview) => uses.extend(mview.raw_expr.depends_on()),
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
            | CatalogItem::Connection(_) => None,
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
        }
    }

    /// Updates the retain history for an item. Returns the previous retain history value. Returns
    /// an error if this item does not support retain history.
    pub fn update_retain_history(
        &mut self,
        value: Option<Value>,
        window: CompactionWindow,
    ) -> Result<Option<WithOptionValue<Raw>>, ()> {
        let update = |ast: &mut Statement<Raw>| {
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
            let previous = match ast {
                Statement::CreateTable(ref mut stmt) => {
                    update_retain_history!(stmt, TableOption, TableOptionName)
                }
                Statement::CreateIndex(ref mut stmt) => {
                    update_retain_history!(stmt, IndexOption, IndexOptionName)
                }
                Statement::CreateSource(ref mut stmt) => {
                    update_retain_history!(stmt, CreateSourceOption, CreateSourceOptionName)
                }
                Statement::CreateMaterializedView(ref mut stmt) => {
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
            | CatalogItem::Connection(Connection { create_sql, .. }) => Some(create_sql),
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
            | CatalogItem::Connection(_) => None,
        }
    }

    pub fn cluster_id(&self) -> Option<ClusterId> {
        match self {
            CatalogItem::MaterializedView(mv) => Some(mv.cluster_id),
            CatalogItem::Index(index) => Some(index.cluster_id),
            CatalogItem::Source(source) => match &source.data_source {
                DataSourceDesc::Ingestion(ingestion) => Some(ingestion.instance_id),
                // This is somewhat of a lie because the export runs on the same
                // cluster as its ingestion but we don't yet have a way of
                // cross-referencing the items
                DataSourceDesc::SourceExport { .. } => None,
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
            | CatalogItem::Connection(_) => None,
        }
    }

    /// Mutable access to the custom compaction window, or None if this type does not support custom
    /// compaction windows.
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
            | CatalogItem::Connection(_) => return None,
        };
        Some(cw)
    }

    /// The initial compaction window, for objects that have one; that is,
    /// tables, sources, indexes, and MVs.
    ///
    /// If `custom_logical_compaction_window()` returns something, use
    /// that.  Otherwise, use a sensible default (currently 1s).
    ///
    /// For objects that do not have the concept of compaction window,
    /// return None.
    pub fn initial_logical_compaction_window(&self) -> Option<CompactionWindow> {
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
            | CatalogItem::Connection(_) => false,
        }
    }

    pub fn to_serialized(&self) -> String {
        match self {
            CatalogItem::Table(table) => table
                .create_sql
                .as_ref()
                .expect("builtin tables cannot be serialized")
                .clone(),
            CatalogItem::Log(_) => unreachable!("builtin logs cannot be serialized"),
            CatalogItem::Source(source) => {
                assert!(
                    !matches!(source.data_source, DataSourceDesc::Introspection(_)),
                    "cannot serialize introspection/builtin sources",
                );
                source
                    .create_sql
                    .as_ref()
                    .expect("builtin sources cannot be serialized")
                    .clone()
            }
            CatalogItem::View(view) => view.create_sql.clone(),
            CatalogItem::MaterializedView(mview) => mview.create_sql.clone(),
            CatalogItem::Index(index) => index.create_sql.clone(),
            CatalogItem::Sink(sink) => sink.create_sql.clone(),
            CatalogItem::Type(typ) => typ
                .create_sql
                .as_ref()
                .expect("builtin types cannot be serialized")
                .clone(),
            CatalogItem::Secret(secret) => secret.create_sql.clone(),
            CatalogItem::Connection(connection) => connection.create_sql.clone(),
            CatalogItem::Func(_) => unreachable!("cannot serialize functions yet"),
        }
    }

    pub fn into_serialized(self) -> String {
        match self {
            CatalogItem::Table(table) => table
                .create_sql
                .expect("builtin tables cannot be serialized"),
            CatalogItem::Log(_) => unreachable!("builtin logs cannot be serialized"),
            CatalogItem::Source(source) => {
                assert!(
                    !matches!(source.data_source, DataSourceDesc::Introspection(_)),
                    "cannot serialize introspection/builtin sources",
                );
                source
                    .create_sql
                    .expect("builtin sources cannot be serialized")
            }
            CatalogItem::View(view) => view.create_sql,
            CatalogItem::MaterializedView(mview) => mview.create_sql,
            CatalogItem::Index(index) => index.create_sql,
            CatalogItem::Sink(sink) => sink.create_sql,
            CatalogItem::Type(typ) => typ.create_sql.expect("builtin types cannot be serialized"),
            CatalogItem::Secret(secret) => secret.create_sql,
            CatalogItem::Connection(connection) => connection.create_sql,
            CatalogItem::Func(_) => unreachable!("cannot serialize functions yet"),
        }
    }
}

impl CatalogEntry {
    /// Like [`CatalogEntry::desc_opt`], but returns an error if the catalog
    /// entry is not of a type that has a description.
    pub fn desc(&self, name: &FullItemName) -> Result<Cow<RelationDesc>, SqlCatalogError> {
        self.item.desc(name)
    }

    /// Reports the description of the rows produced by this catalog entry, if
    /// this catalog entry produces rows.
    pub fn desc_opt(&self) -> Option<Cow<RelationDesc>> {
        self.item.desc_opt()
    }

    /// Reports if the item has columns.
    pub fn has_columns(&self) -> bool {
        self.item.desc_opt().is_some()
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

    /// Reports whether this catalog entry is a subsource.
    pub fn source_exports(&self) -> BTreeMap<GlobalId, usize> {
        match &self.item() {
            CatalogItem::Source(source) => match &source.data_source {
                DataSourceDesc::Ingestion(ingestion) => ingestion
                    .source_exports
                    .iter()
                    .map(|(id, export)| (*id, export.output_index))
                    .collect(),
                _ => BTreeMap::new(),
            },
            _ => BTreeMap::new(),
        }
    }

    /// Reports whether this catalog entry is a progress source.
    pub fn is_progress_source(&self) -> bool {
        self.item().is_progress_source()
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
                DataSourceDesc::SourceExport { .. }
                | DataSourceDesc::Introspection(_)
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
                DataSourceDesc::SourceExport { .. }
                | DataSourceDesc::Introspection(_)
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
    pub fn uses(&self) -> BTreeSet<GlobalId> {
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

    /// Returns the identifiers of the dataflows that are directly referenced by this dataflow.
    pub fn referenced_by(&self) -> &[GlobalId] {
        &self.referenced_by
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
        }
    }
}

impl From<durable::ClusterConfig> for ClusterConfig {
    fn from(config: durable::ClusterConfig) -> Self {
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
    pub replication_factor: u32,
    pub disk: bool,
    pub optimizer_feature_overrides: OptimizerFeatureOverrides,
    pub schedule: ClusterScheduleOptionValue,
}

impl From<ClusterVariantManaged> for durable::ClusterVariantManaged {
    fn from(managed: ClusterVariantManaged) -> Self {
        Self {
            size: managed.size,
            availability_zones: managed.availability_zones,
            logging: managed.logging,
            replication_factor: managed.replication_factor,
            disk: managed.disk,
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
            disk: managed.disk,
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

    fn item_ids(&self) -> Box<dyn Iterator<Item = GlobalId> + '_> {
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

impl mz_sql::catalog::CatalogCluster<'_> for Cluster {
    fn name(&self) -> &str {
        &self.name
    }

    fn id(&self) -> ClusterId {
        self.id
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

    fn managed_size(&self) -> Option<&str> {
        match &self.config.variant {
            ClusterVariant::Managed(ClusterVariantManaged { size, .. }) => Some(size),
            _ => None,
        }
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

    fn references(&self) -> &ResolvedIds {
        self.references()
    }

    fn uses(&self) -> BTreeSet<GlobalId> {
        self.uses()
    }

    fn referenced_by(&self) -> &[GlobalId] {
        self.referenced_by()
    }

    fn used_by(&self) -> &[GlobalId] {
        self.used_by()
    }

    fn is_subsource(&self) -> bool {
        self.is_subsource()
    }

    fn source_exports(&self) -> BTreeMap<GlobalId, usize> {
        self.source_exports()
    }

    fn is_progress_source(&self) -> bool {
        self.is_progress_source()
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

/// A single update to the catalog state.
#[derive(Debug)]
pub struct StateUpdate {
    pub kind: StateUpdateKind,
    // TODO(jkosh44) Add timestamps.
    pub diff: Diff,
}

/// The contents of a single state update.
///
/// Variants are listed in dependency order.
#[derive(Debug)]
pub enum StateUpdateKind {
    Role(durable::objects::Role),
    Database(durable::objects::Database),
    Schema(durable::objects::Schema),
    DefaultPrivilege(durable::objects::DefaultPrivilege),
    SystemPrivilege(MzAclItem),
    SystemConfiguration(durable::objects::SystemConfiguration),
    Comment(durable::objects::Comment),
    // TODO(jkosh44) Add all other object variants.
}

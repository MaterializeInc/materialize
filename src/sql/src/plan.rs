// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! SQL planning.
//!
//! SQL planning is the process of taking the abstract syntax tree of a
//! [`Statement`] and turning it into a [`Plan`] that the dataflow layer can
//! execute.
//!
//! Statements must be purified before they can be planned. See the
//! [`pure`](crate::pure) module for details.

// Internal module layout.
//
// The entry point for planning is `statement::handle_statement`. That function
// dispatches to a more specific `handle` function for the particular statement
// type. For most statements, this `handle` function is uninteresting and short,
// but anything involving a `SELECT` statement gets complicated. `SELECT`
// queries wind through the functions in the `query` module, starting with
// `plan_root_query` and fanning out based on the contents of the `SELECT`
// statement.

use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroUsize;
use std::time::Duration;

use chrono::{DateTime, Utc};
use enum_kinds::EnumKind;
use maplit::btreeset;
use mz_adapter_types::compaction::CompactionWindow;
use mz_controller_types::{ClusterId, ReplicaId};
use mz_expr::refresh_schedule::RefreshSchedule;
use mz_expr::{CollectionPlan, ColumnOrder, MirRelationExpr, MirScalarExpr, RowSetFinishing};
use mz_ore::now::{self, NOW_ZERO};
use mz_pgcopy::CopyFormatParams;
use mz_repr::adt::mz_acl_item::{AclMode, MzAclItem};
use mz_repr::explain::{ExplainConfig, ExplainFormat};
use mz_repr::optimize::OptimizerFeatureOverrides;
use mz_repr::role_id::RoleId;
use mz_repr::{ColumnName, Diff, GlobalId, RelationDesc, Row, ScalarType, Timestamp};
use mz_sql_parser::ast::{
    AlterSourceAddSubsourceOption, ClusterScheduleOptionValue, ConnectionOptionName,
    CreateSourceSubsource, QualifiedReplica, TransactionIsolationLevel, TransactionMode, Value,
    WithOptionValue,
};
use mz_storage_types::connections::inline::ReferencedConnection;
use mz_storage_types::sinks::{SinkEnvelope, StorageSinkConnection};
use mz_storage_types::sources::{SourceDesc, Timeline};
use proptest_derive::Arbitrary;
use serde::{Deserialize, Serialize};

use crate::ast::{
    ExplainStage, Expr, FetchDirection, NoticeSeverity, Raw, Statement, StatementKind,
    TransactionAccessMode,
};
use crate::catalog::{
    CatalogType, DefaultPrivilegeAclItem, DefaultPrivilegeObject, IdReference, ObjectType,
    RoleAttributes,
};
use crate::names::{
    Aug, CommentObjectId, FullItemName, ObjectId, QualifiedItemName, ResolvedDatabaseSpecifier,
    ResolvedIds, SchemaSpecifier, SystemObjectId,
};

pub(crate) mod error;
pub(crate) mod explain;
pub(crate) mod expr;
pub(crate) mod literal;
pub(crate) mod lowering;
pub(crate) mod notice;
pub(crate) mod plan_utils;
pub(crate) mod query;
pub(crate) mod scope;
pub(crate) mod side_effecting_func;
pub(crate) mod statement;
pub(crate) mod transform_ast;
pub(crate) mod transform_expr;
pub(crate) mod typeconv;
pub(crate) mod with_options;

use crate::plan;
use crate::plan::with_options::OptionalDuration;
pub use error::PlanError;
pub use explain::normalize_subqueries;
pub use expr::{
    AggregateExpr, CoercibleScalarExpr, Hir, HirRelationExpr, HirScalarExpr, JoinKind,
    WindowExprType,
};
pub use lowering::Config as HirToMirConfig;
pub use notice::PlanNotice;
pub use query::{ExprContext, QueryContext, QueryLifetime};
pub use scope::Scope;
pub use side_effecting_func::SideEffectingFunc;
pub use statement::ddl::{PlannedAlterRoleOption, PlannedRoleVariable};
pub use statement::{
    describe, plan, plan_copy_from, resolve_cluster_for_materialized_view, StatementContext,
    StatementDesc,
};

/// Instructions for executing a SQL query.
#[derive(Debug, EnumKind)]
#[enum_kind(PlanKind)]
pub enum Plan {
    CreateConnection(CreateConnectionPlan),
    CreateDatabase(CreateDatabasePlan),
    CreateSchema(CreateSchemaPlan),
    CreateRole(CreateRolePlan),
    CreateCluster(CreateClusterPlan),
    CreateClusterReplica(CreateClusterReplicaPlan),
    CreateSource(CreateSourcePlan),
    CreateSources(Vec<CreateSourcePlans>),
    CreateSecret(CreateSecretPlan),
    CreateSink(CreateSinkPlan),
    CreateTable(CreateTablePlan),
    CreateView(CreateViewPlan),
    CreateMaterializedView(CreateMaterializedViewPlan),
    CreateIndex(CreateIndexPlan),
    CreateType(CreateTypePlan),
    Comment(CommentPlan),
    DiscardTemp,
    DiscardAll,
    DropObjects(DropObjectsPlan),
    DropOwned(DropOwnedPlan),
    EmptyQuery,
    ShowAllVariables,
    ShowCreate(ShowCreatePlan),
    ShowColumns(ShowColumnsPlan),
    ShowVariable(ShowVariablePlan),
    InspectShard(InspectShardPlan),
    SetVariable(SetVariablePlan),
    ResetVariable(ResetVariablePlan),
    SetTransaction(SetTransactionPlan),
    StartTransaction(StartTransactionPlan),
    CommitTransaction(CommitTransactionPlan),
    AbortTransaction(AbortTransactionPlan),
    Select(SelectPlan),
    Subscribe(SubscribePlan),
    CopyFrom(CopyFromPlan),
    CopyTo(CopyToPlan),
    ExplainPlan(ExplainPlanPlan),
    ExplainPushdown(ExplainPushdownPlan),
    ExplainTimestamp(ExplainTimestampPlan),
    ExplainSinkSchema(ExplainSinkSchemaPlan),
    Insert(InsertPlan),
    AlterCluster(AlterClusterPlan),
    AlterClusterSwap(AlterClusterSwapPlan),
    AlterNoop(AlterNoopPlan),
    AlterSetCluster(AlterSetClusterPlan),
    AlterConnection(AlterConnectionPlan),
    AlterSource(AlterSourcePlan),
    PurifiedAlterSource {
        // The `ALTER SOURCE` plan
        alter_source: AlterSourcePlan,
        // The plan to create any subsources added in the `ALTER SOURCE` statement.
        subsources: Vec<CreateSourcePlans>,
    },
    AlterClusterRename(AlterClusterRenamePlan),
    AlterClusterReplicaRename(AlterClusterReplicaRenamePlan),
    AlterItemRename(AlterItemRenamePlan),
    AlterItemSwap(AlterItemSwapPlan),
    AlterSchemaRename(AlterSchemaRenamePlan),
    AlterSchemaSwap(AlterSchemaSwapPlan),
    AlterSecret(AlterSecretPlan),
    AlterSystemSet(AlterSystemSetPlan),
    AlterSystemReset(AlterSystemResetPlan),
    AlterSystemResetAll(AlterSystemResetAllPlan),
    AlterRole(AlterRolePlan),
    AlterOwner(AlterOwnerPlan),
    Declare(DeclarePlan),
    Fetch(FetchPlan),
    Close(ClosePlan),
    ReadThenWrite(ReadThenWritePlan),
    Prepare(PreparePlan),
    Execute(ExecutePlan),
    Deallocate(DeallocatePlan),
    Raise(RaisePlan),
    GrantRole(GrantRolePlan),
    RevokeRole(RevokeRolePlan),
    GrantPrivileges(GrantPrivilegesPlan),
    RevokePrivileges(RevokePrivilegesPlan),
    AlterDefaultPrivileges(AlterDefaultPrivilegesPlan),
    ReassignOwned(ReassignOwnedPlan),
    SideEffectingFunc(SideEffectingFunc),
    ValidateConnection(ValidateConnectionPlan),
    AlterRetainHistory(AlterRetainHistoryPlan),
}

impl Plan {
    /// Expresses which [`StatementKind`] can generate which set of
    /// [`PlanKind`].
    pub fn generated_from(stmt: &StatementKind) -> &'static [PlanKind] {
        match stmt {
            StatementKind::AlterCluster => &[PlanKind::AlterNoop, PlanKind::AlterCluster],
            StatementKind::AlterConnection => &[PlanKind::AlterNoop, PlanKind::AlterConnection],
            StatementKind::AlterDefaultPrivileges => &[PlanKind::AlterDefaultPrivileges],
            StatementKind::AlterIndex => &[PlanKind::AlterRetainHistory, PlanKind::AlterNoop],
            StatementKind::AlterObjectRename => &[
                PlanKind::AlterClusterRename,
                PlanKind::AlterClusterReplicaRename,
                PlanKind::AlterItemRename,
                PlanKind::AlterSchemaRename,
                PlanKind::AlterNoop,
            ],
            StatementKind::AlterObjectSwap => &[
                PlanKind::AlterClusterSwap,
                PlanKind::AlterItemSwap,
                PlanKind::AlterSchemaSwap,
                PlanKind::AlterNoop,
            ],
            StatementKind::AlterRole => &[PlanKind::AlterRole],
            StatementKind::AlterSecret => &[PlanKind::AlterNoop, PlanKind::AlterSecret],
            StatementKind::AlterSetCluster => &[PlanKind::AlterNoop, PlanKind::AlterSetCluster],
            // TODO: If we ever support ALTER SINK again, this will need to be changed
            StatementKind::AlterSink => &[PlanKind::AlterNoop],
            StatementKind::AlterSource => &[
                PlanKind::AlterNoop,
                PlanKind::AlterSource,
                PlanKind::AlterRetainHistory,
            ],
            StatementKind::AlterSystemReset => &[PlanKind::AlterNoop, PlanKind::AlterSystemReset],
            StatementKind::AlterSystemResetAll => {
                &[PlanKind::AlterNoop, PlanKind::AlterSystemResetAll]
            }
            StatementKind::AlterSystemSet => &[PlanKind::AlterNoop, PlanKind::AlterSystemSet],
            StatementKind::AlterOwner => &[PlanKind::AlterNoop, PlanKind::AlterOwner],
            StatementKind::Close => &[PlanKind::Close],
            StatementKind::Comment => &[PlanKind::Comment],
            StatementKind::Commit => &[PlanKind::CommitTransaction],
            StatementKind::Copy => &[
                PlanKind::CopyFrom,
                PlanKind::Select,
                PlanKind::Subscribe,
                PlanKind::CopyTo,
            ],
            StatementKind::CreateCluster => &[PlanKind::CreateCluster],
            StatementKind::CreateClusterReplica => &[PlanKind::CreateClusterReplica],
            StatementKind::CreateConnection => &[PlanKind::CreateConnection],
            StatementKind::CreateDatabase => &[PlanKind::CreateDatabase],
            StatementKind::CreateIndex => &[PlanKind::CreateIndex],
            StatementKind::CreateMaterializedView => &[PlanKind::CreateMaterializedView],
            StatementKind::CreateRole => &[PlanKind::CreateRole],
            StatementKind::CreateSchema => &[PlanKind::CreateSchema],
            StatementKind::CreateSecret => &[PlanKind::CreateSecret],
            StatementKind::CreateSink => &[PlanKind::CreateSink],
            StatementKind::CreateSource
            | StatementKind::CreateSubsource
            | StatementKind::CreateWebhookSource => &[PlanKind::CreateSource],
            StatementKind::CreateTable => &[PlanKind::CreateTable],
            StatementKind::CreateType => &[PlanKind::CreateType],
            StatementKind::CreateView => &[PlanKind::CreateView],
            StatementKind::Deallocate => &[PlanKind::Deallocate],
            StatementKind::Declare => &[PlanKind::Declare],
            StatementKind::Delete => &[PlanKind::ReadThenWrite],
            StatementKind::Discard => &[PlanKind::DiscardAll, PlanKind::DiscardTemp],
            StatementKind::DropObjects => &[PlanKind::DropObjects],
            StatementKind::DropOwned => &[PlanKind::DropOwned],
            StatementKind::Execute => &[PlanKind::Execute],
            StatementKind::ExplainPlan => &[PlanKind::ExplainPlan],
            StatementKind::ExplainPushdown => &[PlanKind::ExplainPushdown],
            StatementKind::ExplainTimestamp => &[PlanKind::ExplainTimestamp],
            StatementKind::ExplainSinkSchema => &[PlanKind::ExplainSinkSchema],
            StatementKind::Fetch => &[PlanKind::Fetch],
            StatementKind::GrantPrivileges => &[PlanKind::GrantPrivileges],
            StatementKind::GrantRole => &[PlanKind::GrantRole],
            StatementKind::Insert => &[PlanKind::Insert],
            StatementKind::Prepare => &[PlanKind::Prepare],
            StatementKind::Raise => &[PlanKind::Raise],
            StatementKind::ReassignOwned => &[PlanKind::ReassignOwned],
            StatementKind::ResetVariable => &[PlanKind::ResetVariable],
            StatementKind::RevokePrivileges => &[PlanKind::RevokePrivileges],
            StatementKind::RevokeRole => &[PlanKind::RevokeRole],
            StatementKind::Rollback => &[PlanKind::AbortTransaction],
            StatementKind::Select => &[PlanKind::Select, PlanKind::SideEffectingFunc],
            StatementKind::SetTransaction => &[PlanKind::SetTransaction],
            StatementKind::SetVariable => &[PlanKind::SetVariable],
            StatementKind::Show => &[
                PlanKind::Select,
                PlanKind::ShowVariable,
                PlanKind::ShowCreate,
                PlanKind::ShowColumns,
                PlanKind::ShowAllVariables,
                PlanKind::InspectShard,
            ],
            StatementKind::StartTransaction => &[PlanKind::StartTransaction],
            StatementKind::Subscribe => &[PlanKind::Subscribe],
            StatementKind::Update => &[PlanKind::ReadThenWrite],
            StatementKind::ValidateConnection => &[PlanKind::ValidateConnection],
            StatementKind::AlterRetainHistory => &[PlanKind::AlterRetainHistory],
        }
    }

    /// Returns a human readable name of the plan. Meant for use in messages sent back to a user.
    pub fn name(&self) -> &str {
        match self {
            Plan::CreateConnection(_) => "create connection",
            Plan::CreateDatabase(_) => "create database",
            Plan::CreateSchema(_) => "create schema",
            Plan::CreateRole(_) => "create role",
            Plan::CreateCluster(_) => "create cluster",
            Plan::CreateClusterReplica(_) => "create cluster replica",
            Plan::CreateSource(_) => "create source",
            Plan::CreateSources(_) => "create source",
            Plan::CreateSecret(_) => "create secret",
            Plan::CreateSink(_) => "create sink",
            Plan::CreateTable(_) => "create table",
            Plan::CreateView(_) => "create view",
            Plan::CreateMaterializedView(_) => "create materialized view",
            Plan::CreateIndex(_) => "create index",
            Plan::CreateType(_) => "create type",
            Plan::Comment(_) => "comment",
            Plan::DiscardTemp => "discard temp",
            Plan::DiscardAll => "discard all",
            Plan::DropObjects(plan) => match plan.object_type {
                ObjectType::Table => "drop table",
                ObjectType::View => "drop view",
                ObjectType::MaterializedView => "drop materialized view",
                ObjectType::Source => "drop source",
                ObjectType::Sink => "drop sink",
                ObjectType::Index => "drop index",
                ObjectType::Type => "drop type",
                ObjectType::Role => "drop roles",
                ObjectType::Cluster => "drop clusters",
                ObjectType::ClusterReplica => "drop cluster replicas",
                ObjectType::Secret => "drop secret",
                ObjectType::Connection => "drop connection",
                ObjectType::Database => "drop database",
                ObjectType::Schema => "drop schema",
                ObjectType::Func => "drop function",
            },
            Plan::DropOwned(_) => "drop owned",
            Plan::EmptyQuery => "do nothing",
            Plan::ShowAllVariables => "show all variables",
            Plan::ShowCreate(_) => "show create",
            Plan::ShowColumns(_) => "show columns",
            Plan::ShowVariable(_) => "show variable",
            Plan::InspectShard(_) => "inspect shard",
            Plan::SetVariable(_) => "set variable",
            Plan::ResetVariable(_) => "reset variable",
            Plan::SetTransaction(_) => "set transaction",
            Plan::StartTransaction(_) => "start transaction",
            Plan::CommitTransaction(_) => "commit",
            Plan::AbortTransaction(_) => "abort",
            Plan::Select(_) => "select",
            Plan::Subscribe(_) => "subscribe",
            Plan::CopyFrom(_) => "copy from",
            Plan::CopyTo(_) => "copy to",
            Plan::ExplainPlan(_) => "explain plan",
            Plan::ExplainPushdown(_) => "EXPLAIN FILTER PUSHDOWN",
            Plan::ExplainTimestamp(_) => "explain timestamp",
            Plan::ExplainSinkSchema(_) => "explain schema",
            Plan::Insert(_) => "insert",
            Plan::AlterNoop(plan) => match plan.object_type {
                ObjectType::Table => "alter table",
                ObjectType::View => "alter view",
                ObjectType::MaterializedView => "alter materialized view",
                ObjectType::Source => "alter source",
                ObjectType::Sink => "alter sink",
                ObjectType::Index => "alter index",
                ObjectType::Type => "alter type",
                ObjectType::Role => "alter role",
                ObjectType::Cluster => "alter cluster",
                ObjectType::ClusterReplica => "alter cluster replica",
                ObjectType::Secret => "alter secret",
                ObjectType::Connection => "alter connection",
                ObjectType::Database => "alter database",
                ObjectType::Schema => "alter schema",
                ObjectType::Func => "alter function",
            },
            Plan::AlterCluster(_) => "alter cluster",
            Plan::AlterClusterRename(_) => "alter cluster rename",
            Plan::AlterClusterSwap(_) => "alter cluster swap",
            Plan::AlterClusterReplicaRename(_) => "alter cluster replica rename",
            Plan::AlterSetCluster(_) => "alter set cluster",
            Plan::AlterConnection(_) => "alter connection",
            Plan::AlterSource(_) | Plan::PurifiedAlterSource { .. } => "alter source",
            Plan::AlterItemRename(_) => "rename item",
            Plan::AlterItemSwap(_) => "swap item",
            Plan::AlterSchemaRename(_) => "alter rename schema",
            Plan::AlterSchemaSwap(_) => "alter swap schema",
            Plan::AlterSecret(_) => "alter secret",
            Plan::AlterSystemSet(_) => "alter system",
            Plan::AlterSystemReset(_) => "alter system",
            Plan::AlterSystemResetAll(_) => "alter system",
            Plan::AlterRole(_) => "alter role",
            Plan::AlterOwner(plan) => match plan.object_type {
                ObjectType::Table => "alter table owner",
                ObjectType::View => "alter view owner",
                ObjectType::MaterializedView => "alter materialized view owner",
                ObjectType::Source => "alter source owner",
                ObjectType::Sink => "alter sink owner",
                ObjectType::Index => "alter index owner",
                ObjectType::Type => "alter type owner",
                ObjectType::Role => "alter role owner",
                ObjectType::Cluster => "alter cluster owner",
                ObjectType::ClusterReplica => "alter cluster replica owner",
                ObjectType::Secret => "alter secret owner",
                ObjectType::Connection => "alter connection owner",
                ObjectType::Database => "alter database owner",
                ObjectType::Schema => "alter schema owner",
                ObjectType::Func => "alter function owner",
            },
            Plan::Declare(_) => "declare",
            Plan::Fetch(_) => "fetch",
            Plan::Close(_) => "close",
            Plan::ReadThenWrite(plan) => match plan.kind {
                MutationKind::Insert => "insert into select",
                MutationKind::Update => "update",
                MutationKind::Delete => "delete",
            },
            Plan::Prepare(_) => "prepare",
            Plan::Execute(_) => "execute",
            Plan::Deallocate(_) => "deallocate",
            Plan::Raise(_) => "raise",
            Plan::GrantRole(_) => "grant role",
            Plan::RevokeRole(_) => "revoke role",
            Plan::GrantPrivileges(_) => "grant privilege",
            Plan::RevokePrivileges(_) => "revoke privilege",
            Plan::AlterDefaultPrivileges(_) => "alter default privileges",
            Plan::ReassignOwned(_) => "reassign owned",
            Plan::SideEffectingFunc(_) => "side effecting func",
            Plan::ValidateConnection(_) => "validate connection",
            Plan::AlterRetainHistory(_) => "alter retain history",
        }
    }
}

#[derive(Debug)]
pub struct StartTransactionPlan {
    pub access: Option<TransactionAccessMode>,
    pub isolation_level: Option<TransactionIsolationLevel>,
}

#[derive(Debug)]
pub enum TransactionType {
    Explicit,
    Implicit,
}

impl TransactionType {
    pub fn is_explicit(&self) -> bool {
        matches!(self, TransactionType::Explicit)
    }

    pub fn is_implicit(&self) -> bool {
        matches!(self, TransactionType::Implicit)
    }
}

#[derive(Debug)]
pub struct CommitTransactionPlan {
    pub transaction_type: TransactionType,
}

#[derive(Debug)]
pub struct AbortTransactionPlan {
    pub transaction_type: TransactionType,
}

#[derive(Debug)]
pub struct CreateDatabasePlan {
    pub name: String,
    pub if_not_exists: bool,
}

#[derive(Debug)]
pub struct CreateSchemaPlan {
    pub database_spec: ResolvedDatabaseSpecifier,
    pub schema_name: String,
    pub if_not_exists: bool,
}

#[derive(Debug)]
pub struct CreateRolePlan {
    pub name: String,
    pub attributes: RoleAttributes,
}

#[derive(Debug)]
pub struct CreateClusterPlan {
    pub name: String,
    pub variant: CreateClusterVariant,
}

#[derive(Debug)]
pub enum CreateClusterVariant {
    Managed(CreateClusterManagedPlan),
    Unmanaged(CreateClusterUnmanagedPlan),
}

#[derive(Debug)]
pub struct CreateClusterUnmanagedPlan {
    pub replicas: Vec<(String, ReplicaConfig)>,
}

#[derive(Debug)]
pub struct CreateClusterManagedPlan {
    pub replication_factor: u32,
    pub size: String,
    pub availability_zones: Vec<String>,
    pub compute: ComputeReplicaConfig,
    pub disk: bool,
    pub optimizer_feature_overrides: OptimizerFeatureOverrides,
    pub schedule: ClusterScheduleOptionValue,
}

#[derive(Debug)]
pub struct CreateClusterReplicaPlan {
    pub cluster_id: ClusterId,
    pub name: String,
    pub config: ReplicaConfig,
}

/// Configuration of introspection for a cluster replica.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialOrd, Ord, PartialEq, Eq)]
pub struct ComputeReplicaIntrospectionConfig {
    /// Whether to introspect the introspection.
    pub debugging: bool,
    /// The interval at which to introspect.
    pub interval: Duration,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ComputeReplicaConfig {
    pub introspection: Option<ComputeReplicaIntrospectionConfig>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ReplicaConfig {
    Unorchestrated {
        storagectl_addrs: Vec<String>,
        storage_addrs: Vec<String>,
        computectl_addrs: Vec<String>,
        compute_addrs: Vec<String>,
        workers: usize,
        compute: ComputeReplicaConfig,
    },
    Orchestrated {
        size: String,
        availability_zone: Option<String>,
        compute: ComputeReplicaConfig,
        disk: bool,
        internal: bool,
        billed_as: Option<String>,
    },
}

#[derive(Debug)]
pub struct CreateSourcePlan {
    pub name: QualifiedItemName,
    pub source: Source,
    pub if_not_exists: bool,
    pub timeline: Timeline,
    // None for subsources, which run on the parent cluster.
    pub in_cluster: Option<ClusterId>,
}

#[derive(Debug)]
pub struct CreateSourcePlans {
    pub source_id: GlobalId,
    pub plan: CreateSourcePlan,
    pub resolved_ids: ResolvedIds,
}

#[derive(Debug)]
pub struct CreateConnectionPlan {
    pub name: QualifiedItemName,
    pub if_not_exists: bool,
    pub connection: Connection,
    pub validate: bool,
}

#[derive(Debug)]
pub struct ValidateConnectionPlan {
    pub id: GlobalId,
    /// The connection to validate.
    pub connection: mz_storage_types::connections::Connection<ReferencedConnection>,
}

#[derive(Debug)]
pub struct CreateSecretPlan {
    pub name: QualifiedItemName,
    pub secret: Secret,
    pub if_not_exists: bool,
}

#[derive(Debug)]
pub struct CreateSinkPlan {
    pub name: QualifiedItemName,
    pub sink: Sink,
    pub with_snapshot: bool,
    pub if_not_exists: bool,
    pub in_cluster: ClusterId,
}

#[derive(Debug)]
pub struct CreateTablePlan {
    pub name: QualifiedItemName,
    pub table: Table,
    pub if_not_exists: bool,
}

#[derive(Debug, Clone)]
pub struct CreateViewPlan {
    pub name: QualifiedItemName,
    pub view: View,
    /// The ID of the object that this view is replacing, if any.
    pub replace: Option<GlobalId>,
    /// The IDs of all objects that need to be dropped. This includes `replace` and any dependents.
    pub drop_ids: Vec<GlobalId>,
    pub if_not_exists: bool,
    /// True if the view contains an expression that can make the exact column list
    /// ambiguous. For example `NATURAL JOIN` or `SELECT *`.
    pub ambiguous_columns: bool,
}

#[derive(Debug, Clone)]
pub struct CreateMaterializedViewPlan {
    pub name: QualifiedItemName,
    pub materialized_view: MaterializedView,
    /// The ID of the object that this view is replacing, if any.
    pub replace: Option<GlobalId>,
    /// The IDs of all objects that need to be dropped. This includes `replace` and any dependents.
    pub drop_ids: Vec<GlobalId>,
    pub if_not_exists: bool,
    /// True if the materialized view contains an expression that can make the exact column list
    /// ambiguous. For example `NATURAL JOIN` or `SELECT *`.
    pub ambiguous_columns: bool,
}

#[derive(Debug, Clone)]
pub struct CreateIndexPlan {
    pub name: QualifiedItemName,
    pub index: Index,
    pub if_not_exists: bool,
}

#[derive(Debug)]
pub struct CreateTypePlan {
    pub name: QualifiedItemName,
    pub typ: Type,
}

#[derive(Debug)]
pub struct DropObjectsPlan {
    /// The IDs of only the objects directly referenced in the `DROP` statement.
    pub referenced_ids: Vec<ObjectId>,
    /// All object IDs to drop. Includes `referenced_ids` and all descendants.
    pub drop_ids: Vec<ObjectId>,
    /// The type of object that was dropped explicitly in the DROP statement. `ids` may contain
    /// objects of different types due to CASCADE.
    pub object_type: ObjectType,
}

#[derive(Debug)]
pub struct DropOwnedPlan {
    /// The role IDs that own the objects.
    pub role_ids: Vec<RoleId>,
    /// All object IDs to drop.
    pub drop_ids: Vec<ObjectId>,
    /// The privileges to revoke.
    pub privilege_revokes: Vec<(SystemObjectId, MzAclItem)>,
    /// The default privileges to revoke.
    pub default_privilege_revokes: Vec<(DefaultPrivilegeObject, DefaultPrivilegeAclItem)>,
}

#[derive(Debug)]
pub struct ShowVariablePlan {
    pub name: String,
}

#[derive(Debug)]
pub struct InspectShardPlan {
    pub id: GlobalId,
}

#[derive(Debug)]
pub struct SetVariablePlan {
    pub name: String,
    pub value: VariableValue,
    pub local: bool,
}

#[derive(Debug)]
pub enum VariableValue {
    Default,
    Values(Vec<String>),
}

#[derive(Debug)]
pub struct ResetVariablePlan {
    pub name: String,
}

#[derive(Debug)]
pub struct SetTransactionPlan {
    pub local: bool,
    pub modes: Vec<TransactionMode>,
}

#[derive(Clone, Debug)]
pub struct SelectPlan {
    pub source: HirRelationExpr,
    pub when: QueryWhen,
    pub finishing: RowSetFinishing,
    pub copy_to: Option<CopyFormat>,
}

#[derive(Debug)]
pub enum SubscribeOutput {
    Diffs,
    WithinTimestampOrderBy {
        /// We pretend that mz_diff is prepended to the normal columns, making it index 0
        order_by: Vec<ColumnOrder>,
    },
    EnvelopeUpsert {
        /// Order by with just keys
        order_by_keys: Vec<ColumnOrder>,
    },
    EnvelopeDebezium {
        /// Order by with just keys
        order_by_keys: Vec<ColumnOrder>,
    },
}

#[derive(Debug)]
pub struct SubscribePlan {
    pub from: SubscribeFrom,
    pub with_snapshot: bool,
    pub when: QueryWhen,
    pub up_to: Option<MirScalarExpr>,
    pub copy_to: Option<CopyFormat>,
    pub emit_progress: bool,
    pub output: SubscribeOutput,
}

#[derive(Debug, Clone)]
pub enum SubscribeFrom {
    Id(GlobalId),
    Query {
        expr: MirRelationExpr,
        desc: RelationDesc,
    },
}

impl SubscribeFrom {
    pub fn depends_on(&self) -> BTreeSet<GlobalId> {
        match self {
            SubscribeFrom::Id(id) => BTreeSet::from([*id]),
            SubscribeFrom::Query { expr, .. } => expr.depends_on(),
        }
    }

    pub fn contains_temporal(&self) -> bool {
        match self {
            SubscribeFrom::Id(_) => false,
            SubscribeFrom::Query { expr, .. } => expr.contains_temporal(),
        }
    }
}

#[derive(Debug)]
pub struct ShowCreatePlan {
    pub id: GlobalId,
    pub row: Row,
}

#[derive(Debug)]
pub struct ShowColumnsPlan {
    pub id: GlobalId,
    pub select_plan: SelectPlan,
    pub new_resolved_ids: ResolvedIds,
}

#[derive(Debug)]
pub struct CopyFromPlan {
    pub id: GlobalId,
    pub columns: Vec<usize>,
    pub params: CopyFormatParams<'static>,
}

#[derive(Debug, Clone)]
pub struct CopyToPlan {
    /// The select query plan whose data will be copied to destination uri.
    pub select_plan: SelectPlan,
    pub desc: RelationDesc,
    /// The scalar expression to be resolved to get the destination uri.
    pub to: HirScalarExpr,
    pub connection: mz_storage_types::connections::Connection<ReferencedConnection>,
    /// The ID of the connection.
    pub connection_id: GlobalId,
    pub format_params: CopyFormatParams<'static>,
    pub max_file_size: u64,
}

#[derive(Clone, Debug)]
pub struct ExplainPlanPlan {
    pub stage: ExplainStage,
    pub format: ExplainFormat,
    pub config: ExplainConfig,
    pub explainee: Explainee,
}

/// The type of object to be explained
#[derive(Clone, Debug)]
pub enum Explainee {
    /// Lookup and explain a plan saved for an view.
    View(GlobalId),
    /// Lookup and explain a plan saved for an existing materialized view.
    MaterializedView(GlobalId),
    /// Lookup and explain a plan saved for an existing index.
    Index(GlobalId),
    /// Replan an existing view.
    ReplanView(GlobalId),
    /// Replan an existing materialized view.
    ReplanMaterializedView(GlobalId),
    /// Replan an existing index.
    ReplanIndex(GlobalId),
    /// A SQL statement.
    Statement(ExplaineeStatement),
}

/// Explainee types that are statements.
#[derive(Clone, Debug, EnumKind)]
#[enum_kind(ExplaineeStatementKind)]
pub enum ExplaineeStatement {
    /// The object to be explained is a SELECT statement.
    Select {
        /// Broken flag (see [`ExplaineeStatement::broken()`]).
        broken: bool,
        plan: plan::SelectPlan,
        desc: RelationDesc,
    },
    /// The object to be explained is a CREATE VIEW.
    CreateView {
        /// Broken flag (see [`ExplaineeStatement::broken()`]).
        broken: bool,
        plan: plan::CreateViewPlan,
    },
    /// The object to be explained is a CREATE MATERIALIZED VIEW.
    CreateMaterializedView {
        /// Broken flag (see [`ExplaineeStatement::broken()`]).
        broken: bool,
        plan: plan::CreateMaterializedViewPlan,
    },
    /// The object to be explained is a CREATE INDEX.
    CreateIndex {
        /// Broken flag (see [`ExplaineeStatement::broken()`]).
        broken: bool,
        plan: plan::CreateIndexPlan,
    },
}

impl ExplaineeStatement {
    pub fn depends_on(&self) -> BTreeSet<GlobalId> {
        match self {
            Self::Select { plan, .. } => plan.source.depends_on(),
            Self::CreateView { plan, .. } => plan.view.expr.depends_on(),
            Self::CreateMaterializedView { plan, .. } => plan.materialized_view.expr.depends_on(),
            Self::CreateIndex { plan, .. } => btreeset! {plan.index.on},
        }
    }

    /// Statements that have their `broken` flag set are expected to cause a
    /// panic in the optimizer code. In this case:
    ///
    /// 1. The optimizer pipeline execution will stop, but the panic will be
    ///    intercepted and will not propagate to the caller. The partial
    ///    optimizer trace collected until this point will be available.
    /// 2. The optimizer trace tracing subscriber will delegate regular tracing
    ///    spans and events to the default subscriber.
    ///
    /// This is useful when debugging queries that cause panics.
    pub fn broken(&self) -> bool {
        match self {
            Self::Select { broken, .. } => *broken,
            Self::CreateView { broken, .. } => *broken,
            Self::CreateMaterializedView { broken, .. } => *broken,
            Self::CreateIndex { broken, .. } => *broken,
        }
    }
}

impl ExplaineeStatementKind {
    pub fn supports(&self, stage: &ExplainStage) -> bool {
        use ExplainStage::*;
        match self {
            Self::Select => true,
            Self::CreateView => ![GlobalPlan, PhysicalPlan].contains(stage),
            Self::CreateMaterializedView => true,
            Self::CreateIndex => ![RawPlan, DecorrelatedPlan, LocalPlan].contains(stage),
        }
    }
}

impl std::fmt::Display for ExplaineeStatementKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Select => write!(f, "SELECT"),
            Self::CreateView => write!(f, "CREATE VIEW"),
            Self::CreateMaterializedView => write!(f, "CREATE MATERIALIZED VIEW"),
            Self::CreateIndex => write!(f, "CREATE INDEX"),
        }
    }
}

#[derive(Clone, Debug)]
pub struct ExplainPushdownPlan {
    pub explainee: Explainee,
}

#[derive(Clone, Debug)]
pub struct ExplainTimestampPlan {
    pub format: ExplainFormat,
    pub raw_plan: HirRelationExpr,
    pub when: QueryWhen,
}

#[derive(Debug)]
pub struct ExplainSinkSchemaPlan {
    pub sink_from: GlobalId,
    pub json_schema: String,
}

#[derive(Debug)]
pub struct SendDiffsPlan {
    pub id: GlobalId,
    pub updates: Vec<(Row, Diff)>,
    pub kind: MutationKind,
    pub returning: Vec<(Row, NonZeroUsize)>,
    pub max_result_size: u64,
}

#[derive(Debug)]
pub struct InsertPlan {
    pub id: GlobalId,
    pub values: HirRelationExpr,
    pub returning: Vec<mz_expr::MirScalarExpr>,
}

#[derive(Debug)]
pub struct ReadThenWritePlan {
    pub id: GlobalId,
    pub selection: HirRelationExpr,
    pub finishing: RowSetFinishing,
    pub assignments: BTreeMap<usize, mz_expr::MirScalarExpr>,
    pub kind: MutationKind,
    pub returning: Vec<mz_expr::MirScalarExpr>,
}

/// Generated by `ALTER ... IF EXISTS` if the named object did not exist.
#[derive(Debug)]
pub struct AlterNoopPlan {
    pub object_type: ObjectType,
}

#[derive(Debug)]
pub struct AlterSetClusterPlan {
    pub id: GlobalId,
    pub set_cluster: ClusterId,
}

#[derive(Debug)]
pub struct AlterRetainHistoryPlan {
    pub id: GlobalId,
    pub value: Option<Value>,
    pub window: CompactionWindow,
    pub object_type: ObjectType,
}

#[derive(Debug, Clone)]

pub enum AlterOptionParameter<T = String> {
    Set(T),
    Reset,
    Unchanged,
}

#[derive(Debug)]
pub enum AlterConnectionAction {
    RotateKeys,
    AlterOptions {
        set_options: BTreeMap<ConnectionOptionName, Option<WithOptionValue<Aug>>>,
        drop_options: BTreeSet<ConnectionOptionName>,
        validate: bool,
    },
}

#[derive(Debug)]
pub struct AlterConnectionPlan {
    pub id: GlobalId,
    pub action: AlterConnectionAction,
}

#[derive(Debug)]
pub enum AlterSourceAction {
    DropSubsourceExports {
        to_drop: BTreeSet<GlobalId>,
    },
    AddSubsourceExports {
        subsources: Vec<CreateSourceSubsource<Aug>>,
        details: Option<WithOptionValue<Aug>>,
        options: Vec<AlterSourceAddSubsourceOption<Aug>>,
    },
}

#[derive(Debug)]
pub struct AlterSourcePlan {
    pub id: GlobalId,
    pub action: AlterSourceAction,
}

#[derive(Debug)]
pub struct AlterClusterPlan {
    pub id: ClusterId,
    pub name: String,
    pub options: PlanClusterOption,
}

#[derive(Debug)]
pub struct AlterClusterRenamePlan {
    pub id: ClusterId,
    pub name: String,
    pub to_name: String,
}

#[derive(Debug)]
pub struct AlterClusterReplicaRenamePlan {
    pub cluster_id: ClusterId,
    pub replica_id: ReplicaId,
    pub name: QualifiedReplica,
    pub to_name: String,
}

#[derive(Debug)]
pub struct AlterItemRenamePlan {
    pub id: GlobalId,
    pub current_full_name: FullItemName,
    pub to_name: String,
    pub object_type: ObjectType,
}

#[derive(Debug)]
pub struct AlterSchemaRenamePlan {
    pub cur_schema_spec: (ResolvedDatabaseSpecifier, SchemaSpecifier),
    pub new_schema_name: String,
}

#[derive(Debug)]
pub struct AlterSchemaSwapPlan {
    pub schema_a_spec: (ResolvedDatabaseSpecifier, SchemaSpecifier),
    pub schema_a_name: String,
    pub schema_b_spec: (ResolvedDatabaseSpecifier, SchemaSpecifier),
    pub schema_b_name: String,
    pub name_temp: String,
}

#[derive(Debug)]
pub struct AlterClusterSwapPlan {
    pub id_a: ClusterId,
    pub id_b: ClusterId,
    pub name_a: String,
    pub name_b: String,
    pub name_temp: String,
}

#[derive(Debug)]
pub struct AlterItemSwapPlan {
    pub id_a: GlobalId,
    pub id_b: GlobalId,
    pub full_name_a: FullItemName,
    pub full_name_b: FullItemName,
    pub object_type: ObjectType,
}

#[derive(Debug)]
pub struct AlterSecretPlan {
    pub id: GlobalId,
    pub secret_as: MirScalarExpr,
}

#[derive(Debug)]
pub struct AlterSystemSetPlan {
    pub name: String,
    pub value: VariableValue,
}

#[derive(Debug)]
pub struct AlterSystemResetPlan {
    pub name: String,
}

#[derive(Debug)]
pub struct AlterSystemResetAllPlan {}

#[derive(Debug)]
pub struct AlterRolePlan {
    pub id: RoleId,
    pub name: String,
    pub option: PlannedAlterRoleOption,
}

#[derive(Debug)]
pub struct AlterOwnerPlan {
    pub id: ObjectId,
    pub object_type: ObjectType,
    pub new_owner: RoleId,
}

#[derive(Debug)]
pub struct DeclarePlan {
    pub name: String,
    pub stmt: Statement<Raw>,
    pub sql: String,
    pub params: Params,
}

#[derive(Debug)]
pub struct FetchPlan {
    pub name: String,
    pub count: Option<FetchDirection>,
    pub timeout: ExecuteTimeout,
}

#[derive(Debug)]
pub struct ClosePlan {
    pub name: String,
}

#[derive(Debug)]
pub struct PreparePlan {
    pub name: String,
    pub stmt: Statement<Raw>,
    pub sql: String,
    pub desc: StatementDesc,
}

#[derive(Debug)]
pub struct ExecutePlan {
    pub name: String,
    pub params: Params,
}

#[derive(Debug)]
pub struct DeallocatePlan {
    pub name: Option<String>,
}

#[derive(Debug)]
pub struct RaisePlan {
    pub severity: NoticeSeverity,
}

#[derive(Debug)]
pub struct GrantRolePlan {
    /// The roles that are gaining members.
    pub role_ids: Vec<RoleId>,
    /// The roles that will be added to `role_id`.
    pub member_ids: Vec<RoleId>,
    /// The role that granted the membership.
    pub grantor_id: RoleId,
}

#[derive(Debug)]
pub struct RevokeRolePlan {
    /// The roles that are losing members.
    pub role_ids: Vec<RoleId>,
    /// The roles that will be removed from `role_id`.
    pub member_ids: Vec<RoleId>,
    /// The role that revoked the membership.
    pub grantor_id: RoleId,
}

#[derive(Debug)]
pub struct UpdatePrivilege {
    /// The privileges being granted/revoked on an object.
    pub acl_mode: AclMode,
    /// The ID of the object receiving privileges.
    pub target_id: SystemObjectId,
    /// The role that is granting the privileges.
    pub grantor: RoleId,
}

#[derive(Debug)]
pub struct GrantPrivilegesPlan {
    /// Description of each privilege being granted.
    pub update_privileges: Vec<UpdatePrivilege>,
    /// The roles that will granted the privileges.
    pub grantees: Vec<RoleId>,
}

#[derive(Debug)]
pub struct RevokePrivilegesPlan {
    /// Description of each privilege being revoked.
    pub update_privileges: Vec<UpdatePrivilege>,
    /// The roles that will have privileges revoked.
    pub revokees: Vec<RoleId>,
}
#[derive(Debug)]
pub struct AlterDefaultPrivilegesPlan {
    /// Description of objects that match this default privilege.
    pub privilege_objects: Vec<DefaultPrivilegeObject>,
    /// The privilege to be granted/revoked from the matching objects.
    pub privilege_acl_items: Vec<DefaultPrivilegeAclItem>,
    /// Whether this is a grant or revoke.
    pub is_grant: bool,
}

#[derive(Debug)]
pub struct ReassignOwnedPlan {
    /// The roles whose owned objects are being reassigned.
    pub old_roles: Vec<RoleId>,
    /// The new owner of the objects.
    pub new_role: RoleId,
    /// All object IDs to reassign.
    pub reassign_ids: Vec<ObjectId>,
}

#[derive(Debug)]
pub struct CommentPlan {
    /// The object that this comment is associated with.
    pub object_id: CommentObjectId,
    /// A sub-component of the object that this comment is associated with, e.g. a column.
    ///
    /// TODO(parkmycar): <https://github.com/MaterializeInc/materialize/issues/22246>.
    pub sub_component: Option<usize>,
    /// The comment itself. If `None` that indicates we should clear the existing comment.
    pub comment: Option<String>,
}

#[derive(Clone, Debug)]
pub struct Table {
    pub create_sql: String,
    pub desc: RelationDesc,
    pub defaults: Vec<Expr<Aug>>,
    pub temporary: bool,
    pub compaction_window: Option<CompactionWindow>,
}

#[derive(Clone, Debug)]
pub struct Source {
    pub create_sql: String,
    pub data_source: DataSourceDesc,
    pub desc: RelationDesc,
    pub compaction_window: Option<CompactionWindow>,
}

#[derive(Debug, Clone)]
pub enum DataSourceDesc {
    /// Receives data from an external system.
    Ingestion(Ingestion),
    SourceExport {
        ingestion_id: GlobalId,
        output_index: usize,
    },
    /// Receives data from some other source.
    Source,
    /// Receives data from the source's reclocking/remapping operations.
    Progress,
    /// Receives data from HTTP post requests.
    Webhook {
        validate_using: Option<WebhookValidation>,
        body_format: WebhookBodyFormat,
        headers: WebhookHeaders,
    },
}

#[derive(Clone, Debug)]
pub struct Ingestion {
    pub desc: SourceDesc<ReferencedConnection>,
    pub subsource_exports: BTreeMap<GlobalId, usize>,
    pub progress_subsource: GlobalId,
}

#[derive(Clone, Debug, Serialize)]
pub struct WebhookValidation {
    /// The expression used to validate a request.
    pub expression: MirScalarExpr,
    /// Description of the source that will be created.
    pub relation_desc: RelationDesc,
    /// The column index to provide the request body and whether to provide it as bytes.
    pub bodies: Vec<(usize, bool)>,
    /// The column index to provide the request headers and whether to provide the values as bytes.
    pub headers: Vec<(usize, bool)>,
    /// Any secrets that are used in that validation.
    pub secrets: Vec<WebhookValidationSecret>,
}

impl WebhookValidation {
    const MAX_REDUCE_TIME: Duration = Duration::from_secs(60);

    /// Attempt to reduce the internal [`MirScalarExpr`] into a simpler expression.
    ///
    /// The reduction happens on a separate thread, we also only wait for
    /// `WebhookValidation::MAX_REDUCE_TIME` before timing out and returning an error.
    pub async fn reduce_expression(&mut self) -> Result<(), &'static str> {
        let WebhookValidation {
            expression,
            relation_desc,
            ..
        } = self;

        // On a different thread, attempt to reduce the expression.
        let mut expression_ = expression.clone();
        let desc_ = relation_desc.clone();
        let reduce_task = mz_ore::task::spawn_blocking(
            || "webhook-validation-reduce",
            move || {
                expression_.reduce(&desc_.typ().column_types);
                expression_
            },
        );

        match tokio::time::timeout(Self::MAX_REDUCE_TIME, reduce_task).await {
            Ok(Ok(reduced_expr)) => {
                *expression = reduced_expr;
                Ok(())
            }
            Ok(Err(_)) => Err("joining task"),
            Err(_) => Err("timeout"),
        }
    }
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct WebhookHeaders {
    /// Optionally include a column named `headers` whose content is possibly filtered.
    pub header_column: Option<WebhookHeaderFilters>,
    /// The column index to provide the specific request header, and whether to provide it as bytes.
    pub mapped_headers: BTreeMap<usize, (String, bool)>,
}

impl WebhookHeaders {
    /// Returns the number of columns needed to represent our headers.
    pub fn num_columns(&self) -> usize {
        let header_column = self.header_column.as_ref().map(|_| 1).unwrap_or(0);
        let mapped_headers = self.mapped_headers.len();

        header_column + mapped_headers
    }
}

#[derive(Clone, Debug, Default, Serialize)]
pub struct WebhookHeaderFilters {
    pub block: BTreeSet<String>,
    pub allow: BTreeSet<String>,
}

#[derive(Copy, Clone, Debug, Serialize, Arbitrary)]
pub enum WebhookBodyFormat {
    Json { array: bool },
    Bytes,
    Text,
}

impl From<WebhookBodyFormat> for ScalarType {
    fn from(value: WebhookBodyFormat) -> Self {
        match value {
            WebhookBodyFormat::Json { .. } => ScalarType::Jsonb,
            WebhookBodyFormat::Bytes => ScalarType::Bytes,
            WebhookBodyFormat::Text => ScalarType::String,
        }
    }
}

#[derive(Clone, Debug, Serialize)]
pub struct WebhookValidationSecret {
    /// Identifies the secret by [`GlobalId`].
    pub id: GlobalId,
    /// Column index for the expression context that this secret was originally evaluated in.
    pub column_idx: usize,
    /// Whether or not this secret should be provided to the expression as Bytes or a String.
    pub use_bytes: bool,
}

#[derive(Clone, Debug)]
pub struct Connection {
    pub create_sql: String,
    pub connection: mz_storage_types::connections::Connection<ReferencedConnection>,
}

#[derive(Clone, Debug)]
pub struct Secret {
    pub create_sql: String,
    pub secret_as: MirScalarExpr,
}

#[derive(Clone, Debug)]
pub struct Sink {
    pub create_sql: String,
    pub from: GlobalId,
    pub connection: StorageSinkConnection<ReferencedConnection>,
    // TODO(guswynn): this probably should just be in the `connection`.
    pub envelope: SinkEnvelope,
}

#[derive(Clone, Debug)]
pub struct View {
    pub create_sql: String,
    pub expr: HirRelationExpr,
    pub column_names: Vec<ColumnName>,
    pub temporary: bool,
}

#[derive(Clone, Debug)]
pub struct MaterializedView {
    pub create_sql: String,
    pub expr: HirRelationExpr,
    pub column_names: Vec<ColumnName>,
    pub cluster_id: ClusterId,
    pub non_null_assertions: Vec<usize>,
    pub compaction_window: Option<CompactionWindow>,
    pub refresh_schedule: Option<RefreshSchedule>,
    pub as_of: Option<Timestamp>,
}

#[derive(Clone, Debug)]
pub struct Index {
    pub create_sql: String,
    pub on: GlobalId,
    pub keys: Vec<mz_expr::MirScalarExpr>,
    pub compaction_window: Option<CompactionWindow>,
    pub cluster_id: ClusterId,
}

#[derive(Clone, Debug)]
pub struct Type {
    pub create_sql: String,
    pub inner: CatalogType<IdReference>,
}

/// Specifies when a `Peek` or `Subscribe` should occur.
#[derive(Deserialize, Clone, Debug, PartialEq)]
pub enum QueryWhen {
    /// The peek should occur at the latest possible timestamp that allows the
    /// peek to complete immediately.
    Immediately,
    /// The peek should occur at a timestamp that allows the peek to see all
    /// data written to tables within Materialize.
    FreshestTableWrite,
    /// The peek should occur at the timestamp described by the specified
    /// expression.
    ///
    /// The expression may have any type.
    AtTimestamp(MirScalarExpr),
    /// Same as Immediately, but will also advance to at least the specified
    /// expression.
    AtLeastTimestamp(MirScalarExpr),
}

impl QueryWhen {
    /// Returns a timestamp to which the candidate must be advanced.
    pub fn advance_to_timestamp(&self) -> Option<MirScalarExpr> {
        match self {
            QueryWhen::AtTimestamp(t) | QueryWhen::AtLeastTimestamp(t) => Some(t.clone()),
            QueryWhen::Immediately | QueryWhen::FreshestTableWrite => None,
        }
    }
    /// Returns whether the candidate must be advanced to the since.
    pub fn advance_to_since(&self) -> bool {
        match self {
            QueryWhen::Immediately
            | QueryWhen::AtLeastTimestamp(_)
            | QueryWhen::FreshestTableWrite => true,
            QueryWhen::AtTimestamp(_) => false,
        }
    }
    /// Returns whether the candidate can be advanced to the upper.
    pub fn can_advance_to_upper(&self) -> bool {
        match self {
            QueryWhen::Immediately => true,
            QueryWhen::FreshestTableWrite
            | QueryWhen::AtTimestamp(_)
            | QueryWhen::AtLeastTimestamp(_) => false,
        }
    }

    /// Returns whether the candidate can be advanced to the timeline's timestamp.
    pub fn can_advance_to_timeline_ts(&self) -> bool {
        match self {
            QueryWhen::Immediately | QueryWhen::FreshestTableWrite => true,
            QueryWhen::AtTimestamp(_) | QueryWhen::AtLeastTimestamp(_) => false,
        }
    }
    /// Returns whether the candidate must be advanced to the timeline's timestamp.
    pub fn must_advance_to_timeline_ts(&self) -> bool {
        match self {
            QueryWhen::FreshestTableWrite => true,
            QueryWhen::Immediately | QueryWhen::AtLeastTimestamp(_) | QueryWhen::AtTimestamp(_) => {
                false
            }
        }
    }
    /// Returns whether the selected timestamp should be tracked within the current transaction.
    pub fn is_transactional(&self) -> bool {
        match self {
            QueryWhen::Immediately | QueryWhen::FreshestTableWrite => true,
            QueryWhen::AtLeastTimestamp(_) | QueryWhen::AtTimestamp(_) => false,
        }
    }
}

#[derive(Debug, Copy, Clone)]
pub enum MutationKind {
    Insert,
    Update,
    Delete,
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum CopyFormat {
    Text,
    Csv,
    Binary,
}

#[derive(Debug, Copy, Clone)]
pub enum ExecuteTimeout {
    None,
    Seconds(f64),
    WaitOnce,
}

#[derive(Clone, Debug)]
pub enum IndexOption {
    /// Configures the logical compaction window for an index.
    RetainHistory(CompactionWindow),
}

#[derive(Clone, Debug)]
pub enum TableOption {
    /// Configures the logical compaction window for a table.
    RetainHistory(CompactionWindow),
}

#[derive(Clone, Debug)]
pub struct PlanClusterOption {
    pub availability_zones: AlterOptionParameter<Vec<String>>,
    pub introspection_debugging: AlterOptionParameter<bool>,
    pub introspection_interval: AlterOptionParameter<OptionalDuration>,
    pub managed: AlterOptionParameter<bool>,
    pub replicas: AlterOptionParameter<Vec<(String, ReplicaConfig)>>,
    pub replication_factor: AlterOptionParameter<u32>,
    pub size: AlterOptionParameter,
    pub disk: AlterOptionParameter<bool>,
    pub schedule: AlterOptionParameter<ClusterScheduleOptionValue>,
}

impl Default for PlanClusterOption {
    fn default() -> Self {
        Self {
            availability_zones: AlterOptionParameter::Unchanged,
            introspection_debugging: AlterOptionParameter::Unchanged,
            introspection_interval: AlterOptionParameter::Unchanged,
            managed: AlterOptionParameter::Unchanged,
            replicas: AlterOptionParameter::Unchanged,
            replication_factor: AlterOptionParameter::Unchanged,
            size: AlterOptionParameter::Unchanged,
            disk: AlterOptionParameter::Unchanged,
            schedule: AlterOptionParameter::Unchanged,
        }
    }
}

/// A vector of values to which parameter references should be bound.
#[derive(Debug, Clone)]
pub struct Params {
    pub datums: Row,
    pub types: Vec<ScalarType>,
}

impl Params {
    /// Returns a `Params` with no parameters.
    pub fn empty() -> Params {
        Params {
            datums: Row::pack_slice(&[]),
            types: vec![],
        }
    }
}

/// Controls planning of a SQL query.
#[derive(Ord, PartialOrd, Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash, Copy)]
pub struct PlanContext {
    pub wall_time: DateTime<Utc>,
    pub planning_id: Option<GlobalId>,
    pub ignore_if_exists_errors: bool,
}

impl PlanContext {
    pub fn new(wall_time: DateTime<Utc>) -> Self {
        Self {
            wall_time,
            planning_id: None,
            ignore_if_exists_errors: false,
        }
    }

    /// Return a PlanContext with zero values. This should only be used when
    /// planning is required but unused (like in `plan_create_table()`) or in
    /// tests.
    pub fn zero() -> Self {
        PlanContext {
            wall_time: now::to_datetime(NOW_ZERO()),
            planning_id: None,
            ignore_if_exists_errors: false,
        }
    }

    pub fn with_planning_id(mut self, id: GlobalId) -> Self {
        self.planning_id = Some(id);
        self
    }

    pub fn with_ignore_if_exists_errors(mut self, value: bool) -> Self {
        self.ignore_if_exists_errors = value;
        self
    }
}

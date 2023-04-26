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

// `EnumKind` unconditionally introduces a lifetime. TODO: remove this once
// https://github.com/rust-lang/rust-clippy/pull/9037 makes it into stable
#![allow(clippy::extra_unused_lifetimes)]

use std::collections::{BTreeMap, BTreeSet};
use std::num::NonZeroUsize;
use std::time::Duration;

use chrono::{DateTime, Utc};
use enum_kinds::EnumKind;
use serde::{Deserialize, Serialize};

pub use error::PlanError;
pub use explain::normalize_subqueries;
use mz_controller::clusters::ClusterId;
use mz_expr::{MirRelationExpr, MirScalarExpr, RowSetFinishing};
use mz_ore::now::{self, NOW_ZERO};
use mz_pgcopy::CopyFormatParams;
use mz_repr::adt::mz_acl_item::AclMode;
use mz_repr::explain::{ExplainConfig, ExplainFormat};
use mz_repr::role_id::RoleId;
use mz_repr::{ColumnName, Diff, GlobalId, RelationDesc, Row, ScalarType};
use mz_sql_parser::ast::TransactionIsolationLevel;
use mz_storage_client::types::sinks::{SinkEnvelope, StorageSinkConnectionBuilder};
use mz_storage_client::types::sources::{SourceDesc, Timeline};
pub use optimize::OptimizerConfig;
pub use query::{QueryContext, QueryLifetime};
pub use statement::{describe, plan, plan_copy_from, StatementContext, StatementDesc};

use crate::ast::{
    ExplainStage, Expr, FetchDirection, IndexOptionName, NoticeSeverity, ObjectType, Raw,
    Statement, StatementKind, TransactionAccessMode,
};
use crate::catalog::{CatalogType, IdReference, RoleAttributes};
use crate::names::{Aug, FullItemName, ObjectId, QualifiedItemName, ResolvedDatabaseSpecifier};

pub use self::expr::{
    AggregateExpr, Hir, HirRelationExpr, HirScalarExpr, JoinKind, WindowExprType,
};

pub(crate) mod error;
pub(crate) mod explain;
pub(crate) mod expr;
pub(crate) mod lowering;
pub(crate) mod optimize;
pub(crate) mod plan_utils;
pub(crate) mod query;
pub(crate) mod scope;
pub(crate) mod statement;
pub(crate) mod transform_ast;
pub(crate) mod transform_expr;
pub(crate) mod typeconv;
pub(crate) mod with_options;

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
    DiscardTemp,
    DiscardAll,
    DropObjects(DropObjectsPlan),
    EmptyQuery,
    ShowAllVariables,
    ShowCreate(ShowCreatePlan),
    ShowVariable(ShowVariablePlan),
    SetVariable(SetVariablePlan),
    ResetVariable(ResetVariablePlan),
    StartTransaction(StartTransactionPlan),
    CommitTransaction(CommitTransactionPlan),
    AbortTransaction(AbortTransactionPlan),
    Peek(PeekPlan),
    Subscribe(SubscribePlan),
    CopyFrom(CopyFromPlan),
    CopyRows(CopyRowsPlan),
    Explain(ExplainPlan),
    Insert(InsertPlan),
    AlterNoop(AlterNoopPlan),
    AlterIndexSetOptions(AlterIndexSetOptionsPlan),
    AlterIndexResetOptions(AlterIndexResetOptionsPlan),
    AlterSink(AlterSinkPlan),
    AlterSource(AlterSourcePlan),
    AlterItemRename(AlterItemRenamePlan),
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
    RotateKeys(RotateKeysPlan),
    GrantRole(GrantRolePlan),
    RevokeRole(RevokeRolePlan),
    GrantPrivilege(GrantPrivilegePlan),
    RevokePrivilege(RevokePrivilegePlan),
}

impl Plan {
    /// Expresses which [`StatementKind`] can generate which set of
    /// [`PlanKind`].
    pub fn generated_from(stmt: StatementKind) -> Vec<PlanKind> {
        match stmt {
            StatementKind::AlterConnection => vec![PlanKind::AlterNoop, PlanKind::RotateKeys],
            StatementKind::AlterIndex => vec![
                PlanKind::AlterIndexResetOptions,
                PlanKind::AlterIndexSetOptions,
                PlanKind::AlterNoop,
            ],
            StatementKind::AlterObjectRename => {
                vec![PlanKind::AlterItemRename, PlanKind::AlterNoop]
            }
            StatementKind::AlterRole => vec![PlanKind::AlterRole],
            StatementKind::AlterSecret => vec![PlanKind::AlterNoop, PlanKind::AlterSecret],
            StatementKind::AlterSink => vec![PlanKind::AlterNoop, PlanKind::AlterSink],
            StatementKind::AlterSource => vec![PlanKind::AlterNoop, PlanKind::AlterSource],
            StatementKind::AlterSystemReset => {
                vec![PlanKind::AlterNoop, PlanKind::AlterSystemReset]
            }
            StatementKind::AlterSystemResetAll => {
                vec![PlanKind::AlterNoop, PlanKind::AlterSystemResetAll]
            }
            StatementKind::AlterSystemSet => vec![PlanKind::AlterNoop, PlanKind::AlterSystemSet],
            StatementKind::AlterOwner => vec![PlanKind::AlterNoop, PlanKind::AlterOwner],
            StatementKind::Close => vec![PlanKind::Close],
            StatementKind::Commit => vec![PlanKind::CommitTransaction],
            StatementKind::Copy => vec![PlanKind::CopyFrom, PlanKind::Peek, PlanKind::Subscribe],
            StatementKind::CreateCluster => vec![PlanKind::CreateCluster],
            StatementKind::CreateClusterReplica => vec![PlanKind::CreateClusterReplica],
            StatementKind::CreateConnection => vec![PlanKind::CreateConnection],
            StatementKind::CreateDatabase => vec![PlanKind::CreateDatabase],
            StatementKind::CreateIndex => vec![PlanKind::CreateIndex],
            StatementKind::CreateMaterializedView => vec![PlanKind::CreateMaterializedView],
            StatementKind::CreateRole => vec![PlanKind::CreateRole],
            StatementKind::CreateSchema => vec![PlanKind::CreateSchema],
            StatementKind::CreateSecret => vec![PlanKind::CreateSecret],
            StatementKind::CreateSink => vec![PlanKind::CreateSink],
            StatementKind::CreateSource | StatementKind::CreateSubsource => {
                vec![PlanKind::CreateSource]
            }
            StatementKind::CreateTable => vec![PlanKind::CreateTable],
            StatementKind::CreateType => vec![PlanKind::CreateType],
            StatementKind::CreateView => vec![PlanKind::CreateView],
            StatementKind::Deallocate => vec![PlanKind::Deallocate],
            StatementKind::Declare => vec![PlanKind::Declare],
            StatementKind::Delete => vec![PlanKind::ReadThenWrite],
            StatementKind::Discard => vec![PlanKind::DiscardAll, PlanKind::DiscardTemp],
            StatementKind::DropObjects => vec![PlanKind::DropObjects],
            StatementKind::Execute => vec![PlanKind::Execute],
            StatementKind::Explain => vec![PlanKind::Explain],
            StatementKind::Fetch => vec![PlanKind::Fetch],
            StatementKind::GrantPrivilege => vec![PlanKind::GrantPrivilege],
            StatementKind::GrantRole => vec![PlanKind::GrantRole],
            StatementKind::Insert => vec![PlanKind::Insert],
            StatementKind::Prepare => vec![PlanKind::Prepare],
            StatementKind::Raise => vec![PlanKind::Raise],
            StatementKind::ResetVariable => vec![PlanKind::ResetVariable],
            StatementKind::RevokePrivilege => vec![PlanKind::RevokePrivilege],
            StatementKind::RevokeRole => vec![PlanKind::RevokeRole],
            StatementKind::Rollback => vec![PlanKind::AbortTransaction],
            StatementKind::Select => vec![PlanKind::Peek],
            StatementKind::SetTransaction => vec![],
            StatementKind::SetVariable => vec![PlanKind::SetVariable],
            StatementKind::Show => vec![
                PlanKind::Peek,
                PlanKind::ShowVariable,
                PlanKind::ShowCreate,
                PlanKind::ShowAllVariables,
            ],
            StatementKind::StartTransaction => vec![PlanKind::StartTransaction],
            StatementKind::Subscribe => vec![PlanKind::Subscribe],
            StatementKind::Update => vec![PlanKind::ReadThenWrite],
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
            Plan::EmptyQuery => "do nothing",
            Plan::ShowAllVariables => "show all variables",
            Plan::ShowCreate(_) => "show create",
            Plan::ShowVariable(_) => "show variable",
            Plan::SetVariable(_) => "set variable",
            Plan::ResetVariable(_) => "reset variable",
            Plan::StartTransaction(_) => "start transaction",
            Plan::CommitTransaction(_) => "commit",
            Plan::AbortTransaction(_) => "abort",
            Plan::Peek(_) => "select",
            Plan::Subscribe(_) => "subscribe",
            Plan::CopyRows(_) => "copy rows",
            Plan::CopyFrom(_) => "copy from",
            Plan::Explain(_) => "explain",
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
            Plan::AlterIndexSetOptions(_) => "alter index",
            Plan::AlterIndexResetOptions(_) => "alter index",
            Plan::AlterSink(_) => "alter sink",
            Plan::AlterSource(_) => "alter source",
            Plan::AlterItemRename(_) => "rename item",
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
            Plan::RotateKeys(_) => "rotate keys",
            Plan::GrantRole(_) => "grant role",
            Plan::RevokeRole(_) => "revoke role",
            Plan::GrantPrivilege(_) => "grant privilege",
            Plan::RevokePrivilege(_) => "revoke privilege",
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
    pub replicas: Vec<(String, ReplicaConfig)>,
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
    pub idle_arrangement_merge_effort: Option<u32>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ReplicaConfig {
    Unmanaged {
        storagectl_addrs: Vec<String>,
        storage_addrs: Vec<String>,
        computectl_addrs: Vec<String>,
        compute_addrs: Vec<String>,
        workers: usize,
        compute: ComputeReplicaConfig,
    },
    Managed {
        size: String,
        availability_zone: Option<String>,
        compute: ComputeReplicaConfig,
    },
}

#[derive(Debug)]
pub struct CreateSourcePlan {
    pub name: QualifiedItemName,
    pub source: Source,
    pub if_not_exists: bool,
    pub timeline: Timeline,
    pub cluster_config: SourceSinkClusterConfig,
}

#[derive(Debug)]
pub struct CreateSourcePlans {
    pub source_id: GlobalId,
    pub plan: CreateSourcePlan,
    pub depends_on: Vec<GlobalId>,
}

impl From<(GlobalId, CreateSourcePlan, Vec<GlobalId>)> for CreateSourcePlans {
    fn from(plan: (GlobalId, CreateSourcePlan, Vec<GlobalId>)) -> Self {
        CreateSourcePlans {
            source_id: plan.0,
            plan: plan.1,
            depends_on: plan.2,
        }
    }
}

/// Specifies the cluster for a source or a sink.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum SourceSinkClusterConfig {
    /// Use an existing cluster.
    Existing {
        /// The ID of the cluster to use.
        id: ClusterId,
    },
    /// Create a new linked storage cluster of the specified size.
    ///
    /// NOTE(benesch): in the future, we hope to remove the concept of a linked
    /// cluster, and always associate sources and sinks with an existing
    /// cluster.
    Linked {
        /// The size of the replica to create in the linked cluster.
        size: String,
    },
    /// The user did not specify a cluster behavior, so use the default.
    ///
    /// NOTE(benesch): we plan to remove this variant in the future by having
    /// the planner bind a source or sink with no `SIZE` or `IN CLUSTER` option
    /// to the active cluster. This behavior won't be ergonomic until we have
    /// multipurpose clusters though.
    Undefined,
}

impl SourceSinkClusterConfig {
    /// Returns the ID of the cluster that this source/sink will be created on, if one exists. If
    /// one doesn't exist, then a new cluster will be created.
    pub fn cluster_id(&self) -> Option<&ClusterId> {
        match self {
            SourceSinkClusterConfig::Existing { id } => Some(id),
            SourceSinkClusterConfig::Linked { .. } | SourceSinkClusterConfig::Undefined => None,
        }
    }
}

#[derive(Debug)]
pub struct CreateConnectionPlan {
    pub name: QualifiedItemName,
    pub if_not_exists: bool,
    pub connection: Connection,
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
    pub cluster_config: SourceSinkClusterConfig,
}

#[derive(Debug)]
pub struct CreateTablePlan {
    pub name: QualifiedItemName,
    pub table: Table,
    pub if_not_exists: bool,
}

#[derive(Debug)]
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

#[derive(Debug)]
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

#[derive(Debug)]
pub struct CreateIndexPlan {
    pub name: QualifiedItemName,
    pub index: Index,
    pub options: Vec<IndexOption>,
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
pub struct ShowVariablePlan {
    pub name: String,
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
pub struct PeekPlan {
    pub source: MirRelationExpr,
    pub when: QueryWhen,
    pub finishing: RowSetFinishing,
    pub copy_to: Option<CopyFormat>,
}

#[derive(Debug)]
pub struct SubscribePlan {
    pub from: SubscribeFrom,
    pub with_snapshot: bool,
    pub when: QueryWhen,
    pub up_to: Option<MirScalarExpr>,
    pub copy_to: Option<CopyFormat>,
    pub emit_progress: bool,
}

#[derive(Debug)]
pub enum SubscribeFrom {
    Id(GlobalId),
    Query {
        expr: MirRelationExpr,
        desc: RelationDesc,
    },
}

#[derive(Debug)]
pub struct ShowCreatePlan {
    pub id: GlobalId,
    pub row: Row,
}

#[derive(Debug)]
pub struct CopyFromPlan {
    pub id: GlobalId,
    pub columns: Vec<usize>,
    pub params: CopyFormatParams<'static>,
}

#[derive(Debug)]
pub struct CopyRowsPlan {
    pub id: GlobalId,
    pub columns: Vec<usize>,
    pub rows: Vec<Row>,
}

#[derive(Debug)]
pub struct ExplainPlan {
    pub raw_plan: HirRelationExpr,
    pub row_set_finishing: Option<RowSetFinishing>,
    pub stage: ExplainStage,
    pub format: ExplainFormat,
    pub config: ExplainConfig,
    pub no_errors: bool,
    pub explainee: mz_repr::explain::Explainee,
}

#[derive(Debug)]
pub struct SendDiffsPlan {
    pub id: GlobalId,
    pub updates: Vec<(Row, Diff)>,
    pub kind: MutationKind,
    pub returning: Vec<(Row, NonZeroUsize)>,
    pub max_result_size: u32,
}

#[derive(Debug)]
pub struct InsertPlan {
    pub id: GlobalId,
    pub values: mz_expr::MirRelationExpr,
    pub returning: Vec<mz_expr::MirScalarExpr>,
}

#[derive(Debug)]
pub struct ReadThenWritePlan {
    pub id: GlobalId,
    pub selection: mz_expr::MirRelationExpr,
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
pub struct AlterIndexSetOptionsPlan {
    pub id: GlobalId,
    pub options: Vec<IndexOption>,
}

#[derive(Debug)]
pub struct AlterIndexResetOptionsPlan {
    pub id: GlobalId,
    pub options: BTreeSet<IndexOptionName>,
}

#[derive(Debug, Clone)]

pub enum AlterOptionParameter {
    Set(String),
    Reset,
    Unchanged,
}

#[derive(Debug)]
pub struct AlterSinkPlan {
    pub id: GlobalId,
    pub size: AlterOptionParameter,
}

#[derive(Debug)]
pub struct AlterSourcePlan {
    pub id: GlobalId,
    pub size: AlterOptionParameter,
}

#[derive(Debug)]
pub struct AlterItemRenamePlan {
    pub id: GlobalId,
    pub current_full_name: FullItemName,
    pub to_name: String,
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
    pub attributes: RoleAttributes,
}

#[derive(Debug)]
pub struct AlterOwnerPlan {
    pub id: ObjectId,
    pub object_type: ObjectType,
    pub new_owner: RoleId,
}

#[derive(Debug)]
pub struct RotateKeysPlan {
    pub id: GlobalId,
}

#[derive(Debug)]
pub struct DeclarePlan {
    pub name: String,
    pub stmt: Statement<Raw>,
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
    /// The role that is gaining a member.
    pub role_id: RoleId,
    /// The roles that will be added to `role_id`.
    pub member_ids: Vec<RoleId>,
    /// The role that granted the membership.
    pub grantor_id: RoleId,
}

#[derive(Debug)]
pub struct RevokeRolePlan {
    /// The role that is losing a member.
    pub role_id: RoleId,
    /// The roles that will be removed from `role_id`.
    pub member_ids: Vec<RoleId>,
    /// The role that revoked the membership.
    pub grantor_id: RoleId,
}

#[derive(Debug)]
pub struct GrantPrivilegePlan {
    /// /// The privileges being granted on an object.
    pub acl_mode: AclMode,
    /// The ID of the object.
    pub object_id: ObjectId,
    /// The role that will granted the privileges.
    pub grantee: RoleId,
    /// The role that is granting the privileges.
    pub grantor: RoleId,
}

#[derive(Debug)]
pub struct RevokePrivilegePlan {
    /// The privileges being revoked.
    pub acl_mode: AclMode,
    /// The ID of the object.
    pub object_id: ObjectId,
    /// The role that will have privileges revoked.
    pub revokee: RoleId,
    /// The role that will revoke the privileges.
    pub grantor: RoleId,
}

#[derive(Clone, Debug)]
pub struct Table {
    pub create_sql: String,
    pub desc: RelationDesc,
    pub defaults: Vec<Expr<Aug>>,
    pub temporary: bool,
}

#[derive(Clone, Debug)]
pub struct Source {
    pub create_sql: String,
    pub data_source: DataSourceDesc,
    pub desc: RelationDesc,
}

#[derive(Debug, Clone)]
pub enum DataSourceDesc {
    /// Receives data from an external system
    Ingestion(Ingestion),
    /// Receives data from some other source
    Source,
    /// Receives data from the source's reclocking/remapping operations.
    Progress,
}

#[derive(Clone, Debug)]
pub struct Ingestion {
    pub desc: SourceDesc,
    pub source_imports: BTreeSet<GlobalId>,
    pub subsource_exports: BTreeMap<GlobalId, usize>,
    // MIGRATION: v0.44 This can be converted to a `GlobalId` in v0.46
    pub progress_subsource: Option<GlobalId>,
}

#[derive(Clone, Debug)]
pub struct Connection {
    pub create_sql: String,
    pub connection: mz_storage_client::types::connections::Connection,
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
    pub connection_builder: StorageSinkConnectionBuilder,
    pub envelope: SinkEnvelope,
}

#[derive(Clone, Debug)]
pub struct View {
    pub create_sql: String,
    pub expr: mz_expr::MirRelationExpr,
    pub column_names: Vec<ColumnName>,
    pub temporary: bool,
}

#[derive(Clone, Debug)]
pub struct MaterializedView {
    pub create_sql: String,
    pub expr: mz_expr::MirRelationExpr,
    pub column_names: Vec<ColumnName>,
    pub cluster_id: ClusterId,
}

#[derive(Clone, Debug)]
pub struct Index {
    pub create_sql: String,
    pub on: GlobalId,
    pub keys: Vec<mz_expr::MirScalarExpr>,
    pub cluster_id: ClusterId,
}

#[derive(Clone, Debug)]
pub struct Type {
    pub create_sql: String,
    pub inner: CatalogType<IdReference>,
}

/// Specifies when a `Peek` or `Subscribe` should occur.
#[derive(Debug, PartialEq)]
pub enum QueryWhen {
    /// The peek should occur at the latest possible timestamp that allows the
    /// peek to complete immediately.
    Immediately,
    /// The peek should occur at a timestamp that allows the peek to see all
    /// data written within Materialize.
    Freshest,
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
            QueryWhen::Immediately | QueryWhen::Freshest => None,
        }
    }
    /// Returns whether the candidate must be advanced to the since.
    pub fn advance_to_since(&self) -> bool {
        match self {
            QueryWhen::Immediately | QueryWhen::AtLeastTimestamp(_) | QueryWhen::Freshest => true,
            QueryWhen::AtTimestamp(_) => false,
        }
    }
    /// Returns whether the candidate can be advanced to the upper.
    pub fn can_advance_to_upper(&self) -> bool {
        match self {
            QueryWhen::Immediately | QueryWhen::AtLeastTimestamp(_) | QueryWhen::Freshest => true,
            QueryWhen::AtTimestamp(_) => false,
        }
    }
    /// Returns whether the candidate must be advanced to the upper.
    pub fn must_advance_to_upper(&self) -> bool {
        match self {
            QueryWhen::Freshest => true,
            QueryWhen::Immediately | QueryWhen::AtLeastTimestamp(_) | QueryWhen::AtTimestamp(_) => {
                false
            }
        }
    }
    /// Returns whether the candidate can be advanced to the timeline's timestamp.
    pub fn can_advance_to_timeline_ts(&self) -> bool {
        match self {
            QueryWhen::Immediately | QueryWhen::AtLeastTimestamp(_) | QueryWhen::Freshest => true,
            QueryWhen::AtTimestamp(_) => false,
        }
    }
    /// Returns whether the candidate must be advanced to the timeline's timestamp.
    pub fn must_advance_to_timeline_ts(&self) -> bool {
        match self {
            QueryWhen::Freshest => true,
            QueryWhen::Immediately | QueryWhen::AtLeastTimestamp(_) | QueryWhen::AtTimestamp(_) => {
                false
            }
        }
    }
}

#[derive(Debug)]
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
    /// Configures the logical compaction window for an index. `None` disables
    /// logical compaction entirely.
    LogicalCompactionWindow(Option<Duration>),
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
}

impl PlanContext {
    pub fn new(wall_time: DateTime<Utc>) -> Self {
        Self { wall_time }
    }

    /// Return a PlanContext with zero values. This should only be used when
    /// planning is required but unused (like in `plan_create_table()`) or in
    /// tests.
    pub fn zero() -> Self {
        PlanContext {
            wall_time: now::to_datetime(NOW_ZERO()),
        }
    }
}

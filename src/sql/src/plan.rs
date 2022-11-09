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

use std::collections::{BTreeSet, HashMap, HashSet};
use std::num::NonZeroUsize;
use std::time::Duration;

use chrono::{DateTime, Utc};
use enum_kinds::EnumKind;
use serde::{Deserialize, Serialize};

use mz_compute_client::controller::ComputeInstanceId;
use mz_expr::{MirRelationExpr, MirScalarExpr, RowSetFinishing};
use mz_ore::now::{self, NOW_ZERO};
use mz_pgcopy::CopyFormatParams;
use mz_repr::explain_new::{ExplainConfig, ExplainFormat};
use mz_repr::{ColumnName, Diff, GlobalId, RelationDesc, Row, ScalarType};
use mz_storage_client::types::sinks::{SinkEnvelope, StorageSinkConnectionBuilder};
use mz_storage_client::types::sources::{SourceDesc, Timeline};

use crate::ast::{
    ExplainStage, Expr, FetchDirection, IndexOptionName, NoticeSeverity, ObjectType, Raw,
    SetVariableValue, Statement, StatementKind, TransactionAccessMode,
};
use crate::catalog::{CatalogType, IdReference};
use crate::names::{
    Aug, DatabaseId, FullObjectName, QualifiedObjectName, ResolvedDatabaseSpecifier, SchemaId,
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

pub use self::expr::{
    AggregateExpr, Hir, HirRelationExpr, HirScalarExpr, JoinKind, WindowExprType,
};
pub use error::PlanError;
pub use explain::Explanation;
use mz_sql_parser::ast::TransactionIsolationLevel;
pub use optimize::OptimizerConfig;
pub use query::{QueryContext, QueryLifetime};
pub use statement::{describe, plan, plan_copy_from, StatementContext, StatementDesc};

/// Instructions for executing a SQL query.
#[derive(Debug, EnumKind)]
#[enum_kind(PlanKind)]
pub enum Plan {
    CreateConnection(CreateConnectionPlan),
    CreateDatabase(CreateDatabasePlan),
    CreateSchema(CreateSchemaPlan),
    CreateRole(CreateRolePlan),
    CreateComputeInstance(CreateComputeInstancePlan),
    CreateComputeReplica(CreateComputeReplicaPlan),
    CreateSource(CreateSourcePlan),
    CreateSecret(CreateSecretPlan),
    CreateSink(CreateSinkPlan),
    CreateTable(CreateTablePlan),
    CreateView(CreateViewPlan),
    CreateMaterializedView(CreateMaterializedViewPlan),
    CreateIndex(CreateIndexPlan),
    CreateType(CreateTypePlan),
    DiscardTemp,
    DiscardAll,
    DropDatabase(DropDatabasePlan),
    DropSchema(DropSchemaPlan),
    DropRoles(DropRolesPlan),
    DropComputeInstances(DropComputeInstancesPlan),
    DropComputeReplicas(DropComputeReplicasPlan),
    DropItems(DropItemsPlan),
    EmptyQuery,
    ShowAllVariables,
    ShowVariable(ShowVariablePlan),
    SetVariable(SetVariablePlan),
    ResetVariable(ResetVariablePlan),
    StartTransaction(StartTransactionPlan),
    CommitTransaction,
    AbortTransaction,
    Peek(PeekPlan),
    Subscribe(SubscribePlan),
    SendRows(SendRowsPlan),
    CopyFrom(CopyFromPlan),
    Explain(ExplainPlan),
    SendDiffs(SendDiffsPlan),
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
    Declare(DeclarePlan),
    Fetch(FetchPlan),
    Close(ClosePlan),
    ReadThenWrite(ReadThenWritePlan),
    Prepare(PreparePlan),
    Execute(ExecutePlan),
    Deallocate(DeallocatePlan),
    Raise(RaisePlan),
    RotateKeys(RotateKeysPlan),
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
            StatementKind::Close => vec![PlanKind::Close],
            StatementKind::Commit => vec![PlanKind::CommitTransaction],
            StatementKind::Copy => vec![
                PlanKind::CopyFrom,
                PlanKind::Peek,
                PlanKind::SendDiffs,
                PlanKind::Subscribe,
            ],
            StatementKind::CreateCluster => vec![PlanKind::CreateComputeInstance],
            StatementKind::CreateClusterReplica => vec![PlanKind::CreateComputeReplica],
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
            StatementKind::DropClusterReplicas => vec![PlanKind::DropComputeReplicas],
            StatementKind::DropClusters => vec![PlanKind::DropComputeInstances],
            StatementKind::DropDatabase => vec![PlanKind::DropDatabase],
            StatementKind::DropObjects => vec![PlanKind::DropItems],
            StatementKind::DropRoles => vec![PlanKind::DropRoles],
            StatementKind::DropSchema => vec![PlanKind::DropSchema],
            StatementKind::Execute => vec![PlanKind::Execute],
            StatementKind::Explain => vec![PlanKind::Explain],
            StatementKind::Fetch => vec![PlanKind::Fetch],
            StatementKind::Insert => vec![PlanKind::Insert],
            StatementKind::Prepare => vec![PlanKind::Prepare],
            StatementKind::Raise => vec![PlanKind::Raise],
            StatementKind::ResetVariable => vec![PlanKind::ResetVariable],
            StatementKind::Rollback => vec![PlanKind::AbortTransaction],
            StatementKind::Select => vec![PlanKind::Peek],
            StatementKind::SetTransaction => vec![],
            StatementKind::SetVariable => vec![PlanKind::SetVariable],
            StatementKind::Show => vec![
                PlanKind::Peek,
                PlanKind::SendRows,
                PlanKind::ShowVariable,
                PlanKind::ShowAllVariables,
            ],
            StatementKind::StartTransaction => vec![PlanKind::StartTransaction],
            StatementKind::Subscribe => vec![PlanKind::Subscribe],
            StatementKind::Update => vec![PlanKind::ReadThenWrite, PlanKind::SendRows],
        }
    }
}

#[derive(Debug)]
pub struct StartTransactionPlan {
    pub access: Option<TransactionAccessMode>,
    pub isolation_level: Option<TransactionIsolationLevel>,
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
}

#[derive(Debug)]
pub struct CreateComputeInstancePlan {
    pub name: String,
    pub replicas: Vec<(String, ComputeReplicaConfig)>,
}

#[derive(Debug)]
pub struct CreateComputeReplicaPlan {
    pub name: String,
    pub of_cluster: String,
    pub config: ComputeReplicaConfig,
}

/// Configuration of introspection for a compute replica.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialOrd, Ord, PartialEq, Eq)]
pub struct ComputeReplicaIntrospectionConfig {
    /// Whether to introspect the introspection.
    pub debugging: bool,
    /// The interval at which to introspect.
    pub interval: Duration,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ComputeReplicaConfig {
    Remote {
        addrs: BTreeSet<String>,
        compute_addrs: BTreeSet<String>,
        workers: NonZeroUsize,
        introspection: Option<ComputeReplicaIntrospectionConfig>,
    },
    Managed {
        size: String,
        availability_zone: Option<String>,
        introspection: Option<ComputeReplicaIntrospectionConfig>,
    },
}

impl ComputeReplicaConfig {
    pub fn get_az(&self) -> Option<&str> {
        match self {
            ComputeReplicaConfig::Remote {
                addrs: _,
                compute_addrs: _,
                workers: _,
                introspection: _,
            } => None,
            ComputeReplicaConfig::Managed {
                size: _,
                availability_zone,
                introspection: _,
            } => availability_zone.as_deref(),
        }
    }
}

#[derive(Debug)]
pub struct CreateSourcePlan {
    pub name: QualifiedObjectName,
    pub source: Source,
    pub if_not_exists: bool,
    pub timeline: Timeline,
    pub host_config: StorageHostConfig,
}

/// Settings related to storage hosts
///
/// This represents how resources for a storage instance are going to be
/// provisioned, based on the SQL logic. Storage equivalent of ComputeReplicaConfig.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum StorageHostConfig {
    /// Remote unmanaged storage
    Remote {
        /// The network addresses of the storaged process.
        addr: String,
    },
    /// A remote but managed storage host
    Managed {
        /// SQL size parameter used for allocation
        size: String,
    },
    /// This configuration was not defined in the SQL query, so it should use the default behavior
    Undefined,
}

#[derive(Debug)]
pub struct CreateConnectionPlan {
    pub name: QualifiedObjectName,
    pub if_not_exists: bool,
    pub connection: Connection,
}

#[derive(Debug)]
pub struct CreateSecretPlan {
    pub name: QualifiedObjectName,
    pub secret: Secret,
    pub full_name: FullObjectName,
    pub if_not_exists: bool,
}

#[derive(Debug)]
pub struct CreateSinkPlan {
    pub name: QualifiedObjectName,
    pub sink: Sink,
    pub with_snapshot: bool,
    pub if_not_exists: bool,
    pub host_config: StorageHostConfig,
}

#[derive(Debug)]
pub struct CreateTablePlan {
    pub name: QualifiedObjectName,
    pub table: Table,
    pub if_not_exists: bool,
}

#[derive(Debug)]
pub struct CreateViewPlan {
    pub name: QualifiedObjectName,
    pub view: View,
    /// The ID of the object that this view is replacing, if any.
    pub replace: Option<GlobalId>,
    pub if_not_exists: bool,
}

#[derive(Debug)]
pub struct CreateMaterializedViewPlan {
    pub name: QualifiedObjectName,
    pub materialized_view: MaterializedView,
    /// The ID of the object that this view is replacing, if any.
    pub replace: Option<GlobalId>,
    pub if_not_exists: bool,
}

#[derive(Debug)]
pub struct CreateIndexPlan {
    pub name: QualifiedObjectName,
    pub index: Index,
    pub options: Vec<IndexOption>,
    pub if_not_exists: bool,
}

#[derive(Debug)]
pub struct CreateTypePlan {
    pub name: QualifiedObjectName,
    pub typ: Type,
}

#[derive(Debug)]
pub struct DropDatabasePlan {
    pub id: Option<DatabaseId>,
}

#[derive(Debug)]
pub struct DropSchemaPlan {
    pub id: Option<(DatabaseId, SchemaId)>,
}

#[derive(Debug)]
pub struct DropRolesPlan {
    pub names: Vec<String>,
}

#[derive(Debug)]
pub struct DropComputeInstancesPlan {
    pub names: Vec<String>,
}

#[derive(Debug)]
pub struct DropComputeReplicasPlan {
    pub names: Vec<(String, String)>,
}

#[derive(Debug)]
pub struct DropItemsPlan {
    pub items: Vec<GlobalId>,
    pub ty: ObjectType,
}

#[derive(Debug)]
pub struct ShowVariablePlan {
    pub name: String,
}

#[derive(Debug)]
pub struct SetVariablePlan {
    pub name: String,
    pub value: SetVariableValue,
    pub local: bool,
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
pub struct SendRowsPlan {
    pub rows: Vec<Row>,
}

#[derive(Debug)]
pub struct CopyFromPlan {
    pub id: GlobalId,
    pub columns: Vec<usize>,
    pub params: CopyFormatParams<'static>,
}

#[derive(Debug)]
pub struct ExplainPlan {
    pub raw_plan: HirRelationExpr,
    pub row_set_finishing: Option<RowSetFinishing>,
    pub stage: ExplainStage,
    pub format: ExplainFormat,
    pub config: ExplainConfig,
    pub explainee: mz_repr::explain_new::Explainee,
}

#[derive(Debug)]
pub struct SendDiffsPlan {
    pub id: GlobalId,
    pub updates: Vec<(Row, Diff)>,
    pub kind: MutationKind,
    pub returning: Vec<(Row, NonZeroUsize)>,
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
    pub assignments: HashMap<usize, mz_expr::MirScalarExpr>,
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
    pub options: HashSet<IndexOptionName>,
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
    pub remote: AlterOptionParameter,
}

#[derive(Debug)]
pub struct AlterSourcePlan {
    pub id: GlobalId,
    pub size: AlterOptionParameter,
    pub remote: AlterOptionParameter,
}

#[derive(Debug)]
pub struct AlterItemRenamePlan {
    pub id: GlobalId,
    pub current_full_name: FullObjectName,
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
    pub value: SetVariableValue,
}

#[derive(Debug)]
pub struct AlterSystemResetPlan {
    pub name: String,
}

#[derive(Debug)]
pub struct AlterSystemResetAllPlan {}

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
    pub ingestion: Option<Ingestion>,
    pub desc: RelationDesc,
}

#[derive(Clone, Debug)]
pub struct Ingestion {
    pub desc: SourceDesc,
    pub source_imports: HashSet<GlobalId>,
    pub subsource_exports: HashMap<GlobalId, usize>,
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
    pub compute_instance: ComputeInstanceId,
}

#[derive(Clone, Debug)]
pub struct Index {
    pub create_sql: String,
    pub on: GlobalId,
    pub keys: Vec<mz_expr::MirScalarExpr>,
    pub compute_instance: ComputeInstanceId,
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
            QueryWhen::Immediately => None,
        }
    }
    /// Returns whether the candidate must be advanced to the since.
    pub fn advance_to_since(&self) -> bool {
        match self {
            QueryWhen::Immediately | QueryWhen::AtLeastTimestamp(_) => true,
            QueryWhen::AtTimestamp(_) => false,
        }
    }
    /// Returns whether the candidate must be advanced to the upper.
    pub fn advance_to_upper(&self) -> bool {
        match self {
            QueryWhen::Immediately | QueryWhen::AtLeastTimestamp(_) => true,
            QueryWhen::AtTimestamp(_) => false,
        }
    }
    /// Returns whether the candidate must be advanced to the global timestamp.
    pub fn advance_to_global_ts(&self) -> bool {
        match self {
            QueryWhen::Immediately => true,
            QueryWhen::AtLeastTimestamp(_) | QueryWhen::AtTimestamp(_) => false,
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
    pub qgm_optimizations: bool,
}

impl PlanContext {
    pub fn new(wall_time: DateTime<Utc>, qgm_optimizations: bool) -> Self {
        Self {
            wall_time,
            qgm_optimizations,
        }
    }

    /// Return a PlanContext with zero values. This should only be used when
    /// planning is required but unused (like in `plan_create_table()`) or in
    /// tests.
    pub fn zero() -> Self {
        PlanContext {
            wall_time: now::to_datetime(NOW_ZERO()),
            qgm_optimizations: false,
        }
    }
}

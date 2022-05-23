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

use std::collections::{BTreeSet, HashMap};
use std::time::Duration;

use chrono::{DateTime, Utc};
use enum_kinds::EnumKind;
use serde::{Deserialize, Serialize};

use mz_dataflow_types::client::ComputeInstanceId;
use mz_dataflow_types::sinks::{SinkConnectorBuilder, SinkEnvelope};
use mz_dataflow_types::sources::{ConnectorInner, SourceConnector};
use mz_expr::{MirRelationExpr, MirScalarExpr, RowSetFinishing};
use mz_ore::now::{self, NOW_ZERO};
use mz_repr::{ColumnName, Diff, GlobalId, RelationDesc, Row, ScalarType};

use crate::ast::{
    ExplainOptions, ExplainStage, Expr, FetchDirection, NoticeSeverity, ObjectType, Raw,
    SetVariableValue, Statement, TransactionAccessMode,
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

pub use self::expr::{HirRelationExpr, HirScalarExpr};
pub use error::PlanError;
pub use explain::Explanation;
pub use optimize::OptimizerConfig;
pub use query::{QueryContext, QueryLifetime};
pub use statement::{describe, plan, plan_copy_from, StatementContext, StatementDesc};

/// Instructions for executing a SQL query.
#[derive(Debug)]
pub enum Plan {
    CreateConnector(CreateConnectorPlan),
    CreateDatabase(CreateDatabasePlan),
    CreateSchema(CreateSchemaPlan),
    CreateRole(CreateRolePlan),
    CreateComputeInstance(CreateComputeInstancePlan),
    CreateComputeInstanceReplica(CreateComputeInstanceReplicaPlan),
    CreateSource(CreateSourcePlan),
    CreateSecret(CreateSecretPlan),
    CreateSink(CreateSinkPlan),
    CreateTable(CreateTablePlan),
    CreateView(CreateViewPlan),
    CreateViews(CreateViewsPlan),
    CreateIndex(CreateIndexPlan),
    CreateType(CreateTypePlan),
    DiscardTemp,
    DiscardAll,
    DropDatabase(DropDatabasePlan),
    DropSchema(DropSchemaPlan),
    DropRoles(DropRolesPlan),
    DropComputeInstances(DropComputeInstancesPlan),
    DropComputeInstanceReplica(DropComputeInstanceReplicaPlan),
    DropItems(DropItemsPlan),
    EmptyQuery,
    ShowAllVariables,
    ShowVariable(ShowVariablePlan),
    SetVariable(SetVariablePlan),
    StartTransaction(StartTransactionPlan),
    CommitTransaction,
    AbortTransaction,
    Peek(PeekPlan),
    Tail(TailPlan),
    SendRows(SendRowsPlan),
    CopyFrom(CopyFromPlan),
    Explain(ExplainPlan),
    SendDiffs(SendDiffsPlan),
    Insert(InsertPlan),
    AlterNoop(AlterNoopPlan),
    AlterIndexSetOptions(AlterIndexSetOptionsPlan),
    AlterIndexResetOptions(AlterIndexResetOptionsPlan),
    AlterItemRename(AlterItemRenamePlan),
    AlterSecret(AlterSecretPlan),
    Declare(DeclarePlan),
    Fetch(FetchPlan),
    Close(ClosePlan),
    ReadThenWrite(ReadThenWritePlan),
    Prepare(PreparePlan),
    Execute(ExecutePlan),
    Deallocate(DeallocatePlan),
    Raise(RaisePlan),
}

#[derive(Debug)]
pub struct StartTransactionPlan {
    pub access: Option<TransactionAccessMode>,
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
    pub config: Option<ComputeInstanceIntrospectionConfig>,
    pub replicas: Vec<(String, ReplicaConfig)>,
}

#[derive(Debug)]
pub struct CreateComputeInstanceReplicaPlan {
    pub name: String,
    pub of_cluster: String,
    pub config: ReplicaConfig,
}

/// Configuration of introspection for a compute instance.
#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub struct ComputeInstanceIntrospectionConfig {
    /// Whether to introspect the introspection.
    pub debugging: bool,
    /// The interval at which to introspect.
    pub granularity: Duration,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ReplicaConfig {
    Remote {
        replicas: BTreeSet<String>,
    },
    Managed {
        size: String,
        availability_zone: Option<String>,
    },
}

#[derive(Debug)]
pub struct CreateSourcePlan {
    pub name: QualifiedObjectName,
    pub source: Source,
    pub if_not_exists: bool,
    pub materialized: bool,
}

#[derive(Debug)]
pub struct CreateConnectorPlan {
    pub name: QualifiedObjectName,
    pub if_not_exists: bool,
    pub connector: Connector,
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
    /// whether we should auto-materialize the view
    pub materialize: bool,
    pub if_not_exists: bool,
}

#[derive(Debug)]
pub struct CreateViewsPlan {
    pub views: Vec<(QualifiedObjectName, View)>,
    pub materialize: bool,
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
pub struct DropComputeInstanceReplicaPlan {
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
pub struct PeekPlan {
    pub source: MirRelationExpr,
    pub when: QueryWhen,
    pub finishing: RowSetFinishing,
    pub copy_to: Option<CopyFormat>,
}

#[derive(Debug)]
pub struct TailPlan {
    pub from: TailFrom,
    pub with_snapshot: bool,
    pub when: QueryWhen,
    pub copy_to: Option<CopyFormat>,
    pub emit_progress: bool,
}

#[derive(Debug)]
pub enum TailFrom {
    Id(GlobalId),
    Query {
        expr: MirRelationExpr,
        desc: RelationDesc,
        depends_on: Vec<GlobalId>,
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
    pub params: CopyParams,
}

#[derive(Debug)]
pub struct ExplainPlan {
    pub raw_plan: HirRelationExpr,
    pub row_set_finishing: Option<RowSetFinishing>,
    pub stage: ExplainStage,
    pub options: ExplainOptions,
}

#[derive(Debug)]
pub struct SendDiffsPlan {
    pub id: GlobalId,
    pub updates: Vec<(Row, Diff)>,
    pub kind: MutationKind,
}

#[derive(Debug)]
pub struct InsertPlan {
    pub id: GlobalId,
    pub values: mz_expr::MirRelationExpr,
}

#[derive(Debug)]
pub struct ReadThenWritePlan {
    pub id: GlobalId,
    pub selection: mz_expr::MirRelationExpr,
    pub finishing: RowSetFinishing,
    pub assignments: HashMap<usize, mz_expr::MirScalarExpr>,
    pub kind: MutationKind,
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
    pub options: Vec<IndexOptionName>,
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
    pub depends_on: Vec<GlobalId>,
}

#[derive(Clone, Debug)]
pub struct Source {
    pub create_sql: String,
    pub connector: SourceConnector,
    pub desc: RelationDesc,
    pub depends_on: Vec<GlobalId>,
}

#[derive(Clone, Debug)]
pub struct Connector {
    pub create_sql: String,
    pub connector: ConnectorInner,
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
    pub connector_builder: SinkConnectorBuilder,
    pub envelope: SinkEnvelope,
    pub depends_on: Vec<GlobalId>,
    pub compute_instance: ComputeInstanceId,
}

#[derive(Clone, Debug)]
pub struct View {
    pub create_sql: String,
    pub expr: mz_expr::MirRelationExpr,
    pub column_names: Vec<ColumnName>,
    pub temporary: bool,
    pub depends_on: Vec<GlobalId>,
}

#[derive(Clone, Debug)]
pub struct Index {
    pub create_sql: String,
    pub on: GlobalId,
    pub keys: Vec<mz_expr::MirScalarExpr>,
    pub depends_on: Vec<GlobalId>,
    pub compute_instance: ComputeInstanceId,
}

#[derive(Clone, Debug)]
pub struct Type {
    pub create_sql: String,
    pub inner: CatalogType<IdReference>,
    pub depends_on: Vec<GlobalId>,
}

/// Specifies when a `Peek` or `Tail` should occur.
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
    pub fn advance_to_upper(&self, uses_tables: bool) -> bool {
        match self {
            QueryWhen::Immediately | QueryWhen::AtLeastTimestamp(_) => !uses_tables,
            QueryWhen::AtTimestamp(_) => false,
        }
    }
    /// Returns whether the candidate must be advanced to the global table timestamp.
    pub fn advance_to_table_ts(&self, uses_tables: bool) -> bool {
        match self {
            QueryWhen::Immediately | QueryWhen::AtLeastTimestamp(_) => uses_tables,
            QueryWhen::AtTimestamp(_) => false,
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

#[derive(Debug, Clone)]
pub struct CopyParams {
    pub format: CopyFormat,
    pub null: Option<String>,
    pub delimiter: Option<String>,
    pub quote: Option<String>,
    pub escape: Option<String>,
    pub header: Option<bool>,
}

#[derive(Debug, Copy, Clone)]
pub enum ExecuteTimeout {
    None,
    Seconds(f64),
    WaitOnce,
}

#[derive(Clone, Debug, EnumKind)]
#[enum_kind(IndexOptionName)]
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

// Copyright Materialize, Inc. All rights reserved.
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

use std::time::Duration;

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use ::expr::{GlobalId, RowSetFinishing};
use dataflow_types::{SinkConnectorBuilder, SourceConnector};
use repr::{ColumnName, RelationDesc, Row, ScalarType, Timestamp};

use crate::ast::{ExplainOptions, ExplainStage, ObjectType, Statement};
use crate::catalog::Catalog;
use crate::names::{DatabaseSpecifier, FullName};

pub(crate) mod decorrelate;
pub(crate) mod error;
pub(crate) mod explain;
pub(crate) mod expr;
pub(crate) mod func;
pub(crate) mod query;
pub(crate) mod scope;
pub(crate) mod statement;
pub(crate) mod transform_ast;
pub(crate) mod transform_expr;
pub(crate) mod typeconv;

pub use self::expr::RelationExpr;
pub use error::PlanError;
// This is used by sqllogictest to turn SQL values into `Datum`s.
pub use query::scalar_type_from_sql;
pub use statement::{describe_statement, StatementContext, StatementDesc};

/// Instructions for executing a SQL query.
#[derive(Debug)]
pub enum Plan {
    CreateDatabase {
        name: String,
        if_not_exists: bool,
    },
    CreateSchema {
        database_name: DatabaseSpecifier,
        schema_name: String,
        if_not_exists: bool,
    },
    CreateSource {
        name: FullName,
        source: Source,
        if_not_exists: bool,
        materialized: bool,
    },
    CreateSink {
        name: FullName,
        sink: Sink,
        with_snapshot: bool,
        as_of: Option<Timestamp>,
        if_not_exists: bool,
    },
    CreateTable {
        name: FullName,
        table: Table,
        if_not_exists: bool,
    },
    CreateView {
        name: FullName,
        view: View,
        /// The ID of the object that this view is replacing, if any.
        replace: Option<GlobalId>,
        /// whether we should auto-materialize the view
        materialize: bool,
        if_not_exists: bool,
    },
    CreateIndex {
        name: FullName,
        index: Index,
        if_not_exists: bool,
    },
    CreateType {
        name: FullName,
        typ: Type,
    },
    DiscardTemp,
    DiscardAll,
    DropDatabase {
        name: String,
    },
    DropSchema {
        database_name: DatabaseSpecifier,
        schema_name: String,
    },
    DropItems {
        items: Vec<GlobalId>,
        ty: ObjectType,
    },
    EmptyQuery,
    ShowAllVariables,
    ShowVariable(String),
    SetVariable {
        name: String,
        value: String,
    },
    StartTransaction,
    CommitTransaction,
    AbortTransaction,
    Peek {
        source: ::expr::RelationExpr,
        when: PeekWhen,
        finishing: RowSetFinishing,
        copy_to: Option<CopyFormat>,
    },
    Tail {
        id: GlobalId,
        with_snapshot: bool,
        ts: Option<Timestamp>,
        copy_to: Option<CopyFormat>,
        emit_progress: bool,
        object_columns: usize,
    },
    SendRows(Vec<Row>),
    ExplainPlan {
        raw_plan: RelationExpr,
        decorrelated_plan: ::expr::RelationExpr,
        row_set_finishing: Option<RowSetFinishing>,
        stage: ExplainStage,
        options: ExplainOptions,
    },
    SendDiffs {
        id: GlobalId,
        updates: Vec<(Row, isize)>,
        affected_rows: usize,
        kind: MutationKind,
    },
    Insert {
        id: GlobalId,
        values: ::expr::RelationExpr,
    },
    AlterItemRename {
        id: Option<GlobalId>,
        to_name: String,
        object_type: ObjectType,
    },
    AlterIndexLogicalCompactionWindow(Option<AlterIndexLogicalCompactionWindow>),
}

#[derive(Clone, Debug)]
pub struct Table {
    pub create_sql: String,
    pub desc: RelationDesc,
}

#[derive(Clone, Debug)]
pub struct Source {
    pub create_sql: String,
    pub connector: SourceConnector,
    pub desc: RelationDesc,
}

#[derive(Clone, Debug)]
pub struct Sink {
    pub create_sql: String,
    pub from: GlobalId,
    pub connector_builder: SinkConnectorBuilder,
}

#[derive(Clone, Debug)]
pub struct View {
    pub create_sql: String,
    pub expr: ::expr::RelationExpr,
    pub column_names: Vec<Option<ColumnName>>,
    pub temporary: bool,
}

#[derive(Clone, Debug)]
pub struct Index {
    pub create_sql: String,
    pub on: GlobalId,
    pub keys: Vec<::expr::ScalarExpr>,
}

#[derive(Clone, Debug)]
pub struct Type {
    pub create_sql: String,
    pub inner: TypeInner,
}

#[derive(Clone, Debug)]
pub enum TypeInner {
    List {
        element_id: GlobalId,
    },
    Map {
        key_id: GlobalId,
        value_id: GlobalId,
    },
}

/// Specifies when a `Peek` should occur.
#[derive(Debug, PartialEq)]
pub enum PeekWhen {
    /// The peek should occur at the latest possible timestamp that allows the
    /// peek to complete immediately.
    Immediately,
    /// The peek should occur at the specified timestamp.
    AtTimestamp(Timestamp),
}

#[derive(Debug)]
pub enum MutationKind {
    Insert,
    Update,
    Delete,
}

#[derive(Debug)]
pub enum CopyFormat {
    Text,
    Csv,
    Binary,
}

#[derive(Debug, PartialEq)]
pub struct AlterIndexLogicalCompactionWindow {
    pub index: GlobalId,
    pub logical_compaction_window: LogicalCompactionWindow,
}

/// Specifies what value the `logical_compaction_window` parameter should be set to.
#[derive(Debug, PartialEq)]
pub enum LogicalCompactionWindow {
    /// Disable logical compaction.
    Off,
    /// Set compaction to the system wide default.
    Default,
    Custom(Duration),
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
            datums: Row::pack(&[]),
            types: vec![],
        }
    }
}

/// Controls planning of a SQL query.
#[derive(Ord, PartialOrd, Clone, Debug, Eq, PartialEq, Serialize, Deserialize, Hash)]
pub struct PlanContext {
    pub wall_time: DateTime<Utc>,
}

impl Default for PlanContext {
    fn default() -> PlanContext {
        PlanContext {
            wall_time: Utc::now(),
        }
    }
}

/// Produces a [`Plan`] from the purified statement `stmt`.
///
/// Planning is a pure, synchronous function and so requires that the provided
/// `stmt` does does not depend on any external state. To purify a statement,
/// use [`crate::pure::purify`].
pub fn plan(
    pcx: &PlanContext,
    catalog: &dyn Catalog,
    stmt: Statement,
    params: &Params,
) -> Result<Plan, anyhow::Error> {
    statement::handle_statement(pcx, catalog, stmt, params)
}

/// Creates a description of the purified statement `stmt`.
///
/// See the documentation of [`StatementDesc`] for details.
pub fn describe(
    catalog: &dyn Catalog,
    stmt: Statement,
    param_types: &[Option<pgrepr::Type>],
) -> Result<StatementDesc, anyhow::Error> {
    statement::describe_statement(catalog, stmt, param_types)
}

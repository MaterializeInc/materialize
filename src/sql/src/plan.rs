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

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use ::expr::{GlobalId, RowSetFinishing};
use dataflow_types::{PeekWhen, SinkConnectorBuilder, SourceConnector, Timestamp};
use repr::{ColumnName, RelationDesc, Row, ScalarType};

use crate::ast::{ExplainOptions, ExplainStage, ObjectType, Statement};
use crate::catalog::Catalog;
use crate::names::{DatabaseSpecifier, FullName};

pub(crate) mod cast;
pub(crate) mod explain;
pub(crate) mod expr;
pub(crate) mod func;
pub(crate) mod query;
pub(crate) mod scope;
pub(crate) mod statement;
pub(crate) mod transform;

pub use self::expr::RelationExpr;
// This is used by sqllogictest to turn SQL values into `Datum`s.
pub use query::scalar_type_from_sql;
pub use statement::StatementContext;

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
        if_not_exists: bool,
    },
    CreateTable {
        name: FullName,
        desc: RelationDesc,
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
        materialize: bool,
    },
    Tail {
        id: GlobalId,
        with_snapshot: bool,
        ts: Option<Timestamp>,
    },
    SendRows(Vec<Row>),
    ExplainPlan {
        sql: String,
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
    ShowViews {
        ids: Vec<(String, GlobalId)>,
        full: bool,
        show_queryable: bool,
        limit_materialized: bool,
    },
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

#[derive(Debug)]
pub enum MutationKind {
    Insert,
    Update,
    Delete,
}

/// A vector of values to which parameter references should be bound.
#[derive(Debug, Clone)]
pub struct Params {
    pub datums: Row,
    pub types: Vec<ScalarType>,
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
) -> Result<Plan, failure::Error> {
    statement::handle_statement(pcx, catalog, stmt, params)
}

/// Determines the type of the rows that will be returned by `stmt` and the type
/// of the parameters required by `stmt`. If the statement will not produce a
/// result set (e.g., most `CREATE` or `DROP` statements), no `RelationDesc`
/// will be returned. If the query uses no parameters, then the returned vector
/// of types will be empty.
pub fn describe(
    catalog: &dyn Catalog,
    stmt: Statement,
) -> Result<(Option<RelationDesc>, Vec<pgrepr::Type>), failure::Error> {
    let (desc, types) = statement::describe_statement(catalog, stmt)?;
    let types = types.into_iter().map(|t| pgrepr::Type::from(&t)).collect();
    Ok((desc, types))
}

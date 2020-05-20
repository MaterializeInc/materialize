// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! SQL-dataflow translation.

#![deny(missing_debug_implementations)]

use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

use ::expr::{GlobalId, RowSetFinishing};
use dataflow_types::{PeekWhen, SinkConnectorBuilder, SourceConnector, Timestamp};
use repr::{RelationDesc, Row, ScalarType};
use sql_parser::parser::Parser as SqlParser;

pub use crate::expr::RelationExpr;
pub use catalog::PlanCatalog;
pub use names::{DatabaseSpecifier, FullName, PartialName};
pub use session::{InternalSession, PlanSession, PreparedStatement, Session, TransactionStatus};
pub use sql_parser::ast::{ExplainOptions, ExplainStage, ObjectType, Statement};
pub use statement::StatementContext;

pub mod catalog;
pub mod normalize;

mod explain;
mod expr;
mod kafka_util;
mod names;
mod query;
mod scope;
mod session;
mod statement;
mod transform;

// this is used by sqllogictest to turn sql values into `Datum`
pub use query::scalar_type_from_sql;

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
        /// If the view is temporary, it will only exist for a single connection/
        /// session. Otherwise, this value will be None.
        conn_id: Option<u32>,
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
        raw_plan: crate::expr::RelationExpr,
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
    pub desc: RelationDesc,
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

/// Parses a raw SQL string into a [`Statement`].
pub fn parse(sql: String) -> Result<Vec<Statement>, failure::Error> {
    Ok(SqlParser::parse_sql(sql)?)
}

/// Removes dependencies on external state from `stmt`: inlining schemas in
/// files, fetching schemas from registries, and so on. The [`Statement`]
/// returned from this function will be valid to pass to `Plan`.
///
/// Note that purification is asynchronous, and may take an unboundedly long
/// time to complete.
pub async fn purify(stmt: Statement) -> Result<Statement, failure::Error> {
    statement::purify_statement(stmt).await
}

/// Produces a [`Plan`] from the purified statement `stmt`.
///
/// Planning is a pure, synchronous function and so requires that the provided
/// `stmt` does does not depend on any external state. To purify a statement,
/// use [`purify`].
pub fn plan(
    pcx: &PlanContext,
    catalog: &dyn PlanCatalog,
    session: &dyn PlanSession,
    stmt: Statement,
    params: &Params,
) -> Result<Plan, failure::Error> {
    statement::handle_statement(pcx, catalog, session, stmt, params)
}

/// Determines the type of the rows that will be returned by `stmt` and the type
/// of the parameters required by `stmt`. If the statement will not produce a
/// result set (e.g., most `CREATE` or `DROP` statements), no `RelationDesc`
/// will be returned. If the query uses no parameters, then the returned vector
/// of types will be empty.
pub fn describe(
    catalog: &dyn PlanCatalog,
    session: &Session,
    stmt: Statement,
) -> Result<(Option<RelationDesc>, Vec<pgrepr::Type>), failure::Error> {
    let (desc, types) = statement::describe_statement(catalog, session, stmt)?;
    let types = types.into_iter().map(|t| pgrepr::Type::from(&t)).collect();
    Ok((desc, types))
}

// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

//! SQL-dataflow translation.

#![deny(missing_debug_implementations)]

use dataflow_types::{Index, PeekWhen, RowSetFinishing, Sink, Source, View};

use ::expr::{GlobalId, RelationExpr};
use catalog::names::FullName;
use catalog::{Catalog, CatalogEntry};
use ore::future::MaybeFuture;
use repr::{RelationDesc, Row, ScalarType};
use sql_parser::parser::Parser as SqlParser;

pub use session::{PreparedStatement, Session, TransactionStatus};
pub use sql_parser::ast::{ObjectType, Statement};
pub use statement::StatementContext;

pub mod normalize;

mod expr;
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
        database_name: String,
        schema_name: String,
        if_not_exists: bool,
    },
    CreateIndex {
        name: FullName,
        index: Index,
        if_not_exists: bool,
    },
    CreateSource {
        name: FullName,
        source: Source,
        if_not_exists: bool,
    },
    CreateSources(Vec<(FullName, Source)>),
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
        view: View<RelationExpr>,
        /// The IDs of the object that this view is replacing, if any.
        replace: Vec<GlobalId>,
    },
    DropItems(Vec<GlobalId>, ObjectType),
    EmptyQuery,
    SetVariable {
        /// The name of the variable
        name: String,
        value: String,
    },
    /// Nothing needs to happen, but the frontend must be notified
    StartTransaction,
    /// Commit a transaction
    ///
    /// We don't do anything for transactions, so other than changing the session state
    /// this is a no-op
    Commit,
    /// Rollback a transaction
    ///
    /// We don't do anything for transactions, so other than changing the session state
    /// this is a no-op
    Rollback,
    Peek {
        source: ::expr::RelationExpr,
        when: PeekWhen,
        finishing: RowSetFinishing,
        eval_env: ::expr::EvalEnv,
        materialize: bool,
    },
    Tail(CatalogEntry),
    SendRows(Vec<Row>),
    ExplainPlan(::expr::RelationExpr, ::expr::EvalEnv),
    SendDiffs {
        id: GlobalId,
        updates: Vec<(Row, isize)>,
        affected_rows: usize,
        kind: MutationKind,
    },
}

#[derive(Debug)]
pub enum MutationKind {
    Insert,
    Update,
    Delete,
}

/// A vector of values to which parameter references should be bound.
#[derive(Debug)]
pub struct Params {
    pub datums: Row,
    pub types: Vec<ScalarType>,
}

/// Parses a raw SQL string into a [`Statement`].
pub fn parse(sql: String) -> Result<Vec<Statement>, failure::Error> {
    Ok(SqlParser::parse_sql(sql)?)
}

/// Produces a [`Plan`] from a [`Statement`].
pub fn plan(
    catalog: &Catalog,
    session: &Session,
    stmt: Statement,
    params: &Params,
) -> MaybeFuture<'static, Result<Plan, failure::Error>> {
    statement::handle_statement(catalog, session, stmt, params)
}

/// Determines the type of the rows that will be returned by `stmt` and the type
/// of the parameters required by `stmt`. If the statement will not produce a
/// result set (e.g., most `CREATE` or `DROP` statements), no `RelationDesc`
/// will be returned. If the query uses no parameters, then the returned vector
/// of types will be empty.
pub fn describe(
    catalog: &Catalog,
    session: &Session,
    stmt: Statement,
) -> Result<(Option<RelationDesc>, Vec<pgrepr::Type>), failure::Error> {
    let (desc, types) = statement::describe_statement(catalog, session, stmt)?;
    let types = types.into_iter().map(pgrepr::Type::from).collect();
    Ok((desc, types))
}

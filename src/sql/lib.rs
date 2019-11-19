// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

//! SQL-dataflow translation.

#![deny(missing_debug_implementations)]

use dataflow_types::{Index, PeekWhen, RowSetFinishing, Sink, Source, View};

use ::expr::GlobalId;
use catalog::{Catalog, CatalogEntry};
use repr::{QualName, RelationDesc, Row, ScalarType};
use sqlparser::dialect::AnsiDialect;
use sqlparser::parser::Parser as SqlParser;

pub use session::{PreparedStatement, Session, TransactionStatus};
pub use sqlparser::ast::{ObjectType, Statement};

pub mod names;

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
    CreateIndex(QualName, Index),
    CreateSource(QualName, Source),
    CreateSources(Vec<(QualName, Source)>),
    CreateSink(QualName, Sink),
    CreateTable {
        name: QualName,
        desc: RelationDesc,
    },
    CreateView(QualName, View),
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
    },
    Tail(CatalogEntry),
    SendRows(Vec<Row>),
    ExplainPlan(::expr::RelationExpr),
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

/// Parses a raw SQL string into a [`Statement`].
pub fn parse(sql: String) -> Result<Vec<Statement>, failure::Error> {
    Ok(SqlParser::parse_sql(&AnsiDialect {}, sql)?)
}

/// Produces a [`Plan`] from a [`Statement`].
pub fn plan(
    catalog: &Catalog,
    session: &Session,
    stmt: Statement,
    portal_name: Option<String>,
) -> Result<Plan, failure::Error> {
    statement::handle_statement(catalog, session, stmt, portal_name)
}

/// Determines the type of the rows that will be returned by `stmt` and the type
/// of the parameters required by `stmt`. If the statement will not produce a
/// result set (e.g., most `CREATE` or `DROP` statements), no `RelationDesc`
/// will be returned. If the query uses no parameters, then the returned vector
/// of types will be empty.
pub fn describe(
    catalog: &Catalog,
    stmt: Statement,
) -> Result<(Option<RelationDesc>, Vec<ScalarType>), failure::Error> {
    statement::describe_statement(catalog, stmt)
}

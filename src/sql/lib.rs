// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

//! SQL-dataflow translation.

#![deny(missing_debug_implementations)]

use dataflow_types::{PeekWhen, RowSetFinishing, Sink, Source, View};

use repr::{RelationDesc, Row};
use sqlparser::dialect::AnsiDialect;
use sqlparser::parser::Parser as SqlParser;
use store::{Catalog, CatalogItem};

pub use session::{PreparedStatement, Session};
pub use sqlparser::ast::{ObjectType, Statement};

mod expr;
mod query;
mod scope;
mod session;
mod statement;
pub mod store;
mod transform;

// this is used by sqllogictest to turn sql values into `Datum`
pub use query::scalar_type_from_sql;

/// Instructions for executing a SQL query.
#[derive(Debug)]
pub enum Plan {
    CreateSource(Source),
    CreateSources(Vec<Source>),
    CreateSink(Sink),
    CreateTable {
        name: String,
        desc: RelationDesc,
    },
    CreateView(View),
    DropItems(Vec<String>, ObjectType),
    EmptyQuery,
    SetVariable {
        name: String,
        value: String,
    },
    Peek {
        source: ::expr::RelationExpr,
        desc: RelationDesc,
        when: PeekWhen,
        finishing: RowSetFinishing,
    },
    Tail(CatalogItem),
    SendRows {
        desc: RelationDesc,
        rows: Vec<Row>,
    },
    ExplainPlan {
        desc: RelationDesc,
        relation_expr: ::expr::RelationExpr,
    },
    SendDiffs {
        name: String,
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
pub fn plan(catalog: &Catalog, session: &Session, stmt: Statement) -> Result<Plan, failure::Error> {
    statement::handle_statement(catalog, session, stmt)
}

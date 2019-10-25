// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

//! SQL-dataflow translation.

#![deny(missing_debug_implementations)]

use dataflow_types::{PeekWhen, RowSetFinishing, Sink, Source, View};
use failure::bail;

use ore::collections::CollectionExt;
use repr::{Datum, RelationDesc, ScalarType};
pub use session::Session;
use session::PreparedStatement;
use sqlparser::ast::ObjectName;
use sqlparser::dialect::AnsiDialect;
use sqlparser::parser::Parser as SqlParser;
use store::{Catalog, CatalogItem};

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
    CreateView(View),
    // DropSources(Vec<String>),
    // DropViews(Vec<String>),
    /// Items to drop, and true iff a source.
    DropItems((Vec<String>, bool)),
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
        rows: Vec<Vec<Datum>>,
    },
    ExplainPlan {
        desc: RelationDesc,
        relation_expr: ::expr::RelationExpr,
    },
}

/// Mutates the internal state of the planner and session according to the
/// `plan`. Centralizing mutations here ensures the rest of the planning
/// process is immutable, which is useful for prepared queries.
///
/// Note that there are additional mutations that will be undertaken in the
/// dataflow layer according to this plan; this method only applies
/// mutations internal to the planner and session.
fn apply_plan(
    session: &mut Session,
    plan: &Plan,
    catalog: &mut Catalog,
) -> Result<(), failure::Error> {
    match plan {
        Plan::CreateView(view) => catalog.insert(CatalogItem::View(view.clone())),
        Plan::CreateSource(source) => catalog.insert(CatalogItem::Source(source.clone())),
        Plan::CreateSources(sources) => {
            for source in sources {
                catalog.insert(CatalogItem::Source(source.clone()))?
            }
            Ok(())
        }
        Plan::CreateSink(sink) => catalog.insert(CatalogItem::Sink(sink.clone())),
        Plan::DropItems((names, _is_source)) => {
            for name in names {
                catalog.remove(name);
            }
            Ok(())
        }
        Plan::SetVariable { name, value } => session.set(name, value),
        _ => Ok(()),
    }
}

/// Parses and plans several raw SQL queries.
pub fn handle_commands(
    session: &mut Session,
    sql: String,
    catalog: &mut Catalog,
) -> Result<Vec<Plan>, failure::Error> {
    let statements = SqlParser::parse_sql(&AnsiDialect {}, sql)?;
    let mut results = Vec::new();
    for statement in statements {
        let plan = statement::handle_statement(catalog, session, statement)?;
        apply_plan(session, &plan, catalog)?;
        results.push(plan);
    }
    Ok(results)
}

/// Parses and plans a raw SQL query. See the documentation for
/// [`Result<Plan, failure::Error>`] for details about the meaning of the return type.
pub fn handle_command(
    session: &mut Session,
    sql: String,
    catalog: &mut Catalog,
) -> Result<Plan, failure::Error> {
    let stmts = SqlParser::parse_sql(&AnsiDialect {}, sql)?;
    match stmts.len() {
        0 => Ok(Plan::EmptyQuery),
        1 => {
            let plan = statement::handle_statement(catalog, session, stmts.into_element())?;
            apply_plan(session, &plan, catalog)?;
            Ok(plan)
        }
        _ => bail!("expected one statement, but got {}", stmts.len()),
    }
}

pub fn handle_execute_command(
    session: &mut Session,
    portal_name: &str,
    catalog: &mut Catalog,
) -> Result<Plan, failure::Error> {
    let portal = session
        .get_portal(portal_name)
        .ok_or_else(|| failure::format_err!("portal does not exist {:?}", portal_name))?;
    let prepared = session
        .get_prepared_statement(&portal.statement_name)
        .ok_or_else(|| {
            failure::format_err!(
                "statement for portal does not exist portal={:?} statement={:?}",
                portal_name,
                portal.statement_name
            )
        })?;
    match prepared.sql() {
        Some(sql) => {
            let plan = statement::handle_statement(catalog, session, sql.clone())?;
            apply_plan(session, &plan, catalog)?;
            Ok(plan)
        }
        None => Ok(Plan::EmptyQuery),
    }
}

/// Parses the specified SQL into a prepared statement.
///
/// The prepared statement is saved in the connection's [`sql::Session`]
/// under the specified name.
pub fn handle_parse_command(
    catalog: &Catalog,
    session: &mut Session,
    sql: String,
    name: String,
) -> Result<(), failure::Error> {
    let stmts = SqlParser::parse_sql(&AnsiDialect {}, sql.clone())?;
    match stmts.len() {
        0 => session.set_prepared_statement(name, PreparedStatement::empty()),
        1 => {
            let stmt = stmts.into_element();
            let plan = statement::handle_statement(catalog, session, stmt.clone())?;
            let desc = match plan {
                Plan::Peek { desc, .. } => Some(desc),
                Plan::SendRows { desc, .. } => Some(desc),
                Plan::ExplainPlan { desc, .. } => Some(desc),
                Plan::CreateSources { .. } => {
                    Some(RelationDesc::empty().add_column("Topic", ScalarType::String))
                }
                _ => None,
            };
            session.set_prepared_statement(name, PreparedStatement::new(stmt, desc));
        }
        n => bail!("expected one statement but got {}: {:?}", n, sql),
    }
    Ok(())
}

fn extract_sql_object_name(n: &ObjectName) -> Result<String, failure::Error> {
    if n.0.len() != 1 {
        bail!("qualified names are not yet supported: {}", n.to_string())
    }
    Ok(n.to_string())
}

// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

use std::error::Error;
use std::thread;
use std::time::Duration;

use postgres::error::DbError;
use postgres::row::Row;
use postgres::types::Type;
use sql_parser::ast::Statement;
use sql_parser::dialect::PostgreSqlDialect;
use sql_parser::parser::Parser as SqlParser;

use ore::collections::CollectionExt;
use pgrepr::{Interval, Numeric};

use crate::action::{Action, State};
use crate::parser::{FailSqlCommand, SqlCommand};

pub struct SqlAction {
    cmd: SqlCommand,
    stmt: Statement,
}

pub fn build_sql(mut cmd: SqlCommand) -> Result<SqlAction, String> {
    let stmts = SqlParser::parse_sql(&PostgreSqlDialect {}, cmd.query.clone())
        .map_err(|e| format!("unable to parse SQL: {}: {}", cmd.query, e))?;
    if stmts.len() != 1 {
        return Err(format!("expected one statement, but got {}", stmts.len()));
    }
    // TODO(benesch): one day we'll support SQL queries where order matters.
    cmd.expected_rows.sort();
    Ok(SqlAction {
        cmd,
        stmt: stmts.into_element(),
    })
}

impl Action for SqlAction {
    fn undo(&self, state: &mut State) -> Result<(), String> {
        match &self.stmt {
            Statement::CreateSource { name, .. } => self.try_drop(
                &mut state.pgclient,
                &format!("DROP SOURCE IF EXISTS {} CASCADE", name.to_string()),
            ),
            Statement::CreateView { name, .. } => self.try_drop(
                &mut state.pgclient,
                &format!("DROP VIEW IF EXISTS {} CASCADE", name.to_string()),
            ),
            Statement::CreateTable { name, .. } => self.try_drop(
                &mut state.pgclient,
                &format!("DROP TABLE IF EXISTS {} CASCADE", name.to_string()),
            ),
            _ => Ok(()),
        }
    }

    fn redo(&self, state: &mut State) -> Result<(), String> {
        let query = &self.cmd.query;
        print_query(&query);
        let max = match self.stmt {
            // TODO(benesch): this is horrible. PEEK needs to learn to wait
            // until it's up to date.
            Statement::Peek { .. } | Statement::Query { .. } => 7,
            _ => 0,
        };
        let mut i = 0;
        loop {
            let backoff = Duration::from_millis(100 * 2_u64.pow(i));
            match self.try_redo(&mut state.pgclient, &query) {
                Ok(()) => {
                    println!("rows match; continuing");
                    return Ok(());
                }
                Err(err) => {
                    if i >= max {
                        return Err(err);
                    } else {
                        println!(
                            "rows didn't match; sleeping {:?} to see if dataflow catches up",
                            backoff
                        );
                    }
                }
            }
            thread::sleep(backoff);
            i += 1;
        }
    }
}

impl SqlAction {
    fn try_drop(&self, pgclient: &mut postgres::Client, query: &str) -> Result<(), String> {
        print_query(&query);
        match pgclient.query(query, &[]) {
            Err(err) => Err(err.to_string()),
            Ok(_) => Ok(()),
        }
    }

    fn try_redo(&self, pgclient: &mut postgres::Client, query: &str) -> Result<(), String> {
        let mut rows: Vec<_> = pgclient
            .query(query, &[])
            .map_err(|e| format!("query failed: {}", e))?
            .into_iter()
            .map(decode_row)
            .collect::<Result<_, _>>()?;
        rows.sort();
        if rows == self.cmd.expected_rows {
            Ok(())
        } else {
            // TODO(benesch): a better diff here would be nice.
            Err(format!(
                "non-matching rows: expected:\n{:?}\ngot:\n{:?}",
                self.cmd.expected_rows, rows
            ))
        }
    }
}

pub struct FailSqlAction(FailSqlCommand);

pub fn build_fail_sql(cmd: FailSqlCommand) -> Result<FailSqlAction, String> {
    Ok(FailSqlAction(cmd))
}

impl Action for FailSqlAction {
    fn undo(&self, _state: &mut State) -> Result<(), String> {
        Ok(())
    }

    fn redo(&self, state: &mut State) -> Result<(), String> {
        let query = &self.0.query;
        print_query(&query);
        match state.pgclient.query(query.as_str(), &[]) {
            Ok(_) => Err(format!(
                "query succeeded, but expected error '{}'",
                self.0.expected_error
            )),
            Err(err) => {
                let err_string = err.to_string();
                match err.source().and_then(|err| err.downcast_ref::<DbError>()) {
                    Some(err) => {
                        if err.message().contains(&self.0.expected_error) {
                            Ok(())
                        } else {
                            Err(format!(
                                "expected error containing '{}', but got '{}'",
                                self.0.expected_error, err
                            ))
                        }
                    }
                    None => Err(err_string),
                }
            }
        }
    }
}

fn print_query(query: &str) {
    if query.len() > 72 {
        println!("> {}...", &query[..72]);
    } else {
        println!("> {}", &query);
    }
}

fn decode_row(row: Row) -> Result<Vec<String>, String> {
    let mut out = vec![];
    for (i, col) in row.columns().iter().enumerate() {
        let ty = col.type_();
        out.push(
            match *ty {
                Type::BOOL => row.get::<_, Option<bool>>(i).map(|x| x.to_string()),
                Type::CHAR | Type::TEXT => row.get::<_, Option<String>>(i),
                Type::INT4 => row.get::<_, Option<i32>>(i).map(|x| x.to_string()),
                Type::INT8 => row.get::<_, Option<i64>>(i).map(|x| x.to_string()),
                Type::NUMERIC => row.get::<_, Option<Numeric>>(i).map(|x| x.to_string()),
                Type::TIMESTAMP => row
                    .get::<_, Option<chrono::NaiveDateTime>>(i)
                    .map(|x| x.to_string()),
                Type::TIMESTAMPTZ => row
                    .get::<_, Option<chrono::DateTime<chrono::Utc>>>(i)
                    .map(|x| x.to_string()),
                Type::DATE => row
                    .get::<_, Option<chrono::NaiveDate>>(i)
                    .map(|x| x.to_string()),
                Type::INTERVAL => row.get::<_, Option<Interval>>(i).map(|x| x.to_string()),
                _ => return Err(format!("unable to handle SQL type: {:?}", ty)),
            }
            .unwrap_or_else(|| "<null>".into()),
        )
    }
    Ok(out)
}

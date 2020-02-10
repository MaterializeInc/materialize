// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::env;
use std::error::Error as _;
use std::fmt::Write as _;
use std::io::{self, Write};
use std::thread;
use std::time::Duration;

use postgres::error::DbError;
use postgres::row::Row;
use postgres::types::{Json, Type};
use serde_json::Value;
use sql_parser::ast::Statement;
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
    let stmts = SqlParser::parse_sql(cmd.query.clone())
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
            Statement::CreateDatabase { name, .. } => self.try_drop(
                &mut state.pgclient,
                &format!("DROP DATABASE IF EXISTS {}", name.to_string()),
            ),
            Statement::CreateSchema { name, .. } => self.try_drop(
                &mut state.pgclient,
                &format!("DROP SCHEMA IF EXISTS {} CASCADE", name.to_string()),
            ),
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
            // TODO(benesch): this is horrible. SELECT needs to learn to wait
            // until it's up to date.
            Statement::Query { .. } => env::var("MZ_TD_MAX_RETRIES")
                .unwrap_or_else(|_| "7".into())
                .parse()
                .map_err(|e| format!("invalid MZ_TD_MAX_RETRIES: {}", e))?,
            _ => 0,
        };
        let mut i = 0;
        loop {
            let backoff = Duration::from_millis(100 * 2_u64.pow(i));
            match self.try_redo(&mut state.pgclient, &query) {
                Ok(()) => {
                    if i > 0 {
                        println!();
                    }
                    println!("rows match; continuing");
                    return Ok(());
                }
                Err(err) => {
                    if i == 0 {
                        print!(
                            "rows didn't match; sleeping to see if dataflow catches up {:?}",
                            backoff
                        );
                        io::stdout().flush().unwrap();
                    } else if i < max {
                        print!(" {:?}", backoff);
                        io::stdout().flush().unwrap();
                    } else {
                        println!();
                        return Err(err);
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
        let mut actual: Vec<_> = pgclient
            .query(query, &[])
            .map_err(|e| format!("query failed: {}", e))?
            .into_iter()
            .map(decode_row)
            .collect::<Result<_, _>>()?;
        actual.sort();
        if actual == self.cmd.expected_rows {
            Ok(())
        } else {
            let (mut left, mut right) = (0, 0);
            let expected = &self.cmd.expected_rows;
            let mut buf = String::new();
            while left < expected.len() && right < actual.len() {
                // the ea logic below is complex enough without adding the indirection of Ordering::*
                #[allow(clippy::comparison_chain)]
                match (expected.get(left), actual.get(right)) {
                    (Some(e), Some(a)) => {
                        if e == a {
                            left += 1;
                            right += 1;
                        } else if e > a {
                            writeln!(buf, "extra row: {:?}", a).unwrap();
                            right += 1;
                        } else if e < a {
                            writeln!(buf, "row missing: {:?}", e).unwrap();
                            left += 1;
                        }
                    }
                    (None, Some(a)) => {
                        writeln!(buf, "extra row: {:?}", a).unwrap();
                        right += 1;
                    }
                    (Some(e), None) => {
                        writeln!(buf, "row missing: {:?}", e).unwrap();
                        left += 1;
                    }
                    (None, None) => unreachable!("blocked by while condition"),
                }
            }
            Err(format!(
                "non-matching rows: expected:\n{:?}\ngot:\n{:?}\nDiff:\n{}",
                self.cmd.expected_rows, actual, buf
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

pub fn print_query(query: &str) {
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
                Type::JSONB => row
                    .get::<_, Option<Json<Value>>>(i)
                    .and_then(|v| serde_json::to_string(&v.0).ok()),
                _ => return Err(format!("unable to handle SQL type: {:?}", ty)),
            }
            .unwrap_or_else(|| "<null>".into()),
        )
    }
    Ok(out)
}

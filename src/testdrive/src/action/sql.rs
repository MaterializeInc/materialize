// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::ascii;
use std::error::Error;
use std::fmt::Write as _;
use std::io::{self, Write};
use std::time::Duration;

use async_trait::async_trait;
use md5::{Digest, Md5};
use tokio_postgres::error::DbError;
use tokio_postgres::row::Row;
use tokio_postgres::types::Type;

use ore::collections::CollectionExt;
use ore::retry;
use pgrepr::{Interval, Jsonb, Numeric};
use sql_parser::ast::Statement;

use crate::action::{Action, State};
use crate::parser::{FailSqlCommand, SqlCommand, SqlExpectedResult};

pub struct SqlAction {
    cmd: SqlCommand,
    stmt: Statement,
    timeout: Duration,
}

pub fn build_sql(mut cmd: SqlCommand, timeout: Duration) -> Result<SqlAction, String> {
    let stmts = sql_parser::parser::parse_statements(cmd.query.clone())
        .map_err(|e| format!("unable to parse SQL: {}: {}", cmd.query, e))?;
    if stmts.len() != 1 {
        return Err(format!("expected one statement, but got {}", stmts.len()));
    }
    if let SqlExpectedResult::Full { expected_rows, .. } = &mut cmd.expected_result {
        // TODO(benesch): one day we'll support SQL queries where order matters.
        expected_rows.sort();
    }
    Ok(SqlAction {
        cmd,
        stmt: stmts.into_element(),
        timeout,
    })
}

#[async_trait]
impl Action for SqlAction {
    async fn undo(&self, state: &mut State) -> Result<(), String> {
        match &self.stmt {
            Statement::CreateDatabase { name, .. } => {
                self.try_drop(
                    &mut state.pgclient,
                    &format!("DROP DATABASE IF EXISTS {}", name.to_string()),
                )
                .await
            }
            Statement::CreateSchema { name, .. } => {
                self.try_drop(
                    &mut state.pgclient,
                    &format!("DROP SCHEMA IF EXISTS {} CASCADE", name),
                )
                .await
            }
            Statement::CreateSource { name, .. } => {
                self.try_drop(
                    &mut state.pgclient,
                    &format!("DROP SOURCE IF EXISTS {} CASCADE", name),
                )
                .await
            }
            Statement::CreateView { name, .. } => {
                self.try_drop(
                    &mut state.pgclient,
                    &format!("DROP VIEW IF EXISTS {} CASCADE", name),
                )
                .await
            }
            Statement::CreateTable { name, .. } => {
                self.try_drop(
                    &mut state.pgclient,
                    &format!("DROP TABLE IF EXISTS {} CASCADE", name),
                )
                .await
            }
            _ => Ok(()),
        }
    }

    async fn redo(&self, state: &mut State) -> Result<(), String> {
        let query = &self.cmd.query;
        print_query(&query);

        let pgclient = &state.pgclient;
        retry::retry_for(self.timeout, |retry_state| async move {
            match self.try_redo(pgclient, &query).await {
                Ok(()) => {
                    if retry_state.i != 0 {
                        println!();
                    }
                    println!("rows match; continuing");
                    Ok(())
                }
                Err(e) => {
                    if retry_state.i == 0 {
                        print!("rows didn't match; sleeping to see if dataflow catches up");
                    }
                    if let Some(backoff) = retry_state.next_backoff {
                        print!(" {:?}", backoff);
                        io::stdout().flush().unwrap();
                    } else {
                        println!();
                    }
                    Err(e)
                }
            }
        })
        .await?;

        if let Some(data_dir) = &state.data_dir {
            match self.stmt {
                Statement::CreateDatabase { .. }
                | Statement::CreateIndex { .. }
                | Statement::CreateSchema { .. }
                | Statement::CreateSource { .. }
                | Statement::CreateTable { .. }
                | Statement::CreateView { .. }
                | Statement::DropDatabase { .. }
                | Statement::DropObjects { .. } => {
                    let disk_state = coord::dump_catalog(data_dir).map_err(|e| e.to_string())?;
                    let mem_state = reqwest::get(&format!(
                        "http://{}/internal/catalog",
                        state.materialized_addr,
                    ))
                    .await
                    .map_err(|e| e.to_string())?
                    .text()
                    .await
                    .map_err(|e| e.to_string())?;
                    if disk_state != mem_state {
                        return Err(format!(
                            "the on-disk state of the catalog does not match its in-memory state\n\
                             disk:{}\n\
                             mem:{}",
                            disk_state, mem_state
                        ));
                    }
                }
                _ => (),
            }
        }

        Ok(())
    }
}

impl SqlAction {
    async fn try_drop(
        &self,
        pgclient: &mut tokio_postgres::Client,
        query: &str,
    ) -> Result<(), String> {
        print_query(&query);
        match pgclient.query(query, &[]).await {
            Err(err) => Err(err.to_string()),
            Ok(_) => Ok(()),
        }
    }

    async fn try_redo(&self, pgclient: &tokio_postgres::Client, query: &str) -> Result<(), String> {
        let mut actual: Vec<_> = pgclient
            .query(query, &[])
            .await
            .map_err(|e| format!("query failed: {}", e))?
            .into_iter()
            .map(decode_row)
            .collect::<Result<_, _>>()?;
        actual.sort();
        match &self.cmd.expected_result {
            SqlExpectedResult::Full { expected_rows, .. } => {
                if &actual == expected_rows {
                    Ok(())
                } else {
                    let (mut left, mut right) = (0, 0);
                    let mut buf = String::new();
                    while let (Some(e), Some(a)) = (expected_rows.get(left), actual.get(right)) {
                        match e.cmp(a) {
                            std::cmp::Ordering::Less => {
                                writeln!(buf, "row missing: {:?}", e).unwrap();
                                left += 1;
                            }
                            std::cmp::Ordering::Equal => {
                                left += 1;
                                right += 1;
                            }
                            std::cmp::Ordering::Greater => {
                                writeln!(buf, "extra row: {:?}", a).unwrap();
                                right += 1;
                            }
                        }
                    }
                    while let Some(e) = expected_rows.get(left) {
                        writeln!(buf, "row missing: {:?}", e).unwrap();
                        left += 1;
                    }
                    while let Some(a) = actual.get(right) {
                        writeln!(buf, "extra row: {:?}", a).unwrap();
                        right += 1;
                    }
                    Err(format!(
                        "non-matching rows: expected:\n{:?}\ngot:\n{:?}\nDiff:\n{}",
                        expected_rows, actual, buf
                    ))
                }
            }
            SqlExpectedResult::Hashed { num_values, md5 } => {
                if &actual.len() != num_values {
                    Err(format!(
                        "wrong row count: expected:\n{:?}\ngot:\n{:?}\n",
                        actual.len(),
                        num_values,
                    ))
                } else {
                    let mut hasher = Md5::new();
                    for row in &actual {
                        for entry in row {
                            hasher.input(entry);
                        }
                    }
                    let actual = format!("{:x}", hasher.result());
                    if &actual != md5 {
                        Err(format!(
                            "wrong hash value: expected:{:?} got:{:?}",
                            md5, actual
                        ))
                    } else {
                        Ok(())
                    }
                }
            }
        }
    }
}

pub struct FailSqlAction {
    cmd: FailSqlCommand,
    timeout: Duration,
}

pub fn build_fail_sql(cmd: FailSqlCommand, timeout: Duration) -> Result<FailSqlAction, String> {
    Ok(FailSqlAction { cmd, timeout })
}

#[async_trait]
impl Action for FailSqlAction {
    async fn undo(&self, _state: &mut State) -> Result<(), String> {
        Ok(())
    }

    async fn redo(&self, state: &mut State) -> Result<(), String> {
        let query = &self.cmd.query;
        print_query(&query);

        let pgclient = &state.pgclient;
        retry::retry_for(self.timeout, |retry_state| async move {
            match self.try_redo(pgclient, &query).await {
                Ok(()) => {
                    if retry_state.i != 0 {
                        println!();
                    }
                    println!("query error matches; continuing");
                    Ok(())
                }
                Err(e) => {
                    if retry_state.i == 0 {
                        print!("query error didn't match; sleeping to see if dataflow produces error shortly");
                    }
                    if let Some(backoff) = retry_state.next_backoff {
                        print!(" {:?}", backoff);
                        io::stdout().flush().unwrap();
                    } else {
                        println!();
                    }
                    Err(e)
                }
            }
        }).await
    }
}

impl FailSqlAction {
    async fn try_redo(&self, pgclient: &tokio_postgres::Client, query: &str) -> Result<(), String> {
        match pgclient.query(query, &[]).await {
            Ok(_) => Err(format!(
                "query succeeded, but expected error '{}'",
                self.cmd.expected_error
            )),
            Err(err) => {
                let err_string = err.to_string();
                match err.source().and_then(|err| err.downcast_ref::<DbError>()) {
                    Some(err) => {
                        if err.message().contains(&self.cmd.expected_error) {
                            Ok(())
                        } else {
                            Err(format!(
                                "expected error containing '{}', but got '{}'",
                                self.cmd.expected_error, err
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
                Type::BYTEA => row.get::<_, Option<Vec<u8>>>(i).map(|x| {
                    let s = x.into_iter().map(ascii::escape_default).flatten().collect();
                    String::from_utf8(s).unwrap()
                }),
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
                Type::JSONB => row.get::<_, Option<Jsonb>>(i).map(|v| v.0.to_string()),
                _ => return Err(format!("unable to handle SQL type: {:?}", ty)),
            }
            .unwrap_or_else(|| "<null>".into()),
        )
    }
    Ok(out)
}

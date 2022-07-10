// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::ascii;
use std::error::Error;
use std::fmt::{self, Display, Formatter, Write as _};
use std::io::{self, Write};
use std::time::SystemTime;

use anyhow::{bail, Context};
use async_trait::async_trait;
use md5::{Digest, Md5};
use postgres_array::Array;
use regex::Regex;
use tokio_postgres::error::DbError;
use tokio_postgres::row::Row;
use tokio_postgres::types::{FromSql, Type};

use mz_ore::collections::CollectionExt;
use mz_ore::retry::Retry;
use mz_ore::str::StrExt;
use mz_pgrepr::{Interval, Jsonb, Numeric};
use mz_sql_parser::ast::{
    CreateClusterReplicaStatement, CreateClusterStatement, CreateConnectionStatement,
    CreateDatabaseStatement, CreateSchemaStatement, CreateSecretStatement, CreateSourceStatement,
    CreateTableStatement, CreateViewStatement, Raw, ReplicaDefinition, Statement, ViewDefinition,
};

use crate::action::{Action, ControlFlow, State};
use crate::parser::{FailSqlCommand, SqlCommand, SqlExpectedError, SqlOutput};

pub struct SqlAction {
    cmd: SqlCommand,
    stmt: Statement<Raw>,
}

pub fn build_sql(mut cmd: SqlCommand) -> Result<SqlAction, anyhow::Error> {
    let stmts = mz_sql_parser::parser::parse_statements(&cmd.query)
        .with_context(|| format!("unable to parse SQL: {}", cmd.query))?;
    if stmts.len() != 1 {
        bail!("expected one statement, but got {}", stmts.len());
    }
    if let SqlOutput::Full { expected_rows, .. } = &mut cmd.expected_output {
        // TODO(benesch): one day we'll support SQL queries where order matters.
        expected_rows.sort();
    }
    Ok(SqlAction {
        cmd,
        stmt: stmts.into_element(),
    })
}

#[async_trait]
impl Action for SqlAction {
    async fn undo(&self, state: &mut State) -> Result<(), anyhow::Error> {
        match &self.stmt {
            Statement::CreateDatabase(CreateDatabaseStatement { name, .. }) => {
                self.try_drop(
                    &mut state.pgclient,
                    &format!("DROP DATABASE IF EXISTS {}", name),
                )
                .await
            }
            Statement::CreateSchema(CreateSchemaStatement { name, .. }) => {
                self.try_drop(
                    &mut state.pgclient,
                    &format!("DROP SCHEMA IF EXISTS {} CASCADE", name),
                )
                .await
            }
            Statement::CreateSource(CreateSourceStatement { name, .. }) => {
                self.try_drop(
                    &mut state.pgclient,
                    &format!("DROP SOURCE IF EXISTS {} CASCADE", name),
                )
                .await
            }
            Statement::CreateView(CreateViewStatement {
                definition: ViewDefinition { name, .. },
                ..
            }) => {
                self.try_drop(
                    &mut state.pgclient,
                    &format!("DROP VIEW IF EXISTS {} CASCADE", name),
                )
                .await
            }
            Statement::CreateTable(CreateTableStatement { name, .. }) => {
                self.try_drop(
                    &mut state.pgclient,
                    &format!("DROP TABLE IF EXISTS {} CASCADE", name),
                )
                .await
            }
            Statement::CreateCluster(CreateClusterStatement { name, .. }) => {
                // Modifying the default cluster causes an enormous headache in
                // isolating tests from one another, so testdrive won't let you.
                assert_ne!(
                    name,
                    &mz_sql::ast::Ident::from("default"),
                    "testdrive cannot create default cluster"
                );
                self.try_drop(
                    &mut state.pgclient,
                    &format!("DROP CLUSTER IF EXISTS {} CASCADE", name),
                )
                .await
            }
            Statement::CreateClusterReplica(CreateClusterReplicaStatement {
                definition: ReplicaDefinition { name, .. },
                of_cluster,
                ..
            }) => {
                // Modifying the default cluster causes an enormous headache in
                // isolating tests from one another, so testdrive won't let you.
                assert_ne!(
                    of_cluster.to_string(),
                    "default",
                    "testdrive cannot create default cluster replicas"
                );
                self.try_drop(
                    &mut state.pgclient,
                    &format!(
                        "DROP CLUSTER REPLICA IF EXISTS {} FROM {}",
                        name, of_cluster
                    ),
                )
                .await
            }
            Statement::CreateSecret(CreateSecretStatement { name, .. }) => {
                self.try_drop(
                    &mut state.pgclient,
                    &format!("DROP SECRET IF EXISTS {} CASCADE", name),
                )
                .await
            }
            Statement::CreateConnection(CreateConnectionStatement { name, .. }) => {
                self.try_drop(
                    &mut state.pgclient,
                    &format!("DROP CONNECTION IF EXISTS {} CASCADE", name),
                )
                .await
            }
            _ => Ok(()),
        }
    }

    async fn redo(&self, state: &mut State) -> Result<ControlFlow, anyhow::Error> {
        use Statement::*;

        let query = &self.cmd.query;
        print_query(&query);

        let should_retry = match &self.stmt {
            // Do not retry FETCH statements as subsequent executions are likely
            // to return an empty result. The original result would thus be lost.
            Fetch(_) => false,
            // DDL statements should always provide the expected result on the first try
            CreateDatabase(_) | CreateSchema(_) | CreateSource(_) | CreateSink(_)
            | CreateView(_) | CreateViews(_) | CreateTable(_) | CreateIndex(_) | CreateType(_)
            | CreateRole(_) | AlterObjectRename(_) | AlterIndex(_) | Discard(_)
            | DropDatabase(_) | DropObjects(_) | SetVariable(_) | ShowDatabases(_)
            | ShowObjects(_) | ShowIndexes(_) | ShowColumns(_) | ShowCreateView(_)
            | ShowCreateSource(_) | ShowCreateTable(_) | ShowCreateSink(_) | ShowCreateIndex(_)
            | ShowVariable(_) => false,
            _ => true,
        };

        let state = &state;
        let res = match should_retry {
            true => Retry::default()
                .initial_backoff(state.initial_backoff)
                .factor(state.backoff_factor)
                .max_duration(state.timeout)
                .max_tries(state.max_tries),
            false => Retry::default().max_duration(state.timeout).max_tries(1),
        }
        .retry_async_canceling(|retry_state| async move {
            match self.try_redo(state, &query).await {
                Ok(()) => {
                    if retry_state.i != 0 {
                        println!();
                    }
                    println!(
                        "rows match; continuing at ts {}",
                        SystemTime::now()
                            .duration_since(SystemTime::UNIX_EPOCH)
                            .unwrap()
                            .as_secs_f64()
                    );
                    Ok(())
                }
                Err(e) => {
                    if retry_state.i == 0 && should_retry {
                        print!("rows didn't match; sleeping to see if dataflow catches up");
                    }
                    if let Some(backoff) = retry_state.next_backoff {
                        if !backoff.is_zero() {
                            print!(" {:.0?}", backoff);
                            io::stdout().flush().unwrap();
                        }
                    }
                    Err(e)
                }
            }
        })
        .await;
        if let Err(e) = res {
            println!();
            return Err(e);
        }

        match self.stmt {
            Statement::CreateDatabase { .. }
            | Statement::CreateIndex { .. }
            | Statement::CreateSchema { .. }
            | Statement::CreateSource { .. }
            | Statement::CreateTable { .. }
            | Statement::CreateView { .. }
            | Statement::DropDatabase { .. }
            | Statement::DropObjects { .. } => {
                let disk_state = state
                    .with_catalog_copy(|catalog| catalog.state().dump())
                    .await?;
                if let Some(disk_state) = disk_state {
                    let mem_state = reqwest::get(&format!(
                        "http://{}/api/internal/catalog",
                        state.materialize_http_addr,
                    ))
                    .await?
                    .text()
                    .await?;
                    if disk_state != mem_state {
                        bail!(
                            "the on-disk state of the catalog does not match its in-memory state\n\
                             disk:{}\n\
                             mem:{}",
                            disk_state,
                            mem_state
                        );
                    }
                }
            }
            _ => {}
        }

        Ok(ControlFlow::Continue)
    }
}

impl SqlAction {
    async fn try_drop(
        &self,
        pgclient: &mut tokio_postgres::Client,
        query: &str,
    ) -> Result<(), anyhow::Error> {
        print_query(&query);
        pgclient.query(query, &[]).await?;
        Ok(())
    }

    async fn try_redo(&self, state: &State, query: &str) -> Result<(), anyhow::Error> {
        let stmt = state
            .pgclient
            .prepare(query)
            .await
            .context("preparing query failed")?;
        let mut actual: Vec<_> = state
            .pgclient
            .query(&stmt, &[])
            .await
            .context("executing query failed")?
            .into_iter()
            .map(|row| decode_row(state, row))
            .collect::<Result<_, _>>()?;
        actual.sort();

        match &self.cmd.expected_output {
            SqlOutput::Full {
                expected_rows,
                column_names,
            } => {
                if let Some(column_names) = column_names {
                    let actual_columns: Vec<_> = stmt.columns().iter().map(|c| c.name()).collect();
                    if actual_columns.iter().ne(column_names) {
                        bail!(
                            "column name mismatch\nexpected: {:?}\nactual:   {:?}",
                            column_names,
                            actual_columns
                        );
                    }
                }
                if &actual == expected_rows {
                    Ok(())
                } else {
                    let (mut left, mut right) = (0, 0);
                    let mut buf = String::new();
                    while let (Some(e), Some(a)) = (expected_rows.get(left), actual.get(right)) {
                        match e.cmp(a) {
                            std::cmp::Ordering::Less => {
                                writeln!(buf, "- {}", TestdriveRow(e)).unwrap();
                                left += 1;
                            }
                            std::cmp::Ordering::Equal => {
                                left += 1;
                                right += 1;
                            }
                            std::cmp::Ordering::Greater => {
                                writeln!(buf, "+ {}", TestdriveRow(a)).unwrap();
                                right += 1;
                            }
                        }
                    }
                    while let Some(e) = expected_rows.get(left) {
                        writeln!(buf, "- {}", TestdriveRow(e)).unwrap();
                        left += 1;
                    }
                    while let Some(a) = actual.get(right) {
                        writeln!(buf, "+ {}", TestdriveRow(a)).unwrap();
                        right += 1;
                    }
                    bail!(
                        "non-matching rows: expected:\n{:?}\ngot:\n{:?}\nPoor diff:\n{}",
                        expected_rows,
                        actual,
                        buf
                    )
                }
            }
            SqlOutput::Hashed { num_values, md5 } => {
                if &actual.len() != num_values {
                    bail!(
                        "wrong row count: expected:\n{:?}\ngot:\n{:?}\n",
                        actual.len(),
                        num_values,
                    )
                } else {
                    let mut hasher = Md5::new();
                    for row in &actual {
                        for entry in row {
                            hasher.update(entry);
                        }
                    }
                    let actual = format!("{:x}", hasher.finalize());
                    if &actual != md5 {
                        bail!("wrong hash value: expected:{:?} got:{:?}", md5, actual)
                    } else {
                        Ok(())
                    }
                }
            }
        }
    }
}

pub struct FailSqlAction {
    query: String,
    expected_error: ErrorMatcher,
    stmt: Option<Statement<Raw>>,
}

enum ErrorMatcher {
    Contains(String),
    Exact(String),
    Regex(Regex),
    Timeout,
}

impl ErrorMatcher {
    fn is_match(&self, err: &String) -> bool {
        match self {
            ErrorMatcher::Contains(s) => err.contains(s),
            ErrorMatcher::Exact(s) => err == s,
            ErrorMatcher::Regex(r) => r.is_match(err),
            // Timeouts never match errors directly. If we are matching an error
            // message, it means the query returned a result (i.e., an error
            // result), which means the query did not time out as expected.
            ErrorMatcher::Timeout => false,
        }
    }
}

impl fmt::Display for ErrorMatcher {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            ErrorMatcher::Contains(s) => write!(f, "error containing {}", s.quoted()),
            ErrorMatcher::Exact(s) => write!(f, "exact error {}", s.quoted()),
            ErrorMatcher::Regex(s) => write!(f, "error matching regex {}", s.as_str().quoted()),
            ErrorMatcher::Timeout => f.write_str("timeout"),
        }
    }
}

pub fn build_fail_sql(cmd: FailSqlCommand) -> Result<FailSqlAction, anyhow::Error> {
    let stmts = mz_sql_parser::parser::parse_statements(&cmd.query)
        .map_err(|e| format!("unable to parse SQL: {}: {}", cmd.query, e));

    // Allow for statements that could not be parsed.
    // This way such statements can be used for negative testing in .td files
    let stmt = match stmts {
        Ok(s) => {
            if s.len() != 1 {
                bail!("expected one statement, but got {}", s.len());
            }
            Some(s.into_element())
        }
        Err(_) => None,
    };

    let expected_error = match cmd.expected_error {
        SqlExpectedError::Contains(s) => ErrorMatcher::Contains(s),
        SqlExpectedError::Exact(s) => ErrorMatcher::Exact(s),
        SqlExpectedError::Regex(s) => ErrorMatcher::Regex(s.parse()?),
        SqlExpectedError::Timeout => ErrorMatcher::Timeout,
    };

    Ok(FailSqlAction {
        query: cmd.query,
        expected_error,
        stmt,
    })
}

#[async_trait]
impl Action for FailSqlAction {
    async fn undo(&self, _state: &mut State) -> Result<(), anyhow::Error> {
        Ok(())
    }

    async fn redo(&self, state: &mut State) -> Result<ControlFlow, anyhow::Error> {
        use Statement::{Commit, Rollback};

        let query = &self.query;
        print_query(&query);

        let should_retry = match &self.stmt {
            // Do not retry statements that could not be parsed
            None => false,
            // Do not retry COMMIT and ROLLBACK. Once the transaction has errored out and has
            // been aborted, retrying COMMIT or ROLLBACK will actually start succeeding, which
            // causes testdrive to emit a confusing "query succeded but expected error" message.
            Some(Commit(_)) | Some(Rollback(_)) => false,
            Some(_) => true,
        };

        let state = &state;
        let res = match should_retry {
            true => Retry::default()
                .initial_backoff(state.initial_backoff)
                .factor(state.backoff_factor)
                .max_duration(state.timeout)
                .max_tries(state.max_tries),
            false => Retry::default().max_duration(state.timeout).max_tries(1),
        }.retry_async_canceling(|retry_state| async move {
            match self.try_redo(state, &query).await {
                Ok(()) => {
                    if retry_state.i != 0 {
                        println!();
                    }
                    println!("query error matches; continuing");
                    Ok(())
                }
                Err(e) => {
                    if retry_state.i == 0 && should_retry {
                        print!("query error didn't match; sleeping to see if dataflow produces error shortly");
                    }
                    if let Some(backoff) = retry_state.next_backoff {
                        print!(" {:.0?}", backoff);
                        io::stdout().flush().unwrap();
                    } else {
                        println!();
                    }
                    Err(e)
                }
            }
        }).await;

        // If a timeout was expected, check whether the retry operation timed
        // out, which indicates that the test passed.
        if let ErrorMatcher::Timeout = self.expected_error {
            if let Err(e) = &res {
                if e.is::<tokio::time::error::Elapsed>() {
                    println!("query timed out as expected");
                    return Ok(ControlFlow::Continue);
                }
            }
        }

        // Otherwise, return the error if any. Note that this is the error
        // returned by the retry operation (e.g., "expected timeout, but query
        // succeeded"), *not* an error returned from Materialize itself.
        res?;
        Ok(ControlFlow::Continue)
    }
}

impl FailSqlAction {
    async fn try_redo(&self, state: &State, query: &str) -> Result<(), anyhow::Error> {
        match state.pgclient.query(query, &[]).await {
            Ok(_) => bail!("query succeeded, but expected {}", self.expected_error),
            Err(err) => match err.source().and_then(|err| err.downcast_ref::<DbError>()) {
                Some(err) => {
                    let mut err_string = err.message().to_string();
                    if let Some(regex) = &state.regex {
                        err_string = regex
                            .replace_all(&err_string, state.regex_replacement.as_str())
                            .to_string();
                    }
                    if !self.expected_error.is_match(&err_string) {
                        bail!(
                            "expected {}, got {}",
                            self.expected_error,
                            err_string.quoted()
                        );
                    }
                    Ok(())
                }
                None => Err(err.into()),
            },
        }
    }
}

pub fn print_query(query: &str) {
    println!("> {}", query);
}

pub fn decode_row(state: &State, row: Row) -> Result<Vec<String>, anyhow::Error> {
    enum ArrayElement<T> {
        Null,
        NonNull(T),
    }

    impl<T> fmt::Display for ArrayElement<T>
    where
        T: fmt::Display,
    {
        fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
            match self {
                ArrayElement::Null => f.write_str("NULL"),
                ArrayElement::NonNull(t) => t.fmt(f),
            }
        }
    }

    impl<'a, T> FromSql<'a> for ArrayElement<T>
    where
        T: FromSql<'a>,
    {
        fn from_sql(
            ty: &Type,
            raw: &'a [u8],
        ) -> Result<ArrayElement<T>, Box<dyn Error + Sync + Send>> {
            T::from_sql(ty, raw).map(ArrayElement::NonNull)
        }

        fn from_sql_null(_: &Type) -> Result<ArrayElement<T>, Box<dyn Error + Sync + Send>> {
            Ok(ArrayElement::Null)
        }

        fn accepts(ty: &Type) -> bool {
            T::accepts(ty)
        }
    }

    let mut out = vec![];
    for (i, col) in row.columns().iter().enumerate() {
        let ty = col.type_();
        let mut value = match *ty {
            Type::BOOL => row.get::<_, Option<bool>>(i).map(|x| x.to_string()),
            Type::BPCHAR | Type::TEXT | Type::VARCHAR => row.get::<_, Option<String>>(i),
            Type::TEXT_ARRAY => row
                .get::<_, Option<Array<ArrayElement<String>>>>(i)
                .map(|a| a.to_string()),
            Type::BYTEA => row.get::<_, Option<Vec<u8>>>(i).map(|x| {
                let s = x.into_iter().map(ascii::escape_default).flatten().collect();
                String::from_utf8(s).unwrap()
            }),
            Type::CHAR => row.get::<_, Option<i8>>(i).map(|x| x.to_string()),
            Type::INT2 => row.get::<_, Option<i16>>(i).map(|x| x.to_string()),
            Type::INT4 => row.get::<_, Option<i32>>(i).map(|x| x.to_string()),
            Type::INT8 => row.get::<_, Option<i64>>(i).map(|x| x.to_string()),
            Type::OID => row.get::<_, Option<u32>>(i).map(|x| x.to_string()),
            Type::NUMERIC => row
                .get::<_, Option<Numeric>>(i)
                .map(|x| x.0 .0.to_standard_notation_string()),
            Type::FLOAT4 => row.get::<_, Option<f32>>(i).map(|x| x.to_string()),
            Type::FLOAT8 => row.get::<_, Option<f64>>(i).map(|x| x.to_string()),
            Type::TIMESTAMP => row
                .get::<_, Option<chrono::NaiveDateTime>>(i)
                .map(|x| x.to_string()),
            Type::TIMESTAMPTZ => row
                .get::<_, Option<chrono::DateTime<chrono::Utc>>>(i)
                .map(|x| x.to_string()),
            Type::DATE => row
                .get::<_, Option<chrono::NaiveDate>>(i)
                .map(|x| x.to_string()),
            Type::TIME => row
                .get::<_, Option<chrono::NaiveTime>>(i)
                .map(|x| x.to_string()),
            Type::INTERVAL => row.get::<_, Option<Interval>>(i).map(|x| x.to_string()),
            Type::JSONB => row.get::<_, Option<Jsonb>>(i).map(|v| v.0.to_string()),
            Type::UUID => row.get::<_, Option<uuid::Uuid>>(i).map(|v| v.to_string()),
            _ => bail!("unsupported SQL type in testdrive: {:?}", ty),
        }
        .unwrap_or_else(|| "<null>".into());

        if let Some(regex) = &state.regex {
            value = regex
                .replace_all(&value, state.regex_replacement.as_str())
                .to_string();
        }

        out.push(value);
    }
    Ok(out)
}

struct TestdriveRow<'a>(&'a Vec<String>);

impl Display for TestdriveRow<'_> {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        let mut cols = Vec::<String>::new();

        for col_str in &self.0[0..self.0.len()] {
            if col_str.contains(' ') || col_str.contains('"') {
                cols.push(format!("{:?}", col_str));
            } else {
                cols.push(col_str.to_string());
            }
        }

        write!(f, "{}", cols.join(" "))
    }
}

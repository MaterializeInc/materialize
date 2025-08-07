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
use std::time::SystemTime;

use anyhow::{Context, bail};
use md5::{Digest, Md5};
use mz_ore::collections::CollectionExt;
use mz_ore::retry::Retry;
use mz_ore::str::StrExt;
use mz_pgrepr::{Interval, Jsonb, Numeric, UInt2, UInt4, UInt8};
use mz_repr::adt::range::Range;
use mz_sql_parser::ast::{Raw, Statement};
use postgres_array::Array;
use regex::Regex;
use tokio_postgres::error::DbError;
use tokio_postgres::row::Row;
use tokio_postgres::types::{FromSql, Type};

use crate::action::{ControlFlow, Rewrite, State};
use crate::parser::{FailSqlCommand, SqlCommand, SqlExpectedError, SqlOutput};

pub async fn run_sql(mut cmd: SqlCommand, state: &mut State) -> Result<ControlFlow, anyhow::Error> {
    use Statement::*;

    state.rewrite_pos_start = cmd.expected_start;
    state.rewrite_pos_end = cmd.expected_end;

    let stmts = mz_sql_parser::parser::parse_statements(&cmd.query)
        .with_context(|| format!("unable to parse SQL: {}", cmd.query))?;
    if stmts.len() != 1 {
        bail!("expected one statement, but got {}", stmts.len());
    }
    let stmt = stmts.into_element().ast;
    if let SqlOutput::Full { expected_rows, .. } = &mut cmd.expected_output {
        // TODO(benesch): one day we'll support SQL queries where order matters.
        expected_rows.sort();
    }

    let should_retry = match &stmt {
        // Do not retry FETCH statements as subsequent executions are likely
        // to return an empty result. The original result would thus be lost.
        Fetch(_) => false,
        // EXPLAIN ... PLAN statements should always provide the expected result
        // on the first try
        ExplainPlan(_) => false,
        // DDL statements should always provide the expected result on the first try
        CreateConnection(_)
        | CreateCluster(_)
        | CreateClusterReplica(_)
        | CreateDatabase(_)
        | CreateSchema(_)
        | CreateSource(_)
        | CreateWebhookSource(_)
        | CreateSink(_)
        | CreateMaterializedView(_)
        | CreateView(_)
        | CreateTable(_)
        | CreateTableFromSource(_)
        | CreateIndex(_)
        | CreateType(_)
        | CreateRole(_)
        | AlterObjectRename(_)
        | AlterIndex(_)
        | AlterSink(_)
        | Discard(_)
        | DropObjects(_)
        | SetVariable(_) => false,
        _ => true,
    };

    let query = &cmd.query;
    print_query(query, Some(&stmt));
    let expected_output = &cmd.expected_output;
    state.error_line_count = 0;
    state.error_string = "".to_string();
    let (state, res) = match should_retry {
        true => Retry::default()
            .initial_backoff(state.initial_backoff)
            .factor(state.backoff_factor)
            .max_duration(state.timeout)
            .max_tries(state.max_tries),
        false => Retry::default().max_duration(state.timeout).max_tries(1),
    }
    .retry_async_with_state(state, |retry_state, state| async move {
        let should_continue = retry_state.i + 1 < state.max_tries && should_retry;
        let start = SystemTime::now();
        match try_run_sql(state, query, expected_output, should_continue).await {
            Ok(()) => {
                let now = SystemTime::now();
                let epoch = SystemTime::UNIX_EPOCH;
                let ts = now.duration_since(epoch).unwrap().as_secs_f64();
                let delay = now.duration_since(start).unwrap().as_secs_f64();
                println!("rows match; continuing at ts {ts}, took {delay}s");
                (state, Ok(()))
            }
            Err(e) => {
                if let Some(backoff) = retry_state.next_backoff {
                    if !backoff.is_zero() {
                        let error_string = format!("{:?}", e);
                        if error_string != state.error_string {
                            // Remove old status lines so as not to spam the output
                            for _ in 0..state.error_line_count {
                                print!("\x1B[1A\x1B[2K");
                            }
                            state.error_line_count = 0;
                            state.error_string = error_string;
                            state.error_line_count = state.error_string.lines().count() + 1;
                            // Contains a newline already, so don't print an additional one
                            print!("{}", state.error_string);
                            println!(
                                "rows didn't match; sleeping to see if dataflow catches up 🕑 {:.0?}",
                                retry_state.next_backoff.unwrap_or_default()
                            );
                        }
                    }
                } else {
                    for _ in 0..state.error_line_count {
                        print!("\x1B[1A\x1B[2K");
                    }
                    state.error_line_count = 0;
                }
                (state, Err(e))
            }
        }
    })
    .await;
    if let Err(e) = res {
        return Err(e);
    }
    if state.consistency_checks == super::consistency::Level::Statement {
        super::consistency::run_consistency_checks(state).await?;
    }

    Ok(ControlFlow::Continue)
}

fn rewrite_result(
    state: &mut State,
    columns: Vec<&str>,
    content: Vec<Vec<String>>,
) -> Result<(), anyhow::Error> {
    let mut buf = String::new();
    writeln!(buf, "{}", columns.join(" "))?;
    writeln!(buf, "----")?;
    for row in content {
        let mut formatted_row = Vec::<String>::new();
        for value in row {
            if value.is_empty() || value.contains(|x: char| char::is_ascii_whitespace(&x)) {
                formatted_row.push("\"".to_owned() + &value + "\"");
            } else {
                formatted_row.push(value);
            }
        }
        writeln!(buf, "{}", formatted_row.join(" "))?;
    }
    state.rewrites.push(Rewrite {
        content: buf,
        start: state.rewrite_pos_start,
        end: state.rewrite_pos_end,
    });

    Ok(())
}

async fn try_run_sql(
    state: &mut State,
    query: &str,
    expected_output: &SqlOutput,
    should_retry: bool,
) -> Result<(), anyhow::Error> {
    let stmt = state
        .materialize
        .pgclient
        .prepare(query)
        .await
        .context("preparing query failed")?;

    let query_with_timeout = tokio::time::timeout(
        state.timeout.clone(),
        state.materialize.pgclient.query(&stmt, &[]),
    )
    .await;

    if query_with_timeout.is_err() {
        bail!("query timed out\n")
    }

    let rows: Vec<_> = query_with_timeout
        .unwrap()
        .context("executing query failed")?
        .into_iter()
        .map(|row| decode_row(state, row))
        .collect::<Result<_, _>>()?;

    let (mut actual, raw_actual): (Vec<_>, Vec<_>) = rows.into_iter().unzip();

    let raw_actual: Option<Vec<_>> = if raw_actual.iter().any(|r| r.is_some()) {
        // TODO(guswynn): Note we don't sort the raw rows, because
        // there is no easy way of ensuring they sort the same way as actual.
        Some(
            actual
                .iter()
                .zip(raw_actual.into_iter())
                .map(|(actual, unreplaced)| match unreplaced {
                    Some(raw_row) => raw_row,
                    None => actual.clone(),
                })
                .collect(),
        )
    } else {
        None
    };

    actual.sort();
    let actual_columns: Vec<_> = stmt.columns().iter().map(|c| c.name()).collect();

    match expected_output {
        SqlOutput::Full {
            expected_rows,
            column_names,
        } => {
            if let Some(column_names) = column_names {
                if actual_columns.iter().ne(column_names) {
                    if state.rewrite_results && !should_retry {
                        rewrite_result(state, actual_columns, actual)?;
                        return Ok(());
                    } else {
                        bail!(
                            "column name mismatch\nexpected: {:?}\nactual:   {:?}\n",
                            column_names,
                            actual_columns
                        );
                    }
                }
            }
            if &actual == expected_rows {
                Ok(())
            } else if state.rewrite_results && !should_retry {
                rewrite_result(state, actual_columns, actual)?;
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
                if state.rewrite_results && !should_retry {
                    rewrite_result(state, actual_columns, actual)?;
                    Ok(())
                } else if let Some(raw_actual) = raw_actual {
                    bail!(
                        "non-matching rows: expected:\n{:?}\ngot:\n{:?}\ngot raw rows:\n{:?}\nPoor diff:\n{}",
                        expected_rows,
                        actual,
                        raw_actual,
                        buf,
                    )
                } else {
                    bail!(
                        "non-matching rows: expected:\n{:?}\ngot:\n{:?}\nPoor diff:\n{}",
                        expected_rows,
                        actual,
                        buf
                    )
                }
            }
        }
        SqlOutput::Hashed { num_values, md5 } => {
            if &actual.len() != num_values {
                bail!(
                    "wrong row count: expected:\n{:?}\ngot:\n{:?}\n",
                    num_values,
                    actual.len(),
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

#[derive(Clone)]
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

impl ErrorMatcher {
    fn fmt_with_type(&self, type_: &str) -> String {
        match self {
            ErrorMatcher::Contains(s) => format!("{} containing {}", type_, s.quoted()),
            ErrorMatcher::Exact(s) => format!("exact {} {}", type_, s.quoted()),
            ErrorMatcher::Regex(s) => format!("{} matching regex {}", type_, s.as_str().quoted()),
            ErrorMatcher::Timeout => "timeout".to_string(),
        }
    }
}

pub async fn run_fail_sql(
    cmd: FailSqlCommand,
    state: &mut State,
) -> Result<ControlFlow, anyhow::Error> {
    use Statement::{AlterSink, Commit, CreateConnection, Fetch, Rollback};

    let stmts = mz_sql_parser::parser::parse_statements(&cmd.query)
        .map_err(|e| format!("unable to parse SQL: {}: {}", cmd.query, e));

    // Allow for statements that could not be parsed.
    // This way such statements can be used for negative testing in .td files
    let stmt = match stmts {
        Ok(s) => {
            if s.len() != 1 {
                bail!("expected one statement, but got {}", s.len());
            }
            Some(s.into_element().ast)
        }
        Err(_) => None,
    };

    let expected_error = match cmd.expected_error {
        SqlExpectedError::Contains(s) => ErrorMatcher::Contains(s),
        SqlExpectedError::Exact(s) => ErrorMatcher::Exact(s),
        SqlExpectedError::Regex(s) => ErrorMatcher::Regex(s.parse()?),
        SqlExpectedError::Timeout => ErrorMatcher::Timeout,
    };
    let expected_detail = cmd.expected_detail.map(ErrorMatcher::Contains);
    let expected_hint = cmd.expected_hint.map(ErrorMatcher::Contains);

    let query = &cmd.query;
    print_query(query, stmt.as_ref());

    let should_retry = match &stmt {
        // Do not retry statements that could not be parsed
        None => false,
        // Do not retry COMMIT and ROLLBACK. Once the transaction has errored out and has
        // been aborted, retrying COMMIT or ROLLBACK will actually start succeeding, which
        // causes testdrive to emit a confusing "query succeded but expected error" message.
        Some(Commit(_)) | Some(Rollback(_)) => false,
        // FETCH should not be retried because it consumes data on each response.
        Some(Fetch(_)) => false,
        Some(AlterSink(_)) => false,
        Some(CreateConnection(_)) => false,
        Some(_) => true,
    };

    state.error_line_count = 0;
    state.error_string = "".to_string();
    let res = match should_retry {
        true => Retry::default()
            .initial_backoff(state.initial_backoff)
            .factor(state.backoff_factor)
            .max_duration(state.timeout)
            .max_tries(state.max_tries),
        false => Retry::default().max_duration(state.timeout).max_tries(1),
    }
    .retry_async_with_state_canceling(state, |retry_state, state| {
        let expected_error = expected_error.clone();
        let expected_detail = expected_detail.clone();
        let expected_hint = expected_hint.clone();
        async move {
            match try_run_fail_sql(
                state,
                query,
                &expected_error,
                expected_detail.as_ref(),
                expected_hint.as_ref(),
            )
            .await
            {
                Ok(()) => {
                    println!("query error matches; continuing");
                    (state, Ok(()))
                }
                Err(e) => {
                    if let Some(backoff) = retry_state.next_backoff {
                        if !backoff.is_zero() {
                            let error_string = format!("{:?}", e);
                            if error_string != state.error_string {
                                // Remove old status lines so as not to spam the output
                                for _ in 0..state.error_line_count {
                                    print!("\x1B[1A\x1B[2K");
                                }
                                state.error_line_count = 0;
                                state.error_string = error_string;
                                state.error_line_count = state.error_string.lines().count() + 1;
                                println!("{}", state.error_string);
                                println!("query error didn't match; sleeping to see if dataflow produces error shortly 🕑 {:.0?}", retry_state.next_backoff.unwrap_or_default());
                            }
                        }
                    } else {
                        for _ in 0..state.error_line_count {
                            print!("\x1B[1A\x1B[2K");
                        }
                        state.error_line_count = 0;
                    }
                    (state, Err(e))
                }
            }
        }})
    .await;

    // If a timeout was expected, check whether the retry operation timed
    // out, which indicates that the test passed.
    if let ErrorMatcher::Timeout = expected_error {
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

async fn try_run_fail_sql(
    state: &State,
    query: &str,
    expected_error: &ErrorMatcher,
    expected_detail: Option<&ErrorMatcher>,
    expected_hint: Option<&ErrorMatcher>,
) -> Result<(), anyhow::Error> {
    match state.materialize.pgclient.query(query, &[]).await {
        Ok(_) => bail!("query succeeded, but expected {}", expected_error),
        Err(err) => match err.source().and_then(|err| err.downcast_ref::<DbError>()) {
            Some(err) => {
                let mut err_string = err.message().to_string();
                if let Some(regex) = &state.regex {
                    err_string = regex
                        .replace_all(&err_string, state.regex_replacement.as_str())
                        .to_string();
                }
                if !expected_error.is_match(&err_string) {
                    bail!("expected {}, got {}", expected_error, err_string.quoted());
                }

                let check_additional =
                    |extra: Option<&str>, matcher: Option<&ErrorMatcher>, type_| {
                        let extra = extra.map(|s| s.to_string());
                        match (extra, matcher) {
                            (Some(extra), Some(expected)) => {
                                if !expected.is_match(&extra) {
                                    bail!(
                                        "expected {}, got {}",
                                        expected.fmt_with_type(type_),
                                        extra.quoted()
                                    );
                                }
                            }
                            (None, Some(expected)) => {
                                bail!("expected {}, but found none", expected.fmt_with_type(type_));
                            }
                            _ => {}
                        }
                        Ok(())
                    };

                check_additional(err.detail(), expected_detail, "DETAIL")?;
                check_additional(err.hint(), expected_hint, "HINT")?;

                Ok(())
            }
            None => Err(err.into()),
        },
    }
}

pub fn print_query(query: &str, stmt: Option<&Statement<Raw>>) {
    use Statement::*;
    if let Some(CreateSecret(_)) = stmt {
        println!(
            "> CREATE SECRET [query truncated on purpose so as to not reveal the secret in the log]"
        );
    } else {
        println!("> {}", query)
    }
}

// Returns the row after regex replacments, and the before, if its different
pub fn decode_row(
    state: &State,
    row: Row,
) -> Result<(Vec<String>, Option<Vec<String>>), anyhow::Error> {
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

    /// This lets us:
    /// - Continue using the default method of printing array elements while
    /// preserving SQL-looking output w/ `dec::to_standard_notation_string`.
    /// - Avoid upstreaming a complicated change to `rust-postgres-array`.
    struct NumericStandardNotation(Numeric);

    impl fmt::Display for NumericStandardNotation {
        fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
            write!(f, "{}", self.0.0.0.to_standard_notation_string())
        }
    }

    impl<'a> FromSql<'a> for NumericStandardNotation {
        fn from_sql(
            ty: &Type,
            raw: &'a [u8],
        ) -> Result<NumericStandardNotation, Box<dyn Error + Sync + Send>> {
            Ok(NumericStandardNotation(Numeric::from_sql(ty, raw)?))
        }

        fn from_sql_null(
            ty: &Type,
        ) -> Result<NumericStandardNotation, Box<dyn Error + Sync + Send>> {
            Ok(NumericStandardNotation(Numeric::from_sql_null(ty)?))
        }

        fn accepts(ty: &Type) -> bool {
            Numeric::accepts(ty)
        }
    }

    let mut out = vec![];
    let mut raw_out = vec![];
    for (i, col) in row.columns().iter().enumerate() {
        let ty = col.type_();
        let mut value: String = match *ty {
            Type::ACLITEM => row.get::<_, Option<AclItem>>(i).map(|x| x.0),
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
                .get::<_, Option<NumericStandardNotation>>(i)
                .map(|x| x.to_string()),
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
            Type::BOOL_ARRAY => row
                .get::<_, Option<Array<ArrayElement<bool>>>>(i)
                .map(|a| a.to_string()),
            Type::INT2_ARRAY => row
                .get::<_, Option<Array<ArrayElement<i16>>>>(i)
                .map(|a| a.to_string()),
            Type::INT4_ARRAY => row
                .get::<_, Option<Array<ArrayElement<i32>>>>(i)
                .map(|a| a.to_string()),
            Type::INT8_ARRAY => row
                .get::<_, Option<Array<ArrayElement<i64>>>>(i)
                .map(|a| a.to_string()),
            Type::OID_ARRAY => row
                .get::<_, Option<Array<ArrayElement<u32>>>>(i)
                .map(|x| x.to_string()),
            Type::NUMERIC_ARRAY => row
                .get::<_, Option<Array<ArrayElement<NumericStandardNotation>>>>(i)
                .map(|x| x.to_string()),
            Type::FLOAT4_ARRAY => row
                .get::<_, Option<Array<ArrayElement<f32>>>>(i)
                .map(|x| x.to_string()),
            Type::FLOAT8_ARRAY => row
                .get::<_, Option<Array<ArrayElement<f64>>>>(i)
                .map(|x| x.to_string()),
            Type::TIMESTAMP_ARRAY => row
                .get::<_, Option<Array<ArrayElement<chrono::NaiveDateTime>>>>(i)
                .map(|x| x.to_string()),
            Type::TIMESTAMPTZ_ARRAY => row
                .get::<_, Option<Array<ArrayElement<chrono::DateTime<chrono::Utc>>>>>(i)
                .map(|x| x.to_string()),
            Type::DATE_ARRAY => row
                .get::<_, Option<Array<ArrayElement<chrono::NaiveDate>>>>(i)
                .map(|x| x.to_string()),
            Type::TIME_ARRAY => row
                .get::<_, Option<Array<ArrayElement<chrono::NaiveTime>>>>(i)
                .map(|x| x.to_string()),
            Type::INTERVAL_ARRAY => row
                .get::<_, Option<Array<ArrayElement<Interval>>>>(i)
                .map(|x| x.to_string()),
            Type::JSONB_ARRAY => row
                .get::<_, Option<Array<ArrayElement<Jsonb>>>>(i)
                .map(|v| v.to_string()),
            Type::UUID_ARRAY => row
                .get::<_, Option<Array<ArrayElement<uuid::Uuid>>>>(i)
                .map(|v| v.to_string()),
            Type::INT4_RANGE => row.get::<_, Option<Range<i32>>>(i).map(|v| v.to_string()),
            Type::INT4_RANGE_ARRAY => row
                .get::<_, Option<Array<ArrayElement<Range<i32>>>>>(i)
                .map(|v| v.to_string()),
            Type::INT8_RANGE => row.get::<_, Option<Range<i64>>>(i).map(|v| v.to_string()),
            Type::INT8_RANGE_ARRAY => row
                .get::<_, Option<Array<ArrayElement<Range<i64>>>>>(i)
                .map(|v| v.to_string()),
            Type::NUM_RANGE => row
                .get::<_, Option<Range<NumericStandardNotation>>>(i)
                .map(|v| v.to_string()),
            Type::NUM_RANGE_ARRAY => row
                .get::<_, Option<Array<ArrayElement<Range<NumericStandardNotation>>>>>(i)
                .map(|v| v.to_string()),
            Type::DATE_RANGE => row
                .get::<_, Option<Range<chrono::NaiveDate>>>(i)
                .map(|v| v.to_string()),
            Type::DATE_RANGE_ARRAY => row
                .get::<_, Option<Array<ArrayElement<Range<chrono::NaiveDate>>>>>(i)
                .map(|v| v.to_string()),
            Type::TS_RANGE => row
                .get::<_, Option<Range<chrono::NaiveDateTime>>>(i)
                .map(|v| v.to_string()),
            Type::TS_RANGE_ARRAY => row
                .get::<_, Option<Array<ArrayElement<Range<chrono::NaiveDateTime>>>>>(i)
                .map(|v| v.to_string()),
            Type::TSTZ_RANGE => row
                .get::<_, Option<Range<chrono::DateTime<chrono::Utc>>>>(i)
                .map(|v| v.to_string()),
            Type::TSTZ_RANGE_ARRAY => row
                .get::<_, Option<Array<ArrayElement<Range<chrono::DateTime<chrono::Utc>>>>>>(i)
                .map(|v| v.to_string()),
            _ => match ty.oid() {
                mz_pgrepr::oid::TYPE_UINT2_OID => {
                    row.get::<_, Option<UInt2>>(i).map(|x| x.0.to_string())
                }
                mz_pgrepr::oid::TYPE_UINT4_OID => {
                    row.get::<_, Option<UInt4>>(i).map(|x| x.0.to_string())
                }
                mz_pgrepr::oid::TYPE_UINT8_OID => {
                    row.get::<_, Option<UInt8>>(i).map(|x| x.0.to_string())
                }
                mz_pgrepr::oid::TYPE_MZ_TIMESTAMP_OID => {
                    row.get::<_, Option<MzTimestamp>>(i).map(|x| x.0)
                }
                _ => bail!("unsupported SQL type in testdrive: {:?}", ty),
            },
        }
        .unwrap_or_else(|| "<null>".into());

        raw_out.push(value.clone());
        if let Some(regex) = &state.regex {
            value = regex
                .replace_all(&value, state.regex_replacement.as_str())
                .to_string();
        }

        out.push(value);
    }
    let raw_out = if out != raw_out { Some(raw_out) } else { None };
    Ok((out, raw_out))
}

struct MzTimestamp(String);

impl<'a> FromSql<'a> for MzTimestamp {
    fn from_sql(_: &Type, raw: &'a [u8]) -> Result<MzTimestamp, Box<dyn Error + Sync + Send>> {
        Ok(MzTimestamp(std::str::from_utf8(raw)?.to_string()))
    }

    fn accepts(ty: &Type) -> bool {
        ty.oid() == mz_pgrepr::oid::TYPE_MZ_TIMESTAMP_OID
    }
}

struct MzAclItem(#[allow(dead_code)] String);

impl<'a> FromSql<'a> for MzAclItem {
    fn from_sql(_ty: &Type, raw: &'a [u8]) -> Result<Self, Box<dyn Error + Sync + Send>> {
        Ok(MzAclItem(std::str::from_utf8(raw)?.to_string()))
    }

    fn accepts(ty: &Type) -> bool {
        ty.oid() == mz_pgrepr::oid::TYPE_MZ_ACL_ITEM_OID
    }
}

struct AclItem(String);

impl<'a> FromSql<'a> for AclItem {
    fn from_sql(_ty: &Type, raw: &'a [u8]) -> Result<Self, Box<dyn Error + Sync + Send>> {
        Ok(AclItem(std::str::from_utf8(raw)?.to_string()))
    }

    fn accepts(ty: &Type) -> bool {
        ty.oid() == 1033
    }
}

struct TestdriveRow<'a>(&'a Vec<String>);

impl Display for TestdriveRow<'_> {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        let mut cols = Vec::<String>::new();

        for col_str in &self.0[0..self.0.len()] {
            if col_str.contains(' ') || col_str.contains('"') || col_str.is_empty() {
                cols.push(format!("{:?}", col_str));
            } else {
                cols.push(col_str.to_string());
            }
        }

        write!(f, "{}", cols.join(" "))
    }
}

// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! The Materialize-specific runner for sqllogictest.
//!
//! slt tests expect a serialized execution of sql statements and queries.
//! To get the same results in materialize we track current_timestamp and increment it whenever we execute a statement.
//!
//! The high-level workflow is:
//!   for each record in the test file:
//!     if record is a sql statement:
//!       run sql in postgres, observe changes and copy them to materialize using LocalInput::Updates(..)
//!       advance current_timestamp
//!       promise to never send updates for times < current_timestamp using LocalInput::Watermark(..)
//!       compare to expected results
//!       if wrong, bail out and stop processing this file
//!     if record is a sql query:
//!       peek query at current_timestamp
//!       compare to expected results
//!       if wrong, record the error

use std::error::Error;
use std::fmt;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::ops;
use std::path::Path;
use std::str;
use std::time::Duration;

use anyhow::{anyhow, bail};
use chrono::{DateTime, NaiveDate, NaiveDateTime, NaiveTime, Utc};
use lazy_static::lazy_static;
use md5::{Digest, Md5};
use postgres_protocol::types;
use regex::Regex;
use tokio_postgres::types::FromSql;
use tokio_postgres::types::Type as PgType;
use tokio_postgres::{connect, Client, NoTls, Row};

use materialized::{serve, Config, Server};
use pgrepr::{Interval, Numeric};
use repr::ColumnName;

use crate::ast::{Location, Mode, Output, QueryOutput, Record, Sort, Type};
use crate::util;

// TODO: check that these are all needed still
#[derive(Debug)]
pub enum Outcome<'a> {
    Unsupported {
        error: anyhow::Error,
        location: Location,
    },
    PlanFailure {
        error: anyhow::Error,
        location: Location,
    },
    UnexpectedPlanSuccess {
        expected_error: &'a str,
        location: Location,
    },
    WrongNumberOfRowsInserted {
        expected_count: u64,
        actual_count: u64,
        location: Location,
    },
    WrongColumnCount {
        expected_count: usize,
        actual_count: usize,
        location: Location,
    },
    WrongColumnNames {
        expected_column_names: &'a Vec<ColumnName>,
        actual_column_names: Vec<ColumnName>,
        location: Location,
    },
    OutputFailure {
        expected_output: &'a Output,
        actual_raw_output: Vec<Row>,
        actual_output: Output,
        location: Location,
    },
    Bail {
        cause: Box<Outcome<'a>>,
        location: Location,
    },
    Success,
    FormatFailure {
        error: anyhow::Error,
        location: Location,
    },
}

const NUM_OUTCOMES: usize = 10;
const SUCCESS_OUTCOME: usize = NUM_OUTCOMES - 1;

impl<'a> Outcome<'a> {
    fn code(&self) -> usize {
        match self {
            Outcome::Unsupported { .. } => 0,
            Outcome::PlanFailure { .. } => 1,
            Outcome::UnexpectedPlanSuccess { .. } => 2,
            Outcome::WrongNumberOfRowsInserted { .. } => 3,
            Outcome::WrongColumnCount { .. } => 4,
            Outcome::WrongColumnNames { .. } => 5,
            Outcome::OutputFailure { .. } => 6,
            Outcome::FormatFailure { .. } => 7,
            Outcome::Bail { .. } => 8,
            Outcome::Success => 9,
        }
    }

    fn success(&self) -> bool {
        matches!(self, Outcome::Success)
    }
}

impl fmt::Display for Outcome<'_> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use Outcome::*;
        const INDENT: &str = "\n        ";
        match self {
            Unsupported { error, location } => write!(f, "Unsupported:{}:\n{:#}", location, error),
            PlanFailure { error, location } => write!(f, "PlanFailure:{}:\n{:#}", location, error),
            UnexpectedPlanSuccess {
                expected_error,
                location,
            } => write!(
                f,
                "UnexpectedPlanSuccess:{} expected error: {}",
                location, expected_error
            ),
            WrongNumberOfRowsInserted {
                expected_count,
                actual_count,
                location,
            } => write!(
                f,
                "WrongNumberOfRowsInserted:{}{}expected: {}{}actually: {}",
                location, INDENT, expected_count, INDENT, actual_count
            ),
            WrongColumnCount {
                expected_count,
                actual_count,
                location,
            } => write!(
                f,
                "WrongColumnCount:{}{}expected: {}{}actually: {}",
                location, INDENT, expected_count, INDENT, actual_count
            ),
            WrongColumnNames {
                expected_column_names,
                actual_column_names,
                location,
            } => write!(
                f,
                "Wrong Column Names:{}:{}expected column names: {}{}inferred column names: {}",
                location,
                INDENT,
                expected_column_names
                    .iter()
                    .map(|n| n.to_string())
                    .collect::<Vec<_>>()
                    .join(" "),
                INDENT,
                actual_column_names
                    .iter()
                    .map(|n| n.to_string())
                    .collect::<Vec<_>>()
                    .join(" ")
            ),
            OutputFailure {
                expected_output,
                actual_raw_output,
                actual_output,
                location,
            } => write!(
                f,
                "OutputFailure:{}{}expected: {:?}{}actually: {:?}{}actual raw: {:?}",
                location, INDENT, expected_output, INDENT, actual_output, INDENT, actual_raw_output
            ),
            FormatFailure { error, location } => {
                write!(f, "FormatFailure:{}:\n{:#}", location, error)
            }
            Bail { cause, location } => write!(f, "Bail:{} {}", location, cause),
            Success => f.write_str("Success"),
        }
    }
}

#[derive(Default, Debug, Eq, PartialEq)]
pub struct Outcomes([usize; NUM_OUTCOMES]);

impl ops::AddAssign<Outcomes> for Outcomes {
    fn add_assign(&mut self, rhs: Outcomes) {
        for (lhs, rhs) in self.0.iter_mut().zip(rhs.0.iter()) {
            *lhs += rhs
        }
    }
}

impl fmt::Display for Outcomes {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let total: usize = self.0.iter().sum();
        write!(
            f,
            "{}:",
            if self.0[SUCCESS_OUTCOME] == total {
                "PASS"
            } else {
                "FAIL"
            }
        )?;
        lazy_static! {
            static ref NAMES: Vec<&'static str> = vec![
                "unsupported",
                "plan-failure",
                "unexpected-plan-success",
                "wrong-number-of-rows-inserted",
                "inference-failure",
                "wrong-column-names",
                "output-failure",
                "format-failure",
                "bail",
                "success",
                "total",
            ];
        }
        for (i, n) in self.0.iter().enumerate() {
            if *n > 0 {
                write!(f, " {}={}", NAMES[i], n)?;
            }
        }
        write!(f, " total={}", total)
    }
}

impl Outcomes {
    pub fn any_failed(&self) -> bool {
        self.0[SUCCESS_OUTCOME] < self.0.iter().sum::<usize>()
    }

    pub fn as_json(&self) -> serde_json::Value {
        serde_json::json!({
            "unsupported": self.0[0],
            "plan_failure": self.0[1],
            "unexpected_plan_success": self.0[2],
            "wrong_number_of_rows_affected": self.0[3],
            "inference_failure": self.0[4],
            "wrong_column_names": self.0[5],
            "output_failure": self.0[6],
            "format_failure": self.0[7],
            "bail": self.0[8],
            "success": self.0[9],
        })
    }
}

const NUM_TIMELY_WORKERS: usize = 3;

pub(crate) struct Runner {
    // Drop order matters for these fields.
    client: Client,
    _server: Server,
}

struct SltI(i64);

impl std::fmt::Display for SltI {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl<'a> FromSql<'a> for SltI {
    fn from_sql(
        ty: &PgType,
        raw: &'a [u8],
    ) -> Result<Self, Box<dyn Error + 'static + Send + Sync>> {
        Ok(Self(match ty {
            &PgType::INT2 => types::int2_from_sql(raw)?.into(),
            &PgType::INT4 => types::int4_from_sql(raw)?.into(),
            &PgType::INT8 => types::int8_from_sql(raw)?,
            _ => unreachable!(),
        }))
    }
    fn accepts(ty: &PgType) -> bool {
        match ty {
            &PgType::INT2 | &PgType::INT4 | &PgType::INT8 => true,
            _ => false,
        }
    }
}

enum SltR {
    Float(f64),
    Numeric(Numeric),
}

impl std::fmt::Display for SltR {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            SltR::Float(v) => v.fmt(f),
            SltR::Numeric(v) => v.fmt(f),
        }
    }
}

impl<'a> FromSql<'a> for SltR {
    fn from_sql(
        ty: &PgType,
        raw: &'a [u8],
    ) -> Result<Self, Box<dyn Error + 'static + Send + Sync>> {
        Ok(match ty {
            &PgType::FLOAT4 => SltR::Float(types::float4_from_sql(raw)?.into()),
            &PgType::FLOAT8 => SltR::Float(types::float8_from_sql(raw)?),
            &PgType::NUMERIC => SltR::Numeric(Numeric::from_sql(ty, raw)?),
            _ => unreachable!(),
        })
    }
    fn accepts(ty: &PgType) -> bool {
        match ty {
            &PgType::FLOAT4 | &PgType::FLOAT8 | &PgType::NUMERIC => true,
            _ => false,
        }
    }
}

struct SltT(String);

impl std::fmt::Display for SltT {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl<'a> FromSql<'a> for SltT {
    fn from_sql(
        ty: &PgType,
        raw: &'a [u8],
    ) -> Result<Self, Box<dyn Error + 'static + Send + Sync>> {
        Ok(Self(match ty {
            &PgType::TEXT => {
                let s: String = types::text_from_sql(raw)?.into();
                if s.is_empty() {
                    "(empty)".to_string()
                } else {
                    s
                }
            }
            &PgType::DATE => NaiveDate::from_sql(ty, raw)?.to_string(),
            &PgType::TIMESTAMP => NaiveDateTime::from_sql(ty, raw)?.to_string(),
            &PgType::TIMESTAMPTZ => DateTime::<Utc>::from_sql(ty, raw)?.to_string(),
            &PgType::TIME => NaiveTime::from_sql(ty, raw)?.to_string(),
            &PgType::INTERVAL => Interval::from_sql(ty, raw)?.to_string(),
            _ => unreachable!(),
        }))
    }
    fn accepts(ty: &PgType) -> bool {
        match ty {
            &PgType::TEXT
            | &PgType::DATE
            | &PgType::TIMESTAMP
            | &PgType::TIMESTAMPTZ
            | &PgType::TIME
            | &PgType::INTERVAL => true,
            _ => false,
        }
    }
}

fn maybe_null<T: std::fmt::Display>(
    t: Result<Option<T>, tokio_postgres::error::Error>,
) -> Result<String, anyhow::Error> {
    let t = match t {
        Ok(t) => t,
        Err(e) => return Err(anyhow!(e)),
    };
    Ok(match t {
        Some(t) => t.to_string(),
        None => "NULL".to_string(),
    })
}

fn format_row(
    row: &Row,
    types: &Vec<Type>,
    mode: Mode,
    sort: &Sort,
) -> Result<Vec<String>, anyhow::Error> {
    let mut formatted: Vec<String> = vec![];
    for i in 0..row.len() {
        // Unmarshal into the SltX types. They have been designed to support a variety
        // of source postgres types. The rust postgres driver has very strict type
        // support (i.e., an INT4 can only be decoded into an i32, not an i64). We need
        // much more lenient rules here because the SLT syntax only supports a small
        // number of types. Implement our own FromSql implementations to achieve this.
        let datum: String = match types[i] {
            Type::Text => maybe_null(row.try_get::<usize, Option<SltT>>(i)),
            Type::Integer => maybe_null(row.try_get::<usize, Option<SltI>>(i)),
            Type::Real => maybe_null(row.try_get::<usize, Option<SltR>>(i)),
            Type::Bool => maybe_null(row.try_get::<usize, Option<bool>>(i)),
            Type::Oid => maybe_null(row.try_get::<usize, Option<u32>>(i)),
        }?;
        formatted.push(datum);
    }
    Ok(if mode == Mode::Cockroach && sort.yes() {
        formatted
            .iter()
            .flat_map(|s| {
                crate::parser::split_cols(&s, types.len())
                    .into_iter()
                    .map(ToString::to_string)
                    .collect::<Vec<_>>()
            })
            .collect()
    } else {
        formatted
    })
}

impl Runner {
    pub async fn start() -> Result<Self, anyhow::Error> {
        let config = Config {
            logging: None,
            timestamp_frequency: Duration::from_millis(10),
            cache: None,
            logical_compaction_window: None,
            threads: NUM_TIMELY_WORKERS,
            process: 0,
            addresses: vec![SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0)],
            data_directory: None,
            symbiosis_url: Some("postgres://".into()),
            listen_addr: None,
            tls: None,
            experimental_mode: true,
            telemetry_url: None,
        };
        let server = serve(config).await?;
        let addr = server.local_addr();
        let (client, connection) = connect(
            &format!("host={} port={} user=root", addr.ip(), addr.port()),
            NoTls,
        )
        .await?;

        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("connection error: {}", e);
            }
        });

        Ok(Runner {
            _server: server,
            client,
        })
    }

    async fn run_record<'a>(
        &mut self,
        record: &'a Record<'a>,
    ) -> Result<Outcome<'a>, anyhow::Error> {
        match &record {
            Record::Statement {
                expected_error,
                rows_affected,
                sql,
                location,
            } => {
                match self
                    .run_statement(*expected_error, *rows_affected, sql, location.clone())
                    .await?
                {
                    Outcome::Success => Ok(Outcome::Success),
                    other => {
                        if expected_error.is_some() {
                            Ok(other)
                        } else {
                            // If we failed to execute a statement that was supposed to succeed,
                            // running the rest of the tests in this file will probably cause
                            // false positives, so just give up on the file entirely.
                            Ok(Outcome::Bail {
                                cause: Box::new(other),
                                location: location.clone(),
                            })
                        }
                    }
                }
            }
            Record::Query {
                sql,
                output,
                location,
            } => self.run_query(sql, output, location.clone()).await,
            _ => Ok(Outcome::Success),
        }
    }

    async fn run_statement<'a>(
        &mut self,
        expected_error: Option<&'a str>,
        expected_rows_affected: Option<u64>,
        sql: &'a str,
        location: Location,
    ) -> Result<Outcome<'a>, anyhow::Error> {
        lazy_static! {
            static ref UNSUPPORTED_INDEX_STATEMENT_REGEX: Regex =
                Regex::new("^(CREATE UNIQUE INDEX|REINDEX)").unwrap();
        }
        if UNSUPPORTED_INDEX_STATEMENT_REGEX.is_match(sql) {
            // sure, we totally made you an index
            return Ok(Outcome::Success);
        }

        match self.client.execute(sql, &[]).await {
            Ok(actual) => {
                if let Some(expected_error) = expected_error {
                    return Ok(Outcome::UnexpectedPlanSuccess {
                        expected_error,
                        location,
                    });
                }
                match expected_rows_affected {
                    None => Ok(Outcome::Success),
                    Some(expected) => {
                        if expected != actual {
                            Ok(Outcome::WrongNumberOfRowsInserted {
                                expected_count: expected,
                                actual_count: actual,
                                location,
                            })
                        } else {
                            Ok(Outcome::Success)
                        }
                    }
                }
            }
            Err(error) => {
                if let Some(expected_error) = expected_error {
                    if Regex::new(expected_error)?.is_match(&format!("{:#}", error)) {
                        return Ok(Outcome::Success);
                    }
                }
                Ok(Outcome::PlanFailure {
                    error: anyhow!(error),
                    location,
                })
            }
        }
    }

    async fn run_query<'a>(
        &mut self,
        sql: &'a str,
        output: &'a Result<QueryOutput<'_>, &'a str>,
        location: Location,
    ) -> Result<Outcome<'a>, anyhow::Error> {
        let rows = match self.client.query(sql, &[]).await {
            Ok(rows) => rows,
            Err(error) => {
                return match output {
                    Ok(_) => {
                        let error_string = format!("{}", error);
                        if error_string.contains("supported") || error_string.contains("overload") {
                            // this is a failure, but it's caused by lack of support rather than by bugs
                            Ok(Outcome::Unsupported {
                                error: anyhow!(error),
                                location,
                            })
                        } else {
                            Ok(Outcome::PlanFailure {
                                error: anyhow!(error),
                                location,
                            })
                        }
                    }
                    Err(expected_error) => {
                        if Regex::new(expected_error)?.is_match(&format!("{:#}", error)) {
                            Ok(Outcome::Success)
                        } else {
                            Ok(Outcome::PlanFailure {
                                error: anyhow!(error),
                                location,
                            })
                        }
                    }
                };
            }
        };

        // unpack expected output
        let QueryOutput {
            sort,
            types: expected_types,
            column_names: expected_column_names,
            output: expected_output,
            mode,
            ..
        } = match output {
            Err(expected_error) => {
                return Ok(Outcome::UnexpectedPlanSuccess {
                    expected_error,
                    location,
                });
            }
            Ok(query_output) => query_output,
        };

        // Various checks as long as there are returned rows.
        if let Some(row) = rows.iter().next() {
            // check column names
            if let Some(expected_column_names) = expected_column_names {
                let actual_column_names = row
                    .columns()
                    .iter()
                    .map(|t| ColumnName::from(t.name()))
                    .collect::<Vec<_>>();
                if expected_column_names != &actual_column_names {
                    return Ok(Outcome::WrongColumnNames {
                        expected_column_names,
                        actual_column_names,
                        location,
                    });
                }
            }
        }

        // format output
        let mut formatted_rows = vec![];
        for row in &rows {
            if row.len() != expected_types.len() {
                return Ok(Outcome::WrongColumnCount {
                    expected_count: expected_types.len(),
                    actual_count: row.len(),
                    location,
                });
            }
            match format_row(row, &expected_types, *mode, sort) {
                Ok(row) => formatted_rows.push(row),
                Err(error) => return Ok(Outcome::FormatFailure { error, location }),
            }
        }

        // sort formatted output
        if let Sort::Row = sort {
            formatted_rows.sort();
        }
        let mut values = formatted_rows.into_iter().flatten().collect::<Vec<_>>();
        if let Sort::Value = sort {
            values.sort();
        }

        // check output
        match expected_output {
            Output::Values(expected_values) => {
                if values != *expected_values {
                    return Ok(Outcome::OutputFailure {
                        expected_output,
                        actual_raw_output: rows,
                        actual_output: Output::Values(values),
                        location,
                    });
                }
            }
            Output::Hashed {
                num_values,
                md5: expected_md5,
            } => {
                let mut hasher = Md5::new();
                for value in &values {
                    hasher.update(value);
                    hasher.update("\n");
                }
                let md5 = format!("{:x}", hasher.finalize());
                if values.len() != *num_values || md5 != *expected_md5 {
                    return Ok(Outcome::OutputFailure {
                        expected_output,
                        actual_raw_output: rows,
                        actual_output: Output::Hashed {
                            num_values: values.len(),
                            md5,
                        },
                        location,
                    });
                }
            }
        }

        Ok(Outcome::Success)
    }

    pub(crate) async fn run_sql(&mut self, sql: &str) -> Result<Vec<Row>, anyhow::Error> {
        Ok(self.client.query(sql, &[]).await?)
    }
}

fn print_record(record: &Record) {
    match record {
        Record::Statement { sql, .. } | Record::Query { sql, .. } => {
            println!("{}", crate::util::indent(sql, 4))
        }
        _ => (),
    }
}

pub async fn run_string(
    source: &str,
    input: &str,
    verbosity: usize,
) -> Result<Outcomes, anyhow::Error> {
    let mut outcomes = Outcomes::default();
    let mut state = Runner::start().await.unwrap();
    let mut parser = crate::parser::Parser::new(source, input);
    println!("==> {}", source);
    for record in parser.parse_records()? {
        // In maximal-verbosity mode, print the query before attempting to run
        // it. Running the query might panic, so it is important to print out
        // what query we are trying to run *before* we panic.
        if verbosity >= 2 {
            print_record(&record);
        }

        let outcome = state
            .run_record(&record)
            .await
            .map_err(|err| format!("In {}:\n{}", source, err))
            .unwrap();

        // Print failures in verbose mode.
        if verbosity >= 1 && !outcome.success() {
            if verbosity < 2 {
                // If `verbosity >= 2`, we'll already have printed the record,
                // so don't print it again. Yes, this is an ugly bit of logic.
                // Please don't try to consolidate it with the `print_record`
                // call above, as it's important to have a mode in which records
                // are printed before they are run, so that if running the
                // record panics, you can tell which record caused it.
                print_record(&record);
            }
            println!("{}", util::indent(&outcome.to_string(), 4));
            println!("{}", util::indent("----", 4));
        }

        outcomes.0[outcome.code()] += 1;

        if let Outcome::Bail { .. } = outcome {
            break;
        }
    }
    Ok(outcomes)
}

pub async fn run_file(filename: &Path, verbosity: usize) -> Result<Outcomes, anyhow::Error> {
    let mut input = String::new();
    File::open(filename)?.read_to_string(&mut input)?;
    run_string(&format!("{}", filename.display()), &input, verbosity).await
}

pub async fn run_stdin(verbosity: usize) -> Result<Outcomes, anyhow::Error> {
    let mut input = String::new();
    std::io::stdin().lock().read_to_string(&mut input)?;
    run_string("<stdin>", &input, verbosity).await
}

pub async fn rewrite_file(filename: &Path, _verbosity: usize) -> Result<(), anyhow::Error> {
    let mut file = OpenOptions::new().read(true).write(true).open(filename)?;

    let mut input = String::new();
    file.read_to_string(&mut input)?;

    let mut buf = RewriteBuffer::new(&input);

    let mut state = Runner::start().await?;
    let mut parser = crate::parser::Parser::new(filename.to_str().unwrap_or(""), &input);
    println!("==> {}", filename.display());
    for record in parser.parse_records()? {
        let record = record;
        let outcome = state.run_record(&record).await?;

        // If we see an output failure for a query, rewrite the expected output
        // to match the observed output.
        if let (
            Record::Query {
                output:
                    Ok(QueryOutput {
                        mode,
                        output: Output::Values(_),
                        output_str: expected_output,
                        types,
                        ..
                    }),
                ..
            },
            Outcome::OutputFailure {
                actual_output: Output::Values(actual_output),
                ..
            },
        ) = (&record, &outcome)
        {
            // Output everything before this record.
            let offset = expected_output.as_ptr() as usize - input.as_ptr() as usize;
            buf.flush_to(offset);
            buf.skip_to(offset + expected_output.len());

            // Attempt to install the result separator (----), if it does
            // not already exist.
            if buf.peek_last(5) == "\n----" {
                buf.append("\n");
            } else if buf.peek_last(6) != "\n----\n" {
                buf.append("\n----\n");
            }

            for (i, row) in actual_output.chunks(types.len()).enumerate() {
                match mode {
                    // In Cockroach mode, output each row on its own line, with
                    // two spaces between each column.
                    Mode::Cockroach => {
                        if i != 0 {
                            buf.append("\n");
                        }
                        buf.append(&row.join("  "));
                    }
                    // In standard mode, output each value on its own line,
                    // and ignore row boundaries.
                    Mode::Standard => {
                        for (j, col) in row.iter().enumerate() {
                            if i != 0 || j != 0 {
                                buf.append("\n");
                            }
                            buf.append(col);
                        }
                    }
                }
            }
        } else if let Outcome::Success = outcome {
            // Ok.
        } else {
            bail!("unexpected: {}", outcome);
        }
    }

    file.set_len(0)?;
    file.seek(SeekFrom::Start(0))?;
    file.write_all(buf.finish().as_bytes())?;
    file.sync_all()?;
    Ok(())
}

struct RewriteBuffer<'a> {
    input: &'a str,
    input_offset: usize,
    output: String,
}

impl<'a> RewriteBuffer<'a> {
    fn new(input: &'a str) -> RewriteBuffer<'a> {
        RewriteBuffer {
            input,
            input_offset: 0,
            output: String::new(),
        }
    }

    fn flush_to(&mut self, offset: usize) {
        assert!(offset >= self.input_offset);
        let chunk = &self.input[self.input_offset..offset];
        self.output.push_str(chunk);
        self.input_offset = offset;
    }

    fn skip_to(&mut self, offset: usize) {
        assert!(offset >= self.input_offset);
        self.input_offset = offset;
    }

    fn append(&mut self, s: &str) {
        self.output.push_str(s);
    }

    fn peek_last(&self, n: usize) -> &str {
        &self.output[self.output.len() - n..]
    }

    fn finish(mut self) -> String {
        self.flush_to(self.input.len());
        self.output
    }
}

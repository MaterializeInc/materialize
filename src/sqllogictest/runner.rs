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

use std::borrow::ToOwned;
use std::fmt;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::mem;
use std::ops;
use std::path::Path;
use std::str;
use std::thread;
use std::time::Duration;

use failure::{bail, ResultExt};
use futures::executor::block_on;
use itertools::izip;
use lazy_static::lazy_static;
use regex::Regex;

use coord::ExecuteResponse;
use coord::TimestampChannel;

use dataflow;
use ore::option::OptionExt;
use ore::thread::{JoinHandleExt, JoinOnDropHandle};
use repr::jsonb::Jsonb;
use repr::{ColumnName, ColumnType, Datum, RelationDesc, Row, ScalarType};
use sql::{Session, Statement};
use sql_parser::parser::{Parser as SqlParser, ParserError as SqlParserError};

use crate::ast::{Mode, Output, QueryOutput, Record, Sort, Type};
use crate::util;

#[derive(Debug)]
pub enum Outcome<'a> {
    Unsupported {
        error: failure::Error,
    },
    ParseFailure {
        error: SqlParserError,
    },
    PlanFailure {
        error: failure::Error,
    },
    UnexpectedPlanSuccess {
        expected_error: &'a str,
    },
    WrongNumberOfRowsInserted {
        expected_count: usize,
        actual_count: usize,
    },
    InferenceFailure {
        expected_types: &'a [Type],
        inferred_types: Vec<ColumnType>,
        message: String,
    },
    WrongColumnNames {
        expected_column_names: &'a Vec<ColumnName>,
        inferred_column_names: Vec<ColumnName>,
    },
    OutputFailure {
        expected_output: &'a Output,
        actual_raw_output: Vec<Row>,
        actual_output: Output,
    },
    Bail {
        cause: Box<Outcome<'a>>,
    },
    Success,
}

const NUM_OUTCOMES: usize = 10;

impl<'a> Outcome<'a> {
    fn code(&self) -> usize {
        match self {
            Outcome::Unsupported { .. } => 0,
            Outcome::ParseFailure { .. } => 1,
            Outcome::PlanFailure { .. } => 2,
            Outcome::UnexpectedPlanSuccess { .. } => 3,
            Outcome::WrongNumberOfRowsInserted { .. } => 4,
            Outcome::InferenceFailure { .. } => 5,
            Outcome::WrongColumnNames { .. } => 6,
            Outcome::OutputFailure { .. } => 7,
            Outcome::Bail { .. } => 8,
            Outcome::Success => 9,
        }
    }

    fn success(&self) -> bool {
        if let Outcome::Success = self {
            true
        } else {
            false
        }
    }
}

impl fmt::Display for Outcome<'_> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use Outcome::*;
        const INDENT: &str = "\n        ";
        match self {
            Unsupported { error } => write_err("Unsupported", error, f),
            ParseFailure { error } => write_err("ParseFailure", error, f),
            PlanFailure { error } => write_err("PlanFailure", error, f),
            UnexpectedPlanSuccess { expected_error } => write!(
                f,
                "UnexpectedPlanSuccess! expected error: {}",
                expected_error
            ),
            WrongNumberOfRowsInserted {
                expected_count,
                actual_count,
            } => write!(
                f,
                "WrongNumberOfRowsInserted!{}expected: {}{}actually: {}",
                INDENT, expected_count, INDENT, actual_count
            ),
            InferenceFailure {
                expected_types,
                inferred_types,
                message,
            } => write!(
                f,
                "Inference Failure!{}\
                 expected types: {}{}\
                 inferred types: {}{}\
                 message: {}",
                INDENT,
                expected_types
                    .iter()
                    .map(|s| format!("{:?}", s))
                    .collect::<Vec<_>>()
                    .join(" "),
                INDENT,
                inferred_types
                    .iter()
                    .map(|s| format!("{}", s.scalar_type))
                    .collect::<Vec<_>>()
                    .join(" "),
                INDENT,
                message
            ),
            WrongColumnNames {
                expected_column_names,
                inferred_column_names,
            } => write!(
                f,
                "Wrong Column Names:{}expected column names: {}{}inferred column names: {}",
                INDENT,
                expected_column_names
                    .iter()
                    .map(|n| n.to_string())
                    .collect::<Vec<_>>()
                    .join(" "),
                INDENT,
                inferred_column_names
                    .iter()
                    .map(|n| n.to_string())
                    .collect::<Vec<_>>()
                    .join(" ")
            ),
            OutputFailure {
                expected_output,
                actual_raw_output,
                actual_output,
            } => write!(
                f,
                "OutputFailure!{}expected: {:?}{}actually: {:?}{}actual raw: {:?}",
                INDENT, expected_output, INDENT, actual_output, INDENT, actual_raw_output
            ),
            Bail { cause } => write!(f, "Bail! {}", cause),
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
        write!(f, "{}:", if self.0[9] == total { "PASS" } else { "FAIL" })?;
        lazy_static! {
            static ref NAMES: Vec<&'static str> = vec![
                "unsupported",
                "parse-failure",
                "plan-failure",
                "unexpected-plan-success",
                "wrong-number-of-rows-inserted",
                "inference-failure",
                "wrong-column-names",
                "output-failure",
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
        self.0[9] < self.0.iter().sum::<usize>()
    }

    pub fn as_json(&self) -> serde_json::Value {
        serde_json::json!({
            "unsupported": self.0[0],
            "parse_failure": self.0[1],
            "plan_failure": self.0[2],
            "unexpected_plan_success": self.0[3],
            "wrong_number_of_rows_affected": self.0[4],
            "inference_failure": self.0[5],
            "wrong_column_names": self.0[6],
            "output_failure": self.0[7],
            "bail": self.0[8],
            "success": self.0[9],
        })
    }
}

/// Write an error and its causes in a common format
fn write_err(kind: &str, error: &impl failure::AsFail, f: &mut fmt::Formatter) -> fmt::Result {
    let error = error.as_fail();
    write!(f, "{0}: {1} ({1:?})", kind, error)?;
    for cause in error.iter_causes() {
        write!(f, "\n    caused by: {}", cause)?;
    }
    Ok(())
}

const NUM_TIMELY_WORKERS: usize = 3;

pub(crate) struct State {
    // Drop order matters for these fields.
    cmd_tx: futures::channel::mpsc::UnboundedSender<coord::Command>,
    _dataflow_workers: Box<dyn Drop>,
    _timestamp_thread: Option<JoinOnDropHandle<()>>,
    _coord_thread: JoinOnDropHandle<()>,
    _runtime: tokio::runtime::Runtime,
    session: Session,
    conn_id: u32,
}

fn format_row(
    row: &Row,
    col_types: &[ColumnType],
    slt_types: &[Type],
    mode: Mode,
    sort: &Sort,
) -> Vec<String> {
    let row = izip!(slt_types, col_types, row.iter()).map(|(slt_typ, col_typ, datum)| {
        if let Datum::Null = datum {
            "NULL".to_owned()
        } else if let ScalarType::Jsonb = col_typ.scalar_type {
            Jsonb::from_datum(datum).to_string()
        } else {
            match (slt_typ, datum) {
                // the documented formatting rules in https://www.sqlite.org/sqllogictest/doc/trunk/about.wiki
                (Type::Integer, Datum::Int64(i)) => format!("{}", i),
                (Type::Integer, Datum::Int32(i)) => format!("{}", i),
                (Type::Real, Datum::Float64(f)) => match mode {
                    Mode::Standard => format!("{:.3}", f),
                    Mode::Cockroach => format!("{}", f),
                },
                (Type::Real, Datum::Decimal(d)) => {
                    let (_precision, scale) = col_typ.scalar_type.unwrap_decimal_parts();
                    let d = d.with_scale(scale);
                    match mode {
                        Mode::Standard => format!("{:.3}", d),
                        Mode::Cockroach => format!("{}", d),
                    }
                }
                (Type::Text, Datum::String(string)) => {
                    if string.is_empty() {
                        "(empty)".to_owned()
                    } else {
                        (*string).to_owned()
                    }
                }
                (Type::Bool, Datum::False) => "false".to_owned(),
                (Type::Bool, Datum::True) => "true".to_owned(),

                // weird type coercions that sqllogictest doesn't document
                (Type::Integer, Datum::Decimal(d)) => {
                    let (_precision, scale) = col_typ.scalar_type.unwrap_decimal_parts();
                    let d = d.with_scale(scale);
                    format!("{:.0}", d)
                }
                (Type::Integer, Datum::Float64(f)) => format!("{:.0}", f.trunc()),
                (Type::Integer, Datum::String(_)) => "0".to_owned(),
                (Type::Integer, Datum::False) => "0".to_owned(),
                (Type::Integer, Datum::True) => "1".to_owned(),
                (Type::Real, Datum::Int32(i)) => format!("{:.3}", i),
                (Type::Real, Datum::Int64(i)) => format!("{:.3}", i),
                (Type::Text, Datum::Int32(i)) => format!("{}", i),
                (Type::Text, Datum::Int64(i)) => format!("{}", i),
                (Type::Text, Datum::Float64(f)) => format!("{:.3}", f),
                (Type::Text, Datum::Date(d)) => d.to_string(),
                (Type::Text, Datum::Time(t)) => t.to_string(),
                (Type::Text, Datum::Timestamp(d)) => d.to_string(),
                (Type::Text, Datum::TimestampTz(d)) => d.to_string(),
                (Type::Text, Datum::Interval(iv)) => iv.to_string(),
                // Bytes are printed as text iff they are valid UTF-8. This
                // seems guaranteed to confuse everyone, but it is required for
                // compliance with the CockroachDB sqllogictest runner. [0]
                //
                // [0]: https://github.com/cockroachdb/cockroach/blob/970782487/pkg/sql/logictest/logic.go#L2038-L2043
                (Type::Text, Datum::Bytes(buf)) => match str::from_utf8(buf) {
                    Ok(s) => s.to_owned(),
                    Err(_) => format!("{:?}", buf),
                },
                other => panic!("Don't know how to format {:?}", other),
            }
        }
    });
    if mode == Mode::Cockroach && sort.yes() {
        row.flat_map(|s| {
            crate::parser::split_cols(&s, slt_types.len())
                .into_iter()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
        })
        .collect()
    } else {
        row.collect()
    }
}

impl State {
    pub fn start() -> Result<Self, failure::Error> {
        let logging_config = None;
        let process_id = 0;

        let (switchboard, runtime) = comm::Switchboard::local()?;
        let executor = runtime.handle().clone();

        let (cmd_tx, cmd_rx) = futures::channel::mpsc::unbounded();
        let (source_tx, source_rx) = std::sync::mpsc::channel();
        let (ts_tx, ts_rx) = std::sync::mpsc::channel();

        let mut coord = coord::Coordinator::new(coord::Config {
            switchboard: switchboard.clone(),
            num_timely_workers: NUM_TIMELY_WORKERS,
            symbiosis_url: Some("postgres://"),
            logging: logging_config.as_ref(),
            data_directory: None,
            executor: &executor,
            ts_channel: Some(TimestampChannel {
                sender: source_tx,
                receiver: ts_rx,
            }),
        })?;

        let mut tsper = coord::Timestamper::new(
            Duration::from_millis(10),
            1000,
            None,
            coord::TimestampChannel {
                sender: ts_tx,
                receiver: source_rx,
            },
        );

        let coord_thread = thread::spawn(move || coord.serve(cmd_rx)).join_on_drop();
        let ts_thread = thread::spawn(move || tsper.update()).join_on_drop();

        let dataflow_workers = dataflow::serve(
            vec![None],
            NUM_TIMELY_WORKERS,
            process_id,
            switchboard,
            runtime.handle().clone(),
            true,
            logging_config,
        )
        .unwrap();

        Ok(State {
            cmd_tx,
            _dataflow_workers: Box::new(dataflow_workers),
            _coord_thread: coord_thread,
            _timestamp_thread: Some(ts_thread),
            _runtime: runtime,
            session: Session::default(),
            conn_id: 1,
        })
    }

    fn run_record<'a>(&mut self, record: &'a Record) -> Result<Outcome<'a>, failure::Error> {
        match &record {
            Record::Statement {
                expected_error,
                rows_affected,
                sql,
            } => match self.run_statement(*expected_error, *rows_affected, sql)? {
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
                        })
                    }
                }
            },
            Record::Query { sql, output } => self.run_query(sql, output),
            _ => Ok(Outcome::Success),
        }
    }

    fn run_statement<'a>(
        &mut self,
        expected_error: Option<&'a str>,
        expected_rows_affected: Option<usize>,
        sql: &'a str,
    ) -> Result<Outcome<'a>, failure::Error> {
        lazy_static! {
            static ref UNSUPPORTED_INDEX_STATEMENT_REGEX: Regex =
                Regex::new("^(CREATE UNIQUE INDEX|REINDEX)").unwrap();
        }
        if UNSUPPORTED_INDEX_STATEMENT_REGEX.is_match(sql) {
            // sure, we totally made you an index
            return Ok(Outcome::Success);
        }

        match self.run_sql(sql) {
            Ok((_desc, resp)) => {
                if let Some(expected_error) = expected_error {
                    return Ok(Outcome::UnexpectedPlanSuccess { expected_error });
                }
                match expected_rows_affected {
                    None => Ok(Outcome::Success),
                    Some(expected) => match resp {
                        ExecuteResponse::Inserted(actual)
                        | ExecuteResponse::Updated(actual)
                        | ExecuteResponse::Deleted(actual) => {
                            if expected != actual {
                                Ok(Outcome::WrongNumberOfRowsInserted {
                                    expected_count: expected,
                                    actual_count: actual,
                                })
                            } else {
                                Ok(Outcome::Success)
                            }
                        }

                        _ => Ok(Outcome::PlanFailure {
                            error: failure::format_err!(
                                "Query did not insert any rows, expected {}",
                                expected,
                            ),
                        }),
                    },
                }
            }
            Err(error) => {
                if let Some(expected_error) = expected_error {
                    if Regex::new(expected_error)?.is_match(&error.to_string()) {
                        return Ok(Outcome::Success);
                    }
                }
                Ok(Outcome::PlanFailure { error })
            }
        }
    }

    fn run_query<'a>(
        &mut self,
        sql: &'a str,
        output: &'a Result<QueryOutput, &'a str>,
    ) -> Result<Outcome<'a>, failure::Error> {
        // get statement
        let statements = match SqlParser::parse_sql(sql.to_string()) {
            Ok(statements) => statements,
            Err(error) => {
                if output.is_err() {
                    return Ok(Outcome::Success);
                } else {
                    return Ok(Outcome::ParseFailure { error });
                }
            }
        };
        let statement = match &*statements {
            [] => bail!("Got zero statements?"),
            [statement] => statement,
            _ => bail!("Got multiple statements: {:?}", statements),
        };
        match statement {
            Statement::CreateView { .. }
            | Statement::Query { .. }
            | Statement::ShowIndexes { .. } => (),
            _ => {
                if output.is_err() {
                    // We're not interested in testing our hacky handling of INSERT etc
                    return Ok(Outcome::Success);
                }
            }
        }

        // send plan, read response
        let (desc, rows_rx) = match self.run_sql(sql) {
            Ok((desc, ExecuteResponse::SendRows(rx))) => (
                desc.expect("RelationDesc missing for query that returns rows"),
                rx,
            ),
            Ok(other) => {
                return Ok(Outcome::PlanFailure {
                    error: failure::format_err!(
                        "Query did not result in SendRows, instead got {:?}",
                        other
                    ),
                });
            }
            Err(error) => {
                return match output {
                    Ok(_) => {
                        let error_string = format!("{}", error);
                        if error_string.contains("supported") || error_string.contains("overload") {
                            // this is a failure, but it's caused by lack of support rather than by bugs
                            Ok(Outcome::Unsupported { error })
                        } else {
                            Ok(Outcome::PlanFailure { error })
                        }
                    }
                    Err(expected_error) => {
                        if Regex::new(expected_error)?.is_match(&error.to_string()) {
                            Ok(Outcome::Success)
                        } else {
                            Ok(Outcome::PlanFailure { error })
                        }
                    }
                };
            }
        };

        // get actual output
        let raw_output = block_on(rows_rx)?.unwrap_rows();

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
                return Ok(Outcome::UnexpectedPlanSuccess { expected_error });
            }
            Ok(query_output) => query_output,
        };

        // check that inferred types match expected types
        let inferred_types = &desc.typ().column_types;
        // sqllogictest coerces the output into the expected type, so `expected_types` is often wrong :(
        // but at least it will be the correct length
        if inferred_types.len() != expected_types.len() {
            return Ok(Outcome::InferenceFailure {
                expected_types,
                inferred_types: inferred_types.to_vec(),
                message: format!(
                    "Expected {} types, got {} types",
                    expected_types.len(),
                    inferred_types.len()
                ),
            });
        }

        // check that output matches inferred types
        for row in &raw_output {
            if row.unpack().len() != inferred_types.len() {
                return Ok(Outcome::InferenceFailure {
                    expected_types,
                    inferred_types: inferred_types.to_vec(),
                    message: format!(
                        "Expected {} datums, got {} datums in row {:?}",
                        expected_types.len(),
                        inferred_types.len(),
                        row
                    ),
                });
            }
            for (inferred_type, datum) in inferred_types.iter().zip(row.iter()) {
                if !datum.is_instance_of(inferred_type) {
                    return Ok(Outcome::InferenceFailure {
                        expected_types,
                        inferred_types: inferred_types.to_vec(),
                        message: format!(
                            "Inferred type {:?}, got datum {:?}",
                            inferred_type, datum,
                        ),
                    });
                }
            }
        }

        // check column names
        if let Some(expected_column_names) = expected_column_names {
            let inferred_column_names = desc
                .iter_names()
                .map(|t| t.owned().unwrap_or_else(|| "?column?".into()))
                .collect::<Vec<_>>();
            if expected_column_names != &inferred_column_names {
                return Ok(Outcome::WrongColumnNames {
                    expected_column_names,
                    inferred_column_names,
                });
            }
        }

        // format output
        let mut formatted_rows = raw_output
            .iter()
            .map(|row| format_row(&row, inferred_types, &**expected_types, *mode, sort))
            .collect::<Vec<_>>();

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
                        actual_raw_output: raw_output,
                        actual_output: Output::Values(values),
                    });
                }
            }
            Output::Hashed {
                num_values,
                md5: expected_md5,
            } => {
                let mut md5_context = md5::Context::new();
                for value in &values {
                    md5_context.consume(value);
                    md5_context.consume("\n");
                }
                let md5 = format!("{:x}", md5_context.compute());
                if values.len() != *num_values || md5 != *expected_md5 {
                    return Ok(Outcome::OutputFailure {
                        expected_output,
                        actual_raw_output: raw_output,
                        actual_output: Output::Hashed {
                            num_values: values.len(),
                            md5,
                        },
                    });
                }
            }
        }

        Ok(Outcome::Success)
    }

    pub(crate) fn run_sql(
        &mut self,
        sql: &str,
    ) -> Result<(Option<RelationDesc>, ExecuteResponse), failure::Error> {
        let statement_name = String::from("");
        let portal_name = String::from("");

        // Parse.
        {
            let (tx, rx) = futures::channel::oneshot::channel();
            self.cmd_tx
                .unbounded_send(coord::Command::Parse {
                    name: statement_name.clone(),
                    sql: sql.into(),
                    session: mem::replace(&mut self.session, Session::default()),
                    tx,
                })
                .expect("futures channel should not fail");
            let resp = block_on(rx).expect("futures channel should not fail");
            resp.result?;
            mem::replace(&mut self.session, resp.session);
        }

        // Bind.
        let stmt = self
            .session
            .get_prepared_statement(&statement_name)
            .expect("unnamed prepared statement missing");
        let desc = stmt.desc().cloned();
        let result_formats = vec![pgrepr::Format::Text; stmt.result_width()];
        self.session
            .set_portal(portal_name.clone(), statement_name, vec![], result_formats)?;

        // Execute.
        {
            let (tx, rx) = futures::channel::oneshot::channel();
            self.cmd_tx
                .unbounded_send(coord::Command::Execute {
                    portal_name,
                    session: mem::replace(&mut self.session, Session::default()),
                    conn_id: self.conn_id,
                    tx,
                })
                .expect("futures channel should not fail");
            let resp = block_on(rx).expect("futures channel should not fail");
            mem::replace(&mut self.session, resp.session);
            Ok((desc, resp.result?))
        }
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

pub fn run_string(source: &str, input: &str, verbosity: usize) -> Outcomes {
    let mut outcomes = Outcomes::default();
    let mut state = State::start().unwrap();
    println!("==> {}", source);
    for record in crate::parser::parse_records(&input) {
        let record = record.unwrap();

        // In maximal-verbosity mode, print the query before attempting to run
        // it. Running the query might panic, so it is important to print out
        // what query we are trying to run *before* we panic.
        if verbosity >= 2 {
            print_record(&record);
        }

        let outcome = state
            .run_record(&record)
            .with_context(|err| format!("In {}:\n{}", source, err))
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
    outcomes
}

pub fn run_file(filename: &Path, verbosity: usize) -> Outcomes {
    let mut input = String::new();
    File::open(filename)
        .unwrap()
        .read_to_string(&mut input)
        .unwrap();
    run_string(&format!("{}", filename.display()), &input, verbosity)
}

pub fn run_stdin(verbosity: usize) -> Outcomes {
    let mut input = String::new();
    std::io::stdin().lock().read_to_string(&mut input).unwrap();
    run_string("<stdin>", &input, verbosity)
}

pub fn rewrite_file(filename: &Path, _verbosity: usize) {
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(filename)
        .unwrap();

    let mut input = String::new();
    file.read_to_string(&mut input).unwrap();

    let mut buf = RewriteBuffer::new(&input);

    let mut state = State::start().unwrap();
    println!("==> {}", filename.display());
    for record in crate::parser::parse_records(&input) {
        let record = record.unwrap();
        let outcome = state.run_record(&record).unwrap();

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
        }

        if let Outcome::Bail { .. } = outcome {
            break;
        }
    }

    file.set_len(0).unwrap();
    file.seek(SeekFrom::Start(0)).unwrap();
    file.write_all(buf.finish().as_bytes()).unwrap();
    file.sync_all().unwrap();
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

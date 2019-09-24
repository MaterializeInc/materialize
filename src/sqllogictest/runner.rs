// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

//! The Materialize-specific runner for sqllogictest.

use std::borrow::ToOwned;
use std::collections::HashMap;
use std::fmt;
use std::fs::File;
use std::io::Read;
use std::ops;
use std::path::Path;
use std::str::FromStr;

use failure::{bail, ResultExt};
use futures::Future;
use itertools::izip;
use lazy_static::lazy_static;
use regex::Regex;
use uuid::Uuid;

use coord::{coordinator::Coordinator, SqlResponse};
use dataflow;
use dataflow_types::{
    self, Dataflow, LocalInput, LocalSourceConnector, Source, SourceConnector, Update,
};
use ore::mpmc::Mux;
use ore::option::OptionExt;
use repr::{ColumnType, Datum};
use sql::store::RemoveMode;
use sql::{Planner, Session};
use sqlparser::ast::{ObjectType, Statement};
use sqlparser::dialect::AnsiDialect;
use sqlparser::parser::{Parser as SqlParser, ParserError as SqlParserError};

use crate::ast::{Mode, Output, QueryOutput, Record, Sort, Type};
use crate::postgres::{self, Postgres};

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
        expected_column_names: &'a Vec<&'a str>,
        inferred_column_names: Vec<Option<String>>,
    },
    OutputFailure {
        expected_output: &'a Output,
        actual_raw_output: Vec<Vec<Datum>>,
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
                 column names:   {}{}\
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
                inferred_types
                    .iter()
                    .map(|s| s.name.as_deref().unwrap_or("?").to_string())
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
                expected_column_names.join(" "),
                INDENT,
                inferred_column_names
                    .iter()
                    .map(|s| format!("{:?}", s))
                    .collect::<Vec<_>>()
                    .join(" ")
            ),
            OutputFailure {
                expected_output,
                actual_raw_output,
                actual_output,
            } => write!(
                f,
                "OutputFailure!{}expected: {:?}{}actually: {}{}actual raw: {:?}",
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

impl FromStr for Outcomes {
    type Err = failure::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let pieces: Vec<_> = s.split(',').collect();
        if pieces.len() != NUM_OUTCOMES {
            bail!(
                "expected-outcomes argument needs {} comma-separated ints",
                NUM_OUTCOMES
            );
        }
        Ok(Outcomes([
            pieces[0].parse()?,
            pieces[1].parse()?,
            pieces[2].parse()?,
            pieces[3].parse()?,
            pieces[4].parse()?,
            pieces[5].parse()?,
            pieces[6].parse()?,
            pieces[7].parse()?,
            pieces[8].parse()?,
            pieces[9].parse()?,
        ]))
    }
}

impl fmt::Display for Outcomes {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        let total: usize = self.0.iter().sum();
        let status = if self.0[9] == total {
            "SUCCESS!"
        } else {
            "FAIL!"
        };
        write!(
            f,
            "{} unsupported={} parse-failure={} plan-failure={} unexpected-plan-success={} wrong-number-of-rows-inserted={} inference-failure={} wrong-column-names={} output-failure={} bail={} success={} total={}",
            status,
            self.0[0],
            self.0[1],
            self.0[2],
            self.0[3],
            self.0[4],
            self.0[5],
            self.0[6],
            self.0[7],
            self.0[8],
            self.0[9],
            total,
        )
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
            "wrong_number_of_rows_inserted": self.0[4],
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
    // Hold a reference to the dataflow workers threads to avoid dropping them
    // too early. Order is important here! They must appear before the runtime,
    // so that the runtime is *dropped* after the workers. We rely on the
    // runtime to send the shutdown signal to the workers; if the runtime goes
    // away first, the workers won't ever get the shutdown signal.
    _dataflow_workers: Box<dyn Drop>,
    _runtime: tokio::runtime::Runtime,
    postgres: Postgres,
    pub(crate) planner: Planner,
    pub(crate) session: Session,
    pub(crate) coord: Coordinator<tokio::net::UnixStream>,
    pub(crate) conn_id: u32,
    current_timestamp: u64,
    local_input_uuids: HashMap<String, Uuid>,
    local_input_mux: Mux<Uuid, LocalInput>,
}

fn format_row(
    row: &[Datum],
    col_types: &[ColumnType],
    slt_types: &[Type],
    mode: Mode,
    sort: &Sort,
) -> Vec<String> {
    let row =
        izip!(slt_types, col_types, row).map(|(slt_typ, col_typ, datum)| match (slt_typ, datum) {
            // the documented formatting rules in https://www.sqlite.org/sqllogictest/doc/trunk/about.wiki
            (_, Datum::Null) => "NULL".to_owned(),
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
                    string.to_owned()
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
            (Type::Real, Datum::Int64(i)) => format!("{:.3}", i),
            (Type::Text, Datum::Int64(i)) => format!("{}", i),
            (Type::Text, Datum::Float64(f)) => format!("{:.3}", f),
            (Type::Text, Datum::Date(d)) => d.to_string(),
            (Type::Text, Datum::Timestamp(d)) => d.to_string(),
            (Type::Text, Datum::Interval(iv)) => iv.to_string(),
            other => panic!("Don't know how to format {:?}", other),
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
        let postgres = Postgres::open_and_erase()?;
        let logging_config = None;
        let planner = Planner::new(logging_config);
        let session = Session::default();
        let local_input_mux = Mux::default();
        let process_id = 0;
        let (switchboard, runtime) = comm::Switchboard::local()?;
        let dataflow_workers = dataflow::serve(
            vec![None],
            NUM_TIMELY_WORKERS,
            process_id,
            switchboard.clone(),
            runtime.executor(),
            local_input_mux.clone(),
            None, // disable logging
        )
        .unwrap();

        let coord = Coordinator::new(
            switchboard,
            NUM_TIMELY_WORKERS,
            None, // disable logging
        );

        Ok(State {
            _runtime: runtime,
            postgres,
            planner,
            session,
            coord,
            conn_id: 1,
            _dataflow_workers: Box::new(dataflow_workers),
            current_timestamp: 1,
            local_input_uuids: HashMap::new(),
            local_input_mux,
        })
    }

    fn run_record<'a>(&mut self, record: &'a Record) -> Result<Outcome<'a>, failure::Error> {
        match &record {
            Record::Statement {
                should_run,
                rows_inserted: expected_rows_inserted,
                sql,
            } => {
                if !should_run {
                    // sure, we totally checked it
                    return Ok(Outcome::Success);
                }

                lazy_static! {
                    static ref INDEX_STATEMENT_REGEX: Regex =
                        Regex::new("^(CREATE (UNIQUE )?INDEX|DROP INDEX|REINDEX)").unwrap();
                }
                if INDEX_STATEMENT_REGEX.is_match(sql) {
                    // sure, we totally made you an index
                    return Ok(Outcome::Success);
                }

                // parse statement
                let statements = match SqlParser::parse_sql(&AnsiDialect {}, sql.to_string()) {
                    Ok(statements) => statements,
                    Err(error) => return Ok(Outcome::ParseFailure { error }),
                };
                let statement = match &*statements {
                    [] => bail!("Got zero statements?"),
                    [statement] => statement,
                    _ => bail!("Got multiple statements: {:?}", statements),
                };

                match statement {
                    // run through postgres and send diffs to materialize
                    Statement::CreateTable { .. }
                    | Statement::Drop {
                        object_type: ObjectType::Table,
                        ..
                    }
                    | Statement::Delete { .. }
                    | Statement::Insert { .. }
                    | Statement::Update { .. } => {
                        let outcome = match self
                            .postgres
                            .run_statement(&sql, statement)
                            .context("Unsupported by postgres")
                        {
                            Ok(outcome) => outcome,
                            Err(error) => {
                                return Ok(Outcome::Unsupported {
                                    error: error.into(),
                                });
                            }
                        };
                        let rows_inserted;
                        match outcome {
                            postgres::Outcome::Created(name, typ) => {
                                let uuid = Uuid::new_v4();
                                self.local_input_uuids.insert(name.clone(), uuid);
                                {
                                    self.local_input_mux.write().unwrap().channel(uuid).unwrap();
                                }
                                let dataflow = Dataflow::Source(Source {
                                    name,
                                    connector: SourceConnector::Local(LocalSourceConnector {
                                        uuid,
                                    }),
                                    typ,
                                    pkey_indices: Vec::new(),
                                });
                                self.planner.dataflows.insert(dataflow.clone())?;
                                self.coord.create_dataflows(vec![dataflow]);
                                {
                                    self.local_input_mux
                                        .read()
                                        .unwrap()
                                        .sender(&uuid)
                                        .unwrap()
                                        .unbounded_send(LocalInput::Watermark(
                                            self.current_timestamp,
                                        ))
                                        .unwrap();
                                }
                                rows_inserted = None;
                            }
                            postgres::Outcome::Dropped(names) => {
                                let mut dataflows = vec![];
                                // the only reason we would use RemoveMode::Restrict is to test the error handling, and we already decided to bailed out on !should_run earlier
                                for name in &names {
                                    self.local_input_uuids.remove(name);
                                    self.planner.dataflows.remove(
                                        name,
                                        RemoveMode::Cascade,
                                        &mut dataflows,
                                    )?;
                                }
                                self.coord.drop_dataflows(names);
                                rows_inserted = None;
                            }
                            postgres::Outcome::Changed(name, _typ, updates) => {
                                let updates = updates
                                    .into_iter()
                                    .map(|(row, diff)| Update {
                                        row,
                                        diff,
                                        timestamp: self.current_timestamp,
                                    })
                                    .collect::<Vec<_>>();
                                let updated_uuid = self
                                    .local_input_uuids
                                    .get(&name)
                                    .expect("Unknown table in update");
                                {
                                    let mux = self.local_input_mux.read().unwrap();
                                    for (uuid, sender) in mux.senders() {
                                        if uuid == updated_uuid {
                                            sender
                                                .unbounded_send(LocalInput::Updates(
                                                    updates.clone(),
                                                ))
                                                .unwrap();
                                        }
                                        sender
                                            .unbounded_send(LocalInput::Watermark(
                                                self.current_timestamp + 1,
                                            ))
                                            .unwrap();
                                    }
                                }
                                self.current_timestamp += 1;
                                rows_inserted = Some(updates.len());
                            }
                        }
                        match (rows_inserted, expected_rows_inserted) {
                            (None, Some(expected)) => Ok(Outcome::PlanFailure {
                                error: failure::format_err!(
                                    "Query did not insert any rows, expected {}",
                                    expected,
                                ),
                            }),
                            (Some(actual), Some(expected)) if actual != *expected => {
                                Ok(Outcome::WrongNumberOfRowsInserted {
                                    expected_count: *expected,
                                    actual_count: actual,
                                })
                            }
                            _ => Ok(Outcome::Success),
                        }
                    }

                    // just run through postgres, no diffs
                    Statement::SetVariable { .. } => {
                        match self
                            .postgres
                            .run_statement(&sql, statement)
                            .context("Unsupported by postgres")
                        {
                            Ok(_) => Ok(Outcome::Success),
                            Err(error) => Ok(Outcome::Unsupported {
                                error: error.into(),
                            }),
                        }
                    }

                    // run through materialize directly
                    Statement::Query { .. }
                    | Statement::CreateView { .. }
                    | Statement::CreateSource { .. }
                    | Statement::CreateSink { .. }
                    | Statement::Drop { .. } => {
                        match self
                            .planner
                            .handle_command(&mut self.session, sql.to_string())
                        {
                            Ok(plan) => {
                                self.coord.sequence_plan(
                                    plan,
                                    self.conn_id,
                                    Some(self.current_timestamp - 1),
                                );
                            }
                            Err(error) => return Ok(Outcome::PlanFailure { error }),
                        };
                        Ok(Outcome::Success)
                    }

                    _ => bail!("Unsupported statement: {:?}", statement),
                }
            }
            Record::Query { sql, output } => {
                // get statement
                let statements = match SqlParser::parse_sql(&AnsiDialect {}, sql.to_string()) {
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
                    Statement::CreateView { .. } | Statement::Query { .. } => (),
                    _ => {
                        if output.is_err() {
                            // We're not interested in testing our hacky handling of INSERT etc
                            return Ok(Outcome::Success);
                        }
                    }
                }

                // get plan
                let plan = match self
                    .planner
                    .handle_command(&mut self.session, sql.to_string())
                {
                    Ok(plan) => plan,
                    Err(error) => {
                        // TODO(jamii) check error messages, once ours stabilize
                        if output.is_err() {
                            return Ok(Outcome::Success);
                        } else {
                            let error_string = format!("{}", error);
                            if error_string.contains("supported")
                                || error_string.contains("overload")
                            {
                                // this is a failure, but it's caused by lack of support rather than by bugs
                                return Ok(Outcome::Unsupported { error });
                            } else {
                                return Ok(Outcome::PlanFailure { error });
                            }
                        }
                    }
                };

                // send plan, read response
                let (typ, rows_rx) = match self.coord.sequence_plan(
                    plan,
                    self.conn_id,
                    Some(self.current_timestamp - 1),
                ) {
                    SqlResponse::SendRows { typ, rx } => (typ, rx),
                    other => {
                        return Ok(Outcome::PlanFailure {
                            error: failure::format_err!(
                                "Query did not result in SendRows, instead got {:?}",
                                other
                            ),
                        });
                    }
                };

                // get actual output
                let raw_output = rows_rx.wait()?;

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
                // sqllogictest coerces the output into the expected type, so `expected_types` is often wrong :(
                // but at least it will be the correct length
                let inferred_types = &typ.column_types;
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
                    if row.len() != inferred_types.len() {
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
                    let inferred_column_names = typ
                        .column_types
                        .iter()
                        .map(|t| t.name.clone())
                        .collect::<Vec<_>>();
                    if expected_column_names
                        .iter()
                        .map(|s| Some(&**s))
                        .collect::<Vec<_>>()
                        != inferred_column_names
                            .iter()
                            .map(|n| n.as_ref().map(|s| &**s))
                            .collect::<Vec<_>>()
                    {
                        return Ok(Outcome::WrongColumnNames {
                            expected_column_names,
                            inferred_column_names,
                        });
                    }
                }

                // format output
                let mut formatted_rows = raw_output
                    .iter()
                    .map(|row| format_row(row, &typ.column_types, &**expected_types, *mode, sort))
                    .collect::<Vec<_>>();

                // sort formatted output
                if let Sort::Row = sort {
                    formatted_rows.sort();
                }
                let mut values = formatted_rows
                    .into_iter()
                    .flat_map(|row| row)
                    .collect::<Vec<_>>();
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
            _ => Ok(Outcome::Success),
        }
    }
}

impl Drop for State {
    fn drop(&mut self) {
        self.coord.shutdown();
    }
}

const LIGHT_HORIZONTAL_RULE: &str = "────────────────────────────────────────────────────────────────────────────────────────────────────────────────────────";
const HEAVY_HORIZONTAL_RULE: &str = "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━";

pub fn run_string(source: &str, input: &str, verbosity: usize) -> Outcomes {
    let mut outcomes = Outcomes::default();
    let mut state = State::start().unwrap();
    println!("==> {}", source);
    for record in crate::parser::parse_records(&input) {
        let record = record.unwrap();

        let mut outcome = state
            .run_record(&record)
            .with_context(|err| format!("In {}:\n{}", source, err))
            .unwrap();

        // if we failed to execute a statement, running the rest of the tests in this file will probably cause false positives
        match (&record, &outcome) {
            (_, Outcome::Success) => (),
            (Record::Statement { sql, .. }, _)
                if !sql.to_lowercase().starts_with("create view")
                    && !sql.to_lowercase().starts_with("select") =>
            {
                outcome = Outcome::Bail {
                    cause: Box::new(outcome),
                };
            }
            _ => (),
        }

        // print failures in verbose mode
        match &outcome {
            Outcome::Success => {
                if verbosity >= 2 {
                    match &record {
                        Record::Statement { sql, .. } => println!("    {}", sql),
                        Record::Query { sql, .. } => println!("    {}", sql),
                        _ => (),
                    }
                }
            }
            _ => {
                if verbosity >= 1 {
                    println!("{}", HEAVY_HORIZONTAL_RULE);
                    println!("{}", outcome);
                    match &record {
                        Record::Statement { sql, .. } => {
                            println!("{}\n{}", LIGHT_HORIZONTAL_RULE, sql)
                        }
                        Record::Query { sql, .. } => println!("{}\n{}", LIGHT_HORIZONTAL_RULE, sql),
                        _ => (),
                    }
                }
            }
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
    run_string(
        &format!("{}", filename.display()),
        &input,
        verbosity,
    )
}

pub fn run_stdin(verbosity: usize) -> Outcomes {
    let mut input = String::new();
    std::io::stdin().lock().read_to_string(&mut input).unwrap();
    run_string("<stdin>", &input, verbosity)
}

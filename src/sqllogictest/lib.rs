// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

//! https://www.sqlite.org/sqllogictest/doc/trunk/about.wiki

use std::borrow::ToOwned;
use std::collections::HashMap;
use std::fmt;
use std::fs::File;
use std::io::Read;
use std::ops;
use std::path::Path;
use std::str::FromStr;

use failure::{bail, format_err, ResultExt};
use futures::stream::Stream;
use itertools::izip;
use lazy_static::lazy_static;
use regex::Regex;
use uuid::Uuid;

use materialize::dataflow;
use materialize::dataflow::{Dataflow, LocalSourceConnector, Source, SourceConnector};
use materialize::glue::*;
use materialize::sql::store::RemoveMode;
use materialize::sql::{Planner, Session};
use ore::collections::CollectionExt;
use repr::{ColumnType, Datum};
use sqlparser::ast::{ObjectType, Statement};
use sqlparser::dialect::AnsiDialect;
use sqlparser::parser::{Parser as SqlParser, ParserError as SqlParserError};

mod postgres;
use crate::postgres::Postgres;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Type {
    Text,
    Integer,
    Real,
    Bool,
    Oid,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Sort {
    No,
    Row,
    Value,
}

impl Sort {
    /// Is true if any kind of sorting should happen
    fn yes(&self) -> bool {
        use Sort::*;
        match self {
            No => false,
            Row | Value => true,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Output<'a> {
    Values(Vec<&'a str>),
    Hashed { num_values: usize, md5: &'a str },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum OwnedOutput {
    Values(Vec<String>),
    Hashed { num_values: usize, md5: String },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryOutput<'a> {
    types: Vec<Type>,
    sort: Sort,
    label: Option<&'a str>,
    column_names: Option<Vec<&'a str>>,
    mode: Mode,
    output: Output<'a>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Record<'a> {
    Statement {
        should_run: bool,
        rows_inserted: Option<usize>,
        sql: &'a str,
    },
    Query {
        sql: &'a str,
        output: Result<QueryOutput<'a>, &'a str>,
    },
    HashThreshold {
        threshold: u64,
    },
    Skip,
    Halt,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    /// In `Standard` mode, expected query output is formatted so that every
    /// value is always on its own line, like so:
    ///
    ///
    ///    query II
    ///    SELECT * FROM VALUES (1, 2), (3, 4)
    ///    ----
    ///    1
    ///    2
    ///    3
    ///    4
    ///
    /// Row boundaries are not visually represented, but they can be inferred
    /// because the number of columns per row is specified by the `query`
    /// directive.
    Standard,
    /// In `Cockroach` mode, expected query output is formatted so that rows
    /// can contain multiple whitespace-separated columns:
    ///
    ///    query II
    ///    SELECT * FROM VALUES (1, 2), (3, 4)
    ///    ----
    ///    1 2
    ///    3 4
    ///
    /// This formatting, while easier to parse visually, is thoroughly
    /// frustrating when column values contain whitespace, e.g., strings like
    /// "one two", as there is no way to know where the column boundaries are.
    /// We jump through some hoops to make this work. You might want to
    /// refer to this upstream Cockroach commit [0] for additional details.
    ///
    /// [0]: https://github.com/cockroachdb/cockroach/commit/75c3023ec86a76fe6fb60fe1c6f00752b9784801
    Cockroach,
}

fn split_at<'a>(input: &mut &'a str, sep: &Regex) -> Result<&'a str, failure::Error> {
    match sep.find(input) {
        Some(found) => {
            let result = &input[..found.start()];
            *input = &input[found.end()..];
            Ok(result)
        }
        None => bail!("Couldn't split {:?} at {:?}", input, sep),
    }
}

fn parse_types(input: &str) -> Result<Vec<Type>, failure::Error> {
    input
        .chars()
        .map(|char| {
            Ok(match char {
                'T' => Type::Text,
                'I' => Type::Integer,
                'R' => Type::Real,
                'B' => Type::Bool,
                'O' => Type::Oid,
                _ => bail!("Unexpected type char {} in: {}", char, input),
            })
        })
        .collect()
}

fn parse_sql<'a>(input: &mut &'a str) -> Result<&'a str, failure::Error> {
    lazy_static! {
        static ref QUERY_OUTPUT_REGEX: Regex = Regex::new("(\r?\n----\r?\n?)|$").unwrap();
    }
    split_at(input, &QUERY_OUTPUT_REGEX)
}

lazy_static! {
    static ref WHITESPACE_REGEX: Regex = Regex::new(r"\s+").unwrap();
}

pub fn parse_record<'a>(
    mode: &mut Mode,
    mut input: &'a str,
) -> Result<Option<Record<'a>>, failure::Error> {
    if input == "" {
        // must have just been a bunch of comments
        return Ok(None);
    }

    lazy_static! {
        static ref COMMENT_AND_LINE_REGEX: Regex = Regex::new("(#[^\n]*)?\r?(\n|$)").unwrap();
    }
    let first_line = split_at(&mut input, &COMMENT_AND_LINE_REGEX)?.trim();

    if first_line == "" {
        // query starts on the next line
        return parse_record(mode, input);
    }

    let mut words = first_line.split(' ').peekable();
    match words.next().unwrap() {
        "statement" => {
            let (should_run, rows_inserted) = match words.next() {
                Some("count") => (
                    true,
                    Some(
                        words
                            .next()
                            .ok_or_else(|| format_err!("missing insert count"))?
                            .parse::<usize>()
                            .map_err(|err| format_err!("parsing insert count: {}", err))?,
                    ),
                ),
                Some("ok") | Some("OK") => (true, None),
                Some("error") => (false, None),
                Some(other) => bail!("invalid should_run in: {}", other),
                None => (true, None),
            };
            let sql = parse_sql(&mut input)?;
            if input != "" {
                bail!("leftover input: {}", input)
            }
            Ok(Some(Record::Statement {
                should_run,
                rows_inserted,
                sql,
            }))
        }
        "query" => {
            if words.peek() == Some(&"error") {
                let error = &first_line[12..]; // everything after "query error "
                let sql = input;
                Ok(Some(Record::Query {
                    sql,
                    output: Err(error),
                }))
            } else {
                let types = parse_types(
                    words
                        .next()
                        .ok_or_else(|| format_err!("missing types in: {}", first_line))?,
                )?;
                let mut sort = Sort::No;
                let mut check_column_names = false;
                if let Some(options) = words.next() {
                    for option in options.split(',') {
                        match option {
                            "nosort" => sort = Sort::No,
                            "rowsort" => sort = Sort::Row,
                            "valuesort" => sort = Sort::Value,
                            "colnames" => check_column_names = true,
                            other => {
                                if other.starts_with("partialsort") {
                                    // TODO(jamii) https://github.com/cockroachdb/cockroach/blob/d2f7fbf5dd1fc1a099bbad790a2e1f7c60a66cc3/pkg/sql/logictest/logic.go#L153
                                    // partialsort has comma-separated arguments so our parsing is totally broken
                                    // luckily it always comes last in the existing tests, so we can just bail out for now
                                    sort = Sort::Row;
                                    break;
                                } else {
                                    bail!("Unrecognized option {:?} in {:?}", other, options);
                                }
                            }
                        };
                    }
                }
                let label = words.next();
                let sql = parse_sql(&mut input)?;
                let column_names = if check_column_names {
                    Some(
                        split_at(&mut input, &LINE_REGEX)?
                            .split(' ')
                            .filter(|s| !s.is_empty())
                            .collect(),
                    )
                } else {
                    None
                };
                lazy_static! {
                    static ref LINE_REGEX: Regex = Regex::new("\r?(\n|$)").unwrap();
                    static ref HASH_REGEX: Regex =
                        Regex::new(r"(\S+) values hashing to (\S+)").unwrap();
                }
                let output = match HASH_REGEX.captures(input) {
                    Some(captures) => Output::Hashed {
                        num_values: captures.get(1).unwrap().as_str().parse::<usize>()?,
                        md5: captures.get(2).unwrap().as_str(),
                    },
                    None => {
                        let mut vals: Vec<_> = input.trim().lines().collect();
                        if *mode == Mode::Cockroach {
                            let mut rows = vec![];
                            for line in vals {
                                // Split on whitespace to normalize multiple
                                // spaces to one space. This happens
                                // unconditionally in Cockroach mode, regardless
                                // of the sort option.
                                let cols: Vec<_> = line.split_whitespace().collect();
                                if sort != Sort::No && cols.len() != types.len() {
                                    // We can't check this condition for
                                    // Sort::No, because some tests use strings
                                    // with whitespace that look like extra
                                    // columns. (Note that these tests never
                                    // use any of the sorting options.)
                                    bail!(
                                        "col len ({}) did not match declared col len ({})",
                                        cols.len(),
                                        types.len()
                                    );
                                }
                                rows.push(cols);
                            }
                            if sort == Sort::Row {
                                rows.sort();
                            }
                            vals = rows.into_iter().flat_map(|row| row).collect();
                            if sort == Sort::Value {
                                vals.sort();
                            }
                        }
                        Output::Values(vals)
                    }
                };
                Ok(Some(Record::Query {
                    sql,
                    output: Ok(QueryOutput {
                        types,
                        sort,
                        label,
                        column_names,
                        mode: *mode,
                        output,
                    }),
                }))
            }
        }
        "hash-threshold" => {
            let threshold = words
                .next()
                .ok_or_else(|| format_err!("missing threshold in: {}", first_line))?
                .parse::<u64>()
                .map_err(|err| format_err!("invalid threshold ({}) in: {}", err, first_line))?;
            if input != "" {
                bail!("leftover input: {}", input)
            }
            Ok(Some(Record::HashThreshold { threshold }))
        }

        // we'll follow the postgresql version of all these tests
        "skipif" => {
            match words.next().unwrap() {
                "postgresql" => Ok(None),
                _ => {
                    // query starts on the next line
                    parse_record(mode, input)
                }
            }
        }
        "onlyif" => {
            match words.next().unwrap() {
                "postgresql" => {
                    // query starts on the next line
                    parse_record(mode, input)
                }
                _ => Ok(None),
            }
        }

        "halt" => Ok(Some(Record::Halt)),

        // this is some cockroach-specific thing, we don't care
        "subtest" | "user" | "kv-batch-size" => Ok(None),

        "mode" => {
            *mode = match words.next() {
                Some("cockroach") => Mode::Cockroach,
                Some("standard") | Some("sqlite") => Mode::Standard,
                other => bail!("unknown parse mode: {:?}", other),
            };
            Ok(None)
        }

        other => bail!("Unexpected start of record: {}", other),
    }
}

pub fn parse_records(input: &str) -> impl Iterator<Item = Result<Record, failure::Error>> {
    lazy_static! {
        static ref DOUBLE_LINE_REGEX: Regex = Regex::new("(\n|\r\n)(\n|\r\n)").unwrap();
    }
    let mut mode = Mode::Standard;
    DOUBLE_LINE_REGEX
        .split(input)
        .map(str::trim)
        .filter(|lines| *lines != "")
        .filter_map(move |lines| parse_record(&mut mode, lines).transpose())
        .take_while(|record| match record {
            Ok(Record::Halt) => false,
            _ => true,
        })
}

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
    },
    WrongColumnNames {
        expected_column_names: &'a Vec<&'a str>,
        inferred_column_names: Vec<Option<String>>,
    },
    OutputFailure {
        expected_output: &'a Output<'a>,
        actual_raw_output: Vec<Vec<Datum>>,
        actual_output: OwnedOutput,
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
            Unsupported { error } => write!(f, "Unsupported: {0} ({0:?})", error),
            ParseFailure { error } => write!(f, "ParseFailure: {:?}", error),
            PlanFailure { error } => write!(f, "PlanFailure: {0} ({0:?})", error),
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
            } => write!(
                f,
                "Inference Failure!{}expected types: {}{}inferred types: {}",
                INDENT,
                expected_types
                    .iter()
                    .map(|s| format!("{:?}", s))
                    .collect::<Vec<_>>()
                    .join(" "),
                INDENT,
                inferred_types
                    .iter()
                    .map(|s| format!("{:?}", s))
                    .collect::<Vec<_>>()
                    .join(" ")
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
                "OutputFailure!{}expected: {:?}{}actually: {:?}{}actual raw: {:?}",
                INDENT, expected_output, INDENT, actual_output, INDENT, actual_raw_output
            ),
            Bail { cause } => write!(f, "Bail! caused by: {}", cause),
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
        write!(
            f,
            "unsupported={} parse-failure={} plan-failure={} unexpected-plan-success={} wrong-number-of-rows-inserted={} inference-failure={} wrong-column-names={} output-failure={} bail={} success={} total={}",
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
            self.0.iter().sum::<usize>(),
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

trait RecordRunner {
    fn run_record<'a>(&mut self, record: &'a Record) -> Result<Outcome<'a>, failure::Error>;
}

const NUM_TIMELY_WORKERS: usize = 3;

struct FullState {
    postgres: Postgres,
    planner: Planner,
    session: Session,
    dataflow_command_sender: UnboundedSender<(DataflowCommand, CommandMeta)>,
    worker0_thread: std::thread::Thread,
    // this is only here to avoid dropping it too early
    _dataflow_workers: Box<dyn Drop>,
    current_timestamp: u64,
    local_input_uuids: HashMap<String, Uuid>,
    local_input_mux: LocalInputMux,
    dataflow_results_mux: DataflowResultsMux,
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
            (Type::Integer, Datum::Float64(f)) => format!("{:.0}", f.trunc()),
            (Type::Integer, Datum::String(_)) => "0".to_owned(),
            (Type::Integer, Datum::False) => "0".to_owned(),
            (Type::Integer, Datum::True) => "1".to_owned(),
            (Type::Real, Datum::Int64(i)) => format!("{:.3}", i),
            (Type::Text, Datum::Int64(i)) => format!("{}", i),
            (Type::Text, Datum::Float64(f)) => format!("{:.3}", f),
            other => panic!("Don't know how to format {:?}", other),
        });
    if mode == Mode::Cockroach && sort.yes() {
        row.flat_map(|s| {
            s.split_whitespace()
                .map(ToString::to_string)
                .collect::<Vec<_>>()
        })
        .collect()
    } else {
        row.collect()
    }
}

impl FullState {
    fn start() -> Result<Self, failure::Error> {
        let postgres = Postgres::open_and_erase()?;
        let planner = Planner::default();
        let session = Session::default();
        let (dataflow_command_sender, dataflow_command_receiver) = unbounded();
        let local_input_mux = LocalInputMux::default();
        let dataflow_results_mux = DataflowResultsMux::default();
        let dataflow_workers = dataflow::serve(
            dataflow_command_receiver,
            local_input_mux.clone(),
            dataflow::DataflowResultsHandler::Local(dataflow_results_mux.clone()),
            timely::Configuration::Process(NUM_TIMELY_WORKERS),
            None, // disable logging
        )
        .unwrap();

        let worker0_thread = dataflow_workers.guards().into_first().thread().clone();
        Ok(FullState {
            postgres,
            planner,
            session,
            dataflow_command_sender,
            _dataflow_workers: Box::new(dataflow_workers),
            current_timestamp: 1,
            local_input_uuids: HashMap::new(),
            local_input_mux,
            dataflow_results_mux,
            worker0_thread,
        })
    }

    fn send_dataflow_command(
        &self,
        dataflow_command: DataflowCommand,
    ) -> UnboundedReceiver<DataflowResults> {
        let uuid = Uuid::new_v4();
        let receiver = {
            let mut mux = self.dataflow_results_mux.write().unwrap();
            mux.channel(uuid).unwrap();
            mux.receiver(&uuid).unwrap()
        };
        self.dataflow_command_sender
            .unbounded_send((
                dataflow_command.clone(),
                CommandMeta {
                    connection_uuid: uuid,
                },
            ))
            .unwrap();
        self.worker0_thread.unpark();
        receiver
    }

    fn receive_peek_results(
        &self,
        receiver: UnboundedReceiver<DataflowResults>,
    ) -> Vec<Vec<Datum>> {
        let mut results = vec![];
        let mut receiver = receiver.wait();
        for _ in 0..NUM_TIMELY_WORKERS {
            results.append(&mut receiver.next().unwrap().unwrap().unwrap_peeked());
        }
        results
    }
}

impl RecordRunner for FullState {
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

                // we don't support non-materialized views
                let sql = sql.replace("CREATE VIEW", "CREATE MATERIALIZED VIEW");

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
                                });
                                self.planner.dataflows.insert(dataflow.clone())?;
                                let _receiver = self.send_dataflow_command(
                                    DataflowCommand::CreateDataflows(vec![dataflow]),
                                );
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
                                let _receiver = self
                                    .send_dataflow_command(DataflowCommand::DropDataflows(names));
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
                                    for (uuid, sender) in &mux.senders {
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

                    // run through materialize directly
                    Statement::Query { .. }
                    | Statement::CreateView { .. }
                    | Statement::CreateSource { .. }
                    | Statement::CreateSink { .. }
                    | Statement::Drop { .. } => {
                        let (_response, dataflow_command) = match self
                            .planner
                            .handle_command(&mut self.session, sql.to_owned())
                        {
                            Ok((response, dataflow_command)) => (response, dataflow_command),
                            Err(error) => return Ok(Outcome::PlanFailure { error }),
                        };
                        // make sure we peek at the correct time
                        let dataflow_command = match dataflow_command {
                            Some(DataflowCommand::Peek { source, .. }) => {
                                Some(DataflowCommand::Peek {
                                    source,
                                    when: PeekWhen::AtTimestamp(self.current_timestamp - 1),
                                })
                            }
                            other => other,
                        };
                        let _receiver = self.send_dataflow_command(dataflow_command.unwrap());
                        Ok(Outcome::Success)
                    }

                    _ => bail!("Unsupported statement: {:?}", statement),
                }
            }
            Record::Query { sql, output } => {
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

                let (typ, dataflow_command, immediate_rows) = match self
                    .planner
                    .handle_command(&mut self.session, sql.to_string())
                {
                    Ok((SqlResponse::Peeking { typ }, dataflow_command)) => {
                        (typ, dataflow_command, None)
                    }
                    // impossible for there to be a dataflow command
                    Ok((SqlResponse::SendRows { typ, rows }, None)) => (typ, None, Some(rows)),
                    Ok(other) => {
                        return Ok(Outcome::PlanFailure {
                            error: failure::format_err!(
                                "Query did not result in Peeking, instead got {:?}",
                                other
                            ),
                        });
                    }
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

                match output {
                    Err(expected_error) => Ok(Outcome::UnexpectedPlanSuccess { expected_error }),
                    Ok(QueryOutput {
                        sort,
                        types: expected_types,
                        column_names: expected_column_names,
                        output: expected_output,
                        mode,
                        ..
                    }) => {
                        let inferred_types = &typ.column_types;
                        // sqllogictest coerces the output into the expected type, so expected_type is often wrong :(
                        // but at least it will be the correct length
                        if inferred_types.len() != expected_types.len() {
                            return Ok(Outcome::InferenceFailure {
                                expected_types,
                                inferred_types: inferred_types.to_vec(),
                            });
                        }

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

                        // make sure we peek at the correct time
                        let (mut formatted_rows, raw_output) = match immediate_rows {
                            Some(rows) => (
                                rows.iter()
                                    .map(|row| {
                                        format_row(
                                            row,
                                            &typ.column_types,
                                            &**expected_types,
                                            *mode,
                                            sort,
                                        )
                                    })
                                    .collect::<Vec<_>>(),
                                rows,
                            ),
                            None => {
                                let dataflow_command = match dataflow_command {
                                    Some(DataflowCommand::Peek { source, .. }) => {
                                        Some(DataflowCommand::Peek {
                                            source,
                                            when: PeekWhen::AtTimestamp(self.current_timestamp - 1),
                                        })
                                    }
                                    other => other,
                                };
                                let receiver =
                                    self.send_dataflow_command(dataflow_command.unwrap());
                                let results = self.receive_peek_results(receiver);

                                (
                                    results
                                        .iter()
                                        .map(|row| {
                                            format_row(
                                                row,
                                                &typ.column_types,
                                                &**expected_types,
                                                *mode,
                                                sort,
                                            )
                                        })
                                        .collect::<Vec<_>>(),
                                    results,
                                )
                            }
                        };

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

                        match expected_output {
                            Output::Values(expected_values) => {
                                if values != *expected_values {
                                    return Ok(Outcome::OutputFailure {
                                        expected_output,
                                        actual_raw_output: raw_output,
                                        actual_output: OwnedOutput::Values(values),
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
                                        actual_output: OwnedOutput::Hashed {
                                            num_values: values.len(),
                                            md5,
                                        },
                                    });
                                }
                            }
                        }

                        Ok(Outcome::Success)
                    }
                }
            }
            _ => Ok(Outcome::Success),
        }
    }
}

impl Drop for FullState {
    fn drop(&mut self) {
        drop(self.dataflow_command_sender.unbounded_send((
            DataflowCommand::Shutdown,
            CommandMeta {
                connection_uuid: Uuid::nil(),
            },
        )));

        self.worker0_thread.unpark();
    }
}

struct OnlyParseState;

impl OnlyParseState {
    fn start() -> Self {
        OnlyParseState
    }
}

impl RecordRunner for OnlyParseState {
    fn run_record<'a>(&mut self, record: &'a Record) -> Result<Outcome<'a>, failure::Error> {
        match &record {
            Record::Statement {
                should_run, sql, ..
            } => {
                lazy_static! {
                    static ref INDEX_STATEMENT_REGEX: Regex =
                        Regex::new("^(CREATE (UNIQUE )?INDEX|DROP INDEX|REINDEX)").unwrap();
                }
                if INDEX_STATEMENT_REGEX.is_match(sql) {
                    // sure, we totally made you an index...
                    return Ok(Outcome::Success);
                }

                // we don't support non-materialized views
                let sql = sql.replace("CREATE VIEW", "CREATE MATERIALIZED VIEW");

                if let Err(error) = SqlParser::parse_sql(&AnsiDialect {}, sql.to_string()) {
                    if *should_run {
                        return Ok(Outcome::ParseFailure { error });
                    } else {
                        return Ok(Outcome::Success);
                    }
                }
                Ok(Outcome::Success)
            }
            Record::Query { sql, .. } => {
                if let Err(error) = SqlParser::parse_sql(&AnsiDialect {}, sql.to_string()) {
                    return Ok(Outcome::ParseFailure { error });
                }
                Ok(Outcome::Success)
            }
            _ => Ok(Outcome::Success),
        }
    }
}

pub fn run_string(source: &str, input: &str, verbosity: usize, only_parse: bool) -> Outcomes {
    let mut outcomes = Outcomes::default();
    let mut state: Box<dyn RecordRunner> = if only_parse {
        Box::new(OnlyParseState::start())
    } else {
        Box::new(FullState::start().unwrap())
    };
    if verbosity >= 1 {
        println!("==> {}", source);
    }
    for record in parse_records(&input) {
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
                if verbosity >= 3 {
                    match &record {
                        Record::Statement { sql, .. } => println!("    {}", sql),
                        Record::Query { sql, .. } => println!("    {}", sql),
                        _ => (),
                    }
                }
            }
            _ => {
                if verbosity >= 2 {
                    match &record {
                        Record::Statement { sql, .. } => println!("    error for: {}", sql),
                        Record::Query { sql, .. } => println!("    error for: {}", sql),
                        _ => (),
                    }
                    println!("    {}", outcome);
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

pub fn run_file(filename: &Path, verbosity: usize, only_parse: bool) -> Outcomes {
    let mut input = String::new();
    File::open(filename)
        .unwrap()
        .read_to_string(&mut input)
        .unwrap();
    run_string(
        &format!("{}", filename.display()),
        &input,
        verbosity,
        only_parse,
    )
}

pub fn run_stdin(verbosity: usize, only_parse: bool) -> Outcomes {
    let mut input = String::new();
    std::io::stdin().lock().read_to_string(&mut input).unwrap();
    run_string("<stdin>", &input, verbosity, only_parse)
}

pub fn fuzz(sqls: &str) {
    let mut state = FullState::start().unwrap();
    for sql in sqls.split(';') {
        if let Ok((sql_response, dataflow_command)) = state
            .planner
            .handle_command(&mut state.session, sql.to_owned())
        {
            if let Some(dataflow_command) = dataflow_command {
                let receiver = state.send_dataflow_command(dataflow_command);
                if let SqlResponse::Peeking { typ } = sql_response {
                    for row in state.receive_peek_results(receiver) {
                        for (typ, datum) in typ.column_types.iter().zip(row.into_iter()) {
                            assert!(datum.is_instance_of(typ));
                        }
                    }
                }
            }
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    use std::fs::File;
    use std::io::Read;

    use walkdir::WalkDir;

    #[test]
    fn fuzz_artifacts() {
        let mut input = String::new();
        for entry in WalkDir::new("../../fuzz/artifacts/fuzz_sqllogictest/") {
            let entry = entry.unwrap();
            if entry.path().is_file() && entry.file_name() != ".gitignore" {
                input.clear();
                File::open(&entry.path())
                    .unwrap()
                    .read_to_string(&mut input)
                    .unwrap();
                fuzz(&input);
            }
        }
    }
}

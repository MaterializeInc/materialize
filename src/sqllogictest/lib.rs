// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

//! https://www.sqlite.org/sqllogictest/doc/trunk/about.wiki

use std::fmt;
use std::fs::File;
use std::io::Read;
use std::ops;
use std::path::Path;
use std::str::FromStr;

use failure::{bail, format_err, ResultExt};
use futures::stream::Stream;
use lazy_static::lazy_static;
use regex::Regex;
use uuid::Uuid;

use materialize::clock::Clock;
use materialize::dataflow;
use materialize::glue::*;
use materialize::repr::{Datum, FType};
use materialize::sql::Planner;
use sqlparser::dialect::AnsiSqlDialect;
use sqlparser::sqlparser::Parser;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Type {
    Text,
    Integer,
    Real,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Sort {
    No,
    Row,
    Value,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Output<'a> {
    Values(Vec<&'a str>),
    Hashed { num_values: usize, md5: &'a str },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Record<'a> {
    Statement {
        should_run: bool,
        sql: &'a str,
    },
    Query {
        types: Vec<Type>,
        sort: Sort,
        label: Option<&'a str>,
        sql: &'a str,
        output: Output<'a>,
    },
    HashThreshold {
        threshold: u64,
    },
    Skip,
    Halt,
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

pub fn parse_record(mut input: &str) -> Result<Option<Record>, failure::Error> {
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
        return parse_record(input);
    }

    let mut words = first_line.split(' ');
    match words.next().unwrap() {
        "statement" => {
            let should_run = match words
                .next()
                .ok_or_else(|| format_err!("missing should_run in: {}", first_line))?
            {
                "ok" => true,
                "error" => false,
                other => bail!("invalid should_run in: {}", other),
            };
            let sql = parse_sql(&mut input)?;
            if input != "" {
                bail!("leftover input: {}", input)
            }
            Ok(Some(Record::Statement { should_run, sql }))
        }
        "query" => {
            let types = parse_types(
                words
                    .next()
                    .ok_or_else(|| format_err!("missing types in: {}", first_line))?,
            )?;
            let sort = match words
                .next()
                .ok_or_else(|| format_err!("missing sort in: {}", first_line))?
            {
                "nosort" => Sort::No,
                "rowsort" => Sort::Row,
                "valuesort" => Sort::Value,
                other => bail!("Unknown sort option: {}", other),
            };
            let label = words.next();
            let sql = parse_sql(&mut input)?;
            lazy_static! {
                static ref HASH_REGEX: Regex =
                    Regex::new(r"(\S+) values hashing to (\S+)").unwrap();
            }
            let output = match HASH_REGEX.captures(input) {
                Some(captures) => Output::Hashed {
                    num_values: captures.get(1).unwrap().as_str().parse::<usize>()?,
                    md5: captures.get(2).unwrap().as_str(),
                },
                None => Output::Values(input.trim().lines().collect()),
            };
            Ok(Some(Record::Query {
                types,
                sort,
                label,
                sql,
                output,
            }))
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
                    parse_record(input)
                }
            }
        }
        "onlyif" => {
            match words.next().unwrap() {
                "postgresql" => {
                    // query starts on the next line
                    parse_record(input)
                }
                _ => Ok(None),
            }
        }

        "halt" => Ok(Some(Record::Halt)),
        other => bail!("Unexpected start of record: {}", other),
    }
}

pub fn parse_records(input: &str) -> impl Iterator<Item = Result<Record, failure::Error>> {
    lazy_static! {
        static ref DOUBLE_LINE_REGEX: Regex = Regex::new("(\n|\r\n)(\n|\r\n)").unwrap();
    }
    DOUBLE_LINE_REGEX
        .split(input)
        .map(str::trim)
        .filter(|lines| *lines != "")
        .filter_map(|lines| parse_record(lines).transpose())
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
        error: sqlparser::sqlparser::ParserError,
    },
    PlanFailure {
        error: failure::Error,
    },
    InferenceFailure {
        expected_types: &'a [Type],
        inferred_types: Vec<FType>,
    },
    OutputFailure {
        expected_output: &'a Output<'a>,
        actual_output: Vec<Datum>,
    },
    Bail {
        cause: Box<Outcome<'a>>,
    },
    Success,
}

const NUM_OUTCOMES: usize = 7;

impl<'a> Outcome<'a> {
    fn code(&self) -> usize {
        match self {
            Outcome::Unsupported { .. } => 0,
            Outcome::ParseFailure { .. } => 1,
            Outcome::PlanFailure { .. } => 2,
            Outcome::InferenceFailure { .. } => 3,
            Outcome::OutputFailure { .. } => 4,
            Outcome::Bail { .. } => 5,
            Outcome::Success => 6,
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
        ]))
    }
}

impl fmt::Display for Outcomes {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "unsupported={} parse-failure={} plan-failure={} inference-failure={} output-failure={} bail={} success={} total={}",
            self.0[0],
            self.0[1],
            self.0[2],
            self.0[3],
            self.0[4],
            self.0[5],
            self.0[6],
            self.0.iter().sum::<usize>(),
        )
    }
}

impl Outcomes {
    pub fn any_failed(&self) -> bool {
        self.0[0] + self.0[6] < self.0.iter().sum::<usize>()
    }
}

trait RecordRunner {
    fn run_record<'a>(&mut self, record: &'a Record) -> Result<Outcome<'a>, failure::Error>;
}

const NUM_TIMELY_WORKERS: usize = 3;

struct FullState {
    clock: Clock,
    planner: Planner,
    dataflow_command_senders: Vec<UnboundedSender<(DataflowCommand, CommandMeta)>>,
    // this is only here to avoid dropping it too early
    _dataflow_workers: Box<Drop>,
    peek_results_mux: PeekResultsMux,
}

fn format_datum(datum: &Datum, types: &[Type]) -> Vec<String> {
    match datum {
        Datum::Tuple(datums) => types
            .iter()
            .zip(datums.iter())
            .map(|(typ, datum)| match (typ, datum) {
                (_, Datum::Null) => "NULL".to_owned(),

                (Type::Integer, Datum::Int64(i)) => format!("{}", i),
                (Type::Integer, Datum::Float64(f)) => format!("{:.0}", f.trunc()),
                // sqllogictest does some weird type coercions in practice
                (Type::Integer, Datum::String(_)) => "0".to_owned(),

                (Type::Real, Datum::Float64(f)) => format!("{:.3}", f),

                (Type::Text, Datum::String(string)) => {
                    if string.is_empty() {
                        "(empty)".to_owned()
                    } else {
                        string.to_owned()
                    }
                }
                (Type::Text, Datum::Int64(i)) => format!("{}", i),
                (Type::Text, Datum::Float64(f)) => format!("{:.3}", f),
                other => panic!("Don't know how to format {:?}", other),
            })
            .collect(),
        _ => panic!("Non-datum tuple in select output: {:?}", datum),
    }
}

impl FullState {
    fn start() -> Self {
        let clock = Clock::new();
        let planner = Planner::default();
        let (dataflow_command_senders, dataflow_command_receivers) =
            (0..NUM_TIMELY_WORKERS).map(|_| unbounded()).unzip();
        let peek_results_mux = PeekResultsMux::default();
        let dataflow_workers = dataflow::serve(
            dataflow_command_receivers,
            dataflow::PeekResultsHandler::Local(peek_results_mux.clone()),
            clock.clone(),
            NUM_TIMELY_WORKERS,
        )
        .unwrap();
        FullState {
            clock,
            planner,
            dataflow_command_senders,
            _dataflow_workers: Box::new(dataflow_workers),
            peek_results_mux,
        }
    }

    fn send_dataflow_command(
        &self,
        dataflow_command: DataflowCommand,
    ) -> UnboundedReceiver<PeekResults> {
        let timestamp = self.clock.now();
        let uuid = Uuid::new_v4();
        let receiver = self
            .peek_results_mux
            .write()
            .unwrap()
            .channel(uuid)
            .unwrap();
        for dataflow_command_sender in &self.dataflow_command_senders {
            dataflow_command_sender
                .unbounded_send((
                    dataflow_command.clone(),
                    CommandMeta {
                        connection_uuid: uuid,
                        timestamp: Some(timestamp),
                    },
                ))
                .unwrap();
        }
        receiver
    }

    fn receive_peek_results(&self, receiver: UnboundedReceiver<PeekResults>) -> Vec<Datum> {
        let mut results = vec![];
        let mut receiver = receiver.wait();
        for _ in 0..NUM_TIMELY_WORKERS {
            results.append(&mut receiver.next().unwrap().unwrap());
        }
        results
    }
}

impl RecordRunner for FullState {
    fn run_record<'a>(&mut self, record: &'a Record) -> Result<Outcome<'a>, failure::Error> {
        match &record {
            Record::Statement { should_run, sql } => {
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

                if let Err(error) = Parser::parse_sql(&AnsiSqlDialect {}, sql.to_string()) {
                    if *should_run {
                        return Ok(Outcome::ParseFailure { error });
                    } else {
                        return Ok(Outcome::Success);
                    }
                }

                let dataflow_command = match self.planner.handle_command(sql.to_string()) {
                    Ok((_, dataflow_command)) => dataflow_command,
                    Err(error) => {
                        if *should_run {
                            return Ok(Outcome::PlanFailure { error });
                        } else {
                            return Ok(Outcome::Success);
                        }
                    }
                };
                let _receiver = self.send_dataflow_command(dataflow_command.unwrap());

                Ok(Outcome::Success)
            }
            Record::Query {
                sql,
                sort,
                types: expected_types,
                output: expected_output,
                ..
            } => {
                if let Err(error) = Parser::parse_sql(&AnsiSqlDialect {}, sql.to_string()) {
                    return Ok(Outcome::ParseFailure { error });
                }

                let (typ, dataflow_command) = match self.planner.handle_command(sql.to_string()) {
                    Ok((SqlResponse::Peeking { typ }, dataflow_command)) => (typ, dataflow_command),
                    Ok(other) => {
                        return Ok(Outcome::PlanFailure {
                            error: failure::format_err!(
                                "Query did not result in Peeking, instead got {:?}",
                                other
                            ),
                        });
                    }
                    Err(error) => {
                        let error_string = format!("{}", error);
                        if error_string.contains("supported") || error_string.contains("overload") {
                            // this is a failure, but it's caused by lack of support rather than by bugs
                            return Ok(Outcome::Unsupported { error });
                        } else {
                            return Ok(Outcome::PlanFailure { error });
                        }
                    }
                };

                let inferred_types = match &typ.ftype {
                    FType::Tuple(types) => types.iter().map(|typ| &typ.ftype).collect::<Vec<_>>(),
                    other => panic!("Query with non-tuple type: {:?}", other),
                };

                // sqllogictest coerces the output into the expected type, so expected_type is often wrong :(
                // but at least it will be the correct length
                if inferred_types.len() != expected_types.len() {
                    return Ok(Outcome::InferenceFailure {
                        expected_types,
                        inferred_types: inferred_types.into_iter().cloned().collect(),
                    });
                }

                let receiver = self.send_dataflow_command(dataflow_command.unwrap());
                let results = self.receive_peek_results(receiver);

                let mut rows = results
                    .iter()
                    .map(|datum| format_datum(datum, &**expected_types))
                    .collect::<Vec<_>>();
                if let Sort::Row = sort {
                    rows.sort();
                }
                let mut values = rows.into_iter().flat_map(|row| row).collect::<Vec<_>>();
                if let Sort::Value = sort {
                    values.sort();
                }

                match expected_output {
                    Output::Values(expected_values) => {
                        if values != *expected_values {
                            return Ok(Outcome::OutputFailure {
                                expected_output,
                                actual_output: results,
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
                                actual_output: results,
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

impl Drop for FullState {
    fn drop(&mut self) {
        for dataflow_command_sender in &self.dataflow_command_senders {
            drop(dataflow_command_sender.unbounded_send((
                DataflowCommand::Shutdown,
                CommandMeta {
                    connection_uuid: Uuid::nil(),
                    timestamp: None,
                },
            )));
        }
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
            Record::Statement { should_run, sql } => {
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

                if let Err(error) = Parser::parse_sql(&AnsiSqlDialect {}, sql.to_string()) {
                    if *should_run {
                        return Ok(Outcome::ParseFailure { error });
                    } else {
                        return Ok(Outcome::Success);
                    }
                }
                Ok(Outcome::Success)
            }
            Record::Query { sql, .. } => {
                if let Err(error) = Parser::parse_sql(&AnsiSqlDialect {}, sql.to_string()) {
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
    let mut state: Box<RecordRunner> = if only_parse {
        Box::new(OnlyParseState::start())
    } else {
        Box::new(FullState::start())
    };
    if verbosity >= 1 {
        println!("==> {}", source);
    }
    let mut last_record = None;
    for record in parse_records(&input) {
        let record = record.unwrap();

        // TODO(jamii) this is a hack to workaround an issue where the first query after a bout of statements returns no output
        if let (Some(Record::Statement { .. }), Record::Query { .. }) = (&last_record, &record) {
            std::thread::sleep(std::time::Duration::from_millis(100));
        }

        if verbosity >= 3 {
            match &record {
                Record::Statement { sql, .. } => println!("{}", sql),
                Record::Query { sql, .. } => println!("{}", sql),
                _ => (),
            }
        }

        let mut outcome = state
            .run_record(&record)
            .with_context(|err| format!("In {}:\n{}", source, err))
            .unwrap();

        // if we failed to execute a statement, running the rest of the tests in this file will probably cause false positives
        match (&record, &outcome) {
            (_, Outcome::Success) => (),
            (Record::Statement { sql, .. }, _) if !sql.contains("CREATE VIEW") => {
                outcome = Outcome::Bail {
                    cause: Box::new(outcome),
                };
            }
            _ => (),
        }

        // print failures in verbose mode
        match &outcome {
            Outcome::Success => (),
            _ => {
                if verbosity >= 2 {
                    if verbosity < 3 {
                        match &record {
                            Record::Statement { sql, .. } => println!("{}", sql),
                            Record::Query { sql, .. } => println!("{}", sql),
                            _ => (),
                        }
                    }
                    println!("{:?}", outcome);
                    println!("In {}", source);
                }
            }
        }

        outcomes.0[outcome.code()] += 1;

        if let Outcome::Bail { .. } = outcome {
            break;
        }
        last_record = Some(record);
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
    let mut state = FullState::start();
    for sql in sqls.split(';') {
        if let Ok((sql_response, dataflow_command)) = state.planner.handle_command(sql.to_owned()) {
            if let Some(dataflow_command) = dataflow_command {
                let receiver = state.send_dataflow_command(dataflow_command);
                if let SqlResponse::Peeking { typ } = sql_response {
                    let types = match typ.ftype {
                        FType::Tuple(types) => types,
                        _ => panic!(),
                    };
                    for datum in state.receive_peek_results(receiver) {
                        match datum {
                            Datum::Tuple(datums) => {
                                for (typ, datum) in types.iter().zip(datums.into_iter()) {
                                    assert!(
                                        (typ.ftype == datum.ftype())
                                            || (typ.nullable && datum.is_null()),
                                        "{:?} was inferred to have type {:?}",
                                        typ.ftype,
                                        datum.ftype(),
                                    );
                                }
                            }
                            _ => panic!(),
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
            if entry.path().is_file() {
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

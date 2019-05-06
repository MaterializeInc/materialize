// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

//! https://www.sqlite.org/sqllogictest/doc/trunk/about.wiki

use std::collections::HashMap;
use std::error::Error;
use std::fmt;
use std::fs::File;
use std::io::Read;
use std::ops;
use std::path::Path;
use std::str::FromStr;

use failure::{bail, format_err, ResultExt};
use lazy_static::lazy_static;
use regex::Regex;

use materialize::repr::{FType, Type};
use sqlparser::dialect::AnsiSqlDialect;
use sqlparser::sqlast::{SQLStatement, SQLType};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Record<'a> {
    Statement {
        should_run: bool,
        sql: &'a str,
    },
    Query {
        types: Vec<FType>,
        sort: &'a str,
        label: Option<&'a str>,
        sql: &'a str,
        output: &'a str,
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

fn parse_types(input: &str) -> Result<Vec<FType>, failure::Error> {
    input
        .chars()
        .map(|char| {
            Ok(match char {
                'T' => FType::String,
                'I' => FType::Int64,
                'R' => FType::Float64,
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
            let sort = words
                .next()
                .ok_or_else(|| format_err!("missing sort in: {}", first_line))?;
            let label = words.next();
            let sql = parse_sql(&mut input)?;
            let output = input.trim();
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
        "skipif" => {
            // query starts on the next line
            parse_record(input)
        }
        "onlyif" => {
            // we probably don't want to support any db-specific query
            Ok(None)
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

#[derive(Debug, Clone)]
pub enum Outcome {
    Unsupported = 0,
    ParseFailure = 1,
    PlanFailure = 2,
    InferenceFailure = 3,
    Success = 4,
}

#[derive(Default, Debug, Eq, PartialEq)]
pub struct Outcomes([usize; (Outcome::Success as usize) + 1]);

impl Outcomes {
    pub fn total(&self) -> usize {
        self.0.iter().sum()
    }

    pub fn failed(&self) -> bool {
        self.0[Outcome::Success as usize] != self.total()
    }
}

impl ops::AddAssign<Outcomes> for Outcomes {
    fn add_assign(&mut self, rhs: Outcomes) {
        for (lhs, rhs) in self.0.iter_mut().zip(rhs.0.iter()) {
            *lhs += rhs
        }
    }
}

impl FromStr for Outcomes {
    type Err = Box<dyn Error>;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let pieces: Vec<_> = s.split(',').collect();
        if pieces.len() != 5 {
            return Err("expected-outcomes argument needs five comma-separated ints".into());
        }
        Ok(Outcomes([
            pieces[0].parse()?,
            pieces[1].parse()?,
            pieces[2].parse()?,
            pieces[3].parse()?,
            pieces[4].parse()?,
        ]))
    }
}

impl fmt::Display for Outcomes {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "unsupported={} parse-failure={} plan-failure={} \
             inference-failure={} success={} total={}",
            self.0[Outcome::Unsupported as usize],
            self.0[Outcome::ParseFailure as usize],
            self.0[Outcome::PlanFailure as usize],
            self.0[Outcome::InferenceFailure as usize],
            self.0[Outcome::Success as usize],
            self.total(),
        )
    }
}

#[derive(Default, Debug, Clone)]
pub struct State {
    table_types: HashMap<String, Type>,
}

pub fn run_record(state: &mut State, record: &Record) -> Result<Outcome, failure::Error> {
    match &record {
        Record::Statement { should_run, sql } => {
            lazy_static! {
                static ref UNSUPPORTED_STATEMENT_REGEX: Regex = Regex::new("^(CREATE (UNIQUE )?INDEX|CREATE TRIGGER|DROP INDEX|DROP TRIGGER|INSERT INTO .* SELECT|UPDATE|REINDEX|REPLACE INTO)").unwrap();
            }
            if UNSUPPORTED_STATEMENT_REGEX.is_match(sql) {
                return Ok(Outcome::Unsupported);
            }

            let parse =
                sqlparser::sqlparser::Parser::parse_sql(&AnsiSqlDialect {}, sql.to_string());

            let statement = match parse {
                Ok(ref statements) if statements.len() == 1 => statements.iter().next().unwrap(),
                _ => {
                    if *should_run {
                        return Ok(Outcome::ParseFailure);
                    } else {
                        return Ok(Outcome::Success);
                    }
                }
            };

            // this is mostly testing database constraints, which we don't support
            if !should_run {
                return Ok(Outcome::Success);
            }

            match statement {
                SQLStatement::SQLCreateTable {
                    name,
                    columns,
                    external,
                    file_format,
                    location,
                } => {
                    if *external || file_format.is_some() || location.is_some() {
                        bail!("EXTERNAL tables shouldn't appear in sqllogictest");
                    }
                    let types = columns
                        .iter()
                        .map(|column| {
                            Ok(Type {
                                name: Some(column.name.clone()),
                                ftype: match &column.data_type {
                                    SQLType::Char(_) | SQLType::Varchar(_) | SQLType::Text => {
                                        FType::String
                                    }
                                    SQLType::SmallInt | SQLType::Int | SQLType::BigInt => {
                                        FType::Int64
                                    }
                                    SQLType::Float(_) | SQLType::Real | SQLType::Double => {
                                        FType::Float64
                                    }
                                    other => bail!("Unexpected SQL type: {:?}", other),
                                },
                                nullable: column.allow_null,
                            })
                        })
                        .collect::<Result<Vec<_>, _>>()?;
                    let typ = Type {
                        name: None,
                        ftype: FType::Tuple(types),
                        nullable: false,
                    };
                    state.table_types.insert(name.to_string(), typ);
                }
                SQLStatement::SQLDropTable { names, cascade, .. } => {
                    if *cascade {
                        return Ok(Outcome::Unsupported);
                    }
                    for name in names {
                        state.table_types.remove(&name.to_string());
                    }
                }
                _ => return Ok(Outcome::Unsupported),
            }

            Ok(Outcome::Success)
        }
        Record::Query {
            sql,
            types: expected_types,
            ..
        } => {
            let parse =
                sqlparser::sqlparser::Parser::parse_sql(&AnsiSqlDialect {}, sql.to_string());

            let query = match parse {
                Ok(ref statements) if statements.len() == 1 => {
                    match statements.iter().next().unwrap() {
                        SQLStatement::SQLQuery(query) => query,
                        _ => return Ok(Outcome::ParseFailure),
                    }
                }
                _ => {
                    return Ok(Outcome::ParseFailure);
                }
            };

            let parser = materialize::sql::Parser::new(state.table_types.clone());

            let (_plan, typ) = match parser.parse_view_query(&query) {
                Ok(result) => result,
                _ => return Ok(Outcome::PlanFailure),
            };

            let inferred_types = match &typ.ftype {
                FType::Tuple(types) => types.iter().map(|typ| &typ.ftype).collect::<Vec<_>>(),
                other => panic!("Query with non-tuple type: {:?}", other),
            };

            // sqllogictest coerces the output into the expected type, so expected_type is often wrong :(
            if inferred_types.len() != expected_types.len() {
                return Ok(Outcome::InferenceFailure);
            }

            Ok(Outcome::Success)
        }
        _ => Ok(Outcome::Success),
    }
}

pub fn run(filename: &Path, verbosity: usize) -> Outcomes {
    let mut outcomes = Outcomes::default();
    let mut input = String::new();
    let mut state = State::default();
    input.clear();
    File::open(filename)
        .unwrap()
        .read_to_string(&mut input)
        .unwrap();
    if verbosity >= 1 {
        println!("==> {}", filename.display());
    }
    for record in parse_records(&input) {
        let record = record.unwrap();
        if verbosity >= 2 {
            match record {
                Record::Statement { sql, .. } => println!("{}", sql),
                Record::Query { sql, .. } => println!("{}", sql),
                _ => (),
            }
        }
        let outcome = run_record(&mut state, &record)
            .with_context(|err| format!("In {}:\n{}", filename.display(), err))
            .unwrap();
        outcomes.0[outcome as usize] += 1;
    }
    outcomes
}

#[cfg(test)]
mod test {
    use super::*;

    use std::fs::File;
    use std::io::Read;

    use failure::ResultExt;
    use walkdir::WalkDir;

    #[test]
    fn fuzz_artifacts() {
        let mut input = String::new();
        for entry in WalkDir::new("../../fuzz/artifacts/fuzz_sqllogictest/") {
            let entry = entry.unwrap();
            if entry.path().is_file() {
                let mut state = State::default();
                input.clear();
                File::open(&entry.path())
                    .unwrap()
                    .read_to_string(&mut input)
                    .unwrap();
                for record in parse_records(&input) {
                    match record {
                        Ok(record) => drop(run_record(&mut state, &record)),
                        _ => (),
                    }
                }
            }
        }
    }
}

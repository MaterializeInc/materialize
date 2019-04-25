//! https://www.sqlite.org/sqllogictest/doc/trunk/about.wiki

use std::collections::HashMap;
use std::path::PathBuf;

use failure::{bail, format_err, ResultExt};
use lazy_static::lazy_static;
use regex::Regex;
use walkdir::WalkDir;

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
    lazy_static! {
        static ref COMMENT_AND_LINE_REGEX: Regex = Regex::new("(#[^\n]*)?\r?(\n|$)").unwrap();
    }
    let next_line = split_at(&mut input, &COMMENT_AND_LINE_REGEX)?.trim();

    if next_line == "" {
        // must have just been a bunch of comments
        return Ok(None);
    }

    let mut words = next_line.split(' ');
    match words.next().unwrap() {
        "statement" => {
            let should_run = match words
                .next()
                .ok_or_else(|| format_err!("missing should_run in: {}", next_line))?
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
                    .ok_or_else(|| format_err!("missing types in: {}", next_line))?,
            )?;
            let sort = words
                .next()
                .ok_or_else(|| format_err!("missing sort in: {}", next_line))?;
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
                .ok_or_else(|| format_err!("missing threshold in: {}", next_line))?
                .parse::<u64>()
                .map_err(|err| format_err!("invalid threshold ({}) in: {}", err, next_line))?;
            if input != "" {
                bail!("leftover input: {}", input)
            }
            Ok(Some(Record::HashThreshold { threshold }))
        }
        // query starts on next line
        "skipif" => parse_record(input),
        // we probably don't want to support any db-specific query
        "onlyif" => Ok(None),
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
        .map(|lines| lines.trim())
        .filter(|lines| *lines != "")
        .filter_map(|lines| parse_record(lines).transpose())
        .take_while(|record| match record {
            Ok(Record::Halt) => false,
            _ => true,
        })
}

pub fn all_files() -> impl Iterator<Item = PathBuf> {
    WalkDir::new("../../sqllogictest/test/")
        .into_iter()
        .map(|entry| entry.unwrap().path().to_owned())
        .filter(|path| path.is_file())
}

#[derive(Debug, Clone)]
pub struct Report {
    num_parse_failures: usize,
    num_parse_successes: usize,
    num_parse_unsupported: usize,
    num_plan_successes: usize,
    num_plan_failures: usize,
    num_type_successes: usize,
    num_type_failures: usize,
}

#[allow(clippy::new_without_default)]
impl Report {
    pub fn new() -> Self {
        Report {
            num_parse_successes: 0,
            num_parse_failures: 0,
            num_parse_unsupported: 0,
            num_plan_successes: 0,
            num_plan_failures: 0,
            num_type_successes: 0,
            num_type_failures: 0,
        }
    }
}

#[derive(Debug, Clone)]
pub struct State {
    table_types: HashMap<String, Type>,
}

#[allow(clippy::new_without_default)]
impl State {
    pub fn new() -> Self {
        State {
            table_types: HashMap::new(),
        }
    }
}

pub fn run_record(
    report: &mut Report,
    state: &mut State,
    record: &Record,
) -> Result<(), failure::Error> {
    match &record {
        Record::Statement { should_run, sql } => {
            lazy_static! {
                static ref UNSUPPORTED_STATEMENT_REGEX: Regex = Regex::new("^(CREATE (UNIQUE )?INDEX|CREATE TRIGGER|DROP TABLE|DROP INDEX|DROP TRIGGER|INSERT INTO .* SELECT|UPDATE|REINDEX|REPLACE INTO)").unwrap();
            }
            if UNSUPPORTED_STATEMENT_REGEX.is_match(sql) {
                report.num_parse_unsupported += 1;
            } else {
                let parse =
                    sqlparser::sqlparser::Parser::parse_sql(&AnsiSqlDialect {}, sql.to_string());
                match parse {
                    Ok(ref statements) if statements.len() == 1 => {
                        report.num_parse_successes += 1;
                        if *should_run {
                            match statements.iter().next().unwrap() {
                                SQLStatement::SQLCreateTable { name, columns } => {
                                    let types = columns
                                        .iter()
                                        .map(|column| {
                                            Ok(Type {
                                                name: Some(column.name.clone()),
                                                ftype: match &column.data_type {
                                                    SQLType::Char(_)
                                                    | SQLType::Varchar(_)
                                                    | SQLType::Text => FType::String,
                                                    SQLType::SmallInt
                                                    | SQLType::Int
                                                    | SQLType::BigInt => FType::Int64,
                                                    SQLType::Float(_)
                                                    | SQLType::Real
                                                    | SQLType::Double => FType::Float64,
                                                    other => {
                                                        bail!("Unexpected SQL type: {:?}", other)
                                                    }
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
                                _ => (),
                            }
                        }
                    }
                    _other => {
                        if *should_run {
                            report.num_parse_failures += 1;
                            // println!("Parse failure: {:?} in {}:\n{}", other, filename, sql);
                        }
                    }
                }
            }
        }
        Record::Query {
            sql,
            types: expected_types,
            ..
        } => {
            let parse =
                sqlparser::sqlparser::Parser::parse_sql(&AnsiSqlDialect {}, sql.to_string());
            match parse {
                Ok(ref statements) if statements.len() == 1 => {
                    match statements.iter().next().unwrap() {
                        SQLStatement::SQLSelect(query) => {
                            report.num_parse_successes += 1;
                            let parser = materialize::sql::Parser::new(state.table_types.clone());
                            match parser.parse_view_query(&query) {
                                Ok((_plan, typ)) => {
                                    report.num_plan_successes += 1;
                                    let inferred_types = match &typ.ftype {
                                        FType::Tuple(types) => {
                                            types.iter().map(|typ| &typ.ftype).collect::<Vec<_>>()
                                        }
                                        other => panic!("Query with non-tuple type: {:?}", other),
                                    };
                                    // sqllogictest coerces the output into the expected type, so expected_type is often wrong :(
                                    if inferred_types.len() == expected_types.len() {
                                        report.num_type_successes += 1;
                                    } else {
                                        report.num_type_failures += 1;
                                    }
                                }
                                Err(_) => report.num_plan_failures += 1,
                            }
                        }
                        _ => {
                            report.num_parse_failures += 1;
                            // println!("Parse failure - not a SQLSelect: {}", sql);
                        }
                    }
                }
                _other => {
                    report.num_parse_failures += 1;
                    // println!("Parse failure: {:?} in {}", other, sql);
                }
            }
        }
        _ => (),
    }
    Ok(())
}

#[cfg(test)]
mod test {
    use super::*;

    use std::fs::File;
    use std::io::Read;

    #[test]
    #[ignore]
    fn sqllogictest() {
        let mut input = String::new();
        let mut report = Report::new();

        for filename in all_files() {
            let mut state = State::new();
            input.clear();
            File::open(&filename)
                .unwrap()
                .read_to_string(&mut input)
                .unwrap();
            for record in parse_records(&input) {
                run_record(&mut report, &mut state, &record.unwrap())
                    .with_context(|err| format!("In {}:\n{}", filename.to_str().unwrap(), err))
                    .unwrap();
            }
        }

        // If the number of successes goes up, feel free to edit this test
        dbg!(&report);
        assert_eq!(report.num_parse_successes, 5_376_068);
        assert_eq!(report.num_parse_failures, 535_485);
        assert_eq!(report.num_parse_unsupported, 28_140);
        assert_eq!(report.num_plan_successes, 2_080_584);
        assert_eq!(report.num_plan_failures, 3_127_450);
        assert_eq!(report.num_type_successes, 2_080_584);
        assert_eq!(report.num_type_failures, 0);
    }

    #[test]
    fn fuzz_artifacts() {
        let mut input = String::new();
        let mut report = Report::new();

        for entry in WalkDir::new("../../fuzz/artifacts/fuzz_sqllogictest/") {
            let entry = entry.unwrap();
            if entry.path().is_file() {
                let mut state = State::new();
                input.clear();
                File::open(&entry.path())
                    .unwrap()
                    .read_to_string(&mut input)
                    .unwrap();
                for record in parse_records(&input) {
                    run_record(&mut report, &mut state, &record.unwrap())
                        .with_context(|err| {
                            format!("In {}:\n{}", entry.path().to_str().unwrap(), err)
                        })
                        .unwrap();
                }
            }
        }
    }
}

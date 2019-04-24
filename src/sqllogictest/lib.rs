//! https://www.sqlite.org/sqllogictest/doc/trunk/about.wiki

use std::path::PathBuf;

use failure::{bail, format_err};
use lazy_static::lazy_static;
use regex::Regex;
use walkdir::WalkDir;

use materialize::repr::FType;
use sqlparser::dialect::AnsiSqlDialect;
use sqlparser::sqlast::SQLStatement;

#[derive(Debug, Clone)]
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
                other => bail!("Unexpected type char {} in: {}", char, input),
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
    while input != "" {
        lazy_static! {
            static ref COMMENT_AND_LINE_REGEX: Regex = Regex::new("(#[^\n]*)?\r?(\n|$)").unwrap();
        }
        let next_line = split_at(&mut input, &COMMENT_AND_LINE_REGEX)?.trim();
        if next_line != "" {
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
                    return Ok(Some(Record::Statement { should_run, sql }));
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
                    let output = input;
                    return Ok(Some(Record::Query {
                        types,
                        sort,
                        label,
                        sql,
                        output,
                    }));
                }
                "hash-threshold" => {
                    let threshold = words
                        .next()
                        .ok_or_else(|| format_err!("missing threshold in: {}", next_line))?
                        .parse::<u64>()
                        .map_err(|err| {
                            format_err!("invalid threshold ({}) in: {}", err, next_line)
                        })?;
                    if input != "" {
                        bail!("leftover input: {}", input)
                    }
                    return Ok(Some(Record::HashThreshold { threshold }));
                }
                "skipif" | "onlyif" => return Ok(None),
                other => bail!("Unexpected start of record: {}", other),
            }
        }
    }
    Ok(None)
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
}

pub fn all_files() -> impl Iterator<Item = PathBuf> {
    WalkDir::new("../../sqllogictest/test/")
        .into_iter()
        .map(|entry| entry.unwrap().path().to_owned())
        .filter(|path| path.is_file())
}

#[derive(Debug, Clone)]
pub struct State {
    num_parse_failures: usize,
    num_parse_successes: usize,
    num_parse_unsupported: usize,
}

impl State {
    pub fn new() -> Self {
        State {
            num_parse_failures: 0,
            num_parse_successes: 0,
            num_parse_unsupported: 0,
        }
    }
}

pub fn run_record(state: &mut State, record: &Record) {
    match &record {
        Record::Statement { should_run, sql } => {
            lazy_static! {
                static ref UNSUPPORTED_STATEMENT_REGEX: Regex = Regex::new("^(CREATE (UNIQUE )?INDEX|CREATE TRIGGER|DROP TABLE|DROP INDEX|DROP TRIGGER|INSERT INTO .* SELECT|UPDATE|REINDEX|REPLACE INTO)").unwrap();
            }
            if UNSUPPORTED_STATEMENT_REGEX.is_match(sql) {
                state.num_parse_unsupported += 1;
            } else {
                let parse =
                    sqlparser::sqlparser::Parser::parse_sql(&AnsiSqlDialect {}, sql.to_string());
                match parse {
                    Ok(ref statements) if statements.len() == 1 => {
                        state.num_parse_successes += 1;
                    }
                    _other => {
                        if *should_run {
                            state.num_parse_failures += 1;
                            // println!("Parse failure: {:?} in {}", other, sql);
                        }
                    }
                }
            }
        }
        Record::Query { sql, .. } => {
            let parse =
                sqlparser::sqlparser::Parser::parse_sql(&AnsiSqlDialect {}, sql.to_string());
            match parse {
                Ok(ref statements) if statements.len() == 1 => {
                    state.num_parse_successes += 1;
                    // let parser = materialize::sql::Parser::new(vec![]);
                    // match parser.parse_view_query(&query) {
                    //     Ok(_) => (),
                    //     Err(_) => num_plan_failures += 1,
                    // }
                }
                _other => {
                    state.num_parse_failures += 1;
                    // println!("Parse failure: {:?} in {}", other, sql);
                }
            }
        }
        _ => (),
    }
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
        let mut state = State::new();

        for filename in all_files() {
            input.clear();
            File::open(filename)
                .unwrap()
                .read_to_string(&mut input)
                .unwrap();
            for record in parse_records(&input) {
                run_record(&mut state, &record.unwrap());
            }
        }

        dbg!(&state);

        // If the number of successes goes up, feel free to edit this test
        assert_eq!(state.num_parse_failures, 236080);
        assert_eq!(state.num_parse_successes, 3995082);
        assert_eq!(state.num_parse_unsupported, 28142);
    }

    #[test]
    fn artifacts() {
        for entry in WalkDir::new("../../fuzz/artifacts/fuzz_sqllogictest/") {
            let entry = entry.unwrap();
            if entry.path().is_file() {
                let mut input = String::new();
                let mut state = State::new();

                File::open(&entry.path())
                    .unwrap()
                    .read_to_string(&mut input)
                    .unwrap();
                for record in parse_records(&input) {
                    run_record(&mut state, &record.unwrap());
                }
            }
        }
    }
}

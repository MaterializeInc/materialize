// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

//! A parser for sqllogictest.

use std::borrow::ToOwned;

use failure::{bail, format_err};
use lazy_static::lazy_static;
use regex::Regex;

use crate::ast::{Mode, Output, QueryOutput, Record, Sort, Type};

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

/// Parse a query result type string into a vec of expected types
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
        "statement" => Ok(Some(parse_statement(words, &mut input)?)),

        "query" => Ok(Some(parse_query(words, first_line, &mut input, *mode)?)),

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

fn parse_statement<'a>(
    mut words: impl Iterator<Item = &'a str>,
    input: &mut &'a str,
) -> Result<Record<'a>, failure::Error> {
    let (should_run, rows_affected) = match words.next() {
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
    let sql = parse_sql(input)?;
    if *input != "" {
        bail!("leftover input: {}", input)
    }
    Ok(Record::Statement {
        should_run,
        rows_affected,
        sql,
    })
}

fn parse_query<'a>(
    mut words: std::iter::Peekable<impl Iterator<Item = &'a str>>,
    first_line: &'a str,
    input: &mut &'a str,
    mode: Mode,
) -> Result<Record<'a>, failure::Error> {
    if words.peek() == Some(&"error") {
        let error = &first_line[12..]; // everything after "query error "
        let sql = input;
        return Ok(Record::Query {
            sql,
            output: Err(error),
        });
    }

    let types = parse_types(
        words
            .next()
            .ok_or_else(|| format_err!("missing types in: {}", first_line))?,
    )?;
    let mut sort = Sort::No;
    let mut check_column_names = false;
    let mut multiline = false;
    if let Some(options) = words.next() {
        for option in options.split(',') {
            match option {
                "nosort" => sort = Sort::No,
                "rowsort" => sort = Sort::Row,
                "valuesort" => sort = Sort::Value,
                "colnames" => check_column_names = true,
                "multiline" => multiline = true,
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
    if multiline && (check_column_names || sort.yes()) {
        bail!("multiline option is incompatible with all other options");
    }
    let label = words.next();
    let sql = parse_sql(input)?;
    let column_names = if check_column_names {
        Some(
            split_at(input, &LINE_REGEX)?
                .split(' ')
                .filter(|s| !s.is_empty())
                .collect(),
        )
    } else {
        None
    };
    lazy_static! {
        static ref LINE_REGEX: Regex = Regex::new("\r?(\n|$)").unwrap();
        static ref HASH_REGEX: Regex = Regex::new(r"(\S+) values hashing to (\S+)").unwrap();
    }
    let output_str = *input;
    let output = match HASH_REGEX.captures(input) {
        Some(captures) => Output::Hashed {
            num_values: captures.get(1).unwrap().as_str().parse::<usize>()?,
            md5: captures.get(2).unwrap().as_str().to_owned(),
        },
        None => {
            let mut vals: Vec<String> = input.trim().lines().map(|s| s.to_owned()).collect();
            if mode == Mode::Cockroach {
                let mut rows: Vec<Vec<String>> = vec![];
                for line in vals {
                    let cols = split_cols(&line, types.len());
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
                    rows.push(cols.into_iter().map(|col| col.to_owned()).collect());
                }
                if sort == Sort::Row {
                    rows.sort();
                }
                vals = rows.into_iter().flatten().collect();
                if sort == Sort::Value {
                    vals.sort();
                }
            }
            if multiline {
                vals = vec![vals.join("\n")];
            }
            Output::Values(vals)
        }
    };
    Ok(Record::Query {
        sql,
        output: Ok(QueryOutput {
            types,
            sort,
            label,
            column_names,
            mode,
            output,
            output_str,
        }),
    })
}

/// Split on whitespace to normalize multiple spaces to one space. This happens
/// unconditionally in Cockroach mode, regardless of the sort option.
///
/// TODO: this doesn't have the whitespace-collapsing behavior for
/// single-column values that cockroach relies on
pub(crate) fn split_cols(line: &str, expected_columns: usize) -> Vec<&str> {
    if expected_columns == 1 {
        vec![line.trim()]
    } else {
        line.split_whitespace().collect()
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

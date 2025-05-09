// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A parser for sqllogictest.

use std::borrow::ToOwned;
use std::sync::LazyLock;

use anyhow::{anyhow, bail};
use mz_repr::ColumnName;
use regex::Regex;

use crate::ast::{Location, Mode, Output, QueryOutput, Record, Sort, Type};

static QUERY_OUTPUT_REGEX: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"\r?\n----").unwrap());
static DOUBLE_LINE_REGEX: LazyLock<Regex> =
    LazyLock::new(|| Regex::new(r"(\n|\r\n|$)(\n|\r\n|$)").unwrap());
static EOF_REGEX: LazyLock<Regex> = LazyLock::new(|| Regex::new(r"(\n|\r\n)EOF(\n|\r\n)").unwrap());

#[derive(Debug, Clone)]
pub struct Parser<'a> {
    contents: &'a str,
    fname: String,
    curline: usize,
    mode: Mode,
}

impl<'a> Parser<'a> {
    pub fn new(fname: &str, contents: &'a str) -> Self {
        Parser {
            contents,
            fname: fname.to_string(),
            curline: 1,
            mode: Mode::Standard,
        }
    }

    pub fn is_done(&self) -> bool {
        self.contents.is_empty()
    }

    pub fn location(&self) -> Location {
        Location {
            file: self.fname.clone(),
            line: self.curline,
        }
    }

    fn consume(&mut self, upto: usize) {
        for ch in self.contents[..upto].chars() {
            if ch == '\n' {
                self.curline += 1;
            }
        }
        self.contents = &self.contents[upto..];
    }

    pub fn split_at(&mut self, sep: &Regex) -> Result<&'a str, anyhow::Error> {
        match sep.find(self.contents) {
            Some(found) => {
                let result = &self.contents[..found.start()];
                self.consume(found.end());
                Ok(result)
            }
            None => bail!("Couldn't split {:?} at {:?}", self.contents, sep),
        }
    }

    pub fn parse_record(&mut self) -> Result<Record<'a>, anyhow::Error> {
        if self.is_done() {
            return Ok(Record::Halt);
        }

        let line_number = self.curline;

        static COMMENT_AND_LINE_REGEX: LazyLock<Regex> =
            LazyLock::new(|| Regex::new("(#[^\n]*)?\r?(\n|$)").unwrap());
        let first_line = self.split_at(&COMMENT_AND_LINE_REGEX)?.trim();

        if first_line.is_empty() {
            // query starts on the next line
            return self.parse_record();
        }

        let mut words = first_line.split(' ').peekable();
        match words.next().unwrap() {
            "statement" => self.parse_statement(words, first_line),

            "query" => self.parse_query(words, first_line),

            "simple" => self.parse_simple(words),

            "hash-threshold" => {
                let threshold = words
                    .next()
                    .ok_or_else(|| anyhow!("missing threshold in: {}", first_line))?
                    .parse::<u64>()
                    .map_err(|err| anyhow!("invalid threshold ({}) in: {}", err, first_line))?;
                Ok(Record::HashThreshold { threshold })
            }

            // we'll follow the postgresql version of all these tests
            "skipif" => {
                match words.next().unwrap() {
                    "postgresql" => {
                        // discard next record
                        self.parse_record()?;
                        self.parse_record()
                    }
                    _ => self.parse_record(),
                }
            }
            "onlyif" => {
                match words.next().unwrap() {
                    "postgresql" => self.parse_record(),
                    _ => {
                        // discard next record
                        self.parse_record()?;
                        self.parse_record()
                    }
                }
            }

            "halt" => Ok(Record::Halt),

            // this is some cockroach-specific thing, we don't care
            "subtest" | "user" | "kv-batch-size" => self.parse_record(),

            "mode" => {
                self.mode = match words.next() {
                    Some("cockroach") => Mode::Cockroach,
                    Some("standard") | Some("sqlite") => Mode::Standard,
                    other => bail!("unknown parse mode: {:?}", other),
                };
                self.parse_record()
            }

            "copy" => Ok(Record::Copy {
                table_name: words
                    .next()
                    .ok_or_else(|| anyhow!("load directive missing table name"))?,
                tsv_path: words
                    .next()
                    .ok_or_else(|| anyhow!("load directive missing TSV path"))?,
            }),

            "reset-server" => Ok(Record::ResetServer),

            other => bail!(
                "Unexpected start of record on line {}: {}",
                line_number,
                other
            ),
        }
    }

    pub fn parse_records(&mut self) -> Result<Vec<Record<'a>>, anyhow::Error> {
        let mut records = vec![];
        loop {
            match self.parse_record()? {
                Record::Halt => break,
                record => records.push(record),
            }
        }
        Ok(records)
    }

    fn parse_statement(
        &mut self,
        mut words: impl Iterator<Item = &'a str>,
        first_line: &'a str,
    ) -> Result<Record<'a>, anyhow::Error> {
        let location = self.location();
        let mut expected_error = None;
        let mut rows_affected = None;
        match words.next() {
            Some("count") => {
                rows_affected = Some(
                    words
                        .next()
                        .ok_or_else(|| anyhow!("missing count of rows affected"))?
                        .parse::<u64>()
                        .map_err(|err| anyhow!("parsing count of rows affected: {}", err))?,
                );
            }
            Some("ok") | Some("OK") => (),
            Some("error") => expected_error = Some(parse_expected_error(first_line)),
            _ => bail!("invalid statement disposition: {}", first_line),
        };
        let sql = self.split_at(&DOUBLE_LINE_REGEX)?;
        Ok(Record::Statement {
            expected_error,
            rows_affected,
            sql,
            location,
        })
    }

    fn parse_query(
        &mut self,
        mut words: std::iter::Peekable<impl Iterator<Item = &'a str>>,
        first_line: &'a str,
    ) -> Result<Record<'a>, anyhow::Error> {
        let location = self.location();
        if words.peek() == Some(&"error") {
            let error = parse_expected_error(first_line);
            let sql = self.split_at(&DOUBLE_LINE_REGEX)?;
            return Ok(Record::Query {
                sql,
                output: Err(error),
                location,
            });
        }

        let types = words.next().map_or(Ok(vec![]), parse_types)?;
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
        static LINE_REGEX: LazyLock<Regex> = LazyLock::new(|| Regex::new("\r?(\n|$)").unwrap());
        static HASH_REGEX: LazyLock<Regex> =
            LazyLock::new(|| Regex::new(r"(\S+) values hashing to (\S+)").unwrap());
        let sql = self.split_at(&QUERY_OUTPUT_REGEX)?;
        let mut output_str = self.split_at(if multiline {
            &EOF_REGEX
        } else {
            &DOUBLE_LINE_REGEX
        })?;

        // The `split_at(&QUERY_OUTPUT_REGEX)` stopped at the end of `----`, so `output_str` usually
        // starts with a newline, which is not actually part of the expected output. Strip off this
        // newline.
        output_str = if let Some(output_str_stripped) = regexp_strip_prefix(output_str, &LINE_REGEX)
        {
            output_str_stripped
        } else {
            // There should always be a newline after `----`, because we have a lint that there is
            // always a newline at the end of a file. However, we can still get here, when
            // the expected output is empty, in which case the EOF_REGEX or DOUBLE_LINE_REGEX eats
            // the newline at the end of the `----`.
            assert!(output_str.is_empty());
            output_str
        };

        // We don't want to advance the expected output past the column names so rewriting works,
        // but need to be able to parse past them, so remember the position before possible column
        // names.
        let query_output_str = output_str;
        let column_names = if check_column_names {
            Some(
                split_at(&mut output_str, &LINE_REGEX)?
                    .split(' ')
                    .filter(|s| !s.is_empty())
                    .map(|s| ColumnName::from(s.replace('␠', " ")))
                    .collect(),
            )
        } else {
            None
        };
        let output = match HASH_REGEX.captures(output_str) {
            Some(captures) => Output::Hashed {
                num_values: captures.get(1).unwrap().as_str().parse::<usize>()?,
                md5: captures.get(2).unwrap().as_str().to_owned(),
            },
            None => {
                if multiline {
                    Output::Values(vec![output_str.to_owned()])
                } else if output_str.starts_with('\r') || output_str.starts_with('\n') {
                    Output::Values(vec![])
                } else {
                    let mut vals: Vec<String> = output_str.lines().map(|s| s.to_owned()).collect();
                    match self.mode {
                        Mode::Standard => {
                            if !multiline {
                                vals = vals.into_iter().map(|val| val.replace('⏎', "\n")).collect();
                            }

                            if sort == Sort::Value {
                                vals.sort();
                            }
                        }
                        Mode::Cockroach => {
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
                                rows.push(
                                    cols.into_iter()
                                        .map(|col| {
                                            let mut col = col.replace('␠', " ");
                                            if !multiline {
                                                col = col.replace('⏎', "\n");
                                            }
                                            col
                                        })
                                        .collect(),
                                );
                            }
                            if sort == Sort::Row {
                                rows.sort();
                            }
                            vals = rows.into_iter().flatten().collect();
                            if sort == Sort::Value {
                                vals.sort();
                            }
                        }
                    }
                    Output::Values(vals)
                }
            }
        };
        Ok(Record::Query {
            sql,
            output: Ok(QueryOutput {
                types,
                sort,
                multiline,
                label,
                column_names,
                mode: self.mode,
                output,
                output_str: query_output_str,
            }),
            location,
        })
    }

    fn parse_simple(
        &mut self,
        mut words: std::iter::Peekable<impl Iterator<Item = &'a str>>,
    ) -> Result<Record<'a>, anyhow::Error> {
        let location = self.location();
        let mut conn = None;
        let mut user = None;
        let mut multiline = false;
        let mut sort = Sort::No;
        if let Some(options) = words.next() {
            for option in options.split(',') {
                if let Some(value) = option.strip_prefix("conn=") {
                    conn = Some(value);
                } else if let Some(value) = option.strip_prefix("user=") {
                    user = Some(value);
                } else if option == "rowsort" {
                    sort = Sort::Row;
                } else if option == "multiline" {
                    multiline = true;
                } else {
                    bail!("Unrecognized option {:?} in {:?}", option, options);
                }
            }
        }
        if user.is_some() && conn.is_none() {
            bail!("cannot set user without also setting conn");
        }
        let sql = self.split_at(&QUERY_OUTPUT_REGEX)?;
        let output_str = self
            .split_at(if multiline {
                &EOF_REGEX
            } else {
                &DOUBLE_LINE_REGEX
            })?
            .trim_start();
        let output = if multiline {
            Output::Values({
                let mut v = vec![output_str.to_owned()];
                // for simple queries we still have to pass the COMPLETE string after the EOF
                let complete_str = self.split_at(&DOUBLE_LINE_REGEX)?.trim_start();
                v.extend(complete_str.lines().map(String::from));
                v
            })
        } else {
            // We only apply rowsort in mode cockroach, for "query" statements,
            // so mirror that here.
            let mut output_lines: Vec<String> = output_str.lines().map(String::from).collect();

            if self.mode == Mode::Cockroach && sort == Sort::Row {
                output_lines.sort();
            }

            Output::Values(output_lines)
        };
        Ok(Record::Simple {
            location,
            conn,
            user,
            sql,
            sort,
            output,
            output_str,
        })
    }
}

fn split_at<'a>(input: &mut &'a str, sep: &Regex) -> Result<&'a str, anyhow::Error> {
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
fn parse_types(input: &str) -> Result<Vec<Type>, anyhow::Error> {
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

fn parse_expected_error(line: &str) -> &str {
    static PGCODE_RE: LazyLock<Regex> =
        LazyLock::new(|| Regex::new("(statement|query) error( pgcode [a-zA-Z0-9]{5})? ?").unwrap());
    // TODO(benesch): one day this should record the expected pgcode, if
    // specified.
    let pos = PGCODE_RE.find(line).unwrap().end();
    &line[pos..]
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

pub fn regexp_strip_prefix<'a>(text: &'a str, regexp: &Regex) -> Option<&'a str> {
    match regexp.find(text) {
        Some(found) => {
            if found.start() == 0 {
                Some(&text[found.end()..])
            } else {
                None
            }
        }
        None => None,
    }
}

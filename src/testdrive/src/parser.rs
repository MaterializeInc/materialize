// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::borrow::ToOwned;
use std::collections::{BTreeMap, btree_map};
use std::error::Error;
use std::fmt::Write;
use std::str::FromStr;
use std::sync::LazyLock;

use anyhow::{Context, anyhow, bail};
use regex::Regex;

use crate::error::PosError;

#[derive(Debug, Clone)]
pub struct PosCommand {
    pub pos: usize,
    pub command: Command,
}

// min and max versions, both inclusive
#[derive(Debug, Clone)]
pub struct VersionConstraint {
    pub min: i32,
    pub max: i32,
}

#[derive(Debug, Clone)]
pub enum Command {
    Builtin(BuiltinCommand, Option<VersionConstraint>),
    Sql(SqlCommand, Option<VersionConstraint>),
    FailSql(FailSqlCommand, Option<VersionConstraint>),
}

#[derive(Debug, Clone)]
pub struct BuiltinCommand {
    pub name: String,
    pub args: ArgMap,
    pub input: Vec<String>,
}

impl BuiltinCommand {
    pub fn assert_no_input(&self) -> Result<(), anyhow::Error> {
        if !self.input.is_empty() {
            bail!("{} action does not take input", self.name);
        }
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub enum SqlOutput {
    Full {
        column_names: Option<Vec<String>>,
        expected_rows: Vec<Vec<String>>,
    },
    Hashed {
        num_values: usize,
        md5: String,
    },
}
#[derive(Debug, Clone)]
pub struct SqlCommand {
    pub query: String,
    pub expected_output: SqlOutput,
    pub expected_start: usize,
    pub expected_end: usize,
}

#[derive(Debug, Clone)]
pub struct FailSqlCommand {
    pub query: String,
    pub expected_error: SqlExpectedError,
    pub expected_detail: Option<String>,
    pub expected_hint: Option<String>,
}

#[derive(Debug, Clone)]
pub enum SqlExpectedError {
    Contains(String),
    Exact(String),
    Regex(String),
    Timeout,
}

pub(crate) fn parse(line_reader: &mut LineReader) -> Result<Vec<PosCommand>, PosError> {
    let mut out = Vec::new();
    while let Some((pos, line)) = line_reader.peek() {
        let pos = *pos;
        let command = match line.chars().next() {
            Some('$') => {
                let version = parse_version_constraint(line_reader)?;
                Command::Builtin(parse_builtin(line_reader)?, version)
            }
            Some('>') => {
                let version = parse_version_constraint(line_reader)?;
                Command::Sql(parse_sql(line_reader)?, version)
            }
            Some('?') => {
                let version = parse_version_constraint(line_reader)?;
                Command::Sql(parse_explain_sql(line_reader)?, version)
            }
            Some('!') => {
                let version = parse_version_constraint(line_reader)?;
                Command::FailSql(parse_fail_sql(line_reader)?, version)
            }
            Some('#') => {
                // Comment line.
                line_reader.next();
                continue;
            }
            Some(x) => {
                return Err(PosError {
                    source: anyhow!(format!("unexpected input line at beginning of file: {}", x)),
                    pos: Some(pos),
                });
            }
            None => {
                return Err(PosError {
                    source: anyhow!("unexpected input line at beginning of file"),
                    pos: Some(pos),
                });
            }
        };
        out.push(PosCommand { command, pos });
    }
    Ok(out)
}

fn parse_builtin(line_reader: &mut LineReader) -> Result<BuiltinCommand, PosError> {
    let (pos, line) = line_reader.next().unwrap();
    let mut builtin_reader = BuiltinReader::new(&line, pos);
    let name = match builtin_reader.next() {
        Some(Ok((_, s))) => s,
        Some(Err(e)) => return Err(e),
        None => {
            return Err(PosError {
                source: anyhow!("command line is missing command name"),
                pos: Some(pos),
            });
        }
    };
    let mut args = BTreeMap::new();
    for el in builtin_reader {
        let (pos, token) = el?;
        let pieces: Vec<_> = token.splitn(2, '=').collect();
        let pieces = match pieces.as_slice() {
            [key, value] => vec![*key, *value],
            [key] => vec![*key, ""],
            _ => {
                return Err(PosError {
                    source: anyhow!("command argument is not in required key=value format"),
                    pos: Some(pos),
                });
            }
        };
        validate_ident(pieces[0]).map_err(|e| PosError::new(e, pos))?;

        if let Some(original) = args.insert(pieces[0].to_owned(), pieces[1].to_owned()) {
            return Err(PosError {
                source: anyhow!(
                    "argument '{}' specified twice: {} & {}",
                    pieces[0],
                    original,
                    pieces[1]
                ),
                pos: Some(pos),
            });
        };
    }
    Ok(BuiltinCommand {
        name,
        args: ArgMap(args),
        input: slurp_all(line_reader),
    })
}

/// Validate that the string is an allowed variable name (lowercase letters, numbers and dashes)
pub fn validate_ident(name: &str) -> Result<(), anyhow::Error> {
    static VALID_KEY_REGEX: LazyLock<Regex> =
        LazyLock::new(|| Regex::new("^[a-z0-9\\-]*$").unwrap());
    if !VALID_KEY_REGEX.is_match(name) {
        bail!(
            "invalid builtin argument name '{}': \
             only lowercase letters, numbers, and hyphens allowed",
            name
        );
    }
    Ok(())
}

fn parse_version_constraint(
    line_reader: &mut LineReader,
) -> Result<Option<VersionConstraint>, PosError> {
    let (pos, line) = line_reader.next().unwrap();
    if line[1..2].to_string() != "[" {
        line_reader.push(&line);
        return Ok(None);
    }
    let closed_brace_pos = match line.find(']') {
        Some(x) => x,
        None => {
            return Err(PosError {
                source: anyhow!("version-constraint: found no closing brace"),
                pos: Some(pos),
            });
        }
    };
    let mut begin_version_kw = 2;
    const MIN_VERSION: i32 = 0;
    let mut min_version = MIN_VERSION;
    if line.as_bytes()[2].is_ascii_digit() {
        let Some(op_pos) = line.find('<') else {
            return Err(PosError {
                source: anyhow!("version-constraint: initial number but no '<' following"),
                pos: Some(pos),
            });
        };
        let min_version_str = line[2..op_pos].to_string();
        match min_version_str.parse::<i32>() {
            Ok(mv) => min_version = mv,
            Err(_) => {
                return Err(PosError {
                    source: anyhow!(
                        "version-constraint: invalid version number {}",
                        min_version_str
                    ),
                    pos: Some(pos),
                });
            }
        };

        if line.as_bytes()[op_pos + 1] == b'=' {
            begin_version_kw = op_pos + 2;
        } else {
            begin_version_kw = op_pos + 1;
            min_version += 1;
        }
    };

    let version_start = begin_version_kw + "version".len();
    if line[begin_version_kw..version_start].to_string() != "version" {
        return Err(PosError {
            source: anyhow!(
                "version-constraint: invalid property {} (found '{}', expected 'version' {begin_version_kw})",
                &line[2..closed_brace_pos],
                &line[begin_version_kw..version_start]
            ),
            pos: Some(pos),
        });
    }
    let remainder = line[closed_brace_pos + 1..].to_string();
    line_reader.push(&remainder);
    const MAX_VERSION: i32 = 9999999;

    if version_start >= closed_brace_pos && min_version != MIN_VERSION {
        return Ok(Some(VersionConstraint {
            min: min_version,
            max: MAX_VERSION,
        }));
    }

    let version_pos = if line.as_bytes()[version_start + 1].is_ascii_digit() {
        version_start + 1
    } else {
        version_start + 2
    };
    let version = match line[version_pos..closed_brace_pos].parse::<i32>() {
        Ok(x) => x,
        Err(_) => {
            return Err(PosError {
                source: anyhow!(
                    "version-constraint: invalid version number {}",
                    &line[version_pos..closed_brace_pos]
                ),
                pos: Some(pos),
            });
        }
    };

    match &line[version_start..version_pos] {
        "=" => Ok(Some(VersionConstraint {
            min: version,
            max: version,
        })),
        "<=" => Ok(Some(VersionConstraint {
            min: min_version,
            max: version,
        })),
        "<" => Ok(Some(VersionConstraint {
            min: min_version,
            max: version - 1,
        })),
        ">=" if min_version == MIN_VERSION => Ok(Some(VersionConstraint {
            min: version,
            max: MAX_VERSION,
        })),
        ">" if min_version == MIN_VERSION => Ok(Some(VersionConstraint {
            min: version + 1,
            max: MAX_VERSION,
        })),
        ">=" | ">" => Err(PosError {
            source: anyhow!(
                "version-constraint: found comparison operator {} with a set minimum version {min_version}",
                &line[version_start..version_pos]
            ),
            pos: Some(pos),
        }),
        _ => Err(PosError {
            source: anyhow!(
                "version-constraint: unknown comparison operator {}",
                &line[version_start..version_pos]
            ),
            pos: Some(pos),
        }),
    }
}

fn parse_sql(line_reader: &mut LineReader) -> Result<SqlCommand, PosError> {
    let (_, line1) = line_reader.next().unwrap();
    let query = line1[1..].trim().to_owned();
    let expected_start = line_reader.raw_pos;
    let line2 = slurp_one(line_reader);
    let line3 = slurp_one(line_reader);
    let mut column_names = None;
    let mut expected_rows = Vec::new();
    static HASH_REGEX: LazyLock<Regex> =
        LazyLock::new(|| Regex::new(r"^(\S+) values hashing to (\S+)$").unwrap());
    match (line2, line3) {
        (Some((pos2, line2)), Some((pos3, line3))) => {
            if line3.len() >= 3 && line3.chars().all(|c| c == '-') {
                column_names = Some(split_line(pos2, &line2)?);
            } else {
                expected_rows.push(split_line(pos2, &line2)?);
                expected_rows.push(split_line(pos3, &line3)?);
            }
        }
        (Some((pos2, line2)), None) => match HASH_REGEX.captures(&line2) {
            Some(captures) => match captures[1].parse::<usize>() {
                Ok(num_values) => {
                    return Ok(SqlCommand {
                        query,
                        expected_output: SqlOutput::Hashed {
                            num_values,
                            md5: captures[2].to_owned(),
                        },
                        expected_start: 0,
                        expected_end: 0,
                    });
                }
                Err(err) => {
                    return Err(PosError {
                        source: anyhow!("Error parsing number of expected rows: {}", err),
                        pos: Some(pos2),
                    });
                }
            },
            None => expected_rows.push(split_line(pos2, &line2)?),
        },
        _ => (),
    }
    while let Some((pos, line)) = slurp_one(line_reader) {
        expected_rows.push(split_line(pos, &line)?)
    }
    let expected_end = line_reader.raw_pos;
    Ok(SqlCommand {
        query,
        expected_output: SqlOutput::Full {
            column_names,
            expected_rows,
        },
        expected_start,
        expected_end,
    })
}

fn parse_explain_sql(line_reader: &mut LineReader) -> Result<SqlCommand, PosError> {
    let (_, line1) = line_reader.next().unwrap();
    let expected_start = line_reader.raw_pos;
    // This is a bit of a hack to extract the next chunk of the file with
    // blank lines intact. Ideally the `LineReader` would expose the API we
    // need directly, but that would require a large refactor.
    let mut expected_output: String = line_reader
        .inner
        .lines()
        .filter(|l| !matches!(l.chars().next(), Some('#')))
        .take_while(|l| !is_sigil(l.chars().next()))
        .fold(String::new(), |mut output, l| {
            let _ = write!(output, "{}\n", l);
            output
        });
    while expected_output.ends_with("\n\n") {
        expected_output.pop();
    }
    // We parsed the multiline expected_output directly using line_reader.inner
    // above.
    slurp_all(line_reader);
    let expected_end = line_reader.raw_pos;

    Ok(SqlCommand {
        query: line1[1..].trim().to_owned(),
        expected_output: SqlOutput::Full {
            column_names: None,
            expected_rows: vec![vec![expected_output]],
        },
        expected_start,
        expected_end,
    })
}

fn parse_fail_sql(line_reader: &mut LineReader) -> Result<FailSqlCommand, PosError> {
    let (pos, line1) = line_reader.next().unwrap();
    let line2 = slurp_one(line_reader);
    let (err_pos, expected_error) = match line2 {
        Some((err_pos, line2)) => (err_pos, line2),
        None => {
            return Err(PosError {
                pos: Some(pos),
                source: anyhow!("failing SQL command is missing expected error message"),
            });
        }
    };
    let query = line1[1..].trim().to_string();

    let expected_error = if let Some(e) = expected_error.strip_prefix("regex:") {
        SqlExpectedError::Regex(e.trim().into())
    } else if let Some(e) = expected_error.strip_prefix("contains:") {
        SqlExpectedError::Contains(e.trim().into())
    } else if let Some(e) = expected_error.strip_prefix("exact:") {
        SqlExpectedError::Exact(e.trim().into())
    } else if expected_error == "timeout" {
        SqlExpectedError::Timeout
    } else {
        return Err(PosError {
            pos: Some(err_pos),
            source: anyhow!(
                "Query error must start with match specifier (`regex:`|`contains:`|`exact:`|`timeout`)"
            ),
        });
    };

    let extra_error = |line_reader: &mut LineReader, prefix| {
        if let Some((_pos, line)) = line_reader.peek() {
            if let Some(_) = line.strip_prefix(prefix) {
                let line = line_reader
                    .next()
                    .map(|(_, line)| line)
                    .unwrap()
                    .strip_prefix(prefix)
                    .map(|line| line.to_string())
                    .unwrap();
                Some(line.trim().to_string())
            } else {
                None
            }
        } else {
            None
        }
    };
    // Expect `hint` to always follow `detail` if they are both present, for now.
    let expected_detail = extra_error(line_reader, "detail:");
    let expected_hint = extra_error(line_reader, "hint:");

    Ok(FailSqlCommand {
        query: query.trim().to_string(),
        expected_error,
        expected_detail,
        expected_hint,
    })
}

fn split_line(pos: usize, line: &str) -> Result<Vec<String>, PosError> {
    let mut out = Vec::new();
    let mut field = String::new();
    let mut in_quotes = None;
    let mut escaping = false;
    for (i, c) in line.char_indices() {
        if in_quotes.is_none() && c.is_whitespace() {
            if !field.is_empty() {
                out.push(field);
                field = String::new();
            }
        } else if c == '"' && !escaping {
            if in_quotes.is_none() {
                in_quotes = Some(i)
            } else {
                in_quotes = None;
                out.push(field);
                field = String::new();
            }
        } else if c == '\\' && !escaping && in_quotes.is_some() {
            escaping = true;
        } else if escaping {
            field.push(match c {
                'n' => '\n',
                't' => '\t',
                'r' => '\r',
                '0' => '\0',
                c => c,
            });
            escaping = false;
        } else {
            field.push(c);
        }
    }
    if let Some(i) = in_quotes {
        return Err(PosError {
            source: anyhow!("unterminated quote"),
            pos: Some(pos + i),
        });
    }
    if !field.is_empty() {
        out.push(field);
    }
    Ok(out)
}

fn slurp_all(line_reader: &mut LineReader) -> Vec<String> {
    let mut out = Vec::new();
    while let Some((_, line)) = slurp_one(line_reader) {
        out.push(line);
    }
    out
}

fn slurp_one(line_reader: &mut LineReader) -> Option<(usize, String)> {
    while let Some((_, line)) = line_reader.peek() {
        match line.chars().next() {
            Some('#') => {
                // Comment line. Skip.
                let _ = line_reader.next();
            }
            Some('$') | Some('>') | Some('!') | Some('?') => return None,
            Some('\\') => {
                return line_reader.next().map(|(pos, mut line)| {
                    line.remove(0);
                    (pos, line)
                });
            }
            _ => return line_reader.next(),
        }
    }
    None
}

pub struct LineReader<'a> {
    inner: &'a str,
    #[allow(clippy::option_option)]
    next: Option<Option<(usize, String)>>,

    src_line: usize,
    pos: usize,
    pos_map: BTreeMap<usize, (usize, usize)>,
    raw_pos: usize,
}

impl<'a> LineReader<'a> {
    pub fn new(inner: &'a str) -> LineReader<'a> {
        let mut pos_map = BTreeMap::new();
        pos_map.insert(0, (1, 1));
        LineReader {
            inner,
            src_line: 1,
            next: None,
            pos: 0,
            pos_map,
            raw_pos: 0,
        }
    }

    fn peek(&mut self) -> Option<&(usize, String)> {
        if self.next.is_none() {
            self.next = Some(self.next())
        }
        self.next.as_ref().unwrap().as_ref()
    }

    pub fn line_col(&self, pos: usize) -> (usize, usize) {
        let (base_pos, (line, col)) = self.pos_map.range(..=pos).next_back().unwrap();
        (*line, col + (pos - base_pos))
    }

    fn push(&mut self, text: &String) {
        self.next = Some(Some((0usize, text.to_string())));
    }
}

impl<'a> Iterator for LineReader<'a> {
    type Item = (usize, String);

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(next) = self.next.take() {
            return next;
        }
        if self.inner.is_empty() {
            return None;
        }
        let mut fold_newlines = is_non_sql_sigil(self.inner.chars().next());
        let mut handle_newlines = is_sql_sigil(self.inner.chars().next());
        let mut line = String::new();
        let mut chars = self.inner.char_indices().fuse().peekable();
        while let Some((i, c)) = chars.next() {
            if c == '\n' {
                self.src_line += 1;
                if fold_newlines && self.inner.get(i + 1..i + 3) == Some("  ") {
                    // Chomp the newline and one space. This ensures a SQL query
                    // that is split over two lines does not become invalid. For $ commands the
                    // newline should not be removed so that the argument parser can handle the
                    // arguments correctly.
                    chars.next();
                    self.pos_map.insert(self.pos + i, (self.src_line, 2));
                    continue;
                } else if handle_newlines && self.inner.get(i + 1..i + 3) == Some("  ") {
                    // Chomp the two spaces after newline. This ensures a SQL query
                    // that is split over two lines does not become invalid, and keeping the
                    // newline ensures that comments don't remove the following lines.
                    line.push(c);
                    chars.next();
                    chars.next();
                    self.pos_map.insert(self.pos + i + 1, (self.src_line, 2));
                    continue;
                } else if line.chars().all(char::is_whitespace) {
                    line.clear();
                    fold_newlines = is_non_sql_sigil(chars.peek().map(|c| c.1));
                    handle_newlines = is_sql_sigil(chars.peek().map(|c| c.1));
                    self.pos_map.insert(self.pos, (self.src_line, 1));
                    continue;
                }
                let pos = self.pos;
                self.pos += i;
                self.raw_pos += i + 1; // Include \n character in count
                self.pos_map.insert(self.pos, (self.src_line, 1));
                self.inner = &self.inner[i + 1..];
                return Some((pos, line));
            }
            line.push(c)
        }
        self.inner = "";
        if !line.chars().all(char::is_whitespace) {
            Some((self.pos, line))
        } else {
            None
        }
    }
}

fn is_sigil(c: Option<char>) -> bool {
    is_sql_sigil(c) || is_non_sql_sigil(c)
}

fn is_sql_sigil(c: Option<char>) -> bool {
    matches!(c, Some('>') | Some('!') | Some('?'))
}

fn is_non_sql_sigil(c: Option<char>) -> bool {
    matches!(c, Some('$'))
}

struct BuiltinReader<'a> {
    inner: &'a str,
    pos: usize,
}

impl<'a> BuiltinReader<'a> {
    fn new(line: &str, pos: usize) -> BuiltinReader<'_> {
        BuiltinReader {
            inner: &line[1..],
            pos,
        }
    }
}

impl<'a> Iterator for BuiltinReader<'a> {
    type Item = Result<(usize, String), PosError>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.inner.is_empty() {
            return None;
        }

        let mut iter = self.inner.char_indices().peekable();

        while let Some((i, c)) = iter.peek() {
            if c == &' ' {
                iter.next();
            } else {
                self.pos += i;
                break;
            }
        }

        let mut token = String::new();
        let mut nesting = Vec::new();
        let mut done = false;
        let mut quoted = false;
        for (i, c) in iter {
            if c == ' ' && nesting.is_empty() && !quoted {
                done = true;
                continue;
            } else if done {
                if let Some(nested) = nesting.last() {
                    return Some(Err(PosError {
                        pos: Some(self.pos + i),
                        source: anyhow!(
                            "command argument has unterminated open {}",
                            if nested == &'{' { "brace" } else { "bracket" }
                        ),
                    }));
                }
                let pos = self.pos;
                self.pos += i;
                self.inner = &self.inner[i..];
                return Some(Ok((pos, token)));
            } else if (c == '{' || c == '[') && !quoted {
                nesting.push(c);
            } else if (c == '}' || c == ']') && !quoted {
                if let Some(nested) = nesting.last() {
                    if (nested == &'{' && c == '}') || (nested == &'[' && c == ']') {
                        nesting.pop();
                    } else {
                        return Some(Err(PosError {
                            pos: Some(self.pos + i),
                            source: anyhow!(
                                "command argument has unterminated open {}",
                                if nested == &'{' { "brace" } else { "bracket" }
                            ),
                        }));
                    }
                } else {
                    return Some(Err(PosError {
                        pos: Some(self.pos + i),
                        source: anyhow!(
                            "command argument has unbalanced close {}",
                            if c == '}' { "brace" } else { "bracket" }
                        ),
                    }));
                }
            } else if c == '"' && nesting.is_empty() {
                // remove the double quote for un-nested commands such as: command="\dt public"
                // keep the quotes when inside of a nested object such as: schema={ "type" : "array" }
                quoted = !quoted;
                continue;
            }
            token.push(c);
        }

        if let Some(nested) = nesting.last() {
            return Some(Err(PosError {
                pos: Some(self.pos + self.inner.len() - 1),
                source: anyhow!(
                    "command argument has unterminated open {}",
                    if nested == &'{' { "brace" } else { "bracket" }
                ),
            }));
        }

        if quoted {
            return Some(Err(PosError {
                pos: Some(self.pos),
                source: anyhow!("command argument has unterminated open double quote",),
            }));
        }

        self.inner = "";
        if token.is_empty() {
            None
        } else {
            Some(Ok((self.pos, token)))
        }
    }
}

#[derive(Debug, Clone)]
pub struct ArgMap(BTreeMap<String, String>);

impl ArgMap {
    pub fn values_mut(&mut self) -> btree_map::ValuesMut<'_, String, String> {
        self.0.values_mut()
    }

    pub fn opt_string(&mut self, name: &str) -> Option<String> {
        self.0.remove(name)
    }

    pub fn string(&mut self, name: &str) -> Result<String, anyhow::Error> {
        self.opt_string(name)
            .ok_or_else(|| anyhow!("missing {} parameter", name))
    }

    pub fn opt_parse<T>(&mut self, name: &str) -> Result<Option<T>, anyhow::Error>
    where
        T: FromStr,
        T::Err: Error + Send + Sync + 'static,
    {
        match self.opt_string(name) {
            Some(val) => {
                let t = val
                    .parse()
                    .with_context(|| format!("parsing {} parameter", name))?;
                Ok(Some(t))
            }
            None => Ok(None),
        }
    }

    pub fn parse<T>(&mut self, name: &str) -> Result<T, anyhow::Error>
    where
        T: FromStr,
        T::Err: Error + Send + Sync + 'static,
    {
        match self.opt_parse(name) {
            Ok(None) => bail!("missing {} parameter", name),
            Ok(Some(t)) => Ok(t),
            Err(err) => Err(err),
        }
    }

    pub fn opt_bool(&mut self, name: &str) -> Result<Option<bool>, anyhow::Error> {
        self.opt_string(name)
            .map(|val| {
                if val == "true" {
                    Ok(true)
                } else if val == "false" {
                    Ok(false)
                } else {
                    bail!("bad value for boolean parameter {}: {}", name, val);
                }
            })
            .transpose()
    }

    pub fn done(&self) -> Result<(), anyhow::Error> {
        if let Some(name) = self.0.keys().next() {
            bail!("unknown parameter {}", name);
        }
        Ok(())
    }
}

impl IntoIterator for ArgMap {
    type Item = (String, String);
    type IntoIter = btree_map::IntoIter<String, String>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

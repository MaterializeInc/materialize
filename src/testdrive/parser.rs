// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::borrow::ToOwned;
use std::collections::hash_map;
use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::str::FromStr;

use super::error::{InputError, Positioner};

#[derive(Debug)]
pub struct PosCommand {
    pub pos: usize,
    pub command: Command,
}

#[derive(Debug)]
pub enum Command {
    Builtin(BuiltinCommand),
    Sql(SqlCommand),
    FailSql(FailSqlCommand),
}

#[derive(Debug)]
pub struct BuiltinCommand {
    pub name: String,
    pub args: ArgMap,
    pub input: Vec<String>,
}

#[derive(Debug)]
pub struct SqlCommand {
    pub query: String,
    pub column_names: Vec<String>,
    pub expected_rows: Vec<Vec<String>>,
}

#[derive(Debug)]
pub struct FailSqlCommand {
    pub query: String,
    pub expected_error: String,
}

pub fn parse(line_reader: &mut LineReader) -> Result<Vec<PosCommand>, InputError> {
    let mut out = Vec::new();
    while let Some((pos, line)) = line_reader.peek() {
        let pos = *pos;
        let command = match line.chars().next() {
            Some('$') => Command::Builtin(parse_builtin(line_reader)?),
            Some('>') => Command::Sql(parse_sql(line_reader)?),
            Some('!') => Command::FailSql(parse_fail_sql(line_reader)?),
            Some('#') => {
                // Comment line.
                line_reader.next();
                continue;
            }
            _ => {
                return Err(InputError {
                    msg: "unexpected input line at beginning of file".into(),
                    pos,
                });
            }
        };
        out.push(PosCommand { command, pos });
    }
    Ok(out)
}

fn parse_builtin(line_reader: &mut LineReader) -> Result<BuiltinCommand, InputError> {
    let (pos, line) = line_reader.next().unwrap();
    let mut builtin_reader = BuiltinReader::new(&line, pos);
    let name = match builtin_reader.next() {
        Some(Ok((_, s))) => s,
        Some(Err(e)) => return Err(e),
        None => {
            return Err(InputError {
                msg: "command line is missing command name".into(),
                pos,
            });
        }
    };
    let mut args = HashMap::new();
    for el in builtin_reader {
        let (pos, token) = el?;
        let pieces: Vec<_> = token.splitn(2, '=').collect();
        if pieces.len() != 2 {
            return Err(InputError {
                msg: "command argument is not in required key=value format".into(),
                pos,
            });
        }
        args.insert(pieces[0].to_owned(), pieces[1].to_owned());
    }
    Ok(BuiltinCommand {
        name,
        args: ArgMap(args),
        input: slurp_all(line_reader),
    })
}

fn parse_sql(line_reader: &mut LineReader) -> Result<SqlCommand, InputError> {
    let (_, line1) = line_reader.next().unwrap();
    let line2 = slurp_one(line_reader);
    let line3 = slurp_one(line_reader);
    let mut column_names = Vec::new();
    let mut expected_rows = Vec::new();
    match (line2, line3) {
        (Some((pos2, line2)), Some((pos3, line3))) => {
            if line3.len() >= 3 && line3.chars().all(|c| c == '-') {
                column_names = split_line(pos2, &line2)?;
            } else {
                expected_rows.push(split_line(pos2, &line2)?);
                expected_rows.push(split_line(pos3, &line3)?);
            }
        }
        (Some((pos2, line2)), None) => expected_rows.push(split_line(pos2, &line2)?),
        _ => (),
    }
    while let Some((pos, line)) = slurp_one(line_reader) {
        expected_rows.push(split_line(pos, &line)?)
    }
    Ok(SqlCommand {
        query: line1[1..].trim().to_owned(),
        column_names,
        expected_rows,
    })
}

fn parse_fail_sql(line_reader: &mut LineReader) -> Result<FailSqlCommand, InputError> {
    let (pos, line1) = line_reader.next().unwrap();
    let line2 = slurp_one(line_reader);
    let expected_error = match line2 {
        Some((_, line2)) => line2,
        None => {
            return Err(InputError {
                pos,
                msg: "failing SQL command is missing expected error message".into(),
            });
        }
    };
    Ok(FailSqlCommand {
        query: line1[1..].trim().to_owned(),
        expected_error,
    })
}

fn split_line(pos: usize, line: &str) -> Result<Vec<String>, InputError> {
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
        } else {
            field.push(c);
            escaping = false;
        }
    }
    if let Some(i) = in_quotes {
        return Err(InputError {
            msg: "unterminated quote".into(),
            pos: pos + i,
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
    if let Some((_, line)) = line_reader.peek() {
        match line.chars().next() {
            Some('$') | Some('>') | Some('!') | Some('#') => return None,
            Some('\\') => {
                return line_reader.next().map(|(pos, mut line)| {
                    line.remove(0);
                    (pos, line)
                })
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
}

impl<'a> LineReader<'a> {
    pub fn new(inner: &str) -> LineReader {
        let mut pos_map = BTreeMap::new();
        pos_map.insert(0, (1, 1));
        LineReader {
            inner,
            src_line: 1,
            next: None,
            pos: 0,
            pos_map,
        }
    }

    fn peek(&mut self) -> Option<&(usize, String)> {
        if self.next.is_none() {
            self.next = Some(self.next())
        }
        self.next.as_ref().unwrap().as_ref()
    }
}

impl<'a> Positioner for LineReader<'a> {
    fn line_col(&self, pos: usize) -> (usize, usize) {
        let (base_pos, (line, col)) = self.pos_map.range(..=pos).next_back().unwrap();
        (*line, col + (pos - base_pos))
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
        let mut fold_newlines = should_fold(self.inner.chars().next());
        let mut line = String::new();
        let mut chars = self.inner.char_indices().fuse().peekable();
        while let Some((i, c)) = chars.next() {
            if c == '\n' {
                self.src_line += 1;
                if fold_newlines && i + 3 < self.inner.len() && &self.inner[i + 1..i + 3] == "  " {
                    // Chomp the newline and one space. This ensures a SQL query
                    // that is split over two lines does not become invalid.
                    chars.next();
                    self.pos_map.insert(self.pos + i, (self.src_line, 2));
                    continue;
                } else if line.chars().all(char::is_whitespace) {
                    line.clear();
                    fold_newlines = should_fold(chars.peek().map(|c| c.1));
                    self.pos_map.insert(self.pos, (self.src_line, 1));
                    continue;
                }
                let pos = self.pos;
                self.pos += i;
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

fn should_fold(c: Option<char>) -> bool {
    match c {
        Some('$') | Some('>') | Some('!') => true,
        _ => false,
    }
}

struct BuiltinReader<'a> {
    inner: &'a str,
    pos: usize,
}

impl<'a> BuiltinReader<'a> {
    fn new(line: &str, pos: usize) -> BuiltinReader {
        BuiltinReader {
            inner: &line[1..],
            pos,
        }
    }
}

impl<'a> Iterator for BuiltinReader<'a> {
    type Item = Result<(usize, String), InputError>;

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
        let mut nesting = 0;
        let mut done = false;
        for (i, c) in iter {
            if c == ' ' && nesting == 0 {
                done = true;
                continue;
            } else if done {
                if nesting > 0 {
                    return Some(Err(InputError {
                        pos: self.pos + i,
                        msg: "command argument has unterminated open brace".into(),
                    }));
                }
                let pos = self.pos;
                self.pos += i;
                self.inner = &self.inner[i..];
                return Some(Ok((pos, token)));
            } else if c == '{' {
                nesting += 1
            } else if c == '}' {
                if nesting == 0 {
                    return Some(Err(InputError {
                        pos: self.pos + i,
                        msg: "command argument has unbalanced close brace".into(),
                    }));
                }
                nesting -= 1
            }
            token.push(c);
        }

        if nesting > 0 {
            return Some(Err(InputError {
                pos: self.pos + self.inner.len() - 1,
                msg: "command argument has unterminated open brace".into(),
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

#[derive(Debug)]
pub struct ArgMap(HashMap<String, String>);

impl ArgMap {
    pub fn values_mut(&mut self) -> hash_map::ValuesMut<String, String> {
        self.0.values_mut()
    }

    pub fn opt_string(&mut self, name: &str) -> Option<String> {
        self.0.remove(name)
    }

    pub fn string(&mut self, name: &str) -> Result<String, String> {
        self.opt_string(name)
            .ok_or_else(|| format!("missing {} parameter", name))
    }

    pub fn opt_parse<T>(&mut self, name: &str) -> Result<Option<T>, String>
    where
        T: FromStr,
        T::Err: fmt::Display,
    {
        match self.opt_string(name) {
            Some(val) => {
                let t = val
                    .parse()
                    .map_err(|e| format!("parsing {} parameter: {}", name, e))?;
                Ok(Some(t))
            }
            None => Ok(None),
        }
    }

    #[allow(dead_code)]
    pub fn parse<T>(&mut self, name: &str) -> Result<T, String>
    where
        T: FromStr,
        T::Err: fmt::Display,
    {
        match self.opt_parse(name) {
            Ok(None) => Err(format!("missing {} parameter", name)),
            Ok(Some(t)) => Ok(t),
            Err(err) => Err(err),
        }
    }

    pub fn opt_bool(&mut self, name: &str) -> Result<bool, String> {
        match self.opt_string(name) {
            Some(val) => {
                if val == "true" {
                    Ok(true)
                } else if val == "false" {
                    Ok(false)
                } else {
                    Err(format!("bad value for boolean parameter {}: {}", name, val))
                }
            }
            None => Ok(false),
        }
    }

    pub fn done(&self) -> Result<(), String> {
        if let Some(name) = self.0.keys().next() {
            return Err(format!("unknown parameter {}", name));
        }
        Ok(())
    }
}

impl IntoIterator for ArgMap {
    type Item = (String, String);
    type IntoIter = hash_map::IntoIter<String, String>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Decoding Materialize's humanized type syntax.
//!
//! `pg_typeof` is the only server surface that describes an anonymous record,
//! and it describes it the way `EXPLAIN` does: `record(a: integer,b: text?)`,
//! where a trailing `?` marks a nullable field. This module turns that back
//! into a [`DataType`] so `mz-deploy lock` can record the real type.
//!
//! The syntax is not designed to be read back. Field names are rendered
//! unquoted, so a name containing a delimiter is genuinely ambiguous; such a
//! type is rejected rather than guessed at, and the caller falls back to the
//! catalog's lossy spelling.

use crate::types::{DataType, RecordField};
use thiserror::Error;

/// Characters that would make a field name ambiguous in unquoted output.
const AMBIGUOUS: &[char] = &[',', ':', '?', '(', ')', '[', ']'];

#[derive(Error, Debug)]
#[error("cannot decode type `{input}`: {reason}")]
pub(crate) struct HumanizedTypeError {
    input: String,
    reason: String,
}

impl HumanizedTypeError {
    fn new(input: &str, reason: impl Into<String>) -> Self {
        HumanizedTypeError {
            input: input.to_string(),
            reason: reason.into(),
        }
    }
}

/// Parse a type as rendered by `pg_typeof`.
pub(crate) fn parse(input: &str) -> Result<DataType, HumanizedTypeError> {
    parse_inner(input.trim(), input)
}

fn parse_inner(s: &str, input: &str) -> Result<DataType, HumanizedTypeError> {
    if let Some(body) = enclosed(s, "record(", ')') {
        let mut fields = Vec::new();
        for part in split_top_level(body) {
            fields.push(parse_field(part, input)?);
        }
        if fields.is_empty() {
            return Err(HumanizedTypeError::new(input, "record has no fields"));
        }
        return Ok(DataType::Record(fields));
    }

    if let Some(body) = enclosed(s, "map[", ']') {
        let Some(arrow) = find_top_level(body, "=>") else {
            return Err(HumanizedTypeError::new(input, "map has no value type"));
        };
        return Ok(DataType::Map(Box::new(parse_inner(
            body[arrow + 2..].trim(),
            input,
        )?)));
    }

    if let Some(element) = s.strip_suffix("[]") {
        return Ok(DataType::Array(Box::new(parse_inner(element, input)?)));
    }
    if let Some(element) = s.strip_suffix(" list") {
        return Ok(DataType::List(Box::new(parse_inner(element, input)?)));
    }

    if s.is_empty() {
        return Err(HumanizedTypeError::new(input, "empty type"));
    }
    Ok(DataType::Named(s.to_string()))
}

fn parse_field(part: &str, input: &str) -> Result<RecordField, HumanizedTypeError> {
    let Some(sep) = find_top_level(part, ": ") else {
        return Err(HumanizedTypeError::new(
            input,
            format!("record field `{}` has no type", part.trim()),
        ));
    };
    let name = part[..sep].trim();
    if name.is_empty() || name.contains(AMBIGUOUS) {
        return Err(HumanizedTypeError::new(
            input,
            format!("record field name `{}` is ambiguous unquoted", name),
        ));
    }
    let mut rest = part[sep + 2..].trim();
    let nullable = rest.ends_with('?') && depth_at(rest, rest.len() - 1) == 0;
    if nullable {
        rest = &rest[..rest.len() - 1];
    }
    Ok(RecordField {
        name: name.to_string(),
        r#type: parse_inner(rest, input)?,
        nullable,
    })
}

/// The body of `s` when it is exactly `prefix` … `close`, where `close` is the
/// bracket matching the one `prefix` ends with and lands on the final
/// character.
///
/// This is what separates `map[text=>int]` from `map[text=>int][]`, whose
/// matching bracket closes early.
fn enclosed<'a>(s: &'a str, prefix: &str, close: char) -> Option<&'a str> {
    let body = s.strip_prefix(prefix)?;
    let mut depth = 1;
    for (i, c) in body.char_indices() {
        match c {
            '(' | '[' => depth += 1,
            ')' | ']' => {
                depth -= 1;
                if depth == 0 {
                    return (c == close && i + c.len_utf8() == body.len()).then_some(&body[..i]);
                }
            }
            _ => {}
        }
    }
    None
}

/// Bracket depth immediately before byte offset `at`.
fn depth_at(s: &str, at: usize) -> i32 {
    let mut depth = 0;
    for (i, c) in s.char_indices() {
        if i >= at {
            break;
        }
        match c {
            '(' | '[' => depth += 1,
            ')' | ']' => depth -= 1,
            _ => {}
        }
    }
    depth
}

/// Split on commas that are not inside brackets.
fn split_top_level(s: &str) -> Vec<&str> {
    let mut parts = Vec::new();
    let mut depth = 0;
    let mut start = 0;
    for (i, c) in s.char_indices() {
        match c {
            '(' | '[' => depth += 1,
            ')' | ']' => depth -= 1,
            ',' if depth == 0 => {
                parts.push(&s[start..i]);
                start = i + 1;
            }
            _ => {}
        }
    }
    if !s.is_empty() {
        parts.push(&s[start..]);
    }
    parts
}

/// Offset of the first `needle` that is not inside brackets.
fn find_top_level(s: &str, needle: &str) -> Option<usize> {
    let mut depth = 0;
    for (i, c) in s.char_indices() {
        match c {
            '(' | '[' => depth += 1,
            ')' | ']' => depth -= 1,
            _ => {}
        }
        if depth == 0 && s[i..].starts_with(needle) {
            return Some(i);
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every string here was produced by `pg_typeof` against a live
    /// Materialize, so parsing them back is what the capture path relies on.
    #[mz_ore::test]
    fn pg_typeof_output_round_trips() {
        for input in [
            "integer",
            "bigint",
            "timestamp without time zone",
            "character varying",
            "record(a: integer,b: text?)",
            "record(a: integer,b: text?,c: bigint list?)",
            "record(a: integer,b: text?,nested: record(x: uint8,y: numeric?))",
            "bigint list",
            "text list list",
            "map[text=>integer]",
            "integer[]",
            "record(a: integer,b: text?) list",
            "record(a: integer,b: text?)[]",
        ] {
            let parsed = parse(input).unwrap_or_else(|e| panic!("{input}: {e}"));
            assert_eq!(parsed.to_string(), input, "round trip changed {input}");
        }
    }

    #[mz_ore::test]
    fn map_of_array_is_not_mistaken_for_an_array_of_map() {
        assert_eq!(
            parse("map[text=>integer[]]").unwrap().to_string(),
            "map[text=>integer[]]"
        );
        assert_eq!(
            parse("map[text=>integer][]").unwrap().to_string(),
            "map[text=>integer][]"
        );
    }

    #[mz_ore::test]
    fn nullability_is_read_per_field() {
        let parsed = parse("record(a: integer,b: record(x: text)?)").unwrap();
        let DataType::Record(fields) = parsed else {
            panic!("expected a record");
        };
        assert!(!fields[0].nullable);
        assert!(fields[1].nullable, "the `?` applies to the nested record");
    }

    /// Field names are rendered unquoted, so a name holding a delimiter is
    /// genuinely ambiguous. Fail rather than guess.
    #[mz_ore::test]
    fn ambiguous_field_names_are_rejected() {
        for input in ["record(a,b: integer)", "record(: integer)", "record()"] {
            assert!(parse(input).is_err(), "{input} should not parse");
        }
    }
}

// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use anyhow::bail;

use super::util;

// https://github.com/postgres/postgres/blob/REL_14_0/src/include/access/htup_details.h#L577-L584
pub const MAX_LENGTH: i32 = 10_485_760;

/// A marker type indicating that a Rust string should be interpreted as a
/// [`ScalarType::Char`].
#[derive(Debug, Clone, Copy, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct Char<S: AsRef<str>>(pub S);

pub fn extract_typ_mod(typ_mod: &[u64]) -> Result<Option<usize>, anyhow::Error> {
    let typ_mod = util::extract_typ_mod::<usize>(
        "character",
        typ_mod,
        &[(
            "length",
            1,
            usize::try_from(MAX_LENGTH).expect("max length is positive"),
        )],
    )?;
    Ok(Some(*typ_mod.get(0).unwrap_or(&1)))
}

/// Controls how to handle trailing whitespace at the end of bpchar data.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
enum CharWhiteSpace {
    /// Trim all whitespace from strings, which is appropriate for storing
    /// bpchar data in Materialize. bpchar data is stored in datums with its
    /// trailing whitespace trimmed to enforce the same equality semantics as
    /// PG, while also allowing us to bit-wise equality on rows.
    Trim,
    /// Blank pad strings, which is appropriate for returning bpchar data out of Materialize.
    Pad,
}

impl CharWhiteSpace {
    fn process_str(&self, s: &str, length: Option<usize>) -> String {
        use CharWhiteSpace::*;
        match self {
            Trim => s.trim_end().to_string(),
            Pad => match length {
                Some(length) => format!("{:width$}", s, width = length),
                // This occurs when e.g. printing lists
                None => s.to_string(),
            },
        }
    }
}

/// Returns `s` as a `String` with options to enforce char and varchar
/// semantics.
///
/// # Arguments
/// * `s` - The `str` to format
/// * `length` - An optional maximum length for the string
/// * `fail_on_len` - Return an error if `s`'s character count exceeds the
///   specified maximum length.
/// * `white_space` - Express how to handle trailing whitespace on `s`
fn format_char_str(
    s: &str,
    length: Option<usize>,
    fail_on_len: bool,
    white_space: CharWhiteSpace,
) -> Result<String, anyhow::Error> {
    Ok(match length {
        // Note that length is 1-indexed, so finding `None` means the string's
        // characters don't exceed the length, while finding `Some` means it
        // does.
        Some(l) => match s.char_indices().nth(l) {
            None => white_space.process_str(s, length),
            Some((idx, _)) => {
                if !fail_on_len || s[idx..].chars().all(|c| c.is_ascii_whitespace()) {
                    white_space.process_str(&s[..idx], length)
                } else {
                    bail!("{} exceeds maximum length of {}", s, l)
                }
            }
        },
        None => white_space.process_str(s, None),
    })
}

/// Ensures that `s` has fewer than `length` characters, and returns a `String`
/// version of it with all whitespace trimmed from the end.
///
/// The value returned is appropriate to store in `Datum::String`, but _is not_
/// appropriate to return to clients.
pub fn format_str_trim(
    s: &str,
    length: Option<usize>,
    fail_on_len: bool,
) -> Result<String, anyhow::Error> {
    format_char_str(s, length, fail_on_len, CharWhiteSpace::Trim)
}

/// Ensures that `s` has fewer than `length` characters, and returns a `String`
/// version of it with blank padding so that its width is `length` characters.
///
/// The value returned is appropriate to return to clients, but _is not_
/// appropriate to store in `Datum::String`.
pub fn format_str_pad(s: &str, length: Option<usize>) -> String {
    format_char_str(s, length, false, CharWhiteSpace::Pad).unwrap()
}

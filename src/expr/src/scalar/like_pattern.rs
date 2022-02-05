// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::mem;

use derivative::Derivative;
use regex::{Regex, RegexBuilder};
use serde::{Deserialize, Serialize};

use crate::scalar::EvalError;
use lowertest::MzReflect;

// This implementation supports a couple of different methods of matching
// text against a SQL LIKE pattern.
//
// The most general approach is to convert the LIKE pattern into a
// regular expression and use the well-tested Regex library to perform the
// match. This works well with complex patterns and case-insensitive matches
// that are hard to get right.
//
// That said, regular expressions aren't that efficient. For most patterns
// we can do better using built-in string matching.

/// An object that can test whether a string matches a LIKE pattern.
#[derive(Debug, Clone, Deserialize, Serialize, Derivative, MzReflect)]
#[derivative(Eq, PartialEq, Ord, PartialOrd, Hash)]
pub struct Matcher {
    pub pattern: String,
    pub case_insensitive: bool,
    #[derivative(
        PartialEq = "ignore",
        Hash = "ignore",
        Ord = "ignore",
        PartialOrd = "ignore"
    )]
    matcher_impl: MatcherImpl,
}

impl Matcher {
    pub fn is_match(&self, text: &str) -> bool {
        match &self.matcher_impl {
            MatcherImpl::String(subpatterns) => is_match_subpatterns(subpatterns, text),
            MatcherImpl::Regex(r) => r.is_match(text),
        }
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, MzReflect)]
enum MatcherImpl {
    String(Vec<Subpattern>),
    Regex(#[serde(with = "serde_regex")] Regex),
}

/// Builds a Matcher that matches a SQL LIKE pattern.
pub fn compile(pattern: &str, case_insensitive: bool, escape: char) -> Result<Matcher, EvalError> {
    // We would like to have a consistent, documented limit to the size of
    // supported LIKE patterns. The real limiting factor is the number of states
    // that can be handled by the Regex library. In testing, I was able to
    // create an adversarial pattern "%a%b%c%d%e..." that started failing around
    // 9 KiB, so we chose 8 KiB as the limit. This is consistent with limits
    // set by other databases, like SQL Server.
    // On the other hand, PostgreSQL does not have a documented limit.
    if pattern.len() > 8 << 10 {
        return Err(EvalError::LikePatternTooLong);
    }
    // TODO: Fall back to regex if the chain of sub-patterns is too long?
    let matcher_impl = match case_insensitive {
        false => MatcherImpl::String(build_subpatterns(pattern, escape)?),
        true => MatcherImpl::Regex(build_regex(pattern, case_insensitive, escape)?),
    };
    Ok(Matcher {
        pattern: String::from(pattern),
        case_insensitive,
        matcher_impl: matcher_impl,
    })
}

/// Builds a regular expression that matches the same strings as a SQL
/// LIKE pattern.
fn build_regex(pattern: &str, case_insensitive: bool, escape: char) -> Result<Regex, EvalError> {
    // LIKE patterns always cover the whole string, so we anchor the regex on
    // both sides. An underscore (`_`) in a LIKE pattern matches any single
    // character and a percent sign (`%`) matches any sequence of zero or more
    // characters, so we translate those operators to their equivalent regex
    // operators, `.` and `.*`, respectively. Other characters match themselves
    // and are copied directly, unless they have special meaning in a regex, in
    // which case they are escaped in the regex with a backslash (`\`).
    //
    // Note that characters in LIKE patterns may also be escaped by preceding
    // them with a backslash. This has no effect on most characters, but it
    // removes the special meaning from the underscore and percent sign
    // operators, and means that matching a literal backslash requires doubling
    // the backslash.
    let mut regex = String::from("^");
    let mut in_escape = false;
    for c in pattern.chars() {
        match c {
            c if !in_escape && c == escape => {
                in_escape = true;
            }
            '_' if !in_escape => regex.push('.'),
            '%' if !in_escape => regex.push_str(".*"),
            c => {
                if regex_syntax::is_meta_character(c) {
                    regex.push('\\');
                }
                regex.push(c);
                in_escape = false;
            }
        }
    }
    regex.push('$');
    if in_escape {
        return Err(EvalError::UnterminatedLikeEscapeSequence);
    }

    let mut regex = RegexBuilder::new(&regex);
    regex.dot_matches_new_line(true);
    regex.case_insensitive(case_insensitive);
    match regex.build() {
        Ok(regex) => Ok(regex),
        Err(regex::Error::CompiledTooBig(_)) => Err(EvalError::LikePatternTooLong),
        Err(e) => Err(EvalError::Internal(format!(
            "build_regex produced invalid regex: {}",
            e
        ))),
    }
}

// The algorithm below is based on the observation that any LIKE pattern can be
// decomposed into multiple parts:
//     <PATTERN> := <SUB-PATTERN> (<SUB-PATTERN> ...)
//     <SUB-PATTERN> := <WILDCARDS> <SUFFIX>
//
// The sub-patterns start with zero or more wildcard characters, eventually
// followed by (non-wildcard) literal characters. The last sub-pattern may
// have an empty SUFFIX.
//
// Example: the PATTERN "n__dl%" can be broken into the following parts:
//   1. SUB-PATTERN = <WILDCARDS ""> <SUFFIX "n">
//   2. SUB-PATTERN = <WILDCARDS "__"> <SUFFIX "dl">
//   3. SUB-PATTERN = <WILDCARDS "%"> <SUFFIX "">
//
// The WILDCARDS can be any combination of '_', which matches exactly 1 char,
// and '%' which matches zero or more chars. These wildcards can be simplified
// down to the (min, max) of characters they might consume:
//     ""  = (0, 0)    // doesn't consume any characters
//     "_" = (1, 1)    // consumes exactly one
//     "%" = (0, many) // zero or more
// These are additive, so:
//     "_%"    = (1, many)
//     "__%__" = (4, many)
//     "%%%_"  = (1, many)

#[derive(Debug, Default, Clone, Deserialize, Serialize, MzReflect)]
struct Subpattern {
    /// The minimum number of characters that can be consumed by the wildcard expression.
    consume: usize,
    /// Whether the wildcard expression can consume an arbitrary number of characters.
    many: bool,
    /// A string literal that is expected after the wildcards.
    suffix: String,
}

fn is_match_subpatterns(subpatterns: &[Subpattern], mut text: &str) -> bool {
    let (subpattern, subpatterns) = match subpatterns {
        [] => return text.is_empty(),
        [subpattern, subpatterns @ ..] => (subpattern, subpatterns),
    };
    // Go ahead and skip the minimum number of characters the sub-pattern consumes:
    if subpattern.consume > 0 {
        let mut chars = text.chars();
        if chars.nth(subpattern.consume - 1).is_none() {
            return false;
        }
        text = chars.as_str();
    }
    if subpattern.many {
        // The sub-pattern might consume any number of characters, but we need to find
        // where it terminates so we can match any subsequent sub-patterns. We do this
        // by searching for the suffix string using str::find.
        //
        // We could investigate using a fancier substring search like Boyer-Moore:
        // https://en.wikipedia.org/wiki/Boyer%E2%80%93Moore_string-search_algorithm
        //
        // .. but it's likely not worth it. It's slower for small strings,
        // and doesn't really start outperforming the naive approach until
        // haystack sizes of 1KB or greater. See benchmarking results from:
        // https://github.com/killerswan/boyer-moore-search/blob/master/README.md
        //
        // Another approach that may be interesteing to look at is a
        // hardware-optimized search:
        // http://0x80.pl/articles/simd-strfind.html
        if subpattern.suffix.len() == 0 {
            // Nothing to find... This should only happen in the last sub-pattern.
            assert!(
                subpatterns.is_empty(),
                "empty suffix in middle of a pattern"
            );
            return true;
        }
        // Use rfind so we perform a greedy capture, like Regex.
        let mut found = text.rfind(&subpattern.suffix);
        loop {
            match found {
                None => return false,
                Some(offset) => {
                    let end = offset + subpattern.suffix.len();
                    if is_match_subpatterns(subpatterns, &text[end..]) {
                        return true;
                    }
                    // Didn't match, look for the next rfind.
                    if offset == 0 {
                        return false;
                    }
                    found = text[..(end - 1)].rfind(&subpattern.suffix);
                }
            }
        }
    }
    // No string search needed, we just use a prefix match on rest.
    if !text.starts_with(&subpattern.suffix) {
        return false;
    }
    is_match_subpatterns(subpatterns, &text[subpattern.suffix.len()..])
}

/// Breaks a LIKE pattern into a chain of sub-patterns.
fn build_subpatterns(pattern: &str, escape: char) -> Result<Vec<Subpattern>, EvalError> {
    let mut subpatterns = vec![];
    let mut current = Subpattern::default();
    let mut in_wildcard = true;
    let mut in_escape = false;

    for c in pattern.chars() {
        match c {
            c if !in_escape && c == escape => {
                in_escape = true;
                in_wildcard = false;
            }
            '_' if !in_escape => {
                if !in_wildcard {
                    subpatterns.push(mem::take(&mut current));
                    in_wildcard = true;
                }
                current.consume += 1;
            }
            '%' if !in_escape => {
                if !in_wildcard {
                    subpatterns.push(mem::take(&mut current));
                    in_wildcard = true;
                }
                current.many = true;
            }
            c => {
                current.suffix.push(c);
                in_escape = false;
                in_wildcard = false;
            }
        }
    }
    if in_escape {
        return Err(EvalError::UnterminatedLikeEscapeSequence);
    }
    subpatterns.push(mem::take(&mut current));
    Ok(subpatterns)
}

// Unit Tests
//
// Most of the unit tests for LIKE and ILIKE can be found in:
//    test/sqllogictest/cockroach/like.slt
// These tests are here as a convenient place to run quick tests while
// actively working on changes to the implementation. Make sure you
// run the full test suite before submitting any changes.

#[cfg(test)]
mod test {
    use super::*;
    use ore::str::StrExt;

    #[test]
    fn test_like() {
        struct Input<'a> {
            haystack: &'a str,
            matches: bool,
        }
        let input = |haystack, matches| Input { haystack, matches };
        struct Pattern<'a> {
            needle: &'a str,
            case_insensitive: bool,
            escape: char,
            inputs: Vec<Input<'a>>,
        }
        let test_cases = vec![
            Pattern {
                needle: "ban%na!",
                case_insensitive: false,
                escape: '\\',
                inputs: vec![input("banana!", true)],
            },
            Pattern {
                needle: "ban%na!",
                case_insensitive: false,
                escape: 'n',
                inputs: vec![input("banana!", false), input("ba%a!", true)],
            },
            Pattern {
                needle: "ban%%%na!",
                case_insensitive: false,
                escape: '%',
                inputs: vec![input("banana!", false), input("ban%na!", true)],
            },
            Pattern {
                needle: "foo",
                case_insensitive: true,
                escape: '\\',
                inputs: vec![
                    input("", false),
                    input("f", false),
                    input("fo", false),
                    input("foo", true),
                    input("FOO", true),
                    input("Foo", true),
                    input("fOO", true),
                    input("food", false),
                ],
            },
        ];

        for tc in test_cases {
            let matcher = compile(tc.needle, tc.case_insensitive, tc.escape).unwrap();
            for input in tc.inputs {
                let actual = matcher.is_match(input.haystack);
                assert!(
                    actual == input.matches,
                    "{} {} {}:\n\tactual: {}\n\texpected: {}\n",
                    input.haystack.quoted(),
                    match tc.case_insensitive {
                        true => "ILIKE",
                        false => "LIKE",
                    },
                    tc.needle.quoted(),
                    actual,
                    input.matches,
                );
            }
        }
    }
}

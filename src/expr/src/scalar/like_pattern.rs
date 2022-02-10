// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::fmt::Write;
use std::mem;

use derivative::Derivative;
use regex::{Regex, RegexBuilder};
use serde::{Deserialize, Serialize};

use crate::scalar::EvalError;
use mz_lowertest::MzReflect;

/// The escape string to use by default in LIKE patterns.
pub const DEFAULT_ESCAPE: &str = "\\";

/// Converts a pattern string that uses a custom escape character to one that uses the default.
fn normalize_pattern(pattern: &str, escape: &str) -> Result<String, EvalError> {
    if escape.eq(DEFAULT_ESCAPE) {
        return Ok(String::from(pattern));
    }
    let default_escape_char: char = DEFAULT_ESCAPE.chars().next().unwrap();
    let mut p = String::with_capacity(2 * pattern.len());
    if escape.is_empty() {
        for c in pattern.chars() {
            if c == default_escape_char {
                p.push(c);
            }
            p.push(c);
        }
    } else {
        let mut ecs = escape.chars();
        let custom_escape_char: char = ecs.next().unwrap();
        if !ecs.next().is_none() {
            return Err(EvalError::LikeEscapeTooLong);
        }
        let mut cs = pattern.chars();
        while let Some(c) = cs.next() {
            if c == custom_escape_char {
                match cs.next() {
                    Some(c2) => {
                        p.push(default_escape_char);
                        p.push(c2);
                    }
                    None => return Err(EvalError::UnterminatedLikeEscapeSequence),
                }
                continue;
            }
            if c == default_escape_char {
                p.push(c);
            }
            p.push(c);
        }
    }
    p.shrink_to_fit();
    Ok(p)
}

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
pub fn compile(pattern: &str, case_insensitive: bool, escape: &str) -> Result<Matcher, EvalError> {
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

    // The way we handle user-specified escape strings is to first normalize the
    // LIKE pattern. This process converts the pattern to one that uses the
    // default escape string ('\\') instead. This is slightly less efficient,
    // but works well with Materialize's diagnostic output. Explain plans and
    // other debug output sometimes use the ~~ and ~~* operators; these binary
    // operators do not allow for user-supplied escape strings. Normalizing to
    // the default escape string lets us display the stored pattern in the form
    // that would be expected by these operators.
    let p = normalize_pattern(pattern, escape)?;

    let subpatterns = build_subpatterns(&p)?;
    let matcher_impl = match case_insensitive || subpatterns.len() > 5 {
        false => MatcherImpl::String(subpatterns),
        true => MatcherImpl::Regex(build_regex(&subpatterns, case_insensitive)?),
    };
    Ok(Matcher {
        pattern: p,
        case_insensitive,
        matcher_impl,
    })
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
fn build_subpatterns(pattern: &str) -> Result<Vec<Subpattern>, EvalError> {
    let mut subpatterns = vec![];
    let mut current = Subpattern::default();
    let mut in_wildcard = true;
    let mut in_escape = false;
    let escape_char: char = DEFAULT_ESCAPE.chars().next().unwrap();
    for c in pattern.chars() {
        match c {
            c if !in_escape && c == escape_char => {
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

/// Builds a regular expression that matches some parsed Subpatterns.
fn build_regex(subpatterns: &[Subpattern], case_insensitive: bool) -> Result<Regex, EvalError> {
    let mut r = String::from("^");
    for sp in subpatterns {
        if sp.consume == 0 && sp.many {
            r.push_str(".*");
        } else if sp.consume == 1 {
            r.push('.');
            if sp.many {
                r.push('+');
            }
        } else if sp.consume > 1 {
            r.push_str(".{");
            write!(&mut r, "{}", sp.consume).unwrap();
            if sp.many {
                r.push(',');
            }
            r.push('}');
        }
        regex_syntax::escape_into(&sp.suffix, &mut r);
    }
    r.push('$');
    let mut rb = RegexBuilder::new(&r);
    rb.dot_matches_new_line(true);
    rb.case_insensitive(case_insensitive);
    match rb.build() {
        Ok(regex) => Ok(regex),
        Err(regex::Error::CompiledTooBig(_)) => Err(EvalError::LikePatternTooLong),
        Err(e) => Err(EvalError::Internal(format!(
            "build_regex produced invalid regex: {}",
            e
        ))),
    }
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
    use mz_ore::str::StrExt;

    #[test]
    fn test_normalize_pattern() {
        struct TestCase<'a> {
            pattern: &'a str,
            escape: &'a str,
            expected: &'a str,
        }
        let test_cases = vec![
            TestCase {
                pattern: "",
                escape: "",
                expected: "",
            },
            TestCase {
                pattern: "ban%na!",
                escape: "\\",
                expected: "ban%na!",
            },
            TestCase {
                pattern: "ban%%%na!",
                escape: "%",
                expected: "ban\\%\\na!",
            },
            TestCase {
                pattern: "ban%na\\!",
                escape: "n",
                expected: "ba\\%\\a\\\\!",
            },
            TestCase {
                pattern: "ban%na\\!",
                escape: "",
                expected: "ban%na\\\\!",
            },
            TestCase {
                pattern: "ban\\na!",
                escape: "n",
                expected: "ba\\\\\\a!",
            },
            TestCase {
                pattern: "ban\\\\na!",
                escape: "n",
                expected: "ba\\\\\\\\\\a!",
            },
            TestCase {
                pattern: "food",
                escape: "o",
                expected: "f\\od",
            },
            TestCase {
                pattern: "漢漢",
                escape: "漢",
                expected: "\\漢",
            },
        ];

        for input in test_cases {
            let actual = normalize_pattern(input.pattern, input.escape).unwrap();
            assert!(
                actual == input.expected,
                "normalize_pattern({}, {}):\n\tactual: {}\n\texpected: {}\n",
                input.pattern.quoted(),
                input.escape.quoted(),
                actual.quoted(),
                input.expected.quoted(),
            );
        }
    }

    #[test]
    fn test_escape_too_long() {
        match compile("foo", false, "foo") {
            Err(EvalError::LikeEscapeTooLong) => {}
            _ => {
                panic!("expected error when using escape string with >1 character");
            }
        }
    }

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
            escape: &'a str,
            inputs: Vec<Input<'a>>,
        }
        let test_cases = vec![
            Pattern {
                needle: "ban%na!",
                case_insensitive: false,
                escape: "\\",
                inputs: vec![input("banana!", true)],
            },
            Pattern {
                needle: "ban%na!",
                case_insensitive: false,
                escape: "n",
                inputs: vec![input("banana!", false), input("ba%a!", true)],
            },
            Pattern {
                needle: "ban%%%na!",
                case_insensitive: false,
                escape: "%",
                inputs: vec![input("banana!", false), input("ban%na!", true)],
            },
            Pattern {
                needle: "foo",
                case_insensitive: true,
                escape: "\\",
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

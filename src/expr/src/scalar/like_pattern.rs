// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::mem;
use std::str::FromStr;

use derivative::Derivative;
use proptest::prelude::{Arbitrary, Strategy};
use regex::{Regex, RegexBuilder};
use serde::{Deserialize, Serialize};

use crate::scalar::EvalError;
use mz_lowertest::MzReflect;
use mz_ore::fmt::FormatBuffer;
use mz_repr::proto::{IntoRustIfSome, ProtoType, RustType, TryFromProtoError};
use proto_matcher_impl::ProtoSubpatternVec;

include!(concat!(env!("OUT_DIR"), "/mz_expr.scalar.like_pattern.rs"));

/// The number of subpatterns after which using regexes would be more efficient.
const MAX_SUBPATTERNS: usize = 5;

/// The escape character to use by default in LIKE patterns.
const DEFAULT_ESCAPE: char = '\\';
const DOUBLED_ESCAPE: &str = "\\\\";

/// Specifies escape behavior for the LIKE pattern.
#[derive(Clone, Copy, Debug)]
pub enum EscapeBehavior {
    /// No escape character.
    Disabled,
    /// Use a custom escape character.
    Char(char),
}

impl Default for EscapeBehavior {
    fn default() -> EscapeBehavior {
        EscapeBehavior::Char(DEFAULT_ESCAPE)
    }
}

impl FromStr for EscapeBehavior {
    type Err = EvalError;

    fn from_str(s: &str) -> Result<EscapeBehavior, EvalError> {
        let mut chars = s.chars();
        match chars.next() {
            None => Ok(EscapeBehavior::Disabled),
            Some(c) => match chars.next() {
                None => Ok(EscapeBehavior::Char(c)),
                Some(_) => Err(EvalError::LikeEscapeTooLong),
            },
        }
    }
}

/// Converts a pattern string that uses a custom escape character to one that uses the default.
pub fn normalize_pattern(pattern: &str, escape: EscapeBehavior) -> Result<String, EvalError> {
    match escape {
        EscapeBehavior::Disabled => Ok(pattern.replace(DEFAULT_ESCAPE, DOUBLED_ESCAPE)),
        EscapeBehavior::Char(DEFAULT_ESCAPE) => Ok(pattern.into()),
        EscapeBehavior::Char(custom_escape_char) => {
            let mut p = String::with_capacity(2 * pattern.len());
            let mut cs = pattern.chars();
            while let Some(c) = cs.next() {
                if c == custom_escape_char {
                    match cs.next() {
                        Some(c2) => {
                            p.push(DEFAULT_ESCAPE);
                            p.push(c2);
                        }
                        None => return Err(EvalError::UnterminatedLikeEscapeSequence),
                    }
                } else if c == DEFAULT_ESCAPE {
                    p.push_str(DOUBLED_ESCAPE);
                } else {
                    p.push(c);
                }
            }
            p.shrink_to_fit();
            Ok(p)
        }
    }
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

impl RustType<ProtoMatcher> for Matcher {
    fn into_proto(&self) -> ProtoMatcher {
        ProtoMatcher {
            pattern: self.pattern.clone(),
            case_insensitive: self.case_insensitive,
            matcher_impl: Some(self.matcher_impl.into_proto()),
        }
    }

    fn from_proto(proto: ProtoMatcher) -> Result<Self, TryFromProtoError> {
        Ok(Matcher {
            pattern: proto.pattern,
            case_insensitive: proto.case_insensitive,
            matcher_impl: proto
                .matcher_impl
                .into_rust_if_some("ProtoMatcher::matcher_impl")?,
        })
    }
}

#[derive(Debug, Clone, Deserialize, Serialize, MzReflect)]
enum MatcherImpl {
    String(Vec<Subpattern>),
    Regex(#[serde(with = "serde_regex")] Regex),
}

impl RustType<ProtoMatcherImpl> for MatcherImpl {
    fn into_proto(&self) -> ProtoMatcherImpl {
        use proto_matcher_impl::Kind::*;
        ProtoMatcherImpl {
            kind: Some(match self {
                MatcherImpl::String(subpatterns) => String(subpatterns.into_proto()),
                MatcherImpl::Regex(regex) => Regex(regex.into_proto()),
            }),
        }
    }

    fn from_proto(proto: ProtoMatcherImpl) -> Result<Self, TryFromProtoError> {
        use proto_matcher_impl::Kind::*;
        match proto.kind {
            Some(String(subpatterns)) => Ok(MatcherImpl::String(subpatterns.into_rust()?)),
            Some(Regex(regex)) => Ok(MatcherImpl::Regex(regex.into_rust()?)),
            None => Err(TryFromProtoError::missing_field("ProtoMatcherImpl::kind")),
        }
    }
}

impl RustType<ProtoSubpatternVec> for Vec<Subpattern> {
    fn into_proto(&self) -> ProtoSubpatternVec {
        ProtoSubpatternVec {
            vec: self.into_proto(),
        }
    }

    fn from_proto(proto: ProtoSubpatternVec) -> Result<Self, TryFromProtoError> {
        proto.vec.into_rust()
    }
}

/// Builds a Matcher that matches a SQL LIKE pattern.
pub fn compile(pattern: &str, case_insensitive: bool) -> Result<Matcher, EvalError> {
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
    let subpatterns = build_subpatterns(pattern)?;
    let matcher_impl = match case_insensitive || subpatterns.len() > MAX_SUBPATTERNS {
        false => MatcherImpl::String(subpatterns),
        true => MatcherImpl::Regex(build_regex(&subpatterns, case_insensitive)?),
    };
    Ok(Matcher {
        pattern: pattern.into(),
        case_insensitive,
        matcher_impl,
    })
}

pub fn any_matcher() -> impl Strategy<Value = Matcher> {
    // Generates a string out of a pool of characters. The pool has at least one
    // representative from the following classes of the characters (listed in
    // order of its appearance in the regex):
    // * Alphanumeric characters, both upper and lower-case.
    // * Control characters.
    // * Punctuation minus the escape character.
    // * Space characters.
    // * Multi-byte characters.
    // * _ and %, which are special characters for a like pattern.
    // * Escaped _ and %, plus the escape character itself. This implementation
    //   will have to be modified if we support choosing a different escape character.
    //
    // Syntax reference for LIKE here:
    // https://www.postgresql.org/docs/current/functions-matching.html#FUNCTIONS-LIKE
    (
        r"([[:alnum:]]|[[:cntrl:]]|([[[:punct:]]&&[^\\]])|[[:space:]]|华|_|%|(\\_)|(\\%)|(\\\\)){0, 50}",
        bool::arbitrary(),
    )
        .prop_map(|(pattern, case_insensitive)| compile(&pattern, case_insensitive).unwrap())
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

impl Subpattern {
    /// Converts a Subpattern to an equivalent regular expression and writes it to a given string.
    fn write_regex_to(&self, r: &mut String) {
        match self.consume {
            0 => {
                if self.many {
                    r.push_str(".*");
                }
            }
            1 => {
                r.push('.');
                if self.many {
                    r.push('+');
                }
            }
            n => {
                r.push_str(".{");
                write!(r, "{}", n);
                if self.many {
                    r.push(',');
                }
                r.push('}');
            }
        }
        regex_syntax::escape_into(&self.suffix, r);
    }
}

impl RustType<ProtoSubpattern> for Subpattern {
    fn into_proto(&self) -> ProtoSubpattern {
        ProtoSubpattern {
            consume: self.consume.into_proto(),
            many: self.many,
            suffix: self.suffix.clone(),
        }
    }

    fn from_proto(proto: ProtoSubpattern) -> Result<Self, TryFromProtoError> {
        Ok(Subpattern {
            consume: proto.consume.into_rust()?,
            many: proto.many,
            suffix: proto.suffix,
        })
    }
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
        // Another approach that may be interesting to look at is a
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
    let mut subpatterns = Vec::with_capacity(MAX_SUBPATTERNS);
    let mut current = Subpattern::default();
    let mut in_wildcard = true;
    let mut in_escape = false;
    for c in pattern.chars() {
        match c {
            c if !in_escape && c == DEFAULT_ESCAPE => {
                in_escape = true;
                in_wildcard = false;
            }
            '_' if !in_escape => {
                if !in_wildcard {
                    current.suffix.shrink_to_fit();
                    subpatterns.push(mem::take(&mut current));
                    in_wildcard = true;
                }
                current.consume += 1;
            }
            '%' if !in_escape => {
                if !in_wildcard {
                    current.suffix.shrink_to_fit();
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
    current.suffix.shrink_to_fit();
    subpatterns.push(current);
    subpatterns.shrink_to_fit();
    Ok(subpatterns)
}

/// Builds a regular expression that matches some parsed Subpatterns.
fn build_regex(subpatterns: &[Subpattern], case_insensitive: bool) -> Result<Regex, EvalError> {
    let mut r = String::from("^");
    for sp in subpatterns {
        sp.write_regex_to(&mut r);
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

    #[test]
    fn test_normalize_pattern() {
        struct TestCase<'a> {
            pattern: &'a str,
            escape: EscapeBehavior,
            expected: &'a str,
        }
        let test_cases = vec![
            TestCase {
                pattern: "",
                escape: EscapeBehavior::Disabled,
                expected: "",
            },
            TestCase {
                pattern: "ban%na!",
                escape: EscapeBehavior::default(),
                expected: "ban%na!",
            },
            TestCase {
                pattern: "ban%%%na!",
                escape: EscapeBehavior::Char('%'),
                expected: "ban\\%\\na!",
            },
            TestCase {
                pattern: "ban%na\\!",
                escape: EscapeBehavior::Char('n'),
                expected: "ba\\%\\a\\\\!",
            },
            TestCase {
                pattern: "ban%na\\!",
                escape: EscapeBehavior::Disabled,
                expected: "ban%na\\\\!",
            },
            TestCase {
                pattern: "ban\\na!",
                escape: EscapeBehavior::Char('n'),
                expected: "ba\\\\\\a!",
            },
            TestCase {
                pattern: "ban\\\\na!",
                escape: EscapeBehavior::Char('n'),
                expected: "ba\\\\\\\\\\a!",
            },
            TestCase {
                pattern: "food",
                escape: EscapeBehavior::Char('o'),
                expected: "f\\od",
            },
            TestCase {
                pattern: "漢漢",
                escape: EscapeBehavior::Char('漢'),
                expected: "\\漢",
            },
        ];

        for input in test_cases {
            let actual = normalize_pattern(input.pattern, input.escape).unwrap();
            assert!(
                actual == input.expected,
                "normalize_pattern({:?}, {:?}):\n\tactual: {:?}\n\texpected: {:?}\n",
                input.pattern,
                input.escape,
                actual,
                input.expected,
            );
        }
    }

    #[test]
    fn test_escape_too_long() {
        match EscapeBehavior::from_str("foo") {
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
            inputs: Vec<Input<'a>>,
        }
        let test_cases = vec![
            Pattern {
                needle: "ban%na!",
                case_insensitive: false,
                inputs: vec![input("banana!", true)],
            },
            Pattern {
                needle: "foo",
                case_insensitive: true,
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
            let matcher = compile(tc.needle, tc.case_insensitive).unwrap();
            for input in tc.inputs {
                let actual = matcher.is_match(input.haystack);
                assert!(
                    actual == input.matches,
                    "{:?} {} {:?}:\n\tactual: {:?}\n\texpected: {:?}\n",
                    input.haystack,
                    match tc.case_insensitive {
                        true => "ILIKE",
                        false => "LIKE",
                    },
                    tc.needle,
                    actual,
                    input.matches,
                );
            }
        }
    }
}

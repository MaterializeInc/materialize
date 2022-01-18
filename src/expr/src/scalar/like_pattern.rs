// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use std::cmp::Ordering;
use std::hash::{Hash, Hasher};

use regex::{Regex, RegexBuilder};
use serde::{Deserialize, Serialize};

use crate::scalar::EvalError;

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
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Matcher {
    pub pattern: String,
    pub case_insensitive: bool,
    string_matcher: Option<StringMatcher>,
    #[serde(with = "serde_regex")]
    regex_matcher: Option<Regex>,
}

impl Matcher {
    pub fn is_match(&self, text: &str) -> bool {
        match &self.string_matcher {
            Some(sm) => return sm.is_match(text),
            _ => {}
        }
        match &self.regex_matcher {
            Some(r) => r.is_match(text),
            _ => false,
        }
    }
}

impl PartialEq<Matcher> for Matcher {
    fn eq(&self, other: &Matcher) -> bool {
        self.pattern == *other.pattern && self.case_insensitive == other.case_insensitive
    }
}

impl Eq for Matcher {}

impl PartialOrd for Matcher {
    fn partial_cmp(&self, other: &Matcher) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Matcher {
    fn cmp(&self, other: &Matcher) -> Ordering {
        let result = self.pattern.cmp(&other.pattern);
        if result.is_eq() {
            return self.case_insensitive.cmp(&other.case_insensitive);
        }
        result
    }
}

impl Hash for Matcher {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.pattern.hash(state);
        self.case_insensitive.hash(state);
    }
}

/// Builds a Matcher that matches a SQL LIKE pattern.
pub fn compile(pattern: &str, case_insensitive: bool, escape: char) -> Result<Matcher, EvalError> {
    let mut matcher = Matcher {
        pattern: String::from(pattern),
        case_insensitive,
        string_matcher: None,
        regex_matcher: None,
    };
    if case_insensitive {
        matcher.regex_matcher = Some(build_regex(pattern, case_insensitive, escape)?);
    } else {
        matcher.string_matcher = Some(build_string_matcher(pattern, escape)?);
        // TODO: Fall back to regex if the chain of sub-patterns is too long?
    }
    Ok(matcher)
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

// Matches a chain of SUB-PATTERNs
#[derive(Debug, Clone, Deserialize, Serialize)]
struct StringMatcher {
    /// The minimum number of characters that can be consumed by the wildcard expression.
    consume: usize,
    /// Whether the wildcard expression can consume an arbitrary number of characters.
    many: bool,
    /// A string literal that is expected after the wildcards.
    suffix: String,
    /// Any sub-patterns that come after this one.
    next: Option<Box<StringMatcher>>,
}

impl StringMatcher {
    pub fn is_match(&self, text: &str) -> bool {
        let mut rest = text;
        // Go ahead and skip the minimum number of characters the sub-pattern consumes:
        if self.consume > 0 {
            let mut chars = rest.chars();
            for _ in 0..self.consume {
                if chars.next().is_none() {
                    return false;
                }
            }
            rest = chars.as_str();
        }
        if self.many {
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
            if self.suffix.len() == 0 {
                // Nothing to find... This should only happen in the last sub-pattern.
                assert!(self.next.is_none(), "empty suffix in middle of a pattern");
                return true;
            }
            // Use rfind so we perform a greedy capture, like Regex.
            let mut found = rest.rfind(&self.suffix);
            loop {
                match found {
                    None => return false,
                    Some(offset) => {
                        let end = offset + self.suffix.len();
                        let tmp = &rest[end..];
                        match &self.next {
                            None => return tmp.len() == 0,
                            Some(next) => {
                                if next.is_match(tmp) {
                                    return true;
                                }
                            }
                        }
                        // Didn't match, look for the next rfind.
                        if offset == 0 {
                            return false;
                        }
                        found = rest[..(end - 1)].rfind(&self.suffix);
                    }
                }
            }
        }
        // No string search needed, we just use a prefix match on rest.
        if !rest.starts_with(&self.suffix) {
            return false;
        }
        rest = &rest[self.suffix.len()..];
        match &self.next {
            None => rest.len() == 0,
            Some(matcher) => matcher.is_match(rest),
        }
    }
}

/// Breaks a LIKE pattern into a chain of sub-patterns.
fn build_string_matcher(pattern: &str, escape: char) -> Result<StringMatcher, EvalError> {
    let mut head = StringMatcher {
        consume: 0,
        many: false,
        suffix: String::from(""),
        next: None,
    };
    let mut tail = &mut head;
    let mut in_wildcard = true;
    let mut in_escape = false;

    for c in pattern.chars() {
        match c {
            c if !in_escape && c == escape => {
                in_escape = true;
                in_wildcard = false;
            }
            '_' if !in_escape => {
                if in_wildcard {
                    tail.consume += 1;
                } else {
                    // start a new partial match
                    tail.next = Some(Box::new(StringMatcher {
                        consume: 1,
                        many: false,
                        suffix: String::from(""),
                        next: None,
                    }));
                    tail = tail.next.as_deref_mut().unwrap();
                    in_wildcard = true;
                }
            }
            '%' if !in_escape => {
                if in_wildcard {
                    tail.many = true;
                } else {
                    // start a new partial match
                    tail.next = Some(Box::new(StringMatcher {
                        consume: 0,
                        many: true,
                        suffix: String::from(""),
                        next: None,
                    }));
                    tail = tail.next.as_deref_mut().unwrap();
                    in_wildcard = true;
                }
            }
            c => {
                tail.suffix.push(c);
                in_escape = false;
                in_wildcard = false;
            }
        }
    }
    if in_escape {
        return Err(EvalError::UnterminatedLikeEscapeSequence);
    }
    Ok(head)
}

// Unit Tests

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
        struct Pattern<'a> {
            needle: &'a str,
            case_insensitive: bool,
            escape: char,
            inputs: Vec<Input<'a>>,
        }

        let test_cases = vec![
            Pattern {
                needle: "",
                case_insensitive: false,
                escape: '\\',
                inputs: vec![
                    Input {
                        haystack: "",
                        matches: true,
                    },
                    Input {
                        haystack: "foo",
                        matches: false,
                    },
                ],
            },
            Pattern {
                needle: "foo",
                case_insensitive: false,
                escape: '\\',
                inputs: vec![
                    Input {
                        haystack: "",
                        matches: false,
                    },
                    Input {
                        haystack: "foo",
                        matches: true,
                    },
                    Input {
                        haystack: "bar",
                        matches: false,
                    },
                    Input {
                        haystack: "food",
                        matches: false,
                    },
                ],
            },
            Pattern {
                needle: "b_t",
                case_insensitive: false,
                escape: '\\',
                inputs: vec![
                    Input {
                        haystack: "",
                        matches: false,
                    },
                    Input {
                        haystack: "foo",
                        matches: false,
                    },
                    Input {
                        haystack: "bit",
                        matches: true,
                    },
                    Input {
                        haystack: "but",
                        matches: true,
                    },
                    Input {
                        haystack: "butt",
                        matches: false,
                    },
                    Input {
                        haystack: "bar",
                        matches: false,
                    },
                ],
            },
            Pattern {
                needle: "_o",
                case_insensitive: false,
                escape: '\\',
                inputs: vec![
                    Input {
                        haystack: "",
                        matches: false,
                    },
                    Input {
                        haystack: "fo",
                        matches: true,
                    },
                    Input {
                        haystack: "to",
                        matches: true,
                    },
                    Input {
                        haystack: "foo",
                        matches: false,
                    },
                ],
            },
            Pattern {
                needle: "_漢",
                case_insensitive: false,
                escape: '\\',
                inputs: vec![Input {
                    haystack: "漢漢",
                    matches: true,
                }],
            },
            Pattern {
                needle: "%AA%A",
                case_insensitive: false,
                escape: '\\',
                inputs: vec![Input {
                    haystack: "AAA",
                    matches: true,
                }],
            },
            Pattern {
                needle: "%aa_",
                case_insensitive: false,
                escape: '\\',
                inputs: vec![Input {
                    haystack: "aaa",
                    matches: true,
                }],
            },
            Pattern {
                needle: "__o",
                case_insensitive: false,
                escape: '\\',
                inputs: vec![
                    Input {
                        haystack: "",
                        matches: false,
                    },
                    Input {
                        haystack: "t",
                        matches: false,
                    },
                    Input {
                        haystack: "to",
                        matches: false,
                    },
                    Input {
                        haystack: "too",
                        matches: true,
                    },
                    Input {
                        haystack: "foo",
                        matches: true,
                    },
                    Input {
                        haystack: "food",
                        matches: false,
                    },
                ],
            },
            Pattern {
                needle: "f___",
                case_insensitive: false,
                escape: '\\',
                inputs: vec![
                    Input {
                        haystack: "",
                        matches: false,
                    },
                    Input {
                        haystack: "t",
                        matches: false,
                    },
                    Input {
                        haystack: "fo",
                        matches: false,
                    },
                    Input {
                        haystack: "foo",
                        matches: false,
                    },
                    Input {
                        haystack: "food",
                        matches: true,
                    },
                    Input {
                        haystack: "foods",
                        matches: false,
                    },
                ],
            },
            Pattern {
                needle: "bi_",
                case_insensitive: false,
                escape: '\\',
                inputs: vec![
                    Input {
                        haystack: "",
                        matches: false,
                    },
                    Input {
                        haystack: "bi",
                        matches: false,
                    },
                    Input {
                        haystack: "bin",
                        matches: true,
                    },
                    Input {
                        haystack: "bind",
                        matches: false,
                    },
                    Input {
                        haystack: "bit",
                        matches: true,
                    },
                    Input {
                        haystack: "foo",
                        matches: false,
                    },
                ],
            },
            Pattern {
                needle: "_i_i",
                case_insensitive: false,
                escape: '\\',
                inputs: vec![
                    Input {
                        haystack: "",
                        matches: false,
                    },
                    Input {
                        haystack: "b",
                        matches: false,
                    },
                    Input {
                        haystack: "bi",
                        matches: false,
                    },
                    Input {
                        haystack: "bid",
                        matches: false,
                    },
                    Input {
                        haystack: "bidi",
                        matches: true,
                    },
                    Input {
                        haystack: "bidi!",
                        matches: false,
                    },
                    Input {
                        haystack: "wifi",
                        matches: true,
                    },
                ],
            },
            Pattern {
                needle: "f%",
                case_insensitive: false,
                escape: '\\',
                inputs: vec![
                    Input {
                        haystack: "",
                        matches: false,
                    },
                    Input {
                        haystack: "f",
                        matches: true,
                    },
                    Input {
                        haystack: "fi",
                        matches: true,
                    },
                    Input {
                        haystack: "foo",
                        matches: true,
                    },
                    Input {
                        haystack: "food",
                        matches: true,
                    },
                    Input {
                        haystack: "bar",
                        matches: false,
                    },
                ],
            },
            Pattern {
                needle: "f%d",
                case_insensitive: false,
                escape: '\\',
                inputs: vec![
                    Input {
                        haystack: "",
                        matches: false,
                    },
                    Input {
                        haystack: "f",
                        matches: false,
                    },
                    Input {
                        haystack: "fa",
                        matches: false,
                    },
                    Input {
                        haystack: "fd",
                        matches: true,
                    },
                    Input {
                        haystack: "fad",
                        matches: true,
                    },
                    Input {
                        haystack: "food",
                        matches: true,
                    },
                    Input {
                        haystack: "foodie",
                        matches: false,
                    },
                    Input {
                        haystack: "bar",
                        matches: false,
                    },
                ],
            },
            Pattern {
                needle: "f%%d",
                case_insensitive: false,
                escape: '\\',
                inputs: vec![
                    Input {
                        haystack: "",
                        matches: false,
                    },
                    Input {
                        haystack: "f",
                        matches: false,
                    },
                    Input {
                        haystack: "fa",
                        matches: false,
                    },
                    Input {
                        haystack: "fd",
                        matches: true,
                    },
                    Input {
                        haystack: "fad",
                        matches: true,
                    },
                    Input {
                        haystack: "food",
                        matches: true,
                    },
                    Input {
                        haystack: "foodie",
                        matches: false,
                    },
                    Input {
                        haystack: "bar",
                        matches: false,
                    },
                ],
            },
            Pattern {
                needle: "f%d%e",
                case_insensitive: false,
                escape: '\\',
                inputs: vec![
                    Input {
                        haystack: "",
                        matches: false,
                    },
                    Input {
                        haystack: "f",
                        matches: false,
                    },
                    Input {
                        haystack: "fa",
                        matches: false,
                    },
                    Input {
                        haystack: "fad",
                        matches: false,
                    },
                    Input {
                        haystack: "fade",
                        matches: true,
                    },
                    Input {
                        haystack: "food",
                        matches: false,
                    },
                    Input {
                        haystack: "foodie",
                        matches: true,
                    },
                    Input {
                        haystack: "fiddle",
                        matches: true,
                    },
                    Input {
                        haystack: "feed",
                        matches: false,
                    },
                    Input {
                        haystack: "fde",
                        matches: true,
                    },
                    Input {
                        haystack: "fded",
                        matches: false,
                    },
                    Input {
                        haystack: "fdede",
                        matches: true,
                    },
                    Input {
                        haystack: "fdedede",
                        matches: true,
                    },
                    Input {
                        haystack: "fdedeeee",
                        matches: true,
                    },
                    Input {
                        haystack: "fdedeeeed",
                        matches: false,
                    },
                    Input {
                        haystack: "bar",
                        matches: false,
                    },
                ],
            },
            Pattern {
                needle: "\\es\\_\\a\\p\\e",
                case_insensitive: false,
                escape: '\\',
                inputs: vec![
                    Input {
                        haystack: "escape",
                        matches: false,
                    },
                    Input {
                        haystack: "es_ape",
                        matches: true,
                    },
                ],
            },
            Pattern {
                needle: "\\es\\_\\a\\p\\e",
                case_insensitive: true,
                escape: '\\',
                inputs: vec![
                    Input {
                        haystack: "escape",
                        matches: false,
                    },
                    Input {
                        haystack: "es_ape",
                        matches: true,
                    },
                ],
            },
            Pattern {
                needle: "ban%na!",
                case_insensitive: false,
                escape: 'n',
                inputs: vec![
                    Input {
                        haystack: "banana!",
                        matches: false,
                    },
                    Input {
                        haystack: "ba%a!",
                        matches: true,
                    },
                ],
            },
            Pattern {
                needle: "ban%%%na!",
                case_insensitive: false,
                escape: '%',
                inputs: vec![
                    Input {
                        haystack: "banana!",
                        matches: false,
                    },
                    Input {
                        haystack: "ban%na!",
                        matches: true,
                    },
                ],
            },
            Pattern {
                needle: "foo",
                case_insensitive: true,
                escape: '\\',
                inputs: vec![
                    Input {
                        haystack: "",
                        matches: false,
                    },
                    Input {
                        haystack: "f",
                        matches: false,
                    },
                    Input {
                        haystack: "fo",
                        matches: false,
                    },
                    Input {
                        haystack: "foo",
                        matches: true,
                    },
                    Input {
                        haystack: "FOO",
                        matches: true,
                    },
                    Input {
                        haystack: "Foo",
                        matches: true,
                    },
                    Input {
                        haystack: "fOO",
                        matches: true,
                    },
                    Input {
                        haystack: "food",
                        matches: false,
                    },
                ],
            },
            Pattern {
                needle: "ΜΆΪΟΣ",
                case_insensitive: true,
                escape: '\\',
                inputs: vec![
                    Input {
                        haystack: "",
                        matches: false,
                    },
                    Input {
                        haystack: "M",
                        matches: false,
                    },
                    Input {
                        haystack: "ΜΆΪΟΣ",
                        matches: true,
                    },
                    Input {
                        haystack: "Μάϊος",
                        matches: true,
                    },
                    Input {
                        haystack: "Μάϊοςs",
                        matches: false,
                    },
                    Input {
                        haystack: "Looks Greek to me!",
                        matches: false,
                    },
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

// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Regular expressions.

use std::borrow::Cow;
use std::cmp::Ordering;
use std::collections::BTreeSet;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::ops::Deref;

use mz_lowertest::MzReflect;
use regex::{Error, RegexBuilder};
use serde::de::Error as DeError;
use serde::ser::SerializeStruct;
use serde::{Deserialize, Deserializer, Serialize, Serializer, de};

/// The maximum size of a regex after compilation.
/// This is the same as the `Regex` crate's default at the time of writing.
///
/// Note: This number is mentioned in our user-facing docs at the "String operators" in the function
/// reference.
const MAX_REGEX_SIZE_AFTER_COMPILATION: usize = 10 * 1024 * 1024;

/// We also need a separate limit for the size of regexes before compilation. Even though the
/// `Regex` crate promises that using its `size_limit` option (which we set to the other limit,
/// `MAX_REGEX_SIZE_AFTER_COMPILATION`) would prevent excessive resource usage, this doesn't seem to
/// be the case. Since we compile regexes in envd, we need strict limits to prevent envd OOMs.
/// See <https://github.com/MaterializeInc/database-issues/issues/9907> for an example.
///
/// Note: This number is mentioned in our user-facing docs at the "String operators" in the function
/// reference.
const MAX_REGEX_SIZE_BEFORE_COMPILATION: usize = 1 * 1024 * 1024;

/// A hashable, comparable, and serializable regular expression type.
///
/// The  [`regex::Regex`] type, the de facto standard regex type in Rust, does
/// not implement [`PartialOrd`], [`Ord`] [`PartialEq`], [`Eq`], or [`Hash`].
/// The omissions are reasonable. There is no natural definition of ordering for
/// regexes. There *is* a natural definition of equality—whether two regexes
/// describe the same regular language—but that is an expensive property to
/// compute, and [`PartialEq`] is generally expected to be fast to compute.
///
/// This type wraps [`regex::Regex`] and imbues it with implementations of the
/// above traits. Two regexes are considered equal iff their string
/// representation is identical, plus flags, such as `case_insensitive`,
/// are identical. The [`PartialOrd`], [`Ord`], and [`Hash`] implementations
/// are similarly based upon the string representation plus flags. As
/// mentioned above, this is not the natural equivalence relation for regexes: for
/// example, the regexes `aa*` and `a+` define the same language, but would not
/// compare as equal with this implementation of [`PartialEq`]. Still, it is
/// often useful to have _some_ equivalence relation available (e.g., to store
/// types containing regexes in a hashmap) even if the equivalence relation is
/// imperfect.
///
/// [regex::Regex] is hard to serialize (because of the compiled code), so our approach is to
/// instead serialize this wrapper struct, where we skip serializing the actual regex field, and
/// we reconstruct the regex field from the other fields upon deserialization.
/// (Earlier, serialization was buggy due to <https://github.com/tailhook/serde-regex/issues/14>,
/// and also making the same mistake in our own protobuf serialization code.)
#[derive(Debug, Clone, MzReflect)]
pub struct Regex {
    pub case_insensitive: bool,
    pub dot_matches_new_line: bool,
    pub regex: regex::Regex,
}

impl Regex {
    /// A simple constructor for the default setting of `dot_matches_new_line: true`.
    /// See <https://www.postgresql.org/docs/current/functions-matching.html#POSIX-MATCHING-RULES>
    /// "newline-sensitive matching"
    pub fn new(pattern: &str, case_insensitive: bool) -> Result<Regex, RegexCompilationError> {
        Self::new_dot_matches_new_line(pattern, case_insensitive, true)
    }

    /// Allows explicitly setting `dot_matches_new_line`.
    pub fn new_dot_matches_new_line(
        pattern: &str,
        case_insensitive: bool,
        dot_matches_new_line: bool,
    ) -> Result<Regex, RegexCompilationError> {
        if pattern.len() > MAX_REGEX_SIZE_BEFORE_COMPILATION {
            return Err(RegexCompilationError::PatternTooLarge {
                pattern_size: pattern.len(),
            });
        }
        let mut regex_builder = RegexBuilder::new(pattern);
        regex_builder.case_insensitive(case_insensitive);
        regex_builder.dot_matches_new_line(dot_matches_new_line);
        regex_builder.size_limit(MAX_REGEX_SIZE_AFTER_COMPILATION);
        Ok(Regex {
            case_insensitive,
            dot_matches_new_line,
            regex: regex_builder.build()?,
        })
    }

    /// Returns the pattern string of the regex.
    pub fn pattern(&self) -> &str {
        // `as_str` returns the raw pattern as provided during construction,
        // and doesn't include any of the flags.
        self.regex.as_str()
    }

    /// Returns the indices of the capture groups that are guaranteed to
    /// participate in every successful match of this regex.
    ///
    /// A capture group does *not* participate in a match when it is skipped
    /// over by an optional construct: a `?` or `*` quantifier, a `{0,n}`-style
    /// repetition, or an unmatched branch of an alternation. The indices
    /// returned here are the complement of that set — capture groups that
    /// always capture a value when the regex matches at all, and whose
    /// extracted value is therefore non-nullable.
    ///
    /// Capture group indices match those used by [`regex::Captures`], where
    /// index 0 refers to the entire match and is always included (it
    /// necessarily participates whenever the regex matches).
    ///
    /// If the pattern cannot be parsed — which should not happen for a regex
    /// that compiled successfully — an empty set is returned, conservatively
    /// treating every capture group as nullable.
    pub fn non_nullable_capture_groups(&self) -> BTreeSet<u32> {
        let mut groups = BTreeSet::new();
        if let Ok(ast) = regex_syntax::ast::parse::Parser::new().parse(self.pattern()) {
            // The entire match (index 0) always participates in a match.
            groups.insert(0);
            collect_non_nullable_capture_groups(&ast, &mut groups);
        }
        groups
    }
}

/// Walks the always-matched portion of `ast`, recording into `groups` the index
/// of every capture group that is guaranteed to participate in a match.
///
/// This only recurses through nodes that a match cannot skip over, so every
/// capture group it reaches is non-nullable. It stops descending at any node
/// that introduces an optional context — an optional repetition or an
/// alternation — since nothing beneath such a node is guaranteed to match.
///
/// The recursion depth is bounded by the AST nesting depth. This is safe
/// because the only caller obtains `ast` from `regex_syntax`'s parser, whose
/// default `nest_limit` (250) rejects more deeply nested patterns before we
/// ever get here.
fn collect_non_nullable_capture_groups(ast: &regex_syntax::ast::Ast, groups: &mut BTreeSet<u32>) {
    use regex_syntax::ast::Ast;
    match ast {
        Ast::Group(group) => {
            if let Some(index) = group.capture_index() {
                groups.insert(index);
            }
            collect_non_nullable_capture_groups(&group.ast, groups);
        }
        // A required repetition (e.g. `+` or `{1,n}`) still always matches, so
        // keep descending; an optional one is handled by the catch-all below.
        Ast::Repetition(rep) if !repetition_is_optional(&rep.op.kind) => {
            collect_non_nullable_capture_groups(&rep.ast, groups);
        }
        Ast::Concat(concat) => {
            for ast in &concat.asts {
                collect_non_nullable_capture_groups(ast, groups);
            }
        }
        // Everything else — alternations (only one branch matches), optional
        // repetitions, and leaf nodes — cannot contribute a non-nullable
        // capture group, so we stop here.
        _ => {}
    }
}

/// Returns whether a repetition with the given kind may match zero times, in
/// which case the repeated subexpression might be skipped over by a match.
fn repetition_is_optional(kind: &regex_syntax::ast::RepetitionKind) -> bool {
    use regex_syntax::ast::{RepetitionKind, RepetitionRange};
    match kind {
        RepetitionKind::ZeroOrOne | RepetitionKind::ZeroOrMore => true,
        RepetitionKind::OneOrMore => false,
        RepetitionKind::Range(range) => match range {
            RepetitionRange::Exactly(min) | RepetitionRange::AtLeast(min) => *min == 0,
            RepetitionRange::Bounded(min, _) => *min == 0,
        },
    }
}

/// Error type for regex compilation failures.
#[derive(Debug, Clone)]
pub enum RegexCompilationError {
    /// Wrapper for regex crate's Error type.
    RegexError(Error),
    /// Regex pattern size exceeds MAX_REGEX_SIZE_BEFORE_COMPILATION.
    PatternTooLarge { pattern_size: usize },
}

impl fmt::Display for RegexCompilationError {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            RegexCompilationError::RegexError(e) => write!(f, "{}", e),
            RegexCompilationError::PatternTooLarge {
                pattern_size: patter_size,
            } => write!(
                f,
                "regex pattern too large ({} bytes, max {} bytes)",
                patter_size, MAX_REGEX_SIZE_BEFORE_COMPILATION
            ),
        }
    }
}

impl From<Error> for RegexCompilationError {
    fn from(e: Error) -> Self {
        RegexCompilationError::RegexError(e)
    }
}

impl PartialEq<Regex> for Regex {
    fn eq(&self, other: &Regex) -> bool {
        self.pattern() == other.pattern()
            && self.case_insensitive == other.case_insensitive
            && self.dot_matches_new_line == other.dot_matches_new_line
    }
}

impl Eq for Regex {}

impl PartialOrd for Regex {
    fn partial_cmp(&self, other: &Regex) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Regex {
    fn cmp(&self, other: &Regex) -> Ordering {
        (
            self.pattern(),
            self.case_insensitive,
            self.dot_matches_new_line,
        )
            .cmp(&(
                other.pattern(),
                other.case_insensitive,
                other.dot_matches_new_line,
            ))
    }
}

impl Hash for Regex {
    fn hash<H: Hasher>(&self, hasher: &mut H) {
        self.pattern().hash(hasher);
        self.case_insensitive.hash(hasher);
        self.dot_matches_new_line.hash(hasher);
    }
}

impl Deref for Regex {
    type Target = regex::Regex;

    fn deref(&self) -> &regex::Regex {
        &self.regex
    }
}

impl Serialize for Regex {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let mut state = serializer.serialize_struct("Regex", 3)?;
        state.serialize_field("pattern", &self.pattern())?;
        state.serialize_field("case_insensitive", &self.case_insensitive)?;
        state.serialize_field("dot_matches_new_line", &self.dot_matches_new_line)?;
        state.end()
    }
}

impl<'de> Deserialize<'de> for Regex {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        enum Field {
            Pattern,
            CaseInsensitive,
            DotMatchesNewLine,
        }

        impl<'de> Deserialize<'de> for Field {
            fn deserialize<D>(deserializer: D) -> Result<Field, D::Error>
            where
                D: Deserializer<'de>,
            {
                struct FieldVisitor;

                impl<'de> de::Visitor<'de> for FieldVisitor {
                    type Value = Field;

                    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                        formatter.write_str(
                            "pattern string or case_insensitive bool or dot_matches_new_line bool",
                        )
                    }

                    fn visit_str<E>(self, value: &str) -> Result<Field, E>
                    where
                        E: de::Error,
                    {
                        match value {
                            "pattern" => Ok(Field::Pattern),
                            "case_insensitive" => Ok(Field::CaseInsensitive),
                            "dot_matches_new_line" => Ok(Field::DotMatchesNewLine),
                            _ => Err(de::Error::unknown_field(value, FIELDS)),
                        }
                    }
                }

                deserializer.deserialize_identifier(FieldVisitor)
            }
        }

        struct RegexVisitor;

        impl<'de> de::Visitor<'de> for RegexVisitor {
            type Value = Regex;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("Regex serialized by the manual Serialize impl from above")
            }

            fn visit_seq<V>(self, mut seq: V) -> Result<Regex, V::Error>
            where
                V: de::SeqAccess<'de>,
            {
                let pattern = seq
                    .next_element::<Cow<str>>()?
                    .ok_or_else(|| de::Error::invalid_length(0, &self))?;
                let case_insensitive = seq
                    .next_element()?
                    .ok_or_else(|| de::Error::invalid_length(1, &self))?;
                let dot_matches_new_line = seq
                    .next_element()?
                    .ok_or_else(|| de::Error::invalid_length(2, &self))?;
                Regex::new_dot_matches_new_line(&pattern, case_insensitive, dot_matches_new_line)
                    .map_err(|err| {
                        V::Error::custom(format!(
                            "Unable to recreate regex during deserialization: {}",
                            err
                        ))
                    })
            }

            fn visit_map<V>(self, mut map: V) -> Result<Regex, V::Error>
            where
                V: de::MapAccess<'de>,
            {
                let mut pattern: Option<Cow<str>> = None;
                let mut case_insensitive: Option<bool> = None;
                let mut dot_matches_new_line: Option<bool> = None;
                while let Some(key) = map.next_key()? {
                    match key {
                        Field::Pattern => {
                            if pattern.is_some() {
                                return Err(de::Error::duplicate_field("pattern"));
                            }
                            pattern = Some(map.next_value()?);
                        }
                        Field::CaseInsensitive => {
                            if case_insensitive.is_some() {
                                return Err(de::Error::duplicate_field("case_insensitive"));
                            }
                            case_insensitive = Some(map.next_value()?);
                        }
                        Field::DotMatchesNewLine => {
                            if dot_matches_new_line.is_some() {
                                return Err(de::Error::duplicate_field("dot_matches_new_line"));
                            }
                            dot_matches_new_line = Some(map.next_value()?);
                        }
                    }
                }
                let pattern = pattern.ok_or_else(|| de::Error::missing_field("pattern"))?;
                let case_insensitive =
                    case_insensitive.ok_or_else(|| de::Error::missing_field("case_insensitive"))?;
                let dot_matches_new_line = dot_matches_new_line
                    .ok_or_else(|| de::Error::missing_field("dot_matches_new_line"))?;
                Regex::new_dot_matches_new_line(&pattern, case_insensitive, dot_matches_new_line)
                    .map_err(|err| {
                        V::Error::custom(format!(
                            "Unable to recreate regex during deserialization: {}",
                            err
                        ))
                    })
            }
        }

        const FIELDS: &[&str] = &["pattern", "case_insensitive", "dot_matches_new_line"];
        deserializer.deserialize_struct("Regex", FIELDS, RegexVisitor)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// This was failing before due to the derived serde serialization being incorrect, because of
    /// <https://github.com/tailhook/serde-regex/issues/14>.
    /// Nowadays, we use our own handwritten Serialize/Deserialize impls for our Regex wrapper struct.
    #[mz_ore::test]
    fn regex_serde_case_insensitive() {
        let pattern = "AAA";
        let orig_regex = Regex::new(pattern, true).unwrap();
        let serialized: String = serde_json::to_string(&orig_regex).unwrap();
        let roundtrip_result: Regex = serde_json::from_str(&serialized).unwrap();
        // Equality test between orig and roundtrip_result wouldn't work, because Eq doesn't test
        // the actual regex object. So test the actual regex functionality (concentrating on case
        // sensitivity).
        assert_eq!(orig_regex.regex.is_match("aaa"), true);
        assert_eq!(roundtrip_result.regex.is_match("aaa"), true);
        assert_eq!(pattern, roundtrip_result.pattern());
    }

    /// Test the roundtripping of `dot_matches_new_line`.
    /// (Similar to the above `regex_serde_case_insensitive`.)
    #[mz_ore::test]
    fn regex_serde_dot_matches_new_line() {
        {
            // dot_matches_new_line: true
            let pattern = "A.*B";
            let orig_regex = Regex::new_dot_matches_new_line(pattern, true, true).unwrap();
            let serialized: String = serde_json::to_string(&orig_regex).unwrap();
            let roundtrip_result: Regex = serde_json::from_str(&serialized).unwrap();
            assert_eq!(orig_regex.regex.is_match("axxx\nxxxb"), true);
            assert_eq!(roundtrip_result.regex.is_match("axxx\nxxxb"), true);
            assert_eq!(pattern, roundtrip_result.pattern());
        }
        {
            // dot_matches_new_line: false
            let pattern = "A.*B";
            let orig_regex = Regex::new_dot_matches_new_line(pattern, true, false).unwrap();
            let serialized: String = serde_json::to_string(&orig_regex).unwrap();
            let roundtrip_result: Regex = serde_json::from_str(&serialized).unwrap();
            assert_eq!(orig_regex.regex.is_match("axxx\nxxxb"), false);
            assert_eq!(roundtrip_result.regex.is_match("axxx\nxxxb"), false);
            assert_eq!(pattern, roundtrip_result.pattern());
        }
        {
            // dot_matches_new_line: default
            let pattern = "A.*B";
            let orig_regex = Regex::new(pattern, true).unwrap();
            let serialized: String = serde_json::to_string(&orig_regex).unwrap();
            let roundtrip_result: Regex = serde_json::from_str(&serialized).unwrap();
            assert_eq!(orig_regex.regex.is_match("axxx\nxxxb"), true);
            assert_eq!(roundtrip_result.regex.is_match("axxx\nxxxb"), true);
            assert_eq!(pattern, roundtrip_result.pattern());
        }
    }

    #[mz_ore::test]
    fn regex_non_nullable_capture_groups() {
        // `(idx, expected_non_nullable_indices)`. Index 0 (the whole match)
        // always participates and is always reported.
        let cases: &[(&str, &[u32])] = &[
            // A plain capture group always participates.
            ("(a)(b)", &[0, 1, 2]),
            // `?` and `*` make the group optional.
            ("(a)(b)?", &[0, 1]),
            ("(a)(b)*", &[0, 1]),
            // `+` and `{1,n}` keep the group required.
            ("(a)(b)+", &[0, 1, 2]),
            ("(a)(b){1,3}", &[0, 1, 2]),
            // `{0,n}` and `{0}` make the group optional.
            ("(a)(b){0,3}", &[0, 1]),
            ("(a)(b){0}", &[0, 1]),
            // Both branches of a top-level alternation are optional.
            ("(a)|(b)", &[0]),
            // A group wrapping an alternation still always participates, even
            // though groups *inside* the branches do not.
            ("(a|b)", &[0, 1]),
            ("((a)|(b))", &[0, 1]),
            // The soundness crux: a group that *wraps* an optional sub-expression
            // still always captures (the empty string), so it is non-nullable —
            // unlike a group that is *itself* made optional (`(a)?`, tested
            // above). Note the difference in `?`/`*` position.
            ("(a*)", &[0, 1]),
            ("(a?)", &[0, 1]),
            ("(a*)(b)?", &[0, 1]),
            // Nesting inside an optional construct is contagious.
            ("((a)(b))?", &[0]),
            // A required group following an optional one is still required.
            ("(a)?(b)", &[0, 2]),
            // Named groups are handled the same way.
            ("(?P<x>a)(?P<y>b)?", &[0, 1]),
            // Non-capturing groups don't introduce indices.
            ("(?:a)(b)", &[0, 1]),
            // Real-world patterns exercised by our test suite. The request-log
            // regex used by `test/testdrive/regex-sources.td`: `ip` (1), `ts`
            // (2), `path` (3), and `code` (6) always participate, while
            // `search_kw` (4) and `product_detail_id` (5) live inside
            // alternation branches and are therefore nullable.
            (
                r#"(?P<ip>\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}) - - \[(?P<ts>[^]]+)\] "(?P<path>(?:GET /search/\?kw=(?P<search_kw>[^ ]*) HTTP/\d\.\d)|(?:GET /detail/(?P<product_detail_id>[a-zA-Z0-9]+) HTTP/\d\.\d)|(?:[^"]+))" (?P<code>\d{3}) -"#,
                &[0, 1, 2, 3, 6],
            ),
            // The key/value regexes from
            // `test/testdrive/kafka-include-key-sources.td`, where both groups
            // always participate.
            (r#"(?P<animal>[^,]+),(?P<food>\w+)"#, &[0, 1, 2]),
        ];
        for (pattern, expected) in cases {
            let regex = Regex::new(pattern, false).unwrap();
            let got = regex.non_nullable_capture_groups();
            let expected: BTreeSet<u32> = expected.iter().copied().collect();
            assert_eq!(got, expected, "pattern: {pattern}");
        }
    }

    #[mz_ore::test]
    fn regex_serde_from_reader() {
        let pattern = "A.*B";
        let orig_regex = Regex::new_dot_matches_new_line(pattern, true, true).unwrap();

        let serialized: String = serde_json::to_string(&orig_regex).unwrap();
        let roundtrip_result: Regex = serde_json::from_reader(serialized.as_bytes()).unwrap();

        assert_eq!(orig_regex.regex.is_match("axxx\nxxxb"), true);
        assert_eq!(roundtrip_result.regex.is_match("axxx\nxxxb"), true);
        assert_eq!(pattern, roundtrip_result.pattern());

        let serialized = bincode::serialize(&orig_regex).unwrap();
        let roundtrip_result: Regex = bincode::deserialize_from(&*serialized).unwrap();

        assert_eq!(orig_regex.regex.is_match("axxx\nxxxb"), true);
        assert_eq!(roundtrip_result.regex.is_match("axxx\nxxxb"), true);
        assert_eq!(pattern, roundtrip_result.pattern());
    }
}

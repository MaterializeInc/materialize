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
use std::collections::BTreeMap;
use std::convert::Infallible;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::ops::Deref;

use mz_lowertest::MzReflect;
use regex::{Error, RegexBuilder};
use regex_syntax::ast::{Ast, GroupKind, RepetitionKind, RepetitionRange, Visitor, visit};
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

    /// Computes, for each capture group, whether the column it produces can be
    /// null (i.e., whether the group might fail to participate in a successful
    /// match of the overall regex).
    ///
    /// The returned map is keyed by capture group index. Index 0 (the implicit
    /// group capturing the entire match) is never included, as it always
    /// participates in a match.
    ///
    /// A capture group is *non-nullable* only when it is guaranteed to
    /// participate in every successful match. We determine this by walking the
    /// regex's syntax tree and marking a group as nullable if it appears inside
    /// any construct that its enclosing expression can match without it, namely:
    ///
    ///   * an alternation (`a|b`), where only one branch participates; or
    ///   * a repetition that may match zero times (`*`, `?`, `{0,n}`).
    ///
    /// This is a conservative analysis: a group is only reported as
    /// non-nullable when we can prove it always participates. If the pattern
    /// fails to parse for any reason (it shouldn't, since it already compiled),
    /// we fall back to treating every group as nullable.
    ///
    /// Replaces the previous behavior of unconditionally assuming nullability.
    /// See <https://github.com/MaterializeInc/database-issues/issues/612>.
    pub fn capture_group_nullability(&self) -> BTreeMap<u32, bool> {
        let ast = match regex_syntax::ast::parse::Parser::new().parse(self.pattern()) {
            Ok(ast) => ast,
            Err(_) => {
                // The pattern already compiled, so this should be unreachable;
                // be conservative and report every group as nullable.
                return self
                    .regex
                    .capture_names()
                    .enumerate()
                    .skip(1)
                    // TODO -- we can do better.
                    .map(|(i, _)| (u32::try_from(i).expect("capture group index fits in u32"), true))
                    .collect();
            }
        };
        // The visitor is infallible, so this irrefutable binding cannot fail.
        let Ok(nullable) = visit(&ast, NullabilityVisitor::default());
        nullable
    }
}

/// A [`Visitor`] that determines which capture groups of a regex may be null.
///
/// See [`Regex::capture_group_nullability`].
#[derive(Default)]
struct NullabilityVisitor {
    /// The number of nested "optional" constructs (alternations and
    /// zero-matching repetitions) we are currently inside. A capture group
    /// encountered while this is positive may be absent from a match.
    optional_depth: usize,
    /// Maps each capture group index to whether it is nullable.
    nullable: BTreeMap<u32, bool>,
}

impl Visitor for NullabilityVisitor {
    type Output = BTreeMap<u32, bool>;
    type Err = Infallible;

    fn finish(self) -> Result<BTreeMap<u32, bool>, Infallible> {
        Ok(self.nullable)
    }

    fn visit_pre(&mut self, ast: &Ast) -> Result<(), Infallible> {
        if let Ast::Group(group) = ast {
            if let Some(index) = capture_group_index(&group.kind) {
                // Record nullability based on the optional contexts enclosing
                // this group. (A group node itself never opens an optional
                // context, so this depth reflects only its ancestors.)
                self.nullable.insert(index, self.optional_depth > 0);
            }
        }
        if subexpressions_are_optional(ast) {
            self.optional_depth += 1;
        }
        Ok(())
    }

    fn visit_post(&mut self, ast: &Ast) -> Result<(), Infallible> {
        if subexpressions_are_optional(ast) {
            self.optional_depth -= 1;
        }
        Ok(())
    }
}

/// Returns the capture group index of `kind`, or `None` if it is not a
/// capturing group.
fn capture_group_index(kind: &GroupKind) -> Option<u32> {
    match kind {
        GroupKind::CaptureIndex(index) => Some(*index),
        GroupKind::CaptureName { name, .. } => Some(name.index),
        GroupKind::NonCapturing(_) => None,
    }
}

/// Returns whether the direct subexpressions of `ast` are optional, i.e.,
/// whether `ast` can match successfully without one of its subexpressions
/// participating. Any capture group nested below such an `ast` may be null.
fn subexpressions_are_optional(ast: &Ast) -> bool {
    match ast {
        // In an alternation only one branch participates in a match, so groups
        // in the other branches are absent.
        Ast::Alternation(_) => true,
        // A repetition whose lower bound is zero can match nothing at all, in
        // which case the repeated group does not participate.
        Ast::Repetition(repetition) => match &repetition.op.kind {
            RepetitionKind::ZeroOrOne | RepetitionKind::ZeroOrMore => true,
            RepetitionKind::OneOrMore => false,
            RepetitionKind::Range(range) => match range {
                RepetitionRange::Exactly(n) => *n == 0,
                RepetitionRange::AtLeast(n) => *n == 0,
                RepetitionRange::Bounded(min, _) => *min == 0,
            },
        },
        _ => false,
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
    fn regex_capture_group_nullability() {
        #[track_caller]
        fn check(pattern: &str, expected: &[(u32, bool)]) {
            let regex = Regex::new(pattern, false).unwrap();
            let expected: BTreeMap<u32, bool> = expected.iter().copied().collect();
            assert_eq!(
                regex.capture_group_nullability(),
                expected,
                "pattern: {pattern}"
            );
        }

        // No capture groups.
        check("abc", &[]);
        check("a(?:bc)d", &[]);

        // Top-level groups always participate.
        check("(a)", &[(1, false)]);
        check("(a)(b)", &[(1, false), (2, false)]);
        check("x(a)y(b)z", &[(1, false), (2, false)]);

        // Optional repetitions make groups nullable.
        check("(a)?", &[(1, true)]);
        check("(a)*", &[(1, true)]);
        check("(a)+", &[(1, false)]);
        check("(a){0,3}", &[(1, true)]);
        check("(a){2,3}", &[(1, false)]);
        check("(a){2,}", &[(1, false)]);
        check("(a){0,}", &[(1, true)]);

        // Alternations make all enclosed groups nullable.
        check("(a)|(b)", &[(1, true), (2, true)]);
        // But a group wrapping an alternation always participates, while the
        // inner branches are nullable.
        check("((a)|(b))", &[(1, false), (2, true), (3, true)]);

        // A mix: the first group always participates; the rest are optional.
        check("(a)(b)?", &[(1, false), (2, true)]);
        check("(a)((b)|(c))", &[(1, false), (2, false), (3, true), (4, true)]);

        // Nesting accumulates: a non-optional group inside an optional one is
        // still nullable.
        check("((a)(b))?", &[(1, true), (2, true), (3, true)]);

        // Named capture groups.
        check(
            "(?P<first>a)(?P<second>b)?",
            &[(1, false), (2, true)],
        );
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

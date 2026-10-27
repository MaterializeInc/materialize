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
use std::convert::Infallible;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::ops::Deref;

use regex::{Error, RegexBuilder};
use regex_syntax::ast::{self, Ast, ClassSetItem, Visitor};
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
/// This bounds the AST's node count, since every node needs at least one pattern byte, and with it
/// every node kind whose cost is bounded. Character classes are not, hence
/// `MAX_REGEX_CHARACTER_CLASSES`.
///
/// Note: This number is mentioned in our user-facing docs at the "String operators" in the function
/// reference.
const MAX_REGEX_SIZE_BEFORE_COMPILATION: usize = 1 * 1024 * 1024;

/// The maximum number of character classes a pattern may contain.
///
/// Byte length cannot bound what a compile spends, and neither can `size_limit`, which covers only
/// the compiled NFA. The memory goes to `regex-syntax` translating the AST into its HIR, where a
/// character class expands to hundreds of Unicode ranges out of a handful of pattern bytes. `\p{L}`
/// is five bytes, and under case folding `[a-\x{2FFF}]` is no cheaper, since the translator walks
/// the range codepoint by codepoint keeping one range per fold mapping.
/// See <https://github.com/MaterializeInc/database-issues/issues/9907>.
///
/// Counting rather than pricing is deliberate. Per-kind byte prices need calibrating against the
/// pinned `regex-syntax` and fail silently when one is set too low, whereas a count only asks
/// whether a kind can expand without bound, and over-counting merely costs a few legitimate
/// patterns.
///
/// This and the byte limit are independent, and multiply out to a bound on one compile that
/// `regex_two_limits_bound_what_a_compile_spends` holds against measurement.
///
/// Note: This number is mentioned in our user-facing docs at the "String operators" in the function
/// reference.
///
/// NOTE: `\p{L}{200000}` stays one class in the AST. `size_limit` rejects its expansion later, in
/// the NFA compiler.
const MAX_REGEX_CHARACTER_CLASSES: usize = 2000;

/// Counts the character classes in an AST, the node kinds whose translated size is not bounded by
/// the pattern bytes that spell them.
///
/// Flags do not enter into it. Folding is the expensive direction for every kind, and `(?i)` turns
/// it on from inside the pattern, out of reach of the flag [`Regex`] is built with, so a kind is
/// judged by what it costs folded.
///
/// NOTE: the matches are exhaustive on purpose. A `regex-syntax` bump adding a node kind has to
/// fail to compile here rather than default to "not a class", which no test could catch.
struct CharacterClassCounter {
    count: usize,
}

impl Visitor for CharacterClassCounter {
    type Output = usize;
    type Err = Infallible;

    fn finish(self) -> Result<usize, Infallible> {
        Ok(self.count)
    }

    fn visit_pre(&mut self, ast: &Ast) -> Result<(), Infallible> {
        self.count += match ast {
            Ast::ClassUnicode(_) | Ast::ClassPerl(_) => 1,
            // A bracketed class carries no ranges itself. Its items do, and are counted below.
            Ast::Empty(_)
            | Ast::Flags(_)
            | Ast::Literal(_)
            | Ast::Dot(_)
            | Ast::Assertion(_)
            | Ast::ClassBracketed(_)
            | Ast::Repetition(_)
            | Ast::Group(_)
            | Ast::Alternation(_)
            | Ast::Concat(_) => 0,
        };
        Ok(())
    }

    fn visit_class_set_item_pre(&mut self, item: &ClassSetItem) -> Result<(), Infallible> {
        // Items of a bracketed class, e.g. the `\p{L}` in `[a\p{L}]`. Each contributes its own
        // ranges, and a union only merges ranges, so counting the parts bounds the whole.
        self.count += match item {
            ClassSetItem::Unicode(_)
            | ClassSetItem::Perl(_)
            | ClassSetItem::Ascii(_)
            | ClassSetItem::Range(_) => 1,
            ClassSetItem::Empty(_)
            | ClassSetItem::Literal(_)
            | ClassSetItem::Bracketed(_)
            | ClassSetItem::Union(_) => 0,
        };
        Ok(())
    }
}

/// Counts the character classes in `pattern`, or `None` if it does not parse.
///
/// Not an escape hatch: an unparseable pattern is left to [`RegexBuilder`], and both parse through
/// the same `regex-syntax` with the same configuration (`nest_limit` 250, `octal` off), so a pattern
/// that fails here fails there too.
fn count_character_classes(pattern: &str) -> Option<usize> {
    let ast = ast::parse::Parser::new().parse(pattern).ok()?;
    match ast::visit(&ast, CharacterClassCounter { count: 0 }) {
        Ok(count) => Some(count),
        Err(infallible) => match infallible {},
    }
}

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
#[derive(Debug, Clone)]
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
        if let Some(classes) = count_character_classes(pattern) {
            if classes > MAX_REGEX_CHARACTER_CLASSES {
                return Err(RegexCompilationError::TooManyCharacterClasses { classes });
            }
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
}

/// Error type for regex compilation failures.
#[derive(Debug, Clone)]
pub enum RegexCompilationError {
    /// Wrapper for regex crate's Error type.
    RegexError(Error),
    /// Regex pattern size exceeds MAX_REGEX_SIZE_BEFORE_COMPILATION.
    PatternTooLarge { pattern_size: usize },
    /// Regex pattern contains more than MAX_REGEX_CHARACTER_CLASSES character classes.
    TooManyCharacterClasses { classes: usize },
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
            RegexCompilationError::TooManyCharacterClasses { classes } => write!(
                f,
                "regex pattern has too many character classes ({}, max {}). \
                 A character class is a Unicode, Perl or POSIX class such as `\\p{{L}}`, `\\d` \
                 or `[[:alpha:]]`, or a range such as `a-z`",
                classes, MAX_REGEX_CHARACTER_CLASSES
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
    use std::alloc::{GlobalAlloc, Layout, System};
    use std::cell::Cell;

    use regex_syntax::hir::translate::TranslatorBuilder;

    use super::*;

    /// Wraps the system allocator to record the peak heap the calling thread has asked for.
    ///
    /// Counters are per thread, so tests running in parallel do not perturb each other. Bytes
    /// requested are counted, not resident bytes, which keeps a measurement reproducible across
    /// allocators at the cost of running a little above true RSS.
    struct TrackingAllocator;

    thread_local! {
        /// Bytes this thread has requested and not yet freed.
        static LIVE_BYTES: Cell<usize> = const { Cell::new(0) };
        /// High-water mark of `LIVE_BYTES` since the last [`peak_translate_bytes`] reset.
        static PEAK_BYTES: Cell<usize> = const { Cell::new(0) };
    }

    /// NOTE: keep the bookkeeping allocation-free. A `Cell<usize>` behind a `const`-initialized
    /// `thread_local!` neither allocates on first access nor registers a destructor, so it cannot
    /// re-enter the allocator. `try_with` covers access during thread teardown.
    fn record_alloc(size: usize) {
        let _ = LIVE_BYTES.try_with(|live| {
            let now = live.get().saturating_add(size);
            live.set(now);
            let _ = PEAK_BYTES.try_with(|peak| {
                if now > peak.get() {
                    peak.set(now);
                }
            });
        });
    }

    fn record_dealloc(size: usize) {
        let _ = LIVE_BYTES.try_with(|live| live.set(live.get().saturating_sub(size)));
    }

    // SAFETY: every method forwards to `System` unchanged, so the allocator contract is whatever
    // `System` guarantees. The counters are observation only and never influence a returned
    // pointer.
    unsafe impl GlobalAlloc for TrackingAllocator {
        unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
            let ptr = unsafe { System.alloc(layout) };
            if !ptr.is_null() {
                record_alloc(layout.size());
            }
            ptr
        }

        unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
            let ptr = unsafe { System.alloc_zeroed(layout) };
            if !ptr.is_null() {
                record_alloc(layout.size());
            }
            ptr
        }

        unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
            record_dealloc(layout.size());
            unsafe { System.dealloc(ptr, layout) }
        }

        unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
            // Charged before the call and released after, so a growing `Vec` counts both blocks
            // at once. An out-of-place realloc does hold both, and the peak has to reflect that.
            record_alloc(new_size);
            let new_ptr = unsafe { System.realloc(ptr, layout, new_size) };
            record_dealloc(if new_ptr.is_null() {
                new_size
            } else {
                layout.size()
            });
            new_ptr
        }
    }

    #[global_allocator]
    static ALLOC: TrackingAllocator = TrackingAllocator;

    /// Peak heap the calling thread requests while parsing `pattern` and translating it to HIR,
    /// the stage the two limits bound.
    ///
    /// Compiling the NFA afterwards is left out: `size_limit` bounds it, and its near-constant cost
    /// would swamp the per-node signal at the pattern sizes a test can afford. The translator flags
    /// mirror [`Regex::new_dot_matches_new_line`], since `case_insensitive` alone moves a class's
    /// cost by an order of magnitude.
    fn peak_translate_bytes(pattern: &str, case_insensitive: bool) -> usize {
        let base = LIVE_BYTES.with(|live| live.get());
        PEAK_BYTES.with(|peak| peak.set(base));
        {
            let ast = ast::parse::Parser::new()
                .parse(pattern)
                .expect("pattern parses");
            let mut translator = TranslatorBuilder::new();
            translator.case_insensitive(case_insensitive);
            translator.dot_matches_new_line(true);
            let hir = translator
                .build()
                .translate(pattern, &ast)
                .expect("pattern translates");
            // The HIR and the AST are both live at the peak of a real compile, so hold them here.
            std::hint::black_box((&ast, &hir));
        }
        PEAK_BYTES.with(|peak| peak.get()).saturating_sub(base)
    }

    /// A class-heavy pattern one byte under `MAX_REGEX_SIZE_BEFORE_COMPILATION` costs gigabytes to
    /// translate, and is reachable from an unprivileged `SELECT 'x' ~* <pattern>`, so it has to be
    /// rejected without compiling.
    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // too slow
    fn regex_class_heavy_pattern_rejected_before_compiling() {
        let pattern = r"\p{L}".repeat(MAX_REGEX_SIZE_BEFORE_COMPILATION / r"\p{L}".len());
        assert!(pattern.len() <= MAX_REGEX_SIZE_BEFORE_COMPILATION);
        // The count does not depend on the flag, so both directions must be rejected.
        for case_insensitive in [true, false] {
            let err = Regex::new(&pattern, case_insensitive).expect_err("must be rejected");
            assert!(
                matches!(err, RegexCompilationError::TooManyCharacterClasses { .. }),
                "expected TooManyCharacterClasses, got {err:?}"
            );
        }
    }

    /// Case folding walks a range's whole span, so a wide one buys as many Unicode ranges per
    /// pattern byte as `\p{...}` does. It has to count as a class, not as the two codepoints it
    /// spells.
    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // too slow
    fn regex_wide_bracketed_range_counts_as_a_class() {
        let unit = r"[a-\x{2FFF}]";
        let pattern = unit.repeat(MAX_REGEX_SIZE_BEFORE_COMPILATION / unit.len());
        // `(?i)` reaches the same folding path from inside the pattern, so the flag we build with
        // must make no difference.
        for case_insensitive in [true, false] {
            let err = Regex::new(&pattern, case_insensitive).expect_err("must be rejected");
            assert!(
                matches!(err, RegexCompilationError::TooManyCharacterClasses { .. }),
                "expected TooManyCharacterClasses, got {err:?}"
            );
        }
    }

    /// Nested classes have to count too, else `[\p{L}]` evades the limit while costing what
    /// `\p{L}` costs.
    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // too slow
    fn regex_bracketed_class_is_counted() {
        let unit = r"[\p{L}]";
        let pattern = unit.repeat(MAX_REGEX_SIZE_BEFORE_COMPILATION / unit.len());
        let err = Regex::new(&pattern, true).expect_err("must be rejected");
        assert!(
            matches!(err, RegexCompilationError::TooManyCharacterClasses { .. }),
            "expected TooManyCharacterClasses, got {err:?}"
        );
    }

    /// The limit has to admit as well as reject: a pattern at the limit reaches the compiler, one
    /// class past it does not.
    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // Compiling thousands of classes is far too slow under miri.
    fn regex_class_limit_boundary_is_where_the_constant_puts_it() {
        let unit = r"\p{L}";
        assert!(
            unit.len() * (MAX_REGEX_CHARACTER_CLASSES + 1) <= MAX_REGEX_SIZE_BEFORE_COMPILATION
        );

        // Case-sensitive on purpose: this drives a real compile, and folding costs many times the
        // memory for no extra coverage. Whether that compile succeeds or hits `size_limit` is not
        // this test's business, only that our own limit lets it through.
        let at_limit = Regex::new(&unit.repeat(MAX_REGEX_CHARACTER_CLASSES), false);
        assert!(
            !matches!(
                at_limit,
                Err(RegexCompilationError::TooManyCharacterClasses { .. })
            ),
            "a pattern at the limit must reach the compiler, got {at_limit:?}"
        );

        let err = Regex::new(&unit.repeat(MAX_REGEX_CHARACTER_CLASSES + 1), false)
            .expect_err("must be rejected");
        assert!(
            matches!(err, RegexCompilationError::TooManyCharacterClasses { .. }),
            "one class over the limit must be turned away, got {err:?}"
        );
    }

    /// Literals carry no character classes, so a large pattern of them is bounded by its bytes
    /// alone and still has to compile.
    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // too slow
    fn regex_long_literal_pattern_still_compiles() {
        // A long alternation of literals, the shape a generated pattern takes. Kept at 12000
        // branches, past which the NFA runs into MAX_REGEX_SIZE_AFTER_COMPILATION instead.
        let pattern = vec!["abcdefgh"; 12_000].join("|");
        assert!(pattern.len() > 100 * 1024);
        assert_eq!(count_character_classes(&pattern), Some(0));
        assert!(Regex::new(&pattern, true).is_ok());
    }

    /// The limits bound a compile only if the per-item ceilings below hold, and those are facts
    /// about the pinned `regex-syntax`, so measure them. A bump that makes a node kind pricier
    /// fails here.
    ///
    /// Each case also pins how [`count_character_classes`] classifies its kind, the one judgement
    /// the limits rest on. A kind left uncounted while it can expand without bound shows up as the
    /// measured cost outgrowing the bytes that bought it.
    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // Unicode class translation is far too slow under miri.
    fn regex_two_limits_bound_what_a_compile_spends() {
        /// Ceiling on the heap spent translating one character class. However wide a range is, it
        /// keeps at most one range per entry of the simple case-fold table, so it stays under this.
        const MAX_MEMORY_PER_CHARACTER_CLASS: usize = 96 * 1024;
        /// Ceiling on the heap spent translating one node that is not a character class. A literal
        /// under `i` is the costliest, since it folds to a small class rather than staying a byte.
        const MAX_MEMORY_PER_AST_NODE: usize = 768;

        /// Enough repetitions for a class's own cost to dominate a compile's fixed cost, few
        /// enough that a case stays within a few tens of megabytes.
        const CLASS_UNITS: usize = 150;
        /// A non-class node costs orders of magnitude less, so it needs proportionally more
        /// repetitions to clear that same bar.
        const CHEAP_UNITS: usize = 10_000;

        // (unit, case_insensitive, repetitions, character classes per repetition)
        let cases: &[(&str, bool, usize, usize)] = &[
            // Unicode-property and Perl classes. `\p{Grapheme_Base}` under `i` is the costliest
            // single class found, so it is what sizes `MAX_MEMORY_PER_CHARACTER_CLASS`.
            (r"\p{Grapheme_Base}", true, CLASS_UNITS, 1),
            (r"\p{XID_Continue}", true, CLASS_UNITS, 1),
            (r"\p{Alphabetic}", true, CLASS_UNITS, 1),
            (r"\p{L}", true, CLASS_UNITS, 1),
            (r"\p{L}", false, CLASS_UNITS, 1),
            (r"\w", true, CLASS_UNITS, 1),
            (r"\W", true, CLASS_UNITS, 1),
            (r"\d", true, CLASS_UNITS, 1),
            (r"\s", true, CLASS_UNITS, 1),
            // The same nested in a bracketed class, where every item counts on its own.
            (r"[\p{L}]", true, CLASS_UNITS, 1),
            (r"[a\p{L}\d]", true, CLASS_UNITS, 2),
            // Ranges, from one too narrow to reach the case-fold table up to one covering all of
            // it, including the stretch of Latin where it is densest. Span alone does not predict
            // the cost: `[\x{100}-\x{17F}]` costs 2.5x what the equally wide `[\x00-\x7F]` does,
            // which is why ranges are counted rather than priced.
            (r"[a-\x{10FFFF}]", true, CLASS_UNITS, 1),
            (r"[^a-\x{10FFFF}]", true, CLASS_UNITS, 1),
            (r"[a-\x{2FFF}]", true, CLASS_UNITS, 1),
            (r"[\x{100}-\x{250}]", true, CLASS_UNITS, 1),
            (r"[\x{100}-\x{17F}]", true, CLASS_UNITS, 1),
            (r"[a-\x{FF}]", true, CLASS_UNITS, 1),
            (r"[a-z]", true, CLASS_UNITS, 1),
            (r"[[:alpha:]]", true, CLASS_UNITS, 1),
            // Nodes carrying no class, bounded by the byte limit alone. `[abc]` is here on
            // purpose: a bracketed class of literals is not counted, so its cost has to stay
            // within what its bytes buy.
            ("a", true, CHEAP_UNITS, 0),
            ("a", false, CHEAP_UNITS, 0),
            (".", true, CHEAP_UNITS, 0),
            ("[abc]", true, CHEAP_UNITS, 0),
            ("(a)", false, CHEAP_UNITS, 0),
            ("a|", false, CHEAP_UNITS, 0),
            ("a*", false, CHEAP_UNITS, 0),
            ("a{2}", false, CHEAP_UNITS, 0),
            ("^", false, CHEAP_UNITS, 0),
        ];

        for (unit, case_insensitive, repeats, classes_per_unit) in cases {
            let pattern = unit.repeat(*repeats);
            let expected_classes = repeats * classes_per_unit;
            let classes = count_character_classes(&pattern).expect("pattern parses");
            assert_eq!(
                classes, expected_classes,
                "`{unit}` x{repeats} counts as {classes} character classes, expected \
                 {expected_classes}"
            );

            // The same product the two limits bound, evaluated for this pattern. Pattern bytes
            // stand in for the node count, which they bound.
            let bound =
                classes * MAX_MEMORY_PER_CHARACTER_CLASS + pattern.len() * MAX_MEMORY_PER_AST_NODE;
            let measured = peak_translate_bytes(&pattern, *case_insensitive);
            assert!(
                measured <= bound,
                "`{unit}` x{repeats} (case_insensitive: {case_insensitive}) allocated {measured} \
                 bytes, above the {bound} bytes the limits allow it, so they no longer bound what \
                 a compile spends"
            );
        }
    }

    /// A counted repetition stays one class in the AST, and the NFA compiler's incremental
    /// `size_limit` check rejects it. Pin that, since the count does not multiply by repetition
    /// bounds.
    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // too slow
    fn regex_counted_repetition_rejected_by_size_limit() {
        let err = Regex::new(r"\p{L}{200000}", true).expect_err("must be rejected");
        assert!(
            matches!(err, RegexCompilationError::RegexError(_)),
            "expected the regex crate's own error, got {err:?}"
        );
    }

    /// Short patterns, the overwhelmingly common case, must be unaffected.
    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // too slow
    fn regex_ordinary_patterns_unaffected() {
        for pattern in [
            r"a+b",
            r"^\d{3}-\d{4}$",
            r"\p{L}+",
            r"[a-zA-Z0-9_]*",
            r"(?i)foo|bar",
        ] {
            assert!(
                Regex::new(pattern, false).is_ok(),
                "{pattern} should compile"
            );
        }
    }

    /// A pattern we cannot parse must fall through to `RegexBuilder`, so users keep getting the
    /// regex crate's error message rather than one about the budget.
    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // too slow
    fn regex_unparseable_pattern_reports_regex_error() {
        let err = Regex::new("(", false).expect_err("must be rejected");
        assert!(
            matches!(err, RegexCompilationError::RegexError(_)),
            "expected the regex crate's own error, got {err:?}"
        );
    }

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

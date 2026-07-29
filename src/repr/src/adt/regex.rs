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
/// This bounds the AST parse, which is what `estimate_compile_memory` needs before it can say
/// anything about the pattern. It does not bound the compile itself, see
/// `MAX_REGEX_COMPILE_MEMORY`.
///
/// Note: This number is mentioned in our user-facing docs at the "String operators" in the function
/// reference.
const MAX_REGEX_SIZE_BEFORE_COMPILATION: usize = 1 * 1024 * 1024;

/// The maximum heap a single compile may be projected to allocate.
///
/// Neither limit above bounds what a compile actually spends. `size_limit` bounds the compiled
/// NFA, and a pattern's byte length bounds only itself. The memory goes to `regex-syntax`
/// translating the pattern's AST into its HIR, a stage that runs before anything `size_limit`
/// measures. Cost there is linear in the AST's node count, but the cost *per node* spans two
/// orders of magnitude, from a few hundred bytes for a literal or a group up to tens of kilobytes
/// for a Unicode property under the `i` flag, so a byte budget cannot bound it. The per-kind costs
/// themselves are a fact about the `regex-syntax` version we pin, so they are measured rather than
/// written down here, in `regex_compile_memory_charges_cover_measured_cost`.
///
/// A 1 MiB pattern of `\p{L}` under the `i` flag, one byte under
/// `MAX_REGEX_SIZE_BEFORE_COMPILATION`, therefore allocates gigabytes before returning
/// `CompiledTooBig`. Since we compile regexes in envd, that made any `SELECT` an OOM vector,
/// which is what this limit exists to close.
///
/// NOTE: a counted repetition needs no budget of its own. `\p{L}{200000}` stays two AST nodes,
/// and the expansion happens later in the NFA compiler, where `size_limit` already rejects it
/// incrementally.
const MAX_REGEX_COMPILE_MEMORY: usize = 1024 * 1024 * 1024;

/// Charged per AST node against `MAX_REGEX_COMPILE_MEMORY`. Covers every node kind that expands
/// to at most a handful of Unicode ranges: literals, `.`, `[a-c]`, groups, repetitions,
/// assertions. Sized a few times above the costliest of them so it absorbs allocator differences,
/// but capped well under `MAX_REGEX_COMPILE_MEMORY / MAX_REGEX_SIZE_BEFORE_COMPILATION`, since a
/// 1 MiB pattern of plain literals, which is what a machine-generated alternation looks like, has
/// to stay within the budget. See `regex_cheap_nodes_do_not_collide_with_the_byte_limit`.
const COMPILE_MEMORY_PER_NODE: usize = 768;

/// Charged in place of `COMPILE_MEMORY_PER_NODE` for a Unicode-property or Perl class item, the
/// node kinds that expand to hundreds of ranges.
///
/// Sized well above the costliest such node measured, which is what
/// `regex_compile_memory_charges_cover_measured_cost` checks. The margin is deliberate. Which
/// property costs the most is a fact about the Unicode tables `regex-syntax` ships, so a version
/// bump can shift it, and no legitimate pattern carries the thousands of Unicode classes it takes
/// to reach the budget.
const COMPILE_MEMORY_PER_CLASS: usize = 96 * 1024;

/// Sums the projected translation cost of every node in an AST.
struct CompileMemoryEstimator {
    estimate: usize,
}

impl Visitor for CompileMemoryEstimator {
    type Output = usize;
    type Err = Infallible;

    fn finish(self) -> Result<usize, Infallible> {
        Ok(self.estimate)
    }

    fn visit_pre(&mut self, ast: &Ast) -> Result<(), Infallible> {
        self.estimate = self.estimate.saturating_add(match ast {
            Ast::ClassUnicode(_) | Ast::ClassPerl(_) => COMPILE_MEMORY_PER_CLASS,
            _ => COMPILE_MEMORY_PER_NODE,
        });
        Ok(())
    }

    fn visit_class_set_item_pre(&mut self, item: &ClassSetItem) -> Result<(), Infallible> {
        // Items nested inside a bracketed class, e.g. the `\p{L}` in `[a\p{L}]`. Each contributes
        // its own ranges to the one translated class, so each is charged. A union can only merge
        // ranges, never add them, so the sum of the parts is an upper bound on the whole.
        self.estimate = self.estimate.saturating_add(match item {
            ClassSetItem::Unicode(_) | ClassSetItem::Perl(_) => COMPILE_MEMORY_PER_CLASS,
            _ => COMPILE_MEMORY_PER_NODE,
        });
        Ok(())
    }
}

/// Projects the heap that compiling `pattern` would allocate, or `None` if it does not parse.
///
/// A pattern we cannot parse is left to [`RegexBuilder`], which rejects it with the message users
/// already see. That is not an escape hatch: both parse through the same `regex-syntax` version
/// with the same default configuration (`nest_limit` 250, `octal` off), so a pattern that fails
/// here fails there too.
fn estimate_compile_memory(pattern: &str) -> Option<usize> {
    let ast = ast::parse::Parser::new().parse(pattern).ok()?;
    match ast::visit(&ast, CompileMemoryEstimator { estimate: 0 }) {
        Ok(estimate) => Some(estimate),
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
        if let Some(estimate) = estimate_compile_memory(pattern) {
            if estimate > MAX_REGEX_COMPILE_MEMORY {
                return Err(RegexCompilationError::PatternTooExpensive { estimate });
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
    /// Compiling the pattern is projected to allocate more than
    /// MAX_REGEX_COMPILE_MEMORY.
    PatternTooExpensive { estimate: usize },
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
            RegexCompilationError::PatternTooExpensive { estimate } => write!(
                f,
                "regex pattern too expensive to compile (needs about {} bytes, max {} bytes); \
                 reduce the number of character classes",
                estimate, MAX_REGEX_COMPILE_MEMORY
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

    /// Wraps the system allocator to record the peak heap the calling thread has asked for, so
    /// [`regex_compile_memory_charges_cover_measured_cost`] can hold the charges in this module
    /// against what `regex-syntax` actually allocates.
    ///
    /// Counters are per thread, so tests running in parallel do not perturb each other. What is
    /// counted is bytes requested, not resident bytes, which makes a measurement reproducible
    /// across allocators at the cost of running a little above true RSS.
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
    /// the stage [`estimate_compile_memory`] projects.
    ///
    /// Compiling the NFA afterwards is deliberately left out. `size_limit` bounds it, and its
    /// cost is close to a constant (a few times `MAX_REGEX_SIZE_AFTER_COMPILATION`) that would
    /// swamp the per-node signal at the pattern sizes this test can afford. The translator flags
    /// mirror what [`Regex::new_dot_matches_new_line`] hands to [`RegexBuilder`], since
    /// `case_insensitive` alone moves a class's cost by an order of magnitude.
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

    /// A class-heavy pattern one byte under `MAX_REGEX_SIZE_BEFORE_COMPILATION` used to allocate
    /// ~7.8 GB in envd before erroring, reachable from an unprivileged `SELECT 'x' ~* <pattern>`.
    /// It must now be rejected without compiling.
    #[mz_ore::test]
    fn regex_class_heavy_pattern_rejected_before_compiling() {
        let pattern = r"\p{L}".repeat(MAX_REGEX_SIZE_BEFORE_COMPILATION / r"\p{L}".len());
        assert!(pattern.len() <= MAX_REGEX_SIZE_BEFORE_COMPILATION);
        // Case-insensitive is the expensive direction, but the estimate does not depend on the
        // flag, so both must be rejected.
        for case_insensitive in [true, false] {
            let err = Regex::new(&pattern, case_insensitive).expect_err("must be rejected");
            assert!(
                matches!(err, RegexCompilationError::PatternTooExpensive { .. }),
                "expected PatternTooExpensive, got {err:?}"
            );
        }
    }

    /// The guard has to charge classes nested in a bracketed class too, else `[\p{L}]` repeated
    /// evades it while costing the same as `\p{L}` repeated.
    #[mz_ore::test]
    fn regex_bracketed_class_is_charged() {
        let unit = r"[\p{L}]";
        let pattern = unit.repeat(MAX_REGEX_SIZE_BEFORE_COMPILATION / unit.len());
        let err = Regex::new(&pattern, true).expect_err("must be rejected");
        assert!(
            matches!(err, RegexCompilationError::PatternTooExpensive { .. }),
            "expected PatternTooExpensive, got {err:?}"
        );
    }

    /// The budget has to admit as well as reject. A pattern one class short of it must still reach
    /// the compiler, and one class past it must not, so that the limit lands where the constants
    /// say it does rather than somewhere earlier.
    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // Compiling tens of thousands of classes is far too slow under miri.
    fn regex_budget_boundary_is_where_the_constants_put_it() {
        let unit = r"\p{L}";
        // The concat node holding the classes is charged too, hence the extra node.
        let classes =
            (MAX_REGEX_COMPILE_MEMORY - COMPILE_MEMORY_PER_NODE) / COMPILE_MEMORY_PER_CLASS;
        assert!(unit.len() * (classes + 1) <= MAX_REGEX_SIZE_BEFORE_COMPILATION);

        // Case-sensitive on purpose. The estimate does not depend on the flag, but the compile
        // this drives all the way into the NFA compiler does, and folding costs many times the
        // memory for no extra coverage here.
        let err = Regex::new(&unit.repeat(classes), false).expect_err("must be rejected");
        assert!(
            matches!(err, RegexCompilationError::RegexError(_)),
            "one class under the budget must reach the compiler, got {err:?}"
        );

        let err = Regex::new(&unit.repeat(classes + 1), false).expect_err("must be rejected");
        assert!(
            matches!(err, RegexCompilationError::PatternTooExpensive { .. }),
            "one class over the budget must be turned away, got {err:?}"
        );
    }

    /// The budget must not cost legitimate long patterns. A plain literal is orders of magnitude
    /// cheaper per node than a Unicode class, so a large pattern of them still has to compile.
    #[mz_ore::test]
    fn regex_long_literal_pattern_still_compiles() {
        // A long alternation of literals, the shape a generated pattern takes. Kept at 12000
        // branches: past that the compiled NFA runs into MAX_REGEX_SIZE_AFTER_COMPILATION, which
        // would make this pass or fail for an unrelated reason.
        let pattern = vec!["abcdefgh"; 12_000].join("|");
        assert!(pattern.len() > 100 * 1024);
        assert!(Regex::new(&pattern, true).is_ok());
    }

    /// `COMPILE_MEMORY_PER_NODE` has to stay low enough that the largest pattern
    /// `MAX_REGEX_SIZE_BEFORE_COMPILATION` admits still fits the budget when every byte is a cheap
    /// node. Otherwise the two limits collide and the byte limit becomes unreachable, silently
    /// tightening what users can submit.
    #[mz_ore::test]
    fn regex_cheap_nodes_do_not_collide_with_the_byte_limit() {
        let pattern = "a".repeat(MAX_REGEX_SIZE_BEFORE_COMPILATION);
        assert!(
            estimate_compile_memory(&pattern).unwrap() <= MAX_REGEX_COMPILE_MEMORY,
            "a pattern of single-byte nodes at the byte limit must fit the memory budget"
        );
    }

    /// The charges are calibrated against the `regex-syntax` version we pin, so hold them against
    /// what that version actually allocates, one case per cost tier. A bump that makes a node kind
    /// pricier then fails here, rather than silently reopening the OOM vector the budget closes.
    ///
    /// Costs are measured here rather than written down, so nothing to keep in sync: the failure
    /// message reports what a kind now costs against what it is charged, which is what a bump
    /// needs in order to decide whether the charge or the case list has to move.
    #[mz_ore::test]
    #[cfg_attr(miri, ignore)] // Unicode class translation is far too slow under miri.
    fn regex_compile_memory_charges_cover_measured_cost() {
        /// Enough class nodes for their own cost to dominate a compile's fixed cost, few enough
        /// that a case stays within a few tens of megabytes.
        const CLASS_NODES: usize = 150;
        /// Cheap nodes cost orders of magnitude less each, so they need proportionally more
        /// repetitions to clear that same bar.
        const CHEAP_NODES: usize = 10_000;

        let cases: &[(&str, bool, usize)] = &[
            // Unicode-property and Perl classes, charged `COMPILE_MEMORY_PER_CLASS`.
            // `\p{Grapheme_Base}` under `i` is the costliest of these we have found.
            (r"\p{Grapheme_Base}", true, CLASS_NODES),
            (r"\p{XID_Continue}", true, CLASS_NODES),
            (r"\p{Alphabetic}", true, CLASS_NODES),
            (r"\p{L}", true, CLASS_NODES),
            (r"\p{L}", false, CLASS_NODES),
            (r"\w", true, CLASS_NODES),
            (r"\W", true, CLASS_NODES),
            (r"\d", true, CLASS_NODES),
            (r"\s", true, CLASS_NODES),
            // The same classes nested in a bracketed class, where each item is charged separately.
            (r"[\p{L}]", true, CLASS_NODES),
            (r"[a\p{L}\d]", true, CLASS_NODES),
            // Everything else, charged `COMPILE_MEMORY_PER_NODE`. A literal under `i` is the
            // costliest of these, since it folds to a small class rather than staying a byte.
            ("a", true, CHEAP_NODES),
            ("a", false, CHEAP_NODES),
            (".", true, CHEAP_NODES),
            ("[a-c]", true, CHEAP_NODES),
            ("(a)", false, CHEAP_NODES),
            ("a|", false, CHEAP_NODES),
            ("a*", false, CHEAP_NODES),
            ("a{2}", false, CHEAP_NODES),
            ("^", false, CHEAP_NODES),
        ];

        for (unit, case_insensitive, repeats) in cases {
            let pattern = unit.repeat(*repeats);
            let charged = estimate_compile_memory(&pattern).expect("pattern parses");
            let measured = peak_translate_bytes(&pattern, *case_insensitive);
            assert!(
                measured <= charged,
                "`{unit}` x{repeats} (case_insensitive: {case_insensitive}) allocated {measured} \
                 bytes, above the {charged} bytes charged for it, so the budget no longer bounds \
                 what a compile spends"
            );
        }
    }

    /// A counted repetition needs no budget of its own: it stays small in the AST, and the NFA
    /// compiler's incremental `size_limit` check rejects it. Pin that, since the budget
    /// deliberately does not multiply by repetition bounds.
    #[mz_ore::test]
    fn regex_counted_repetition_rejected_by_size_limit() {
        let err = Regex::new(r"\p{L}{200000}", true).expect_err("must be rejected");
        assert!(
            matches!(err, RegexCompilationError::RegexError(_)),
            "expected the regex crate's own error, got {err:?}"
        );
    }

    /// Short patterns, the overwhelmingly common case, must be unaffected.
    #[mz_ore::test]
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

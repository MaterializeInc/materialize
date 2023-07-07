// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Regular expressions.

use std::cmp::Ordering;
use std::hash::{Hash, Hasher};
use std::ops::Deref;

use mz_lowertest::MzReflect;
use mz_proto::{RustType, TryFromProtoError};
use proptest::prelude::any;
use proptest::prop_compose;
use regex::{Error, RegexBuilder};
use serde::{Deserialize, Serialize};

include!(concat!(env!("OUT_DIR"), "/mz_repr.adt.regex.rs"));

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
/// representation is identical. The [`PartialOrd`], [`Ord`], and [`Hash`]
/// implementations are similarly based upon the string representation. As
/// mentioned above, is not the natural equivalence relation for regexes: for
/// example, the regexes `aa*` and `a+` define the same language, but would not
/// compare as equal with this implementation of [`PartialEq`]. Still, it is
/// often useful to have _some_ equivalence relation available (e.g., to store
/// types containing regexes in a hashmap) even if the equivalence relation is
/// imperfect.
///
/// This type also implements [`Serialize`] and [`Deserialize`] via the
/// [`serde_regex`] crate.
#[derive(Debug, Clone, Deserialize, Serialize, MzReflect)]
pub struct Regex {
    pub pattern: String,
    pub case_insensitive: bool,
    // TODO(benesch): watch for a more efficient binary serialization for
    // [`regex::Regex`] (https://github.com/rust-lang/regex/issues/258). The
    // `serde_regex` crate serializes to a string and is forced to recompile the
    // regex during deserialization.
    // TODO(ggevay): The serialization based on `as_str()` is actually incorrect, because it's
    // not capturing `case_insensitive`! I fixed this for the protobuf serialization, but not yet
    // for the Deserialize/Serialize implementations (which are derived by `serde_regex`).
    // This is not so urgent to fix, because this is only used by non-essential things AFAIK:
    // - `MzReflect`
    // - `EXPLAIN AS JSON` (through `DisplayJson`)
    #[serde(with = "serde_regex")]
    pub regex: regex::Regex,
}

impl Regex {
    pub fn new(pattern: String, case_insensitive: bool) -> Result<Regex, Error> {
        let mut regex_builder = RegexBuilder::new(pattern.as_str());
        regex_builder.case_insensitive(case_insensitive);
        Ok(Regex {
            pattern,
            case_insensitive,
            regex: regex_builder.build()?,
        })
    }
}

impl PartialEq<Regex> for Regex {
    fn eq(&self, other: &Regex) -> bool {
        self.pattern == other.pattern && self.case_insensitive == other.case_insensitive
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
        (&self.pattern, self.case_insensitive).cmp(&(&other.pattern, other.case_insensitive))
    }
}

impl Hash for Regex {
    fn hash<H: Hasher>(&self, hasher: &mut H) {
        self.pattern.hash(hasher);
        self.case_insensitive.hash(hasher);
    }
}

impl Deref for Regex {
    type Target = regex::Regex;

    fn deref(&self) -> &regex::Regex {
        &self.regex
    }
}

impl RustType<ProtoRegex> for Regex {
    fn into_proto(&self) -> ProtoRegex {
        ProtoRegex {
            pattern: self.pattern.clone(),
            case_insensitive: self.case_insensitive,
        }
    }

    fn from_proto(proto: ProtoRegex) -> Result<Self, TryFromProtoError> {
        Ok(Regex::new(proto.pattern, proto.case_insensitive)?)
    }
}

// TODO: this is not really high priority, but this could modified to generate a
// greater variety of regexes. Ignoring the beginning-of-file/line and EOF/EOL
// symbols, the only regexes being generated are `.{#repetitions}` and
// `x{#repetitions}`.
const BEGINNING_SYMBOLS: &str = r"((\\A)|\^)?";
const CHARACTERS: &str = r"[\.x]{1}";
const REPETITIONS: &str = r"((\*|\+|\?|(\{[1-9],?\}))\??)?";
const END_SYMBOLS: &str = r"(\$|(\\z))?";

prop_compose! {
    pub fn any_regex()
                (b in BEGINNING_SYMBOLS, c in CHARACTERS,
                 r in REPETITIONS, e in END_SYMBOLS, case_insensitive in any::<bool>())
                -> Regex {
        let string = format!("{}{}{}{}", b, c, r, e);
        Regex::new(string, case_insensitive).unwrap()
    }
}

#[cfg(test)]
mod tests {
    use mz_proto::protobuf_roundtrip;
    use proptest::prelude::*;

    use super::*;

    proptest! {
        #[mz_ore::test]
        #[cfg_attr(miri, ignore)] // too slow
        fn regex_protobuf_roundtrip( expect in any_regex() ) {
            let actual =  protobuf_roundtrip::<_, ProtoRegex>(&expect);
            assert!(actual.is_ok());
            assert_eq!(actual.unwrap(), expect);
        }
    }
}

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

use proptest::prop_compose;
use serde::{Deserialize, Serialize};

use mz_lowertest::MzReflect;

use crate::proto::newapi::{RustType, TryFromProtoError};

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
pub struct Regex(
    // TODO(benesch): watch for a more efficient binary serialization for
    // [`regex::Regex`] (https://github.com/rust-lang/regex/issues/258). The
    // `serde_regex` crate serializes to a string and is forced to recompile the
    // regex during deserialization.
    #[serde(with = "serde_regex")] pub regex::Regex,
);

impl PartialEq<Regex> for Regex {
    fn eq(&self, other: &Regex) -> bool {
        self.0.as_str() == other.0.as_str()
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
        self.0.as_str().cmp(other.0.as_str())
    }
}

impl Hash for Regex {
    fn hash<H: Hasher>(&self, hasher: &mut H) {
        self.0.as_str().hash(hasher)
    }
}

impl Deref for Regex {
    type Target = regex::Regex;

    fn deref(&self) -> &regex::Regex {
        &self.0
    }
}

impl RustType<String> for regex::Regex {
    fn into_proto(&self) -> String {
        self.as_str().to_string()
    }

    fn from_proto(proto: String) -> Result<Self, TryFromProtoError> {
        Ok(regex::Regex::new(&proto)?)
    }
}

impl RustType<ProtoRegex> for Regex {
    fn into_proto(&self) -> ProtoRegex {
        ProtoRegex {
            regex: self.0.as_str().to_string(),
        }
    }

    fn from_proto(proto: ProtoRegex) -> Result<Self, TryFromProtoError> {
        Ok(Regex(regex::Regex::from_proto(proto.regex)?))
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
                 r in REPETITIONS, e in END_SYMBOLS)
                -> Regex {
        let string = format!("{}{}{}{}", b, c, r, e);
        Regex(regex::Regex::new(&string).unwrap())
    }
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;
    use crate::proto::newapi::protobuf_roundtrip;

    proptest! {
        #[test]
        fn regex_protobuf_roundtrip( expect in any_regex() ) {
            let actual =  protobuf_roundtrip::<_, ProtoRegex>(&expect);
            assert!(actual.is_ok());
            assert_eq!(actual.unwrap(), expect);
        }
    }
}

// Copyright Materialize, Inc. All rights reserved.
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

use serde::{Deserialize, Serialize};

/// A hashable, comparable, and serializable regular expression type.
///
/// The  [`regex::Regex`] type, the de facto standard regex type in Rust, does
/// not implement [`PartialOrd`], [`Ord`] [`PartialEq`], [`Eq`], or [`Hash`].
/// The omissions are reasonable. There is no natural definition of ordering for
/// regexes. There *is* a natural definition of equality—whether two regexes
/// describe the same regular language—but that is an expensive propery to
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
#[derive(Debug, Clone, Deserialize, Serialize)]
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

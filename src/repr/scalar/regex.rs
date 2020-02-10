// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::hash::{Hash, Hasher};

/// A regular expression object suitable for use in `repr::Datum`. The
/// object has perhaps surprising comparison semantics as a result: two regexes
/// are considered equal iff their string representation is identical. The
/// [`PartialOrd`], [`Ord`], and [`Hash`] implementations are similarly based
/// upon the string representation. This is not the natural equivalence relation
/// for regexes: for example, the regexes `aa*` and `a+` define the same
/// language, but would not compare as equal with this implementation of
/// [`PartialEq`].
///
/// TODO(benesch): watch for a more efficient binary serialization for
/// [`regex::Regex`] (https://github.com/rust-lang/regex/issues/258). The
/// `serde_regex` crate serializes to a string and is forced to recompile
/// the regex during deserialization.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Regex(#[serde(with = "serde_regex")] pub regex::Regex);

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

impl std::ops::Deref for Regex {
    type Target = regex::Regex;
    fn deref(&self) -> &regex::Regex {
        &self.0
    }
}

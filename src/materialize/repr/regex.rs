// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::hash::{Hash, Hasher};

/// A regular expression object suitable for use in `crate::repr::Datum`. The
/// object exhibits a number of strange properties as a result: it claims to
/// implement [`PartialEq`], [`Eq`], [`PartialOrd`], and [`Ord`], though it will
/// panic if any of those traits' methods are invoked. Efficiently implementing
/// these traits with reasonable semantics is challenging, as doing so requires
/// either peeking inside the regex object's internal state, or serializing the
/// regex to a string, both of which are expensive. Since it is not currently
/// possible to produce a plan that compares regexes, for now it seems best to
/// panic.
///
/// We could avoid introducing this weirdness by store regexes as strings in the
/// dataflow graph, compiling them into regex objects at the last possible
/// moment (e.g., in the LIKE operator). But this would require special casing
/// all regex optimizations to recover acceptable performance; we'd need
/// regex-specific constant propagation to avoid recompiling constant regexes on
/// every expression evaluation, and we wouldn't be able to pass compiled
/// regexes between workers without forcing a recompilation.*
///
/// *Actually, I think the serde_regex serialization currently serializes
/// regexes to a string and therefore recompiles them during deserialization.
/// There is interest in the official regex crate about a more efficient
/// binary serialization, though: https://github.com/rust-lang/regex/issues/258.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Regex(#[serde(with = "serde_regex")] pub regex::Regex);

impl Regex {
    pub fn is_match(&self, s: &str) -> bool {
        self.0.is_match(s)
    }
}

impl PartialEq<Regex> for Regex {
    fn eq(&self, _: &Regex) -> bool {
        panic!("Regex is not comparable")
    }
}

impl Eq for Regex {}

impl PartialOrd for Regex {
    fn partial_cmp(&self, _: &Regex) -> Option<Ordering> {
        panic!("Regex is not comparable")
    }
}

impl Ord for Regex {
    fn cmp(&self, _: &Regex) -> Ordering {
        panic!("Regex is not comparable")
    }
}

impl Hash for Regex {
    fn hash<H: Hasher>(&self, _: &mut H) {
        panic!("Regex is not hashable")
    }
}

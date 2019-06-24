// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::hash::{Hash, Hasher};

/// A regular expression object suitable for use in `crate::repr::Datum`. The
/// object exhibits a number of strange properties as a result: it claims to
/// implement [`PartialEq`], [`Eq`], [`PartialOrd`], and [`Ord`], though it
/// will panic if any of those traits' methods are invoked.
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

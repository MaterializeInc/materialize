// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Apache license, Version 2.0

//! Hash utilities.

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

/// Computes the hash of an object implementing [`Hash`].
pub fn hash<T: Hash>(t: &T) -> u64 {
    let mut hasher = DefaultHasher::new();
    t.hash(&mut hasher);
    hasher.finish()
}

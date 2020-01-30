// Copyright 2019-2020 Materialize Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

//! Hash utilities.

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

/// Computes the hash of an object implementing [`Hash`].
pub fn hash<T: Hash>(t: &T) -> u64 {
    let mut hasher = DefaultHasher::new();
    t.hash(&mut hasher);
    hasher.finish()
}

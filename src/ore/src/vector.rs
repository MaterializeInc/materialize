// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Vector utilities

use std::mem::{align_of, size_of};

/// Extension methods for vectors
pub trait VecExt {
    /// Create a new vector that re-uses the same allocation as an old one.
    fn repurpose_allocation<T2>(self) -> Vec<T2>;
}

impl<T1> VecExt for Vec<T1> {
    fn repurpose_allocation<T2>(mut self) -> Vec<T2> {
        assert!(size_of::<T1>() == size_of::<T2>());
        assert!(align_of::<T1>() == align_of::<T2>());
        self.clear();
        let cap = self.capacity();
        let p = self.as_mut_ptr().cast();
        std::mem::forget(self);
        // This is safe because `T1` and `T2` have the same size and alignment,
        // `p`'s allocation is no longer owned by `self` (since that has been forgotten),
        // and `p` was previously allocated with capacity `cap`.
        unsafe { Vec::from_raw_parts(p, 0, cap) }
    }
}

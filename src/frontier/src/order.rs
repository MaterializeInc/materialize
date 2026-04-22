// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License").

//! Partial order trait for frontier comparison.
//!
//! This mirrors `timely::order::PartialOrder` so call sites can migrate from
//! `timely::PartialOrder` to `mz_frontier::PartialOrder` with just an import
//! change. For totally-ordered primitives, the impls are provided here; for
//! antichains, see [`crate::antichain`].

use crate::antichain::{Antichain, AntichainRef, MutableAntichain};

/// A type that is partially ordered.
pub trait PartialOrder<Rhs: ?Sized = Self>: PartialEq<Rhs> {
    /// Returns true iff `self <= other`.
    fn less_equal(&self, other: &Rhs) -> bool;

    /// Returns true iff `self < other`.
    #[must_use]
    fn less_than(&self, other: &Rhs) -> bool {
        self.less_equal(other) && self != other
    }
}

/// A type whose [`PartialOrder`] is also total.
pub trait TotalOrder: PartialOrder {}

macro_rules! impl_total {
    ($($t:ty),*) => {
        $(
            impl PartialOrder for $t {
                #[inline] fn less_equal(&self, other: &Self) -> bool { self <= other }
                #[inline] fn less_than(&self, other: &Self) -> bool { self < other }
            }
            impl TotalOrder for $t {}
        )*
    };
}

impl_total!(u8, u16, u32, u64, u128, usize, i8, i16, i32, i64, i128, isize, (), std::time::Duration);

// ---------------------------------------------------------------------------
// Antichain impls
// ---------------------------------------------------------------------------

// Lattice ordering on antichains: `a <= b` iff every element of `b` is
// dominated by some element of `a`. For a totally-ordered, 0-or-1-element
// antichain this simplifies to: `a <= b` iff `a` contains a smaller-or-equal
// element than `b`, with the empty antichain acting as the top of the lattice.

impl<T: Ord> PartialOrder for Antichain<T> {
    fn less_equal(&self, other: &Self) -> bool {
        match (self.as_option(), other.as_option()) {
            (_, None) => true,
            (None, Some(_)) => false,
            (Some(a), Some(b)) => a <= b,
        }
    }
}

impl<'a, T: Ord> PartialOrder for AntichainRef<'a, T> {
    fn less_equal(&self, other: &Self) -> bool {
        match (self.first(), other.first()) {
            (_, None) => true,
            (None, Some(_)) => false,
            (Some(a), Some(b)) => a <= b,
        }
    }
}

impl<T: Ord> PartialOrder for MutableAntichain<T> {
    fn less_equal(&self, other: &Self) -> bool {
        PartialOrder::less_equal(&self.frontier(), &other.frontier())
    }
}

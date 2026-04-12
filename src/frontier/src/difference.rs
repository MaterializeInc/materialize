// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License").

//! Difference types for update streams.

/// A type that can report whether it is zero.
pub trait IsZero {
    fn is_zero(&self) -> bool;
}

/// A type with an associative addition operation.
pub trait Semigroup: Clone + IsZero {
    fn plus_equals(&mut self, other: &Self);
}

/// A [`Semigroup`] with an identity element.
pub trait Monoid: Semigroup {
    fn zero() -> Self;
}

macro_rules! impl_diff {
    ($($t:ty),*) => {
        $(
            impl IsZero for $t {
                #[inline]
                fn is_zero(&self) -> bool { *self == 0 }
            }
            impl Semigroup for $t {
                #[inline]
                fn plus_equals(&mut self, other: &Self) { *self += other; }
            }
            impl Monoid for $t {
                #[inline]
                fn zero() -> Self { 0 }
            }
        )*
    };
}

impl_diff!(i8, i16, i32, i64, i128, isize, u8, u16, u32, u64, u128, usize);

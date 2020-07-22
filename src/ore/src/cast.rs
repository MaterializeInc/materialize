// Copyright Materialize, Inc. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Cast utilities.

/// A trait for safe, simple, and infallible casts.
///
/// `CastFrom` is like [`std::convert::From`], but it is implemented for some
/// platform-specific casts that are missing from the standard library. For
/// example, there is no `From<u32> for usize` implementation, because Rust may
/// someday support platforms where usize is smaller than 32 bits. Since we
/// don't care about such platforms, we are happy to provide a `CastFrom<u32>
/// for usize` implementation.
///
/// `CastFrom` should be preferred to the `as` operator, since the `as` operator
/// will silently truncate if the target type is smaller than the source type.
/// When applicable, `CastFrom` should also be preferred to the
/// [`std::convert::TryFrom`] trait, as `TryFrom` will produce a runtime error,
/// while `CastFrom` will produce a compile-time error.
pub trait CastFrom<T> {
    /// Performs the cast.
    fn cast_from(from: T) -> Self;
}

macro_rules! cast_from {
    ($from:ty, $to:ty) => {
        impl CastFrom<$from> for $to {
            fn cast_from(from: $from) -> $to {
                from as $to
            }
        }
    };
}

#[cfg(any(target_pointer_width = "32", target_pointer_width = "64"))]
cast_from!(u32, usize);

#[cfg(target_pointer_width = "64")]
cast_from!(i32, usize);

#[cfg(target_pointer_width = "64")]
cast_from!(u64, usize);

cast_from!(usize, u64);

#[cfg(target_pointer_width = "64")]
cast_from!(i64, isize);

cast_from!(isize, i64);

// Copyright 2019 Materialize, Inc. All rights reserved.
//
// This file is part of Materialize. Materialize may not be used or
// distributed without the express permission of Materialize, Inc.

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

#[cfg(any(target_pointer_width = "32", target_pointer_width = "64"))]
impl CastFrom<u32> for usize {
    fn cast_from(from: u32) -> usize {
        from as usize
    }
}

#[cfg(target_pointer_width = "64")]
impl CastFrom<u64> for usize {
    fn cast_from(from: u64) -> usize {
        from as usize
    }
}

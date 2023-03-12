// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License in the LICENSE file at the
// root of this repository, or online at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

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
        paste::paste! {
            impl crate::cast::CastFrom<$from> for $to {
                #[allow(clippy::as_conversions)]
                #[inline(always)]
                fn cast_from(from: $from) -> $to {
                    from as $to
                }
            }

            /// Casts [`$from`] to [`$to`].
            ///
            /// This is equivalent to the [`crate::cast::CastFrom`] implementation but is
            /// available as a `const fn`.
            #[allow(clippy::as_conversions)]
            #[inline(always)]
            pub const fn [< $from _to_ $to >](from: $from) -> $to {
                from as $to
            }
        }
    };
}

#[cfg(target_pointer_width = "32")]
/// Safe casts for 32bit platforms
mod target32 {
    // size_of<from> < size_of<target>
    cast_from!(u8, usize);
    cast_from!(u16, usize);
    cast_from!(u8, isize);
    cast_from!(i8, isize);
    cast_from!(u16, isize);
    cast_from!(i16, isize);

    cast_from!(usize, u64);
    cast_from!(usize, i64);
    cast_from!(usize, u128);
    cast_from!(usize, i128);
    cast_from!(isize, i64);
    cast_from!(isize, i128);

    // size_of<from> == size_of<target>
    cast_from!(usize, u32);
    cast_from!(isize, i32);
    cast_from!(u32, usize);
    cast_from!(i32, isize);
}
#[cfg(target_pointer_width = "32")]
pub use target32::*;

#[cfg(target_pointer_width = "64")]
/// Safe casts for 64bit platforms
pub mod target64 {
    // size_of<from> < size_of<target>
    cast_from!(u8, usize);
    cast_from!(u16, usize);
    cast_from!(u32, usize);
    cast_from!(u8, isize);
    cast_from!(i8, isize);
    cast_from!(u16, isize);
    cast_from!(i16, isize);
    cast_from!(u32, isize);
    cast_from!(i32, isize);

    cast_from!(usize, u128);
    cast_from!(usize, i128);
    cast_from!(isize, i128);

    // size_of<from> == size_of<target>
    cast_from!(usize, u64);
    cast_from!(isize, i64);
    cast_from!(u64, usize);
    cast_from!(i64, isize);
}
#[cfg(target_pointer_width = "64")]
pub use target64::*;

// TODO(petrosagg): remove these once the std From impls become const
cast_from!(u8, u16);
cast_from!(u8, i16);
cast_from!(u8, u32);
cast_from!(u8, i32);
cast_from!(u8, u64);
cast_from!(u8, i64);
cast_from!(u8, u128);
cast_from!(u8, i128);
cast_from!(u16, u32);
cast_from!(u16, i32);
cast_from!(u16, u64);
cast_from!(u16, i64);
cast_from!(u16, u128);
cast_from!(u16, i128);
cast_from!(u32, u64);
cast_from!(u32, i64);
cast_from!(u32, u128);
cast_from!(u32, i128);
cast_from!(u64, u128);
cast_from!(u64, i128);
cast_from!(i8, i16);
cast_from!(i8, i32);
cast_from!(i8, i64);
cast_from!(i8, i128);
cast_from!(i16, i32);
cast_from!(i16, i64);
cast_from!(i16, i128);
cast_from!(i32, i64);
cast_from!(i32, i128);
cast_from!(i64, i128);

/// A trait for reinterpreting casts.
///
/// `ReinterpretCast` is like `as`, but it allows the caller to be specific about their
/// intentions to reinterpreting the bytes from one type to another. For example, if we
/// have some `u32` that we want to use as the return value of a postgres function, and
/// we don't mind converting large unsigned numbers to negative signed numbers, then
/// we would use `ReinterpretCast<i32>`.
///
/// `ReinterpretCast` should be preferred to the `as` operator, since it explicitly
/// conveys the intention to reinterpret the type.
pub trait ReinterpretCast<T> {
    /// Performs the cast.
    fn reinterpret_cast(from: T) -> Self;
}

macro_rules! reinterpret_cast {
    ($from:ty, $to:ty) => {
        impl ReinterpretCast<$from> for $to {
            #[allow(clippy::as_conversions)]
            fn reinterpret_cast(from: $from) -> $to {
                from as $to
            }
        }
    };
}

reinterpret_cast!(u8, i8);
reinterpret_cast!(i8, u8);
reinterpret_cast!(u16, i16);
reinterpret_cast!(i16, u16);
reinterpret_cast!(u32, i32);
reinterpret_cast!(i32, u32);
reinterpret_cast!(u64, i64);
reinterpret_cast!(i64, u64);

/// A trait for attempted casts.
///
/// `TryCast` is like `as`, but returns `None` if
/// the conversion can't be round-tripped.
///
/// Note: there may be holes in the domain of `try_cast_from`,
/// which is probably why `TryFrom` wasn't implemented for floats in the
/// standard library. For example, `i64::MAX` can be converted to
/// `f64`, but `i64::MAX - 1` can't.
pub trait TryCastFrom<T>: Sized {
    /// Attempts to perform the cast
    fn try_cast_from(from: T) -> Option<Self>;
}

/// Implement `TryCastFrom` for the specified types.
/// This is only necessary for types for which `as` exists,
/// but `TryFrom` doesn't (notably floats).
macro_rules! try_cast_from {
    ($from:ty, $to:ty) => {
        impl crate::cast::TryCastFrom<$from> for $to {
            #[allow(clippy::as_conversions)]
            fn try_cast_from(from: $from) -> Option<$to> {
                let to = from as $to;
                let inverse = to as $from;
                if from == inverse {
                    Some(to)
                } else {
                    None
                }
            }
        }
    };
}

try_cast_from!(f64, i64);
try_cast_from!(i64, f64);
try_cast_from!(f64, u64);
try_cast_from!(u64, f64);

/// A trait for potentially-lossy casts. Typically useful when converting from integers
/// to floating point, and you want the nearest floating-point number to your integer
/// when your integer is large.
pub trait CastLossy<T> {
    /// Perform the lossy cast.
    fn cast_lossy(from: T) -> Self;
}

impl CastLossy<usize> for f64 {
    #[allow(clippy::as_conversions)]
    fn cast_lossy(from: usize) -> Self {
        from as f64
    }
}

#[test]
fn test_try_cast_from() {
    let f64_i64_cases = vec![
        (0.0, Some(0)),
        (1.0, Some(1)),
        (1.5, None),
        (f64::INFINITY, None),
        (f64::NAN, None),
        (f64::EPSILON, None),
        (f64::MAX, None),
        (f64::MIN, None),
        (9223372036854775807f64, Some(i64::MAX)),
        (-9223372036854775808f64, Some(i64::MIN)),
        (9223372036854775807f64 + 10_000f64, None),
        (-9223372036854775808f64 - 10_000f64, None),
    ];
    let i64_f64_cases = vec![
        (0, Some(0.0)),
        (1, Some(1.0)),
        (-1, Some(-1.0)),
        (i64::MAX, Some(9223372036854775807f64)),
        (i64::MIN, Some(-9223372036854775808f64)),
        (i64::MAX - 1, None),
        (i64::MIN + 1, None),
    ];
    let f64_u64_cases = vec![
        (0.0, Some(0)),
        (1.0, Some(1)),
        (1.5, None),
        (f64::INFINITY, None),
        (f64::NAN, None),
        (f64::EPSILON, None),
        (f64::MAX, None),
        (f64::MIN, None),
        (-1.0, None),
        (18446744073709551615f64, Some(u64::MAX)),
        (18446744073709551615f64 + 10_000f64, None),
        // 2^53
        (9007199254740992f64, Some(9007199254740992)),
        // 2^53 - 1
        (9007199254740991f64, Some(9007199254740991)),
    ];
    let u64_f64_cases = vec![
        (0, Some(0.0)),
        (1, Some(1.0)),
        (u64::MAX, Some(18446744073709551615f64)),
        (u64::MAX - 1, None),
        // 2^53
        (9007199254740992, Some(9007199254740992f64)),
        // 2^53 - 1
        (9007199254740991, Some(9007199254740991f64)),
        // 2^53 + 1
        (9007199254740993, None),
    ];
    for (f, expect) in f64_i64_cases {
        let r = i64::try_cast_from(f);
        assert_eq!(r, expect, "input: {f}");
    }
    for (i, expect) in i64_f64_cases {
        let r = f64::try_cast_from(i);
        assert_eq!(r, expect, "input: {i}");
    }
    for (f, expect) in f64_u64_cases {
        let r = u64::try_cast_from(f);
        assert_eq!(r, expect, "input: {f}");
    }
    for (u, expect) in u64_f64_cases {
        let r = f64::try_cast_from(u);
        assert_eq!(r, expect, "input: {u}");
    }
}

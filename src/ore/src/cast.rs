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

use paste::paste;

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
        paste! {
            impl CastFrom<$from> for $to {
                #[allow(clippy::as_conversions)]
                fn cast_from(from: $from) -> $to {
                    from as $to
                }
            }

            /// Casts [`$from`] to [`$to`].
            ///
            /// This is equivalent to the [`CastFrom`] implementation but is
            /// available as a `const fn`.
            #[allow(clippy::as_conversions)]
            pub const fn [< $from _to_ $to >](from: $from) -> $to {
                from as $to
            }
        }
    };
}

#[cfg(any(target_pointer_width = "32", target_pointer_width = "64"))]
cast_from!(u8, usize);

#[cfg(any(target_pointer_width = "32", target_pointer_width = "64"))]
cast_from!(u32, usize);

#[cfg(target_pointer_width = "64")]
cast_from!(u64, usize);

cast_from!(usize, u64);

cast_from!(u8, i32);

#[cfg(any(target_pointer_width = "32", target_pointer_width = "64"))]
cast_from!(i32, isize);

#[cfg(target_pointer_width = "64")]
cast_from!(i64, isize);

cast_from!(isize, i64);

#[cfg(any(target_pointer_width = "32", target_pointer_width = "64"))]
cast_from!(isize, i128);

/// Returns `Some` if `f` can losslessly be converted to an i64.
#[allow(clippy::as_conversions)]
pub fn f64_to_i64(f: f64) -> Option<i64> {
    let i = f as i64;
    let i_as_f = i as f64;
    if f == i_as_f {
        Some(i)
    } else {
        None
    }
}

#[test]
fn test_f64_to_i64() {
    let cases = vec![
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
    for (f, expect) in cases {
        let r = f64_to_i64(f);
        assert_eq!(r, expect, "input: {f}");
    }
}
